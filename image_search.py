"""
IMAGE SEARCH ENGINE
===================
Search by Image untuk part — pakai DINOv2-base (Meta AI) feature
embedding (768 dim) yang disimpan di Supabase + pgvector.

Kenapa DINOv2? Self-supervised pretraining di 142M foto → robust
terhadap perubahan angle, lighting, background. Sangat cocok untuk
"cari part dengan bentuk sama, foto dari sudut berbeda".

Sumber foto: SIMS (via sims_fetcher.py). Admin manual pilih PN mana
yang mau di-index. Foto upload manual di tabel part_photos TIDAK
ikut di-index.

Setup:
  1. Jalankan migrations/003_switch_to_dinov2.sql di Supabase SQL Editor.
  2. Pastikan torch + torchvision + Pillow + requests sudah ter-install.
  3. First-run akan download DINOv2-base ~85 MB dari Meta GitHub.

Cara pakai di app.py:
  from image_search import render_search_image_tab
  # tambahkan tab ("tab_search_image", "🖼️ Cari by Foto", "__search_image__")

Public API:
  - compute_embedding(image_bytes) -> list[float]
  - index_part_number(pn, indexed_by) -> dict
  - search_by_image(image_bytes, top_k, threshold) -> list[dict]
  - delete_pn_from_index(pn) -> bool
  - list_indexed_pns(query, limit) -> list[dict]
  - get_index_stats() -> dict
  - render_search_image_tab()   # UI user
"""

from __future__ import annotations

import io
import os
import re
import time
import hashlib
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
import streamlit as st

# ── Lazy import torch — supaya error message bagus kalau belum install ────
try:
    import torch
    from torch import nn
    from torchvision import transforms
    from PIL import Image
    _TORCH_AVAILABLE = True
    _TORCH_ERR = None
except Exception as e:
    _TORCH_AVAILABLE = False
    _TORCH_ERR = str(e)


# ══════════════════════════════════════════════════════════════════════════
#  KONSTANTA
# ══════════════════════════════════════════════════════════════════════════

INDEX_TABLE       = "part_image_index"
RPC_SEARCH        = "match_part_images"
RPC_STATS         = "image_index_stats"
EMBEDDING_DIM     = 768
DEFAULT_TOP_K     = 10
DEFAULT_THRESHOLD = 0.50   # similarity ≥ 50% (cosine distance ≤ 0.50)
                           # DINOv2 untuk visual similarity sparepart
                           # umumnya menghasilkan score 0.5–0.85, jadi 0.5 baseline OK.
HTTP_TIMEOUT      = 20

DINOV2_MODEL_NAME = "dinov2_vitb14"   # base size, 768 dim, ~85 MB
DINOV2_REPO       = "facebookresearch/dinov2"
DINOV2_INPUT_SIZE = 224               # patch 14 × 16 = 224

# Parallelism config — di-tune untuk CPU laptop biasa
DOWNLOAD_WORKERS  = 8                 # foto SIMS download paralel
EMBED_BATCH_SIZE  = 8                 # DINOv2 forward pass per batch
                                      # (batch 8 ≈ 1.5GB RAM saat inference)

# Disk cache foto SIMS — sekali download, re-index PN sama jadi instant
SIMS_CACHE_DIR    = Path("images/sims_cache")
DOWNLOAD_RETRIES  = 1                 # 1 retry → max 2 attempt per URL


# ══════════════════════════════════════════════════════════════════════════
#  CONFIG HELPERS (sama pattern dengan admin_foto_part.py)
# ══════════════════════════════════════════════════════════════════════════

def _get_cfg() -> dict:
    try:
        cfg = st.secrets.get("supabase", {})
        url = cfg.get("url", "").rstrip("/")
        if url.endswith("/rest/v1"):
            url = url[: -len("/rest/v1")]
        return {
            "url":         url,
            "key":         cfg.get("key", ""),
            "service_key": cfg.get("service_key", "") or cfg.get("key", ""),
        }
    except Exception:
        return {"url": "", "key": "", "service_key": ""}


def _is_configured() -> bool:
    c = _get_cfg()
    return bool(c["url"] and c["key"] and "supabase.co" in c["url"])


def _rest_headers(use_service: bool = True) -> dict:
    cfg = _get_cfg()
    key = cfg["service_key"] if use_service and cfg["service_key"] else cfg["key"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }


def _rest_url(table: str) -> str:
    return f"{_get_cfg()['url']}/rest/v1/{table}"


def _rpc_url(fn_name: str) -> str:
    return f"{_get_cfg()['url']}/rest/v1/rpc/{fn_name}"


# ══════════════════════════════════════════════════════════════════════════
#  MODEL LOADING (cached singleton)
# ══════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner="🧠 Memuat model AI (DINOv2-base)...")
def _load_model():
    """
    Load DINOv2-base dari Meta AI via torch.hub.

    Output: CLS token feature 768-dim per gambar, sangat robust terhadap
    angle, lighting, dan background — cocok untuk visual similarity
    sparepart fisik yang sama tapi difoto berbeda.

    First-run akan download:
      - Repo facebookresearch/dinov2 (~1 MB code)
      - Model weights dinov2_vitb14 (~85 MB)
    di-cache di ~/.cache/torch/hub/.
    """
    if not _TORCH_AVAILABLE:
        raise RuntimeError(f"torch/torchvision belum terinstall: {_TORCH_ERR}")

    # trust_repo=True diperlukan karena DINOv2 dari pihak ke-3.
    # skip_validation=True hindari panggilan ke GitHub API yang kadang lambat.
    try:
        model = torch.hub.load(
            DINOV2_REPO, DINOV2_MODEL_NAME,
            trust_repo=True, skip_validation=True,
        )
    except TypeError:
        # versi torch lama tidak punya skip_validation
        model = torch.hub.load(DINOV2_REPO, DINOV2_MODEL_NAME, trust_repo=True)

    model.eval()

    # DINOv2 menggunakan ImageNet mean/std untuk normalisasi.
    preprocess = transforms.Compose([
        transforms.Resize(256, interpolation=transforms.InterpolationMode.BICUBIC),
        transforms.CenterCrop(DINOV2_INPUT_SIZE),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225],
        ),
    ])

    return model, preprocess


# ══════════════════════════════════════════════════════════════════════════
#  EMBEDDING
# ══════════════════════════════════════════════════════════════════════════

def compute_embedding(image_bytes: bytes) -> list[float]:
    """
    Hitung embedding vector (768 dim, L2-normalized) dari image bytes.
    L2 normalize → cosine distance = euclidean distance² / 2.
    """
    if not _TORCH_AVAILABLE:
        raise RuntimeError(f"torch tidak tersedia: {_TORCH_ERR}")

    model, preprocess = _load_model()

    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    tensor = preprocess(img).unsqueeze(0)   # [1, 3, H, W]

    with torch.no_grad():
        feat = model(tensor)                # [1, 768] — DINOv2 CLS token
        feat = nn.functional.normalize(feat, p=2, dim=1)
        vec  = feat.squeeze(0).cpu().tolist()

    return vec


def compute_embeddings_batch(image_bytes_list: list[bytes]) -> list[Optional[list[float]]]:
    """
    Batch version: hitung embedding untuk banyak foto dalam 1 forward pass.
    Jauh lebih cepat dari panggil compute_embedding berulang (1 model call
    vs N calls, plus batch parallelism di dalam tensor ops).

    Return: list dengan panjang sama dengan input. Item None = foto gagal
    di-decode (bytes corrupt/bukan gambar).
    """
    if not _TORCH_AVAILABLE:
        raise RuntimeError(f"torch tidak tersedia: {_TORCH_ERR}")

    if not image_bytes_list:
        return []

    model, preprocess = _load_model()

    # ── Step 1: decode & preprocess (sequential, fast) ──
    tensors:  list = []
    valid_ix: list[int] = []
    for i, b in enumerate(image_bytes_list):
        try:
            img = Image.open(io.BytesIO(b)).convert("RGB")
            tensors.append(preprocess(img))
            valid_ix.append(i)
        except Exception as e:
            print(f"[image_search] decode error #{i}: {e}")

    out: list = [None] * len(image_bytes_list)
    if not tensors:
        return out

    # ── Step 2: chunked batch inference (control RAM) ──
    batch_tensor = torch.stack(tensors)    # [N, 3, 224, 224]
    all_feats: list = []
    with torch.no_grad():
        for start in range(0, batch_tensor.shape[0], EMBED_BATCH_SIZE):
            chunk = batch_tensor[start:start + EMBED_BATCH_SIZE]
            feat  = model(chunk)                          # [b, 768]
            feat  = nn.functional.normalize(feat, p=2, dim=1)
            all_feats.append(feat.cpu())

    feats_cat = torch.cat(all_feats, dim=0)               # [N, 768]
    for j, src_ix in enumerate(valid_ix):
        out[src_ix] = feats_cat[j].tolist()

    return out


def _cache_path_for_url(url: str) -> Path:
    """Path file cache untuk URL (hash 16-char dari URL → nama file)."""
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
    return SIMS_CACHE_DIR / f"{h}.bin"


def _read_cache(url: str) -> Optional[bytes]:
    """Baca foto dari disk cache (kalau ada)."""
    p = _cache_path_for_url(url)
    if p.exists() and p.stat().st_size > 100:
        try:
            return p.read_bytes()
        except Exception:
            pass
    return None


def _write_cache(url: str, content: bytes) -> None:
    """Simpan foto ke disk cache."""
    try:
        SIMS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _cache_path_for_url(url).write_bytes(content)
    except Exception as e:
        print(f"[image_search] cache write fail: {e}")


def _download_image(url: str, use_cache: bool = True) -> Optional[bytes]:
    """
    Download foto dari URL dengan disk cache + retry.

    Flow:
      1. Cek disk cache → kalau ada, return langsung (super cepat)
      2. Download dari URL (dengan Bearer token kalau SIMS)
      3. Kalau timeout/error, retry 1x
      4. Kalau sukses, simpan ke disk cache untuk next time
    """
    # ── Step 1: cache hit? ──
    if use_cache:
        cached = _read_cache(url)
        if cached:
            return cached

    # ── Step 2: prepare headers (SIMS auth kalau perlu) ──
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        from sims_fetcher import _get_token, SIMS_BASE_URL
        sims_host = SIMS_BASE_URL.replace("http://", "").replace("https://", "").split("/")[0]
        if sims_host in url or "simscloud" in url or "cnhtcerp" in url:
            headers["Authorization"] = _get_token()
            headers["Referer"]       = SIMS_BASE_URL + "/"
            headers["Origin"]        = SIMS_BASE_URL
            headers["language"]      = "en"
    except Exception as e:
        print(f"[image_search] auth header skip: {e}")

    # ── Step 3: download dengan retry ──
    last_err = ""
    for attempt in range(DOWNLOAD_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200 and resp.content:
                content_type = resp.headers.get("Content-Type", "")
                is_image = (
                    any(t in content_type for t in ("image", "octet-stream",
                                                    "jpeg", "png", "gif", "webp"))
                    or len(resp.content) > 1000
                )
                if is_image:
                    if use_cache:
                        _write_cache(url, resp.content)
                    return resp.content
                last_err = f"bukan gambar (CT={content_type})"
                break   # tidak retry kalau response valid tapi bukan gambar
            last_err = f"HTTP {resp.status_code}"
        except requests.exceptions.Timeout:
            last_err = "timeout"
        except Exception as e:
            last_err = f"err: {e}"

        if attempt < DOWNLOAD_RETRIES:
            time.sleep(0.5)   # short backoff sebelum retry

    print(f"[image_search] {last_err}: {url[:60]}…")
    return None


def _fetch_indexed_urls_for_pn(pn: str) -> set[str]:
    """Ambil daftar URL yang sudah ter-index di DB untuk PN ini."""
    if not _is_configured() or not pn:
        return set()
    try:
        resp = requests.get(
            _rest_url(INDEX_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params={
                "select":      "sims_url",
                "part_number": f"eq.{pn.strip().upper()}",
            },
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return set()
        return {r.get("sims_url", "") for r in (resp.json() or []) if r.get("sims_url")}
    except Exception as e:
        print(f"[image_search] fetch indexed urls error: {e}")
        return set()


# ══════════════════════════════════════════════════════════════════════════
#  INDEXING — INSERT / UPSERT
# ══════════════════════════════════════════════════════════════════════════

def _vec_to_str(vec: list[float]) -> str:
    """
    Format vector untuk pgvector via PostgREST: '[0.1,0.2,...]'.
    PostgREST tidak otomatis cast JSON array → vector, harus string.
    """
    return "[" + ",".join(f"{x:.7g}" for x in vec) + "]"


def _upsert_embedding(pn: str, sims_url: str, embedding: list[float],
                      indexed_by: str) -> tuple[bool, str]:
    """
    Insert/update 1 baris di part_image_index (on_conflict=pn,url).
    Return (ok, error_message). error_message kosong kalau ok.
    """
    try:
        resp = requests.post(
            _rest_url(INDEX_TABLE),
            headers={
                **_rest_headers(use_service=True),
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": "part_number,sims_url"},
            json={
                "part_number": pn.strip().upper(),
                "sims_url":    sims_url,
                "embedding":   _vec_to_str(embedding),
                "indexed_by":  indexed_by or "admin",
            },
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code in (200, 201, 204):
            return True, ""
        err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        print(f"[image_search] upsert fail — {err}")
        return False, err
    except Exception as e:
        err = f"exception: {e}"
        print(f"[image_search] upsert error: {err}")
        return False, err


def _upsert_embeddings_bulk(rows: list[dict]) -> tuple[int, str]:
    """
    Bulk upsert N baris ke part_image_index dalam 1 request.
    rows: [{"part_number": ..., "sims_url": ..., "embedding": [...], "indexed_by": ...}, ...]
    Return: (n_ok, error_message). n_ok = jumlah baris yang berhasil
    (PostgREST tidak return per-row status, jadi all-or-nothing).
    """
    if not rows:
        return 0, ""

    payload = [{
        "part_number": r["part_number"].strip().upper(),
        "sims_url":    r["sims_url"],
        "embedding":   _vec_to_str(r["embedding"]),
        "indexed_by":  r.get("indexed_by") or "admin",
    } for r in rows]

    try:
        resp = requests.post(
            _rest_url(INDEX_TABLE),
            headers={
                **_rest_headers(use_service=True),
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": "part_number,sims_url"},
            json=payload,
            timeout=HTTP_TIMEOUT * 2,
        )
        if resp.status_code in (200, 201, 204):
            return len(rows), ""
        err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        print(f"[image_search] bulk upsert fail — {err}")
        return 0, err
    except Exception as e:
        err = f"exception: {e}"
        print(f"[image_search] bulk upsert error: {err}")
        return 0, err


def index_part_number(pn: str, indexed_by: str = "admin",
                      force_reindex: bool = False) -> dict:
    """
    Proses 1 PN: ambil foto dari SIMS → compute embedding → simpan ke DB.

    Optimasi:
      - Skip URL yang sudah ter-index di DB (kecuali force_reindex=True)
      - Disk cache foto SIMS (re-download dihindari)
      - Parallel download + batch inference + bulk upsert

    Return dict:
      {
        "ok":         bool,
        "pn":         str,
        "n_photos":   int,     # jumlah foto SIMS yang ditemukan
        "n_indexed":  int,     # jumlah berhasil di-embed & disimpan
        "n_skipped":  int,     # jumlah foto yang skip (sudah ter-index)
        "error":      str,     # kalau ok=False atau parsial
      }
    """
    result = {"ok": False, "pn": pn.strip().upper(),
              "n_photos": 0, "n_indexed": 0, "n_skipped": 0, "error": ""}

    if not _is_configured():
        result["error"] = "Supabase belum dikonfigurasi."
        return result

    if not _TORCH_AVAILABLE:
        result["error"] = f"torch tidak tersedia: {_TORCH_ERR}"
        return result

    pn_clean = pn.strip().upper()
    if not pn_clean:
        result["error"] = "Part Number kosong."
        return result

    # ── Ambil foto dari SIMS ──
    try:
        from sims_fetcher import fetch_sims_images
    except Exception as e:
        result["error"] = f"sims_fetcher tidak tersedia: {e}"
        return result

    try:
        urls = fetch_sims_images(pn_clean) or []
    except Exception as e:
        result["error"] = f"Gagal ambil foto SIMS: {e}"
        return result

    result["n_photos"] = len(urls)
    if not urls:
        result["error"] = "Tidak ada foto SIMS untuk PN ini."
        return result

    # ── Step 0: skip URL yang sudah ter-index di DB ──
    if not force_reindex:
        already_indexed = _fetch_indexed_urls_for_pn(pn_clean)
        if already_indexed:
            new_urls = [u for u in urls if u not in already_indexed]
            result["n_skipped"] = len(urls) - len(new_urls)
            urls = new_urls

    if not urls:
        # Semua sudah ter-index sebelumnya — itu sukses, bukan error
        result["ok"]        = True
        result["n_indexed"] = 0
        result["error"]     = f"Semua {result['n_skipped']} foto sudah ter-index sebelumnya."
        return result

    n_err   = 0
    err_log = []

    # ── Step A: parallel download foto SIMS (I/O bound) ──
    # Gunakan thread pool — gain terbesar di sini karena network latency.
    bytes_map: dict[str, bytes] = {}   # {url: img_bytes}
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as ex:
        future_to_url = {ex.submit(_download_image, u): u for u in urls}
        for fut in as_completed(future_to_url):
            u = future_to_url[fut]
            try:
                b = fut.result()
            except Exception as e:
                b = None
                err_log.append(f"download exc: {e}")
            if b:
                bytes_map[u] = b
            else:
                n_err += 1
                err_log.append(f"download gagal: {u[:50]}…")

    if not bytes_map:
        result["n_indexed"] = 0
        result["ok"]        = False
        result["error"]     = "Semua download foto gagal. " + " | ".join(err_log[:3])
        return result

    # ── Step B: batch embedding DINOv2 (CPU bound, but batched) ──
    # Pertahankan urutan url:bytes saat memanggil batch — supaya hasil
    # bisa dipasangkan kembali ke URL aslinya.
    ordered_urls   = list(bytes_map.keys())
    ordered_bytes  = [bytes_map[u] for u in ordered_urls]
    try:
        embeddings = compute_embeddings_batch(ordered_bytes)
    except Exception as e:
        result["error"] = f"Batch embed error: {e}"
        return result

    # ── Step C: rakit row untuk bulk upsert ──
    rows_to_upsert: list[dict] = []
    for u, vec in zip(ordered_urls, embeddings):
        if vec is None:
            n_err += 1
            err_log.append(f"embed gagal: {u[:50]}…")
            continue
        rows_to_upsert.append({
            "part_number": pn_clean,
            "sims_url":    u,
            "embedding":   vec,
            "indexed_by":  indexed_by,
        })

    # ── Step D: bulk upsert (1 round-trip Supabase) ──
    n_ok = 0
    if rows_to_upsert:
        n_ok, up_err = _upsert_embeddings_bulk(rows_to_upsert)
        if n_ok < len(rows_to_upsert):
            n_err += (len(rows_to_upsert) - n_ok)
            err_log.append(f"bulk upsert: {up_err or 'partial'}")

    result["n_indexed"] = n_ok
    result["ok"]        = n_ok > 0
    if n_err > 0:
        result["error"] = f"{n_err} foto gagal diproses. " + " | ".join(err_log[:3])

    return result


def index_bulk(pn_list: list[str], indexed_by: str = "admin",
               progress_callback=None) -> list[dict]:
    """
    Index banyak PN sekaligus. progress_callback(i, total, pn, result) — opsional.
    Return list of dict (1 per PN).
    """
    pn_clean = [p.strip().upper() for p in pn_list if p and p.strip()]
    pn_clean = list(dict.fromkeys(pn_clean))   # dedupe, preserve order

    results = []
    total   = len(pn_clean)
    for i, pn in enumerate(pn_clean, start=1):
        r = index_part_number(pn, indexed_by=indexed_by)
        results.append(r)
        if progress_callback:
            try:
                progress_callback(i, total, pn, r)
            except Exception:
                pass
    return results


# ══════════════════════════════════════════════════════════════════════════
#  SEARCH
# ══════════════════════════════════════════════════════════════════════════

def search_by_image(image_bytes: bytes,
                    top_k: int = DEFAULT_TOP_K,
                    threshold: float = DEFAULT_THRESHOLD) -> list[dict]:
    """
    Cari part berdasarkan foto query.

    Args:
        image_bytes  — foto query (jpg/png/webp bytes)
        top_k        — jumlah hasil max
        threshold    — similarity minimum (0.0–1.0); 0.70 = 70%

    Return list of dict (urut similarity desc):
      [{ "part_number": str, "sims_url": str, "similarity": float, "distance": float }, ...]
    """
    if not _is_configured():
        return []

    try:
        query_vec = compute_embedding(image_bytes)
    except Exception as e:
        print(f"[image_search] compute_embedding error: {e}")
        return []

    # threshold similarity 0.70  →  cosine distance < 0.30
    distance_threshold = max(0.0, min(1.0 - threshold, 2.0))

    try:
        resp = requests.post(
            _rpc_url(RPC_SEARCH),
            headers=_rest_headers(use_service=True),
            json={
                "query_embedding": _vec_to_str(query_vec),
                "match_threshold": distance_threshold,
                "match_count":     int(top_k),
            },
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            print(f"[image_search] RPC search status={resp.status_code}: {resp.text[:200]}")
            return []
        rows = resp.json() or []
    except Exception as e:
        print(f"[image_search] RPC search error: {e}")
        return []

    out = []
    for r in rows:
        out.append({
            "part_number": r.get("part_number", ""),
            "sims_url":    r.get("sims_url", ""),
            "similarity":  float(r.get("similarity") or 0.0),
            "distance":    float(r.get("distance")   or 0.0),
        })
    return out


# ══════════════════════════════════════════════════════════════════════════
#  SMART FILTER — adaptive cutoff tanpa threshold manual
# ══════════════════════════════════════════════════════════════════════════

# Konstanta filter — dipilih berdasarkan karakter DINOv2 untuk sparepart:
# noise floor 55% (di bawah itu = bentuk silinder/balok generik),
# cliff 8% (gap antar hasil yang nyata = beda kategori part),
# relative drop 15% (lebih dari ini dari top = different family).
_SMART_ABS_FLOOR = 0.55
_SMART_REL_DROP  = 0.15
_SMART_CLIFF     = 0.08
_SMART_HIGH_CONF = 0.80


def smart_filter_results(results: list[dict]) -> dict:
    """
    Filter hasil pencarian otomatis tanpa threshold manual.

    Mekanisme (semua kondisi dicek per item; cut-off saat salah satu kena):
      1. Absolute floor: skip item < 55% (DINOv2 noise zone)
      2. Relative drop: skip item > 15% lebih rendah dari top hit
      3. Cliff: skip kalau ada gap > 8% dari item sebelumnya,
         KECUALI item masih di zona high-confidence (≥80%) — di zona ini
         semua hasil di-keep karena gap kecil tetap match valid.
      4. Kalau top hit < 55% → return kosong (tidak ada match meyakinkan)

    Return:
      {
        "kept":    list[dict],   # hasil yang lolos filter
        "dropped": list[dict],   # sisanya (jadi fallback kandidat)
        "reason":  str,          # alasan cut-off untuk caption UI
        "mode":    str,          # "strong" | "moderate" | "weak" | "none"
        "top_sim": float,        # similarity item teratas (0.0 kalau kosong)
      }
    """
    if not results:
        return {"kept": [], "dropped": [], "reason": "tidak ada hasil",
                "mode": "none", "top_sim": 0.0}

    sorted_r = sorted(results, key=lambda x: x.get("similarity", 0.0), reverse=True)
    top_sim  = sorted_r[0]["similarity"]

    # Top hit terlalu lemah → tidak ada match yang meyakinkan
    if top_sim < _SMART_ABS_FLOOR:
        return {
            "kept":    [],
            "dropped": sorted_r,
            "reason":  f"similarity tertinggi hanya {top_sim*100:.1f}% — tidak ada match meyakinkan",
            "mode":    "none",
            "top_sim": top_sim,
        }

    kept:    list[dict] = [sorted_r[0]]
    dropped: list[dict] = []
    reason  = "semua hasil lolos filter"

    for i, r in enumerate(sorted_r[1:], start=1):
        sim      = r["similarity"]
        prev_sim = kept[-1]["similarity"]

        if sim < _SMART_ABS_FLOOR:
            reason  = f"sisa hasil di bawah noise floor ({_SMART_ABS_FLOOR*100:.0f}%)"
            dropped = sorted_r[i:]
            break
        if (top_sim - sim) > _SMART_REL_DROP:
            reason  = f"selisih > {_SMART_REL_DROP*100:.0f}% dari top — bukan family yang sama"
            dropped = sorted_r[i:]
            break
        if (prev_sim - sim) > _SMART_CLIFF and sim < _SMART_HIGH_CONF:
            reason  = f"jurang similarity terdeteksi ({prev_sim*100:.1f}% → {sim*100:.1f}%)"
            dropped = sorted_r[i:]
            break
        kept.append(r)

    if top_sim >= 0.85:
        mode = "strong"
    elif top_sim >= 0.70:
        mode = "moderate"
    else:
        mode = "weak"

    return {
        "kept":    kept,
        "dropped": dropped,
        "reason":  reason,
        "mode":    mode,
        "top_sim": top_sim,
    }


# ══════════════════════════════════════════════════════════════════════════
#  ADMIN OPERATIONS — list / delete / stats
# ══════════════════════════════════════════════════════════════════════════

def list_indexed_pns(query: str = "", limit: int = 200) -> list[dict]:
    """
    Daftar PN yang sudah di-index, dengan jumlah foto-nya.
    Filter optional pakai query (ilike on part_number).
    """
    if not _is_configured():
        return []

    params = {
        "select": "part_number,sims_url,indexed_by,indexed_at",
        "order":  "indexed_at.desc",
        "limit":  str(limit * 5),   # 1 PN bisa multi-row → ambil lebih
    }
    q = query.strip().upper()
    if q:
        params["part_number"] = f"ilike.*{q}*"

    try:
        resp = requests.get(
            _rest_url(INDEX_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return []
        rows = resp.json() or []
    except Exception as e:
        print(f"[image_search] list error: {e}")
        return []

    # Group per PN
    grouped: dict[str, dict] = {}
    for r in rows:
        pn = r.get("part_number", "")
        if not pn:
            continue
        g = grouped.setdefault(pn, {
            "part_number": pn,
            "n_photos":    0,
            "indexed_by":  r.get("indexed_by") or "",
            "indexed_at":  r.get("indexed_at") or "",
        })
        g["n_photos"] += 1
        # ambil indexed_at terbaru
        if (r.get("indexed_at") or "") > g["indexed_at"]:
            g["indexed_at"] = r["indexed_at"]

    out = sorted(grouped.values(),
                 key=lambda x: x["indexed_at"], reverse=True)
    return out[:limit]


def get_all_indexed_pns() -> set[str]:
    """
    Ambil SET semua part_number yang sudah ter-index di Supabase.
    Dipakai untuk "fast mode" bulk indexing — cek PN existing 1× di awal,
    skip yang sudah ada tanpa harus fetch SIMS per PN.

    Return set kosong kalau Supabase belum dikonfigurasi atau error.
    """
    if not _is_configured():
        return set()
    try:
        resp = requests.get(
            _rest_url(INDEX_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params={
                "select": "part_number",
                "limit":  "100000",   # cukup untuk ribuan PN
            },
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return set()
        rows = resp.json() or []
        return {r["part_number"] for r in rows if r.get("part_number")}
    except Exception as e:
        print(f"[image_search] get_all_indexed_pns error: {e}")
        return set()


def delete_pn_from_index(pn: str) -> bool:
    """Hapus semua baris part_image_index untuk PN tertentu."""
    if not _is_configured() or not pn:
        return False
    try:
        resp = requests.delete(
            _rest_url(INDEX_TABLE),
            headers={**_rest_headers(use_service=True), "Prefer": "return=minimal"},
            params={"part_number": f"eq.{pn.strip().upper()}"},
            timeout=HTTP_TIMEOUT,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        print(f"[image_search] delete error: {e}")
        return False


def get_index_stats() -> dict:
    """Statistik global index: total PN, total foto, last_indexed_at."""
    default = {"total_pn": 0, "total_images": 0, "last_indexed_at": None}
    if not _is_configured():
        return default
    try:
        resp = requests.post(
            _rpc_url(RPC_STATS),
            headers=_rest_headers(use_service=True),
            json={},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            return default
        rows = resp.json() or []
        if not rows:
            return default
        row = rows[0] if isinstance(rows, list) else rows
        return {
            "total_pn":        int(row.get("total_pn") or 0),
            "total_images":    int(row.get("total_images") or 0),
            "last_indexed_at": row.get("last_indexed_at"),
        }
    except Exception as e:
        print(f"[image_search] stats error: {e}")
        return default


# ══════════════════════════════════════════════════════════════════════════
#  STREAMLIT UI — TAB USER: "Cari by Foto"
# ══════════════════════════════════════════════════════════════════════════

_SS_QUERY_BYTES   = "_img_search_query_bytes"
_SS_QUERY_NAME    = "_img_search_query_name"
_SS_RESULTS       = "_img_search_results"


def _try_get_part_name(pn: str) -> str:
    """Best-effort: ambil part name dari session excel_files cache."""
    try:
        excel_files = st.session_state.get("excel_files", [])
        pn_upper = pn.strip().upper()
        for fi in excel_files:
            idx_map = fi.get("part_number_index", {})
            if pn_upper in idx_map:
                row = fi["dataframe"].iloc[idx_map[pn_upper][0]]
                return str(row.get("part_name", "")).strip()
    except Exception:
        pass
    return ""


def render_search_image_tab():
    """Tab user: Cari Part by Foto."""
    st.markdown("### 🖼️ Cari Part by Foto")
    st.caption(
        "Upload foto part, sistem akan mencari Part Number yang paling mirip "
        "berdasarkan visual (bentuk, warna, tekstur)."
    )

    if not _TORCH_AVAILABLE:
        st.error(
            f"❌ **PyTorch belum terinstall.** {_TORCH_ERR}\n\n"
            "Jalankan: `pip install torch torchvision`"
        )
        return

    if not _is_configured():
        st.error(
            "❌ **Supabase belum dikonfigurasi.**\n\n"
            "Tambahkan `[supabase]` url, key, dan service_key di `.streamlit/secrets.toml`."
        )
        return

    # ── Statistik cepat di atas ──
    stats = get_index_stats()
    col_s1, col_s2 = st.columns(2)
    col_s1.metric("PN ter-index", f"{stats['total_pn']:,}")
    col_s2.metric("Total foto", f"{stats['total_images']:,}")

    if stats["total_pn"] == 0:
        st.info(
            "ℹ️ Belum ada PN yang di-index. "
            "Admin harus menambahkan PN di tab **🧠 Image Index** terlebih dahulu."
        )
        return

    st.markdown("---")

    # ── Upload foto ──
    col_upload, col_preview = st.columns([3, 2])

    with col_upload:
        uploaded = st.file_uploader(
            "📤 Upload foto part:",
            type=["jpg", "jpeg", "png", "webp"],
            key="img_search_uploader",
            help="Format: JPG, JPEG, PNG, WEBP",
        )

        if uploaded is not None:
            st.session_state[_SS_QUERY_BYTES] = uploaded.read()
            st.session_state[_SS_QUERY_NAME]  = uploaded.name

        top_k = st.slider(
            "Jumlah kandidat dipertimbangkan:",
            min_value=5, max_value=30,
            value=DEFAULT_TOP_K, step=1,
            key="img_search_topk",
            help="Smart filter akan menyaring otomatis dari kandidat ini.",
        )
        st.caption(
            "🤖 **Smart filter aktif** — hasil tidak relevan disaring otomatis "
            "berdasarkan distribusi similarity (deteksi noise & jurang score)."
        )

        col_b1, col_b2 = st.columns([3, 1])
        with col_b1:
            do_search = st.button(
                "🔍 Cari Part", type="primary",
                use_container_width=True,
                disabled=not st.session_state.get(_SS_QUERY_BYTES),
                key="btn_img_search",
            )
        with col_b2:
            if st.button("✖", use_container_width=True,
                         key="btn_img_clear", help="Reset"):
                st.session_state.pop(_SS_QUERY_BYTES, None)
                st.session_state.pop(_SS_QUERY_NAME,  None)
                st.session_state.pop(_SS_RESULTS,     None)
                st.rerun()

    with col_preview:
        q_bytes = st.session_state.get(_SS_QUERY_BYTES)
        q_name  = st.session_state.get(_SS_QUERY_NAME, "")
        if q_bytes:
            st.markdown("**Preview foto query:**")
            st.image(q_bytes, use_container_width=True, caption=q_name[:40])

    # ── Eksekusi search ──
    if do_search and q_bytes:
        with st.spinner("🔍 Menghitung embedding & mencari di database..."):
            t0 = time.time()
            # Ambil kandidat mentah tanpa threshold — biar smart filter
            # punya distribusi lengkap untuk deteksi cliff/noise.
            raw = search_by_image(
                q_bytes,
                top_k=int(top_k),
                threshold=0.0,
            )
            filtered = smart_filter_results(raw)
            elapsed  = time.time() - t0

        st.session_state[_SS_RESULTS]            = filtered["kept"]
        st.session_state["_img_search_fallback"] = filtered["dropped"][:5]
        st.session_state["_img_search_filter_reason"] = filtered["reason"]
        st.session_state["_img_search_filter_mode"]   = filtered["mode"]
        st.session_state["_img_search_top_sim"]       = filtered["top_sim"]
        st.session_state["_img_search_elapsed"]       = elapsed
        st.rerun()

    # ── Tampilkan hasil ──
    results = st.session_state.get(_SS_RESULTS)
    if results is None:
        return

    elapsed       = st.session_state.get("_img_search_elapsed", 0.0)
    filter_mode   = st.session_state.get("_img_search_filter_mode", "none")
    filter_reason = st.session_state.get("_img_search_filter_reason", "")
    top_sim       = st.session_state.get("_img_search_top_sim", 0.0)
    fallback      = st.session_state.get("_img_search_fallback", [])

    st.markdown("---")
    st.markdown(f"### 📋 Hasil Pencarian ({len(results)} ditemukan, {elapsed:.2f}s)")

    # Badge confidence dari smart filter
    if filter_mode == "strong":
        st.success(f"✅ **Match kuat** — top similarity {top_sim*100:.1f}%. Filter: {filter_reason}.")
    elif filter_mode == "moderate":
        st.info(f"ℹ️ **Match sedang** — top similarity {top_sim*100:.1f}%. Filter: {filter_reason}.")
    elif filter_mode == "weak":
        st.warning(f"⚠️ **Match lemah** — top similarity hanya {top_sim*100:.1f}%. Filter: {filter_reason}.")

    if not results:
        if fallback:
            best_sim = fallback[0]["similarity"] * 100
            st.warning(
                f"⚠️ **Tidak ada match meyakinkan.** Kandidat terdekat hanya **{best_sim:.1f}%** — "
                f"kemungkinan part belum di-index, atau foto query terlalu beda dari foto SIMS."
            )
            with st.expander(f"🔍 Lihat {len(fallback)} kandidat terdekat (low confidence)", expanded=False):
                for i in range(0, len(fallback), 2):
                    cols = st.columns(2)
                    for j, col in enumerate(cols):
                        idx = i + j
                        if idx >= len(fallback):
                            continue
                        with col:
                            _render_result_card(idx + 1, fallback[idx])
        else:
            st.warning(
                "⚠️ Tidak ada hasil sama sekali. "
                "Pastikan sudah ada PN yang di-index di tab **🧠 Image Index**."
            )
        return

    # Grid 2 kolom untuk hasil lolos filter
    for i in range(0, len(results), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(results):
                continue
            r = results[idx]
            with col:
                _render_result_card(idx + 1, r)

    # Show dropped candidates di expander (transparansi — user tahu apa yang disaring)
    if fallback:
        with st.expander(
            f"👁️ Lihat {len(fallback)} kandidat yang disaring smart filter",
            expanded=False,
        ):
            st.caption(f"Disaring karena: {filter_reason}")
            for i in range(0, len(fallback), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx >= len(fallback):
                        continue
                    with col:
                        _render_result_card(len(results) + idx + 1, fallback[idx])


def _render_result_card(rank: int, r: dict):
    """Render 1 kartu hasil pencarian."""
    pn         = r["part_number"]
    sim_pct    = r["similarity"] * 100.0
    sims_url   = r["sims_url"]
    part_name  = _try_get_part_name(pn) or "—"

    # Border + bg color berdasarkan similarity
    if sim_pct >= 90:
        badge_color = "#16a34a"   # hijau
    elif sim_pct >= 80:
        badge_color = "#0891b2"   # cyan
    elif sim_pct >= 70:
        badge_color = "#d97706"   # amber
    else:
        badge_color = "#6b7280"   # abu

    st.markdown(
        f"""
        <div style="border:1px solid #e5e7eb; border-radius:10px;
                    padding:10px; margin-bottom:8px; background:#fafafa;">
          <div style="display:flex; justify-content:space-between; align-items:center;">
            <div style="font-weight:600; font-size:15px;">#{rank} &nbsp; {pn}</div>
            <div style="background:{badge_color}; color:white; padding:2px 10px;
                        border-radius:12px; font-weight:600; font-size:13px;">
              {sim_pct:.1f}%
            </div>
          </div>
          <div style="color:#374151; font-size:13px; margin-top:4px;">{part_name}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Tampilkan foto SIMS (pakai _download_image agar header SIMS otomatis)
    if sims_url:
        img_bytes = _download_image(sims_url)
        if img_bytes:
            st.image(img_bytes, use_container_width=True)
        else:
            st.caption("⚠️ Foto gagal dimuat")

    # Tombol "Cari detail PN ini" → trigger search PN di tab utama
    if st.button(f"🔎 Cari Detail {pn}", key=f"img_res_detail_{rank}_{pn}",
                 use_container_width=True):
        st.session_state["_trigger_search_pn"] = pn
        st.rerun()
