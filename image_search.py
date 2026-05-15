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
from datetime import datetime
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
    from PIL import Image, ImageOps
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

# Parallelism config — di-tune untuk Streamlit Cloud (RAM ~1 GB)
DOWNLOAD_WORKERS  = 3                 # foto SIMS download paralel (was 8)
EMBED_BATCH_SIZE  = 2                 # DINOv2 forward pass per batch (was 8)
                                      # batch 2 ≈ 400 MB RAM saat inference
                                      # (batch 8 ≈ 1.5 GB → OOM di cloud)

# Disk cache foto SIMS — sekali download, re-index PN sama jadi instant
SIMS_CACHE_DIR    = Path("images/sims_cache")
DOWNLOAD_RETRIES  = 1                 # 1 retry → max 2 attempt per URL

# Cache size management
CACHE_MAX_MB        = 500    # cap total cache (LRU eviction kalau lewat)
CACHE_TARGET_RATIO  = 0.90   # cleanup turunkan ke 90% cap
CACHE_THUMB_MAX_PX  = 512    # resize max dimension saat save ke cache
CACHE_THUMB_QUALITY = 85     # WebP quality
_CACHE_CLEANUP_EVERY = 50    # cek cleanup tiap N write

# Bulk hardening — supaya bulk PN banyak tidak gagal seluruhnya kalau ada hiccup
BULK_UPSERT_CHUNK     = 50   # potong payload bulk upsert tiap N row (hindari body-too-large)
BULK_HTTP_RETRIES     = 2    # retry Supabase REST kalau 5xx/timeout (max 3 attempt)
BULK_RETRY_BASE_SLEEP = 1.0  # detik — backoff awal, di-double per attempt
BULK_MAX_CONSEC_FAIL  = 10   # circuit-breaker: stop bulk kalau N PN beruntun fatal-error
BULK_GC_EVERY         = 25   # paksa gc.collect() tiap N PN untuk lepas RAM
BULK_INTER_PN_SLEEP   = 0.0  # detik — jeda antar PN (0 = no throttle; naikkan kalau API rate-limit)


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

    img = Image.open(io.BytesIO(image_bytes))
    img = ImageOps.exif_transpose(img).convert("RGB")
    tensor = preprocess(img).unsqueeze(0)   # [1, 3, H, W]

    with torch.no_grad():
        feat = model(tensor)                # [1, 768] — DINOv2 CLS token
        feat = nn.functional.normalize(feat, p=2, dim=1)
        if not torch.isfinite(feat).all():
            raise ValueError("embedding non-finite (NaN/Inf)")
        vec  = feat.squeeze(0).cpu().tolist()

    return vec


def compute_embedding_tta(image_bytes: bytes) -> list[float]:
    """
    Test-time augmentation: hitung embedding rata-rata dari foto asli +
    horizontal-flip, lalu re-normalize. Untuk part yang relatif simetris,
    TTA flip biasanya menambah 1–3% recall tanpa ubah index.

    Latency ≈ 2× compute_embedding (dua forward pass).
    """
    if not _TORCH_AVAILABLE:
        raise RuntimeError(f"torch tidak tersedia: {_TORCH_ERR}")

    model, preprocess = _load_model()

    img = Image.open(io.BytesIO(image_bytes))
    img = ImageOps.exif_transpose(img).convert("RGB")
    img_flipped = ImageOps.mirror(img)

    tensor = torch.stack([preprocess(img), preprocess(img_flipped)])  # [2, 3, H, W]

    with torch.no_grad():
        feats = model(tensor)                           # [2, 768]
        feats = nn.functional.normalize(feats, p=2, dim=1)
        mean  = feats.mean(dim=0, keepdim=True)         # [1, 768]
        mean  = nn.functional.normalize(mean, p=2, dim=1)
        if not torch.isfinite(mean).all():
            raise ValueError("embedding non-finite (NaN/Inf)")
        vec   = mean.squeeze(0).cpu().tolist()

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
            img = Image.open(io.BytesIO(b))
            img = ImageOps.exif_transpose(img).convert("RGB")
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
    # Skip embedding non-finite (NaN/Inf) — kalau diteruskan ke pgvector
    # akan menyebabkan distance NaN dan ranking jadi rusak.
    finite_mask = torch.isfinite(feats_cat).all(dim=1)
    for j, src_ix in enumerate(valid_ix):
        if bool(finite_mask[j]):
            out[src_ix] = feats_cat[j].tolist()
        else:
            print(f"[image_search] non-finite embedding #{src_ix} — skip")

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


_cache_write_counter = 0


def _resize_for_cache(content: bytes) -> Optional[bytes]:
    """
    Resize image ke max CACHE_THUMB_MAX_PX × CACHE_THUMB_MAX_PX, encode WebP.
    Hemat ruang ~30× tanpa kehilangan kualitas untuk thumbnail + embedding
    (DINOv2 input cuma 224 px, jadi 512 px masih oversampling).
    Return None kalau gagal — caller fallback ke bytes original.
    """
    if not _TORCH_AVAILABLE:   # PIL diimport via blok torch di atas
        return None
    try:
        img = Image.open(io.BytesIO(content)).convert("RGB")
        img.thumbnail((CACHE_THUMB_MAX_PX, CACHE_THUMB_MAX_PX), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="WEBP", quality=CACHE_THUMB_QUALITY, method=4)
        return buf.getvalue()
    except Exception as e:
        print(f"[image_search] resize for cache fail: {e}")
        return None


def _maybe_cleanup_cache(force: bool = False) -> None:
    """
    Throttled LRU eviction. Dipanggil per write tapi cek tiap N call saja.
    Saat cache > CACHE_MAX_MB, hapus file paling lama diakses sampai 90% cap.
    """
    global _cache_write_counter
    _cache_write_counter += 1
    if not force and (_cache_write_counter % _CACHE_CLEANUP_EVERY) != 0:
        return
    try:
        if not SIMS_CACHE_DIR.exists():
            return
        files: list = []
        total = 0
        for p in SIMS_CACHE_DIR.glob("*.bin"):
            try:
                st = p.stat()
                files.append((p, st.st_size, st.st_atime))
                total += st.st_size
            except Exception:
                pass
        total_mb = total / (1024 * 1024)
        if total_mb <= CACHE_MAX_MB:
            return
        target_mb = CACHE_MAX_MB * CACHE_TARGET_RATIO
        files.sort(key=lambda x: x[2])   # oldest atime dulu
        deleted = 0
        for p, sz, _ in files:
            if total_mb <= target_mb:
                break
            try:
                p.unlink()
                total_mb -= sz / (1024 * 1024)
                deleted += 1
            except Exception:
                pass
        if deleted:
            print(f"[image_search] cache cleanup: deleted {deleted} files, "
                  f"now {total_mb:.1f} MB")
    except Exception as e:
        print(f"[image_search] cleanup error: {e}")


def _write_cache(url: str, content: bytes) -> None:
    """
    Simpan foto ke disk cache — resize dulu untuk hemat ruang, lalu
    trigger LRU eviction throttled kalau cache mendekati cap.
    """
    try:
        SIMS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        small = _resize_for_cache(content)
        # Pakai versi resize HANYA kalau lebih kecil (untuk file kecil
        # original kadang lebih efisien daripada hasil encode WebP)
        payload = small if (small and len(small) < len(content)) else content
        _cache_path_for_url(url).write_bytes(payload)
        _maybe_cleanup_cache()
    except Exception as e:
        print(f"[image_search] cache write fail: {e}")


def get_sims_cache_stats() -> dict:
    """
    Statistik cache foto SIMS untuk admin UI.
    Return: {n_files, size_mb, oldest_at, max_mb, pct_used}
    """
    out = {
        "n_files":   0,
        "size_mb":   0.0,
        "oldest_at": None,
        "max_mb":    CACHE_MAX_MB,
        "pct_used":  0.0,
    }
    if not SIMS_CACHE_DIR.exists():
        return out
    oldest_atime = None
    total = 0
    for p in SIMS_CACHE_DIR.glob("*.bin"):
        try:
            st = p.stat()
            out["n_files"] += 1
            total += st.st_size
            if oldest_atime is None or st.st_atime < oldest_atime:
                oldest_atime = st.st_atime
        except Exception:
            pass
    out["size_mb"]  = total / (1024 * 1024)
    out["pct_used"] = (out["size_mb"] / CACHE_MAX_MB) * 100.0
    if oldest_atime:
        from datetime import datetime
        out["oldest_at"] = datetime.fromtimestamp(oldest_atime).strftime("%Y-%m-%d %H:%M")
    return out


def clear_sims_cache() -> dict:
    """Hapus semua file di sims_cache. Return: {n_deleted, freed_mb, error}"""
    result = {"n_deleted": 0, "freed_mb": 0.0, "error": ""}
    if not SIMS_CACHE_DIR.exists():
        return result
    total = 0
    try:
        for p in SIMS_CACHE_DIR.glob("*.bin"):
            try:
                sz = p.stat().st_size
                p.unlink()
                result["n_deleted"] += 1
                total += sz
            except Exception:
                pass
        result["freed_mb"] = total / (1024 * 1024)
    except Exception as e:
        result["error"] = str(e)
    return result


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
    """
    Ambil daftar URL yang sudah ter-index di DB untuk PN ini.
    Retry pada transient HTTP error supaya hiccup jaringan tidak men-skip
    foto yang sebenarnya sudah ter-index (kalau gagal total → return set kosong,
    konsekuensinya hanya redownload SIMS, bukan corrupt data).
    """
    if not _is_configured() or not pn:
        return set()

    last_err = ""
    for attempt in range(BULK_HTTP_RETRIES + 1):
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
            if resp.status_code == 200:
                return {r.get("sims_url", "") for r in (resp.json() or []) if r.get("sims_url")}
            last_err = f"HTTP {resp.status_code}"
            if resp.status_code not in _TRANSIENT_HTTP:
                break
        except Exception as e:
            last_err = f"err: {e}"
            if not _is_transient_exception(e):
                break

        if attempt < BULK_HTTP_RETRIES:
            _retry_sleep(attempt + 1)

    print(f"[image_search] fetch indexed urls fail ({pn}): {last_err}")
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


# Status code yang dianggap transient — boleh retry dengan backoff.
# 408 request timeout, 425 too early, 429 rate-limit, 5xx server-side.
_TRANSIENT_HTTP = {408, 425, 429, 500, 502, 503, 504, 522, 524}


def _is_transient_exception(exc: Exception) -> bool:
    """True kalau exception dari requests yang layak di-retry (network hiccup)."""
    return isinstance(exc, (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
    ))


def _retry_sleep(attempt: int) -> None:
    """Backoff exponential: 1s, 2s, 4s, ..."""
    time.sleep(BULK_RETRY_BASE_SLEEP * (2 ** max(0, attempt - 1)))


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
    Bulk upsert N baris ke part_image_index — di-chunk supaya body tidak
    kelewat besar, dan tiap chunk di-retry kalau kena error transient.

    rows: [{"part_number": ..., "sims_url": ..., "embedding": [...], "indexed_by": ...}, ...]
    Return: (n_ok, error_message). n_ok = jumlah baris yang berhasil di seluruh chunk.
    """
    if not rows:
        return 0, ""

    payload = [{
        "part_number": r["part_number"].strip().upper(),
        "sims_url":    r["sims_url"],
        "embedding":   _vec_to_str(r["embedding"]),
        "indexed_by":  r.get("indexed_by") or "admin",
    } for r in rows]

    chunk_size = max(1, int(BULK_UPSERT_CHUNK))
    n_ok       = 0
    errors: list[str] = []

    for start in range(0, len(payload), chunk_size):
        chunk = payload[start:start + chunk_size]

        last_err = ""
        for attempt in range(BULK_HTTP_RETRIES + 1):
            try:
                resp = requests.post(
                    _rest_url(INDEX_TABLE),
                    headers={
                        **_rest_headers(use_service=True),
                        "Prefer": "resolution=merge-duplicates,return=minimal",
                    },
                    params={"on_conflict": "part_number,sims_url"},
                    json=chunk,
                    timeout=HTTP_TIMEOUT * 2,
                )
                if resp.status_code in (200, 201, 204):
                    n_ok += len(chunk)
                    last_err = ""
                    break
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                # Hanya retry kalau status code transient (5xx, 429, dll).
                if resp.status_code not in _TRANSIENT_HTTP:
                    break
            except Exception as e:
                last_err = f"exception: {e}"
                if not _is_transient_exception(e):
                    break

            if attempt < BULK_HTTP_RETRIES:
                _retry_sleep(attempt + 1)

        if last_err:
            errors.append(f"chunk {start}-{start + len(chunk)}: {last_err}")
            print(f"[image_search] bulk upsert fail — {errors[-1]}")

    err_msg = " | ".join(errors[:3]) if errors else ""
    return n_ok, err_msg


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
    except MemoryError as e:
        result["error"] = f"OOM saat embed ({len(ordered_bytes)} foto): {e}"
        return result
    except Exception as e:
        result["error"] = f"Batch embed error: {e}"
        return result
    finally:
        # Foto bytes sudah ter-encode ke tensor di langkah embedding —
        # lepas ref-nya supaya RAM bebas sebelum tahap upsert (penting di Cloud).
        bytes_map.clear()
        del ordered_bytes

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
               progress_callback=None, stop_event=None) -> list[dict]:
    """
    Index banyak PN sekaligus dengan defensive handling supaya 1 PN gagal
    tidak menjatuhkan seluruh batch.

    Args:
        pn_list           — daftar PN (akan di-dedupe + uppercased)
        indexed_by        — username admin yang trigger
        progress_callback — fn(i, total, pn, result) dipanggil tiap PN selesai
        stop_event        — threading.Event opsional. Kalau is_set() → loop berhenti
                            dan PN sisa di-tandai sebagai dibatalkan.

    Hardening:
      - Tiap PN dibungkus try/except: exception apa pun jadi result dict
        gagal, bukan crash loop.
      - Circuit-breaker: kalau ada BULK_MAX_CONSEC_FAIL PN fatal-error
        beruntun → stop bulk (mencegah hammering API yang lagi down).
      - gc.collect() periodik untuk lepas RAM (penting di Streamlit Cloud).
      - Throttling antar-PN opsional (BULK_INTER_PN_SLEEP).

    Return list of dict (1 per PN). PN yang dibatalkan / kena circuit-breaker
    tetap di-list dengan flag ok=False + error penjelas.
    """
    import gc

    pn_clean = [p.strip().upper() for p in pn_list if p and p.strip()]
    pn_clean = list(dict.fromkeys(pn_clean))   # dedupe, preserve order

    results: list[dict] = []
    total          = len(pn_clean)
    consec_fail    = 0
    aborted        = False
    abort_reason   = ""

    for i, pn in enumerate(pn_clean, start=1):
        # ── Cancel check sebelum mulai PN baru ──
        if stop_event is not None and stop_event.is_set():
            aborted, abort_reason = True, "dibatalkan oleh user"
            r = {"ok": False, "pn": pn, "n_photos": 0, "n_indexed": 0,
                 "n_skipped": 0, "error": abort_reason, "cancelled": True}
            results.append(r)
            if progress_callback:
                try: progress_callback(i, total, pn, r)
                except Exception: pass
            continue

        # ── Circuit breaker ──
        if consec_fail >= BULK_MAX_CONSEC_FAIL:
            aborted = True
            abort_reason = (
                f"circuit-breaker: {consec_fail} PN beruntun fatal-error "
                "(API mungkin down)"
            )
            r = {"ok": False, "pn": pn, "n_photos": 0, "n_indexed": 0,
                 "n_skipped": 0, "error": abort_reason, "aborted": True}
            results.append(r)
            if progress_callback:
                try: progress_callback(i, total, pn, r)
                except Exception: pass
            continue

        # ── Eksekusi 1 PN, jangan biarkan exception lolos ──
        try:
            r = index_part_number(pn, indexed_by=indexed_by)
        except MemoryError as e:
            r = {"ok": False, "pn": pn, "n_photos": 0, "n_indexed": 0,
                 "n_skipped": 0, "error": f"OOM: {e}"}
            consec_fail += 1
            # OOM → paksa gc untuk PN berikutnya
            try: gc.collect()
            except Exception: pass
        except Exception as e:
            r = {"ok": False, "pn": pn, "n_photos": 0, "n_indexed": 0,
                 "n_skipped": 0, "error": f"exception tak terduga: {e}"}
            consec_fail += 1
        else:
            # PN selesai normal. "fatal-error" = ok=False DAN ada foto SIMS
            # (artinya proses indexing-nya yang gagal, bukan PN tanpa foto).
            if (not r.get("ok")) and r.get("n_photos", 0) > 0:
                consec_fail += 1
            else:
                consec_fail = 0

        results.append(r)

        if progress_callback:
            try:
                progress_callback(i, total, pn, r)
            except Exception:
                pass

        # ── House-keeping: lepas RAM secara periodik ──
        if BULK_GC_EVERY > 0 and i % BULK_GC_EVERY == 0:
            try: gc.collect()
            except Exception: pass

        # ── Optional throttling antar PN ──
        if BULK_INTER_PN_SLEEP > 0 and i < total:
            time.sleep(BULK_INTER_PN_SLEEP)

    if aborted:
        print(f"[image_search] bulk aborted: {abort_reason}")

    return results


# ══════════════════════════════════════════════════════════════════════════
#  SEARCH
# ══════════════════════════════════════════════════════════════════════════

# Konstanta untuk aggregate scoring
_AGG_FETCH_MULT      = 5      # fetch top_k × 5 foto mentah dari DB
_AGG_FETCH_MIN       = 30     # minimum kandidat foto
_AGG_STRONG_TH       = 0.70   # foto dianggap "strong match" kalau ≥ 70%
_AGG_BOOST_PER_MATCH = 0.04   # +4% confidence per foto strong match (di luar foto terbaik)
_AGG_BOOST_CAP       = 0.10   # boost maksimum +10%


def search_by_image(image_bytes: bytes,
                    top_k: int = DEFAULT_TOP_K,
                    threshold: float = DEFAULT_THRESHOLD,
                    use_tta: bool = False) -> list[dict]:
    """
    Cari part berdasarkan foto query — DENGAN PN-level aggregate scoring.

    Flow:
      1. Compute embedding query (opsional pakai TTA flip)
      2. Fetch top_k × 5 foto kandidat dari DB (over-fetch)
      3. Group per PN, ambil foto similarity tertinggi sebagai representative
      4. Hitung confidence boost: PN dengan banyak foto strong match dapat bonus
         (tanda kepercayaan: bukan fluke 1 foto kebetulan mirip)
      5. Sort by aggregated similarity, return top_k PN unik

    Args:
        image_bytes  — foto query (jpg/png/webp bytes)
        top_k        — jumlah PN unik max yang di-return
        threshold    — aggregated similarity minimum (0.0–1.0)
        use_tta      — kalau True, hitung embedding rata-rata foto + flip
                       (lebih akurat 1–3%, latency 2× embed)

    Return list of dict (urut similarity desc, 1 entry per PN):
      [{
        "part_number":    str,
        "sims_url":       str,    # foto terbaik untuk preview
        "similarity":     float,  # aggregated score (base + boost)
        "raw_similarity": float,  # raw max sim (sebelum boost)
        "n_matches":      int,    # total foto PN ini di top-50
        "n_strong":       int,    # berapa di antara n_matches yang ≥ 70%
        "boost":          float,  # bonus yang ditambahkan
        "distance":       float,
      }, ...]
    """
    if not _is_configured():
        return []

    try:
        if use_tta:
            query_vec = compute_embedding_tta(image_bytes)
        else:
            query_vec = compute_embedding(image_bytes)
    except Exception as e:
        print(f"[image_search] compute_embedding error: {e}")
        return []

    # threshold similarity 0.70  →  cosine distance < 0.30
    distance_threshold = max(0.0, min(1.0 - threshold, 2.0))

    # Over-fetch: untuk aggregate per PN, butuh lebih banyak foto kandidat
    fetch_count = max(int(top_k) * _AGG_FETCH_MULT, _AGG_FETCH_MIN)

    try:
        resp = requests.post(
            _rpc_url(RPC_SEARCH),
            headers=_rest_headers(use_service=True),
            json={
                "query_embedding": _vec_to_str(query_vec),
                "match_threshold": distance_threshold,
                "match_count":     fetch_count,
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

    # ── Group per PN, simpan foto terbaik + statistik ──
    by_pn: dict[str, dict] = {}
    for r in rows:
        pn = r.get("part_number", "")
        if not pn:
            continue
        sim = float(r.get("similarity") or 0.0)
        url = r.get("sims_url", "")

        entry = by_pn.setdefault(pn, {
            "part_number": pn,
            "best_url":    url,
            "best_sim":    sim,
            "n_matches":   0,
            "n_strong":    0,
        })
        if sim > entry["best_sim"]:
            entry["best_sim"] = sim
            entry["best_url"] = url
        entry["n_matches"] += 1
        if sim >= _AGG_STRONG_TH:
            entry["n_strong"] += 1

    # ── Hitung aggregated score + confidence boost ──
    # Boost berdasarkan jumlah strong match SELAIN foto terbaik —
    # bukti bahwa banyak sudut/lighting tetap mirip.
    aggregated: list[dict] = []
    for pn, info in by_pn.items():
        extra_strong = max(0, info["n_strong"] - 1)
        boost        = min(_AGG_BOOST_PER_MATCH * extra_strong, _AGG_BOOST_CAP)
        agg_score    = min(info["best_sim"] + boost, 1.0)
        aggregated.append({
            "part_number":    pn,
            "sims_url":       info["best_url"],
            "similarity":     agg_score,
            "raw_similarity": info["best_sim"],
            "n_matches":      info["n_matches"],
            "n_strong":       info["n_strong"],
            "boost":          boost,
            "distance":       1.0 - info["best_sim"],
        })

    aggregated.sort(key=lambda x: x["similarity"], reverse=True)

    # Diagnostic: top-5 aggregated similarity untuk tuning threshold di kemudian hari.
    top5 = [round(a["similarity"], 3) for a in aggregated[:5]]
    print(f"[image_search] top5 sims = {top5} (tta={use_tta}, n_candidates={len(aggregated)})")

    if threshold > 0:
        aggregated = [a for a in aggregated if a["similarity"] >= threshold]

    return aggregated[:int(top_k)]


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

# Ambiguous mode: top-N dalam gap kecil → tidak pasti mana yang BEST
# Tidak aktif kalau top sim sudah tinggi (≥80%) — di zona itu semua match valid.
_AMBIGUOUS_GAP       = 0.05
_AMBIGUOUS_HIGH_CONF = 0.80


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
                "mode": "none", "top_sim": 0.0,
                "ambiguous": False, "ambiguous_count": 0}

    sorted_r = sorted(results, key=lambda x: x.get("similarity", 0.0), reverse=True)
    top_sim  = sorted_r[0]["similarity"]

    # Top hit terlalu lemah → tidak ada match yang meyakinkan
    if top_sim < _SMART_ABS_FLOOR:
        return {
            "kept":            [],
            "dropped":         sorted_r,
            "reason":          f"similarity tertinggi hanya {top_sim*100:.1f}% — tidak ada match meyakinkan",
            "mode":            "none",
            "top_sim":         top_sim,
            "ambiguous":       False,
            "ambiguous_count": 0,
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

    # ── Ambiguous detection ──
    # Saat top-N dalam gap kecil DAN top sim tidak tinggi → sistem tidak yakin.
    # Skip kalau top ≥ 0.80 (zona high-conf, multiple match valid).
    ambiguous       = False
    ambiguous_count = 1
    if len(kept) >= 2 and top_sim < _AMBIGUOUS_HIGH_CONF:
        for r in kept[1:]:
            if (top_sim - r["similarity"]) < _AMBIGUOUS_GAP:
                ambiguous_count += 1
            else:
                break
        if ambiguous_count >= 2:
            ambiguous = True

    return {
        "kept":            kept,
        "dropped":         dropped,
        "reason":          reason,
        "mode":            mode,
        "top_sim":         top_sim,
        "ambiguous":       ambiguous,
        "ambiguous_count": ambiguous_count,
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
# Track file_id terakhir yang sudah diproses per source — supaya block upload
# dan kamera tidak saling overwrite query state di setiap rerun.
_SS_PROCESSED_UPLOAD = "_img_search_processed_upload_id"
_SS_PROCESSED_CAMERA = "_img_search_processed_camera_id"
_SS_ACTIVE_SOURCE    = "_img_search_active_source"  # "upload" | "camera"


def _clear_global_pn_results() -> None:
    """
    Hapus state hasil pencarian PN global (tabel "Hasil Pencarian (N ditemukan)"
    + section "Gambar Part") yang dirender oleh display_search_results() di
    app.py — biar tidak ikut menggantung saat user mulai pencarian gambar baru.
    """
    for k in ("search_results", "search_type", "search_term"):
        st.session_state.pop(k, None)


def _on_query_file_change() -> None:
    """
    Callback file_uploader — dipanggil HANYA saat user benar-benar ganti
    foto (atau remove). TIDAK dipanggil saat rerun biasa (mis. klik tombol
    Detail PN), jadi aman untuk clearing state lama di sini.
    """
    st.session_state.pop(_SS_QUERY_BYTES, None)
    st.session_state.pop(_SS_QUERY_NAME,  None)
    st.session_state.pop(_SS_RESULTS,     None)
    _clear_global_pn_results()


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


# Validasi minimum untuk foto query — cegah garbage masuk ke embedding.
_QUERY_MIN_SIDE_PX = 200
_QUERY_MIN_BYTES   = 5 * 1024
_QUERY_OK_FORMATS  = {"JPEG", "PNG", "WEBP"}


def _inject_mobile_camera_js() -> None:
    """
    Inject JS:
      - Di MOBILE: tambahkan attribute `capture="environment"` ke file input
        di sub-tab "📷 Kamera" supaya tap langsung buka kamera belakang HP
        (bukan file picker, bukan webcam browser).
      - Di DESKTOP: sembunyikan tab "📷 Kamera" — fitur khusus mobile.

    Marker `mp-mobile-camera-marker` ditanam di dalam sub-tab kamera supaya
    JS bisa nemu file input yang benar (bukan uploader di tab lain).
    """
    import streamlit.components.v1 as _stc
    _stc.html(
        """
        <script>
        (function() {
          function isMobile() {
            return /Mobi|Android|iPhone|iPad|iPod|IEMobile|Opera Mini/i
                     .test(navigator.userAgent);
          }
          var w = window.parent || window;
          var d = w.document;

          function findCameraTab() {
            var tabs = d.querySelectorAll('[data-baseweb="tab"]');
            for (var i = 0; i < tabs.length; i++) {
              var t = (tabs[i].innerText || '').trim();
              if (t.indexOf('Kamera') !== -1) return tabs[i];
            }
            return null;
          }

          function apply() {
            var camTab = findCameraTab();
            var marker = d.getElementById('mp-mobile-camera-marker');

            if (!isMobile()) {
              if (camTab) camTab.style.display = 'none';
              return;
            }
            if (camTab) camTab.style.display = '';

            if (!marker) return;
            var panel = marker.closest('[role="tabpanel"]') || marker.parentElement;
            if (!panel) return;
            var inputs = panel.querySelectorAll('input[type="file"]');
            inputs.forEach(function(inp) {
              if (inp.getAttribute('data-mp-camera') === '1') return;
              inp.setAttribute('capture', 'environment');
              inp.setAttribute('accept', 'image/*');
              inp.setAttribute('data-mp-camera', '1');
            });
          }

          apply();
          var n = 0;
          var iv = setInterval(function() {
            apply();
            if (++n > 25) clearInterval(iv);
          }, 150);

          if (!w.__mpCameraObserver) {
            w.__mpCameraObserver = new MutationObserver(apply);
            w.__mpCameraObserver.observe(d.body, {
              childList: true, subtree: true
            });
          }
        })();
        </script>
        """,
        height=0,
    )


def _validate_query_image(raw: bytes, name: str = "") -> tuple[bool, str]:
    """
    Sanity check foto query sebelum embed. Return (ok, error_msg).
    Tolak foto kekecilan/korup karena bicubic upscale dari 50px jadi 224px
    menghasilkan embedding generik → hasil pencarian ngawur.
    """
    if not raw or len(raw) < _QUERY_MIN_BYTES:
        return False, f"File terlalu kecil ({len(raw)/1024:.1f} KB) — minimum 5 KB."
    try:
        img = Image.open(io.BytesIO(raw))
        fmt = (img.format or "").upper()
        w, h = img.size
    except Exception as e:
        return False, f"File tidak bisa dibaca sebagai gambar ({e})."
    if fmt not in _QUERY_OK_FORMATS:
        return False, f"Format {fmt or 'tidak dikenali'} tidak didukung — pakai JPG/PNG/WEBP."
    if min(w, h) < _QUERY_MIN_SIDE_PX:
        return False, (
            f"Resolusi foto {w}×{h} px terlalu kecil — minimum sisi terpendek "
            f"{_QUERY_MIN_SIDE_PX} px untuk hasil akurat."
        )
    return True, ""


def render_search_image_tab():
    """Tab user: Cari Part by Foto."""
    # ── Header ringkas ─────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="margin-bottom:12px;">
          <h2 style="margin:0; font-size:24px; font-weight:700;">
            🖼️ Cari Part by Foto
          </h2>
          <p style="color:#6b7280; font-size:13px; margin:4px 0 0 0;">
            Upload foto part → AI cari Part Number paling mirip dari index visual.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
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

    # ── Statistik index sebagai pill compact ──────────────────────────────
    stats = get_index_stats()
    if stats["total_pn"] == 0:
        st.info(
            "ℹ️ Belum ada PN yang di-index. "
            "Admin harus menambahkan PN di tab **🧠 Image Index** terlebih dahulu."
        )
        return

    last_indexed = stats.get("last_indexed_at") or "—"
    if isinstance(last_indexed, str) and len(last_indexed) >= 16:
        last_indexed = last_indexed[:16].replace("T", " ")

    st.markdown(
        f"""
        <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
          <span style="background:#eff6ff; color:#1e40af; padding:4px 12px;
                       border-radius:20px; font-size:12px; font-weight:600;">
            📦 {stats['total_pn']:,} PN ter-index
          </span>
          <span style="background:#f0fdf4; color:#166534; padding:4px 12px;
                       border-radius:20px; font-size:12px; font-weight:600;">
            🖼️ {stats['total_images']:,} foto
          </span>
          <span style="background:#fefce8; color:#854d0e; padding:4px 12px;
                       border-radius:20px; font-size:12px; font-weight:600;">
            🕐 Update: {last_indexed}
          </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Tips foto bagus (collapsible) ─────────────────────────────────────
    with st.expander("💡 Tips foto untuk hasil terbaik", expanded=False):
        st.markdown(
            """
            - **Sudut**: foto dari depan atau samping, hindari sangat miring
            - **Background**: polos & kontras (lantai putih, kain gelap)
            - **Cahaya**: terang & merata, hindari bayangan kuat / silau
            - **Fokus**: crop ke part, buang background yang dominan
            - **Resolusi**: ≥ 400×400 px (lebih besar lebih baik)
            - **Format**: JPG, PNG, atau WEBP
            """
        )

    # ── Upload section (bordered container) ───────────────────────────────
    upload_box = st.container(border=True)
    with upload_box:
        col_upload, col_preview = st.columns([3, 2], gap="medium")

        with col_upload:
            # Sub-tabs: pilih sumber foto (Upload file vs Kamera langsung).
            # Kamera khusus mobile — di desktop tab Kamera disembunyikan via JS,
            # di HP file input di sub-tab kamera dipatch jadi capture=environment
            # supaya tap langsung buka kamera belakang (bukan file picker).
            src_tab_upload, src_tab_camera = st.tabs(
                ["📤 Upload File", "📷 Kamera"]
            )
            _inject_mobile_camera_js()

            with src_tab_upload:
                uploaded = st.file_uploader(
                    "Upload atau drag-drop foto part di sini",
                    type=["jpg", "jpeg", "png", "webp"],
                    key="img_search_uploader",
                    help="Format didukung: JPG, JPEG, PNG, WEBP. Max 200 MB.",
                    on_change=_on_query_file_change,
                    label_visibility="collapsed",
                )

                if uploaded is not None:
                    up_id = getattr(uploaded, "file_id", None) or uploaded.name
                    if st.session_state.get(_SS_PROCESSED_UPLOAD) != up_id:
                        raw_bytes = uploaded.getvalue()
                        ok, err   = _validate_query_image(raw_bytes, uploaded.name)
                        if not ok:
                            st.error(f"❌ {err}")
                            st.session_state.pop(_SS_QUERY_BYTES, None)
                            st.session_state.pop(_SS_QUERY_NAME,  None)
                            st.session_state.pop(_SS_RESULTS,     None)
                        else:
                            st.session_state[_SS_QUERY_BYTES]    = raw_bytes
                            st.session_state[_SS_QUERY_NAME]     = uploaded.name
                            st.session_state[_SS_ACTIVE_SOURCE]  = "upload"
                        st.session_state[_SS_PROCESSED_UPLOAD] = up_id

            with src_tab_camera:
                # Marker untuk JS — file_uploader di bawahnya akan di-patch
                # dengan attribute capture="environment" supaya buka kamera HP.
                st.markdown(
                    '<div id="mp-mobile-camera-marker" style="height:0;"></div>',
                    unsafe_allow_html=True,
                )
                st.caption(
                    "📱 Khusus untuk HP — tombol di bawah akan langsung "
                    "buka kamera belakang. Tap, jepret part, lalu klik "
                    "**Cari Part Sekarang**."
                )
                captured = st.file_uploader(
                    "Ambil foto part dengan kamera HP",
                    type=["jpg", "jpeg", "png", "webp"],
                    key="img_search_camera_uploader",
                    on_change=_on_query_file_change,
                    label_visibility="collapsed",
                )

                if captured is not None:
                    cam_id = getattr(captured, "file_id", None) or captured.name
                    if st.session_state.get(_SS_PROCESSED_CAMERA) != cam_id:
                        raw_bytes = captured.getvalue()
                        ok, err   = _validate_query_image(raw_bytes, captured.name)
                        if not ok:
                            st.error(f"❌ {err}")
                            st.session_state.pop(_SS_QUERY_BYTES, None)
                            st.session_state.pop(_SS_QUERY_NAME,  None)
                            st.session_state.pop(_SS_RESULTS,     None)
                        else:
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            ext = Path(captured.name).suffix.lower() or ".jpg"
                            st.session_state[_SS_QUERY_BYTES]   = raw_bytes
                            st.session_state[_SS_QUERY_NAME]    = f"kamera_{ts}{ext}"
                            st.session_state[_SS_ACTIVE_SOURCE] = "camera"
                        st.session_state[_SS_PROCESSED_CAMERA] = cam_id

            q_bytes_check = st.session_state.get(_SS_QUERY_BYTES)

            # Action buttons
            col_b1, col_b2 = st.columns([4, 1])
            with col_b1:
                do_search = st.button(
                    "🔍 **Cari Part Sekarang**", type="primary",
                    use_container_width=True,
                    disabled=not q_bytes_check,
                    key="btn_img_search",
                )
            with col_b2:
                if st.button(
                    "🔄", use_container_width=True,
                    key="btn_img_clear",
                    help="Reset — hapus foto & hasil",
                ):
                    for k in (
                        _SS_QUERY_BYTES, _SS_QUERY_NAME, _SS_RESULTS,
                        _SS_PROCESSED_UPLOAD, _SS_PROCESSED_CAMERA,
                        _SS_ACTIVE_SOURCE,
                        "img_search_uploader", "img_search_camera_uploader",
                    ):
                        st.session_state.pop(k, None)
                    _clear_global_pn_results()
                    st.rerun()

            # Advanced settings (collapsed default)
            with st.expander("⚙️ Pengaturan lanjut", expanded=False):
                top_k = st.slider(
                    "Jumlah PN dipertimbangkan",
                    min_value=5, max_value=30,
                    value=DEFAULT_TOP_K, step=1,
                    key="img_search_topk",
                    help=(
                        "Setelah smart filter, dari N PN ini diambil yang paling relevan. "
                        "Lebih tinggi = lebih banyak kandidat dipertimbangkan."
                    ),
                )
                use_tta = st.checkbox(
                    "🔄 Augmentasi flip (lebih akurat, ~2× lebih lambat)",
                    value=False,
                    key="img_search_tta",
                    help=(
                        "Hitung embedding rata-rata dari foto asli + horizontal-flip. "
                        "Cocok untuk part simetris kiri-kanan."
                    ),
                )
                st.caption(
                    "🤖 **Smart filter + PN-level aggregate scoring** aktif otomatis — "
                    "hasil disaring berdasarkan distribusi similarity dan multi-foto match."
                )

        with col_preview:
            q_bytes = st.session_state.get(_SS_QUERY_BYTES)
            q_name  = st.session_state.get(_SS_QUERY_NAME, "")
            q_src   = st.session_state.get(_SS_ACTIVE_SOURCE, "")
            if q_bytes:
                size_kb   = len(q_bytes) / 1024
                src_icon  = "📷" if q_src == "camera" else "📤"
                src_label = "Dari kamera" if q_src == "camera" else "Dari file"
                st.markdown(
                    f"""
                    <div style="font-size:13px; font-weight:600; margin-bottom:4px;">
                      📸 Preview foto query
                      <span style="font-weight:500; color:#6b7280; font-size:11px;
                                   margin-left:6px;">{src_icon} {src_label}</span>
                    </div>
                    <div style="color:#6b7280; font-size:11px; margin-bottom:4px;
                                white-space:nowrap; overflow:hidden; text-overflow:ellipsis;"
                         title="{q_name}">
                      {q_name[:50]} · {size_kb:.0f} KB
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                st.image(q_bytes, use_container_width=True)
            else:
                # Empty preview placeholder
                st.markdown(
                    """
                    <div style="border:2px dashed #e5e7eb; border-radius:10px;
                                padding:20px; text-align:center; color:#9ca3af;
                                min-height:180px; display:flex; flex-direction:column;
                                align-items:center; justify-content:center;
                                background:#fafafa;">
                      <div style="font-size:42px; line-height:1;">📷</div>
                      <div style="font-size:12px; margin-top:8px;">
                        Preview foto query<br>akan tampil di sini
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ── Eksekusi search ──
    if do_search and q_bytes:
        # Bersihkan hasil PN search lama (tabel di bawah tabs) supaya tidak
        # menggantung saat user run image search baru.
        _clear_global_pn_results()
        with st.spinner("🔍 Menghitung embedding & mencari di database..."):
            t0 = time.time()
            # Ambil kandidat mentah tanpa threshold — biar smart filter
            # punya distribusi lengkap untuk deteksi cliff/noise.
            raw = search_by_image(
                q_bytes,
                top_k=int(top_k),
                threshold=0.0,
                use_tta=bool(use_tta),
            )
            filtered = smart_filter_results(raw)
            elapsed  = time.time() - t0

        st.session_state[_SS_RESULTS]                  = filtered["kept"]
        st.session_state["_img_search_fallback"]       = filtered["dropped"][:5]
        st.session_state["_img_search_filter_reason"]  = filtered["reason"]
        st.session_state["_img_search_filter_mode"]    = filtered["mode"]
        st.session_state["_img_search_top_sim"]        = filtered["top_sim"]
        st.session_state["_img_search_ambiguous"]      = filtered["ambiguous"]
        st.session_state["_img_search_ambiguous_count"] = filtered["ambiguous_count"]
        st.session_state["_img_search_elapsed"]        = elapsed
        st.rerun()

    # ── Handle "Detail PN" trigger dari klik tombol di card ──────────────
    # Spinner muncul di lokasi yang sama dengan spinner "Cari Part Sekarang"
    # (antara upload box dan section hasil) supaya user langsung lihat
    # tanda loading tanpa harus scroll.
    trigger_pn = st.session_state.pop("_trigger_search_pn", None)
    if trigger_pn:
        try:
            from app import search_part_number  # late import — hindari circular
        except ImportError:
            search_part_number = None
        if search_part_number is not None:
            with st.spinner(f"🔍 Mencari detail untuk {trigger_pn}..."):
                try:
                    st.session_state.search_results = search_part_number(
                        trigger_pn,
                        st.session_state.get("excel_files", []),
                        st.session_state.get("stok_data"),
                        st.session_state.get("harga_lookup", {}),
                    )
                    st.session_state.search_type = "Part Number"
                    st.session_state.search_term = trigger_pn
                except Exception as _e:
                    st.warning(f"⚠️ Gagal menjalankan search dari image: {_e}")

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

    # ── Result header dengan summary inline ───────────────────────────────
    n_kept = len(results)
    n_drop = len(fallback)
    drop_text = f" · 🗑️ {n_drop} disaring" if n_drop > 0 else ""

    st.markdown(
        f"""
        <div style="display:flex; justify-content:space-between; align-items:flex-end;
                    margin-bottom:10px; flex-wrap:wrap; gap:8px;">
          <h3 style="margin:0; font-size:20px;">📋 Hasil Pencarian</h3>
          <div style="color:#6b7280; font-size:12px;">
            ⚡ {elapsed:.2f}s · ✅ {n_kept} cocok{drop_text}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Badge confidence dari smart filter (compact)
    if filter_mode == "strong":
        st.success(f"✅ **Match kuat** — top similarity **{top_sim*100:.1f}%**")
    elif filter_mode == "moderate":
        st.info(f"ℹ️ **Match sedang** — top similarity **{top_sim*100:.1f}%**")

    if not results:
        if fallback:
            best_sim = fallback[0]["similarity"] * 100
            st.warning(
                f"⚠️ **Tidak ada match meyakinkan.** Kandidat terdekat hanya **{best_sim:.1f}%** — "
                f"kemungkinan part belum di-index, atau foto query terlalu beda dari foto SIMS."
            )
            with st.expander(
                f"🔍 Lihat {len(fallback)} kandidat terdekat (low confidence)",
                expanded=False,
            ):
                _render_card_grid(fallback, start_rank=1)
        else:
            st.warning(
                "⚠️ Tidak ada hasil sama sekali. "
                "Pastikan sudah ada PN yang di-index di tab **🧠 Image Index**."
            )
        return

    is_ambiguous = st.session_state.get("_img_search_ambiguous", False)
    ambig_count  = st.session_state.get("_img_search_ambiguous_count", 1)

    if is_ambiguous:
        # ── Ambiguous mode: top-N sejajar, no BEST MATCH crown ────────────
        st.markdown(
            """
            <div style="margin:8px 0 6px 0; color:#92400e; font-size:12px;
                        background:#fffbeb; border-left:4px solid #f59e0b;
                        padding:8px 12px; border-radius:4px;">
              💡 <b>Tip:</b> kalau sering ambigu, coba upload foto dengan
              background polos & crop tight ke part — kurangi noise visual
              seperti watermark/logo/permukaan tekstur.
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Render top ambig_count sebagai grid 2 kolom equal-weight
        ambiguous_results = results[:ambig_count]
        _render_card_grid(ambiguous_results, start_rank=1, is_ambiguous=True)

        # Sisa hasil di bawah tier ambiguous
        rest_results = results[ambig_count:]
        if rest_results:
            st.markdown(
                """
                <div style="margin:14px 0 6px 0; color:#6b7280; font-size:12px;
                            font-weight:600;">
                  🔎 Kandidat dengan similarity lebih rendah
                </div>
                """,
                unsafe_allow_html=True,
            )
            _render_card_grid(rest_results, start_rank=ambig_count + 1)
    else:
        # ── Normal mode: render semua hasil dalam grid 2 kolom ────────────
        _render_card_grid(results, start_rank=1)

    # Show dropped candidates di expander (transparansi — user tahu apa yang disaring)
    if fallback:
        with st.expander(
            f"👁️ Lihat {len(fallback)} kandidat yang disaring smart filter",
            expanded=False,
        ):
            st.caption(f"Disaring karena: {filter_reason}")
            _render_card_grid(fallback, start_rank=len(results) + 1)


_CARD_IMG_MAX_PX      = 240   # tinggi foto dalam card hasil (regular)
_CARD_IMG_BEST_PX     = 360   # tinggi foto dalam card BEST MATCH (lebih besar)


def _render_card_grid(items: list[dict], start_rank: int = 1,
                      is_ambiguous: bool = False) -> None:
    """
    Render daftar card hasil dalam grid 2-kolom dengan prefetch paralel.

    Flow:
      1. Prefetch semua foto SIMS paralel via ThreadPoolExecutor
         (spinner terlihat selama prefetch)
      2. Render cards normal — semua foto sudah di disk cache, instant.

    Tanpa prefetch, _download_image di-call sequential per card —
    card terakhir baru render setelah card sebelumnya selesai download.
    """
    if not items:
        return

    # ── Inject CSS lightbox (fullscreen modal) sekali per grid call ──
    # CSS-only via :target pseudo-class — no JS needed, no Streamlit rerun.
    st.markdown(
        """
        <style>
        .fs-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.92);
                  z-index:99999;align-items:center;justify-content:center;
                  padding:24px;box-sizing:border-box;}
        .fs-modal:target{display:flex;}
        .fs-modal img{max-width:96vw;max-height:92vh;object-fit:contain;
                      box-shadow:0 8px 32px rgba(0,0,0,0.5);border-radius:6px;
                      background:#fff;}
        .fs-modal .fs-close{position:absolute;top:18px;right:24px;color:#fff;
                            text-decoration:none;font-size:32px;font-weight:300;
                            line-height:1;width:44px;height:44px;display:flex;
                            align-items:center;justify-content:center;
                            background:rgba(255,255,255,0.12);border-radius:50%;
                            transition:background .15s;}
        .fs-modal .fs-close:hover{background:rgba(255,255,255,0.25);}
        .fs-modal .fs-backdrop{position:absolute;inset:0;cursor:zoom-out;}
        .fs-trigger{position:absolute;top:6px;right:6px;width:30px;height:30px;
                    background:rgba(255,255,255,0.92);border:1px solid #e5e7eb;
                    border-radius:6px;display:flex;align-items:center;
                    justify-content:center;text-decoration:none;color:#374151;
                    cursor:zoom-in;z-index:2;opacity:0;transition:opacity .15s;
                    box-shadow:0 1px 3px rgba(0,0,0,0.08);}
        .fs-imgbox:hover .fs-trigger{opacity:1;}
        .fs-trigger:hover{background:#fff;color:#111827;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Step 1: prefetch paralel semua foto sebelum render apapun ──
    urls = [r["sims_url"] for r in items if r.get("sims_url")]
    if urls:
        with st.spinner(f"📥 Memuat {len(urls)} foto..."):
            try:
                with ThreadPoolExecutor(
                    max_workers=min(DOWNLOAD_WORKERS, len(urls))
                ) as ex:
                    list(ex.map(_download_image, urls))
            except Exception as e:
                print(f"[image_search] prefetch error: {e}")

    # ── Step 2: render cards (foto sudah di cache → render bareng-bareng) ──
    for i in range(0, len(items), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(items):
                continue
            with col:
                _render_result_card(start_rank + idx, items[idx],
                                    is_ambiguous=is_ambiguous)


def _render_result_card(rank: int, r: dict, is_best: bool = False,
                        is_ambiguous: bool = False):
    """
    Render 1 kartu hasil pencarian.
    is_best=True       → BEST MATCH: foto besar, border emas, font naik.
    is_ambiguous=True  → kandidat ambigu: foto medium, border oranye dashed.
    """
    import base64

    pn         = r["part_number"]
    sim_pct    = r["similarity"] * 100.0
    sims_url   = r["sims_url"]
    part_name  = _try_get_part_name(pn) or "—"

    # PN-level aggregate fields (mungkin tidak ada untuk legacy callers)
    n_matches = r.get("n_matches", 0)
    n_strong  = r.get("n_strong", 0)
    raw_sim   = r.get("raw_similarity")
    boost     = r.get("boost", 0.0)

    if sim_pct >= 90:
        badge_color = "#16a34a"   # hijau
    elif sim_pct >= 80:
        badge_color = "#0891b2"   # cyan
    elif sim_pct >= 70:
        badge_color = "#d97706"   # amber
    else:
        badge_color = "#6b7280"   # abu

    # Confidence chip: tampil saat PN ini punya banyak foto match
    confidence_chip = ""
    if n_strong >= 2:
        tooltip = (
            f"{n_strong} foto PN ini punya similarity ≥70% "
            f"(boost +{boost*100:.1f}%). Top raw: {(raw_sim or 0)*100:.1f}%."
        )
        confidence_chip = (
            f'<span style="background:#dcfce7; color:#166534; padding:1px 6px; '
            f'border-radius:8px; font-size:10px; font-weight:600; margin-left:4px;" '
            f'title="{tooltip}">✨ {n_strong} foto</span>'
        )
    elif n_matches >= 3:
        confidence_chip = (
            f'<span style="background:#fef3c7; color:#92400e; padding:1px 6px; '
            f'border-radius:8px; font-size:10px; font-weight:600; margin-left:4px;" '
            f'title="{n_matches} foto PN ini muncul di kandidat (cuma {n_strong} strong).">'
            f'{n_matches} foto</span>'
        )

    # Variasi style: BEST > AMBIGUOUS > normal
    if is_best:
        img_h, pn_font, name_font, badge_font = _CARD_IMG_BEST_PX, "20px", "13px", "14px"
        badge_pad, card_pad = "3px 12px", "14px"
        card_border = "2px solid #f59e0b; box-shadow:0 4px 12px rgba(245,158,11,0.15);"
        card_bg     = "linear-gradient(180deg,#fffbeb,#ffffff)"
    elif is_ambiguous:
        # Foto agak besar (di antara best & normal), border oranye dashed,
        # tanpa shadow — sinyal visual: "salah satu dari ini"
        img_h, pn_font, name_font, badge_font = 260, "15px", "12px", "12px"
        badge_pad, card_pad = "2px 10px", "10px"
        card_border = "2px dashed #f59e0b;"
        card_bg     = "#fffbeb"
    else:
        img_h, pn_font, name_font, badge_font = _CARD_IMG_MAX_PX, "13px", "11px", "11px"
        badge_pad, card_pad = "1px 8px", "8px"
        card_border = "1px solid #e5e7eb;"
        card_bg     = "#fafafa"

    # Ambil foto + encode base64 — pakai background-image (lebih reliable
    # upscale dibanding object-fit:contain di Streamlit context)
    img_bg_style     = ""
    img_inner_html   = ""
    fs_trigger_html  = ""
    fs_modal_html    = ""
    has_img          = False
    if sims_url:
        img_bytes = _download_image(sims_url)
        if img_bytes:
            b64 = base64.b64encode(img_bytes).decode()
            if img_bytes[:4] == b"RIFF":
                mime = "image/webp"
            elif img_bytes[:3] == b"\xff\xd8\xff":
                mime = "image/jpeg"
            elif img_bytes[:8] == b"\x89PNG\r\n\x1a\n":
                mime = "image/png"
            else:
                mime = "image/jpeg"
            img_bg_style = (
                f"background-image:url(data:{mime};base64,{b64});"
                "background-size:contain;"
                "background-position:center;"
                "background-repeat:no-repeat;"
            )
            has_img = True
            # Unique modal id per card (rank + sanitized PN)
            pn_safe = re.sub(r"[^A-Za-z0-9]", "", pn)
            fs_id   = f"fs-{rank}-{pn_safe}"
            data_uri = f"data:{mime};base64,{b64}"
            # Tombol trigger di pojok kanan-atas foto (muncul saat hover)
            fs_trigger_html = (
                f'<a href="#{fs_id}" class="fs-trigger" title="Fullscreen" '
                f'aria-label="Lihat fullscreen">'
                f'<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" '
                f'viewBox="0 0 24 24" fill="none" stroke="currentColor" '
                f'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
                f'<polyline points="15 3 21 3 21 9"></polyline>'
                f'<polyline points="9 21 3 21 3 15"></polyline>'
                f'<line x1="21" y1="3" x2="14" y2="10"></line>'
                f'<line x1="3" y1="21" x2="10" y2="14"></line>'
                f'</svg></a>'
            )
            # Modal overlay (CSS-only via :target). Klik backdrop / tombol ✕
            # → href="#" tutup modal tanpa rerun Streamlit.
            fs_modal_html = (
                f'<div id="{fs_id}" class="fs-modal">'
                f'<a href="#" class="fs-backdrop" aria-label="Tutup"></a>'
                f'<a href="#" class="fs-close" aria-label="Tutup">&times;</a>'
                f'<img src="{data_uri}" alt="{pn}" />'
                f'</div>'
            )

    if not has_img:
        img_inner_html = (
            '<div style="color:#9ca3af; font-size:11px;">⚠️ Foto gagal dimuat</div>'
        )
        flex_center = "display:flex; align-items:center; justify-content:center;"
    else:
        flex_center = ""

    # Build HTML as single line (no newlines/indent) — penting karena
    # markdown processor Streamlit treats indented HTML sebagai code block,
    # dan multi-line attribute value (style=...) yang mengandung base64
    # panjang bisa bikin parser bocor `</div>` sebagai teks.
    card_html = (
        f'<div style="border-radius:10px;padding:{card_pad};margin-bottom:4px;'
        f'background:{card_bg};{card_border}">'
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;gap:8px;">'
        f'<div style="font-weight:700;font-size:{pn_font};'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
        f'#{rank}&nbsp;{pn}{confidence_chip}'
        f'</div>'
        f'<div style="background:{badge_color};color:white;padding:{badge_pad};'
        f'border-radius:14px;font-weight:700;font-size:{badge_font};'
        f'flex-shrink:0;">'
        f'{sim_pct:.1f}%'
        f'</div>'
        f'</div>'
        f'<div style="color:#374151;font-size:{name_font};margin:4px 0 8px 0;'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" '
        f'title="{part_name}">'
        f'{part_name}'
        f'</div>'
        f'<div class="fs-imgbox" style="position:relative;height:{img_h}px;'
        f'background:#fff;border-radius:8px;border:1px solid #f3f4f6;'
        f'overflow:hidden;{img_bg_style}{flex_center}">'
        f'{fs_trigger_html}'
        f'{img_inner_html}'
        f'</div>'
        f'</div>'
        f'{fs_modal_html}'
    )
    st.markdown(card_html, unsafe_allow_html=True)

    # Tombol detail kompak
    if is_best:
        btn_label, btn_type = f"🔎 Cari Detail {pn}", "primary"
    elif is_ambiguous:
        btn_label, btn_type = f"🔎 Cari Detail {pn}", "primary"
    else:
        btn_label, btn_type = f"🔎 Detail {pn}", "secondary"
    if st.button(btn_label, key=f"img_res_detail_{rank}_{pn}",
                 use_container_width=True, type=btn_type):
        st.session_state["_trigger_search_pn"] = pn
        st.rerun()
