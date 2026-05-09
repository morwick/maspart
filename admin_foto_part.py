"""
ADMIN FOTO PART
===============
Modul untuk upload foto part ke Supabase Storage per Part Number.
Hanya bisa diakses oleh admin.

Fitur:
  - Cari part berdasarkan Part Number atau Part Name
  - Upload foto part ke Supabase Storage
  - Foto dari Supabase menjadi foto utama (prioritas #1)
  - Fallback ke foto SIMS jika tidak ada di Supabase
  - Lihat, hapus, dan kelola foto per Part Number

Setup Supabase:
  1. Buat bucket 'part-photos' di Supabase Storage (public)
  2. Pastikan [supabase] url, key, service_key sudah di secrets.toml
  3. Jalankan SQL berikut di Supabase SQL Editor:

     -- Schema dikelola via migrations/001_cleanup.sql
     -- Tabel part_photos final:
     --   id, part_number, file_name, storage_path, storage_url,
     --   file_size, uploaded_by, created_at
     -- UNIQUE (part_number, file_name) — re-upload nama sama
     -- akan di-merge (upsert) lewat resolution=merge-duplicates.

Cara Pakai di app.py:
  from admin_foto_part import render_foto_part_tab, get_supabase_photo_urls

  # Di display_dashboard, tambah tab admin:
  ALL_TAB_DEFS.append(("tab_foto_part", "📷 Upload Foto Part", "__foto_part__"))

  # Di render loop:
  elif fn == "__foto_part__":
      render_foto_part_tab()

  # Di display_search_results, gabungkan foto Supabase + SIMS:
  supabase_urls = get_supabase_photo_urls(pn)
  all_img_links = supabase_urls + sims_urls  # Supabase duluan
"""

from __future__ import annotations

import io
import time
import base64
import threading
from pathlib import Path
from typing import Optional

import requests
import streamlit as st
import streamlit.components.v1 as _stc

# ── Konstanta ────────────────────────────────────────────────────────────────
STORAGE_BUCKET   = "part-photos"
METADATA_TABLE   = "part_photos"
MAX_FILE_SIZE_MB = 10
ALLOWED_TYPES    = ["jpg", "jpeg", "png", "webp"]

# Cache foto per PN — supaya tidak GET ulang tiap rerun
_photo_cache:      dict = {}   # {pn_upper: [url, ...]}
_photo_cache_lock  = threading.Lock()
_PHOTO_CACHE_TTL   = 120       # detik


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

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
            "table":       cfg.get("table", "users"),
        }
    except Exception:
        return {"url": "", "key": "", "service_key": "", "table": "users"}


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


def _storage_headers(use_service: bool = True) -> dict:
    cfg = _get_cfg()
    key = cfg["service_key"] if use_service and cfg["service_key"] else cfg["key"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
    }


def _rest_url(table: str) -> str:
    return f"{_get_cfg()['url']}/rest/v1/{table}"


def _storage_url(path: str = "") -> str:
    base = f"{_get_cfg()['url']}/storage/v1/object"
    return f"{base}/{STORAGE_BUCKET}/{path}" if path else f"{base}/{STORAGE_BUCKET}"


def _storage_public_url(path: str) -> str:
    return f"{_get_cfg()['url']}/storage/v1/object/public/{STORAGE_BUCKET}/{path}"


# ═══════════════════════════════════════════════════════════════════════════════
#  STORAGE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _upload_to_storage(pn: str, file_name: str, file_bytes: bytes, mime_type: str) -> tuple[bool, str]:
    """
    Upload file ke Supabase Storage.
    Return (success, storage_path_or_error_msg).
    Storage path: part-photos/{pn}/{file_name}
    """
    pn_safe  = pn.strip().upper().replace("/", "_").replace(" ", "_")
    obj_path = f"{pn_safe}/{file_name}"
    url      = _storage_url(obj_path)

    headers = {
        **_storage_headers(use_service=True),
        "Content-Type": mime_type,
        "x-upsert":     "true",  # Replace jika sudah ada
    }

    try:
        resp = requests.post(url, headers=headers, data=file_bytes, timeout=30)
        if resp.status_code in (200, 201):
            return True, obj_path
        # Coba PUT jika POST gagal
        resp2 = requests.put(url, headers=headers, data=file_bytes, timeout=30)
        if resp2.status_code in (200, 201):
            return True, obj_path
        return False, f"Storage error {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, f"Upload exception: {e}"


def _delete_from_storage(obj_path: str) -> bool:
    """Hapus file dari Supabase Storage."""
    url = f"{_get_cfg()['url']}/storage/v1/object/{STORAGE_BUCKET}"
    try:
        resp = requests.delete(
            url,
            headers=_storage_headers(use_service=True),
            json={"prefixes": [obj_path]},
            timeout=15,
        )
        return resp.status_code in (200, 204)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  METADATA OPERATIONS (tabel part_photos)
# ═══════════════════════════════════════════════════════════════════════════════

def _save_metadata(pn: str, file_name: str, storage_path: str,
                   file_size: int, uploaded_by: str) -> bool:
    """
    Simpan metadata foto ke tabel part_photos.
    Menggunakan upsert (on_conflict=part_number,file_name) supaya re-upload
    nama yang sama tidak menghasilkan duplikat — kolom akan di-merge.
    """
    public_url = _storage_public_url(storage_path)
    try:
        resp = requests.post(
            _rest_url(METADATA_TABLE),
            headers={
                **_rest_headers(use_service=True),
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": "part_number,file_name"},
            json={
                "part_number":  pn.strip().upper(),
                "file_name":    file_name,
                "storage_path": storage_path,
                "storage_url":  public_url,
                "file_size":    file_size,
                "uploaded_by":  uploaded_by,
            },
            timeout=15,
        )
        return resp.status_code in (200, 201, 204)
    except Exception as e:
        print(f"[foto_part] ❌ save_metadata: {e}")
        return False


def _load_metadata(pn: str) -> list[dict]:
    """Ambil semua foto untuk part_number dari tabel part_photos."""
    if not _is_configured():
        return []
    try:
        resp = requests.get(
            _rest_url(METADATA_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params={
                "select":      "id,file_name,storage_path,storage_url,file_size,uploaded_by,created_at",
                "part_number": f"eq.{pn.strip().upper()}",
                "order":       "created_at.asc",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json() or []
    except Exception as e:
        print(f"[foto_part] ❌ load_metadata: {e}")
    return []


def _delete_metadata(record_id: int) -> bool:
    """Hapus baris metadata dari part_photos berdasarkan id."""
    try:
        resp = requests.delete(
            _rest_url(METADATA_TABLE),
            headers={**_rest_headers(use_service=True), "Prefer": "return=minimal"},
            params={"id": f"eq.{record_id}"},
            timeout=10,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        print(f"[foto_part] ❌ delete_metadata({record_id}): {e}")
        return False


def _search_metadata_by_pn(query: str) -> list[dict]:
    """Cari part_number yang punya foto di tabel part_photos."""
    if not _is_configured():
        return []
    try:
        resp = requests.get(
            _rest_url(METADATA_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params={
                "select":      "part_number,file_name,storage_url,created_at",
                "part_number": f"ilike.*{query.strip().upper()}*",
                "order":       "part_number.asc",
                "limit":       "50",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json() or []
    except Exception as e:
        print(f"[foto_part] ❌ search_metadata: {e}")
    return []


# ═══════════════════════════════════════════════════════════════════════════════
#  PUBLIC API — dipakai oleh app.py untuk menggabungkan foto
# ═══════════════════════════════════════════════════════════════════════════════

def get_supabase_photo_urls(pn: str) -> list[str]:
    """
    Ambil daftar URL foto dari Supabase untuk part_number.
    Return list URL (bisa kosong). Selalu lebih fresh dari cache jika TTL expired.
    Dipanggil dari display_search_results di app.py.
    """
    if not _is_configured() or not pn:
        return []

    pn_key = pn.strip().upper()
    now    = time.time()

    with _photo_cache_lock:
        entry = _photo_cache.get(pn_key)
        if entry and (now - entry["ts"]) < _PHOTO_CACHE_TTL:
            return list(entry["urls"])

    rows = _load_metadata(pn_key)
    urls = [r["storage_url"] for r in rows if r.get("storage_url")]

    with _photo_cache_lock:
        _photo_cache[pn_key] = {"urls": urls, "ts": now}

    return urls


def invalidate_photo_cache(pn: str):
    """Paksa reload foto dari Supabase pada request berikutnya."""
    pn_key = pn.strip().upper()
    with _photo_cache_lock:
        _photo_cache.pop(pn_key, None)


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPER — Search Excel Files (dipakai di tab admin)
# ═══════════════════════════════════════════════════════════════════════════════

def _search_excel_by_pn(query: str, excel_files: list) -> list[dict]:
    """Cari di excel_files berdasarkan Part Number."""
    results = []
    q = query.strip().upper()
    if not q:
        return results
    seen = set()
    for fi in excel_files:
        for pn_key, indices in fi.get("part_number_index", {}).items():
            if q in pn_key:
                row = fi["dataframe"].iloc[indices[0]]
                pn  = str(row.get("part_number", "")).strip()
                if pn and pn.upper() not in seen:
                    seen.add(pn.upper())
                    results.append({
                        "part_number": pn,
                        "part_name":   str(row.get("part_name", "")).strip(),
                        "file":        fi.get("simple_name", ""),
                    })
                if len(results) >= 20:
                    return results
    return results


def _search_excel_by_name(query: str, excel_files: list) -> list[dict]:
    """Cari di excel_files berdasarkan Part Name."""
    results  = []
    q_words  = query.strip().upper().split()
    if not q_words:
        return results
    seen = set()
    for fi in excel_files:
        pni = fi.get("part_name_index", {})
        matching = set()
        for word in pni:
            for qw in q_words:
                if qw in word or word in qw:
                    matching.update(pni[word])
        for idx in matching:
            try:
                row  = fi["dataframe"].iloc[idx]
                pn   = str(row.get("part_number", "")).strip()
                name = str(row.get("part_name",   "")).strip()
                if pn and pn.upper() not in seen:
                    if any(qw in name.upper() for qw in q_words):
                        seen.add(pn.upper())
                        results.append({
                            "part_number": pn,
                            "part_name":   name,
                            "file":        fi.get("simple_name", ""),
                        })
            except Exception:
                continue
        if len(results) >= 20:
            break
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════════════════

# Session state keys
_SS_SELECTED_PN   = "_foto_part_selected_pn"
_SS_SELECTED_NAME = "_foto_part_selected_name"
_SS_UPLOAD_RESULT = "_foto_part_upload_result"
_SS_DELETE_RESULT = "_foto_part_delete_result"


def _mime_from_bytes(file_bytes: bytes, file_name: str) -> str:
    ext = Path(file_name).suffix.lower().lstrip(".")
    mapping = {
        "jpg":  "image/jpeg",
        "jpeg": "image/jpeg",
        "png":  "image/png",
        "webp": "image/webp",
        "gif":  "image/gif",
    }
    # Deteksi dari magic bytes
    if file_bytes[:4] == b'\x89PNG':
        return "image/png"
    if file_bytes[:2] in (b'\xff\xd8',):
        return "image/jpeg"
    return mapping.get(ext, "image/jpeg")


def _human_size(n_bytes: int) -> str:
    if n_bytes < 1024:
        return f"{n_bytes} B"
    if n_bytes < 1024 * 1024:
        return f"{n_bytes/1024:.1f} KB"
    return f"{n_bytes/1024/1024:.1f} MB"


def render_foto_part_tab():
    """
    Tab admin: Upload Foto Part.
    Dipanggil dari display_dashboard di app.py.
    """
    st.markdown("### 📷 Upload Foto Part")
    st.caption(
        "Upload foto part ke Supabase Storage per Part Number. "
        "Foto dari Supabase akan menjadi foto utama (prioritas di atas foto SIMS)."
    )

    if not _is_configured():
        st.error(
            "❌ **Supabase belum dikonfigurasi.**\n\n"
            "Tambahkan `[supabase]` url, key, dan service_key di `.streamlit/secrets.toml`."
        )
        return

    tab_single, tab_batch = st.tabs(["📷 Upload per Part", "📦 Batch Upload"])

    with tab_batch:
        _render_batch_upload_tab()

    with tab_single:
    
        # ── Notifikasi hasil upload/hapus ────────────────────────────────────────
        notif_area = st.empty()
        up_res = st.session_state.pop(_SS_UPLOAD_RESULT, None)
        dl_res = st.session_state.pop(_SS_DELETE_RESULT, None)
        if up_res:
            with notif_area.container():
                if up_res["ok"]:
                    st.success(f"✅ **{up_res['msg']}**")
                    st.balloons()
                else:
                    st.error(f"❌ {up_res['msg']}")
        if dl_res:
            with notif_area.container():
                if dl_res["ok"]:
                    st.success(f"🗑️ {dl_res['msg']}")
                else:
                    st.error(f"❌ {dl_res['msg']}")
    
        st.markdown("---")
    
        # ── Ambil excel_files dari session ───────────────────────────────────────
        excel_files = st.session_state.get("excel_files", [])
    
        # ══════════════════════════════════════════════════════════════════════════
        # PANEL KIRI: Cari Part | PANEL KANAN: Foto yang sudah ada
        # ══════════════════════════════════════════════════════════════════════════
        col_left, col_right = st.columns([2, 3])
    
        with col_left:
            st.markdown("#### 🔍 Cari Part")
    
            search_mode = st.radio(
                "Cari berdasarkan:",
                ["Part Number", "Part Name"],
                horizontal=True,
                key="foto_search_mode",
            )
    
            search_q = st.text_input(
                "Kata Kunci:",
                placeholder="WG1641230025" if search_mode == "Part Number" else "kampas rem",
                key="foto_search_q",
            )
    
            col_s1, col_s2 = st.columns([3, 1])
            with col_s1:
                do_search = st.button(
                    "🔍 Cari", type="primary", use_container_width=True, key="btn_foto_search"
                )
            with col_s2:
                if st.button("✖", use_container_width=True, key="btn_foto_clear",
                             help="Reset pencarian"):
                    st.session_state.pop(_SS_SELECTED_PN,   None)
                    st.session_state.pop(_SS_SELECTED_NAME, None)
                    st.session_state.pop("foto_search_q",    None)
                    st.rerun()
    
            # ── Hasil pencarian ───────────────────────────────────────────────
            if do_search and search_q.strip():
                with st.spinner("Mencari..."):
                    if search_mode == "Part Number":
                        found = _search_excel_by_pn(search_q, excel_files)
                    else:
                        found = _search_excel_by_name(search_q, excel_files)
    
                    # Tambahkan juga dari Supabase (part yang punya foto)
                    sb_found = _search_metadata_by_pn(search_q)
                    sb_pns   = {r["part_number"] for r in sb_found}
                    excel_pns = {r["part_number"].upper() for r in found}
                    for r in sb_found:
                        if r["part_number"] not in excel_pns:
                            found.append({
                                "part_number": r["part_number"],
                                "part_name":   "(dari Supabase)",
                                "file":        "📦 Supabase",
                            })
    
                if found:
                    st.session_state["_foto_search_results"] = found
                else:
                    st.warning("⚠️ Tidak ada part yang ditemukan.")
                    st.session_state.pop("_foto_search_results", None)
                st.rerun()
    
            # ── Tampilkan hasil & pilih ───────────────────────────────────────
            search_results = st.session_state.get("_foto_search_results", [])
            if search_results:
                st.markdown(f"**{len(search_results)} part ditemukan:**")
                for r in search_results:
                    pn    = r["part_number"]
                    pname = r["part_name"] or "—"
                    label = f"**{pn}**\n{pname[:45]}{'…' if len(pname) > 45 else ''}"
                    is_selected = st.session_state.get(_SS_SELECTED_PN) == pn
                    btn_lbl = f"{'✅ ' if is_selected else ''}{pn}"
                    if st.button(btn_lbl, key=f"fp_sel_{pn}", use_container_width=True,
                                 help=pname, type="primary" if is_selected else "secondary"):
                        st.session_state[_SS_SELECTED_PN]   = pn
                        st.session_state[_SS_SELECTED_NAME] = pname
                        invalidate_photo_cache(pn)
                        st.rerun()
    
            # ── Input manual Part Number ──────────────────────────────────────
            st.markdown("---")
            st.markdown("**Atau masukkan Part Number langsung:**")
            manual_pn = st.text_input(
                "Part Number:",
                placeholder="WG1641230025",
                key="foto_manual_pn",
            )
            if st.button("📌 Gunakan PN Ini", key="btn_foto_manual", use_container_width=True):
                if manual_pn.strip():
                    pn_clean = manual_pn.strip().upper()
                    st.session_state[_SS_SELECTED_PN]   = pn_clean
                    st.session_state[_SS_SELECTED_NAME] = ""
                    invalidate_photo_cache(pn_clean)
                    st.rerun()
                else:
                    st.warning("Masukkan Part Number terlebih dahulu.")
    
        with col_right:
            selected_pn   = st.session_state.get(_SS_SELECTED_PN)
            selected_name = st.session_state.get(_SS_SELECTED_NAME, "")
    
            if not selected_pn:
                st.info("👈 Pilih part dari hasil pencarian, atau masukkan Part Number di kiri.")
                _render_supabase_gallery()
                return
    
            st.markdown(f"#### 📦 **{selected_pn}**")
            if selected_name and selected_name != "(dari Supabase)":
                st.caption(selected_name)
    
            # ── Foto yang sudah ada di Supabase ──────────────────────────────
            existing = _load_metadata(selected_pn)
    
            if existing:
                st.markdown(f"**📸 {len(existing)} foto di Supabase:**")
                thumb_cols = st.columns(min(len(existing), 3))
                for ci, rec in enumerate(existing):
                    with thumb_cols[ci % 3]:
                        url = rec.get("storage_url", "")
                        try:
                            resp = requests.get(url, timeout=8)
                            if resp.status_code == 200:
                                st.image(resp.content, use_container_width=True,
                                         caption=rec.get("file_name", "")[:20])
                            else:
                                st.caption(f"⚠️ Gagal muat\n{rec.get('file_name','')[:20]}")
                        except Exception:
                            st.caption(f"⚠️ Error\n{rec.get('file_name','')[:20]}")
    
                        size_str = _human_size(rec.get("file_size") or 0)
                        upby     = rec.get("uploaded_by") or "—"
                        ts       = (rec.get("created_at") or "")[:16].replace("T", " ")
                        st.caption(f"📦 {size_str} | 👤 {upby}\n🕐 {ts}")
    
                        if st.button(
                            "🗑️ Hapus", key=f"fp_del_{rec['id']}", use_container_width=True,
                            help=f"Hapus {rec['file_name']}"
                        ):
                            _do_delete(rec, selected_pn)
    
            else:
                st.info("📷 Belum ada foto di Supabase untuk part ini.")
    
    
            # ── Paste dari Clipboard (Ctrl+V) ────────────────────────────────
            st.markdown("**📋 Atau Paste dari Clipboard (Ctrl+V):**")
    
            paste_b64_key = f"_paste_b64_{selected_pn}"
            has_paste     = bool(st.session_state.get(paste_b64_key, ""))
    
    
            st.markdown("---")
    
            # ── Upload Foto Baru ──────────────────────────────────────────────
            st.markdown("**⬆️ Upload Foto Baru:**")
            uploaded_files = st.file_uploader(
                "Pilih file foto (bisa lebih dari 1):",
                type=ALLOWED_TYPES,
                accept_multiple_files=True,
                key=f"foto_uploader_{selected_pn}",
                help=f"Format: JPG, JPEG, PNG, WEBP | Maks {MAX_FILE_SIZE_MB} MB per file",
            )
    
            if uploaded_files:
                st.markdown(f"**{len(uploaded_files)} file dipilih:**")
                prev_cols = st.columns(min(len(uploaded_files), 3))
                all_ok    = True
                for ci, uf in enumerate(uploaded_files):
                    with prev_cols[ci % 3]:
                        fb = uf.read()
                        uf.seek(0)
                        sz_mb = len(fb) / 1024 / 1024
                        if sz_mb > MAX_FILE_SIZE_MB:
                            st.error(f"❌ {uf.name}\nTerlalu besar ({sz_mb:.1f} MB)")
                            all_ok = False
                        else:
                            st.image(fb, use_container_width=True, caption=uf.name[:25])
                            st.caption(f"📦 {_human_size(len(fb))}")
    
                if all_ok:
                    user = st.session_state.get("current_user", {})
                    upby = user.get("username", "admin") if isinstance(user, dict) else "admin"
    
                    if st.button(
                        f"🚀 Upload {len(uploaded_files)} Foto ke Supabase",
                        type="primary", use_container_width=True,
                        key=f"btn_do_upload_{selected_pn}",
                    ):
                        _do_upload(uploaded_files, selected_pn, upby)
    
    
_SS_BATCH_RESULT      = "_foto_part_batch_result"
_SS_BATCH_DEL_RESULT  = "_foto_part_batch_del_result"


def _fetch_pn_photo_counts(pns: list[str]) -> dict[str, int]:
    """
    Ambil jumlah foto di Supabase untuk daftar PN sekaligus (1 request).
    Return {pn_upper: count}.
    """
    if not _is_configured() or not pns:
        return {}
    pns_upper = [p.strip().upper() for p in pns if p]
    # PostgREST: ?part_number=in.(A,B,C)
    in_filter = "({})".format(",".join(pns_upper))
    try:
        resp = requests.get(
            _rest_url(METADATA_TABLE),
            headers={**_rest_headers(use_service=True), "Accept": "application/json"},
            params={
                "select":      "part_number",
                "part_number": f"in.{in_filter}",
                "limit":       "1000",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            rows = resp.json() or []
            counts: dict[str, int] = {}
            for r in rows:
                pn_key = r.get("part_number", "").upper()
                counts[pn_key] = counts.get(pn_key, 0) + 1
            return counts
    except Exception as e:
        print(f"[foto_part] ❌ _fetch_pn_photo_counts: {e}")
    return {}


def _delete_all_photos_for_pn(pn: str) -> tuple[bool, str]:
    """
    Hapus semua foto (storage + metadata) untuk satu PN.
    Return (success, message).
    """
    pn_upper = pn.strip().upper()
    records  = _load_metadata(pn_upper)
    if not records:
        return True, f"Tidak ada foto untuk {pn_upper}."

    n_total = len(records)
    n_ok    = 0
    n_err   = 0

    pn_safe  = pn_upper.replace("/", "_").replace(" ", "_")
    for rec in records:
        fn       = rec.get("file_name", "")
        rec_id   = rec.get("id")
        obj_path = rec.get("storage_path") or f"{pn_safe}/{fn}"

        ok_storage = _delete_from_storage(obj_path)
        ok_meta    = _delete_metadata(rec_id)

        if ok_meta:
            n_ok += 1
        else:
            n_err += 1

    invalidate_photo_cache(pn_upper)

    if n_err == 0:
        return True,  f"✅ Semua {n_ok} foto untuk **{pn_upper}** berhasil dihapus."
    else:
        return False, f"⚠️ {n_ok}/{n_total} foto dihapus dari {pn_upper} ({n_err} gagal)."


def _render_batch_upload_tab():
    """
    Tab Batch Upload: upload banyak foto ke banyak Part Number sekaligus.

    Mode 1 — Nama file = Part Number
      Nama file langsung dijadikan PN.
      Contoh: WG1641230025.jpg  →  PN = WG1641230025

    Mode 2 — Pakai CSV mapping
      Upload CSV dengan kolom: part_number,file_name
      Contoh baris: WG1641230025,foto_mesin.jpg
    """
    st.markdown("#### 📦 Batch Upload Foto ke Banyak Part Number")
    st.caption(
        "Upload foto untuk beberapa Part Number sekaligus. "
        "Pilih mode pemetaan di bawah."
    )

    # ── Notifikasi ────────────────────────────────────────────────────────────
    batch_res = st.session_state.pop(_SS_BATCH_RESULT, None)
    bdel_res  = st.session_state.pop(_SS_BATCH_DEL_RESULT, None)
    if batch_res:
        if batch_res["ok"]:
            st.success(f"✅ {batch_res['msg']}")
            if batch_res.get("detail"):
                with st.expander("📋 Detail hasil upload"):
                    st.text(batch_res["detail"])
        else:
            st.error(f"❌ {batch_res['msg']}")
            if batch_res.get("detail"):
                with st.expander("📋 Detail error"):
                    st.text(batch_res["detail"])
    if bdel_res:
        if bdel_res["ok"]:
            st.success(bdel_res["msg"])
        else:
            st.warning(bdel_res["msg"])

    st.markdown("---")

    mode = st.radio(
        "Mode pemetaan PN → Foto:",
        [
            "📄 Nama file = Part Number  (contoh: WG1641230025.jpg)",
            "📋 Pakai CSV mapping  (part_number, file_name)",
        ],
        key="_batch_mode",
        horizontal=False,
    )
    use_csv = mode.startswith("📋")

    # ── Upload foto ───────────────────────────────────────────────────────────
    _uploader_key = f"_batch_foto_uploader_{st.session_state.get('_batch_uploader_key', 0)}"
    batch_files = st.file_uploader(
        "Pilih foto (bisa banyak sekaligus):",
        type=ALLOWED_TYPES,
        accept_multiple_files=True,
        key=_uploader_key,
        help=f"Format: JPG, JPEG, PNG, WEBP | Maks {MAX_FILE_SIZE_MB} MB per file",
    )

    # ── Upload CSV (opsional) ─────────────────────────────────────────────────
    mapping: dict[str, str] = {}   # {file_name_lower: pn_upper}

    if use_csv:
        st.markdown("**Upload file CSV mapping:**")

        # Template download
        csv_template = "part_number,file_name\nWG1641230025,foto_mesin.jpg\nWG9000360067,kampas_rem.png\n"
        st.download_button(
            "⬇️ Download Template CSV",
            data=csv_template,
            file_name="template_batch_upload.csv",
            mime="text/csv",
            key="_batch_csv_template_dl",
        )

        _csv_key = f"_batch_csv_uploader_{st.session_state.get('_batch_csv_key', 0)}"
        csv_file = st.file_uploader(
            "Upload CSV mapping (part_number, file_name):",
            type=["csv"],
            key=_csv_key,
        )

        if csv_file:
            try:
                import csv as _csv
                import io as _io
                content = csv_file.read().decode("utf-8-sig", errors="replace")
                reader  = _csv.DictReader(_io.StringIO(content))
                rows    = list(reader)

                # Normalisasi header
                header_map: dict[str, str] = {}
                if rows:
                    for h in rows[0].keys():
                        hn = h.strip().lower().replace(" ", "_")
                        header_map[hn] = h

                pn_col   = header_map.get("part_number") or header_map.get("partnumber") or header_map.get("pn")
                fn_col   = header_map.get("file_name")   or header_map.get("filename")   or header_map.get("foto")

                if not pn_col or not fn_col:
                    st.error("❌ CSV harus punya kolom **part_number** dan **file_name**.")
                else:
                    for row in rows:
                        pn_val = str(row.get(pn_col, "")).strip().upper()
                        fn_val = str(row.get(fn_col, "")).strip()
                        if pn_val and fn_val:
                            mapping[fn_val.lower()] = pn_val

                    if mapping:
                        st.success(f"✅ CSV dibaca: {len(mapping)} baris mapping.")
                        with st.expander("👁️ Preview mapping"):
                            preview_rows = list(mapping.items())[:20]
                            for fn_k, pn_v in preview_rows:
                                st.text(f"{fn_k}  →  {pn_v}")
                            if len(mapping) > 20:
                                st.caption(f"… dan {len(mapping)-20} baris lainnya.")
                    else:
                        st.warning("⚠️ Tidak ada baris valid di CSV.")

            except Exception as e:
                st.error(f"❌ Gagal baca CSV: {e}")

    # ── Preview foto yang diupload ────────────────────────────────────────────
    if batch_files:
        st.markdown(f"**{len(batch_files)} foto dipilih:**")

        # Tentukan PN untuk setiap file
        file_pn_map: list[dict] = []

        for uf in batch_files:
            if use_csv:
                pn = mapping.get(uf.name.lower(), "")
            else:
                # Nama file tanpa ekstensi = PN
                pn = Path(uf.name).stem.strip().upper().replace(" ", "_")

            fb = uf.read()
            uf.seek(0)
            sz_mb = len(fb) / 1024 / 1024
            too_big = sz_mb > MAX_FILE_SIZE_MB
            no_pn   = not pn

            file_pn_map.append({
                "uf":      uf,
                "pn":      pn,
                "fb":      fb,
                "sz_mb":   sz_mb,
                "too_big": too_big,
                "no_pn":   no_pn,
            })

        # ── Cek status foto di Supabase untuk semua PN unik ──────────────────
        unique_pns = list({x["pn"] for x in file_pn_map if x["pn"]})
        with st.spinner("🔍 Cek status foto di Supabase..."):
            pn_counts = _fetch_pn_photo_counts(unique_pns)  # {pn: count}

        # ── PN yang di-exclude dari upload (disingkirkan dari daftar) ─────────
        excluded_key = "_batch_excluded_pns"
        if excluded_key not in st.session_state:
            st.session_state[excluded_key] = set()
        excluded_pns: set = st.session_state[excluded_key]

        # ── Header tabel ──────────────────────────────────────────────────────
        h0, h1, h2, h3, h4 = st.columns([3, 3, 2, 2, 1])
        h0.markdown("**File**")
        h1.markdown("**Part Number**")
        h2.markdown("**Ukuran**")
        h3.markdown("**Status Supabase**")
        h4.markdown("**Upload**")
        st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

        for i, item in enumerate(file_pn_map):
            pn  = item["pn"]
            cnt = pn_counts.get(pn, 0)
            is_excluded = pn in excluded_pns

            c0, c1, c2, c3, c4 = st.columns([3, 3, 2, 2, 1])

            # Nama file — coret jika di-exclude
            fn_display = item["uf"].name[:28]
            c0.markdown(f"~~{fn_display}~~" if is_excluded else fn_display)

            if item["no_pn"]:
                c1.markdown("⚠️ *Tidak ditemukan*")
            else:
                c1.text(pn)

            c2.text(_human_size(len(item["fb"])))

            # Status Supabase
            if item["no_pn"] or item["too_big"]:
                c3.markdown("—")
            elif cnt > 0:
                c3.markdown(f"📸 **{cnt}** foto ada")
            else:
                c3.markdown("🆕 Belum ada")

            # Kolom status / tombol exclude — pakai index i supaya key selalu unik
            if item["too_big"]:
                c4.markdown("❌ Besar")
            elif item["no_pn"]:
                c4.markdown("⚠️ Skip")
            elif is_excluded:
                if c4.button("↩️", key=f"_incl_{i}",
                             help=f"Masukkan kembali {pn} ke daftar upload"):
                    excluded_pns.discard(pn)
                    st.rerun()
            elif cnt > 0:
                if c4.button("✖ Skip", key=f"_excl_{i}",
                             help=f"Keluarkan {pn} dari daftar upload (foto Supabase tetap aman)"):
                    excluded_pns.add(pn)
                    st.rerun()
            else:
                c4.markdown("✅")

        st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

        # ── Panel PN yang sudah punya foto ────────────────────────────────────
        pns_with_photos = {
            pn: cnt for pn, cnt in pn_counts.items()
            if cnt > 0 and pn in unique_pns
        }
        if pns_with_photos:
            st.markdown("---")
            st.markdown("**📸 PN yang sudah punya foto di Supabase:**")
            st.caption(
                "Klik **✖ Keluarkan** untuk skip PN tersebut dari daftar upload. "
                "Foto di Supabase **tidak** akan terhapus."
            )
            for pn, cnt in sorted(pns_with_photos.items()):
                col_pn, col_cnt, col_act = st.columns([4, 2, 2])
                col_pn.markdown(f"**{pn}**")
                col_cnt.markdown(f"📸 {cnt} foto")
                if pn in excluded_pns:
                    if col_act.button("↩️ Masukkan", key=f"_panel_incl_{pn}",
                                      use_container_width=True):
                        excluded_pns.discard(pn)
                        st.rerun()
                else:
                    if col_act.button("✖ Keluarkan", key=f"_panel_excl_{pn}",
                                      use_container_width=True,
                                      help="Keluarkan dari daftar upload (foto Supabase aman)"):
                        excluded_pns.add(pn)
                        st.rerun()

        st.markdown("---")

        # ── Ringkasan ─────────────────────────────────────────────────────────
        active_items  = [x for x in file_pn_map
                         if not x["too_big"] and not x["no_pn"]
                         and x["pn"] not in excluded_pns]
        valid_count   = len(active_items)
        skipped_count = sum(1 for x in file_pn_map if x["no_pn"])
        big_count     = sum(1 for x in file_pn_map if x["too_big"])
        excl_count    = sum(1 for x in file_pn_map
                           if not x["too_big"] and not x["no_pn"]
                           and x["pn"] in excluded_pns)

        summary_parts = [f"**Siap upload:** {valid_count} foto"]
        if excl_count:
            summary_parts.append(f"🚫 {excl_count} dikeluarkan dari daftar")
        if skipped_count:
            summary_parts.append(f"⚠️ {skipped_count} tidak dikenal")
        if big_count:
            summary_parts.append(f"❌ {big_count} terlalu besar")
        st.markdown(" | ".join(summary_parts))

        if valid_count == 0:
            st.warning("Tidak ada foto yang akan diupload.")
        else:
            user = st.session_state.get("current_user", {})
            upby = user.get("username", "admin") if isinstance(user, dict) else "admin"

            if st.button(
                f"🚀 Batch Upload {valid_count} Foto",
                type="primary",
                use_container_width=True,
                key="_btn_do_batch_upload",
            ):
                # Bersihkan excluded setelah upload
                st.session_state.pop(excluded_key, None)
                _do_batch_upload(active_items, upby)

    else:
        if use_csv:
            st.info("📤 Upload CSV mapping dan foto di atas untuk memulai.")
        else:
            st.info(
                "📤 Upload foto di atas.\n\n"
                "**Contoh nama file:** `WG1641230025.jpg` → diupload ke Part Number **WG1641230025**"
            )


def _do_batch_upload(items: list[dict], uploaded_by: str):
    """
    Proses batch upload: tiap item = {uf, pn, fb, sz_mb}.
    Menampilkan progress bar dan ringkasan per PN.
    """
    total         = len(items)
    success_count = 0
    errors: list[str] = []
    pn_results: dict[str, list[str]] = {}   # {pn: ["ok:fname", "err:fname:msg"]}

    prog = st.progress(0, text="Memulai batch upload...")

    for i, item in enumerate(items):
        uf   = item["uf"]
        pn   = item["pn"]
        fb   = item["fb"]
        prog.progress(i / total, text=f"[{i+1}/{total}] {uf.name} → {pn}")

        mime_type = _mime_from_bytes(fb, uf.name)

        ts_str    = time.strftime("%Y%m%d_%H%M%S")
        safe_name = uf.name.replace(" ", "_")
        file_name = f"{ts_str}_{safe_name}"

        ok_storage, path_or_err = _upload_to_storage(pn, file_name, fb, mime_type)
        if not ok_storage:
            msg = f"  • {uf.name}: {path_or_err}"
            errors.append(msg)
            pn_results.setdefault(pn, []).append(f"err:{uf.name}:{path_or_err}")
            continue

        ok_meta = _save_metadata(pn, file_name, path_or_err, len(fb), uploaded_by)
        pn_results.setdefault(pn, []).append(
            f"ok:{uf.name}" if ok_meta else f"partial:{uf.name}"
        )

        if not ok_meta:
            errors.append(f"  • {uf.name}: gagal simpan metadata (foto mungkin terupload)")
        success_count += 1

        # Invalidate cache per PN
        invalidate_photo_cache(pn)

    prog.progress(1.0, text="Selesai!")
    time.sleep(0.4)
    prog.empty()

    # Buat detail ringkasan per PN
    detail_lines = []
    for pn, results in sorted(pn_results.items()):
        ok_files  = [r.split(":", 2)[1] for r in results if r.startswith("ok:")]
        err_files = [r.split(":", 2)[1:] for r in results if r.startswith("err:")]
        part_lines = [f"  {pn}:"]
        for fn in ok_files:
            part_lines.append(f"    ✅ {fn}")
        for parts in err_files:
            part_lines.append(f"    ❌ {parts[0]}: {parts[1] if len(parts)>1 else ''}")
        detail_lines.extend(part_lines)
    detail_str = "\n".join(detail_lines)

    n_pn = len(pn_results)
    if errors:
        st.session_state[_SS_BATCH_RESULT] = {
            "ok":     False,
            "msg":    f"Batch upload selesai: {success_count}/{total} berhasil ke {n_pn} Part Number.",
            "detail": detail_str,
        }
    else:
        st.session_state[_SS_BATCH_RESULT] = {
            "ok":     True,
            "msg":    f"{success_count} foto berhasil diupload ke {n_pn} Part Number.",
            "detail": detail_str,
        }
        # ── Reset tampilan batch upload jika sukses penuh ──────────────────
        # Hapus file uploader dengan mengganti key-nya (trigger Streamlit reset)
        st.session_state["_batch_uploader_key"] = st.session_state.get("_batch_uploader_key", 0) + 1
        st.session_state["_batch_csv_key"]      = st.session_state.get("_batch_csv_key", 0) + 1
        # Bersihkan excluded PNs
        st.session_state.pop("_batch_excluded_pns", None)

    st.rerun()


def _do_upload(uploaded_files, pn: str, uploaded_by: str):
    """Proses upload semua file yang dipilih ke Supabase."""
    success_count = 0
    errors        = []

    prog = st.progress(0, text="Memulai upload...")
    total = len(uploaded_files)

    for i, uf in enumerate(uploaded_files):
        prog.progress((i) / total, text=f"Upload {uf.name}...")

        file_bytes = uf.read()
        mime_type  = _mime_from_bytes(file_bytes, uf.name)

        # Buat nama file unik: {timestamp}_{original_name}
        ts_str    = time.strftime("%Y%m%d_%H%M%S")
        safe_name = uf.name.replace(" ", "_")
        file_name = f"{ts_str}_{safe_name}"

        # 1. Upload ke storage
        ok_storage, path_or_err = _upload_to_storage(pn, file_name, file_bytes, mime_type)
        if not ok_storage:
            errors.append(f"{uf.name}: {path_or_err}")
            continue

        # 2. Simpan metadata
        ok_meta = _save_metadata(pn, file_name, path_or_err, len(file_bytes), uploaded_by)
        if not ok_meta:
            errors.append(f"{uf.name}: gagal simpan metadata (foto mungkin terupload)")
            success_count += 1  # foto sudah di storage, anggap partial success
            continue

        success_count += 1

    prog.progress(1.0, text="Selesai!")
    time.sleep(0.5)
    prog.empty()

    # Invalidate cache
    invalidate_photo_cache(pn)

    if errors:
        msg = (
            f"Upload selesai: {success_count}/{total} berhasil.\n"
            + "\n".join(f"• {e}" for e in errors)
        )
        st.session_state[_SS_UPLOAD_RESULT] = {"ok": False, "msg": msg}
    else:
        st.session_state[_SS_UPLOAD_RESULT] = {
            "ok":  True,
            "msg": f"{success_count} foto berhasil diupload untuk {pn}.",
        }
    st.rerun()


def _do_delete(rec: dict, pn: str):
    """Hapus satu foto: dari storage + metadata."""
    file_name  = rec.get("file_name", "")
    record_id  = rec.get("id")
    pn_safe    = pn.strip().upper().replace("/", "_").replace(" ", "_")
    obj_path   = rec.get("storage_path") or f"{pn_safe}/{file_name}"

    with st.spinner(f"Menghapus {file_name}..."):
        ok_storage = _delete_from_storage(obj_path)
        ok_meta    = _delete_metadata(record_id)

    invalidate_photo_cache(pn)

    if ok_meta:  # metadata terhapus = anggap sukses
        st.session_state[_SS_DELETE_RESULT] = {
            "ok":  True,
            "msg": f"Foto '{file_name}' berhasil dihapus dari {pn}.",
        }
    else:
        st.session_state[_SS_DELETE_RESULT] = {
            "ok":  False,
            "msg": f"Gagal menghapus metadata foto '{file_name}'.",
        }
    st.rerun()


def _render_supabase_gallery():
    """Tampilkan galeri semua PN yang punya foto di Supabase."""
    if not _is_configured():
        return

    with st.expander("📚 Galeri — Part yang sudah punya foto di Supabase", expanded=False):
        try:
            resp = requests.get(
                _rest_url(METADATA_TABLE),
                headers={**_rest_headers(use_service=True), "Accept": "application/json"},
                params={
                    "select": "part_number,file_name,storage_url,created_at",
                    "order":  "created_at.desc",
                    "limit":  "60",
                },
                timeout=10,
            )
            if resp.status_code != 200 or not resp.json():
                st.caption("Belum ada foto yang diupload.")
                return

            rows = resp.json()
            # Group by part_number
            groups: dict = {}
            for r in rows:
                pn = r["part_number"]
                groups.setdefault(pn, []).append(r)

            st.caption(f"{len(groups)} part number punya foto di Supabase.")
            for pn, recs in list(groups.items())[:20]:
                st.markdown(f"**{pn}** — {len(recs)} foto")

        except Exception as e:
            st.caption(f"⚠️ Gagal memuat galeri: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  SETUP GUIDE — tampilkan petunjuk SQL
# ═══════════════════════════════════════════════════════════════════════════════

def render_setup_guide():
    """Tampilkan panduan setup untuk admin."""
    with st.expander("🛠️ Panduan Setup Supabase untuk Foto Part", expanded=False):
        st.markdown("""
**1. Buat Bucket Storage**
- Buka Supabase Dashboard → Storage → New Bucket
- Nama: `part-photos`
- Aktifkan: **Public bucket** ✅

**2. Buat Tabel Metadata (SQL Editor):**
```sql
CREATE TABLE IF NOT EXISTS part_photos (
    id          BIGSERIAL PRIMARY KEY,
    part_number TEXT NOT NULL,
    file_name   TEXT NOT NULL,
    storage_url TEXT NOT NULL,
    file_size   INTEGER,
    uploaded_by TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_part_photos_pn
    ON part_photos (part_number);
ALTER TABLE part_photos DISABLE ROW LEVEL SECURITY;
```

**3. Tambahkan `service_key` di secrets.toml:**
```toml
[supabase]
url         = "https://xxxx.supabase.co"
key         = "eyJhbGci..."      # anon key
service_key = "eyJhbGci..."      # service_role key ← TAMBAHKAN INI
table       = "users"
```

**4. Tambahkan tab di `app.py`:**
```python
# Import di bagian atas app.py:
try:
    from admin_foto_part import render_foto_part_tab, get_supabase_photo_urls
    FOTO_PART_ENABLED = True
except ImportError:
    FOTO_PART_ENABLED = False
    def render_foto_part_tab(): st.warning("admin_foto_part.py tidak ditemukan.")
    def get_supabase_photo_urls(pn): return []

# Di get_allowed_tabs() → tambah ke admin tabs:
# (sudah otomatis jika di-append di ALL_TAB_DEFS)

# Di display_dashboard → ALL_TAB_DEFS.append:
ALL_TAB_DEFS.append(("tab_foto_part", "📷 Upload Foto Part", "__foto_part__"))

# Di render loop → tambah kondisi:
elif fn == "__foto_part__":
    render_foto_part_tab()
```

**5. Gabungkan foto Supabase + SIMS di `display_search_results`:**
```python
# Sebelum img_links dipakai, prepend foto Supabase:
if FOTO_PART_ENABLED:
    sb_urls   = get_supabase_photo_urls(pn)
    img_links = sb_urls + (img_links or [])
```
        """)