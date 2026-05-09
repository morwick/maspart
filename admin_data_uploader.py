"""
ADMIN DATA UPLOADER (Supabase Storage)
======================================
Modul terpadu untuk upload/replace file data ke Supabase Storage.
Semua dataset disimpan di bucket privat `data`.

    Dataset    Path di Storage   Path lokal (fallback)
    ---------  ----------------  ---------------------------
    stok       stok.xlsx         data/stok/stok.xlsx
    harga      harga.xlsx        data/harga/harga.xlsx
    populasi   populasi.xlsx     data/populasi/populasi.xlsx

Cara pakai di app.py:

    from admin_data_uploader import (
        download_dataset,
        render_data_uploader_tab,
    )

    # saat load file:
    file_bytes = download_dataset("stok")

    # tab admin:
    render_data_uploader_tab()
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import streamlit as st


# ── Registry dataset ─────────────────────────────────────────────────────────
DATASETS: dict = {
    "stok": {
        "label":        "Stok",
        "icon":         "📦",
        "bucket":       "data",
        "remote_path":  "stok.xlsx",
        "local_path":   Path("data/stok/stok.xlsx"),
        "session_keys": ["stok_data"],
    },
    "harga": {
        "label":        "Harga",
        "icon":         "💰",
        "bucket":       "data",
        "remote_path":  "harga.xlsx",
        "local_path":   Path("data/harga/harga.xlsx"),
        "session_keys": ["harga_data", "harga_lookup"],
    },
    "populasi": {
        "label":        "Populasi Unit",
        "icon":         "🚛",
        "bucket":       "data",
        "remote_path":  "populasi.xlsx",
        "local_path":   Path("data/populasi/populasi.xlsx"),
        "session_keys": ["populasi_df"],
    },
}

MAX_FILE_SIZE_MB = 20

_SS_RESULT_PREFIX = "_data_uploader_result_"


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
        }
    except Exception:
        return {"url": "", "key": "", "service_key": ""}


def _is_configured() -> bool:
    c = _get_cfg()
    return bool(
        c["url"] and c["key"]
        and "supabase.co" in c["url"]
        and not c["url"].startswith("https://xxxxxxxxxxx")
    )


def _storage_url(bucket: str, path: str = "") -> str:
    base = f"{_get_cfg()['url']}/storage/v1/object/{bucket}"
    return f"{base}/{path}" if path else base


def _storage_headers(content_type: str = "application/octet-stream",
                     use_service: bool = True) -> dict:
    cfg = _get_cfg()
    key = cfg["service_key"] if use_service and cfg["service_key"] else cfg["key"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  content_type,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CORE: UPLOAD / DOWNLOAD / INFO
# ═══════════════════════════════════════════════════════════════════════════════

def upload_dataset(key: str, file_bytes: bytes) -> tuple[bool, str]:
    """Upload file dataset ke Supabase Storage (replace via PUT, fallback POST)."""
    if key not in DATASETS:
        return False, f"Dataset '{key}' tidak terdaftar."
    if not _is_configured():
        return False, "Supabase belum dikonfigurasi di secrets.toml."

    ds  = DATASETS[key]
    url = _storage_url(ds["bucket"], ds["remote_path"])
    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    headers = _storage_headers(mime, use_service=True)

    try:
        head = requests.head(url, headers=headers, timeout=10)
        exists = head.status_code == 200
        if exists:
            resp = requests.put(url, headers=headers, data=file_bytes, timeout=60)
        else:
            resp = requests.post(url, headers=headers, data=file_bytes, timeout=60)

        if resp.status_code in (200, 201):
            verb = "diperbarui" if exists else "diunggah"
            return True, f"`{ds['remote_path']}` berhasil {verb} ke bucket `{ds['bucket']}`."

        # Fallback: kalau PUT gagal coba POST
        if exists:
            resp2 = requests.post(url, headers=headers, data=file_bytes, timeout=60)
            if resp2.status_code in (200, 201):
                return True, f"`{ds['remote_path']}` berhasil diunggah ke bucket `{ds['bucket']}`."
            return False, f"Upload gagal: {resp2.status_code} — {resp2.text[:200]}"

        return False, f"Upload gagal: {resp.status_code} — {resp.text[:200]}"

    except requests.exceptions.Timeout:
        return False, "Timeout saat menghubungi Supabase Storage."
    except Exception as e:
        return False, f"Error: {e}"


def download_dataset(key: str) -> Optional[bytes]:
    """
    Download file dataset dari Supabase Storage. Return bytes atau None.
    Pakai service_key supaya bekerja walau bucket private dan tanpa policy
    SELECT khusus untuk anon — dipanggil dari server-side Streamlit.
    """
    if key not in DATASETS or not _is_configured():
        return None
    ds = DATASETS[key]
    try:
        resp = requests.get(
            _storage_url(ds["bucket"], ds["remote_path"]),
            headers=_storage_headers(use_service=True),
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.content
    except Exception as e:
        print(f"[data_uploader] ❌ download '{key}': {e}")
    return None


def get_dataset_info(key: str) -> Optional[dict]:
    """Ambil metadata file dataset dari Supabase Storage."""
    if key not in DATASETS or not _is_configured():
        return None
    ds  = DATASETS[key]
    cfg = _get_cfg()
    try:
        url = f"{cfg['url']}/storage/v1/object/list/{ds['bucket']}"
        resp = requests.post(
            url,
            headers={
                "apikey":        cfg["service_key"] or cfg["key"],
                "Authorization": f"Bearer {cfg['service_key'] or cfg['key']}",
                "Content-Type":  "application/json",
            },
            json={"prefix": "", "limit": 100},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        for f in resp.json():
            if f.get("name") == ds["remote_path"]:
                meta    = f.get("metadata", {})
                size_b  = meta.get("size", 0)
                updated = f.get("updated_at", f.get("created_at", ""))
                if updated:
                    try:
                        dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                        updated = dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
                return {
                    "name":       f.get("name"),
                    "size_kb":    round(size_b / 1024, 1),
                    "updated_at": updated,
                    "bucket":     ds["bucket"],
                }
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  FALLBACK LOKAL
# ═══════════════════════════════════════════════════════════════════════════════

def upload_dataset_to_local(key: str, file_bytes: bytes) -> tuple[bool, str]:
    if key not in DATASETS:
        return False, f"Dataset '{key}' tidak terdaftar."
    p = DATASETS[key]["local_path"]
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(file_bytes)
        return True, f"File disimpan lokal di `{p}`."
    except Exception as e:
        return False, f"Gagal simpan lokal: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
#  VALIDASI
# ═══════════════════════════════════════════════════════════════════════════════

def _file_size_ok(file_bytes: bytes) -> bool:
    return len(file_bytes) <= MAX_FILE_SIZE_MB * 1024 * 1024


def _validate_xlsx(file_bytes: bytes) -> tuple[bool, str, Optional[pd.DataFrame]]:
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), nrows=5)
        if df.empty:
            return False, "File Excel kosong.", None
        return True, f"OK — {len(df.columns)} kolom terdeteksi.", df
    except Exception as e:
        return False, f"Tidak bisa dibaca sebagai Excel: {e}", None


# ═══════════════════════════════════════════════════════════════════════════════
#  CACHE INVALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _invalidate_dataset_cache(key: str):
    """Hapus session_state keys terkait dataset agar app reload data."""
    if key not in DATASETS:
        return
    for sk in DATASETS[key]["session_keys"]:
        st.session_state.pop(sk, None)


# ═══════════════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════════════════

def _render_uploader_section(key: str):
    """Render UI uploader untuk satu dataset."""
    ds          = DATASETS[key]
    label       = ds["label"]
    icon        = ds["icon"]
    file_label  = ds["remote_path"]
    ss_result   = _SS_RESULT_PREFIX + key

    st.markdown(f"#### {icon} {label}")
    st.caption(f"File: `{file_label}` di bucket `{ds['bucket']}`")

    # ── Notifikasi hasil aksi terakhir ───────────────────────────────────
    result = st.session_state.get(ss_result)
    if result:
        rtype = result.get("type", "info")
        text  = f"**{result.get('title','')}**\n\n{result.get('body','')}"
        getattr(st, rtype if rtype in ("success","warning","error","info") else "info")(text)
        if st.button("✖ Tutup", key=f"btn_close_{key}"):
            st.session_state.pop(ss_result, None)
            st.rerun()

    # ── Status & info file di Supabase ───────────────────────────────────
    if _is_configured():
        info = get_dataset_info(key)
        if info:
            st.info(
                f"📄 **File saat ini:** `{info['name']}`  \n"
                f"📦 **Ukuran:** {info['size_kb']} KB  \n"
                f"🕒 **Update terakhir:** {info['updated_at']}"
            )
            with st.expander(f"⬇️ Download `{file_label}` saat ini", expanded=False):
                if st.button("📥 Ambil dari Supabase", key=f"btn_dl_{key}"):
                    with st.spinner("Mengunduh..."):
                        b = download_dataset(key)
                    if b:
                        st.download_button(
                            label="💾 Simpan ke komputer",
                            data=b,
                            file_name=file_label,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"btn_save_{key}",
                        )
                    else:
                        st.error("❌ Gagal mengunduh.")
        else:
            st.warning(f"⚠️ Belum ada `{file_label}` di Supabase.")
    else:
        st.error("❌ Supabase belum terkonfigurasi — file akan disimpan lokal saja.")

    # ── File uploader ────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        label=f"Pilih file `{file_label}`",
        type=["xlsx"],
        accept_multiple_files=False,
        key=f"uploader_{key}",
        help=f"Maksimum {MAX_FILE_SIZE_MB} MB",
    )

    if uploaded is None:
        return

    file_bytes = uploaded.read()
    size_kb    = len(file_bytes) / 1024
    st.caption(f"📦 Ukuran file: **{size_kb:.1f} KB**")

    if not _file_size_ok(file_bytes):
        st.error(f"❌ File terlalu besar. Maksimum {MAX_FILE_SIZE_MB} MB.")
        return

    is_valid, msg_valid, df_preview = _validate_xlsx(file_bytes)
    if not is_valid:
        st.error(f"❌ {msg_valid}")
        return

    st.success(f"✅ {msg_valid}")
    with st.expander("👁️ Preview 5 baris", expanded=False):
        st.dataframe(df_preview, use_container_width=True)

    col_up, col_cancel = st.columns([3, 1])
    with col_up:
        if st.button(f"🚀 Upload & Replace `{file_label}`",
                     type="primary", use_container_width=True,
                     key=f"btn_upload_{key}"):
            _do_upload(key, file_bytes)
            st.rerun()
    with col_cancel:
        if st.button("✖️ Batal", use_container_width=True, key=f"btn_cancel_{key}"):
            st.session_state.pop(ss_result, None)
            st.rerun()


def _do_upload(key: str, file_bytes: bytes):
    """Eksekusi upload — Supabase dulu, fallback lokal kalau gagal."""
    ss_result = _SS_RESULT_PREFIX + key

    if _is_configured():
        with st.spinner("⏳ Mengunggah ke Supabase..."):
            ok, msg = upload_dataset(key, file_bytes)
        if ok:
            st.session_state[ss_result] = {
                "type":  "success",
                "title": "Upload berhasil! 🎉",
                "body":  msg,
            }
            _invalidate_dataset_cache(key)
            return

        # Supabase gagal → simpan lokal
        sb_err = msg
        with st.spinner("⏳ Supabase gagal, simpan lokal..."):
            ok_local, msg_local = upload_dataset_to_local(key, file_bytes)
        if ok_local:
            st.session_state[ss_result] = {
                "type":  "warning",
                "title": "Supabase gagal — file disimpan lokal",
                "body":  f"**Supabase error:** {sb_err}\n\n**Lokal:** {msg_local}",
            }
            _invalidate_dataset_cache(key)
        else:
            st.session_state[ss_result] = {
                "type":  "error",
                "title": "Upload gagal sepenuhnya",
                "body":  f"**Supabase:** {sb_err}\n\n**Lokal:** {msg_local}",
            }
        return

    # Tidak ada Supabase
    with st.spinner("⏳ Menyimpan ke lokal..."):
        ok, msg = upload_dataset_to_local(key, file_bytes)
    if ok:
        st.session_state[ss_result] = {
            "type": "success", "title": "Disimpan lokal", "body": msg,
        }
        _invalidate_dataset_cache(key)
    else:
        st.session_state[ss_result] = {
            "type": "error", "title": "Gagal menyimpan", "body": msg,
        }


def render_data_uploader_tab():
    """Tab admin: kelola upload stok + harga + populasi."""
    st.markdown("### 📊 Upload Data")
    st.caption("Replace file Excel data — perubahan langsung dipakai semua user.")
    st.markdown("---")

    sub_keys = list(DATASETS.keys())  # ["stok", "harga", "populasi"]
    sub_tabs = st.tabs([f"{DATASETS[k]['icon']} {DATASETS[k]['label']}" for k in sub_keys])
    for tab_obj, k in zip(sub_tabs, sub_keys):
        with tab_obj:
            _render_uploader_section(k)
