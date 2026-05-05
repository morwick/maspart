"""
ADMIN STOK UPLOADER
===================
Modul untuk upload/replace file stok.xlsx ke GitHub.
Hanya bisa diakses oleh admin.

Cara Pakai di app.py:
---------------------
    from admin_stok_uploader import render_stok_uploader_tab

Lalu panggil di dalam tab admin:
    render_stok_uploader_tab()

Pastikan github_sync.py sudah dikonfigurasi di .streamlit/secrets.toml:
    [github]
    token  = "ghp_xxxxxxxxxxxxxxxxxxxx"
    repo   = "morwick/maspart"
    branch = "main"
"""

from __future__ import annotations

import io
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# Path file stok di GitHub (relatif dari root repo)
STOK_GITHUB_PATH = "data/stok/stok.xlsx"

# Path lokal fallback (Streamlit Cloud / lokal)
STOK_LOCAL_PATH  = Path("data/stok/stok.xlsx")

# Batas ukuran file upload (MB)
MAX_FILE_SIZE_MB = 20

# ── Session State Keys ───────────────────────────────────────────────────────────
_SS_RESULT        = "stok_uploader_result"        # dict: {type, title, body} | None
_SS_SHOW_BALLOON  = "stok_uploader_show_balloon"  # bool


# ── Internal helpers ────────────────────────────────────────────────────────────

def _get_github_sync():
    try:
        import github_sync
        return github_sync
    except ImportError:
        return None


def _file_size_ok(file_bytes: bytes) -> bool:
    return len(file_bytes) <= MAX_FILE_SIZE_MB * 1024 * 1024


def _validate_xlsx(file_bytes: bytes) -> tuple[bool, str, pd.DataFrame | None]:
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), nrows=5)
        if df.empty:
            return False, "File Excel kosong (tidak ada data).", None
        return True, f"OK — {len(df.columns)} kolom terdeteksi.", df
    except Exception as e:
        return False, f"File tidak bisa dibaca sebagai Excel: {e}", None


def _show_result_in(placeholder, result: dict):
    """Render notifikasi hasil upload ke dalam placeholder."""
    rtype = result.get("type", "info")
    title = result.get("title", "")
    body  = result.get("body", "")
    text  = f"**{title}**\n\n{body}" if body else f"**{title}**"

    with placeholder.container():
        if rtype == "success":
            st.success(text)
        elif rtype == "warning":
            st.warning(text)
        elif rtype == "error":
            st.error(text)
        else:
            st.info(text)

        if st.button("✖ Tutup", key="btn_close_notif"):
            st.session_state.pop(_SS_RESULT, None)
            st.rerun()


# ── Public: upload logic ─────────────────────────────────────────────────────────

def upload_stok_to_github(file_bytes: bytes) -> tuple[bool, str]:
    gh = _get_github_sync()
    if gh is None:
        return False, "Modul `github_sync` tidak ditemukan."
    if not gh.gh_is_configured():
        return False, (
            "GitHub belum dikonfigurasi. "
            "Tambahkan [github] token, repo, dan branch di `.streamlit/secrets.toml`."
        )
    commit_msg = f"Update stok.xlsx via admin upload — {time.strftime('%Y-%m-%d %H:%M:%S')}"
    ok = gh.gh_write_bytes(STOK_GITHUB_PATH, file_bytes, commit_msg=commit_msg)
    if ok:
        return True, f"File berhasil diunggah ke `{STOK_GITHUB_PATH}` di GitHub."
    return False, "Upload ke GitHub gagal. Cek log Streamlit untuk detail error."


def upload_stok_to_local(file_bytes: bytes) -> tuple[bool, str]:
    try:
        STOK_LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        STOK_LOCAL_PATH.write_bytes(file_bytes)
        return True, f"File disimpan lokal di `{STOK_LOCAL_PATH}`."
    except Exception as e:
        return False, f"Gagal simpan lokal: {e}"


def get_stok_info_from_github() -> dict | None:
    gh = _get_github_sync()
    if gh is None or not gh.gh_is_configured():
        return None
    try:
        import requests
        token  = st.secrets["github"]["token"]
        repo   = st.secrets["github"]["repo"]
        branch = st.secrets["github"].get("branch", "main")
        r = requests.get(
            f"https://api.github.com/repos/{repo}/contents/{STOK_GITHUB_PATH}",
            headers={
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
            },
            params={"ref": branch},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            return {
                "name":     data.get("name"),
                "size_kb":  round(data.get("size", 0) / 1024, 1),
                "sha":      data.get("sha", "")[:10] + "...",
                "html_url": data.get("html_url", ""),
            }
    except Exception:
        pass
    return None


# ── Public: Streamlit UI ─────────────────────────────────────────────────────────

def render_stok_uploader_tab():
    st.markdown("### 📤 Upload File Stok Terbaru")
    st.caption(
        f"Upload `stok.xlsx` terbaru untuk menggantikan file di `{STOK_GITHUB_PATH}`."
    )

    # ═══════════════════════════════════════════════════════════════════
    # ZONA NOTIFIKASI — st.empty() di paling atas.
    # Setelah rerun, session_state masih ada sehingga notifikasi tetap
    # muncul meski file_uploader sudah di-reset oleh Streamlit.
    # ═══════════════════════════════════════════════════════════════════
    notif_placeholder = st.empty()

    result = st.session_state.get(_SS_RESULT)
    if result:
        _show_result_in(notif_placeholder, result)
        if st.session_state.pop(_SS_SHOW_BALLOON, False):
            st.balloons()

    # ── Info file saat ini di GitHub ────────────────────────────────────
    gh        = _get_github_sync()
    github_ok = gh is not None and gh.gh_is_configured()

    if github_ok:
        with st.spinner("Mengambil info file dari GitHub..."):
            info = get_stok_info_from_github()
        if info:
            st.info(
                f"📄 **File saat ini:** `{info['name']}`  \n"
                f"📦 **Ukuran:** {info['size_kb']} KB  \n"
                f"🔑 **SHA:** `{info['sha']}`  \n"
                f"🔗 [Lihat di GitHub]({info['html_url']})"
            )
        else:
            st.warning("⚠️ Belum ada `stok.xlsx` di GitHub, atau belum bisa diambil infonya.")
    else:
        st.warning(
            "⚠️ GitHub **belum terkonfigurasi**. "
            "File akan disimpan **lokal** saja (tidak persisten di Streamlit Cloud)."
        )

    st.markdown("---")

    # ── File uploader ────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        label="Pilih file stok.xlsx",
        type=["xlsx"],
        accept_multiple_files=False,
        key="stok_uploader_widget",
        help=f"Maksimum ukuran file: {MAX_FILE_SIZE_MB} MB",
    )

    if uploaded is not None:
        file_bytes = uploaded.read()

        size_kb = len(file_bytes) / 1024
        st.caption(f"📦 Ukuran file: **{size_kb:.1f} KB** ({size_kb/1024:.2f} MB)")

        if not _file_size_ok(file_bytes):
            st.error(f"❌ File terlalu besar. Maksimum {MAX_FILE_SIZE_MB} MB.")
            return

        is_valid, msg_valid, df_preview = _validate_xlsx(file_bytes)
        if not is_valid:
            st.error(f"❌ Validasi gagal: {msg_valid}")
            return

        st.success(f"✅ Validasi file: {msg_valid}")

        with st.expander("👁️ Preview 5 baris pertama", expanded=False):
            st.dataframe(df_preview, use_container_width=True)

        st.markdown("---")

        col_upload, col_cancel = st.columns([3, 1])

        with col_upload:
            if st.button(
                "🚀 Upload & Replace stok.xlsx",
                type="primary",
                use_container_width=True,
                key="btn_upload_stok",
            ):
                # ── Proses upload ────────────────────────────────────────
                if github_ok:
                    with st.spinner("⏳ Mengunggah ke GitHub, mohon tunggu..."):
                        ok, msg = upload_stok_to_github(file_bytes)

                    if ok:
                        st.session_state[_SS_RESULT] = {
                            "type":  "success",
                            "title": "Upload ke GitHub berhasil!",
                            "body":  msg,
                        }
                        st.session_state[_SS_SHOW_BALLOON] = True
                        # Bersihkan cache stok agar data di-reload
                        for k in list(st.session_state.keys()):
                            if "stok" in k.lower() and k not in (
                                _SS_RESULT, _SS_SHOW_BALLOON,
                                "stok_uploader_widget",
                            ):
                                del st.session_state[k]
                    else:
                        # GitHub gagal → coba lokal
                        gh_err = msg
                        with st.spinner("⏳ GitHub gagal, mencoba simpan lokal..."):
                            ok_local, msg_local = upload_stok_to_local(file_bytes)

                        if ok_local:
                            st.session_state[_SS_RESULT] = {
                                "type":  "warning",
                                "title": "GitHub gagal — file disimpan lokal (sementara)",
                                "body":  (
                                    f"**GitHub error:** {gh_err}\n\n"
                                    f"**Lokal:** {msg_local}\n\n"
                                    "_File lokal akan hilang saat Streamlit Cloud restart._"
                                ),
                            }
                        else:
                            st.session_state[_SS_RESULT] = {
                                "type":  "error",
                                "title": "Upload gagal sepenuhnya",
                                "body":  (
                                    f"**GitHub:** {gh_err}\n\n"
                                    f"**Lokal:** {msg_local}"
                                ),
                            }
                else:
                    # Tidak ada GitHub → simpan lokal
                    with st.spinner("⏳ Menyimpan ke lokal..."):
                        ok, msg = upload_stok_to_local(file_bytes)

                    if ok:
                        st.session_state[_SS_RESULT] = {
                            "type":  "success",
                            "title": "File berhasil disimpan lokal!",
                            "body":  msg,
                        }
                        st.session_state[_SS_SHOW_BALLOON] = True
                    else:
                        st.session_state[_SS_RESULT] = {
                            "type":  "error",
                            "title": "Gagal menyimpan file",
                            "body":  msg,
                        }

                # Rerun — notifikasi dibaca dari session_state di atas
                st.rerun()

        with col_cancel:
            if st.button("✖️ Batal", use_container_width=True, key="btn_cancel_stok"):
                st.session_state.pop(_SS_RESULT, None)
                st.rerun()

    # ── Tips ─────────────────────────────────────────────────────────────
    with st.expander("ℹ️ Petunjuk & Catatan", expanded=False):
        st.markdown(
            f"""
            **Format file yang diterima:** `.xlsx` (Excel 2007+)

            **Lokasi file di GitHub:** `{STOK_GITHUB_PATH}`

            **Proses upload:**
            1. Pilih file `stok.xlsx` terbaru dari komputer Anda
            2. Sistem akan memvalidasi file (format & konten)
            3. Klik **Upload & Replace** untuk menggantikan file lama
            4. File lama di GitHub **tidak dihapus**, tapi ter-*replace* (Git history tetap ada)

            **Catatan:**
            - Hanya admin yang dapat mengakses fitur ini
            - Ukuran maksimum file: **{MAX_FILE_SIZE_MB} MB**
            - Perubahan langsung aktif setelah upload berhasil
            - Jika GitHub tidak terkonfigurasi, file hanya tersimpan **lokal** dan akan **hilang** saat Streamlit Cloud restart
            """
        )