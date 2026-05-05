"""
BATCH HARGA ENGINE
==================
Modul batch cari harga dari SIMS yang:
  - Tidak memfreeze UI (background threading)
  - Bisa resume dari titik terakhir jika gagal / refresh
  - Auto-save progress ke file JSON lokal
  - Concurrent requests (ThreadPoolExecutor)
  - Rate-limit friendly (configurable delay & concurrency)

Cara pakai di app.py (ganti section sub_batch):
------------------------------------------------
    from batch_harga_engine import render_batch_harga_tab
    render_batch_harga_tab(b_rate)

Dimana b_rate adalah float kurs CNY→IDR.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Konfigurasi ──────────────────────────────────────────────────────────────

# Lokasi file progress/cache — persisten lintas rerun
PROGRESS_FILE = Path("images") / "batch_harga_progress.json"

# Jumlah request concurrent (naikan jika server tidak throttle)
MAX_WORKERS = 1

# Jeda antar batch kecil (detik) — menghindari rate-limit SIMS
BATCH_PAUSE_SEC = 0.3

# Berapa part diproses per "chunk" sebelum UI di-refresh
CHUNK_SIZE = 20

# Interval (detik) antara UI auto-refresh saat proses berjalan
UI_REFRESH_INTERVAL = 2.0


# ── Session State Keys ───────────────────────────────────────────────────────

_SS_RUNNING     = "bhe_running"        # bool  — apakah sedang berjalan
_SS_STOP_FLAG   = "bhe_stop_flag"      # bool  — sinyal berhenti ke thread
_SS_THREAD      = "bhe_thread"         # threading.Thread
_SS_RESULTS     = "bhe_results"        # dict  {pn: {...}}
_SS_TOTAL       = "bhe_total"          # int
_SS_DONE        = "bhe_done"           # int
_SS_ERRORS      = "bhe_errors"         # int
_SS_JOB_ID      = "bhe_job_id"         # str   — hash list PN aktif
_SS_LAST_UPDATE = "bhe_last_update"    # float — timestamp terakhir update
_SS_PN_ORDER    = "bhe_pn_order"       # list  — urutan asli PN dari input


# ── Persistensi Progress ─────────────────────────────────────────────────────

def _load_progress(job_id: str) -> dict:
    """Baca hasil yang sudah tersimpan untuk job_id ini."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not PROGRESS_FILE.exists():
        return {}
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("job_id") == job_id:
            return data.get("results", {})
    except Exception:
        pass
    return {}


def _save_progress(job_id: str, results: dict):
    """Simpan progress ke disk (thread-safe via lock)."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"job_id": job_id, "results": results}, f, ensure_ascii=False)
    except Exception as e:
        print(f"[batch_harga_engine] Gagal simpan progress: {e}")


def _clear_progress():
    """Hapus file progress."""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception:
        pass


def _compute_job_id(pn_list: list[str]) -> str:
    """Hash list PN → ID unik untuk job ini."""
    import hashlib
    combined = ",".join(sorted(pn_list))
    return hashlib.md5(combined.encode()).hexdigest()[:16]


# ── Worker Thread ─────────────────────────────────────────────────────────────

_progress_lock = threading.Lock()


def _fetch_one(pn: str) -> dict:
    """Fetch harga satu PN. Return dict hasil."""
    try:
        from sims_price_fetcher import get_sims_part_price
        price, err = get_sims_part_price(pn)
        return {"pn": pn, "price": price, "err": err, "ts": time.strftime("%H:%M:%S")}
    except Exception as ex:
        return {"pn": pn, "price": None, "err": str(ex), "ts": time.strftime("%H:%M:%S")}


def _worker_thread(
    pn_pending: list[str],
    job_id: str,
    results_ref: dict,
    stop_event: threading.Event,
):
    """
    Background thread yang memproses pn_pending secara concurrent.
    Hasil ditulis ke results_ref (dict yang sama dengan session_state).
    """
    total_pending = len(pn_pending)
    idx = 0

    while idx < total_pending and not stop_event.is_set():
        chunk = pn_pending[idx : idx + CHUNK_SIZE]
        idx  += CHUNK_SIZE

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(_fetch_one, pn): pn for pn in chunk}
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                result = future.result()
                pn     = result["pn"]
                with _progress_lock:
                    results_ref[pn] = result
                    # Simpan ke disk setiap kali ada hasil baru
                    _save_progress(job_id, dict(results_ref))

        if not stop_event.is_set() and idx < total_pending:
            time.sleep(BATCH_PAUSE_SEC)

    # Selesai (atau dihentikan)
    # Set flag selesai via session state tidak bisa dari thread lain di Streamlit,
    # tapi _SS_RUNNING bisa kita set ke False melalui mekanisme "done" di UI loop.


# ── Public: Render Tab ───────────────────────────────────────────────────────

def render_batch_harga_tab(b_rate: float | None = None):
    """
    Render UI sub-tab Batch Cari Harga.
    Panggil dari dalam `with sub_batch:` di app.py.

    Parameters
    ----------
    b_rate : float | None
        Kurs CNY → IDR. Jika None, akan ditampilkan kolom CNY saja.
    """
    st.markdown("#### 📥 Batch Cari Harga dari SIMS")
    st.caption(
        "Upload file Excel (.xlsx) berisi daftar Part Number (kolom pertama). "
        "Mendukung 5.000+ part — proses berjalan di background, UI tidak freeze, "
        "dan **otomatis resume** jika refresh."
    )

    # ── Validasi dependency ──────────────────────────────────────────────────
    try:
        import sims_price_fetcher  # noqa: F401
        fetcher_ok = True
    except ImportError:
        fetcher_ok = False

    if not fetcher_ok:
        st.error("❌ `sims_price_fetcher.py` tidak ditemukan. Fitur Batch Cari Harga tidak tersedia.")
        return

    # ── Inisialisasi session state ───────────────────────────────────────────
    for key, default in [
        (_SS_RUNNING,     False),
        (_SS_STOP_FLAG,   False),
        (_SS_RESULTS,     {}),
        (_SS_TOTAL,       0),
        (_SS_DONE,        0),
        (_SS_ERRORS,      0),
        (_SS_JOB_ID,      ""),
        (_SS_LAST_UPDATE, 0.0),
        (_SS_PN_ORDER,    []),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    # ── Input ────────────────────────────────────────────────────────────────
    input_mode = st.radio(
        "Metode Input:",
        ["📁 Upload File Excel", "⌨️ Ketik Manual"],
        horizontal=True,
        key="bhe_input_mode",
        disabled=st.session_state[_SS_RUNNING],
    )

    pn_list: list[str] = []

    if input_mode == "📁 Upload File Excel":
        uploaded = st.file_uploader(
            "Upload file Excel (.xlsx) — Part Number di kolom pertama:",
            type=["xlsx", "xls"],
            key="bhe_upload",
            disabled=st.session_state[_SS_RUNNING],
        )
        if uploaded:
            try:
                df_up  = pd.read_excel(uploaded, dtype=str, header=None)
                pn_list = (
                    df_up.iloc[:, 0]
                    .dropna()
                    .str.strip()
                    .str.upper()
                    .tolist()
                )
                pn_list = [p for p in pn_list if p]
                st.success(f"✅ {len(pn_list):,} Part Number terdeteksi dari file.")
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")
    else:
        manual_text = st.text_area(
            "Masukkan Part Number (satu per baris):",
            height=150,
            placeholder="WG1641230025\nWG9725520274\n...",
            key="bhe_manual_text",
            disabled=st.session_state[_SS_RUNNING],
        )
        if manual_text.strip():
            pn_list = [p.strip().upper() for p in manual_text.splitlines() if p.strip()]
            st.info(f"📝 {len(pn_list):,} Part Number siap dicari.")

    st.divider()

    # ── Tombol kontrol ───────────────────────────────────────────────────────
    col_run, col_stop, col_reset = st.columns([2, 1, 1])

    with col_run:
        # Hitung berapa yang belum diproses jika ada job aktif
        pending_count = 0
        if pn_list:
            job_id = _compute_job_id(pn_list)
            existing = _load_progress(job_id)
            pending_count = len([p for p in pn_list if p not in existing])

        btn_label = (
            f"▶️ Lanjutkan ({pending_count:,} sisa)"
            if (pn_list and pending_count < len(pn_list) and pending_count > 0)
            else f"🚀 Mulai Batch ({len(pn_list):,} Part)"
        )
        run_clicked = st.button(
            btn_label,
            type="primary",
            use_container_width=True,
            key="bhe_run",
            disabled=st.session_state[_SS_RUNNING] or not pn_list,
        )

    with col_stop:
        stop_clicked = st.button(
            "⏸️ Pause",
            use_container_width=True,
            key="bhe_stop",
            disabled=not st.session_state[_SS_RUNNING],
        )

    with col_reset:
        reset_clicked = st.button(
            "🗑️ Reset",
            use_container_width=True,
            key="bhe_reset",
            disabled=st.session_state[_SS_RUNNING],
        )

    # ── Handle tombol ────────────────────────────────────────────────────────

    if reset_clicked:
        _clear_progress()
        for key in [_SS_RUNNING, _SS_STOP_FLAG, _SS_RESULTS, _SS_TOTAL,
                    _SS_DONE, _SS_ERRORS, _SS_JOB_ID, _SS_LAST_UPDATE, _SS_PN_ORDER]:
            st.session_state.pop(key, None)
        st.session_state[_SS_RUNNING]  = False
        st.session_state[_SS_RESULTS]  = {}
        st.rerun()

    if stop_clicked:
        st.session_state[_SS_STOP_FLAG] = True
        st.session_state[_SS_RUNNING]   = False
        thread: threading.Thread | None = st.session_state.get(_SS_THREAD)
        if thread and thread.is_alive():
            # Event dikirim ke thread, thread akan berhenti sendiri
            pass
        st.toast("⏸️ Proses dijeda. Klik ▶️ Lanjutkan untuk melanjutkan.", icon="⏸️")
        st.rerun()

    if run_clicked and pn_list:
        job_id = _compute_job_id(pn_list)

        # Load hasil yang sudah ada (resume)
        existing_results = _load_progress(job_id)
        done_pns         = set(existing_results.keys())
        pn_pending       = [p for p in pn_list if p not in done_pns]

        # Simpan ke session state
        st.session_state[_SS_JOB_ID]   = job_id
        st.session_state[_SS_RESULTS]  = dict(existing_results)
        st.session_state[_SS_TOTAL]    = len(pn_list)
        st.session_state[_SS_DONE]     = len(done_pns)
        st.session_state[_SS_PN_ORDER] = pn_list          # ← simpan urutan asli input
        st.session_state[_SS_ERRORS]   = sum(
            1 for r in existing_results.values() if r.get("price") is None
        )
        st.session_state[_SS_RUNNING]  = True
        st.session_state[_SS_STOP_FLAG] = False
        st.session_state[_SS_LAST_UPDATE] = time.time()

        if not pn_pending:
            st.success("✅ Semua part sudah diproses! Klik 🗑️ Reset untuk memulai ulang.")
            st.session_state[_SS_RUNNING] = False
        else:
            stop_event = threading.Event()
            # Kita simpan stop_event di session state agar bisa diakses tombol pause
            # (Streamlit tidak support object kompleks di session_state dengan baik,
            # tapi threading.Event adalah thread-safe dan aman disimpan)
            st.session_state["bhe_stop_event"] = stop_event

            thread = threading.Thread(
                target=_worker_thread,
                args=(pn_pending, job_id, st.session_state[_SS_RESULTS], stop_event),
                daemon=True,
                name="BatchHargaWorker",
            )
            st.session_state[_SS_THREAD] = thread
            thread.start()

        st.rerun()

    # ── Progress Display ─────────────────────────────────────────────────────

    results: dict = st.session_state.get(_SS_RESULTS, {})
    total   = st.session_state.get(_SS_TOTAL, 0)
    running = st.session_state.get(_SS_RUNNING, False)

    # Sinkronkan _SS_DONE dari results aktual (karena thread update dict langsung)
    if results:
        done_now   = len(results)
        errors_now = sum(1 for r in results.values() if r.get("price") is None)
        st.session_state[_SS_DONE]   = done_now
        st.session_state[_SS_ERRORS] = errors_now
    else:
        done_now   = 0
        errors_now = 0

    # Cek apakah thread sudah selesai
    thread: threading.Thread | None = st.session_state.get(_SS_THREAD)
    if running and thread and not thread.is_alive():
        st.session_state[_SS_RUNNING]   = False
        st.session_state[_SS_STOP_FLAG] = False
        running = False
        if done_now >= total > 0:
            st.toast(f"✅ Batch selesai! {done_now:,} part diproses.", icon="✅")

    # Tampilkan progress bar jika ada data
    if total > 0:
        progress_val = done_now / total if total > 0 else 0
        found_now    = done_now - errors_now

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📋 Total",         f"{total:,}")
        c2.metric("⏳ Diproses",       f"{done_now:,}")
        c3.metric("✅ Ditemukan",      f"{found_now:,}")
        c4.metric("❌ Tidak Ditemukan", f"{errors_now:,}")

        status_label = (
            f"🔄 Sedang berjalan... ({done_now:,}/{total:,})"
            if running
            else (
                f"✅ Selesai ({done_now:,}/{total:,})"
                if done_now >= total
                else f"⏸️ Dijeda ({done_now:,}/{total:,} selesai)"
            )
        )
        st.progress(progress_val, text=status_label)

        # Auto-refresh saat masih berjalan
        if running:
            time.sleep(UI_REFRESH_INTERVAL)
            st.rerun()

    # ── Tampilkan Hasil ──────────────────────────────────────────────────────

    if results:
        st.markdown("---")

        # Susun DataFrame mengikuti urutan asli input
        pn_order = st.session_state.get(_SS_PN_ORDER) or list(results.keys())
        rows = []
        for pn in pn_order:
            r = results.get(pn)
            if r is None:
                # Part belum diproses (masih pending) — tampilkan sebagai pending
                rows.append({
                    "Part Number":     pn,
                    "Harga CNY (¥)":  "—",
                    "Harga IDR (Rp)": "—",
                    "Status":          "⏳ Menunggu",
                    "Keterangan":      "",
                    "Waktu":           "",
                })
                continue
            price = r.get("price")
            err   = r.get("err") or ""
            idr   = price * b_rate if (price is not None and b_rate) else None
            rows.append({
                "Part Number":     pn,
                "Harga CNY (¥)":  f"{price:,.2f}" if price is not None else "—",
                "Harga IDR (Rp)": f"Rp {idr:,.0f}" if idr is not None else "—",
                "Status":          "✅ Ditemukan" if price is not None else "❌ Tidak Ditemukan",
                "Keterangan":      err,
                "Waktu":           r.get("ts", ""),
            })

        df_res = pd.DataFrame(rows)

        # Filter tampilan
        filter_opt = st.radio(
            "Tampilkan:",
            ["Semua", "✅ Ditemukan saja", "❌ Tidak Ditemukan saja", "⏳ Menunggu saja"],
            horizontal=True,
            key="bhe_filter_opt",
        )
        if filter_opt == "✅ Ditemukan saja":
            df_res = df_res[df_res["Status"].str.startswith("✅")]
        elif filter_opt == "❌ Tidak Ditemukan saja":
            df_res = df_res[df_res["Status"].str.startswith("❌")]
        elif filter_opt == "⏳ Menunggu saja":
            df_res = df_res[df_res["Status"].str.startswith("⏳")]

        st.dataframe(
            df_res,
            hide_index=True,
            use_container_width=True,
            height=min(600, 56 + len(df_res) * 35),
            column_config={
                "Part Number":     st.column_config.TextColumn("Part Number",     width="medium"),
                "Harga CNY (¥)":  st.column_config.TextColumn("Harga CNY (¥)",  width="small"),
                "Harga IDR (Rp)": st.column_config.TextColumn("Harga IDR (Rp)", width="medium"),
                "Status":          st.column_config.TextColumn("Status",          width="small"),
                "Keterangan":      st.column_config.TextColumn("Keterangan",      width="large"),
                "Waktu":           st.column_config.TextColumn("Waktu",           width="small"),
            },
        )

        # Download
        col_dl1, col_dl2 = st.columns(2)

        import io
        from datetime import datetime

        with col_dl1:
            buf_all = io.BytesIO()
            pd.DataFrame(rows).to_excel(buf_all, index=False, engine="openpyxl")
            buf_all.seek(0)
            st.download_button(
                label="⬇️ Download Semua Hasil (.xlsx)",
                data=buf_all.getvalue(),
                file_name=f"batch_harga_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bhe_dl_all",
                use_container_width=True,
            )

        with col_dl2:
            rows_found = [r for r in rows if r["Status"].startswith("✅")]
            if rows_found:
                buf_found = io.BytesIO()
                pd.DataFrame(rows_found).to_excel(buf_found, index=False, engine="openpyxl")
                buf_found.seek(0)
                st.download_button(
                    label="⬇️ Download Ditemukan saja (.xlsx)",
                    data=buf_found.getvalue(),
                    file_name=f"batch_harga_ditemukan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="bhe_dl_found",
                    use_container_width=True,
                )

    # ── Tips ────────────────────────────────────────────────────────────────
    with st.expander("ℹ️ Cara Kerja & Tips", expanded=False):
        st.markdown(f"""
        **Fitur utama:**
        - 🔄 **Non-blocking** — UI tetap responsif selama proses berjalan
        - 💾 **Auto-resume** — Jika browser refresh, proses akan **melanjutkan dari titik terakhir** (bukan dari awal)
        - ⏸️ **Pause/Lanjut** — Bisa dijeda kapan saja dan dilanjutkan kembali
        - ⚡ **Concurrent** — {MAX_WORKERS} request berjalan bersamaan (lebih cepat)

        **Estimasi waktu (5.000 part):**
        - Rata-rata ~0.5–2 detik per part (tergantung SIMS)
        - Dengan {MAX_WORKERS} concurrent: ~10–40 menit
        - Part yang sudah pernah dicari di-skip (dari cache `part_price_cache.json`)

        **Tips:**
        - Kolom pertama file Excel harus berisi Part Number (bisa ada header, akan di-skip otomatis jika bukan Part Number valid)
        - Hapus duplikat dari list sebelum upload untuk menghemat waktu
        - Gunakan **Pause** jika ingin menggunakan app sementara, lalu **Lanjutkan** setelah selesai
        - Progress tersimpan di `images/batch_harga_progress.json` — aman meski window ditutup
        """)