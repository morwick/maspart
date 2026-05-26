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
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Supabase (sumber utama progress, tahan restart container Streamlit Cloud)
try:
    from supabase import SupabaseBatchHarga, SupabaseBatchHargaJobs
    _SB_BATCH_OK = SupabaseBatchHarga.is_available()
    _SB_JOBS_OK  = SupabaseBatchHargaJobs.is_available()
except Exception:
    SupabaseBatchHarga     = None      # type: ignore[assignment]
    SupabaseBatchHargaJobs = None      # type: ignore[assignment]
    _SB_BATCH_OK = False
    _SB_JOBS_OK  = False

# ── Konfigurasi ──────────────────────────────────────────────────────────────

# Lokasi file progress/cache — backup lokal kalau Supabase down
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
_SS_COMPLETED_AT = "bhe_completion_time"  # float — saat batch selesai (auto-clear setelah TTL)

# Setelah batch selesai, hasil disimpan di session_state. Untuk hindari
# numpuk RAM kalau user pindah tab dan tidak balik lagi, auto-clear
# results setelah TTL. User masih bisa lihat hasil <TTL menit setelah
# batch selesai sebelum di-bersihin.
_BATCH_RESULT_TTL = 600   # 10 menit


# ── Persistensi Progress ─────────────────────────────────────────────────────

def _load_file_progress(job_id: str) -> dict:
    """Baca progress dari file lokal (backup/fallback)."""
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


def _save_file_progress(job_id: str, results: dict):
    """Simpan seluruh dict ke file lokal (backup)."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"job_id": job_id, "results": results}, f, ensure_ascii=False)
    except Exception as e:
        print(f"[batch_harga_engine] Gagal simpan file lokal: {e}")


def _load_progress(job_id: str) -> dict:
    """Baca hasil yang sudah tersimpan untuk job_id ini.

    Strategi:
      1. Coba Supabase dulu (sumber utama, tahan restart container)
      2. Fallback ke file lokal kalau Supabase kosong/gagal
      3. Auto-migrasi: kalau Supabase kosong tapi file punya data,
         dorong file ke Supabase agar persisten di run berikutnya.
    """
    sb_data: dict = {}
    if _SB_BATCH_OK and SupabaseBatchHarga is not None:
        try:
            sb_data = SupabaseBatchHarga.load_progress(job_id)
        except Exception as e:
            print(f"[batch_harga_engine] Supabase load gagal, fallback ke file: {e}")

    file_data = _load_file_progress(job_id)

    # Auto-migrasi file → Supabase sekali (kalau Supabase aktif tapi kosong)
    if (
        _SB_BATCH_OK
        and SupabaseBatchHarga is not None
        and not sb_data
        and file_data
    ):
        try:
            if SupabaseBatchHarga.save_many(job_id, file_data):
                print(f"[batch_harga_engine] ✅ Migrasi {len(file_data)} entri "
                      f"ke Supabase untuk job '{job_id}'.")
                sb_data = file_data
        except Exception as e:
            print(f"[batch_harga_engine] Migrasi ke Supabase gagal: {e}")

    return sb_data or file_data


def _save_progress(job_id: str, results: dict):
    """Simpan seluruh snapshot — hanya ke file lokal (backup).
    Update per-PN ke Supabase dilakukan via _save_one_result() di worker.
    """
    _save_file_progress(job_id, results)


def _save_one_result(job_id: str, pn: str, result: dict):
    """Simpan satu hasil PN ke Supabase (best-effort)."""
    if not (_SB_BATCH_OK and SupabaseBatchHarga is not None):
        return
    try:
        SupabaseBatchHarga.save_one(job_id, pn, result)
    except Exception as e:
        print(f"[batch_harga_engine] Supabase save '{pn}' gagal: {e}")


def _clear_progress(job_id: str = ""):
    """Hapus progress + metadata di Supabase (untuk job_id ini) + file lokal."""
    # 1) Supabase — progress per-PN
    if _SB_BATCH_OK and SupabaseBatchHarga is not None and job_id:
        try:
            SupabaseBatchHarga.clear(job_id)
        except Exception as e:
            print(f"[batch_harga_engine] Supabase clear gagal: {e}")
    # 2) Supabase — metadata batch (pn_list, label, …)
    if _SB_JOBS_OK and SupabaseBatchHargaJobs is not None and job_id:
        try:
            SupabaseBatchHargaJobs.delete_job(job_id)
        except Exception as e:
            print(f"[batch_harga_engine] Supabase delete_job gagal: {e}")
    # 3) File lokal
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception:
        pass


def _save_job_metadata(job_id: str, pn_list: list[str], label: str, input_mode: str):
    """Simpan metadata batch (best-effort)."""
    if not (_SB_JOBS_OK and SupabaseBatchHargaJobs is not None):
        return
    try:
        SupabaseBatchHargaJobs.save_job(job_id, pn_list, label, input_mode)
    except Exception as e:
        print(f"[batch_harga_engine] Supabase save_job '{job_id}' gagal: {e}")


def _list_saved_jobs() -> list[dict]:
    """List batch tersimpan untuk UI 'Lanjutkan batch sebelumnya'."""
    if not (_SB_JOBS_OK and SupabaseBatchHargaJobs is not None):
        return []
    try:
        return SupabaseBatchHargaJobs.list_jobs(limit=30)
    except Exception as e:
        print(f"[batch_harga_engine] Supabase list_jobs gagal: {e}")
        return []


def _fetch_full_job(job_id: str) -> dict | None:
    """Ambil 1 job lengkap (termasuk pn_list) dari Supabase."""
    if not (_SB_JOBS_OK and SupabaseBatchHargaJobs is not None):
        return None
    try:
        return SupabaseBatchHargaJobs.get_job(job_id)
    except Exception as e:
        print(f"[batch_harga_engine] Supabase get_job '{job_id}' gagal: {e}")
        return None


def _format_ago(iso_ts: str) -> str:
    """Format ISO timestamp jadi 'X menit lalu' / 'X jam lalu' / tanggal."""
    if not iso_ts:
        return ""
    try:
        from datetime import datetime, timezone
        ts = iso_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return f"{secs}d lalu"
        mins = secs // 60
        if mins < 60:
            return f"{mins} menit lalu"
        hrs = mins // 60
        if hrs < 24:
            return f"{hrs} jam lalu"
        days = hrs // 24
        if days < 30:
            return f"{days} hari lalu"
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return iso_ts


def _compute_job_id(pn_list: list[str]) -> str:
    """Hash list PN → ID unik untuk job ini."""
    import hashlib
    combined = ",".join(sorted(pn_list))
    return hashlib.md5(combined.encode()).hexdigest()[:16]


# ── Worker Thread ─────────────────────────────────────────────────────────────

_progress_lock = threading.Lock()


def _fetch_one(pn: str) -> dict:
    """Fetch harga satu PN. Return dict hasil.

    Jika PN punya suffix '/<digit>' (mis. 'WG9525880022/1' atau
    'WG9525880022+011/1') dan tidak ditemukan, otomatis coba lagi
    tanpa suffix tersebut.
    """
    try:
        from sims_price_fetcher import get_sims_part_price
        price, err = get_sims_part_price(pn)

        if price is None and re.search(r"/\d+$", pn):
            fallback_pn = re.sub(r"/\d+$", "", pn)
            price2, err2 = get_sims_part_price(fallback_pn)
            if price2 is not None:
                return {
                    "pn":      pn,
                    "price":   price2,
                    "err":     f"(via {fallback_pn})",
                    "ts":      time.strftime("%H:%M:%S"),
                    "via_pn":  fallback_pn,
                }

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
                    snapshot = dict(results_ref)
                # Network/disk write di luar lock supaya tidak block
                # worker lain saat MAX_WORKERS > 1.
                _save_one_result(job_id, pn, result)   # Supabase (per-PN)
                _save_progress(job_id, snapshot)        # File lokal (backup)

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

    # ── Migrasi: dedup _SS_PN_ORDER (legacy session sebelum dedup di input) ─
    # Tanpa dedup, Total dihitung dengan duplikat tapi _SS_RESULTS unik →
    # status nyangkut di "Dijeda 3,281/4,862" walau worker sudah selesai.
    order_legacy = st.session_state.get(_SS_PN_ORDER) or []
    if order_legacy:
        order_unique = list(dict.fromkeys(order_legacy))
        if len(order_unique) != len(order_legacy):
            st.session_state[_SS_PN_ORDER] = order_unique
            st.session_state[_SS_TOTAL]    = len(order_unique)

    # ── Auto-clear hasil lama setelah TTL ────────────────────────────────────
    # Kalau batch sudah selesai >TTL detik yang lalu dan user tidak balik
    # ke tab ini, bebasin RAM yang dipakai dict results.
    completed_at = st.session_state.get(_SS_COMPLETED_AT, 0.0)
    if (
        completed_at
        and (time.time() - completed_at) > _BATCH_RESULT_TTL
        and not st.session_state.get(_SS_RUNNING, False)
    ):
        for key in (_SS_RESULTS, _SS_TOTAL, _SS_DONE, _SS_ERRORS,
                    _SS_JOB_ID, _SS_PN_ORDER, _SS_COMPLETED_AT):
            st.session_state.pop(key, None)
        # Re-init defaults supaya UI tidak crash
        st.session_state[_SS_RESULTS] = {}
        st.session_state[_SS_TOTAL]   = 0
        st.session_state[_SS_DONE]    = 0
        st.session_state[_SS_ERRORS]  = 0
        st.session_state[_SS_JOB_ID]  = ""
        st.session_state[_SS_PN_ORDER] = []

    # ── Resume Batch Sebelumnya (dari Supabase) ─────────────────────────────
    saved_jobs = _list_saved_jobs() if not st.session_state[_SS_RUNNING] else []
    if saved_jobs:
        with st.expander(
            f"🕘 Lanjutkan Batch Sebelumnya ({len(saved_jobs)} tersimpan)",
            expanded=False,
        ):
            options: list[str] = []
            opt_map: dict[str, str] = {}
            for job in saved_jobs:
                jid    = job.get("job_id", "")
                label  = (job.get("label") or "").strip()
                imode  = job.get("input_mode") or ""
                total  = int(job.get("total_pn") or 0)
                ago    = _format_ago(job.get("updated_at") or "")
                icon   = "📁" if imode == "upload" else "⌨️"
                title  = label or f"{icon} Manual {total} PN"
                display = f"{icon} {title}  •  {total:,} part  •  {ago}"
                options.append(display)
                opt_map[display] = jid

            selected = st.selectbox(
                "Pilih batch untuk dilanjutkan:",
                options,
                key="bhe_resume_select",
            )
            col_load, col_del = st.columns([3, 1])
            with col_load:
                load_clicked = st.button(
                    "📥 Muat Batch Ini",
                    key="bhe_resume_load",
                    use_container_width=True,
                    type="primary",
                )
            with col_del:
                del_clicked = st.button(
                    "🗑️ Hapus",
                    key="bhe_resume_del",
                    use_container_width=True,
                )

            if load_clicked and selected:
                jid_pick  = opt_map[selected]
                full_job  = _fetch_full_job(jid_pick)
                if not full_job:
                    st.error("❌ Gagal memuat batch dari Supabase.")
                else:
                    pn_raw      = list(full_job.get("pn_list") or [])
                    # Dedup supaya Total & Diproses konsisten (batch lama bisa
                    # punya duplikat dari sebelum fitur dedup di input).
                    pn_loaded   = list(dict.fromkeys(pn_raw))
                    res_loaded  = _load_progress(jid_pick)
                    st.session_state[_SS_JOB_ID]   = jid_pick
                    st.session_state[_SS_RESULTS]  = dict(res_loaded)
                    st.session_state[_SS_PN_ORDER] = pn_loaded
                    st.session_state[_SS_TOTAL]    = len(pn_loaded)
                    st.session_state[_SS_DONE]     = len(res_loaded)
                    st.session_state[_SS_ERRORS]   = sum(
                        1 for r in res_loaded.values() if r.get("price") is None
                    )
                    # Stash label/mode supaya bisa re-passed saat Lanjutkan,
                    # tanpa harus tipu Supabase berisi string kosong.
                    st.session_state["bhe_resume_label"] = full_job.get("label") or ""
                    st.session_state["bhe_resume_mode"]  = full_job.get("input_mode") or ""
                    st.session_state.pop(_SS_COMPLETED_AT, None)
                    st.toast(
                        f"✅ Dimuat {len(res_loaded):,}/{len(pn_loaded):,} part",
                        icon="📥",
                    )
                    st.rerun()

            if del_clicked and selected:
                jid_pick = opt_map[selected]
                _clear_progress(jid_pick)
                # Kalau yang dihapus = job aktif di session, bersihkan juga
                if st.session_state.get(_SS_JOB_ID) == jid_pick:
                    for key in (_SS_RESULTS, _SS_TOTAL, _SS_DONE, _SS_ERRORS,
                                _SS_JOB_ID, _SS_PN_ORDER, _SS_COMPLETED_AT):
                        st.session_state.pop(key, None)
                    st.session_state[_SS_RESULTS] = {}
                    st.session_state[_SS_TOTAL]   = 0
                    st.session_state[_SS_DONE]    = 0
                    st.session_state[_SS_ERRORS]  = 0
                    st.session_state[_SS_JOB_ID]  = ""
                    st.session_state[_SS_PN_ORDER] = []
                st.toast("🗑️ Batch dihapus.", icon="🗑️")
                st.rerun()

        st.divider()

    # ── Input ────────────────────────────────────────────────────────────────
    input_mode = st.radio(
        "Metode Input:",
        ["📁 Upload File Excel", "⌨️ Ketik Manual"],
        horizontal=True,
        key="bhe_input_mode",
        disabled=st.session_state[_SS_RUNNING],
    )

    pn_list: list[str] = []
    input_label: str = ""        # nama file / preview manual — untuk display
    input_mode_tag: str = ""     # "upload" | "manual" — disimpan ke jobs table

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
                pn_raw = (
                    df_up.iloc[:, 0]
                    .dropna()
                    .str.strip()
                    .str.upper()
                    .tolist()
                )
                pn_raw = [p for p in pn_raw if p]
                # Dedup sambil pertahankan urutan asli — kalau ada PN duplikat,
                # Total & Diproses jadi mismatch (worker tetap proses dupes, dict
                # menyimpan unique) sehingga status nyangkut di "Dijeda".
                pn_list = list(dict.fromkeys(pn_raw))
                dup_count = len(pn_raw) - len(pn_list)
                input_label    = uploaded.name
                input_mode_tag = "upload"
                if dup_count > 0:
                    st.success(
                        f"✅ {len(pn_list):,} Part Number unik terdeteksi "
                        f"({dup_count:,} duplikat dibuang dari {len(pn_raw):,} baris)."
                    )
                else:
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
            pn_raw = [p.strip().upper() for p in manual_text.splitlines() if p.strip()]
            pn_list = list(dict.fromkeys(pn_raw))
            dup_count = len(pn_raw) - len(pn_list)
            input_mode_tag = "manual"
            # Label: preview 1-2 PN pertama + total
            preview        = ", ".join(pn_list[:2])
            input_label    = f"Manual {len(pn_list)} PN ({preview}…)"
            if dup_count > 0:
                st.info(
                    f"📝 {len(pn_list):,} Part Number unik siap dicari "
                    f"({dup_count:,} duplikat dibuang dari {len(pn_raw):,} baris)."
                )
            else:
                st.info(f"📝 {len(pn_list):,} Part Number siap dicari.")

    # Effective list — kalau user habis resume batch lewat expander di atas,
    # input UI kosong tapi session_state[_SS_PN_ORDER] sudah terisi. Pakai itu
    # supaya tombol Lanjutkan & handler run muncul tanpa user perlu input ulang.
    effective_pn_list: list[str] = pn_list or list(st.session_state.get(_SS_PN_ORDER) or [])
    resumed_from_history: bool = bool(effective_pn_list) and not pn_list

    if resumed_from_history:
        st.info(
            f"📌 Batch dimuat dari riwayat — {len(effective_pn_list):,} part total. "
            f"Klik **Lanjutkan** untuk meneruskan."
        )

    st.divider()

    # ── Tombol kontrol ───────────────────────────────────────────────────────
    col_run, col_stop, col_reset = st.columns([2, 1, 1])

    with col_run:
        # Hitung berapa yang belum diproses jika ada job aktif.
        # Optimasi: kalau job ini sudah ter-load di session_state, pakai itu
        # supaya tidak hit Supabase setiap rerun (~2 detik sekali saat batch jalan).
        pending_count   = 0
        all_done_flag   = False
        if effective_pn_list:
            job_id_calc      = _compute_job_id(effective_pn_list)
            session_results  = st.session_state.get(_SS_RESULTS) or {}
            effective_set    = set(effective_pn_list)
            # Toleran job_id berbeda: kalau seluruh key di session adalah subset
            # dari input efektif, anggap itu progress yang valid (mis. setelah
            # dedup, hash list berubah tapi datanya sama).
            if session_results and all(pn in effective_set for pn in session_results):
                existing = session_results
            elif st.session_state.get(_SS_JOB_ID) == job_id_calc and session_results:
                existing = session_results
            else:
                existing = _load_progress(job_id_calc)
            pending_count = len([p for p in effective_pn_list if p not in existing])
            all_done_flag = bool(existing) and pending_count == 0

        if all_done_flag:
            btn_label = f"✅ Selesai ({len(effective_pn_list):,} Part) — Reset untuk ulang"
        elif effective_pn_list and 0 < pending_count < len(effective_pn_list):
            btn_label = f"▶️ Lanjutkan ({pending_count:,} sisa)"
        else:
            btn_label = f"🚀 Mulai Batch ({len(effective_pn_list):,} Part)"

        run_clicked = st.button(
            btn_label,
            type="primary",
            use_container_width=True,
            key="bhe_run",
            disabled=(
                st.session_state[_SS_RUNNING]
                or not effective_pn_list
                or all_done_flag
            ),
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
        # Clear progress untuk job aktif di session, atau job dari input
        # yang sedang ada (mana saja yang relevan).
        job_to_clear = st.session_state.get(_SS_JOB_ID, "") or (
            _compute_job_id(effective_pn_list) if effective_pn_list else ""
        )
        _clear_progress(job_to_clear)
        for key in [_SS_RUNNING, _SS_STOP_FLAG, _SS_RESULTS, _SS_TOTAL,
                    _SS_DONE, _SS_ERRORS, _SS_JOB_ID, _SS_LAST_UPDATE,
                    _SS_PN_ORDER, _SS_COMPLETED_AT]:
            st.session_state.pop(key, None)
        st.session_state[_SS_RUNNING]  = False
        st.session_state[_SS_RESULTS]  = {}
        st.rerun()

    if stop_clicked:
        st.session_state[_SS_STOP_FLAG] = True
        st.session_state[_SS_RUNNING]   = False
        # ── FIX: kirim sinyal stop ke thread via Event ──
        stop_event: threading.Event | None = st.session_state.get("bhe_stop_event")
        if stop_event is not None:
            stop_event.set()
        st.toast("⏸️ Proses dijeda. Klik ▶️ Lanjutkan untuk melanjutkan.", icon="⏸️")
        st.rerun()

    if run_clicked and effective_pn_list:
        # ── FIX: hentikan thread lama jika masih hidup ──
        old_stop_event: threading.Event | None = st.session_state.get("bhe_stop_event")
        if old_stop_event is not None:
            old_stop_event.set()
        old_thread: threading.Thread | None = st.session_state.get(_SS_THREAD)
        if old_thread and old_thread.is_alive():
            old_thread.join(timeout=3)   # tunggu maks 3 detik

        new_job_id       = _compute_job_id(effective_pn_list)
        session_results  = st.session_state.get(_SS_RESULTS) or {}
        saved_job_id     = st.session_state.get(_SS_JOB_ID, "")
        effective_set    = set(effective_pn_list)

        # Kalau session_results valid untuk input ini (semua key ⊆ input),
        # pakai itu langsung — supaya progress tidak hilang saat dedup migrasi
        # mengubah hash job_id.
        if session_results and all(pn in effective_set for pn in session_results):
            existing_results = dict(session_results)
            job_id           = saved_job_id or new_job_id
        else:
            job_id           = new_job_id
            existing_results = _load_progress(job_id)
        done_pns   = set(existing_results.keys())
        pn_pending = [p for p in effective_pn_list if p not in done_pns]

        # Simpan ke session state
        st.session_state[_SS_JOB_ID]   = job_id
        st.session_state[_SS_RESULTS]  = dict(existing_results)
        st.session_state[_SS_TOTAL]    = len(effective_pn_list)
        st.session_state[_SS_DONE]     = len(done_pns)
        st.session_state[_SS_PN_ORDER] = effective_pn_list  # ← simpan urutan asli input
        st.session_state[_SS_ERRORS]   = sum(
            1 for r in existing_results.values() if r.get("price") is None
        )
        st.session_state[_SS_RUNNING]  = True
        st.session_state[_SS_STOP_FLAG] = False
        st.session_state[_SS_LAST_UPDATE] = time.time()
        # Reset completion timestamp — supaya auto-clear tidak salah trigger
        # selama batch baru ini jalan.
        st.session_state.pop(_SS_COMPLETED_AT, None)

        # Simpan / refresh metadata batch ke Supabase. Saat fresh input pakai
        # label & mode dari input UI; saat resume dari riwayat pakai yang sudah
        # disimpan supaya tidak ditimpa string kosong.
        if pn_list:
            _save_job_metadata(job_id, effective_pn_list, input_label, input_mode_tag)
        else:
            saved_label = st.session_state.get("bhe_resume_label", "") or ""
            saved_mode  = st.session_state.get("bhe_resume_mode", "")  or ""
            _save_job_metadata(job_id, effective_pn_list, saved_label, saved_mode)

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
            # Tandai waktu selesai → results bakal auto-clear setelah TTL
            # supaya RAM tidak nempel kalau user pindah tab.
            st.session_state[_SS_COMPLETED_AT] = time.time()

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
                    "PN Dicari":       "",
                    "Mark /N Dihapus": "",
                    "Harga CNY (¥)":  "—",
                    "Harga IDR (Rp)": "—",
                    "Status":          "⏳ Menunggu",
                    "Keterangan":      "",
                    "Waktu":           "",
                })
                continue
            price   = r.get("price")
            err     = r.get("err") or ""
            via_pn  = r.get("via_pn") or ""
            idr     = price * b_rate if (price is not None and b_rate) else None
            rows.append({
                "Part Number":     pn,
                "PN Dicari":       via_pn if via_pn else pn,
                "Mark /N Dihapus": "✔" if via_pn else "",
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
                "PN Dicari":       st.column_config.TextColumn("PN Dicari",       width="medium"),
                "Mark /N Dihapus": st.column_config.TextColumn("Mark /N Dihapus", width="small"),
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
        storage_info = (
            "☁️ **Supabase** (utama) + 💾 file lokal (backup)"
            if _SB_BATCH_OK
            else "💾 File lokal `images/batch_harga_progress.json`"
        )
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
        - Penyimpanan progress: {storage_info} — tahan refresh, restart container, dan tutup browser
        """)