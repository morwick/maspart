"""
ADMIN — IMAGE INDEX MANAGER
===========================
Tab admin untuk mengelola index foto SIMS yang dipakai fitur
"Cari by Foto" (image_search.py).

Fitur:
  - Single PN: input 1 PN → indexing langsung
  - Bulk PN: textarea (1 PN per baris) → indexing dengan progress bar
  - Daftar PN yang sudah ter-index + tombol hapus
  - Search/filter daftar PN
  - Statistik total PN & foto

Cara pakai di app.py:
  from admin_image_index import render_image_index_tab
  # tambahkan tab admin ("tab_image_index", "🧠 Image Index", "__image_index__")
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

from image_search import (
    _is_configured,
    _TORCH_AVAILABLE,
    _TORCH_ERR,
    index_part_number,
    index_bulk,
    list_indexed_pns,
    delete_pn_from_index,
    get_index_stats,
    get_all_indexed_pns,
    get_sims_cache_stats,
    clear_sims_cache,
)


# ── Session keys ──────────────────────────────────────────────────────────
_SS_SINGLE_RESULT = "_img_idx_single_result"
_SS_DELETE_RESULT = "_img_idx_delete_result"
_SS_LIST_QUERY    = "_img_idx_list_query"
_SS_LIST_PAGE     = "_img_idx_list_page"

_PAGE_SIZE = 20


# ══════════════════════════════════════════════════════════════════════════
#  BULK JOB CHECKPOINT — resume tahan refresh / sesi mati / circuit-breaker
# ══════════════════════════════════════════════════════════════════════════
#
#  Status tiap PN ditulis ke file JSON SETIAP PN selesai (atomic replace),
#  bukan hanya di akhir batch. Jadi kalau web di-refresh / sesi Streamlit
#  mati / circuit-breaker nyala → progress tidak hilang dan bulk bisa
#  dilanjutkan dari titik terakhir (PN yang sudah selesai di-skip).
#
#  File ini hanya untuk ORKESTRASI/VISIBILITAS. Data embedding sendiri
#  tetap di-upsert per-PN ke Supabase oleh index_part_number (tidak berubah).

_JOB_DIR  = Path(".cache")
_JOB_PATH = _JOB_DIR / "image_index_bulk_job.json"

# State yang masih bisa di-resume (belum tuntas / perlu dicoba ulang).
# "empty" (tidak ada foto SIMS) & "done"/"skipped" dianggap final.
_RESUMABLE_STATES = {"pending", "failed", "cancelled"}


def _job_load() -> dict | None:
    try:
        if _JOB_PATH.exists():
            with open(_JOB_PATH, "r", encoding="utf-8") as f:
                job = json.load(f)
            if isinstance(job, dict) and job.get("order"):
                return job
    except Exception as e:
        print(f"[image_index] gagal baca job checkpoint: {e}")
    return None


def _job_save(job: dict) -> None:
    """Tulis atomik (tmp + os.replace) supaya tidak korup kalau proses mati di tengah write."""
    try:
        _JOB_DIR.mkdir(parents=True, exist_ok=True)
        job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        tmp = _JOB_PATH.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False)
        os.replace(tmp, _JOB_PATH)
    except Exception as e:
        print(f"[image_index] gagal simpan job checkpoint: {e}")


def _job_clear() -> None:
    try:
        if _JOB_PATH.exists():
            _JOB_PATH.unlink()
    except Exception as e:
        print(f"[image_index] gagal hapus job checkpoint: {e}")


def _job_init(order: list[str], indexed_by: str, skip_existing: bool) -> dict:
    now = datetime.now().isoformat(timespec="seconds")
    job = {
        "created_at":    now,
        "updated_at":    now,
        "indexed_by":    indexed_by,
        "skip_existing": bool(skip_existing),
        "status":        "running",
        "order":         list(order),
        "pns": {pn: {"state": "pending", "n_indexed": 0, "n_photos": 0,
                     "n_skipped": 0, "error": ""} for pn in order},
    }
    _job_save(job)
    return job


def _state_from_result(r: dict) -> str:
    if r.get("skipped_existing"):
        return "skipped"
    if r.get("cancelled") or r.get("aborted"):
        return "pending"          # belum benar-benar diproses → bisa di-resume
    if r.get("ok"):
        return "done"
    if r.get("n_photos", 0) == 0:
        return "empty"            # tidak ada foto SIMS → final, tidak di-retry
    return "failed"


def _job_apply(job: dict, pn: str, r: dict) -> None:
    e = job["pns"].get(pn)
    if e is None:
        e = {}
        job["pns"][pn] = e
        if pn not in job["order"]:
            job["order"].append(pn)
    e["state"]     = _state_from_result(r)
    e["n_indexed"] = r.get("n_indexed", 0)
    e["n_photos"]  = r.get("n_photos", 0)
    e["n_skipped"] = r.get("n_skipped", 0)
    e["error"]     = (r.get("error") or "")[:300]


def _job_remaining(job: dict) -> list[str]:
    return [pn for pn in job.get("order", [])
            if job.get("pns", {}).get(pn, {}).get("state") in _RESUMABLE_STATES]


def _job_counts(job: dict) -> dict:
    c = {"total": len(job.get("order", [])), "done": 0, "skipped": 0,
         "empty": 0, "failed": 0, "pending": 0}
    for pn in job.get("order", []):
        s = job.get("pns", {}).get(pn, {}).get("state", "pending")
        if   s == "done":    c["done"]    += 1
        elif s == "skipped": c["skipped"] += 1
        elif s == "empty":   c["empty"]   += 1
        elif s == "failed":  c["failed"]  += 1
        else:                c["pending"] += 1
    return c


# ══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

def render_image_index_tab():
    """Tab admin: kelola Image Index."""
    st.markdown("### 🧠 Image Index — Cari by Foto")
    st.caption(
        "Tambahkan Part Number ke index pencarian foto. "
        "Sistem akan mengambil foto dari SIMS, menghitung embedding, "
        "dan menyimpannya ke Supabase untuk fitur **Cari by Foto**."
    )

    # ── Sanity checks ─────────────────────────────────────────────────────
    if not _TORCH_AVAILABLE:
        st.error(
            f"❌ **PyTorch belum terinstall.** {_TORCH_ERR}\n\n"
            "Jalankan: `pip install torch torchvision`"
        )
        return

    if not _is_configured():
        st.error(
            "❌ **Supabase belum dikonfigurasi.**\n\n"
            "Pastikan `[supabase]` url, key, dan service_key sudah di `.streamlit/secrets.toml`."
        )
        return

    # ── Statistik di atas ─────────────────────────────────────────────────
    stats = get_index_stats()
    col1, col2, col3 = st.columns(3)
    col1.metric("PN ter-index", f"{stats['total_pn']:,}")
    col2.metric("Total foto", f"{stats['total_images']:,}")
    last = stats.get("last_indexed_at") or "—"
    if isinstance(last, str) and len(last) >= 16:
        last = last[:16].replace("T", " ")
    col3.metric("Index terakhir", last)

    # ── Cache management (expander, default collapsed) ────────────────────
    _render_cache_panel()

    st.markdown("---")

    # ── 2 mode: Single / Bulk ─────────────────────────────────────────────
    tab_single, tab_bulk, tab_list = st.tabs([
        "➕ Single PN",
        "📦 Bulk PN",
        f"📋 Daftar PN Ter-index ({stats['total_pn']})",
    ])

    with tab_single:
        _render_single_mode()

    with tab_bulk:
        _render_bulk_mode()

    with tab_list:
        _render_indexed_list()


# ══════════════════════════════════════════════════════════════════════════
#  CACHE PANEL
# ══════════════════════════════════════════════════════════════════════════

def _render_cache_panel():
    """Panel kelola cache foto SIMS — stats + tombol clear."""
    cs = get_sims_cache_stats()

    # Warna icon berdasarkan persentase usage
    if cs["pct_used"] >= 90:
        icon = "🔴"
    elif cs["pct_used"] >= 70:
        icon = "🟡"
    else:
        icon = "🟢"

    label = (
        f"{icon} Cache foto SIMS: **{cs['size_mb']:.1f} MB** / {cs['max_mb']} MB "
        f"({cs['n_files']} file)"
    )

    with st.expander(label, expanded=False):
        st.caption(
            "Cache menyimpan foto SIMS yang sudah di-resize (WebP 512px) "
            "untuk thumbnail di kartu hasil pencarian. "
            "Sistem otomatis hapus file paling lama saat cache mencapai 500 MB."
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Jumlah file", f"{cs['n_files']:,}")
        c2.metric("Ukuran total", f"{cs['size_mb']:.1f} MB")
        c3.metric("File tertua", cs["oldest_at"] or "—")

        st.progress(min(cs["pct_used"] / 100.0, 1.0),
                    text=f"{cs['pct_used']:.1f}% dari kapasitas {cs['max_mb']} MB")

        if cs["pct_used"] >= 90:
            st.warning(
                "⚠️ Cache hampir penuh — sistem akan mulai hapus file lama otomatis."
            )

        # Tombol clear (dengan confirm 2-step)
        notif = st.session_state.pop("_cache_clear_notif", None)
        if notif:
            st.success(notif) if notif.startswith("✅") else st.error(notif)

        col_a, col_b = st.columns([1, 3])
        with col_a:
            confirm = st.checkbox(
                "Saya yakin",
                key="_cache_clear_confirm",
                help="Centang untuk aktifkan tombol clear.",
            )
        with col_b:
            if st.button(
                "🗑️ Clear semua cache foto SIMS",
                disabled=not confirm or cs["n_files"] == 0,
                use_container_width=True,
                key="btn_clear_sims_cache",
            ):
                res = clear_sims_cache()
                if res["error"]:
                    st.session_state["_cache_clear_notif"] = (
                        f"❌ Error: {res['error']}"
                    )
                else:
                    st.session_state["_cache_clear_notif"] = (
                        f"✅ Cache dihapus — {res['n_deleted']} file, "
                        f"{res['freed_mb']:.1f} MB dibebaskan."
                    )
                st.session_state.pop("_cache_clear_confirm", None)
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════
#  MODE 1 — SINGLE PN
# ══════════════════════════════════════════════════════════════════════════

def _render_single_mode():
    """Input 1 PN → langsung index."""
    st.markdown("#### Tambah 1 Part Number ke Index")

    # Tampilkan hasil sebelumnya (kalau ada)
    prev = st.session_state.pop(_SS_SINGLE_RESULT, None)
    if prev:
        _render_single_result(prev)

    user = st.session_state.get("current_user", {})
    indexed_by = user.get("username", "admin") if isinstance(user, dict) else "admin"

    col_input, col_btn = st.columns([4, 1])
    with col_input:
        pn = st.text_input(
            "Part Number:",
            placeholder="WG1641230025",
            key="img_idx_single_pn",
            label_visibility="collapsed",
        )
    with col_btn:
        do_index = st.button(
            "🚀 Index", type="primary",
            use_container_width=True,
            key="btn_img_idx_single",
            disabled=not pn.strip(),
        )

    if do_index and pn.strip():
        with st.spinner(f"🔍 Mengambil foto SIMS untuk **{pn.strip().upper()}**..."):
            result = index_part_number(pn.strip(), indexed_by=indexed_by)
        st.session_state[_SS_SINGLE_RESULT] = result
        st.rerun()


def _render_single_result(r: dict):
    """Render hasil 1 PN indexing."""
    pn        = r.get("pn", "")
    n_photos  = r.get("n_photos", 0)
    n_indexed = r.get("n_indexed", 0)
    n_skipped = r.get("n_skipped", 0)
    err       = r.get("error", "")

    if r.get("ok"):
        if n_indexed > 0 and n_skipped > 0:
            st.success(
                f"✅ **{pn}** — **{n_indexed}** foto baru di-index, "
                f"**{n_skipped}** foto di-skip (sudah ter-index sebelumnya)."
            )
        elif n_indexed == 0 and n_skipped > 0:
            st.info(
                f"ℹ️ **{pn}** sudah lengkap — semua **{n_skipped}** foto "
                f"sudah ter-index sebelumnya. Tidak ada yang baru."
            )
        else:
            st.success(
                f"✅ **{pn}** berhasil di-index — "
                f"**{n_indexed}/{n_photos}** foto disimpan."
            )
        if err and n_indexed > 0:
            st.caption(f"⚠️ {err}")
    else:
        if n_photos == 0:
            st.warning(f"⚠️ **{pn}** — tidak ada foto di SIMS untuk PN ini.")
        else:
            st.error(f"❌ **{pn}** — gagal di-index. {err}")


# ══════════════════════════════════════════════════════════════════════════
#  MODE 2 — BULK PN
# ══════════════════════════════════════════════════════════════════════════

def _execute_bulk(pns_to_process: list[str], indexed_by: str, job: dict,
                  prefilter_msg: str | None = None):
    """
    Jalankan index_bulk + tulis progress ke job checkpoint SETIAP PN selesai.
    Dipakai baik untuk bulk baru maupun lanjutkan job lama.
    """
    progress_area = st.container()
    with progress_area:
        if prefilter_msg:
            st.info(prefilter_msg)
        progress_bar = st.progress(0.0, text="Memulai...")
        status_text  = st.empty()
        log_area     = st.empty()

    t0 = time.time()
    log_lines: list[str] = []

    def _cb(i, total, pn, r):
        pct = i / total if total else 1.0
        progress_bar.progress(pct, text=f"{i}/{total} — {pn}")

        # ── Checkpoint: persist status PN ini SEKARANG (tahan refresh) ──
        try:
            _job_apply(job, pn, r)
            _job_save(job)
        except Exception:
            pass

        n_idx = r.get("n_indexed", 0)
        n_skp = r.get("n_skipped", 0)
        if r.get("ok"):
            if n_idx > 0 and n_skp > 0:
                sym, msg = "✅", f"+{n_idx} foto baru, {n_skp} skip"
            elif n_idx == 0 and n_skp > 0:
                sym, msg = "⏭️", f"semua {n_skp} foto sudah ter-index"
            else:
                sym, msg = "✅", f"{n_idx}/{r.get('n_photos', 0)} foto"
        elif r.get("n_photos", 0) == 0:
            sym, msg = "⚪", "tidak ada foto SIMS"
        else:
            sym, msg = "❌", (r.get("error") or "gagal")[:60]
        log_lines.append(f"{sym} {pn} — {msg}")
        log_area.text("\n".join(log_lines[-20:]))
        status_text.caption(f"Memproses **{i}/{total}** — {pn}")

    if pns_to_process:
        index_bulk(pns_to_process, indexed_by=indexed_by, progress_callback=_cb)
    else:
        progress_bar.progress(1.0, text="Tidak ada PN untuk diproses")

    elapsed = time.time() - t0

    # ── Finalisasi status job ──
    remaining = _job_remaining(job)
    job["status"] = "done" if not remaining else "stopped"
    _job_save(job)

    progress_bar.progress(1.0, text=f"Selesai dalam {elapsed:.1f}s")
    status_text.empty()
    st.rerun()


def _render_bulk_mode():
    """Textarea PN (1 per baris) → batch index, dengan checkpoint resume."""
    st.markdown("#### Tambah Banyak Part Number Sekaligus")
    st.caption(
        "Masukkan 1 PN per baris. "
        "Proses bisa memakan waktu (~1-3 detik per PN, tergantung jumlah foto). "
        "Progress disimpan otomatis tiap PN — aman dari refresh / sesi putus."
    )

    user = st.session_state.get("current_user", {})
    indexed_by = user.get("username", "admin") if isinstance(user, dict) else "admin"

    job = _job_load()

    # ── Ada job tersimpan? ────────────────────────────────────────────────
    if job:
        remaining = _job_remaining(job)
        if remaining:
            _render_resume_panel(job, remaining, indexed_by)
            return
        # Job sudah tuntas → tampilkan ringkasan + tombol mulai baru
        _render_bulk_summary(job)
        return

    # ── Form bulk baru ────────────────────────────────────────────────────
    bulk_text = st.text_area(
        "Daftar Part Number:",
        placeholder="WG1641230025\nKRTC1700001\nAZ9100440006\n…",
        key="img_idx_bulk_text",
        height=200,
    )

    pn_list = [p.strip().upper() for p in bulk_text.splitlines() if p.strip()]
    pn_list_unique = list(dict.fromkeys(pn_list))
    n_unique = len(pn_list_unique)

    skip_existing = st.checkbox(
        "⚡ **Fast mode** — skip PN yang sudah ter-index (tanpa fetch SIMS)",
        value=True,
        key="img_idx_bulk_skip",
        help=(
            "Cek tabel part_image_index sekali di awal, lalu skip PN yang sudah ada. "
            "Jauh lebih cepat untuk bulk besar. Matikan kalau ingin re-check semua PN "
            "untuk foto SIMS yang baru ditambahkan."
        ),
    )

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.caption(
            f"📊 **{n_unique}** PN unik akan diproses "
            f"({len(pn_list) - n_unique} duplikat akan diabaikan)"
        )
    with col_b:
        do_bulk = st.button(
            f"🚀 Index {n_unique} PN", type="primary",
            use_container_width=True,
            key="btn_img_idx_bulk",
            disabled=(n_unique == 0),
        )

    if do_bulk and pn_list_unique:
        # Buat checkpoint job — semua PN "pending" dulu
        job = _job_init(pn_list_unique, indexed_by, skip_existing)

        prefilter_msg = None
        if skip_existing:
            with st.spinner("⚡ Cek PN yang sudah ter-index..."):
                existing = get_all_indexed_pns()
            skipped_pns = [pn for pn in pn_list_unique if pn in existing]
            for pn in skipped_pns:
                _job_apply(job, pn, {
                    "ok": True, "pn": pn, "n_photos": 0, "n_indexed": 0,
                    "n_skipped": 0, "error": "", "skipped_existing": True,
                })
            _job_save(job)
            if skipped_pns:
                prefilter_msg = (
                    f"⚡ **{len(skipped_pns)}** PN sudah ter-index → di-skip langsung "
                    f"(fast mode). Memproses **{n_unique - len(skipped_pns)}** PN..."
                )

        _execute_bulk(_job_remaining(job), indexed_by, job, prefilter_msg)


def _render_resume_panel(job: dict, remaining: list[str], indexed_by: str):
    """Panel saat ada bulk belum selesai (refresh / sesi mati / di-stop)."""
    c = _job_counts(job)
    done_terminal = c["done"] + c["skipped"] + c["empty"]

    st.warning(
        f"⏸️ **Ada bulk yang belum selesai** (dibuat {job.get('created_at', '—')}, "
        f"oleh `{job.get('indexed_by', '—')}`). "
        f"Progress tersimpan otomatis — bisa dilanjutkan."
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total PN", c["total"])
    m2.metric("Selesai", done_terminal)
    m3.metric("Sisa diproses", len(remaining))
    m4.metric("Gagal", c["failed"])
    if c["total"]:
        st.progress(min(done_terminal / c["total"], 1.0),
                    text=f"{done_terminal}/{c['total']} tuntas")

    _render_job_detail(job)

    col_go, col_drop = st.columns([2, 1])
    with col_go:
        if st.button(
            f"▶️ Lanjutkan ({len(remaining)} PN sisa)",
            type="primary", use_container_width=True,
            key="btn_img_idx_bulk_resume",
        ):
            job["status"] = "running"
            _job_save(job)
            _execute_bulk(remaining, indexed_by, job,
                          prefilter_msg=f"▶️ Melanjutkan {len(remaining)} PN "
                                        f"yang belum selesai...")
    with col_drop:
        if st.button("🗑️ Buang job ini", use_container_width=True,
                     key="btn_img_idx_bulk_discard"):
            _job_clear()
            st.rerun()


def _render_job_detail(job: dict):
    """Expander detail per-PN dari job checkpoint."""
    order = job.get("order", [])
    with st.expander(f"📋 Detail per PN ({len(order)})", expanded=False):
        sym = {"done": "✅", "skipped": "⚡", "empty": "⚪",
               "failed": "❌", "pending": "⏳", "cancelled": "⏳"}
        for pn in order:
            e = job.get("pns", {}).get(pn, {})
            s = e.get("state", "pending")
            if s == "done":
                st.markdown(f"- ✅ **{pn}** — {e.get('n_indexed',0)}/{e.get('n_photos',0)} foto")
            elif s == "skipped":
                st.markdown(f"- ⚡ **{pn}** — sudah ter-index (fast mode skip)")
            elif s == "empty":
                st.markdown(f"- ⚪ **{pn}** — tidak ada foto SIMS")
            elif s == "failed":
                st.markdown(f"- ❌ **{pn}** — {e.get('error','') or 'gagal'}")
            else:
                st.markdown(f"- ⏳ **{pn}** — belum diproses")


def _render_bulk_summary(job: dict):
    """Ringkasan saat job sudah tuntas (tidak ada PN sisa)."""
    c = _job_counts(job)
    total_photos = sum(e.get("n_indexed", 0)
                       for e in job.get("pns", {}).values())

    parts = []
    if c["done"]:    parts.append(f"{c['done']} sukses")
    if c["skipped"]: parts.append(f"⚡ {c['skipped']} skip (sudah ter-index)")
    if c["empty"]:   parts.append(f"{c['empty']} tanpa foto SIMS")
    if c["failed"]:  parts.append(f"{c['failed']} error")

    elapsed_txt = ""
    try:
        t0 = datetime.fromisoformat(job["created_at"])
        t1 = datetime.fromisoformat(job["updated_at"])
        elapsed_txt = f" dalam **{(t1 - t0).total_seconds():.0f}s**"
    except Exception:
        pass

    st.success(
        f"✅ Bulk selesai{elapsed_txt} — " + ", ".join(parts) +
        f". Total **{total_photos}** foto baru ter-index."
    )

    _render_job_detail(job)

    if st.button("🆕 Mulai bulk baru", key="btn_img_idx_bulk_reset"):
        _job_clear()
        st.session_state["img_idx_bulk_text"] = ""
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
#  MODE 3 — DAFTAR PN TER-INDEX
# ══════════════════════════════════════════════════════════════════════════

def _reset_list_page():
    """Reset pagination ke halaman 1 — dipanggil saat filter query berubah."""
    st.session_state[_SS_LIST_PAGE] = 1
    st.session_state["img_idx_pg_jump"] = 1


def _on_jump_page_change():
    """Sync nilai widget jump ke _SS_LIST_PAGE saat user ubah angka."""
    try:
        st.session_state[_SS_LIST_PAGE] = int(st.session_state["img_idx_pg_jump"])
    except (ValueError, TypeError):
        pass


def _render_indexed_list():
    """Tampilkan daftar PN yang sudah di-index + tombol hapus."""
    st.markdown("#### Daftar Part Number Ter-index")

    # Notifikasi hasil hapus
    del_res = st.session_state.pop(_SS_DELETE_RESULT, None)
    if del_res:
        if del_res["ok"]:
            st.success(del_res["msg"])
        else:
            st.error(del_res["msg"])

    # Search/filter
    col_q, col_btn = st.columns([4, 1])
    with col_q:
        query = st.text_input(
            "Filter PN:",
            placeholder="Ketik untuk filter…",
            key=_SS_LIST_QUERY,
            label_visibility="collapsed",
            on_change=_reset_list_page,
        )
    with col_btn:
        if st.button("🔄 Refresh", use_container_width=True,
                     key="btn_img_idx_refresh"):
            st.rerun()

    with st.spinner("Memuat daftar..."):
        rows = list_indexed_pns(query=query, limit=500)

    if not rows:
        if query:
            st.info(f"ℹ️ Tidak ada PN ter-index yang cocok dengan '{query}'.")
        else:
            st.info(
                "ℹ️ Belum ada PN ter-index. "
                "Tambahkan di tab **Single PN** atau **Bulk PN**."
            )
        return

    # ── Pagination ────────────────────────────────────────────────────────
    total      = len(rows)
    n_pages    = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    cur_page   = int(st.session_state.get(_SS_LIST_PAGE, 1) or 1)
    cur_page   = max(1, min(cur_page, n_pages))
    start_idx  = (cur_page - 1) * _PAGE_SIZE
    end_idx    = min(start_idx + _PAGE_SIZE, total)
    page_rows  = rows[start_idx:end_idx]

    st.caption(
        f"Menampilkan **{start_idx + 1}–{end_idx}** dari **{total}** PN "
        f"(urutan: terbaru ke lama) · Halaman **{cur_page}/{n_pages}**"
    )

    # Header row
    h1, h2, h3, h4, h5 = st.columns([3, 1, 2, 2, 1])
    h1.markdown("**Part Number**")
    h2.markdown("**Foto**")
    h3.markdown("**Indexed by**")
    h4.markdown("**Indexed at**")
    h5.markdown("**Aksi**")
    st.markdown("<hr style='margin:4px 0; border:none; border-top:1px solid #e5e7eb;'/>",
                unsafe_allow_html=True)

    # Rows
    for r in page_rows:
        pn          = r["part_number"]
        n_photos    = r["n_photos"]
        indexed_by  = r["indexed_by"] or "—"
        indexed_at  = (r["indexed_at"] or "")[:16].replace("T", " ")

        c1, c2, c3, c4, c5 = st.columns([3, 1, 2, 2, 1])
        c1.markdown(f"**{pn}**")
        c2.markdown(f"{n_photos}")
        c3.markdown(indexed_by)
        c4.markdown(indexed_at)
        with c5:
            if st.button("🗑️", key=f"img_idx_del_{pn}",
                         help=f"Hapus {pn} dari index"):
                _do_delete(pn)

    # ── Pagination controls (di bawah list) ───────────────────────────────
    if n_pages > 1:
        st.markdown(
            "<hr style='margin:8px 0; border:none; border-top:1px solid #e5e7eb;'/>",
            unsafe_allow_html=True,
        )
        cp1, cp2, cp3, cp4, cp5 = st.columns([1, 1, 2, 1, 1])
        with cp1:
            if st.button("⏮️ Awal", use_container_width=True,
                         disabled=(cur_page <= 1),
                         key="img_idx_pg_first"):
                st.session_state[_SS_LIST_PAGE] = 1
                st.rerun()
        with cp2:
            if st.button("◀️ Prev", use_container_width=True,
                         disabled=(cur_page <= 1),
                         key="img_idx_pg_prev"):
                st.session_state[_SS_LIST_PAGE] = cur_page - 1
                st.rerun()
        with cp3:
            # Sync widget ke cur_page — handles update dari tombol prev/next/awal/akhir.
            # Harus di-set SEBELUM widget render. on_change callback hanya fire saat
            # user manual ubah, jadi tidak konflik dengan sync ini.
            st.session_state["img_idx_pg_jump"] = cur_page
            st.number_input(
                "Lompat ke halaman",
                min_value=1, max_value=n_pages, step=1,
                key="img_idx_pg_jump",
                label_visibility="collapsed",
                on_change=_on_jump_page_change,
            )
        with cp4:
            if st.button("Next ▶️", use_container_width=True,
                         disabled=(cur_page >= n_pages),
                         key="img_idx_pg_next"):
                st.session_state[_SS_LIST_PAGE] = cur_page + 1
                st.rerun()
        with cp5:
            if st.button("Akhir ⏭️", use_container_width=True,
                         disabled=(cur_page >= n_pages),
                         key="img_idx_pg_last"):
                st.session_state[_SS_LIST_PAGE] = n_pages
                st.rerun()


def _do_delete(pn: str):
    """Hapus 1 PN dari index."""
    ok = delete_pn_from_index(pn)
    if ok:
        st.session_state[_SS_DELETE_RESULT] = {
            "ok": True,
            "msg": f"🗑️ **{pn}** dihapus dari image index.",
        }
    else:
        st.session_state[_SS_DELETE_RESULT] = {
            "ok": False,
            "msg": f"❌ Gagal menghapus {pn}.",
        }
    st.rerun()
