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

import time
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
_SS_BULK_RESULTS  = "_img_idx_bulk_results"
_SS_BULK_RUNNING  = "_img_idx_bulk_running"
_SS_DELETE_RESULT = "_img_idx_delete_result"
_SS_LIST_QUERY    = "_img_idx_list_query"


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

def _render_bulk_mode():
    """Textarea PN (1 per baris) → batch index."""
    st.markdown("#### Tambah Banyak Part Number Sekaligus")
    st.caption(
        "Masukkan 1 PN per baris. "
        "Proses bisa memakan waktu (~1-3 detik per PN, tergantung jumlah foto)."
    )

    user = st.session_state.get("current_user", {})
    indexed_by = user.get("username", "admin") if isinstance(user, dict) else "admin"

    bulk_text = st.text_area(
        "Daftar Part Number:",
        placeholder="WG1641230025\nKRTC1700001\nAZ9100440006\n…",
        key="img_idx_bulk_text",
        height=200,
    )

    # Parse PN list (preview count)
    pn_list = [p.strip().upper() for p in bulk_text.splitlines()
               if p.strip()]
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

    # Tampilkan hasil sebelumnya (kalau ada)
    prev_results = st.session_state.get(_SS_BULK_RESULTS)
    if prev_results and not do_bulk:
        _render_bulk_summary(prev_results)
        return

    # Eksekusi bulk
    if do_bulk and pn_list_unique:
        progress_area = st.container()
        with progress_area:
            prefilter_box = st.empty()
            progress_bar  = st.progress(0.0, text="Memulai...")
            status_text   = st.empty()
            log_area      = st.empty()

        t0 = time.time()

        # ── Pre-filter: fast mode skip PN yang sudah ter-index ──
        skipped_results: list[dict] = []
        pns_to_process = pn_list_unique
        if skip_existing:
            with st.spinner("⚡ Cek PN yang sudah ter-index..."):
                existing = get_all_indexed_pns()
            pns_to_process = [pn for pn in pn_list_unique if pn not in existing]
            skipped_pns    = [pn for pn in pn_list_unique if pn in existing]
            skipped_results = [{
                "ok":               True,
                "pn":               pn,
                "n_photos":         0,
                "n_indexed":        0,
                "n_skipped":        0,
                "error":            "",
                "skipped_existing": True,
            } for pn in skipped_pns]
            if skipped_pns:
                prefilter_box.info(
                    f"⚡ **{len(skipped_pns)}** PN sudah ter-index → di-skip langsung "
                    f"(fast mode). Memproses **{len(pns_to_process)}** PN baru..."
                )

        log_lines = []

        def _cb(i, total, pn, r):
            pct = i / total if total else 1.0
            progress_bar.progress(pct, text=f"{i}/{total} — {pn}")
            n_idx = r.get("n_indexed", 0)
            n_skp = r.get("n_skipped", 0)
            if r.get("ok"):
                if n_idx > 0 and n_skp > 0:
                    sym = "✅"
                    msg = f"+{n_idx} foto baru, {n_skp} skip"
                elif n_idx == 0 and n_skp > 0:
                    sym = "⏭️"
                    msg = f"semua {n_skp} foto sudah ter-index"
                else:
                    sym = "✅"
                    msg = f"{n_idx}/{r['n_photos']} foto"
            elif r.get("n_photos", 0) == 0:
                sym = "⚪"
                msg = "tidak ada foto SIMS"
            else:
                sym = "❌"
                msg = (r.get("error") or "gagal")[:60]
            log_lines.append(f"{sym} {pn} — {msg}")
            # tampilkan 20 baris terakhir
            log_area.text("\n".join(log_lines[-20:]))
            status_text.caption(f"Memproses **{i}/{total}** — {pn}")

        if pns_to_process:
            new_results = index_bulk(pns_to_process, indexed_by=indexed_by,
                                     progress_callback=_cb)
        else:
            new_results = []
            progress_bar.progress(1.0, text="Tidak ada PN baru untuk diproses")

        elapsed = time.time() - t0
        all_results = skipped_results + new_results

        progress_bar.progress(1.0, text=f"Selesai dalam {elapsed:.1f}s")
        status_text.empty()

        st.session_state[_SS_BULK_RESULTS] = {
            "results": all_results,
            "elapsed": elapsed,
        }
        st.rerun()


def _render_bulk_summary(payload: dict):
    """Render ringkasan hasil bulk indexing."""
    results = payload["results"]
    elapsed = payload["elapsed"]

    n_total      = len(results)
    n_fast_skip  = sum(1 for r in results if r.get("skipped_existing"))
    n_ok         = sum(1 for r in results
                       if r.get("ok") and not r.get("skipped_existing"))
    n_empty      = sum(1 for r in results
                       if not r.get("ok") and r.get("n_photos", 0) == 0)
    n_err        = n_total - n_ok - n_empty - n_fast_skip

    total_photos = sum(r.get("n_indexed", 0) for r in results)

    parts = []
    if n_ok:        parts.append(f"{n_ok} sukses")
    if n_fast_skip: parts.append(f"⚡ {n_fast_skip} skip (sudah ter-index)")
    if n_empty:     parts.append(f"{n_empty} tanpa foto SIMS")
    if n_err:       parts.append(f"{n_err} error")

    st.success(
        f"✅ Selesai dalam **{elapsed:.1f}s** — " + ", ".join(parts) +
        f". Total **{total_photos}** foto baru ter-index."
    )

    # Detail expandable
    with st.expander(f"📋 Detail per PN ({n_total} hasil)", expanded=False):
        for r in results:
            pn        = r.get("pn", "")
            n_indexed = r.get("n_indexed", 0)
            n_photos  = r.get("n_photos", 0)
            err       = r.get("error", "")
            if r.get("skipped_existing"):
                st.markdown(f"- ⚡ **{pn}** — sudah ter-index (fast mode skip)")
            elif r.get("ok"):
                st.markdown(f"- ✅ **{pn}** — {n_indexed}/{n_photos} foto")
            elif n_photos == 0:
                st.markdown(f"- ⚪ **{pn}** — tidak ada foto SIMS")
            else:
                st.markdown(f"- ❌ **{pn}** — {err}")

    if st.button("🆕 Mulai bulk baru", key="btn_img_idx_bulk_reset"):
        st.session_state.pop(_SS_BULK_RESULTS, None)
        st.session_state["img_idx_bulk_text"] = ""
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
#  MODE 3 — DAFTAR PN TER-INDEX
# ══════════════════════════════════════════════════════════════════════════

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
        )
    with col_btn:
        if st.button("🔄 Refresh", use_container_width=True,
                     key="btn_img_idx_refresh"):
            st.rerun()

    with st.spinner("Memuat daftar..."):
        rows = list_indexed_pns(query=query, limit=200)

    if not rows:
        if query:
            st.info(f"ℹ️ Tidak ada PN ter-index yang cocok dengan '{query}'.")
        else:
            st.info(
                "ℹ️ Belum ada PN ter-index. "
                "Tambahkan di tab **Single PN** atau **Bulk PN**."
            )
        return

    st.caption(f"Menampilkan **{len(rows)}** PN (urutan: terbaru ke lama)")

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
    for r in rows:
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
