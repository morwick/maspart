"""
Stok Opname — Per-User Sessions
================================
Modul untuk mengelola sesi stok opname per-user.

Backend penyimpanan (otomatis dipilih):
  1. Supabase (table `opname_sessions`) — kalau dikonfigurasi
  2. Fallback: file JSON lokal di opname/<user>/{draft,history}.json
"""
from __future__ import annotations

import io
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


OPNAME_DIR = Path("opname")

# ── Backend Supabase (opsional) ──────────────────────────────────
try:
    from supabase import SupabaseOpname as _SupaOpname
    _SUPA_AVAILABLE = True
except Exception:
    _SupaOpname = None
    _SUPA_AVAILABLE = False


def _use_supabase() -> bool:
    """Return True jika Supabase tersedia & ter-konfigurasi."""
    if not _SUPA_AVAILABLE or _SupaOpname is None:
        return False
    try:
        return bool(_SupaOpname.is_available())
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
#  PATHS PER USER
# ─────────────────────────────────────────────────────────────
def _safe_username(username: str) -> str:
    u = (username or "anonymous").strip().lower()
    u = re.sub(r"[^a-z0-9._-]+", "_", u)
    return u or "anonymous"


def _user_dir(username: str) -> Path:
    p = OPNAME_DIR / _safe_username(username)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _draft_path(username: str) -> Path:
    return _user_dir(username) / "draft.json"


def _history_path(username: str) -> Path:
    return _user_dir(username) / "history.json"


# ─────────────────────────────────────────────────────────────
#  COERCION HELPERS
# ─────────────────────────────────────────────────────────────
def to_int(v) -> Optional[int]:
    """Coerce nilai stok jadi int. Return None jika kosong/non-numerik."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("—", "-", "nan", "None", "NaN"):
        return None
    s = s.replace(",", "").replace(".", "")  # buang separator
    try:
        return int(float(s))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
#  SESSION BUILD (dari hasil parse upload user)
# ─────────────────────────────────────────────────────────────
def build_new_session(parsed_items: Dict[str, Dict], username: str, source_filename: str = "") -> Dict:
    """
    Bangun sesi opname baru dari parsed items hasil upload user.
    parsed_items: {PN: {"qty_sistem": int|None, "part_name": str}}.
    Items disimpan sebagai dict {PN: {qty_sistem, qty_fisik, note, part_name}}.
    """
    items: Dict[str, Dict] = {}
    for pn, payload in (parsed_items or {}).items():
        pn_key = str(pn).strip().upper()
        if not pn_key:
            continue
        items[pn_key] = {
            "qty_sistem": to_int(payload.get("qty_sistem") if isinstance(payload, dict) else payload),
            "qty_fisik":  None,
            "note":       "",
            "part_name":  (payload.get("part_name", "") if isinstance(payload, dict) else "") or "",
        }
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "session_id":      str(uuid.uuid4()),
        "username":        username,
        "started_at":      now,
        "updated_at":      now,
        "finalized":       False,
        "source_filename": source_filename or "",
        "items":           items,
    }


# ─────────────────────────────────────────────────────────────
#  FILE BACKEND (fallback)
# ─────────────────────────────────────────────────────────────
def _load_draft_file(username: str) -> Optional[Dict]:
    p = _draft_path(username)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_draft_file(username: str, session: Dict) -> Tuple[bool, Optional[str]]:
    try:
        p = _draft_path(username)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(session, f, ensure_ascii=False, indent=2)
        return True, None
    except Exception as e:
        return False, str(e)


def _delete_draft_file(username: str) -> bool:
    p = _draft_path(username)
    try:
        if p.exists():
            p.unlink()
        return True
    except Exception:
        return False


def _load_history_file(username: str) -> List[Dict]:
    p = _history_path(username)
    if not p.exists():
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_history_file(username: str, history: List[Dict]) -> Tuple[bool, Optional[str]]:
    try:
        p = _history_path(username)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        return True, None
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────
#  PUBLIC API — dispatch ke Supabase atau file
# ─────────────────────────────────────────────────────────────
def backend() -> str:
    """Return 'supabase' atau 'file' — backend yang sedang dipakai."""
    return "supabase" if _use_supabase() else "file"


def load_draft(username: str) -> Optional[Dict]:
    if _use_supabase():
        return _SupaOpname.load_draft(username)
    return _load_draft_file(username)


def save_draft(username: str, session: Dict) -> Tuple[bool, Optional[str]]:
    session["updated_at"] = datetime.now().isoformat(timespec="seconds")
    if _use_supabase():
        return _SupaOpname.save_draft(username, session)
    return _save_draft_file(username, session)


def delete_draft(username: str) -> bool:
    if _use_supabase():
        return _SupaOpname.delete_draft(username)
    return _delete_draft_file(username)


def load_history(username: str) -> List[Dict]:
    if _use_supabase():
        return _SupaOpname.load_history(username)
    return _load_history_file(username)


def finalize_session(username: str, session: Dict) -> Tuple[bool, Optional[str]]:
    """
    Tandai sesi sebagai final → masuk history, draft hilang.
    """
    session["finalized"]    = True
    session["finalized_at"] = datetime.now().isoformat(timespec="seconds")
    session["summary"]      = summarize(session)

    if _use_supabase():
        return _SupaOpname.finalize(username, session)

    # File fallback
    history = _load_history_file(username)
    history.insert(0, session)
    ok, err = _save_history_file(username, history)
    if not ok:
        return False, err
    _delete_draft_file(username)
    return True, None


def delete_history_entry(username: str, session_id: str) -> bool:
    if _use_supabase():
        return _SupaOpname.delete_history_entry(username, session_id)

    history = _load_history_file(username)
    new_hist = [s for s in history if s.get("session_id") != session_id]
    if len(new_hist) == len(history):
        return False
    ok, _ = _save_history_file(username, new_hist)
    return ok


# ─────────────────────────────────────────────────────────────
#  SUMMARY / SELISIH
# ─────────────────────────────────────────────────────────────
def summarize(session: Dict) -> Dict:
    items = session.get("items", {}) or {}
    total       = len(items)
    counted     = 0
    match       = 0
    diff_count  = 0
    selisih_pos = 0
    selisih_neg = 0
    for pn, it in items.items():
        qf = it.get("qty_fisik")
        qs = it.get("qty_sistem")
        if qf is None:
            continue
        counted += 1
        if qs is None:
            qs_eff = 0
        else:
            qs_eff = qs
        diff = qf - qs_eff
        if diff == 0:
            match += 1
        else:
            diff_count += 1
            if diff > 0:
                selisih_pos += diff
            else:
                selisih_neg += diff
    return {
        "total":       total,
        "counted":     counted,
        "uncounted":   total - counted,
        "match":       match,
        "diff_count":  diff_count,
        "selisih_pos": selisih_pos,
        "selisih_neg": selisih_neg,
        "selisih_net": selisih_pos + selisih_neg,
    }


# ─────────────────────────────────────────────────────────────
#  ITEMS ↔ DATAFRAME
# ─────────────────────────────────────────────────────────────
def items_to_df(items: Dict[str, Dict], part_name_lookup: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    rows = []
    pnl  = part_name_lookup or {}
    for pn, it in items.items():
        qs = it.get("qty_sistem")
        qf = it.get("qty_fisik")
        diff = None
        if qf is not None:
            diff = qf - (qs or 0)
        # Prioritas Part Name: yang tersimpan di items (dari upload user) → fallback lookup
        pname = (it.get("part_name") or "").strip() or pnl.get(pn, "")
        rows.append({
            "Part Number": pn,
            "Part Name":   pname,
            "Qty Sistem":  qs,
            "Qty Fisik":   qf,
            "Selisih":     diff,
            "Note":        it.get("note", "") or "",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Part Number").reset_index(drop=True)
    return df


def df_to_items(df: pd.DataFrame, base_items: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Update base_items dengan nilai dari df (qty_fisik + note).
    Hanya field yang dapat diedit user yang di-merge.
    """
    if df is None or df.empty:
        return base_items
    new_items = dict(base_items)
    for _, row in df.iterrows():
        pn = str(row.get("Part Number", "")).strip().upper()
        if not pn or pn not in new_items:
            continue
        qf = to_int(row.get("Qty Fisik"))
        new_items[pn]["qty_fisik"] = qf
        new_items[pn]["note"]      = str(row.get("Note", "") or "").strip()
    return new_items


# ─────────────────────────────────────────────────────────────
#  INITIAL STOK UPLOAD (data stok awal dari user)
# ─────────────────────────────────────────────────────────────
def make_initial_template_excel() -> bytes:
    """Template kosong: kolom Part Number, Part Name (opsional), Qty Sistem."""
    df = pd.DataFrame(
        [
            ["WG1642821034", "(opsional contoh)", 12],
            ["WG1642821035", "",                  5],
            ["WG9725520274", "",                  0],
        ],
        columns=["Part Number", "Part Name", "Qty Sistem"],
    )
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Stok Awal", index=False)
        ws = w.sheets["Stok Awal"]
        ws.column_dimensions["A"].width = 22
        ws.column_dimensions["B"].width = 36
        ws.column_dimensions["C"].width = 12
    return buf.getvalue()


def parse_stok_upload(file_bytes: bytes) -> Tuple[Dict[str, Dict], List[str]]:
    """
    Parse Excel data stok awal yang diupload user.
    Cari kolom: Part Number, Qty Sistem (atau Stok), Part Name (opsional).
    Return ({PN: {qty_sistem, part_name}}, list_warning).
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    except Exception as e:
        return {}, [f"Gagal baca Excel: {e}"]

    if df.empty:
        return {}, ["File Excel kosong."]

    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    def find_col(*candidates) -> Optional[str]:
        for cand in candidates:
            for low, orig in cols_lower.items():
                if cand in low:
                    return orig
        return None

    pn_col    = find_col("part number", "partnumber", "part_no", "part no", "kode", "no part")
    qs_col    = find_col("qty sistem", "qty_sistem", "qty", "stok", "stock", "system")
    pname_col = find_col("part name", "partname", "nama", "deskripsi", "description")

    # Fallback: kalau tidak ada header yang dikenali, anggap kol A=PN, kol B=qty
    if not pn_col or not qs_col:
        try:
            df2 = pd.read_excel(io.BytesIO(file_bytes), header=None, dtype=str)
            if df2.shape[1] >= 2:
                # Skip baris header jika ada
                first = str(df2.iloc[0, 0]).strip().lower()
                if first in ("part number", "partnumber", "kode", "pn", "no part"):
                    df2 = df2.iloc[1:]
                df = df2.rename(columns={0: "Part Number", 1: "Qty Sistem"})
                if df.shape[1] >= 3:
                    df = df.rename(columns={2: "Part Name"})
                pn_col = "Part Number"
                qs_col = "Qty Sistem"
                pname_col = "Part Name" if "Part Name" in df.columns else None
                warnings.append("Header tidak terdeteksi — pakai kolom A (PN), B (Qty Sistem), C (Part Name).")
        except Exception:
            pass

    if not pn_col:
        return {}, ["Kolom 'Part Number' tidak ditemukan."]
    if not qs_col:
        return {}, ["Kolom 'Qty Sistem' / 'Stok' tidak ditemukan."]

    out: Dict[str, Dict] = {}
    duplicates = 0
    for _, row in df.iterrows():
        pn = str(row.get(pn_col, "") or "").strip().upper()
        if not pn or pn in ("NAN", "NONE"):
            continue
        qs = to_int(row.get(qs_col))
        pname = ""
        if pname_col:
            pname = str(row.get(pname_col, "") or "").strip()
            if pname.lower() in ("nan", "none"):
                pname = ""
        if pn in out:
            duplicates += 1
        out[pn] = {"qty_sistem": qs, "part_name": pname}

    if duplicates:
        warnings.append(f"{duplicates} PN duplikat — yang terakhir dipakai.")
    if not out:
        warnings.append("Tidak ada baris valid yang berhasil dibaca.")
    return out, warnings


# ─────────────────────────────────────────────────────────────
#  EXCEL TEMPLATE & PARSER & REPORT (untuk qty fisik)
# ─────────────────────────────────────────────────────────────
def make_template_excel(session: Dict, part_name_lookup: Optional[Dict[str, str]] = None) -> bytes:
    """Excel template untuk diisi offline."""
    df = items_to_df(session.get("items", {}), part_name_lookup)
    df_out = df[["Part Number", "Part Name", "Qty Sistem", "Qty Fisik", "Note"]].copy()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_out.to_excel(w, sheet_name="Opname", index=False)
        ws = w.sheets["Opname"]
        ws.column_dimensions["A"].width = 22
        ws.column_dimensions["B"].width = 38
        ws.column_dimensions["C"].width = 12
        ws.column_dimensions["D"].width = 12
        ws.column_dimensions["E"].width = 30
    return buf.getvalue()


def parse_uploaded_excel(file_bytes: bytes) -> Tuple[Dict[str, Dict], List[str]]:
    """
    Parse Excel hasil opname yang diupload user.
    Return (dict {PN: {qty_fisik, note}}, list_warning).
    Cari kolom Part Number, Qty Fisik, dan opsional Note.
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    except Exception as e:
        return {}, [f"Gagal baca Excel: {e}"]

    if df.empty:
        return {}, ["File Excel kosong."]

    # cari kolom secara longgar
    cols_lower = {str(c).strip().lower(): c for c in df.columns}

    def find_col(*candidates) -> Optional[str]:
        for cand in candidates:
            for low, orig in cols_lower.items():
                if cand in low:
                    return orig
        return None

    pn_col   = find_col("part number", "partnumber", "part_no", "part no", "kode")
    qf_col   = find_col("qty fisik", "fisik", "actual", "physical")
    note_col = find_col("note", "catatan", "ket")

    if not pn_col:
        return {}, ["Kolom 'Part Number' tidak ditemukan di Excel."]
    if not qf_col:
        return {}, ["Kolom 'Qty Fisik' tidak ditemukan di Excel."]

    out: Dict[str, Dict] = {}
    for _, row in df.iterrows():
        pn = str(row.get(pn_col, "")).strip().upper()
        if not pn or pn in ("NAN", "NONE"):
            continue
        qf = to_int(row.get(qf_col))
        note = ""
        if note_col:
            note = str(row.get(note_col, "") or "").strip()
            if note.lower() in ("nan", "none"):
                note = ""
        out[pn] = {"qty_fisik": qf, "note": note}

    if not out:
        warnings.append("Tidak ada baris valid yang berhasil dibaca.")
    return out, warnings


def merge_uploaded(items: Dict[str, Dict], uploaded: Dict[str, Dict]) -> Tuple[Dict[str, Dict], int, int]:
    """Apply uploaded {PN:{qty_fisik,note}} ke items. Return (new_items, matched, unmatched)."""
    matched   = 0
    unmatched = 0
    new_items = dict(items)
    for pn, payload in uploaded.items():
        if pn in new_items:
            new_items[pn]["qty_fisik"] = payload.get("qty_fisik")
            new_items[pn]["note"]      = payload.get("note", "") or new_items[pn].get("note", "")
            matched += 1
        else:
            unmatched += 1
    return new_items, matched, unmatched


def make_report_excel(session: Dict, part_name_lookup: Optional[Dict[str, str]] = None) -> bytes:
    """
    Excel laporan: 1 sheet semua + 1 sheet hanya selisih + 1 sheet ringkasan.
    """
    df_all = items_to_df(session.get("items", {}), part_name_lookup)
    df_diff = df_all[(df_all["Selisih"].notna()) & (df_all["Selisih"] != 0)].copy() if not df_all.empty else df_all
    df_uncounted = df_all[df_all["Qty Fisik"].isna()].copy() if not df_all.empty else df_all

    summary = session.get("summary") or summarize(session)
    df_sum = pd.DataFrame([
        ["Session ID",       session.get("session_id", "")],
        ["Username",         session.get("username", "")],
        ["Started",          session.get("started_at", "")],
        ["Updated",          session.get("updated_at", "")],
        ["Finalized",        session.get("finalized_at", "")],
        ["Total PN",         summary.get("total", 0)],
        ["Sudah Dihitung",   summary.get("counted", 0)],
        ["Belum Dihitung",   summary.get("uncounted", 0)],
        ["Cocok",            summary.get("match", 0)],
        ["Berselisih",       summary.get("diff_count", 0)],
        ["Selisih Plus (+)", summary.get("selisih_pos", 0)],
        ["Selisih Minus (-)", summary.get("selisih_neg", 0)],
        ["Selisih Net",      summary.get("selisih_net", 0)],
    ], columns=["Keterangan", "Nilai"])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df_sum.to_excel(w, sheet_name="Ringkasan", index=False)
        df_all.to_excel(w, sheet_name="Semua", index=False)
        df_diff.to_excel(w, sheet_name="Selisih", index=False)
        df_uncounted.to_excel(w, sheet_name="Belum Dihitung", index=False)
        for s in ("Ringkasan", "Semua", "Selisih", "Belum Dihitung"):
            ws = w.sheets[s]
            ws.column_dimensions["A"].width = 22
            ws.column_dimensions["B"].width = 36
    return buf.getvalue()
