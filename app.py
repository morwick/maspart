"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING + LOGIN SYSTEM + THRESHOLD + BATCH DOWNLOAD + EDIT POPULASI (GitHub Sync)
=============================================================
"""

import streamlit as st
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import pickle
import hmac
import re
import io
import json
import requests


warnings.filterwarnings('ignore')

# ── Sinonim Loader ──────────────────────────────────────────────────
SINONIM_FILE = Path("data/sinonim/sinonim.json")

def load_synonym_map() -> list:
    """
    Muat kamus sinonim dari data/sinonim/sinonim.json.
    Return list of dict: [{triggers: [...], keywords: [...]}, ...]
    Di-cache di session_state agar tidak dibaca ulang setiap request.
    """
    cache_key = "_synonym_map_cache"
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    if not SINONIM_FILE.exists():
        st.session_state[cache_key] = []
        return []

    try:
        with open(SINONIM_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Validasi struktur
        result = []
        for item in data:
            if "triggers" in item and "keywords" in item:
                result.append({
                    "grup"    : item.get("grup", ""),
                    "triggers": [str(t).lower().strip() for t in item["triggers"]],
                    "keywords": [str(k).strip() for k in item["keywords"]],
                })
        st.session_state[cache_key] = result
        return result
    except Exception as e:
        st.warning(f"⚠️ Gagal membaca sinonim.json: {e}")
        st.session_state[cache_key] = []
        return []


def apply_synonyms(term: str) -> list:
    """
    Kembalikan list keyword pencarian berdasarkan sinonim dari file JSON.
    Selalu include term asli + semua sinonim yang cocok.
    """
    term_lower = term.strip().lower()
    keywords   = [term_lower]

    synonym_map = load_synonym_map()
    for group in synonym_map:
        triggers = group["triggers"]
        matched  = any(
            term_lower in trigger or trigger in term_lower
            for trigger in triggers
        )
        if matched:
            keywords.extend(group["keywords"])

    # Hapus duplikat, pertahankan urutan
    seen   = set()
    result = []
    for k in keywords:
        k_lower = k.lower()
        if k_lower not in seen:
            seen.add(k_lower)
            result.append(k)
    return result


# ── GitHub Integration ──────────────────────────────────────────────
def _github_cfg():
    """Ambil konfigurasi GitHub dari st.secrets."""
    try:
        token  = st.secrets["GITHUB_TOKEN"]
        repo   = st.secrets["GITHUB_REPO"]          # format: "owner/repo"
        branch = st.secrets.get("GITHUB_BRANCH", "main")
        return token, repo, branch
    except Exception:
        return None, None, None

def _github_get_file(repo, branch, path, token):
    """GET file dari GitHub API — return (content_bytes, sha) atau (None, None)."""
    url  = f"https://api.github.com/repos/{repo}/contents/{path}"
    hdrs = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    r    = requests.get(url, headers=hdrs, params={"ref": branch}, timeout=20)
    if r.status_code == 200:
        data = r.json()
        import base64 as _b64
        return _b64.b64decode(data["content"]), data["sha"]
    return None, f"HTTP {r.status_code} — {r.text[:200]}"

def _github_push_file(repo, branch, path, content_bytes, sha, token, commit_msg):
    """PUT (update) file ke GitHub — return (True, None) atau (False, error_str)."""
    import base64 as _b64
    url  = f"https://api.github.com/repos/{repo}/contents/{path}"
    hdrs = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    body = {
        "message": commit_msg,
        "content": _b64.b64encode(content_bytes).decode(),
        "branch":  branch,
    }
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=hdrs, json=body, timeout=30)
    if r.status_code in (200, 201):
        return True, None
    return False, f"HTTP {r.status_code}: {r.json().get('message','Unknown error')}"

def _github_path_for_populasi(filename):
    """Konversi nama file lokal ke path relatif di GitHub (data/...)."""
    return f"data/populasi/{filename}"

def save_populasi_to_github(df_updated: pd.DataFrame, source_file: str, source_sheet: str, user: str) -> tuple:
    """
    Simpan df_updated ke file Excel di GitHub.
    Hanya baris dengan _source_file == source_file dan _source_sheet == source_sheet yang diubah.
    Return (True, None) atau (False, error_msg).
    """
    token, repo, branch = _github_cfg()
    if not token:
        return False, "GitHub credentials belum dikonfigurasi di st.secrets.\nTambahkan GITHUB_TOKEN, GITHUB_REPO, dan GITHUB_BRANCH."

    gh_path = _github_path_for_populasi(source_file)

    # Ambil file saat ini dari GitHub untuk mendapatkan SHA
    current_bytes, sha = _github_get_file(repo, branch, gh_path, token)
    if current_bytes is None:
        return False, f"Gagal akses GitHub\nRepo: {repo}\nPath: {gh_path}\nDetail: {sha}"

    # Baca file Excel existing dari GitHub
    try:
        pd.ExcelFile(io.BytesIO(current_bytes), engine="openpyxl")
    except Exception as e:
        return False, f"Gagal membaca file Excel dari GitHub: {e}"

    # Tulis ulang semua sheet, dengan sheet target di-update
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(current_bytes))

    if source_sheet not in wb.sheetnames:
        return False, f"Sheet '{source_sheet}' tidak ditemukan dalam file '{source_file}'."

    ws = wb[source_sheet]

    # Ambil kolom dari header baris pertama
    header_row = [cell.value for cell in ws[1]]

    # Filter df_updated hanya untuk source_file + source_sheet ini
    mask = (
        (df_updated["_source_file"]  == source_file) &
        (df_updated["_source_sheet"] == source_sheet)
    )
    df_sheet = df_updated[mask].copy()

    # Hapus kolom internal
    display_cols = [c for c in df_sheet.columns if not c.startswith("_source")]
    df_sheet = df_sheet[display_cols].reset_index(drop=True)

    # Update baris data di worksheet (mulai baris 2, baris 1 adalah header)
    for row_idx, (_, data_row) in enumerate(df_sheet.iterrows(), start=2):
        for col_idx, col_name in enumerate(header_row, start=1):
            if col_name in df_sheet.columns:
                val = data_row.get(col_name, None)
                # Konversi NaN ke None agar cell kosong
                if pd.isna(val) if not isinstance(val, str) else False:
                    val = None
                ws.cell(row=row_idx, column=col_idx, value=val)

    # Hapus baris sisa jika jumlah baris berkurang
    last_data_row = len(df_sheet) + 1
    while ws.max_row > last_data_row:
        ws.delete_rows(ws.max_row)

    # Simpan ke bytes
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    new_bytes = buf.getvalue()

    # Push ke GitHub
    commit_msg = f"[Admin] Edit populasi unit '{source_file}' sheet '{source_sheet}' oleh {user} — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    ok, err = _github_push_file(repo, branch, gh_path, new_bytes, sha, token, commit_msg)
    if ok:
        # Update file lokal juga agar sinkron
        local_path = DATA_FOLDER / "populasi" / source_file
        try:
            local_path.write_bytes(new_bytes)
        except Exception:
            pass
        return True, None
    return False, err


# ── SIMS Image Fetcher ─────────────────────────────────────────────
try:
    from sims_fetcher import get_sims_images as _sims_fetch
    SIMS_ENABLED = True
except ImportError:
    SIMS_ENABLED = False
    def _sims_fetch(pn, force_refresh=False):
        return [], "sims_fetcher.py tidak ditemukan"

st.set_page_config(
    page_title="Part Number Finder",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={'Get Help': None, 'Report a bug': None, 'About': None}
)

KEEP_ALIVE_JS = """
<script>
(function() {
    if (window.__keepAliveActive) return;
    window.__keepAliveActive = true;
    const INTERVAL_MS = 4 * 60 * 1000;  // setiap 4 menit
    function ping() {
        var xhr = new XMLHttpRequest();
        xhr.open('GET', window.location.href + '?_ka=' + Date.now(), true);
        xhr.send();
    }
    window.__keepAliveTimer = setInterval(ping, INTERVAL_MS);
    setTimeout(ping, 30 * 1000);  // ping pertama setelah 30 detik
})();
</script>
"""

def inject_keep_alive():
    st.markdown(KEEP_ALIVE_JS, unsafe_allow_html=True)

TAB_PERSIST_JS = """
<script>
(function() {
    const KEY = 'pnf_active_tab';
    function attachListeners() {
        document.querySelectorAll('[data-baseweb="tab"]').forEach(function(tab, idx) {
            if (!tab._pnf_listener) {
                tab._pnf_listener = true;
                tab.addEventListener('click', function() {
                    sessionStorage.setItem(KEY, idx);
                });
            }
        });
    }
    function restoreTab() {
        var saved = sessionStorage.getItem(KEY);
        if (saved === null) return;
        var idx = parseInt(saved);
        var tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length > idx && tabs[idx].getAttribute('aria-selected') !== 'true') {
            tabs[idx].click();
        }
    }
    var _lastTabCount = 0;
    var observer = new MutationObserver(function() {
        var tabs = document.querySelectorAll('[data-baseweb="tab"]');
        if (tabs.length !== _lastTabCount) {
            _lastTabCount = tabs.length;
            attachListeners();
            setTimeout(restoreTab, 50);
        }
    });
    observer.observe(document.body, { childList: true, subtree: true });
    setTimeout(function() { attachListeners(); restoreTab(); }, 400);
})();
</script>
"""

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    header[data-testid="stHeader"] {display: none !important;}
    div[data-testid="stToolbar"] {display: none !important;}
    .login-page [data-testid="stSidebar"] > div { display: none !important; }
    [data-testid="collapsedControl"] { display: block !important; visibility: visible !important; z-index: 9999 !important; }
    .main-header { font-size: 2.5rem; color: #1E88E5; text-align: center; margin-bottom: 1.5rem; padding-top: 0.8rem; }
    .sub-header { font-size: 1.5rem; color: #0D47A1; margin-top: 1.5rem; margin-bottom: 1rem; }
    .search-box { background-color: #F5F5F5; padding: 1.5rem; border-radius: 0.5rem; margin-bottom: 1.5rem; }
    .user-badge { display: inline-flex; align-items: center; gap: 0.4rem; background: #E3F2FD; border: 1px solid #90CAF9; border-radius: 20px; padding: 0.3rem 0.85rem; font-size: 0.85rem; color: #1565C0; font-weight: 600; }
    .role-admin { color: #E65100; font-weight: 700; }
    .role-user  { color: #1565C0; font-weight: 600; }
    iframe[height="0"] { display: none !important; }
    .batch-info-box { background: #E8F5E9; border-left: 4px solid #4CAF50; padding: 0.8rem 1rem; border-radius: 0 8px 8px 0; margin-bottom: 1rem; }
    .synonym-info { background: #FFF3E0; border-left: 4px solid #FF9800; padding: 0.5rem 0.9rem;
                    border-radius: 0 6px 6px 0; font-size: 0.85rem; margin-bottom: 0.5rem; }
</style>
""", unsafe_allow_html=True)

SESSION_TIMEOUT_MINUTES = 75
LOGIN_FOLDER    = Path("login")
DATA_FOLDER     = Path("data")
CACHE_FOLDER    = Path(".cache")
IMAGES_FOLDER   = Path("images")


# ── Login Manager ───────────────────────────────────────────────────
class LoginManager:
    def __init__(self):
        LOGIN_FOLDER.mkdir(parents=True, exist_ok=True)
        if "login_users_df" not in st.session_state:
            st.session_state.login_users_df = self._load_users()

    @staticmethod
    def _load_users() -> pd.DataFrame:
        excel_ext = (".xlsx", ".xls", ".xlsm")
        all_rows  = []
        for fpath in LOGIN_FOLDER.iterdir():
            if fpath.suffix.lower() not in excel_ext:
                continue
            try:
                xls = pd.ExcelFile(fpath, engine="openpyxl")
                for sheet in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet, dtype=str, header=None)
                    if len(df) == 0:
                        continue
                    first = df.iloc[0].astype(str).str.strip().str.lower().tolist()
                    if any(v in ("username", "user", "nama") for v in first):
                        df = df.iloc[1:].reset_index(drop=True)
                    if len(df.columns) >= 4:
                        df = df.iloc[:, 1:4]
                    elif len(df.columns) == 3:
                        pass
                    else:
                        continue
                    df.columns = ["username", "password", "role"]
                    df = df.dropna(subset=["username", "password"])
                    df["username"] = df["username"].str.strip().str.lower()
                    df["password"] = df["password"].str.strip()
                    df["role"]     = df["role"].str.strip().str.lower().fillna("user")
                    all_rows.append(df)
            except Exception:
                continue
        if all_rows:
            return pd.concat(all_rows, ignore_index=True).drop_duplicates(subset=["username"])
        return pd.DataFrame(columns=["username", "password", "role"])

    def authenticate(self, username: str, password: str):
        df = st.session_state.login_users_df
        if df.empty:
            return None
        username = username.strip().lower()
        row = df[df["username"] == username]
        if row.empty:
            return None
        if hmac.compare_digest(password.strip(), row.iloc[0]["password"]):
            return {"username": username, "role": row.iloc[0]["role"],
                    "login_time": datetime.now(), "last_active": datetime.now()}
        return None

    @staticmethod
    def init_session():
        for k, v in {"is_logged_in": False, "current_user": None, "login_error": None}.items():
            if k not in st.session_state:
                st.session_state[k] = v

    @staticmethod
    def is_authenticated() -> bool:
        if not st.session_state.get("is_logged_in"):
            return False
        user = st.session_state.get("current_user")
        if user is None:
            return False
        elapsed = (datetime.now() - user["last_active"]).total_seconds() / 60
        if elapsed > SESSION_TIMEOUT_MINUTES:
            LoginManager.logout()
            st.session_state["login_error"] = "⏰ Sesi telah berakhir. Silakan login ulang."
            return False
        user["last_active"] = datetime.now()
        return True

    @staticmethod
    def logout():
        st.session_state["is_logged_in"] = False
        st.session_state["current_user"] = None

    @staticmethod
    def get_current_user():
        return st.session_state.get("current_user")


def render_login_page(login_mgr: LoginManager):
    error_msg = st.session_state.get("login_error")
    inject_keep_alive()
    st.markdown('<div class="login-page">', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("# 🔍 Part Number Finder")
        st.markdown("Silakan login untuk melanjutkan.")
        st.divider()
        if error_msg:
            st.error(error_msg, icon="⚠️")
            st.session_state["login_error"] = None
        with st.form(key="login_form", clear_on_submit=True):
            username  = st.text_input("👤 Username", placeholder="Masukkan username")
            password  = st.text_input("🔑 Password", type="password", placeholder="Masukkan password")
            submitted = st.form_submit_button("Login", type="primary", use_container_width=True)
    if submitted:
        if not username or not password:
            st.session_state["login_error"] = "Username dan password tidak boleh kosong."
            st.rerun()
        user_info = login_mgr.authenticate(username, password)
        if user_info:
            st.session_state["is_logged_in"] = True
            st.session_state["current_user"] = user_info
            st.session_state["login_error"]  = None
            st.rerun()
        else:
            st.session_state["login_error"] = "Username atau password salah."
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# ── Search Functions ────────────────────────────────────────────────
def search_part_number(term, excel_files, stok_cache):
    results, seen = [], set()
    term_up = term.strip().upper()
    if not term_up:
        return results
    for fi in excel_files:
        sn = fi["simple_name"]
        if sn in seen:
            continue
        df = fi["dataframe"]
        for indexed_pn, indices in fi.get("part_number_index", {}).items():
            if term_up in indexed_pn:
                row        = df.iloc[indices[0]]
                pn_value   = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                results.append({
                    "File": sn, "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value,
                    "Part Name": str(row["part_name"]) if pd.notna(row["part_name"]) else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Excel Row": indices[0] + 2, "Full Path": fi["full_path"]
                })
                seen.add(sn)
                break
    return results


def search_part_name(term, excel_files, stok_cache):
    """
    Cari berdasarkan Part Name dengan dukungan sinonim dari data/sinonim/sinonim.json.
    Jika user ketik 'baut roda', otomatis juga cari 'wheel bolt', 'hub bolt', dst.
    """
    results = []
    term_up = term.strip().upper()
    if not term_up:
        return results

    # ── Terapkan sinonim ──
    search_keywords = apply_synonyms(term.strip().lower())

    for fi in excel_files:
        df  = fi["dataframe"]
        pni = fi.get("part_name_index", {})
        matching_indices = set()

        # Cari untuk SEMUA keyword (asli + sinonim)
        for keyword in search_keywords:
            kw_up        = keyword.upper()
            search_words = kw_up.split()
            for word in pni.keys():
                for sw in search_words:
                    if sw in word or word in sw:
                        matching_indices.update(pni[word])
            # Fallback untuk keyword pendek (≤3 huruf)
            if not matching_indices and len(kw_up) <= 3:
                for idx, row in df.iterrows():
                    pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
                    if kw_up in pname.upper():
                        matching_indices.add(idx)

        for idx in matching_indices:
            row   = df.iloc[idx]
            pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
            # Harus cocok dengan salah satu keyword
            matched = any(kw.upper() in pname.upper() for kw in search_keywords)
            if matched:
                pn_value   = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                results.append({
                    "File": fi["simple_name"], "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value, "Part Name": pname if pname else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Excel Row": idx + 2, "Full Path": fi["full_path"]
                })
    return results


# ── Build Excel Functions ───────────────────────────────────────────
def build_batch_excel(df_result: pd.DataFrame) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Batch Search Result"

    headers      = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    header_fill  = PatternFill("solid", fgColor="1565C0")
    header_font  = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_align   = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    thin         = Side(style="thin", color="BDBDBD")
    border       = Border(left=thin, right=thin, top=thin, bottom=thin)

    for col_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=h)
        cell.font = header_font; cell.fill = header_fill
        cell.alignment = center_align; cell.border = border
    ws.row_dimensions[1].height = 22

    fill_found     = PatternFill("solid", fgColor="E3F2FD")
    fill_not_found = PatternFill("solid", fgColor="FFEBEE")
    fill_alt       = PatternFill("solid", fgColor="FAFAFA")

    export_cols = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    group_start = {}
    group_end   = {}

    for row_offset, (_, r) in enumerate(df_result.iterrows()):
        excel_row = row_offset + 2
        pn_group  = r["_pn_group"]
        row_data  = [r.get(c, "") for c in export_cols]
        is_nf     = (row_data[6] == "❌ Tidak ditemukan")
        fill      = fill_not_found if is_nf else (fill_found if row_offset % 2 == 0 else fill_alt)

        if pn_group not in group_start:
            group_start[pn_group] = excel_row
        group_end[pn_group] = excel_row

        for col_idx, val in enumerate(row_data, start=1):
            cell = ws.cell(row=excel_row, column=col_idx, value=val)
            cell.fill = fill; cell.border = border
            cell.alignment = center_align if col_idx in (1, 5, 6, 7) else left_align
            cell.font = Font(name="Arial", size=10)

    for pn, start_row in group_start.items():
        end_row = group_end[pn]
        if end_row > start_row:
            ws.merge_cells(start_row=start_row, start_column=1, end_row=end_row, end_column=1)
            mc = ws.cell(row=start_row, column=1)
            mc.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    col_widths = [22, 30, 20, 40, 8, 10, 18]
    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def build_catalog_excel(df_result: pd.DataFrame, progress_callback=None) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.drawing.image import Image as XLImage
    from PIL import Image as PILImage
    import tempfile

    wb = Workbook()
    ws = wb.active
    ws.title = "Catalog"

    header_fill = PatternFill("solid", fgColor="1565C0")
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    center      = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left        = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    thin        = Side(style="thin", color="BDBDBD")
    border      = Border(left=thin, right=thin, top=thin, bottom=thin)

    headers    = ["Part Number", "Part Name", "Kecocokan", "Gambar 1", "Gambar 2"]
    col_widths = [20, 30, 45, 38, 38]
    for ci, (h, w) in enumerate(zip(headers, col_widths), start=1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.font = header_font; cell.fill = header_fill
        cell.alignment = center; cell.border = border
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[1].height = 22

    fill_even = PatternFill("solid", fgColor="E3F2FD")
    fill_odd  = PatternFill("solid", fgColor="FAFAFA")
    fill_nf   = PatternFill("solid", fgColor="FFEBEE")

    # Pertahankan urutan PN sesuai template (urutan kemunculan pertama di df_result)
    pn_order = list(dict.fromkeys(df_result["_pn_group"].tolist()))
    grouped  = {pn: {"Part Name": "", "kecocokan_list": [], "found": False} for pn in pn_order}
    for _, r in df_result.iterrows():
        pn     = r["_pn_group"]
        status = r.get("Status", "")
        hasil  = r.get("Hasil", "")
        pname  = r.get("Part Name", "")
        if status == "✅ Ditemukan" and hasil:
            grouped[pn]["kecocokan_list"].append(hasil)
            grouped[pn]["found"] = True
            if not grouped[pn]["Part Name"]:
                grouped[pn]["Part Name"] = pname
        elif not grouped[pn]["Part Name"] and pname:
            grouped[pn]["Part Name"] = pname

    def _make_xl_image(img_bytes, max_h=200):
        pil_img = PILImage.open(io.BytesIO(img_bytes)).convert("RGB")
        w_px, h_px = pil_img.size
        if h_px > max_h:
            ratio  = max_h / h_px
            w_px   = int(w_px * ratio)
            h_px   = max_h
            pil_img = pil_img.resize((w_px, h_px), PILImage.LANCZOS)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        pil_img.save(tmp.name, format="PNG")
        tmp.close()
        xl = XLImage(tmp.name)
        xl.width  = w_px
        xl.height = h_px
        return xl, w_px, h_px, tmp.name

    tmp_files = []
    row_idx   = 2
    total_pn  = len(grouped)

    for i, (pn, info) in enumerate(grouped.items()):
        if progress_callback:
            progress_callback(i, total_pn, pn)

        kecocokan  = "\n".join(info["kecocokan_list"]) if info["kecocokan_list"] else "—"
        is_found   = info["found"]
        fill       = (fill_even if i % 2 == 0 else fill_odd) if is_found else fill_nf
        row_height = 80
        img_d      = None
        img_e      = None

        if SIMS_ENABLED and is_found:
            try:
                urls, _ = _sims_fetch(pn)
                if urls:
                    b1, _ = ExcelSearchApp.fetch_image_bytes(urls[0])
                    if b1:
                        xl, w, h, tmp_path = _make_xl_image(b1)
                        img_d = xl
                        tmp_files.append(tmp_path)
                        row_height = max(int(h * 0.75) + 10, row_height)
                        hash1 = hashlib.md5(b1).hexdigest()
                        for url2 in urls[1:]:
                            b2, _ = ExcelSearchApp.fetch_image_bytes(url2)
                            if b2 and hashlib.md5(b2).hexdigest() != hash1:
                                xl, w, h, tmp_path = _make_xl_image(b2)
                                img_e = xl
                                tmp_files.append(tmp_path)
                                row_height = max(int(h * 0.75) + 10, row_height)
                                break
            except Exception as e:
                print(f"[catalog] Gagal ambil gambar {pn}: {e}")

        ws.row_dimensions[row_idx].height = row_height

        for ci, (val, aln) in enumerate(
            [(pn, center), (info["Part Name"], left), (kecocokan, left)], start=1
        ):
            cell = ws.cell(row=row_idx, column=ci, value=val)
            cell.fill = fill; cell.border = border
            cell.alignment = aln; cell.font = Font(name="Arial", size=10)

        for ci in (4, 5):
            c = ws.cell(row=row_idx, column=ci, value="")
            c.fill = fill; c.border = border; c.alignment = center

        if img_d:
            ws.add_image(img_d, f"D{row_idx}")
        if img_e:
            ws.add_image(img_e, f"E{row_idx}")

        row_idx += 1

    ws.freeze_panes = "A2"
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    result = buf.getvalue()

    for f in tmp_files:
        try:
            os.unlink(f)
        except Exception:
            pass

    return result


def make_template_excel() -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    wb = Workbook()
    ws = wb.active
    ws.title = "Part Number List"
    ws["A1"] = "Part Number"
    ws["A1"].font      = Font(bold=True, name="Arial", size=11, color="FFFFFF")
    ws["A1"].fill      = PatternFill("solid", fgColor="1565C0")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 20
    for i, ex in enumerate(["WG1642821034", "WG9925520270", "AZ9100443082", "WG9718820030"], start=2):
        ws.cell(row=i, column=1, value=ex).font = Font(name="Arial", size=10)
    ws.column_dimensions["A"].width = 28
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf.getvalue()


# ── Main App ────────────────────────────────────────────────────────
class ExcelSearchApp:
    def __init__(self):
        self.data_folder   = DATA_FOLDER
        self.cache_folder  = CACHE_FOLDER
        self.images_folder = IMAGES_FOLDER
        self.supported_ext = [".jpg", ".jpeg", ".png"]
        self.cache_folder.mkdir(exist_ok=True)
        self.images_folder.mkdir(exist_ok=True)
        self.stok_file       = DATA_FOLDER / "stok" / "stok.xlsx"
        self.stok_cache      = None
        self.threshold_file  = DATA_FOLDER / "stok" / "threshold.xlsx"
        self.threshold_cache = None
        self.populasi_folder = DATA_FOLDER / "populasi"
        self._load_stok_data()
        self._load_threshold_data()

        if "excel_files" not in st.session_state:
            st.session_state.excel_files        = []
            st.session_state.index_data         = []
            st.session_state.last_index_time    = None
            st.session_state.search_results     = []
            st.session_state.loaded_files_count = 0
            st.session_state.last_file_count    = 0
            st.session_state.file_hashes        = {}

        if not st.session_state.excel_files:
            self.auto_load_excel_files()

    def create_data_folder(self):
        if not self.data_folder.exists():
            self.data_folder.mkdir(parents=True)

    def get_file_hash(self, fp):
        try:
            s = fp.stat()
            return hashlib.md5(f"{fp}_{s.st_size}_{s.st_mtime}".encode()).hexdigest()
        except Exception:
            return None

    def load_file_cache(self, fp, fh):
        cf = self.cache_folder / f"{fh}.pkl"
        if cf.exists():
            try:
                with open(cf, "rb") as f:
                    return pickle.load(f)
            except Exception:
                return None
        return None

    def save_file_cache(self, fp, fh, data):
        try:
            with open(self.cache_folder / f"{fh}.pkl", "wb") as f:
                pickle.dump(data, f)
        except Exception:
            pass

    @staticmethod
    def extract_simple_filename(filename):
        name = os.path.splitext(filename)[0]
        return name.split(" - ")[-1] if " - " in name else name

    def normalize_base_part_number(self, pn):
        if not pn or pd.isna(pn):
            return ""
        pn_str = str(pn).strip().upper()
        base   = pn_str.split("/", 1)[0].strip()
        return re.sub(r'[^A-Z0-9\-]', '_', base)

    def get_image_path(self, pn):
        base = self.normalize_base_part_number(pn)
        if not base:
            return None
        sub_folder = self.images_folder / base
        if sub_folder.exists() and sub_folder.is_dir():
            for ext in self.supported_ext:
                candidates = sorted(sub_folder.glob(f"*{ext}"))
                if candidates:
                    return candidates[0]
        for ext in self.supported_ext:
            p = self.images_folder / f"{base}{ext}"
            if p.exists():
                return p
        return None

    def get_all_image_paths(self, pn):
        base = self.normalize_base_part_number(pn)
        if not base:
            return []
        paths      = []
        sub_folder = self.images_folder / base
        if sub_folder.exists() and sub_folder.is_dir():
            for ext in self.supported_ext:
                paths.extend(sorted(sub_folder.glob(f"*{ext}")))
        for ext in self.supported_ext:
            p = self.images_folder / f"{base}{ext}"
            if p.exists() and p not in paths:
                paths.append(p)
        return paths

    @staticmethod
    def render_zoomable_image(img_bytes: bytes, caption: str = "", zoom_key: str = "zoom_default"):
        import base64
        zk = f"zoom_scale_{zoom_key}"
        if zk not in st.session_state:
            st.session_state[zk] = 100

        scale = st.session_state[zk]
        c1, c2, c3, c4 = st.columns([1, 1, 1, 3])
        with c1:
            if st.button("🔍＋", key=f"zi_{zoom_key}", help="Zoom In", use_container_width=True):
                st.session_state[zk] = min(scale + 25, 300)
                st.rerun()
        with c2:
            if st.button("🔍－", key=f"zo_{zoom_key}", help="Zoom Out", use_container_width=True):
                st.session_state[zk] = max(scale - 25, 25)
                st.rerun()
        with c3:
            if st.button("⟳", key=f"zr_{zoom_key}", help="Reset zoom", use_container_width=True):
                st.session_state[zk] = 100
                st.rerun()
        with c4:
            st.markdown(
                f"<div style='padding:6px 0;color:#555;font-size:.85rem;'>Zoom: <b>{st.session_state[zk]}%</b></div>",
                unsafe_allow_html=True
            )

        b64  = base64.b64encode(img_bytes).decode()
        sig  = img_bytes[:4]
        mime = "image/jpeg"
        if sig[:4] == b'\x89PNG':
            mime = "image/png"
        elif sig[:3] == b'GIF':
            mime = "image/gif"

        cur_scale    = st.session_state[zk]
        safe_caption = caption.replace("<", "&lt;").replace(">", "&gt;")
        st.markdown(f"""
<div style="overflow:auto; width:100%; text-align:center; padding:4px 0;">
  <img src="data:{mime};base64,{b64}"
       style="width:{cur_scale}%; max-width:none; transform-origin:top center;
              border-radius:8px; box-shadow:0 2px 12px rgba(0,0,0,.18); transition:width .2s ease;"
       title="{safe_caption}" />
  <div style="font-size:.78rem;color:#666;margin-top:4px;">{safe_caption}</div>
</div>""", unsafe_allow_html=True)

    @staticmethod
    def fetch_image_bytes(url: str):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            if SIMS_ENABLED:
                try:
                    from sims_fetcher import _get_token, SIMS_BASE_URL
                    sims_host = SIMS_BASE_URL.replace("http://", "").replace("https://", "").split("/")[0]
                    if sims_host in url or "simscloud" in url or "cnhtcerp" in url:
                        headers["Authorization"] = _get_token()
                        headers["Referer"]       = SIMS_BASE_URL + "/"
                        headers["Origin"]        = SIMS_BASE_URL
                        headers["language"]      = "en"
                except Exception as e:
                    print(f"[debug] Gagal ambil token SIMS: {e}")

            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                if any(t in content_type for t in ("image", "octet-stream", "jpeg", "png", "gif", "webp")):
                    return resp.content, None
                if len(resp.content) > 1000:
                    return resp.content, None
                return None, f"Konten bukan gambar (Content-Type: {content_type})"
            return None, f"HTTP {resp.status_code}"
        except requests.exceptions.ConnectionError:
            return None, "Tidak dapat terhubung ke server"
        except requests.exceptions.Timeout:
            return None, "Timeout: server tidak merespons"
        except Exception as e:
            return None, str(e)

    def _load_stok_data(self):
        if self.stok_cache is not None:
            return
        if "stok_data" in st.session_state:
            self.stok_cache = st.session_state.stok_data
            return
        if not self.stok_file.exists():
            self.stok_cache = {}
            st.session_state.stok_data = self.stok_cache
            return
        try:
            df_stok = pd.read_excel(self.stok_file, usecols=[0, 3], header=None, dtype=str)
            if len(df_stok) > 0 and any(str(x).lower() in ["part number","kode","no part"] for x in df_stok.iloc[0]):
                df_stok = df_stok.iloc[1:]
            df_stok.columns = ["part_number","stok"]
            df_stok["part_number"] = df_stok["part_number"].astype(str).str.strip().str.upper()
            df_stok = df_stok.dropna(subset=["part_number"])
            self.stok_cache = dict(zip(df_stok["part_number"], df_stok["stok"].fillna("—")))
            st.session_state.stok_data = self.stok_cache
        except Exception as e:
            st.error(f"Gagal membaca stok.xlsx → {e}")
            self.stok_cache = {}
            st.session_state.stok_data = self.stok_cache

    def _load_threshold_data(self):
        if self.threshold_cache is not None:
            return
        if "threshold_data" in st.session_state:
            self.threshold_cache = st.session_state.threshold_data
            return
        if not self.threshold_file.exists():
            self.threshold_cache = {}
            st.session_state.threshold_data = self.threshold_cache
            return
        try:
            df_t = pd.read_excel(self.threshold_file, usecols=[0,1], header=None, dtype=str)
            if len(df_t) > 0 and any(str(x).lower() in ["part number","kode","no part","threshold"] for x in df_t.iloc[0]):
                df_t = df_t.iloc[1:]
            df_t.columns = ["part_number","threshold"]
            df_t["part_number"] = df_t["part_number"].astype(str).str.strip().str.upper()
            df_t = df_t.dropna(subset=["part_number"])
            self.threshold_cache = dict(zip(df_t["part_number"], df_t["threshold"].fillna("0")))
            st.session_state.threshold_data = self.threshold_cache
        except Exception as e:
            st.error(f"Gagal membaca threshold.xlsx → {e}")
            self.threshold_cache = {}
            st.session_state.threshold_data = self.threshold_cache

    def get_threshold_alerts(self):
        results = []
        if not self.stok_cache or not self.threshold_cache:
            return results
        for pn, thr_str in self.threshold_cache.items():
            try:
                thr = float(thr_str)
            except (ValueError, TypeError):
                continue
            try:
                stok = float(self.stok_cache.get(pn, "0"))
            except (ValueError, TypeError):
                continue
            if stok < thr:
                pname = "N/A"
                for fi in st.session_state.excel_files:
                    pn_idx = fi.get("part_number_index", {})
                    if pn in pn_idx and pn_idx[pn]:
                        row   = fi["dataframe"].iloc[pn_idx[pn][0]]
                        pname = str(row["part_name"]) if pd.notna(row["part_name"]) else "N/A"
                        break
                results.append({"Part Number": pn, "Part Name": pname,
                                 "Stok Saat Ini": int(stok), "Minimal Stok": int(thr),
                                 "Qty": int(thr - stok)})
        return results

    def process_single_file(self, file_path, relative_path):
        results     = []
        file_name   = file_path.name
        simple_name = self.extract_simple_filename(file_name)
        file_hash   = self.get_file_hash(file_path)
        if file_hash:
            cached = self.load_file_cache(file_path, file_hash)
            if cached:
                return cached
        try:
            xls = pd.ExcelFile(file_path, engine="openpyxl")
            for sheet_name in xls.sheet_names:
                try:
                    df = pd.read_excel(xls, sheet_name=sheet_name, usecols=[1,3,4], dtype=str)
                    df.columns = ["part_number","part_name","quantity"]
                    pn_idx, nm_idx = {}, {}
                    for idx, row in df.iterrows():
                        pn = str(row["part_number"]).strip().upper() if pd.notna(row["part_number"]) else ""
                        nm = str(row["part_name"]).strip().upper()   if pd.notna(row["part_name"])   else ""
                        if pn:
                            pn_idx.setdefault(pn, []).append(idx)
                        if nm:
                            for word in nm.split():
                                if len(word) > 2:
                                    nm_idx.setdefault(word, []).append(idx)
                    results.append({
                        "full_path": str(file_path), "file_name": file_name,
                        "relative_path": str(relative_path), "simple_name": simple_name,
                        "sheet": sheet_name, "dataframe": df, "row_count": len(df),
                        "col_count": len(df.columns), "part_number_index": pn_idx,
                        "part_name_index": nm_idx,
                        "last_modified": datetime.fromtimestamp(file_path.stat().st_mtime),
                    })
                except Exception:
                    continue
        except Exception:
            pass
        if file_hash and results:
            self.save_file_cache(file_path, file_hash, results)
        return results

    def auto_load_excel_files(self):
        try:
            self.create_data_folder()
            excel_ext = (".xlsx", ".xls", ".xlsm")
            all_files = []
            for root, _, files in os.walk(self.data_folder):
                for f in files:
                    if f.lower().endswith(excel_ext):
                        fp = Path(root) / f
                        all_files.append((fp, fp.relative_to(self.data_folder)))

            if not all_files:
                return

            results = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(self.process_single_file, fp, rp): fp
                           for fp, rp in all_files}
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        if res:
                            results.extend(res)
                    except Exception:
                        pass

            st.session_state.excel_files        = results
            st.session_state.last_index_time    = datetime.now()
            st.session_state.loaded_files_count = len(results)
            st.session_state.last_file_count    = len(all_files)
        except Exception as e:
            st.error(f"Error loading Excel files: {e}")

    # ── Batch Download Tab ───────────────────────────────────────────
    def render_batch_download_tab(self):
        st.markdown("### 📥 Batch Download")
        st.markdown("""
<div class="batch-info-box">
Upload file Excel berisi daftar Part Number (1 kolom, mulai baris 1 atau 2).<br>
Sistem akan mencari semua PN secara otomatis dan menghasilkan file katalog.
</div>
""", unsafe_allow_html=True)

        st.download_button(
            label="📄 Download Template Input",
            data=make_template_excel(),
            file_name="template_batch_input.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.divider()

        uploaded = st.file_uploader(
            "📂 Upload file Part Number:",
            type=["xlsx", "xls", "xlsm", "csv"],
            key="batch_upload",
        )

        if uploaded is None:
            return

        try:
            if uploaded.name.endswith(".csv"):
                df_input = pd.read_csv(uploaded, header=None, dtype=str)
            else:
                df_input = pd.read_excel(uploaded, header=None, dtype=str)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            return

        col_a = df_input.iloc[:, 0].dropna().astype(str).str.strip()
        if col_a.iloc[0].lower() in ("part number","part_number","partnumber","no part","kode"):
            col_a = col_a.iloc[1:]

        part_numbers = col_a[col_a.str.len() > 0].tolist()

        if not part_numbers:
            st.warning("Tidak ada Part Number yang valid dalam file.")
            return

        st.info(f"📊 **{len(part_numbers)}** Part Number ditemukan dalam file input.")

        with st.expander("👁️ Preview Part Number"):
            st.dataframe(pd.DataFrame({"Part Number": part_numbers}), hide_index=True, height=200)

        if st.button("🔍 Proses Batch Search", type="primary", use_container_width=True, key="batch_process_btn"):
            if not st.session_state.excel_files:
                st.error("Tidak ada file Excel yang ter-index di folder data/.")
                st.stop()

            prog        = st.progress(0)
            status_txt  = st.empty()
            total       = len(part_numbers)
            results_all = []

            for i, pn in enumerate(part_numbers):
                status_txt.text(f"🔍 Mencari {i+1}/{total}: {pn}")
                prog.progress((i + 1) / total)
                found = search_part_number(pn, st.session_state.excel_files, self.stok_cache)
                if found:
                    first = True
                    for r in found:
                        results_all.append({
                            "Part Number": pn if first else "",
                            "_pn_group":   pn,
                            "Hasil":       r["File"],
                            "Sheet":       r["Sheet"],
                            "Part Name":   r["Part Name"],
                            "Qty":         r["Quantity"],
                            "Stok":        r["Stok"],
                            "Status":      "✅ Ditemukan",
                        })
                        first = False
                else:
                    results_all.append({
                        "Part Number": pn, "_pn_group": pn,
                        "Hasil": "", "Sheet": "", "Part Name": "",
                        "Qty": "", "Stok": "", "Status": "❌ Tidak ditemukan",
                    })

            prog.empty()
            status_txt.empty()
            df_result = pd.DataFrame(results_all)

            prog_cat   = st.progress(0)
            status_cat = st.empty()

            def _prog(i, tot, pn):
                prog_cat.progress((i + 1) / max(tot, 1))
                status_cat.text(f"🖼️ Fetch gambar {i+1}/{tot}: {pn}")

            try:
                cat_bytes = build_catalog_excel(df_result, progress_callback=_prog)
                st.session_state["batch_catalog_bytes"]     = cat_bytes
                st.session_state["batch_catalog_df"]        = df_result
                st.session_state["batch_catalog_timestamp"] = datetime.now().strftime("%Y%m%d_%H%M%S")
            except Exception as e:
                st.error(f"❌ Gagal membuat katalog: {e}")
            finally:
                prog_cat.empty()
                status_cat.empty()

            st.rerun()

        if "batch_catalog_df" not in st.session_state:
            return

        df_result = st.session_state["batch_catalog_df"]
        found_pn  = df_result[df_result["Status"] == "✅ Ditemukan"]["_pn_group"].nunique()
        not_found = df_result["_pn_group"].nunique() - found_pn

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Part Number", df_result["_pn_group"].nunique())
        c2.metric("✅ Ditemukan", found_pn)
        c3.metric("❌ Tidak Ditemukan", not_found)

        st.markdown("#### 📋 Preview Hasil")
        disp_cols = ["Part Number","Hasil","Sheet","Part Name","Qty","Stok","Status"]
        st.dataframe(
            df_result[disp_cols], hide_index=True,
            column_config={
                "Part Number": st.column_config.TextColumn(width="medium"),
                "Hasil":       st.column_config.TextColumn(width="medium"),
                "Sheet":       st.column_config.TextColumn(width="medium"),
                "Part Name":   st.column_config.TextColumn(width="large"),
                "Qty":         st.column_config.TextColumn(width="small"),
                "Stok":        st.column_config.TextColumn(width="small"),
                "Status":      st.column_config.TextColumn(width="medium"),
            }
        )

        if "batch_catalog_bytes" in st.session_state:
            ts = st.session_state.get("batch_catalog_timestamp", "result")
            st.download_button(
                label="⬇️ Download Hasil (.xlsx)",
                data=st.session_state["batch_catalog_bytes"],
                file_name=f"catalog_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )

    # ── Sinonim Tab (Admin) ──────────────────────────────────────────
    def render_sinonim_tab(self):
        """Tab admin untuk melihat dan mengedit kamus sinonim dari data/sinonim/sinonim.json."""
        st.markdown("### 🔤 Kelola Kamus Sinonim")
        st.markdown(
            "Sinonim memungkinkan user mencari dengan kata bahasa Indonesia/informal "
            "dan tetap menemukan part dengan nama teknis di database."
        )
        st.markdown(f"📁 File: `{SINONIM_FILE}`")
        st.divider()

        # ── Reload cache sinonim ─────────────────────────────────────
        col_r, _ = st.columns([1, 4])
        with col_r:
            if st.button("🔄 Reload Sinonim", key="reload_sinonim"):
                st.session_state.pop("_synonym_map_cache", None)
                st.rerun()

        synonym_map = load_synonym_map()

        if not synonym_map:
            st.warning(f"File sinonim tidak ditemukan atau kosong: `{SINONIM_FILE}`")
        else:
            st.success(f"✅ {len(synonym_map)} grup sinonim aktif.")

            # ── Tampilkan tabel ringkasan ─────────────────────────────
            rows = []
            for g in synonym_map:
                rows.append({
                    "Grup"    : g.get("grup", "-"),
                    "Triggers (kata pencarian)": ", ".join(g["triggers"]),
                    "Keywords (dicari di data)": ", ".join(g["keywords"]),
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True, height=400)

        st.divider()

        # ── Editor JSON langsung ──────────────────────────────────────
        st.markdown("#### ✏️ Edit File Sinonim (JSON)")
        st.markdown(
            "Edit langsung isi file JSON di bawah. "
            "Setiap grup harus memiliki `triggers` (pemicu) dan `keywords` (kata yang dicari di data)."
        )

        # Baca konten file saat ini
        current_json = ""
        if SINONIM_FILE.exists():
            try:
                current_json = SINONIM_FILE.read_text(encoding="utf-8")
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")

        edited_json = st.text_area(
            "Isi sinonim.json:",
            value=current_json,
            height=450,
            key="sinonim_editor",
        )

        col_save, col_fmt = st.columns([1, 1])
        with col_fmt:
            if st.button("🔍 Validasi JSON", use_container_width=True, key="validate_json"):
                try:
                    parsed = json.loads(edited_json)
                    st.success(f"✅ JSON valid! ({len(parsed)} grup ditemukan)")
                except json.JSONDecodeError as e:
                    st.error(f"❌ JSON tidak valid: {e}")

        with col_save:
            if st.button("💾 Simpan Sinonim", type="primary", use_container_width=True, key="save_sinonim"):
                try:
                    parsed = json.loads(edited_json)
                    SINONIM_FILE.parent.mkdir(parents=True, exist_ok=True)
                    SINONIM_FILE.write_text(
                        json.dumps(parsed, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )
                    # Hapus cache agar langsung terbaca ulang
                    st.session_state.pop("_synonym_map_cache", None)
                    st.success("✅ Sinonim berhasil disimpan dan di-reload!")
                    st.rerun()
                except json.JSONDecodeError as e:
                    st.error(f"❌ Gagal menyimpan — JSON tidak valid: {e}")
                except Exception as e:
                    st.error(f"❌ Gagal menyimpan file: {e}")

        st.markdown("---")
        st.markdown("#### 📖 Panduan Format JSON")
        st.code("""[
  {
    "grup": "Nama Grup (opsional, untuk label)",
    "triggers": ["kata pencarian 1", "kata pencarian 2", "dst"],
    "keywords": ["keyword di data 1", "keyword di data 2", "dst"]
  },
  {
    "grup": "Baut Roda",
    "triggers": ["baut roda", "mur roda", "baut velg"],
    "keywords": ["wheel bolt", "hub bolt", "wheel nut"]
  }
]""", language="json")

    # ── SIDEBAR & DASHBOARD ──────────────────────────────────────────
    def display_dashboard(self):
        user = LoginManager.get_current_user()
        role = user["role"] if user else "user"
        inject_keep_alive()
        st.markdown('<h1 class="main-header">🔍 Part Number Finder</h1>', unsafe_allow_html=True)

        with st.sidebar:
            badge_cls = "role-admin" if role == "admin" else "role-user"
            st.markdown(
                f'<div class="user-badge">👤 {user["username"].title()}' +
                f' — <span class="{badge_cls}">{role.upper()}</span></div>',
                unsafe_allow_html=True
            )
            st.caption(f"Login pukul {user['login_time'].strftime('%H:%M')} · Timeout {SESSION_TIMEOUT_MINUTES} min")

            if st.button("🚪 Logout", type="secondary", use_container_width=True):
                LoginManager.logout()
                for k in ("excel_files","index_data","search_results",
                          "last_index_time","loaded_files_count","last_file_count"):
                    st.session_state.pop(k, None)
                st.rerun()
            st.divider()

            if role == "admin":
                st.markdown("### 🛡️ Admin Panel")
                if st.button("👥 Reload Users", type="secondary", use_container_width=True):
                    st.session_state.login_users_df = LoginManager._load_users()
                    st.toast("✅ Data user telah di-reload!")
                df_users = st.session_state.get("login_users_df", pd.DataFrame())
                if not df_users.empty:
                    with st.expander("📋 Daftar User"):
                        st.dataframe(df_users[["username","role"]].rename(
                            columns={"username":"Username","role":"Role"}),
                            hide_index=True)
                st.divider()

            st.markdown("### 📊 Status Sistem")
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                for cf in CACHE_FOLDER.glob("*.pkl"):
                    try: cf.unlink()
                    except Exception: pass
                for k in ("excel_files","last_index_time","last_file_count","stok_data",
                          "threshold_data","_synonym_map_cache"):
                    st.session_state.pop(k, None)
                self.stok_cache = None; self.threshold_cache = None
                self._load_stok_data(); self._load_threshold_data()
                self.auto_load_excel_files()
                st.rerun()

            if st.session_state.get("last_index_time"):
                st.markdown(f"**Terakhir di-index:**\n`{st.session_state.last_index_time.strftime('%Y-%m-%d %H:%M:%S')}`")
            st.divider()
            st.markdown("### 📈 Statistik")
            st.metric("File Excel", st.session_state.get("loaded_files_count", 0))

            # Info sinonim aktif di sidebar
            syn_map = load_synonym_map()
            st.metric("Grup Sinonim", len(syn_map))
            st.divider()

            st.markdown("### 📁 Struktur Folder")
            st.info(f"File Excel dibaca dari:\n```\n{self.data_folder.absolute()}\n```")

            with st.expander("📖 Panduan Cepat"):
                st.markdown("""
1. Letakkan file Excel di folder `data/`
2. **Part Number** → kolom B | **Part Name** → kolom D
3. **Stok:** data/stok/stok.xlsx (Kol A=PN, Kol D=Stok)
4. **Threshold (Admin):** data/stok/threshold.xlsx
5. **Batch Download:** Upload Excel berisi PN di Kol A
6. **Sinonim:** Edit di `data/sinonim/sinonim.json`
                """)

        # ── TABS ────────────────────────────────────────────────────
        st.markdown(TAB_PERSIST_JS, unsafe_allow_html=True)
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        if role == "admin":
            tab1, tab2, tab4, tab5, tab_admin_img, tab_sinonim = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "📥 Batch Download", "🚛 Populasi Unit",
                "🖼️ Kelola Foto", "🔤 Kelola Sinonim"])
        else:
            tab1, tab2, tab4, tab5 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "📥 Batch Download", "🚛 Populasi Unit"])
            tab_admin_img = None
            tab_sinonim   = None

        with tab1:
            with st.form(key="search_pn_form", clear_on_submit=False):
                sn_input = st.text_input(
                    "Masukkan Part Number:",
                    placeholder="Contoh: WG1642821034/1",
                    key="sn_input"
                )
                if st.form_submit_button("🔍 Cari Part Number", type="primary", use_container_width=True):
                    if sn_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_number(
                                sn_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Number"
                            st.session_state.search_term = sn_input
                            st.rerun()
                    else:
                        st.warning("Masukkan part number untuk mencari.")

        with tab2:
            with st.form(key="search_name_form", clear_on_submit=False):
                name_input = st.text_input(
                    "Masukkan Part Name:",
                    placeholder="Contoh: baut roda, bearing, kampas rem",
                    key="name_input"
                )
                if st.form_submit_button("🔍 Cari Part Name", type="primary", use_container_width=True):
                    if name_input:
                        # Tampilkan info sinonim yang akan digunakan
                        synonyms_used = apply_synonyms(name_input.strip().lower())
                        if len(synonyms_used) > 1:
                            st.session_state["last_synonyms_used"] = synonyms_used[1:]
                        else:
                            st.session_state["last_synonyms_used"] = []
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_name(
                                name_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Name"
                            st.session_state.search_term = name_input
                            st.rerun()
                    else:
                        st.warning("Masukkan nama part untuk mencari.")

            # Tampilkan sinonim yang digunakan (di luar form)
            syns = st.session_state.get("last_synonyms_used", [])
            if syns and st.session_state.get("search_type") == "Part Name":
                st.markdown(
                    f'<div class="synonym-info">🔄 <b>Sinonim yang dicari:</b> '
                    f'{", ".join(f"<code>{s}</code>" for s in syns)}</div>',
                    unsafe_allow_html=True
                )

        if tab_admin_img is not None:
            with tab_admin_img:
                self.render_admin_image_tab()

        if tab_sinonim is not None:
            with tab_sinonim:
                self.render_sinonim_tab()

        with tab4:
            self.render_batch_download_tab()

        with tab5:
            self.render_populasi_tab()

        st.markdown("</div>", unsafe_allow_html=True)
        self.display_search_results()

    def display_search_results(self):
        results = st.session_state.get("search_results", [])
        if results:
            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>',
                        unsafe_allow_html=True)
            df_res = pd.DataFrame(results)
            cols   = [c for c in ["File","Part Number","Part Name","Quantity","Stok","Sheet","Excel Row"]
                      if c in df_res.columns]
            st.dataframe(df_res[cols], hide_index=True,
                         column_config={
                             "File":        st.column_config.TextColumn(width="medium"),
                             "Part Number": st.column_config.TextColumn(width="medium"),
                             "Part Name":   st.column_config.TextColumn(width="large"),
                             "Quantity":    st.column_config.NumberColumn(width="small"),
                             "Stok":        st.column_config.TextColumn(width="small"),
                             "Sheet":       st.column_config.TextColumn(width="medium"),
                             "Excel Row":   st.column_config.NumberColumn(width="small"),
                         })
            if st.session_state.get("search_type") == "Part Number":
                st.markdown("### 🖼️ Gambar Part")
                for pn in df_res["Part Number"].dropna().unique():
                    rows     = df_res[df_res["Part Number"] == pn]
                    pname_ex = rows.iloc[0]["Part Name"] if not rows.empty else "N/A"

                    sims_key     = f"sims_fetched_{pn}"
                    sims_err_key = f"sims_err_{pn}"

                    if sims_key not in st.session_state:
                        if SIMS_ENABLED:
                            with st.spinner(f"🔍 Mengambil gambar dari SIMS untuk {pn}..."):
                                fetched_urls, fetch_err = _sims_fetch(pn)
                            st.session_state[sims_key]     = fetched_urls
                            st.session_state[sims_err_key] = fetch_err
                        else:
                            st.session_state[sims_key]     = []
                            st.session_state[sims_err_key] = "SIMS tidak aktif"

                    img_links = st.session_state[sims_key]
                    img_path  = self.get_image_path(pn)
                    if img_path and not img_path.exists():
                        img_path = None

                    with st.expander(f"🖼️ {pn}", expanded=True):
                        if SIMS_ENABLED:
                            col_ref, _ = st.columns([1, 4])
                            with col_ref:
                                if st.button("🔄 Refresh dari SIMS", key=f"sims_refresh_{pn}"):
                                    st.session_state.pop(sims_key, None)
                                    st.session_state.pop(sims_err_key, None)
                                    st.rerun()
                            sims_err = st.session_state.get(sims_err_key)
                            if sims_err and not img_links and not img_path:
                                st.warning(f"⚠️ SIMS: {sims_err}")

                        if img_links:
                            idx_key = f"img_idx_{pn}"
                            if idx_key not in st.session_state:
                                st.session_state[idx_key] = 0

                            total       = len(img_links)
                            current_idx = st.session_state[idx_key]

                            if total > 1:
                                col_prev, col_info, col_next = st.columns([1, 3, 1])
                                with col_prev:
                                    if st.button("◀ Prev", key=f"prev_{pn}", disabled=(current_idx == 0)):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"next_{pn}", disabled=(current_idx == total - 1)):
                                        st.session_state[idx_key] = min(total - 1, current_idx + 1)
                                        st.rerun()

                            active_url = img_links[current_idx]
                            with st.spinner("Memuat gambar..."):
                                img_bytes, err = ExcelSearchApp.fetch_image_bytes(active_url)
                            if img_bytes:
                                try:
                                    _, col_img, _ = st.columns([1, 2, 1])
                                    with col_img:
                                        ExcelSearchApp.render_zoomable_image(
                                            img_bytes,
                                            caption=f"{pn} - {pname_ex}  (Gambar {current_idx + 1}/{total})",
                                            zoom_key=f"{pn}_{current_idx}"
                                        )
                                except Exception as e:
                                    st.error(f"⚠️ Gambar berhasil diunduh tapi gagal ditampilkan: {e}")
                                    st.caption(f"URL: {active_url}")
                            else:
                                st.warning(f"⚠️ Gagal memuat gambar: {err}")
                                st.caption(f"URL: {active_url}")

                            if total > 1:
                                st.markdown("**Pilih gambar:**")
                                thumb_cols = st.columns(min(total, 5))
                                for ti, (tc, lnk) in enumerate(zip(thumb_cols, img_links)):
                                    with tc:
                                        label = f"{'✅' if ti == current_idx else '🔲'} {ti+1}"
                                        if st.button(label, key=f"thumb_{pn}_{ti}"):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            local_paths   = self.get_all_image_paths(pn)
                            if not local_paths:
                                local_paths = [img_path]
                            local_idx_key = f"local_img_idx_{pn}"
                            if local_idx_key not in st.session_state:
                                st.session_state[local_idx_key] = 0
                            local_total = len(local_paths)
                            local_cur   = min(st.session_state[local_idx_key], local_total - 1)
                            if local_total > 1:
                                col_p, col_i, col_n = st.columns([1, 3, 1])
                                with col_p:
                                    if st.button("◀ Prev", key=f"loc_prev_{pn}", disabled=(local_cur == 0)):
                                        st.session_state[local_idx_key] = max(0, local_cur - 1)
                                        st.rerun()
                                with col_i:
                                    st.markdown(f"<div style='text-align:center;padding:6px 0;font-weight:600;color:#1565C0;'>Foto {local_cur+1} / {local_total}</div>", unsafe_allow_html=True)
                                with col_n:
                                    if st.button("Next ▶", key=f"loc_next_{pn}", disabled=(local_cur == local_total - 1)):
                                        st.session_state[local_idx_key] = min(local_total - 1, local_cur + 1)
                                        st.rerun()
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = local_paths[local_cur].read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=f"{pn} - {pname_ex} (Foto {local_cur+1}/{local_total})", zoom_key=f"{pn}_local_{local_cur}")
                            if local_total > 1:
                                st.markdown("**Pilih foto:**")
                                thumb_cols = st.columns(min(local_total, 5))
                                for ti, (tc, lp) in enumerate(zip(thumb_cols, local_paths)):
                                    with tc:
                                        lbl = f"{'✅' if ti == local_cur else '🔲'} {ti+1}"
                                        if st.button(lbl, key=f"loc_thumb_{pn}_{ti}"):
                                            st.session_state[local_idx_key] = ti
                                            st.rerun()
                        else:
                            if SIMS_ENABLED and st.session_state.get(f"sims_fetched_{pn}") is not None:
                                st.caption("📷 Tidak ada gambar di SIMS untuk part ini")
                            else:
                                st.caption("Tidak ada gambar tersedia")

        elif "search_term" in st.session_state and st.session_state.get("search_results") is not None:
            search_term = st.session_state.search_term
            st.warning(f"❌ Tidak ditemukan hasil untuk '{search_term}'")

            if st.session_state.get("search_type") == "Part Number":
                sims_key     = f"sims_fetched_{search_term}"
                sims_err_key = f"sims_err_{search_term}"

                if sims_key not in st.session_state:
                    if SIMS_ENABLED:
                        with st.spinner(f"🔍 Mengambil gambar dari SIMS untuk {search_term}..."):
                            fetched_urls, fetch_err = _sims_fetch(search_term)
                        st.session_state[sims_key]     = fetched_urls
                        st.session_state[sims_err_key] = fetch_err
                    else:
                        st.session_state[sims_key]     = []
                        st.session_state[sims_err_key] = "SIMS tidak aktif"

                img_links = st.session_state[sims_key]
                img_path  = self.get_image_path(search_term)
                if img_path and not img_path.exists():
                    img_path = None

                st.markdown("### 🖼️ Gambar Part")
                with st.expander(f"🖼️ {search_term}", expanded=True):
                    if SIMS_ENABLED:
                        col_ref, _ = st.columns([1, 4])
                        with col_ref:
                            if st.button("🔄 Refresh dari SIMS", key=f"nf_sims_refresh_{search_term}"):
                                st.session_state.pop(sims_key, None)
                                st.session_state.pop(sims_err_key, None)
                                st.rerun()

                    if img_links or img_path:
                        if img_links:
                            idx_key = f"img_idx_{search_term}"
                            if idx_key not in st.session_state:
                                st.session_state[idx_key] = 0

                            total       = len(img_links)
                            current_idx = st.session_state[idx_key]

                            if total > 1:
                                col_prev, col_info, col_next = st.columns([1, 3, 1])
                                with col_prev:
                                    if st.button("◀ Prev", key=f"nf_prev_{search_term}", disabled=(current_idx == 0)):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"nf_next_{search_term}", disabled=(current_idx == total - 1)):
                                        st.session_state[idx_key] = min(total - 1, current_idx + 1)
                                        st.rerun()

                            active_url = img_links[current_idx]
                            with st.spinner("Memuat gambar..."):
                                img_bytes, err = ExcelSearchApp.fetch_image_bytes(active_url)
                            if img_bytes:
                                try:
                                    _, col_img, _ = st.columns([1, 2, 1])
                                    with col_img:
                                        ExcelSearchApp.render_zoomable_image(
                                            img_bytes,
                                            caption=f"{search_term}  (Gambar {current_idx + 1}/{total})",
                                            zoom_key=f"nf_{search_term}_{current_idx}"
                                        )
                                except Exception as e:
                                    st.error(f"⚠️ Gambar berhasil diunduh tapi gagal ditampilkan: {e}")
                                    st.caption(f"URL: {active_url}")
                            else:
                                st.warning(f"⚠️ Gagal memuat gambar: {err}")
                                st.caption(f"URL: {active_url}")

                            if total > 1:
                                st.markdown("**Pilih gambar:**")
                                thumb_cols = st.columns(min(total, 5))
                                for ti, (tc, lnk) in enumerate(zip(thumb_cols, img_links)):
                                    with tc:
                                        label = f"{'✅' if ti == current_idx else '🔲'} {ti+1}"
                                        if st.button(label, key=f"nf_thumb_{search_term}_{ti}"):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            local_paths_nf = self.get_all_image_paths(search_term)
                            if not local_paths_nf:
                                local_paths_nf = [img_path]
                            nf_local_idx_key = f"local_img_idx_{search_term}"
                            if nf_local_idx_key not in st.session_state:
                                st.session_state[nf_local_idx_key] = 0
                            nf_total = len(local_paths_nf)
                            nf_cur   = min(st.session_state[nf_local_idx_key], nf_total - 1)
                            if nf_total > 1:
                                col_p, col_i, col_n = st.columns([1, 3, 1])
                                with col_p:
                                    if st.button("◀ Prev", key=f"nf_loc_prev_{search_term}", disabled=(nf_cur == 0)):
                                        st.session_state[nf_local_idx_key] = max(0, nf_cur - 1)
                                        st.rerun()
                                with col_i:
                                    st.markdown(f"<div style='text-align:center;padding:6px 0;font-weight:600;color:#1565C0;'>Foto {nf_cur+1} / {nf_total}</div>", unsafe_allow_html=True)
                                with col_n:
                                    if st.button("Next ▶", key=f"nf_loc_next_{search_term}", disabled=(nf_cur == nf_total - 1)):
                                        st.session_state[nf_local_idx_key] = min(nf_total - 1, nf_cur + 1)
                                        st.rerun()
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = local_paths_nf[nf_cur].read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=f"{search_term} (Foto {nf_cur+1}/{nf_total})", zoom_key=f"nf_{search_term}_local_{nf_cur}")
                            if nf_total > 1:
                                st.markdown("**Pilih foto:**")
                                thumb_cols = st.columns(min(nf_total, 5))
                                for ti, (tc, lp) in enumerate(zip(thumb_cols, local_paths_nf)):
                                    with tc:
                                        lbl = f"{'✅' if ti == nf_cur else '🔲'} {ti+1}"
                                        if st.button(lbl, key=f"nf_loc_thumb_{search_term}_{ti}"):
                                            st.session_state[nf_local_idx_key] = ti
                                            st.rerun()
                    else:
                        sims_err = st.session_state.get(sims_err_key)
                        if sims_err:
                            st.warning(f"⚠️ SIMS: {sims_err}")
                        else:
                            st.caption("📷 Tidak ada gambar di SIMS untuk part ini")

    def _load_populasi_data(self):
        if "populasi_df" in st.session_state:
            return st.session_state.populasi_df
        excel_ext = (".xlsx", ".xls", ".xlsm")
        frames    = []
        if self.populasi_folder.exists():
            for fp in sorted(self.populasi_folder.iterdir()):
                if fp.suffix.lower() not in excel_ext:
                    continue
                try:
                    with open(fp, "rb") as f:
                        file_bytes = io.BytesIO(f.read())
                    xl = pd.ExcelFile(file_bytes, engine="openpyxl")
                    for sheet in xl.sheet_names:
                        df = pd.read_excel(xl, sheet_name=sheet, dtype=str)
                        df.columns = [str(c).strip() for c in df.columns]
                        df["_source_file"]  = fp.name
                        df["_source_sheet"] = sheet
                        frames.append(df)
                except Exception as e:
                    st.warning(f"Gagal membaca {fp.name}: {e}")
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        st.session_state.populasi_df = combined
        return combined

    def render_admin_image_tab(self):
        st.markdown("### 🖼️ Kelola Foto Part (Manual)")
        st.markdown(
            "Upload foto untuk part yang tidak memiliki gambar di SIMS. "
            "Foto disimpan di folder `images/` dan akan otomatis ditampilkan saat pencarian."
        )
        st.markdown("---")

        st.markdown("#### ➕ Upload Foto Baru")
        col_pn, col_up = st.columns([1, 2])
        with col_pn:
            part_input = st.text_input(
                "Part Number:", placeholder="Contoh: WG9925520270",
                key="admin_img_pn_input",
            ).strip().upper()
        with col_up:
            uploaded_files = st.file_uploader(
                "Pilih file foto (JPG / PNG):",
                type=["jpg", "jpeg", "png"],
                accept_multiple_files=True,
                key="admin_img_uploader",
            )

        if st.button("💾 Simpan Foto", type="primary", key="admin_img_save_btn"):
            if not part_input:
                st.warning("⚠️ Masukkan Part Number terlebih dahulu.")
            elif not uploaded_files:
                st.warning("⚠️ Pilih minimal satu file foto.")
            else:
                saved, skipped = [], []
                part_folder = self.images_folder / part_input
                part_folder.mkdir(parents=True, exist_ok=True)
                for uf in uploaded_files:
                    dest = part_folder / uf.name
                    if dest.exists():
                        skipped.append(uf.name)
                    else:
                        dest.write_bytes(uf.read())
                        saved.append(uf.name)
                if saved:
                    st.success(f"✅ {len(saved)} foto berhasil disimpan untuk **{part_input}**: {', '.join(saved)}")
                if skipped:
                    st.info(f"ℹ️ {len(skipped)} file dilewati (sudah ada): {', '.join(skipped)}")

        st.markdown("---")
        st.markdown("#### 📂 Foto yang Sudah Ada")

        img_ext      = {".jpg", ".jpeg", ".png"}
        part_folders = sorted([
            p for p in self.images_folder.iterdir()
            if p.is_dir() and any(f.suffix.lower() in img_ext for f in p.iterdir())
        ])
        direct_files = sorted([
            f for f in self.images_folder.iterdir()
            if f.is_file() and f.suffix.lower() in img_ext
        ])

        total_parts = len(part_folders) + len(direct_files)
        if total_parts == 0:
            st.info("Belum ada foto manual yang tersimpan di folder `images/`.")
        else:
            st.caption(f"Ditemukan foto untuk **{total_parts}** part.")
            search_pn = st.text_input(
                "🔍 Filter Part Number:", placeholder="Ketik untuk filter",
                key="admin_img_filter"
            ).strip().upper()

            for pf in part_folders:
                pn_name = pf.name.upper()
                if search_pn and search_pn not in pn_name:
                    continue
                files_in = sorted([f for f in pf.iterdir() if f.suffix.lower() in img_ext])
                with st.expander(f"📁 {pn_name}  ({len(files_in)} foto)", expanded=False):
                    cols_per_row = 3
                    rows = [files_in[i:i+cols_per_row] for i in range(0, len(files_in), cols_per_row)]
                    for row_files in rows:
                        img_cols = st.columns(cols_per_row)
                        for col, fpath in zip(img_cols, row_files):
                            with col:
                                try:
                                    st.image(fpath.read_bytes(), caption=fpath.name, use_container_width=True)
                                except Exception:
                                    st.caption(f"⚠️ {fpath.name} (gagal dimuat)")
                                if st.button(f"🗑️ Hapus", key=f"del_{pn_name}_{fpath.name}",
                                             help=f"Hapus {fpath.name}"):
                                    try:
                                        fpath.unlink()
                                        st.toast(f"✅ {fpath.name} dihapus.")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Gagal hapus: {e}")
                    remaining = [f for f in pf.iterdir() if f.suffix.lower() in img_ext]
                    if not remaining:
                        try:
                            pf.rmdir()
                        except Exception:
                            pass

            for fpath in direct_files:
                pn_name = fpath.stem.upper()
                if search_pn and search_pn not in pn_name:
                    continue
                with st.expander(f"🖼️ {pn_name}  (1 foto — format lama)", expanded=False):
                    col_img, col_act = st.columns([2, 1])
                    with col_img:
                        try:
                            st.image(fpath.read_bytes(), caption=fpath.name, use_container_width=True)
                        except Exception:
                            st.caption(f"⚠️ {fpath.name} (gagal dimuat)")
                    with col_act:
                        if st.button("🗑️ Hapus", key=f"del_direct_{fpath.name}"):
                            try:
                                fpath.unlink()
                                st.toast(f"✅ {fpath.name} dihapus.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Gagal hapus: {e}")

    def render_populasi_tab(self):
        user     = LoginManager.get_current_user()
        role     = user["role"] if user else "user"
        is_admin = (role == "admin")

        st.markdown("### 🚛 Populasi Unit")

        col_r, col_mode, _ = st.columns([1, 2, 3])
        with col_r:
            if st.button("🔄 Refresh", key="refresh_populasi"):
                st.session_state.pop("populasi_df", None)
                st.session_state.pop("pop_edit_mode", None)
                st.rerun()

        df = self._load_populasi_data()

        if df.empty:
            st.warning("Tidak ada file Excel di folder data/populasi/. Pastikan file populasi sudah ditempatkan di folder tersebut.")
            return

        display_cols = [c for c in df.columns if not c.startswith("_source")]
        df_display   = df[display_cols].copy()

        edit_mode = False
        if is_admin:
            with col_mode:
                edit_mode = st.toggle(
                    "✏️ Mode Edit",
                    value=st.session_state.get("pop_edit_mode", False),
                    key="pop_edit_toggle",
                    help="Aktifkan untuk mengedit data populasi dan menyimpan ke GitHub",
                )
                st.session_state["pop_edit_mode"] = edit_mode

        if is_admin and edit_mode:
            st.info("✏️ **Mode Edit Aktif** — Pilih file & sheet, edit data, lalu klik Simpan ke GitHub.", icon="ℹ️")

            source_files  = sorted(df["_source_file"].dropna().unique().tolist())
            sel_file      = st.selectbox("📁 Pilih File Populasi:", source_files, key="pop_edit_file")
            source_sheets = sorted(df[df["_source_file"] == sel_file]["_source_sheet"].dropna().unique().tolist())
            sel_sheet     = st.selectbox("📋 Pilih Sheet:", source_sheets, key="pop_edit_sheet")

            mask_edit   = (df["_source_file"] == sel_file) & (df["_source_sheet"] == sel_sheet)
            df_edit_raw = df[mask_edit][display_cols].copy().reset_index(drop=True)

            st.caption(f"📊 {len(df_edit_raw)} baris ditemukan di `{sel_file}` → sheet `{sel_sheet}`")
            st.markdown("#### 📝 Edit Data")
            st.markdown("<small>Klik sel untuk mengedit. Gunakan tombol ➕ untuk tambah baris baru.</small>", unsafe_allow_html=True)

            edited_df = st.data_editor(
                df_edit_raw,
                use_container_width=True,
                num_rows="dynamic",
                height=500,
                key=f"pop_data_editor_{sel_file}_{sel_sheet}",
            )

            st.markdown("---")
            col_save, col_cancel = st.columns([1, 1])

            with col_save:
                if st.button("💾 Simpan ke GitHub", type="primary", use_container_width=True, key="pop_save_github"):
                    df_others               = df[~mask_edit].copy()
                    edited_df_with_src      = edited_df.copy()
                    edited_df_with_src["_source_file"]  = sel_file
                    edited_df_with_src["_source_sheet"] = sel_sheet
                    df_full_updated = pd.concat([df_others, edited_df_with_src], ignore_index=True)

                    with st.spinner("⏳ Menyimpan ke GitHub..."):
                        ok, err = save_populasi_to_github(
                            df_full_updated, sel_file, sel_sheet, user["username"]
                        )

                    if ok:
                        st.success(f"✅ Berhasil disimpan ke GitHub!\nFile: `{sel_file}` | Sheet: `{sel_sheet}`")
                        st.session_state.pop("populasi_df", None)
                        st.session_state["pop_edit_mode"] = False
                        st.rerun()
                    else:
                        st.error(f"❌ Gagal menyimpan:\n{err}")

            with col_cancel:
                if st.button("↩️ Batal", use_container_width=True, key="pop_cancel_edit"):
                    st.session_state["pop_edit_mode"] = False
                    st.rerun()

            dl_buf = io.BytesIO()
            edited_df.to_excel(dl_buf, index=False, engine="openpyxl")
            dl_buf.seek(0)
            st.download_button(
                label="⬇️ Download Preview Edit (.xlsx)",
                data=dl_buf.getvalue(),
                file_name=f"preview_edit_{sel_file}_{sel_sheet}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="pop_preview_dl",
            )
            return

        with st.expander("🔍 Filter & Pencarian", expanded=True):
            search_col, filter_area = st.columns([2, 3])
            with search_col:
                keyword = st.text_input(
                    "Cari (semua kolom):", placeholder="Ketik kata kunci",
                    key="pop_keyword",
                    value=st.session_state.get("pop_keyword_val", ""),
                )
                st.session_state["pop_keyword_val"] = keyword
            with filter_area:
                fcols             = st.columns(2)
                filter_vals       = {}
                candidate_filters = ["MODEL", "JENIS", "TIPE UNIT", "LOKASI KERJA", "TAHUN", "Euro"]
                available_filters = [c for c in candidate_filters if c in df_display.columns][:4]
                for i, col in enumerate(available_filters):
                    with fcols[i % 2]:
                        options = ["Semua"] + sorted(df_display[col].dropna().unique().tolist())
                        sk      = f"pop_filter_{col}"
                        saved   = st.session_state.get(sk, "Semua")
                        if saved not in options:
                            saved = "Semua"
                        filter_vals[col] = st.selectbox(col, options, index=options.index(saved), key=sk)

        mask = pd.Series([True] * len(df_display), index=df_display.index)
        if keyword.strip():
            kw      = keyword.strip().upper()
            kw_mask = pd.Series([False] * len(df_display), index=df_display.index)
            for col in df_display.columns:
                kw_mask |= df_display[col].astype(str).str.upper().str.contains(kw, na=False)
            mask &= kw_mask
        for col, val in filter_vals.items():
            if val != "Semua":
                mask &= (df_display[col].astype(str) == val)

        df_filtered = df_display[mask].reset_index(drop=True)

        c1, c2 = st.columns(2)
        c1.metric("Total Unit", len(df_display))
        c2.metric("Hasil Filter", len(df_filtered))
        st.markdown("---")

        if df_filtered.empty:
            st.info("Tidak ada data yang cocok dengan filter.")
        else:
            df_show = df_filtered.rename(columns=lambda c: c.strip())
            st.dataframe(df_show, hide_index=True, use_container_width=True, height=500)
            dl_buf = io.BytesIO()
            df_show.to_excel(dl_buf, index=False, engine="openpyxl")
            dl_buf.seek(0)
            st.download_button(
                label="⬇️ Download Excel",
                data=dl_buf.getvalue(),
                file_name=f"populasi_unit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="pop_download",
            )

    def run(self):
        self.display_dashboard()


def main():
    LoginManager.init_session()
    login_mgr = LoginManager()
    if not LoginManager.is_authenticated():
        render_login_page(login_mgr)
    else:
        ExcelSearchApp().run()


if __name__ == "__main__":
    main()
