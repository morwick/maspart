"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING + LOGIN SYSTEM + THRESHOLD + BATCH DOWNLOAD
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
    # Inject via st.markdown agar tidak butuh iframe
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
</style>
""", unsafe_allow_html=True)

SESSION_TIMEOUT_MINUTES = 75
LOGIN_FOLDER    = Path("login")
DATA_FOLDER     = Path("data")
CACHE_FOLDER    = Path(".cache")
IMAGES_FOLDER   = Path("images")



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
                row = df.iloc[indices[0]]
                pn_value = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
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
    results = []
    term_up = term.strip().upper()
    if not term_up:
        return results
    for fi in excel_files:
        df  = fi["dataframe"]
        pni = fi.get("part_name_index", {})
        matching_indices = set()
        search_words = term_up.split()
        for word in pni.keys():
            for sw in search_words:
                if sw in word or word in sw:
                    matching_indices.update(pni[word])
        if not matching_indices and len(term_up) <= 3:
            for idx, row in df.iterrows():
                pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
                if term_up in pname.upper():
                    matching_indices.add(idx)
        for idx in matching_indices:
            row   = df.iloc[idx]
            pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
            if term_up in pname.upper():
                pn_value   = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                results.append({
                    "File": fi["simple_name"], "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value, "Part Name": pname if pname else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Excel Row": idx + 2, "Full Path": fi["full_path"]
                })
    return results


def build_batch_excel(df_result: pd.DataFrame) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Batch Search Result"

    headers = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    header_fill  = PatternFill("solid", fgColor="1565C0")
    header_font  = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_align   = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    thin   = Side(style="thin", color="BDBDBD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for col_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=h)
        cell.font = header_font; cell.fill = header_fill
        cell.alignment = center_align; cell.border = border
    ws.row_dimensions[1].height = 22

    fill_found     = PatternFill("solid", fgColor="E3F2FD")
    fill_not_found = PatternFill("solid", fgColor="FFEBEE")
    fill_alt       = PatternFill("solid", fgColor="FAFAFA")

    export_cols   = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    group_start   = {}
    group_end     = {}

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
    """
    Build catalog Excel: Part Number | Part Name | Kecocokan | Gambar 1 | Gambar 2
    Gambar diambil dari SIMS — foto ke-1 di kolom D, foto ke-2 di kolom E.
    Jika hanya 1 foto, kolom D diisi, kolom E kosong.
    Satu baris per unique Part Number.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.drawing.image import Image as XLImage
    from PIL import Image as PILImage
    import tempfile, os

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
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = center
        cell.border    = border
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[1].height = 22

    fill_even = PatternFill("solid", fgColor="E3F2FD")
    fill_odd  = PatternFill("solid", fgColor="FAFAFA")
    fill_nf   = PatternFill("solid", fgColor="FFEBEE")

    # kumpulkan 1 baris per PN
    grouped = {}
    for _, r in df_result.iterrows():
        pn     = r["_pn_group"]
        status = r.get("Status", "")
        hasil  = r.get("Hasil", "")
        pname  = r.get("Part Name", "")
        if pn not in grouped:
            grouped[pn] = {"Part Name": pname, "kecocokan_list": [], "found": False}
        if status == "✅ Ditemukan" and hasil:
            grouped[pn]["kecocokan_list"].append(hasil)
            grouped[pn]["found"] = True
            if not grouped[pn]["Part Name"]:
                grouped[pn]["Part Name"] = pname

    def _make_xl_image(img_bytes, max_h=200):
        """Resize gambar dan simpan ke file tmp, return (XLImage, w_px, h_px, tmp_path)."""
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

        # fetch 2 gambar BERBEDA dari SIMS
        if SIMS_ENABLED and is_found:
            try:
                import hashlib
                urls, _ = _sims_fetch(pn)
                if urls:
                    # foto 1 → kolom D (index 0)
                    b1, _ = ExcelSearchApp.fetch_image_bytes(urls[0])
                    if b1:
                        xl, w, h, tmp_path = _make_xl_image(b1)
                        img_d = xl
                        tmp_files.append(tmp_path)
                        row_height = max(int(h * 0.75) + 10, row_height)
                        hash1 = hashlib.md5(b1).hexdigest()

                        # foto 2 → cari URL berikutnya yang berbeda hash
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

        # tulis sel A–C
        for ci, (val, aln) in enumerate(
            [(pn, center), (info["Part Name"], left), (kecocokan, left)], start=1
        ):
            cell           = ws.cell(row=row_idx, column=ci, value=val)
            cell.fill      = fill
            cell.border    = border
            cell.alignment = aln
            cell.font      = Font(name="Arial", size=10)

        # sel D dan E (gambar)
        for ci in (4, 5):
            c           = ws.cell(row=row_idx, column=ci, value="")
            c.fill      = fill
            c.border    = border
            c.alignment = center

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

    # cleanup tmp files
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
    ws["A1"].font = Font(bold=True, name="Arial", size=11, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="1565C0")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 20
    for i, ex in enumerate(["WG1642821034", "WG9925520270", "AZ9100443082", "WG9718820030"], start=2):
        ws.cell(row=i, column=1, value=ex).font = Font(name="Arial", size=10)
    ws.column_dimensions["A"].width = 28
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf.getvalue()


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
        base = pn_str.split("/", 1)[0].strip()
        return re.sub(r'[^A-Z0-9\-]', '_', base)

    def get_image_path(self, pn):
        base = self.normalize_base_part_number(pn)
        if not base:
            return None
        # Cek subfolder: images/PARTNUMBER/foto.jpg (format baru dari admin upload)
        sub_folder = self.images_folder / base
        if sub_folder.exists() and sub_folder.is_dir():
            for ext in self.supported_ext:
                candidates = sorted(sub_folder.glob(f"*{ext}"))
                if candidates:
                    return candidates[0]
        # Fallback ke file langsung: images/PARTNUMBER.jpg (format lama)
        for ext in self.supported_ext:
            p = self.images_folder / f"{base}{ext}"
            if p.exists():
                return p
        return None

    def get_all_image_paths(self, pn):
        """Return semua path gambar lokal untuk suatu part number (subfolder + file langsung)."""
        base = self.normalize_base_part_number(pn)
        if not base:
            return []
        paths = []
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
        """Tampilkan gambar dengan kontrol zoom menggunakan st.image + CSS transform."""
        import base64

        zk = f"zoom_scale_{zoom_key}"
        if zk not in st.session_state:
            st.session_state[zk] = 100  # persen

        scale = st.session_state[zk]

        # Tombol zoom
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

        # Render gambar dengan CSS transform scale
        b64 = base64.b64encode(img_bytes).decode()
        sig = img_bytes[:4]
        if sig[:2] == b'\xff\xd8':
            mime = "image/jpeg"
        elif sig[:4] == b'\x89PNG':
            mime = "image/png"
        elif sig[:3] == b'GIF':
            mime = "image/gif"
        else:
            mime = "image/jpeg"

        cur_scale = st.session_state[zk]
        safe_caption = caption.replace("<", "&lt;").replace(">", "&gt;")
        img_html = f"""
<div style="overflow:auto; width:100%; text-align:center; padding:4px 0;">
  <img src="data:{mime};base64,{b64}"
       style="width:{cur_scale}%; max-width:none;
              transform-origin:top center;
              border-radius:8px;
              box-shadow:0 2px 12px rgba(0,0,0,.18);
              transition:width .2s ease;"
       title="{safe_caption}" />
  <div style="font-size:.78rem;color:#666;margin-top:4px;">{safe_caption}</div>
</div>
"""
        st.markdown(img_html, unsafe_allow_html=True)

    @staticmethod
    def fetch_image_bytes(url: str):
        """Fetch image from URL and return bytes."""
        try:
            headers = {"User-Agent": "Mozilla/5.0"}

            # Sertakan token Authorization untuk semua URL dari server SIMS
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

            # Debug info ke terminal
            print(f"[debug] URL: {url}")
            print(f"[debug] Status: {resp.status_code}")
            print(f"[debug] Content-Type: {resp.headers.get('Content-Type', '-')}")
            print(f"[debug] Content-Length: {len(resp.content)}")

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
            st.warning("File stok tidak ditemukan: data/stok/stok.xlsx")
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
                        row = fi["dataframe"].iloc[pn_idx[pn][0]]
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
                st.session_state.last_file_count = 0
                return
            need_reindex = (len(all_files) != st.session_state.last_file_count
                            or st.session_state.last_index_time is None)
            if need_reindex:
                with st.spinner("🔄 Mengindeks file Excel…"):
                    st.session_state.excel_files = []
                    st.session_state.index_data  = []
                    prog = st.progress(0)
                    txt  = st.empty()
                    completed = 0
                    with ThreadPoolExecutor(max_workers=min(4, len(all_files))) as ex:
                        futures = {ex.submit(self.process_single_file, fp, rp): (fp, rp)
                                   for fp, rp in all_files}
                        for future in as_completed(futures):
                            completed += 1
                            prog.progress(completed / len(all_files))
                            txt.text(f"Processing {completed}/{len(all_files)} files…")
                            try:
                                for fi in (future.result() or []):
                                    st.session_state.excel_files.append(fi)
                                    st.session_state.index_data.append({
                                        "file": fi["simple_name"], "relative_path": fi["relative_path"],
                                        "sheet": fi["sheet"], "rows": fi["row_count"],
                                        "last_modified": fi["last_modified"],
                                    })
                            except Exception:
                                continue
                    st.session_state.loaded_files_count = len(st.session_state.excel_files)
                    st.session_state.last_file_count    = len(all_files)
                    st.session_state.last_index_time    = datetime.now()
                    prog.empty(); txt.empty()
        except Exception as e:
            st.sidebar.error(f"Error auto-load: {e}")

    # ── IMAGE SEARCH TAB ────────────────────────────────────────────
    # ── BATCH DOWNLOAD TAB ──────────────────────────────────────────────────
    def render_batch_download_tab(self):
        st.markdown("### 📥 Batch Download — Cari Banyak Part Number Sekaligus")

        st.markdown("""
        <div class="batch-info-box">
        <b>📋 Format File Input:</b><br>
        • File Excel (.xlsx / .xls / .xlsm) atau CSV<br>
        • <b>Kolom A</b> = Part Number (boleh ada header "Part Number" atau langsung data)<br>
        • Satu Part Number per baris
        </div>
        """, unsafe_allow_html=True)

        # Template download
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

        # Baca file upload
        try:
            if uploaded.name.endswith(".csv"):
                df_input = pd.read_csv(uploaded, header=None, dtype=str)
            else:
                df_input = pd.read_excel(uploaded, header=None, dtype=str)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            return

        col_a = df_input.iloc[:, 0].dropna().astype(str).str.strip()
        # Buang header jika ada
        if col_a.iloc[0].lower() in ("part number","part_number","partnumber","no part","kode"):
            col_a = col_a.iloc[1:]

        part_numbers = col_a[col_a.str.len() > 0].tolist()

        if not part_numbers:
            st.warning("Tidak ada Part Number yang valid dalam file.")
            return

        st.info(f"📊 **{len(part_numbers)}** Part Number ditemukan dalam file input.")

        with st.expander("👁️ Preview Part Number"):
            st.dataframe(pd.DataFrame({"Part Number": part_numbers}),
                         hide_index=True, height=200)

        if st.button("🔍 Proses Batch Search", type="primary",
                     use_container_width=True, key="batch_process_btn"):

            if not st.session_state.excel_files:
                st.error("Tidak ada file Excel yang ter-index di folder data/.")
                st.stop()

            # ── Proses pencarian ──
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

            # ── Fetch gambar SIMS & build catalog bytes ──
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

        # ── Tampilkan hasil & tombol download (persisten via session_state) ──
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
            df_result[disp_cols],
            hide_index=True,
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

    # ── SIDEBAR & DASHBOARD ──────────────────────────────────────────────────
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
                for k in ("excel_files","last_index_time","last_file_count","stok_data","threshold_data"):
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
                """)

        # ── TABS ──
        st.markdown(TAB_PERSIST_JS, unsafe_allow_html=True)
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        if role == "admin":
            tab1, tab2, tab4, tab5, tab_admin_img = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "📥 Batch Download", "🚛 Populasi Unit", "🖼️ Kelola Foto"])
        else:
            tab1, tab2, tab4, tab5 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "📥 Batch Download", "🚛 Populasi Unit"])
            tab_admin_img = None
        tab3 = None

        with tab1:
            with st.form(key="search_pn_form", clear_on_submit=False):
                sn_input = st.text_input("Masukkan Part Number:", placeholder="Contoh: WG1642821034/1", key="sn_input")
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
                name_input = st.text_input("Masukkan Part Name:", placeholder="Contoh: Bearing, Screw", key="name_input")
                if st.form_submit_button("🔍 Cari Part Name", type="primary", use_container_width=True):
                    if name_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_name(
                                name_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Name"
                            st.session_state.search_term = name_input
                            st.rerun()
                    else:
                        st.warning("Masukkan nama part untuk mencari.")

        if tab_admin_img is not None:
            with tab_admin_img:
                self.render_admin_image_tab()

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
            cols = [c for c in ["File","Part Number","Part Name","Quantity","Stok","Sheet","Excel Row"]
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
                    rows = df_res[df_res["Part Number"] == pn]
                    pname_ex = rows.iloc[0]["Part Name"] if not rows.empty else "N/A"

                    sims_key     = f"sims_fetched_{pn}"
                    sims_err_key = f"sims_err_{pn}"

                    # Fetch dari SIMS — cache di session_state selama session ini
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

                    # Fallback ke file lokal
                    img_path = self.get_image_path(pn)
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
                            # Session state key untuk index gambar per PN
                            idx_key = f"img_idx_{pn}"
                            if idx_key not in st.session_state:
                                st.session_state[idx_key] = 0

                            total = len(img_links)
                            current_idx = st.session_state[idx_key]

                            # Navigasi panah (hanya tampil jika > 1 gambar)
                            if total > 1:
                                col_prev, col_info, col_next = st.columns([1, 3, 1])
                                with col_prev:
                                    if st.button("◀ Prev", key=f"prev_{pn}",
                                                 disabled=(current_idx == 0),
                                                 ):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; "
                                        f"font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"next_{pn}",
                                                 disabled=(current_idx == total - 1),
                                                 ):
                                        st.session_state[idx_key] = min(total - 1, current_idx + 1)
                                        st.rerun()

                            # Tampilkan gambar aktif dalam kolom agar lebih kecil & rapi
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
                                    st.error(f"⚠️ Gambar berhasil diunduh ({len(img_bytes):,} bytes) tapi gagal ditampilkan: {e}")
                                    st.caption(f"URL: {active_url}")
                            else:
                                st.warning(f"⚠️ Gagal memuat gambar: {err}")
                                st.caption(f"URL: {active_url}")

                            # Thumbnail strip (jika > 1)
                            if total > 1:
                                st.markdown("**Pilih gambar:**")
                                thumb_cols = st.columns(min(total, 5))
                                for ti, (tc, lnk) in enumerate(zip(thumb_cols, img_links)):
                                    with tc:
                                        label = f"{'✅' if ti == current_idx else '🔲'} {ti+1}"
                                        if st.button(label, key=f"thumb_{pn}_{ti}",
                                                     ):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            local_paths = self.get_all_image_paths(pn)
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

            # Tetap tampilkan gambar part jika tersedia, meskipun tidak ditemukan di Excel
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

                img_path = self.get_image_path(search_term)
                if img_path and not img_path.exists():
                    img_path = None

                st.markdown("### 🖼️ Gambar Part")
                with st.expander(f"🖼️ {search_term}", expanded=True):
                    # Tombol refresh
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
                                    if st.button("◀ Prev", key=f"nf_prev_{search_term}",
                                                 disabled=(current_idx == 0)):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; "
                                        f"font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"nf_next_{search_term}",
                                                 disabled=(current_idx == total - 1)):
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
                                        if st.button(label, key=f"nf_thumb_{search_term}_{ti}",
                                                     ):
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
        """Baca semua file Excel dari folder data/populasi/ dan gabungkan."""
        if "populasi_df" in st.session_state:
            return st.session_state.populasi_df
        excel_ext = (".xlsx", ".xls", ".xlsm")
        frames = []
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
        """Tab khusus admin: upload & kelola foto manual untuk part yang tidak ada di SIMS."""
        st.markdown("### 🖼️ Kelola Foto Part (Manual)")
        st.markdown(
            "Upload foto untuk part yang tidak memiliki gambar di SIMS. "
            "Foto disimpan di folder `images/` dan akan otomatis ditampilkan saat pencarian."
        )
        st.markdown("---")

        # ── Upload Foto Baru ──────────────────────────────────────────────
        st.markdown("#### ➕ Upload Foto Baru")
        col_pn, col_up = st.columns([1, 2])
        with col_pn:
            part_input = st.text_input(
                "Part Number:",
                placeholder="Contoh: WG9925520270",
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

        # ── Daftar Foto yang Sudah Ada ────────────────────────────────────
        st.markdown("#### 📂 Foto yang Sudah Ada")

        # Kumpulkan semua folder/part yang punya foto lokal
        img_ext = {".jpg", ".jpeg", ".png"}
        part_folders = sorted([
            p for p in self.images_folder.iterdir()
            if p.is_dir() and any(f.suffix.lower() in img_ext for f in p.iterdir())
        ])

        # Juga cek file langsung di images/ (format lama: PARTNUMBER.jpg)
        direct_files = sorted([
            f for f in self.images_folder.iterdir()
            if f.is_file() and f.suffix.lower() in img_ext
        ])

        total_parts = len(part_folders) + len(direct_files)
        if total_parts == 0:
            st.info("Belum ada foto manual yang tersimpan di folder `images/`.")
        else:
            st.caption(f"Ditemukan foto untuk **{total_parts}** part.")

            # Filter pencarian
            search_pn = st.text_input(
                "🔍 Filter Part Number:", placeholder="Ketik untuk filter",
                key="admin_img_filter"
            ).strip().upper()

            # Tampilkan per-folder (subfolder)
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
                                if st.button(
                                    f"🗑️ Hapus", key=f"del_{pn_name}_{fpath.name}",
                                    help=f"Hapus {fpath.name}"
                                ):
                                    try:
                                        fpath.unlink()
                                        st.toast(f"✅ {fpath.name} dihapus.")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Gagal hapus: {e}")
                    # Hapus folder jika sudah kosong setelah hapus file
                    remaining = [f for f in pf.iterdir() if f.suffix.lower() in img_ext]
                    if not remaining:
                        try:
                            pf.rmdir()
                        except Exception:
                            pass

            # Tampilkan file langsung di images/ (format lama)
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
        st.markdown("### Populasi Unit")

        col_r, _ = st.columns([1, 5])
        with col_r:
            if st.button("Refresh Data Populasi", key="refresh_populasi"):
                st.session_state.pop("populasi_df", None)
                # Tidak st.rerun() — biarkan Streamlit re-render alami

        df = self._load_populasi_data()

        if df.empty:
            st.warning("Tidak ada file Excel di folder data/populasi/. Pastikan file populasi sudah ditempatkan di folder tersebut.")
            return

        display_cols = [c for c in df.columns if not c.startswith("_source")]
        df_display   = df[display_cols].copy()

        with st.expander("Filter & Pencarian", expanded=True):
            search_col, filter_area = st.columns([2, 3])
            with search_col:
                keyword = st.text_input(
                    "Cari (semua kolom):", placeholder="Ketik kata kunci",
                    key="pop_keyword",
                    value=st.session_state.get("pop_keyword_val", ""),
                )
                st.session_state["pop_keyword_val"] = keyword
            with filter_area:
                fcols = st.columns(2)
                filter_vals = {}
                candidate_filters = ["MODEL", "JENIS", "TIPE UNIT", "LOKASI KERJA", "TAHUN", "Euro"]
                available_filters = [c for c in candidate_filters if c in df_display.columns][:4]
                for i, col in enumerate(available_filters):
                    with fcols[i % 2]:
                        options = ["Semua"] + sorted(df_display[col].dropna().unique().tolist())
                        sk = f"pop_filter_{col}"
                        saved = st.session_state.get(sk, "Semua")
                        if saved not in options:
                            saved = "Semua"
                        filter_vals[col] = st.selectbox(
                            col, options,
                            index=options.index(saved),
                            key=sk,
                        )

        mask = pd.Series([True] * len(df_display), index=df_display.index)
        if keyword.strip():
            kw = keyword.strip().upper()
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
                label="Download Excel",
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
