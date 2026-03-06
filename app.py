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
    const INTERVAL_MS = 5 * 60 * 1000;
    function ping() {
        fetch(window.location.href, { method: 'GET', cache: 'no-store' })
            .then(r => console.log('[KeepAlive] ping ok', new Date().toLocaleTimeString()))
            .catch(e => console.warn('[KeepAlive] ping gagal:', e));
    }
    window.__keepAliveTimer = setInterval(ping, INTERVAL_MS);
    setTimeout(ping, 60 * 1000);
})();
</script>
"""

def inject_keep_alive():
    st.components.v1.html(KEEP_ALIVE_JS, height=0, scrolling=False)

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    header[data-testid="stHeader"] {display: none !important;}
    div[data-testid="stToolbar"] {display: none !important;}
    iframe {display: none !important;}
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
IMAGES_JSON     = Path("images") / "image_links.json"


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
            submitted = st.form_submit_button("Login", type="primary", width="stretch")
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
        self._load_stok_data()
        self._load_threshold_data()
        self._load_image_links()

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
        for ext in self.supported_ext:
            p = self.images_folder / f"{base}{ext}"
            if p.exists():
                return p
        return None

    def _load_image_links(self):
        """Load image links from images/image_links.json"""
        if "image_links" in st.session_state:
            self.image_links = st.session_state.image_links
            return
        self.image_links = {}
        if IMAGES_JSON.exists():
            try:
                with open(IMAGES_JSON, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                # Normalize keys to uppercase, values always list of strings
                for pn_key, links in raw.items():
                    norm_key = str(pn_key).strip().upper()
                    if isinstance(links, list):
                        self.image_links[norm_key] = [str(l) for l in links if l]
                    elif isinstance(links, str) and links:
                        self.image_links[norm_key] = [links]
                st.session_state.image_links = self.image_links
            except Exception as e:
                st.warning(f"Gagal membaca image_links.json: {e}")

    def get_image_links(self, pn):
        """Return list of image URLs for a part number, or empty list."""
        if not pn:
            return []
        pn_up = str(pn).strip().upper()
        # Try exact match first
        if pn_up in self.image_links:
            return self.image_links[pn_up]
        # Try base (before '/')
        base = pn_up.split("/", 1)[0].strip()
        if base in self.image_links:
            return self.image_links[base]
        return []

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
            resp = requests.get(url, timeout=15,
                                headers={"User-Agent": "Mozilla/5.0"})
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
                         width="stretch", hide_index=True, height=200)

        if not st.button("🔍 Proses Batch Search", type="primary",
                         width="stretch", key="batch_process_btn"):
            return

        if not st.session_state.excel_files:
            st.error("Tidak ada file Excel yang ter-index di folder data/.")
            return

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

        # Statistik
        found_pn  = df_result[df_result["Status"] == "✅ Ditemukan"]["_pn_group"].nunique()
        not_found = total - found_pn
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Part Number", total)
        c2.metric("✅ Ditemukan", found_pn)
        c3.metric("❌ Tidak Ditemukan", not_found)

        # Preview tabel
        st.markdown("#### 📋 Preview Hasil")
        disp_cols = ["Part Number","Hasil","Sheet","Part Name","Qty","Stok","Status"]
        st.dataframe(
            df_result[disp_cols],
            width="stretch",
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

        # Download Excel
        excel_bytes = build_batch_excel(df_result)
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="⬇️ Download Hasil (.xlsx)",
            data=excel_bytes,
            file_name=f"batch_result_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            width="stretch",
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

            if st.button("🚪 Logout", type="secondary", width="stretch"):
                LoginManager.logout()
                for k in ("excel_files","index_data","search_results",
                          "last_index_time","loaded_files_count","last_file_count"):
                    st.session_state.pop(k, None)
                st.rerun()
            st.divider()

            if role == "admin":
                st.markdown("### 🛡️ Admin Panel")
                if st.button("👥 Reload Users", type="secondary", width="stretch"):
                    st.session_state.login_users_df = LoginManager._load_users()
                    st.toast("✅ Data user telah di-reload!")
                df_users = st.session_state.get("login_users_df", pd.DataFrame())
                if not df_users.empty:
                    with st.expander("📋 Daftar User"):
                        st.dataframe(df_users[["username","role"]].rename(
                            columns={"username":"Username","role":"Role"}),
                            hide_index=True, width="stretch")
                st.divider()

            st.markdown("### 📊 Status Sistem")
            if st.button("🔄 Refresh Data", type="secondary", width="stretch"):
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
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        if role == "admin":
            tab1, tab2, tab3, tab4 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "⚠️ Threshold", "📥 Batch Download"])
        else:
            tab1, tab2, tab4 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name", "📥 Batch Download"])
            tab3 = None

        with tab1:
            with st.form(key="search_pn_form", clear_on_submit=False):
                sn_input = st.text_input("Masukkan Part Number:", placeholder="Contoh: WG1642821034/1", key="sn_input")
                if st.form_submit_button("🔍 Cari Part Number", type="primary", width="stretch"):
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
                if st.form_submit_button("🔍 Cari Part Name", type="primary", width="stretch"):
                    if name_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_name(
                                name_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Name"
                            st.session_state.search_term = name_input
                            st.rerun()
                    else:
                        st.warning("Masukkan nama part untuk mencari.")

        if tab3 is not None:
            with tab3:
                st.markdown("**Part yang stoknya di bawah threshold minimal:**")
                st.markdown("---")
                threshold_results = self.get_threshold_alerts()
                if threshold_results:
                    st.markdown(f"**🚨 {len(threshold_results)} part memerlukan perhatian:**")
                    st.dataframe(pd.DataFrame(threshold_results), width="stretch", hide_index=True,
                                 column_config={
                                     "Part Number":   st.column_config.TextColumn(width="medium"),
                                     "Part Name":     st.column_config.TextColumn(width="large"),
                                     "Stok Saat Ini": st.column_config.NumberColumn(width="small"),
                                     "Minimal Stok":  st.column_config.NumberColumn(width="small"),
                                     "Qty":           st.column_config.NumberColumn(width="small"),
                                 })
                else:
                    st.success("✅ Semua part memiliki stok mencukupi!")

        with tab4:
            self.render_batch_download_tab()

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
            st.dataframe(df_res[cols], width="stretch", hide_index=True,
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

                    # Pastikan image_links sudah di-load
                    if not hasattr(self, 'image_links') or self.image_links is None:
                        self._load_image_links()

                    # 1) Cek link dari JSON
                    img_links = self.get_image_links(pn)
                    # 2) Fallback ke file lokal
                    img_path = self.get_image_path(pn)
                    if img_path and not img_path.exists():
                        img_path = None

                    with st.expander(f"🖼️ {pn}", expanded=False):
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
                                                 width="stretch"):
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
                                                 width="stretch"):
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
                                                     width="stretch"):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = img_path.read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=f"{pn} - {pname_ex}", zoom_key=f"{pn}_local")
                        else:
                            st.caption("Tidak ada gambar tersedia")
        elif "search_term" in st.session_state and st.session_state.get("search_results") is not None:
            search_term = st.session_state.search_term
            st.warning(f"❌ Tidak ditemukan hasil untuk '{search_term}'")

            # Tetap tampilkan gambar part jika tersedia, meskipun tidak ditemukan di Excel
            if st.session_state.get("search_type") == "Part Number":
                if not hasattr(self, 'image_links') or self.image_links is None:
                    self._load_image_links()

                img_links = self.get_image_links(search_term)
                img_path  = self.get_image_path(search_term)
                if img_path and not img_path.exists():
                    img_path = None

                if img_links or img_path:
                    st.markdown("### 🖼️ Gambar Part")
                    with st.expander(f"🖼️ {search_term}", expanded=True):
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
                                                 disabled=(current_idx == 0), width="stretch"):
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
                                                 disabled=(current_idx == total - 1), width="stretch"):
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
                                                     width="stretch"):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = img_path.read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=search_term, zoom_key=f"nf_{search_term}_local")

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
