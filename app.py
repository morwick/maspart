"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING + LOGIN SYSTEM + IMAGE VIEWER
===========================================================================
Login berbasis file Excel di folder /login
- Struktur Excel: Kolom A = No, Kolom B = Username, Kolom C = Password, Kolom D = Role
- Role: 'admin' atau 'user'
- Menampilkan gambar dari folder /images jika nama file sesuai Part Number
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

warnings.filterwarnings('ignore')

# ==============================================
# KONFIGURASI AWAL
# ==============================================
st.set_page_config(
    page_title="Part Number Finder",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': None
    }
)

# ==============================================
# CSS — hide menu + styling
# ==============================================
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    
    /* ── halaman utama ── */
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1.5rem;
        padding-top: 0.8rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .search-box {
        background-color: #F5F5F5;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
    }
    .user-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: #E3F2FD;
        border: 1px solid #90CAF9;
        border-radius: 20px;
        padding: 0.3rem 0.85rem;
        font-size: 0.85rem;
        color: #1565C0;
        font-weight: 600;
    }
    .role-admin { color: #E65100; font-weight: 700; }
    .role-user  { color: #1565C0; font-weight: 600; }

    /* ── hide sidebar on login ── */
    .hide-sidebar [data-testid="stSidebar"] { display: none !important; }
    .hide-sidebar [data-testid="collapsedControl"] { display: none !important; }
</style>
""", unsafe_allow_html=True)


# ================================================
# KONSTANTA
# ================================================
SESSION_TIMEOUT_MINUTES = 30
LOGIN_FOLDER            = Path("login")
DATA_FOLDER             = Path("data")
CACHE_FOLDER            = Path(".cache")
IMAGES_FOLDER           = Path("images")  # <--- Folder Gambar


# ================================================
# LOGIN MANAGER
# ================================================
class LoginManager:
    """
    Autentikasi berbasis Excel di folder /login.
    """

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

                    # buang header kalau ada
                    first = df.iloc[0].astype(str).str.strip().str.lower().tolist()
                    if any(v in ("username", "user", "nama") for v in first):
                        df = df.iloc[1:].reset_index(drop=True)

                    # ambil kolom yang tepat
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
            return {
                "username":   username,
                "role":       row.iloc[0]["role"],
                "login_time": datetime.now(),
                "last_active": datetime.now(),
            }
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
            st.session_state["login_error"] = "⏰ Sesi telah berakhir karena tidak aktif. Silakan login ulang."
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


# ================================================
# HALAMAN LOGIN
# ================================================
def render_login_page(login_mgr: LoginManager):
    error_msg = st.session_state.get("login_error")

    st.markdown("""
        <style>
            [data-testid="stSidebar"] { display: none !important; }
            [data-testid="collapsedControl"] { display: none !important; }
        </style>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])

    with col:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("# 🔍 Part Number Finder", unsafe_allow_html=False)
        st.markdown("Silakan login untuk melanjutkan.", unsafe_allow_html=False)
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
            st.session_state["login_error"] = "Username atau password salah. Periksa kembali."
            st.rerun()


# ================================================
# APLIKASI PENCARIAN
# ================================================
class ExcelSearchApp:

    def __init__(self):
        self.data_folder  = DATA_FOLDER
        self.cache_folder = CACHE_FOLDER
        self.images_folder = IMAGES_FOLDER
        
        # Init folders
        self.cache_folder.mkdir(exist_ok=True)
        self.images_folder.mkdir(exist_ok=True) # Buat folder images jika belum ada

        if "excel_files" not in st.session_state:
            st.session_state.excel_files         = []
            st.session_state.index_data          = []
            st.session_state.last_index_time     = None
            st.session_state.search_results      = []
            st.session_state.loaded_files_count  = 0
            st.session_state.last_file_count     = 0
            st.session_state.file_hashes         = {}
            st.session_state.search_index        = {"part_number": {}, "part_name": {}}

        if not st.session_state.excel_files:
            self.auto_load_excel_files()

    # ---------- helpers ----------
    def create_data_folder(self):
        if not self.data_folder.exists():
            self.data_folder.mkdir(parents=True)

    def get_file_hash(self, file_path):
        try:
            s = file_path.stat()
            return hashlib.md5(f"{file_path}_{s.st_size}_{s.st_mtime}".encode()).hexdigest()
        except Exception:
            return None

    def load_file_cache(self, file_path, file_hash):
        cache_file = self.cache_folder / f"{file_hash}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            except Exception:
                return None
        return None

    def save_file_cache(self, file_path, file_hash, data):
        try:
            with open(self.cache_folder / f"{file_hash}.pkl", "wb") as f:
                pickle.dump(data, f)
        except Exception:
            pass

    @staticmethod
    def extract_simple_filename(filename):
        name = os.path.splitext(filename)[0]
        return name.split(" - ")[-1] if " - " in name else name

    # ---------- IMAGE HELPER (NEW) ----------
    def get_part_image_path(self, part_number):
        """Mencari gambar di folder images yang namanya sesuai part number"""
        if not part_number:
            return None
        
        # Bersihkan part number dari karakter aneh (opsional, tergantung nama file)
        # Tapi biasanya nama file = part number persis.
        clean_pn = str(part_number).strip()
        
        # Cek ekstensi umum
        for ext in [".jpg", ".jpeg", ".png", ".JPG", ".JPEG", ".PNG"]:
            img_path = self.images_folder / f"{clean_pn}{ext}"
            if img_path.exists():
                return str(img_path)
        return None

    # ---------- process file ----------
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
                    df = pd.read_excel(xls, sheet_name=sheet_name, usecols=[1, 3, 4], dtype=str)
                    df.columns = ["part_number", "part_name", "quantity"]

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
                        "full_path":         str(file_path),
                        "file_name":         file_name,
                        "relative_path":     str(relative_path),
                        "simple_name":       simple_name,
                        "sheet":             sheet_name,
                        "dataframe":         df,
                        "row_count":         len(df),
                        "col_count":         len(df.columns),
                        "part_number_index": pn_idx,
                        "part_name_index":   nm_idx,
                        "last_modified":     datetime.fromtimestamp(file_path.stat().st_mtime),
                    })
                except Exception:
                    continue
        except Exception:
            pass

        if file_hash and results:
            self.save_file_cache(file_path, file_hash, results)
        return results

    # ---------- auto-load ----------
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

            need_reindex = (
                len(all_files) != st.session_state.last_file_count
                or st.session_state.last_index_time is None
            )

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
                                        "file":          fi["simple_name"],
                                        "relative_path": fi["relative_path"],
                                        "sheet":         fi["sheet"],
                                        "rows":          fi["row_count"],
                                        "last_modified": fi["last_modified"],
                                    })
                            except Exception:
                                continue

                    st.session_state.loaded_files_count = len(st.session_state.excel_files)
                    st.session_state.last_file_count    = len(all_files)
                    st.session_state.last_index_time    = datetime.now()
                    prog.empty()
                    txt.empty()
        except Exception as e:
            st.sidebar.error(f"Error auto-load: {e}")

    # ---------- search ----------
    def search_part_number(self, term):
        results, seen = [], set()
        term_up = term.strip().upper()
        if not term_up:
            return results
        for fi in st.session_state.excel_files:
            sn = fi["simple_name"]
            if sn in seen:
                continue
            df = fi["dataframe"]
            for indexed_pn, indices in fi.get("part_number_index", {}).items():
                if term_up in indexed_pn:
                    row = df.iloc[indices[0]]
                    results.append({
                        "File":        sn,
                        "Path":        fi["relative_path"],
                        "Sheet":       fi["sheet"],
                        "Part Number": str(row["part_number"]) if pd.notna(row["part_number"]) else "N/A",
                        "Part Name":   str(row["part_name"])   if pd.notna(row["part_name"])   else "N/A",
                        "Quantity":    str(row["quantity"])    if pd.notna(row["quantity"])    else "N/A",
                        "Excel Row":   indices[0] + 2,
                        "Full Path":   fi["full_path"],
                    })
                    seen.add(sn)
                    break
        return results

    def search_part_name(self, term):
        results = []
        term_up = term.strip().upper()
        
        if not term_up:
            return results
        
        for fi in st.session_state.excel_files:
            df = fi["dataframe"]
            pni = fi.get("part_name_index", {})
            matching_indices = set()
            search_words = term_up.split()
            
            for word in pni.keys():
                for search_word in search_words:
                    if search_word in word or word in search_word:
                        matching_indices.update(pni[word])
            
            if not matching_indices and len(term_up) <= 3:
                for idx, row in df.iterrows():
                    pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
                    if term_up in pname.upper():
                        matching_indices.add(idx)
            
            for idx in matching_indices:
                row = df.iloc[idx]
                pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
                
                if term_up in pname.upper():
                    results.append({
                        "File":        fi["simple_name"],
                        "Path":        fi["relative_path"],
                        "Sheet":       fi["sheet"],
                        "Part Number": str(row["part_number"]) if pd.notna(row["part_number"]) else "N/A",
                        "Part Name":   pname if pname else "N/A",
                        "Quantity":    str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                        "Excel Row":   idx + 2,
                        "Full Path":   fi["full_path"],
                    })
        
        return results

    # ---------- UI ----------
    def display_dashboard(self):
        user = LoginManager.get_current_user()
        role = user["role"] if user else "user"

        st.markdown('<h1 class="main-header">🔍 Part Number Finder</h1>', unsafe_allow_html=True)

        # ---- SIDEBAR ----
        with st.sidebar:
            badge_cls = "role-admin" if role == "admin" else "role-user"
            st.markdown(
                f'<div class="user-badge">👤 {user["username"].title()}'
                f' — <span class="{badge_cls}">{role.upper()}</span></div>',
                unsafe_allow_html=True
            )
            st.caption(f"Login pukul {user['login_time'].strftime('%H:%M')} · Timeout {SESSION_TIMEOUT_MINUTES} min")

            if st.button("🚪 Logout", type="secondary", use_container_width=True):
                LoginManager.logout()
                for k in ("excel_files", "index_data", "search_results",
                          "last_index_time", "loaded_files_count", "last_file_count"):
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
                        st.dataframe(
                            df_users[["username", "role"]].rename(columns={"username": "Username", "role": "Role"}),
                            hide_index=True, use_container_width=True
                        )
                st.divider()

            st.markdown("### 📊 Status Sistem")
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                for cf in CACHE_FOLDER.glob("*.pkl"):
                    try:
                        cf.unlink()
                    except Exception:
                        pass
                for k in ("excel_files", "last_index_time", "last_file_count"):
                    st.session_state.pop(k, None)
                self.auto_load_excel_files()
                st.rerun()

            if st.session_state.get("last_index_time"):
                st.markdown(f"**Terakhir di-index:**\n`{st.session_state.last_index_time.strftime('%Y-%m-%d %H:%M:%S')}`")

            st.divider()
            st.markdown("### 📈 Statistik")
            st.metric("File Excel", st.session_state.get("loaded_files_count", 0))

            with st.expander("📖 Panduan"):
                st.markdown("""
                1. Letakkan file Excel di folder `data/`
                2. Letakkan gambar (JPG/PNG) di folder `images/` (nama file = Part Number).
                """)

        # ---- MAIN CONTENT ----
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["🔢 Search Part Number", "📝 Search Part Name"])

        with tab1:
            with st.form(key="search_pn_form", clear_on_submit=False):
                sn_input = st.text_input("Masukkan Part Number:", placeholder="Contoh: ABC-123", key="sn_input")
                if st.form_submit_button("🔍 Cari Part Number", type="primary", use_container_width=True):
                    if sn_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = self.search_part_number(sn_input)
                            st.session_state.search_type    = "Part Number"
                            st.session_state.search_term    = sn_input
                            st.rerun()
                    else:
                        st.warning("Masukkan part number untuk mencari.")

        with tab2:
            with st.form(key="search_name_form", clear_on_submit=False):
                name_input = st.text_input("Masukkan Part Name:", placeholder="Contoh: Bearing, Screw", key="name_input")
                if st.form_submit_button("🔍 Cari Part Name", type="primary", use_container_width=True):
                    if name_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = self.search_part_name(name_input)
                            st.session_state.search_type    = "Part Name"
                            st.session_state.search_term    = name_input
                            st.rerun()
                    else:
                        st.warning("Masukkan nama part untuk mencari.")

        st.markdown("</div>", unsafe_allow_html=True)
        self.display_search_results()

    def display_search_results(self):
        results = st.session_state.get("search_results", [])
        if results:
            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>', unsafe_allow_html=True)
            
            # --- TABEL HASIL ---
            df_res = pd.DataFrame(results)
            cols = [c for c in ["File", "Part Number", "Part Name", "Quantity", "Sheet", "Excel Row"] if c in df_res.columns]
            st.dataframe(df_res[cols], use_container_width=True, hide_index=True,
                         column_config={
                             "File":        st.column_config.TextColumn(width="medium"),
                             "Part Number": st.column_config.TextColumn(width="medium"),
                             "Part Name":   st.column_config.TextColumn(width="large"),
                             "Quantity":    st.column_config.NumberColumn(width="small"),
                             "Sheet":       st.column_config.TextColumn(width="medium"),
                             "Excel Row":   st.column_config.NumberColumn(width="small"),
                         })
            
            with st.expander("📁 Detail File"):
                for r in results:
                    st.markdown(f"**{r['File']}** — Path: `{r['Path']}` | Sheet: {r['Sheet']} | Row: {r['Excel Row']}")

            # --- BAGIAN GAMBAR (FITUR BARU) ---
            # Cari gambar untuk part number yang unik dari hasil pencarian
            unique_pns = list(set(r['Part Number'] for r in results if r['Part Number'] != "N/A"))
            
            images_found = []
            for pn in unique_pns:
                img_path = self.get_part_image_path(pn)
                if img_path:
                    images_found.append((pn, img_path))
            
            if images_found:
                st.markdown("### 🖼️ Preview Gambar")
                st.caption("Klik tombol di bawah untuk melihat gambar part.")
                
                # Tampilkan dalam grid sederhana
                cols = st.columns(3) # Maks 3 kolom per baris
                for i, (pn, path) in enumerate(images_found):
                    with cols[i % 3]:
                        # Menggunakan expander sebagai tombol toggle sederhana
                        with st.expander(f"📷 Lihat {pn}", expanded=False):
                            # FIXED: Menggunakan use_column_width agar kompatibel dengan versi Streamlit lama
                            st.image(path, caption=f"Part: {pn}", use_column_width=True)

        elif "search_term" in st.session_state and st.session_state.get("search_results") is not None:
            st.warning(f"❌ Tidak ditemukan hasil untuk '{st.session_state.search_term}'")

    def run(self):
        self.display_dashboard()


# ================================================
# MAIN
# ================================================
def main():
    LoginManager.init_session()
    login_mgr = LoginManager()

    if not LoginManager.is_authenticated():
        render_login_page(login_mgr)
    else:
        ExcelSearchApp().run()


if __name__ == "__main__":
    main()
