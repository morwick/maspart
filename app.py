"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING (OPTIMIZED VERSION)
=================================================================
Aplikasi web untuk mencari part number dan nama part di database Excel.
Optimasi:
1. Parallel processing untuk membaca file
2. Lazy loading untuk dataframe
3. Index caching
4. Optimasi pembacaan Excel
"""

import streamlit as st
import pandas as pd
import os
from pathlib import Path
import time
from datetime import datetime
import base64
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import pickle
warnings.filterwarnings('ignore')

# ==============================================
# KONFIGURASI AWAL UNTUK HIDE MENU
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
# CSS UNTUK HIDE SEMUA LOGO DI SUDUT KANAN ATAS
# ==============================================
hide_menu_style = """
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    .viewerBadge_link__qRIco {display: none !important;}
    .viewerBadge_container__r5tak {display: none !important;}
    [data-testid="collapsedControl"] {display: none !important;}
    header[data-testid="stHeader"] {display: none !important;}
  /*  div[data-testid="stToolbar"] {display: none !important;}
    div[data-testid="stToolbar"] > div {display: none !important;} */
    [title="Edit this app"] {display: none !important;}
    iframe {display: none !important;}
</style>
"""

st.markdown(hide_menu_style, unsafe_allow_html=True)

# CSS Custom untuk styling aplikasi
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
        padding-top: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .success-box {
        background-color: #E8F5E9;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #4CAF50;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #E3F2FD;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #2196F3;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #FFF3E0;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #FF9800;
        margin: 1rem 0;
    }
    .stDataFrame {
        font-size: 0.9rem;
    }
    .search-box {
        background-color: #F5F5F5;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

class ExcelSearchApp:
    """
    KELAS UTAMA APLIKASI PENCARIAN EXCEL (OPTIMIZED)
    ------------------------------------------------
    Mengelola seluruh logika pencarian dan indexing dengan optimasi kecepatan
    """
    
    def __init__(self):
        """Inisialisasi aplikasi dan setup session state"""
        self.data_folder = Path("data")
        self.stock_folder = Path("data/stok")
        self.cache_folder = Path(".cache")
        self.cache_folder.mkdir(exist_ok=True)
        self.stock_folder.mkdir(parents=True, exist_ok=True)
        
        # Inisialisasi session state
        if 'excel_files' not in st.session_state:
            st.session_state.excel_files = []
            st.session_state.stock_database = {}
            st.session_state.index_data = []
            st.session_state.last_index_time = None
            st.session_state.search_results = []
            st.session_state.loaded_files_count = 0
            st.session_state.last_file_count = 0
            st.session_state.file_hashes = {}
            st.session_state.search_index = {'part_number': {}, 'part_name': {}}
        
        # Auto-load data saat startup
        if not st.session_state.excel_files:
            self.auto_load_excel_files()
    
    def create_data_folder(self):
        """Membuat folder data jika belum ada"""
        if not self.data_folder.exists():
            self.data_folder.mkdir(parents=True)
            st.sidebar.success(f"📁 Folder 'data' dibuat di: {self.data_folder.absolute()}")
    
    def get_file_hash(self, file_path):
        """Generate hash untuk file untuk cache validation"""
        try:
            file_stat = file_path.stat()
            hash_str = f"{file_path}_{file_stat.st_size}_{file_stat.st_mtime}"
            return hashlib.md5(hash_str.encode()).hexdigest()
        except:
            return None
    
    def load_file_cache(self, file_path, file_hash):
        """Load cached file data jika tersedia"""
        cache_file = self.cache_folder / f"{file_hash}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return None
        return None
    
    def save_file_cache(self, file_path, file_hash, data):
        """Save file data to cache"""
        cache_file = self.cache_folder / f"{file_hash}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
        except:
            pass
    
    def process_single_file(self, file_path, relative_path):
        """
        OPTIMIZED: Process single Excel file dengan caching
        """
        results = []
        file_name = file_path.name
        simple_name = self.extract_simple_filename(file_name)
        
        # Check cache
        file_hash = self.get_file_hash(file_path)
        if file_hash:
            cached_data = self.load_file_cache(file_path, file_hash)
            if cached_data:
                return cached_data
        
        try:
            # Baca Excel dengan optimasi
            xls = pd.ExcelFile(file_path, engine='openpyxl')
            
            for sheet_name in xls.sheet_names:
                try:
                    # Baca hanya kolom yang diperlukan (B, D, E = index 1, 3, 4)
                    df = pd.read_excel(
                        xls, 
                        sheet_name=sheet_name,
                        usecols=[1, 3, 4],  # Hanya baca kolom B, D, E
                        dtype=str  # Read as string untuk performa lebih baik
                    )
                    
                    # Rename columns untuk konsistensi
                    df.columns = ['part_number', 'part_name', 'quantity']
                    
                    # Build search index untuk file ini
                    part_number_index = {}
                    part_name_index = {}
                    
                    for idx, row in df.iterrows():
                        part_num = str(row['part_number']).strip().upper() if pd.notna(row['part_number']) else ""
                        part_name = str(row['part_name']).strip().upper() if pd.notna(row['part_name']) else ""
                        
                        if part_num:
                            if part_num not in part_number_index:
                                part_number_index[part_num] = []
                            part_number_index[part_num].append(idx)
                        
                        if part_name:
                            # Index per kata untuk part name
                            for word in part_name.split():
                                if len(word) > 2:  # Skip kata pendek
                                    if word not in part_name_index:
                                        part_name_index[word] = []
                                    part_name_index[word].append(idx)
                    
                    file_info = {
                        'full_path': str(file_path),
                        'file_name': file_name,
                        'relative_path': str(relative_path),
                        'simple_name': simple_name,
                        'sheet': sheet_name,
                        'dataframe': df,
                        'row_count': len(df),
                        'col_count': len(df.columns),
                        'part_number_index': part_number_index,
                        'part_name_index': part_name_index,
                        'last_modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                    }
                    
                    results.append(file_info)
                    
                except Exception as e:
                    continue
        
        except Exception as e:
            pass
        
        # Cache hasil
        if file_hash and results:
            self.save_file_cache(file_path, file_hash, results)
        
        return results
    
    def auto_load_excel_files(self):
        """
        OPTIMIZED: AUTO-LOAD dengan parallel processing
        """
        try:
            self.create_data_folder()
            
            # Cari semua file Excel
            excel_extensions = ['.xlsx', '.xls', '.xlsm']
            all_files = []
            
            for root, dirs, files in os.walk(self.data_folder):
                for file in files:
                    if any(file.lower().endswith(ext) for ext in excel_extensions):
                        full_path = Path(root) / file
                        relative_path = full_path.relative_to(self.data_folder)
                        all_files.append((full_path, relative_path))
            
            if not all_files:
                st.session_state.last_file_count = 0
                return
            
            current_file_count = len(all_files)
            
            # Check jika perlu re-index
            need_reindex = (
                current_file_count != st.session_state.last_file_count or 
                st.session_state.last_index_time is None
            )
            
            if need_reindex:
                with st.spinner("🔄 Mengindeks file Excel..."):
                    st.session_state.excel_files = []
                    st.session_state.index_data = []
                    
                    progress_bar = st.progress(0)
                    progress_text = st.empty()
                    
                    # PARALLEL PROCESSING dengan ThreadPoolExecutor
                    max_workers = min(4, len(all_files))  # Maksimal 4 thread
                    completed = 0
                    
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submit semua tasks
                        future_to_file = {
                            executor.submit(self.process_single_file, fp, rp): (fp, rp) 
                            for fp, rp in all_files
                        }
                        
                        # Process hasil saat selesai
                        for future in as_completed(future_to_file):
                            completed += 1
                            progress = completed / len(all_files)
                            progress_bar.progress(progress)
                            progress_text.text(f"Processing {completed}/{len(all_files)} files...")
                            
                            try:
                                file_results = future.result()
                                if file_results:
                                    for file_info in file_results:
                                        st.session_state.excel_files.append(file_info)
                                        
                                        # Simpan untuk summary
                                        st.session_state.index_data.append({
                                            'file': file_info['simple_name'],
                                            'relative_path': file_info['relative_path'],
                                            'sheet': file_info['sheet'],
                                            'rows': file_info['row_count'],
                                            'last_modified': file_info['last_modified']
                                        })
                            except Exception as e:
                                continue
                    
                    # Update session state
                    st.session_state.loaded_files_count = len(st.session_state.excel_files)
                    st.session_state.last_file_count = current_file_count
                    st.session_state.last_index_time = datetime.now()
                    
                    # Load stock database
                    self.load_stock_database()
                    
                    progress_bar.empty()
                    progress_text.empty()
                    
        except Exception as e:
            st.sidebar.error(f"Error dalam auto-load: {str(e)}")
    
    def extract_simple_filename(self, filename):
        """Membersihkan nama file"""
        name_without_ext = os.path.splitext(filename)[0]
        if ' - ' in name_without_ext:
            return name_without_ext.split(' - ')[-1]
        return name_without_ext
    
    def load_stock_database(self):
        """
        OPTIMIZED: Load stock database dari folder data/stok/
        Membaca semua file Excel di folder stok
        Kolom A = Part Number, Kolom D = Stock
        Format: 000001.WG9160580508 → Extract WG9160580508
        """
        if not self.stock_folder.exists():
            st.sidebar.info("ℹ️ Folder 'data/stok/' tidak ditemukan. Fitur stock tidak aktif.")
            return
        
        try:
            with st.spinner("📊 Memuat database stok..."):
                # Reset stock database
                st.session_state.stock_database = {}
                
                # Cari semua file Excel di folder stok
                excel_extensions = ['.xlsx', '.xls', '.xlsm']
                stock_files = []
                
                for file in self.stock_folder.iterdir():
                    if file.is_file() and any(file.name.lower().endswith(ext) for ext in excel_extensions):
                        stock_files.append(file)
                
                if not stock_files:
                    st.sidebar.warning("⚠️ Tidak ada file Excel di folder 'data/stok/'")
                    return
                
                total_parts_loaded = 0
                
                # Process setiap file stok
                for stock_file in stock_files:
                    try:
                        # Baca hanya kolom A dan D (index 0 dan 3)
                        df = pd.read_excel(
                            stock_file,
                            usecols=[0, 3],  # Kolom A dan D
                            dtype=str,
                            engine='openpyxl'
                        )
                        
                        if df.empty:
                            continue
                        
                        # Rename columns untuk kemudahan
                        df.columns = ['part_number', 'stock']
                        
                        # Clean dan normalize data
                        df['part_number'] = df['part_number'].fillna('').str.strip()
                        df['stock'] = df['stock'].fillna('0').str.strip()
                        
                        # Remove empty part numbers
                        df = df[df['part_number'] != '']
                        
                        # Build dictionary dengan extract part setelah titik
                        for _, row in df.iterrows():
                            part_num_full = row['part_number']
                            stock_val = row['stock']
                            
                            # Skip header rows (yang mengandung kata "kode", "barang", "total", dll)
                            skip_keywords = ['kode', 'barang', 'total', 'nama', 'gudang', 'part', 'number', 'stock', 'qty']
                            if any(keyword in part_num_full.lower() for keyword in skip_keywords):
                                continue
                            
                            if part_num_full:
                                # Extract part number setelah titik (.)
                                # Format: 000001.WG9160580508 → WG9160580508
                                if '.' in part_num_full:
                                    part_num_clean = part_num_full.split('.')[-1].strip().upper()
                                else:
                                    part_num_clean = part_num_full.strip().upper()
                                
                                # Skip jika part number kosong setelah cleaning
                                if not part_num_clean:
                                    continue
                                
                                # Simpan dengan key yang clean
                                st.session_state.stock_database[part_num_clean] = stock_val
                                total_parts_loaded += 1
                        
                    except Exception as e:
                        st.sidebar.warning(f"⚠️ Error membaca {stock_file.name}: {str(e)}")
                        continue
                
                if total_parts_loaded > 0:
                    st.sidebar.success(f"✅ {total_parts_loaded} part numbers loaded dari {len(stock_files)} file stok")
                else:
                    st.sidebar.warning("⚠️ Tidak ada data stok yang berhasil dimuat")
                    
        except Exception as e:
            st.sidebar.error(f"❌ Error loading stock database: {str(e)}")
    
    def get_stock_from_database(self, part_number):
        """
        Get stock value dengan exact match lookup
        Kolom A (Part Number) → Kolom D (Stock)
        """
        if not st.session_state.stock_database:
            return "0"
        
        if not part_number or part_number == "N/A":
            return "0"
        
        # Normalize untuk matching
        search_key = part_number.strip().upper()
        
        # Exact match
        stock_value = st.session_state.stock_database.get(search_key, "0")
        
        # Debug info (uncomment untuk troubleshooting)
        # if stock_value == "0":
        #     st.sidebar.write(f"🔍 Not found: {search_key}")
        #     st.sidebar.write(f"Available keys sample: {list(st.session_state.stock_database.keys())[:5]}")
        
        return stock_value
    
    def search_part_number(self, search_term):
        """
        OPTIMIZED: Search menggunakan pre-built index
        """
        results = []
        processed_files = set()
        search_term_upper = search_term.strip().upper()
        
        if not search_term_upper:
            return results
        
        for file_info in st.session_state.excel_files:
            simple_name = file_info['simple_name']
            
            if simple_name in processed_files:
                continue
            
            # Gunakan index untuk pencarian cepat
            part_number_index = file_info.get('part_number_index', {})
            df = file_info['dataframe']
            
            # Cari di index
            found = False
            for indexed_part, row_indices in part_number_index.items():
                if search_term_upper in indexed_part:
                    # Ambil row pertama yang match
                    idx = row_indices[0]
                    row = df.iloc[idx]
                    
                    part_num = str(row['part_number']) if pd.notna(row['part_number']) else "N/A"
                    part_name = str(row['part_name']) if pd.notna(row['part_name']) else "N/A"
                    qty = str(row['quantity']) if pd.notna(row['quantity']) else "N/A"
                    stock = self.get_stock_from_database(part_num)
                    
                    results.append({
                        'File': simple_name,
                        'Path': file_info['relative_path'],
                        'Sheet': file_info['sheet'],
                        'Part Number': part_num,
                        'Part Name': part_name,
                        'Quantity': qty,
                        'Stock': stock,
                        'Excel Row': idx + 2,
                        'Full Path': file_info['full_path']
                    })
                    
                    processed_files.add(simple_name)
                    found = True
                    break
            
            if found:
                continue
        
        return results
    
    def search_part_name(self, search_term):
        """
        OPTIMIZED: Search part name menggunakan index
        """
        results = []
        processed_files = set()
        search_term_upper = search_term.strip().upper()
        
        if not search_term_upper:
            return results
        
        for file_info in st.session_state.excel_files:
            simple_name = file_info['simple_name']
            
            if simple_name in processed_files:
                continue
            
            part_name_index = file_info.get('part_name_index', {})
            df = file_info['dataframe']
            
            # Cari di index
            found_indices = set()
            for word in search_term_upper.split():
                if word in part_name_index:
                    found_indices.update(part_name_index[word])
            
            if found_indices:
                # Ambil row pertama yang match
                idx = min(found_indices)
                row = df.iloc[idx]
                
                part_num = str(row['part_number']) if pd.notna(row['part_number']) else "N/A"
                part_name = str(row['part_name']) if pd.notna(row['part_name']) else "N/A"
                qty = str(row['quantity']) if pd.notna(row['quantity']) else "N/A"
                stock = self.get_stock_from_database(part_num)
                
                # Verify match
                if search_term_upper in part_name.upper():
                    results.append({
                        'File': simple_name,
                        'Path': file_info['relative_path'],
                        'Sheet': file_info['sheet'],
                        'Part Number': part_num,
                        'Part Name': part_name,
                        'Quantity': qty,
                        'Stock': stock,
                        'Excel Row': idx + 2,
                        'Full Path': file_info['full_path']
                    })
                    
                    processed_files.add(simple_name)
        
        return results
    
    def display_dashboard(self):
        """Menampilkan dashboard utama"""
        st.markdown('<h1 class="main-header">🔍 Part Number Finder</h1>', unsafe_allow_html=True)
        
        # Sidebar
        with st.sidebar:
            st.markdown("### 📊 Status Sistem")
            
            # Debug mode toggle
            debug_mode = st.checkbox("🐛 Debug Mode", value=False, help="Tampilkan info debugging untuk troubleshooting stock")
            
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                # Clear cache
                for cache_file in self.cache_folder.glob("*.pkl"):
                    try:
                        cache_file.unlink()
                    except:
                        pass
                self.auto_load_excel_files()
                st.rerun()
            
            if st.session_state.last_index_time:
                st.markdown(f"**Terakhir di-index:**")
                st.markdown(f"`{st.session_state.last_index_time.strftime('%Y-%m-%d %H:%M:%S')}`")
            
            st.markdown("---")
            
            st.markdown("### 📈 Statistik")
            st.metric("File Excel", st.session_state.loaded_files_count)
            
            if st.session_state.stock_database:
                st.metric("Part di Database Stok", len(st.session_state.stock_database))
                
                # Debug info
                if debug_mode and st.session_state.stock_database:
                    with st.expander("🔍 Debug: Sample Stock Data"):
                        sample_keys = list(st.session_state.stock_database.keys())[:10]
                        for key in sample_keys:
                            st.text(f"{key} → {st.session_state.stock_database[key]}")
                        if len(st.session_state.stock_database) > 10:
                            st.text(f"... dan {len(st.session_state.stock_database) - 10} lainnya")
            
            st.markdown("---")
            st.markdown("### 📁 Struktur Folder")
            st.info(f"""
            **Data Excel:**
            ```
            {self.data_folder.absolute()}
            ```
            
            **Database Stok:**
            ```
            {self.stock_folder.absolute()}
            ```
            
            Format stok: Kolom A = Part Number, Kolom D = Stock
            """)
            
            with st.expander("📖 Panduan Cepat"):
                st.markdown("""
                1. **Letakkan file Excel** di folder `data/`
                2. **Format file**: .xlsx, .xls, .xlsm
                3. **Pencarian Part Number**: Mencari di kolom B
                4. **Pencarian Part Name**: Mencari di kolom D
                5. **Database Stok**: Letakkan file Excel di folder `data/stok/`
                   - Kolom A: Part Number
                   - Kolom D: Stock
                
                **Optimasi:**
                - ✅ Parallel file processing
                - ✅ Smart caching
                - ✅ Index-based search
                - ✅ Multi-file stock support
                """)
        
        # Main content
        col2 = st.columns(1)[0]
        
        with col2:
            st.markdown('<div class="search-box">', unsafe_allow_html=True)
            st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)
            
            tab1, tab2 = st.tabs(["🔢 Search Part Number", "📝 Search Part Name"])
            
            with tab1:
                with st.form(key="search_part_number_form", clear_on_submit=False):
                    search_number = st.text_input(
                        "Masukkan Part Number:",
                        placeholder="Contoh: ABC-123, XYZ789",
                        key="search_part_number_input"
                    )
                    
                    submit_button = st.form_submit_button("🔍 Cari Part Number", type="primary", use_container_width=True)
                    
                    if submit_button:
                        if search_number:
                            with st.spinner("Mencari..."):
                                results = self.search_part_number(search_number)
                                st.session_state.search_results = results
                                st.session_state.search_type = "Part Number"
                                st.session_state.search_term = search_number
                                st.rerun()  # Force immediate update
                        else:
                            st.warning("Masukkan part number untuk mencari")
            
            with tab2:
                with st.form(key="search_part_name_form", clear_on_submit=False):
                    search_name = st.text_input(
                        "Masukkan Part Name:",
                        placeholder="Contoh: Bearing, Screw, Motor",
                        key="search_part_name_input"
                    )
                    
                    submit_button = st.form_submit_button("🔍 Cari Part Name", type="primary", use_container_width=True)
                    
                    if submit_button:
                        if search_name:
                            with st.spinner("Mencari..."):
                                results = self.search_part_name(search_name)
                                st.session_state.search_results = results
                                st.session_state.search_type = "Part Name"
                                st.session_state.search_term = search_name
                                st.rerun()  # Force immediate update
                        else:
                            st.warning("Masukkan nama part untuk mencari")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        self.display_search_results()
    
    def display_search_results(self):
        """Menampilkan hasil pencarian"""
        if 'search_results' in st.session_state and st.session_state.search_results:
            results = st.session_state.search_results
            
            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>', unsafe_allow_html=True)
            
            df_results = pd.DataFrame(results)
            
            display_cols = ['File', 'Part Number', 'Part Name', 'Quantity', 'Stock', 'Sheet', 'Excel Row']
            available_cols = [col for col in display_cols if col in df_results.columns]
            
            if available_cols:
                st.dataframe(
                    df_results[available_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "File": st.column_config.TextColumn(width="medium"),
                        "Part Number": st.column_config.TextColumn(width="medium"),
                        "Part Name": st.column_config.TextColumn(width="large"),
                        "Quantity": st.column_config.NumberColumn(width="small"),
                        "Stock": st.column_config.TextColumn(width="small"),
                        "Sheet": st.column_config.TextColumn(width="medium"),
                        "Excel Row": st.column_config.NumberColumn(width="small")
                    }
                )
            
            with st.expander("📁 Detail File yang Ditemukan"):
                for result in results:
                    st.markdown(f"""
                    **File**: {result['File']}
                    - **Path**: `{result['Path']}`
                    - **Sheet**: {result['Sheet']}
                    - **Row**: {result['Excel Row']}
                    """)
        
        elif 'search_results' in st.session_state and not st.session_state.search_results:
            if 'search_term' in st.session_state:
                st.warning(f"❌ Tidak ditemukan hasil untuk '{st.session_state.search_term}'")
    
    def run(self):
        """Menjalankan aplikasi"""
        self.display_dashboard()

def main():
    """Fungsi utama untuk menjalankan aplikasi"""
    app = ExcelSearchApp()
    app.run()

if __name__ == "__main__":
    main()

