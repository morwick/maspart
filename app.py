"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING
==============================================
Aplikasi web untuk mencari part number dan nama part di database Excel.
Fitur utama:
1. Auto-load semua file Excel dari folder 'data' saat startup
2. Auto-reload ketika file baru ditambahkan
3. Pencarian Part Number (Kolom B)
4. Pencarian Part Name (Kolom D)
5. Stock database integration
6. Export hasil
"""

import streamlit as st
import pandas as pd
import os
from pathlib import Path
import time
from datetime import datetime
import base64
import warnings
warnings.filterwarnings('ignore')

# ==============================================
# KONFIGURASI AWAL UNTUK HIDE MENU
# HARUS DIPANGGIL DI BARIS PALING AWAL
# ==============================================
st.set_page_config(
    page_title="Excel Part Search Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,        # Hapus menu "Get Help"
        'Report a bug': None,    # Hapus menu "Report a bug"
        'About': None            # Hapus menu "About"
    }
)

# ==============================================
# CSS UNTUK HIDE SEMUA LOGO DI SUDUT KANAN ATAS
# ==============================================
hide_menu_style = """
<style>
    /* Hide hamburger menu (tiga garis) */
    #MainMenu {visibility: hidden;}
    
    /* Hide footer */
    footer {visibility: hidden;}
    
    /* Hide deploy button (logo bintang) */
    .stDeployButton {display: none !important;}
    
    /* Hide GitHub corner logo */
    .viewerBadge_link__qRIco {
        display: none !important;
    }
    
    /* Hide container for badges */
    .viewerBadge_container__r5tak {
        display: none !important;
    }
    
    /* Hide "Made with Streamlit" */
    .viewerBadge_container__r5tak {
        display: none !important;
    }
    
    /* Hide the three dots menu completely */
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    
    /* Hide app header */
    header[data-testid="stHeader"] {
        display: none !important;
    }
    
    /* Hide all buttons in header */
    div[data-testid="stToolbar"] {
        display: none !important;
    }
    
    /* Hide any remaining elements in top right corner */
    div[data-testid="stToolbar"] > div {
        display: none !important;
    }
    
    /* Hide edit app button */
    [title="Edit this app"] {
        display: none !important;
    }
    
    /* Hide any iframe from Streamlit sharing */
    iframe {
        display: none !important;
    }
</style>
"""

# Terapkan CSS
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
    KELAS UTAMA APLIKASI PENCARIAN EXCEL
    ------------------------------------
    Mengelola seluruh logika pencarian dan indexing
    """
    
    def __init__(self):
        """Inisialisasi aplikasi dan setup session state"""
        self.data_folder = Path("data")  # Folder default untuk file Excel
        self.stock_file = Path("stock_db.xlsx")  # File database stok
        
        # Inisialisasi session state jika belum ada
        if 'excel_files' not in st.session_state:
            st.session_state.excel_files = []
            st.session_state.stock_database = {}
            st.session_state.index_data = []
            st.session_state.last_index_time = None
            st.session_state.search_results = []
            st.session_state.loaded_files_count = 0
            st.session_state.last_file_count = 0
        
        # Auto-load data saat startup
        if not st.session_state.excel_files:
            self.auto_load_excel_files()
    
    def create_data_folder(self):
        """Membuat folder data jika belum ada"""
        if not self.data_folder.exists():
            self.data_folder.mkdir(parents=True)
            st.sidebar.success(f"📁 Folder 'data' dibuat di: {self.data_folder.absolute()}")
    
    def auto_load_excel_files(self):
        """
        AUTO-LOAD SEMUA FILE EXCEL
        --------------------------
        Secara otomatis membaca semua file Excel dari folder 'data'
        dan subfolder di dalamnya.
        """
        try:
            # Pastikan folder data ada
            self.create_data_folder()
            
            # Cari semua file Excel secara rekursif
            excel_extensions = ['.xlsx', '.xls', '.xlsm']
            all_files = []
            
            # Walk through semua subfolder
            for root, dirs, files in os.walk(self.data_folder):
                for file in files:
                    if any(file.lower().endswith(ext) for ext in excel_extensions):
                        full_path = Path(root) / file
                        relative_path = full_path.relative_to(self.data_folder)
                        all_files.append((full_path, relative_path))
            
            if not all_files:
                st.session_state.last_file_count = 0
                return
            
            # Reset data jika ada file baru atau perubahan
            current_file_count = len(all_files)
            if (current_file_count != st.session_state.last_file_count or 
                st.session_state.last_index_time is None):
                
                with st.spinner("🔄 Mengindeks file Excel..."):
                    st.session_state.excel_files = []
                    st.session_state.index_data = []
                    
                    # Progress bar
                    progress_bar = st.progress(0)
                    total_files = len(all_files)
                    
                    for idx, (file_path, relative_path) in enumerate(all_files):
                        try:
                            # Baca file Excel
                            file_name = file_path.name
                            simple_name = self.extract_simple_filename(file_name)
                            
                            # Baca semua sheet dalam file
                            xls = pd.ExcelFile(file_path)
                            
                            for sheet_name in xls.sheet_names:
                                try:
                                    df = pd.read_excel(xls, sheet_name=sheet_name)
                                    
                                    # Simpan data ke session state
                                    file_info = {
                                        'full_path': str(file_path),
                                        'file_name': file_name,
                                        'relative_path': str(relative_path),
                                        'simple_name': simple_name,
                                        'sheet': sheet_name,
                                        'dataframe': df,
                                        'row_count': len(df),
                                        'col_count': len(df.columns) if not df.empty else 0
                                    }
                                    
                                    st.session_state.excel_files.append(file_info)
                                    
                                    # Simpan data untuk index summary
                                    st.session_state.index_data.append({
                                        'file': simple_name,
                                        'relative_path': str(relative_path),
                                        'sheet': sheet_name,
                                        'rows': len(df),
                                        'columns': list(df.columns),
                                        'last_modified': datetime.fromtimestamp(file_path.stat().st_mtime)
                                    })
                                    
                                except Exception as e:
                                    st.sidebar.warning(f"Error membaca sheet {sheet_name} di {file_name}: {str(e)}")
                                    
                        except Exception as e:
                            st.sidebar.warning(f"Error membaca file {file_path.name}: {str(e)}")
                        
                        # Update progress bar
                        progress = (idx + 1) / total_files
                        progress_bar.progress(progress)
                    
                    # Update session state
                    st.session_state.loaded_files_count = len(st.session_state.excel_files)
                    st.session_state.last_file_count = current_file_count
                    st.session_state.last_index_time = datetime.now()
                    
                    # Load stock database jika ada
                    self.load_stock_database()
                    
                    progress_bar.empty()
                    
        except Exception as e:
            st.sidebar.error(f"Error dalam auto-load: {str(e)}")
    
    def extract_simple_filename(self, filename):
        """
        Membersihkan nama file
        Contoh: "ABC - Part List.xlsx" menjadi "Part List"
        """
        name_without_ext = os.path.splitext(filename)[0]
        if ' - ' in name_without_ext:
            return name_without_ext.split(' - ')[-1]
        return name_without_ext
    
    def load_stock_database(self):
        """
        Memuat database stok jika file tersedia
        """
        if self.stock_file.exists():
            try:
                with st.spinner("📊 Memuat database stok..."):
                    df = pd.read_excel(self.stock_file)
                    
                    # Reset stock database
                    st.session_state.stock_database = {}
                    
                    # Asumsi: Kolom A = Part Number, Kolom AG = Stock
                    for _, row in df.iterrows():
                        if len(row) > 0:
                            part_number = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                            
                            if len(row) > 32:  # Kolom AG (indeks 32)
                                stock_value = row.iloc[32]
                                stock_str = str(stock_value) if pd.notna(stock_value) else "0"
                            else:
                                stock_str = "0"
                            
                            if part_number and part_number.strip():
                                key = part_number.strip().upper()
                                st.session_state.stock_database[key] = stock_str.strip()
                                
            except Exception as e:
                st.sidebar.warning(f"⚠️ Tidak dapat memuat database stok: {str(e)}")
    
    def get_stock_from_database(self, part_number):
        """
        Mendapatkan nilai stok untuk part number tertentu
        """
        if not st.session_state.stock_database:
            return "0"
        
        search_key = part_number.strip().upper()
        
        # Exact match
        if search_key in st.session_state.stock_database:
            return st.session_state.stock_database[search_key]
        
        # Partial match
        for key, value in st.session_state.stock_database.items():
            if search_key in key or key in search_key:
                return value
        
        return "0"
    
    def search_part_number(self, search_term):
        """
        Mencari part number (di kolom B/indeks 1)
        Hanya mengambil 1 hasil per file
        """
        results = []
        processed_files = set()
        
        search_term_lower = search_term.strip().lower()
        
        if not search_term_lower:
            return results
        
        for file_info in st.session_state.excel_files:
            df = file_info['dataframe']
            simple_name = file_info['simple_name']
            
            # Skip file yang sudah memberikan hasil
            if simple_name in processed_files:
                continue
            
            # Pastikan dataframe memiliki cukup kolom
            if len(df.columns) < 5:  # Minimal sampai kolom E
                continue
            
            try:
                # Cari di kolom B (indeks 1)
                for idx, row in df.iterrows():
                    if len(row) > 1:
                        cell_value = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                        
                        if search_term_lower in cell_value.lower():
                            # Ambil data dari kolom yang relevan
                            part_num = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else "N/A"
                            part_name = str(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else "N/A"
                            qty = str(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else "N/A"
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
                            break  # Hanya ambil 1 hasil per file
                            
            except Exception as e:
                continue
        
        return results
    
    def search_part_name(self, search_term):
        """
        Mencari part name (di kolom D/indeks 3)
        Hanya mengambil 1 hasil per file
        """
        results = []
        processed_files = set()
        
        search_term_lower = search_term.strip().lower()
        
        if not search_term_lower:
            return results
        
        for file_info in st.session_state.excel_files:
            df = file_info['dataframe']
            simple_name = file_info['simple_name']
            
            # Skip file yang sudah memberikan hasil
            if simple_name in processed_files:
                continue
            
            # Pastikan dataframe memiliki cukup kolom
            if len(df.columns) < 4:  # Minimal sampai kolom D
                continue
            
            try:
                # Cari di kolom D (indeks 3)
                for idx, row in df.iterrows():
                    if len(row) > 3:
                        cell_value = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
                        
                        if search_term_lower in cell_value.lower():
                            # Ambil data dari kolom yang relevan
                            part_num = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else "N/A"
                            part_name = str(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else "N/A"
                            qty = str(row.iloc[4]) if len(row) > 4 and pd.notna(row.iloc[4]) else "N/A"
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
                            break  # Hanya ambil 1 hasil per file
                            
            except Exception as e:
                continue
        
        return results
    
    def display_dashboard(self):
        """Menampilkan dashboard utama"""
        st.markdown('<h1 class="main-header">🔍 Excel Part Search Tool</h1>', unsafe_allow_html=True)
        
        # Sidebar - Status dan Info
        with st.sidebar:
            st.markdown("### 📊 Status Sistem")
            
            # Tombol refresh manual
            if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
                self.auto_load_excel_files()
                st.rerun()
            
            # Info indexing
            if st.session_state.last_index_time:
                st.markdown(f"**Terakhir di-index:**")
                st.markdown(f"`{st.session_state.last_index_time.strftime('%Y-%m-%d %H:%M:%S')}`")
            
            st.markdown("---")
            
            # Statistik
            st.markdown("### 📈 Statistik")
            st.metric("File Excel", st.session_state.loaded_files_count)
            
            if st.session_state.stock_database:
                st.metric("Part di Database Stok", len(st.session_state.stock_database))
            
            # Info folder
            st.markdown("---")
            st.markdown("### 📁 Struktur Folder")
            st.info(f"""
            Aplikasi akan secara otomatis membaca semua file Excel dari:
            ```
            {self.data_folder.absolute()}
            ```
            """)
            
            # Panduan cepat
            with st.expander("📖 Panduan Cepat"):
                st.markdown("""
                1. **Letakkan file Excel** di folder `data/`
                2. **Format file**: .xlsx, .xls, .xlsm
                3. **Pencarian Part Number**: Mencari di kolom B
                4. **Pencarian Part Name**: Mencari di kolom D
                5. **Database Stok**: Letakkan `stock_db.xlsx` di root folder
                """)
        
        # Main content area
        col2 = st.columns(1)[0]
        
        with col2:
            # Box pencarian
            st.markdown('<div class="search-box">', unsafe_allow_html=True)
            st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)
            
            # Tab untuk tipe pencarian
            tab1, tab2 = st.tabs(["🔢 Search Part Number", "📝 Search Part Name"])
            
            with tab1:
                search_number = st.text_input(
                    "Masukkan Part Number:",
                    placeholder="Contoh: ABC-123, XYZ789",
                    key="search_part_number_input"
                )
                
                if st.button("🔍 Cari Part Number", type="primary", use_container_width=True):
                    if search_number:
                        with st.spinner("Mencari..."):
                            results = self.search_part_number(search_number)
                            st.session_state.search_results = results
                            st.session_state.search_type = "Part Number"
                            st.session_state.search_term = search_number
                    else:
                        st.warning("Masukkan part number untuk mencari")
            
            with tab2:
                search_name = st.text_input(
                    "Masukkan Part Name:",
                    placeholder="Contoh: Bearing, Screw, Motor",
                    key="search_part_name_input"
                )
                
                if st.button("🔍 Cari Part Name", type="primary", use_container_width=True):
                    if search_name:
                        with st.spinner("Mencari..."):
                            results = self.search_part_name(search_name)
                            st.session_state.search_results = results
                            st.session_state.search_type = "Part Name"
                            st.session_state.search_term = search_name
                    else:
                        st.warning("Masukkan nama part untuk mencari")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Tampilkan hasil pencarian
        self.display_search_results()
    
    def display_search_results(self):
        """Menampilkan hasil pencarian"""
        if 'search_results' in st.session_state and st.session_state.search_results:
            results = st.session_state.search_results
            
            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>', unsafe_allow_html=True)
            
            # Convert to DataFrame untuk display yang lebih baik
            df_results = pd.DataFrame(results)
            
            # Hapus kolom yang tidak perlu untuk display
            display_cols = ['File', 'Part Number', 'Part Name', 'Quantity', 'Stock', 'Sheet', 'Excel Row']
            available_cols = [col for col in display_cols if col in df_results.columns]
            
            if available_cols:
                # Tampilkan DataFrame dengan formatting
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
            
            # Detail file yang ditemukan
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
    
    def display_export_options(self):
        """Menampilkan opsi export"""
        results = st.session_state.search_results
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📄 Export ke Excel", use_container_width=True):
                self.export_to_excel(results)
        
        with col2:
            if st.button("📝 Export ke CSV", use_container_width=True):
                self.export_to_csv(results)
    
    def export_to_excel(self, results):
        """Export results to Excel"""
        try:
            df = pd.DataFrame(results)
            
            # Create download link
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"search_results_{timestamp}.xlsx"
            
            # Save to bytes
            output = pd.ExcelWriter(filename, engine='openpyxl')
            df.to_excel(output, index=False, sheet_name='Results')
            output.close()
            
            with open(filename, 'rb') as f:
                data = f.read()
            
            # Create download button
            b64 = base64.b64encode(data).decode()
            href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}">📥 Klik untuk download Excel</a>'
            st.markdown(href, unsafe_allow_html=True)
            
            # Clean up
            os.remove(filename)
            
        except Exception as e:
            st.error(f"Error exporting to Excel: {str(e)}")
    
    def export_to_csv(self, results):
        """Export results to CSV"""
        try:
            df = pd.DataFrame(results)
            
            # Create download link
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"search_results_{timestamp}.csv"
            
            # Save to bytes
            csv = df.to_csv(index=False, encoding='utf-8')
            b64 = base64.b64encode(csv.encode()).decode()
            
            # Create download button
            href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">📥 Klik untuk download CSV</a>'
            st.markdown(href, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Error exporting to CSV: {str(e)}")
    
    def run(self):
        """Menjalankan aplikasi"""
        self.display_dashboard()
        
        # Auto-check for new files setiap 30 detik
        if st.session_state.last_index_time:
            time_diff = (datetime.now() - st.session_state.last_index_time).seconds
            if time_diff > 30:  # Auto-refresh setiap 30 detik
                self.auto_load_excel_files()

def main():
    """Fungsi utama untuk menjalankan aplikasi"""
    app = ExcelSearchApp()
    app.run()

if __name__ == "__main__":
    main()
