"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING + LOGIN SYSTEM + THRESHOLD + BATCH DOWNLOAD + EDIT POPULASI (GitHub Sync)
=============================================================
"""

import streamlit as st
import streamlit.components.v1 as _stc
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

# ── GitHub Sync ─────────────────────────────────────────────────────
try:
    from github_sync import gh_read_json, gh_write_json, gh_is_configured
    GITHUB_SYNC_ENABLED = True
except ImportError:
    GITHUB_SYNC_ENABLED = False
    def gh_read_json(path): return None
    def gh_write_json(path, content, commit_msg=""): return False
    def gh_is_configured(): return False

# ── Admin Stok Uploader ──────────────────────────────────────────────
try:
    from admin_stok_uploader import render_stok_uploader_tab
    STOK_UPLOADER_ENABLED = True
except ImportError:
    STOK_UPLOADER_ENABLED = False
    def render_stok_uploader_tab():
        st.warning("⚠️ `admin_stok_uploader.py` tidak ditemukan.")


warnings.filterwarnings('ignore')

# ── Admin Menu Control (inline — tidak perlu file terpisah) ─────────────────

# Semua tab yang tersedia
ALL_MENU_TABS: dict = {
    "tab_search_pn":   "🔢 Search Part Number",
    "tab_search_name": "📝 Search Part Name",
    "tab_batch":       "📥 Batch Download",
    "tab_populasi":    "🚛 Populasi Unit",
    "tab_harga":       "💰 Harga",
}

# Tab yang SELALU aktif (tidak bisa dinonaktifkan admin)
ALWAYS_ALLOWED: set = {"tab_search_pn"}

# File penyimpanan konfigurasi akses menu
MENU_CONFIG_FILE = Path("login/menu_permissions.json")

# ── Konfigurasi Akses Kolom ─────────────────────────────────────────────────

# Kolom yang dapat dikontrol aksesnya oleh admin
ALL_COLUMN_ACCESS: dict = {
    "col_stok":  "📦 Kolom Stok",
    "col_harga": "💲 Kolom Harga",
}

# File penyimpanan konfigurasi akses kolom
COLUMN_CONFIG_FILE = Path("login/column_permissions.json")

# ── Konfigurasi Akses Sub-Tab Harga ─────────────────────────────────────────

# Sub-tab yang ada di dalam Tab Harga
ALL_HARGA_SUBTABS: dict = {
    "subtab_list_harga":  "📋 List Harga",
    "subtab_cari_harga":  "🔍 Cari Harga",
    "subtab_batch_harga": "📥 Batch Cari Harga",
}

# File penyimpanan konfigurasi akses sub-tab harga
HARGA_SUBTAB_CONFIG_FILE = Path("login/harga_subtab_permissions.json")


class HargaSubTabManager:
    """
    Mengelola izin akses sub-tab List Harga dan Cari Harga per username.
    Disimpan di login/harga_subtab_permissions.json.
    Format: {"username": ["subtab_list_harga", "subtab_cari_harga"], "__default__": [...]}
    """
    _CACHE_KEY = "_harga_subtab_permissions_cache"

    @classmethod
    def load_permissions(cls, force: bool = False) -> dict:
        if not force and cls._CACHE_KEY in st.session_state:
            return st.session_state[cls._CACHE_KEY]
        data = {}
        # 1. Coba dari GitHub dulu
        gh_data = gh_read_json("login/harga_subtab_permissions.json")
        if gh_data is not None:
            data = gh_data
            # Sync ke lokal
            try:
                HARGA_SUBTAB_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(HARGA_SUBTAB_CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
        else:
            # 2. Fallback ke file lokal
            if HARGA_SUBTAB_CONFIG_FILE.exists():
                try:
                    with open(HARGA_SUBTAB_CONFIG_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
        if "__default__" not in data:
            data["__default__"] = list(ALL_HARGA_SUBTABS.keys())
        st.session_state[cls._CACHE_KEY] = data
        return data

    @classmethod
    def save_permissions(cls, permissions: dict) -> tuple:
        path = HARGA_SUBTAB_CONFIG_FILE
        try:
            # Simpan lokal
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(permissions, f, indent=2, ensure_ascii=False)
            st.session_state[cls._CACHE_KEY] = permissions
            # Sync ke GitHub
            if not gh_write_json("login/harga_subtab_permissions.json",
                                 permissions, "Update harga subtab permissions"):
                if gh_is_configured():
                    st.toast("⚠️ Tersimpan lokal, gagal sync ke GitHub.", icon="⚠️")
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_user_subtabs(cls, username: str) -> list:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            return perms[uname]
        return perms.get("__default__", list(ALL_HARGA_SUBTABS.keys()))

    @classmethod
    def set_user_subtabs(cls, username: str, subtab_keys: list) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        final = [sk for sk in subtab_keys if sk in ALL_HARGA_SUBTABS]
        perms[uname] = final
        return cls.save_permissions(perms)

    @classmethod
    def set_default_subtabs(cls, subtab_keys: list) -> tuple:
        perms = cls.load_permissions()
        final = [sk for sk in subtab_keys if sk in ALL_HARGA_SUBTABS]
        perms["__default__"] = final
        return cls.save_permissions(perms)

    @classmethod
    def remove_user_config(cls, username: str) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            del perms[uname]
            return cls.save_permissions(perms)
        return True, None


def get_allowed_harga_subtabs(username: str, role: str) -> set:
    """Admin selalu mendapat semua sub-tab harga. User lain sesuai konfigurasi."""
    if role == "admin":
        return set(ALL_HARGA_SUBTABS.keys())
    return set(HargaSubTabManager.get_user_subtabs(username))


class ColumnAccessManager:
    """
    Mengelola izin tampil kolom Stok dan Harga per username.
    Disimpan di login/column_permissions.json.
    Format: {"username": ["col_stok", "col_harga"], "__default__": ["col_stok", "col_harga"]}
    """
    _CACHE_KEY = "_column_permissions_cache"

    @classmethod
    def load_permissions(cls, force: bool = False) -> dict:
        if not force and cls._CACHE_KEY in st.session_state:
            return st.session_state[cls._CACHE_KEY]
        data = {}
        # 1. Coba dari GitHub dulu
        gh_data = gh_read_json("login/column_permissions.json")
        if gh_data is not None:
            data = gh_data
            try:
                COLUMN_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(COLUMN_CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
        else:
            # 2. Fallback ke file lokal
            if COLUMN_CONFIG_FILE.exists():
                try:
                    with open(COLUMN_CONFIG_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
        if "__default__" not in data:
            data["__default__"] = list(ALL_COLUMN_ACCESS.keys())
        st.session_state[cls._CACHE_KEY] = data
        return data

    @classmethod
    def save_permissions(cls, permissions: dict) -> tuple:
        path = COLUMN_CONFIG_FILE
        try:
            # Simpan lokal
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(permissions, f, indent=2, ensure_ascii=False)
            st.session_state[cls._CACHE_KEY] = permissions
            # Sync ke GitHub
            if not gh_write_json("login/column_permissions.json",
                                 permissions, "Update column permissions"):
                if gh_is_configured():
                    st.toast("⚠️ Tersimpan lokal, gagal sync ke GitHub.", icon="⚠️")
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_user_columns(cls, username: str) -> list:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            return perms[uname]
        return perms.get("__default__", list(ALL_COLUMN_ACCESS.keys()))

    @classmethod
    def set_user_columns(cls, username: str, col_keys: list) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        final = [ck for ck in col_keys if ck in ALL_COLUMN_ACCESS]
        perms[uname] = final
        return cls.save_permissions(perms)

    @classmethod
    def set_default_columns(cls, col_keys: list) -> tuple:
        perms = cls.load_permissions()
        final = [ck for ck in col_keys if ck in ALL_COLUMN_ACCESS]
        perms["__default__"] = final
        return cls.save_permissions(perms)

    @classmethod
    def remove_user_config(cls, username: str) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            del perms[uname]
            return cls.save_permissions(perms)
        return True, None


def get_allowed_columns(username: str, role: str) -> set:
    """
    Kembalikan set col_key yang boleh ditampilkan untuk user ini.
    Admin selalu mendapatkan semua kolom.
    """
    if role == "admin":
        return set(ALL_COLUMN_ACCESS.keys())
    return set(ColumnAccessManager.get_user_columns(username))


class MenuAccessManager:
    """
    Mengelola izin akses menu per username.
    Disimpan di login/menu_permissions.json.
    Format: {"username": ["tab_key", ...], "__default__": ["tab_key", ...]}
    """
    _CACHE_KEY = "_menu_permissions_cache"

    @classmethod
    def load_permissions(cls, force: bool = False) -> dict:
        if not force and cls._CACHE_KEY in st.session_state:
            return st.session_state[cls._CACHE_KEY]
        data = {}
        # 1. Coba dari GitHub dulu
        gh_data = gh_read_json("login/menu_permissions.json")
        if gh_data is not None:
            data = gh_data
            try:
                MENU_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(MENU_CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
        else:
            # 2. Fallback ke file lokal
            if MENU_CONFIG_FILE.exists():
                try:
                    with open(MENU_CONFIG_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
        if "__default__" not in data:
            data["__default__"] = list(ALL_MENU_TABS.keys())
        st.session_state[cls._CACHE_KEY] = data
        return data

    @classmethod
    def save_permissions(cls, permissions: dict) -> tuple:
        path = MENU_CONFIG_FILE
        try:
            # Simpan lokal
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(permissions, f, indent=2, ensure_ascii=False)
            st.session_state[cls._CACHE_KEY] = permissions
            # Sync ke GitHub
            if not gh_write_json("login/menu_permissions.json",
                                 permissions, "Update menu permissions"):
                if gh_is_configured():
                    st.toast("⚠️ Tersimpan lokal, gagal sync ke GitHub.", icon="⚠️")
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_user_tabs(cls, username: str) -> list:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            allowed = perms[uname]
        else:
            allowed = perms.get("__default__", list(ALL_MENU_TABS.keys()))
        result = list(ALWAYS_ALLOWED)
        for tab in allowed:
            if tab in ALL_MENU_TABS and tab not in result:
                result.append(tab)
        return result

    @classmethod
    def set_user_tabs(cls, username: str, tab_keys: list) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        final = list(ALWAYS_ALLOWED)
        for tk in tab_keys:
            if tk in ALL_MENU_TABS and tk not in final:
                final.append(tk)
        perms[uname] = final
        return cls.save_permissions(perms)

    @classmethod
    def set_default_tabs(cls, tab_keys: list) -> tuple:
        perms = cls.load_permissions()
        final = list(ALWAYS_ALLOWED)
        for tk in tab_keys:
            if tk in ALL_MENU_TABS and tk not in final:
                final.append(tk)
        perms["__default__"] = final
        return cls.save_permissions(perms)

    @classmethod
    def remove_user_config(cls, username: str) -> tuple:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            del perms[uname]
            return cls.save_permissions(perms)
        return True, None

    @classmethod
    def get_all_configured_users(cls) -> list:
        perms = cls.load_permissions()
        return [k for k in perms.keys() if k != "__default__"]


def get_allowed_tabs(username: str, role: str) -> list:
    """Admin selalu dapat semua tab. User lain sesuai konfigurasi."""
    if role == "admin":
        return list(ALL_MENU_TABS.keys()) + ["tab_menu_control", "tab_stok_upload"]
    return MenuAccessManager.get_user_tabs(username)


def render_admin_menu_control_tab():
    """UI tab 🛡️ Menu Control — hanya tampil untuk admin."""
    st.markdown("### 🛡️ Kontrol Akses per User")
    st.caption(
        "Atur akses menu, kolom, dan sub-tab harga untuk masing-masing user. "
        "Tab **Search Part Number** selalu aktif dan tidak bisa dinonaktifkan."
    )

    df_users: pd.DataFrame = st.session_state.get("login_users_df", pd.DataFrame())
    if df_users.empty:
        st.warning("⚠️ Data user belum dimuat. Klik **Reload Users** di sidebar terlebih dahulu.")
        return

    non_admin_users = df_users[df_users["role"] != "admin"]["username"].tolist()
    if not non_admin_users:
        st.info("Tidak ada user non-admin yang terdaftar.")
        return

    perms      = MenuAccessManager.load_permissions()
    col_perms  = ColumnAccessManager.load_permissions()
    hs_perms   = HargaSubTabManager.load_permissions()
    tab_items  = list(ALL_MENU_TABS.items())
    col_items  = list(ALL_COLUMN_ACCESS.items())
    hs_items   = list(ALL_HARGA_SUBTABS.items())

    selected_user = st.selectbox("Pilih Username:", options=non_admin_users, key="mac_sel_user")

    if not selected_user:
        return

    uname = selected_user.strip().lower()

    # ── Akses Menu ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 📋 Akses Menu")
    current_tabs = perms.get(uname, perms.get("__default__", list(ALL_MENU_TABS.keys())))
    new_selection: list = []
    cols = st.columns(2)
    for i, (key, label) in enumerate(tab_items):
        with cols[i % 2]:
            forced = key in ALWAYS_ALLOWED
            val = st.checkbox(
                label=label + (" *(wajib)*" if forced else ""),
                value=True if forced else (key in current_tabs),
                key=f"mac_cb_{uname}_{key}",
                disabled=forced,
                help="Tab ini selalu aktif." if forced else "",
            )
            if forced or val:
                new_selection.append(key)

    # ── Akses Kolom ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔒 Akses Kolom")
    cur_cols = col_perms.get(uname, col_perms.get("__default__", list(ALL_COLUMN_ACCESS.keys())))
    new_col_sel: list = []
    col_cb = st.columns(2)
    for i, (ckey, clabel) in enumerate(col_items):
        with col_cb[i % 2]:
            cval = st.checkbox(
                label=clabel,
                value=(ckey in cur_cols),
                key=f"cac_cb_{uname}_{ckey}",
            )
            if cval:
                new_col_sel.append(ckey)

    # ── Akses Sub-Tab Harga ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔖 Akses Sub-Tab Harga")
    cur_hs = hs_perms.get(uname, hs_perms.get("__default__", list(ALL_HARGA_SUBTABS.keys())))
    new_hs_sel: list = []
    hs_cols = st.columns(2)
    for i, (hskey, hslabel) in enumerate(hs_items):
        with hs_cols[i % 2]:
            hsval = st.checkbox(
                label=hslabel,
                value=(hskey in cur_hs),
                key=f"hsc_cb_{uname}_{hskey}",
            )
            if hsval:
                new_hs_sel.append(hskey)

    # ── Tombol Simpan ─────────────────────────────────────────────────
    st.markdown("---")
    if st.button("💾 Simpan Semua Akses", key=f"mac_save_all_{uname}",
                 type="primary", use_container_width=True):
        ok1, err1 = MenuAccessManager.set_user_tabs(uname, new_selection)
        ok2, err2 = ColumnAccessManager.set_user_columns(uname, new_col_sel)
        ok3, err3 = HargaSubTabManager.set_user_subtabs(uname, new_hs_sel)
        if ok1 and ok2 and ok3:
            st.success(f"✅ Semua akses untuk **{uname}** berhasil disimpan!")
            st.rerun()
        else:
            errors = [e for e in [err1, err2, err3] if e]
            st.error(f"❌ Gagal menyimpan: {', '.join(errors)}")








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

SIDEBAR_TOGGLE_JS = """
<script>
(function() {
    if (window.__sidebarToggleInjected) return;
    window.__sidebarToggleInjected = true;

    function getSidebar() {
        return document.querySelector('[data-testid="stSidebar"]');
    }

    function isCollapsed() {
        var sb = getSidebar();
        if (!sb) return false;
        return sb.getAttribute('aria-expanded') === 'false'
            || sb.classList.contains('st-emotion-cache-hidden')
            || getComputedStyle(sb).transform.includes('translateX(-')
            || getComputedStyle(sb).marginLeft.startsWith('-');
    }

    function clickOriginalToggle() {
        // Coba klik tombol toggle bawaan Streamlit
        var btn = document.querySelector('[data-testid="collapsedControl"] button')
                || document.querySelector('button[kind="header"]')
                || document.querySelector('[data-testid="stSidebarCollapsedControl"] button');
        if (btn) { btn.click(); return true; }
        // Fallback: toggle class langsung
        var sb = getSidebar();
        if (!sb) return false;
        var expanded = sb.getAttribute('aria-expanded');
        if (expanded === 'false') {
            sb.setAttribute('aria-expanded', 'true');
        } else {
            sb.setAttribute('aria-expanded', 'false');
        }
        return true;
    }

    function updateIcon() {
        var btn = document.getElementById('custom-sidebar-toggle');
        if (!btn) return;
        btn.innerHTML = isCollapsed() ? '&#9776;' : '&#10005;';
        btn.title = isCollapsed() ? 'Buka Sidebar' : 'Tutup Sidebar';
    }

    function injectButton() {
        if (document.getElementById('custom-sidebar-toggle')) return;
        var btn = document.createElement('button');
        btn.id = 'custom-sidebar-toggle';
        btn.innerHTML = '&#9776;';
        btn.title = 'Buka/Tutup Sidebar';
        btn.addEventListener('click', function() {
            clickOriginalToggle();
            setTimeout(updateIcon, 300);
        });
        document.body.appendChild(btn);
        updateIcon();
    }

    // Inject setelah DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', injectButton);
    } else {
        setTimeout(injectButton, 500);
    }

    // Re-inject kalau Streamlit rerun (MutationObserver)
    var observer = new MutationObserver(function() {
        injectButton();
        updateIcon();
    });
    observer.observe(document.body, { childList: true, subtree: false });
})();
</script>
"""

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
    # Gunakan components.html agar <script> benar-benar dieksekusi browser
    _stc.html(KEEP_ALIVE_JS + SIDEBAR_TOGGLE_JS, height=0, scrolling=False)

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
    #custom-sidebar-toggle {
        position: fixed !important;
        top: 10px !important;
        left: 10px !important;
        z-index: 999999 !important;
        width: 36px !important;
        height: 36px !important;
        background: #1E88E5 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        cursor: pointer !important;
        font-size: 18px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.25) !important;
        transition: background 0.2s !important;
    }
    #custom-sidebar-toggle:hover { background: #1565C0 !important; }
    .main-header { font-size: 2.5rem; color: #1E88E5; text-align: center; margin-bottom: 1.5rem; padding-top: 0.8rem; }
    .sub-header { font-size: 1.5rem; color: #0D47A1; margin-top: 1.5rem; margin-bottom: 1rem; }
    .search-box { background-color: #F5F5F5; padding: 1.5rem; border-radius: 0.5rem; margin-bottom: 1.5rem; }
    .user-badge { display: inline-flex; align-items: center; gap: 0.4rem; background: #E3F2FD; border: 1px solid #90CAF9; border-radius: 20px; padding: 0.3rem 0.85rem; font-size: 0.85rem; color: #1565C0; font-weight: 600; }
    .role-admin { color: #E65100; font-weight: 700; }
    .role-user  { color: #1565C0; font-weight: 600; }
    iframe[height="0"] { display: none !important; }
    .batch-info-box { background: #E8F5E9; border-left: 4px solid #4CAF50; padding: 0.8rem 1rem; border-radius: 0 8px 8px 0; margin-bottom: 1rem; }

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
def search_part_number(term, excel_files, stok_cache, harga_lookup=None):
    results, seen = [], set()
    term_up = term.strip().upper()
    if not term_up:
        return results

    harga_lookup = harga_lookup or {}

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
                harga_value = harga_lookup.get(pn_value.upper(), "—")
                results.append({
                    "File": sn, "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value,
                    "Part Name": str(row["part_name"]) if pd.notna(row["part_name"]) else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Harga": harga_value,
                    "Excel Row": indices[0] + 2, "Full Path": fi["full_path"]
                })
                seen.add(sn)
                break
    return results


def search_part_name(term, excel_files, stok_cache, harga_lookup=None):
    """
    Cari berdasarkan Part Name.
    """
    results = []
    term_up = term.strip().upper()
    if not term_up:
        return results

    harga_lookup = harga_lookup or {}
    search_keywords = [term.strip().lower()]

    for fi in excel_files:
        df  = fi["dataframe"]
        pni = fi.get("part_name_index", {})
        matching_indices = set()

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
            # Harus cocok dengan keyword
            matched = any(kw.upper() in pname.upper() for kw in search_keywords)
            if matched:
                pn_value   = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                harga_value = harga_lookup.get(pn_value.upper(), "—")
                results.append({
                    "File": fi["simple_name"], "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value, "Part Name": pname if pname else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Harga": harga_value, "Excel Row": idx + 2, "Full Path": fi["full_path"]
                })
    return results


# ── Build Excel Functions ───────────────────────────────────────────

def build_catalog_excel(df_result: pd.DataFrame, progress_callback=None, all_part_numbers: list = None) -> bytes:
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

    # Bangun lookup dari df_result berdasarkan index urutan (bukan PN sebagai key)
    # sehingga duplikat PN tetap muncul sebagai baris terpisah
    df_lookup = {}
    for _, r in df_result.iterrows():
        pn = r["_pn_group"]
        if pn not in df_lookup:
            df_lookup[pn] = {
                "Part Name": r.get("Part Name", ""),
                "kecocokan": r.get("Hasil", ""),
                "found":     r.get("Status", "") == "✅ Ditemukan",
            }

    # Gunakan urutan asli dari all_part_numbers — TANPA deduplikasi
    # sehingga PN yang muncul 2x di input tetap 2 baris di output
    pn_order = all_part_numbers if all_part_numbers else list(dict.fromkeys(df_result["_pn_group"].tolist()))

    # Bangun grouped sebagai list (bukan dict) agar duplikat tidak hilang
    grouped_list = []
    for pn in pn_order:
        if pn in df_lookup:
            grouped_list.append((pn, df_lookup[pn]))
        else:
            grouped_list.append((pn, {"Part Name": "", "kecocokan": "", "found": False}))

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
    total_pn  = len(grouped_list)

    for i, (pn, info) in enumerate(grouped_list):
        if progress_callback:
            progress_callback(i, total_pn, pn)

        kecocokan  = info["kecocokan"] if info["kecocokan"] else "—"
        is_found   = info["found"]
        part_name  = info["Part Name"]
        fill       = (fill_even if i % 2 == 0 else fill_odd) if is_found else fill_nf
        row_height = 80
        img_d      = None
        img_e      = None

        if SIMS_ENABLED:
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

                # Jika PN tidak ditemukan di Excel tapi ada di SIMS,
                # ambil Part Name dari SIMS
                if not is_found and not part_name:
                    try:
                        from sims_fetcher import get_sims_part_info
                        sims_info, _ = get_sims_part_info(pn)
                        if sims_info.get("partName"):
                            part_name = sims_info["partName"]
                    except Exception:
                        pass

            except Exception as e:
                print(f"[catalog] Gagal ambil gambar {pn}: {e}")

        ws.row_dimensions[row_idx].height = row_height

        for ci, (val, aln) in enumerate(
            [(pn, center), (part_name, left), (kecocokan, left)], start=1
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
        self.harga_file      = DATA_FOLDER / "harga" / "harga.xlsx"
        self.harga_cache     = None
        self.harga_lookup    = {}  # dict PN -> "Rp xxx" — dibangun sekali saat load
        self.populasi_folder = DATA_FOLDER / "populasi"
        self._load_stok_data()
        self._load_harga_data()

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


    def _load_harga_data(self):
        if self.harga_cache is not None:
            return
        if "harga_data" in st.session_state:
            self.harga_cache  = st.session_state.harga_data
            self.harga_lookup = st.session_state.get("harga_lookup", {})
            return
        if not self.harga_file.exists():
            self.harga_cache  = pd.DataFrame(columns=["Part Number", "Part Name", "Harga"])
            self.harga_lookup = {}
            st.session_state.harga_data   = self.harga_cache
            st.session_state.harga_lookup = self.harga_lookup
            return
        try:
            df_h = pd.read_excel(self.harga_file, dtype=str)
            df_h.columns = [c.strip() for c in df_h.columns]
            col_map = {}
            for c in df_h.columns:
                cl = c.lower()
                if "part number" in cl or "partnumber" in cl or "no part" in cl or "kode" in cl:
                    col_map[c] = "Part Number"
                elif "part name" in cl or "nama" in cl or "deskripsi" in cl:
                    col_map[c] = "Part Name"
                elif "harga" in cl or "price" in cl:
                    col_map[c] = "Harga"
            df_h = df_h.rename(columns=col_map)
            for req in ("Part Number", "Part Name", "Harga"):
                if req not in df_h.columns:
                    df_h[req] = ""
            df_h["Part Number"] = df_h["Part Number"].astype(str).str.strip().str.upper()
            df_h = df_h.dropna(subset=["Part Number"])
            df_h = df_h[df_h["Part Number"] != ""]
            self.harga_cache = df_h.reset_index(drop=True)
            # Bangun lookup dict sekali di sini
            lookup = {}
            for pn_key, harga_val in zip(df_h["Part Number"], df_h["Harga"]):
                try:
                    num = float(str(harga_val).replace(",", "").strip())
                    lookup[pn_key] = f"Rp {num:,.0f}"
                except Exception:
                    lookup[pn_key] = str(harga_val) if pd.notna(harga_val) else "—"
            self.harga_lookup = lookup
            st.session_state.harga_data   = self.harga_cache
            st.session_state.harga_lookup = self.harga_lookup
        except Exception as e:
            st.error(f"Gagal membaca harga.xlsx → {e}")
            self.harga_cache  = pd.DataFrame(columns=["Part Number", "Part Name", "Harga"])
            self.harga_lookup = {}
            st.session_state.harga_data   = self.harga_cache
            st.session_state.harga_lookup = self.harga_lookup

    def render_harga_tab(self):
        st.markdown("### 💰 Daftar Harga Sparepart")

        # Cek izin akses kolom harga
        user_h = LoginManager.get_current_user()
        role_h = user_h["role"] if user_h else "user"
        if "col_harga" not in get_allowed_columns(user_h["username"], role_h):
            st.warning("🔒 Anda tidak memiliki akses untuk melihat data harga. Hubungi admin jika diperlukan.")
            return

        col_reload, _ = st.columns([1, 5])
        with col_reload:
            if st.button("🔄 Reload Harga", key="reload_harga"):
                st.session_state.pop("harga_data", None)
                self.harga_cache = None
                self._load_harga_data()
                st.rerun()

        df_h = self.harga_cache if self.harga_cache is not None else pd.DataFrame()

        if df_h.empty:
            st.warning(
                "File harga tidak ditemukan atau kosong. "
                "Pastikan file ada di `data/harga/harga.xlsx`."
            )
            return

        st.divider()

        with st.expander("🔍 Filter & Pencarian", expanded=True):
            col_s, col_sort = st.columns([3, 2])
            with col_s:
                kw_harga = st.text_input(
                    "Cari Part Number / Part Name:",
                    placeholder="Ketik untuk filter…",
                    key="harga_search_kw",
                )
            with col_sort:
                sort_by = st.selectbox(
                    "Urutkan berdasarkan:",
                    ["Part Number", "Part Name", "Harga (Terendah)", "Harga (Tertinggi)"],
                    key="harga_sort",
                )

        df_show = df_h.copy()

        if kw_harga.strip():
            kw_up = kw_harga.strip().upper()
            mask = (
                df_show["Part Number"].str.upper().str.contains(kw_up, na=False) |
                df_show["Part Name"].astype(str).str.upper().str.contains(kw_up, na=False)
            )
            df_show = df_show[mask].reset_index(drop=True)

        try:
            if sort_by == "Harga (Terendah)":
                df_show["_harga_num"] = pd.to_numeric(
                    df_show["Harga"].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")
                df_show = df_show.sort_values("_harga_num", ascending=True).drop(columns=["_harga_num"])
            elif sort_by == "Harga (Tertinggi)":
                df_show["_harga_num"] = pd.to_numeric(
                    df_show["Harga"].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")
                df_show = df_show.sort_values("_harga_num", ascending=False).drop(columns=["_harga_num"])
            elif sort_by == "Part Name":
                df_show = df_show.sort_values("Part Name", key=lambda x: x.str.upper())
            else:
                df_show = df_show.sort_values("Part Number")
        except Exception:
            pass

        df_show = df_show.reset_index(drop=True)

        c1, c2 = st.columns(2)
        c1.metric("Total Part", len(df_h))
        c2.metric("Hasil Filter", len(df_show))

        st.markdown("---")

        def fmt_harga(val):
            try:
                num = float(str(val).replace(",", "").strip())
                return f"Rp {num:,.0f}"
            except Exception:
                return str(val)

        df_display = df_show[["Part Number", "Part Name", "Harga"]].copy()
        df_display["Harga (Rp)"] = df_display["Harga"].apply(fmt_harga)
        df_display = df_display.drop(columns=["Harga"])

        if df_display.empty:
            st.info("Tidak ada data yang cocok dengan pencarian.")
        else:
            st.dataframe(
                df_display,
                hide_index=True,
                use_container_width=True,
                height=min(400, 56 + len(df_display) * 35),
                column_config={
                    "Part Number": st.column_config.TextColumn("Part Number", width="medium"),
                    "Part Name":   st.column_config.TextColumn("Part Name",   width="large"),
                    "Harga (Rp)":  st.column_config.TextColumn("Harga",       width="medium"),
                },
            )

            dl_buf = io.BytesIO()
            df_display.to_excel(dl_buf, index=False, engine="openpyxl")
            dl_buf.seek(0)
            st.download_button(
                label="⬇️ Download Excel",
                data=dl_buf.getvalue(),
                file_name=f"harga_sparepart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="harga_download",
            )


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

    # ── Tab: Search Part Number ──────────────────────────────────────
    def _render_tab_search_pn(self):
        with st.form(key="search_pn_form", clear_on_submit=False):
            sn_input = st.text_input(
                "Masukkan Part Number:",
                placeholder="Contoh: WG1642821034/1",
                key="sn_input"
            )
            if st.form_submit_button("\U0001f50d Cari Part Number", type="primary", use_container_width=True):
                if sn_input:
                    with st.spinner("Mencari\u2026"):
                        st.session_state.search_results = search_part_number(
                            sn_input, st.session_state.excel_files, self.stok_cache, self.harga_lookup)
                        st.session_state.search_type = "Part Number"
                        st.session_state.search_term = sn_input
                        st.rerun()
                else:
                    st.warning("Masukkan part number untuk mencari.")

    # ── Tab: Search Part Name ────────────────────────────────────────
    def _render_tab_search_name(self):
        with st.form(key="search_name_form", clear_on_submit=False):
            name_input = st.text_input(
                "Masukkan Part Name:",
                placeholder="Contoh: baut roda, bearing, kampas rem",
                key="name_input"
            )
            if st.form_submit_button("\U0001f50d Cari Part Name", type="primary", use_container_width=True):
                if name_input:
                    with st.spinner("Mencari\u2026"):
                        st.session_state.search_results = search_part_name(
                            name_input, st.session_state.excel_files, self.stok_cache, self.harga_lookup)
                        st.session_state.search_type = "Part Name"
                        st.session_state.search_term = name_input
                        st.rerun()
                else:
                    st.warning("Masukkan nama part untuk mencari.")

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

        # ── Pilih metode input ────────────────────────────────────────
        input_mode_batch = st.radio(
            "Metode Input:",
            ["📁 Upload File Excel", "⌨️ Ketik Manual"],
            horizontal=True,
            key="batch_download_input_mode",
        )

        part_numbers_raw = []

        if input_mode_batch == "📁 Upload File Excel":
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
            if len(col_a) > 0 and col_a.iloc[0].lower() in ("part number","part_number","partnumber","no part","kode"):
                col_a = col_a.iloc[1:]

            part_numbers_raw = col_a[col_a.str.len() > 0].tolist()

        else:  # Ketik Manual
            manual_text_batch = st.text_area(
                "Masukkan Part Number (satu per baris):",
                height=180,
                placeholder="WG1641230025\nWG9725520274\n...",
                key="batch_download_manual_text",
            )
            if not manual_text_batch.strip():
                return
            part_numbers_raw = [p.strip() for p in manual_text_batch.splitlines() if p.strip()]

        if not part_numbers_raw:
            st.warning("Tidak ada Part Number yang valid dalam file.")
            return

        # Hapus duplikat, pertahankan urutan kemunculan pertama
        seen = set()
        part_numbers = []
        duplicates   = []
        for pn in part_numbers_raw:
            pn_up = pn.upper()
            if pn_up not in seen:
                seen.add(pn_up)
                part_numbers.append(pn)
            else:
                duplicates.append(pn)

        st.info(f"📊 **{len(part_numbers)}** Part Number unik ditemukan dalam file input.")
        if duplicates:
            st.warning(f"⚠️ **{len(duplicates)}** duplikat dihapus: {', '.join(duplicates)}")

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
                found = search_part_number(pn, st.session_state.excel_files, self.stok_cache, self.harga_lookup)
                if found:
                    # Gabungkan semua file yang cocok ke 1 baris saja
                    hasil_list = [r["File"] for r in found]
                    sheet_list = [r["Sheet"] for r in found]
                    results_all.append({
                        "Part Number": pn,
                        "_pn_group":   pn,
                        "Hasil":       ", ".join(hasil_list),
                        "Sheet":       ", ".join(sheet_list),
                        "Part Name":   found[0]["Part Name"],
                        "Qty":         found[0]["Quantity"],
                        "Stok":        found[0]["Stok"],
                        "Status":      "✅ Ditemukan",
                    })
                else:
                    results_all.append({
                        "Part Number": pn, "_pn_group": pn,
                        "Hasil": "", "Sheet": "", "Part Name": "",
                        "Qty": "", "Stok": "", "Status": "❌ Tidak ditemukan",
                    })

            prog.empty()
            status_txt.empty()
            df_result = pd.DataFrame(results_all)

            # ── Ambil Part Name dari SIMS untuk PN yang tidak ditemukan di database lokal ──
            if SIMS_ENABLED:
                try:
                    from sims_fetcher import get_sims_part_info
                except ImportError:
                    get_sims_part_info = None

                if get_sims_part_info:
                    not_found_mask = df_result["Part Name"] == ""
                    not_found_pns  = df_result.loc[not_found_mask, "Part Number"].tolist()
                    if not_found_pns:
                        sims_prog   = st.progress(0)
                        sims_status = st.empty()
                        for si, nf_pn in enumerate(not_found_pns):
                            sims_status.text(f"🔎 Ambil Part Name dari SIMS {si+1}/{len(not_found_pns)}: {nf_pn}")
                            sims_prog.progress((si + 1) / len(not_found_pns))
                            try:
                                sims_info, _ = get_sims_part_info(nf_pn)
                                if sims_info and sims_info.get("partName"):
                                    idx = df_result.index[df_result["Part Number"] == nf_pn].tolist()
                                    for i in idx:
                                        df_result.at[i, "Part Name"] = sims_info["partName"]
                            except Exception:
                                pass
                        sims_prog.empty()
                        sims_status.empty()

            # Import get_sims_part_info jika belum tersedia di scope ini
            try:
                from sims_fetcher import get_sims_part_info
            except ImportError:
                pass

            prog_cat   = st.progress(0)
            status_cat = st.empty()

            def _prog(i, tot, pn):
                prog_cat.progress((i + 1) / max(tot, 1))
                status_cat.text(f"🖼️ Fetch gambar {i+1}/{tot}: {pn}")

            try:
                cat_bytes = build_catalog_excel(df_result, progress_callback=_prog, all_part_numbers=part_numbers)
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
        user_b = LoginManager.get_current_user()
        role_b = user_b["role"] if user_b else "user"
        allowed_cols_b = get_allowed_columns(user_b["username"], role_b)

        disp_cols = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty"]
        if "col_stok" in allowed_cols_b:
            disp_cols.append("Stok")
        disp_cols.append("Status")

        # Pastikan Part Name yang kosong/N/A tetap tampil dengan benar
        df_preview = df_result[disp_cols].copy()
        df_preview["Part Name"] = df_preview["Part Name"].replace({"": "—", "N/A": "—"}).fillna("—")

        st.dataframe(
            df_preview, hide_index=True,
            use_container_width=True,
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

    # ── Tab Harga ────────────────────────────────────────────────────
    def render_harga_tab(self):
        st.markdown("### 💰 Harga Sparepart")

        # Cek izin akses kolom harga
        user_h = LoginManager.get_current_user()
        role_h = user_h["role"] if user_h else "user"
        if "col_harga" not in get_allowed_columns(user_h["username"], role_h):
            st.warning("🔒 Anda tidak memiliki akses untuk melihat data harga. Hubungi admin jika diperlukan.")
            return

        # ── Sub-tab di dalam Tab Harga ────────────────────────────────
        # Cek izin akses sub-tab harga
        allowed_hs = get_allowed_harga_subtabs(user_h["username"], role_h)
        _hs_tabs   = []
        _hs_keys   = []
        if "subtab_list_harga" in allowed_hs:
            _hs_tabs.append("📋 List Harga")
            _hs_keys.append("subtab_list_harga")
        if "subtab_cari_harga" in allowed_hs:
            _hs_tabs.append("🔍 Cari Harga")
            _hs_keys.append("subtab_cari_harga")
        if "subtab_batch_harga" in allowed_hs:
            _hs_tabs.append("📥 Batch Cari Harga")
            _hs_keys.append("subtab_batch_harga")
        if not _hs_tabs:
            st.warning("🔒 Anda tidak memiliki akses ke sub-tab manapun di Harga. Hubungi admin.")
            return
        _rendered_tabs = st.tabs(_hs_tabs)
        sub_list  = _rendered_tabs[_hs_keys.index("subtab_list_harga")]  if "subtab_list_harga"  in _hs_keys else None
        sub_cari  = _rendered_tabs[_hs_keys.index("subtab_cari_harga")]  if "subtab_cari_harga"  in _hs_keys else None
        sub_batch = _rendered_tabs[_hs_keys.index("subtab_batch_harga")] if "subtab_batch_harga" in _hs_keys else None


        # ══════════════════════════════════════════════════════════════
        # SUB-TAB 1: LIST HARGA
        # ══════════════════════════════════════════════════════════════
        if sub_list is not None:
         with sub_list:
            st.markdown("#### 📋 Daftar Harga Sparepart")

            col_reload, _ = st.columns([1, 5])
            with col_reload:
                if st.button("🔄 Reload Harga", key="reload_harga"):
                    st.session_state.pop("harga_data", None)
                    self.harga_cache = None
                    self._load_harga_data()
                    st.rerun()

            df_h = self.harga_cache if self.harga_cache is not None else pd.DataFrame()

            if df_h.empty:
                st.warning(
                    "File harga tidak ditemukan atau kosong. "
                    "Pastikan file ada di `data/harga/harga.xlsx`."
                )
            else:
                st.divider()

                with st.expander("🔍 Filter & Pencarian", expanded=True):
                    col_s, col_sort = st.columns([3, 2])
                    with col_s:
                        kw_harga = st.text_input(
                            "Cari Part Number / Part Name:",
                            placeholder="Ketik untuk filter…",
                            key="harga_search_kw",
                        )
                    with col_sort:
                        sort_by = st.selectbox(
                            "Urutkan berdasarkan:",
                            ["Part Number", "Part Name", "Harga (Terendah)", "Harga (Tertinggi)"],
                            key="harga_sort",
                        )

                df_show = df_h.copy()

                if kw_harga.strip():
                    kw_up = kw_harga.strip().upper()
                    mask = (
                        df_show["Part Number"].str.upper().str.contains(kw_up, na=False) |
                        df_show["Part Name"].astype(str).str.upper().str.contains(kw_up, na=False)
                    )
                    df_show = df_show[mask].reset_index(drop=True)

                try:
                    if sort_by == "Harga (Terendah)":
                        df_show["_harga_num"] = pd.to_numeric(
                            df_show["Harga"].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")
                        df_show = df_show.sort_values("_harga_num", ascending=True).drop(columns=["_harga_num"])
                    elif sort_by == "Harga (Tertinggi)":
                        df_show["_harga_num"] = pd.to_numeric(
                            df_show["Harga"].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce")
                        df_show = df_show.sort_values("_harga_num", ascending=False).drop(columns=["_harga_num"])
                    elif sort_by == "Part Name":
                        df_show = df_show.sort_values("Part Name", key=lambda x: x.str.upper())
                    else:
                        df_show = df_show.sort_values("Part Number")
                except Exception:
                    pass

                df_show = df_show.reset_index(drop=True)

                c1, c2 = st.columns(2)
                c1.metric("Total Part", len(df_h))
                c2.metric("Hasil Filter", len(df_show))

                st.markdown("---")

                def fmt_harga(val):
                    try:
                        num = float(str(val).replace(",", "").strip())
                        return f"Rp {num:,.0f}"
                    except Exception:
                        return str(val)

                df_display = df_show[["Part Number", "Part Name", "Harga"]].copy()
                df_display["Harga (Rp)"] = df_display["Harga"].apply(fmt_harga)
                df_display = df_display.drop(columns=["Harga"])

                if df_display.empty:
                    st.info("Tidak ada data yang cocok dengan pencarian.")
                else:
                    st.dataframe(
                        df_display,
                        hide_index=True,
                        use_container_width=True,
                        height=min(400, 56 + len(df_display) * 35),
                        column_config={
                            "Part Number": st.column_config.TextColumn("Part Number", width="medium"),
                            "Part Name":   st.column_config.TextColumn("Part Name",   width="large"),
                            "Harga (Rp)":  st.column_config.TextColumn("Harga",       width="medium"),
                        },
                    )

                    dl_buf = io.BytesIO()
                    df_display.to_excel(dl_buf, index=False, engine="openpyxl")
                    dl_buf.seek(0)
                    st.download_button(
                        label="⬇️ Download Excel",
                        data=dl_buf.getvalue(),
                        file_name=f"harga_sparepart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="harga_download",
                    )

        # ══════════════════════════════════════════════════════════════
        # SUB-TAB 2: CARI HARGA (dari SIMS)
        # ══════════════════════════════════════════════════════════════
        if sub_cari is not None:
         with sub_cari:
            st.markdown("#### 🔍 Cari Harga Part dari SIMS")
            st.caption("Ambil Part Price langsung dari SIMS berdasarkan Part Number. Harga SIMS dalam CNY, dikonversi ke IDR otomatis.")

            # ── Helper: ambil kurs CNY → IDR ─────────────────────────
            def _get_cny_to_idr_rate() -> tuple:
                """Ambil kurs CNY→IDR dari API. Cache di session_state selama 30 menit."""
                import time as _time
                cache_key   = "_cny_idr_rate_cache"
                cache_ts_key = "_cny_idr_rate_ts"
                now = _time.time()
                cached_rate = st.session_state.get(cache_key)
                cached_ts   = st.session_state.get(cache_ts_key, 0)
                if cached_rate and (now - cached_ts) < 1800:
                    return cached_rate, None
                try:
                    resp = requests.get(
                        "https://api.exchangerate-api.com/v4/latest/CNY",
                        timeout=8
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    rate = data["rates"].get("IDR")
                    if rate:
                        st.session_state[cache_key]   = float(rate)
                        st.session_state[cache_ts_key] = now
                        return float(rate), None
                    return None, "IDR tidak ada di response"
                except Exception as e:
                    # Fallback ke kurs statis jika API gagal
                    FALLBACK_RATE = 2200.0
                    st.session_state[cache_key]   = FALLBACK_RATE
                    st.session_state[cache_ts_key] = now
                    return FALLBACK_RATE, f"API gagal ({e}), menggunakan kurs fallback Rp {FALLBACK_RATE:,.0f}/CNY"

            # Tampilkan info kurs saat ini
            rate_cny_idr, rate_err = _get_cny_to_idr_rate()
            if rate_err:
                st.warning(f"⚠️ {rate_err}")
            if rate_cny_idr:
                col_rate, col_rate_refresh = st.columns([4, 1])
                with col_rate:
                    st.info(f"💱 Kurs saat ini: **1 CNY = Rp {rate_cny_idr:,.2f}**")
                with col_rate_refresh:
                    if st.button("🔄 Update Kurs", key="refresh_kurs_cny", use_container_width=True):
                        st.session_state.pop("_cny_idr_rate_cache", None)
                        st.session_state.pop("_cny_idr_rate_ts", None)
                        st.rerun()

            try:
                from sims_price_fetcher import get_sims_part_price
                price_fetcher_ok = True
            except ImportError:
                price_fetcher_ok = False

            if not price_fetcher_ok:
                st.warning("⚠️ `sims_price_fetcher.py` tidak ditemukan.")
            else:
                col_input, col_btn, col_refresh = st.columns([3, 1, 1])
                with col_input:
                    pn_input = st.text_input(
                        "Part Number",
                        placeholder="Contoh: WG1641230025",
                        label_visibility="collapsed",
                        key="sims_harga_pn_input",
                    ).strip().upper()
                with col_btn:
                    cari = st.button("🔍 Cari", type="primary", use_container_width=True, key="sims_harga_cari_btn")
                with col_refresh:
                    force = st.button("🔄 Refresh", use_container_width=True, key="sims_harga_refresh_btn",
                                      help="Abaikan cache, ambil ulang dari SIMS")

                if (cari or force) and pn_input:
                    result_key = f"sims_harga_result_{pn_input}"
                    if force:
                        st.session_state.pop(result_key, None)
                    if result_key not in st.session_state:
                        with st.spinner(f"Mengambil harga **{pn_input}** dari SIMS..."):
                            price, err = get_sims_part_price(pn_input, force_refresh=bool(force))
                        st.session_state[result_key] = {"price": price, "err": err, "pn": pn_input}

                result_key = f"sims_harga_result_{pn_input}" if pn_input else None
                res = st.session_state.get(result_key) if result_key else None

                if res:
                    if res["price"] is not None:
                        cny_price = res["price"]
                        idr_price = cny_price * rate_cny_idr if rate_cny_idr else None
                        idr_str   = f"Rp {idr_price:,.0f}" if idr_price is not None else "—"
                        st.markdown(
                            f"""
                            <div style="background:#E8F5E9;border-left:5px solid #4CAF50;
                                        padding:1rem 1.5rem;border-radius:0 10px 10px 0;margin-top:1rem;">
                                <div style="font-size:0.85rem;color:#555;">Part Number</div>
                                <div style="font-size:1.2rem;font-weight:700;color:#1B5E20;">{res['pn']}</div>
                                <div style="display:flex;gap:3rem;margin-top:12px;flex-wrap:wrap;">
                                    <div>
                                        <div style="font-size:0.8rem;color:#777;">Harga SIMS (CNY)</div>
                                        <div style="font-size:1.6rem;font-weight:800;color:#1565C0;">¥ {cny_price:,.2f}</div>
                                    </div>
                                    <div>
                                        <div style="font-size:0.8rem;color:#777;">Harga IDR</div>
                                        <div style="font-size:1.6rem;font-weight:800;color:#2E7D32;">{idr_str}</div>
                                    </div>
                                </div>
                                <div style="font-size:0.75rem;color:#999;margin-top:8px;">
                                    Kurs: 1 CNY = Rp {rate_cny_idr:,.2f}
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )
                    else:
                        st.warning(f"⚠️ Harga tidak ditemukan untuk **{res['pn']}**.")
                        if res["err"]:
                            st.caption(f"Detail: {res['err']}")
                elif pn_input and not (cari or force):
                    st.info("Klik **Cari** untuk mengambil harga dari SIMS.")


        # ══════════════════════════════════════════════════════════════
        # SUB-TAB 3: BATCH CARI HARGA
        # ══════════════════════════════════════════════════════════════
        if sub_batch is not None:
         with sub_batch:
            # ── Kurs CNY → IDR ────────────────────────────────────────
            import time as _bhe_time
            _bhe_cache_key    = "_cny_idr_rate_cache"
            _bhe_cache_ts_key = "_cny_idr_rate_ts"
            _bhe_now = _bhe_time.time()
            _bhe_cached_rate = st.session_state.get(_bhe_cache_key)
            _bhe_cached_ts   = st.session_state.get(_bhe_cache_ts_key, 0)
            if _bhe_cached_rate and (_bhe_now - _bhe_cached_ts) < 1800:
                b_rate_batch     = _bhe_cached_rate
                b_rate_err_batch = None
            else:
                try:
                    _bhe_resp = requests.get("https://api.exchangerate-api.com/v4/latest/CNY", timeout=8)
                    _bhe_resp.raise_for_status()
                    _bhe_data = _bhe_resp.json()
                    _bhe_rate = _bhe_data["rates"].get("IDR")
                    if _bhe_rate:
                        st.session_state[_bhe_cache_key]    = float(_bhe_rate)
                        st.session_state[_bhe_cache_ts_key] = _bhe_now
                        b_rate_batch     = float(_bhe_rate)
                        b_rate_err_batch = None
                    else:
                        b_rate_batch     = 2200.0
                        b_rate_err_batch = "IDR tidak ada di response"
                except Exception as _bhe_e:
                    b_rate_batch     = 2200.0
                    b_rate_err_batch = f"API gagal ({_bhe_e}), menggunakan kurs fallback Rp 2.200/CNY"

            col_brate2, col_brate_ref2 = st.columns([4, 1])
            with col_brate2:
                if b_rate_err_batch:
                    st.warning(f"⚠️ {b_rate_err_batch}")
                if b_rate_batch:
                    st.info(f"💱 Kurs saat ini: **1 CNY = Rp {b_rate_batch:,.2f}**")
            with col_brate_ref2:
                if st.button("🔄 Update Kurs", key="bhe_kurs_refresh", use_container_width=True):
                    st.session_state.pop("_cny_idr_rate_cache", None)
                    st.session_state.pop("_cny_idr_rate_ts", None)
                    st.rerun()

            st.divider()

            try:
                from batch_harga_engine import render_batch_harga_tab
                render_batch_harga_tab(b_rate=b_rate_batch)
            except ImportError:
                st.error("❌ `batch_harga_engine.py` tidak ditemukan. Pastikan file sudah ada di direktori yang sama dengan app.py.")

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
                if st.button("🔐 Reload Menu Config", type="secondary", use_container_width=True):
                    MenuAccessManager.load_permissions(force=True)
                    st.toast("✅ Konfigurasi akses menu di-reload!")
                if st.button("🔒 Reload Kolom Config", type="secondary", use_container_width=True):
                    ColumnAccessManager.load_permissions(force=True)
                    st.toast("✅ Konfigurasi akses kolom di-reload!")
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
                          "harga_data","harga_lookup"):
                    st.session_state.pop(k, None)
                self.stok_cache = None; self.harga_cache = None; self.harga_lookup = {}
                self._load_stok_data(); self._load_harga_data()
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
4. **Batch Download:** Upload Excel berisi PN di Kol A

                """)

        # ── TABS ────────────────────────────────────────────────────
        st.markdown(TAB_PERSIST_JS, unsafe_allow_html=True)
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        # Definisi semua tab dan method render-nya
        ALL_TAB_DEFS = [
            ("tab_search_pn",   "🔢 Search Part Number", "_render_tab_search_pn"),
            ("tab_search_name", "📝 Search Part Name",    "_render_tab_search_name"),
            ("tab_batch",       "📥 Batch Download",      "render_batch_download_tab"),
            ("tab_populasi",    "🚛 Populasi Unit",       "render_populasi_tab"),
            ("tab_harga",       "💰 Harga",               "render_harga_tab"),
        ]
        if role == "admin":
            ALL_TAB_DEFS.append(("tab_menu_control", "🛡️ Menu Control", "__menu_control__"))
            ALL_TAB_DEFS.append(("tab_stok_upload",  "📤 Upload Stok",  "__stok_upload__"))

        # Tentukan tab yang boleh dilihat user ini
        allowed_keys = set(get_allowed_tabs(user["username"], role))
        visible_tabs = [(k, lbl, fn) for k, lbl, fn in ALL_TAB_DEFS if k in allowed_keys]

        # Render tabs secara dinamis
        tab_objects = st.tabs([lbl for _, lbl, _ in visible_tabs])
        for tab_obj, (key, lbl, fn) in zip(tab_objects, visible_tabs):
            with tab_obj:
                if fn == "__menu_control__":
                    render_admin_menu_control_tab()
                elif fn == "__stok_upload__":
                    render_stok_uploader_tab()
                else:
                    getattr(self, fn)()

        st.markdown("</div>", unsafe_allow_html=True)
        self.display_search_results()

    def display_search_results(self):
        results = st.session_state.get("search_results", [])
        if results:
            user = LoginManager.get_current_user()
            role = user["role"] if user else "user"
            allowed_cols = get_allowed_columns(user["username"], role)

            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>',
                        unsafe_allow_html=True)
            df_res = pd.DataFrame(results)
            # Daftar kolom yang ingin ditampilkan, filter Stok/Harga sesuai izin
            candidate_cols = ["File", "Part Number", "Part Name", "Quantity"]
            if "col_stok" in allowed_cols:
                candidate_cols.append("Stok")
            if "col_harga" in allowed_cols:
                candidate_cols.append("Harga")
            candidate_cols += ["Sheet", "Excel Row"]
            cols = [c for c in candidate_cols if c in df_res.columns]

            col_cfg = {
                "File":        st.column_config.TextColumn(width="medium"),
                "Part Number": st.column_config.TextColumn(width="medium"),
                "Part Name":   st.column_config.TextColumn(width="large"),
                "Quantity":    st.column_config.NumberColumn(width="small"),
                "Stok":        st.column_config.TextColumn(width="small"),
                "Harga":       st.column_config.TextColumn(width="medium"),
                "Sheet":       st.column_config.TextColumn(width="medium"),
                "Excel Row":   st.column_config.NumberColumn(width="small"),
            }
            st.dataframe(df_res[cols], hide_index=True,
                         column_config={k: v for k, v in col_cfg.items() if k in cols})
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
            st.warning(f"❌ Tidak ditemukan hasil untuk '{search_term}' di database lokal")

            if st.session_state.get("search_type") == "Part Number":
                # ── Ambil Part Name dari SIMS dan tampilkan tabel ringkas ──
                sims_info_key = f"sims_part_info_{search_term}"
                if sims_info_key not in st.session_state:
                    if SIMS_ENABLED:
                        try:
                            from sims_fetcher import get_sims_part_info
                            with st.spinner(f"🔎 Mengambil info part dari SIMS..."):
                                sims_info, _ = get_sims_part_info(search_term)
                            st.session_state[sims_info_key] = sims_info
                        except Exception:
                            st.session_state[sims_info_key] = {}
                    else:
                        st.session_state[sims_info_key] = {}

                sims_info = st.session_state.get(sims_info_key, {})
                part_name_sims = sims_info.get("partName", "") if sims_info else ""

                st.markdown("---")
                st.markdown("#### 📋 Info Part")
                df_info = pd.DataFrame([{
                    "Part Number": search_term,
                    "Part Name":   part_name_sims if part_name_sims else "—",
                    "File":        "—",
                    "Sheet":       "—",
                    "Qty":         "—",
                    "Stok":        "—",
                    "Status":      "❌ Tidak ditemukan di database lokal",
                }])
                st.dataframe(
                    df_info[["Part Number", "Part Name", "File", "Sheet", "Qty", "Stok", "Status"]],
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Part Number": st.column_config.TextColumn(width="medium"),
                        "Part Name":   st.column_config.TextColumn(width="large"),
                        "File":        st.column_config.TextColumn(width="medium"),
                        "Sheet":       st.column_config.TextColumn(width="medium"),
                        "Qty":         st.column_config.TextColumn(width="small"),
                        "Stok":        st.column_config.TextColumn(width="small"),
                        "Status":      st.column_config.TextColumn(width="large"),
                    }
                )
                st.markdown("---")

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

    def render_populasi_tab(self):
        st.markdown("### 🚛 Populasi Unit")

        if st.button("🔄 Refresh", key="refresh_populasi"):
            st.session_state.pop("populasi_df", None)
            st.rerun()

        df = self._load_populasi_data()

        if df.empty:
            st.warning("Tidak ada file Excel di folder data/populasi/. Pastikan file populasi sudah ditempatkan di folder tersebut.")
            return

        display_cols = [c for c in df.columns if not c.startswith("_source")]
        df_display   = df[display_cols].copy()

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