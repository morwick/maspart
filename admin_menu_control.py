"""
ADMIN MENU CONTROL
==================
Modul untuk mengontrol akses menu per username DAN akses kolom Stok/Harga.
Admin dapat mengaktifkan/menonaktifkan tab dan kolom untuk setiap user.

Cara Pakai:
-----------
1. Tambahkan import di app.py:
       from admin_menu_control import (
           MenuAccessManager, ColumnAccessManager,
           render_admin_menu_control_tab,
           get_allowed_tabs, get_allowed_columns,
       )

2. Di display_dashboard(), sebelum st.tabs(), tambahkan:
       allowed_tabs = get_allowed_tabs(user["username"], role)

3. Di display_search_results() dan render tempat Stok/Harga ditampilkan:
       allowed_cols = get_allowed_columns(user["username"], role)
       # Sembunyikan kolom jika "col_stok" / "col_harga" tidak ada di allowed_cols

4. Di sidebar admin panel, tambahkan tombol reload:
       MenuAccessManager.load_permissions(force=True)
       ColumnAccessManager.load_permissions(force=True)
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import streamlit as st

# ── Konfigurasi ────────────────────────────────────────────────────────────────

# Semua tab yang tersedia beserta key uniknya
ALL_MENU_TABS: dict[str, str] = {
    "tab_search_pn":   "🔢 Search Part Number",
    "tab_search_name": "📝 Search Part Name",
    "tab_batch":       "📥 Batch Download",
    "tab_populasi":    "🚛 Populasi Unit",
    "tab_harga":       "💰 Harga",
}

# Tab yang selalu aktif untuk semua user (tidak bisa dinonaktifkan)
ALWAYS_ALLOWED: set[str] = {"tab_search_pn"}

# File penyimpanan konfigurasi akses menu
MENU_CONFIG_FILE = Path("login/menu_permissions.json")

# ── Konfigurasi Akses Kolom ────────────────────────────────────────────────────

ALL_COLUMN_ACCESS: dict[str, str] = {
    "col_stok":  "📦 Kolom Stok",
    "col_harga": "💲 Kolom Harga",
}

COLUMN_CONFIG_FILE = Path("login/column_permissions.json")


class ColumnAccessManager:
    """
    Mengelola izin tampil kolom Stok & Harga per username.
    Disimpan di login/column_permissions.json.
    Format: {"username": ["col_stok", "col_harga"], "__default__": ["col_stok", "col_harga"]}
    """
    _CACHE_KEY = "_column_permissions_cache"

    @classmethod
    def load_permissions(cls, force: bool = False) -> dict:
        if not force and cls._CACHE_KEY in st.session_state:
            return st.session_state[cls._CACHE_KEY]
        path = COLUMN_CONFIG_FILE
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        else:
            data = {}
        if "__default__" not in data:
            data["__default__"] = list(ALL_COLUMN_ACCESS.keys())
        st.session_state[cls._CACHE_KEY] = data
        return data

    @classmethod
    def save_permissions(cls, permissions: dict) -> tuple[bool, str | None]:
        path = COLUMN_CONFIG_FILE
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(permissions, f, indent=2, ensure_ascii=False)
            st.session_state[cls._CACHE_KEY] = permissions
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_user_columns(cls, username: str) -> list[str]:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            return perms[uname]
        return perms.get("__default__", list(ALL_COLUMN_ACCESS.keys()))

    @classmethod
    def set_user_columns(cls, username: str, col_keys: list[str]) -> tuple[bool, str | None]:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        final = [ck for ck in col_keys if ck in ALL_COLUMN_ACCESS]
        perms[uname] = final
        return cls.save_permissions(perms)

    @classmethod
    def set_default_columns(cls, col_keys: list[str]) -> tuple[bool, str | None]:
        perms = cls.load_permissions()
        final = [ck for ck in col_keys if ck in ALL_COLUMN_ACCESS]
        perms["__default__"] = final
        return cls.save_permissions(perms)

    @classmethod
    def remove_user_config(cls, username: str) -> tuple[bool, str | None]:
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            del perms[uname]
            return cls.save_permissions(perms)
        return True, None


def get_allowed_columns(username: str, role: str) -> set[str]:
    """Admin selalu mendapat semua kolom. User lain sesuai konfigurasi."""
    if role == "admin":
        return set(ALL_COLUMN_ACCESS.keys())
    return set(ColumnAccessManager.get_user_columns(username))


# ── MenuAccessManager ──────────────────────────────────────────────────────────

class MenuAccessManager:
    """
    Mengelola izin akses menu per username.
    Data disimpan di login/menu_permissions.json.
    Format JSON:
    {
        "username1": ["tab_search_pn", "tab_search_name", "tab_harga"],
        "username2": ["tab_search_pn", "tab_populasi"],
        "__default__": ["tab_search_pn", "tab_search_name"]
    }
    Key "__default__" berlaku untuk username yang belum dikonfigurasi.
    """

    _CACHE_KEY = "_menu_permissions_cache"

    @classmethod
    def _config_path(cls) -> Path:
        return MENU_CONFIG_FILE

    @classmethod
    def load_permissions(cls, force: bool = False) -> dict:
        if not force and cls._CACHE_KEY in st.session_state:
            return st.session_state[cls._CACHE_KEY]

        path = cls._config_path()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        else:
            data = {}

        # Pastikan __default__ selalu ada
        if "__default__" not in data:
            data["__default__"] = list(ALL_MENU_TABS.keys())

        st.session_state[cls._CACHE_KEY] = data
        return data

    @classmethod
    def save_permissions(cls, permissions: dict) -> tuple[bool, str | None]:
        path = cls._config_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(permissions, f, indent=2, ensure_ascii=False)
            st.session_state[cls._CACHE_KEY] = permissions
            return True, None
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_user_tabs(cls, username: str) -> list[str]:
        """Kembalikan list tab key yang diizinkan untuk user tertentu."""
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            allowed = perms[uname]
        else:
            allowed = perms.get("__default__", list(ALL_MENU_TABS.keys()))
        # Selalu sertakan tab yang wajib
        result = list(ALWAYS_ALLOWED)
        for tab in allowed:
            if tab in ALL_MENU_TABS and tab not in result:
                result.append(tab)
        return result

    @classmethod
    def set_user_tabs(cls, username: str, tab_keys: list[str]) -> tuple[bool, str | None]:
        """Simpan konfigurasi tab untuk user tertentu."""
        perms = cls.load_permissions()
        uname = username.strip().lower()
        # Pastikan tab wajib selalu ada
        final = list(ALWAYS_ALLOWED)
        for tk in tab_keys:
            if tk in ALL_MENU_TABS and tk not in final:
                final.append(tk)
        perms[uname] = final
        return cls.save_permissions(perms)

    @classmethod
    def set_default_tabs(cls, tab_keys: list[str]) -> tuple[bool, str | None]:
        """Simpan konfigurasi tab default (untuk user yang belum dikonfigurasi)."""
        perms = cls.load_permissions()
        final = list(ALWAYS_ALLOWED)
        for tk in tab_keys:
            if tk in ALL_MENU_TABS and tk not in final:
                final.append(tk)
        perms["__default__"] = final
        return cls.save_permissions(perms)

    @classmethod
    def remove_user_config(cls, username: str) -> tuple[bool, str | None]:
        """Hapus konfigurasi khusus untuk user (kembali ke default)."""
        perms = cls.load_permissions()
        uname = username.strip().lower()
        if uname in perms:
            del perms[uname]
            return cls.save_permissions(perms)
        return True, None

    @classmethod
    def get_all_configured_users(cls) -> list[str]:
        perms = cls.load_permissions()
        return [k for k in perms.keys() if k != "__default__"]


# ── Helper untuk app.py ────────────────────────────────────────────────────────

def get_allowed_tabs(username: str, role: str) -> list[str]:
    """
    Kembalikan list tab key yang diizinkan.
    Admin selalu mendapatkan semua tab.
    """
    if role == "admin":
        return list(ALL_MENU_TABS.keys()) + ["tab_menu_control"]
    return MenuAccessManager.get_user_tabs(username)


# ── UI Admin: render tab Menu Control ──────────────────────────────────────────

def render_admin_menu_control_tab():
    """
    Render UI admin untuk mengontrol akses menu per username.
    Panggil di dalam blok tab 'Menu Control'.
    """
    st.markdown("### 🛡️ Kontrol Akses Menu per User")
    st.caption(
        "Admin dapat mengatur tab mana saja yang bisa diakses oleh masing-masing user. "
        "Tab **Search Part Number** selalu aktif dan tidak bisa dinonaktifkan."
    )

    # ── Load data user dari session ──
    df_users: pd.DataFrame = st.session_state.get("login_users_df", pd.DataFrame())
    if df_users.empty:
        st.warning("⚠️ Data user belum dimuat. Klik 'Reload Users' di sidebar terlebih dahulu.")
        return

    non_admin_users = df_users[df_users["role"] != "admin"]["username"].tolist()
    if not non_admin_users:
        st.info("Tidak ada user non-admin yang terdaftar.")
        return

    perms = MenuAccessManager.load_permissions()
    tab_items = list(ALL_MENU_TABS.items())   # [(key, label), ...]

    # ── Tabs internal ──
    t_user, t_default, t_summary, t_columns = st.tabs(
        ["👤 Per User", "⚙️ Default Akses", "📋 Ringkasan", "🔒 Akses Kolom"]
    )

    # ═══ Tab: Per User ═══════════════════════════════════════════════════════
    with t_user:
        st.markdown("#### Atur Akses Menu untuk User Tertentu")

        selected_user = st.selectbox(
            "Pilih Username:",
            options=non_admin_users,
            key="mac_sel_user",
        )

        if selected_user:
            uname = selected_user.strip().lower()
            current_tabs = perms.get(uname, perms.get("__default__", list(ALL_MENU_TABS.keys())))
            has_custom   = uname in perms

            if has_custom:
                st.info(f"ℹ️ User **{uname}** memiliki konfigurasi akses **khusus**.")
            else:
                st.info(f"ℹ️ User **{uname}** menggunakan konfigurasi **default**.")

            st.markdown("**Centang tab yang boleh diakses:**")

            new_selection: list[str] = []
            cols = st.columns(2)
            for i, (key, label) in enumerate(tab_items):
                with cols[i % 2]:
                    forced   = key in ALWAYS_ALLOWED
                    checked  = key in current_tabs
                    disabled = forced
                    val = st.checkbox(
                        label=f"{label}" + (" *(wajib)*" if forced else ""),
                        value=True if forced else checked,
                        key=f"mac_cb_{uname}_{key}",
                        disabled=disabled,
                        help="Tab ini selalu aktif dan tidak bisa dinonaktifkan." if forced else "",
                    )
                    if forced or val:
                        new_selection.append(key)

            st.markdown("---")
            col_save, col_reset = st.columns([1, 1])

            with col_save:
                if st.button(
                    "💾 Simpan Akses",
                    key=f"mac_save_{uname}",
                    type="primary",
                    use_container_width=True,
                ):
                    ok, err = MenuAccessManager.set_user_tabs(uname, new_selection)
                    if ok:
                        st.success(f"✅ Akses menu untuk **{uname}** berhasil disimpan!")
                        st.rerun()
                    else:
                        st.error(f"❌ Gagal menyimpan: {err}")

            with col_reset:
                if has_custom:
                    if st.button(
                        "↩️ Reset ke Default",
                        key=f"mac_reset_{uname}",
                        use_container_width=True,
                        help="Hapus konfigurasi khusus, gunakan default.",
                    ):
                        ok, err = MenuAccessManager.remove_user_config(uname)
                        if ok:
                            st.success(f"✅ Konfigurasi **{uname}** direset ke default.")
                            st.rerun()
                        else:
                            st.error(f"❌ Gagal reset: {err}")
                else:
                    st.button(
                        "↩️ Reset ke Default",
                        key=f"mac_reset_{uname}",
                        use_container_width=True,
                        disabled=True,
                        help="User ini sudah menggunakan konfigurasi default.",
                    )

    # ═══ Tab: Default Akses ══════════════════════════════════════════════════
    with t_default:
        st.markdown("#### Atur Akses Default")
        st.caption("Pengaturan ini berlaku untuk semua user yang **belum** dikonfigurasi secara khusus.")

        default_tabs = perms.get("__default__", list(ALL_MENU_TABS.keys()))

        new_default: list[str] = []
        cols2 = st.columns(2)
        for i, (key, label) in enumerate(tab_items):
            with cols2[i % 2]:
                forced  = key in ALWAYS_ALLOWED
                checked = key in default_tabs
                val = st.checkbox(
                    label=f"{label}" + (" *(wajib)*" if forced else ""),
                    value=True if forced else checked,
                    key=f"mac_def_{key}",
                    disabled=forced,
                    help="Tab ini selalu aktif." if forced else "",
                )
                if forced or val:
                    new_default.append(key)

        st.markdown("---")
        if st.button("💾 Simpan Default", type="primary", use_container_width=True, key="mac_save_default"):
            ok, err = MenuAccessManager.set_default_tabs(new_default)
            if ok:
                st.success("✅ Konfigurasi default berhasil disimpan!")
                st.rerun()
            else:
                st.error(f"❌ Gagal menyimpan: {err}")

    # ═══ Tab: Ringkasan ══════════════════════════════════════════════════════
    with t_summary:
        st.markdown("#### Ringkasan Akses Menu Semua User")

        rows = []
        for uname in non_admin_users:
            uname_lower   = uname.strip().lower()
            user_tabs     = MenuAccessManager.get_user_tabs(uname_lower)
            has_custom    = uname_lower in perms
            source        = "Khusus" if has_custom else "Default"
            row           = {"Username": uname, "Sumber": source}
            for key, label in tab_items:
                # Hanya tampilkan nama pendek tanpa emoji
                short = label.split(" ", 1)[-1] if " " in label else label
                row[short] = "✅" if key in user_tabs else "❌"
            rows.append(row)

        if rows:
            df_sum = pd.DataFrame(rows)
            st.dataframe(df_sum, hide_index=True, use_container_width=True)

        st.markdown("---")
        st.markdown("**Konfigurasi JSON saat ini:**")
        st.json(perms)

        if st.button("🔄 Reload Konfigurasi", key="mac_reload_cfg"):
            MenuAccessManager.load_permissions(force=True)
            st.success("✅ Konfigurasi dimuat ulang dari file.")
            st.rerun()

    # ═══ Tab: Akses Kolom ═════════════════════════════════════════════════════
    with t_columns:
        st.markdown("#### 🔒 Kontrol Akses Kolom Stok & Harga")
        st.caption(
            "Admin dapat menyembunyikan kolom **Stok** dan/atau **Harga** "
            "dari tampilan hasil pencarian dan batch download untuk user tertentu."
        )

        col_perms = ColumnAccessManager.load_permissions()
        col_items = list(ALL_COLUMN_ACCESS.items())

        tc_user, tc_default, tc_summary = st.tabs(
            ["👤 Per User", "⚙️ Default Kolom", "📋 Ringkasan Kolom"]
        )

        # ── Per User (kolom) ──────────────────────────────────────────────────
        with tc_user:
            st.markdown("##### Atur Akses Kolom untuk User Tertentu")
            sel_col_user = st.selectbox(
                "Pilih Username:", options=non_admin_users, key="cac_sel_user"
            )
            if sel_col_user:
                cu = sel_col_user.strip().lower()
                cur_cols    = col_perms.get(cu, col_perms.get("__default__", list(ALL_COLUMN_ACCESS.keys())))
                has_col_cfg = cu in col_perms

                if has_col_cfg:
                    st.info(f"ℹ️ User **{cu}** memiliki konfigurasi kolom **khusus**.")
                else:
                    st.info(f"ℹ️ User **{cu}** menggunakan konfigurasi kolom **default**.")

                st.markdown("**Centang kolom yang boleh dilihat user:**")
                new_col_sel: list[str] = []
                col_cb = st.columns(2)
                for i, (ckey, clabel) in enumerate(col_items):
                    with col_cb[i % 2]:
                        cval = st.checkbox(
                            label=clabel,
                            value=(ckey in cur_cols),
                            key=f"cac_cb_{cu}_{ckey}",
                        )
                        if cval:
                            new_col_sel.append(ckey)

                st.markdown("---")
                cc_save, cc_reset = st.columns(2)
                with cc_save:
                    if st.button("💾 Simpan Akses Kolom", key=f"cac_save_{cu}",
                                 type="primary", use_container_width=True):
                        ok, err = ColumnAccessManager.set_user_columns(cu, new_col_sel)
                        if ok:
                            st.success(f"✅ Akses kolom untuk **{cu}** berhasil disimpan!")
                            st.rerun()
                        else:
                            st.error(f"❌ Gagal menyimpan: {err}")
                with cc_reset:
                    if has_col_cfg:
                        if st.button("↩️ Reset ke Default Kolom", key=f"cac_reset_{cu}",
                                     use_container_width=True):
                            ok, err = ColumnAccessManager.remove_user_config(cu)
                            if ok:
                                st.success(f"✅ Konfigurasi kolom **{cu}** direset ke default.")
                                st.rerun()
                            else:
                                st.error(f"❌ Gagal reset: {err}")
                    else:
                        st.button("↩️ Reset ke Default Kolom", key=f"cac_reset_{cu}",
                                  use_container_width=True, disabled=True,
                                  help="User ini sudah menggunakan konfigurasi default.")

        # ── Default Kolom ─────────────────────────────────────────────────────
        with tc_default:
            st.markdown("##### Atur Akses Kolom Default")
            st.caption("Berlaku untuk semua user yang **belum** dikonfigurasi kolom secara khusus.")
            def_cols = col_perms.get("__default__", list(ALL_COLUMN_ACCESS.keys()))
            new_def_col: list[str] = []
            for ckey, clabel in col_items:
                cv = st.checkbox(
                    label=clabel,
                    value=(ckey in def_cols),
                    key=f"cac_def_{ckey}",
                )
                if cv:
                    new_def_col.append(ckey)
            st.markdown("---")
            if st.button("💾 Simpan Default Kolom", type="primary",
                         use_container_width=True, key="cac_save_default"):
                ok, err = ColumnAccessManager.set_default_columns(new_def_col)
                if ok:
                    st.success("✅ Konfigurasi default kolom berhasil disimpan!")
                    st.rerun()
                else:
                    st.error(f"❌ Gagal menyimpan: {err}")

        # ── Ringkasan Kolom ───────────────────────────────────────────────────
        with tc_summary:
            st.markdown("##### Ringkasan Akses Kolom Semua User")
            col_rows = []
            for uname in non_admin_users:
                ul    = uname.strip().lower()
                ucols = ColumnAccessManager.get_user_columns(ul)
                src   = "Khusus" if ul in col_perms else "Default"
                row_c = {"Username": uname, "Sumber": src}
                for ckey, clabel in col_items:
                    short = clabel.split(" ", 1)[-1] if " " in clabel else clabel
                    row_c[short] = "✅" if ckey in ucols else "❌"
                col_rows.append(row_c)
            if col_rows:
                st.dataframe(pd.DataFrame(col_rows), hide_index=True, use_container_width=True)
            st.markdown("---")
            st.markdown("**Konfigurasi JSON kolom saat ini:**")
            st.json(col_perms)
            if st.button("🔄 Reload Konfigurasi Kolom", key="cac_reload_cfg"):
                ColumnAccessManager.load_permissions(force=True)
                st.success("✅ Konfigurasi kolom dimuat ulang dari file.")
                st.rerun()