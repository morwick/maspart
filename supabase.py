"""
SUPABASE  (Auth + Permissions)
===============================
Gabungan supabase_auth.py dan supabase_permissions.py menjadi satu modul.
Tidak perlu install library supabase — hanya pakai requests.

═══════════════════════════════════════════════════════════
SETUP — secrets.toml
═══════════════════════════════════════════════════════════

    [supabase]
    url   = "https://wqmvzlhnvjxokghpexrm.supabase.co"
    key   = "eyJhbGci..."
    table = "users"

═══════════════════════════════════════════════════════════
SETUP — SQL Editor Supabase
═══════════════════════════════════════════════════════════

Untuk schema awal dan migrasi perbaikan, lihat:
    migrations/001_cleanup.sql

Ringkasan schema final:
    users         (id, username, password_hash, password*, role,
                   is_active, created_at, updated_at)
    permissions   (id, perm_type, username, keys, updated_at)
                   UNIQUE (perm_type, username)
    part_photos   (id, part_number, file_name, storage_path,
                   storage_url, file_size, uploaded_by, created_at)
                   UNIQUE (part_number, file_name)

    *kolom 'password' plaintext = legacy, akan di-purge
     setelah semua user login & ter-upgrade ke password_hash.

═══════════════════════════════════════════════════════════
CARA PAKAI DI app.py
═══════════════════════════════════════════════════════════

    try:
        from supabase import (
            # Auth
            SUPABASE_ENABLED,
            load_users_from_supabase,
            authenticate_from_supabase,
            render_user_management_tab,
            # Permissions
            SupabasePermissions,
            PERM_MENU, PERM_COLUMN, PERM_HARGA,
            SUPABASE_PERMS_ENABLED,
        )
    except ImportError:
        SUPABASE_ENABLED       = False
        SUPABASE_PERMS_ENABLED = False
        load_users_from_supabase   = None
        authenticate_from_supabase = None
        render_user_management_tab = None
        SupabasePermissions        = None
"""

from __future__ import annotations

import hmac
import threading
import time
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

try:
    import streamlit as st
    _HAS_ST = True
except ImportError:
    _HAS_ST = False

try:
    import bcrypt
    _HAS_BCRYPT = True
except ImportError:
    _HAS_BCRYPT = False


# ═══════════════════════════════════════════════════════════════════════════════
#  PASSWORD HASHING (bcrypt)
# ═══════════════════════════════════════════════════════════════════════════════

def _hash_password(plain: str) -> str:
    """Hash password plaintext jadi bcrypt. Return string hash atau '' jika gagal."""
    if not plain or not _HAS_BCRYPT:
        return ""
    try:
        return bcrypt.hashpw(plain.encode("utf-8"),
                             bcrypt.gensalt(rounds=10)).decode("utf-8")
    except Exception as e:
        print(f"[supabase] hash error: {e}")
        return ""


def _verify_password(plain: str, stored_hash: str, legacy_plain: str = "") -> bool:
    """
    Verifikasi password.
      - Kalau stored_hash ada (bcrypt) → pakai bcrypt.checkpw
      - Kalau hanya legacy_plain (kolom 'password' lama) → pakai compare_digest
    """
    if not plain:
        return False
    if stored_hash and _HAS_BCRYPT:
        try:
            return bcrypt.checkpw(plain.encode("utf-8"),
                                  stored_hash.encode("utf-8"))
        except Exception:
            return False
    if legacy_plain:
        return hmac.compare_digest(plain.strip(), legacy_plain.strip())
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED CONFIG HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _get_config() -> dict:
    """Baca konfigurasi Supabase dari .streamlit/secrets.toml."""
    if _HAS_ST:
        try:
            cfg = st.secrets.get("supabase", {})
            url = cfg.get("url", "").rstrip("/")
            if url.endswith("/rest/v1"):
                url = url[: -len("/rest/v1")]
            return {
                "url":   url,
                "key":   cfg.get("key", ""),
                "table": cfg.get("table", "users"),
            }
        except Exception:
            pass
    return {"url": "", "key": "", "table": "users"}


def _is_configured() -> bool:
    c = _get_config()
    return bool(
        c["url"] and c["key"]
        and "supabase.co" in c["url"]
        and not c["url"].startswith("https://xxxxxxxxxxx")
    )


def _headers(prefer: str = "") -> dict:
    key = _get_config()["key"]
    h = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _rest_url(table: str) -> str:
    return f"{_get_config()['url']}/rest/v1/{table}"


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH — Load & Authenticate Users
# ═══════════════════════════════════════════════════════════════════════════════

_AUTH_CACHE_TTL  = 120        # detik
_AUTH_TIMEOUT    = 10

_users_cache: Optional[pd.DataFrame] = None
_users_cache_time: float = 0.0
_auth_cache_lock = threading.Lock()

SUPABASE_ENABLED = False  # di-set True saat konfigurasi valid


def _invalidate_users_cache():
    global _users_cache, _users_cache_time
    with _auth_cache_lock:
        _users_cache      = None
        _users_cache_time = 0.0


def load_users_from_supabase(force: bool = False) -> pd.DataFrame:
    """
    Ambil semua user aktif dari Supabase via REST API.
    Return DataFrame kolom: username, password_hash, password, role
      - password_hash : bcrypt hash (kolom utama untuk verify login)
      - password      : kolom legacy plaintext (fallback transisi)
    """
    global _users_cache, _users_cache_time

    cols = ["username", "password_hash", "password", "role"]

    with _auth_cache_lock:
        if not force and _users_cache is not None:
            if time.time() - _users_cache_time < _AUTH_CACHE_TTL:
                return _users_cache.copy()

    if not _is_configured():
        return pd.DataFrame(columns=cols)

    cfg   = _get_config()
    table = cfg["table"]

    try:
        resp = requests.get(
            _rest_url(table),
            headers={**_headers(), "Accept": "application/json"},
            params={
                "select":    "username,password_hash,password,role",
                "is_active": "eq.true",
            },
            timeout=_AUTH_TIMEOUT,
        )
        resp.raise_for_status()
        rows = resp.json()

        if not rows:
            print(f"[supabase] Tabel '{table}' kosong.")
            empty = pd.DataFrame(columns=cols)
            with _auth_cache_lock:
                _users_cache      = empty
                _users_cache_time = time.time()
            return empty.copy()

        df = pd.DataFrame(rows)
        for col in cols:
            if col not in df.columns:
                df[col] = ""

        df["username"]      = df["username"].astype(str).str.strip().str.lower()
        df["password_hash"] = df["password_hash"].fillna("").astype(str).str.strip()
        df["password"]      = df["password"].fillna("").astype(str).str.strip()
        df["role"]          = df["role"].astype(str).str.strip().str.lower().fillna("user")
        df = df[cols].drop_duplicates("username")

        with _auth_cache_lock:
            _users_cache      = df
            _users_cache_time = time.time()

        print(f"[supabase] ✅ {len(df)} user dimuat dari Supabase.")
        return df.copy()

    except requests.exceptions.ConnectionError:
        print("[supabase] ❌ Tidak bisa terhubung ke Supabase.")
    except requests.exceptions.Timeout:
        print("[supabase] ❌ Timeout koneksi ke Supabase.")
    except Exception as e:
        print(f"[supabase] ❌ Gagal load users: {e}")

    with _auth_cache_lock:
        if _users_cache is not None:
            print("[supabase] ⚠️ Menggunakan cache user terakhir.")
            return _users_cache.copy()

    return pd.DataFrame(columns=cols)


def authenticate_from_supabase(username: str, password: str) -> Optional[dict]:
    """
    Verifikasi username + password dari Supabase.
    Prioritas verifikasi:
      1. password_hash (bcrypt) — kolom utama
      2. password plaintext     — fallback legacy (akan di-upgrade ke hash)
    Return dict user atau None.
    """
    if not username or not password:
        return None

    df    = load_users_from_supabase()
    uname = username.strip().lower()
    row   = df[df["username"] == uname]

    if row.empty:
        print(f"[supabase] User '{uname}' tidak ditemukan.")
        return None

    stored_hash = str(row.iloc[0].get("password_hash", "") or "")
    legacy_pw   = str(row.iloc[0].get("password", "") or "")

    if _verify_password(password, stored_hash, legacy_plain=legacy_pw):
        now = datetime.now()
        print(f"[supabase] ✅ Login: {uname} ({row.iloc[0]['role']})")

        # Upgrade transparan: kalau user masih pakai password plaintext,
        # hash dan simpan ke password_hash agar plaintext bisa di-purge.
        if not stored_hash and legacy_pw and _HAS_BCRYPT:
            new_hash = _hash_password(password)
            if new_hash:
                ok, _ = update_user(uname, {"password_hash": new_hash, "password": None})
                if ok:
                    print(f"[supabase] 🔐 Hash password di-upgrade utk '{uname}'.")

        return {
            "username":    uname,
            "role":        row.iloc[0]["role"],
            "login_time":  now,
            "last_active": now,
        }

    print(f"[supabase] ❌ Password salah: {uname}")
    return None


# ── CRUD User ─────────────────────────────────────────────────────────────────

def add_user(username: str, password: str, role: str = "user") -> tuple[bool, str]:
    if not _is_configured():
        return False, "Supabase tidak terkonfigurasi."
    if not _HAS_BCRYPT:
        return False, "Library 'bcrypt' belum terinstall (pip install bcrypt)."
    uname = username.strip().lower()
    pw_hash = _hash_password(password.strip())
    if not pw_hash:
        return False, "Gagal hash password."
    try:
        resp = requests.post(
            _rest_url(_get_config()["table"]),
            headers=_headers("return=minimal"),
            json={"username": uname, "password_hash": pw_hash,
                  "role": role.strip().lower(), "is_active": True},
            timeout=_AUTH_TIMEOUT,
        )
        if resp.status_code in (200, 201):
            _invalidate_users_cache()
            return True, f"User '{uname}' berhasil ditambahkan."
        err = resp.text
        if "duplicate" in err.lower() or "unique" in err.lower():
            return False, f"Username '{uname}' sudah terdaftar."
        return False, f"Gagal: {err[:200]}"
    except Exception as e:
        return False, str(e)


def update_user(username: str, data: dict) -> tuple[bool, str]:
    if not _is_configured():
        return False, "Supabase tidak terkonfigurasi."
    uname = username.strip().lower()
    try:
        resp = requests.patch(
            _rest_url(_get_config()["table"]),
            headers=_headers("return=minimal"),
            params={"username": f"eq.{uname}"},
            json=data,
            timeout=_AUTH_TIMEOUT,
        )
        if resp.status_code in (200, 204):
            _invalidate_users_cache()
            return True, f"User '{uname}' diperbarui."
        return False, f"Gagal: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)


def update_user_password(username: str, new_password: str) -> tuple[bool, str]:
    if not _HAS_BCRYPT:
        return False, "Library 'bcrypt' belum terinstall (pip install bcrypt)."
    pw_hash = _hash_password(new_password.strip())
    if not pw_hash:
        return False, "Gagal hash password."
    # Set hash baru sekaligus null-kan plaintext legacy
    return update_user(username, {"password_hash": pw_hash, "password": None})


def update_user_role(username: str, new_role: str) -> tuple[bool, str]:
    if new_role not in ("admin", "user"):
        return False, "Role tidak valid. Gunakan 'admin' atau 'user'."
    return update_user(username, {"role": new_role})


def deactivate_user(username: str) -> tuple[bool, str]:
    ok, msg = update_user(username, {"is_active": False})
    return ok, f"User '{username}' dinonaktifkan." if ok else msg


def delete_user(username: str) -> tuple[bool, str]:
    if not _is_configured():
        return False, "Supabase tidak terkonfigurasi."
    uname = username.strip().lower()
    try:
        resp = requests.delete(
            _rest_url(_get_config()["table"]),
            headers=_headers("return=minimal"),
            params={"username": f"eq.{uname}"},
            timeout=_AUTH_TIMEOUT,
        )
        if resp.status_code in (200, 204):
            _invalidate_users_cache()
            return True, f"User '{uname}' dihapus."
        return False, f"Gagal: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)


# ── Streamlit UI — Tab Manajemen User ─────────────────────────────────────────

def render_user_management_tab():
    """
    UI Streamlit kelola user Supabase.
    Panggil dari tab admin di app.py:
        from supabase import render_user_management_tab
        render_user_management_tab()
    """
    import streamlit as st

    st.markdown("### 👥 Manajemen User (Supabase)")

    if not _is_configured():
        st.error(
            "❌ Supabase belum dikonfigurasi.\n\n"
            "Tambahkan di `.streamlit/secrets.toml`:\n"
            "```toml\n[supabase]\nurl   = \"https://xxx.supabase.co\"\n"
            "key   = \"eyJ...\"\ntable = \"users\"\n```"
        )
        return

    col_r, _ = st.columns([1, 5])
    with col_r:
        if st.button("🔄 Refresh", key="sb_refresh"):
            _invalidate_users_cache()
            st.session_state.pop("_sb_users_df", None)

    if "_sb_users_df" not in st.session_state:
        with st.spinner("Memuat data user..."):
            st.session_state["_sb_users_df"] = load_users_from_supabase(force=True)

    df = st.session_state["_sb_users_df"]

    if df.empty:
        st.warning("Belum ada user di database Supabase.")
    else:
        st.dataframe(df[["username", "role"]], use_container_width=True, hide_index=True)
        st.caption(f"Total: {len(df)} user aktif")

    st.markdown("---")

    with st.expander("➕ Tambah User Baru", expanded=False):
        nu_user = st.text_input("Username:", key="sb_nu_user")
        nu_pass = st.text_input("Password:", type="password", key="sb_nu_pass")
        nu_role = st.selectbox("Role:", ["user", "admin"], key="sb_nu_role")
        if st.button("Tambah User", type="primary", key="sb_btn_add"):
            if nu_user and nu_pass:
                ok, msg = add_user(nu_user, nu_pass, nu_role)
                st.success(msg) if ok else st.error(msg)
                if ok:
                    st.session_state.pop("_sb_users_df", None)
                    st.rerun()
            else:
                st.warning("Username dan password wajib diisi.")

    with st.expander("🔑 Ubah Password", expanded=False):
        if not df.empty:
            up_sel  = st.selectbox("Pilih user:", df["username"].tolist(), key="sb_up_sel")
            up_pass = st.text_input("Password baru:", type="password", key="sb_up_pass")
            if st.button("Simpan Password", key="sb_btn_pass"):
                if up_pass:
                    ok, msg = update_user_password(up_sel, up_pass)
                    st.success(msg) if ok else st.error(msg)
                else:
                    st.warning("Password tidak boleh kosong.")

    with st.expander("🎭 Ubah Role", expanded=False):
        if not df.empty:
            ur_sel  = st.selectbox("Pilih user:", df["username"].tolist(), key="sb_ur_sel")
            ur_role = st.selectbox("Role baru:", ["user", "admin"], key="sb_ur_role")
            if st.button("Simpan Role", key="sb_btn_role"):
                ok, msg = update_user_role(ur_sel, ur_role)
                st.success(msg) if ok else st.error(msg)
                if ok:
                    st.session_state.pop("_sb_users_df", None)
                    st.rerun()

    with st.expander("🗑️ Nonaktifkan / Hapus User", expanded=False):
        if not df.empty:
            dl_sel = st.selectbox("Pilih user:", df["username"].tolist(), key="sb_dl_sel")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("⛔ Nonaktifkan", key="sb_btn_deact"):
                    ok, msg = deactivate_user(dl_sel)
                    st.success(msg) if ok else st.error(msg)
                    if ok:
                        st.session_state.pop("_sb_users_df", None)
                        st.rerun()
            with c2:
                if st.button("🗑️ Hapus Permanen", key="sb_btn_del", type="primary"):
                    ok, msg = delete_user(dl_sel)
                    st.success(msg) if ok else st.error(msg)
                    if ok:
                        st.session_state.pop("_sb_users_df", None)
                        st.rerun()


# ─── Cek konfigurasi saat import ──────────────────────────────────────────────
if _is_configured():
    SUPABASE_ENABLED = True
    print("[supabase] ✅ Konfigurasi Supabase ditemukan.")
else:
    print("[supabase] ⚠️  Supabase belum dikonfigurasi di secrets.toml.")


# ═══════════════════════════════════════════════════════════════════════════════
#  PERMISSIONS — Menu, Column, Harga SubTab
# ═══════════════════════════════════════════════════════════════════════════════

_PERMS_TIMEOUT   = 10
_PERMS_CACHE_TTL = 30          # detik
_PERMS_TABLE     = "permissions"

# Tipe permissions yang valid
PERM_MENU   = "menu"
PERM_COLUMN = "column"
PERM_HARGA  = "harga_subtab"

# Cache internal: { "menu": {"__ts": float, "data": dict}, ... }
_perms_cache: dict = {}
_perms_cache_lock  = threading.Lock()

SUPABASE_PERMS_ENABLED = _is_configured()


# ── Core: baca & tulis ke Supabase ────────────────────────────────────────────

def _perms_load_from_supabase(perm_type: str) -> dict:
    """Baca semua baris perm_type dari Supabase. Return {username: [keys]}."""
    try:
        resp = requests.get(
            _rest_url(_PERMS_TABLE),
            headers={**_headers(), "Accept": "application/json"},
            params={"select": "username,keys", "perm_type": f"eq.{perm_type}"},
            timeout=_PERMS_TIMEOUT,
        )
        resp.raise_for_status()
        rows   = resp.json()
        result = {}
        for row in rows:
            uname = row.get("username", "")
            keys  = row.get("keys", [])
            if uname:
                result[uname] = keys if isinstance(keys, list) else list(keys)
        return result
    except Exception as e:
        print(f"[supabase] ❌ perms load '{perm_type}': {e}")
        return {}


def _perms_save_to_supabase(perm_type: str, username: str, keys: list) -> bool:
    """Upsert satu baris (perm_type, username) ke Supabase.

    Strategi: SELECT dulu untuk tahu existing/baru → branch UPDATE atau INSERT.
    Tidak mengandalkan Content-Range karena PostgREST balas '*/*' saat 0 rows
    match (ambigu dengan rows-updated).
    """
    payload = {
        "keys":       keys,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    try:
        # 1) Cek existing row
        r0 = requests.get(
            _rest_url(_PERMS_TABLE),
            headers={**_headers(), "Accept": "application/json"},
            params={
                "select":    "id",
                "perm_type": f"eq.{perm_type}",
                "username":  f"eq.{username}",
                "limit":     "1",
            },
            timeout=_PERMS_TIMEOUT,
        )
        r0.raise_for_status()
        existing = r0.json()

        if existing:
            # 2a) UPDATE
            resp = requests.patch(
                _rest_url(_PERMS_TABLE),
                headers=_headers("return=minimal"),
                params={
                    "perm_type": f"eq.{perm_type}",
                    "username":  f"eq.{username}",
                },
                json=payload,
                timeout=_PERMS_TIMEOUT,
            )
            if resp.status_code in (200, 204):
                return True
            print(f"[supabase] ❌ perms PATCH '{perm_type}/{username}': "
                  f"{resp.status_code} {resp.text[:200]}")
            return False

        # 2b) INSERT
        insert_payload = {
            "perm_type":  perm_type,
            "username":   username,
            **payload,
        }
        resp2 = requests.post(
            _rest_url(_PERMS_TABLE),
            headers=_headers("return=minimal"),
            json=insert_payload,
            timeout=_PERMS_TIMEOUT,
        )
        if resp2.status_code in (200, 201, 204):
            return True
        print(f"[supabase] ❌ perms INSERT '{perm_type}/{username}': "
              f"{resp2.status_code} {resp2.text[:200]}")
        return False

    except Exception as e:
        print(f"[supabase] ❌ perms save '{perm_type}/{username}': {e}")
        return False


def _perms_delete_from_supabase(perm_type: str, username: str) -> bool:
    """Hapus baris (perm_type, username) dari Supabase."""
    try:
        resp = requests.delete(
            _rest_url(_PERMS_TABLE),
            headers=_headers("return=minimal"),
            params={
                "perm_type": f"eq.{perm_type}",
                "username":  f"eq.{username}",
            },
            timeout=_PERMS_TIMEOUT,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        print(f"[supabase] ❌ perms delete '{perm_type}/{username}': {e}")
        return False


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _perms_get_cached(perm_type: str) -> Optional[dict]:
    with _perms_cache_lock:
        entry = _perms_cache.get(perm_type)
        if entry and time.time() - entry["__ts"] < _PERMS_CACHE_TTL:
            return dict(entry["data"])
    return None


def _perms_set_cached(perm_type: str, data: dict):
    with _perms_cache_lock:
        _perms_cache[perm_type] = {"__ts": time.time(), "data": dict(data)}


def _perms_invalidate(perm_type: str):
    with _perms_cache_lock:
        _perms_cache.pop(perm_type, None)
    if _HAS_ST:
        for key in list(st.session_state.keys()):
            if perm_type in key and "permissions" in key:
                st.session_state.pop(key, None)


# ── Public API: SupabasePermissions ───────────────────────────────────────────

class SupabasePermissions:
    """
    Drop-in replacement untuk GitHub sync + file JSON lokal.

    Contoh:
        perms = SupabasePermissions.load(PERM_MENU, default_keys)
        SupabasePermissions.save(PERM_MENU, "user1", ["tab_search_pn"])
        SupabasePermissions.remove(PERM_MENU, "user1")
    """

    @staticmethod
    def is_available() -> bool:
        return _is_configured()

    @staticmethod
    def load(perm_type: str, default_keys: list, force: bool = False) -> dict:
        """
        Load permissions untuk perm_type.
        Return dict: {"username": [keys...], "__default__": [keys...]}
        """
        if not _is_configured():
            return {"__default__": list(default_keys)}

        if not force:
            cached = _perms_get_cached(perm_type)
            if cached is not None:
                return cached

        data = _perms_load_from_supabase(perm_type)
        if "__default__" not in data:
            data["__default__"] = list(default_keys)

        _perms_set_cached(perm_type, data)
        return data

    @staticmethod
    def save(perm_type: str, username: str, keys: list) -> bool:
        """Simpan/update permissions satu user (atau __default__)."""
        if not _is_configured():
            return False
        ok = _perms_save_to_supabase(perm_type, username.strip().lower(), keys)
        if ok:
            _perms_invalidate(perm_type)
        return ok

    @staticmethod
    def save_all(perm_type: str, permissions: dict) -> bool:
        """Simpan seluruh dict permissions sekaligus (berguna saat migrasi)."""
        if not _is_configured():
            return False
        success = True
        for username, keys in permissions.items():
            ok = _perms_save_to_supabase(perm_type, username, keys)
            if not ok:
                success = False
        if success:
            _perms_invalidate(perm_type)
        return success

    @staticmethod
    def remove(perm_type: str, username: str) -> bool:
        """Hapus konfigurasi permissions untuk username tertentu."""
        if not _is_configured():
            return False
        ok = _perms_delete_from_supabase(perm_type, username.strip().lower())
        if ok:
            _perms_invalidate(perm_type)
        return ok

    @staticmethod
    def invalidate(perm_type: str):
        """Reset cache untuk perm_type tertentu."""
        _perms_invalidate(perm_type)


# ═══════════════════════════════════════════════════════════════════════════════
#  STOK OPNAME — Sessions per User
# ═══════════════════════════════════════════════════════════════════════════════
# Tabel: opname_sessions  (lihat migrations/002_opname.sql)
#   id, session_id (uuid unique), username, is_draft, payload (jsonb),
#   started_at, updated_at, finalized_at
# ═══════════════════════════════════════════════════════════════════════════════

_OPNAME_TABLE   = "opname_sessions"
_OPNAME_TIMEOUT = 15

SUPABASE_OPNAME_ENABLED = _is_configured()


def _opname_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class SupabaseOpname:
    """
    REST CRUD untuk sesi stok opname.
    Setiap sesi disimpan utuh sebagai 1 row dengan payload JSONB.

    Public API mengikuti modul stok_opname (drop-in replacement):
      load_draft / save_draft / delete_draft
      load_history / finalize / delete_history_entry
    """

    @staticmethod
    def is_available() -> bool:
        return SUPABASE_OPNAME_ENABLED and _is_configured()

    # ── Draft ─────────────────────────────────────────────────────────────────

    @staticmethod
    def load_draft(username: str) -> Optional[dict]:
        try:
            resp = requests.get(
                _rest_url(_OPNAME_TABLE),
                headers={**_headers(), "Accept": "application/json"},
                params={
                    "select":   "payload",
                    "username": f"eq.{username}",
                    "is_draft": "eq.true",
                    "limit":    "1",
                },
                timeout=_OPNAME_TIMEOUT,
            )
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                return None
            return rows[0].get("payload")
        except Exception as e:
            print(f"[supabase] ❌ opname load_draft '{username}': {e}")
            return None

    @staticmethod
    def save_draft(username: str, session: dict) -> tuple[bool, Optional[str]]:
        """Upsert draft. SELECT dulu untuk tahu existing/tidak → branch UPDATE/INSERT."""
        sid = session.get("session_id", "")
        if not sid:
            return False, "session_id kosong"
        session["updated_at"] = _opname_now_iso()
        try:
            # 1) Cek apakah draft existing untuk user ini
            r0 = requests.get(
                _rest_url(_OPNAME_TABLE),
                headers={**_headers(), "Accept": "application/json"},
                params={
                    "select":   "id",
                    "username": f"eq.{username}",
                    "is_draft": "eq.true",
                    "limit":    "1",
                },
                timeout=_OPNAME_TIMEOUT,
            )
            r0.raise_for_status()
            existing = r0.json()

            if existing:
                # 2a) UPDATE existing draft
                resp = requests.patch(
                    _rest_url(_OPNAME_TABLE),
                    headers=_headers("return=minimal"),
                    params={
                        "username": f"eq.{username}",
                        "is_draft": "eq.true",
                    },
                    json={
                        "session_id": sid,
                        "payload":    session,
                        "started_at": session.get("started_at"),
                        "updated_at": session["updated_at"],
                    },
                    timeout=_OPNAME_TIMEOUT,
                )
                if resp.status_code in (200, 204):
                    return True, None
                return False, f"PATCH HTTP {resp.status_code}: {resp.text[:200]}"

            # 2b) INSERT baru
            insert_payload = {
                "session_id":   sid,
                "username":     username,
                "is_draft":     True,
                "payload":      session,
                "started_at":   session.get("started_at"),
                "updated_at":   session["updated_at"],
                "finalized_at": None,
            }
            resp2 = requests.post(
                _rest_url(_OPNAME_TABLE),
                headers=_headers("return=minimal"),
                json=insert_payload,
                timeout=_OPNAME_TIMEOUT,
            )
            if resp2.status_code in (200, 201, 204):
                return True, None
            return False, f"INSERT HTTP {resp2.status_code}: {resp2.text[:200]}"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def delete_draft(username: str) -> bool:
        try:
            resp = requests.delete(
                _rest_url(_OPNAME_TABLE),
                headers=_headers("return=minimal"),
                params={
                    "username": f"eq.{username}",
                    "is_draft": "eq.true",
                },
                timeout=_OPNAME_TIMEOUT,
            )
            return resp.status_code in (200, 204)
        except Exception as e:
            print(f"[supabase] ❌ opname delete_draft '{username}': {e}")
            return False

    # ── History ───────────────────────────────────────────────────────────────

    @staticmethod
    def load_history(username: str, limit: int = 200) -> list:
        try:
            resp = requests.get(
                _rest_url(_OPNAME_TABLE),
                headers={**_headers(), "Accept": "application/json"},
                params={
                    "select":   "payload",
                    "username": f"eq.{username}",
                    "is_draft": "eq.false",
                    "order":    "finalized_at.desc",
                    "limit":    str(limit),
                },
                timeout=_OPNAME_TIMEOUT,
            )
            resp.raise_for_status()
            rows = resp.json()
            return [r.get("payload") for r in rows if r.get("payload")]
        except Exception as e:
            print(f"[supabase] ❌ opname load_history '{username}': {e}")
            return []

    @staticmethod
    def finalize(username: str, session: dict) -> tuple[bool, Optional[str]]:
        """
        Ubah draft milik user ini jadi history (is_draft=false, finalized_at=now).
        SELECT dulu untuk tahu existing — kalau tidak ada, INSERT langsung sebagai history.
        """
        sid = session.get("session_id", "")
        if not sid:
            return False, "session_id kosong"
        now = _opname_now_iso()
        session["finalized"]    = True
        session["finalized_at"] = now
        try:
            r0 = requests.get(
                _rest_url(_OPNAME_TABLE),
                headers={**_headers(), "Accept": "application/json"},
                params={
                    "select":     "id",
                    "username":   f"eq.{username}",
                    "session_id": f"eq.{sid}",
                    "is_draft":   "eq.true",
                    "limit":      "1",
                },
                timeout=_OPNAME_TIMEOUT,
            )
            r0.raise_for_status()
            existing = r0.json()

            if existing:
                resp = requests.patch(
                    _rest_url(_OPNAME_TABLE),
                    headers=_headers("return=minimal"),
                    params={
                        "username":   f"eq.{username}",
                        "is_draft":   "eq.true",
                        "session_id": f"eq.{sid}",
                    },
                    json={
                        "is_draft":     False,
                        "payload":      session,
                        "finalized_at": now,
                        "updated_at":   now,
                    },
                    timeout=_OPNAME_TIMEOUT,
                )
                if resp.status_code in (200, 204):
                    return True, None
                return False, f"PATCH HTTP {resp.status_code}: {resp.text[:200]}"

            # Tidak ada draft — insert langsung sebagai history
            insert_payload = {
                "session_id":   sid,
                "username":     username,
                "is_draft":     False,
                "payload":      session,
                "started_at":   session.get("started_at"),
                "updated_at":   now,
                "finalized_at": now,
            }
            r2 = requests.post(
                _rest_url(_OPNAME_TABLE),
                headers=_headers("return=minimal"),
                json=insert_payload,
                timeout=_OPNAME_TIMEOUT,
            )
            if r2.status_code in (200, 201, 204):
                return True, None
            return False, f"INSERT HTTP {r2.status_code}: {r2.text[:200]}"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def delete_history_entry(username: str, session_id: str) -> bool:
        try:
            resp = requests.delete(
                _rest_url(_OPNAME_TABLE),
                headers=_headers("return=minimal"),
                params={
                    "username":   f"eq.{username}",
                    "session_id": f"eq.{session_id}",
                    "is_draft":   "eq.false",
                },
                timeout=_OPNAME_TIMEOUT,
            )
            return resp.status_code in (200, 204)
        except Exception as e:
            print(f"[supabase] ❌ opname delete_history '{username}/{session_id}': {e}")
            return False


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI Test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    print("=" * 55)
    print("  SUPABASE (Auth + Permissions) — CLI Test")
    print("=" * 55)
    print(f"  Configured : {_is_configured()}")
    print(f"  Auth       : {SUPABASE_ENABLED}")
    print(f"  Perms      : {SUPABASE_PERMS_ENABLED}")

    df = load_users_from_supabase()
    print(f"\nTotal users: {len(df)}")
    if not df.empty:
        print(df[["username", "role"]].to_string(index=False))

    if len(sys.argv) >= 3:
        result = authenticate_from_supabase(sys.argv[1], sys.argv[2])
        print(f"\n{'✅ Login OK: ' + str(result) if result else '❌ Login gagal'}")