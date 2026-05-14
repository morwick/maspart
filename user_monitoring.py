"""
USER MONITORING — Logging helpers + Dashboard Admin
====================================================
Modul ini menyediakan:

  Writer (fail-silently — error tidak boleh menjatuhkan fitur utama):
    - log_login(username, success, reason, ip=None, user_agent=None)
    - log_activity(username, action, target=None, details=None)
    - touch_active(username)                # update users.last_active_at (throttled)

  Reader (untuk dashboard admin):
    - get_users_with_status()
    - get_activity_log(username, action, since, until, limit, offset)
    - get_login_history(username, success, since, until, limit, offset)
    - get_daily_stats(date)

  UI Streamlit:
    - render_user_monitoring_tab()          # 4 sub-tab

Backend memakai Supabase REST API (sama seperti supabase.py). Fungsi reader
dan writer adalah Python murni — tidak ada st.* di dalamnya, sehingga saat
migrasi FastAPI nanti cukup bikin router yang panggil fungsi-fungsi ini.

Skema tabel: lihat migrations/002_user_monitoring.sql
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

try:
    import streamlit as st
    _HAS_ST = True
except ImportError:
    _HAS_ST = False

# Pakai config helpers dari supabase.py supaya single source of truth.
try:
    from supabase import (
        _get_config,
        _headers,
        _rest_url,
        _is_configured,
        SUPABASE_ENABLED,
    )
    _BACKEND_READY = True
except Exception:
    _BACKEND_READY = False
    SUPABASE_ENABLED = False


# ── Konstanta ────────────────────────────────────────────────────────────────
_TIMEOUT             = 8                    # detik per request
_ONLINE_WINDOW_MIN   = 5                    # user dianggap online jika last_active < 5 menit lalu
_TOUCH_THROTTLE_SEC  = 60                   # interval minimum update last_active_at per session
_DEFAULT_PAGE_LIMIT  = 100
_USERS_TABLE         = "users"
_LOGIN_TABLE         = "login_history"
_ACTIVITY_TABLE      = "user_activity"


# ═══════════════════════════════════════════════════════════════════════════════
#  WRITER — Fail-silently helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_post(table: str, payload: dict) -> bool:
    """POST ke tabel Supabase, swallow error. Return True jika sukses."""
    if not _BACKEND_READY or not _is_configured():
        return False
    try:
        resp = requests.post(
            _rest_url(table),
            headers=_headers("return=minimal"),
            json=payload,
            timeout=_TIMEOUT,
        )
        if resp.status_code in (200, 201, 204):
            return True
        print(f"[monitoring] POST {table} gagal: {resp.status_code} {resp.text[:120]}")
        return False
    except Exception as e:
        print(f"[monitoring] POST {table} error: {e}")
        return False


def _safe_patch_user(username: str, data: dict) -> bool:
    """PATCH kolom di users.username = ?. Swallow error."""
    if not _BACKEND_READY or not _is_configured() or not username:
        return False
    try:
        resp = requests.patch(
            _rest_url(_USERS_TABLE),
            headers=_headers("return=minimal"),
            params={"username": f"eq.{username.strip().lower()}"},
            json=data,
            timeout=_TIMEOUT,
        )
        return resp.status_code in (200, 204)
    except Exception as e:
        print(f"[monitoring] PATCH users error: {e}")
        return False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_login(
    username: str,
    success: bool,
    reason: str = "",
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> None:
    """Catat percobaan login (sukses atau gagal). Fail-silently."""
    uname = (username or "").strip().lower()
    if not uname:
        return
    payload = {
        "username":   uname,
        "success":    bool(success),
        "reason":     reason or ("ok" if success else "fail"),
        "ip_address": ip_address,
        "user_agent": user_agent,
    }
    _safe_post(_LOGIN_TABLE, payload)

    # Jika sukses, refresh last_login_at & last_active_at.
    if success:
        _safe_patch_user(uname, {
            "last_login_at":  _now_iso(),
            "last_active_at": _now_iso(),
        })


def log_activity(
    username: str,
    action: str,
    target: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    """Catat aksi user (search, download, upload, dll). Fail-silently."""
    uname = (username or "").strip().lower()
    if not uname or not action:
        return
    payload = {
        "username": uname,
        "action":   action,
        "target":   target,
        "details":  details if details is None else json.loads(json.dumps(details, default=str)),
    }
    _safe_post(_ACTIVITY_TABLE, payload)
    # Sekalian refresh online status — throttle agar tidak spam.
    _touch_active_throttled(uname)


def _touch_active_throttled(username: str) -> None:
    """Update users.last_active_at, dengan throttle per-session 60 detik."""
    uname = (username or "").strip().lower()
    if not uname:
        return
    key = f"_mon_last_touch_{uname}"
    now_ts = time.time()
    if _HAS_ST:
        last_ts = st.session_state.get(key, 0.0)
        if now_ts - last_ts < _TOUCH_THROTTLE_SEC:
            return
        st.session_state[key] = now_ts
    _safe_patch_user(uname, {"last_active_at": _now_iso()})


# Public alias (di-import oleh app.py untuk LoginManager.is_authenticated)
def touch_active(username: str) -> None:
    _touch_active_throttled(username)


def get_client_context() -> tuple[Optional[str], Optional[str]]:
    """
    Best-effort ambil IP & User-Agent dari Streamlit request headers.
    Streamlit ≥ 1.37 expose lewat st.context.headers. Jika tidak ada, return (None, None).
    """
    if not _HAS_ST:
        return None, None
    try:
        ctx = getattr(st, "context", None)
        if ctx is None:
            return None, None
        headers = getattr(ctx, "headers", None) or {}
        # Beberapa platform pakai X-Forwarded-For (Cloudflare, proxy)
        ip = (
            headers.get("X-Forwarded-For")
            or headers.get("x-forwarded-for")
            or headers.get("X-Real-IP")
            or headers.get("x-real-ip")
        )
        if ip and "," in ip:
            ip = ip.split(",")[0].strip()
        ua = headers.get("User-Agent") or headers.get("user-agent")
        return (ip, ua)
    except Exception:
        return None, None


# ═══════════════════════════════════════════════════════════════════════════════
#  READER — Query helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_get(table: str, params: dict) -> list[dict]:
    """GET dari tabel Supabase, return list (empty jika error)."""
    if not _BACKEND_READY or not _is_configured():
        return []
    try:
        resp = requests.get(
            _rest_url(table),
            headers={**_headers(), "Accept": "application/json"},
            params=params,
            timeout=_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json() or []
        print(f"[monitoring] GET {table} gagal: {resp.status_code} {resp.text[:120]}")
        return []
    except Exception as e:
        print(f"[monitoring] GET {table} error: {e}")
        return []


def get_users_with_status() -> pd.DataFrame:
    """
    Return DataFrame semua user (active + inactive) dengan kolom:
      username, role, is_active, last_login_at, last_active_at, online
    """
    rows = _safe_get(_USERS_TABLE, {
        "select": "username,role,is_active,created_at,updated_at,last_login_at,last_active_at",
        "order":  "username.asc",
    })
    if not rows:
        return pd.DataFrame(columns=[
            "username", "role", "is_active",
            "last_login_at", "last_active_at", "online",
        ])
    df = pd.DataFrame(rows)
    for col in ("last_login_at", "last_active_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        else:
            df[col] = pd.NaT

    now_utc = pd.Timestamp.now(tz="UTC")
    threshold = now_utc - pd.Timedelta(minutes=_ONLINE_WINDOW_MIN)
    df["online"] = df["last_active_at"].notna() & (df["last_active_at"] >= threshold)
    return df


def get_activity_log(
    username: Optional[str] = None,
    action: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: int = _DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> pd.DataFrame:
    params = {
        "select": "id,username,action,target,details,created_at",
        "order":  "created_at.desc",
        "limit":  str(limit),
        "offset": str(offset),
    }
    if username:
        params["username"] = f"eq.{username.strip().lower()}"
    if action:
        params["action"] = f"eq.{action}"
    if since:
        params["created_at"] = f"gte.{since.astimezone(timezone.utc).isoformat()}"
    if until:
        # PostgREST hanya boleh satu filter per kolom kecuali via and=... — keep simple.
        # Pakai until via filter tambahan jika since juga ada.
        if since:
            params["and"] = (
                f"(created_at.gte.{since.astimezone(timezone.utc).isoformat()},"
                f"created_at.lte.{until.astimezone(timezone.utc).isoformat()})"
            )
            params.pop("created_at", None)
        else:
            params["created_at"] = f"lte.{until.astimezone(timezone.utc).isoformat()}"
    rows = _safe_get(_ACTIVITY_TABLE, params)
    if not rows:
        return pd.DataFrame(columns=["created_at", "username", "action", "target", "details"])
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df.sort_values("created_at", ascending=False).reset_index(drop=True)


def get_login_history(
    username: Optional[str] = None,
    success: Optional[bool] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    limit: int = _DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> pd.DataFrame:
    params = {
        "select": "id,username,success,reason,ip_address,user_agent,created_at",
        "order":  "created_at.desc",
        "limit":  str(limit),
        "offset": str(offset),
    }
    if username:
        params["username"] = f"eq.{username.strip().lower()}"
    if success is not None:
        params["success"] = f"eq.{'true' if success else 'false'}"
    if since:
        params["created_at"] = f"gte.{since.astimezone(timezone.utc).isoformat()}"
    if until:
        if since:
            params["and"] = (
                f"(created_at.gte.{since.astimezone(timezone.utc).isoformat()},"
                f"created_at.lte.{until.astimezone(timezone.utc).isoformat()})"
            )
            params.pop("created_at", None)
        else:
            params["created_at"] = f"lte.{until.astimezone(timezone.utc).isoformat()}"
    rows = _safe_get(_LOGIN_TABLE, params)
    if not rows:
        return pd.DataFrame(columns=[
            "created_at", "username", "success", "reason", "ip_address", "user_agent",
        ])
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    return df.sort_values("created_at", ascending=False).reset_index(drop=True)


def get_daily_stats(target_date: Optional[datetime] = None) -> dict:
    """
    Ringkasan statistik untuk satu tanggal (default: hari ini, UTC).
    Return dict:
      {
        "date": "YYYY-MM-DD",
        "login_success": int, "login_failed": int,
        "active_users": int,
        "actions_total": int,
        "top_users":    [(username, count), ...],
        "top_actions":  [(action,   count), ...],
      }
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc)
    start = datetime(
        target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc,
    )
    end = start + timedelta(days=1)

    # Pull semua row hari itu (limit besar; bisa diperketat nanti)
    logins = get_login_history(since=start, until=end, limit=2000)
    activities = get_activity_log(since=start, until=end, limit=5000)

    login_ok   = int(logins["success"].sum()) if not logins.empty else 0
    login_fail = int((~logins["success"]).sum()) if not logins.empty else 0

    active_users = (
        activities["username"].nunique() if not activities.empty else 0
    )
    actions_total = len(activities)

    top_users = []
    top_actions = []
    if not activities.empty:
        tu = activities.groupby("username").size().sort_values(ascending=False).head(5)
        top_users = list(tu.items())
        ta = activities.groupby("action").size().sort_values(ascending=False).head(5)
        top_actions = list(ta.items())

    return {
        "date":          start.date().isoformat(),
        "login_success": login_ok,
        "login_failed":  login_fail,
        "active_users":  active_users,
        "actions_total": actions_total,
        "top_users":     top_users,
        "top_actions":   top_actions,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  MIGRATION HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

def _check_migration_status() -> dict:
    """
    Cek apakah migrations/002_user_monitoring.sql sudah dijalankan.
    Return {'ok': bool, 'missing': [pesan]}.
    Hasil di-cache 60 detik di session_state agar tidak spam Supabase.
    """
    cache_key = "_mon_migration_status"
    if _HAS_ST:
        cached = st.session_state.get(cache_key)
        if cached and (time.time() - cached.get("ts", 0)) < 60:
            return cached["value"]

    missing: list[str] = []

    if _BACKEND_READY and _is_configured():
        # 1) Cek kolom last_login_at di users (pakai select untuk validasi schema)
        try:
            r = requests.get(
                _rest_url(_USERS_TABLE),
                headers={**_headers(), "Accept": "application/json", "Range": "0-0"},
                params={"select": "username,last_login_at,last_active_at", "limit": "1"},
                timeout=_TIMEOUT,
            )
            if r.status_code >= 400:
                body = r.text.lower()
                if "last_login_at" in body or "last_active_at" in body or "42703" in body:
                    missing.append("Kolom `users.last_login_at` & `users.last_active_at` belum ada.")
        except Exception:
            pass

        # 2) Cek tabel login_history
        try:
            r = requests.get(
                _rest_url(_LOGIN_TABLE),
                headers={**_headers(), "Accept": "application/json", "Range": "0-0"},
                params={"select": "id", "limit": "1"},
                timeout=_TIMEOUT,
            )
            if r.status_code == 404 or "PGRST205" in r.text:
                missing.append("Tabel `login_history` belum ada.")
        except Exception:
            pass

        # 3) Cek tabel user_activity
        try:
            r = requests.get(
                _rest_url(_ACTIVITY_TABLE),
                headers={**_headers(), "Accept": "application/json", "Range": "0-0"},
                params={"select": "id", "limit": "1"},
                timeout=_TIMEOUT,
            )
            if r.status_code == 404 or "PGRST205" in r.text:
                missing.append("Tabel `user_activity` belum ada.")
        except Exception:
            pass

    result = {"ok": len(missing) == 0, "missing": missing}
    if _HAS_ST:
        st.session_state[cache_key] = {"ts": time.time(), "value": result}
    return result


def _invalidate_dashboard_caches() -> None:
    """Bersihkan cache yang dipakai dashboard supaya refresh fetch baru."""
    if not _HAS_ST:
        return
    for key in ("_mon_migration_status",):
        st.session_state.pop(key, None)


# ═══════════════════════════════════════════════════════════════════════════════
#  UI — Streamlit dashboard
# ═══════════════════════════════════════════════════════════════════════════════

_ACTIONS = [
    "(semua)",
    "login", "logout",
    "search_pn", "search_name", "search_image",
    "download_excel", "upload_foto", "upload_data",
    "edit_opname", "permission_change",
]


def _fmt_dt_local(series: pd.Series) -> pd.Series:
    """Convert UTC timestamp ke timezone lokal (Asia/Jakarta) untuk display."""
    if series.empty:
        return series
    try:
        return series.dt.tz_convert("Asia/Jakarta").dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return series.dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return series.astype(str)


def render_user_monitoring_tab():
    """Render tab utama 'Monitoring User' — 4 sub-tab."""
    if not _HAS_ST:
        return
    if not _BACKEND_READY or not _is_configured():
        st.warning("Supabase tidak terkonfigurasi — dashboard monitoring tidak tersedia.")
        return

    status = _check_migration_status()
    if not status["ok"]:
        st.error("⚠️ **Migration belum dijalankan.** Dashboard monitoring memerlukan tabel & kolom yang belum ada di Supabase.")
        with st.expander("📋 Detail yang belum tersedia", expanded=True):
            for msg in status["missing"]:
                st.markdown(f"- {msg}")
            st.markdown(
                "**Langkah:** buka **Supabase Dashboard → SQL Editor**, paste isi "
                "file `migrations/002_user_monitoring.sql`, lalu klik **RUN**. "
                "Reload halaman ini setelah selesai."
            )
        if st.button("🔄 Cek ulang", key="mon_recheck_migration"):
            st.session_state.pop("_mon_migration_status", None)
            st.rerun()
        return

    # ── Header: judul + tombol refresh + caption waktu terakhir ────────────
    head_left, head_right = st.columns([4, 1])
    with head_left:
        st.markdown("### 📊 Monitoring User")
    with head_right:
        if st.button("🔄 Refresh", key="mon_refresh_btn",
                     help="Ambil data terbaru dari Supabase",
                     width="stretch"):
            _invalidate_dashboard_caches()
            st.session_state["_mon_last_refresh"] = datetime.now()
            st.rerun()
    last_refresh = st.session_state.get("_mon_last_refresh")
    if last_refresh is None:
        last_refresh = datetime.now()
        st.session_state["_mon_last_refresh"] = last_refresh
    st.caption(f"⏱️ Data terakhir diperbarui: {last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")

    tab_overview, tab_users, tab_activity, tab_logins = st.tabs([
        "📈 Overview", "👥 User List", "📋 Activity Log", "🔐 Login History",
    ])

    with tab_overview:
        _render_overview()
    with tab_users:
        _render_user_list()
    with tab_activity:
        _render_activity_log()
    with tab_logins:
        _render_login_history()


# ── Sub-tab: Overview ────────────────────────────────────────────────────────

def _render_overview():
    pick_date = st.date_input(
        "Tanggal",
        value=datetime.now().date(),
        key="mon_overview_date",
    )
    target_dt = datetime(pick_date.year, pick_date.month, pick_date.day, tzinfo=timezone.utc)
    stats = get_daily_stats(target_dt)

    users_df = get_users_with_status()
    online_now = int(users_df["online"].sum()) if not users_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🟢 Online sekarang", online_now)
    c2.metric("✅ Login sukses", stats["login_success"])
    c3.metric("❌ Login gagal",  stats["login_failed"])
    c4.metric("⚡ Total aksi",    stats["actions_total"])

    st.markdown("---")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("#### 🏆 Top 5 User Paling Aktif")
        if stats["top_users"]:
            df_tu = pd.DataFrame(stats["top_users"], columns=["username", "jumlah_aksi"])
            st.dataframe(df_tu, hide_index=True, width="stretch")
        else:
            st.caption("_Belum ada aktivitas pada tanggal ini._")

    with col_b:
        st.markdown("#### 📊 Distribusi Action")
        if stats["top_actions"]:
            df_ta = pd.DataFrame(stats["top_actions"], columns=["action", "jumlah"])
            st.bar_chart(df_ta.set_index("action"))
        else:
            st.caption("_Belum ada aktivitas pada tanggal ini._")


# ── Sub-tab: User List ───────────────────────────────────────────────────────

def _render_user_list():
    df = get_users_with_status()
    if df.empty:
        st.info("Belum ada user di database.")
        return

    col_f1, col_f2, col_f3 = st.columns([1, 1, 1])
    with col_f1:
        role_filter = st.selectbox(
            "Role", ["(semua)", "admin", "user"], key="mon_users_role",
        )
    with col_f2:
        only_online = st.checkbox("Hanya online", value=False, key="mon_users_online")
    with col_f3:
        only_active = st.checkbox("Hanya aktif (is_active)", value=False, key="mon_users_active")

    view = df.copy()
    if role_filter != "(semua)":
        view = view[view["role"] == role_filter]
    if only_online:
        view = view[view["online"]]
    if only_active:
        view = view[view["is_active"] == True]  # noqa: E712

    view["status"] = view["online"].map({True: "🟢 online", False: "⚫ offline"})
    view["last_login_at"]  = _fmt_dt_local(view["last_login_at"])
    view["last_active_at"] = _fmt_dt_local(view["last_active_at"])

    show_cols = ["username", "role", "is_active", "status", "last_login_at", "last_active_at"]
    show_cols = [c for c in show_cols if c in view.columns]
    st.dataframe(view[show_cols], hide_index=True, width="stretch")
    st.caption(
        f"Total: {len(view)} user · Online: {int(df['online'].sum())} · "
        f"Threshold online: {_ONLINE_WINDOW_MIN} menit"
    )

    st.caption(
        "💡 Untuk reset password / deactivate user, gunakan tab "
        "**👥 User Management**. Pakai tombol **🔄 Refresh** di atas "
        "untuk memuat ulang data."
    )


# ── Sub-tab: Activity Log ────────────────────────────────────────────────────

def _render_activity_log():
    users_df = get_users_with_status()
    user_options = ["(semua)"] + (users_df["username"].tolist() if not users_df.empty else [])

    col1, col2, col3, col4 = st.columns([1.2, 1.2, 1, 1])
    with col1:
        sel_user = st.selectbox("User", user_options, key="mon_act_user")
    with col2:
        sel_action = st.selectbox("Action", _ACTIONS, key="mon_act_action")
    with col3:
        date_from = st.date_input(
            "Dari", value=datetime.now().date() - timedelta(days=7),
            key="mon_act_from",
        )
    with col4:
        date_to = st.date_input("Sampai", value=datetime.now().date(), key="mon_act_to")

    limit = st.slider("Maks. baris", 50, 1000, 200, step=50, key="mon_act_limit")

    df = get_activity_log(
        username = None if sel_user == "(semua)" else sel_user,
        action   = None if sel_action == "(semua)" else sel_action,
        since    = datetime(date_from.year, date_from.month, date_from.day, tzinfo=timezone.utc),
        until    = datetime(date_to.year, date_to.month, date_to.day, tzinfo=timezone.utc) + timedelta(days=1),
        limit    = limit,
    )
    if df.empty:
        st.info("Tidak ada aktivitas untuk filter ini.")
        return

    df["waktu"] = _fmt_dt_local(df["created_at"])
    df["details"] = df["details"].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else (x or "")
    )
    show = df[["waktu", "username", "action", "target", "details"]]
    st.dataframe(show, hide_index=True, width="stretch")
    st.caption(f"Menampilkan {len(show)} baris (limit {limit}).")


# ── Sub-tab: Login History ───────────────────────────────────────────────────

def _render_login_history():
    users_df = get_users_with_status()
    user_options = ["(semua)"] + (users_df["username"].tolist() if not users_df.empty else [])

    col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
    with col1:
        sel_user = st.selectbox("User", user_options, key="mon_log_user")
    with col2:
        sel_status = st.selectbox(
            "Status", ["(semua)", "Sukses", "Gagal"], key="mon_log_status",
        )
    with col3:
        date_from = st.date_input(
            "Dari", value=datetime.now().date() - timedelta(days=7),
            key="mon_log_from",
        )
    with col4:
        date_to = st.date_input("Sampai", value=datetime.now().date(), key="mon_log_to")

    limit = st.slider("Maks. baris", 50, 1000, 200, step=50, key="mon_log_limit")

    success_filter: Optional[bool]
    if sel_status == "Sukses":
        success_filter = True
    elif sel_status == "Gagal":
        success_filter = False
    else:
        success_filter = None

    df = get_login_history(
        username = None if sel_user == "(semua)" else sel_user,
        success  = success_filter,
        since    = datetime(date_from.year, date_from.month, date_from.day, tzinfo=timezone.utc),
        until    = datetime(date_to.year, date_to.month, date_to.day, tzinfo=timezone.utc) + timedelta(days=1),
        limit    = limit,
    )
    if df.empty:
        st.info("Tidak ada login history untuk filter ini.")
        return

    df["waktu"] = _fmt_dt_local(df["created_at"])
    df["status"] = df["success"].map({True: "✅ sukses", False: "❌ gagal"})
    show = df[["waktu", "username", "status", "reason", "ip_address", "user_agent"]]

    try:
        styler = show.style.apply(
            lambda row: [
                "background-color: #ffe5e5" if row["status"] == "❌ gagal" else ""
            ] * len(row),
            axis=1,
        )
        st.dataframe(styler, hide_index=True, width="stretch")
    except Exception:
        st.dataframe(show, hide_index=True, width="stretch")

    fail_count = int((df["success"] == False).sum())  # noqa: E712
    st.caption(f"Menampilkan {len(show)} baris · Gagal: {fail_count}.")
