"""
GITHUB SYNC HELPER
==================
Modul untuk sinkronisasi file JSON dan binary ke GitHub via API.
Digunakan oleh app.py untuk menyimpan konfigurasi permissions
agar tidak hilang saat Streamlit Cloud restart.

Setup di .streamlit/secrets.toml:
    [github]
    token  = "ghp_xxxxxxxxxxxxxxxxxxxx"
    repo   = "morwick/maspart"
    branch = "main"
"""

import base64
import json
import time
import threading
import requests
import streamlit as st

# ── Konstanta ────────────────────────────────────────────────────────
_GH_API = "https://api.github.com"

# Lock agar tidak ada race condition saat multi-thread
_gh_lock = threading.Lock()

# Simple in-memory SHA cache agar tidak harus GET setiap kali write
# Format: {"path/to/file.json": "sha_string"}
_sha_cache: dict = {}


# ── Internal helpers ─────────────────────────────────────────────────

def _is_configured() -> bool:
    """Cek apakah secrets GitHub sudah diisi."""
    try:
        token = st.secrets.get("github", {}).get("token", "")
        repo  = st.secrets.get("github", {}).get("repo", "")
        return bool(token and repo and not token.startswith("ghp_xxx"))
    except Exception:
        return False


def _headers() -> dict:
    token = st.secrets["github"]["token"]
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }


def _repo() -> str:
    return st.secrets["github"]["repo"]


def _branch() -> str:
    return st.secrets["github"].get("branch", "main")


def _get_sha(path: str) -> str | None:
    """Ambil SHA file yang ada di GitHub (diperlukan untuk update)."""
    if path in _sha_cache:
        return _sha_cache[path]
    url = f"{_GH_API}/repos/{_repo()}/contents/{path}"
    try:
        r = requests.get(url, headers=_headers(),
                         params={"ref": _branch()}, timeout=10)
        if r.status_code == 200:
            sha = r.json().get("sha", "")
            _sha_cache[path] = sha
            return sha
    except Exception:
        pass
    return None


# ── Public API ───────────────────────────────────────────────────────

def gh_read_json(path: str) -> dict | None:
    """
    Baca file JSON dari GitHub.
    Return dict isi file, atau None jika file tidak ada / error.
    """
    if not _is_configured():
        return None
    url = f"{_GH_API}/repos/{_repo()}/contents/{path}"
    try:
        r = requests.get(url, headers=_headers(),
                         params={"ref": _branch()}, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data    = r.json()
        content = base64.b64decode(data["content"]).decode("utf-8")
        _sha_cache[path] = data.get("sha", "")
        return json.loads(content)
    except Exception as e:
        print(f"[github_sync] gh_read_json({path}) ERROR: {e}")
        return None


def gh_write_json(path: str, content: dict, commit_msg: str = "Update via app") -> bool:
    """
    Tulis / update file JSON ke GitHub.
    Return True jika sukses, False jika gagal.
    """
    if not _is_configured():
        return False
    with _gh_lock:
        url     = f"{_GH_API}/repos/{_repo()}/contents/{path}"
        encoded = base64.b64encode(
            json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
        ).decode("utf-8")

        payload: dict = {
            "message": commit_msg,
            "content": encoded,
            "branch":  _branch(),
        }

        # Cek SHA — wajib ada kalau file sudah exist
        sha = _get_sha(path)
        if sha:
            payload["sha"] = sha

        try:
            r = requests.put(url, headers=_headers(),
                             json=payload, timeout=15)
            if r.status_code in (200, 201):
                new_sha = r.json().get("content", {}).get("sha", "")
                if new_sha:
                    _sha_cache[path] = new_sha
                return True
            else:
                print(f"[github_sync] gh_write_json({path}) FAILED: {r.status_code} {r.text[:200]}")
                # Invalidate SHA cache supaya next call GET ulang
                _sha_cache.pop(path, None)
                return False
        except Exception as e:
            print(f"[github_sync] gh_write_json({path}) ERROR: {e}")
            _sha_cache.pop(path, None)
            return False


def gh_write_bytes(path: str, file_bytes: bytes, commit_msg: str = "Upload file") -> bool:
    """
    Upload file binary (Excel, gambar, dll) ke GitHub.
    Return True jika sukses.
    """
    if not _is_configured():
        return False
    with _gh_lock:
        url     = f"{_GH_API}/repos/{_repo()}/contents/{path}"
        encoded = base64.b64encode(file_bytes).decode("utf-8")

        payload: dict = {
            "message": commit_msg,
            "content": encoded,
            "branch":  _branch(),
        }

        sha = _get_sha(path)
        if sha:
            payload["sha"] = sha

        try:
            r = requests.put(url, headers=_headers(),
                             json=payload, timeout=30)
            if r.status_code in (200, 201):
                new_sha = r.json().get("content", {}).get("sha", "")
                if new_sha:
                    _sha_cache[path] = new_sha
                return True
            else:
                print(f"[github_sync] gh_write_bytes({path}) FAILED: {r.status_code}")
                _sha_cache.pop(path, None)
                return False
        except Exception as e:
            print(f"[github_sync] gh_write_bytes({path}) ERROR: {e}")
            _sha_cache.pop(path, None)
            return False


def gh_read_bytes(path: str) -> bytes | None:
    """
    Download file binary dari GitHub.
    Return bytes isi file, atau None jika tidak ada.
    """
    if not _is_configured():
        return None
    url = f"{_GH_API}/repos/{_repo()}/contents/{path}"
    try:
        r = requests.get(url, headers=_headers(),
                         params={"ref": _branch()}, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        _sha_cache[path] = data.get("sha", "")
        return base64.b64decode(data["content"])
    except Exception as e:
        print(f"[github_sync] gh_read_bytes({path}) ERROR: {e}")
        return None


def gh_is_configured() -> bool:
    """Expose ke modul lain untuk cek apakah GitHub tersambung."""
    return _is_configured()
