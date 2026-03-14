"""
SIMS Image Fetcher  (v4 — Playwright Login)
============================================
Install:
  pip install playwright requests
  playwright install chromium
"""

import json
import time
import threading
import requests
from pathlib import Path

# ══════════════════════════════════════════════
#  KONFIGURASI
# ══════════════════════════════════════════════
SIMS_BASE_URL = "http://simscloud.cnhtcerp.com:8082"
SIMS_USERNAME = "IDZ0050005"
SIMS_PASSWORD = "Jiahong@010366"

IMAGES_JSON   = Path("images") / "image_links.json"
LOGIN_PAGE    = f"{SIMS_BASE_URL}/#/login"
PHOTO_API_URL = f"{SIMS_BASE_URL}/intlapi/intl.service.basic/partPhoto/getPhotoUrlByPartCode"

BASE_HEADERS  = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "id,en-US;q=0.9,en;q=0.8",
    "Origin":          SIMS_BASE_URL,
    "Referer":         f"{SIMS_BASE_URL}/",
    "language":        "en",
}

# ══════════════════════════════════════════════
#  SINGLETON TOKEN
# ══════════════════════════════════════════════
_token        = None
_token_lock   = threading.Lock()
_token_expiry = 0
SESSION_TTL   = 55 * 60


# ══════════════════════════════════════════════
#  LOGIN VIA PLAYWRIGHT (subprocess terpisah)
#  Menghindari konflik event loop dengan Streamlit
# ══════════════════════════════════════════════
def _login_playwright() -> str:
    """
    Memanggil sims_login_helper.py sebagai subprocess Python terpisah.
    Cara ini menghindari konflik asyncio event loop antara Playwright & Streamlit.
    sims_login_helper.py harus berada di folder yang sama dengan sims_fetcher.py.
    """
    import subprocess
    import sys

    # Cari path helper — di folder yang sama dengan file ini
    helper_path = Path(__file__).parent / "sims_login_helper.py"
    if not helper_path.exists():
        raise RuntimeError(
            f"sims_login_helper.py tidak ditemukan di:\n{helper_path}\n"
            "Pastikan file sims_login_helper.py ada di folder yang sama dengan sims_fetcher.py"
        )

    print("[sims_fetcher] Membuka browser untuk login SIMS (subprocess)...")

    try:
        result = subprocess.run(
            [sys.executable, str(helper_path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Login SIMS timeout (120 detik)")
    except Exception as e:
        raise RuntimeError(f"Gagal menjalankan login helper: {e}")

    # Parse output baris per baris
    token = None
    error = None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("TOKEN:"):
            token = line[len("TOKEN:"):]
        elif line.startswith("ERROR:"):
            error = line[len("ERROR:"):]

    if token:
        print(f"[sims_fetcher] Token tertangkap: {token[:50]}...")
        print("[sims_fetcher] ✅ Login berhasil via Playwright!")
        return token

    # Tampilkan stderr untuk debugging jika gagal
    if result.stderr:
        print(f"[sims_fetcher] stderr:\n{result.stderr[:500]}")

    raise RuntimeError(
        f"Login gagal: {error or 'token tidak tertangkap'}\n"
        "Periksa username/password di sims_login_helper.py"
    )



# ══════════════════════════════════════════════
#  GET TOKEN
# ══════════════════════════════════════════════
def _get_token() -> str:
    global _token, _token_expiry
    with _token_lock:
        if _token is None or time.time() >= _token_expiry:
            _token        = _login_playwright()
            _token_expiry = time.time() + SESSION_TTL
        return _token


def _reset_token():
    global _token, _token_expiry
    with _token_lock:
        _token        = None
        _token_expiry = 0


# ══════════════════════════════════════════════
#  LOAD & SAVE image_links.json
# ══════════════════════════════════════════════
_json_lock = threading.Lock()

def _load_json() -> dict:
    IMAGES_JSON.parent.mkdir(parents=True, exist_ok=True)
    if IMAGES_JSON.exists():
        try:
            with open(IMAGES_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_json(data: dict):
    IMAGES_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(IMAGES_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════
#  FETCH GAMBAR
# ══════════════════════════════════════════════
def fetch_sims_images(part_number: str, force_refresh: bool = False) -> list:
    pn_key = str(part_number).strip().upper()
    if not pn_key:
        return []

    with _json_lock:
        cache = _load_json()
    if not force_refresh and pn_key in cache:
        print(f"[sims_fetcher] Cache hit: {pn_key} ({len(cache[pn_key])} gambar)")
        return cache[pn_key]

    urls = []
    try:
        token   = _get_token()
        headers = {**BASE_HEADERS, "Authorization": token}

        resp = requests.get(
            PHOTO_API_URL,
            params={"partCode": part_number.strip()},
            headers=headers,
            timeout=15,
        )

        if resp.status_code in (401, 403):
            print("[sims_fetcher] Token expired, login ulang...")
            _reset_token()
            token   = _get_token()
            headers = {**BASE_HEADERS, "Authorization": token}
            resp    = requests.get(
                PHOTO_API_URL,
                params={"partCode": part_number.strip()},
                headers=headers,
                timeout=15,
            )

        resp.raise_for_status()
        raw = resp.json()

        url_list = raw if isinstance(raw, list) else (
            raw.get("data") or raw.get("result") or
            raw.get("photos") or raw.get("urls") or []
        )

        for u in url_list:
            u = str(u).strip()
            if u:
                urls.append(u if u.startswith("http") else f"{SIMS_BASE_URL}{u}")

        print(f"[sims_fetcher] {pn_key}: {len(urls)} gambar ditemukan")

    except RuntimeError:
        raise
    except Exception as e:
        print(f"[sims_fetcher] Error fetch '{pn_key}': {e}")
        return []

    with _json_lock:
        cache = _load_json()
        cache[pn_key] = urls
        _save_json(cache)

    return urls


# ══════════════════════════════════════════════
#  WRAPPER untuk app.py
# ══════════════════════════════════════════════
def get_sims_images(part_number: str, force_refresh: bool = False) -> tuple:
    try:
        return fetch_sims_images(part_number, force_refresh=force_refresh), None
    except RuntimeError as e:
        return [], str(e)
    except Exception as e:
        return [], f"Error: {e}"


# ══════════════════════════════════════════════
#  CLI TEST
# ══════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    pn = sys.argv[1] if len(sys.argv) > 1 else "811W25503-0244"
    print(f"{'='*55}")
    print(f"  Test SIMS fetch: {pn}")
    print(f"{'='*55}")
    try:
        result = fetch_sims_images(pn, force_refresh=True)
        print(f"\n✅ Ditemukan {len(result)} gambar:")
        for i, u in enumerate(result, 1):
            print(f"  {i}. {u}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")