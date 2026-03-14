"""
SIMS Image Fetcher  (v7 — Direct API Login dengan RSA + CAPTCHA)
================================================================
Install:
  pip install requests pycryptodome
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
PHOTO_API_URL = f"{SIMS_BASE_URL}/intlapi/intl.service.basic/partPhoto/getPhotoUrlByPartCode"

BASE_HEADERS = {
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
#  RSA ENCRYPT
# ══════════════════════════════════════════════
def _rsa_encrypt(public_key_b64: str, plaintext: str) -> str:
    """Enkripsi password dengan RSA public key (PKCS#1 v1.5)."""
    import base64
    try:
        from Crypto.PublicKey import RSA
        from Crypto.Cipher import PKCS1_v1_5
    except ImportError:
        raise RuntimeError(
            "pycryptodome belum terinstall.\n"
            "Jalankan: pip install pycryptodome"
        )
    der = base64.b64decode(public_key_b64)
    key = RSA.import_key(der)
    cipher = PKCS1_v1_5.new(key)
    encrypted = cipher.encrypt(plaintext.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


# ══════════════════════════════════════════════
#  LOGIN VIA DIRECT API
# ══════════════════════════════════════════════
def _login_direct() -> str:
    """
    Login ke SIMS via HTTP langsung:
    1. Ambil RSA public key
    2. Ambil captchaId
    3. POST login dengan password ter-enkripsi + captchaId
    """
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    # ── Step 1: Ambil RSA public key ──
    print("[sims_fetcher] Ambil RSA public key...")
    resp = session.get(
        f"{SIMS_BASE_URL}/intlapi/intl.auth/common/login/rsa-public-key",
        timeout=15
    )
    resp.raise_for_status()
    public_key = resp.json().get("publicKey", "")
    if not public_key:
        raise RuntimeError("Gagal ambil RSA public key")
    print(f"[sims_fetcher] RSA public key OK ({len(public_key)} chars)")

    # ── Step 2: Enkripsi password ──
    print("[sims_fetcher] Enkripsi password...")
    encrypted_password = _rsa_encrypt(public_key, SIMS_PASSWORD)
    print(f"[sims_fetcher] Password terenkripsi OK ({len(encrypted_password)} chars)")

    # ── Step 3: Ambil captchaId ──
    print("[sims_fetcher] Ambil captcha config...")
    resp = session.get(
        f"{SIMS_BASE_URL}/intlapi/intl.auth/common/login-captcha-config",
        timeout=15
    )
    resp.raise_for_status()
    captcha_data = resp.json()
    captcha_enabled = captcha_data.get("captchaEnabled", False)
    captcha_id = captcha_data.get("captchaId", "")
    print(f"[sims_fetcher] captchaEnabled={captcha_enabled}, captchaId={captcha_id}")

    # ── Step 4: Fetch captcha image (diperlukan agar captchaId valid di server) ──
    if captcha_enabled and captcha_id:
        session.get(
            f"{SIMS_BASE_URL}/intlapi/intl.auth/common/getLoginCaptchaCode/{captcha_id}",
            timeout=15
        )
        print(f"[sims_fetcher] Captcha image fetched")

    # ── Step 5: POST login dengan multipart/form-data ──
    print("[sims_fetcher] POST login...")
    form_data = {
        "username": (None, SIMS_USERNAME),
        "password": (None, encrypted_password),
    }
    if captcha_enabled and captcha_id:
        form_data["captchaId"] = (None, captcha_id)

    resp = session.post(
        f"{SIMS_BASE_URL}/intlapi/intl.auth/login",
        files=form_data,
        timeout=30,
    )
    print(f"[sims_fetcher] Login response: {resp.status_code} | {resp.text[:300]}")
    resp.raise_for_status()

    data = resp.json()
    token = data.get("token", "")
    if not token:
        raise RuntimeError(f"Token tidak ada di response: {resp.text[:200]}")

    if not token.startswith("Bearer "):
        token = f"Bearer {token}"

    print(f"[sims_fetcher] ✅ Login berhasil via direct API!")
    print(f"[sims_fetcher] Token: {token[:60]}...")
    return token


# ══════════════════════════════════════════════
#  GET TOKEN
# ══════════════════════════════════════════════
def _get_token() -> str:
    global _token, _token_expiry
    with _token_lock:
        if _token is None or time.time() >= _token_expiry:
            _token        = _login_direct()
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
