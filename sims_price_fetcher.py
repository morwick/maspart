"""
SIMS Price Fetcher  (v4 — getOrderPartPriceInfoByCode)
=======================================================
Mengambil partPrice dari endpoint yang sama persis dengan UI SIMS.
Login sama seperti sims_fetcher.py.

Install:
  pip install requests pycryptodome
"""

import json
import time
import threading
from pathlib import Path

import requests

# ══════════════════════════════════════════════
#  KONFIGURASI
# ══════════════════════════════════════════════
SIMS_BASE_URL    = "http://simscloud.cnhtcerp.com:8082"
SIMS_USERNAME    = "IDZ0050005"
SIMS_PASSWORD    = "Jiahong@010366"

# roId dari repair order mana saja yang ada di sistem
# (dipakai sebagai "konteks" untuk kalkulasi harga, tidak diubah)
# Ganti dengan roId dari akun kamu jika perlu
RO_ID   = "2049391821456355330"
VMODEL  = "10031050"

PRICE_CACHE_FILE = Path("images") / "part_price_cache.json"

PRICE_API_URL = (
    f"{SIMS_BASE_URL}/intlapi/intl.service.repair/orderCommon"
    f"/getOrderPartPriceInfoByCode"
)

BASE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "id,en-US;q=0.9,en;q=0.8",
    "Origin":          SIMS_BASE_URL,
    "Referer":         f"{SIMS_BASE_URL}/",
    "language":        "en",
}

# ══════════════════════════════════════════════
#  SINGLETON TOKEN  (sama seperti sims_fetcher.py)
# ══════════════════════════════════════════════
_token        = None
_token_lock   = threading.Lock()
_token_expiry = 0
SESSION_TTL   = 55 * 60


def _rsa_encrypt(public_key_b64: str, plaintext: str) -> str:
    import base64
    try:
        from Crypto.PublicKey import RSA
        from Crypto.Cipher import PKCS1_v1_5
    except ImportError:
        raise RuntimeError("Jalankan: pip install pycryptodome")
    der     = base64.b64decode(public_key_b64)
    key     = RSA.import_key(der)
    cipher  = PKCS1_v1_5.new(key)
    encrypted = cipher.encrypt(plaintext.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def _login_direct() -> str:
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    print("[price_fetcher] Ambil RSA public key...")
    resp = session.get(
        f"{SIMS_BASE_URL}/intlapi/intl.auth/common/login/rsa-public-key",
        timeout=15
    )
    resp.raise_for_status()
    public_key = resp.json().get("publicKey", "")
    if not public_key:
        raise RuntimeError("Gagal ambil RSA public key")
    print(f"[price_fetcher] RSA public key OK ({len(public_key)} chars)")

    encrypted_password = _rsa_encrypt(public_key, SIMS_PASSWORD)
    print(f"[price_fetcher] Password terenkripsi OK")

    print("[price_fetcher] Ambil captcha config...")
    resp = session.get(
        f"{SIMS_BASE_URL}/intlapi/intl.auth/common/login-captcha-config",
        timeout=15
    )
    resp.raise_for_status()
    captcha_data    = resp.json()
    captcha_enabled = captcha_data.get("captchaEnabled", False)
    captcha_id      = captcha_data.get("captchaId", "")
    print(f"[price_fetcher] captchaEnabled={captcha_enabled}, captchaId={captcha_id}")

    if captcha_enabled and captcha_id:
        session.get(
            f"{SIMS_BASE_URL}/intlapi/intl.auth/common/getLoginCaptchaCode/{captcha_id}",
            timeout=15
        )
        print("[price_fetcher] Captcha image fetched")

    print("[price_fetcher] POST login...")
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
    print(f"[price_fetcher] Login response: {resp.status_code} | {resp.text[:300]}")
    resp.raise_for_status()

    data  = resp.json()
    token = data.get("token", "")
    if not token:
        raise RuntimeError(f"Token tidak ada di response: {resp.text[:200]}")
    if not token.startswith("Bearer "):
        token = f"Bearer {token}"

    print(f"[price_fetcher] ✅ Login berhasil!")
    print(f"[price_fetcher] Token: {token[:60]}...")
    return token


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
#  CACHE HELPERS
# ══════════════════════════════════════════════
_cache_lock = threading.Lock()


def _load_price_cache() -> dict:
    PRICE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if PRICE_CACHE_FILE.exists():
        try:
            with open(PRICE_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_price_cache(data: dict):
    PRICE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PRICE_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════
#  AMBIL HARGA
# ══════════════════════════════════════════════
def _fetch_price_via_api(part_number: str) -> float | None:
    """
    POST ke getOrderPartPriceInfoByCode dengan roId + vmodel yang sudah diketahui.
    Response: { "WG1641230025": { "partCode": "...", "partPrice": 24.68, ... } }
    """
    pn = part_number.strip()

    token   = _get_token()
    headers = {**BASE_HEADERS, "Authorization": token}

    body = [{"partCode": pn, "partPriceOrigin": 0}]
    params = {"roId": RO_ID, "vmodel": VMODEL}

    resp = requests.post(
        PRICE_API_URL,
        params=params,
        json=body,
        headers=headers,
        timeout=15,
    )

    # Retry jika token expired
    if resp.status_code in (401, 403):
        print("[price_fetcher] Token expired, login ulang...")
        _reset_token()
        token   = _get_token()
        headers = {**BASE_HEADERS, "Authorization": token}
        resp    = requests.post(
            PRICE_API_URL,
            params=params,
            json=body,
            headers=headers,
            timeout=15,
        )

    print(f"[price_fetcher] Response {resp.status_code}: {resp.text[:300]}")
    resp.raise_for_status()

    data = resp.json()
    # Response: { "PARTCODE": { "partCode": "...", "partPrice": 24.68, ... } }
    # Key bisa uppercase dari partCode
    pn_upper = pn.upper()
    entry = data.get(pn_upper) or data.get(pn) or next(iter(data.values()), None)

    if entry and isinstance(entry, dict):
        price = entry.get("partPrice")
        if price is not None:
            return float(price)

    print(f"[price_fetcher] partPrice tidak ditemukan di response")
    return None


# ══════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════
def fetch_sims_part_price(part_number: str, force_refresh: bool = False) -> float | None:
    """
    Ambil partPrice dari SIMS untuk part_number.
    Selalu fetch langsung dari server SIMS — tidak pernah pakai cache,
    karena harga di SIMS bisa berubah kapan saja.
    Cache hanya ditulis sebagai log/history, tidak dibaca.
    Return float harga atau None jika tidak ditemukan.
    """
    pn_key = str(part_number).strip().upper()
    if not pn_key:
        return None

    # Tidak ada pengecekan cache — selalu fetch dari SIMS
    try:
        price = _fetch_price_via_api(part_number)
        print(f"[price_fetcher] Live fetch: {pn_key} → {price}")

        # Tulis ke cache sebagai log history saja (tidak dibaca kembali)
        with _cache_lock:
            cache = _load_price_cache()
            cache[pn_key] = {
                "price":      price,
                "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            _save_price_cache(cache)

        return price

    except Exception as e:
        print(f"[price_fetcher] ❌ Error: {e}")
        return None


def get_sims_part_price(part_number: str, force_refresh: bool = False) -> tuple:
    """Wrapper untuk app.py — return (price_float_or_None, error_str_or_None).
    Parameter force_refresh dipertahankan untuk kompatibilitas, tapi tidak
    berpengaruh karena fetch selalu dilakukan langsung dari SIMS.
    """
    try:
        price = fetch_sims_part_price(part_number)
        if price is None:
            return None, "Harga tidak ditemukan"
        return price, None
    except Exception as e:
        return None, str(e)


def get_cached_price(part_number: str) -> float | None:
    """Hanya baca dari cache, tidak trigger request ke SIMS."""
    pn_key = str(part_number).strip().upper()
    cache  = _load_price_cache()
    entry  = cache.get(pn_key)
    if entry is None:
        return None
    return entry.get("price") if isinstance(entry, dict) else entry


def get_all_cached_prices() -> dict:
    """Return semua cache harga: {part_number: price}"""
    cache  = _load_price_cache()
    result = {}
    for pn, entry in cache.items():
        result[pn] = entry.get("price") if isinstance(entry, dict) else entry
    return result


# ══════════════════════════════════════════════
#  CLI TEST
# ══════════════════════════════════════════════
if __name__ == "__main__":
    import sys
    pn    = sys.argv[1] if len(sys.argv) > 1 else "WG1641230025"
    force = "--force" in sys.argv

    print(f"{'='*55}")
    print(f"  Test Price Fetch : {pn}")
    print(f"  roId             : {RO_ID}")
    print(f"  vmodel           : {VMODEL}")
    print(f"  Force refresh    : {force}")
    print(f"{'='*55}")

    price, err = get_sims_part_price(pn, force_refresh=force)
    if err:
        print(f"\n❌ ERROR: {err}")
    else:
        print(f"\n✅ Part Price: {price}")