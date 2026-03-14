"""
SIMS Login Helper — dijalankan sebagai subprocess terpisah oleh sims_fetcher.py
"""

import sys
import os
import subprocess
import glob

SIMS_BASE_URL = "http://simscloud.cnhtcerp.com:8082"
SIMS_USERNAME = "IDZ0050005"
SIMS_PASSWORD = "Jiahong@010366"
LOGIN_PAGE    = f"{SIMS_BASE_URL}/#/login"


def _ensure_chromium():
    cache_dir = os.path.expanduser("~/.cache/ms-playwright")
    patterns = [
        os.path.join(cache_dir, "**/chrome-headless-shell"),
        os.path.join(cache_dir, "**/chrome"),
        os.path.join(cache_dir, "**/chromium"),
    ]
    found = any(glob.glob(p, recursive=True) for p in patterns)
    if not found:
        print("INFO:Install chromium...", flush=True)
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=300
        )
    else:
        print("INFO:Chromium cache ditemukan", flush=True)


def main():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR:Playwright belum terinstall", flush=True)
        sys.exit(1)

    _ensure_chromium()

    token_holder = {"token": None}

    # Cek apakah ada chromium di sistem (Debian Trixie Streamlit Cloud)
    import shutil
    system_chromium = (
        shutil.which("chromium") or
        shutil.which("chromium-browser") or
        shutil.which("google-chrome-stable") or
        shutil.which("google-chrome")
    )
    print(f"INFO:System chromium: {system_chromium}", flush=True)

    launch_kwargs = {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--no-first-run",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
        ],
    }

    # Jika ada chromium sistem, pakai itu — lebih stabil di container
    if system_chromium and os.path.exists(system_chromium):
        launch_kwargs["executable_path"] = system_chromium
        print(f"INFO:Pakai system chromium: {system_chromium}", flush=True)
    else:
        print("INFO:Pakai playwright chromium", flush=True)

    try:
        with sync_playwright() as p:
            print("INFO:Launch browser...", flush=True)
            browser = p.chromium.launch(**launch_kwargs)
            print("INFO:Browser OK", flush=True)

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            def on_response(response):
                if "intl.auth/login" in response.url and response.status == 200:
                    try:
                        data = response.json()
                        t = (
                            data.get("token") or
                            (data.get("data") or {}).get("token") or
                            data.get("accessToken") or ""
                        )
                        if t:
                            if not t.startswith("Bearer "):
                                t = f"Bearer {t}"
                            token_holder["token"] = t
                    except Exception:
                        pass

            page.on("response", on_response)
            print(f"INFO:Goto login page...", flush=True)
            page.goto(LOGIN_PAGE, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)

            username_input = (
                page.query_selector("input[type='text']") or
                page.query_selector("input[autocomplete='username']") or
                page.query_selector("input:not([type='password'])")
            )
            password_input = page.query_selector("input[type='password']")

            if not username_input or not password_input:
                all_inputs = page.query_selector_all("input")
                if len(all_inputs) >= 2:
                    username_input = all_inputs[0]
                    password_input = all_inputs[1]

            if not username_input or not password_input:
                print("ERROR:Form login tidak ditemukan", flush=True)
                browser.close()
                sys.exit(1)

            username_input.click()
            username_input.fill(SIMS_USERNAME)
            page.wait_for_timeout(300)
            password_input.click()
            password_input.fill(SIMS_PASSWORD)
            page.wait_for_timeout(300)

            login_btn = (
                page.query_selector("button[type='submit']") or
                page.query_selector("button:has-text('Login')") or
                page.query_selector("button:has-text('login')") or
                page.query_selector("button:has-text('登录')") or
                page.query_selector(".el-button--primary")
            )

            if login_btn:
                login_btn.click()
            else:
                password_input.press("Enter")

            try:
                page.wait_for_url(lambda url: "login" not in url, timeout=15000)
            except Exception:
                pass

            for _ in range(10):
                if token_holder["token"]:
                    break
                page.wait_for_timeout(500)

            if not token_holder["token"]:
                try:
                    ls_data = page.evaluate("""() => {
                        const result = {};
                        for (let i = 0; i < localStorage.length; i++) {
                            const k = localStorage.key(i);
                            result[k] = localStorage.getItem(k);
                        }
                        return result;
                    }""")
                    for k, v in ls_data.items():
                        if v and "eyJ" in str(v):
                            t = str(v).strip()
                            if not t.startswith("Bearer "):
                                t = f"Bearer {t}"
                            token_holder["token"] = t
                            break
                except Exception:
                    pass

            browser.close()

        if token_holder["token"]:
            print(f"TOKEN:{token_holder['token']}", flush=True)
            sys.exit(0)
        else:
            print("ERROR:Token tidak tertangkap", flush=True)
            sys.exit(1)

    except Exception as e:
        print(f"ERROR:{e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
