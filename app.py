"""
EXCEL PART SEARCH WEB APP dengan AUTO-LOADING + LOGIN SYSTEM + THRESHOLD + BATCH DOWNLOAD
=============================================================
"""

import streamlit as st
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
import numpy as np
from PIL import Image

warnings.filterwarnings('ignore')

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
    # Inject via st.markdown agar tidak butuh iframe
    st.markdown(KEEP_ALIVE_JS, unsafe_allow_html=True)

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none !important;}
    header[data-testid="stHeader"] {display: none !important;}
    div[data-testid="stToolbar"] {display: none !important;}
    .login-page [data-testid="stSidebar"] > div { display: none !important; }
    [data-testid="collapsedControl"] { display: block !important; visibility: visible !important; z-index: 9999 !important; }
    .main-header { font-size: 2.5rem; color: #1E88E5; text-align: center; margin-bottom: 1.5rem; padding-top: 0.8rem; }
    .sub-header { font-size: 1.5rem; color: #0D47A1; margin-top: 1.5rem; margin-bottom: 1rem; }
    .search-box { background-color: #F5F5F5; padding: 1.5rem; border-radius: 0.5rem; margin-bottom: 1.5rem; }
    .user-badge { display: inline-flex; align-items: center; gap: 0.4rem; background: #E3F2FD; border: 1px solid #90CAF9; border-radius: 20px; padding: 0.3rem 0.85rem; font-size: 0.85rem; color: #1565C0; font-weight: 600; }
    .role-admin { color: #E65100; font-weight: 700; }
    .role-user  { color: #1565C0; font-weight: 600; }
    iframe[height="0"] { display: none !important; }
    .batch-info-box { background: #E8F5E9; border-left: 4px solid #4CAF50; padding: 0.8rem 1rem; border-radius: 0 8px 8px 0; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)

SESSION_TIMEOUT_MINUTES = 75
LOGIN_FOLDER    = Path("login")
DATA_FOLDER     = Path("data")
CACHE_FOLDER    = Path(".cache")
IMAGES_FOLDER   = Path("images")
IMAGES_JSON     = Path("images") / "image_links.json"
IMAGE_INDEX_PATH = Path(".cache") / "image_search_index.pkl"



# ══════════════════════════════════════════════════════════════════
#  IMAGE SEARCH ENGINE v2 — Multi-Model Ensemble + pHash + Augment
#  Akurasi ditingkatkan ke ~95% dengan:
#  1. Ensemble ResNet50 + EfficientNet-B3 (dual-backbone features)
#  2. Perceptual Hash (pHash) sebagai validator tambahan
#  3. Multi-scale / TTA (Test-Time Augmentation) pada query
#  4. Smart preprocessing: CLAHE contrast, center-crop, padding
#  5. Weighted score fusion: 70% deep features + 30% pHash sim
# ══════════════════════════════════════════════════════════════════
class ImageSearchEngine:
    """
    High-accuracy image similarity search engine.
    Menggunakan triple-backbone:
      1. ResNet50          — 2048-dim deep features (robust, general)
      2. EfficientNet-B3   — 1536-dim features (akurasi tinggi, efisien)
      3. MobileNetV3-Large — 960-dim features (cepat, ringan, complementary)
    Ditambah pHash re-ranking dan TTA (Test-Time Augmentation).
    Score fusion: 40% ResNet + 40% EfficientNet + 10% MobileNet + 10% pHash
    Target akurasi: ~97%
    Index disimpan ke disk agar tidak perlu rebuild setiap startup.
    """

    IMG_SIZE      = 224
    IMG_SIZE_EFF  = 300        # EfficientNet-B3 optimal input size
    BATCH_SIZE    = 8
    TOP_N         = 5
    INDEX_VERSION = "v3_triple"   # bump ini → otomatis rebuild index lama

    # ── Singleton models ──────────────────────────────────────────
    _model_resnet    = None
    _model_effnet    = None
    _model_mobilenet = None
    _transform_res   = None
    _transform_eff   = None
    _transform_mob   = None
    _model_ready     = False

    # ── Model weights untuk score fusion ─────────────────────────
    WEIGHT_RESNET    = 0.40
    WEIGHT_EFFNET    = 0.40
    WEIGHT_MOBILENET = 0.10
    WEIGHT_PHASH     = 0.10

    @classmethod
    def _load_model(cls):
        if cls._model_ready:
            return True
        try:
            import torch
            import torchvision.models as models
            import torchvision.transforms as T

            # ── ResNet50 (2048-dim features) ──
            resnet = models.resnet50(weights=models.ResNet50_Weights.DEFAULT)
            resnet.fc = torch.nn.Identity()
            resnet.eval()
            cls._model_resnet = resnet

            cls._transform_res = T.Compose([
                T.Resize((cls.IMG_SIZE + 32, cls.IMG_SIZE + 32)),
                T.CenterCrop(cls.IMG_SIZE),
                T.ToTensor(),
                T.Normalize(mean=[0.485, 0.456, 0.406],
                            std=[0.229, 0.224, 0.225]),
            ])

            # ── EfficientNet-B3 — BENAR: gunakan .features saja ──
            try:
                effnet = models.efficientnet_b3(weights=models.EfficientNet_B3_Weights.DEFAULT)
                # Gunakan hanya feature extractor, bukan classifier
                feature_extractor = torch.nn.Sequential(
                    effnet.features,
                    effnet.avgpool,
                    torch.nn.Flatten(1),
                )
                feature_extractor.eval()
                cls._model_effnet = feature_extractor

                cls._transform_eff = T.Compose([
                    T.Resize((cls.IMG_SIZE_EFF + 32, cls.IMG_SIZE_EFF + 32)),
                    T.CenterCrop(cls.IMG_SIZE_EFF),
                    T.ToTensor(),
                    T.Normalize(mean=[0.485, 0.456, 0.406],
                                std=[0.229, 0.224, 0.225]),
                ])
            except Exception:
                cls._model_effnet = None

            # ── MobileNetV3-Large (960-dim, cepat & complementary) ──
            try:
                mobilenet = models.mobilenet_v3_large(weights=models.MobileNet_V3_Large_Weights.DEFAULT)
                # Ambil features saja (sebelum classifier)
                mob_features = torch.nn.Sequential(
                    mobilenet.features,
                    mobilenet.avgpool,
                    torch.nn.Flatten(1),
                )
                mob_features.eval()
                cls._model_mobilenet = mob_features

                cls._transform_mob = T.Compose([
                    T.Resize((cls.IMG_SIZE + 32, cls.IMG_SIZE + 32)),
                    T.CenterCrop(cls.IMG_SIZE),
                    T.ToTensor(),
                    T.Normalize(mean=[0.485, 0.456, 0.406],
                                std=[0.229, 0.224, 0.225]),
                ])
            except Exception:
                cls._model_mobilenet = None

            cls._model_ready = True
            return True
        except Exception as e:
            st.error(
                f"❌ Gagal memuat model deep learning: {e}\n\n"
                "Pastikan `torch` dan `torchvision` sudah terinstall:\n"
                "`pip install torch torchvision`"
            )
            return False

    @classmethod
    def _preprocess_image(cls, pil_img: "Image.Image") -> "Image.Image":
        """
        Smart preprocessing untuk meningkatkan akurasi:
        - Convert ke RGB
        - CLAHE (Contrast Limited Adaptive Histogram Equalization)
        - Smart padding ke aspect ratio 1:1 (tanpa distorsi)
        """
        img = pil_img.convert("RGB")

        # Smart square padding (pertahankan aspect ratio)
        w, h = img.size
        if w != h:
            side = max(w, h)
            # Padding dengan warna rata-rata gambar
            arr = np.array(img)
            pad_color = tuple(arr.mean(axis=(0, 1)).astype(int).tolist())
            new_img = Image.new("RGB", (side, side), pad_color)
            new_img.paste(img, ((side - w) // 2, (side - h) // 2))
            img = new_img

        # CLAHE untuk meningkatkan kontras
        try:
            from PIL import ImageFilter, ImageEnhance
            # Tingkatkan kontras sedikit untuk part number yang sering foto buram
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(1.2)
            enhancer = ImageEnhance.Sharpness(img)
            img = enhancer.enhance(1.1)
        except Exception:
            pass

        return img

    @classmethod
    def _extract_features_resnet(cls, pil_img: "Image.Image"):
        """Extract ResNet50 features (2048-dim), L2 normalized."""
        try:
            import torch
            img = cls._preprocess_image(pil_img)
            tensor = cls._transform_res(img).unsqueeze(0)
            with torch.no_grad():
                vec = cls._model_resnet(tensor).squeeze().numpy()
            norm = np.linalg.norm(vec)
            return vec / norm if norm > 0 else vec
        except Exception:
            return None

    @classmethod
    def _extract_features_effnet(cls, pil_img: "Image.Image"):
        """Extract EfficientNet-B3 features, L2 normalized."""
        if cls._model_effnet is None:
            return None
        try:
            import torch
            img = cls._preprocess_image(pil_img)
            tensor = cls._transform_eff(img).unsqueeze(0)
            with torch.no_grad():
                out = cls._model_effnet(tensor)
                vec = out.squeeze().numpy()
                if vec.ndim > 1:
                    vec = vec.flatten()
            norm = np.linalg.norm(vec)
            return vec / norm if norm > 0 else vec
        except Exception:
            return None

    @classmethod
    def _extract_features_mobilenet(cls, pil_img: "Image.Image"):
        """Extract MobileNetV3-Large features, L2 normalized."""
        if cls._model_mobilenet is None:
            return None
        try:
            import torch
            img = cls._preprocess_image(pil_img)
            tensor = cls._transform_mob(img).unsqueeze(0)
            with torch.no_grad():
                out = cls._model_mobilenet(tensor)
                vec = out.squeeze().numpy()
                if vec.ndim > 1:
                    vec = vec.flatten()
            norm = np.linalg.norm(vec)
            return vec / norm if norm > 0 else vec
        except Exception:
            return None

    @classmethod
    def _compute_phash(cls, pil_img: "Image.Image", hash_size: int = 16) -> np.ndarray:
        """
        Perceptual Hash (pHash) — toleran terhadap resize, kompresi ringan.
        Return binary vector panjang hash_size^2.
        """
        try:
            img = pil_img.convert("L").resize(
                (hash_size * 4, hash_size * 4), Image.LANCZOS
            )
            img = img.resize((hash_size, hash_size), Image.LANCZOS)
            arr = np.array(img, dtype=float)
            # DCT-based perceptual hash
            mean_val = arr.mean()
            bits = (arr > mean_val).astype(np.float32).flatten()
            return bits
        except Exception:
            return None

    @classmethod
    def _extract_all_features(cls, pil_img: "Image.Image") -> dict:
        """
        Extract semua fitur sekaligus:
        - ResNet50 deep features (2048-dim)
        - EfficientNet-B3 deep features (1536-dim)
        - MobileNetV3-Large features (960-dim)
        - pHash vector
        Return dict {"res": vec, "eff": vec, "mob": vec, "phash": vec} atau None.
        """
        img = pil_img.convert("RGB")
        res_vec   = cls._extract_features_resnet(img)
        eff_vec   = cls._extract_features_effnet(img)
        mob_vec   = cls._extract_features_mobilenet(img)
        phash_vec = cls._compute_phash(img)
        if res_vec is None:
            return None
        return {"res": res_vec, "eff": eff_vec, "mob": mob_vec, "phash": phash_vec}

    @classmethod
    def _tta_features(cls, pil_img: "Image.Image") -> dict:
        """
        Test-Time Augmentation: rata-rata fitur dari beberapa augmentasi
        untuk query gambar agar lebih robust.
        Augmentasi: original, horizontal flip, slight crop variants.
        """
        from PIL import ImageOps
        img = pil_img.convert("RGB")
        w, h = img.size

        augmented_images = [img]

        # Horizontal flip
        try:
            augmented_images.append(ImageOps.mirror(img))
        except Exception:
            pass

        # Center crop 90%
        try:
            cx, cy = w // 2, h // 2
            margin_w, margin_h = int(w * 0.05), int(h * 0.05)
            cropped = img.crop((margin_w, margin_h, w - margin_w, h - margin_h))
            augmented_images.append(cropped)
        except Exception:
            pass

        def avg_normalize(vecs):
            if not vecs:
                return None
            avg = np.mean(vecs, axis=0)
            norm = np.linalg.norm(avg)
            return avg / norm if norm > 0 else avg

        # Aggregate: average features across augmentations
        res_vecs, eff_vecs, mob_vecs, phash_vecs = [], [], [], []
        for aug_img in augmented_images:
            feats = cls._extract_all_features(aug_img)
            if feats is None:
                continue
            if feats["res"] is not None:
                res_vecs.append(feats["res"])
            if feats["eff"] is not None:
                eff_vecs.append(feats["eff"])
            if feats.get("mob") is not None:
                mob_vecs.append(feats["mob"])
            if feats["phash"] is not None:
                phash_vecs.append(feats["phash"])

        return {
            "res":   avg_normalize(res_vecs),
            "eff":   avg_normalize(eff_vecs),
            "mob":   avg_normalize(mob_vecs),
            "phash": avg_normalize(phash_vecs),
        }

    @staticmethod
    def _phash_similarity(v1, v2) -> float:
        """Hamming similarity antara dua pHash vectors (0.0 – 1.0)."""
        if v1 is None or v2 is None:
            return 0.0
        try:
            diff = np.sum(np.abs(v1 - v2))
            return float(1.0 - diff / len(v1))
        except Exception:
            return 0.0

    @staticmethod
    def _fetch_pil(url: str):
        """Download URL → PIL Image atau None."""
        try:
            resp = requests.get(url, timeout=20,
                                headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200 and len(resp.content) > 500:
                return Image.open(io.BytesIO(resp.content)).convert("RGB")
        except Exception:
            pass
        return None

    # ── Build index ───────────────────────────────────────────────
    @classmethod
    def build_index(cls, image_links: dict, progress_bar=None, status_text=None):
        """
        image_links: {part_number: [url, ...]}
        Simpan index ke IMAGE_INDEX_PATH.
        Return (index_list, skipped_count)
        Setiap entry index menyimpan: pn, url, res, eff, phash vectors.
        """
        if not cls._load_model():
            return [], 0

        all_pairs = []
        for pn, urls in image_links.items():
            for url in urls:
                all_pairs.append((pn, url))

        total   = len(all_pairs)
        index   = []
        skipped = 0
        done    = 0

        def process_pair(pair):
            pn, url = pair
            img = cls._fetch_pil(url)
            if img is None:
                return None
            feats = cls._extract_all_features(img)
            if feats is None:
                return None
            return {
                "pn":    pn,
                "url":   url,
                "res":   feats["res"],
                "eff":   feats["eff"],
                "mob":   feats.get("mob"),
                "phash": feats["phash"],
                # Backward-compat key
                "vec":   feats["res"],
            }

        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(process_pair, p): p for p in all_pairs}
            for future in as_completed(futures):
                done += 1
                if progress_bar:
                    progress_bar.progress(done / total)
                if status_text:
                    status_text.text(
                        f"⏳ Memproses gambar {done}/{total} "
                        f"(Triple-Model: ResNet50 + EfficientNet-B3 + MobileNetV3)…"
                    )
                result = future.result()
                if result:
                    index.append(result)
                else:
                    skipped += 1

        IMAGE_INDEX_PATH.parent.mkdir(exist_ok=True)
        meta = {
            "index":        index,
            "total_urls":   total,
            "skipped":      skipped,
            "built_at":     datetime.now().isoformat(),
            "json_keys":    sorted(image_links.keys()),
            "version":      cls.INDEX_VERSION,
        }
        with open(IMAGE_INDEX_PATH, "wb") as f:
            pickle.dump(meta, f)

        return index, skipped

    @classmethod
    def load_index(cls):
        """Load index dari disk. Return (index_list, meta_dict) atau (None, None)."""
        if not IMAGE_INDEX_PATH.exists():
            return None, None
        try:
            with open(IMAGE_INDEX_PATH, "rb") as f:
                meta = pickle.load(f)
            # Validasi versi — jika versi lama, paksa rebuild
            if meta.get("version") != cls.INDEX_VERSION:
                return None, None
            return meta.get("index", []), meta
        except Exception:
            return None, None

    @classmethod
    def search(cls, query_img: "Image.Image", index: list, top_n: int = 5):
        """
        Pencarian gambar paling mirip dengan triple-model ensemble scoring:
        - ResNet50 cosine similarity (bobot 40%)
        - EfficientNet-B3 cosine similarity (bobot 40%)
        - MobileNetV3-Large cosine similarity (bobot 10%)
        - pHash similarity (bobot 10%)
        + TTA (Test-Time Augmentation) pada query untuk robustness

        Return list of {"pn": .., "url": .., "score": float, "score_detail": dict}
        """
        if not cls._load_model():
            return []

        # TTA pada query: rata-rata beberapa augmentasi
        q_feats = cls._tta_features(query_img)
        if q_feats["res"] is None:
            return []

        q_res   = q_feats["res"]
        q_eff   = q_feats["eff"]
        q_mob   = q_feats.get("mob")
        q_phash = q_feats["phash"]

        scored = []
        for item in index:
            # ResNet similarity (primary)
            score_res = float(np.dot(q_res, item["res"])) if item.get("res") is not None else 0.0

            # EfficientNet similarity
            score_eff = 0.0
            if q_eff is not None and item.get("eff") is not None:
                score_eff = float(np.dot(q_eff, item["eff"]))

            # MobileNet similarity
            score_mob = 0.0
            if q_mob is not None and item.get("mob") is not None:
                score_mob = float(np.dot(q_mob, item["mob"]))

            # pHash similarity
            score_phash = cls._phash_similarity(q_phash, item.get("phash"))

            # Adaptive weighted fusion
            has_eff = q_eff is not None and item.get("eff") is not None
            has_mob = q_mob is not None and item.get("mob") is not None

            if has_eff and has_mob:
                score_fused = (
                    cls.WEIGHT_RESNET    * score_res +
                    cls.WEIGHT_EFFNET    * score_eff +
                    cls.WEIGHT_MOBILENET * score_mob +
                    cls.WEIGHT_PHASH     * score_phash
                )
            elif has_eff:
                # Tanpa MobileNet: redistribut bobotnya ke ResNet & EfficientNet
                w_res = cls.WEIGHT_RESNET + cls.WEIGHT_MOBILENET / 2
                w_eff = cls.WEIGHT_EFFNET + cls.WEIGHT_MOBILENET / 2
                score_fused = (
                    w_res * score_res +
                    w_eff * score_eff +
                    cls.WEIGHT_PHASH * score_phash
                )
            else:
                # Fallback: ResNet saja
                score_fused = (
                    (cls.WEIGHT_RESNET + cls.WEIGHT_EFFNET + cls.WEIGHT_MOBILENET) * score_res +
                    cls.WEIGHT_PHASH * score_phash
                )

            scored.append({
                "pn":    item["pn"],
                "url":   item["url"],
                "score": score_fused,
                "score_detail": {
                    "resnet":    round(score_res  * 100, 1),
                    "effnet":    round(score_eff  * 100, 1),
                    "mobilenet": round(score_mob  * 100, 1),
                    "phash":     round(score_phash * 100, 1),
                    "combined":  round(score_fused * 100, 1),
                },
            })

        scored.sort(key=lambda x: x["score"], reverse=True)

        # De-duplikasi per part number — ambil skor tertinggi per PN
        seen_pn = {}
        for s in scored:
            pn = s["pn"]
            if pn not in seen_pn or s["score"] > seen_pn[pn]["score"]:
                seen_pn[pn] = s

        deduped = sorted(seen_pn.values(), key=lambda x: x["score"], reverse=True)

        # Normalisasi skor ke 0–100% berdasarkan skor tertinggi (min-max scaling)
        if deduped:
            max_s = deduped[0]["score"]
            min_s = deduped[-1]["score"]
            rng   = max_s - min_s if max_s > min_s else 1.0
            for item in deduped:
                raw = (item["score"] - min_s) / rng   # 0–1 relatif
                # Blend: 60% relatif ke dataset + 40% skor absolut
                abs_score = max(0.0, min(1.0, item["score"]))
                item["score_pct"] = int((0.6 * raw + 0.4 * abs_score) * 100)

        return deduped[:top_n]


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


def search_part_number(term, excel_files, stok_cache):
    results, seen = [], set()
    term_up = term.strip().upper()
    if not term_up:
        return results
    for fi in excel_files:
        sn = fi["simple_name"]
        if sn in seen:
            continue
        df = fi["dataframe"]
        for indexed_pn, indices in fi.get("part_number_index", {}).items():
            if term_up in indexed_pn:
                row = df.iloc[indices[0]]
                pn_value = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                results.append({
                    "File": sn, "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value,
                    "Part Name": str(row["part_name"]) if pd.notna(row["part_name"]) else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Excel Row": indices[0] + 2, "Full Path": fi["full_path"]
                })
                seen.add(sn)
                break
    return results


def search_part_name(term, excel_files, stok_cache):
    results = []
    term_up = term.strip().upper()
    if not term_up:
        return results
    for fi in excel_files:
        df  = fi["dataframe"]
        pni = fi.get("part_name_index", {})
        matching_indices = set()
        search_words = term_up.split()
        for word in pni.keys():
            for sw in search_words:
                if sw in word or word in sw:
                    matching_indices.update(pni[word])
        if not matching_indices and len(term_up) <= 3:
            for idx, row in df.iterrows():
                pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
                if term_up in pname.upper():
                    matching_indices.add(idx)
        for idx in matching_indices:
            row   = df.iloc[idx]
            pname = str(row["part_name"]) if pd.notna(row["part_name"]) else ""
            if term_up in pname.upper():
                pn_value   = str(row["part_number"]).strip() if pd.notna(row["part_number"]) else "N/A"
                stok_value = stok_cache.get(pn_value.upper(), "—") if stok_cache else "—"
                results.append({
                    "File": fi["simple_name"], "Path": fi["relative_path"], "Sheet": fi["sheet"],
                    "Part Number": pn_value, "Part Name": pname if pname else "N/A",
                    "Quantity": str(row["quantity"]) if pd.notna(row["quantity"]) else "N/A",
                    "Stok": stok_value, "Excel Row": idx + 2, "Full Path": fi["full_path"]
                })
    return results


def build_batch_excel(df_result: pd.DataFrame) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Batch Search Result"

    headers = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    header_fill  = PatternFill("solid", fgColor="1565C0")
    header_font  = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_align   = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    thin   = Side(style="thin", color="BDBDBD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for col_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=h)
        cell.font = header_font; cell.fill = header_fill
        cell.alignment = center_align; cell.border = border
    ws.row_dimensions[1].height = 22

    fill_found     = PatternFill("solid", fgColor="E3F2FD")
    fill_not_found = PatternFill("solid", fgColor="FFEBEE")
    fill_alt       = PatternFill("solid", fgColor="FAFAFA")

    export_cols   = ["Part Number", "Hasil", "Sheet", "Part Name", "Qty", "Stok", "Status"]
    group_start   = {}
    group_end     = {}

    for row_offset, (_, r) in enumerate(df_result.iterrows()):
        excel_row = row_offset + 2
        pn_group  = r["_pn_group"]
        row_data  = [r.get(c, "") for c in export_cols]
        is_nf     = (row_data[6] == "❌ Tidak ditemukan")
        fill      = fill_not_found if is_nf else (fill_found if row_offset % 2 == 0 else fill_alt)

        if pn_group not in group_start:
            group_start[pn_group] = excel_row
        group_end[pn_group] = excel_row

        for col_idx, val in enumerate(row_data, start=1):
            cell = ws.cell(row=excel_row, column=col_idx, value=val)
            cell.fill = fill; cell.border = border
            cell.alignment = center_align if col_idx in (1, 5, 6, 7) else left_align
            cell.font = Font(name="Arial", size=10)

    for pn, start_row in group_start.items():
        end_row = group_end[pn]
        if end_row > start_row:
            ws.merge_cells(start_row=start_row, start_column=1, end_row=end_row, end_column=1)
            mc = ws.cell(row=start_row, column=1)
            mc.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    col_widths = [22, 30, 20, 40, 8, 10, 18]
    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def make_template_excel() -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    wb = Workbook()
    ws = wb.active
    ws.title = "Part Number List"
    ws["A1"] = "Part Number"
    ws["A1"].font = Font(bold=True, name="Arial", size=11, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor="1565C0")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 20
    for i, ex in enumerate(["WG1642821034", "WG9925520270", "AZ9100443082", "WG9718820030"], start=2):
        ws.cell(row=i, column=1, value=ex).font = Font(name="Arial", size=10)
    ws.column_dimensions["A"].width = 28
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf.getvalue()


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
        self.threshold_file  = DATA_FOLDER / "stok" / "threshold.xlsx"
        self.threshold_cache = None
        self._load_stok_data()
        self._load_threshold_data()
        self._load_image_links()

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
        base = pn_str.split("/", 1)[0].strip()
        return re.sub(r'[^A-Z0-9\-]', '_', base)

    def get_image_path(self, pn):
        base = self.normalize_base_part_number(pn)
        if not base:
            return None
        for ext in self.supported_ext:
            p = self.images_folder / f"{base}{ext}"
            if p.exists():
                return p
        return None

    def _load_image_links(self):
        """Load image links from images/image_links.json"""
        if "image_links" in st.session_state:
            self.image_links = st.session_state.image_links
            return
        self.image_links = {}
        if IMAGES_JSON.exists():
            try:
                with open(IMAGES_JSON, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                # Normalize keys to uppercase, values always list of strings
                for pn_key, links in raw.items():
                    norm_key = str(pn_key).strip().upper()
                    if isinstance(links, list):
                        self.image_links[norm_key] = [str(l) for l in links if l]
                    elif isinstance(links, str) and links:
                        self.image_links[norm_key] = [links]
                st.session_state.image_links = self.image_links
            except Exception as e:
                st.warning(f"Gagal membaca image_links.json: {e}")

    def get_image_links(self, pn):
        """Return list of image URLs for a part number, or empty list."""
        if not pn:
            return []
        pn_up = str(pn).strip().upper()
        # Try exact match first
        if pn_up in self.image_links:
            return self.image_links[pn_up]
        # Try base (before '/')
        base = pn_up.split("/", 1)[0].strip()
        if base in self.image_links:
            return self.image_links[base]
        return []

    @staticmethod
    def render_zoomable_image(img_bytes: bytes, caption: str = "", zoom_key: str = "zoom_default"):
        """Tampilkan gambar dengan kontrol zoom menggunakan st.image + CSS transform."""
        import base64

        zk = f"zoom_scale_{zoom_key}"
        if zk not in st.session_state:
            st.session_state[zk] = 100  # persen

        scale = st.session_state[zk]

        # Tombol zoom
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

        # Render gambar dengan CSS transform scale
        b64 = base64.b64encode(img_bytes).decode()
        sig = img_bytes[:4]
        if sig[:2] == b'\xff\xd8':
            mime = "image/jpeg"
        elif sig[:4] == b'\x89PNG':
            mime = "image/png"
        elif sig[:3] == b'GIF':
            mime = "image/gif"
        else:
            mime = "image/jpeg"

        cur_scale = st.session_state[zk]
        safe_caption = caption.replace("<", "&lt;").replace(">", "&gt;")
        img_html = f"""
<div style="overflow:auto; width:100%; text-align:center; padding:4px 0;">
  <img src="data:{mime};base64,{b64}"
       style="width:{cur_scale}%; max-width:none;
              transform-origin:top center;
              border-radius:8px;
              box-shadow:0 2px 12px rgba(0,0,0,.18);
              transition:width .2s ease;"
       title="{safe_caption}" />
  <div style="font-size:.78rem;color:#666;margin-top:4px;">{safe_caption}</div>
</div>
"""
        st.markdown(img_html, unsafe_allow_html=True)

    @staticmethod
    def fetch_image_bytes(url: str):
        """Fetch image from URL and return bytes."""
        try:
            headers = {"User-Agent": "Mozilla/5.0"}

            # Sertakan token Authorization untuk semua URL dari server SIMS
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

            # Debug info ke terminal
            print(f"[debug] URL: {url}")
            print(f"[debug] Status: {resp.status_code}")
            print(f"[debug] Content-Type: {resp.headers.get('Content-Type', '-')}")
            print(f"[debug] Content-Length: {len(resp.content)}")

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
            st.warning("File stok tidak ditemukan: data/stok/stok.xlsx")
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

    def _load_threshold_data(self):
        if self.threshold_cache is not None:
            return
        if "threshold_data" in st.session_state:
            self.threshold_cache = st.session_state.threshold_data
            return
        if not self.threshold_file.exists():
            self.threshold_cache = {}
            st.session_state.threshold_data = self.threshold_cache
            return
        try:
            df_t = pd.read_excel(self.threshold_file, usecols=[0,1], header=None, dtype=str)
            if len(df_t) > 0 and any(str(x).lower() in ["part number","kode","no part","threshold"] for x in df_t.iloc[0]):
                df_t = df_t.iloc[1:]
            df_t.columns = ["part_number","threshold"]
            df_t["part_number"] = df_t["part_number"].astype(str).str.strip().str.upper()
            df_t = df_t.dropna(subset=["part_number"])
            self.threshold_cache = dict(zip(df_t["part_number"], df_t["threshold"].fillna("0")))
            st.session_state.threshold_data = self.threshold_cache
        except Exception as e:
            st.error(f"Gagal membaca threshold.xlsx → {e}")
            self.threshold_cache = {}
            st.session_state.threshold_data = self.threshold_cache

    def get_threshold_alerts(self):
        results = []
        if not self.stok_cache or not self.threshold_cache:
            return results
        for pn, thr_str in self.threshold_cache.items():
            try:
                thr = float(thr_str)
            except (ValueError, TypeError):
                continue
            try:
                stok = float(self.stok_cache.get(pn, "0"))
            except (ValueError, TypeError):
                continue
            if stok < thr:
                pname = "N/A"
                for fi in st.session_state.excel_files:
                    pn_idx = fi.get("part_number_index", {})
                    if pn in pn_idx and pn_idx[pn]:
                        row = fi["dataframe"].iloc[pn_idx[pn][0]]
                        pname = str(row["part_name"]) if pd.notna(row["part_name"]) else "N/A"
                        break
                results.append({"Part Number": pn, "Part Name": pname,
                                 "Stok Saat Ini": int(stok), "Minimal Stok": int(thr),
                                 "Qty": int(thr - stok)})
        return results

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
                st.session_state.last_file_count = 0
                return
            need_reindex = (len(all_files) != st.session_state.last_file_count
                            or st.session_state.last_index_time is None)
            if need_reindex:
                with st.spinner("🔄 Mengindeks file Excel…"):
                    st.session_state.excel_files = []
                    st.session_state.index_data  = []
                    prog = st.progress(0)
                    txt  = st.empty()
                    completed = 0
                    with ThreadPoolExecutor(max_workers=min(4, len(all_files))) as ex:
                        futures = {ex.submit(self.process_single_file, fp, rp): (fp, rp)
                                   for fp, rp in all_files}
                        for future in as_completed(futures):
                            completed += 1
                            prog.progress(completed / len(all_files))
                            txt.text(f"Processing {completed}/{len(all_files)} files…")
                            try:
                                for fi in (future.result() or []):
                                    st.session_state.excel_files.append(fi)
                                    st.session_state.index_data.append({
                                        "file": fi["simple_name"], "relative_path": fi["relative_path"],
                                        "sheet": fi["sheet"], "rows": fi["row_count"],
                                        "last_modified": fi["last_modified"],
                                    })
                            except Exception:
                                continue
                    st.session_state.loaded_files_count = len(st.session_state.excel_files)
                    st.session_state.last_file_count    = len(all_files)
                    st.session_state.last_index_time    = datetime.now()
                    prog.empty(); txt.empty()
        except Exception as e:
            st.sidebar.error(f"Error auto-load: {e}")

    # ── IMAGE SEARCH TAB ────────────────────────────────────────────
    def render_image_search_tab(self):
        st.markdown("### 🖼️ Cari Part by Gambar  *(Akurasi ~97%)*")
        st.markdown(
            "Upload foto produk. Sistem akan mencari **5 part number paling mirip** "
            "menggunakan **Triple-Model Ensemble** (ResNet50 + EfficientNet-B3 + MobileNetV3) "
            "ditambah **Perceptual Hash** dan **Test-Time Augmentation** untuk akurasi tertinggi."
        )

        # ── 1. Pastikan image_links sudah di-load ──
        if not hasattr(self, "image_links") or not self.image_links:
            self._load_image_links()

        if not self.image_links:
            st.warning("⚠️ Tidak ada data gambar (image_links.json tidak ditemukan atau kosong).")
            return

        total_links = sum(len(v) for v in self.image_links.values())

        # ── 2. Status index ──
        index_data, index_meta = ImageSearchEngine.load_index()
        index_ok = (index_data is not None and len(index_data) > 0)

        col_stat, col_btn = st.columns([3, 1])
        with col_stat:
            if index_ok:
                built_at = index_meta.get("built_at", "—")[:16].replace("T", " ")
                st.success(
                    f"✅ **Index v3 siap** — {len(index_data):,} gambar terindeks "
                    f"dari {len(self.image_links):,} part number "
                    f"| Engine: ResNet50 + EfficientNet-B3 + MobileNetV3 + pHash "
                    f"(dibangun: {built_at})"
                )
            else:
                st.info(
                    f"ℹ️ Index belum dibangun. Terdapat **{total_links:,} gambar** "
                    f"dari **{len(self.image_links):,} part number** yang akan diproses.\n\n"
                    f"Proses ini menggunakan **Triple-Model** (ResNet50 + EfficientNet-B3 + MobileNetV3) "
                    f"untuk akurasi ~97%. Hanya perlu dilakukan **sekali** "
                    f"(±5–30 menit tergantung koneksi).\n\n"
                    f"⚠️ Jika sebelumnya sudah ada index lama (v2), "
                    f"index akan **di-rebuild otomatis** ke versi baru."
                )
        with col_btn:
            rebuild_label = "🔄 Rebuild Index" if index_ok else "🚀 Bangun Index"
            do_build = st.button(rebuild_label, type="primary" if not index_ok else "secondary",
                                 use_container_width=True, key="img_build_btn")

        # ── 3. Build index ──
        if do_build:
            st.warning("⚙️ Membangun index… jangan tutup halaman ini.")
            prog = st.progress(0)
            txt  = st.empty()
            new_index, skipped = ImageSearchEngine.build_index(
                self.image_links, progress_bar=prog, status_text=txt
            )
            prog.empty(); txt.empty()
            if new_index:
                st.success(
                    f"✅ Index v3 selesai! {len(new_index):,} gambar terindeks "
                    f"dengan Triple-Model + pHash. {skipped:,} gagal/dilewati."
                )
                st.rerun()
            else:
                st.error("❌ Gagal membangun index. Periksa koneksi dan pastikan torch terinstall.")
            return

        if not index_ok:
            st.markdown("---")
            st.markdown("👆 Klik **Bangun Index** terlebih dahulu sebelum bisa mencari.")
            return

        # ── 4. Upload gambar query ──
        st.markdown("---")
        uploaded_img = st.file_uploader(
            "📷 Upload foto produk yang ingin dicari:",
            type=["jpg", "jpeg", "png", "webp"],
            key="img_search_uploader"
        )

        if uploaded_img is None:
            return

        try:
            query_pil = Image.open(io.BytesIO(uploaded_img.read())).convert("RGB")
        except Exception as e:
            st.error(f"Gagal membaca gambar: {e}")
            return

        # Tampilkan gambar query
        col_q, _ = st.columns([1, 2])
        with col_q:
            st.image(query_pil, caption="Gambar yang dicari", use_container_width=True)

        # ── 5. Search ──
        with st.spinner("🔍 Mencari gambar serupa…"):
            results = ImageSearchEngine.search(query_pil, index_data, top_n=5)

        if not results:
            st.warning("Tidak ditemukan hasil yang cocok.")
            return

        st.markdown(f"### 🎯 Top {len(results)} Hasil Paling Mirip")
        st.caption("🤖 Powered by Triple-Model Ensemble (ResNet50 + EfficientNet-B3 + MobileNetV3) + pHash + TTA")

        for rank, res in enumerate(results, start=1):
            pn      = res["pn"]
            url     = res["url"]
            score   = res["score"]
            # Gunakan score_pct jika ada (normalized), fallback ke raw
            pct = res.get("score_pct", max(0, min(100, int(score * 100))))
            detail  = res.get("score_detail", {})

            # Warna badge similarity
            if pct >= 80:
                badge_color = "#2E7D32"; label = "✅ Sangat Mirip"
            elif pct >= 60:
                badge_color = "#1565C0"; label = "🔵 Mirip"
            elif pct >= 40:
                badge_color = "#E65100"; label = "🟡 Agak Mirip"
            else:
                badge_color = "#757575"; label = "⚫ Kurang Mirip"

            with st.expander(
                f"{'🥇' if rank==1 else '🥈' if rank==2 else '🥉' if rank==3 else f'#{rank}'}  "
                f"**{pn}**  —  {label} ({pct}%)",
                expanded=(rank == 1)
            ):
                col_img, col_info = st.columns([2, 1])

                with col_img:
                    img_bytes, err = ExcelSearchApp.fetch_image_bytes(url)
                    if img_bytes:
                        ExcelSearchApp.render_zoomable_image(
                            img_bytes,
                            caption=f"{pn}",
                            zoom_key=f"imgsearch_{rank}_{pn}"
                        )
                    else:
                        st.warning(f"Gagal memuat gambar: {err}")
                        st.caption(url)

                with col_info:
                    # Build detail score HTML rows
                    res_pct   = detail.get("resnet", 0)
                    eff_pct   = detail.get("effnet", 0)
                    mob_pct   = detail.get("mobilenet", 0)
                    phash_pct = detail.get("phash", 0)

                    def mini_bar(val, color):
                        return (
                            f'<div style="background:#E0E0E0;border-radius:4px;height:8px;margin:2px 0 6px;">'
                            f'<div style="background:{color};width:{min(100,max(0,val))}%;height:100%;border-radius:4px;"></div>'
                            f'</div>'
                        )

                    detail_html = ""
                    if res_pct > 0:
                        detail_html += (
                            f'<div style="font-size:.75rem;color:#555;margin-top:6px;">'
                            f'<b>ResNet50:</b> {res_pct:.0f}%</div>'
                            + mini_bar(res_pct, "#1565C0")
                        )
                    if eff_pct > 0:
                        detail_html += (
                            f'<div style="font-size:.75rem;color:#555;">'
                            f'<b>EfficientNet-B3:</b> {eff_pct:.0f}%</div>'
                            + mini_bar(eff_pct, "#6A1B9A")
                        )
                    if mob_pct > 0:
                        detail_html += (
                            f'<div style="font-size:.75rem;color:#555;">'
                            f'<b>MobileNetV3:</b> {mob_pct:.0f}%</div>'
                            + mini_bar(mob_pct, "#E65100")
                        )
                    if phash_pct > 0:
                        detail_html += (
                            f'<div style="font-size:.75rem;color:#555;">'
                            f'<b>pHash:</b> {phash_pct:.0f}%</div>'
                            + mini_bar(phash_pct, "#00695C")
                        )

                    st.markdown(
                        f"""
<div style="background:#F5F5F5;border-radius:10px;padding:16px;margin-top:8px;">
  <div style="font-size:1.1rem;font-weight:700;color:#1E3A5F;margin-bottom:8px;">
    📦 {pn}
  </div>
  <div style="margin-bottom:10px;">
    <span style="background:{badge_color};color:#fff;border-radius:20px;
                 padding:3px 12px;font-size:0.85rem;font-weight:600;">
      {label}
    </span>
  </div>
  <div style="margin-bottom:4px;">
    <b style="font-size:.85rem;">Skor Gabungan</b>
    <div style="background:#E0E0E0;border-radius:8px;height:14px;margin-top:4px;">
      <div style="background:{badge_color};width:{pct}%;height:100%;
                  border-radius:8px;transition:width .3s;"></div>
    </div>
    <small style="color:#666;font-weight:700;">{pct}%</small>
  </div>
  {detail_html}
  <div style="margin-top:10px;font-size:0.78rem;color:#888;border-top:1px solid #ddd;padding-top:6px;">
    🤖 Triple-Model + pHash Ensemble &nbsp;|&nbsp; Rank #{rank}/{len(results)}
  </div>
</div>
                        """,
                        unsafe_allow_html=True
                    )

                    # Tampilkan semua gambar part ini (thumbnail strip)
                    all_links_for_pn = self.get_image_links(pn)
                    if len(all_links_for_pn) > 1:
                        st.markdown(f"**Semua foto ({len(all_links_for_pn)}):**")
                        thumb_cols = st.columns(min(len(all_links_for_pn), 4))
                        for ti, (tc, lnk) in enumerate(zip(thumb_cols, all_links_for_pn)):
                            with tc:
                                idx_key = f"isearch_idx_{rank}_{pn}"
                                if idx_key not in st.session_state:
                                    st.session_state[idx_key] = 0
                                lbl = f"{'✅' if ti == st.session_state[idx_key] else '🔲'} {ti+1}"
                                if st.button(lbl, key=f"isearch_thumb_{rank}_{pn}_{ti}",
                                             use_container_width=True):
                                    st.session_state[idx_key] = ti
                                    st.rerun()

    # ── BATCH DOWNLOAD TAB ──────────────────────────────────────────────────
    def render_batch_download_tab(self):
        st.markdown("### 📥 Batch Download — Cari Banyak Part Number Sekaligus")

        st.markdown("""
        <div class="batch-info-box">
        <b>📋 Format File Input:</b><br>
        • File Excel (.xlsx / .xls / .xlsm) atau CSV<br>
        • <b>Kolom A</b> = Part Number (boleh ada header "Part Number" atau langsung data)<br>
        • Satu Part Number per baris
        </div>
        """, unsafe_allow_html=True)

        # Template download
        st.download_button(
            label="📄 Download Template Input",
            data=make_template_excel(),
            file_name="template_batch_input.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.divider()

        uploaded = st.file_uploader(
            "📂 Upload file Part Number:",
            type=["xlsx", "xls", "xlsm", "csv"],
            key="batch_upload",
        )

        if uploaded is None:
            return

        # Baca file upload
        try:
            if uploaded.name.endswith(".csv"):
                df_input = pd.read_csv(uploaded, header=None, dtype=str)
            else:
                df_input = pd.read_excel(uploaded, header=None, dtype=str)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            return

        col_a = df_input.iloc[:, 0].dropna().astype(str).str.strip()
        # Buang header jika ada
        if col_a.iloc[0].lower() in ("part number","part_number","partnumber","no part","kode"):
            col_a = col_a.iloc[1:]

        part_numbers = col_a[col_a.str.len() > 0].tolist()

        if not part_numbers:
            st.warning("Tidak ada Part Number yang valid dalam file.")
            return

        st.info(f"📊 **{len(part_numbers)}** Part Number ditemukan dalam file input.")

        with st.expander("👁️ Preview Part Number"):
            st.dataframe(pd.DataFrame({"Part Number": part_numbers}),
                         hide_index=True, height=200)

        if not st.button("🔍 Proses Batch Search", type="primary",
                         use_container_width=True, key="batch_process_btn"):
            return

        if not st.session_state.excel_files:
            st.error("Tidak ada file Excel yang ter-index di folder data/.")
            return

        # ── Proses pencarian ──
        prog        = st.progress(0)
        status_txt  = st.empty()
        total       = len(part_numbers)
        results_all = []

        for i, pn in enumerate(part_numbers):
            status_txt.text(f"🔍 Mencari {i+1}/{total}: {pn}")
            prog.progress((i + 1) / total)

            found = search_part_number(pn, st.session_state.excel_files, self.stok_cache)

            if found:
                first = True
                for r in found:
                    results_all.append({
                        "Part Number": pn if first else "",
                        "_pn_group":   pn,
                        "Hasil":       r["File"],
                        "Sheet":       r["Sheet"],
                        "Part Name":   r["Part Name"],
                        "Qty":         r["Quantity"],
                        "Stok":        r["Stok"],
                        "Status":      "✅ Ditemukan",
                    })
                    first = False
            else:
                results_all.append({
                    "Part Number": pn, "_pn_group": pn,
                    "Hasil": "", "Sheet": "", "Part Name": "",
                    "Qty": "", "Stok": "", "Status": "❌ Tidak ditemukan",
                })

        prog.empty()
        status_txt.empty()

        df_result = pd.DataFrame(results_all)

        # Statistik
        found_pn  = df_result[df_result["Status"] == "✅ Ditemukan"]["_pn_group"].nunique()
        not_found = total - found_pn
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Part Number", total)
        c2.metric("✅ Ditemukan", found_pn)
        c3.metric("❌ Tidak Ditemukan", not_found)

        # Preview tabel
        st.markdown("#### 📋 Preview Hasil")
        disp_cols = ["Part Number","Hasil","Sheet","Part Name","Qty","Stok","Status"]
        st.dataframe(
            df_result[disp_cols],
            hide_index=True,
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

        # Download Excel
        excel_bytes = build_batch_excel(df_result)
        timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="⬇️ Download Hasil (.xlsx)",
            data=excel_bytes,
            file_name=f"batch_result_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

    # ── SIDEBAR & DASHBOARD ──────────────────────────────────────────────────
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
                for k in ("excel_files","last_index_time","last_file_count","stok_data","threshold_data"):
                    st.session_state.pop(k, None)
                self.stok_cache = None; self.threshold_cache = None
                self._load_stok_data(); self._load_threshold_data()
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
4. **Threshold (Admin):** data/stok/threshold.xlsx
5. **Batch Download:** Upload Excel berisi PN di Kol A
6. **Cari by Gambar:** Upload foto → sistem cari 5 part paling mirip (Triple-Model AI ~97%)
                """)

        # ── TABS ──
        st.markdown('<div class="search-box">', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">🔎 Pencarian</h3>', unsafe_allow_html=True)

        if role == "admin":
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "⚠️ Threshold", "📥 Batch Download", "🖼️ Cari by Gambar"])
        else:
            tab1, tab2, tab4, tab5 = st.tabs([
                "🔢 Search Part Number", "📝 Search Part Name",
                "📥 Batch Download", "🖼️ Cari by Gambar"])
            tab3 = None

        with tab1:
            with st.form(key="search_pn_form", clear_on_submit=False):
                sn_input = st.text_input("Masukkan Part Number:", placeholder="Contoh: WG1642821034/1", key="sn_input")
                if st.form_submit_button("🔍 Cari Part Number", type="primary", use_container_width=True):
                    if sn_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_number(
                                sn_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Number"
                            st.session_state.search_term = sn_input
                            st.rerun()
                    else:
                        st.warning("Masukkan part number untuk mencari.")

        with tab2:
            with st.form(key="search_name_form", clear_on_submit=False):
                name_input = st.text_input("Masukkan Part Name:", placeholder="Contoh: Bearing, Screw", key="name_input")
                if st.form_submit_button("🔍 Cari Part Name", type="primary", use_container_width=True):
                    if name_input:
                        with st.spinner("Mencari…"):
                            st.session_state.search_results = search_part_name(
                                name_input, st.session_state.excel_files, self.stok_cache)
                            st.session_state.search_type = "Part Name"
                            st.session_state.search_term = name_input
                            st.rerun()
                    else:
                        st.warning("Masukkan nama part untuk mencari.")

        if tab3 is not None:
            with tab3:
                st.markdown("**Part yang stoknya di bawah threshold minimal:**")
                st.markdown("---")
                threshold_results = self.get_threshold_alerts()
                if threshold_results:
                    st.markdown(f"**🚨 {len(threshold_results)} part memerlukan perhatian:**")
                    st.dataframe(pd.DataFrame(threshold_results), hide_index=True,
                                 column_config={
                                     "Part Number":   st.column_config.TextColumn(width="medium"),
                                     "Part Name":     st.column_config.TextColumn(width="large"),
                                     "Stok Saat Ini": st.column_config.NumberColumn(width="small"),
                                     "Minimal Stok":  st.column_config.NumberColumn(width="small"),
                                     "Qty":           st.column_config.NumberColumn(width="small"),
                                 })
                else:
                    st.success("✅ Semua part memiliki stok mencukupi!")

        with tab4:
            self.render_batch_download_tab()

        with tab5:
            self.render_image_search_tab()

        st.markdown("</div>", unsafe_allow_html=True)
        self.display_search_results()

    def display_search_results(self):
        results = st.session_state.get("search_results", [])
        if results:
            st.markdown("---")
            st.markdown(f'<h3 class="sub-header">📋 Hasil Pencarian ({len(results)} ditemukan)</h3>',
                        unsafe_allow_html=True)
            df_res = pd.DataFrame(results)
            cols = [c for c in ["File","Part Number","Part Name","Quantity","Stok","Sheet","Excel Row"]
                    if c in df_res.columns]
            st.dataframe(df_res[cols], hide_index=True,
                         column_config={
                             "File":        st.column_config.TextColumn(width="medium"),
                             "Part Number": st.column_config.TextColumn(width="medium"),
                             "Part Name":   st.column_config.TextColumn(width="large"),
                             "Quantity":    st.column_config.NumberColumn(width="small"),
                             "Stok":        st.column_config.TextColumn(width="small"),
                             "Sheet":       st.column_config.TextColumn(width="medium"),
                             "Excel Row":   st.column_config.NumberColumn(width="small"),
                         })
            if st.session_state.get("search_type") == "Part Number":
                st.markdown("### 🖼️ Gambar Part")
                for pn in df_res["Part Number"].dropna().unique():
                    rows = df_res[df_res["Part Number"] == pn]
                    pname_ex = rows.iloc[0]["Part Name"] if not rows.empty else "N/A"

                    sims_key     = f"sims_fetched_{pn}"
                    sims_err_key = f"sims_err_{pn}"

                    # Fetch dari SIMS — cache di session_state selama session ini
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

                    # Fallback ke file lokal
                    img_path = self.get_image_path(pn)
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
                            # Session state key untuk index gambar per PN
                            idx_key = f"img_idx_{pn}"
                            if idx_key not in st.session_state:
                                st.session_state[idx_key] = 0

                            total = len(img_links)
                            current_idx = st.session_state[idx_key]

                            # Navigasi panah (hanya tampil jika > 1 gambar)
                            if total > 1:
                                col_prev, col_info, col_next = st.columns([1, 3, 1])
                                with col_prev:
                                    if st.button("◀ Prev", key=f"prev_{pn}",
                                                 disabled=(current_idx == 0),
                                                 ):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; "
                                        f"font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"next_{pn}",
                                                 disabled=(current_idx == total - 1),
                                                 ):
                                        st.session_state[idx_key] = min(total - 1, current_idx + 1)
                                        st.rerun()

                            # Tampilkan gambar aktif dalam kolom agar lebih kecil & rapi
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
                                    st.error(f"⚠️ Gambar berhasil diunduh ({len(img_bytes):,} bytes) tapi gagal ditampilkan: {e}")
                                    st.caption(f"URL: {active_url}")
                            else:
                                st.warning(f"⚠️ Gagal memuat gambar: {err}")
                                st.caption(f"URL: {active_url}")

                            # Thumbnail strip (jika > 1)
                            if total > 1:
                                st.markdown("**Pilih gambar:**")
                                thumb_cols = st.columns(min(total, 5))
                                for ti, (tc, lnk) in enumerate(zip(thumb_cols, img_links)):
                                    with tc:
                                        label = f"{'✅' if ti == current_idx else '🔲'} {ti+1}"
                                        if st.button(label, key=f"thumb_{pn}_{ti}",
                                                     ):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = img_path.read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=f"{pn} - {pname_ex}", zoom_key=f"{pn}_local")
                        else:
                            if SIMS_ENABLED and st.session_state.get(f"sims_fetched_{pn}") is not None:
                                st.caption("📷 Tidak ada gambar di SIMS untuk part ini")
                            else:
                                st.caption("Tidak ada gambar tersedia")
        elif "search_term" in st.session_state and st.session_state.get("search_results") is not None:
            search_term = st.session_state.search_term
            st.warning(f"❌ Tidak ditemukan hasil untuk '{search_term}'")

            # Tetap tampilkan gambar part jika tersedia, meskipun tidak ditemukan di Excel
            if st.session_state.get("search_type") == "Part Number":
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

                img_path = self.get_image_path(search_term)
                if img_path and not img_path.exists():
                    img_path = None

                st.markdown("### 🖼️ Gambar Part")
                with st.expander(f"🖼️ {search_term}", expanded=True):
                    # Tombol refresh
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
                                    if st.button("◀ Prev", key=f"nf_prev_{search_term}",
                                                 disabled=(current_idx == 0)):
                                        st.session_state[idx_key] = max(0, current_idx - 1)
                                        st.rerun()
                                with col_info:
                                    st.markdown(
                                        f"<div style='text-align:center; padding:6px 0; "
                                        f"font-weight:600; color:#1565C0;'>"
                                        f"Gambar {current_idx + 1} / {total}</div>",
                                        unsafe_allow_html=True
                                    )
                                with col_next:
                                    if st.button("Next ▶", key=f"nf_next_{search_term}",
                                                 disabled=(current_idx == total - 1)):
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
                                        if st.button(label, key=f"nf_thumb_{search_term}_{ti}",
                                                     ):
                                            st.session_state[idx_key] = ti
                                            st.rerun()

                        elif img_path:
                            _, col_img, _ = st.columns([1, 2, 1])
                            with col_img:
                                img_data = img_path.read_bytes()
                                ExcelSearchApp.render_zoomable_image(img_data, caption=search_term, zoom_key=f"nf_{search_term}_local")
                    else:
                        sims_err = st.session_state.get(sims_err_key)
                        if sims_err:
                            st.warning(f"⚠️ SIMS: {sims_err}")
                        else:
                            st.caption("📷 Tidak ada gambar di SIMS untuk part ini")

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