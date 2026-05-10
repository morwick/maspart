"""
Part Compare — Interchange-Oriented Image & Name Similarity (Pure Python)
=========================================================================
Tujuan: menilai apakah 2 part number kemungkinan SALING INTERCHANGE
berdasarkan analisis foto SIMS + nama part.

Sinyal yang dihitung:
  • SHAPE  → pHash + dHash + SSIM-lite + edge density + aspect ratio
             (yang paling menentukan fitment fisik)
  • NAME   → difflib SequenceMatcher + Jaccard token-overlap pada partName
  • COLOR  → histogram + mean color  (info saja, bobot sangat kecil
             karena part interchangeable bisa beda warna/finish)

Pure Python: hanya butuh PIL + numpy + stdlib (difflib, re).
"""
from __future__ import annotations

import io
import re
import difflib
from typing import Dict, List, Tuple

import numpy as np
from PIL import Image, ImageFilter


# ─────────────────────────────────────────────────────────────
#  IMAGE LOADER
# ─────────────────────────────────────────────────────────────
def _load(img_bytes: bytes) -> Image.Image:
    img = Image.open(io.BytesIO(img_bytes))
    if img.mode != "RGB":
        img = img.convert("RGB")
    return img


# ─────────────────────────────────────────────────────────────
#  PERCEPTUAL HASHES (shape signals)
# ─────────────────────────────────────────────────────────────
def _bits_to_int(bits: np.ndarray) -> int:
    h = 0
    for b in bits.flatten():
        h = (h << 1) | int(b)
    return h


def _ahash(img: Image.Image, size: int = 8) -> int:
    g = img.convert("L").resize((size, size), Image.LANCZOS)
    arr = np.asarray(g, dtype=np.float32)
    return _bits_to_int(arr > arr.mean())


def _dhash(img: Image.Image, size: int = 8) -> int:
    g = img.convert("L").resize((size + 1, size), Image.LANCZOS)
    arr = np.asarray(g, dtype=np.int16)
    return _bits_to_int(arr[:, 1:] > arr[:, :-1])


def _dct_1d(x: np.ndarray) -> np.ndarray:
    N = x.shape[-1]
    n = np.arange(N)
    k = n.reshape(-1, 1)
    M = np.cos(np.pi * (2 * n + 1) * k / (2 * N))
    return x @ M.T


def _dct_2d(x: np.ndarray) -> np.ndarray:
    return _dct_1d(_dct_1d(x).T).T


def _phash(img: Image.Image, size: int = 32, hash_size: int = 8) -> int:
    g = img.convert("L").resize((size, size), Image.LANCZOS)
    arr = np.asarray(g, dtype=np.float32)
    dct = _dct_2d(arr)
    low = dct[:hash_size, :hash_size]
    flat = low.flatten()
    median = np.median(flat[1:])  # buang DC
    return _bits_to_int(low > median)


def _hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def _hash_sim(a: int, b: int, bits: int = 64) -> float:
    return 1.0 - (_hamming(a, b) / bits)


# ─────────────────────────────────────────────────────────────
#  HISTOGRAM & COLOR (color signal — info only)
# ─────────────────────────────────────────────────────────────
def _histogram(img: Image.Image, bins: int = 32) -> np.ndarray:
    arr = np.asarray(img.resize((128, 128), Image.LANCZOS))
    parts = []
    for c in range(3):
        h, _ = np.histogram(arr[:, :, c], bins=bins, range=(0, 256))
        parts.append(h.astype(np.float32))
    h = np.concatenate(parts)
    s = h.sum()
    return h / s if s > 0 else h


def _hist_sim(h1: np.ndarray, h2: np.ndarray) -> float:
    return float(np.minimum(h1, h2).sum())


def _mean_color(img: Image.Image) -> np.ndarray:
    arr = np.asarray(img.resize((64, 64), Image.LANCZOS), dtype=np.float32)
    return arr.reshape(-1, 3).mean(axis=0)


def _mean_color_sim(c1: np.ndarray, c2: np.ndarray) -> float:
    d = float(np.linalg.norm(c1 - c2))
    return max(0.0, 1.0 - d / 441.6729)  # sqrt(3*255^2)


# ─────────────────────────────────────────────────────────────
#  EDGE, SSIM, ASPECT (shape signals)
# ─────────────────────────────────────────────────────────────
def _edge_density(img: Image.Image) -> float:
    g = img.convert("L").resize((128, 128), Image.LANCZOS)
    edges = g.filter(ImageFilter.FIND_EDGES)
    arr = np.asarray(edges, dtype=np.float32)
    return float(arr.mean() / 255.0)


def _ssim_lite(img1: Image.Image, img2: Image.Image, size: int = 64) -> float:
    a = np.asarray(img1.convert("L").resize((size, size), Image.LANCZOS), dtype=np.float64)
    b = np.asarray(img2.convert("L").resize((size, size), Image.LANCZOS), dtype=np.float64)
    mu_a, mu_b = a.mean(), b.mean()
    sa, sb = a.var(), b.var()
    cov = ((a - mu_a) * (b - mu_b)).mean()
    C1 = (0.01 * 255) ** 2
    C2 = (0.03 * 255) ** 2
    num = (2 * mu_a * mu_b + C1) * (2 * cov + C2)
    den = (mu_a ** 2 + mu_b ** 2 + C1) * (sa + sb + C2)
    return float(np.clip(num / den, 0.0, 1.0))


def _aspect_sim(size1: Tuple[int, int], size2: Tuple[int, int]) -> float:
    """Similarity dari rasio sisi (proxy kasar untuk dimensi)."""
    w1, h1 = size1
    w2, h2 = size2
    if h1 == 0 or h2 == 0:
        return 0.0
    r1 = w1 / h1
    r2 = w2 / h2
    if r1 == 0 or r2 == 0:
        return 0.0
    return float(min(r1, r2) / max(r1, r2))


# ─────────────────────────────────────────────────────────────
#  PART NAME SIMILARITY (name signal)
# ─────────────────────────────────────────────────────────────
_TOKEN_RX = re.compile(r"[A-Za-z0-9]+")


def _normalize_name(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[\(\)\[\]\{\}\.,;:/\\\-_]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _tokens(s: str) -> List[str]:
    return _TOKEN_RX.findall(_normalize_name(s))


def _jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def name_similarity(name1: str, name2: str) -> float:
    """Skor kemiripan nama 0..1. Kombinasi SequenceMatcher + token Jaccard."""
    n1 = _normalize_name(name1)
    n2 = _normalize_name(name2)
    if not n1 and not n2:
        return 0.0
    if not n1 or not n2:
        return 0.0
    if n1 == n2:
        return 1.0
    seq  = difflib.SequenceMatcher(None, n1, n2).ratio()
    jac  = _jaccard(_tokens(n1), _tokens(n2))
    return float(0.6 * seq + 0.4 * jac)


# ─────────────────────────────────────────────────────────────
#  SUB-SCORES (shape / color / name)
# ─────────────────────────────────────────────────────────────
SHAPE_WEIGHTS: Dict[str, float] = {
    "phash":        0.35,
    "dhash":        0.25,
    "ssim":         0.20,
    "edge_density": 0.10,
    "aspect":       0.10,
}

COLOR_WEIGHTS: Dict[str, float] = {
    "histogram":  0.65,
    "mean_color": 0.35,
}

METRIC_LABELS: Dict[str, str] = {
    "phash":        "pHash (struktur frekuensi)",
    "dhash":        "dHash (gradien)",
    "ahash":        "aHash (rata-rata)",
    "ssim":         "SSIM (struktur)",
    "edge_density": "Edge Density",
    "aspect":       "Aspect Ratio",
    "histogram":    "Histogram Warna",
    "mean_color":   "Mean Color",
}


def _shape_score(img1: Image.Image, img2: Image.Image, raw: Dict) -> float:
    return sum(raw[k] * SHAPE_WEIGHTS[k] for k in SHAPE_WEIGHTS)


def _color_score(raw: Dict) -> float:
    return sum(raw[k] * COLOR_WEIGHTS[k] for k in COLOR_WEIGHTS)


# ─────────────────────────────────────────────────────────────
#  INTERCHANGE VERDICT
# ─────────────────────────────────────────────────────────────
def interchange_verdict(shape: float, name: float | None, overall: float) -> Tuple[str, str]:
    """
    Verdict berbasis SHAPE sebagai gating + dukungan dari NAME.
    Bentuk yang sangat berbeda → langsung tidak interchange,
    apapun overall-nya.
    """
    if shape < 0.50:
        return "🔴 Tidak Interchangeable (bentuk berbeda)", "#DC2626"

    has_name = name is not None
    if shape >= 0.85 and (not has_name or name >= 0.70):
        return "🟢 Sangat Mungkin Interchangeable", "#16A34A"
    if shape >= 0.75 and (not has_name or name >= 0.55):
        return "🟢 Kemungkinan Besar Interchangeable", "#16A34A"
    if shape >= 0.65:
        return "🟡 Mungkin Interchangeable (perlu verifikasi)", "#CA8A04"
    if shape >= 0.55:
        return "🟠 Belum Pasti — Verifikasi Mendalam", "#EA580C"
    return "🔴 Kemungkinan Tidak Interchangeable", "#DC2626"


# ─────────────────────────────────────────────────────────────
#  PUBLIC API
# ─────────────────────────────────────────────────────────────
def compare_parts(
    b1: bytes,
    b2: bytes,
    name1: str = "",
    name2: str = "",
) -> Dict:
    """
    Bandingkan 2 gambar + (opsional) 2 part name.
    Return dict berisi:
      shape_score, color_score, name_score, overall,
      verdict (string), color (hex),
      metrics (raw 0..1 per metrik), extras (info teknis)
    """
    img1 = _load(b1)
    img2 = _load(b2)

    ph1, ph2 = _phash(img1), _phash(img2)
    dh1, dh2 = _dhash(img1), _dhash(img2)
    ah1, ah2 = _ahash(img1), _ahash(img2)
    h1, h2   = _histogram(img1), _histogram(img2)
    mc1, mc2 = _mean_color(img1), _mean_color(img2)
    ed1, ed2 = _edge_density(img1), _edge_density(img2)

    raw = {
        "phash":        _hash_sim(ph1, ph2),
        "dhash":        _hash_sim(dh1, dh2),
        "ahash":        _hash_sim(ah1, ah2),
        "ssim":         _ssim_lite(img1, img2),
        "edge_density": 1.0 - abs(ed1 - ed2),
        "aspect":       _aspect_sim(img1.size, img2.size),
        "histogram":    _hist_sim(h1, h2),
        "mean_color":   _mean_color_sim(mc1, mc2),
    }

    shape = _shape_score(img1, img2, raw)
    color = _color_score(raw)

    name_present = bool((name1 or "").strip()) and bool((name2 or "").strip())
    name = name_similarity(name1, name2) if name_present else None

    # Bobot overall: shape gating utama, name penguat, color sangat kecil
    if name is not None:
        overall = 0.70 * shape + 0.25 * name + 0.05 * color
    else:
        overall = 0.95 * shape + 0.05 * color

    verdict, vcolor = interchange_verdict(shape, name, overall)

    return {
        "shape_score": float(shape),
        "color_score": float(color),
        "name_score":  None if name is None else float(name),
        "overall":     float(overall),
        "verdict":     verdict,
        "color":       vcolor,
        "metrics":     raw,
        "size1":       img1.size,
        "size2":       img2.size,
        "name1":       name1,
        "name2":       name2,
        "extras": {
            "edge1":         ed1,
            "edge2":         ed2,
            "mean_rgb1":     mc1.tolist(),
            "mean_rgb2":     mc2.tolist(),
            "hamming_phash": _hamming(ph1, ph2),
            "hamming_dhash": _hamming(dh1, dh2),
            "hamming_ahash": _hamming(ah1, ah2),
        },
    }


def best_match(
    images1: List[bytes],
    images2: List[bytes],
    name1: str = "",
    name2: str = "",
) -> Dict:
    """
    Bandingkan setiap kombinasi gambar antara 2 list.
    Pilih pasangan dengan SHAPE-score tertinggi (bentuk = penentu utama
    untuk interchange).
    """
    best: Dict | None = None
    pairs: List[Dict] = []
    for i, b1 in enumerate(images1):
        if not b1:
            continue
        for j, b2 in enumerate(images2):
            if not b2:
                continue
            try:
                r = compare_parts(b1, b2, name1=name1, name2=name2)
                r["i"] = i
                r["j"] = j
                pairs.append(r)
                if best is None or r["shape_score"] > best["shape_score"]:
                    best = r
            except Exception:
                continue
    return {"best": best, "pairs": pairs}
