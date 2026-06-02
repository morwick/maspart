"""
GUDANG CONFIG — Pemetaan akun ↔ gudang + logika "stok terdekat"
================================================================
Mengatur cabang mana yang dilihat tiap akun pada kolom Stok:

  • Akun cabang  → HANYA stok gudangnya sendiri.
  • Akun `mas` & role `admin` → SELURUH cabang (total stok).
  • Kalau stok gudang sendiri = 0, tampilkan stok dari cabang
    TERDEKAT yang masih punya stok (lintas pulau bila perlu).

Cara menyesuaikan:
  - Tambah/ubah akun di ACCOUNT_GUDANG (key = username lowercase,
    value = nama kolom gudang PERSIS seperti di Excel stok).
  - Akun yang boleh lihat semua: tambahkan ke SEE_ALL_ACCOUNTS.
  - Untuk akurasi "terdekat", isi koordinat gudang di GUDANG_COORDS.
    Gudang tanpa koordinat otomatis dianggap paling jauh (dipakai
    terakhir sebagai fallback).
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional


# ── Akun yang melihat SELURUH cabang (selain role admin) ─────────────
SEE_ALL_ACCOUNTS = {"mas"}


# ── Pemetaan username akun cabang → nama gudang (persis header Excel) ─
ACCOUNT_GUDANG: Dict[str, str] = {
    "jakarta":     "01.Jakarta",
    "balikpapan":  "03.Balikpapan",
    "palembang":   "04.Palembang",
    "makassar":    "05.Makasar",
    "jambi":       "08.TJP Jambi",
    "banjarmasin": "10.Banjarbaru",
    "muarateweh":  "11.Muara Teweh",
    "pontianak":   "18.Pontianak",
    "medan":       "23.Medan",
}


# ── Koordinat (lat, lon) gudang yang diketahui kotanya ───────────────
#    Dipakai untuk menghitung gudang "terdekat" saat stok sendiri = 0.
#    Gudang yang tidak ada di sini dianggap paling jauh.
GUDANG_COORDS: Dict[str, tuple] = {
    "01.Jakarta":     (-6.21, 106.85),
    "06.B80 H1":      (-6.21, 106.85),   # area Jakarta (asumsi)
    "07.B80 H2":      (-6.21, 106.85),   # area Jakarta (asumsi)
    "28.Ruko Stadion":(-6.21, 106.85),   # asumsi area Jakarta — sesuaikan bila perlu
    "02.Pekanbaru":   (0.51, 101.45),
    "09.Kerinci pku": (0.51, 101.45),    # area Pekanbaru (asumsi)
    "04.Palembang":   (-2.99, 104.76),
    "08.TJP Jambi":   (-1.61, 103.61),
    "23.Medan":       (3.59, 98.67),
    "03.Balikpapan":  (-1.27, 116.83),
    "10.Banjarbaru":  (-3.45, 114.84),
    "25. PT BJM":     (-3.32, 114.59),   # Banjarmasin
    "11.Muara Teweh": (-0.95, 114.89),
    "18.Pontianak":   (-0.02, 109.34),
    "05.Makasar":     (-5.13, 119.42),
    "26. BELOPA":     (-3.38, 120.36),
}


def _haversine(a: tuple, b: tuple) -> float:
    """Jarak (km) antara dua koordinat (lat, lon)."""
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def gudang_for_user(username: str, role: str) -> Optional[str]:
    """
    Tentukan cakupan gudang untuk user.

    Return:
      None  → lihat SEMUA gudang (total)        — role admin / akun di SEE_ALL.
      str   → nama gudang khusus user            — akun cabang.
      None  → user tak terpetakan → default lihat semua (aman, tak merusak).
    """
    u = (username or "").strip().lower()
    if (role or "").strip().lower() == "admin" or u in SEE_ALL_ACCOUNTS:
        return None
    return ACCOUNT_GUDANG.get(u)  # None bila tak terdaftar → lihat semua


def fallback_order(own_gudang: str, all_gudang: List[str]) -> List[str]:
    """
    Urutan gudang fallback (terdekat → terjauh) relatif ke own_gudang.
    Gudang tanpa koordinat ditaruh paling akhir (urutan kolom asli).
    """
    own = GUDANG_COORDS.get(own_gudang)
    known, unknown = [], []
    for g in all_gudang:
        if g == own_gudang:
            continue
        if own and g in GUDANG_COORDS:
            known.append((_haversine(own, GUDANG_COORDS[g]), g))
        else:
            unknown.append(g)
    known.sort(key=lambda t: t[0])
    return [g for _, g in known] + unknown


def gudang_label(gudang_name: str) -> str:
    """Nama gudang tanpa prefix nomor: '04.Palembang' → 'Palembang'."""
    if not gudang_name:
        return ""
    # Buang pola awal 'NN.' atau 'NN. ' (mis. '25. PT BJM' → 'PT BJM')
    return re.sub(r"^\s*\d+\s*\.\s*", "", gudang_name).strip() or gudang_name
