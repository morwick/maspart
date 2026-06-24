"""
Chat AI — DeepSeek Reasoner  (chat_ai.py)
==========================================
Tab 🤖 Chat AI untuk aplikasi Part Number Finder.

Peningkatan v4 — Model Upgrade ke DeepSeek Reasoner:
  • ✨ Model: deepseek-reasoner (R1) — model paling pintar DeepSeek, setara OpenAI o1
  • ✨ Chain-of-thought reasoning tersembunyi: AI berpikir lebih dalam sebelum menjawab
  • ✨ MAX_TOKENS dinaikkan ke 8000 untuk jawaban teknis yang jauh lebih detail
  • ✨ Streaming reasoning_content difilter — hanya jawaban final yang tampil ke user
  • ✨ Temperature dihapus (reasoner tidak mendukung temperature parameter)
  • Memory konteks percakapan: AI ingat PN/unit yang sudah dibahas sebelumnya
  • Smart PN cache per-sesi: lookup lokal tidak diulang untuk PN yang sama
  • Fuzzy match: jika PN tidak ditemukan, cari PN serupa (Levenshtein distance)
  • Multi-intent detection: satu pesan bisa punya >1 intent (harga+stok sekaligus)
  • Kurs CNY live: fetch kurs real-time dari ExchangeRate-API, fallback ke kurs lokal
  • Export chat history sebagai .txt (tombol download)
  • Smart retry dengan exponential backoff jika API rate-limited
  • Confidence scoring pada hasil pencarian nama (relevansi %)
  • Dynamic system prompt: mode AI berubah otomatis sesuai pola percakapan
  • Typing indicator lebih informatif (menampilkan apa yang sedang dikerjakan)
  • Guardrail: validasi PN format sebelum lookup ke SIMS (diperkuat)
  • Auto-expand multi-PN dari satu query dengan format "A, B, dan C"

Integrasi:
  • Dipanggil dari app.py:
      render_chat_ai_tab(excel_files, stok_cache, harga_lookup)
  • Menggunakan DeepSeek Reasoner API (model: deepseek-reasoner)
  • Opsional: sims_price_fetcher.get_sims_part_price(pn) untuk harga live
"""

from __future__ import annotations

import json
import os
import re
import time
import difflib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import streamlit as st

# ══════════════════════════════════════════════════════════════════════
#  KONFIGURASI
# ══════════════════════════════════════════════════════════════════════

DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL   = "deepseek-reasoner"   # ✨ Model R1 — paling pintar, chain-of-thought
MAX_HISTORY      = 30          # naik dari 20 → percakapan lebih panjang diingat
MAX_TOKENS       = 8000        # naik dari 3000 → reasoning + jawaban teknis lebih detail
TIMEOUT_SEC      = 120         # naik dari 50 → reasoner butuh waktu berpikir lebih lama
MAX_RETRIES      = 3           # retry otomatis jika 429
RETRY_BACKOFF    = [2, 5, 10]  # detik antar retry

# Kurs fallback jika live fetch gagal
KURS_CNY_IDR_FALLBACK = 2_200.0
KURS_CACHE_TTL_SEC    = 3600   # 1 jam

# API key TIDAK boleh di-hardcode. Diisi via st.secrets atau environment variable.
DEEPSEEK_API_KEY = ""


def _get_api_key() -> str | None:
    # Prioritas: st.secrets["deepseek"]["api_key"] → env DEEPSEEK_API_KEY → konstanta lokal
    try:
        key = st.secrets.get("deepseek", {}).get("api_key", "")
    except Exception:
        key = ""
    return key or os.environ.get("DEEPSEEK_API_KEY", "") or DEEPSEEK_API_KEY or None


# ── Optional: SIMS live price ────────────────────────────────────────
try:
    from sims_price_fetcher import get_sims_part_price
    SIMS_PRICE_ENABLED = True
except ImportError:
    SIMS_PRICE_ENABLED = False
    def get_sims_part_price(pn, **_):
        return None, "sims_price_fetcher tidak tersedia"


# ══════════════════════════════════════════════════════════════════════
#  LIVE KURS CNY → IDR
# ══════════════════════════════════════════════════════════════════════

_SS_KURS_CACHE    = "chat_ai_kurs_cache"
_SS_KURS_LAST_TS  = "chat_ai_kurs_ts"


def _get_live_kurs() -> float:
    """Ambil kurs CNY→IDR live. Cache 1 jam di session state."""
    now = time.time()
    cached_kurs = st.session_state.get(_SS_KURS_CACHE)
    cached_ts   = st.session_state.get(_SS_KURS_LAST_TS, 0)

    if cached_kurs and (now - cached_ts) < KURS_CACHE_TTL_SEC:
        return float(cached_kurs)

    try:
        resp = requests.get(
            "https://api.exchangerate-api.com/v4/latest/CNY",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        idr_per_cny = float(data["rates"]["IDR"])
        st.session_state[_SS_KURS_CACHE]   = idr_per_cny
        st.session_state[_SS_KURS_LAST_TS] = now
        return idr_per_cny
    except Exception:
        return float(cached_kurs or KURS_CNY_IDR_FALLBACK)


# ══════════════════════════════════════════════════════════════════════
#  INTENT DETECTION — sekarang multi-intent
# ══════════════════════════════════════════════════════════════════════

class Intent:
    HARGA          = "harga"
    STOK           = "stok"
    CARI           = "cari"
    COMPARE        = "compare"
    TEKNIS         = "teknis"
    KOMPATIBILITAS = "kompatibilitas"
    UNKNOWN        = "unknown"


_HARGA_RX = re.compile(
    r"\b(harga\w*|price|berapa|cost|biaya\w*|rate|kurs|jual|beli|tarif\w*|nilai\w*)\b", re.I
)
_STOK_RX = re.compile(
    r"\b(stok\w*|stock|tersedia|ada|ready|gudang\w*|qty|quantity|sisa\w*|ketersediaan|nyedia)\b", re.I
)
_COMPARE_RX = re.compile(
    r"\b(banding\w*|compare|vs\.?|versus|selisih|beda\w*|sama|interchangeable|apa\s+bedanya)\b", re.I
)
_TEKNIS_RX = re.compile(
    r"\b(fungsi\w*|cara\w*|pasang\w*|install|ganti\w*|torsi|spesifikasi|spec|ukuran\w*|interval|"
    r"penggantian|oli|kapasitas|letak|posisi\w*|kerja\w*|bagaimana|apa\s+itu|jelas\w*|"
    r"seberapa|kenapa|penyebab\w*|gejala\w*|masalah\w*|trouble|perbaiki|overhaul|prosedur|"
    r"langkah|tahap|cara\s+kerja|prinsip)\b", re.I
)
# Pertanyaan "cocok/bisa dipasang di unit X?" — beda dari COMPARE (yang butuh 2 PN)
# karena ini soal kompatibilitas 1 part ke 1 unit/model tertentu.
_KOMPAT_RX = re.compile(
    r"\b(bisa\s+(di)?pasang\w*|bisa\s+dipakai|cocok\w*\s+(untuk|di|ke|buat)|muat\w*\s+(di|ke)|"
    r"fit\s+(di|untuk)|interchangeable|gantiin|substitut\w*|substitusi\w*|pengganti\w*|"
    r"sama\s+(gak|ga|nggak|tidak)|work(s)?\s+(on|for)|compatible|kompatibel)\b", re.I
)
# Frasa negasi — dipakai supaya "ga usah harga, cek stok aja" tidak ikut ditandai HARGA.
_NEGASI_RX = re.compile(
    r"\b(jangan|tanpa|kecuali|ga(k)?\s+usah|nggak?\s+usah|tidak\s+usah|gausah)\b", re.I
)


def _split_clauses(query: str) -> list[str]:
    """Pecah query jadi klausa per koneksi (koma/titik/tapi/atau) supaya deteksi intent
    & negasi lebih akurat untuk pertanyaan gabungan/rumit."""
    parts = re.split(r"[,.;]|\btapi\b|\btetapi\b|\bnamun\b|\batau\b", query, flags=re.I)
    parts = [p.strip() for p in parts if p.strip()]
    return parts or [query]


def detect_intents(query: str, excel_files: list | None = None) -> list[str]:
    """Deteksi SEMUA intent dalam satu query (multi-intent), klausa per klausa supaya
    pertanyaan gabungan/rumit (mis. "harga AZ123, tapi ga usah cek stok, sama jelasin
    fungsinya juga") terbaca lebih akurat — termasuk menghindari intent yang dinegasikan."""
    pn_total = _extract_all_pn(query, excel_files)
    intents: list[str] = []

    for clause in _split_clauses(query):
        c = clause.lower()
        negated = bool(_NEGASI_RX.search(c))

        if _COMPARE_RX.search(c) and len(pn_total) >= 2 and Intent.COMPARE not in intents:
            intents.append(Intent.COMPARE)
        if _KOMPAT_RX.search(c) and not negated and Intent.KOMPATIBILITAS not in intents:
            intents.append(Intent.KOMPATIBILITAS)
        if _HARGA_RX.search(c) and not negated and Intent.HARGA not in intents:
            intents.append(Intent.HARGA)
        if _STOK_RX.search(c) and not negated and Intent.STOK not in intents:
            intents.append(Intent.STOK)
        if _TEKNIS_RX.search(c) and not pn_total and not negated and Intent.TEKNIS not in intents:
            intents.append(Intent.TEKNIS)

    if not intents:
        intents.append(Intent.UNKNOWN)
    return intents


def detect_intent(query: str, excel_files: list | None = None) -> str:
    """Backward-compat: kembalikan intent utama (pertama dari list)."""
    return detect_intents(query, excel_files)[0]


# ══════════════════════════════════════════════════════════════════════
#  DATA LOADER — muat mapping sinonim dari file JSON eksternal
#  Fallback ke hardcoded default hanya jika file JSON hilang/rusak.
# ══════════════════════════════════════════════════════════════════════

_DATA_DIR = Path(__file__).resolve().parent / "data" / "sinonim"


def _load_json_mapping(filename: str, default: dict) -> dict:
    """Muat mapping dari file JSON, fallback ke `default` jika gagal."""
    try:
        filepath = _DATA_DIR / filename
        if filepath.exists():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default


# ══════════════════════════════════════════════════════════════════════
#  SEMANTIC ALIAS MAP — "pemahaman manusia" antar nama komponen
#  Setiap grup adalah satu konsep yang sama, apapun sebutannya di lapangan.
#  Dipakai untuk: (1) memperluas keyword pencarian, (2) memberi konteks ke AI
#  agar bisa menjelaskan "X = Y = Z" saat menjawab.
# ══════════════════════════════════════════════════════════════════════

_SEMANTIC_ALIASES_FALLBACK: dict[str, list[str]] = {
    # ── Sistem Emisi / DEF / SCR ─────────────────────────────────────
    "UREA":           ["ADBLUE", "DEF", "DIESEL EXHAUST FLUID", "AUS32", "SCR FLUID",
                       "CAIRAN UREA", "AIR UREA", "CAIRAN EMISI"],
    "ADBLUE":         ["UREA", "DEF", "DIESEL EXHAUST FLUID", "AUS32", "SCR FLUID",
                       "CAIRAN UREA", "AIR UREA"],
    "DEF":            ["UREA", "ADBLUE", "DIESEL EXHAUST FLUID", "AUS32"],
    "TANGKI UREA":    ["UREA TANK", "ADBLUE TANK", "DEF TANK", "SCR TANK",
                       "TANGKI ADBLUE", "TANGKI DEF", "TANKI UREA"],
    "UREA TANK":      ["ADBLUE TANK", "DEF TANK", "SCR TANK", "TANGKI UREA",
                       "TANGKI ADBLUE", "UREA RESERVOIR"],
    "ADBLUE TANK":    ["UREA TANK", "DEF TANK", "SCR TANK", "TANGKI UREA"],
    "POMPA UREA":     ["UREA PUMP", "ADBLUE PUMP", "DEF PUMP", "SCR PUMP",
                       "DOSING PUMP", "DOSING MODULE"],
    "UREA PUMP":      ["ADBLUE PUMP", "DEF PUMP", "DOSING PUMP", "DOSING MODULE",
                       "SCR DOSING UNIT", "POMPA UREA", "POMPA ADBLUE"],
    "SCR":            ["SELECTIVE CATALYTIC REDUCTION", "CATALYTIC CONVERTER",
                       "AFTER TREATMENT", "AFTERTREATMENT", "KATALIS SCR"],
    "SCR CATALYST":   ["CATALYTIC CONVERTER", "AFTER TREATMENT", "AFTERTREATMENT SYSTEM",
                       "EXHAUST TREATMENT", "KATALIS", "KATALISATOR"],
    "NOZZLE UREA":    ["UREA INJECTOR", "DEF INJECTOR", "ADBLUE INJECTOR",
                       "DOSING VALVE", "INJECTION NOZZLE SCR"],
    "SENSOR UREA":    ["UREA LEVEL SENSOR", "DEF SENSOR", "ADBLUE SENSOR",
                       "NOX SENSOR", "SENSOR NOX", "QUALITY SENSOR"],

    # ── Bearing / Laher ──────────────────────────────────────────────
    "LAHER":          ["BEARING", "BALL BEARING", "ROLLER BEARING", "NEEDLE BEARING",
                       "TAPERED BEARING", "THRUST BEARING"],
    "BEARING":        ["LAHER", "BALL BEARING", "ROLLER BEARING", "NEEDLE BEARING",
                       "TAPERED ROLLER BEARING", "THRUST BEARING", "BANTALAN"],

    # ── Rem ──────────────────────────────────────────────────────────
    "KAMPAS REM":     ["BRAKE LINING", "BRAKE PAD", "FRICTION LINING", "BRAKE SHOE",
                       "LINING REM", "KANVAS REM", "KAMPAS"],
    "BRAKE PAD":      ["KAMPAS REM", "BRAKE LINING", "FRICTION LINING", "PAD REM",
                       "KANVAS REM"],
    "BRAKE LINING":   ["KAMPAS REM", "KANVAS REM", "FRICTION LINING", "BRAKE SHOE LINING",
                       "LINING"],
    "TROMOL":         ["BRAKE DRUM", "DRUM BRAKE", "DRUM REM", "TROMOL REM"],
    "BRAKE DRUM":     ["TROMOL", "TROMOL REM", "DRUM REM"],
    "CAKRAM":         ["BRAKE DISC", "DISC ROTOR", "ROTOR REM", "PIRINGAN REM"],
    "BRAKE DISC":     ["CAKRAM", "PIRINGAN REM", "DISC ROTOR", "ROTOR"],

    # ── Seal / Gasket / Paking ───────────────────────────────────────
    "PAKING":         ["GASKET", "SEAL", "O-RING", "SEALING", "PAKING MESIN",
                       "PAPAN PAKING", "HEAD GASKET"],
    "GASKET":         ["PAKING", "SEAL", "O-RING", "SEALING RING", "PERAPAT"],
    "SIL":            ["SEAL", "OIL SEAL", "SEALING", "PERAPAT OLI", "KARET PERAPAT",
                       "DUST SEAL", "LIP SEAL"],
    "OIL SEAL":       ["SIL", "SIL OLI", "SEAL OLI", "LIP SEAL", "ROTARY SEAL"],

    # ── Kopling / Clutch ─────────────────────────────────────────────
    "KOPLING":        ["CLUTCH", "CLUTCH DISC", "CLUTCH PLATE", "CLUTCH ASSY",
                       "KAMPAS KOPLING", "PLAT KOPLING", "DISC KOPLING"],
    "CLUTCH DISC":    ["KOPLING", "DISC KOPLING", "PLAT KOPLING", "KAMPAS KOPLING",
                       "CLUTCH PLATE", "FRICTION DISC"],
    "RELEASE BEARING":["LAHER KOPLING", "CLUTCH BEARING", "THROWOUT BEARING",
                       "BEARING KOPLING"],

    # ── Suspensi ─────────────────────────────────────────────────────
    "PER DAUN":       ["LEAF SPRING", "SPRING ASSEMBLY", "DAUN PER", "MAIN LEAF",
                       "SPRING LEAF"],
    "LEAF SPRING":    ["PER DAUN", "DAUN PER", "SPRING ASSEMBLY", "SEMI ELLIPTIC SPRING"],
    "SHOCK":          ["SHOCK ABSORBER", "DAMPER", "ABSORBER", "PEREDAM",
                       "SHOCKBREAKER", "SOKBREKER", "PEREDAM KEJUT"],
    "SHOCK ABSORBER": ["SHOCK", "SHOCKBREAKER", "SOKBREKER", "DAMPER", "PEREDAM KEJUT"],
    "BUSHING":        ["BUSH", "BUSHING KARET", "RUBBER BUSH", "SILENT BLOCK",
                       "KARET BUSHING", "BOS", "BUSHING ARM"],

    # ── Bahan Bakar ──────────────────────────────────────────────────
    "SOLAR":          ["DIESEL", "FUEL", "BBM", "BAHAN BAKAR SOLAR", "HIGH SPEED DIESEL"],
    "TANGKI SOLAR":   ["FUEL TANK", "DIESEL TANK", "TANGKI BBM", "TANGKI BAHAN BAKAR",
                       "FUEL RESERVOIR"],
    "FUEL TANK":      ["TANGKI SOLAR", "TANGKI BBM", "DIESEL TANK", "TANGKI BAHAN BAKAR"],
    "POMPA SOLAR":    ["FUEL PUMP", "INJECTION PUMP", "FEED PUMP", "LIFT PUMP",
                       "TRANSFER PUMP", "POMPA BAHAN BAKAR"],
    "INJECTION PUMP": ["POMPA SOLAR", "POMPA INJEKSI", "HIGH PRESSURE PUMP",
                       "POMPA TINGGI", "FUEL INJECTION PUMP", "IP"],
    "INJEKTOR":       ["INJECTOR", "FUEL INJECTOR", "INJECTION NOZZLE", "NOSEL",
                       "NOZZLE HOLDER", "SEMPROTAN BAHAN BAKAR"],
    "INJECTOR":       ["INJEKTOR", "FUEL INJECTOR", "INJECTION NOZZLE", "NOSEL",
                       "NOZZLE", "SPRAY NOZZLE"],
    "COMMON RAIL":    ["FUEL RAIL", "RAIL TEKANAN TINGGI", "HIGH PRESSURE RAIL",
                       "CR RAIL", "FUEL ACCUMULATOR"],
    "WATER SEPARATOR":["WATER SEDIMENTER", "FUEL WATER SEPARATOR", "PEMISAH AIR",
                       "FUEL FILTER SEPARATOR", "PRE FILTER"],
    "FILTER SOLAR":   ["FUEL FILTER", "DIESEL FILTER", "FUEL ELEMENT", "FILTER BBM",
                       "SARINGAN SOLAR", "SARINGAN BBM"],
    "FUEL FILTER":    ["FILTER SOLAR", "SARINGAN SOLAR", "DIESEL FILTER",
                       "FUEL ELEMENT", "FILTER BBM"],

    # ── Pelumasan ─────────────────────────────────────────────────────
    "FILTER OLI":     ["OIL FILTER", "LUBE FILTER", "SARINGAN OLI", "OIL ELEMENT",
                       "LUBE ELEMENT"],
    "OIL FILTER":     ["FILTER OLI", "SARINGAN OLI", "LUBE FILTER", "OIL ELEMENT"],
    "POMPA OLI":      ["OIL PUMP", "LUBRICATING PUMP", "LUBE PUMP", "GEAR OIL PUMP"],
    "OIL PUMP":       ["POMPA OLI", "LUBE PUMP", "LUBRICATING PUMP"],
    "OLI MESIN":      ["ENGINE OIL", "LUBRICATING OIL", "MOTOR OIL", "LUBE OIL"],

    # ── Pendingin / Cooling ───────────────────────────────────────────
    "RADIATOR":       ["COOLING RADIATOR", "HEAT EXCHANGER", "PENDINGIN MESIN",
                       "RADIATOR CORE", "RADIATOR ASSY"],
    "POMPA AIR":      ["WATER PUMP", "COOLANT PUMP", "COOLING PUMP",
                       "POMPA PENDINGIN", "WATER PUMP ASSY"],
    "WATER PUMP":     ["POMPA AIR", "POMPA PENDINGIN", "COOLANT PUMP"],
    "SELANG RADIATOR":["RADIATOR HOSE", "COOLANT HOSE", "UPPER HOSE", "LOWER HOSE",
                       "SELANG PENDINGIN", "WATER HOSE"],
    "RADIATOR HOSE":  ["SELANG RADIATOR", "COOLANT HOSE", "SELANG PENDINGIN",
                       "UPPER HOSE", "LOWER HOSE"],
    "THERMOSTAT":     ["TERMOSTAT", "KATUP TERMOSTAT", "COOLING THERMOSTAT",
                       "TEMPERATUR VALVE"],
    "KIPAS RADIATOR": ["COOLING FAN", "RADIATOR FAN", "FAN BLADE", "FAN ASSY",
                       "KIPAS PENDINGIN"],
    "COOLING FAN":    ["KIPAS RADIATOR", "KIPAS PENDINGIN", "FAN BLADE", "FAN ASSY"],
    "KOPLING FAN":    ["FAN CLUTCH", "VISCOUS FAN", "FAN COUPLING", "KOPLING KIPAS",
                       "VISCOUS CLUTCH"],
    "FAN CLUTCH":     ["KOPLING FAN", "VISCOUS FAN", "FAN COUPLING", "VISCOUS CLUTCH"],

    # ── Udara / Turbo ────────────────────────────────────────────────
    "FILTER UDARA":   ["AIR FILTER", "AIR ELEMENT", "AIR CLEANER", "SARINGAN UDARA",
                       "FILTER ANGIN", "AIR FILTER ELEMENT"],
    "AIR FILTER":     ["FILTER UDARA", "SARINGAN UDARA", "AIR ELEMENT", "AIR CLEANER"],
    "TURBO":          ["TURBOCHARGER", "TURBO ASSY", "TURBINE", "TURBO UNIT",
                       "SUPERCHARGER", "TURBO COMPRESSOR"],
    "TURBOCHARGER":   ["TURBO", "TURBO ASSY", "TURBINE HOUSING", "COMPRESSOR HOUSING"],
    "INTERCOOLER":    ["CHARGE AIR COOLER", "AIR COOLER", "AFTER COOLER",
                       "PENDINGIN UDARA", "CAC"],

    # ── Drivetrain / Transmisi ────────────────────────────────────────
    "GARDAN":         ["DIFFERENTIAL", "AXLE", "DRIVE AXLE", "REAR AXLE",
                       "DIFF ASSY", "FINAL DRIVE"],
    "DIFFERENTIAL":   ["GARDAN", "DIFF", "DIFF ASSY", "FINAL DRIVE", "DRIVE AXLE"],
    "TRANSMISI":      ["GEARBOX", "TRANSMISSION", "GEAR BOX", "PERSNELING",
                       "GEAR TRANSMISSION"],
    "GEARBOX":        ["TRANSMISI", "PERSNELING", "GEAR BOX", "TRANSMISSION ASSY"],
    "AS KOPEL":       ["PROPELLER SHAFT", "DRIVE SHAFT", "CARDAN SHAFT", "KOPEL",
                       "DRIVESHAFT", "UNIVERSAL SHAFT"],
    "PROPELLER SHAFT":["AS KOPEL", "KOPEL", "DRIVE SHAFT", "CARDAN SHAFT",
                       "DRIVESHAFT"],
    "CROSS JOINT":    ["UNIVERSAL JOINT", "U-JOINT", "UJOINT", "CROSS BEARING",
                       "SPIDER JOINT", "CARDAN JOINT"],
    "U-JOINT":        ["CROSS JOINT", "UNIVERSAL JOINT", "SPIDER", "CROSS BEARING"],
    "AS RODA":        ["AXLE SHAFT", "DRIVE SHAFT", "HALF SHAFT", "WHEEL SHAFT"],
    "AXLE SHAFT":     ["AS RODA", "HALF SHAFT", "WHEEL AXLE", "DRIVE AXLE SHAFT"],
    "FLYWHEEL":       ["RODA PENERUS", "RING GEAR FLYWHEEL", "FLYWHEEL ASSY",
                       "FLEX PLATE"],

    # ── Mesin / Engine ───────────────────────────────────────────────
    "PISTON":         ["TORAK", "PISTON ASSY", "ENGINE PISTON", "CYLINDER PISTON"],
    "RING PISTON":    ["PISTON RING", "COMPRESSION RING", "OIL RING", "RING TORAK"],
    "LINER":          ["CYLINDER LINER", "SLEEVE", "CYLINDER SLEEVE", "BORING"],
    "CYLINDER LINER": ["LINER", "SLEEVE", "CYLINDER SLEEVE", "BORING"],
    "BLOK MESIN":     ["ENGINE BLOCK", "CYLINDER BLOCK", "BLOK SILINDER",
                       "ENGINE CASE"],
    "KEPALA SILINDER":["CYLINDER HEAD", "HEAD ASSY", "KEPALA MESIN", "CYLINDER HEAD ASSY"],
    "CYLINDER HEAD":  ["KEPALA SILINDER", "KEPALA MESIN", "HEAD ASSY"],
    "KRUK AS":        ["CRANKSHAFT", "CRANK SHAFT", "AS ENGKOL", "POROS ENGKOL"],
    "CRANKSHAFT":     ["KRUK AS", "AS ENGKOL", "CRANK SHAFT", "POROS ENGKOL"],
    "METAL JALAN":    ["CONNECTING ROD BEARING", "CON ROD BEARING", "BIG END BEARING",
                       "ROD BEARING"],
    "METAL DUDUK":    ["MAIN BEARING", "CRANKSHAFT BEARING", "MAIN JOURNAL BEARING"],
    "NOK":            ["CAMSHAFT", "CAM", "CAMSHAFT ASSY", "NOKEN AS", "POROS NOK"],
    "CAMSHAFT":       ["NOK", "NOKEN AS", "POROS NOK", "CAM SHAFT"],
    "KLEP":           ["VALVE", "EXHAUST VALVE", "INTAKE VALVE", "ENGINE VALVE",
                       "KATUP MESIN"],
    "VALVE":          ["KLEP", "KATUP", "EXHAUST VALVE", "INTAKE VALVE"],
    "ROCKER ARM":     ["LENGAN ROCKER", "ROCKER ARM ASSY", "VALVE ROCKER"],
    "PUSH ROD":       ["BATANG PENEKAN", "PUSH ROD ASSY", "VALVE PUSH ROD"],
    "TIMING CHAIN":   ["RANTAI KETENG", "RANTAI TIMING", "CAM CHAIN", "TIMING GEAR"],
    "RANTAI KETENG":  ["TIMING CHAIN", "CAM CHAIN", "TIMING BELT", "RANTAI TIMING"],

    # ── Kemudi / Steering ────────────────────────────────────────────
    "STIR":           ["STEERING WHEEL", "STEERING", "KEMUDI", "RODA KEMUDI"],
    "STEERING WHEEL": ["STIR", "KEMUDI", "RODA KEMUDI"],
    "RACK STIR":      ["STEERING RACK", "RACK AND PINION", "POWER STEERING RACK"],
    "TIE ROD":        ["TIE ROD END", "TRACK ROD", "STEERING TIE ROD", "SAMBUNGAN STIR"],
    "BALL JOINT":     ["SAMBUNGAN BOLA", "JOINT BOLA", "LOWER BALL JOINT",
                       "UPPER BALL JOINT"],
    "POWER STEERING": ["POWER STEER", "HYDRAULIC STEERING", "POMPA STIR",
                       "STEERING PUMP", "PS PUMP"],

    # ── Kelistrikan ──────────────────────────────────────────────────
    "AKI":            ["BATTERY", "ACCU", "ACCUMULATOR", "BATERAI", "BATU AKI"],
    "BATTERY":        ["AKI", "ACCU", "ACCUMULATOR", "BATERAI"],
    "DINAMO AMPERE":  ["ALTERNATOR", "GENERATOR", "DYNAMO AMPERE", "CHARGING UNIT"],
    "ALTERNATOR":     ["DINAMO AMPERE", "DYNAMO AMPERE", "GENERATOR", "CHARGING SYSTEM"],
    "DINAMO STARTER": ["STARTER MOTOR", "STARTER ASSY", "DYNAMO STARTER",
                       "ELECTRIC STARTER"],
    "STARTER MOTOR":  ["DINAMO STARTER", "STARTER ASSY", "ELECTRIC MOTOR STARTER"],
    "ECU":            ["ENGINE CONTROL UNIT", "ENGINE CONTROL MODULE", "ECM",
                       "KOMPUTER MESIN", "MODUL KONTROL MESIN", "ENGINE COMPUTER"],
    "SENSOR NOX":     ["NOX SENSOR", "NITROGEN OXIDE SENSOR", "EXHAUST SENSOR",
                       "EMISI SENSOR"],
    "SENSOR OLI":     ["OIL PRESSURE SENSOR", "OIL SENSOR", "PENDETEKSI TEKANAN OLI"],
    "SENSOR SUHU":    ["TEMPERATURE SENSOR", "COOLANT TEMPERATURE SENSOR",
                       "WATER TEMP SENSOR", "CTS"],
    "SENSOR RPM":     ["SPEED SENSOR", "RPM SENSOR", "CRANKSHAFT POSITION SENSOR",
                       "CPS", "ENGINE SPEED SENSOR"],

    # ── AC / Pendingin Kabin ──────────────────────────────────────────
    "KOMPRESOR AC":   ["AC COMPRESSOR", "COMPRESSOR AC", "COOLING COMPRESSOR",
                       "KOMPRESOR PENDINGIN"],
    "AC COMPRESSOR":  ["KOMPRESOR AC", "COMPRESSOR AC", "COOLING COMPRESSOR"],
    "EVAPORATOR":     ["AC EVAPORATOR", "PENDINGIN KABIN", "COOLING COIL", "COIL AC"],
    "KONDENSOR":      ["CONDENSER", "AC CONDENSER", "COOLING CONDENSER"],
    "FREON":          ["REFRIGERANT", "AC GAS", "COOLANT GAS", "BAHAN PENDINGIN AC",
                       "R134A", "R404"],
    "FILTER AC":      ["CABIN FILTER", "CABIN AIR FILTER", "AC FILTER",
                       "FILTER KABIN", "SARINGAN KABIN"],
    "BLOWER AC":      ["BLOWER", "AC BLOWER", "FAN AC", "KABIN BLOWER",
                       "EVAPORATOR FAN"],

    # ── Bodi / Eksterior ─────────────────────────────────────────────
    "BAK":            ["DUMP BODY", "CARGO BODY", "TRUCK BODY", "BOX TRUK",
                       "VESSEL", "BIN"],
    "SPAKBOR":        ["FENDER", "MUDGUARD", "MUD FLAP", "PELINDUNG LUMPUR"],
    "FENDER":         ["SPAKBOR", "MUDGUARD", "PELINDUNG RODA"],
    "LAMPU DEPAN":    ["HEADLIGHT", "HEAD LAMP", "HEADLAMP", "LAMPU UTAMA"],
    "HEADLIGHT":      ["LAMPU DEPAN", "HEAD LAMP", "HEADLAMP"],
    "LAMPU BELAKANG": ["TAIL LAMP", "REAR LAMP", "STOP LAMP", "TAIL LIGHT"],
    "LAMPU SEIN":     ["TURN SIGNAL", "SIGNAL LAMP", "INDICATOR LAMP",
                       "LAMPU SEIN KIRI", "LAMPU SEIN KANAN"],
    "WIPER":          ["WINDSHIELD WIPER", "WIPER BLADE", "WIPER ARM",
                       "PEMBERSIH KACA", "PENGHAPUS KACA"],
    "SPION":          ["MIRROR", "REAR VIEW MIRROR", "SIDE MIRROR", "KACA SPION",
                       "OUTSIDE MIRROR"],
    "KACA DEPAN":     ["WINDSHIELD", "WINDSCREEN", "FRONT GLASS", "KACA WINDSHIELD"],
    "KAP MESIN":      ["HOOD", "BONNET", "ENGINE COVER", "ENGINE HOOD"],

    # ── Hidrolik / PTO ───────────────────────────────────────────────
    "HIDROLIK":       ["HYDRAULIC", "HYDRAULIC CYLINDER", "HYDRAULIC PUMP",
                       "HYDRAULIC SYSTEM", "SILINDER HIDROLIK"],
    "HYDRAULIC PUMP": ["POMPA HIDROLIK", "HIDROLIK PUMP", "HYDRAULIC GEAR PUMP"],
    "PTO":            ["POWER TAKE OFF", "PENGGERAK TAMBAHAN", "AUXILIARY DRIVE"],

    # ── Belt ─────────────────────────────────────────────────────────
    "TALI KIPAS":     ["FAN BELT", "V-BELT", "DRIVE BELT", "SERPENTINE BELT",
                       "BELT KIPAS"],
    "V-BELT":         ["FAN BELT", "TALI KIPAS", "DRIVE BELT", "POLY BELT"],
    "FAN BELT":       ["TALI KIPAS", "V-BELT", "DRIVE BELT", "ALTERNATOR BELT"],

    # ── Roda / Wheel ─────────────────────────────────────────────────
    "VELG":           ["WHEEL RIM", "RIM", "HUB", "DISC WHEEL", "PELEK"],
    "WHEEL RIM":      ["VELG", "PELEK", "RIM", "DISC WHEEL"],
    "BAN":            ["TYRE", "TIRE", "PNEUMATIC TYRE", "TUBELESS"],

    # ── Sistem Knalpot / Exhaust ──────────────────────────────────────
    "KNALPOT":        ["EXHAUST PIPE", "MUFFLER", "SILENCER", "EXHAUST SYSTEM",
                       "PIPA KNALPOT", "EXHAUST MUFFLER"],
    "EXHAUST PIPE":   ["KNALPOT", "PIPA BUANG", "MUFFLER", "EXHAUST TUBE"],
    "DPF":            ["DIESEL PARTICULATE FILTER", "PARTIKEL FILTER",
                       "FILTER PARTIKULAT", "SARINGAN PARTIKEL"],
    "EGR":            ["EXHAUST GAS RECIRCULATION", "EGR VALVE", "KATUP EGR",
                       "RECIRCULATION VALVE"],
    "EGR VALVE":      ["KATUP EGR", "EGR", "EXHAUST RECIRCULATION VALVE"],

    # ── Baut / Fastener ──────────────────────────────────────────────
    "MUR":            ["NUT", "HEX NUT", "WHEEL NUT", "LOCK NUT"],
    "BAUT":           ["BOLT", "SCREW", "HEX BOLT", "WHEEL BOLT", "STUD BOLT"],
    "STUD":           ["WHEEL STUD", "STUD BOLT", "THREADED ROD"],
}

# ── Muat dari JSON, fallback ke hardcoded default ────────────────────
SEMANTIC_ALIASES: dict[str, list[str]] = _load_json_mapping(
    "semantic_aliases.json", _SEMANTIC_ALIASES_FALLBACK
)

# ── Build reverse lookup: dari setiap alias → set keyword yang harus disearch ──
# Ini memungkinkan pencarian dua arah tanpa duplikasi entri.
def _build_alias_keywords(term: str) -> list[str]:
    """Kembalikan semua keyword yang relevan untuk satu istilah (inkl. alias)."""
    t_upper = term.upper().strip()
    result: list[str] = [t_upper]
    if t_upper in SEMANTIC_ALIASES:
        result.extend(SEMANTIC_ALIASES[t_upper])
    # Juga cek apakah term ini adalah VALUE dari alias lain (reverse lookup)
    for key, aliases in SEMANTIC_ALIASES.items():
        if t_upper in [a.upper() for a in aliases] and key not in result:
            result.append(key)
            result.extend(a for a in aliases if a not in result)
    # Deduplikasi sambil pertahankan urutan
    seen: set[str] = set()
    unique: list[str] = []
    for r in result:
        if r.upper() not in seen:
            seen.add(r.upper())
            unique.append(r)
    return unique


# ── Daftar SEMUA istilah alias (key + value), dipakai untuk scan dua-arah ──
# BUG LAMA: kode di _expand_query_with_ai() & _lookup_part_info() hanya cek
# apakah query mengandung salah satu KEY dari SEMANTIC_ALIASES. Tapi dict-nya
# asimetris — mis. "TANGKI ADBLUE" cuma muncul sebagai VALUE di bawah key
# "TANGKI UREA", bukan sebagai key sendiri. Akibatnya user yang ketik
# "tangki adblue" tidak pernah match apa pun selain key tunggal "ADBLUE"
# (yang cuma expand ke UREA/DEF/dll, BUKAN ke UREA TANK/ADBLUE TANK/DEF TANK)
# → pencarian part tangki gagal walau ada di katalog dengan nama "urea tank".
# Fix: scan terhadap SEMUA istilah (key maupun value), lalu expand pakai
# _build_alias_keywords() yang sudah bidirectional.
_ALL_ALIAS_TERMS: list[str] = sorted(
    {k.upper() for k in SEMANTIC_ALIASES} | {v.upper() for vs in SEMANTIC_ALIASES.values() for v in vs},
    key=len, reverse=True,
)


def _scan_alias_hits(query: str, q_lower: str, q_upper_tokens: list[str]) -> dict[str, list[str]]:
    """Cari semua istilah (key ATAU value) dari SEMANTIC_ALIASES yang disebut di query,
    return {istilah_ditemukan: [daftar sinonim lengkapnya]}."""
    hits: dict[str, list[str]] = {}
    for term in _ALL_ALIAS_TERMS:
        term_lower = term.lower()
        matched = (term_lower in q_lower) if " " in term else (term in q_upper_tokens)
        if matched and term not in hits:
            hits[term] = [v for v in _build_alias_keywords(term) if v.upper() != term]
    return hits


# ── Kata lapangan/bengkel → keyword katalog ──────────────────────────
# CATATAN: BENGKEL_DICT sekarang JUGA otomatis diperkaya dari SEMANTIC_ALIASES.
# Tambahkan sinonim baru cukup di SEMANTIC_ALIASES di atas — tidak perlu dua kali.
_BENGKEL_DICT_FALLBACK: dict[str, list[str]] = {
    # ── Istilah Indonesia → keyword teknis ───────────────────────────
    "laher":           ["BEARING", "BALL BEARING", "ROLLER BEARING"],
    "bearing laher":   ["BEARING"],
    "kopling":         ["CLUTCH", "CLUTCH DISC", "CLUTCH PLATE", "CLUTCH ASSY"],
    "gardan":          ["DIFFERENTIAL", "AXLE", "DRIVE AXLE", "DIFF ASSY"],
    "tromol":          ["BRAKE DRUM", "DRUM BRAKE"],
    "kampas rem":      ["BRAKE LINING", "BRAKE PAD", "FRICTION LINING"],
    "kampas":          ["LINING", "BRAKE LINING", "CLUTCH DISC"],
    "kanvas rem":      ["BRAKE LINING", "BRAKE PAD", "FRICTION LINING"],
    "klep":            ["VALVE", "EXHAUST VALVE", "INTAKE VALVE"],
    "paking":          ["GASKET", "SEAL", "O-RING"],
    "sil":             ["SEAL", "OIL SEAL", "SEALING"],
    "aki":             ["BATTERY", "ACCU", "ACCUMULATOR"],
    "accu":            ["BATTERY", "AKI", "ACCUMULATOR"],
    "dynamo ampere":   ["ALTERNATOR", "GENERATOR"],
    "dinamo ampere":   ["ALTERNATOR", "GENERATOR"],
    "alternator":      ["ALTERNATOR", "GENERATOR", "CHARGING"],
    "dynamo starter":  ["STARTER MOTOR", "STARTER ASSY"],
    "dinamo starter":  ["STARTER MOTOR", "STARTER ASSY"],
    "starter":         ["STARTER MOTOR", "STARTER ASSY"],
    "shock":           ["SHOCK ABSORBER", "DAMPER", "ABSORBER"],
    "shockbreaker":    ["SHOCK ABSORBER", "DAMPER", "ABSORBER"],
    "sokbreker":       ["SHOCK ABSORBER", "DAMPER", "ABSORBER"],
    "per":             ["SPRING", "LEAF SPRING", "SPRING ASSEMBLY"],
    "per daun":        ["LEAF SPRING", "SPRING ASSEMBLY"],
    "daun per":        ["LEAF SPRING", "SPRING ASSEMBLY"],
    "filter solar":    ["FUEL FILTER", "DIESEL FILTER", "FUEL ELEMENT"],
    "saringan solar":  ["FUEL FILTER", "DIESEL FILTER", "FUEL ELEMENT"],
    "filter bensin":   ["FUEL FILTER", "FUEL ELEMENT"],
    "filter oli":      ["OIL FILTER", "LUBE FILTER"],
    "saringan oli":    ["OIL FILTER", "LUBE FILTER", "OIL ELEMENT"],
    "filter udara":    ["AIR FILTER", "AIR ELEMENT", "AIR CLEANER"],
    "saringan udara":  ["AIR FILTER", "AIR ELEMENT", "AIR CLEANER"],
    "filter angin":    ["AIR FILTER", "AIR ELEMENT"],
    "pompa solar":     ["FUEL PUMP", "INJECTION PUMP", "FEED PUMP"],
    "pompa oli":       ["OIL PUMP", "LUBRICATING PUMP"],
    "pompa air":       ["WATER PUMP", "COOLANT PUMP"],
    "selang radiator": ["RADIATOR HOSE", "COOLANT HOSE"],
    "selang pendingin":["RADIATOR HOSE", "COOLANT HOSE", "WATER HOSE"],
    "kipas radiator":  ["FAN", "COOLING FAN", "RADIATOR FAN"],
    "kipas pendingin": ["COOLING FAN", "FAN BLADE", "FAN ASSY"],
    "tutup radiator":  ["RADIATOR CAP", "PRESSURE CAP"],
    "radiator":        ["RADIATOR", "COOLING SYSTEM"],
    "pendingin":       ["RADIATOR", "COOLER", "COOLING"],
    "busi":            ["SPARK PLUG", "GLOW PLUG"],
    "injektor":        ["INJECTOR", "FUEL INJECTOR", "INJECTION NOZZLE"],
    "injector":        ["FUEL INJECTOR", "INJECTOR", "INJECTION NOZZLE"],
    "nozel":           ["NOZZLE", "INJECTOR NOZZLE"],
    "nosel":           ["NOZZLE", "INJECTOR NOZZLE", "INJECTION NOZZLE"],
    "turbo":           ["TURBOCHARGER", "TURBO ASSY"],
    "intercooler":     ["INTERCOOLER", "CHARGE AIR COOLER"],
    "rem angin":       ["AIR BRAKE", "BRAKE VALVE", "BRAKE CHAMBER"],
    "silinder rem":    ["BRAKE CYLINDER", "WHEEL CYLINDER"],
    "master rem":      ["MASTER CYLINDER", "BRAKE MASTER"],
    "as roda":         ["AXLE SHAFT", "DRIVE SHAFT", "HALF SHAFT"],
    "as kopel":        ["PROPELLER SHAFT", "DRIVE SHAFT", "CARDAN SHAFT"],
    "kopel":           ["PROPELLER SHAFT", "DRIVE SHAFT"],
    "cross joint":     ["UNIVERSAL JOINT", "U-JOINT", "CROSS BEARING"],
    "u-joint":         ["UNIVERSAL JOINT", "U-JOINT"],
    "ujoint":          ["UNIVERSAL JOINT", "U-JOINT", "CROSS JOINT"],
    "roda gigi":       ["GEAR", "RING GEAR", "PINION GEAR"],
    "persneling":      ["GEARBOX", "TRANSMISSION", "GEAR SHIFT"],
    "transmisi":       ["TRANSMISSION", "GEARBOX"],
    "flywheel":        ["FLYWHEEL", "RING GEAR", "FLYWHEEL ASSY"],
    "roda penerus":    ["FLYWHEEL"],
    "ban":             ["TYRE", "TIRE"],
    "velg":            ["WHEEL RIM", "RIM", "HUB"],
    "pelek":           ["WHEEL RIM", "RIM", "DISC WHEEL"],
    "mur roda":        ["WHEEL NUT", "HUB NUT"],
    "baut roda":       ["WHEEL BOLT", "STUD"],
    "engsel":          ["HINGE"],
    "kunci pintu":     ["DOOR LOCK", "LATCH"],
    "kaca":            ["GLASS", "WINDSHIELD", "WINDSCREEN"],
    "kaca depan":      ["WINDSHIELD", "FRONT GLASS", "WINDSCREEN"],
    "spion":           ["MIRROR", "REAR VIEW MIRROR", "SIDE MIRROR"],
    "lampu":           ["LAMP", "LIGHT", "HEADLIGHT"],
    "lampu depan":     ["HEADLIGHT", "HEAD LAMP"],
    "lampu belakang":  ["TAIL LAMP", "REAR LAMP"],
    "wiper":           ["WIPER", "WINDSHIELD WIPER", "WIPER BLADE"],
    "klakson":         ["HORN", "AIR HORN"],
    "relay":           ["RELAY"],
    "sekring":         ["FUSE", "FUSE BOX"],
    "kabel":           ["WIRE", "WIRING", "CABLE", "HARNESS"],
    "rantai keteng":   ["TIMING CHAIN", "CAM CHAIN"],
    "timing":          ["TIMING", "TIMING CHAIN", "TIMING BELT"],
    "piston":          ["PISTON", "PISTON RING"],
    "torak":           ["PISTON", "PISTON ASSY"],
    "ring piston":     ["PISTON RING", "COMPRESSION RING"],
    "liner":           ["CYLINDER LINER", "SLEEVE"],
    "blok mesin":      ["ENGINE BLOCK", "CYLINDER BLOCK"],
    "kepala silinder": ["CYLINDER HEAD", "HEAD ASSY"],
    "kepala mesin":    ["CYLINDER HEAD", "HEAD ASSY"],
    "rocker arm":      ["ROCKER ARM", "ROCKER SHAFT"],
    "push rod":        ["PUSH ROD", "PUSHROD"],
    "nok":             ["CAMSHAFT", "CAM", "CAMSHAFT ASSY"],
    "noken as":        ["CAMSHAFT", "CAM SHAFT"],
    "kruk as":         ["CRANKSHAFT", "CRANK SHAFT"],
    "as engkol":       ["CRANKSHAFT", "CRANK SHAFT"],
    "metal jalan":     ["CONNECTING ROD BEARING", "CON ROD BEARING"],
    "metal duduk":     ["MAIN BEARING", "CRANKSHAFT BEARING"],
    "stir":            ["STEERING WHEEL", "STEERING"],
    "kemudi":          ["STEERING", "STEERING ASSY"],
    "rack stir":       ["STEERING RACK", "RACK AND PINION"],
    "tie rod":         ["TIE ROD", "TIE ROD END"],
    "ball joint":      ["BALL JOINT"],
    "lower arm":       ["LOWER ARM", "CONTROL ARM"],
    "stabilizer":      ["STABILIZER BAR", "SWAY BAR", "STABILIZER LINK"],
    "bak":             ["DUMP BODY", "CARGO BODY", "TRUCK BODY"],
    "hidrolik":        ["HYDRAULIC", "HYDRAULIC CYLINDER", "HYDRAULIC PUMP"],
    "silinder hidrolik":["HYDRAULIC CYLINDER"],
    "pompa hidrolik":  ["HYDRAULIC PUMP"],
    "pto":             ["PTO", "POWER TAKE OFF"],
    "throttle":        ["THROTTLE", "ACCELERATOR"],
    "gas":             ["THROTTLE", "ACCELERATOR CABLE"],
    "koil":            ["COIL", "IGNITION COIL"],
    "kondensor":       ["CONDENSER", "AC CONDENSER"],
    "kompressor ac":   ["AC COMPRESSOR", "COMPRESSOR"],
    "kompresor ac":    ["AC COMPRESSOR", "COMPRESSOR"],
    "freon":           ["REFRIGERANT", "AC GAS", "R134A"],
    "kompresor angin": ["AIR COMPRESSOR"],
    "cakram":          ["BRAKE DISC", "DISC ROTOR"],
    "piringan rem":    ["BRAKE DISC", "DISC ROTOR"],
    "minyak rem":      ["BRAKE FLUID"],
    "booster rem":     ["BRAKE BOOSTER", "VACUUM BOOSTER"],
    "vacuum booster":  ["VACUUM BOOSTER", "BRAKE BOOSTER"],
    "selang rem":      ["BRAKE HOSE"],
    "support mesin":   ["ENGINE MOUNT", "ENGINE MOUNTING"],
    "dudukan mesin":   ["ENGINE MOUNT", "ENGINE MOUNTING"],
    "bushing":         ["BUSHING", "BUSH"],
    "bos":             ["BUSHING", "BUSH"],
    "tangki solar":    ["FUEL TANK", "DIESEL TANK"],
    "tangki bbm":      ["FUEL TANK", "DIESEL TANK"],
    "tutup tangki":    ["FUEL TANK CAP"],
    "water sedimenter":["WATER SEPARATOR", "FUEL WATER SEPARATOR"],
    "common rail":     ["COMMON RAIL", "FUEL RAIL"],
    "pompa tinggi":    ["HIGH PRESSURE PUMP", "INJECTION PUMP"],
    "v-belt":          ["V-BELT", "FAN BELT", "DRIVE BELT"],
    "tali kipas":      ["FAN BELT", "V-BELT"],
    "kopling fan":     ["FAN CLUTCH", "VISCOUS FAN"],
    "fan clutch":      ["FAN CLUTCH", "VISCOUS FAN"],
    "filter ac":       ["CABIN FILTER", "AC FILTER"],
    "filter kabin":    ["CABIN FILTER", "CABIN AIR FILTER", "AC FILTER"],
    "blower ac":       ["BLOWER", "AC BLOWER"],
    "evaporator":      ["EVAPORATOR", "AC EVAPORATOR"],
    "sensor oli":      ["OIL PRESSURE SENSOR"],
    "sensor rpm":      ["RPM SENSOR", "SPEED SENSOR"],
    "sensor suhu":     ["TEMPERATURE SENSOR", "COOLANT SENSOR"],
    "ecu":             ["ECU", "ENGINE CONTROL UNIT", "ECM"],
    "ecm":             ["ECM", "ENGINE CONTROL MODULE", "ECU"],
    "jok":             ["SEAT", "DRIVER SEAT"],
    "kursi sopir":     ["DRIVER SEAT", "SEAT"],
    "sabuk pengaman":  ["SEAT BELT"],
    "spakbor":         ["FENDER", "MUDGUARD"],
    "bumper":          ["BUMPER"],
    "kap mesin":       ["HOOD", "BONNET", "ENGINE COVER"],
    "lampu sein":      ["TURN SIGNAL", "SIGNAL LAMP"],
    "lampu rem":       ["BRAKE LIGHT", "STOP LAMP"],
    "sakelar":         ["SWITCH"],
    "saklar":          ["SWITCH"],
    "knalpot":         ["EXHAUST PIPE", "MUFFLER", "SILENCER"],
    "pipa buang":      ["EXHAUST PIPE", "EXHAUST TUBE"],
    "thermostat":      ["THERMOSTAT", "COOLING THERMOSTAT"],
    "termostat":       ["THERMOSTAT", "COOLING THERMOSTAT"],
    "power steering":  ["POWER STEERING", "STEERING PUMP", "HYDRAULIC STEERING"],
    "pompa stir":      ["STEERING PUMP", "POWER STEERING PUMP"],
    # ── DEF / Urea / AdBlue — contoh utama sistem alias ─────────────
    "urea":            ["UREA TANK", "UREA PUMP", "DEF", "ADBLUE", "SCR", "AUS32"],
    "adblue":          ["UREA", "UREA TANK", "DEF", "AUS32", "SCR FLUID"],
    "def":             ["DEF TANK", "DIESEL EXHAUST FLUID", "UREA", "ADBLUE", "AUS32"],
    "tangki urea":     ["UREA TANK", "ADBLUE TANK", "DEF TANK", "SCR TANK"],
    "tangki adblue":   ["UREA TANK", "ADBLUE TANK", "DEF TANK", "SCR TANK"],
    "tanki urea":      ["UREA TANK", "ADBLUE TANK", "DEF TANK"],
    "pompa urea":      ["UREA PUMP", "ADBLUE PUMP", "DEF PUMP", "DOSING PUMP"],
    "pompa adblue":    ["UREA PUMP", "ADBLUE PUMP", "DOSING PUMP", "DOSING MODULE"],
    "scr":             ["SCR CATALYST", "AFTER TREATMENT", "AFTERTREATMENT",
                        "SELECTIVE CATALYTIC REDUCTION", "UREA SYSTEM"],
    "katalis":         ["SCR CATALYST", "CATALYTIC CONVERTER", "AFTER TREATMENT"],
    "dpf":             ["DIESEL PARTICULATE FILTER", "PARTICULATE FILTER"],
    "egr":             ["EGR VALVE", "EXHAUST GAS RECIRCULATION"],
    "sensor nox":      ["NOX SENSOR", "NITROGEN OXIDE SENSOR", "EXHAUST SENSOR"],
}

# ── Muat dari JSON, fallback ke hardcoded default ────────────────────
BENGKEL_DICT: dict[str, list[str]] = _load_json_mapping(
    "bengkel_dict.json", _BENGKEL_DICT_FALLBACK
)


_unit_token_cache: dict[int, set[str]] = {}
_real_pn_cache: dict[int, set[str]] = {}


def _known_unit_tokens(excel_files: list | None) -> set[str]:
    """Kumpulkan semua signature token nama unit/tipe (mis. 'NX371', 'NX280', '6X4')
    dari semua file katalog. Dipakai supaya token semacam ini tidak salah dianggap PN.
    Di-cache per-list (by id) supaya tidak diulang setiap pesan."""
    if not excel_files:
        return set()
    key = id(excel_files)
    cached = _unit_token_cache.get(key)
    if cached is not None:
        return cached
    tokens: set[str] = set()
    for fi in excel_files:
        sn = fi.get("simple_name", "")
        if not sn:
            continue
        u_tokens = re.findall(r"[A-Z0-9]+", sn.upper())
        tokens.update(_unit_signature_tokens(u_tokens))
    _unit_token_cache[key] = tokens
    return tokens


def _known_pn_set(excel_files: list | None) -> set[str]:
    """Kumpulkan semua PN asli yang benar-benar ada di index katalog (semua file).
    Dipakai sebagai 'penyelamat' supaya PN asli yang kebetulan mirip nama unit
    (mis. ada PN literal 'NX371') tetap dianggap PN, bukan di-exclude membabi buta."""
    if not excel_files:
        return set()
    key = id(excel_files)
    cached = _real_pn_cache.get(key)
    if cached is not None:
        return cached
    pns: set[str] = set()
    for fi in excel_files:
        pns.update(fi.get("part_number_index", {}).keys())
    _real_pn_cache[key] = pns
    return pns


def _extract_all_pn(query: str, excel_files: list | None = None) -> list[str]:
    """Ekstrak semua kandidat Part Number dari query (+ format koma-pisah).

    excel_files (opsional): kalau diisi, token yang justru cocok dengan nama/tipe
    unit di katalog (mis. "NX371", "NX280" — bukan PN, tapi nama model truk) TIDAK
    akan dianggap kandidat PN, KECUALI token itu memang benar-benar terdaftar
    sebagai PN asli di salah satu file (jadi tidak salah exclude PN asli yang
    kebetulan mirip nama unit).
    """
    q_upper = query.upper()
    candidates = re.findall(r'[A-Z0-9]{4,}(?:[-/][A-Z0-9]+)*', q_upper)
    candidates = [p for p in candidates if re.search(r'\d{3,}', p)]
    if not candidates or not excel_files:
        return candidates

    unit_tokens = _known_unit_tokens(excel_files)
    if not unit_tokens:
        return candidates

    real_pns = _known_pn_set(excel_files)
    return [p for p in candidates if p not in unit_tokens or p in real_pns]


def _normalize_query(query: str) -> str:
    """Ganti istilah bengkel dengan keyword teknis standar."""
    q_lower = query.lower()
    for term, replacements in BENGKEL_DICT.items():
        if term in q_lower:
            q_lower = q_lower.replace(term, f"{term} ({' OR '.join(replacements)})")
    return q_lower


_BENGKEL_SINGLE_KEYS = [k for k in BENGKEL_DICT if " " not in k and "-" not in k and len(k) >= 4]


def _fuzzy_bengkel_match(q_lower: str) -> list[str]:
    """Toleransi typo untuk istilah bengkel satu kata, mis. 'kampaz' / 'bearng' / 'turbo0'
    tetap dikenali sebagai 'kampas' / 'bearing' / 'turbo'. Hanya dipakai sebagai fallback
    kalau BENGKEL_DICT tidak match exact, jadi tidak menambah false-positive untuk istilah
    yang sudah benar penulisannya."""
    words = re.findall(r"[a-z]+", q_lower)
    matches: list[str] = []
    for w in words:
        if len(w) < 4 or w in BENGKEL_DICT:
            continue
        close = difflib.get_close_matches(w, _BENGKEL_SINGLE_KEYS, n=1, cutoff=0.8)
        if close:
            matches.extend(BENGKEL_DICT[close[0]])
    return matches


# Stop-words generik untuk pencarian by-nama — dipakai juga untuk mendeteksi sisa
# keyword "tambahan" pada query gabungan (PN + permintaan cari nama sekaligus).
_NAME_SEARCH_STOPWORDS = {
    "YANG", "UNTUK", "DARI", "DAN", "APA", "PART", "HARGA", "STOK",
    "BERAPA", "CARI", "CARIKAN", "CEK", "INFO", "DATA", "LIST",
    "SEMUA", "ADA", "INI", "ITU", "PADA", "DI", "KE", "DENGAN",
    "MANA", "SAJA", "AJA", "MAU", "BUTUH", "PERLU", "KASIH", "TOLONG",
    "NOMOR", "KODE", "TYPE", "TIPE", "UNIT", "TRUK", "MOBIL",
    "ATAU", "JUGA", "BISA", "KALAU", "NAMANYA", "SAMA", "SEKALIAN",
    "TERUS", "LALU", "TADI", "DONG", "YA", "NYA", "GAK", "GA", "NGGAK",
}


def _has_additional_name_request(query: str, pn_candidates: list[str]) -> str | None:
    """Deteksi apakah query gabungan (sudah ada PN) MASIH menyiratkan permintaan cari-by-nama
    terpisah, mis. "harga AZ123, terus carikan juga semua filter oli HOWO A7". Kalau ya,
    kembalikan sisa teks (tanpa PN & stop-word) untuk dipakai sebagai keyword pencarian."""
    q_upper = query.upper()
    for pn in pn_candidates:
        q_upper = q_upper.replace(pn.upper(), " ")
    raw_tokens = re.findall(r"[A-Z]{3,}", q_upper)
    meaningful = [t for t in raw_tokens if t not in _NAME_SEARCH_STOPWORDS]
    if len(meaningful) >= 1:
        return " ".join(meaningful)
    return None


# Kata generik di nama tipe unit yang TIDAK boleh dianggap "signature" saat mendeteksi
# unit yang disebut user (mis. "HOWO" muncul di hampir semua file, jadi bukan pembeda).
_UNIT_NAME_GENERIC_WORDS = {
    "HOWO", "SINOTRUK", "CNHTC", "TRUCK", "TRUK", "EURO", "UNIT",
    "SERIES", "TYPE", "TIPE", "CATALOG", "KATALOG", "PART", "SPAREPART",
    "SITRAK",   # muncul di semua file SITRAK → bukan pembeda antar varian
}


def _unit_signature_tokens(u_tokens: list[str]) -> list[str]:
    """Token dari nama tipe unit yang benar-benar membedakan unit itu dari unit lain
    (mengandung digit, atau kata non-generik cukup panjang)."""
    return [
        t for t in u_tokens
        if t not in _UNIT_NAME_GENERIC_WORDS and (any(c.isdigit() for c in t) or len(t) >= 3)
    ]


def _primary_model_token(sig_tokens: list[str]) -> str | None:
    """Pilih token yang paling mungkin jadi 'kode model' utama suatu unit.

    Urutan prioritas:
    1. Token campuran huruf+angka yang paling panjang (mis. "LZZ7CLXB", "NX280") —
       ini ciri khas kode seri/chassis yang paling spesifik.
    2. Token pure-huruf (min 4 karakter) yang paling panjang (mis. "SITRAK" — sudah
       difilter via _UNIT_NAME_GENERIC_WORDS jika generik).
    3. Token pure-angka (mis. "540", "480") — dipakai sebagai last resort karena
       angka seperti "540" bisa jadi kode daya tapi juga jadi pembeda seri utama.

    Token generik drivetrain ("6X4", "8X4") sengaja tidak dipilih sebagai primary
    karena bukan kode model — mereka tetap jadi secondary token untuk penyempitan.
    Drivetrain token dikenali sebagai: hanya 3-4 karakter, pola X di tengah angka.
    """
    if not sig_tokens:
        return None

    # Buang token yang terlihat seperti drivetrain ("6X4", "4X2", "8X4")
    drivetrain_rx = re.compile(r'^\d+X\d+$')
    non_drivetrain = [t for t in sig_tokens if not drivetrain_rx.match(t)]
    pool = non_drivetrain or sig_tokens

    # Prioritas 1: campuran huruf+angka (chassis/seri code)
    mixed = [t for t in pool if any(c.isalpha() for c in t) and any(c.isdigit() for c in t)]
    if mixed:
        return max(mixed, key=len)

    # Prioritas 2: pure-huruf cukup panjang
    alpha_only = [t for t in pool if t.isalpha() and len(t) >= 4]
    if alpha_only:
        return max(alpha_only, key=len)

    # Prioritas 3: pure-angka (mis. "540") — last resort
    return max(pool, key=len)


def _detect_units_in_query(query: str, excel_files: list | None, min_ratio: float = 0.5) -> list[str]:
    """Deteksi tipe unit (nama file katalog) yang disebut user di query, termasuk kalau
    cuma disebut sebagian atau ada typo ringan (mis. 'a7' tetap match 'HOWO A7 EURO 2',
    'nx28o' tetap match 'NX280' karena toleransi fuzzy).

    Kode internal/chassis dalam kurung di nama file (mis. "NX371 6X4 (LZZDDLSD)") DIBUANG
    dulu sebelum dipakai — kode semacam itu praktis tidak pernah diketik user.

    PENTING: deteksi sekarang berbasis 'kode model utama' (mis. "NX280"), bukan rasio
    semua token. Kalau katalog punya beberapa varian dengan kode model yang sama tapi
    drivetrain/konfigurasi berbeda (mis. "NX280 6X4", "NX280 8X4", "NX280 4X2"), dan user
    cuma sebut kode modelnya saja tanpa drivetrain spesifik, SEMUA varian itu ikut
    terdeteksi — bukan cuma satu yang kebetulan jumlah token-nya paling sedikit. Kalau
    user JUGA menyebut drivetrain/atribut tambahan secara eksplisit, baru dipersempit ke
    varian yang atribut tambahannya benar-benar disebut."""
    if not excel_files or not query.strip():
        return []
    q_tokens = re.findall(r"[A-Z0-9]+", query.upper())
    if not q_tokens:
        return []

    all_units = list({fi.get("simple_name", "") for fi in excel_files if fi.get("simple_name")})
    matched: list[tuple[str, list[str], list[str]]] = []  # (unit, secondary_tokens, secondary_hits)

    for u in all_units:
        u_main = re.sub(r"\([^)]*\)", "", u)  # buang kode chassis/internal dlm kurung
        u_tokens = re.findall(r"[A-Z0-9]+", u_main.upper())
        sig_tokens = _unit_signature_tokens(u_tokens)
        if not sig_tokens:
            # fallback: kalau bagian utama nama tidak punya token khas, baru pakai
            # nama lengkap (termasuk kode dalam kurung) supaya tidak skip total.
            u_tokens_full = re.findall(r"[A-Z0-9]+", u.upper())
            sig_tokens = _unit_signature_tokens(u_tokens_full)
            if not sig_tokens:
                continue

        primary = _primary_model_token(sig_tokens)
        if not primary:
            continue

        # Untuk token campuran huruf+angka atau panjang ≥ 4: izinkan fuzzy match ringan.
        # Untuk token pure-angka pendek (mis. "540", "480"): WAJIB exact match supaya
        # "injector untuk sitrak 540" tidak salah cocok ke unit lain yang kebetulan ada
        # token berakhiran angka mirip (false positive dari fuzzy).
        if primary.isdigit() or len(primary) <= 3:
            primary_hit = primary in q_tokens
        else:
            primary_hit = primary in q_tokens or (
                len(primary) >= 3 and bool(difflib.get_close_matches(primary, q_tokens, n=1, cutoff=0.82))
            )
        if not primary_hit:
            continue

        secondary = [t for t in sig_tokens if t != primary]
        secondary_hits = [t for t in secondary if t in q_tokens]
        matched.append((u, secondary, secondary_hits))

    if not matched:
        return []

    # Kalau user juga menyebut atribut tambahan (mis. drivetrain "6X4") yang persis cocok
    # ke sebagian varian, persempit ke varian itu saja. Kalau tidak ada atribut tambahan
    # yang disebut sama sekali, kembalikan SEMUA varian dengan kode model yang sama.
    if any(hits for _, _, hits in matched):
        narrowed = [u for u, secondary, hits in matched if not secondary or hits]
        if narrowed:
            return narrowed
    return [u for u, _, _ in matched]


# ══════════════════════════════════════════════════════════════════════
#  SMART PN CACHE (per sesi)
# ══════════════════════════════════════════════════════════════════════

_SS_PN_CACHE = "chat_ai_pn_cache"


def _pn_cache_get(pn: str) -> dict | None:
    cache = st.session_state.get(_SS_PN_CACHE, {})
    entry = cache.get(pn.upper())
    if entry and (time.time() - entry.get("_ts", 0)) < 1800:  # 30 menit TTL
        return entry
    return None


def _pn_cache_set(pn: str, data: dict):
    cache = st.session_state.get(_SS_PN_CACHE, {})
    data["_ts"] = time.time()
    cache[pn.upper()] = data
    st.session_state[_SS_PN_CACHE] = cache


# ══════════════════════════════════════════════════════════════════════
#  FUZZY PN MATCH — saran PN serupa jika tidak ditemukan
# ══════════════════════════════════════════════════════════════════════

def _fuzzy_similar_pn(
    pn: str,
    excel_files: list | None,
    n: int = 5,
    cutoff: float = 0.65,
) -> list[str]:
    """Cari PN serupa menggunakan SequenceMatcher (difflib)."""
    if not excel_files:
        return []
    all_pns: list[str] = []
    for fi in excel_files:
        idx_dict = fi.get("part_number_index", {})
        all_pns.extend(idx_dict.keys())

    # batasi ke 50k PN untuk performa
    all_pns = list(set(all_pns))[:50_000]
    matches = difflib.get_close_matches(pn.upper(), all_pns, n=n, cutoff=cutoff)
    return matches


# ══════════════════════════════════════════════════════════════════════
#  CONVERSATION CONTEXT MEMORY
# ══════════════════════════════════════════════════════════════════════

_SS_CTX_MEMORY = "chat_ai_context_memory"


def _update_context_memory(user_msg: str, assistant_reply: str, excel_files: list | None = None):
    """Catat PN dan unit yang disebutkan dalam percakapan terakhir."""
    mem = st.session_state.get(_SS_CTX_MEMORY, {
        "recent_pns": [],
        "recent_units": [],
        "session_intents": [],
        "turn_count": 0,
    })

    pns_found = _extract_all_pn(user_msg, excel_files)
    if pns_found:
        for pn in pns_found:
            if pn not in mem["recent_pns"]:
                mem["recent_pns"].insert(0, pn)
        mem["recent_pns"] = mem["recent_pns"][:10]  # simpan 10 terakhir

    # Deteksi unit dari pesan secara dinamis berdasarkan katalog yang sedang dimuat
    # (bukan daftar hardcode terbatas) — otomatis mendukung semua unit, termasuk
    # yang sebelumnya terlewat seperti "NX371".
    units_found = _detect_units_in_query(user_msg + " " + assistant_reply, excel_files)
    for u in units_found:
        if u not in mem["recent_units"]:
            mem["recent_units"].insert(0, u)
    mem["recent_units"] = mem["recent_units"][:5]

    intent = detect_intent(user_msg, excel_files)
    mem["session_intents"].append(intent)
    mem["session_intents"] = mem["session_intents"][-20:]
    mem["turn_count"] = mem.get("turn_count", 0) + 1


    st.session_state[_SS_CTX_MEMORY] = mem


def _get_context_memory() -> dict:
    return st.session_state.get(_SS_CTX_MEMORY, {
        "recent_pns": [],
        "recent_units": [],
        "session_intents": [],
        "turn_count": 0,
    })


def _build_memory_hint() -> str:
    """Ringkas memori konteks untuk dimasukkan ke system prompt."""
    mem = _get_context_memory()
    parts = []
    if mem["recent_pns"]:
        parts.append(f"PN yang baru dibahas: {', '.join(mem['recent_pns'][:5])}")
    if mem["recent_units"]:
        parts.append(f"Unit/model yang disebut: {', '.join(mem['recent_units'])}")
    if mem["turn_count"] > 0:
        parts.append(f"Total pertanyaan sesi ini: {mem['turn_count']}")
    return "\n".join(parts) if parts else ""


def _dominant_mode() -> str:
    """Tentukan mode dominan dari pola pertanyaan sesi ini."""
    mem = _get_context_memory()
    intents = mem.get("session_intents", [])
    if not intents:
        return Intent.UNKNOWN
    from collections import Counter
    count = Counter(intents)
    return count.most_common(1)[0][0]


# ══════════════════════════════════════════════════════════════════════
#  SYSTEM PROMPT (dinamis berdasarkan mode & memori)
# ══════════════════════════════════════════════════════════════════════

def _build_system_prompt(
    excel_files: list | None,
    stok_cache:  dict | None,
    harga_lookup: dict | None,
    intents: list[str] | None = None,
    kurs: float = KURS_CNY_IDR_FALLBACK,
) -> str:
    intents = intents or [Intent.UNKNOWN]
    n_parts = sum(len(fi.get("dataframe", [])) for fi in (excel_files or []))
    unit_labels = list({
        fi.get("simple_name", "") for fi in (excel_files or [])
        if fi.get("simple_name", "")
    })[:20]
    n_stok  = len(stok_cache or {})
    n_harga = len(harga_lookup or {})
    # NB: unit_labels diambil dari nama file Excel (tanpa ekstensi). Di
    # database ini, 1 file Excel = 1 katalog part untuk 1 tipe/model unit —
    # nama file SENGAJA dibuat sama dengan nama tipe unitnya. Jadi nilai
    # di unit_labels harus dianggap sebagai nama tipe unit/model kendaraan,
    # bukan sekadar nama file komputer biasa.
    unit_list_str = ", ".join(unit_labels) if unit_labels else "—"

    # Bangun guidance per intent (multi-intent!)
    guidance_blocks: list[str] = []
    if Intent.COMPARE in intents:
        guidance_blocks.append(
            "[MODE: Compare] Bandingkan 2 part side-by-side dalam tabel Markdown. "
            "Kolom: PN | Nama | Stok | Harga IDR | Harga CNY. "
            "Tambahkan analisis perbedaan singkat dan kesimpulan mana yang lebih cocok."
        )
    if Intent.HARGA in intents:
        guidance_blocks.append(
            f"[MODE: Harga] Fokus jawab harga. Format: 'Rp X.XXX.XXX (lokal)' atau "
            f"'¥ X.XX (~Rp X.XXX.XXX @ kurs {kurs:,.0f})'. "
            "Jika ada keduanya, tampilkan keduanya dan hitung selisihnya."
        )
    if Intent.STOK in intents:
        guidance_blocks.append(
            "[MODE: Stok] Fokus ketersediaan. Sebutkan qty total dan per-gudang jika ada. "
            "Jika stok 0, sarankan alternatif atau konfirmasi ke tim sales."
        )
    if Intent.TEKNIS in intents:
        guidance_blocks.append(
            "[MODE: Teknis] Jawab dari knowledge HOWO/Sinotruk/CNHTC. "
            "Sertakan: fungsi komponen, interval penggantian (jika relevan), "
            "posisi di kendaraan, gejala kerusakan umum, dan tips perawatan."
        )
    if Intent.KOMPATIBILITAS in intents:
        guidance_blocks.append(
            "[MODE: Kompatibilitas] User tanya apakah part cocok/bisa dipasang di unit tertentu. "
            "WAJIB cek blok [UNIT DIMINTA] dan [PERHATIAN] di bawah ini. Kalau unit yang diminta ADA "
            "di daftar Tipe Unit pada [DATA]/[HASIL PENCARIAN] → jawab tegas 'Ya, cocok untuk [unit]'. "
            "Kalau ada [PERHATIAN] (PN tidak terdaftar untuk unit itu) → jawab tegas 'Tidak terdaftar "
            "untuk unit itu', sebutkan unit yang sebenarnya terdaftar, dan sarankan konfirmasi ke "
            "teknisi/sales sebelum dipasang paksa. Jangan menyimpulkan 'cocok' hanya dari nama part "
            "yang mirip."
        )
    if Intent.CARI in intents or Intent.UNKNOWN in intents:
        guidance_blocks.append(
            "[MODE: Cari] Tampilkan hasil pencarian dalam tabel: | PN | Nama Part | Tipe Unit | Stok | Harga |. "
            "Urutkan dari yang paling relevan (stok > 0 duluan)."
        )

    intent_guidance = "\n".join(guidance_blocks)

    # Memori konteks sesi
    mem_hint = _build_memory_hint()
    mem_section = f"\n=== MEMORI SESI ===\n{mem_hint}" if mem_hint else ""

    # Mode dominan → pengaruhi nada jawaban
    dominant = _dominant_mode()
    nada_extra = ""
    if dominant == Intent.TEKNIS:
        nada_extra = " Pengguna sesi ini banyak tanya teknis — berikan penjelasan detail dan edukatif."
    elif dominant == Intent.HARGA:
        nada_extra = " Pengguna sesi ini fokus harga — langsung ke angka, hemat kata."

    return f"""Kamu adalah HIRO — Asisten AI Sparepart Truk HOWO/Sinotruk milik PT MAS Automobil Sejahtera, dealer resmi truk HOWO di Indonesia.

=== IDENTITAS ===
Nama: HIRO v4 (HOWO Intelligent Repair & Order assistant)
Model: DeepSeek Reasoner (R1) — kemampuan reasoning tingkat tinggi
Bahasa: Indonesia (lapangan & profesional)
Nada: Ramah, cepat, to-the-point seperti mekanik senior yang berpengalaman.{nada_extra}
{mem_section}

=== DATABASE AKTIF ===
• Katalog part    : {n_parts:,} baris dari {len(excel_files or [])} file Excel
• Tipe unit       : {unit_list_str}
• Data stok       : {n_stok:,} part number
• Data harga IDR  : {n_harga:,} part number
• Kurs CNY → IDR  : × {kurs:,.0f} (live, untuk referensi internal)

=== KONSEP KATALOG: 1 FILE EXCEL = 1 TIPE UNIT ===
Setiap file Excel di atas adalah katalog part KHUSUS untuk satu tipe/model unit (truk atau alat berat)
tertentu — BUKAN kumpulan part acak. Nama file tersebut SENGAJA dibuat sama dengan nama tipe unit itu
sendiri oleh admin, jadi nama file = label tipe unit/model kendaraan yang sah, bukan sekadar nama file
komputer biasa. Akibatnya:
• Field "Tipe Unit" / "Unit" yang muncul di blok [DATA] dan [HASIL PENCARIAN] pada pesan user = nama
  file katalog tempat PN tersebut ditemukan, yang berarti PN itu memang bagian dari unit tersebut.
• Jika satu PN tercatat di lebih dari satu "Tipe Unit", itu artinya part tersebut dipakai/interchangeable
  di semua model unit yang disebutkan — bukan kesalahan data, dan jangan diabaikan sebagian.
• Saat menjawab pencarian by nama/fungsi atau perbandingan part, selalu sebutkan Tipe Unit-nya supaya
  user tahu part itu cocok untuk unit apa.

{intent_guidance}

=== INSTRUKSI UTAMA ===
KRITIS: Blok [DATA], [STOK], [HARGA], [SIMS], [HASIL PENCARIAN], [PENCARIAN TAMBAHAN], [SERUPA],
[UNIT DIMINTA], [PERHATIAN] di pesan user = sumber kebenaran.
Jangan PERNAH mengarang PN, harga, atau nama part. Kalau data tidak ada, bilang terus terang.

1. HARGA
   - Tampilkan dari data lokal: "Rp X.XXX.XXX"
   - Jika tidak ada, arahkan ke tab "💰 Harga" → sub-tab "🔍 Cari Harga"
   - JANGAN tampilkan harga dari SIMS (hanya untuk internal)

2. STOK
   - Jawab qty total dan per-gudang jika ada
   - Stok 0: "Kosong di gudang ini. Konfirmasi ke bagian spare part."
   - Stok "—": data tidak tersedia, bukan berarti kosong

3. PERBANDINGAN 2 PART
   - Buat tabel Markdown side-by-side
   - Sebutkan apakah saling interchangeable berdasarkan nama & unit
   - Beri rekomendasi mana yang lebih baik dipilih (stok & harga)

4. PENCARIAN NAMA/FUNGSI
   - Gunakan [HASIL PENCARIAN] jika ada
   - Tabel: | No | PN | Nama Part | Tipe Unit | Stok | Harga |
   - Highlight yang stok > 0 dengan ✅

5. PERTANYAAN TEKNIS
   - Jawab dari knowledge base HOWO/Sinotruk/CNHTC
   - Sertakan interval servis (contoh: "Ganti setiap 10.000 km atau 6 bulan")
   - Sertakan gejala kerusakan umum jika relevan
   - Jika terkait part spesifik, kaitkan dengan PN dari katalog

6. PN SERUPA (fuzzy)
   - Jika ada blok [PN SERUPA], sarankan ke user bahwa PN tersebut mungkin yang dimaksud
   - Jangan asumsikan — tanyakan konfirmasi dulu

7. PEMAHAMAN SEMANTIK PART (PENTING — SEPERTI MEKANIK SENIOR)
   Kamu WAJIB memahami bahwa satu komponen bisa punya banyak nama berbeda tergantung
   bahasa, merek, atau kebiasaan lapangan. Saat user menyebut salah satu nama, kamu TAHU
   semua nama lainnya dan mencari dengan semua nama tersebut.

   WAJIB hapal pemetaan ini (dan jelaskan ke user jika relevan):

   🔵 DEF / AdBlue / Urea:
      tangki urea = tangki adblue = urea tank = adblue tank = def tank = scr tank
      pompa urea = adblue pump = def pump = dosing pump = dosing module
      urea = adblue = def = diesel exhaust fluid = aus32 = scr fluid = air urea

   🔵 Laher / Bearing:
      laher = bearing = ball bearing = roller bearing = tapered bearing = needle bearing

   🔵 Kampas Rem:
      kampas rem = kanvas rem = brake pad = brake lining = friction lining = brake shoe

   🔵 Injektor / Injector:
      injektor = injector = fuel injector = injection nozzle = nosel = nozzle holder

   🔵 Kopling / Clutch:
      kopling = clutch = clutch disc = clutch plate = kampas kopling = disc kopling

   🔵 Gardan / Differential:
      gardan = differential = diff = drive axle = final drive = rear axle

   🔵 Shock / Absorber:
      shock = shockbreaker = sokbreker = shock absorber = damper = peredam kejut

   🔵 Dinamo Ampere / Alternator:
      dinamo ampere = dynamo ampere = alternator = generator = charging unit

   🔵 Dinamo Starter / Starter:
      dinamo starter = dynamo starter = starter motor = starter assy = electric starter

   🔵 Aki / Battery:
      aki = accu = battery = accumulator = batu aki = baterai

   🔵 ECU / Komputer Mesin:
      ecu = ecm = engine control unit = engine control module = komputer mesin

   🔵 Paking / Gasket:
      paking = gasket = seal = perapat = o-ring = head gasket

   🔵 Sil / Oil Seal:
      sil = seal = oil seal = lip seal = rotary seal = perapat oli

   🔵 Turbo / Turbocharger:
      turbo = turbocharger = turbo assy = turbine = supercharger

   🔵 Kruk As / Crankshaft:
      kruk as = crankshaft = crank shaft = as engkol = poros engkol

   🔵 Metal Jalan / Con Rod Bearing:
      metal jalan = connecting rod bearing = con rod bearing = big end bearing

   🔵 Metal Duduk / Main Bearing:
      metal duduk = main bearing = crankshaft bearing = main journal bearing

   🔵 Nok / Camshaft:
      nok = noken as = camshaft = cam shaft = poros nok

   🔵 Common Rail / CR:
      common rail = fuel rail = high pressure rail = cr rail = fuel accumulator

   🔵 EGR:
      egr = egr valve = exhaust gas recirculation = katup egr

   🔵 DPF:
      dpf = diesel particulate filter = particulate filter = filter partikulat

   🔵 SCR / After Treatment:
      scr = selective catalytic reduction = after treatment = aftertreatment = katalisator

   🔵 Intercooler:
      intercooler = charge air cooler = after cooler = air cooler = pendingin udara = cac

   🔵 Water Separator:
      water separator = water sedimenter = fuel water separator = pemisah air = pre filter

   CARA PAKAI: Saat data pencarian ditemukan dengan keyword berbeda dari yang user tulis,
   JELASKAN ke user: "FYI: [istilah user] = [nama teknis di katalog] — saya sudah carikan
   dengan semua nama alternatifnya."

   Saat tidak ada data sama sekali, sarankan user mencoba dengan nama-nama alternatif tersebut
   di tab Search Part Name.

8. FORMAT JAWABAN
   - Singkat jika 1 PN → 2-4 baris
   - Tabel jika multiple PN atau compare
   - Poin jika penjelasan teknis
   - Emoji sparingly: ✅ stok ada, ⚠️ stok tipis, ❌ kosong, 💰 harga, 🔧 teknis

9. SUGGESTION (akhir jawaban)
   Tambahkan 1-2 pertanyaan lanjutan relevan:
   > 💡 *Mau cek juga: "[pertanyaan relevan]"?*

10. MULTI-INTENT
    Jika pertanyaan mengandung beberapa intent sekaligus (contoh: "harga dan stok AZ123"),
    jawab semua intent dalam satu respons yang terstruktur.

11. TIPE UNIT / KATALOG (PENTING)
    - "Tipe Unit" pada data = nama tipe/model kendaraan resmi (diambil dari nama file katalog),
      BUKAN sekadar metadata file komputer. Perlakukan sebagai informasi kompatibilitas part.
    - Jika PN ditemukan di beberapa Tipe Unit sekaligus, sebutkan SEMUANYA — itu menunjukkan
      part bisa dipakai lintas unit/model.
    - Jika user menyebut nama unit tertentu (mis. "HOWO A7", "NX280"), dan part yang ditemukan
      berasal dari Tipe Unit yang berbeda, beri tahu user secara jelas bahwa part tersebut
      sebenarnya dari katalog unit lain — jangan diam-diam menganggapnya cocok.

12. KOMPATIBILITAS PART-UNIT
    - Blok [UNIT DIMINTA] = unit yang terdeteksi disebut user (toleran typo/sebagian penyebutan).
    - Blok [PERHATIAN] = peringatan eksplisit kalau PN yang dicari TIDAK terdaftar untuk unit
      tersebut. WAJIB dibaca & disampaikan ke user sebelum menjawab "bisa dipasang di X?" atau
      "cocok ga buat X?".
    - Kalau tidak ada blok [PERHATIAN] tapi ada [UNIT DIMINTA], dan Tipe Unit pada [DATA] cocok,
      berarti memang kompatibel — boleh jawab "Ya, cocok".
"""


# ══════════════════════════════════════════════════════════════════════
#  AI QUERY EXPANSION (cache per session)
# ══════════════════════════════════════════════════════════════════════

_EXPAND_CACHE: dict[str, list[str]] = {}


def _expand_query_with_ai(query: str, api_key: str) -> list[str]:
    cache_key = query.upper().strip()
    if cache_key in _EXPAND_CACHE:
        return _EXPAND_CACHE[cache_key]

    q_lower = query.lower()
    local_matches: list[str] = []

    # ── Tier 1: exact match di BENGKEL_DICT ──────────────────────────
    for term, replacements in BENGKEL_DICT.items():
        if term in q_lower:
            local_matches.extend(replacements)

    # ── Tier 1.5: SEMANTIC_ALIASES — ekspansi sinonim lintas bahasa ──
    # Cek setiap token/frasa dari query terhadap semua alias yang dikenal.
    # Ini yang membuat "tangki adblue" → ["UREA TANK", "DEF TANK", "SCR TANK", ...]
    # dan "freon" → ["REFRIGERANT", "R134A", "AC GAS"] tanpa perlu hit AI.
    q_upper_tokens = re.findall(r"[A-Z0-9]+", query.upper())
    for alias_key, alias_vals in _scan_alias_hits(query, q_lower, q_upper_tokens).items():
        for v in alias_vals:
            if v not in local_matches:
                local_matches.append(v)

    if local_matches:
        # Deduplikasi
        seen: set[str] = set()
        deduped: list[str] = []
        for m in local_matches:
            mu = m.upper()
            if mu not in seen:
                seen.add(mu)
                deduped.append(m)
        _EXPAND_CACHE[cache_key] = deduped
        return deduped

    # ── Tier 2: toleransi typo terhadap istilah bengkel ──────────────
    fuzzy_matches = _fuzzy_bengkel_match(q_lower)
    if fuzzy_matches:
        _EXPAND_CACHE[cache_key] = fuzzy_matches
        return fuzzy_matches

    # ── Tier 3: fallback ke AI untuk istilah yang benar-benar asing ──
    prompt = (
        "Kamu ahli sparepart truk HOWO/Sinotruk/CNHTC dan diesel engine. "
        "Tugas kamu: ubah istilah/nama part berikut menjadi SEMUA keyword teknis "
        "bahasa Inggris yang mungkin dipakai di katalog sparepart. "
        "Sertakan variasi nama, singkatan, dan nama merek generik. "
        "Contoh: 'adblue' → [\"UREA TANK\", \"DEF TANK\", \"SCR TANK\", \"UREA PUMP\", "
        "\"DOSING MODULE\", \"ADBLUE TANK\", \"AUS32\"]\n"
        "Balas HANYA JSON array string, tanpa markdown, tanpa penjelasan.\n\n"
        f"Istilah: {query}"
    )
    try:
        resp = requests.post(
            DEEPSEEK_API_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": DEEPSEEK_MODEL, "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=15,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = re.sub(r"```[a-z]*", "", raw).strip().strip("`").strip()
        keywords = [k.upper() for k in json.loads(raw) if isinstance(k, str)]
        _EXPAND_CACHE[cache_key] = keywords
        return keywords
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════
#  PN VALIDATOR
# ══════════════════════════════════════════════════════════════════════

def _is_valid_pn(pn: str) -> bool:
    """Validasi format PN HOWO/Sinotruk sebelum hit SIMS."""
    pn = pn.strip()
    if len(pn) < 6 or len(pn) > 30:
        return False
    if not re.search(r'\d{3,}', pn):
        return False
    if not re.match(r'^[A-Z0-9]', pn):
        return False
    return True


# ══════════════════════════════════════════════════════════════════════
#  DATA LOOKUP — PN & nama part
# ══════════════════════════════════════════════════════════════════════

def _lookup_pn(
    pn: str,
    excel_files: list | None,
    stok_cache:  dict | None,
    harga_lookup: dict | None,
    fetch_sims: bool = False,
    kurs: float = KURS_CNY_IDR_FALLBACK,
) -> dict:
    """Cari semua data untuk satu PN. Gunakan cache sesi jika ada."""
    pn = pn.upper().strip()

    # Coba ambil dari cache
    cached = _pn_cache_get(pn)
    if cached:
        return cached

    result = {"pn": pn, "name": None, "units": [], "stok": None,
              "harga_local": None, "harga_sims": None, "found": False}

    if excel_files:
        for fi in excel_files:
            pni = fi.get("part_number_index", {})
            if pn in pni:
                result["found"] = True
                ul = fi.get("simple_name", "")
                if ul and ul not in result["units"]:
                    result["units"].append(ul)
                if result["name"] is None:
                    df  = fi.get("dataframe")
                    idx = pni[pn][0]
                    if df is not None:
                        try:
                            row = df.iloc[idx]
                            nm = str(row.get("part_name", "")).strip()
                            if nm and nm.upper() not in ("NAN", "N/A", "NONE", ""):
                                result["name"] = nm
                        except Exception:
                            pass

    if stok_cache:
        sv = stok_cache.get(pn)
        if sv is not None:
            result["stok"] = sv

    if harga_lookup:
        result["harga_local"] = harga_lookup.get(pn)

    if fetch_sims and SIMS_PRICE_ENABLED and _is_valid_pn(pn):
        try:
            price, err = get_sims_part_price(pn)
            if price is not None:
                result["harga_sims"] = price
        except Exception:
            pass

    # Simpan ke cache
    _pn_cache_set(pn, result)
    return result


def _format_pn_context(info: dict, kurs: float = KURS_CNY_IDR_FALLBACK) -> str:
    """Format dict info PN menjadi string konteks untuk AI."""
    pn = info["pn"]
    lines = [f"[DATA] PN {pn}:"]
    if info["name"]:
        lines[0] += f" Nama='{info['name']}'"
    if info["units"]:
        lines[0] += f" | Tipe Unit: {', '.join(info['units'][:6])}"
    if info["stok"] is not None:
        stok_val = info["stok"]
        icon = "✅" if (isinstance(stok_val, (int, float)) and stok_val > 0) else "❌"
        lines.append(f"[STOK] {icon} {stok_val}")
    if info["harga_local"]:
        lines.append(f"[HARGA IDR] {info['harga_local']}")
    # Harga SIMS tidak ditampilkan (hanya untuk internal)
    if not info["found"]:
        lines.append(f"[INFO] PN {pn} tidak ditemukan di katalog lokal.")
    return "\n".join(lines)


def _search_parts_by_name(
    query: str,
    excel_files: list | None,
    stok_cache:  dict | None,
    harga_lookup: dict | None,
    api_key: str | None = None,
    max_results: int = 15,
    target_units: list[str] | None = None,
) -> tuple[list[dict], bool]:
    """Cari part berdasarkan nama/deskripsi bebas. Return (results, used_fallback).

    target_units (opsional): daftar nama tipe unit (simple_name) yang sudah terdeteksi
    relevan dari query (lihat _detect_units_in_query) — kalau diisi, ini dipakai untuk
    filter unit yang lebih presisi daripada heuristik "token yang ada angkanya".
    """
    if not excel_files:
        return [], False

    q_upper = query.upper()
    raw_tokens = re.findall(r'[A-Z0-9]{3,}', q_upper)
    base_keywords = [t for t in raw_tokens if t not in _NAME_SEARCH_STOPWORDS and not re.match(r'^\d+$', t)]

    ai_keywords: list[str] = []
    if base_keywords:
        # ✨ Expansion lokal (Tier 1/1.5/2) TIDAK butuh API key —
        #    cuma Tier 3 (AI fallback) yang perlu. Kalau api_key kosong,
        #    Tier 1+1.5+2 tetap jalan; Tier 3 gagal graceful → return [].
        ai_keywords = _expand_query_with_ai(query, api_key or "")

    all_keywords = ai_keywords + [k for k in base_keywords if k not in ai_keywords]
    if not all_keywords:
        return [], False

    unit_kws = [k for k in all_keywords if re.search(r'\d', k)]
    name_kws = [k for k in all_keywords if not re.search(r'\d', k)]
    phrase_kws = [nk for nk in name_kws if " " in nk]  # multi-word → phrase boost
    target_units_set = set(target_units) if target_units else None

    def _do_search(file_list, filter_unit):
        results = []
        seen_pn: set[str] = set()

        # Kalau pencarian dibatasi ke target_units tertentu (biasanya cuma beberapa
        # varian unit, mis. NX360 6X4 / NX360TH / NX360 DUMP), JANGAN berhenti di file
        # pertama begitu max_results tercapai — itu bikin hasil cuma muncul dari 1 unit
        # saja walau target_units-nya ada beberapa. Scan semua file target dulu (dengan
        # cap per-file supaya tetap terkendali), gabung, urutkan, baru trim ke
        # max_results di akhir.
        #
        # BUG LAMA (pencarian TANPA target unit spesifik, mis. "tangki adblue" tanpa
        # sebut unit): early-stop begitu len(results) >= max_results membuat scan
        # berhenti di FILE PERTAMA yang kebagian cukup match, walau file itu tidak
        # selalu yang paling relevan (mis. katalog DH08-B3/L36 kebetulan ada duluan
        # di excel_files dengan match generik "OIL TANK"/"FUEL TANK", sehingga PN
        # urea-tank asli di katalog HOWO-7/NX350 yang ada lebih belakang TIDAK PERNAH
        # ikut di-scan sama sekali). Fix: kumpulkan kandidat dari SEMUA file dulu
        # (dibatasi GATHER_CAP yang jauh lebih besar dari max_results supaya tetap
        # terkendali performanya), baru urutkan berdasar confidence/stok lalu trim
        # ke max_results di akhir — bukan berhenti di file pertama yang ditemui.
        restrict_to_targets = filter_unit and target_units_set is not None
        per_file_cap = max_results if restrict_to_targets else None
        GATHER_CAP = max(max_results * 10, 150)

        for fi in file_list:
            if not restrict_to_targets and len(results) >= GATHER_CAP:
                break
            simple_name = fi.get("simple_name", "").upper()
            if filter_unit:
                if target_units_set is not None:
                    if fi.get("simple_name") not in target_units_set:
                        continue
                elif unit_kws and not any(uk in simple_name for uk in unit_kws):
                    continue
            df      = fi.get("dataframe")
            pni_idx = fi.get("part_name_index", {})
            if df is None:
                continue

            # idx_hits: index baris -> set keyword yang match (substring ATAU fuzzy/typo).
            idx_hits: dict[int, set] = {}
            index_words = list(pni_idx.keys())
            for nk in name_kws:
                nk_hit_words = [w for w in index_words if nk in w or w in nk]
                if not nk_hit_words and len(nk) >= 4:
                    # Toleransi typo: nk mungkin salah ketik dari kata di katalog
                    # (mis. user ketik "BEARNG", katalog punya "BEARING"). Dibatasi
                    # 5000 kata pertama per file supaya tetap cepat di katalog besar.
                    nk_hit_words = difflib.get_close_matches(nk, index_words[:5000], n=3, cutoff=0.78)
                for w in nk_hit_words:
                    for idx in pni_idx[w]:
                        idx_hits.setdefault(idx, set()).add(nk)

            file_added = 0
            for idx, matched_kws in idx_hits.items():
                if restrict_to_targets:
                    if file_added >= per_file_cap:
                        break
                elif len(results) >= GATHER_CAP:
                    break
                if name_kws and not matched_kws:
                    continue
                try:
                    row   = df.iloc[idx]
                    pn_v  = str(row["part_number"]).strip().upper()
                    pname = str(row["part_name"]).strip()
                    if not pn_v or pn_v in ("NAN", "N/A", "NONE", ""):
                        continue
                    if pn_v in seen_pn:
                        for r in results:
                            if r["pn"] == pn_v:
                                ul = fi.get("simple_name", "")
                                if ul and ul not in r["units"]:
                                    r["units"].append(ul)
                        continue
                    seen_pn.add(pn_v)
                    stok_v  = stok_cache.get(pn_v, "—") if stok_cache else "—"
                    harga_v = harga_lookup.get(pn_v, "") if harga_lookup else ""

                    # Confidence score: porsi keyword yang match + phrase boost.
                    # Multi-word keyword yang muncul sebagai frasa PERSIS di nama part
                    # (mis. "UREA TANK" ada di "UREA TANK ASSY") dapat bonus besar
                    # supaya tidak kalah oleh match parsial (mis. "FUEL TANK" cuma
                    # match kata "TANK" doang dari frasa "UREA TANK").
                    pname_upper = pname.upper()
                    phrase_bonus = 0
                    if phrase_kws:
                        for pk in phrase_kws:
                            if pk in pname_upper:
                                # Bobot 3× per kata dalam frasa yang match persis
                                phrase_bonus += len(pk.split()) * 3
                    confidence = round((len(matched_kws) + phrase_bonus) / max(len(name_kws), 1) * 100)

                    results.append({
                        "pn": pn_v, "name": pname,
                        "units": [fi.get("simple_name", "")] if fi.get("simple_name") else [],
                        "stok": str(stok_v), "harga": harga_v,
                        "confidence": confidence,
                    })
                    file_added += 1
                except Exception:
                    continue

        if restrict_to_targets:
            # Kelompokkan per unit asal, urutkan tiap grup (stok lalu confidence), lalu
            # gabung dengan ROUND-ROBIN supaya setiap unit target kebagian slot secara
            # adil. Tanpa ini, sort-lalu-trim biasa bisa tetap didominasi 1 unit yang
            # kebetulan punya match terbanyak (mis. semua confidence sama -> stable sort
            # mempertahankan urutan file, unit pertama menghabiskan semua slot sebelum
            # unit lain kebagian).
            groups: dict[str, list[dict]] = {}
            for r in results:
                key = r["units"][0] if r["units"] else ""
                groups.setdefault(key, []).append(r)
            for g in groups.values():
                g.sort(key=lambda r: (
                    -1 if (r["stok"] not in ("—", "0", "") and r["stok"] != "0") else 0,
                    -r.get("confidence", 0),
                ))
            merged: list[dict] = []
            group_lists = list(groups.values())
            row = 0
            while len(merged) < max_results and any(row < len(g) for g in group_lists):
                for g in group_lists:
                    if row < len(g):
                        merged.append(g[row])
                        if len(merged) >= max_results:
                            break
                row += 1
            return merged

        # Pencarian tanpa target unit spesifik: urutkan stok > 0 dulu, lalu confidence
        # turun, BARU trim ke max_results — supaya hasil terbaik dari SELURUH katalog
        # yang menang, bukan sekadar 15 match pertama yang ditemukan apa adanya.
        results.sort(key=lambda r: (
            -1 if (r["stok"] not in ("—", "0", "") and r["stok"] != "0") else 0,
            -r.get("confidence", 0),
        ))
        return results[:max_results]

    filter_unit_active = bool(unit_kws or target_units_set)
    results = _do_search(excel_files, filter_unit=filter_unit_active)
    used_fallback = False
    if not results and filter_unit_active and name_kws:
        results = _do_search(excel_files, filter_unit=False)
        used_fallback = True

    return results, used_fallback


def _format_name_search_lines(label: str, results: list[dict], used_fallback: bool) -> list[str]:
    """Format hasil _search_parts_by_name jadi baris konteks untuk AI. Dipakai baik untuk
    pencarian by-nama utama maupun pencarian tambahan pada query gabungan."""
    if not results:
        return [f"[{label}] Tidak ditemukan di katalog lokal."]
    suffix = " (dari semua katalog, bukan unit spesifik)" if used_fallback else ""
    lines = [f"[{label}{suffix}] {len(results)} part:"]
    for r in results:
        conf_str = f" [{r.get('confidence', 0)}%]" if r.get("confidence") else ""
        ln = f"  • {r['pn']}{conf_str}"
        if r["name"]:  ln += f" — {r['name']}"
        if r["units"]: ln += f" | Tipe Unit: {', '.join(r['units'][:4])}"
        if r["stok"] and r["stok"] != "—": ln += f" | Stok: {r['stok']}"
        if r["harga"]: ln += f" | Harga: {r['harga']}"
        lines.append(ln)
    return lines


# Batas jumlah PN yang dicek detail per query — dinaikkan dari 3 supaya pertanyaan
# gabungan/rumit (mis. menyebut 4-5 PN sekaligus) tetap kebagian data.
MAX_PN_PER_QUERY        = 5
MAX_SIMS_FETCH_PER_QUERY = 3   # tetap dibatasi supaya tidak terlalu banyak hit API SIMS


def _lookup_part_info(
    query: str,
    excel_files: list | None,
    stok_cache:  dict | None,
    harga_lookup: dict | None,
    intents: list[str] | None = None,
    kurs: float = KURS_CNY_IDR_FALLBACK,
) -> str:
    """Main context builder: deteksi PN vs nama, inject data relevan ke AI."""
    if not query:
        return ""

    intents = intents or [Intent.UNKNOWN]
    api_key = _get_api_key()
    pn_candidates = _extract_all_pn(query, excel_files)
    lines: list[str] = []

    # ── Inject alias context: kalau query mengandung istilah yang punya sinonim,
    # beritahu AI semua nama alternatifnya. Ini yang bikin AI bisa bilang:
    # "tangki adblue = urea tank = def tank — saya carikan dengan semua nama itu."
    q_lower = query.lower()
    q_upper_tokens = re.findall(r"[A-Z0-9]+", query.upper())
    alias_hits = _scan_alias_hits(query, q_lower, q_upper_tokens)
    if alias_hits:
        for ak, avs in alias_hits.items():
            top_aliases = avs[:6]
            lines.append(f"[ALIAS] '{ak}' = {' | '.join(top_aliases)}")

    # Deteksi tipe unit yang disebut user (toleran typo/sebagian) — dipakai untuk
    # cross-check kompatibilitas PN dan untuk mempersempit pencarian by-nama.
    mentioned_units = _detect_units_in_query(query, excel_files)
    if mentioned_units:
        lines.append(f"[UNIT DIMINTA] {', '.join(mentioned_units)}")

    # ── MODE 1: Ada PN dalam query ────────────────────────────────────
    if pn_candidates:
        fetch_sims_price = bool(
            set(intents) & {Intent.HARGA, Intent.COMPARE, Intent.UNKNOWN}
        )
        for i, pn in enumerate(pn_candidates[:MAX_PN_PER_QUERY]):
            info = _lookup_pn(
                pn, excel_files, stok_cache, harga_lookup,
                fetch_sims=fetch_sims_price and i < MAX_SIMS_FETCH_PER_QUERY, kurs=kurs
            )
            lines.append(_format_pn_context(info, kurs=kurs))

            # Jika PN tidak ditemukan, coba fuzzy match
            if not info["found"]:
                similar = _fuzzy_similar_pn(pn, excel_files, n=3)
                if similar:
                    lines.append(f"[PN SERUPA untuk {pn}] {', '.join(similar)}")
            # Cross-check kompatibilitas: kalau user nyebut unit tertentu tapi PN ini
            # ternyata cuma terdaftar di unit lain, AI WAJIB tahu ini secara eksplisit.
            elif mentioned_units and info["units"] and not any(mu in info["units"] for mu in mentioned_units):
                lines.append(
                    f"[PERHATIAN] PN {pn} TIDAK terdaftar untuk unit yang diminta "
                    f"({', '.join(mentioned_units)}). PN ini hanya terdaftar untuk: "
                    f"{', '.join(info['units'][:6])}."
                )

        # Query gabungan: selain sebut PN, user mungkin JUGA minta pencarian by-nama
        # terpisah (mis. "harga AZ123, terus carikan juga filter oli HOWO A7").
        extra_kw = _has_additional_name_request(query, pn_candidates)
        if extra_kw:
            extra_results, extra_fallback = _search_parts_by_name(
                extra_kw, excel_files, stok_cache, harga_lookup, api_key,
                max_results=8, target_units=mentioned_units or None,
            )
            if extra_results:
                lines.extend(_format_name_search_lines(
                    f"PENCARIAN TAMBAHAN '{extra_kw}'", extra_results, extra_fallback
                ))

    # ── MODE 2: Cari by nama ──────────────────────────────────────────
    else:
        results, used_fallback = _search_parts_by_name(
            query, excel_files, stok_cache, harga_lookup, api_key,
            target_units=mentioned_units or None,
        )
        lines.extend(_format_name_search_lines(
            f"HASIL PENCARIAN '{query.strip()}'", results, used_fallback
        ))
        if not results:
            lines.append("Coba tab '📝 Search Part Name' untuk pencarian lebih lanjut.")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  DEEPSEEK API — dengan retry & backoff
# ══════════════════════════════════════════════════════════════════════

def _call_deepseek(
    messages: list[dict],
    api_key: str,
    stream: bool = True,
    retries: int = MAX_RETRIES,
) -> Any:
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {
        "model":      DEEPSEEK_MODEL,
        "messages":   messages,
        "max_tokens": MAX_TOKENS,
        # ✨ DeepSeek Reasoner (R1) tidak mendukung parameter temperature —
        # model secara internal menggunakan chain-of-thought reasoning sebelum menjawab.
        "stream":     stream,
    }

    for attempt in range(retries):
        try:
            resp = requests.post(
                DEEPSEEK_API_URL, headers=headers, json=body,
                timeout=TIMEOUT_SEC, stream=stream,
            )

            if resp.status_code == 429:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                time.sleep(wait)
                continue

            resp.raise_for_status()

            if not stream:
                data = resp.json()
                # Reasoner: ambil content dari message (reasoning_content diabaikan)
                return data["choices"][0]["message"]["content"]

            def _gen(r=resp):
                """Stream generator — filter reasoning_content, hanya yield content final."""
                for raw_line in r.iter_lines():
                    if not raw_line:
                        continue
                    line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                    if line.startswith("data: "):
                        line = line[6:]
                    if line.strip() == "[DONE]":
                        break
                    try:
                        chunk = json.loads(line)
                        delta = chunk["choices"][0].get("delta", {})
                        # ✨ Reasoner mengirim dua field:
                        #   "reasoning_content" → proses berpikir internal (tidak ditampilkan)
                        #   "content"           → jawaban final (ini yang ditampilkan)
                        txt = delta.get("content") or ""
                        if txt:
                            yield txt
                    except Exception:
                        continue
            return _gen()

        except requests.exceptions.Timeout:
            raise
        except requests.exceptions.HTTPError:
            raise

    raise RuntimeError("Gagal memanggil DeepSeek API setelah beberapa percobaan.")


# ══════════════════════════════════════════════════════════════════════
#  FALLBACK — Jawab langsung dari data jika AI tidak tersedia
# ══════════════════════════════════════════════════════════════════════

def _fallback_answer(ctx_data: str, query: str, intents: list[str], excel_files: list | None = None) -> str:
    """Jawaban minimal dari data lokal tanpa memanggil AI."""
    if not ctx_data:
        return "Maaf, data tidak ditemukan di katalog lokal. Coba cek di tab pencarian."
    lines = ctx_data.strip().split("\n")
    pn_candidates = _extract_all_pn(query, excel_files)
    if pn_candidates and Intent.HARGA in intents:
        harga_lines = [l for l in lines if "[HARGA" in l]
        if harga_lines:
            return "**Harga dari data lokal:**\n" + "\n".join(harga_lines)
    if pn_candidates and Intent.STOK in intents:
        stok_lines = [l for l in lines if "[STOK]" in l]
        if stok_lines:
            return "**Stok:**\n" + "\n".join(stok_lines)
    return "**Data ditemukan:**\n" + "\n".join(lines[:10])


# ══════════════════════════════════════════════════════════════════════
#  EXPORT CHAT HISTORY
# ══════════════════════════════════════════════════════════════════════

def _export_chat_txt(history: list[dict]) -> str:
    """Konversi history chat menjadi teks yang bisa di-download."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"=== HIRO Chat Export — {now} ===\n"]
    for msg in history:
        role = "👤 User" if msg["role"] == "user" else "🤖 HIRO"
        lines.append(f"{role}:\n{msg['content']}\n")
        lines.append("-" * 40)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════

_SS_HISTORY = "chat_ai_history"
_SS_INPUT   = "chat_ai_pending_input"


def _get_history() -> list[dict]:
    return st.session_state.get(_SS_HISTORY, [])

def _set_history(h: list[dict]):
    st.session_state[_SS_HISTORY] = h

def _trim_history(h: list[dict]) -> list[dict]:
    pairs = MAX_HISTORY * 2
    return h[-pairs:] if len(h) > pairs else h


# ══════════════════════════════════════════════════════════════════════
#  RENDER UI
# ══════════════════════════════════════════════════════════════════════

def render_chat_ai_tab(
    excel_files:  list | None = None,
    stok_cache:   dict | None = None,
    harga_lookup: dict | None = None,
):
    st.markdown(
        '<div style="margin:.25rem 0 .75rem;">'
        '<div style="font-size:15px;font-weight:700;letter-spacing:-.01em;color:var(--mp-ink);">'
        '🤖 <span style="color:var(--mp-green);">HIRO v4</span> — Asisten Sparepart HOWO</div>'
        '<div style="font-size:12px;color:var(--mp-ink-50);margin-top:2px;">'
        'Powered by DeepSeek R1 Reasoner · Tanya harga, stok, cari part, atau info teknis HOWO/Sinotruk.'
        '</div></div>',
        unsafe_allow_html=True,
    )

    api_key = _get_api_key()

    if not api_key:
        st.error(
            "⚠️ **DeepSeek API key belum dikonfigurasi.**\n\n"
            "Tambahkan di `.streamlit/secrets.toml`:\n"
            "```toml\nDEEPSEEK_API_KEY = \"sk-...\"\n```"
        )
        return

    # ── Ambil kurs live (background, tidak blok UI) ──────────────────
    kurs = _get_live_kurs()

    # ── Info bar ─────────────────────────────────────────────────────
    col_info, col_export, col_clear = st.columns([5, 1, 1])
    with col_info:
        n_parts = sum(len(fi.get("dataframe", [])) for fi in (excel_files or []))
        n_stok  = len(stok_cache or {})
        n_harga = len(harga_lookup or {})
        st.caption(
            f"📦 **{n_parts:,}** part · 📊 **{n_stok:,}** stok · "
            f"💰 **{n_harga:,}** harga"
        )

    with col_export:
        history = _get_history()
        if history:
            export_txt = _export_chat_txt(history)
            st.download_button(
                label="💾",
                data=export_txt,
                file_name=f"HIRO_chat_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain",
                key="chat_export_btn",
                use_container_width=True,
                help="Download riwayat chat",
            )

    with col_clear:
        if st.button("🗑️", key="chat_ai_clear", use_container_width=True, help="Reset chat"):
            _set_history([])
            st.session_state.pop(_SS_CTX_MEMORY, None)
            st.session_state.pop(_SS_PN_CACHE, None)
            st.rerun()

    st.markdown("---")

    # ── Chat history ─────────────────────────────────────────────────
    history = _get_history()

    if not history:
        st.markdown(
            """
            <div style="text-align:center;padding:2rem 1rem;color:var(--mp-ink-50);">
                <div style="font-size:2.5rem;margin-bottom:.5rem;">🤖</div>
                <div style="font-size:14px;font-weight:600;">Halo! Saya HIRO v4.</div>
                <div style="font-size:12.5px;margin-top:.5rem;">
                    ✨ <i>Kini didukung DeepSeek R1 Reasoner — AI lebih cerdas &amp; akurat.</i><br><br>
                    Tanya saya dalam bahasa sehari-hari:<br>
                    • <i>"Berapa harga dan stok laher AZ9114410043?"</i><br>
                    • <i>"Bandingkan WG9114410043 sama AZ9114410043"</i><br>
                    • <i>"Carikan filter solar NX280"</i><br>
                    • <i>"Berapa interval ganti oli mesin HOWO A7?"</i><br>
                    • <i>"Stok kampas rem HOWO 371 ada nggak?"</i>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        for msg in history:
            role = msg["role"]
            if role == "user":
                with st.chat_message("user"):
                    st.markdown(msg["content"])
            elif role == "assistant":
                with st.chat_message("assistant", avatar="🤖"):
                    st.markdown(msg["content"])

    # ── Chat input ───────────────────────────────────────────────────
    user_input = st.chat_input(
        "Tanya HIRO... (harga, stok, cari part, info teknis)",
        key="chat_ai_input",
    )

    # Quick buttons hanya saat kosong
    if not user_input and not history:
        st.markdown(
            '<div style="font-size:12px;color:var(--mp-ink-50);'
            'margin-top:.5rem;margin-bottom:.3rem;">💡 Coba tanya ini:</div>',
            unsafe_allow_html=True,
        )
        quick_qs = [
            "Berapa interval ganti oli gardan HOWO?",
            "Apa bedanya kampas rem depan dan belakang?",
            "Fungsi intercooler pada truk diesel?",
        ]
        for col, q in zip(st.columns(3), quick_qs):
            with col:
                if st.button(q, key=f"chat_quick_{hash(q)}", use_container_width=True):
                    st.session_state[_SS_INPUT] = q
                    st.rerun()

    # Ambil dari quick button jika ada
    if _SS_INPUT in st.session_state and not user_input:
        user_input = st.session_state.pop(_SS_INPUT)

    if not user_input or not user_input.strip():
        return

    user_input = user_input.strip()

    # ── Deteksi multi-intent ─────────────────────────────────────────
    intents = detect_intents(user_input, excel_files)

    # ── Tambah ke history ────────────────────────────────────────────
    history = _get_history()
    history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # ── Lookup data lokal + SIMS (dengan status informatif) ──────────
    pn_count = len(_extract_all_pn(user_input, excel_files))
    if pn_count:
        status_msg = f"🔍 Mencari data untuk {pn_count} part number..."
    elif Intent.COMPARE in intents:
        status_msg = "⚖️ Menyiapkan perbandingan..."
    elif Intent.HARGA in intents:
        status_msg = "💰 Mengambil data harga..."
    elif Intent.STOK in intents:
        status_msg = "📦 Mengecek ketersediaan stok..."
    elif Intent.TEKNIS in intents:
        status_msg = "🔧 Menyiapkan jawaban teknis..."
    else:
        status_msg = "🔍 Mencari di katalog..."

    ctx_data = _lookup_part_info(
        user_input, excel_files, stok_cache, harga_lookup,
        intents=intents, kurs=kurs,
    )

    # ── Bangun API messages ──────────────────────────────────────────
    system_prompt = _build_system_prompt(
        excel_files, stok_cache, harga_lookup, intents=intents, kurs=kurs
    )

    augmented_user = user_input
    if ctx_data:
        augmented_user = (
            f"{user_input}\n\n"
            f"[Konteks dari database]\n{ctx_data}"
        )

    trimmed = _trim_history(history[:-1])
    api_messages = (
        [{"role": "system", "content": system_prompt}]
        + trimmed
        + [{"role": "user", "content": augmented_user}]
    )

    # ── Stream response ──────────────────────────────────────────────
    assistant_reply = ""
    with st.chat_message("assistant", avatar="🤖"):
        placeholder = st.empty()
        placeholder.markdown(f"*{status_msg}*")
        try:
            stream_gen = _call_deepseek(api_messages, api_key, stream=True)
            buffer = ""
            thinking_done = False
            for chunk in stream_gen:
                buffer += chunk
                if not thinking_done:
                    # Tampilkan indikator bahwa AI sudah mulai menjawab (selesai berpikir)
                    thinking_done = True
                placeholder.markdown(buffer + "▌")
            placeholder.markdown(buffer)
            assistant_reply = buffer

        except requests.exceptions.Timeout:
            assistant_reply = _fallback_answer(ctx_data, user_input, intents, excel_files)
            assistant_reply += "\n\n⚠️ *AI timeout — jawaban dari data lokal. (R1 Reasoner butuh waktu lebih lama, coba lagi jika perlu.)*"
            placeholder.markdown(assistant_reply)

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            if code == 401:
                msg = "❌ API key tidak valid atau expired."
            elif code == 429:
                msg = "⚠️ Rate limit tercapai. Sudah dicoba ulang otomatis. Tunggu sebentar."
            elif code == 402:
                msg = "❌ Kredit DeepSeek habis. Isi ulang di platform.deepseek.com."
            else:
                msg = f"❌ Error API (HTTP {code})."
                if ctx_data:
                    msg += "\n\n" + _fallback_answer(ctx_data, user_input, intents, excel_files)
            assistant_reply = msg
            placeholder.error(msg)

        except Exception as e:
            assistant_reply = _fallback_answer(ctx_data, user_input, intents, excel_files)
            if not assistant_reply:
                assistant_reply = f"❌ Error: {e}"
            placeholder.markdown(assistant_reply)

    # ── Update memori konteks ────────────────────────────────────────
    if assistant_reply:
        _update_context_memory(user_input, assistant_reply, excel_files)

    # ── Simpan ke history ─────────────────────────────────────────────
    if assistant_reply:
        history.append({"role": "assistant", "content": assistant_reply})
        _set_history(history)