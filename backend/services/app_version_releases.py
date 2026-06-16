"""
Uygulama sürüm yayın tarihleri — iOS & Android (Google Sheet kaynaklı).

Kaynak: paylaşılan Google Sheet (sütunlar: Platform, Versiyon, Build, Tarih).
GA4 ve /ad grafiklerinde yatay (tarih) ekseninde sürüm noktaları olarak
gösterilir; üzerine gelindiğinde sürüm notu (versiyon/build/tarih) görünür.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SHEET_ID = "1Wx7ojwlZSQMmUYktB8HpQ5xTtvbl4_1wL1inY2vlm5U"
SHEET_GID = "0"
CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 6 * 60 * 60  # 6 saat

# Türkçe kısa ay adları -> ay numarası
_TR_MONTHS = {
    "oca": 1, "şub": 2, "sub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6,
    "tem": 7, "ağu": 8, "agu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12,
}


def _parse_tr_datetime(raw: str) -> tuple[str | None, str | None]:
    """'5 Haz 2026 21:22' -> ('2026-06-05', '2026-06-05 21:22').

    Döner: (date_iso, datetime_iso). Saat yoksa datetime_iso == date_iso.
    """
    s = (raw or "").strip()
    if not s:
        return None, None
    m = re.match(r"^(\d{1,2})\s+(\S+)\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?", s)
    if not m:
        return None, None
    day = int(m.group(1))
    mon = _TR_MONTHS.get(m.group(2).strip().lower())
    if not mon:
        return None, None
    year = int(m.group(3))
    date_iso = f"{year:04d}-{mon:02d}-{day:02d}"
    if m.group(4) is not None:
        return date_iso, f"{date_iso} {int(m.group(4)):02d}:{m.group(5)}"
    return date_iso, date_iso


def _platform_key(raw: str) -> str | None:
    s = (raw or "").strip().lower()
    if s.startswith("ios"):
        return "ios"
    if s.startswith("android"):
        return "android"
    return None


def _parse_csv(text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in csv.reader(io.StringIO(text)):
        if not row or len(row) < 4:
            continue
        plat = _platform_key(row[0])
        if not plat:
            continue  # başlık ya da boş satır
        version = (row[1] or "").strip()
        build = (row[2] or "").strip()
        if build in ("—", "-", ""):
            build = ""
        date_iso, dt_iso = _parse_tr_datetime(row[3])
        if not date_iso:
            continue
        label = ("iOS" if plat == "ios" else "Android") + " " + version
        if build:
            label += f" (build {build})"
        out.append(
            {
                "platform": plat,
                "version": version,
                "build": build,
                "date": date_iso,
                "datetime": dt_iso,
                "label": label.strip(),
            }
        )
    return out


def fetch_version_releases(force: bool = False) -> dict[str, Any]:
    """Sheet'i okuyup iOS/Android olarak ayrıştırılmış sürüm listesini döner."""
    if not force:
        hit = _CACHE.get("all")
        if hit and time.time() - hit[0] <= _CACHE_TTL:
            return hit[1]
    try:
        resp = httpx.get(CSV_URL, timeout=15.0, follow_redirects=True)
        resp.raise_for_status()
        items = _parse_csv(resp.text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Sürüm yayınları okunamadı: %s", exc)
        hit = _CACHE.get("all")
        if hit:
            return hit[1]
        return {"ios": [], "android": [], "count": 0, "updated_at": None, "error": str(exc)}

    ios = sorted((x for x in items if x["platform"] == "ios"), key=lambda r: r["date"])
    android = sorted((x for x in items if x["platform"] == "android"), key=lambda r: r["date"])
    data = {
        "ios": ios,
        "android": android,
        "count": len(items),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    _CACHE["all"] = (time.time(), data)
    return data
