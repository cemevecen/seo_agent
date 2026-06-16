"""
Google Sheets — sürüm güncellemeler tablosu (Platform / Versiyon / Build / Tarih).
"""
from __future__ import annotations

import csv
import io
import logging
import re
import time
import unicodedata
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from backend.services.backlink_csv import fetch_public_sheet_csv

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Europe/Istanbul")

# Döviz uygulaması — kullanıcı tablosu
DEFAULT_RELEASE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1Wx7ojwlZSQMmUYktB8HpQ5xTtvbl4_1wL1inY2vlm5U/edit#gid=0"
)

_PRODUCT_SHEET_URLS: dict[str, str] = {
    "doviz": DEFAULT_RELEASE_SHEET_URL,
}

_CACHE: dict[str, tuple[float, tuple[list[dict[str, Any]], list[dict[str, Any]]]]] = {}
_CACHE_TTL = 15 * 60

_TR_MONTHS: dict[str, int] = {
    "oca": 1,
    "sub": 2,
    "şub": 2,
    "mar": 3,
    "nis": 4,
    "may": 5,
    "haz": 6,
    "tem": 7,
    "agu": 8,
    "ağu": 8,
    "eyl": 9,
    "eki": 10,
    "kas": 11,
    "ara": 12,
}


def _norm_month_token(raw: str) -> str:
    s = unicodedata.normalize("NFKD", (raw or "").strip().lower())
    return "".join(c for c in s if not unicodedata.combining(c))


def parse_tr_release_datetime(text: str) -> datetime | None:
    """Örn. «5 Haz 2026 21:22» — Europe/Istanbul, UTC ISO için dönüştürülür."""
    s = (text or "").strip()
    if not s:
        return None
    m = re.match(
        r"^(\d{1,2})\s+([A-Za-zÇĞİÖŞÜçğıöşü]+)\s+(\d{4})\s+(\d{1,2}):(\d{2})$",
        s,
    )
    if not m:
        return None
    day = int(m.group(1))
    mon_raw = m.group(2)
    year = int(m.group(3))
    hour = int(m.group(4))
    minute = int(m.group(5))
    mon_key = _norm_month_token(mon_raw)
    month = _TR_MONTHS.get(mon_key)
    if not month:
        return None
    try:
        local = datetime(year, month, day, hour, minute, tzinfo=_IST)
    except ValueError:
        return None
    return local.astimezone(timezone.utc)


def _clean_cell(v: str | None) -> str:
    s = (v or "").strip()
    if s in ("—", "-", "–", ""):
        return ""
    return s


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_release_sheet_csv(csv_text: str, *, since: date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    since_dt = datetime(since.year, since.month, since.day, tzinfo=timezone.utc)
    ios: list[dict[str, Any]] = []
    android: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(csv_text or ""))
    for row in reader:
        platform = _clean_cell(row.get("Platform") or row.get("platform"))
        version = _clean_cell(row.get("Versiyon") or row.get("version"))
        build = _clean_cell(row.get("Build") or row.get("build"))
        tarih = _clean_cell(row.get("Tarih") or row.get("tarih") or row.get("date"))
        if not platform or not tarih:
            continue
        if not version:
            continue
        dt = parse_tr_release_datetime(tarih)
        if dt is None or dt < since_dt:
            continue
        rec: dict[str, Any] = {
            "version": version,
            "released_at": _iso_utc(dt),
            "source": "google_sheets",
        }
        if build:
            rec["build"] = build
        plat = platform.lower()
        if plat == "ios":
            ios.append(rec)
        elif plat == "android":
            android.append(rec)
    ios.sort(key=lambda x: x.get("released_at") or "")
    android.sort(key=lambda x: x.get("released_at") or "")
    return ios, android


def fetch_releases_from_sheet(
    product_id: str,
    *,
    since: date,
    use_cache: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pid = (product_id or "").strip().lower()
    url = _PRODUCT_SHEET_URLS.get(pid)
    if not url:
        return [], []

    cache_key = f"{pid}:{since.isoformat()}:{url}"
    if use_cache:
        hit = _CACHE.get(cache_key)
        if hit and time.time() - hit[0] <= _CACHE_TTL:
            return hit[1]

    try:
        csv_text = fetch_public_sheet_csv(url)
        ios, android = parse_release_sheet_csv(csv_text, since=since)
    except Exception as exc:
        logger.warning("App release sheet fetch failed (%s): %s", pid, exc)
        return [], []

    _CACHE[cache_key] = (time.time(), (ios, android))
    return ios, android
