"""Investing.com borsa tatil takvimi — parse, sakla, panel context."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import HolidayCalendarCache

LOGGER = logging.getLogger(__name__)

CACHE_KEY = "current"
SOURCE_URL = "https://tr.investing.com/holiday-calendar/"
_DATE_TR_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")

# Investing .ceFlags sınıf adı → ISO 3166-1 alpha-2
_FLAG_CLASS_TO_ISO: dict[str, str] = {
    "USA": "us",
    "Germany": "de",
    "Argentina": "ar",
    "Australia": "au",
    "Austria": "at",
    "Bahrain": "bh",
    "Bangladesh": "bd",
    "Belgium": "be",
    "Bermuda": "bm",
    "Dubai": "ae",
    "UAE": "ae",
    "UK": "gb",
    "Bosnia": "ba",
    "Botswana": "bw",
    "Brazil": "br",
    "Bulgaria": "bg",
    "Denmark": "dk",
    "Ecuador": "ec",
    "Indonesia": "id",
    "Estonia": "ee",
    "Philippines": "ph",
    "Palestine": "ps",
    "Finland": "fi",
    "France": "fr",
    "South_Africa": "za",
    "South_Korea": "kr",
    "India": "in",
    "Netherlands": "nl",
    "Hong_Kong": "hk",
    "Croatia": "hr",
    "Jamaica": "jm",
    "Japan": "jp",
    "Canada": "ca",
    "Qatar": "qa",
    "Kazakhstan": "kz",
    "Kenya": "ke",
    "Colombia": "co",
    "Costa_Rica": "cr",
    "Cyprus": "cy",
    "Latvia": "lv",
    "Lithuania": "lt",
    "Lebanon": "lb",
    "Luxembourg": "lu",
    "Hungary": "hu",
    "Malaysia": "my",
    "Malta": "mt",
    "Mexico": "mx",
    "Mongolia": "mn",
    "Mauritius": "mu",
    "Namibia": "na",
    "Nigeria": "ng",
    "Norway": "no",
    "Pakistan": "pk",
    "Peru": "pe",
    "Poland": "pl",
    "Portugal": "pt",
    "Romania": "ro",
    "Rwanda": "rw",
    "Russian_Federation": "ru",
    "Russia": "ru",
    "Singapore": "sg",
    "Slovakia": "sk",
    "Slovenia": "si",
    "Sri_Lanka": "lk",
    "Tanzania": "tz",
    "Thailand": "th",
    "Taiwan": "tw",
    "Tunisia": "tn",
    "Ukraine": "ua",
    "Venezuela": "ve",
    "Vietnam": "vn",
    "New_Zealand": "nz",
    "Greece": "gr",
    "Zimbabwe": "zw",
    "Czech_Republic": "cz",
    "Jordan": "jo",
    "Ireland": "ie",
    "Spain": "es",
    "Sweden": "se",
    "Switzerland": "ch",
    "Italy": "it",
    "Iceland": "is",
    "Chile": "cl",
    "Turkey": "tr",
    "China": "cn",
    "Egypt": "eg",
    "Israel": "il",
    "Saudi_Arabia": "sa",
    "Kuwait": "kw",
    "Oman": "om",
    "Morocco": "ma",
    "Serbia": "rs",
}

# TR ülke adı → ISO (flag sınıfı yoksa yedek)
_COUNTRY_TR_TO_ISO: dict[str, str] = {
    "ABD": "us",
    "Almanya": "de",
    "Arjantin": "ar",
    "Avustralya": "au",
    "Avusturya": "at",
    "Bahreyn": "bh",
    "Bangladeş": "bd",
    "Belçika": "be",
    "Bermuda": "bm",
    "Birleşik Arap Emirlikleri": "ae",
    "Birleşik Krallık": "gb",
    "Bosna Hersek": "ba",
    "Botsvana": "bw",
    "Brezilya": "br",
    "Bulgaristan": "bg",
    "Danimarka": "dk",
    "Ekvador": "ec",
    "Endonezya": "id",
    "Estonya": "ee",
    "Filipinler": "ph",
    "Filistin Bölgesi": "ps",
    "Finlandiya": "fi",
    "Fransa": "fr",
    "Güney Afrika": "za",
    "Güney Kore": "kr",
    "Hindistan": "in",
    "Hollanda": "nl",
    "Hong Kong": "hk",
    "Hırvatistan": "hr",
    "Jamaika": "jm",
    "Japonya": "jp",
    "Kanada": "ca",
    "Katar": "qa",
    "Kazakistan": "kz",
    "Kenya": "ke",
    "Kolombiya": "co",
    "Kosta Rika": "cr",
    "Kıbrıs": "cy",
    "Letonya": "lv",
    "Litvanya": "lt",
    "Lübnan": "lb",
    "Lüksemburg": "lu",
    "Macaristan": "hu",
    "Malezya": "my",
    "Malta": "mt",
    "Meksika": "mx",
    "Mongolya": "mn",
    "Morityus": "mu",
    "Namibya": "na",
    "Nijerya": "ng",
    "Norveç": "no",
    "Pakistan": "pk",
    "Peru": "pe",
    "Polonya": "pl",
    "Portekiz": "pt",
    "Romanya": "ro",
    "Ruanda": "rw",
    "Rusya": "ru",
    "Singapur": "sg",
    "Slovakya": "sk",
    "Slovenya": "si",
    "Sri Lanka": "lk",
    "Tanzanya": "tz",
    "Tayland": "th",
    "Tayvan": "tw",
    "Tunus": "tn",
    "Ukrayna": "ua",
    "Venezuela": "ve",
    "Vietnam": "vn",
    "Yeni Zelanda": "nz",
    "Yunanistan": "gr",
    "Zimbabve": "zw",
    "Çek Cumhuriyeti": "cz",
    "Ürdün": "jo",
    "İrlanda": "ie",
    "İspanya": "es",
    "İsveç": "se",
    "İsviçre": "ch",
    "İtalya": "it",
    "İzlanda": "is",
    "Şili": "cl",
    "Türkiye": "tr",
}


def resolve_country_iso(*, country: str = "", country_flag: str = "") -> str:
    """Investing flag sınıfı veya TR ülke adından ISO2 (küçük harf)."""
    flag = (country_flag or "").strip()
    if flag in _FLAG_CLASS_TO_ISO:
        return _FLAG_CLASS_TO_ISO[flag]
    # bazen class "ceFlags USA" gibi gelebilir
    for part in flag.replace("-", "_").split():
        if part in _FLAG_CLASS_TO_ISO:
            return _FLAG_CLASS_TO_ISO[part]
    name = (country or "").strip()
    if name in _COUNTRY_TR_TO_ISO:
        return _COUNTRY_TR_TO_ISO[name]
    return ""


def flag_emoji(iso: str) -> str:
    cc = (iso or "").upper().strip()
    if len(cc) != 2 or not cc.isalpha():
        return ""
    return chr(0x1F1E6 + ord(cc[0]) - 65) + chr(0x1F1E6 + ord(cc[1]) - 65)


def flag_img_url(iso: str) -> str:
    code = (iso or "").lower().strip()
    if len(code) != 2 or not code.isalpha():
        return ""
    return f"https://flagcdn.com/16x12/{code}.png"


def parse_date_tr(value: str | None) -> str | None:
    """dd.mm.YYYY → YYYY-MM-DD."""
    raw = (value or "").strip()
    m = _DATE_TR_RE.match(raw)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def normalize_rows(raw_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_date_tr = ""
    last_iso = ""
    seen: set[tuple[str, str, str, str]] = set()
    for item in raw_rows or []:
        if not isinstance(item, dict):
            continue
        date_tr = str(item.get("date_tr") or "").strip()
        if date_tr:
            last_date_tr = date_tr
            last_iso = parse_date_tr(date_tr) or ""
        else:
            date_tr = last_date_tr
        country = str(item.get("country") or "").strip()
        exchange = str(item.get("exchange") or "").strip()
        holiday = str(item.get("holiday") or "").strip()
        if not date_tr or not country or not holiday:
            continue
        iso = parse_date_tr(date_tr) or last_iso
        key = (date_tr, country, exchange, holiday)
        if key in seen:
            continue
        seen.add(key)
        flag_class = str(item.get("country_flag") or "").strip()
        country_code = str(item.get("country_code") or "").strip().lower()
        if len(country_code) != 2:
            country_code = resolve_country_iso(country=country, country_flag=flag_class)
        out.append(
            {
                "date_tr": date_tr,
                "date_iso": iso,
                "country": country,
                "country_flag": flag_class,
                "country_code": country_code,
                "country_href": str(item.get("country_href") or "").strip(),
                "flag_emoji": flag_emoji(country_code),
                "flag_img": flag_img_url(country_code),
                "exchange": exchange,
                "holiday": holiday,
            }
        )
    out.sort(key=lambda r: (r.get("date_iso") or "9999", r.get("country") or "", r.get("exchange") or ""))
    return out


def save_holiday_payload(db: Session, payload: dict[str, Any], *, commit: bool = True) -> dict[str, Any]:
    rows = normalize_rows(payload.get("rows") if isinstance(payload, dict) else None)
    body = {
        "source": str((payload or {}).get("source") or SOURCE_URL),
        "country": str((payload or {}).get("country") or "all"),
        "date_from": str((payload or {}).get("date_from") or ""),
        "date_to": str((payload or {}).get("date_to") or ""),
        "fetched_at": str((payload or {}).get("fetched_at") or datetime.utcnow().isoformat()),
        "row_count": len(rows),
        "rows": rows,
        "countries": sorted({r["country"] for r in rows}),
    }
    row = db.get(HolidayCalendarCache, CACHE_KEY)
    now = datetime.utcnow()
    if row is None:
        row = HolidayCalendarCache(
            cache_key=CACHE_KEY,
            payload_json=json.dumps(body, ensure_ascii=False),
            updated_at=now,
        )
        db.add(row)
    else:
        row.payload_json = json.dumps(body, ensure_ascii=False)
        row.updated_at = now
    if commit:
        db.commit()
    return {"ok": True, "row_count": len(rows), "updated_at": now.isoformat()}


def load_holiday_payload(db: Session) -> dict[str, Any] | None:
    row = db.get(HolidayCalendarCache, CACHE_KEY)
    if row is None:
        return None
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at"] = row.updated_at.isoformat() if row.updated_at else ""
    payload["rows"] = normalize_rows(payload.get("rows"))
    payload["row_count"] = len(payload["rows"])
    return payload


def build_holidays_page_context(db: Session) -> dict[str, Any]:
    payload = load_holiday_payload(db) or {
        "rows": [],
        "row_count": 0,
        "countries": [],
        "fetched_at": "",
        "updated_at": "",
        "date_from": "",
        "date_to": "",
        "source": SOURCE_URL,
    }
    rows = list(payload.get("rows") or [])
    # Investing düzeni: aynı gün tekrarında tarih hücresi boş
    display_rows: list[dict[str, Any]] = []
    last_iso = ""
    for r in rows:
        iso = str(r.get("date_iso") or "")
        show_date = iso != last_iso
        if show_date:
            last_iso = iso
        display_rows.append(
            {
                **r,
                "show_date": show_date,
                "date_display": r.get("date_tr") if show_date else "",
            }
        )
    country_map: dict[str, dict[str, str]] = {}
    for r in rows:
        name = str(r.get("country") or "")
        if not name or name in country_map:
            continue
        code = str(r.get("country_code") or "")
        country_map[name] = {
            "name": name,
            "code": code,
            "flag_emoji": str(r.get("flag_emoji") or flag_emoji(code)),
            "flag_img": str(r.get("flag_img") or flag_img_url(code)),
        }
    countries = sorted(country_map.values(), key=lambda c: c["name"])
    return {
        "rows": display_rows,
        "row_count": len(display_rows),
        "countries": countries,
        "country_names": [c["name"] for c in countries],
        "country_count": len(countries),
        "fetched_at": payload.get("fetched_at") or payload.get("updated_at") or "",
        "updated_at": payload.get("updated_at") or "",
        "date_from": payload.get("date_from") or "",
        "date_to": payload.get("date_to") or "",
        "source_url": SOURCE_URL,
        "empty": not display_rows,
    }
