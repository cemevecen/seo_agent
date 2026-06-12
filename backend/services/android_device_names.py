"""Android cihaz model kodlarını pazarlama adına çevirir (Google Play supported devices)."""

from __future__ import annotations

import csv
import io
import logging
import re
import threading
from typing import Any

logger = logging.getLogger(__name__)

_SUPPORTED_DEVICES_URL = (
    "https://storage.googleapis.com/play_public/supported_devices.csv"
)
_CACHE: dict[str, str] | None = None
_CACHE_LOCK = threading.Lock()

# Sık görülen modeller — CSV yüklenemezse fallback
_FALLBACK: dict[str, str] = {
    "SM-A515F": "Galaxy A51",
    "SM-A325F": "Galaxy A32",
    "SM-A536B": "Galaxy A53 5G",
    "SM-G991B": "Galaxy S21 5G",
    "SM-G998B": "Galaxy S21 Ultra 5G",
    "SM-N770F": "Galaxy Note 10 Lite",
    "SM-N9750": "Galaxy Note 10+",
    "SM-P610": "Galaxy Tab S6 Lite",
    "SM-M146B": "Galaxy M14 5G",
    "SM-F966B": "Galaxy Z Fold7",
    "G318": "GM 9",
    "KM9": "Camon 30 5G",
    "2303CRA44A": "Redmi 12C",
    "2209116AG": "Redmi Note 12 Pro 4G",
    "M2101K7AG": "Redmi Note 10 5G",
    "2201116TG": "Redmi Note 11 Pro 5G",
    "VCE-L22": "nova Y70",
    "JAD-LX9": "P50 Pro",
    "VOG-TL00": "P30 Pro",
    "VOG-L29": "P30 Pro",
    "Mi 10 Pro": "Mi 10 Pro",
    "Redmi Note 8 Pro": "Redmi Note 8 Pro",
}

_IOS_MODELS: dict[str, str] = {
    "IPHONE14,2": "iPhone 13 Pro",
    "IPHONE14,3": "iPhone 13 Pro Max",
    "IPHONE14,4": "iPhone 13 mini",
    "IPHONE14,5": "iPhone 13",
    "IPHONE15,2": "iPhone 14 Pro",
    "IPHONE15,3": "iPhone 14 Pro Max",
    "IPHONE15,4": "iPhone 14",
    "IPHONE15,5": "iPhone 14 Plus",
    "IPHONE16,1": "iPhone 15 Pro",
    "IPHONE16,2": "iPhone 15 Pro Max",
    "IPHONE17,1": "iPhone 16 Pro",
    "IPHONE17,2": "iPhone 16 Pro Max",
}


def _norm_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def _parse_csv(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    reader = csv.reader(io.StringIO(text, newline=""), strict=False)
    header = next(reader, None)
    if not header:
        return out
    # Retail Branding, Marketing Name, Device, Model
    for row in reader:
        if len(row) < 4:
            continue
        brand, marketing, _device, model = [c.strip() for c in row[:4]]
        if not model or not marketing:
            continue
        label = marketing
        if brand and brand.lower() not in marketing.lower():
            label = f"{brand} {marketing}"
        key = _norm_key(model)
        if key and key not in out:
            out[key] = label
    return out


def _load_mappings() -> dict[str, str]:
    global _CACHE
    with _CACHE_LOCK:
        if _CACHE is not None:
            return _CACHE
        merged = {_norm_key(k): v for k, v in _FALLBACK.items()}
        try:
            import urllib.request

            req = urllib.request.Request(
                _SUPPORTED_DEVICES_URL,
                headers={"User-Agent": "seo-agent-crashlytics/1.0"},
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
            if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
                text = raw.decode("utf-16")
            else:
                text = raw.decode("utf-8", errors="replace")
            parsed = _parse_csv(text)
            merged.update(parsed)
            logger.info("Android cihaz adları yüklendi: %d model", len(merged))
        except Exception as exc:
            logger.warning("supported_devices.csv yüklenemedi, fallback kullanılıyor: %s", exc)
        _CACHE = merged
        return _CACHE


def lookup_marketing_name(model: str) -> str | None:
    key = _norm_key(model)
    if not key:
        return None
    return _load_mappings().get(key)


def _title_brand(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    aliases = {
        "samsung": "Samsung",
        "xiaomi": "Xiaomi",
        "huawei": "Huawei",
        "honor": "Honor",
        "oppo": "OPPO",
        "vivo": "vivo",
        "google": "Google",
        "oneplus": "OnePlus",
        "realme": "realme",
        "motorola": "Motorola",
        "nokia": "Nokia",
        "tecno": "TECNO",
        "infinix": "Infinix",
    }
    low = n.lower()
    return aliases.get(low, n[:1].upper() + n[1:] if n else n)


def parse_device_label(label: str) -> tuple[str, str]:
    """'samsung SM-A515F' → ('samsung', 'SM-A515F')."""
    s = (label or "").strip()
    if not s:
        return "", ""
    parts = s.split(None, 1)
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], parts[1]


def infer_platform_from_label(label: str) -> str:
    low = (label or "").lower()
    if "iphone" in low or "ipad" in low:
        return "ios"
    return "android"


def friendly_device_label(
    manufacturer: str,
    model: str,
    *,
    platform: str = "android",
) -> str:
    """Crashlytics device satırı için okunabilir etiket."""
    man = (manufacturer or "").strip()
    mod = (model or "").strip()
    if not mod or mod in ("—", "bilinmiyor"):
        return man or "bilinmiyor"

    plat = (platform or "android").lower()
    if plat == "ios":
        ios_name = _IOS_MODELS.get(_norm_key(mod))
        if ios_name:
            return ios_name
        if mod.lower().startswith("iphone"):
            return mod.replace(",", ", ")
        return f"{man} {mod}".strip() if man else mod

    marketing = lookup_marketing_name(mod)
    if not marketing and man:
        marketing = lookup_marketing_name(f"{man}{mod}")
    if marketing:
        brand = _title_brand(man)
        if brand and brand.lower() not in marketing.lower():
            return f"{brand} {marketing}"
        return marketing
    combined = f"{_title_brand(man)} {mod}".strip() if man else mod
    return combined or "bilinmiyor"


def friendly_breakdown_row(row: dict[str, Any], *, platform: str | None = None) -> dict[str, Any]:
    """Cache'teki ham etiketleri de pazarlama adına çevirir."""
    raw = (row.get("label_raw") or row.get("label") or "").strip()
    man = (row.get("manufacturer") or "").strip()
    mod = (row.get("model") or "").strip()
    if not mod and raw:
        man, mod = parse_device_label(raw)
    if not man and not mod:
        return row
    plat = platform or infer_platform_from_label(raw or mod)
    friendly = friendly_device_label(man, mod, platform=plat)
    if not friendly or friendly.lower() == (row.get("label") or "").lower():
        return row
    out = dict(row)
    out["label"] = friendly
    if raw and raw.lower() != friendly.lower():
        out["label_raw"] = raw
    return out


def apply_device_friendly_labels(data: dict[str, Any], platform: str = "all") -> dict[str, Any]:
    """Payload'daki cihaz kırılımını okunabilir adlara çevir (cache uyumlu)."""
    if not data or not data.get("ok"):
        return data
    plat_hint = platform if platform in ("ios", "android") else None
    out = dict(data)

    def _map_rows(rows: list[dict] | None) -> list[dict]:
        if not rows:
            return []
        return [friendly_breakdown_row(r, platform=plat_hint) for r in rows]

    if data.get("device_breakdown"):
        out["device_breakdown"] = _map_rows(data["device_breakdown"])
    by_plat = data.get("device_breakdown_by_platform") or {}
    if by_plat:
        out["device_breakdown_by_platform"] = {
            p: [friendly_breakdown_row(r, platform=p) for r in rows]
            for p, rows in by_plat.items()
        }
    return out


def enrich_device_row(row: dict[str, Any], *, platform: str) -> dict[str, Any]:
    man = row.get("manufacturer") or ""
    mod = row.get("model") or ""
    raw = f"{man} {mod}".strip() or "bilinmiyor"
    friendly = friendly_device_label(man, mod, platform=platform)
    return {
        **row,
        "label": friendly,
        "label_raw": raw if friendly.lower() != raw.lower() else None,
    }
