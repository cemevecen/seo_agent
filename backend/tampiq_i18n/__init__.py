"""Load and merge TAMPIQ public locale catalogs (fallback: en-US)."""
from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.tampiq_i18n.locales import DEFAULT_LOCALE, normalize_locale

_CATALOG_DIR = Path(__file__).resolve().parent / "catalogs"


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = deepcopy(value)
    return out


@lru_cache(maxsize=16)
def _load_raw(locale: str) -> dict[str, Any]:
    path = _CATALOG_DIR / f"{locale}.json"
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_catalog(locale: str) -> dict[str, Any]:
    code = normalize_locale(locale) or DEFAULT_LOCALE
    base = _load_raw(DEFAULT_LOCALE)
    if code == DEFAULT_LOCALE:
        return deepcopy(base)
    overlay = _load_raw(code)
    if not overlay:
        return deepcopy(base)
    return _deep_merge(base, overlay)


def clear_catalog_cache() -> None:
    _load_raw.cache_clear()
