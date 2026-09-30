"""Locale codes, normalization, and Accept-Language resolution for TAMPIQ public site."""
from __future__ import annotations

from dataclasses import dataclass

SUPPORTED_LOCALES: tuple[str, ...] = (
    "tr",
    "en-US",
    "ar",
    "zh-Hant",
    "fr",
    "de",
    "hi",
    "ja",
    "pt-BR",
    "es-ES",
    "ur",
)

DEFAULT_LOCALE = "en-US"
COOKIE_NAME = "tampiq_public_locale"
STORAGE_KEY = "tampiq.public.locale"
RTL_LOCALES = frozenset({"ar", "ur"})

NATIVE_NAMES: dict[str, str] = {
    "tr": "Türkçe",
    "en-US": "English",
    "ar": "العربية",
    "zh-Hant": "繁體中文",
    "fr": "Français",
    "de": "Deutsch",
    "hi": "हिन्दी",
    "ja": "日本語",
    "pt-BR": "Português (Brasil)",
    "es-ES": "Español",
    "ur": "اردو",
}


@dataclass(frozen=True)
class LocaleInfo:
    code: str
    native_name: str
    dir: str  # "ltr" | "rtl"


def locale_info(code: str) -> LocaleInfo:
    loc = normalize_locale(code) or DEFAULT_LOCALE
    return LocaleInfo(
        code=loc,
        native_name=NATIVE_NAMES[loc],
        dir="rtl" if loc in RTL_LOCALES else "ltr",
    )


def normalize_locale(raw: str | None) -> str | None:
    if not raw:
        return None
    value = raw.strip().replace("_", "-")
    if not value:
        return None
    lower = value.lower()

    # Exact supported codes (case-insensitive except Hant / BR / US suffixes)
    for code in SUPPORTED_LOCALES:
        if lower == code.lower():
            return code

    primary = lower.split("-", 1)[0]

    if primary == "tr":
        return "tr"
    if primary == "en":
        return "en-US"
    if primary == "ar":
        return "ar"
    if primary == "fr":
        return "fr"
    if primary == "de":
        return "de"
    if primary == "hi":
        return "hi"
    if primary == "ja":
        return "ja"
    if primary == "ur":
        return "ur"
    if primary == "es":
        return "es-ES"
    if primary == "pt":
        if "br" in lower:
            return "pt-BR"
        return "pt-BR"
    if primary == "zh":
        if "hant" in lower or "tw" in lower or "hk" in lower or "mo" in lower:
            return "zh-Hant"
        # Simplified or bare zh → still map to Traditional for our supported set
        if "hans" in lower or "cn" in lower or "sg" in lower:
            return "zh-Hant"
        return "zh-Hant"
    return None


def parse_accept_language(header: str | None) -> str | None:
    if not header:
        return None
    # Example: "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7"
    parts: list[tuple[float, str]] = []
    for chunk in header.split(","):
        item = chunk.strip()
        if not item:
            continue
        if ";q=" in item:
            lang, q_raw = item.split(";q=", 1)
            try:
                q = float(q_raw.strip())
            except ValueError:
                q = 0.0
        else:
            lang, q = item, 1.0
        parts.append((q, lang.strip()))
    parts.sort(key=lambda x: x[0], reverse=True)
    for _, lang in parts:
        found = normalize_locale(lang)
        if found:
            return found
    return None


def resolve_locale(
    *,
    query_lang: str | None = None,
    cookie_lang: str | None = None,
    accept_language: str | None = None,
) -> str:
    for candidate in (
        normalize_locale(query_lang),
        normalize_locale(cookie_lang),
        parse_accept_language(accept_language),
    ):
        if candidate:
            return candidate
    return DEFAULT_LOCALE


def locale_selector_items(current: str) -> list[dict[str, str | bool]]:
    cur = normalize_locale(current) or DEFAULT_LOCALE
    return [
        {
            "code": code,
            "native_name": NATIVE_NAMES[code],
            "selected": code == cur,
            "dir": "rtl" if code in RTL_LOCALES else "ltr",
        }
        for code in SUPPORTED_LOCALES
    ]
