"""GA4 mobil — event + custom parameter kırılım panelleri (iOS screen_view, Android news_detail_opened)."""

from __future__ import annotations

from typing import Any, TypedDict


class AppEventParamSection(TypedDict, total=False):
    param: str
    param2: str
    label: str
    alt_params: list[str]
    alt_params_2: list[str]
    combined: bool


class AppEventDetailProfile(TypedDict):
    event_name: str
    title: str
    sections: list[AppEventParamSection]


# Doviz mobil — GA4 property'de kayıtlı custom dimension'lar ile uyumlu parametreler.
# iOS: screen_view + news_* (firebase_screen GA4'te custom dimension değil → unifiedScreenName).
APP_EVENT_DETAIL_BY_PROFILE: dict[str, AppEventDetailProfile] = {
    "ios": {
        "event_name": "screen_view",
        "title": "screen_view — haber ve ekran",
        "sections": [
            {
                "param": "news_id",
                "param2": "news_title",
                "label": "Haberler",
                "combined": True,
                "alt_params": ["newsId"],
                "alt_params_2": ["newsTitle"],
            },
            {"param": "unifiedScreenName", "label": "Ekranlar"},
            {"param": "from", "label": "from (kaynak)"},
        ],
    },
    "android": {
        "event_name": "news_detail_opened",
        "title": "news_detail_opened — haber açılışı",
        "sections": [
            {
                "param": "news_id",
                "param2": "news_title",
                "label": "Haberler",
                "combined": True,
                "alt_params": ["newsId"],
                "alt_params_2": ["newsTitle"],
            },
            {"param": "from", "label": "from (kaynak)"},
        ],
    },
}


def app_event_detail_config(profile: str) -> AppEventDetailProfile | None:
    key = (profile or "").strip().lower()
    cfg = APP_EVENT_DETAIL_BY_PROFILE.get(key)
    return cfg if cfg else None


def app_event_detail_tab_label(profile: str) -> str:
    cfg = app_event_detail_config(profile)
    if not cfg:
        return "Event detay"
    if profile in ("ios", "android"):
        return "Haber detay"
    return cfg.get("event_name") or "Event detay"
