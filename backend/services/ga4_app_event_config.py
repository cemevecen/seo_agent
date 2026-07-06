"""GA4 mobil — event + custom parameter kırılım panelleri (iOS screen_view, Android news_detail_opened)."""

from __future__ import annotations

from typing import Any, TypedDict


class AppEventParamSection(TypedDict, total=False):
    param: str
    param2: str
    label: str
    alt_params: list[str]
    combined: bool


class AppEventDetailProfile(TypedDict):
    event_name: str
    title: str
    sections: list[AppEventParamSection]


# Doviz mobil — GA4 UI event drill-down ile uyumlu parametreler.
APP_EVENT_DETAIL_BY_PROFILE: dict[str, AppEventDetailProfile] = {
    "ios": {
        "event_name": "screen_view",
        "title": "screen_view — ekran ve içerik parametreleri",
        "sections": [
            {"param": "firebase_screen", "label": "firebase_screen (ekran adı)"},
            {"param": "news_title", "label": "news_title (haber başlığı)"},
            {"param": "from", "label": "from (navigasyon kaynağı)"},
            {"param": "search_text", "label": "search_text (arama metni)"},
            {"param": "asset_key", "label": "asset_key (varlık)"},
            {"param": "action", "label": "action"},
            {"param": "category", "label": "category"},
            {
                "param": "news_id",
                "param2": "news_title",
                "label": "news_id + news_title (haber)",
                "combined": True,
                "alt_params": ["newsId"],
            },
        ],
    },
    "android": {
        "event_name": "news_detail_opened",
        "title": "news_detail_opened — haber açılışı",
        "sections": [
            {"param": "news_id", "label": "News ID", "alt_params": ["newsId", "newsID"]},
            {"param": "news_title", "label": "News Title", "alt_params": ["newsTitle"]},
            {
                "param": "news_id",
                "param2": "news_title",
                "label": "News ID + başlık",
                "combined": True,
                "alt_params": ["newsId"],
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
    if profile == "ios":
        return "screen_view"
    if profile == "android":
        return "Haber detay"
    return cfg.get("event_name") or "Event detay"
