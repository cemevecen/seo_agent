"""Empower Intelligence — panel Metrik sekmesi sütun / etiket tanımları."""

from __future__ import annotations

import os

PLATFORMS: tuple[tuple[str, str], ...] = (
    ("web", "Web"),
    ("mweb", "Mobile Web"),
    ("ios", "iOS"),
    ("android", "Android"),
)

# iOS / Android — Empower Columns (ilk set)
APP_COLUMNS: tuple[str, ...] = (
    "view",
    "match",
    "usdSpent",
    "usdEcpm",
    "request",
    "active1DayUsers",
    "active7DayUsers",
    "dauPerMau",
    "appVersion",
    "arpdauTry",
    "arpdauUsd",
    "averageSessionDuration",
    "bounceRate",
    "crashAffectedUsers",
    "crashFreeUsersRate",
    "engagementRate",
    "impdau",
    "is_holiday",
    "newUsers",
    "rpiUsd",
    "rpmTry",
    "rpsTry",
    "sessions",
    "totalUsers",
    "tryEcpm",
    "trySpent",
    "userEngagementDuration",
    "avgEngagementTimePerUser",
    "avgEngagementTimePerSession",
)

# Web / mweb — Empower Columns (ikinci set)
WEB_COLUMNS: tuple[str, ...] = (
    "view",
    "match",
    "usdSpent",
    "usdEcpm",
    "request",
    "active1DayUsers",
    "active7DayUsers",
    "dauPerMau",
    "avgEngagementTimePerUser",
    "avgEngagementTimePerSession",
    "arpdauTry",
    "arpdauUsd",
    "averageSessionDuration",
    "bounceRate",
    "engagementRate",
    "impdau",
    "newUsers",
    "organicGoogleSearchClicks",
    "organicGoogleSearchClickThroughRate",
    "rpmTry",
    "rpmUsd",
    "screenPageViews",
    "screenPageViewsPerSession",
    "screenPageViewsPerUser",
    "sessions",
    "totalUsers",
    "tryEcpm",
    "trySpent",
    "userEngagementDuration",
)

COLUMNS_BY_PLATFORM: dict[str, tuple[str, ...]] = {
    "web": WEB_COLUMNS,
    "mweb": WEB_COLUMNS,
    "ios": APP_COLUMNS,
    "android": APP_COLUMNS,
}

METRIC_LABELS: dict[str, str] = {
    "view": "Impression",
    "match": "Match",
    "usdSpent": "Revenue ($)",
    "usdEcpm": "eCPM ($)",
    "request": "Requests",
    "active1DayUsers": "DAU (1 Day)",
    "active7DayUsers": "DAU (7 Days)",
    "dauPerMau": "DAU per MAU",
    "appVersion": "App Version",
    "arpdauTry": "ARPDAU (₺)",
    "arpdauUsd": "ARPDAU ($)",
    "averageSessionDuration": "Average Session Duration",
    "bounceRate": "Bounce Rate",
    "crashAffectedUsers": "Crash Affected Users",
    "crashFreeUsersRate": "Crash Free Users Rate",
    "engagementRate": "Engagement Rate",
    "impdau": "IMPDAU",
    "is_holiday": "Is Holiday",
    "newUsers": "New Users",
    "rpiUsd": "RPI ($)",
    "rpmTry": "RPM (₺)",
    "rpmUsd": "RPM ($)",
    "rpsTry": "RPS (₺)",
    "sessions": "Sessions",
    "totalUsers": "Total Users",
    "tryEcpm": "eCPM (₺)",
    "trySpent": "Revenue (₺)",
    "userEngagementDuration": "User Engagement Duration",
    "avgEngagementTimePerUser": "Avg Engagement Time / User",
    "avgEngagementTimePerSession": "Avg Engagement Time / Session",
    "organicGoogleSearchClicks": "Organic Google Search Clicks",
    "organicGoogleSearchClickThroughRate": "Organic Google Search CTR",
    "screenPageViews": "Screen/Page Views",
    "screenPageViewsPerSession": "Views per Session",
    "screenPageViewsPerUser": "Views per User",
}


def columns_for_platform(platform: str) -> list[str]:
    p = (platform or "").strip().lower()
    return list(COLUMNS_BY_PLATFORM.get(p) or WEB_COLUMNS)


def meta_payload() -> dict:
    return {
        "project": "doviz",
        "platforms": [{"id": pid, "label": label} for pid, label in PLATFORMS],
        "columns_by_platform": {k: list(v) for k, v in COLUMNS_BY_PLATFORM.items()},
        "labels": dict(METRIC_LABELS),
    }


# Play Console / App Store Connect scrape skip — Metrik (Empower) aynı KPI'yı tutuyor.
# Mağaza gösterim / IAP gelir / vitals kilitlenme sayısı burada yok (farklı kaynak).
# Geri açmak: STORE_SCRAPE_KEEP_EMPOWER_OVERLAP=1

PLAY_CONSOLE_SKIP_METRIC_KEYS: frozenset[str] = frozenset(
    {
        "dau",
        "dau_mau",
        "active_users",
    }
)
PLAY_CONSOLE_SKIP_KNOWN_TITLES: frozenset[str] = frozenset(
    {
        "Günlük etkin kullanıcı sayısı",
        "Günlük etkin kullanıcı",
        "Daily active users",
        "Daily active user",
        "DAU",
        "MAU",
        "DAU/MAU",
        "Aylık etkin kullanıcı sayısı",
        "Etkin kullanıcılar",
        "Active users",
        "Active user",
    }
)
ASC_CONSOLE_SKIP_MEASURE_KEYS: frozenset[str] = frozenset({"sessions"})
ASC_CONSOLE_SKIP_WAREHOUSE_METRICS: frozenset[str] = frozenset({"sessions"})

STORE_EMPOWER_OVERLAP: tuple[dict[str, str], ...] = (
    {
        "empower": "DAU (1 Day)",
        "empower_key": "active1DayUsers",
        "play": "Günlük etkin kullanıcı",
        "play_key": "dau",
        "asc": "",
    },
    {
        "empower": "DAU per MAU",
        "empower_key": "dauPerMau",
        "play": "DAU/MAU",
        "play_key": "dau_mau",
        "asc": "",
    },
    {
        "empower": "DAU (1 Day) / Total Users",
        "empower_key": "active1DayUsers",
        "play": "Etkin kullanıcılar",
        "play_key": "active_users",
        "asc": "",
    },
    {
        "empower": "Sessions",
        "empower_key": "sessions",
        "play": "",
        "play_key": "",
        "asc": "Sessions",
        "asc_key": "sessions",
    },
)


def store_scrape_keep_empower_overlap() -> bool:
    v = (os.environ.get("STORE_SCRAPE_KEEP_EMPOWER_OVERLAP") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def play_console_skip_metric_keys() -> frozenset[str]:
    if store_scrape_keep_empower_overlap():
        return frozenset()
    return PLAY_CONSOLE_SKIP_METRIC_KEYS


def play_console_skip_known_titles() -> frozenset[str]:
    if store_scrape_keep_empower_overlap():
        return frozenset()
    return PLAY_CONSOLE_SKIP_KNOWN_TITLES


def asc_console_skip_measure_keys() -> frozenset[str]:
    if store_scrape_keep_empower_overlap():
        return frozenset()
    return ASC_CONSOLE_SKIP_MEASURE_KEYS


def asc_console_skip_warehouse_metrics() -> frozenset[str]:
    if store_scrape_keep_empower_overlap():
        return frozenset()
    return ASC_CONSOLE_SKIP_WAREHOUSE_METRICS
