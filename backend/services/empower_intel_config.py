"""Empower Intelligence — panel Metrik sekmesi sütun / etiket tanımları."""

from __future__ import annotations

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
