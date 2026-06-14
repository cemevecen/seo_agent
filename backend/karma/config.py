from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrendItem:
    slug: str
    title: str
    group: str
    description: str
    order: int


# Geriye uyumluluk
KarmaItem = TrendItem

TREND_ITEMS: tuple[TrendItem, ...] = (
    TrendItem(
        "trend-radar",
        "Trend Radar",
        "Keşif",
        "",
        1,
    ),
    TrendItem(
        "query-haber",
        "Query → Haber",
        "Keşif",
        "",
        2,
    ),
    TrendItem(
        "seasonality",
        "Seasonality",
        "Keşif",
        "",
        3,
    ),
    TrendItem(
        "anomaly-tree",
        "Anomaly Ağacı",
        "Keşif",
        "",
        4,
    ),
    TrendItem(
        "brief-generator",
        "Brief Generator",
        "Aksiyon",
        "",
        5,
    ),
    TrendItem(
        "headline-lab",
        "Headline Lab",
        "Aksiyon",
        "",
        6,
    ),
    TrendItem(
        "ic-link",
        "İç Link",
        "Aksiyon",
        "",
        7,
    ),
    TrendItem(
        "content-decay",
        "Content Decay",
        "Aksiyon",
        "",
        8,
    ),
    TrendItem(
        "topic-cluster",
        "Topic Cluster",
        "Aksiyon",
        "",
        9,
    ),
)

KARMA_ITEMS = TREND_ITEMS
TREND_BY_SLUG = {i.slug: i for i in TREND_ITEMS}
KARMA_BY_SLUG = TREND_BY_SLUG
TREND_GROUPS: tuple[str, ...] = tuple(dict.fromkeys(i.group for i in TREND_ITEMS))
KARMA_GROUPS = TREND_GROUPS

REFRESH_SEC = 30
