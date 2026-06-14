from __future__ import annotations

from dataclasses import dataclass

from backend.karma.vertical import ContentVertical, vertical_for_domain


@dataclass(frozen=True)
class TrendItem:
    slug: str
    title: str
    group: str
    description: str
    order: int


# Geriye uyumluluk
KarmaItem = TrendItem

TREND_COMPETITORS: dict[str, list[str]] = {
    "doviz.com": ["paratic.com", "foreks.com", "cnbce.com"],
    "www.doviz.com": ["paratic.com", "foreks.com", "cnbce.com"],
    "sinemalar.com": ["beyazperde.com", "birsenaltuntas.com", "rottentomatoes.com"],
    "www.sinemalar.com": ["beyazperde.com", "birsenaltuntas.com", "rottentomatoes.com"],
}
KARMA_COMPETITORS = TREND_COMPETITORS

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
        "rakip-gap",
        "Rakip Gap",
        "Keşif",
        "",
        3,
    ),
    TrendItem(
        "seasonality",
        "Seasonality",
        "Keşif",
        "",
        4,
    ),
    TrendItem(
        "anomaly-tree",
        "Anomaly Ağacı",
        "Keşif",
        "",
        5,
    ),
    TrendItem(
        "brief-generator",
        "Brief Generator",
        "Aksiyon",
        "",
        6,
    ),
    TrendItem(
        "headline-lab",
        "Headline Lab",
        "Aksiyon",
        "",
        7,
    ),
    TrendItem(
        "ic-link",
        "İç Link",
        "Aksiyon",
        "",
        8,
    ),
    TrendItem(
        "content-decay",
        "Content Decay",
        "Aksiyon",
        "",
        9,
    ),
    TrendItem(
        "topic-cluster",
        "Topic Cluster",
        "Aksiyon",
        "",
        10,
    ),
)

KARMA_ITEMS = TREND_ITEMS
TREND_BY_SLUG = {i.slug: i for i in TREND_ITEMS}
KARMA_BY_SLUG = TREND_BY_SLUG
TREND_GROUPS: tuple[str, ...] = tuple(dict.fromkeys(i.group for i in TREND_ITEMS))
KARMA_GROUPS = TREND_GROUPS

REFRESH_SEC = 30


def trend_competitors_for_domain(domain: str) -> list[str]:
    v = vertical_for_domain(domain)
    if v == ContentVertical.ENTERTAINMENT:
        return TREND_COMPETITORS["www.sinemalar.com"]
    if v == ContentVertical.FINANCE:
        return TREND_COMPETITORS["doviz.com"]
    return []


karma_competitors_for_domain = trend_competitors_for_domain
