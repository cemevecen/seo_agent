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
        "Anlık haber, trafik, alarm ve rakip sinyallerini tek skorla birleştirir — son 30 dk kritik.",
        1,
    ),
    TrendItem(
        "query-haber",
        "Query → Haber",
        "Keşif",
        "Yükselen GSC sorgularını haber/intelligence ile eşleştirir; gap ve fırsat anında.",
        2,
    ),
    TrendItem(
        "rakip-gap",
        "Rakip Gap",
        "Keşif",
        "Rakip domainlerden geniş tarama — bizde olmayan başlıklar ve topic boşlukları.",
        3,
    ),
    TrendItem(
        "seasonality",
        "Seasonality",
        "Keşif",
        "Mevsimsel takvim + geçmiş alarm spike pattern — ne zaman hazırlık gerekir.",
        4,
    ),
    TrendItem(
        "anomaly-tree",
        "Anomaly Ağacı",
        "Keşif",
        "Realtime alarm → GA4 driver ağacı; web/mweb kök neden drill-down.",
        5,
    ),
    TrendItem(
        "brief-generator",
        "Brief Generator",
        "Aksiyon",
        "Kritik gap + trafik + GSC bağlamından acil editoryal brief.",
        6,
    ),
    TrendItem(
        "headline-lab",
        "Headline Lab",
        "Aksiyon",
        "Trend haberlerden SEO/CTR odaklı başlık varyantları — anlık skor.",
        7,
    ),
    TrendItem(
        "ic-link",
        "İç Link",
        "Aksiyon",
        "Anlık top sayfalar + yükselen sorgular → iç link kaynak/hedef eşlemesi.",
        8,
    ),
    TrendItem(
        "content-decay",
        "Content Decay",
        "Aksiyon",
        "GSC pozisyon kaybı + anlık trafik düşüşü birleşik decay skoru.",
        9,
    ),
    TrendItem(
        "topic-cluster",
        "Topic Cluster",
        "Aksiyon",
        "Haber + GSC + realtime sayfa cluster haritası — otorite boşlukları.",
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
    d = (domain or "").lower().strip()
    if "sinemalar" in d:
        return TREND_COMPETITORS["www.sinemalar.com"]
    if "doviz" in d:
        return TREND_COMPETITORS["doviz.com"]
    return []


karma_competitors_for_domain = trend_competitors_for_domain
