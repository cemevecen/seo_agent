from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KarmaItem:
    slug: str
    title: str
    group: str
    description: str
    order: int


KARMA_COMPETITORS: dict[str, list[str]] = {
    "doviz.com": ["paratic.com", "foreks.com", "cnbce.com"],
    "www.doviz.com": ["paratic.com", "foreks.com", "cnbce.com"],
    "sinemalar.com": ["beyazperde.com", "birsenaltuntas.com", "rottentomatoes.com"],
    "www.sinemalar.com": ["beyazperde.com", "birsenaltuntas.com", "rottentomatoes.com"],
}

KARMA_ITEMS: tuple[KarmaItem, ...] = (
    KarmaItem("trend-radar", "Trend Radar", "Trend", "Çok kaynaklı trend skoru ve editör önerileri", 1),
    KarmaItem("query-haber", "Query → Haber", "Trend", "GSC sorguları vs haber kapsamı", 2),
    KarmaItem("rakip-gap", "Rakip Gap", "Trend", "Rakip başlıkları vs sizin coverage", 3),
    KarmaItem("seasonality", "Seasonality", "Trend", "Mevsimsel içerik hazırlık takvimi", 4),
    KarmaItem("anomaly-tree", "Anomaly Ağacı", "Trend", "Alarm kök neden drill-down", 5),
    KarmaItem("brief-generator", "Brief Generator", "İçerik", "Trend kartından editoryal brief", 6),
    KarmaItem("headline-lab", "Headline Lab", "İçerik", "Başlık varyantları ve skor", 7),
    KarmaItem("ic-link", "İç Link", "İçerik", "Sayfa bazlı iç link önerileri", 8),
    KarmaItem("content-decay", "Content Decay", "İçerik", "Düşen içerik ve aksiyon", 9),
    KarmaItem("topic-cluster", "Topic Cluster", "İçerik", "Konu cluster authority haritası", 10),
    KarmaItem("push-roi", "Push ROI", "Dağıtım", "Push konuları ve geri dönüş", 11),
    KarmaItem("serp-tracker", "SERP Tracker", "Dağıtım", "Snippet ve SERP feature izleme", 12),
    KarmaItem("programmatic-seo", "Prog. SEO", "Dağıtım", "Şablon sayfa kalite guardrail", 13),
    KarmaItem("international", "International", "Dağıtım", "Dil / pazar fırsatları", 14),
    KarmaItem("editorial-sla", "Editoryal SLA", "Operasyon", "Trend → yayın gecikme takibi", 15),
    KarmaItem("war-room", "War Room", "Operasyon", "Kriz anı tek ekran", 16),
    KarmaItem("post-mortem", "Post-mortem", "Operasyon", "Spike sonrası otomatik rapor", 17),
    KarmaItem("morning-brief", "Sabah Brifingi", "Operasyon", "Role göre günlük özet", 18),
    KarmaItem("cwv-trafik", "CWV ↔ Trafik", "Kalite", "Core Web Vitals vs trafik", 19),
    KarmaItem("ai-action", "AI → Aksiyon", "Kalite", "AI Talk'tan issue / brief köprüsü", 20),
)

KARMA_BY_SLUG = {i.slug: i for i in KARMA_ITEMS}
KARMA_GROUPS: tuple[str, ...] = tuple(dict.fromkeys(i.group for i in KARMA_ITEMS))


def karma_competitors_for_domain(domain: str) -> list[str]:
    d = (domain or "").lower().strip()
    if "sinemalar" in d:
        return KARMA_COMPETITORS["www.sinemalar.com"]
    if "doviz" in d:
        return KARMA_COMPETITORS["doviz.com"]
    return []
