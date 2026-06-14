"""Trend modülü — döviz (finans/ekonomi) vs sinemalar (eğlence) içerik ayrımı."""

from __future__ import annotations

from enum import Enum

from backend.models import NewsIntelligenceItem, Site


class ContentVertical(str, Enum):
    FINANCE = "finance"
    ENTERTAINMENT = "entertainment"


FINANCE_DOMAINS = frozenset({"doviz.com", "www.doviz.com"})
ENTERTAINMENT_DOMAINS = frozenset({"sinemalar.com", "www.sinemalar.com", "m.sinemalar.com"})

FINANCE_CATEGORIES = frozenset({"Finans & Borsa", "İş Dünyası"})
ENTERTAINMENT_CATEGORIES = frozenset({"Sinema & Eğlence"})

# Ortak haber kategorileri — anahtar kelime skoru ile dikeye ayrılır.
SHARED_CATEGORIES = frozenset({"Türkiye", "Genel", "Dünya", "Teknoloji"})

FINANCE_KEYWORDS = frozenset(
    {
        "döviz", "dolar", "euro", "eur", "usd", "try", "altın", "gümüş", "borsa", "bist",
        "faiz", "enflasyon", "tüfe", "üfe", "tcmb", "merkez bankası", "ekonomi", "finans",
        "hisse", "kur", "petrol", "bitcoin", "kripto", "fed", "ecb", "piyasa", "tahvil",
        "politika", "seçim", "meclis", "bakan", "cumhurbaşkan", "hükümet", "ankara",
        "ihracat", "ithalat", "yatırım", "ipo", "vergi", "bütçe", "cari açık",
    }
)

ENTERTAINMENT_KEYWORDS = frozenset(
    {
        "film", "sinema", "dizi", "vizyon", "fragman", "trailer", "oyuncu", "oyuncular",
        "yönetmen", "box office", "gişe", "netflix", "disney", "hbo", "amazon prime",
        "oscar", "cannes", "emmy", "golden globe", "imdb", "yeşilçam", "platform",
        "sezon", "bölüm", "cast", "premiere", "galası", "vizyona", "sinemalarda",
        "belgesel", "animasyon", "marvel", "dc ", "star wars", "dizi fragman",
    }
)

VERTICAL_LABELS = {
    ContentVertical.FINANCE: "Finans · Ekonomi · Politika",
    ContentVertical.ENTERTAINMENT: "Sinema · Dizi · Eğlence",
}


def normalize_domain(domain: str | None) -> str:
    d = str(domain or "").strip().lower()
    if d.startswith("http://"):
        d = d[7:]
    if d.startswith("https://"):
        d = d[8:]
    return d.strip("/").split("/")[0]


def vertical_for_domain(domain: str | None) -> ContentVertical | None:
    d = normalize_domain(domain)
    if d in FINANCE_DOMAINS:
        return ContentVertical.FINANCE
    if d in ENTERTAINMENT_DOMAINS:
        return ContentVertical.ENTERTAINMENT
    return None


def vertical_for_site(site: Site | None) -> ContentVertical | None:
    if site is None:
        return None
    return vertical_for_domain(site.domain)


def _keyword_score(text: str, keywords: frozenset[str]) -> int:
    low = (text or "").lower()
    return sum(1 for kw in keywords if kw in low)


def intel_row_matches_vertical(row: NewsIntelligenceItem, vertical: ContentVertical) -> bool:
    cat = (row.category or "").strip()
    blob = " ".join(
        part
        for part in (row.headline, row.topic, row.category, row.content)
        if part
    )
    fin = _keyword_score(blob, FINANCE_KEYWORDS)
    ent = _keyword_score(blob, ENTERTAINMENT_KEYWORDS)

    if vertical == ContentVertical.FINANCE:
        if cat in ENTERTAINMENT_CATEGORIES:
            return False
        if cat in FINANCE_CATEGORIES:
            return True
        if ent >= 2 and ent > fin:
            return False
        if fin >= 1:
            return True
        if cat in SHARED_CATEGORIES:
            return ent == 0
        return cat not in ENTERTAINMENT_CATEGORIES

    if cat in ENTERTAINMENT_CATEGORIES:
        return True
    if cat in FINANCE_CATEGORIES and ent == 0:
        return False
    if ent >= 1:
        return True
    if fin >= 2 and ent == 0:
        return False
    return ent > fin


def headline_variants(base: str, vertical: ContentVertical | None, *, age_m: float) -> list[str]:
    base = (base or "").strip()
    if not base:
        return []
    if vertical == ContentVertical.ENTERTAINMENT:
        return [
            base,
            f"Vizyonda: {base}" if len(base) < 58 else base,
            f"{base} — Fragman ve detaylar",
            f"{base.rstrip('.!')} | Oyuncu kadrosu ve özet",
            f"{base} ({age_m:.0f} dk önce)" if age_m < 180 else base,
        ]
    return [
        base,
        f"SON DAKİKA: {base}" if len(base) < 65 else base,
        f"{base} | Güncel gelişmeler",
        base.rstrip(".!") + " — Detaylar ve analiz",
        f"{base} ({age_m:.0f} dk önce)" if age_m < 120 else base,
    ]


def brief_internal_links_hint(vertical: ContentVertical | None) -> str:
    if vertical == ContentVertical.ENTERTAINMENT:
        return "Vizyon takvimi + film/dizi hub + ilgili liste"
    return "Ana sayfa + kategori + canlı widget"


def brief_deadline_label(urgency: str, age_m: float, vertical: ContentVertical | None) -> str:
    mins = max(15, int(90 - age_m))
    if vertical == ContentVertical.ENTERTAINMENT:
        if urgency == "ACİL":
            return f"{mins} dk içinde vizyon/fragman sayfası"
        return f"{mins} dk içinde liste veya inceleme güncellemesi"
    if urgency == "ACİL":
        return f"{mins} dk içinde yayın hedefi"
    return f"{mins} dk içinde haber güncellemesi"
