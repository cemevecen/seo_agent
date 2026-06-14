"""Trend modülü — döviz (finans/ekonomi) vs sinemalar (eğlence) içerik ayrımı."""

from __future__ import annotations

from enum import Enum

from backend.models import NewsIntelligenceItem, Site


class ContentVertical(str, Enum):
    FINANCE = "finance"
    ENTERTAINMENT = "entertainment"


FINANCE_DOMAINS = frozenset({"doviz.com", "www.doviz.com"})
ENTERTAINMENT_DOMAINS = frozenset({"sinemalar.com", "www.sinemalar.com", "m.sinemalar.com"})

FINANCE_CATEGORIES = frozenset({"Finans & Borsa", "İş Dünyası", "Politika & Ekonomi", "Meteoroloji"})
ENTERTAINMENT_CATEGORIES = frozenset({"Sinema & Eğlence"})

# Ortak haber kategorileri — anahtar kelime skoru ile dikeye ayrılır.
SHARED_CATEGORIES = frozenset({"Türkiye", "Genel", "Dünya", "Teknoloji"})

FINANCE_MACRO_KEYWORDS = frozenset(
    {
        "döviz", "dolar", "euro", "eur", "usd", "try", "altın", "gümüş", "borsa", "bist",
        "faiz", "enflasyon", "tüfe", "üfe", "tcmb", "merkez bankası", "ekonomi", "finans",
        "hisse", "kur", "petrol", "doğalgaz", "enerji", "bitcoin", "kripto", "fed", "ecb",
        "piyasa", "tahvil", "ihracat", "ithalat", "yatırım", "ipo", "vergi", "bütçe",
        "cari açık", "gsyih", "işsizlik", "sanayi", "üretim", "tüketici", "konut", "kredi",
        "banka", "sigorta", "reel sektör", "para politikası", "resesyon", "büyüme",
    }
)

FINANCE_POLITICS_KEYWORDS = frozenset(
    {
        "politika", "siyaset", "seçim", "meclis", "bakan", "cumhurbaşkan", "hükümet",
        "ankara", "tbmm", "parti", "milletvekili", "kabine", "diplomasi", "diplomatik",
        "anlaşma", "yaptırım", "ambargo", "referandum", "koalisyon", "muhalefet",
        "dış politika", "iç politika", "güvenlik", "terör", "operasyon",
    }
)

FINANCE_WORLD_KEYWORDS = frozenset(
    {
        "dünya", "uluslararası", "global", "abd", "avrupa", "ab ", "nato", "bm ",
        "g7", "g20", "ukrayna", "rusya", "çin", "orta doğu", "israil", "filistin",
        "iran", "suriye", "taiwan", "taipei", "brexit", "trump", "biden", "putin",
        "savaş", "ateşkes", "barış", "kriz", "gündem",
    }
)

FINANCE_WEATHER_KEYWORDS = frozenset(
    {
        "hava durumu", "meteoroloji", "mgm", "kar yağış", "sel ", "fırtına", "deprem",
        "iklim", "sıcaklık", "don ", "kuraklık", "heyelan", "uçuş iptal", "yol kapalı",
    }
)

FINANCE_KEYWORDS = FINANCE_MACRO_KEYWORDS | FINANCE_POLITICS_KEYWORDS | FINANCE_WORLD_KEYWORDS | FINANCE_WEATHER_KEYWORDS

ENTERTAINMENT_KEYWORDS = frozenset(
    {
        "film", "sinema", "dizi", "vizyon", "fragman", "trailer", "oyuncu", "oyuncular",
        "yönetmen", "box office", "gişe", "netflix", "disney", "disney+", "hbo", "amazon prime",
        "exxen", "blutv", "gain", "puhutv", "tabii", "mubi", "max ", "apple tv",
        "oscar", "cannes", "emmy", "golden globe", "imdb", "yeşilçam", "streaming",
        "sezon", "bölüm", "cast", "premiere", "galası", "vizyona", "sinemalarda",
        "belgesel", "animasyon", "marvel", "dc ", "star wars", "dizi fragman",
        "platform", "ott", "yayın takvimi", "vizyon takvimi", "final sezon",
        "boxoffice", "sinema salon", "film festivali", "dizi platform",
    }
)

# Saf magazin / spor — finans dikeyinde elenir (ekonomi sinyali yoksa).
FINANCE_NOISE_KEYWORDS = frozenset(
    {
        "magazin", "dedikodu", "ünlü çift", "süper lig", "derbi", "transfer haberi",
        "futbol maçı", "basketbol maçı", "reality", "survivor", "masterchef",
    }
)

VERTICAL_LABELS = {
    ContentVertical.FINANCE: "Finans · Ekonomi · Politika · Dünya · Hava",
    ContentVertical.ENTERTAINMENT: "Sinema · Dizi · Platform · Vizyon",
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


def _intel_blob(row: NewsIntelligenceItem) -> str:
    return " ".join(part for part in (row.headline, row.topic, row.category, row.content) if part)


def _finance_signal(blob: str) -> int:
    return (
        _keyword_score(blob, FINANCE_MACRO_KEYWORDS)
        + _keyword_score(blob, FINANCE_POLITICS_KEYWORDS)
        + _keyword_score(blob, FINANCE_WORLD_KEYWORDS)
        + _keyword_score(blob, FINANCE_WEATHER_KEYWORDS)
    )


def intel_row_matches_vertical(row: NewsIntelligenceItem, vertical: ContentVertical) -> bool:
    cat = (row.category or "").strip()
    blob = _intel_blob(row)
    fin = _finance_signal(blob)
    ent = _keyword_score(blob, ENTERTAINMENT_KEYWORDS)
    noise = _keyword_score(blob, FINANCE_NOISE_KEYWORDS)
    wth = _keyword_score(blob, FINANCE_WEATHER_KEYWORDS)

    if vertical == ContentVertical.FINANCE:
        if cat in ENTERTAINMENT_CATEGORIES:
            return False
        if cat in FINANCE_CATEGORIES:
            return True
        if noise >= 1 and fin == 0:
            return False
        if ent >= 2 and ent > fin:
            return False
        if fin >= 1:
            return True
        if cat in SHARED_CATEGORIES:
            return ent == 0
        return cat not in ENTERTAINMENT_CATEGORIES

    # Entertainment
    if wth >= 1 and ent == 0 and fin == 0:
        return False
    if cat in ENTERTAINMENT_CATEGORIES:
        return True
    if cat in FINANCE_CATEGORIES and ent == 0:
        return False
    if ent >= 1:
        return True
    if fin >= 3 and ent == 0:
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
