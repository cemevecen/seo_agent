"""Realtime «Haberler» sekmesi: yalnızca haber ana/kategori/detay URL'leri."""

from __future__ import annotations

import re
from urllib.parse import urlparse

from backend.services.ga4_page_urls import (
    _is_ga4_placeholder_path,
    ga4_canonical_page_url,
    ga4_site_host,
)

_HABER_CATEGORY_SEGMENT = re.compile(r"^[a-z0-9][a-z0-9-]*-haberleri$", re.I)
_HABERLERI_IN_PATH = re.compile(r"(?:^|/)[a-z0-9][a-z0-9-]*-haberleri(?:/|$)", re.I)
_NEWS_DETAIL_PATH_RE = re.compile(r"/\d+(?:[/?#].*)?$")


def is_news_detail_path(path: str) -> bool:
    """Son segment sayısal ID ise haber makalesi (GA4 landing / realtime ortak)."""
    return bool(_NEWS_DETAIL_PATH_RE.search(path or ""))


def _is_news_detail_path(path: str) -> bool:
    return is_news_detail_path(path)


def path_has_haberleri_segment(path: str) -> bool:
    """*-haberleri kategori / etiket / detay URL yapısı."""
    return bool(_HABERLERI_IN_PATH.search(_normalize_path(path) if path else ""))

# unifiedScreenName (path değil) — finans/canlı fiyat gürültüsü
_UNIFIED_TITLE_MARKET_FRAGMENTS: tuple[str, ...] = (
    "canlı ",
    "güncel ",
    "anlık ",
    " gram ",
    "gram-altin",
    " altın fiyat",
    "altın fiyat",
    " dolar ",
    " euro ",
    " sterlin ",
    " borsa ",
    " hisse ",
    " kripto",
    " çevirici",
    " serbest piyasa",
    " harem ",
    " vizyondaki",
    " filmler",
    " sinema",
    " en iyi ",
    " tüm filmler",
    " tüm zamanların",
    " çerez ",
    " odaci",
    " papara",
    " getirfinans",
)


def _normalize_path(path: str) -> str:
    p = (path or "").strip()
    if not p:
        return ""
    if p.startswith("http://") or p.startswith("https://"):
        p = urlparse(p).path or ""
    p = p.split("?", 1)[0].split("#", 1)[0].strip()
    if not p:
        return "/"
    if not p.startswith("/"):
        p = "/" + p
    if p != "/":
        p = p.rstrip("/")
    return p or "/"


def _haber_subdomain_path_ok(path: str) -> bool:
    """haber.doviz.com (ve aynı path yapısı): /, /xxx-haberleri, /xxx-haberleri/makale-slug[/id]."""
    p = _normalize_path(path)
    if p == "/":
        return True
    parts = [x for x in p.split("/") if x]
    if not parts:
        return True
    if len(parts) == 1:
        return bool(_HABER_CATEGORY_SEGMENT.match(parts[0]))
    # haber.doviz.com kökünde kategori öneksiz makale: /makale-slug/837872
    if not _HABER_CATEGORY_SEGMENT.match(parts[0]):
        if len(parts) == 2 and parts[-1].isdigit() and len(parts[0]) >= 3 and not parts[0].isdigit():
            return True
        return False
    rest = parts[1:]
    if rest and rest[-1].lower() == "amp":
        rest = rest[:-1]
    if not rest:
        return True
    if len(rest) == 1:
        slug = rest[0]
        return bool(slug) and not slug.isdigit() and len(slug) >= 2
    if len(rest) == 2 and rest[-1].isdigit():
        return len(rest[0]) >= 2
    if rest[-1].isdigit() and len(rest) >= 2:
        return len(rest[-2]) >= 2
    return all(len(s) >= 1 for s in rest)


def _path_on_haber_host(path: str, *, site_domain: str) -> bool:
    """Path, canonical host haber.doviz.com ise haber path kuralları."""
    pl = _normalize_path(path)
    if _is_ga4_placeholder_path(pl):
        return False
    site_h = ga4_site_host(site_domain) or ""
    probe_hosts: list[str] = []
    for h in (site_h, "www.doviz.com", "doviz.com", "m.doviz.com", "haber.doviz.com"):
        if h and h not in probe_hosts:
            probe_hosts.append(h)
    for host in probe_hosts:
        canon = ga4_canonical_page_url(host, pl)
        if not canon:
            continue
        parsed = urlparse(canon)
        ch = (parsed.hostname or "").lower()
        cp = _normalize_path(parsed.path or "/")
        if ch == "haber.doviz.com":
            return _haber_subdomain_path_ok(cp)
    return False


def is_realtime_news_path(path_or_label: str, *, site_domain: str = "") -> bool:
    """GA4 pagePath veya / ile başlayan unifiedScreenName."""
    raw = (path_or_label or "").strip()
    if not raw:
        return False
    if raw.lower().startswith("http://") or raw.lower().startswith("https://"):
        u = urlparse(raw)
        if (u.hostname or "").lower() == "haber.doviz.com":
            return _haber_subdomain_path_ok(u.path or "/")
        return is_realtime_news_path(u.path or "/", site_domain=site_domain)

    if not raw.startswith("/"):
        if "haber.doviz.com/" in raw.lower() or raw.lower().startswith("haber.doviz.com"):
            tail = raw.split("haber.doviz.com", 1)[-1].split("?", 1)[0]
            return _haber_subdomain_path_ok(tail or "/")
        return False

    pl = _normalize_path(raw)
    if _is_ga4_placeholder_path(pl):
        return False

    if _path_on_haber_host(pl, site_domain=site_domain):
        return True

    if path_has_haberleri_segment(pl) and _haber_subdomain_path_ok(pl):
        return True

    if is_news_detail_path(pl) and path_has_haberleri_segment(pl):
        return True

    if is_news_detail_path(pl) and _path_on_haber_host(pl, site_domain=site_domain):
        return True

    return False


def unified_screen_news_candidate(name: str, *, site_domain: str = "") -> bool:
    """Path yoksa yalnızca güvenilir haber başlık ipuçları; finans/canlı liste gürültüsünü ele."""
    n = (name or "").strip()
    if not n:
        return False
    if n.startswith("/"):
        return is_realtime_news_path(n, site_domain=site_domain)

    low = n.lower()
    if len(low) < 8:
        return False

    compact = low.replace(" ", "-")
    if "-haberleri" in compact or low.endswith(" haberleri") or " haberleri " in f" {low} ":
        return True

    for frag in _UNIFIED_TITLE_MARKET_FRAGMENTS:
        if frag in low:
            return False

    # Makale başlıkları (unifiedScreenName) — kategori adı değil, uzun metin
    return len(low) >= 10


def realtime_news_page_link(path_or_label: str, *, site_domain: str) -> str:
    raw = (path_or_label or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        if is_realtime_news_path(raw, site_domain=site_domain):
            return raw.split("?", 1)[0]
        return ""

    pl = _normalize_path(raw) if raw.startswith("/") else raw
    if not is_realtime_news_path(pl if pl.startswith("/") else "/" + pl, site_domain=site_domain):
        return ""

    if not pl.startswith("/"):
        pl = "/" + pl

    site_h = ga4_site_host(site_domain) or ""
    for host in (site_h, "www.doviz.com", "doviz.com", "m.doviz.com", "haber.doviz.com"):
        if not host:
            continue
        url = ga4_canonical_page_url(host, pl)
        if url and is_realtime_news_path(pl, site_domain=site_domain):
            return url
    if site_h:
        return f"https://{site_h}{pl}"
    return ""
