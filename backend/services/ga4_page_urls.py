"""GA4 landing satırları için tam URL: hostName + path (site.domain ile tahminden kaçınır)."""

from __future__ import annotations

import re
from urllib.parse import quote


def _host_cmp_key(host: str) -> str:
    h = host.lower().strip()
    if h.startswith("www."):
        return h[4:]
    return h


def ga4_site_host(domain: str | None) -> str | None:
    d = (domain or "").strip()
    if not d:
        return None
    d = d.lower().lstrip("http://").lstrip("https://").split("/")[0].rstrip("/")
    return d or None


_DOVIZ_MAIN_HOSTS = frozenset({"www.doviz.com", "doviz.com", "m.doviz.com"})
_DOVIZ_ALTIN_HOST = "altin.doviz.com"
# www.doviz.com'da 404; yalnızca altin.doviz.com kökünde olan tek segmentli yollar (kur ile çakışmaz)
_DOVIZ_ALTIN_ONLY_ROOT_SLUGS = frozenset({"altinkaynak"})

# GA4 boyut yer tutucuları — tıklanabilir URL üretme
_GA4_PLACEHOLDER_HOSTS = frozenset(
    {
        "(not set)",
        "not set",
        "(other)",
        "other",
        "(not provided)",
    }
)
_GA4_PLACEHOLDER_PATHS = _GA4_PLACEHOLDER_HOSTS | {"(data not available)"}


def _is_ga4_placeholder_host(host: str) -> bool:
    h = (host or "").strip().lower()
    return h in {x.lower() for x in _GA4_PLACEHOLDER_HOSTS}


def _is_ga4_placeholder_path(path: str) -> bool:
    p = (path or "").strip().lower()
    return p in {x.lower() for x in _GA4_PLACEHOLDER_PATHS}


def _doviz_two_segment_bank_gumus_or_ons(path_lc: str) -> bool:
    """Örn. /vakifbank/gumus, /akbank/ons — www 404, altin 200; /emtia/gumus-ons ile karışmasın."""
    p = path_lc.strip().lower()
    if not p.startswith("/"):
        p = "/" + p
    if "/emtia/" in p:
        return False
    parts = [x for x in p.split("/") if x]
    if len(parts) != 2:
        return False
    second = (parts[1] or "").split("?")[0].lower()
    return second in ("gumus", "ons")


def _doviz_altin_only_single_segment_root(path_lc: str) -> bool:
    """Tek path segmenti; yalnızca altin kökünde (ör. /altinkaynak)."""
    p = path_lc.strip().lower()
    if not p.startswith("/"):
        p = "/" + p
    parts = [x for x in p.split("/") if x]
    if len(parts) != 1:
        return False
    return (parts[0] or "").split("?")[0].lower() in _DOVIZ_ALTIN_ONLY_ROOT_SLUGS


def _doviz_serbest_has_currency_segment(path_lc: str) -> bool:
    """Örn. /serbest-piyasa/euro → kur; sadece /serbest-piyasa kökü (altin'de) kur'a taşınmaz."""
    p = path_lc.strip().lower()
    if not p.startswith("/"):
        p = "/" + p
    prefix = "/serbest-piyasa"
    if not p.startswith(prefix):
        return False
    tail = p[len(prefix) :].strip("/")
    return bool(tail)


def _doviz_path_should_use_altin_host(path_lc: str) -> bool:
    """www/m ana sitede ölçülen altın/banka altın path'leri altin.doviz.com'da yayında."""
    p = path_lc.strip()
    if not p.startswith("/"):
        p = "/" + p
    # Kur / emtia / serbest piyasa kökleri ana veya kur sayfasında; altin'e taşıma
    if "/emtia/" in p or _doviz_serbest_has_currency_segment(p):
        return False
    if _doviz_two_segment_bank_gumus_or_ons(p):
        return True
    if any(
        m in p
        for m in (
            "/gram-altin",
            "/ceyrek-altin",
            "/yarim-altin",
            "/tam-altin",
            "/cumhuriyet-altini",
            "/ata-altin",
            "/gram-gumus",
            "/gram-platin",
            "/gram-paladyum",
            "/gram-has-altin",
            "/ons-altin",
            "/odaci/",
            "/papara/",
            "/hepsipay/",
            "/getirfinans/",
        )
    ):
        return True
    # gümüş/ons emtia yolu: /emtia/gumus-ons — 'gumus' alt dizgesi ile karışmasın
    if "/gram-gumus" in p or re.search(r"/[^/]+/gram-gumus(?:/|$)", p):
        return True
    if re.search(r"/[^/]+/(gram-altin|ceyrek-altin|gram-platin|gram-gumus)(?:/|$)", p):
        return True
    return False


def _doviz_harem_segment_is_precious_metal(path_lc: str) -> bool:
    """'/harem/…' altında kur'a taşınmaması gereken altın/gümüş ürün path'i mi."""
    if not path_lc.startswith("/harem/"):
        return False
    parts = [x for x in path_lc.split("/") if x]
    if len(parts) < 2:
        return True
    seg = (parts[1] or "").split("?")[0].lower()
    if seg in (
        "gram-altin",
        "gumus",
        "ceyrek-altin",
        "ons",
        "22-ayar-bilezik",
        "eski-ceyrek-altin",
        "yarim-altin",
        "tam-altin",
        "usd-kg",
        "eur-kg",
        "14-ayar-altin",
        "hamit-altin",
        "eski-ata-altin",
        "eski-yarim-altin",
        "eski-tam-altin",
        "eski-gremse-altin",
        "gremse-altin",
        "resat-altin",
        "besli-altin",
        "ikibucuk-altin",
        "18-ayar-altin",
        "ata-altin",
    ):
        return True
    return any(k in seg for k in ("altin", "ceyrek", "gumus", "bilezik"))


def _doviz_rewrite_host(host: str, path_for_rules: str) -> str:
    h = (host or "").strip().lower()
    pl = path_for_rules.lower().strip()
    if not pl.startswith("/"):
        pl = "/" + pl

    # www/m: /altinkaynak gibi yalnızca altin kökünde olan tek segment (kur.doviz.com/altinkaynak ayrı sayfa)
    if h in _DOVIZ_MAIN_HOSTS and _doviz_altin_only_single_segment_root(pl):
        return _DOVIZ_ALTIN_HOST

    # Harem altında döviz çifti (USD/EUR …) kur'da; gram/gümüş/çeyrek altın altin'de kalır
    if h == "altin.doviz.com" and pl.startswith("/harem/") and not _doviz_harem_segment_is_precious_metal(pl):
        return "kur.doviz.com"
    # kur üzerinde emtia yolu www'de
    if "/emtia/" in pl and h == "kur.doviz.com":
        return "www.doviz.com"
    # Emtia kökü www'de (/emtia-haberleri gibi haber kategorilerine dokunma)
    if ("/emtia/" in pl or pl in ("/emtia", "/emtia/")) and h in (
        "altin.doviz.com",
        "haber.doviz.com",
        "borsa.doviz.com",
    ):
        return "www.doviz.com"
    if "/doviz-cevirici" in pl and h == "altin.doviz.com":
        return "www.doviz.com"
    # Kripto, endeks, parite, akaryakıt: www (GA4 bazen altin gösterir; borsa kendi host'unda)
    if h == "altin.doviz.com" and (
        pl.startswith("/kripto-paralar")
        or pl.startswith("/endeksler")
        or pl.startswith("/pariteler")
        or pl.startswith("/akaryakit")
    ):
        return "www.doviz.com"
    if h == "haber.doviz.com" and (
        pl.startswith("/kripto-paralar")
        or pl.startswith("/doviz-cevirici")
        or pl.startswith("/endeksler")
    ):
        return "www.doviz.com"
    if (pl.startswith("/kripto-paralar") or pl.startswith("/endeksler")) and h == "kur.doviz.com":
        return "www.doviz.com"
    if pl.startswith("/kripto-paralar") and h == "borsa.doviz.com":
        return "www.doviz.com"
    if "/doviz-cevirici" in pl and h == "borsa.doviz.com":
        return "www.doviz.com"
    # Haber URL'leri GA4'te bazen www'de görünür
    if h in _DOVIZ_MAIN_HOSTS and (
        pl.startswith("/gundem-haberleri")
        or pl.startswith("/altin-ve-degerli-metal-haberleri")
    ):
        return "haber.doviz.com"
    # Borsa hisseleri
    if pl.startswith("/hisseler") and h in (
        "www.doviz.com",
        "doviz.com",
        "m.doviz.com",
        "haber.doviz.com",
        "altin.doviz.com",
    ):
        return "borsa.doviz.com"
    # Ons (kısa URL) ana sitede değil, altin'de
    if pl in ("/ons", "/ons/") and h in _DOVIZ_MAIN_HOSTS:
        return _DOVIZ_ALTIN_HOST
    if pl in ("/ons", "/ons/") and h in ("haber.doviz.com", "borsa.doviz.com"):
        return _DOVIZ_ALTIN_HOST
    # Serbest piyasa altındaki döviz çiftleri: kur.doviz.com'da (kök /serbest-piyasa altin'de kalır)
    if _doviz_serbest_has_currency_segment(pl) and h in (
        "altin.doviz.com",
        "www.doviz.com",
        "doviz.com",
        "m.doviz.com",
        "haber.doviz.com",
        "borsa.doviz.com",
    ):
        return "kur.doviz.com"
    # Haber/borsa üzerinde ölçülen altın URL'leri → altin
    if h in ("haber.doviz.com", "borsa.doviz.com") and _doviz_path_should_use_altin_host(pl):
        return _DOVIZ_ALTIN_HOST
    if h == "haber.doviz.com" and (pl in ("/harem", "/harem/") or pl.startswith("/harem/gram-altin")):
        return _DOVIZ_ALTIN_HOST
    # Harem altında değerli maden ürünleri: kur/www/m → altin (kur.doviz.com/harem döviz kökü ayrı sayfa)
    if h in ("kur.doviz.com", "www.doviz.com", "doviz.com", "m.doviz.com"):
        if pl.startswith("/harem/") and _doviz_harem_segment_is_precious_metal(pl):
            return _DOVIZ_ALTIN_HOST
    # Tek segment /gumus → altin
    if pl in ("/gumus", "/gumus/") and (
        h in _DOVIZ_MAIN_HOSTS
        or h in ("kur.doviz.com", "borsa.doviz.com", "haber.doviz.com")
    ):
        return _DOVIZ_ALTIN_HOST
    if h in _DOVIZ_MAIN_HOSTS and _doviz_path_should_use_altin_host(pl):
        return _DOVIZ_ALTIN_HOST
    if h == "kur.doviz.com" and _doviz_path_should_use_altin_host(pl):
        return _DOVIZ_ALTIN_HOST
    return h


def _doviz_normalize_path(path: str, host: str | None = None) -> str:
    """Bilinen kısa path takıları (ör. /harem → /harem/gram-altin; /altin/x → /x)."""
    p = path.strip()
    if not p.startswith("/"):
        p = "/" + p
    while "//" in p:
        p = p.replace("//", "/")
    low = p.lower()
    h = (host or "").strip().lower()
    # kur.doviz.com/harem ve /kapalicarsi kökleri döviz borsası; altin gram-altin ile birleştirme
    if low in ("/harem", "/harem/"):
        if h == "kur.doviz.com":
            return "/harem"
        return "/harem/gram-altin"
    if low.startswith("/altin/"):
        rest = p[len("/altin/") :].lstrip("/")
        if rest:
            return "/" + rest
    if low in ("/emtialar", "/emtialar/"):
        return "/emtia"
    if low.startswith("/emtialar/brent-petrol"):
        return "/emtia/brent-petrol"
    return p


def ga4_canonical_page_url(host: str | None, path: str | None) -> str:
    """GA4 hostName + landing path -> https URL. Host boş veya (not set) ise ''."""
    p = (path or "").strip()
    if not p:
        return ""
    if _is_ga4_placeholder_path(p):
        return ""
    if p.startswith(("http://", "https://")):
        return p
    if not p.startswith("/"):
        p = "/" + p
    h_raw = (host or "").strip().lower()
    if h_raw.endswith("doviz.com"):
        p = _doviz_normalize_path(p, h_raw)
    if not h_raw or _is_ga4_placeholder_host(h_raw):
        return ""
    if h_raw.endswith("doviz.com"):
        h_raw = _doviz_rewrite_host(h_raw, p)
    return f"https://{h_raw}{quote(p, safe='/-._~%?&=#@!$()*+,;:')}"


def ga4_email_page_url(
    *,
    site_domain: str | None,
    path: str | None,
    page_host: str | None,
    stored_page_url: str | None,
) -> str:
    """E-posta GA4 satırları: host + path ile kanonik URL; yoksa kayıtlı URL veya site domain fallback."""
    p = (path or "").strip()
    if not p:
        return (stored_page_url or "").strip()
    ph = (page_host or "").strip()
    if ph and not _is_ga4_placeholder_host(ph):
        u = ga4_canonical_page_url(ph, p)
        if u:
            return u
    su = (stored_page_url or "").strip()
    if su:
        return su
    return ga4_fallback_page_url(p, site_domain) or ""


def ga4_fallback_page_url(path: str | None, domain: str | None) -> str:
    """Yalnızca site domain + path (GA4'te host yoksa veya eski snapshot)."""
    p = (path or "").strip() if path is not None else ""
    if _is_ga4_placeholder_path(p):
        return ""
    d = ga4_site_host(str(domain) if domain is not None else None)
    if not p or not d:
        return ""
    if p.startswith(("http://", "https://")):
        return p
    if not p.startswith("/"):
        p = "/" + p
    if d.endswith("doviz.com"):
        p = _doviz_normalize_path(p, d)
        d = _doviz_rewrite_host(d, p)
    return f"https://{d}{quote(p, safe='/-._~%?&=#@!$()*+,;:')}"


def ga4_row_page_href(row: dict | None, site_domain: str | None) -> str:
    if not row:
        return ""
    ph = str(row.get("page_host") or "").strip()
    pg = str(row.get("page") or "").strip()
    if ph and pg and not _is_ga4_placeholder_host(ph):
        u = ga4_canonical_page_url(ph, pg)
        if u:
            return u
    u = (row.get("page_url") or "").strip()
    if u:
        return u
    return ga4_fallback_page_url(row.get("page"), site_domain)


def enrich_ga4_page_rows(rows: list | None) -> list:
    """Snapshot satırlarında page_url eksikse page_host + page ile tamamla (/ga4 ve partial uyumu)."""
    if not rows:
        return []
    out: list = []
    for r in rows:
        if not isinstance(r, dict):
            out.append(r)
            continue
        row = dict(r)
        pu = (row.get("page_url") or "").strip()
        ph = str(row.get("page_host") or "").strip()
        pg = str(row.get("page") or "").strip()
        if ph and pg and not _is_ga4_placeholder_host(ph):
            new_u = ga4_canonical_page_url(ph, pg)
            if new_u:
                row["page_url"] = new_u
        elif not pu and ph and pg:
            row["page_url"] = ga4_canonical_page_url(ph, pg)
        out.append(row)
    return out


def ga4_row_page_label(row: dict | None, site_domain: str | None) -> str:
    """Liste metni: site ile aynı host'ta path; farklı host'ta host+path."""
    if not row:
        return ""
    path = str(row.get("page") or "")
    host = str(row.get("page_host") or "").strip()
    site_h = ga4_site_host(str(site_domain) if site_domain is not None else None)
    if not host or host.lower() in ("(not set)", "not set"):
        return path
    if site_h and _host_cmp_key(host) == _host_cmp_key(site_h):
        return path
    if path.startswith("/"):
        return f"{host}{path}"
    return f"{host}/{path}" if path else host
