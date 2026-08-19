"""halkarz.com × doviz.com halka arz eşleştirme ve eksik/fazla karşılaştırması."""
from __future__ import annotations

import html as html_lib
import json
import logging
import math
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Any
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

HALKARZ_HOME = "https://halkarz.com/"
DOVIZ_IPO = "https://borsa.doviz.com/halka-arz"
DOVIZ_TASLAK = "https://borsa.doviz.com/halka-arz/taslak-halka-arzlar"
DOVIZ_GECMIS = "https://borsa.doviz.com/halka-arz/gecmis-halka-arzlar"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SEOAgent/1.0; +https://projectcontrol.up.railway.app)"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.4",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}
_REQUEST_TIMEOUT = 22
_CACHE_TTL_S = 20 * 60
_DETAIL_LIMIT = 24

_cache_lock = threading.Lock()
_cache: dict[str, Any] = {"ts": 0.0, "payload": None}
# Kaynak sayfa çekimi patlarsa listeyi boş sanıp her şeyi "karşı tarafta yok"
# göstermemek için son başarılı kopyayı saklarız.
_page_cache_lock = threading.Lock()
_page_cache: dict[str, str] = {}
# Bir sayfa beklenenden azını döndürüyorsa (WAF / kısmi render) yine stale'e düş.
_MIN_ROWS = {"halkarz": 20, "doviz_taslak": 40, "doviz_gecmis": 40}

_LEGAL = re.compile(
    r"\b(a\.?\s*ş\.?|anonim\s+sirketi|san(?:ayi)?\.?|tic(?:aret)?\.?|"
    r"ltd\.?|sti\.?|ve|and)\b",
    re.I,
)
_PAREN = re.compile(r"\(([^)]{2,40})\)")
_ARTICLE_RE = re.compile(
    r"<article class=\"index-list\">(.*?)</article>",
    re.I | re.S,
)
_HREF_TITLE_RE = re.compile(
    r'<h3 class="il-halka-arz-sirket">\s*<a href="([^"]+)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
_BIST_RE = re.compile(r'<span class="il-bist-kod">\s*([^<]*)</span>', re.I | re.S)
_TIME_RE = re.compile(r"<time[^>]*>(.*?)</time>", re.I | re.S)
_BADGE_ERT_RE = re.compile(
    r'<div class="il-ert">\s*<a[^>]*>([^<]+)</a>',
    re.I | re.S,
)
_SP_TABLE_RE = re.compile(
    r"<tr[^>]*>\s*<td[^>]*>\s*<em>(.*?)</em>.*?</td>\s*<td[^>]*>(.*?)</td>",
    re.I | re.S,
)


def _strip_tags(raw: str) -> str:
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", raw or ""))
    return re.sub(r"\s+", " ", text).strip()


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip().casefold())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("ı", "i").replace("i̇", "i")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def _core_name(name: str) -> str:
    s = _fold(name)
    s = _PAREN.sub(" ", s)
    s = _LEGAL.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _name_keys(name: str, url: str = "") -> set[str]:
    keys: set[str] = set()
    slug = _slug_key(url)
    if len(slug) >= 6:
        keys.add(slug)
        legal_free = _LEGAL.sub(" ", slug)
        legal_free = re.sub(r"\s+", " ", legal_free).strip()
        if len(legal_free) >= 6:
            keys.add(legal_free)
    raw = (name or "").strip()
    if not raw:
        return {k for k in keys if k}
    keys.add(_fold(raw))
    core = _core_name(raw)
    if core:
        keys.add(core)
    for alias in _PAREN.findall(raw):
        a = _core_name(alias)
        if len(a) >= 3:
            keys.add(a)
    return {k for k in keys if k}


_GENERIC_TOKENS = {
    "anonim", "sirketi", "sirket", "holding", "grup", "group", "turk", "turkiye",
    "sanayi", "sanayii", "ticaret", "ticari", "yatirim", "yatirimlari", "enerji",
    "gida", "insaat", "teknoloji", "teknolojileri", "uretim", "ithalat", "ihracat",
    "elektrik", "makina", "makine", "otomotiv", "tekstil", "kimya", "lojistik",
    "hizmetleri", "hizmet", "endustri", "endustriyel", "urunleri", "pazarlama",
}


def _slug_key(url: str) -> str:
    """doviz ve halkarz detay URL'lerindeki slug'ı ada indirger.

    halkarz: /bewen-enerji-a-s/  ·  doviz: /halka-arz/bewen-enerji-a-s/196
    """
    raw = (url or "").split("?", 1)[0].rstrip("/")
    if not raw:
        return ""
    parts = [p for p in raw.split("/") if p]
    if not parts:
        return ""
    slug = parts[-1]
    if slug.isdigit() and len(parts) >= 2:
        slug = parts[-2]
    if slug.isdigit() or slug in {"halka-arz", "halkarz.com"}:
        return ""
    return _core_name(slug.replace("-", " "))


def _tokens(name: str) -> set[str]:
    return {
        t
        for t in _core_name(name).split()
        if len(t) >= 3 and t not in _GENERIC_TOKENS
    }


def _token_weights(*groups: list[dict[str, Any]]) -> dict[str, float]:
    """Korpusta seyrek geçen kelime ayırt edicidir (IDF benzeri ağırlık)."""
    counts: dict[str, int] = {}
    total = 0
    for group in groups:
        for item in group:
            total += 1
            for tok in _tokens(item.get("name") or ""):
                counts[tok] = counts.get(tok, 0) + 1
    if not counts:
        return {}
    base = max(2, total)
    return {tok: math.log(1 + base / (1 + c)) for tok, c in counts.items()}


def _weighted_overlap(
    a: str, b: str, weights: dict[str, float]
) -> tuple[float, float]:
    """(örtüşme oranı, ortak kelimelerin en yüksek ağırlığı)."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0, 0.0
    inter = ta & tb
    if not inter:
        return 0.0, 0.0

    def w(tokens: set[str]) -> float:
        return sum(weights.get(t, 1.0) for t in tokens) or 0.0

    denom = max(w(ta), w(tb))
    if denom <= 0:
        return 0.0, 0.0
    return w(inter) / denom, max(weights.get(t, 1.0) for t in inter)


def _ticker_norm(s: str) -> str:
    t = re.sub(r"[^A-Za-z0-9]", "", (s or "")).upper()
    if t in {"AS", "A", "SAN", "TIC"}:
        return ""
    return t if 3 <= len(t) <= 8 else ""


def _digits(s: str) -> str:
    return re.sub(r"[^\d]", "", s or "")


def _http_get(url: str) -> str:
    resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_halkarz_home(raw_html: str) -> dict[str, list[dict[str, Any]]]:
    """İlk Halka Arzlar + Taslak Arzlar listeleri."""
    ilk_html, taslak_html = raw_html, ""
    m = re.search(
        r'<ul class="halka-arz-list taslak">(.*)$',
        raw_html,
        re.I | re.S,
    )
    if m:
        split_at = m.start()
        ilk_html = raw_html[:split_at]
        taslak_html = raw_html[split_at:]
    return {
        "ilk": _parse_halkarz_articles(ilk_html, section="ilk"),
        "taslak": _parse_halkarz_articles(taslak_html, section="taslak"),
    }


def _parse_halkarz_articles(chunk: str, *, section: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block in _ARTICLE_RE.findall(chunk or ""):
        hm = _HREF_TITLE_RE.search(block)
        if not hm:
            continue
        url = hm.group(1).strip()
        name = _strip_tags(hm.group(2))
        if not name:
            continue
        ticker = _ticker_norm(_strip_tags(_BIST_RE.search(block).group(1) if _BIST_RE.search(block) else ""))
        date_label = _strip_tags(_TIME_RE.search(block).group(1) if _TIME_RE.search(block) else "")
        status = "Taslak" if section == "taslak" else "Takvim"
        ert = _BADGE_ERT_RE.search(block)
        if ert:
            status = _strip_tags(ert.group(1))
        elif re.search(r'class="il-gonk"', block, re.I):
            status = "Gong"
        elif re.search(r"snc-badge", block, re.I):
            status = "Sonuçlandı"
        elif re.search(r'class="il-new"', block, re.I) and section == "ilk":
            status = "Yeni"
        badges: list[str] = []
        if re.search(r'class="il-new"', block, re.I):
            badges.append("Yeni")
        key = url or name
        if key in seen:
            continue
        seen.add(key)
        item = {
            "source": "halkarz",
            "section": section,
            "name": name,
            "ticker": ticker,
            "status": status,
            "date_label": date_label,
            "url": url,
            "logo": "",
            "badges": badges,
            "fields": _halkarz_list_fields(name, ticker, status, date_label),
        }
        lm = re.search(r'<img src="([^"]+)" class="slogo"', block, re.I)
        if lm:
            item["logo"] = lm.group(1)
        out.append(item)
    return out


def _halkarz_list_fields(
    name: str, ticker: str, status: str, date_label: str
) -> dict[str, str]:
    fields = {"şirket": name}
    if ticker:
        fields["bist_kodu"] = ticker
    if date_label:
        fields["halka_arz_tarihi"] = date_label
    if status:
        fields["durum"] = status
    return fields


def parse_halkarz_detail(raw_html: str) -> dict[str, str]:
    """Şirket sayfasındaki Halka Arz Bilgileri tablosu."""
    fields: dict[str, str] = {}
    label_map = {
        "halka arz tarihi": "halka_arz_tarihi",
        "halka arz fiyati araligi": "fiyat",
        "halka arz fiyati/araligi": "fiyat",
        "dagitim yontemi": "dagitim",
        "pay": "pay",
        "ek pay": "ek_pay",
        "araci kurum": "araci_kurum",
        "bist kodu": "bist_kodu",
        "pazar": "pazar",
        "bist ilk islem tarihi": "ilk_islem",
    }
    for raw_label, raw_val in _SP_TABLE_RE.findall(raw_html or ""):
        label = _fold(_strip_tags(raw_label).rstrip(" :"))
        key = label_map.get(label) or label_map.get(label.replace(" ", "/"))
        if not key and "/" in _strip_tags(raw_label):
            key = label_map.get(re.sub(r"\s+", " ", label))
        if not key:
            continue
        val = _strip_tags(raw_val)
        val = re.sub(r"\(Konsorsiyum\)", "", val, flags=re.I).strip()
        if val:
            fields[key] = val
    return fields


def parse_doviz_aktif(raw_html: str) -> list[dict[str, Any]]:
    chunk = raw_html
    m = re.search(r'<div class="active-ipos[^"]*">(.*?)</div>\s*<span class="icon icon-ipo-slider-previous"', raw_html, re.I | re.S)
    if m:
        chunk = m.group(1)
    else:
        m = re.search(r"Aktif Halka Arzlar(.*?)Taslak Halka Arzlar", raw_html, re.I | re.S)
        chunk = m.group(1) if m else raw_html
    out: list[dict[str, Any]] = []
    starts = [m.start() for m in re.finditer(r'<div class="ipo ipo-\d+">', chunk, re.I)]
    blocks = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else len(chunk)
        blocks.append(chunk[s:e])
    for block in blocks:
        am = re.search(r'<a href="([^"]+)" class="ticker">([^<]+)</a>', block, re.I)
        if not am:
            continue
        fields: dict[str, str] = {"şirket": _strip_tags(am.group(2)), "durum": "Aktif"}
        label_map = {
            "satış fiyatı": "fiyat",
            "satis fiyati": "fiyat",
            "talep toplama": "halka_arz_tarihi",
            "satılacak lot": "pay",
            "satilacak lot": "pay",
            "halka arz büyüklüğü": "buyukluk",
            "halka arz buyuklugu": "buyukluk",
            "dağıtım yöntemi": "dagitim",
            "dagitim yontemi": "dagitim",
        }
        for lab, val in re.findall(
            r'<span class="label">\s*([^:<]+)\s*:?\s*</span>\s*<span class="value">\s*([^<]*)</span>',
            block,
            re.I | re.S,
        ):
            key = label_map.get(_fold(lab))
            if key and val.strip():
                fields[key] = _strip_tags(val)
        name = fields["şirket"]
        out.append(
            {
                "source": "doviz",
                "section": "aktif",
                "name": name,
                "ticker": _ticker_from_doviz_url(am.group(1), name),
                "status": "Aktif",
                "date_label": fields.get("halka_arz_tarihi", ""),
                "url": am.group(1),
                "fields": fields,
            }
        )
    return out


def parse_doviz_taslak(raw_html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    row_re = re.compile(
        r'<tr>\s*<td>\s*<a href="(https://borsa\.doviz\.com/halka-arz/[^"]+)">(.*?)</a>\s*</td>\s*'
        r'<td class="h-padding-8">([^<]+)</td>',
        re.I | re.S,
    )
    for url, inner, status in row_re.findall(raw_html or ""):
        name, ticker = _doviz_name_ticker(inner)
        if not name:
            continue
        key = url
        if key in seen:
            continue
        seen.add(key)
        st = _strip_tags(status)
        fields = {"şirket": name, "durum": st}
        if ticker:
            fields["bist_kodu"] = ticker
        out.append(
            {
                "source": "doviz",
                "section": "taslak",
                "name": name,
                "ticker": ticker,
                "status": st,
                "date_label": "",
                "url": url,
                "fields": fields,
            }
        )
    return out


def parse_doviz_gecmis(raw_html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    row_re = re.compile(
        r"<tr>\s*<td>\s*<a href=\"([^\"]+)\">(.*?)</a>\s*</td>\s*"
        r"<td[^>]*>([^<]*)</td>\s*"
        r"<td[^>]*>([^<]*)</td>\s*"
        r"<td[^>]*>([^<]*)</td>\s*"
        r"<td[^>]*>([^<]*)</td>\s*"
        r"<td[^>]*>([^<]*)</td>\s*"
        r"<td[^>]*>(.*?)</td>",
        re.I | re.S,
    )
    for url, inner, first, participants, size, price, last, ret in row_re.findall(raw_html or ""):
        name, ticker = _doviz_name_ticker(inner)
        if not name:
            alt = re.search(r'alt="([^"]+)"', inner)
            if alt:
                name = _strip_tags(alt.group(1))
        if not name:
            continue
        fields = {
            "şirket": name,
            "durum": "İşlem görüyor",
            "ilk_islem": _strip_tags(first),
            "katilimci": _strip_tags(participants),
            "buyukluk": _strip_tags(size),
            "fiyat": _strip_tags(price),
            "son": _strip_tags(last),
            "getiri": _strip_tags(ret),
        }
        if ticker:
            fields["bist_kodu"] = ticker
        out.append(
            {
                "source": "doviz",
                "section": "gecmis",
                "name": name,
                "ticker": ticker,
                "status": "İşlem görüyor",
                "date_label": fields["ilk_islem"],
                "url": url,
                "fields": fields,
            }
        )
    return out


def _doviz_name_ticker(inner: str) -> tuple[str, str]:
    cname = re.search(r'<div class="cname">([^<]+)</div>', inner, re.I)
    divs = re.findall(r"<div>([^<]+)</div>", inner)
    ticker = ""
    name = ""
    if cname and divs:
        left, right = _strip_tags(divs[0]), _strip_tags(cname.group(1))
        t_left, t_right = _ticker_norm(left), _ticker_norm(right)
        if t_left and not t_right:
            ticker, name = t_left, right
        elif t_right and not t_left:
            ticker, name = t_right, left
        else:
            name = right or left
            ticker = t_left or t_right
    elif cname:
        name = _strip_tags(cname.group(1))
    elif divs:
        name = _strip_tags(divs[0])
        ticker = _ticker_norm(name)
        if ticker == _ticker_norm(name) and len(name) <= 8 and name.isupper():
            name = ""
    if not name:
        alt = re.search(r'alt="([^"]+)"', inner)
        if alt:
            name = _strip_tags(alt.group(1))
    return name, ticker


def _ticker_from_doviz_url(url: str, name: str) -> str:
    return ""


def _first_token(s: str) -> str:
    return (s.split() or [""])[0]


# Ayırt edici kelime örtüşmesiyle eşleşme için alt sınırlar
_OVERLAP_MIN = 0.72
_RARE_WEIGHT_MIN = 1.6


def _score_pair(
    a: dict[str, Any],
    b: dict[str, Any],
    weights: dict[str, float] | None = None,
) -> float:
    ta, tb = _ticker_norm(a.get("ticker") or ""), _ticker_norm(b.get("ticker") or "")
    if ta and tb and ta == tb:
        return 1.0
    # Farklı BIST kodu taşıyan iki kayıt aynı şirket değildir
    if ta and tb and ta != tb:
        return 0.0
    keys_a = _name_keys(a.get("name") or "", a.get("url") or "")
    keys_b = _name_keys(b.get("name") or "", b.get("url") or "")
    if keys_a & keys_b:
        return 0.97
    first_a = {_first_token(k) for k in keys_a if _first_token(k)}
    first_b = {_first_token(k) for k in keys_b if _first_token(k)}
    share_first = bool(first_a & first_b)
    best = 0.0
    for ka in keys_a:
        for kb in keys_b:
            if not ka or not kb:
                continue
            if ka in kb or kb in ka:
                shorter = ka if len(ka) <= len(kb) else kb
                if len(shorter) >= 12:
                    best = max(best, 0.93)
            if share_first:
                best = max(best, SequenceMatcher(None, ka, kb).ratio())
    if weights and best < 0.9:
        ratio, rare = _weighted_overlap(
            a.get("name") or "", b.get("name") or "", weights
        )
        if ratio >= _OVERLAP_MIN and rare >= _RARE_WEIGHT_MIN:
            # "Cms Jant ve Makina Sanayii" ↔ "CMS Jant Makina San. Tic." gibi
            # yazımı farklı ama ayırt edici kelimeleri aynı olan çiftler
            best = max(best, 0.88 if ratio < 0.9 else 0.92)
    return best


def match_companies(
    halkarz: list[dict[str, Any]],
    doviz: list[dict[str, Any]],
    *,
    threshold: float = 0.84,
) -> list[dict[str, Any]]:
    """1-1 eşleştirme; artanları halkarz_only / doviz_only bırakır."""
    used_d: set[int] = set()
    pairs: list[tuple[float, int, int]] = []
    weights = _token_weights(halkarz, doviz)
    for i, ha in enumerate(halkarz):
        for j, dv in enumerate(doviz):
            sc = _score_pair(ha, dv, weights)
            if sc >= threshold:
                pairs.append((sc, i, j))
    pairs.sort(reverse=True)
    matched_h: set[int] = set()
    rows: list[dict[str, Any]] = []
    for sc, i, j in pairs:
        if i in matched_h or j in used_d:
            continue
        matched_h.add(i)
        used_d.add(j)
        rows.append(_paired_row(halkarz[i], doviz[j], score=sc))
    for i, ha in enumerate(halkarz):
        if i not in matched_h:
            rows.append(_paired_row(ha, None, score=0.0))
    for j, dv in enumerate(doviz):
        if j not in used_d:
            rows.append(_paired_row(None, dv, score=0.0))
    return rows


def _paired_row(
    ha: dict[str, Any] | None,
    dv: dict[str, Any] | None,
    *,
    score: float,
) -> dict[str, Any]:
    name = (ha or dv or {}).get("name") or ""
    ticker = _ticker_norm((ha or {}).get("ticker") or "") or _ticker_norm((dv or {}).get("ticker") or "")
    ha_fields = dict((ha or {}).get("fields") or {})
    dv_fields = dict((dv or {}).get("fields") or {})
    if ticker and "bist_kodu" not in ha_fields and ha:
        ha_fields["bist_kodu"] = ticker
    if ticker and "bist_kodu" not in dv_fields and dv:
        dv_fields["bist_kodu"] = ticker
    diffs = _diff_fields(ha_fields, dv_fields) if ha and dv else []
    if ha and not dv:
        diffs = [
            {"field": k, "halkarz": v, "doviz": "", "kind": "missing"}
            for k, v in ha_fields.items()
            if k != "şirket"
        ]
    elif dv and not ha:
        diffs = [
            {"field": k, "halkarz": "", "doviz": v, "kind": "extra"}
            for k, v in dv_fields.items()
            if k != "şirket"
        ]
    missing_on_doviz = [d["field"] for d in diffs if d["kind"] == "missing"]
    mismatch = [d["field"] for d in diffs if d["kind"] == "mismatch"]
    date_iso, date_source = _row_date(ha, dv)
    ha_order = (ha or {}).get("order")
    dv_order = (dv or {}).get("order")
    return {
        "name": name,
        "ticker": ticker,
        "row_key": row_key(ha, dv, ticker),
        "match": "both" if ha and dv else ("halkarz_only" if ha else "doviz_only"),
        "score": round(float(score), 3),
        "bucket": _bucket(ha, dv),
        "halkarz": _public_side(ha) if ha else None,
        "doviz": _public_side(dv) if dv else None,
        "diffs": diffs,
        "missing_on_doviz": missing_on_doviz,
        "mismatch": mismatch,
        "gap_count": len(missing_on_doviz) + len(mismatch),
        "date_iso": date_iso,
        "date_source": date_source,
        "ha_order": ha_order if isinstance(ha_order, int) else None,
        "dv_order": dv_order if isinstance(dv_order, int) else None,
        "is_new": "Yeni" in ((ha or {}).get("badges") or []),
        "status": ((ha or {}).get("status") or (dv or {}).get("status") or ""),
    }


def row_key(
    ha: dict[str, Any] | None,
    dv: dict[str, Any] | None,
    ticker: str = "",
) -> str:
    """Taramalar arasında sabit kalan satır kimliği (gizleme için)."""
    for src in (ha, dv):
        slug = _slug_key((src or {}).get("url") or "")
        if len(slug) >= 4:
            return "s:" + slug
    t = _ticker_norm(ticker or (ha or {}).get("ticker") or (dv or {}).get("ticker") or "")
    if t:
        return "t:" + t
    name = (ha or dv or {}).get("name") or ""
    return "n:" + (_core_name(name) or _fold(name))


def _public_side(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": item.get("name") or "",
        "ticker": item.get("ticker") or "",
        "status": item.get("status") or "",
        "section": item.get("section") or "",
        "date_label": item.get("date_label") or "",
        "url": item.get("url") or "",
        "logo": item.get("logo") or "",
        "badges": list(item.get("badges") or []),
        "order": item.get("order") if isinstance(item.get("order"), int) else None,
        "fields": dict(item.get("fields") or {}),
    }


def _bucket(ha: dict[str, Any] | None, dv: dict[str, Any] | None) -> str:
    dv_sec = (dv or {}).get("section") or ""
    ha_sec = (ha or {}).get("section") or ""
    ha_st = ((ha or {}).get("status") or "").casefold()
    dv_st = ((dv or {}).get("status") or "").casefold()
    if dv_sec == "aktif":
        return "olan"
    if dv_sec == "gecmis":
        return "olmus"
    if ha_st in {"gong", "sonuçlandı", "sonuclandi"} and dv_sec != "aktif":
        return "olmus"
    if any(x in ha_st or x in dv_st for x in ("iptal", "reddedildi")):
        return "arz_olacak"
    if ha_sec == "ilk" and ha_st not in {"ertelendi"} and dv_sec != "taslak":
        return "olan"
    return "arz_olacak"


_FIELD_LABELS = {
    "şirket": "Şirket",
    "bist_kodu": "BIST kodu",
    "halka_arz_tarihi": "Talep / tarih",
    "fiyat": "Fiyat",
    "pay": "Pay / lot",
    "ek_pay": "Ek pay",
    "dagitim": "Dağıtım",
    "araci_kurum": "Aracı kurum",
    "pazar": "Pazar",
    "ilk_islem": "İlk işlem",
    "buyukluk": "Arz büyüklüğü",
    "durum": "Durum",
    "katilimci": "Katılımcı",
    "son": "Son fiyat",
    "getiri": "Getiri",
}


def _diff_fields(ha: dict[str, str], dv: dict[str, str]) -> list[dict[str, str]]:
    keys = []
    for k in list(ha) + [k for k in dv if k not in ha]:
        if k not in keys:
            keys.append(k)
    out: list[dict[str, str]] = []
    for key in keys:
        if key == "şirket" or key == "durum":
            # durum kelimeleri kaynaklara göre farklı (Aktif / Sonuçlandı / Taslak)
            continue
        a, b = (ha.get(key) or "").strip(), (dv.get(key) or "").strip()
        if not a and not b:
            continue
        if a and not b:
            out.append({"field": key, "label": _FIELD_LABELS.get(key, key), "halkarz": a, "doviz": "", "kind": "missing"})
        elif b and not a:
            out.append({"field": key, "label": _FIELD_LABELS.get(key, key), "halkarz": "", "doviz": b, "kind": "extra"})
        elif _values_equal(key, a, b):
            out.append({"field": key, "label": _FIELD_LABELS.get(key, key), "halkarz": a, "doviz": b, "kind": "ok"})
        else:
            out.append({"field": key, "label": _FIELD_LABELS.get(key, key), "halkarz": a, "doviz": b, "kind": "mismatch"})
    return out


def _values_equal(key: str, a: str, b: str) -> bool:
    if _fold(a) == _fold(b):
        return True
    if key in {"fiyat", "pay", "ek_pay", "buyukluk", "katilimci"}:
        da, db = _digits(a), _digits(b)
        return bool(da) and da == db
    if key in {"halka_arz_tarihi", "ilk_islem"}:
        ta, tb = set(_date_tokens(a)), set(_date_tokens(b))
        return bool(ta) and bool(tb) and bool(ta & tb)
    if key == "dagitim":
        fa, fb = _fold(a), _fold(b)
        return ("esit" in fa and "esit" in fb) or fa == fb
    if key == "durum":
        return _fold(a) == _fold(b)
    return False


def _date_tokens(s: str) -> tuple[str, ...]:
    months = {
        "ocak": "01", "subat": "02", "mart": "03", "nisan": "04",
        "mayis": "05", "haziran": "06", "temmuz": "07", "agustos": "08",
        "eylul": "09", "ekim": "10", "kasim": "11", "aralik": "12",
    }
    folded = _fold(s)
    found: list[str] = []
    for m in re.finditer(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})", s):
        found.append(f"{int(m.group(1)):02d}{int(m.group(2)):02d}{m.group(3)}")
    year = ""
    ym = re.search(r"(20\d{2})", folded)
    if ym:
        year = ym.group(1)
    days = [int(x) for x in re.findall(r"\b(\d{1,2})\b", folded) if 1 <= int(x) <= 31]
    mon = ""
    for name, num in months.items():
        if name in folded:
            mon = num
            break
    if year and mon and days:
        for d in days:
            token = f"{d:02d}{mon}{year}"
            if token not in found:
                found.append(token)
    return tuple(sorted(set(found)))


def _iso_dates(s: str) -> list[str]:
    """'15-16 Eylül 2025' -> ['2025-09-15', '2025-09-16']."""
    out = {f"{t[4:]}-{t[2:4]}-{t[:2]}" for t in _date_tokens(s or "")}
    return sorted(out)


def _first_iso_date(*values: str) -> str:
    for value in values:
        dates = _iso_dates(value)
        if dates:
            return dates[0]
    return ""


_DATE_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("halkarz", "halka_arz_tarihi", "halkarz · talep"),
    ("doviz", "halka_arz_tarihi", "doviz · talep"),
    ("halkarz", "ilk_islem", "halkarz · ilk işlem"),
    ("doviz", "ilk_islem", "doviz · ilk işlem"),
)


def _row_date(ha: dict[str, Any] | None, dv: dict[str, Any] | None) -> tuple[str, str]:
    """Satır için sıralanabilir ISO tarih + hangi alandan geldiği."""
    sides = {"halkarz": ha or {}, "doviz": dv or {}}
    for side, key, label in _DATE_SOURCES:
        item = sides.get(side) or {}
        value = (dict(item.get("fields") or {})).get(key) or ""
        iso = _first_iso_date(value)
        if iso:
            return iso, label
    for side in ("halkarz", "doviz"):
        iso = _first_iso_date((sides.get(side) or {}).get("date_label") or "")
        if iso:
            return iso, f"{side} · liste"
    return "", ""


def merge_halkarz(ilk: list[dict[str, Any]], taslak: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """İlk listedeki şirket taslakta da varsa ilk kazanır."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def key(it: dict[str, Any]) -> str:
        t = _ticker_norm(it.get("ticker") or "")
        return t or _core_name(it.get("name") or "") or (it.get("url") or "")

    for it in ilk + taslak:
        k = key(it)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def merge_doviz(
    aktif: list[dict[str, Any]],
    taslak: list[dict[str, Any]],
    gecmis: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def key(it: dict[str, Any]) -> str:
        t = _ticker_norm(it.get("ticker") or "")
        return t or _core_name(it.get("name") or "") or (it.get("url") or "")

    for it in aktif + taslak + gecmis:
        k = key(it)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def build_payload(
    *,
    halkarz_home_html: str,
    doviz_home_html: str,
    doviz_taslak_html: str,
    doviz_gecmis_html: str,
    halkarz_details: dict[str, dict[str, str]] | None = None,
    errors: dict[str, str] | None = None,
) -> dict[str, Any]:
    ha_lists = parse_halkarz_home(halkarz_home_html)
    aktif = parse_doviz_aktif(doviz_home_html)
    taslak = parse_doviz_taslak(doviz_taslak_html)
    gecmis = parse_doviz_gecmis(doviz_gecmis_html)
    details = halkarz_details or {}
    for item in ha_lists["ilk"] + ha_lists["taslak"]:
        extra = details.get(item.get("url") or "")
        if extra:
            item["fields"] = {**item["fields"], **extra}
            if extra.get("bist_kodu") and not item.get("ticker"):
                item["ticker"] = _ticker_norm(extra["bist_kodu"])
    ha_all = merge_halkarz(ha_lists["ilk"], ha_lists["taslak"])
    dv_all = merge_doviz(aktif, taslak, gecmis)
    # kaynak sayfalardaki sıra = "en son girilen üstte"; sıralama için sakla
    for idx, item in enumerate(ha_all):
        item["order"] = idx
    for idx, item in enumerate(dv_all):
        item["order"] = idx
    rows = match_companies(ha_all, dv_all)
    # halkarz ana sayfada geçmiş arz arşivi yok; eşleşmeyen eski doviz
    # geçmişini "fazla" diye şişirme.
    rows = [
        r
        for r in rows
        if not (
            r["match"] == "doviz_only"
            and ((r.get("doviz") or {}).get("section") == "gecmis")
        )
    ]
    buckets = {"olan": [], "arz_olacak": [], "olmus": []}
    for row in rows:
        buckets.setdefault(row["bucket"], []).append(row)
    for key in buckets:
        buckets[key].sort(
            key=lambda r: (
                {"halkarz_only": 0, "doviz_only": 1, "both": 2}.get(r["match"], 9),
                r["name"].casefold(),
            )
        )

    missing = [r for r in rows if r["match"] == "halkarz_only"]
    extra = [r for r in rows if r["match"] == "doviz_only"]
    both = [r for r in rows if r["match"] == "both"]
    field_missing = sum(len(r["missing_on_doviz"]) for r in both)
    field_mismatch = sum(len(r["mismatch"]) for r in both)

    def _brief(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "name": r["name"],
            "ticker": r["ticker"],
            "bucket": r["bucket"],
            "halkarz_url": (r.get("halkarz") or {}).get("url") if r.get("halkarz") else "",
            "doviz_url": (r.get("doviz") or {}).get("url") if r.get("doviz") else "",
            "halkarz_status": (r.get("halkarz") or {}).get("status") if r.get("halkarz") else "",
            "doviz_status": (r.get("doviz") or {}).get("status") if r.get("doviz") else "",
        }

    dated = sum(1 for r in rows if r.get("date_iso"))
    today_iso = datetime.now(_TR).date().isoformat()
    upcoming = sum(1 for r in rows if (r.get("date_iso") or "") >= today_iso and r.get("date_iso"))
    return {
        "ok": True,
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "today": today_iso,
        "errors": errors or {},
        "counts": {
            "halkarz_ilk": len(ha_lists["ilk"]),
            "halkarz_taslak": len(ha_lists["taslak"]),
            "halkarz": len(ha_all),
            "doviz_aktif": len(aktif),
            "doviz_taslak": len(taslak),
            "doviz_gecmis": len(gecmis),
            "doviz": len(dv_all),
            "matched": len(both),
            "missing": len(missing),
            "extra": len(extra),
            "field_missing": field_missing,
            "field_mismatch": field_mismatch,
            "olan": len(buckets["olan"]),
            "arz_olacak": len(buckets["arz_olacak"]),
            "olmus": len(buckets["olmus"]),
            "total": len(rows),
            "dated": dated,
            "upcoming": upcoming,
            "past": dated - upcoming,
            "new_badge": sum(1 for r in rows if r.get("is_new")),
        },
        "buckets": buckets,
        "missing": [_brief(r) for r in missing],
        "extra": [_brief(r) for r in extra],
        "field_labels": _FIELD_LABELS,
        "sources": {
            "halkarz": HALKARZ_HOME,
            "doviz_aktif": DOVIZ_IPO,
            "doviz_taslak": DOVIZ_TASLAK,
            "doviz_gecmis": DOVIZ_GECMIS,
        },
        "halkarz_snapshot": [snapshot_company(it) for it in ha_all],
        "halkarz_flagged_new": _flagged_new(ha_all, rows),
    }


def _flagged_new(ha_all: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lookup = doviz_lookup_from_payload({"buckets": {"_": rows}})
    out: list[dict[str, Any]] = []
    for it in ha_all:
        if "Yeni" not in (it.get("badges") or []):
            continue
        snap = snapshot_company(it)
        dv = lookup.get(snap["id"]) or {}
        snap["on_doviz"] = bool(dv.get("on_doviz"))
        snap["doviz_url"] = dv.get("doviz_url") or ""
        snap["doviz_status"] = dv.get("doviz_status") or ""
        out.append(snap)
    return out


def _fetch_halkarz_details(urls: list[str], *, limit: int | None = None) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    cap = _DETAIL_LIMIT if limit is None else max(0, int(limit))
    uniq = []
    seen: set[str] = set()
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
        if len(uniq) >= cap:
            break

    def one(url: str) -> tuple[str, dict[str, str]]:
        try:
            html = _http_get(url)
            return url, parse_halkarz_detail(html)
        except Exception as exc:
            logger.warning("halkarz detail fail %s: %s", url, exc)
            return url, {}

    if not uniq:
        return out
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(one, u) for u in uniq]
        for fut in as_completed(futs):
            url, fields = fut.result()
            if fields:
                out[url] = fields
    return out


def _page_row_count(key: str, html: str) -> int:
    """Sayfa gerçekten liste döndürmüş mü? (boş/kısmi yanıtı yakalamak için)"""
    if not html:
        return 0
    try:
        if key == "halkarz":
            lists = parse_halkarz_home(html)
            return len(lists["ilk"]) + len(lists["taslak"])
        if key == "doviz_home":
            return len(parse_doviz_aktif(html))
        if key == "doviz_taslak":
            return len(parse_doviz_taslak(html))
        if key == "doviz_gecmis":
            return len(parse_doviz_gecmis(html))
    except Exception:  # parse patlarsa sayfayı sağlıksız say
        return 0
    return 0


def fetch_compare(*, force: bool = False, details: bool = True, detail_scope: str = "ilk") -> dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        if (
            not force
            and _cache["payload"] is not None
            and (now - float(_cache["ts"] or 0)) < _CACHE_TTL_S
        ):
            return _cache["payload"]

    errors: dict[str, str] = {}
    pages: dict[str, str] = {}

    def grab(key: str, url: str) -> None:
        try:
            html = _http_get(url)
            if _page_row_count(key, html) < _MIN_ROWS.get(key, 0):
                raise ValueError("liste beklenenden kısa geldi")
            pages[key] = html
            with _page_cache_lock:
                _page_cache[key] = html
        except Exception as exc:
            with _page_cache_lock:
                stale = _page_cache.get(key) or ""
            pages[key] = stale
            errors[key] = (
                f"{exc} (son başarılı kopya kullanıldı)" if stale else str(exc)
            )
            logger.warning("ipo fetch %s failed: %s (stale=%s)", key, exc, bool(stale))

    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = [
            pool.submit(grab, "halkarz", HALKARZ_HOME),
            pool.submit(grab, "doviz_home", DOVIZ_IPO),
            pool.submit(grab, "doviz_taslak", DOVIZ_TASLAK),
            pool.submit(grab, "doviz_gecmis", DOVIZ_GECMIS),
        ]
        for fut in futs:
            fut.result()

    detail_map: dict[str, dict[str, str]] = {}
    if details and pages.get("halkarz"):
        ha_lists = parse_halkarz_home(pages["halkarz"])
        urls = [it["url"] for it in ha_lists["ilk"] if it.get("url")]
        if detail_scope == "all":
            urls.extend(it["url"] for it in ha_lists["taslak"] if it.get("url"))
            detail_map = _fetch_halkarz_details(urls, limit=280)
        else:
            detail_map = _fetch_halkarz_details(urls)

    payload = build_payload(
        halkarz_home_html=pages.get("halkarz") or "",
        doviz_home_html=pages.get("doviz_home") or "",
        doviz_taslak_html=pages.get("doviz_taslak") or "",
        doviz_gecmis_html=pages.get("doviz_gecmis") or "",
        halkarz_details=detail_map,
        errors=errors,
    )
    payload["ok"] = not bool(errors.get("halkarz") and errors.get("doviz_taslak"))
    with _cache_lock:
        _cache["ts"] = time.monotonic()
        _cache["payload"] = payload
    return payload


_TR = ZoneInfo("Europe/Istanbul")
VISIT_SLOTS: tuple[tuple[int, int], ...] = ((9, 9), (14, 14))
_VISIT_KEEP = 40


def snapshot_company(item: dict[str, Any]) -> dict[str, Any]:
    fields = dict(item.get("fields") or {})
    fields.pop("şirket", None)
    return {
        "id": _company_id(item),
        "row_key": row_key(item, None),
        "name": item.get("name") or "",
        "ticker": item.get("ticker") or "",
        "section": item.get("section") or "",
        "status": item.get("status") or "",
        "date_label": item.get("date_label") or "",
        "url": item.get("url") or "",
        "badges": list(item.get("badges") or []),
        "order": item.get("order") if isinstance(item.get("order"), int) else None,
        "date_iso": _first_iso_date(
            fields.get("halka_arz_tarihi") or "",
            item.get("date_label") or "",
            fields.get("ilk_islem") or "",
        ),
        "fields": fields,
    }


def _company_id(item: dict[str, Any]) -> str:
    url = (item.get("url") or "").rstrip("/")
    slug = url.rsplit("/", 1)[-1] if url else ""
    if slug and slug not in {"halkarz.com", ""}:
        return "u:" + slug
    t = _ticker_norm(item.get("ticker") or "")
    if t:
        return "t:" + t
    return "n:" + (_core_name(item.get("name") or "") or _fold(item.get("name") or ""))


def _snap_changes(prev: dict[str, Any], curr: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for key, label in (
        ("status", "Durum"),
        ("date_label", "Tarih"),
        ("ticker", "BIST kodu"),
    ):
        a, b = str(prev.get(key) or "").strip(), str(curr.get(key) or "").strip()
        if a != b:
            out.append({"field": key, "label": label, "before": a, "after": b})
    prev_b = {str(x) for x in (prev.get("badges") or [])}
    curr_b = {str(x) for x in (curr.get("badges") or [])}
    if prev_b != curr_b:
        out.append(
            {
                "field": "badges",
                "label": "Rozet",
                "before": ", ".join(sorted(prev_b)) or "—",
                "after": ", ".join(sorted(curr_b)) or "—",
            }
        )
    pf, cf = dict(prev.get("fields") or {}), dict(curr.get("fields") or {})
    keys = list(dict.fromkeys([*pf, *cf]))
    for key in keys:
        if key == "durum":
            continue
        a, b = str(pf.get(key) or "").strip(), str(cf.get(key) or "").strip()
        if a == b:
            continue
        out.append(
            {
                "field": key,
                "label": _FIELD_LABELS.get(key, key),
                "before": a,
                "after": b,
            }
        )
    return out


def doviz_lookup_from_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    buckets = payload.get("buckets") or {}
    for rows in buckets.values():
        for r in rows or []:
            ha = r.get("halkarz") or {}
            cid = _company_id(
                {
                    "url": ha.get("url") or "",
                    "ticker": r.get("ticker") or ha.get("ticker") or "",
                    "name": r.get("name") or ha.get("name") or "",
                }
            )
            dv = r.get("doviz") or {}
            out[cid] = {
                "on_doviz": r.get("match") == "both",
                "doviz_url": dv.get("url") or "",
                "doviz_status": dv.get("status") or "",
                "match": r.get("match") or "",
            }
    return out


def diff_halkarz_snapshots(
    prev: list[dict[str, Any]],
    curr: list[dict[str, Any]],
    doviz_lookup: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    lookup = doviz_lookup or {}
    prev_m = {str(x.get("id") or ""): x for x in prev if x.get("id")}
    curr_m = {str(x.get("id") or ""): x for x in curr if x.get("id")}
    new_items: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []

    def _attach(item: dict[str, Any]) -> dict[str, Any]:
        dv = lookup.get(item.get("id") or "") or {}
        return {
            "id": item.get("id") or "",
            "row_key": item.get("row_key") or row_key(item, None),
            "name": item.get("name") or "",
            "ticker": item.get("ticker") or "",
            "section": item.get("section") or "",
            "status": item.get("status") or "",
            "date_label": item.get("date_label") or "",
            "date_iso": item.get("date_iso") or _first_iso_date(
                (dict(item.get("fields") or {})).get("halka_arz_tarihi") or "",
                item.get("date_label") or "",
            ),
            "order": item.get("order") if isinstance(item.get("order"), int) else None,
            "url": item.get("url") or "",
            "on_doviz": bool(dv.get("on_doviz")),
            "doviz_url": dv.get("doviz_url") or "",
            "doviz_status": dv.get("doviz_status") or "",
        }

    for cid, item in curr_m.items():
        rec = _attach(item)
        old = prev_m.get(cid)
        if old is None:
            new_items.append(rec)
            continue
        changes = _snap_changes(old, item)
        if changes:
            rec["changes"] = changes
            changed.append(rec)
    for cid, item in prev_m.items():
        if cid not in curr_m:
            removed.append(
                {
                    "id": cid,
                    "name": item.get("name") or "",
                    "ticker": item.get("ticker") or "",
                    "url": item.get("url") or "",
                }
            )
    new_items.sort(key=lambda r: ((not r["on_doviz"]), r["name"].casefold()))
    changed.sort(key=lambda r: ((not r["on_doviz"]), r["name"].casefold()))
    return {"new": new_items, "changed": changed, "removed": removed}


def next_visit_slot(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(_TR)
    if now.tzinfo is None:
        now = now.replace(tzinfo=_TR)
    else:
        now = now.astimezone(_TR)
    candidates: list[datetime] = []
    for h, m in VISIT_SLOTS:
        dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if dt <= now:
            dt = dt + timedelta(days=1)
        candidates.append(dt)
    nxt = min(candidates)
    return {
        "slot": f"{nxt.hour:02d}:{nxt.minute:02d}",
        "at": nxt.strftime("%Y-%m-%d %H:%M"),
        "in_min": max(0, int((nxt - now).total_seconds() // 60)),
        "slots": [f"{h:02d}:{m:02d}" for h, m in VISIT_SLOTS],
    }


def _public_visit(row) -> dict[str, Any]:
    try:
        summary = json.loads(row.summary_json or "{}")
    except json.JSONDecodeError:
        summary = {}
    try:
        delta = json.loads(row.delta_json or "{}")
    except json.JSONDecodeError:
        delta = {}
    fetched = row.fetched_at
    if isinstance(fetched, datetime):
        if fetched.tzinfo is None:
            label = fetched.strftime("%Y-%m-%d %H:%M:%S")
        else:
            label = fetched.astimezone(_TR).strftime("%Y-%m-%d %H:%M:%S")
    else:
        label = str(fetched or "")
    return {
        "id": row.id,
        "slot": row.slot,
        "fetched_at": label,
        "is_baseline": bool(row.is_baseline),
        "summary": summary,
        "new": delta.get("new") or [],
        "changed": delta.get("changed") or [],
        "removed": delta.get("removed") or [],
    }


def latest_visit_public(db) -> dict[str, Any] | None:
    from backend.models import IpoHalkarzVisit

    row = (
        db.query(IpoHalkarzVisit)
        .order_by(IpoHalkarzVisit.fetched_at.desc(), IpoHalkarzVisit.id.desc())
        .first()
    )
    if row is None:
        return None
    return _public_visit(row)


def run_scheduled_visit(db, *, slot: str = "manual") -> dict[str, Any]:
    """halkarz.com ziyareti: snapshot al, öncekiyle farkı kaydet, doviz var/yok işaretle."""
    from backend.models import IpoHalkarzVisit

    prev_row = (
        db.query(IpoHalkarzVisit)
        .order_by(IpoHalkarzVisit.fetched_at.desc(), IpoHalkarzVisit.id.desc())
        .first()
    )
    if (
        prev_row is not None
        and slot in {"09:09", "14:14"}
        and (prev_row.slot or "") == slot
    ):
        prev_at = prev_row.fetched_at
        if isinstance(prev_at, datetime):
            now_naive = datetime.now(_TR).replace(tzinfo=None)
            age = (now_naive - prev_at.replace(tzinfo=None)).total_seconds()
            if 0 <= age < 20 * 60:
                logger.info("IPO halkarz visit %s skipped (duplicate %.0fs)", slot, age)
                return _public_visit(prev_row)

    payload = fetch_compare(force=True, details=True, detail_scope="all")
    curr = list(payload.get("halkarz_snapshot") or [])
    lookup = doviz_lookup_from_payload(payload)
    is_baseline = prev_row is None
    prev_snap: list[dict[str, Any]] = []
    if prev_row is not None:
        try:
            prev_snap = json.loads(prev_row.snapshot_json or "[]")
        except json.JSONDecodeError:
            prev_snap = []
    delta = (
        {"new": [], "changed": [], "removed": []}
        if is_baseline
        else diff_halkarz_snapshots(prev_snap, curr, lookup)
    )
    new_items = delta.get("new") or []
    changed = delta.get("changed") or []
    summary = {
        "new": len(new_items),
        "changed": len(changed),
        "removed": len(delta.get("removed") or []),
        "on_doviz": sum(1 for x in new_items + changed if x.get("on_doviz")),
        "not_on_doviz": sum(1 for x in new_items + changed if not x.get("on_doviz")),
        "halkarz": len(curr),
        "compare_missing": (payload.get("counts") or {}).get("missing"),
    }
    row = IpoHalkarzVisit(
        slot=slot,
        fetched_at=datetime.now(_TR).replace(tzinfo=None),
        is_baseline=is_baseline,
        summary_json=json.dumps(summary, ensure_ascii=False),
        delta_json=json.dumps(delta, ensure_ascii=False),
        snapshot_json=json.dumps(curr, ensure_ascii=False),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    old = (
        db.query(IpoHalkarzVisit)
        .order_by(IpoHalkarzVisit.fetched_at.desc(), IpoHalkarzVisit.id.desc())
        .offset(_VISIT_KEEP)
        .all()
    )
    for gone in old:
        db.delete(gone)
    if old:
        db.commit()
    out = _public_visit(row)
    logger.info(
        "IPO halkarz visit slot=%s baseline=%s new=%s changed=%s not_on_doviz=%s",
        slot,
        is_baseline,
        summary["new"],
        summary["changed"],
        summary["not_on_doviz"],
    )
    return out



# --- «Bunu bir daha gösterme» ------------------------------------------------


def hidden_keys(db) -> list[str]:
    from backend.models import IpoHiddenCompany

    rows = db.query(IpoHiddenCompany).order_by(IpoHiddenCompany.created_at.desc()).all()
    return [r.row_key for r in rows if r.row_key]


def hidden_rows_public(db) -> list[dict[str, Any]]:
    from backend.models import IpoHiddenCompany

    rows = db.query(IpoHiddenCompany).order_by(IpoHiddenCompany.created_at.desc()).all()
    out: list[dict[str, Any]] = []
    for r in rows:
        created = r.created_at
        out.append(
            {
                "row_key": r.row_key,
                "name": r.name or "",
                "ticker": r.ticker or "",
                "hidden_by": r.hidden_by or "",
                "created_at": created.strftime("%Y-%m-%d %H:%M") if created else "",
            }
        )
    return out


def set_hidden(
    db,
    *,
    key: str,
    hidden: bool,
    name: str = "",
    ticker: str = "",
    by: str = "",
) -> dict[str, Any]:
    """Bir satırı gizle / geri getir. Sonuç: güncel gizli anahtar listesi."""
    from backend.models import IpoHiddenCompany

    key = (key or "").strip()[:191]
    if not key:
        raise ValueError("row_key gerekli")
    row = db.query(IpoHiddenCompany).filter(IpoHiddenCompany.row_key == key).first()
    if hidden:
        if row is None:
            db.add(
                IpoHiddenCompany(
                    row_key=key,
                    name=(name or "")[:255],
                    ticker=_ticker_norm(ticker or "")[:16],
                    hidden_by=(by or "")[:191],
                )
            )
        else:
            if name:
                row.name = name[:255]
            if ticker:
                row.ticker = _ticker_norm(ticker)[:16]
    elif row is not None:
        db.delete(row)
    db.commit()
    return {"ok": True, "hidden": hidden_keys(db)}
