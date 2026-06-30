"""Sinemalar.com — TMDB başlığına göre film/dizi varlık kontrolü (/ara?q= SSR)."""
from __future__ import annotations

import logging
import re
import threading
import time
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Literal
from urllib.parse import quote_plus

import requests

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.sinemalar.com/ara?q={query}"
_UA = (
    "Mozilla/5.0 (compatible; SEOAgent/1.0; +https://projectcontrol.up.railway.app)"
)
_ITEM_RE = re.compile(
    r'<a class="item-title link" href="(https://www\.sinemalar\.com/(film|dizi)/(\d+)/[^"]+)">([^<]+)</a>',
    re.I,
)
_YEAR_IN_ALT_RE = re.compile(r'alt="[^"]*\((\d{4})\)[^"]*afişi"', re.I)
_VIZYON_BLOCK_RE = re.compile(
    r"<b>Vizyon Tarihi:</b>\s*([\s\S]*?)\s*</div>",
    re.I,
)
_YAYIN_BLOCK_RE = re.compile(
    r"<b>Yayın Tarihi:</b>\s*([\s\S]*?)\s*</div>",
    re.I,
)
_TR_MONTH: dict[str, int] = {
    "ocak": 1,
    "subat": 2,
    "şubat": 2,
    "mart": 3,
    "nisan": 4,
    "mayis": 5,
    "mayıs": 5,
    "haziran": 6,
    "temmuz": 7,
    "agustos": 8,
    "ağustos": 8,
    "eylul": 9,
    "eylül": 9,
    "ekim": 10,
    "kasim": 11,
    "kasım": 11,
    "aralik": 12,
    "aralık": 12,
}
_EMPTY_RELEASE = frozenset({"", "-", "—", "?", "yok", "belirlenmedi", "henüz belirlenmedi"})

_cache_lock = threading.Lock()
_cache: dict[str, dict[str, Any]] = {}
_CACHE_TTL_S = 7 * 24 * 3600


def _cache_key(media_type: str, title: str, year: str) -> str:
    return f"v2:{media_type}:{_normalize(title)}:{year}"


def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip().casefold())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    return re.sub(r"\s+", " ", s).strip()


def _year_from_date(d: str | None) -> str:
    if d and len(d) >= 4 and d[:4].isdigit():
        return d[:4]
    return ""


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.92
    return SequenceMatcher(None, a, b).ratio()


def _parse_search_html(html: str) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for m in _ITEM_RE.finditer(html):
        url, kind, sid, title = m.group(1), m.group(2).lower(), m.group(3), m.group(4).strip()
        year = ""
        start = max(0, m.start() - 400)
        chunk = html[start : m.start()]
        ym = _YEAR_IN_ALT_RE.search(chunk)
        if ym:
            year = ym.group(1)
        hits.append(
            {
                "url": url,
                "kind": kind,
                "sinemalar_id": int(sid),
                "title": title,
                "year": year,
            }
        )
    return hits


def _pick_best(
    *,
    title: str,
    original_title: str,
    year: str,
    media_type: Literal["movie", "tv"],
    hits: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not hits:
        return None

    want_kind = "dizi" if media_type == "tv" else "film"
    ordered = sorted(
        hits,
        key=lambda h: (0 if h["kind"] == want_kind else 1, hits.index(h)),
    )

    norms = [_normalize(title), _normalize(original_title)]
    norms = [n for n in norms if n]

    best: dict[str, Any] | None = None
    best_score = 0.0

    for h in ordered:
        cand = _normalize(h["title"])
        score = max(_similarity(n, cand) for n in norms) if norms else 0.0
        if year and h.get("year"):
            if h["year"] == year:
                score = min(1.0, score + 0.08)
            elif abs(int(h["year"]) - int(year)) > 1:
                score *= 0.85
        if score > best_score:
            best_score = score
            best = h

    if not best or best_score < 0.72:
        return None
    quality = "exact" if best_score >= 0.95 else "fuzzy"
    return {**best, "match_score": round(best_score, 3), "match_quality": quality}


def _fetch_search(query: str) -> list[dict[str, Any]]:
    url = _SEARCH_URL.format(query=quote_plus(query))
    try:
        resp = requests.get(url, headers={"User-Agent": _UA}, timeout=12)
        resp.raise_for_status()
        return _parse_search_html(resp.text)
    except Exception as exc:
        logger.warning("Sinemalar arama hatası [%s]: %s", query[:60], exc)
        return []


def _turkish_date_label_to_iso(label: str) -> str | None:
    """Örn. '01 Mart 2024' → '2024-03-01'."""
    label = re.sub(r"\s+", " ", (label or "").strip())
    m = re.match(r"(\d{1,2})\s+([A-Za-zÇĞİÖŞÜçğıöşü]+)\s+(\d{4})", label)
    if not m:
        return None
    day, month_name, year = int(m.group(1)), m.group(2).casefold(), m.group(3)
    month = _TR_MONTH.get(month_name.replace("i̇", "i"))
    if not month:
        return None
    return f"{year}-{month:02d}-{day:02d}"


def _parse_release_from_detail_html(html: str, *, kind: str) -> dict[str, Any]:
    """Film: Vizyon Tarihi; dizi: Yayın Tarihi."""
    if kind == "dizi":
        block = _YAYIN_BLOCK_RE.search(html)
        label_key = "yayın"
    else:
        block = _VIZYON_BLOCK_RE.search(html)
        label_key = "vizyon"
    if not block:
        return {
            "sinemalar_has_release_date": False,
            "sinemalar_release_date": None,
            "sinemalar_release_label": None,
            "sinemalar_release_kind": label_key,
        }
    raw = re.sub(r"<[^>]+>", " ", block.group(1))
    raw = re.sub(r"\s+", " ", raw).strip()
    if raw.casefold() in _EMPTY_RELEASE:
        return {
            "sinemalar_has_release_date": False,
            "sinemalar_release_date": None,
            "sinemalar_release_label": None,
            "sinemalar_release_kind": label_key,
        }
    iso = _turkish_date_label_to_iso(raw)
    return {
        "sinemalar_has_release_date": True,
        "sinemalar_release_date": iso or raw,
        "sinemalar_release_label": raw,
        "sinemalar_release_kind": label_key,
    }


def _fetch_detail_release(url: str, *, kind: str) -> dict[str, Any]:
    try:
        resp = requests.get(url, headers={"User-Agent": _UA}, timeout=12)
        resp.raise_for_status()
        return _parse_release_from_detail_html(resp.text, kind=kind)
    except Exception as exc:
        logger.warning("Sinemalar detay (vizyon) hatası [%s]: %s", url[:80], exc)
        return {
            "sinemalar_has_release_date": False,
            "sinemalar_release_date": None,
            "sinemalar_release_label": None,
            "sinemalar_release_kind": "vizyon" if kind != "dizi" else "yayın",
        }


def lookup(
    *,
    title: str,
    original_title: str = "",
    release_date: str = "",
    media_type: Literal["movie", "tv"] = "movie",
    use_cache: bool = True,
) -> dict[str, Any]:
    """Tek yapım için Sinemalar eşleşmesi. Önbellek: 7 gün."""
    year = _year_from_date(release_date)
    key = _cache_key(media_type, title or original_title, year)
    now = time.monotonic()

    if use_cache:
        with _cache_lock:
            row = _cache.get(key)
            if row and now - row["mono"] < _CACHE_TTL_S:
                return dict(row["payload"])

    queries: list[str] = []
    for q in (title, original_title):
        q = (q or "").strip()
        if q and q not in queries:
            queries.append(q)

    hits: list[dict[str, Any]] = []
    for q in queries:
        hits = _fetch_search(q)
        if hits:
            break

    match = _pick_best(
        title=title,
        original_title=original_title,
        year=year,
        media_type=media_type,
        hits=hits,
    )

    if match:
        release = _fetch_detail_release(match["url"], kind=match["kind"])
        payload = {
            "sinemalar_found": True,
            "sinemalar_url": match["url"],
            "sinemalar_title": match["title"],
            "sinemalar_id": match["sinemalar_id"],
            "sinemalar_match_quality": match["match_quality"],
            **release,
        }
    else:
        payload = {
            "sinemalar_found": False,
            "sinemalar_url": None,
            "sinemalar_title": None,
            "sinemalar_id": None,
            "sinemalar_match_quality": None,
            "sinemalar_has_release_date": None,
            "sinemalar_release_date": None,
            "sinemalar_release_label": None,
            "sinemalar_release_kind": None,
        }

    with _cache_lock:
        _cache[key] = {"mono": now, "payload": payload}
    return dict(payload)


def _merge_item(item: dict[str, Any], payload: dict[str, Any]) -> None:
    item.update(payload)


def apply_cached_sinemalar(items: list[dict[str, Any]]) -> None:
    """Yalnızca bellek önbelleğinden alanları doldurur (HTTP yok)."""
    now = time.monotonic()
    for m in items:
        media_type: Literal["movie", "tv"] = (
            "tv" if m.get("media_type") == "tv" else "movie"
        )
        year = _year_from_date(m.get("release_date") or m.get("first_air_date"))
        key = _cache_key(media_type, m.get("title") or m.get("original_title") or "", year)
        with _cache_lock:
            row = _cache.get(key)
        if row and now - row["mono"] < _CACHE_TTL_S:
            _merge_item(m, row["payload"])
        else:
            m.setdefault("sinemalar_found", None)


def warm_sinemalar_cache(
    items: list[dict[str, Any]],
    *,
    max_lookups: int = 200,
    delay_s: float = 0.18,
) -> int:
    """Önbellekte olmayan kayıtlar için Sinemalar araması yapar."""
    done = 0
    for m in items:
        if done >= max_lookups:
            break
        if m.get("sinemalar_found") is not None:
            continue
        media_type: Literal["movie", "tv"] = (
            "tv" if m.get("media_type") == "tv" else "movie"
        )
        payload = lookup(
            title=m.get("title") or "",
            original_title=m.get("original_title") or "",
            release_date=m.get("release_date") or m.get("first_air_date") or "",
            media_type=media_type,
            use_cache=True,
        )
        _merge_item(m, payload)
        done += 1
        if delay_s > 0:
            time.sleep(delay_s)
    return done


def lookup_items_batch(items: list[dict[str, Any]], *, max_items: int = 20) -> dict[str, dict[str, Any]]:
    """API / istemci: TMDB id → sinemalar payload."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    out: dict[str, dict[str, Any]] = {}
    slice_ = items[:max_items]

    def _one(raw: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
        tid = raw.get("id")
        if tid is None:
            return None
        media_type: Literal["movie", "tv"] = (
            "tv" if raw.get("media_type") == "tv" else "movie"
        )
        payload = lookup(
            title=str(raw.get("title") or ""),
            original_title=str(raw.get("original_title") or ""),
            release_date=str(raw.get("release_date") or raw.get("first_air_date") or ""),
            media_type=media_type,
            use_cache=True,
        )
        return str(int(tid)), payload

    workers = min(4, max(1, len(slice_)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_one, raw) for raw in slice_]
        for fut in as_completed(futures, timeout=45):
            try:
                row = fut.result()
                if row:
                    out[row[0]] = row[1]
            except Exception as exc:
                logger.debug("Sinemalar batch satırı atlandı: %s", exc)
    return out


def _sinemalar_warm_sort_key(m: dict[str, Any], current_month: str) -> tuple[int, int, float]:
    rm = m.get("release_month") or (m.get("release_date") or m.get("first_air_date") or "")[:7]
    missing = 0 if m.get("sinemalar_found") is None else 1
    in_cur = 0 if rm == current_month else 1
    pop = float(m.get("popularity") or 0)
    return (missing, in_cur, -pop)


def attach_to_upcoming_data(
    data: dict[str, Any],
    *,
    max_lookups: int = 0,
    current_month: str | None = None,
) -> None:
    """TMDB upcoming dict içindeki tüm film/dizi listelerine Sinemalar alanları ekler."""
    from datetime import date

    if not current_month:
        current_month = date.today().strftime("%Y-%m")
    lists: list[list[dict[str, Any]]] = []
    for key in ("theatrical", "streaming", "turkish_only", "tv_series", "high_potential"):
        raw = data.get(key)
        if isinstance(raw, list):
            lists.append(raw)
    for key in ("theatrical_by_month", "streaming_by_month", "turkish_by_month", "tv_by_month"):
        by_m = data.get(key) or {}
        if isinstance(by_m, dict):
            for month_items in by_m.values():
                if isinstance(month_items, list):
                    lists.append(month_items)

    seen: set[int] = set()
    unique: list[dict[str, Any]] = []
    for lst in lists:
        for m in lst:
            mid = m.get("id")
            if mid is not None and mid in seen:
                continue
            if mid is not None:
                seen.add(mid)
            unique.append(m)

    apply_cached_sinemalar(unique)
    if max_lookups > 0:
        unique.sort(key=lambda m: _sinemalar_warm_sort_key(m, current_month))
        warm_sinemalar_cache(unique, max_lookups=max_lookups, delay_s=0.12)
