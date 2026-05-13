"""Mağaza katalog araması (Vivindis prensipleriyle uyumlu, bağımsız modül).

- Google Play: ``google_play_scraper.search``
- App Store listesi: Apple iTunes Search API (httpx async)
"""

from __future__ import annotations

import asyncio
import logging
import re
from functools import lru_cache
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_ITUNES_SEARCH = "https://itunes.apple.com/search"

_PLAY_SEARCH_APP_ID_ALIASES: dict[tuple[str, str], str] = {
    ("sofascore: canlı skor", "sofascore"): "com.sofascore.results",
    ("sofascore: live sports scores", "sofascore"): "com.sofascore.results",
}


def _play_store_url(package_name: str) -> str:
    return f"https://play.google.com/store/apps/details?id={package_name}"


@lru_cache(maxsize=256)
def _resolve_play_app_id_from_web_search(title: str, developer: str, lang: str, country: str) -> str:
    query = " ".join(part for part in (title.strip(), developer.strip()) if part)
    if not query:
        return ""
    try:
        resp = httpx.get(
            "https://play.google.com/store/search",
            params={"q": query, "c": "apps", "hl": lang.lower(), "gl": country.upper()},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8.0,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("google_play_web_resolve_failed query=%s err=%s", query[:80], exc)
        return ""

    seen: set[str] = set()
    for match in re.finditer(r"/store/apps/details\?id=([A-Za-z0-9_\.]+)", resp.text):
        candidate = match.group(1)
        if candidate in seen:
            continue
        seen.add(candidate)
        return candidate
    return ""


def _infer_play_app_id(row: dict[str, Any]) -> str:
    app_id = str(row.get("appId") or "").strip()
    if app_id:
        return app_id
    title = str(row.get("title") or "").strip().lower()
    developer = str(row.get("developer") or "").strip().lower()
    return _PLAY_SEARCH_APP_ID_ALIASES.get((title, developer), "")


def _app_store_url(country: str, track_id: str, fallback: str | None) -> str:
    if fallback and fallback.startswith("http"):
        return fallback
    cc = country.lower()
    return f"https://apps.apple.com/{cc}/app/id{track_id}"


def _google_play_fetch_raw(query: str, lang: str, country: str, num: int) -> list[dict[str, Any]]:
    from google_play_scraper import search as gp_search

    raw = gp_search(query, n_hits=num, lang=lang, country=country)
    if raw is None:
        return []
    return raw


def _google_play_rows_from_raw(
    raw: list[dict[str, Any]], lang: str, country: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            app_id = _infer_play_app_id(row)
            if not app_id:
                app_id = _resolve_play_app_id_from_web_search(
                    str(row.get("title") or ""),
                    str(row.get("developer") or ""),
                    lang,
                    country,
                )
            if not app_id:
                continue
            score = row.get("score")
            out.append(
                {
                    "id": app_id,
                    "name": str(row.get("title") or app_id).strip() or app_id,
                    "developer": (str(row["developer"]).strip() if row.get("developer") else None),
                    "icon": (str(row["icon"]).strip() if row.get("icon") else None),
                    "rating": float(score) if isinstance(score, (int, float)) else None,
                    "review_count": None,
                    "platform": "google_play",
                    "store_url": _play_store_url(app_id),
                },
            )
        except Exception as exc:
            logger.warning("google_play_row_parse_failed err=%s", exc)
    return out


def google_play_search_sync(query: str, lang: str, country: str, num: int) -> list[dict[str, Any]]:
    attempts: list[tuple[str, str]] = [(lang, country), (lang, "tr"), ("tr", "tr"), ("en", "us")]
    seen: set[tuple[str, str]] = set()
    attempts = [a for a in attempts if not (a in seen or seen.add(a))]

    last_exc: BaseException | None = None
    for attempt_lang, attempt_country in attempts:
        try:
            raw = _google_play_fetch_raw(query, attempt_lang, attempt_country, num)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "google_play_search_failed query=%s lang=%s country=%s err=%s",
                query[:80], attempt_lang, attempt_country, exc,
            )
            continue
        items = _google_play_rows_from_raw(raw, attempt_lang, attempt_country)
        if items or not raw:
            return items

    if last_exc is not None:
        return []
    return []


async def app_store_search_async(
    query: str,
    lang: str,
    country: str,
    num: int,
    *,
    offset: int = 0,
) -> list[dict[str, Any]]:
    _ = lang
    lim = max(1, min(num, 200))
    off = max(0, min(offset, 10_000))
    params = {
        "term": query,
        "entity": "software",
        "limit": str(lim),
        "offset": str(off),
        "country": country.lower(),
    }
    async with httpx.AsyncClient(timeout=25.0) as client:
        res = await client.get(_ITUNES_SEARCH, params=params)
        res.raise_for_status()
        data = res.json()
    raw_rows: list[dict[str, Any]] = list(data.get("results") or [])
    out: list[dict[str, Any]] = []
    for row in raw_rows:
        tid = row.get("trackId")
        if tid is None:
            continue
        bid = str(tid).strip()
        name = str(row.get("trackName") or bid).strip()
        rating = row.get("averageUserRating")
        reviews_raw = row.get("userRatingCount")
        review_count = int(reviews_raw) if isinstance(reviews_raw, int) else None
        view = row.get("trackViewUrl")
        view_s = str(view).strip() if isinstance(view, str) else None
        out.append(
            {
                "id": bid,
                "name": name or bid,
                "developer": (str(row["artistName"]).strip() if row.get("artistName") else None),
                "icon": (str(row["artworkUrl100"]).strip() if row.get("artworkUrl100") else None),
                "rating": float(rating) if isinstance(rating, (int, float)) else None,
                "review_count": review_count,
                "platform": "app_store",
                "store_url": _app_store_url(country, bid, view_s),
            },
        )
    return out


async def search_catalog(
    query: str,
    platform: str,
    lang: str,
    country: str,
    num: int,
    offset: int,
) -> tuple[list[dict[str, Any]], bool, int]:
    """platform: google_play | app_store | both"""
    query = query.strip()
    if platform == "google_play":
        rows = await asyncio.to_thread(google_play_search_sync, query, lang, country, num)
        return rows, len(rows) >= num, 0

    if platform == "app_store":
        rows = await app_store_search_async(query, lang, country, num, offset=offset)
        return rows, len(rows) >= num, offset

    gp_task = asyncio.to_thread(google_play_search_sync, query, lang, country, num)
    as_task = app_store_search_async(query, lang, country, num, offset=0)
    gp_rows, as_rows = await asyncio.gather(gp_task, as_task)
    merged = list(gp_rows) + list(as_rows)
    return merged, False, 0
