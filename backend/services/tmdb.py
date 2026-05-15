"""TMDB (The Movie Database) entegrasyonu — vizyon takvimi ve film içerik planlama."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import requests

from backend.config import settings

logger = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w185"


def _headers() -> dict[str, str]:
    token = (settings.tmdb_read_access_token or "").strip()
    if not token:
        raise RuntimeError("TMDB_READ_ACCESS_TOKEN tanımlanmamış. Railway Variables'a ekleyin.")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _get(path: str, params: dict | None = None) -> dict[str, Any]:
    url = TMDB_BASE + path
    resp = requests.get(url, headers=_headers(), params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Yardımcılar ──────────────────────────────────────────────────────────────

def _poster_url(poster_path: str | None) -> str:
    if poster_path:
        return TMDB_IMG + poster_path
    return ""


def _tr_release(release_dates: dict) -> str | None:
    """Türkiye vizyon tarihi (ISO)."""
    for entry in release_dates.get("results", []):
        if entry.get("iso_3166_1") == "TR":
            dates = entry.get("release_dates", [])
            if dates:
                return (dates[0].get("release_date") or "")[:10]
    return None


def _popularity_label(pop: float) -> str:
    if pop >= 500:  return "🔥 Çok Yüksek"
    if pop >= 200:  return "⭐ Yüksek"
    if pop >= 80:   return "📈 Orta"
    return "📉 Düşük"


# ── Ana veri çekme fonksiyonları ──────────────────────────────────────────────

def fetch_upcoming_in_turkey(
    months_ahead: int = 4,
    page_limit: int = 5,
) -> list[dict[str, Any]]:
    """
    Türkiye'de vizyona girecek filmler (TMDB /discover/movie, region=TR).
    Bugünden itibaren `months_ahead` ay ilerisi sorgulanır.
    """
    today      = date.today()
    date_from  = today.strftime("%Y-%m-%d")
    date_to    = (today + timedelta(days=30 * months_ahead)).strftime("%Y-%m-%d")

    movies: list[dict] = []
    seen: set[int]     = set()

    for page in range(1, page_limit + 1):
        data = _get("/discover/movie", {
            "region":                      "TR",
            "language":                    "tr-TR",
            "primary_release_date.gte":    date_from,
            "primary_release_date.lte":    date_to,
            "sort_by":                     "popularity.desc",
            "include_adult":               "false",
            "page":                        page,
        })
        results = data.get("results", [])
        if not results:
            break
        for m in results:
            mid = m["id"]
            if mid in seen:
                continue
            seen.add(mid)
            movies.append(_enrich(m))
        if page >= data.get("total_pages", 1):
            break

    return sorted(movies, key=lambda x: x["release_date"] or "9999")


def fetch_turkish_productions(
    year_from: int | None = None,
    months_ahead: int = 6,
    page_limit: int = 4,
) -> list[dict[str, Any]]:
    """
    Türk yapımı filmler (original_language=tr).
    Hem yakında çıkacaklar hem popüler yakın tarihli Türk filmler.
    """
    today     = date.today()
    date_from = (date(year_from, 1, 1) if year_from
                 else today).strftime("%Y-%m-%d")
    date_to   = (today + timedelta(days=30 * months_ahead)).strftime("%Y-%m-%d")

    movies: list[dict] = []
    seen: set[int]     = set()

    for page in range(1, page_limit + 1):
        data = _get("/discover/movie", {
            "with_original_language":       "tr",
            "primary_release_date.gte":     date_from,
            "primary_release_date.lte":     date_to,
            "sort_by":                      "popularity.desc",
            "include_adult":                "false",
            "language":                     "tr-TR",
            "page":                         page,
        })
        results = data.get("results", [])
        if not results:
            break
        for m in results:
            mid = m["id"]
            if mid in seen:
                continue
            seen.add(mid)
            entry = _enrich(m)
            entry["is_turkish"] = True
            movies.append(entry)
        if page >= data.get("total_pages", 1):
            break

    return sorted(movies, key=lambda x: x["release_date"] or "9999")


def _enrich(m: dict) -> dict[str, Any]:
    """Ham TMDB film kaydını UI için hazır formata çevirir."""
    genres = m.get("genre_ids", [])
    release = (m.get("release_date") or "")[:10]

    return {
        "id":               m["id"],
        "title":            m.get("title") or m.get("original_title") or "",
        "original_title":   m.get("original_title") or "",
        "release_date":     release,
        "release_month":    release[:7] if release else "",   # "2026-06"
        "poster_url":       _poster_url(m.get("poster_path")),
        "popularity":       round(float(m.get("popularity") or 0), 1),
        "popularity_label": _popularity_label(float(m.get("popularity") or 0)),
        "vote_average":     round(float(m.get("vote_average") or 0), 1),
        "vote_count":       int(m.get("vote_count") or 0),
        "overview":         (m.get("overview") or "")[:300],
        "genre_ids":        genres,
        "is_turkish":       m.get("original_language") == "tr",
        "tmdb_url":         f"https://www.themoviedb.org/movie/{m['id']}",
    }


def fetch_combined_upcoming(months_ahead: int = 5) -> dict[str, Any]:
    """
    Dashboard sayfası için tek çağrı:
    - Türkiye'deki tüm yaklaşan filmler
    - Türk yapımları birleşik
    Sonuçlar aya göre gruplanmış döner.
    """
    try:
        all_movies = fetch_upcoming_in_turkey(months_ahead=months_ahead)
    except Exception as exc:
        logger.error("TMDB upcoming Turkey hatası: %s", exc)
        all_movies = []

    try:
        turkish = fetch_turkish_productions(months_ahead=months_ahead)
    except Exception as exc:
        logger.error("TMDB Turkish productions hatası: %s", exc)
        turkish = []

    # Türk yapımlarını birleştir — zaten upcoming listesinde olabilir
    seen_ids = {m["id"] for m in all_movies}
    for m in turkish:
        if m["id"] not in seen_ids:
            all_movies.append(m)
            seen_ids.add(m["id"])
        else:
            # Var olanı Türk yapımı olarak işaretle
            for existing in all_movies:
                if existing["id"] == m["id"]:
                    existing["is_turkish"] = True
                    break

    # Release date'e göre sırala
    all_movies.sort(key=lambda x: x["release_date"] or "9999")

    # Aya göre grupla
    by_month: dict[str, list] = {}
    for m in all_movies:
        month_key = m["release_month"] or "Tarih yok"
        by_month.setdefault(month_key, []).append(m)

    # Ay içinde popülerliğe göre sırala
    for month_key in by_month:
        by_month[month_key].sort(key=lambda x: -x["popularity"])

    # Özet istatistikler
    turkish_count  = sum(1 for m in all_movies if m.get("is_turkish"))
    high_potential = [m for m in all_movies if m["popularity"] >= 100]

    return {
        "all_movies":       all_movies,
        "by_month":         by_month,
        "total":            len(all_movies),
        "turkish_count":    turkish_count,
        "high_potential":   high_potential,
        "months_ahead":     months_ahead,
    }
