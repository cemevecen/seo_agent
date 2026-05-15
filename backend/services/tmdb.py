"""TMDB (The Movie Database) — vizyon takvimi + platform yayınları, içerik planlama."""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import requests

from backend.config import settings

logger = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w185"

# Sinemalar.com için anlamlı diller — Hintçe/Korece/Tagalogca vs. hariç
ACCEPTED_LANGUAGES = {"en", "tr", "fr", "de", "es", "it"}

# Türkiye'deki major streaming platformları (TMDB provider ID)
# Netflix=8, Prime=119, Disney+=337, AppleTV+=350, Mubi=11, BluTV=341, beIN=32, exxen=1869
STREAMING_PROVIDERS_TR = "8|119|337|350|11|341|32"

# Türkiye karasal + dijital-karasal kanalları (TMDB network ID)
# ATV=1932, Kanal D=1560, Show TV=1573, Star TV=1566,
# TRT1=545, TRT(genel)=544, FOX/NOW TV=1567, TV8=2120,
# Kanal 7=2119, TRT2=2218, Show Max ≈ BluTV ile örtüşür
TR_KARASAL_NETWORKS = "1932|1560|1573|1566|545|544|1567|2120|2119|2218"

MONTH_NAMES_TR = {
    "01": "Ocak",  "02": "Şubat",  "03": "Mart",   "04": "Nisan",
    "05": "Mayıs", "06": "Haziran","07": "Temmuz",  "08": "Ağustos",
    "09": "Eylül", "10": "Ekim",   "11": "Kasım",   "12": "Aralık",
}

PROVIDER_NAMES = {
    8: "Netflix", 119: "Prime Video", 337: "Disney+",
    350: "Apple TV+", 11: "Mubi", 341: "BluTV", 32: "beIN",
    1869: "exxen",
}


def _headers() -> dict[str, str]:
    token = (settings.tmdb_read_access_token or "").strip()
    if not token:
        raise RuntimeError("TMDB_READ_ACCESS_TOKEN tanımlanmamış. Railway Variables'a ekleyin.")
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _get(path: str, params: dict | None = None) -> dict[str, Any]:
    resp = requests.get(TMDB_BASE + path, headers=_headers(),
                        params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _poster_url(p: str | None) -> str:
    return TMDB_IMG + p if p else ""


def _popularity_label(pop: float) -> str:
    if pop >= 500: return "Çok Yüksek"
    if pop >= 200: return "Yüksek"
    if pop >= 80:  return "Orta"
    return "Düşük"


def _enrich(m: dict, providers: list[str] | None = None) -> dict[str, Any]:
    release = (m.get("release_date") or "")[:10]
    return {
        "id":               m["id"],
        "title":            m.get("title") or m.get("original_title") or "",
        "original_title":   m.get("original_title") or "",
        "original_language":m.get("original_language") or "",
        "release_date":     release,
        "release_month":    release[:7] if release else "",
        "poster_url":       _poster_url(m.get("poster_path")),
        "popularity":       round(float(m.get("popularity") or 0), 1),
        "popularity_label": _popularity_label(float(m.get("popularity") or 0)),
        "vote_average":     round(float(m.get("vote_average") or 0), 1),
        "vote_count":       int(m.get("vote_count") or 0),
        "overview":         (m.get("overview") or "")[:280],
        "is_turkish":       m.get("original_language") == "tr",
        "tmdb_url":         f"https://www.themoviedb.org/movie/{m['id']}",
        "providers":        providers or [],   # platform isimleri listesi
    }


def _fetch_pages(params: dict, page_limit: int = 5) -> list[dict]:
    """Sayfalı TMDB /discover/movie sorgusu."""
    results: list[dict] = []
    seen: set[int] = set()
    for page in range(1, page_limit + 1):
        data = _get("/discover/movie", {**params, "page": page})
        for m in data.get("results", []):
            if m["id"] not in seen:
                seen.add(m["id"])
                results.append(m)
        if page >= data.get("total_pages", 1):
            break
    return results


# Bitiş tarihi sabit; başlangıç her çağrıda bu ayın 1'i hesaplanır
YEAR_TO = "2026-12-31"


def _current_month_start() -> str:
    """Bugünün ayının ilk günü — örn. "2026-05-01". Her ay otomatik ilerler."""
    today = date.today()
    return today.replace(day=1).strftime("%Y-%m-%d")

# ── 1. Sinema vizyon (theatrical) ─────────────────────────────────────────────

def fetch_theatrical_turkey(months_ahead: int = 4) -> list[dict[str, Any]]:
    """
    Türkiye'de sinemada gösterilecek filmler — çift sorgu ile tam kapsam.

    Sorgu A: release_date + region=TR
      → Türkiye'ye özgü vizyon tarihi kayıtlıları + yeniden vizyonlar (eski filmler).

    Sorgu B: primary_release_date (bölge fark etmez)
      → Dünya prömiyeri 2025-2026 olan tüm filmler.
      → "Avengers: Doomsday", "Şrek 5" gibi büyük yapımlar
        TR vizyon tarihi TMDB'ye henüz girmemiş olsa bile yakalanır.

    İkisi birleştirilip ID bazlı tekrar önlenir.
    """
    date_from = _current_month_start()
    date_to   = YEAR_TO

    seen: set[int] = set()
    raw: list[dict] = []

    # A: Türkiye bölgesel vizyon tarihi (yeniden vizyonlar dahil)
    for m in _fetch_pages({
        "region":            "TR",
        "language":          "tr-TR",
        "release_date.gte":  date_from,
        "release_date.lte":  date_to,
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "with_release_type": "3",
    }, page_limit=12):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    # B: Dünya prömiyeri 2025-2026 (TR vizyon tarihi TMDB'de yoksa da gelir)
    for m in _fetch_pages({
        "language":                 "tr-TR",
        "primary_release_date.gte": date_from,
        "primary_release_date.lte": date_to,
        "sort_by":                  "popularity.desc",
        "include_adult":            "false",
    }, page_limit=15):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    movies = [_enrich(m) for m in raw]
    movies.sort(key=lambda x: x["release_date"] or "9999")
    return movies


# ── 2. Platform yayınları (streaming) ────────────────────────────────────────

def fetch_streaming_turkey(months_ahead: int = 4) -> list[dict[str, Any]]:
    """
    Netflix, Disney+, Prime, BluTV vb. platformlarda Türkiye'de yayına girecek filmler.
    Dijital/yayın vizyon tarihi (release_type=4) kullanılır.
    """
    date_from = _current_month_start()
    date_to   = YEAR_TO

    seen: set[int] = set()
    raw: list[dict] = []

    # A: Türkiye'de platforma girmiş / girişi kayıtlı
    for m in _fetch_pages({
        "watch_region":          "TR",
        "with_watch_providers":  STREAMING_PROVIDERS_TR,
        "language":              "tr-TR",
        "release_date.gte":      date_from,
        "release_date.lte":      date_to,
        "sort_by":               "popularity.desc",
        "include_adult":         "false",
    }, page_limit=12):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    # B: Dünya geneli platform çıkışları (watch_region kaydı henüz yoksa da gelir)
    for m in _fetch_pages({
        "with_watch_providers":     STREAMING_PROVIDERS_TR,
        "language":                 "tr-TR",
        "primary_release_date.gte": date_from,
        "primary_release_date.lte": date_to,
        "sort_by":                  "popularity.desc",
        "include_adult":            "false",
    }, page_limit=10):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    movies = [_enrich(m) for m in raw]
    movies.sort(key=lambda x: x["release_date"] or "9999")
    return movies


# ── 3. Türk yapımları (tüm platformlar) ───────────────────────────────────────

def fetch_turkish_productions(months_ahead: int = 6) -> list[dict[str, Any]]:
    """Türkçe orijinal dilli filmler — sinema + platform fark etmez."""
    date_from = _current_month_start()
    date_to   = YEAR_TO

    raw = _fetch_pages({
        "with_original_language": "tr",
        "release_date.gte":       date_from,
        "release_date.lte":       date_to,
        "sort_by":                "popularity.desc",
        "include_adult":          "false",
        "language":               "tr-TR",
    }, page_limit=6)

    movies = [_enrich(m) for m in raw]
    movies.sort(key=lambda x: x["release_date"] or "9999")
    return movies


# ── TV Dizileri ───────────────────────────────────────────────────────────────

TV_STATUS_TR = {
    "Returning Series":  "Devam Ediyor",
    "Planned":           "Planlandı",
    "In Production":     "Yapım Aşamasında",
    "Ended":             "Bitti",
    "Cancelled":         "İptal",
    "Pilot":             "Pilot",
}


def _enrich_tv(m: dict) -> dict[str, Any]:
    """Ham TMDB TV kaydını UI formatına çevirir."""
    first_air = (m.get("first_air_date") or "")[:10]
    networks = m.get("networks") or []
    network_names = [n.get("name", "") for n in networks if n.get("name")]
    status_en = m.get("status", "")
    return {
        "id":               m["id"],
        "title":            m.get("name") or m.get("original_name") or "",
        "original_title":   m.get("original_name") or "",
        "original_language":m.get("original_language") or "",
        "first_air_date":   first_air,
        "release_date":     first_air,          # ortak alan adı (template uyumu)
        "release_month":    first_air[:7] if first_air else "",
        "poster_url":       _poster_url(m.get("poster_path")),
        "popularity":       round(float(m.get("popularity") or 0), 1),
        "popularity_label": _popularity_label(float(m.get("popularity") or 0)),
        "vote_average":     round(float(m.get("vote_average") or 0), 1),
        "vote_count":       int(m.get("vote_count") or 0),
        "overview":         (m.get("overview") or "")[:280],
        "is_turkish":       m.get("original_language") == "tr",
        "tmdb_url":         f"https://www.themoviedb.org/tv/{m['id']}",
        "networks":         network_names,
        "status":           TV_STATUS_TR.get(status_en, status_en),
        "seasons":          int(m.get("number_of_seasons") or 0),
        "media_type":       "tv",
    }


def _fetch_tv_pages(params: dict, page_limit: int = 4) -> list[dict]:
    """Sayfalı /discover/tv sorgusu."""
    results: list[dict] = []
    seen: set[int] = set()
    for page in range(1, page_limit + 1):
        data = _get("/discover/tv", {**params, "page": page})
        for m in data.get("results", []):
            if m["id"] not in seen:
                seen.add(m["id"])
                results.append(m)
        if page >= data.get("total_pages", 1):
            break
    return results


def fetch_turkish_tv_karasal(months_ahead: int = 6) -> list[dict[str, Any]]:
    """
    Karasal kanallarda (ATV, Kanal D, Show, Star, TRT1, NOW/FOX, TV8 …)
    yayına girecek veya yeni sezonu başlayacak Türk dizileri.

    İki kaynaktan çeker:
    1. with_networks filtresi ile bilinen kanal ID'leri
    2. with_original_language=tr  (ID eşleşmeyeni kurtarmak için)
    Her ikisini birleştirip dil + popularity filtresi uygular.
    """
    date_from = _current_month_start()
    date_to   = YEAR_TO

    base_params = {
        "language":          "tr-TR",
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "air_date.gte":      date_from,
        "air_date.lte":      date_to,
    }

    seen: set[int] = set()
    raw: list[dict] = []

    # 1. Bilinen karasal kanal ID'leri ile
    for m in _fetch_tv_pages({**base_params, "with_networks": TR_KARASAL_NETWORKS}, page_limit=5):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    # 2. Türkçe orijinal dil + karasal kanalda olabilecekler
    for m in _fetch_tv_pages({**base_params, "with_original_language": "tr"}, page_limit=4):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    # Filtrele: sadece Türkçe orijinal dilli veya Türk kanalı
    series = []
    for m in raw:
        lang = m.get("original_language", "")
        if lang == "tr":
            series.append(_enrich_tv(m))

    series.sort(key=lambda x: (-x["popularity"], x["release_date"] or "9999"))
    return series


def fetch_turkish_tv_returning(page_limit: int = 4) -> list[dict[str, Any]]:
    """
    Hâlihazırda devam eden Türk dizileri — yeni sezonu yakında başlayacaklar.
    /tv/on_the_air + dil filtresi.
    """
    seen: set[int] = set()
    series: list[dict] = []
    for page in range(1, page_limit + 1):
        data = _get("/tv/on_the_air", {"language": "tr-TR", "page": page})
        for m in data.get("results", []):
            if m.get("original_language") == "tr" and m["id"] not in seen:
                seen.add(m["id"])
                series.append(_enrich_tv(m))
        if page >= data.get("total_pages", 1):
            break
    series.sort(key=lambda x: -x["popularity"])
    return series


# ── Ana birleştirici ──────────────────────────────────────────────────────────

def fetch_combined_upcoming(months_ahead: int = 5) -> dict[str, Any]:
    """
    Dashboard için tek çağrı — üç liste döner:
    theatrical, streaming, turkish_only (diğerlerinde olmayan Türk yapımları)
    """
    theatrical: list[dict] = []
    streaming:  list[dict] = []
    turkish:    list[dict] = []

    try:
        theatrical = fetch_theatrical_turkey(months_ahead)
    except Exception as exc:
        logger.error("TMDB theatrical hatası: %s", exc)

    try:
        streaming = fetch_streaming_turkey(months_ahead)
    except Exception as exc:
        logger.error("TMDB streaming hatası: %s", exc)

    try:
        turkish = fetch_turkish_productions(months_ahead)
    except Exception as exc:
        logger.error("TMDB Turkish productions hatası: %s", exc)

    # ── Diziler ──────────────────────────────────────────────────────────────
    tv_upcoming: list[dict] = []
    tv_returning: list[dict] = []
    try:
        tv_upcoming = fetch_turkish_tv_karasal(months_ahead)
    except Exception as exc:
        logger.error("TMDB TV karasal hatası: %s", exc)
    try:
        tv_returning = fetch_turkish_tv_returning()
    except Exception as exc:
        logger.error("TMDB TV returning hatası: %s", exc)

    # Birleştir: upcoming + returning (daha önce eklenmediyse)
    tv_seen = {m["id"] for m in tv_upcoming}
    for m in tv_returning:
        if m["id"] not in tv_seen:
            tv_upcoming.append(m)
            tv_seen.add(m["id"])

    tv_upcoming.sort(key=lambda x: (-x["popularity"], x["release_date"] or "9999"))

    # theatrical + streaming ID'lerini işaretle
    theatrical_ids = {m["id"] for m in theatrical}
    streaming_ids  = {m["id"] for m in streaming}
    known_ids      = theatrical_ids | streaming_ids

    # Türk yapımlarından diğerlerinde olmayanları ayır
    turkish_only = [m for m in turkish if m["id"] not in known_ids]

    # theatrical ve streaming'e de Türk işareti ekle
    for lst in (theatrical, streaming):
        for m in lst:
            if m.get("original_language") == "tr":
                m["is_turkish"] = True

    def group_by_month(lst: list[dict]) -> dict[str, list]:
        by_m: dict[str, list] = {}
        for m in sorted(lst, key=lambda x: x["release_date"] or "9999"):
            key = m["release_month"] or "Tarih yok"
            by_m.setdefault(key, []).append(m)
        # Ay içinde populariteye göre sırala
        for k in by_m:
            by_m[k].sort(key=lambda x: -x["popularity"])
        return by_m

    all_combined = theatrical + [m for m in streaming if m["id"] not in theatrical_ids] + turkish_only
    high_potential = sorted(
        [m for m in all_combined if m["popularity"] >= 100],
        key=lambda x: -x["popularity"],
    )

    return {
        "theatrical":          theatrical,
        "theatrical_by_month": group_by_month(theatrical),
        "streaming":           streaming,
        "streaming_by_month":  group_by_month(streaming),
        "turkish_only":        turkish_only,
        "turkish_by_month":    group_by_month(turkish_only),
        "tv_series":           tv_upcoming,
        "tv_by_month":         group_by_month(tv_upcoming),
        "high_potential":      high_potential[:15],
        "months_ahead":        months_ahead,
        "total_theatrical":    len(theatrical),
        "total_streaming":     len(streaming),
        "total_turkish":       len(turkish),
        "total_tv":            len(tv_upcoming),
    }
