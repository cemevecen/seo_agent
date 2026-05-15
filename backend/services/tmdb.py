"""TMDB (The Movie Database) — vizyon takvimi + platform yayınları, içerik planlama."""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, timedelta
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


# Dil kodu → varsayılan ülke kodu (origin_country yoksa fallback)
_LANG_TO_COUNTRY: dict[str, str] = {
    "en": "US", "tr": "TR", "fr": "FR", "de": "DE", "es": "ES",
    "it": "IT", "ja": "JP", "ko": "KR", "zh": "CN", "ru": "RU",
    "pt": "BR", "ar": "SA", "hi": "IN", "pl": "PL", "nl": "NL",
    "sv": "SE", "da": "DK", "fi": "FI", "nb": "NO", "hu": "HU",
    "cs": "CZ", "ro": "RO", "uk": "UA", "he": "IL", "th": "TH",
    "id": "ID", "ms": "MY", "vi": "VN", "fa": "IR", "ur": "PK",
    "az": "AZ", "ka": "GE", "hy": "AM", "bg": "BG", "hr": "HR",
    "sk": "SK", "sl": "SI", "sr": "RS", "lt": "LT", "lv": "LV",
    "et": "EE", "el": "GR", "mk": "MK", "sq": "AL", "bs": "BA",
    "gl": "ES", "ca": "ES", "eu": "ES", "af": "ZA", "sw": "KE",
    "bn": "BD", "ta": "IN", "te": "IN", "ml": "IN", "mr": "IN",
    "pa": "IN", "gu": "IN", "kn": "IN", "si": "LK", "ne": "NP",
    "km": "KH", "lo": "LA", "my": "MM", "mn": "MN", "uz": "UZ",
    "kk": "KZ", "tg": "TJ", "tk": "TM", "ky": "KG",
    "am": "ET", "ha": "NG", "yo": "NG", "ig": "NG", "so": "SO",
    "is": "IS",
}


def _country_flag(country_code: str) -> str:
    """ISO 3166-1 alpha-2 → bayrak emoji (ör. 'US' → '🇺🇸')."""
    cc = (country_code or "").upper().strip()
    if len(cc) != 2 or not cc.isalpha():
        return ""
    return chr(0x1F1E6 + ord(cc[0]) - 65) + chr(0x1F1E6 + ord(cc[1]) - 65)


def _resolve_country_code(m: dict) -> str:
    """TMDB ham kaydından ISO 3166-1 alpha-2 ülke kodu (küçük harf, flagcdn için)."""
    countries = m.get("origin_country") or []
    code = countries[0] if countries else _LANG_TO_COUNTRY.get(
        str(m.get("original_language") or ""), ""
    )
    return code.lower() if code else ""


def _resolve_flag(m: dict) -> str:
    """TMDB ham kaydından ülke bayrağı emoji'si çıkar."""
    code = _resolve_country_code(m).upper()
    return _country_flag(code)


# ── In-memory cache (3 saat TTL) ─────────────────────────────────────────────
_cache_lock    = threading.Lock()
_cache_data:  dict | None = None
_cache_time:  datetime | None = None
_CACHE_TTL    = timedelta(hours=3)


def _enrich_missing_countries(movies: list[dict], limit: int = 25) -> None:
    """country_code boş filmler için /movie/{id} detay çekip günceller. Max `limit` film."""
    empty = [m for m in movies if not m.get("country_code")][:limit]
    if not empty:
        return
    for m in empty:
        try:
            detail = _get(f"/movie/{m['id']}")
            prod = detail.get("production_countries") or []
            if prod:
                cc = prod[0]["iso_3166_1"]
                m["country_code"] = cc.lower()
                m["country_flag"] = _country_flag(cc.upper())
            else:
                oc = detail.get("origin_country") or []
                if oc:
                    m["country_code"] = oc[0].lower()
                    m["country_flag"] = _country_flag(oc[0].upper())
            time.sleep(0.05)
        except Exception as exc:
            logger.debug("Country detail fetch atlandı [%s]: %s", m.get("id"), exc)


def get_combined_upcoming(months_ahead: int = 5) -> dict:
    """Cache'li fetch_combined_upcoming — 3 saatte bir yenilenir."""
    global _cache_data, _cache_time
    with _cache_lock:
        if (
            _cache_data is not None
            and _cache_time is not None
            and datetime.utcnow() - _cache_time < _CACHE_TTL
        ):
            return _cache_data
    # Cache miss veya süresi dolmuş
    fresh = fetch_combined_upcoming(months_ahead)
    with _cache_lock:
        _cache_data = fresh
        _cache_time = datetime.utcnow()
    return fresh


def refresh_combined_cache(months_ahead: int = 5) -> dict:
    """Scheduler job'u için: cache'i zorunlu yeniler."""
    global _cache_data, _cache_time
    fresh = fetch_combined_upcoming(months_ahead)
    with _cache_lock:
        _cache_data = fresh
        _cache_time = datetime.utcnow()
    logger.info("TMDB combined cache yenilendi — theatrical=%d streaming=%d tv=%d",
                len(fresh.get("theatrical", [])),
                len(fresh.get("streaming", [])),
                len(fresh.get("tv_series", [])))
    return fresh


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
        "country_flag":     _resolve_flag(m),
        "country_code":     _resolve_country_code(m),
        "tmdb_url":         f"https://www.themoviedb.org/movie/{m['id']}",
        "providers":        providers or [],
    }


def search_movie_by_title(title: str) -> dict[str, Any] | None:
    """Başlık ile TMDB'de film arar, en iyi eşleşmeyi zenginleştirilmiş dict olarak döner."""
    try:
        data = _get("/search/movie", {
            "query":          title,
            "language":       "tr-TR",
            "include_adult":  "false",
        })
        results = data.get("results", [])
        if results:
            return _enrich(results[0])
    except Exception as exc:
        logger.warning("TMDB title search hatası [%s]: %s", title, exc)
    return None


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


# Film sekmeleri (Sinema, Platform, Türk Filmleri): bu ayın 1'inden başlar — _current_month_start()
# Türk Dizileri sekmesi: yıl başından başlar — TV_YEAR_FROM (dönem başı ilerleyebilir)
YEAR_TO     = "2026-12-31"
TV_YEAR_FROM = "2025-09-01"  # Türk dizileri — Eylül 2025'ten itibaren


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
    # with_release_type=3 → sadece theatrical; streaming özel çekimler elenir
    for m in _fetch_pages({
        "language":                 "tr-TR",
        "primary_release_date.gte": date_from,
        "primary_release_date.lte": date_to,
        "sort_by":                  "popularity.desc",
        "include_adult":            "false",
        "with_release_type":        "3",
    }, page_limit=15):
        if m["id"] not in seen:
            seen.add(m["id"])
            raw.append(m)

    movies = [_enrich(m) for m in raw]
    movies.sort(key=lambda x: x["release_date"] or "9999")
    _enrich_missing_countries(movies)
    return movies


# Platform ID → görünen isim  (watch_region=TR)
TR_PROVIDERS: dict[int, str] = {
    8:    "Netflix",
    337:  "Disney+",
    1899: "Max",           # HBO Max → Max
    119:  "Prime Video",
    350:  "Apple TV+",
    11:   "Mubi",
    341:  "BluTV",
    32:   "beIN Connect",
    1869: "exxen",
    # TV+ (Turkcell) henüz TMDB'de kayıtlı değil; eklenirse buraya
}
STREAMING_PROVIDERS_TR = "|".join(str(k) for k in TR_PROVIDERS)

# Kart üzerindeki platform rozet renkleri
PROVIDER_COLORS: dict[str, str] = {
    "Netflix":       "bg-red-600 text-white",
    "Disney+":       "bg-blue-700 text-white",
    "Max":           "bg-purple-700 text-white",
    "Prime Video":   "bg-sky-600 text-white",
    "Apple TV+":     "bg-slate-900 text-white",
    "Mubi":          "bg-rose-900 text-white",
    "BluTV":         "bg-orange-500 text-white",
    "beIN Connect":  "bg-green-700 text-white",
    "exxen":         "bg-indigo-700 text-white",
}


# ── 2. Platform yayınları (streaming) ────────────────────────────────────────

def fetch_streaming_turkey(months_ahead: int = 4) -> list[dict[str, Any]]:
    """
    Her platformu ayrı sorgular; hangi platformda olduğu etiketlenir.
    Başlangıç: bu ayın 1'i (Sinema sekmesiyle aynı mantık).
    """
    date_from = _current_month_start()
    date_to   = YEAR_TO

    # id → {movie_dict, providers:[...]}
    all_movies: dict[int, dict] = {}

    base_params = {
        "language":      "tr-TR",
        "sort_by":       "popularity.desc",
        "include_adult": "false",
    }

    for provider_id, provider_name in TR_PROVIDERS.items():
        pid = str(provider_id)

        # A: watch_region=TR — doğru ama az veri
        for m in _fetch_pages({
            **base_params,
            "watch_region":         "TR",
            "with_watch_providers":  pid,
            "release_date.gte":     date_from,
            "release_date.lte":     date_to,
        }, page_limit=4):
            mid = m["id"]
            if mid not in all_movies:
                all_movies[mid] = _enrich(m)
                all_movies[mid]["providers"] = []
                all_movies[mid]["_provider_hits"] = 0
            if provider_name not in all_movies[mid]["providers"]:
                all_movies[mid]["providers"].append(provider_name)
            all_movies[mid]["_provider_hits"] += 1

        # B: global (watch_region yok) — daha fazla içerik ama overlap riski var
        for m in _fetch_pages({
            **base_params,
            "with_watch_providers":     pid,
            "primary_release_date.gte": date_from,
            "primary_release_date.lte": date_to,
        }, page_limit=4):
            mid = m["id"]
            pop = float(m.get("popularity") or 0)
            if mid not in all_movies:
                all_movies[mid] = _enrich(m)
                all_movies[mid]["providers"] = []
                all_movies[mid]["_provider_hits"] = 0
            if provider_name not in all_movies[mid]["providers"]:
                all_movies[mid]["providers"].append(provider_name)
            all_movies[mid]["_provider_hits"] += 1

    # 5+ farklı platformda çıkıyorsa global katalog false positive — etiketi sıfırla
    for m in all_movies.values():
        if len(m.get("providers", [])) >= 5:
            m["providers"] = []
        m.pop("_provider_hits", None)

    # Bu aydan önceki orijinal vizyon tarihli filmleri filtrele
    movies = [
        m for m in all_movies.values()
        if (m.get("release_date") or "9999") >= date_from
    ]
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
        "country_flag":     _resolve_flag(m),
        "country_code":     _resolve_country_code(m),
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
    date_from = TV_YEAR_FROM   # Türk dizileri: yıl başından — bu ay filtresiz
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

    # Box office Turkey — gişedeki ama TMDB theatrical listesinde olmayan filmleri ekle
    try:
        from backend.services.boxoffice_turkey import fetch_current_boxoffice, find_missing_from_tmdb
        boxoffice_films = fetch_current_boxoffice()
        if boxoffice_films:
            existing_ids: set[int] = {m["id"] for m in theatrical}
            extra = find_missing_from_tmdb(boxoffice_films, existing_ids, search_movie_by_title)
            for film in extra:
                film["boxoffice_source"] = True
                theatrical.append(film)
            _enrich_missing_countries(extra)
            if extra:
                logger.info("Gişe takviminden %d film eklendi", len(extra))
    except Exception as exc:
        logger.error("BOT gişe entegrasyon hatası: %s", exc)

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

    # first_air_date < TV_YEAR_FROM olan eski dizileri filtrele
    # (Survivor gibi /tv/on_the_air'den gelen 2005 tarihliler çıkar)
    tv_upcoming = [
        m for m in tv_upcoming
        if (m.get("first_air_date") or "0000") >= TV_YEAR_FROM
    ]

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
        # Ay içinde tarihe göre artan sıra (05-13 önce, 05-27 sonra)
        for k in by_m:
            by_m[k].sort(key=lambda x: x["release_date"] or "9999")
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
