"""TMDB (The Movie Database) — vizyon takvimi + platform yayınları, içerik planlama."""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, timedelta
from typing import Any

import requests

from backend.config import settings

logger = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w185"

# Sinemalar.com için anlamlı diller — Hintçe/Korece/Tagalogca vs. hariç
ACCEPTED_LANGUAGES = {"en", "tr", "fr", "de", "es", "it"}

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

PROVIDER_NAMES = dict(TR_PROVIDERS) if "TR_PROVIDERS" in dir() else {}  # noqa: updated below after TR_STREAMING_PROVIDERS


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


# ── In-memory cache (6 saat TTL, stale-while-revalidate) ─────────────────────
_cache_lock        = threading.Lock()
_refresh_lock      = threading.Lock()   # aynı anda sadece bir yenileme
_bg_refresh_active = threading.Event()  # birden fazla bg thread spawn olmasın
_cache_data:   dict | None = None
_cache_mono:   float | None = None  # time.monotonic() snapshots
_CACHE_TTL_S   = 6 * 3600           # 6 saat TTL


def _cache_fresh() -> bool:
    return _cache_mono is not None and time.monotonic() - _cache_mono < _CACHE_TTL_S


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


def _do_refresh(months_ahead: int) -> None:
    """Refresh lock altında cache'i yeniler (hem sync hem bg thread kullanır)."""
    global _cache_data, _cache_mono
    with _refresh_lock:
        with _cache_lock:
            if _cache_fresh():
                return  # başkası yetişti
        fresh = fetch_combined_upcoming(months_ahead)
        try:
            from backend.services.sinemalar_match import attach_to_upcoming_data

            attach_to_upcoming_data(fresh, max_lookups=250)
        except Exception:
            logger.exception("Sinemalar eşleştirme (TMDB cache refresh) atlandı")
        with _cache_lock:
            _cache_data = fresh
            _cache_mono = time.monotonic()


def get_combined_upcoming(months_ahead: int = 5) -> dict:
    """Stale-while-revalidate cache — kullanıcıyı asla bekletmez.

    • Cache taze  → anında dön.
    • Cache eski ama veri var → eskiyi anında dön, arka planda yenile.
    • Cache hiç yok (ilk başlatma) → bekle, prewarm thread bitene kadar.
    """
    global _cache_data, _cache_mono
    with _cache_lock:
        fresh  = _cache_fresh()
        stale  = _cache_data  # None veya eski dict

    if fresh:
        return stale  # type: ignore[return-value]

    if stale is not None:
        # Eski veriyi anında dön, arka planda yenile
        if not _bg_refresh_active.is_set():
            _bg_refresh_active.set()
            def _bg():
                try:
                    _do_refresh(months_ahead)
                finally:
                    _bg_refresh_active.clear()
            threading.Thread(target=_bg, daemon=True, name="tmdb-stale-refresh").start()
        return stale

    # İlk başlatma — veri hiç yok, beklemek zorunda
    _do_refresh(months_ahead)
    with _cache_lock:
        return _cache_data or {}


def refresh_combined_cache(months_ahead: int = 5) -> dict:
    """Scheduler job'u veya manuel tetik için: cache'i zorunlu yeniler."""
    global _cache_data, _cache_mono
    fresh = fetch_combined_upcoming(months_ahead)
    with _cache_lock:
        _cache_data = fresh
        _cache_mono = time.monotonic()
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


# /movie/{id}/release_dates — aynı refresh döngüsünde tekrar çağrıyı keser
_release_dates_cache: dict[int, dict[str, Any]] = {}

# TMDB release_dates.type — https://developer.themoviedb.org/reference/movie-release-dates
_RELEASE_TYPE_THEATRICAL = frozenset({1, 2, 3})  # prömiyer, sınırlı, geniş vizyon
_RELEASE_TYPE_DIGITAL = frozenset({4})


def _clear_release_dates_cache() -> None:
    global _release_dates_cache
    _release_dates_cache = {}


def _fetch_movie_release_dates_payload(movie_id: int) -> dict[str, Any]:
    if movie_id in _release_dates_cache:
        return _release_dates_cache[movie_id]
    try:
        data = _get(f"/movie/{movie_id}/release_dates")
    except Exception as exc:
        logger.debug("release_dates atlandı movie=%s: %s", movie_id, exc)
        data = {}
    _release_dates_cache[movie_id] = data
    return data


def _earliest_tr_release_by_types(data: dict[str, Any], allowed_types: frozenset[int]) -> str | None:
    """TR bölgesinde belirtilen yayın tiplerinden en erken tarih (YYYY-MM-DD)."""
    best: str | None = None
    for country in data.get("results") or []:
        if (country.get("iso_3166_1") or "").upper() != "TR":
            continue
        for rd in country.get("release_dates") or []:
            if int(rd.get("type") or 0) not in allowed_types:
                continue
            raw = (rd.get("release_date") or "")[:10]
            if len(raw) < 10:
                continue
            if best is None or raw < best:
                best = raw
    return best


def _tr_movie_release_dates(data: dict[str, Any]) -> tuple[str | None, str | None]:
    """(Türkiye sinema vizyonu, Türkiye dijital) — yoksa None."""
    theatrical = _earliest_tr_release_by_types(data, _RELEASE_TYPE_THEATRICAL)
    digital = _earliest_tr_release_by_types(data, _RELEASE_TYPE_DIGITAL)
    return theatrical, digital


def _apply_tr_release_dates_for_catalog(
    theatrical: list[dict[str, Any]],
    streaming: list[dict[str, Any]],
    turkish: list[dict[str, Any]],
) -> None:
    """Kartlarda mümkün olduğunda Türkiye vizyon / dijital tarihini göster."""
    theatrical_ids = {int(m["id"]) for m in theatrical}
    turkish_ids = {int(m["id"]) for m in turkish}
    streaming_movie_ids = {
        int(m["id"])
        for m in streaming
        if (m.get("media_type") or "movie") == "movie"
    }
    by_id: dict[int, list[dict[str, Any]]] = {}
    for m in theatrical + turkish + streaming:
        if (m.get("media_type") or "movie") != "movie":
            continue
        by_id.setdefault(int(m["id"]), []).append(m)

    for mid, items in by_id.items():
        payload = _fetch_movie_release_dates_payload(mid)
        tr_theatrical, tr_digital = _tr_movie_release_dates(payload)
        streaming_only = mid in streaming_movie_ids and mid not in theatrical_ids and mid not in turkish_ids
        for item in items:
            primary = (item.get("release_date") or "")[:10]
            tr_date = (tr_digital if streaming_only else None) or tr_theatrical or tr_digital
            if not tr_date:
                continue
            if tr_date != primary and primary:
                item["release_date_global"] = primary
            item["release_date"] = tr_date
            item["release_month"] = tr_date[:7]
            item["release_date_tr"] = tr_date
        time.sleep(0.035)


def _enrich(m: dict, providers: list[str] | None = None) -> dict[str, Any]:
    release = (m.get("release_date") or "")[:10]
    prov = providers or []
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
        "providers":        prov,
        "provider_slugs":   _provider_slugs(prov),
        "media_type":       "movie",
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
# Türk Dizileri — çok eski /tv/on_the_air gürültüsünü kesmek için alt sınır (first_air_date)
TV_FIRST_AIR_FLOOR = "2024-01-01"

# Dijital Türk platformları (discover/tv watch_region=TR)
TR_OTT_TV_PROVIDER_IDS = "341|1869|4893"  # BluTV, exxen, Tabii


def _year_end() -> str:
    """Vizyon ufku: bir sonraki takvim yılının sonu — yıl geçişinde otomatik ilerler."""
    return f"{date.today().year + 1}-12-31"


def _current_month_start() -> str:
    """Bugünün ayının ilk günü — örn. "2026-05-01". Her ay otomatik ilerler."""
    today = date.today()
    return today.replace(day=1).strftime("%Y-%m-%d")


def _is_turkish_origin(m: dict) -> bool:
    """TMDB ham kaydı: Türk yapımı (dil veya origin_country)."""
    if (m.get("original_language") or "") == "tr":
        return True
    countries = m.get("origin_country") or []
    return "TR" in countries


_ACTIVE_TV_STATUS_TR = frozenset({
    "Devam Ediyor", "Planlandı", "Yapım Aşamasında", "Pilot",
})


def _tv_keep_after_on_air_filter(m: dict) -> bool:
    """Devam eden eski formatları ele, yeni sezon / yakın tarihli yapımları tut."""
    first = (m.get("first_air_date") or "0000")[:10]
    if first >= TV_FIRST_AIR_FLOOR:
        return True
    return (m.get("status") or "") in _ACTIVE_TV_STATUS_TR

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
    date_to   = _year_end()

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


# Türkiye OTT — TMDB watch/provider ID (watch_region=TR)
# Sıra: UI filtre çubuğu + tarama önceliği
TR_STREAMING_PROVIDERS: list[dict[str, Any]] = [
    {"id": 8,    "name": "Netflix",      "slug": "netflix",  "filter_label": "Netflix"},
    {"id": 119,  "name": "Prime Video",  "slug": "prime",    "filter_label": "Prime"},
    {"id": 337,  "name": "Disney+",      "slug": "disney",   "filter_label": "Disney+"},
    {"id": 1899, "name": "Max",          "slug": "max",      "filter_label": "Max (HBO)"},
    {"id": 350,  "name": "Apple TV+",    "slug": "apple",    "filter_label": "Apple TV+"},
    {"id": 1904, "name": "TV+",          "slug": "tvplus",   "filter_label": "TV+"},
    {"id": 11,   "name": "Mubi",         "slug": "mubi",     "filter_label": "Mubi"},
    {"id": 341,  "name": "BluTV",        "slug": "blutv",    "filter_label": "BluTV"},
    {"id": 32,   "name": "beIN Connect", "slug": "bein",     "filter_label": "beIN"},
    {"id": 1869, "name": "exxen",        "slug": "exxen",    "filter_label": "exxen"},
]

TR_PROVIDERS: dict[int, str] = {int(p["id"]): str(p["name"]) for p in TR_STREAMING_PROVIDERS}
STREAMING_PROVIDERS_TR = "|".join(str(p["id"]) for p in TR_STREAMING_PROVIDERS)
_PROVIDER_SLUG_BY_NAME = {p["name"]: p["slug"] for p in TR_STREAMING_PROVIDERS}

# Kart üzerindeki platform rozet renkleri
PROVIDER_COLORS: dict[str, str] = {
    "Netflix":       "bg-red-600 text-white",
    "Disney+":       "bg-blue-700 text-white",
    "Max":           "bg-purple-700 text-white",
    "Prime Video":   "bg-sky-600 text-white",
    "Apple TV+":     "bg-slate-900 text-white",
    "TV+":           "bg-cyan-700 text-white",
    "Mubi":          "bg-rose-900 text-white",
    "BluTV":         "bg-orange-500 text-white",
    "beIN Connect":  "bg-green-700 text-white",
    "exxen":         "bg-indigo-700 text-white",
}


def streaming_provider_filters() -> list[dict[str, str]]:
    """UI platform filtre çubuğu — slug + görünen etiket."""
    return [{"slug": p["slug"], "label": p["filter_label"], "name": p["name"]} for p in TR_STREAMING_PROVIDERS]


def _provider_slugs(names: list[str]) -> str:
    """Kart data-provider-slugs — pipe ile ayrılmış filtre anahtarları."""
    slugs = [_PROVIDER_SLUG_BY_NAME.get(n, "") for n in names]
    return "|".join(s for s in slugs if s)


# ── 2. Platform yayınları (streaming) ────────────────────────────────────────

def _streaming_store_key(media_type: str, item_id: int) -> str:
    return f"{media_type}:{item_id}"


def _merge_streaming_movie(store: dict[str, dict], raw: dict) -> None:
    key = _streaming_store_key("movie", raw["id"])
    if key not in store:
        store[key] = _enrich(raw)
        store[key]["providers"] = []


def _merge_streaming_tv(store: dict[str, dict], raw: dict) -> None:
    key = _streaming_store_key("tv", raw["id"])
    if key not in store:
        entry = _enrich_tv(raw)
        entry["providers"] = []
        store[key] = entry


def _fetch_tr_ott_provider_names(media_type: str, tmdb_id: int) -> list[str]:
    """TR bölgesinde flatrate OTT — yalnızca izlediğimiz sağlayıcı ID'leri."""
    mt = "tv" if media_type == "tv" else "movie"
    try:
        data = _get(f"/{mt}/{tmdb_id}/watch/providers", {"watch_region": "TR"})
    except Exception as exc:  # noqa: BLE001
        logger.debug("watch/providers atlandı %s/%s: %s", mt, tmdb_id, exc)
        return []
    tr = (data.get("results") or {}).get("TR") or {}
    order = {str(p["name"]): i for i, p in enumerate(TR_STREAMING_PROVIDERS)}
    tracked_ids = {int(p["id"]) for p in TR_STREAMING_PROVIDERS}
    id_to_name = {int(p["id"]): str(p["name"]) for p in TR_STREAMING_PROVIDERS}
    seen: set[str] = set()
    names: list[str] = []
    for prov in tr.get("flatrate") or []:
        pid = int(prov.get("provider_id") or 0)
        if pid not in tracked_ids:
            continue
        name = id_to_name[pid]
        if name in seen:
            continue
        seen.add(name)
        names.append(name)
    names.sort(key=lambda n: order.get(n, 999))
    return names


def _apply_tr_watch_providers_to_store(store: dict[str, dict]) -> None:
    """Discover birleşiminden sonra rozetleri TMDB TR watch/providers ile doğrula."""
    for entry in store.values():
        mt = entry.get("media_type") or "movie"
        names = _fetch_tr_ott_provider_names(mt, int(entry["id"]))
        entry["providers"] = names
        entry["provider_slugs"] = _provider_slugs(names)
        time.sleep(0.04)


def fetch_streaming_turkey(months_ahead: int = 4) -> list[dict[str, Any]]:
    """
    Her OTT platformu için ayrı TMDB discover (film + dizi) — aday havuzu.
    Kart rozetleri discover etiketi değil; birleşim sonrası watch/providers (TR)
    flatrate ile doğrulanır (global discover rozet biriktirmez).
    """
    date_from = _current_month_start()
    date_to   = _year_end()
    store: dict[str, dict] = {}

    movie_base = {
        "language":      "tr-TR",
        "sort_by":       "popularity.desc",
        "include_adult": "false",
    }
    tv_base = {
        "language":          "tr-TR",
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "air_date.gte":      date_from,
        "air_date.lte":      date_to,
    }

    for prov in TR_STREAMING_PROVIDERS:
        pid = str(int(prov["id"]))

        # Film — TR bölgesel yayın tarihi
        for m in _fetch_pages({
            **movie_base,
            "watch_region":            "TR",
            "with_watch_providers":    pid,
            "with_watch_monetization_types": "flatrate",
            "release_date.gte":        date_from,
            "release_date.lte":        date_to,
        }, page_limit=10):
            _merge_streaming_movie(store, m)

        # Film — global keşif (TR vizyon tarihi yoksa; rozet discover'dan eklenmez)
        for m in _fetch_pages({
            **movie_base,
            "with_watch_providers":         pid,
            "with_watch_monetization_types": "flatrate",
            "primary_release_date.gte":     date_from,
            "primary_release_date.lte":     date_to,
        }, page_limit=6):
            _merge_streaming_movie(store, m)

        # Dizi — TR
        for m in _fetch_tv_pages({
            **tv_base,
            "watch_region":            "TR",
            "with_watch_providers":    pid,
            "with_watch_monetization_types": "flatrate",
        }, page_limit=8):
            _merge_streaming_tv(store, m)

        # Dizi — global (aday; rozet API ile)
        for m in _fetch_tv_pages({
            **tv_base,
            "with_watch_providers":         pid,
            "with_watch_monetization_types": "flatrate",
        }, page_limit=5):
            _merge_streaming_tv(store, m)

    if store:
        _apply_tr_watch_providers_to_store(store)

    items = [
        m for m in store.values()
        if (m.get("release_date") or "9999") >= date_from
    ]
    items.sort(key=lambda x: (x.get("release_date") or "9999", -(x.get("popularity") or 0)))
    return items


# ── 3. Türk yapımları (tüm platformlar) ───────────────────────────────────────

def fetch_turkish_productions(months_ahead: int = 6) -> list[dict[str, Any]]:
    """Türk yapımı filmler — dil TR veya origin_country TR; vizyon + global prömiyer."""
    date_from = _current_month_start()
    date_to   = _year_end()

    seen: set[int] = set()
    raw: list[dict] = []

    queries = [
        {
            "with_original_language": "tr",
            "release_date.gte":       date_from,
            "release_date.lte":       date_to,
        },
        {
            "with_origin_country":    "TR",
            "release_date.gte":       date_from,
            "release_date.lte":       date_to,
        },
        {
            "with_original_language": "tr",
            "primary_release_date.gte": date_from,
            "primary_release_date.lte": date_to,
        },
    ]
    common = {
        "sort_by":       "popularity.desc",
        "include_adult": "false",
        "language":      "tr-TR",
    }
    for q in queries:
        for m in _fetch_pages({**common, **q}, page_limit=8):
            if not _is_turkish_origin(m):
                continue
            if m["id"] not in seen:
                seen.add(m["id"])
                raw.append(m)

    movies = [_enrich(m) for m in raw]
    movies.sort(key=lambda x: (x["release_date"] or "9999", -(x.get("popularity") or 0)))
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
        "providers":        [],
        "provider_slugs":   "",
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


def _merge_turkish_tv_raw(raw: list[dict], seen: set[int], m: dict) -> None:
    if not _is_turkish_origin(m):
        return
    if m["id"] not in seen:
        seen.add(m["id"])
        raw.append(m)


def fetch_turkish_tv_karasal(months_ahead: int = 6) -> list[dict[str, Any]]:
    """
    Karasal + TR yapımı diziler — bölüm yayın tarihi penceresi (bu ay → yıl sonu+1).

    Kaynaklar: with_networks (ATV, Kanal D, …), dil TR, origin_country TR.
    """
    date_from = _current_month_start()
    date_to   = _year_end()

    base_params = {
        "language":          "tr-TR",
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "air_date.gte":      date_from,
        "air_date.lte":      date_to,
    }

    seen: set[int] = set()
    raw: list[dict] = []

    for m in _fetch_tv_pages({**base_params, "with_networks": TR_KARASAL_NETWORKS}, page_limit=8):
        _merge_turkish_tv_raw(raw, seen, m)
    for m in _fetch_tv_pages({**base_params, "with_original_language": "tr"}, page_limit=8):
        _merge_turkish_tv_raw(raw, seen, m)
    for m in _fetch_tv_pages({**base_params, "with_origin_country": "TR"}, page_limit=6):
        _merge_turkish_tv_raw(raw, seen, m)

    series = [_enrich_tv(m) for m in raw]
    series.sort(key=lambda x: (-x["popularity"], x["release_date"] or "9999"))
    return series


def fetch_turkish_tv_planned_production() -> list[dict[str, Any]]:
    """Planlandı / yapım aşamasında TR dizileri — henüz air_date olmayanlar dahil."""
    seen: set[int] = set()
    raw: list[dict] = []
    base = {
        "language":          "tr-TR",
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "with_status":       "1|2",  # Planned, In Production
    }
    for m in _fetch_tv_pages({**base, "with_origin_country": "TR"}, page_limit=6):
        _merge_turkish_tv_raw(raw, seen, m)
    for m in _fetch_tv_pages({**base, "with_original_language": "tr"}, page_limit=5):
        _merge_turkish_tv_raw(raw, seen, m)
    return [_enrich_tv(m) for m in raw]


def fetch_turkish_tv_ott() -> list[dict[str, Any]]:
    """BluTV, exxen, Tabii vb. TR OTT — flatrate discover (bölüm tarihi penceresi)."""
    date_from = _current_month_start()
    date_to   = _year_end()
    seen: set[int] = set()
    raw: list[dict] = []
    base = {
        "language":          "tr-TR",
        "sort_by":           "popularity.desc",
        "include_adult":     "false",
        "watch_region":      "TR",
        "with_watch_monetization_types": "flatrate",
        "air_date.gte":      date_from,
        "air_date.lte":      date_to,
    }
    for pid in TR_OTT_TV_PROVIDER_IDS.split("|"):
        for m in _fetch_tv_pages({**base, "with_watch_providers": pid}, page_limit=5):
            _merge_turkish_tv_raw(raw, seen, m)
    return [_enrich_tv(m) for m in raw]


def fetch_turkish_tv_returning(page_limit: int = 8) -> list[dict[str, Any]]:
    """
    Hâlihazırda devam eden Türk dizileri — /tv/on_the_air (konuşulan / sezon devam).
    """
    seen: set[int] = set()
    series: list[dict] = []
    for page in range(1, page_limit + 1):
        data = _get("/tv/on_the_air", {"language": "tr-TR", "page": page})
        for m in data.get("results", []):
            if not _is_turkish_origin(m) or m["id"] in seen:
                continue
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
    _clear_release_dates_cache()
    theatrical: list[dict] = []
    streaming:  list[dict] = []
    turkish:    list[dict] = []

    try:
        theatrical = fetch_theatrical_turkey(months_ahead)
    except Exception as exc:
        logger.error("TMDB theatrical hatası: %s", exc)

    # Box office Turkey — gişedeki ama TMDB theatrical listesinde olmayan filmleri ekle
    # Sadece bu hafta fiilen vizyonda olan filmler aranır (weekly_audience > 0).
    # Takvimde listelenen ama seyircisi olmayan filmler TMDB'de zaten vardır.
    try:
        from backend.services.boxoffice_turkey import fetch_current_boxoffice, find_missing_from_tmdb
        boxoffice_films = fetch_current_boxoffice()
        if boxoffice_films:
            showing_now = [f for f in boxoffice_films if f.get("weekly_audience", 0) > 0]
            existing_ids: set[int] = {m["id"] for m in theatrical}
            extra = find_missing_from_tmdb(showing_now, existing_ids, search_movie_by_title)
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
    tv_planned: list[dict] = []
    tv_ott: list[dict] = []
    tv_returning: list[dict] = []
    try:
        tv_upcoming = fetch_turkish_tv_karasal(months_ahead)
    except Exception as exc:
        logger.error("TMDB TV karasal hatası: %s", exc)
    try:
        tv_planned = fetch_turkish_tv_planned_production()
    except Exception as exc:
        logger.error("TMDB TV planned hatası: %s", exc)
        tv_planned = []
    try:
        tv_ott = fetch_turkish_tv_ott()
    except Exception as exc:
        logger.error("TMDB TV OTT hatası: %s", exc)
        tv_ott = []
    try:
        tv_returning = fetch_turkish_tv_returning()
    except Exception as exc:
        logger.error("TMDB TV returning hatası: %s", exc)

    tv_seen = {m["id"] for m in tv_upcoming}
    for extra_batch in (tv_planned, tv_ott, tv_returning):
        for m in extra_batch:
            if m["id"] not in tv_seen:
                tv_upcoming.append(m)
                tv_seen.add(m["id"])

    tv_upcoming = [m for m in tv_upcoming if _tv_keep_after_on_air_filter(m)]

    tv_upcoming.sort(key=lambda x: (-x["popularity"], x["release_date"] or "9999"))

    # theatrical + platform film ID'lerini işaretle (diziler Türk film listesinden düşülmez)
    theatrical_ids = {m["id"] for m in theatrical}
    streaming_movie_ids = {
        m["id"] for m in streaming if (m.get("media_type") or "movie") == "movie"
    }
    known_movie_ids = theatrical_ids | streaming_movie_ids

    # Türk yapımlarından diğerlerinde olmayanları ayır
    # Sinema/platformda da olsa Türk Filmleri sekmesinde tam liste
    turkish_only = [m for m in turkish if m["id"] not in known_movie_ids]
    for m in turkish:
        m["in_theatrical_or_streaming"] = m["id"] in known_movie_ids

    # theatrical ve streaming'e de Türk işareti ekle
    for lst in (theatrical, streaming):
        for m in lst:
            if m.get("original_language") == "tr":
                m["is_turkish"] = True

    try:
        _apply_tr_release_dates_for_catalog(theatrical, streaming, turkish)
    except Exception as exc:
        logger.error("TMDB TR vizyon tarihi zenginleştirme hatası: %s", exc)

    def group_by_month(lst: list[dict]) -> dict[str, list]:
        by_m: dict[str, list] = {}
        for m in sorted(lst, key=lambda x: x["release_date"] or "9999"):
            key = m["release_month"] or "Tarih yok"
            by_m.setdefault(key, []).append(m)
        # Ay içinde tarihe göre artan sıra (05-13 önce, 05-27 sonra)
        for k in by_m:
            by_m[k].sort(key=lambda x: x["release_date"] or "9999")
        return by_m

    all_combined = theatrical + [
        m for m in streaming
        if (m.get("media_type") or "movie") == "movie" and m["id"] not in theatrical_ids
    ] + turkish_only
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
        "turkish_by_month":    group_by_month(turkish),
        "tv_series":           tv_upcoming,
        "tv_by_month":         group_by_month(tv_upcoming),
        "high_potential":      high_potential[:15],
        "months_ahead":        months_ahead,
        "total_theatrical":    len(theatrical),
        "total_streaming":     len(streaming),
        "total_turkish":       len(turkish),
        "total_tv":            len(tv_upcoming),
    }
