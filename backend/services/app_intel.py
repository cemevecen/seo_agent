"""Mobil mağaza (Google Play + App Store web) yorum analitiği."""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_RAW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 90 * 60

# Google Play: continuation token ile sayfalama (çok büyük değerler ilk yüklemeyi uzatır).
GOOGLE_PLAY_MAX_REVIEWS = 1_200

# App Store web: tek vitrin ~50 yorum; birçok ülke vitrini birleştirerek geçmiş artar.
_IOS_STOREFRONTS: tuple[str, ...] = (
    "tr",
    "us",
    "gb",
    "de",
    "fr",
    "nl",
    "se",
    "no",
    "dk",
    "fi",
    "it",
    "pt",
    "pl",
    "ru",
    "cz",
    "ro",
    "ie",
    "at",
    "ch",
    "be",
    "jp",
    "kr",
    "au",
    "nz",
    "ca",
    "br",
    "mx",
    "in",
    "ae",
    "sa",
    "sg",
    "hk",
    "tw",
    "th",
    "vn",
    "id",
    "my",
    "ph",
)

APP_PRODUCTS: dict[str, dict[str, str]] = {
    "doviz": {
        "label": "Döviz",
        "android_package": "com.Doviz",
        "android_url": "https://play.google.com/store/apps/details?id=com.Doviz&hl=tr",
        "ios_app_id": "465599322",
        "ios_slug": "d%C3%B6viz-kur-alt%C4%B1n-borsa-koin",
        "ios_url": "https://apps.apple.com/tr/app/d%C3%B6viz-kur-alt%C4%B1n-borsa-koin/id465599322",
    },
    "sinemalar": {
        "label": "Sinemalar",
        "android_package": "com.nokta.sinemalar",
        "android_url": "https://play.google.com/store/apps/details?id=com.nokta.sinemalar&hl=tr",
        "ios_app_id": "711475888",
        "ios_slug": "sinemalar-com-vizyon-filmleri",
        "ios_url": "https://apps.apple.com/tr/app/sinemalar-com-vizyon-filmleri/id711475888",
    },
}

# (category_id, Türkçe etiket, anahtar kelimeler küçük harf)
_REVIEW_CATEGORIES: list[tuple[str, str, tuple[str, ...]]] = [
    ("reklam", "Reklam / IAP", ("reklam", "reklamlı", "ads", "advertisement", "içi satın", "in-app purchase")),
    ("performans", "Performans / hata", ("hata", "çök", "crash", "donma", "yavaş", "açılmıyor", "bug", "update", "güncelle")),
    ("bildirim", "Bildirim / alarm", ("bildirim", "notification", "alarm", "push")),
    ("arama", "Arama / keşif", ("arama", "search", "bulamıyorum", "bulamadım", "listem")),
    ("arayuz", "Arayüz / UX", ("tasarım", "arayüz", "arayuz", "ui", "karanlık mod", "dark mode", "çözünürlük")),
    ("dogruluk", "Veri / doğruluk", ("yanlış", "doğru değil", "güncel değil", "eksik veri", "sinema yoktu")),
    ("ozellik", "Özellik isteği", ("özellik", "ekleyin", "ekler misiniz", "olsun", "feature", "lütfen")),
    ("olumlu", "Olumlu (genel)", ("teşekkür", "süper", "mükemmel", "harika", "başarılı", "çok iyi", "great", "love", "perfect")),
    ("olumsuz", "Olumsuz (genel)", ("kötü", "berbat", "rezalet", "kullanılmaz", "sil", "waste", "terrible", "annoying")),
]

_UTC = timezone.utc


def list_products() -> list[dict[str, str]]:
    return [{"id": k, "label": v["label"]} for k, v in APP_PRODUCTS.items()]


def _parse_ios_review_page(html: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """App Store 'see-all=reviews' HTML içinden yorum + mağaza rozet özeti."""
    reviews_out: list[dict[str, Any]] = []
    pat = re.compile(
        r'"date":"([0-9]{4}-[0-9]{2}-[0-9]{2}T[^"]+)".{0,800}?"contents":"([^"]{0,12000})","rating":([1-5])',
        re.DOTALL,
    )
    for m in pat.finditer(html):
        raw_date, raw_body, r_s = m.group(1), m.group(2), m.group(3)
        try:
            body = raw_body.encode("utf-8").decode("unicode_escape")
        except Exception:
            body = raw_body.replace('\\"', '"').replace("\\n", "\n")
        try:
            if raw_date.endswith("Z"):
                dt = datetime.strptime(raw_date[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=_UTC)
            else:
                dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            continue
        reviews_out.append({"at": dt, "score": int(r_s), "text": body})

    snap: dict[str, Any] = {}
    m_badge = re.search(
        r'"type":"rating"[^}]*"content":\{"rating":([0-9.]+),"ratingFormatted":"([0-9.]+)"\}[^}]*"heading":"([^"]+)"',
        html,
    )
    if m_badge:
        snap["score"] = float(m_badge.group(1))
        snap["score_formatted"] = m_badge.group(2)
        snap["ratings_caption"] = m_badge.group(3)
    m_icon = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html)
    if m_icon:
        snap["icon"] = m_icon.group(1)
    return reviews_out, snap


def _ios_review_key(at: datetime, text: str, score: int) -> str:
    payload = f"{at.isoformat()}\0{text}\0{score}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def _fetch_ios_one_storefront(
    app_id: str, ios_slug: str, loc: str,
) -> tuple[str, list[dict[str, Any]], dict[str, Any], bool, str | None]:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    url = f"https://apps.apple.com/{loc}/app/{ios_slug}/id{app_id}"
    params = {"see-all": "reviews"}
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
        revs, page_snap = _parse_ios_review_page(r.text)
        return loc, revs, page_snap, True, None
    except Exception as e:
        return loc, [], {}, False, str(e)


def _fetch_ios_reviews_multistore(
    app_id: str, ios_slug: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None, int, int]:
    """Birden fazla ülke mağaza sayfasından yorum birleştir (kimlik: tarih+metin+puan)."""
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    snap: dict[str, Any] = {}
    storefronts_ok = 0
    last_err: str | None = None
    n_sf = len(_IOS_STOREFRONTS)
    max_workers = min(12, n_sf)
    by_loc: dict[str, tuple[list[dict[str, Any]], dict[str, Any], bool, str | None]] = {}

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_loc = {
                pool.submit(_fetch_ios_one_storefront, app_id, ios_slug, loc): loc
                for loc in _IOS_STOREFRONTS
            }
            for fut in as_completed(future_to_loc):
                loc = future_to_loc[fut]
                _loc, revs, page_snap, ok, one_err = fut.result()
                by_loc[loc] = (revs, page_snap, ok, one_err)
    except Exception as e:
        last_err = str(e)
        logger.warning("App Store çoklu vitrin hatası (%s): %s", app_id, e)

    for loc in _IOS_STOREFRONTS:
        if loc not in by_loc:
            continue
        revs, page_snap, ok, one_err = by_loc[loc]
        if ok:
            storefronts_ok += 1
            if page_snap and not snap:
                snap = page_snap
            for rv in revs:
                k = _ios_review_key(rv["at"], rv["text"], rv["score"])
                if k in seen:
                    continue
                seen.add(k)
                merged.append(rv)
        elif one_err:
            last_err = one_err
            logger.debug("App Store vitrin atlandı (%s %s): %s", loc, app_id, one_err)

    err: str | None = None if merged else last_err
    return merged, snap, err, storefronts_ok, n_sf


def _fetch_google_bundle(
    package: str, max_reviews: int = GOOGLE_PLAY_MAX_REVIEWS,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    """google-play-scraper ile mağaza özeti + en yeni yorumlar."""
    try:
        from google_play_scraper import Sort, app as gp_app
        from google_play_scraper import reviews as gp_reviews
    except ImportError:
        return {}, [], "google-play-scraper kurulu değil; sunucuda: pip install -r requirements.txt"

    err: str | None = None
    try:
        meta = gp_app(package, lang="tr", country="tr")
    except Exception as e:
        logger.warning("Play meta alınamadı (%s): %s", package, e)
        meta = {}
        err = str(e)

    collected: list[dict[str, Any]] = []
    token = None
    try:
        while len(collected) < max_reviews:
            batch_n = min(200, max_reviews - len(collected))
            chunk, token = gp_reviews(
                package,
                lang="tr",
                country="tr",
                sort=Sort.NEWEST,
                count=batch_n,
                continuation_token=token,
            )
            collected.extend(chunk)
            if not token:
                break
    except Exception as e:
        logger.warning("Play yorumları alınamadı (%s): %s", package, e)
        if err is None:
            err = str(e)

    norm: list[dict[str, Any]] = []
    for rv in collected:
        at = rv.get("at")
        if isinstance(at, datetime):
            dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
        else:
            continue
        norm.append({"at": dt, "score": int(rv.get("score") or 0), "text": (rv.get("content") or "")})

    return meta, norm, err


def _categorize(text: str) -> str:
    low = (text or "").lower()
    for cat_id, _label, keys in _REVIEW_CATEGORIES:
        if any(k in low for k in keys):
            return cat_id
    return "diger"


def _cutoff(days: int) -> datetime:
    return datetime.now(tz=_UTC) - timedelta(days=days)


def _filter_by_period(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    start = _cutoff(days)
    return [r for r in rows if r["at"] >= start]


def _filter_by_period_or_anchor(
    rows: list[dict[str, Any]], days: int
) -> tuple[list[dict[str, Any]], datetime | None, str | None]:
    """Önce bugünden geriye `days` gün; yorum yoksa çekilen kümedeki en yeni tarihe göre son `days` gün."""
    cal = _filter_by_period(rows, days)
    if cal:
        return cal, None, None
    if not rows:
        return [], None, None
    anchor = max(r["at"] for r in rows)
    start = anchor - timedelta(days=days)
    anchored = [r for r in rows if r["at"] >= start]
    note = (
        f"Bu aralıkta (bugünden geri {days} gün) örnek yorum yok; "
        f"grafikler en güncel örnek tarihine göre ({anchor.astimezone(_UTC).date().isoformat()} UTC günü) kaydırıldı."
    )
    return anchored, anchor, note


def _daily_rating_series(
    rows: list[dict[str, Any]],
    days: int,
    anchor_end: datetime | None = None,
) -> list[dict[str, Any]]:
    """UTC takvim günü bazında, dönemdeki yorumların günlük ortalama yıldızı (mağaza genel ortalama geçmişi değil)."""
    by_day: dict[str, list[int]] = {}
    if anchor_end is None:
        end_d = datetime.now(tz=_UTC).date()
    else:
        end_d = anchor_end.astimezone(_UTC).date()
    start_d = end_d - timedelta(days=days - 1)
    for r in rows:
        d = r["at"].date()
        if d < start_d or d > end_d:
            continue
        k = d.isoformat()
        by_day.setdefault(k, []).append(r["score"])
    out: list[dict[str, Any]] = []
    cur = start_d
    while cur <= end_d:
        k = cur.isoformat()
        scores = by_day.get(k, [])
        out.append(
            {
                "day": k,
                "avg_rating": round(sum(scores) / len(scores), 3) if scores else None,
                "review_count": len(scores),
            }
        )
        cur += timedelta(days=1)
    return out


def _histogram_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    c = Counter()
    for r in rows:
        s = r["score"]
        if 1 <= s <= 5:
            c[str(s)] += 1
    return {str(i): c.get(str(i), 0) for i in range(1, 6)}


def _satisfaction_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sat = uns = neu = 0
    for r in rows:
        s = r["score"]
        if s >= 4:
            sat += 1
        elif s <= 2:
            uns += 1
        else:
            neu += 1
    n = len(rows) or 1
    return {
        "memnun": sat,
        "notr": neu,
        "memnun_degil": uns,
        "memnun_oran": round(100.0 * sat / n, 1),
        "memnun_degil_oran": round(100.0 * uns / n, 1),
        "notr_oran": round(100.0 * neu / n, 1),
    }


def _category_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cc: Counter[str] = Counter()
    labels = {cid: lab for cid, lab, _ in _REVIEW_CATEGORIES}
    labels["diger"] = "Diğer"
    for r in rows:
        cc[_categorize(r.get("text") or "")] += 1
    return [{"id": k, "label": labels.get(k, k), "count": v} for k, v in cc.most_common()]


def _android_histogram_overall(meta: dict[str, Any]) -> dict[str, int] | None:
    h = meta.get("histogram")
    if not h or len(h) != 5:
        return None
    return {str(i + 1): int(h[i]) for i in range(5)}


def get_raw_product_data(product_id: str) -> dict[str, Any]:
    if product_id not in APP_PRODUCTS:
        return {"error": "unknown_product"}
    spec = APP_PRODUCTS[product_id]
    now = time.time()
    cache_key = product_id
    with _CACHE_LOCK:
        hit = _RAW_CACHE.get(cache_key)
        if hit and now - hit[0] < _CACHE_TTL_SEC:
            return hit[1]

    meta, g_rows, g_err = _fetch_google_bundle(spec["android_package"])
    i_rows, i_snap, i_err, i_sf_ok, i_sf_n = _fetch_ios_reviews_multistore(
        spec["ios_app_id"], spec["ios_slug"],
    )

    payload = {
        "product_id": product_id,
        "label": spec["label"],
        "urls": {
            "android": spec["android_url"],
            "ios": spec["ios_url"],
        },
        "fetched_at": datetime.now(tz=_UTC).isoformat(),
        "android": {
            "meta": {
                "score": meta.get("score"),
                "ratings": meta.get("ratings"),
                "histogram": _android_histogram_overall(meta),
                "reviews": meta.get("reviews"),
                "icon": meta.get("icon"),
            },
            "reviews": g_rows,
            "error": g_err,
        },
        "ios": {
            "meta": i_snap,
            "reviews": i_rows,
            "error": i_err,
            "storefronts_ok": i_sf_ok,
            "storefronts_total": i_sf_n,
            "note_tr": None,
        },
    }

    with _CACHE_LOCK:
        _RAW_CACHE[cache_key] = (now, payload)
    return payload


def build_intel_payload(product_id: str, period_days: int) -> dict[str, Any]:
    valid_periods = (1, 7, 30, 180, 365)
    if period_days not in valid_periods:
        period_days = 7
    raw = get_raw_product_data(product_id)
    if raw.get("error"):
        return raw

    intel: dict[str, Any] = {
        "product_id": product_id,
        "label": raw["label"],
        "product_key": product_id,
        "app_icon": raw["android"]["meta"].get("icon") or (raw["ios"]["meta"] or {}).get("icon"),
        "urls": raw["urls"],
        "fetched_at": raw["fetched_at"],
        "errors": {"android": raw["android"].get("error"), "ios": raw["ios"].get("error")},
        "scrape": {
            "android_review_samples": len(raw["android"]["reviews"]),
            "ios_review_samples": len(raw["ios"]["reviews"]),
            "ios_storefronts_ok": raw["ios"].get("storefronts_ok"),
            "ios_storefronts_total": raw["ios"].get("storefronts_total"),
        },
        "windows": {},
    }

    for p in valid_periods:
        fa, fa_anchor, fa_note = _filter_by_period_or_anchor(raw["android"]["reviews"], p)
        fi, fi_anchor, fi_note = _filter_by_period_or_anchor(raw["ios"]["reviews"], p)
        intel["windows"][str(p)] = {
            "period_days": p,
            "android": {
                "review_count_period": len(fa),
                "period_note_tr": fa_note,
                "rating_series": _daily_rating_series(fa, p, fa_anchor),
                "star_distribution_period": _histogram_counts(fa),
                "star_distribution_overall": raw["android"]["meta"].get("histogram"),
                "store_score": raw["android"]["meta"].get("score"),
                "store_ratings": raw["android"]["meta"].get("ratings"),
                "satisfaction": _satisfaction_split(fa),
                "categories": _category_counts(fa),
            },
            "ios": {
                "review_count_period": len(fi),
                "period_note_tr": fi_note,
                "rating_series": _daily_rating_series(fi, p, fi_anchor),
                "star_distribution_period": _histogram_counts(fi),
                "store_score": (raw["ios"]["meta"] or {}).get("score"),
                "store_ratings_caption": (raw["ios"]["meta"] or {}).get("ratings_caption"),
                "satisfaction": _satisfaction_split(fi),
                "categories": _category_counts(fi),
                "note_tr": raw["ios"].get("note_tr"),
            },
        }

    intel["active_window"] = intel["windows"][str(period_days)]
    return intel


def intel_json_safe(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: intel_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [intel_json_safe(v) for v in obj]
    if isinstance(obj, float):
        return float(obj) if obj == obj else None
    return obj
