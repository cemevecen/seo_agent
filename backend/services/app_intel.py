"""Mobil mağaza (Google Play + App Store web) yorum analitiği."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from backend.database import SessionLocal
from backend.models import AppStoreRankSnapshot
from backend.services.timezone_utils import (
    inclusive_local_period_start_utc,
    report_calendar_today,
    report_calendar_tz,
)

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_RAW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
# Normal API isteklerinde mağaza kaynaklarını tekrar tekrar çağırmamak için uzun TTL.
# Zorunlu güncelleme sadece manuel tetikleme ve zamanlanmış job'da force_refresh ile yapılır.
_CACHE_TTL_SEC = 26 * 60 * 60
_FORCED_REFRESH_META_FILE = Path(__file__).resolve().parent / "app_intel_last_refresh.json"
_RANK_HISTORY_FILE = Path(__file__).resolve().parent / "app_intel_rank_history.json"
_FORCED_REFRESH_AT: dict[str, str] = {}
_RANK_HISTORY: dict[str, Any] = {}

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


def _play_updated_iso(value: Any) -> str | None:
    """google-play-scraper `updated` alanını JSON için ISO UTC string yap."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=_UTC)
        return dt.astimezone(_UTC).isoformat()
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=_UTC).isoformat()
        except Exception:
            return None
    s = str(value).strip()
    return s if s else None


def _load_forced_refresh_meta() -> None:
    global _FORCED_REFRESH_AT
    try:
        if not _FORCED_REFRESH_META_FILE.exists():
            _FORCED_REFRESH_AT = {}
            return
        data = json.loads(_FORCED_REFRESH_META_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _FORCED_REFRESH_AT = {str(k): str(v) for k, v in data.items() if v}
        else:
            _FORCED_REFRESH_AT = {}
    except Exception:
        _FORCED_REFRESH_AT = {}


def _save_forced_refresh_meta() -> None:
    try:
        _FORCED_REFRESH_META_FILE.write_text(
            json.dumps(_FORCED_REFRESH_AT, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.debug("App intel forced refresh metadata kaydedilemedi.")


def _load_rank_history() -> None:
    # Backward-compat: legacy dosya geçmişi artık kullanılmıyor (DB kullanılıyor).
    global _RANK_HISTORY
    _RANK_HISTORY = {}


def _save_rank_history() -> None:
    # Backward-compat no-op: yeni geçmiş yazımı veritabanına yapılıyor.
    return


def _parse_utc_iso(at_iso: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(at_iso or "").replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(tz=_UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def _rank_row_to_dict(row: AppStoreRankSnapshot) -> dict[str, Any]:
    dt = row.collected_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)
    return {
        "at": dt.astimezone(_UTC).isoformat(),
        "rank": int(row.rank),
        "total": row.total,
        "category": row.category_name,
        "chart": row.chart,
    }


def _load_rank_rows(product_id: str, platform: str) -> list[dict[str, Any]]:
    try:
        with SessionLocal() as db:
            rows = (
                db.query(AppStoreRankSnapshot)
                .filter(
                    AppStoreRankSnapshot.product_id == product_id,
                    AppStoreRankSnapshot.platform == platform,
                )
                .order_by(AppStoreRankSnapshot.collected_at.asc(), AppStoreRankSnapshot.id.asc())
                .all()
            )
        return [_rank_row_to_dict(r) for r in rows]
    except Exception as e:
        logger.debug("Rank history DB okunamadi (%s/%s): %s", product_id, platform, e)
        return (((_RANK_HISTORY.get(product_id) or {}).get(platform)) or [])


def _append_rank_snapshot(
    product_id: str,
    platform: str,
    rank_info: dict[str, Any] | None,
    *,
    at_iso: str,
) -> None:
    if platform not in ("android", "ios"):
        return
    if not rank_info:
        return
    rank_val = rank_info.get("rank")
    if rank_val is None:
        return
    try:
        rank_int = int(rank_val)
    except Exception:
        return
    rec = {
        "at": at_iso,
        "rank": rank_int,
        "total": rank_info.get("total"),
        "category": rank_info.get("category_name"),
        "chart": rank_info.get("chart"),
    }
    arr = _load_rank_rows(product_id, platform)
    last = arr[-1] if arr else None
    should_push = False
    if not last:
        should_push = True
    else:
        last_rank = int(last.get("rank") or -1)
        last_at = str(last.get("at") or "")
        # Sıra değiştiyse her zaman kaydet
        if last_rank != rank_int:
            should_push = True
        # Kategori değiştiyse kaydet
        elif str(last.get("category") or "") != str(rec.get("category") or ""):
            should_push = True
        else:
            # Aynı sıra — son kayıttan en az 3 saat geçtiyse yeniden kaydet
            try:
                last_dt = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=_UTC)
                now_dt = _parse_utc_iso(at_iso)
                if now_dt.tzinfo is None:
                    now_dt = now_dt.replace(tzinfo=_UTC)
                if (now_dt - last_dt).total_seconds() >= 3 * 3600:
                    should_push = True
            except Exception:
                # Tarih parse edilemezse gün bazında kontrol
                if last_at[:10] != at_iso[:10]:
                    should_push = True
    if not should_push:
        return
    try:
        with SessionLocal() as db:
            db.add(
                AppStoreRankSnapshot(
                    product_id=product_id,
                    platform=platform,
                    rank=rank_int,
                    total=int(rec.get("total")) if rec.get("total") is not None else None,
                    category_name=(str(rec.get("category") or "").strip() or None),
                    chart=(str(rec.get("chart") or "").strip() or None),
                    collected_at=_parse_utc_iso(at_iso).replace(tzinfo=None),
                )
            )
            db.commit()
    except Exception as e:
        logger.debug("Rank snapshot DB kaydedilemedi (%s/%s): %s", product_id, platform, e)


def _at_report_tz_date(at: datetime) -> date:
    dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
    return dt.astimezone(report_calendar_tz()).date()


def _rank_history_series(product_id: str, platform: str, *, days: int = 7) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    start = inclusive_local_period_start_utc(n_calendar_days=days)
    if start is None:
        return []
    out: list[dict[str, Any]] = []
    for x in arr:
        at_s = str(x.get("at") or "")
        try:
            dt = datetime.fromisoformat(at_s.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_UTC)
        if dt < start:
            continue
        out.append(
            {
                "at": at_s,
                "rank": x.get("rank"),
                "total": x.get("total"),
                "category": x.get("category"),
            }
        )
    return out[-200:]


def _rank_history_changes(product_id: str, platform: str, *, limit: int = 50) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    out: list[dict[str, Any]] = []
    prev_rank: int | None = None
    for x in arr:
        try:
            cur = int(x.get("rank"))
        except Exception:
            continue
        if prev_rank is None or cur != prev_rank:
            out.append(
                {
                    "at": x.get("at"),
                    "rank": cur,
                    "total": x.get("total"),
                    "category": x.get("category"),
                }
            )
        prev_rank = cur
    return out[-limit:]


def _rank_history_daily(product_id: str, platform: str, *, days: int = 30) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    start_d = report_calendar_today() - timedelta(days=days - 1)
    by_day: dict[str, dict[str, Any]] = {}
    for x in arr:
        at_s = str(x.get("at") or "")
        try:
            dt = datetime.fromisoformat(at_s.replace("Z", "+00:00"))
        except Exception:
            continue
        if _at_report_tz_date(dt) < start_d:
            continue
        day_key = _at_report_tz_date(dt).isoformat()
        prev = by_day.get(day_key)
        if prev is None or str(prev.get("at") or "") < at_s:
            by_day[day_key] = {
                "at": at_s,
                "rank": x.get("rank"),
                "total": x.get("total"),
                "category": x.get("category"),
            }
    out = [by_day[k] for k in sorted(by_day.keys())]
    return out[-days:]


_load_forced_refresh_meta()
_load_rank_history()


def list_products() -> list[dict[str, str]]:
    return [{"id": k, "label": v["label"]} for k, v in APP_PRODUCTS.items()]


def _normalize_review_text(text: str) -> str:
    src = (text or "").strip()
    if not src:
        return ""
    markers = ("Ã", "Ä", "Å", "â", "Ð", "Þ", "�")

    def score(s: str) -> tuple[int, int]:
        bad = sum(s.count(m) for m in markers)
        return bad, len(s)

    candidates = [src]
    for enc in ("latin1", "cp1252"):
        try:
            repaired = src.encode(enc).decode("utf-8")
            candidates.append(repaired)
        except Exception:
            continue

    best = min(candidates, key=score)
    return " ".join(best.split())


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
        # App Store review JSON parçalarında sürüm sinyali geçebiliyor (userReviewVersion / softwareVersion).
        snippet_start = max(0, m.start() - 400)
        snippet_end = min(len(html), m.end() + 400)
        snippet = html[snippet_start:snippet_end]
        ver: str | None = None
        mv = re.search(r'"(?:userReviewVersion|softwareVersion|reviewVersion)"\s*:\s*"([^"]+)"', snippet)
        if mv:
            cand = str(mv.group(1)).strip()
            if cand:
                ver = cand
        reviews_out.append({"at": dt, "score": int(r_s), "text": _normalize_review_text(body), "version": ver})

    snap: dict[str, Any] = {}
    m_badge = re.search(
        r'"type":"rating"[^}]*"content":\{"rating":([0-9.]+),"ratingFormatted":"([0-9.]+)"\}[^}]*"heading":"([^"]+)"',
        html,
    )
    if m_badge:
        snap["score"] = float(m_badge.group(1))
        snap["score_formatted"] = m_badge.group(2)
        snap["ratings_caption"] = m_badge.group(3)
    # Total ratings count from dedicated count fields
    for cnt_pat in (
        r'"ratingCount"\s*:\s*([0-9]+)',
        r'"userRatingCount"\s*:\s*([0-9]+)',
    ):
        m_cnt = re.search(cnt_pat, html)
        if m_cnt:
            try:
                snap["ratings_count"] = int(m_cnt.group(1))
                break
            except Exception:
                pass
    m_icon = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html)
    if m_icon:
        snap["icon"] = m_icon.group(1)
    return reviews_out, snap


def _fetch_ios_ssr_ratings(
    app_id: str,
    ios_slug: str,
    *,
    country: str = "tr",
) -> dict[str, Any] | None:
    """App Store uygulama sayfasının SSR JSON'undan ülkeye özgü puan ve dağılımı çeker.

    Sıralama için kullandığımız SSR scrape yöntemiyle aynı prensip:
    HTML içindeki ``<script type="application/json">`` bloğunu parse eder.
    Dönen yapı:
      {
        "score": 4.8,
        "ratings_count": 1844,
        "star_histogram": {"1": 31, "2": 10, "3": 46, "4": 137, "5": 1620},
      }
    Tüm değerler belirtilen ülkeye (country) özgüdür; global toplama yapılmaz.
    """
    url = f"https://apps.apple.com/{country}/app/{ios_slug}/id{app_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        )
    }
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
        html = r.text
    except Exception as exc:
        logger.debug("iOS SSR ratings fetch hatası (%s, %s): %s", app_id, country, exc)
        return None

    # SSR JSON bloğunu parse et
    m_json = re.search(
        r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not m_json:
        logger.debug("iOS SSR ratings: JSON bloğu bulunamadı (%s)", app_id)
        return _fetch_ios_main_page_histogram_single(app_id, ios_slug, country)

    try:
        page_data = json.loads(m_json.group(1))
        ratings_item = (
            page_data["data"][0]["data"]["shelfMapping"]["productRatings"]["items"][0]
        )
        score = float(ratings_item["ratingAverage"])
        ratings_count = int(ratings_item["totalNumberOfRatings"])
        counts_raw: list[int] = ratings_item["ratingCounts"]  # [5★, 4★, 3★, 2★, 1★]
        if len(counts_raw) != 5 or sum(counts_raw) == 0:
            raise ValueError("ratingCounts geçersiz")
        star_histogram = {
            "5": counts_raw[0],
            "4": counts_raw[1],
            "3": counts_raw[2],
            "2": counts_raw[3],
            "1": counts_raw[4],
        }
        logger.info(
            "iOS SSR ratings (%s, %s): %.2f★ %d puan – 5★=%d 4★=%d 3★=%d 2★=%d 1★=%d",
            app_id, country.upper(), score, ratings_count,
            star_histogram["5"], star_histogram["4"], star_histogram["3"],
            star_histogram["2"], star_histogram["1"],
        )
        return {"score": score, "ratings_count": ratings_count, "star_histogram": star_histogram}
    except Exception as exc:
        logger.debug("iOS SSR ratings JSON parse hatası (%s): %s", app_id, exc)
        # Fallback: regex ile tek vitrin
        return _fetch_ios_main_page_histogram_single(app_id, ios_slug, country)


def _fetch_ios_main_page_histogram_single(
    app_id: str,
    ios_slug: str,
    country: str = "tr",
) -> dict[str, Any] | None:
    """SSR JSON başarısız olursa regex fallback — sadece tek ülke."""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    _pat = re.compile(
        r'"ratingCounts"\s*:\s*\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*\]'
    )
    url = f"https://apps.apple.com/{country}/app/{ios_slug}/id{app_id}"
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
        m = _pat.search(r.text)
        if not m:
            return None
        counts = [int(m.group(i)) for i in range(1, 6)]
        total = sum(counts)
        if total == 0:
            return None
        star_histogram = {"5": counts[0], "4": counts[1], "3": counts[2], "2": counts[3], "1": counts[4]}
        logger.info("iOS regex histogram (%s, %s): toplam=%d", app_id, country, total)
        return {"star_histogram": star_histogram}
    except Exception as exc:
        logger.debug("iOS regex histogram hatası (%s, %s): %s", app_id, country, exc)
        return None


def _fetch_ios_main_page_histogram(app_id: str, ios_slug: str) -> dict[str, int] | None:
    """Geriye dönük uyumluluk için bırakıldı. Artık `_fetch_ios_ssr_ratings` kullanılıyor."""
    result = _fetch_ios_ssr_ratings(app_id, ios_slug, country="tr")
    return result.get("star_histogram") if result else None


def _ios_review_key(at: datetime, text: str, score: int) -> str:
    payload = f"{at.isoformat()}\0{text}\0{score}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def _fetch_ios_rss_reviews(
    app_id: str,
    *,
    country: str = "tr",
    max_pages: int = 5,
) -> list[dict[str, Any]]:
    """iTunes RSS feed'inden en son reviewları tarih sıralı çeker.

    ``sortby=mostrecent`` ile sayfa sayfa giderek max_pages * 50 yorum döner.
    Dönen her yorum: {"at": datetime, "score": int, "text": str}.
    Zaman filtreli histogram hesaplamak için kullanılır.
    """
    headers = {"User-Agent": "iTunes/12.0 (Macintosh; OS X 10.15.7)"}
    reviews: list[dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        url = (
            f"https://itunes.apple.com/rss/customerreviews"
            f"/page={page}/id={app_id}/sortby=mostrecent/json"
        )
        try:
            with httpx.Client(timeout=12.0, follow_redirects=True, headers=headers) as client:
                r = client.get(url, params={"l": country, "cc": country})
                r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
        except Exception as exc:
            logger.debug("iTunes RSS reviews hata (page=%d, %s): %s", page, app_id, exc)
            break

        if not entries:
            break

        for e in entries:
            try:
                score = int(e.get("im:rating", {}).get("label", 0) or 0)
                if not (1 <= score <= 5):
                    continue
                date_str = (e.get("updated", {}).get("label") or "")[:10]
                at = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_UTC)
                text = e.get("content", {}).get("label") or e.get("title", {}).get("label") or ""
                reviews.append({"at": at, "score": score, "text": text.strip(), "source": "rss"})
            except Exception:
                continue

    logger.info("iTunes RSS reviews (%s, %s): %d yorum çekildi", app_id, country.upper(), len(reviews))
    return reviews


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
    # Apple rate limit'i aşmamak için eşzamanlı istek sayısı sınırlı tutulur.
    max_workers = min(4, n_sf)
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


def normalize_ios_app_id(raw: str | None) -> str:
    """App Store uygulama id'si: URL/kullanıcı girişi 'id465599322', 'ID 465 ...' → '465599322'.

    iTunes lookup `id=` parametresi yalnızca rakam kabul eder; önek verilirse sonuç 0 gelir.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"^id\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+", "", s)
    if re.fullmatch(r"\d+", s):
        return s
    m = re.search(r"\d{6,}", s)
    return m.group(0) if m else ""


def _fetch_ios_lookup_meta(app_id: str) -> dict[str, Any]:
    """iTunes lookup API'den çoklu ülke toplanmış all-time rating count al."""
    app_id = normalize_ios_app_id(app_id)
    if not app_id:
        return {}
    url = "https://itunes.apple.com/lookup"

    primary_genre_id: int | None = None
    primary_genre_name: str | None = None
    icon_candidate: str | None = None
    version_candidate: str | None = None
    cvrd_candidate: str | None = None

    def one(country: str) -> tuple[int, float | None, int | None, str | None, str | None, str | None, str | None]:
        try:
            with httpx.Client(timeout=10.0, follow_redirects=True) as client:
                r = client.get(url, params={"id": app_id, "country": country})
                r.raise_for_status()
                data = r.json()
            first = ((data or {}).get("results") or [])[0] or {}
        except Exception:
            return 0, None, None, None, None, None, None
        cnt_raw = first.get("userRatingCount")
        if cnt_raw is None:
            cnt_raw = first.get("userRatingCountForCurrentVersion")
        score_raw = first.get("averageUserRating")
        if score_raw is None:
            score_raw = first.get("averageUserRatingForCurrentVersion")
        try:
            cnt = int(cnt_raw or 0)
        except Exception:
            cnt = 0
        try:
            score = float(score_raw) if score_raw is not None else None
        except Exception:
            score = None
        gid = first.get("primaryGenreId")
        gname = first.get("primaryGenreName")
        try:
            gid_i = int(gid) if gid is not None else None
        except Exception:
            gid_i = None
        art = first.get("artworkUrl512") or first.get("artworkUrl100") or first.get("artworkUrl60")
        art_s = str(art).strip() if art else None
        ver_s = str(first.get("version")).strip() if first.get("version") else None
        cvrd_s = str(first.get("currentVersionReleaseDate")).strip() if first.get("currentVersionReleaseDate") else None
        return cnt, score, gid_i, (str(gname).strip() if gname else None), art_s, ver_s, cvrd_s

    total_count = 0
    weighted_sum = 0.0
    with ThreadPoolExecutor(max_workers=min(12, len(_IOS_STOREFRONTS))) as pool:
        futs = [pool.submit(one, c) for c in _IOS_STOREFRONTS]
        for fut in futs:
            cnt, score, gid_i, gname, art_s, ver_s, cvrd_s = fut.result()
            if cnt > 0:
                total_count += cnt
                if score is not None:
                    weighted_sum += cnt * score
            if primary_genre_id is None and gid_i is not None:
                primary_genre_id = gid_i
            if primary_genre_name is None and gname:
                primary_genre_name = gname
            if icon_candidate is None and art_s:
                icon_candidate = art_s
            if version_candidate is None and ver_s:
                version_candidate = ver_s
            if cvrd_candidate is None and cvrd_s:
                cvrd_candidate = cvrd_s

    out: dict[str, Any] = {}
    if total_count > 0:
        out["ratings_count"] = total_count
        if weighted_sum > 0:
            out["score"] = round(weighted_sum / total_count, 5)
    if primary_genre_id is not None:
        out["primary_genre_id"] = primary_genre_id
    if primary_genre_name:
        out["primary_genre_name"] = primary_genre_name
    if icon_candidate:
        out["icon"] = icon_candidate
    if version_candidate:
        out["version"] = version_candidate
    if cvrd_candidate:
        out["currentVersionReleaseDate"] = cvrd_candidate
    return out


_CHART_LABELS: dict[str, str] = {
    "top-free": "Ücretsiz",
    "top-paid": "Ücretli",
    "top-grossing": "En Çok Kazanan",
    # Legacy RSS chart names
    "topfreeapplications": "Ücretsiz",
    "topgrossingapplications": "En Çok Kazanan",
    "toppaidapplications": "Ücretli",
}

_IOS_CHARTS_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


def _fetch_ios_category_rank(
    app_id: str,
    *,
    country: str = "tr",
    genre_id: int | None = None,
) -> dict[str, Any] | None:
    """App Store charts sayfasının SSR JSON'undan gerçek sıralamayı çek.

    Apple App Store charts sayfası (https://apps.apple.com/{country}/iphone/charts/{genre_id})
    SSR HTML'inde iki veri kaynağı barındırır:
      - shelves[].items  : sayfada ilk görünen (sparseLimit=25) uygulamalar
      - nextPage.remainingContent : geri kalan uygulamalar (toplam limit=200)
    İkisi birleşince tüm sıralama elde edilir; RSS'in 100 uygulama sınırı yoktur.

    genre_id yoksa veya sayfadan sıra bulunamazsa RSS fallback çalışır.
    """
    if not app_id:
        return None

    app_id_str = str(app_id).strip()

    # ------- 1) App Store charts sayfası scrape (birincil yöntem) -------
    if genre_id:
        try:
            url = f"https://apps.apple.com/{country}/iphone/charts/{genre_id}"
            with httpx.Client(timeout=20.0, follow_redirects=True) as client:
                r = client.get(url, headers={"User-Agent": _IOS_CHARTS_UA})
                r.raise_for_status()
            html = r.text

            # SSR JSON bloğunu bul
            json_match = re.search(
                r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
                html, re.DOTALL
            )
            if json_match:
                page_data = json.loads(json_match.group(1))
                segments = (
                    (page_data.get("data") or [{}])[0]
                    .get("data", {})
                    .get("segments", [])
                )
                all_charts: dict[str, dict[str, Any]] = {}

                for segment in segments:
                    chart_key = segment.get("chart", "")  # "top-free", "top-paid", ...
                    # İlk görünen uygulamalar (shelves)
                    shelf_items: list[str] = []
                    for shelf in segment.get("shelves", []):
                        for item in shelf.get("items", []):
                            if isinstance(item, dict):
                                shelf_items.append(str(item.get("id", "")))
                    # Geri kalan uygulamalar
                    remaining_items: list[str] = [
                        str(item.get("id", ""))
                        for item in segment.get("nextPage", {}).get("remainingContent", [])
                        if isinstance(item, dict)
                    ]
                    all_app_ids = shelf_items + remaining_items

                    if app_id_str in all_app_ids:
                        rank = all_app_ids.index(app_id_str) + 1
                        total = len(all_app_ids)
                        all_charts[chart_key] = {
                            "rank": rank,
                            "total": total,
                            "chart": chart_key,
                            "chart_label": _CHART_LABELS.get(chart_key, chart_key),
                            "scope": "category",
                        }
                        logger.info(
                            "iOS charts scrape sırası (%s, %s, genre=%s): #%d/%d",
                            app_id, chart_key, genre_id, rank, total,
                        )

                if all_charts:
                    # Birincil: top-free; yoksa ilk bulunan
                    primary_key = "top-free" if "top-free" in all_charts else next(iter(all_charts))
                    primary = dict(all_charts[primary_key])
                    if len(all_charts) > 1:
                        primary["all_charts"] = all_charts
                    return primary

                logger.debug("iOS charts scrape: uygulama bulunamadı (%s, genre=%s)", app_id, genre_id)
        except Exception as exc:
            logger.warning("iOS charts scrape hatası (%s, genre=%s): %s", app_id, genre_id, exc)

    # ------- 2) RSS fallback (top 100, genre filtreli) -------
    rss_chart_types = ("topfreeapplications", "topgrossingapplications", "toppaidapplications")

    def _search_rss(chart: str, genre: int | None) -> tuple[int | None, int]:
        try:
            if genre:
                rss_url = f"https://itunes.apple.com/{country}/rss/{chart}/genre={genre}/limit=200/json"
            else:
                rss_url = f"https://itunes.apple.com/{country}/rss/{chart}/limit=200/json"
            with httpx.Client(timeout=14.0, follow_redirects=True) as client:
                rss_r = client.get(rss_url)
                rss_r.raise_for_status()
            entries = ((rss_r.json() or {}).get("feed") or {}).get("entry") or []
            for idx, e in enumerate(entries, start=1):
                eid = str((((e.get("id") or {}).get("attributes") or {}).get("im:id") or "")).strip()
                if eid == app_id_str:
                    return idx, len(entries)
            return None, len(entries)
        except Exception:
            return None, 0

    rss_results: dict[str, dict[str, Any]] = {}
    if genre_id:
        for chart in rss_chart_types:
            rank, total = _search_rss(chart, genre_id)
            if rank is not None:
                logger.info("iOS RSS sırası (%s, %s, genre=%s): #%d/%d", app_id, chart, genre_id, rank, total)
                rss_results[chart] = {
                    "rank": rank, "total": total, "chart": chart,
                    "chart_label": _CHART_LABELS.get(chart, chart), "scope": "category",
                }

    if rss_results:
        primary_chart = "topfreeapplications" if "topfreeapplications" in rss_results else next(iter(rss_results))
        primary = dict(rss_results[primary_chart])
        if len(rss_results) > 1:
            primary["all_charts"] = rss_results
        return primary

    # ------- 3) Genel RSS (genre filtresi yok) -------
    for chart in rss_chart_types:
        rank, total = _search_rss(chart, None)
        if rank is not None:
            logger.info("iOS genel RSS sırası (%s, %s): #%d/%d", app_id, chart, rank, total)
            return {
                "rank": rank, "total": total, "chart": chart,
                "chart_label": _CHART_LABELS.get(chart, chart), "scope": "overall",
            }

    logger.debug("iOS sıra bulunamadı (%s, genre=%s)", app_id, genre_id)
    return None


def _fetch_android_category_rank(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
    category_id: str | None = None,
) -> dict[str, Any] | None:
    """Play Store Finance kategorisindeki gerçek sıralamayı döner.

    Önce batchexecute API yanıtını Playwright ile yakalar (en doğru).
    Başarısız olursa statik HTTP fallback dener.
    """
    if not package:
        return None

    # ── 1) Playwright + batchexecute (birincil, en doğru) ────────────────────
    result = _fetch_android_rank_playwright(package, country=country, lang=lang)
    if result is not None:
        return result

    # ── 2) HTTP fallback: detay sayfasından metin tarama ─────────────────────
    return _fetch_android_rank_http_fallback(package, country=country, lang=lang, category_id=category_id)


def _extract_android_packages(text: str) -> list[str]:
    """batchexecute yanıt metninden Android paket adlarını sıralı döner (tekrarsız)."""
    # Format: [\"com.package.name\" veya ["com.package.name"
    pkgs_raw = re.findall(r'\[\\"([a-zA-Z][a-zA-Z0-9._]{4,})\\"', text)
    if not pkgs_raw:
        pkgs_raw = re.findall(r'\["([a-zA-Z][a-zA-Z0-9._]{4,})"', text)
    _pkg_re = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$')
    seen: set[str] = set()
    result: list[str] = []
    for p in pkgs_raw:
        if p not in seen and _pkg_re.match(p):
            seen.add(p)
            result.append(p)
    return result


def _fetch_android_rank_playwright(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
) -> dict[str, Any] | None:
    """Play Store top-free Finance chart'ını scroll ederek gerçek sıralamayı çeker.

    Tam chart sayfasına gider, batchexecute yanıtlarını yakalar ve sayfayı aşağı
    kaydırarak lazy-load ile ek sayfaları tetikler. ~200 uygulamayı tarayabilir.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.debug("playwright kurulu değil; android rank fallback kullanılıyor")
        return None

    # Tam top-free chart sayfası (kategori landing değil, asıl liste sayfası)
    chart_url = (
        f"https://play.google.com/store/apps/category/FINANCE"
        f"/collection/top_free?hl={lang}&gl={country}"
    )

    # Her batchexecute yanıtını sırayla sakla; scroll sonrası yenileri eklenir
    captured_texts: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                locale=f"{lang}-{country.upper()}",
                user_agent=(
                    "Mozilla/5.0 (Linux; Android 12; Pixel 6) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Mobile Safari/537.36"
                ),
            )
            page = ctx.new_page()

            def _on_response(response) -> None:  # noqa: ANN001
                if "batchexecute" in response.url:
                    try:
                        captured_texts.append(response.text())
                    except Exception:
                        pass

            page.on("response", _on_response)

            try:
                page.goto(chart_url, wait_until="domcontentloaded", timeout=35_000)
                page.wait_for_timeout(4_000)
            except PWTimeout:
                pass

            # Scroll ile lazy-load: her adımda ~50 uygulama daha yüklenebilir.
            # 12 scroll × ~50 = ~600 uygulama üst sınırı (Finance'de genelde ~200)
            seen_global: set[str] = set()
            ordered_global: list[str] = []

            for scroll_i in range(12):
                # Mevcut yanıtları işle
                for txt in captured_texts:
                    for p in _extract_android_packages(txt):
                        if p not in seen_global:
                            seen_global.add(p)
                            ordered_global.append(p)
                captured_texts.clear()

                logger.debug(
                    "android rank scroll %d: %d paket tarandı",
                    scroll_i,
                    len(ordered_global),
                )

                # Hedef paket bulunduysa dur
                if package in seen_global:
                    break

                # Yeterince tarandıysa dur (Finance listesi genellikle ~200)
                if len(ordered_global) >= 250:
                    break

                # Sayfayı aşağı kaydır, yeni batchexecute tetikle
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(2_500)
                except Exception:
                    break

            # Son kalan yanıtları işle
            for txt in captured_texts:
                for p in _extract_android_packages(txt):
                    if p not in seen_global:
                        seen_global.add(p)
                        ordered_global.append(p)

            browser.close()
    except Exception as exc:
        logger.warning("Playwright android rank hatası: %s", exc)
        return None

    if not ordered_global:
        logger.debug("batchexecute yanıtından paket listesi çıkarılamadı")
        return None

    logger.info("batchexecute toplam: %d paket tarandı", len(ordered_global))

    if package in ordered_global:
        rank = ordered_global.index(package) + 1
        logger.info("batchexecute: %s #%d sırada (Finance)", package, rank)
        return {
            "rank": rank,
            "total": len(ordered_global),
            "chart": "top_free",
            "chart_label": "Ücretsiz",
            "category_name": "Finance",
            "estimated": False,
        }

    # Listede bulunamadı → alt sınır tahmini
    logger.info(
        "batchexecute: %s %d uygulama tarandı; listede bulunamadı (>%d tahmini)",
        package,
        len(ordered_global),
        len(ordered_global),
    )
    return {
        "rank": len(ordered_global) + 1,
        "total": len(ordered_global),
        "chart": "top_free",
        "chart_label": "Ücretsiz",
        "category_name": "Finance",
        "estimated": True,
    }


def _fetch_android_rank_http_fallback(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
    category_id: str | None = None,
) -> dict[str, Any] | None:
    """HTTP ile Play Store detay sayfası ve kategori chart tarama (fallback)."""
    url = "https://play.google.com/store/apps/details"
    params = {"id": package, "hl": lang, "gl": country}
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    try:
        with httpx.Client(timeout=12.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
        html = r.text
    except Exception:
        return None

    category_name = None
    m_cat = re.search(r'"applicationCategory":"([^"]+)"', html)
    if m_cat:
        category_name = m_cat.group(1).strip()

    patterns = [
        r"#\s*([0-9]{1,4})\s*(?:in|içinde)?\s*(Finance|Finans|Business|İş)",
        r"Top charts[^#]{0,120}#\s*([0-9]{1,4})",
        r"ranking[^#]{0,120}#\s*([0-9]{1,4})",
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            rank = int(m.group(1))
            if m.lastindex and m.lastindex >= 2 and m.group(2):
                category_name = category_name or m.group(2)
            return {
                "rank": rank,
                "total": None,
                "chart": "details_page",
                "chart_label": "Ücretsiz",
                "category_name": category_name,
            }

    return {"rank": None, "total": None, "chart": "details_page", "category_name": category_name} if category_name else None


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
        norm.append(
            {
                "at": dt,
                "score": int(rv.get("score") or 0),
                "text": _normalize_review_text(rv.get("content") or ""),
                "version": (str(rv.get("reviewCreatedVersion") or "").strip() or None),
            }
        )

    return meta, norm, err


def _categorize(text: str) -> str:
    low = (text or "").lower()
    for cat_id, _label, keys in _REVIEW_CATEGORIES:
        if any(k in low for k in keys):
            return cat_id
    return "diger"


def _cutoff(days: int) -> datetime:
    start = inclusive_local_period_start_utc(n_calendar_days=days)
    if start is None:
        return datetime.now(tz=_UTC)
    return start


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
        f"Bu aralıkta (TSİ takviminde son {days} gün) örnek yorum yok; "
        f"grafikler en güncel örnek tarihine göre ({_at_report_tz_date(anchor).isoformat()} yerel gün) kaydırıldı."
    )
    return anchored, anchor, note


def _daily_rating_series(
    rows: list[dict[str, Any]],
    days: int,
    anchor_end: datetime | None = None,
) -> list[dict[str, Any]]:
    """Rapor takvimi (TSİ) günü bazında, dönemdeki yorumların günlük ortalama yıldızı (mağaza genel ortalama geçmişi değil)."""
    by_day: dict[str, list[int]] = {}
    if anchor_end is None:
        end_d = report_calendar_today()
    else:
        end_d = _at_report_tz_date(anchor_end)
    start_d = end_d - timedelta(days=days - 1)
    for r in rows:
        d = _at_report_tz_date(r["at"])
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


def _category_review_map(rows: list[dict[str, Any]], *, per_category_limit: int = 40) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    for r in ordered:
        cid = _categorize(r.get("text") or "")
        bucket = out.setdefault(cid, [])
        if len(bucket) >= per_category_limit:
            continue
        bucket.append(
            {
                "at": r["at"],
                "score": int(r.get("score") or 0),
                "text": _normalize_review_text(r.get("text") or ""),
            }
        )
    return out


def _star_review_map(rows: list[dict[str, Any]], *, per_star_limit: int = 60) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {str(i): [] for i in range(1, 6)}
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    for r in ordered:
        s = int(r.get("score") or 0)
        if s < 1 or s > 5:
            continue
        key = str(s)
        bucket = out[key]
        if len(bucket) >= per_star_limit:
            continue
        bucket.append(
            {
                "at": r["at"],
                "score": s,
                "text": _normalize_review_text(r.get("text") or ""),
            }
        )
    return out


def _dedupe_reviews(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    seen_text: set[str] = set()
    seen_fallback: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in ordered:
        txt = _normalize_review_text(r.get("text") or "")
        canonical = re.sub(r"\s+", " ", txt).strip().lower()
        if canonical:
            if canonical in seen_text:
                continue
            seen_text.add(canonical)
        else:
            fb = f'{r["at"].isoformat()}\0{int(r.get("score") or 0)}'
            if fb in seen_fallback:
                continue
            seen_fallback.add(fb)
        out.append(
            {
                "at": r["at"],
                "score": int(r.get("score") or 0),
                "text": txt,
            }
        )
    return out


def _latest_reviews(rows: list[dict[str, Any]], limit: int = 100) -> list[dict[str, Any]]:
    return _dedupe_reviews(rows)[:limit]


def _android_histogram_overall(meta: dict[str, Any]) -> dict[str, int] | None:
    h = meta.get("histogram")
    if not h or len(h) != 5:
        return None
    return {str(i + 1): int(h[i]) for i in range(5)}




def invalidate_raw_cache(product_id: str | None = None) -> None:
    with _CACHE_LOCK:
        if product_id:
            _RAW_CACHE.pop(product_id, None)
        else:
            _RAW_CACHE.clear()


def get_cached_raw_product_data(product_id: str) -> dict[str, Any] | None:
    """Ağ çağrısı yapmadan yalnızca sıcak cache'deki raw payload'ı döndür."""
    if product_id not in APP_PRODUCTS:
        return None
    now = time.time()
    with _CACHE_LOCK:
        hit = _RAW_CACHE.get(product_id)
        if not hit:
            return None
        ts, payload = hit
        if now - ts >= _CACHE_TTL_SEC:
            return None
        return payload


def get_last_forced_refresh_at(product_id: str) -> str | None:
    v = _FORCED_REFRESH_AT.get(product_id)
    return str(v) if v else None


def get_raw_product_data(product_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
    if product_id not in APP_PRODUCTS:
        return {"error": "unknown_product"}
    spec = APP_PRODUCTS[product_id]
    now = time.time()
    cache_key = product_id
    with _CACHE_LOCK:
        hit = _RAW_CACHE.get(cache_key)
        if (not force_refresh) and hit and now - hit[0] < _CACHE_TTL_SEC:
            return hit[1]

    # Ağ çağrılarını paralelleştir: soğuk açılış TTFB'sini düşür.
    with ThreadPoolExecutor(max_workers=5) as pool:
        f_play = pool.submit(_fetch_google_bundle, spec["android_package"])
        f_ios_reviews = pool.submit(_fetch_ios_reviews_multistore, spec["ios_app_id"], spec["ios_slug"])
        f_ios_lookup = pool.submit(_fetch_ios_lookup_meta, spec["ios_app_id"])
        f_ios_ratings = pool.submit(
            _fetch_ios_ssr_ratings, spec["ios_app_id"], spec["ios_slug"], country="tr"
        )
        # Zaman filtreli histogram için RSS'ten en son yorumlar (max 250 adet)
        f_ios_rss = pool.submit(_fetch_ios_rss_reviews, spec["ios_app_id"], country="tr", max_pages=5)
        meta, g_rows, g_err = f_play.result()
        i_rows, i_snap, i_err, i_sf_ok, i_sf_n = f_ios_reviews.result()
        i_lookup = f_ios_lookup.result()
        i_ssr_ratings = f_ios_ratings.result()  # {"score", "ratings_count", "star_histogram"}
        i_rss_rows = f_ios_rss.result()  # en son TR yorumları, tarih sıralı

    # RSS reviewlarını mevcut listeyie ekle (dedup: aynı tarih+puan+metin önlenir)
    if i_rss_rows:
        existing_keys = {_ios_review_key(rv["at"], rv["text"], rv["score"]) for rv in i_rows}
        for rv in i_rss_rows:
            k = _ios_review_key(rv["at"], rv["text"], rv["score"])
            if k not in existing_keys:
                existing_keys.add(k)
                i_rows.append(rv)
    if i_lookup:
        i_snap = {**(i_snap or {}), **{k: v for k, v in i_lookup.items() if v is not None}}
    # SSR JSON ile çekilen ülkeye özgü gerçek dağılım — öncelikli kaynak
    if i_ssr_ratings:
        i_snap = {**(i_snap or {}), "star_histogram": i_ssr_ratings.get("star_histogram")}
        # SSR'dan gelen score/count, iTunes lookup'tan gelmiyorsa kullan
        if i_ssr_ratings.get("score") is not None and not (i_snap or {}).get("score"):
            i_snap = {**(i_snap or {}), "score": i_ssr_ratings["score"]}
        if i_ssr_ratings.get("ratings_count") is not None and not (i_snap or {}).get("ratings_count"):
            i_snap = {**(i_snap or {}), "ratings_count": i_ssr_ratings["ratings_count"]}
    i_rank = _fetch_ios_category_rank(
        spec["ios_app_id"],
        country="tr",
        genre_id=(i_snap or {}).get("primary_genre_id"),
    )
    if i_rank:
        i_snap = {**(i_snap or {}), **{"category_rank": i_rank}}
    a_rank = _fetch_android_category_rank(
        spec["android_package"],
        country="tr",
        lang="tr",
        category_id=str(meta.get("genreId") or "") or None,
    )
    if a_rank:
        meta = {**(meta or {}), **{"category_rank": a_rank}}

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
                "genre": meta.get("genre"),
                "category_rank": meta.get("category_rank"),
                "play_version": meta.get("version"),
                "play_last_updated_at": _play_updated_iso(meta.get("updated")),
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
    _append_rank_snapshot(
        product_id,
        "android",
        {
            **(((payload.get("android", {}).get("meta") or {}).get("category_rank") or {})),
            "category_name": (payload.get("android", {}).get("meta") or {}).get("genre"),
        },
        at_iso=payload["fetched_at"],
    )
    _append_rank_snapshot(
        product_id,
        "ios",
        {
            **(((payload.get("ios", {}).get("meta") or {}).get("category_rank") or {})),
            "category_name": (payload.get("ios", {}).get("meta") or {}).get("primary_genre_name"),
        },
        at_iso=payload["fetched_at"],
    )
    _save_rank_history()

    if force_refresh:
        _FORCED_REFRESH_AT[product_id] = payload["fetched_at"]
        _save_forced_refresh_meta()

    with _CACHE_LOCK:
        _RAW_CACHE[cache_key] = (now, payload)
    return payload


def build_intel_payload(product_id: str, period_days: int, *, force_refresh: bool = False) -> dict[str, Any]:
    valid_periods = (0, 7, 30, 90, 180, 365, 730)
    if period_days not in valid_periods:
        period_days = 7
    raw = get_raw_product_data(product_id, force_refresh=force_refresh)
    if raw.get("error"):
        return raw

    intel: dict[str, Any] = {
        "product_id": product_id,
        "label": raw["label"],
        "product_key": product_id,
        "app_icon": raw["android"]["meta"].get("icon") or (raw["ios"]["meta"] or {}).get("icon"),
        "urls": raw["urls"],
        "fetched_at": raw["fetched_at"],
        "display_fetched_at": get_last_forced_refresh_at(product_id) or raw["fetched_at"],
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
        if p == 0:
            fa = list(raw["android"]["reviews"])
            fi = list(raw["ios"]["reviews"])
            fa_anchor = None
            fi_anchor = None
            fa_note = "Tüm zaman görünümünde detaylar çekilen örnek yorum havuzundan hesaplanır."
            fi_note = "Tüm zaman görünümünde detaylar çekilen örnek yorum havuzundan hesaplanır."
        else:
            fa, fa_anchor, fa_note = _filter_by_period_or_anchor(raw["android"]["reviews"], p)
            fi, fi_anchor, fi_note = _filter_by_period_or_anchor(raw["ios"]["reviews"], p)
        fa = _dedupe_reviews(fa)
        fi = _dedupe_reviews(fi)
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
                "store_category_rank": raw["android"]["meta"].get("category_rank"),
                "store_category_name": raw["android"]["meta"].get("genre")
                or ((raw["android"]["meta"].get("category_rank") or {}).get("category_name")),
                "store_rank_history_7d": _rank_history_series(product_id, "android", days=7),
                "store_rank_history_30d": _rank_history_series(product_id, "android", days=30),
                "store_rank_daily_30d": _rank_history_daily(product_id, "android", days=30),
                "store_rank_changes_50": _rank_history_changes(product_id, "android", limit=50),
                "satisfaction": _satisfaction_split(fa),
                "categories": _category_counts(fa),
                "category_reviews": _category_review_map(fa),
                "star_reviews": _star_review_map(fa),
                "latest_reviews": _latest_reviews(raw["android"]["reviews"], 100),
            },
            "ios": {
                "review_count_period": len(fi),
                "period_note_tr": fi_note,
                "rating_series": _daily_rating_series(fi, p, fi_anchor),
                "star_distribution_period": _histogram_counts(fi),
                "star_distribution_overall": (raw["ios"]["meta"] or {}).get("star_histogram"),
                "store_score": (raw["ios"]["meta"] or {}).get("score"),
                "store_ratings_count": (raw["ios"]["meta"] or {}).get("ratings_count"),
                "store_ratings_caption": (raw["ios"]["meta"] or {}).get("ratings_caption"),
                "store_category_rank": (raw["ios"]["meta"] or {}).get("category_rank"),
                "store_category_name": (raw["ios"]["meta"] or {}).get("primary_genre_name"),
                "store_rank_history_7d": _rank_history_series(product_id, "ios", days=7),
                "store_rank_history_30d": _rank_history_series(product_id, "ios", days=30),
                "store_rank_daily_30d": _rank_history_daily(product_id, "ios", days=30),
                "store_rank_changes_50": _rank_history_changes(product_id, "ios", limit=50),
                "satisfaction": _satisfaction_split(fi),
                "categories": _category_counts(fi),
                "category_reviews": _category_review_map(fi),
                "star_reviews": _star_review_map(fi),
                "note_tr": raw["ios"].get("note_tr"),
                "latest_reviews": _latest_reviews(raw["ios"]["reviews"], 100),
            },
        }

    intel["active_window"] = intel["windows"][str(period_days)]
    return intel


def refresh_category_ranks() -> dict[str, Any]:
    """Tüm ürünler için sadece kategori sırasını çekip DB'ye kaydeder.

    Tam yorum yenileme yapmaz — sadece iOS ve Android chart sıralarını
    günceller. 3 saatlik cron job tarafından çağrılır.
    """
    now_iso = datetime.now(tz=_UTC).isoformat()
    results: dict[str, Any] = {}

    for product_id, spec in APP_PRODUCTS.items():
        try:
            # iOS: genre_id'yi lookup'tan al (cache varsa oradan, yoksa API'den)
            ios_genre_id: int | None = None
            with _CACHE_LOCK:
                for cache_val in _RAW_CACHE.values():
                    if isinstance(cache_val, tuple) and len(cache_val) == 2:
                        cached_payload = cache_val[1]
                        if (cached_payload or {}).get("product_id") == product_id:
                            ios_genre_id = (
                                (cached_payload.get("ios") or {})
                                .get("meta") or {}
                            ).get("primary_genre_id")
                            break

            if ios_genre_id is None:
                # Cache boşsa lookup'tan çek
                lookup = _fetch_ios_lookup_meta(spec["ios_app_id"])
                ios_genre_id = lookup.get("primary_genre_id")

            i_rank = _fetch_ios_category_rank(
                spec["ios_app_id"], country="tr", genre_id=ios_genre_id
            )
            if i_rank:
                _append_rank_snapshot(
                    product_id, "ios",
                    {**i_rank, "category_name": i_rank.get("chart_label") or "Finans"},
                    at_iso=now_iso,
                )
            results[product_id] = {"ios": i_rank}
            logger.info("Rank refresh (%s) iOS: %s", product_id, i_rank)

            # Android
            a_rank = _fetch_android_category_rank(spec["android_package"], country="tr", lang="tr")
            if a_rank:
                _append_rank_snapshot(product_id, "android", a_rank, at_iso=now_iso)
            results[product_id]["android"] = a_rank
            logger.info("Rank refresh (%s) Android: %s", product_id, a_rank)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Rank refresh hatası (%s): %s", product_id, exc)
            results[product_id] = {"error": str(exc)}

    _save_rank_history()
    return results


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
