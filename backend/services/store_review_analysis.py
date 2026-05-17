"""Mağaza yorum çekimi + heuristic analiz servisi (store panel için)."""
from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ─── Türkçe ve İngilizce genel stopword'ler ───────────────────────────────────
_STOPWORDS: set[str] = {
    # Türkçe
    "bir", "bu", "ve", "ile", "de", "da", "ki", "ne", "çok", "daha",
    "için", "ama", "hem", "ya", "her", "en", "ben", "sen", "biz",
    "mi", "mu", "mı", "mü", "gibi", "kadar", "şimdi", "artık", "hep",
    "hiç", "bile", "sadece", "var", "yok", "oldu", "oluyor", "olur",
    "değil", "ama", "işte", "yani", "zaten", "çünkü", "nasıl", "neden",
    "olarak", "şey", "şu", "hangi", "bazı", "tüm", "bütün", "hiçbir",
    "benim", "onun", "bana", "sana", "bunu", "onu", "bize", "bana",
    "beni", "seni", "onu", "bizim", "sizin", "onların", "bunun",
    "şunun", "orada", "burada", "şurada", "orada", "buraya", "şuraya",
    "oraya", "iken", "olan", "olan", "olmuş", "yapıyor", "yapılan",
    "uygulama", "uygulamamız", "uygulama", "app",
    # İngilizce
    "the", "and", "for", "that", "this", "with", "not", "but", "from",
    "are", "was", "has", "have", "can", "will", "just", "use", "all",
    "app", "its", "your", "you", "very", "get", "got", "like", "when",
    "they", "too", "also", "more", "then", "than", "about", "out",
    "into", "been", "would", "what", "now",
}

_UTC = timezone.utc
_MAX_REVIEWS = 500  # tek bir analiz başına üst sınır


# ─── Google Play ──────────────────────────────────────────────────────────────
def _fetch_play_reviews(
    package: str,
    *,
    days: int,
    lang: str = "tr",
    country: str = "tr",
    limit: int = _MAX_REVIEWS,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """google-play-scraper ile yorum çeker; (meta, reviews) döner."""
    try:
        from google_play_scraper import Sort
        from google_play_scraper import app as gp_app
        from google_play_scraper import reviews as gp_reviews
    except ImportError:
        logger.warning("google-play-scraper kurulu değil")
        return {}, []

    cutoff = datetime.now(_UTC) - timedelta(days=days)
    meta: dict[str, Any] = {}
    try:
        meta = gp_app(package, lang=lang, country=country)
    except Exception as exc:
        logger.warning("Play meta alınamadı (%s): %s", package, exc)

    collected: list[dict[str, Any]] = []
    token = None
    stop_early = False
    try:
        while len(collected) < limit and not stop_early:
            batch_n = min(200, limit - len(collected))
            chunk, token = gp_reviews(
                package,
                lang=lang,
                country=country,
                sort=Sort.NEWEST,
                count=batch_n,
                continuation_token=token,
            )
            for rv in chunk:
                at = rv.get("at")
                if not isinstance(at, datetime):
                    continue
                dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
                if dt < cutoff:
                    stop_early = True
                    break
                collected.append({
                    "at": dt.isoformat(),
                    "score": int(rv.get("score") or 0),
                    "text": (rv.get("content") or "").strip(),
                    "author": (rv.get("userName") or "").strip(),
                    "version": str(rv.get("reviewCreatedVersion") or "") or None,
                })
            if not token:
                break
    except Exception as exc:
        logger.warning("Play yorumları alınamadı (%s): %s", package, exc)

    return meta, collected


# ─── App Store (iTunes RSS) ────────────────────────────────────────────────────
def _fetch_appstore_reviews(
    track_id: str,
    *,
    days: int,
    country: str = "tr",
    limit: int = _MAX_REVIEWS,
) -> list[dict[str, Any]]:
    """iTunes RSS ile App Store yorumlarını çeker."""
    cutoff = datetime.now(_UTC) - timedelta(days=days)
    collected: list[dict[str, Any]] = []
    headers = {"User-Agent": "iTunes/12.0 (Macintosh; OS X 10.15.7)"}

    for page in range(1, 11):
        if len(collected) >= limit:
            break
        url = (
            f"https://itunes.apple.com/rss/customerreviews"
            f"/page={page}/id={track_id}/sortby=mostrecent/json"
        )
        try:
            with httpx.Client(timeout=15.0, follow_redirects=True, headers=headers) as c:
                r = c.get(url, params={"l": country, "cc": country})
                r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
        except Exception as exc:
            logger.debug("iTunes RSS hata (page=%d, %s): %s", page, track_id, exc)
            break

        if not entries:
            break

        stop_early = False
        for e in entries:
            try:
                score = int(e.get("im:rating", {}).get("label", 0) or 0)
                if not (1 <= score <= 5):
                    continue
                date_str = (e.get("updated", {}).get("label") or "")[:10]
                dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_UTC)
                if dt < cutoff:
                    stop_early = True
                    break
                title = (e.get("title", {}).get("label") or "").strip()
                body = (e.get("content", {}).get("label") or "").strip()
                text = (title + " " + body).strip() if title else body
                author = e.get("author", {}).get("name", {}).get("label") or ""
                version = e.get("im:version", {}).get("label") or None
                collected.append({
                    "at": dt.isoformat(),
                    "score": score,
                    "text": text,
                    "author": author.strip(),
                    "version": str(version) if version else None,
                })
            except Exception:
                continue
        if stop_early:
            break

    logger.info("App Store RSS (%s, %s): %d yorum çekildi", track_id, country.upper(), len(collected))
    return collected


# ─── Heuristic Analiz ─────────────────────────────────────────────────────────
def run_heuristic(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    """Yorum listesi üzerinde heuristic analiz çalıştırır."""
    if not reviews:
        return {"total_reviews": 0, "error": "Belirlenen dönemde yorum bulunamadı."}

    n = len(reviews)

    # Puan dağılımı
    dist: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total_score = 0
    for rv in reviews:
        s = int(rv.get("score") or 0)
        if 1 <= s <= 5:
            dist[s] += 1
            total_score += s
    avg = round(total_score / n, 2) if n else 0.0

    # Sentiment (1-2 = negatif, 3 = nötr, 4-5 = pozitif)
    pos = sum(1 for rv in reviews if int(rv.get("score") or 0) >= 4)
    neu = sum(1 for rv in reviews if int(rv.get("score") or 0) == 3)
    neg = sum(1 for rv in reviews if int(rv.get("score") or 0) <= 2)

    # Anahtar kelimeler
    word_re = re.compile(r"[a-zA-ZçÇğĞıİöÖşŞüÜ]{3,}")
    word_counts: Counter[str] = Counter()
    for rv in reviews:
        for w in word_re.findall((rv.get("text") or "").lower()):
            if w not in _STOPWORDS:
                word_counts[w] += 1
    top_keywords = [{"word": w, "count": c} for w, c in word_counts.most_common(20)]

    # En iyi / en kötü örnekler (metin olan yorumlar)
    with_text = [rv for rv in reviews if len(rv.get("text") or "") > 20]
    best_samples = sorted(
        [rv for rv in with_text if int(rv.get("score") or 0) >= 4],
        key=lambda r: r.get("at") or "",
        reverse=True,
    )[:3]
    worst_samples = sorted(
        [rv for rv in with_text if int(rv.get("score") or 0) <= 2],
        key=lambda r: r.get("at") or "",
        reverse=True,
    )[:3]

    return {
        "total_reviews": n,
        "average_rating": avg,
        "rating_distribution": dist,
        "sentiment": {
            "positive": pos,
            "neutral": neu,
            "negative": neg,
            "positive_pct": round(pos / n * 100, 1) if n else 0.0,
            "neutral_pct": round(neu / n * 100, 1) if n else 0.0,
            "negative_pct": round(neg / n * 100, 1) if n else 0.0,
        },
        "top_keywords": top_keywords,
        "best_samples": best_samples,
        "worst_samples": worst_samples,
    }


# ─── Ana fonksiyon ────────────────────────────────────────────────────────────
def analyze_store_app(
    app_id: str,
    platform: str,
    *,
    days: int = 30,
    lang: str = "tr",
    country: str = "tr",
    limit: int = 500,
) -> dict[str, Any]:
    """
    Tek bir mağaza uygulamasının yorumlarını çekip heuristic analiz yapar.
    Döner: { app_meta, reviews_fetched, analysis }
    """
    limit = min(limit, _MAX_REVIEWS)
    app_meta: dict[str, Any] = {}
    reviews: list[dict[str, Any]] = []

    if platform == "google_play":
        app_meta, reviews = _fetch_play_reviews(
            app_id, days=days, lang=lang, country=country, limit=limit
        )
    elif platform == "app_store":
        reviews = _fetch_appstore_reviews(
            app_id, days=days, country=country, limit=limit
        )
    else:
        raise ValueError(f"Geçersiz platform: {platform}")

    analysis = run_heuristic(reviews)
    return {
        "app_meta": app_meta,
        "reviews_fetched": len(reviews),
        "analysis": analysis,
    }
