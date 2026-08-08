"""Google Play Store genel yorumları (son N gün) — Play Console DOM'a bağımlı değil."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

_UTC = timezone.utc
_TR_MONTHS = (
    "Oca",
    "Şub",
    "Mar",
    "Nis",
    "May",
    "Haz",
    "Tem",
    "Ağu",
    "Eyl",
    "Eki",
    "Kas",
    "Ara",
)

# TR ağırlıklı + yayılım ülkeleri
_DEFAULT_LOCALES: tuple[tuple[str, str], ...] = (
    ("tr", "tr"),
    ("en", "tr"),
    ("de", "de"),
    ("nl", "nl"),
    ("en", "us"),
    ("az", "az"),
    ("ar", "ae"),
)


def _fmt_tr_dt(dt: datetime) -> str:
    local = dt.astimezone(_UTC)
    return (
        f"{local.day} {_TR_MONTHS[local.month - 1]} {local.year}, "
        f"{local.hour:02d}:{local.minute:02d}"
    )


def fetch_play_store_reviews(
    package_name: str = "com.Doviz",
    *,
    days: int = 365,
    locales: tuple[tuple[str, str], ...] | None = None,
    max_per_locale: int = 5000,
) -> list[dict[str, Any]]:
    """Play Store'dan son `days` günün tüm metinli yorumlarını çeker."""
    try:
        from google_play_scraper import Sort
        from google_play_scraper import reviews as gp_reviews
    except ImportError as exc:
        raise RuntimeError("google-play-scraper kurulu değil") from exc

    days = max(28, min(400, int(days or 365)))
    cutoff = datetime.now(_UTC) - timedelta(days=days)
    by_id: dict[str, dict[str, Any]] = {}

    for lang, country in locales or _DEFAULT_LOCALES:
        token = None
        fetched = 0
        try:
            while fetched < max_per_locale:
                batch_n = min(200, max_per_locale - fetched)
                chunk, token = gp_reviews(
                    package_name,
                    lang=lang,
                    country=country,
                    sort=Sort.NEWEST,
                    count=batch_n,
                    continuation_token=token,
                )
                if not chunk:
                    break
                fetched += len(chunk)
                stop = False
                for rv in chunk:
                    at = rv.get("at")
                    if not isinstance(at, datetime):
                        continue
                    dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
                    if dt < cutoff:
                        stop = True
                        break
                    rid = str(rv.get("reviewId") or "").strip()
                    if not rid:
                        rid = f"{rv.get('userName')}|{dt.isoformat()}"
                    body = (rv.get("content") or "").strip()
                    score = int(rv.get("score") or 0)
                    # Yorum paneli: metinli veya en azından puanı olan kayıt
                    if not body and not score:
                        continue
                    prev = by_id.get(rid)
                    # Daha uzun metni tercih et
                    if prev and len(str(prev.get("body") or "")) >= len(body):
                        continue
                    by_id[rid] = {
                        "review_id": rid,
                        "author": (rv.get("userName") or "Anonim").strip()[:80] or "Anonim",
                        "body": body[:1200],
                        "raw": body[:2000],
                        "stars": f"{score} yıldız" if 1 <= score <= 5 else None,
                        "date": _fmt_tr_dt(dt),
                        "date_iso": dt.date().isoformat(),
                        "device": "",
                        "app_version": str(
                            rv.get("appVersion") or rv.get("reviewCreatedVersion") or ""
                        )[:40],
                        "reply": (rv.get("replyContent") or "").strip()[:800],
                        "source": "play_store_public",
                        "locale": f"{lang}-{country}",
                    }
                if stop or not token:
                    break
        except Exception as exc:  # noqa: BLE001
            logger.warning("Play Store reviews %s-%s: %s", lang, country, exc)
            continue
        logger.info(
            "Play Store reviews %s-%s fetched=%s uniq_total=%s",
            lang,
            country,
            fetched,
            len(by_id),
        )

    rows = list(by_id.values())
    rows.sort(key=lambda r: str(r.get("date_iso") or ""), reverse=True)
    return rows


def sync_store_reviews_to_workspace(
    db: Any,
    *,
    package_name: str = "com.Doviz",
    days: int = 365,
) -> dict[str, Any]:
    from backend.services.play_console_store import ingest_play_console_payload

    reviews = fetch_play_store_reviews(package_name, days=days)
    if not reviews:
        return {
            "ok": False,
            "message": "Play Store’dan yorum gelmedi",
            "review_count": 0,
        }
    result = ingest_play_console_payload(
        db,
        metrics=[],
        panels={},
        reviews=reviews,
        rating_summary={},
        source="play_store_public",
        source_url=f"https://play.google.com/store/apps/details?id={package_name}",
        package_name=package_name,
        sync_ok=True,
        sync_message=f"Play Store · son {days} gün · {len(reviews)} yorum",
        sync_mode="reviews_store",
        merge_reviews=True,
    )
    return {
        "ok": True,
        "message": f"Play Store · son {days} gün · {len(reviews)} yorum senkron",
        "review_count": len(reviews),
        "ingest": result,
    }
