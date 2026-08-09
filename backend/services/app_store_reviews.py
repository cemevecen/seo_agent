"""App Store genel yorumları (iTunes RSS) — ASC DOM'a bağımlı değil."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

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
_DEFAULT_COUNTRIES = ("tr", "us", "de", "nl", "az", "ae")
_QUICK_COUNTRIES = ("tr", "us", "de")


def _fmt_tr_dt(dt: datetime) -> str:
    local = dt.astimezone(_UTC)
    return (
        f"{local.day} {_TR_MONTHS[local.month - 1]} {local.year}, "
        f"{local.hour:02d}:{local.minute:02d}"
    )


def fetch_app_store_reviews(
    track_id: str = "465599322",
    *,
    days: int = 365,
    countries: tuple[str, ...] | None = None,
    max_pages: int = 10,
    quick: bool = False,
) -> list[dict[str, Any]]:
    """iTunes customerreviews RSS — son `days` gün, çok ülke, dedupe."""
    days = max(28, min(400, int(days or 365)))
    cutoff = datetime.now(_UTC) - timedelta(days=days)
    by_id: dict[str, dict[str, Any]] = {}
    use_countries = countries or (_QUICK_COUNTRIES if quick else _DEFAULT_COUNTRIES)
    pages = max(1, min(20, int(max_pages or 10)))
    headers = {"User-Agent": "iTunes/12.0 (Macintosh; OS X 10.15.7)"}

    for country in use_countries:
        cc = (country or "tr").strip().lower() or "tr"
        collected_here = 0
        try:
            with httpx.Client(timeout=20.0, follow_redirects=True, headers=headers) as client:
                for page in range(1, pages + 1):
                    # Hem path-country hem query cc — RSS bazı bölgelerde birini tercih eder
                    urls = (
                        (
                            f"https://itunes.apple.com/{cc}/rss/customerreviews"
                            f"/page={page}/id={track_id}/sortby=mostrecent/json",
                            None,
                        ),
                        (
                            f"https://itunes.apple.com/rss/customerreviews"
                            f"/page={page}/id={track_id}/sortby=mostrecent/json",
                            {"l": cc, "cc": cc},
                        ),
                    )
                    entries = []
                    for url, params in urls:
                        try:
                            resp = client.get(url, params=params)
                            resp.raise_for_status()
                            entries = resp.json().get("feed", {}).get("entry", [])
                            if entries:
                                break
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("App Store RSS %s page=%s: %s", cc, page, exc)
                            continue
                    if not entries:
                        break
                    # İlk entry bazen app meta
                    stop = False
                    for e in entries:
                        if not isinstance(e, dict):
                            continue
                        if "im:rating" not in e:
                            continue
                        try:
                            score = int(e.get("im:rating", {}).get("label", 0) or 0)
                        except (TypeError, ValueError):
                            continue
                        if not (1 <= score <= 5):
                            continue
                        date_str = (e.get("updated", {}).get("label") or "")[:19]
                        try:
                            if "T" in date_str:
                                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                            else:
                                dt = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=_UTC)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=_UTC)
                        except Exception:
                            continue
                        if dt < cutoff:
                            stop = True
                            break
                        rid = str(e.get("id", {}).get("label") or "").strip()
                        title = (e.get("title", {}).get("label") or "").strip()
                        body = (e.get("content", {}).get("label") or "").strip()
                        text = (title + "\n" + body).strip() if title and body else (body or title)
                        author = (e.get("author", {}).get("name", {}).get("label") or "Anonim").strip()
                        version = e.get("im:version", {}).get("label") or ""
                        if not rid:
                            rid = f"{author}|{dt.isoformat()}|{score}"
                        prev = by_id.get(rid)
                        if prev and len(str(prev.get("body") or "")) >= len(text):
                            continue
                        by_id[rid] = {
                            "review_id": rid[:80],
                            "author": (author or "Anonim")[:80],
                            "body": text[:1200],
                            "raw": text[:2000],
                            "stars": f"{score} yıldız",
                            "date": _fmt_tr_dt(dt),
                            "date_iso": dt.date().isoformat(),
                            "device": "",
                            "app_version": str(version)[:40],
                            "reply": "",
                            "source": "app_store_public",
                            "locale": cc,
                        }
                        collected_here += 1
                    if stop:
                        break
        except Exception as exc:  # noqa: BLE001
            logger.warning("App Store reviews %s: %s", cc, exc)
            continue
        logger.info(
            "App Store reviews %s fetched_page=%s uniq_total=%s",
            cc,
            collected_here,
            len(by_id),
        )

    rows = list(by_id.values())
    rows.sort(key=lambda r: str(r.get("date_iso") or ""), reverse=True)
    return rows


def sync_app_store_reviews_to_workspace(
    db: Any,
    *,
    track_id: str = "465599322",
    days: int = 365,
    quick: bool = False,
) -> dict[str, Any]:
    from backend.services.asc_console_store import ingest_asc_console_payload

    tid = str(track_id or "465599322").strip() or "465599322"
    reviews = fetch_app_store_reviews(tid, days=days, quick=quick, max_pages=10 if quick else 12)
    if not reviews:
        return {
            "ok": False,
            "message": "App Store’dan yorum gelmedi",
            "review_count": 0,
        }
    result = ingest_asc_console_payload(
        db,
        reviews=reviews,
        source="app_store_public",
        source_url=f"https://apps.apple.com/app/id{tid}",
        app_id=tid,
        sync_ok=True,
        sync_message=f"App Store · son {days} gün · {len(reviews)} yorum",
        sync_mode="reviews_store",
        merge_reviews=True,
    )
    return {
        "ok": True,
        "message": f"App Store · son {days} gün · {len(reviews)} yorum senkron",
        "review_count": len(reviews),
        "ingest": result,
    }
