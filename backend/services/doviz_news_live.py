"""haber.doviz.com canlı tarama — Google Sheet gecikince son içerikleri tamamlar."""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

_TR_TZ = ZoneInfo("Europe/Istanbul")

logger = logging.getLogger(__name__)

LIVE_HOME = "https://haber.doviz.com/"
LIVE_CATEGORIES = (
    "gundem-haberleri",
    "borsa-haberleri",
    "emtia-haberleri",
    "altin-ve-degerli-metal-haberleri",
    "doviz-haberleri",
    "kripto-para-haberleri",
    "yerel-ve-sektorel-haberleri",
    "dunya-haberleri",
)

_ARTICLE_HREF_RE = re.compile(
    r'href="(https://haber\.doviz\.com/([a-z0-9-]+)/[^"]+/(\d{5,}))"',
    re.I,
)
_ARTICLE_PATH_RE = re.compile(
    r'href="(/([a-z0-9-]+)/[^"]+/(\d{5,}))"',
    re.I,
)

_UA = "Mozilla/5.0 (compatible; SEOAgent-DovizNews/1.0)"
_TIMEOUT = 18


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )
    return s


def _get_html(sess: requests.Session, url: str) -> str:
    resp = sess.get(url, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.text or ""


def discover_live_article_refs(
    *,
    limit: int = 120,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    """Ana sayfa + kategori listelerinden en yeni makale URL/ID’lerini toplar."""
    sess = session or _session()
    found: dict[str, dict[str, str]] = {}

    urls = [LIVE_HOME] + [urljoin(LIVE_HOME, f"{slug}/") for slug in LIVE_CATEGORIES]
    for page_url in urls:
        try:
            html = _get_html(sess, page_url)
        except Exception as exc:  # noqa: BLE001
            logger.info("doviz news live list failed %s: %s", page_url, exc)
            continue
        for m in _ARTICLE_HREF_RE.finditer(html):
            href, cat_slug, news_id = m.group(1), m.group(2), m.group(3)
            if news_id not in found:
                found[news_id] = {
                    "id": news_id,
                    "url": href,
                    "category_slug": cat_slug,
                }
        for m in _ARTICLE_PATH_RE.finditer(html):
            path, cat_slug, news_id = m.group(1), m.group(2), m.group(3)
            if news_id not in found:
                found[news_id] = {
                    "id": news_id,
                    "url": urljoin(LIVE_HOME, path.lstrip("/")),
                    "category_slug": cat_slug,
                }

    refs = list(found.values())
    refs.sort(key=lambda r: int(r["id"]) if r["id"].isdigit() else 0, reverse=True)
    return refs[: max(1, min(int(limit), 300))]


def _parse_iso_dt(raw: str | None) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        # 2026-08-07T14:54:00+03:00
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _to_naive_turkey(dt: datetime | None) -> datetime | None:
    """Aware → Europe/Istanbul duvar saati (naive). Impala/sheet ile uyumlu."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(_TR_TZ).replace(tzinfo=None)


def _apply_dt_fields(row: dict[str, Any], dt: datetime) -> None:
    row["date"] = dt.isoformat(sep=" ", timespec="seconds")
    row["date_day"] = dt.strftime("%Y-%m-%d")
    row["hour"] = dt.hour
    row["weekday"] = dt.weekday()
    row["iso_week"] = f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"


def _first_news_article_ld(html: str) -> dict[str, Any] | None:
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html or "",
        flags=re.I | re.S,
    ):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            t = str(item.get("@type") or "")
            if t in ("NewsArticle", "Article", "ReportageNewsArticle"):
                return item
    return None


def fetch_article_row(
    ref: dict[str, str],
    *,
    session: requests.Session | None = None,
) -> dict[str, Any] | None:
    """Tek makale JSON-LD → sheet satır şeması."""
    sess = session or _session()
    url = ref.get("url") or ""
    news_id = str(ref.get("id") or "").strip()
    if not url or not news_id:
        return None
    try:
        html = _get_html(sess, url)
    except Exception as exc:  # noqa: BLE001
        logger.debug("doviz news article fetch failed %s: %s", url, exc)
        return None
    ld = _first_news_article_ld(html)
    if not ld:
        return None

    # Yayına alma zamanı: datePublished (dateCreated genelde taslak/import saati)
    dt = _to_naive_turkey(
        _parse_iso_dt(ld.get("datePublished"))
        or _parse_iso_dt(ld.get("dateModified"))
        or _parse_iso_dt(ld.get("dateCreated"))
    )
    title = str(ld.get("headline") or ld.get("alternativeHeadline") or "").strip()
    category = str(ld.get("articleSection") or "").strip() or "Diğer"
    if not title:
        return None

    row: dict[str, Any] = {
        "id": news_id,
        "active": True,
        "title": title,
        "source": "",
        "source_key": "",
        "is_own": True,
        "category": category,
        "date": None,
        "date_day": None,
        "hour": None,
        "weekday": None,
        "iso_week": None,
        "_live": True,
        "_url": url,
    }
    if dt is not None:
        _apply_dt_fields(row, dt)
    return row


def fetch_live_gap_rows(
    *,
    known_ids: set[str] | frozenset[str],
    min_id: int = 0,
    discover_limit: int = 120,
    fetch_limit: int = 80,
    workers: int = 8,
) -> list[dict[str, Any]]:
    """Sheet’te olmayan (veya min_id üstü) canlı makaleleri çeker."""
    t0 = time.monotonic()
    sess = _session()
    refs = discover_live_article_refs(limit=discover_limit, session=sess)
    todo: list[dict[str, str]] = []
    for ref in refs:
        nid = ref["id"]
        try:
            n_int = int(nid)
        except ValueError:
            continue
        if nid in known_ids and n_int <= min_id:
            continue
        if nid in known_ids:
            # Sheet’te var — yine de daha yeni tarih için zorlamıyoruz
            continue
        todo.append(ref)
        if len(todo) >= fetch_limit:
            break

    if not todo:
        logger.info(
            "doviz news live: no gap (discovered=%s known=%s min_id=%s) in %.1fs",
            len(refs),
            len(known_ids),
            min_id,
            time.monotonic() - t0,
        )
        return []

    out: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as pool:
        # Session thread-safe değil — her worker kendi isteğini atar
        futs = {pool.submit(fetch_article_row, ref): ref for ref in todo}
        for fut in as_completed(futs):
            try:
                row = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("doviz news live worker error: %s", exc)
                continue
            if row:
                out.append(row)

    out.sort(key=lambda r: r.get("date") or "", reverse=True)
    logger.info(
        "doviz news live: gap fetched=%s/%s discovered=%s in %.1fs",
        len(out),
        len(todo),
        len(refs),
        time.monotonic() - t0,
    )
    return out


def merge_sheet_with_live(
    sheet_rows: list[dict[str, Any]],
    live_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Sheet birincil; canlı satırlar eksik ID’leri tamamlar.

    Aynı ID için canlı tarih daha yeniyse (yayın saati) sheet tarihini günceller.
    """
    by_id: dict[str, dict[str, Any]] = {}
    for r in sheet_rows or []:
        nid = str(r.get("id") or "").strip()
        if nid:
            by_id[nid] = dict(r)
    added = 0
    updated = 0
    for r in live_rows or []:
        nid = str(r.get("id") or "").strip()
        if not nid:
            continue
        live_date = str(r.get("date") or "")
        if nid not in by_id:
            by_id[nid] = r
            added += 1
            continue
        sheet_date = str(by_id[nid].get("date") or "")
        if live_date and live_date > sheet_date:
            cur = dict(by_id[nid])
            for k in ("date", "date_day", "hour", "weekday", "iso_week"):
                if r.get(k) is not None:
                    cur[k] = r.get(k)
            by_id[nid] = cur
            updated += 1
    merged = list(by_id.values())
    merged.sort(key=lambda r: r.get("date") or "", reverse=True)
    if added or updated:
        logger.info(
            "doviz news merge: +%s live · %s tarih güncellendi → total %s",
            added,
            updated,
            len(merged),
        )
    return merged


def enrich_rows_with_publish_dates(
    rows: list[dict[str, Any]],
    *,
    limit: int = 80,
    discover_limit: int = 160,
    workers: int = 8,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Admin/Impala dateCreated yerine JSON-LD datePublished ile son kayıtları düzeltir."""
    if not rows:
        return [], {"ok": True, "checked": 0, "updated": 0}

    t0 = time.monotonic()
    try:
        refs = discover_live_article_refs(limit=discover_limit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("doviz news publish enrich discover failed: %s", exc)
        return list(rows), {"ok": False, "error": str(exc)[:200], "updated": 0}

    ref_by_id = {str(r.get("id") or ""): r for r in refs if r.get("id")}
    todo: list[dict[str, str]] = []
    for row in sorted(
        rows,
        key=lambda r: int(str(r.get("id") or "0") or 0) if str(r.get("id") or "").isdigit() else 0,
        reverse=True,
    ):
        nid = str(row.get("id") or "").strip()
        ref = ref_by_id.get(nid)
        if not ref:
            continue
        todo.append(ref)
        if len(todo) >= max(1, min(int(limit), 200)):
            break

    if not todo:
        return list(rows), {
            "ok": True,
            "checked": 0,
            "updated": 0,
            "discovered": len(refs),
            "elapsed_sec": round(time.monotonic() - t0, 2),
        }

    live_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 12))) as pool:
        futs = {pool.submit(fetch_article_row, ref): ref for ref in todo}
        for fut in as_completed(futs):
            try:
                article = fut.result()
            except Exception:  # noqa: BLE001
                continue
            if article and article.get("id") and article.get("date"):
                live_by_id[str(article["id"])] = article

    updated = 0
    out: list[dict[str, Any]] = []
    for row in rows:
        nid = str(row.get("id") or "").strip()
        live = live_by_id.get(nid)
        if not live:
            out.append(row)
            continue
        live_date = str(live.get("date") or "")
        sheet_date = str(row.get("date") or "")
        if live_date and live_date != sheet_date:
            cur = dict(row)
            for k in ("date", "date_day", "hour", "weekday", "iso_week"):
                if live.get(k) is not None:
                    cur[k] = live.get(k)
            out.append(cur)
            updated += 1
        else:
            out.append(row)

    out.sort(key=lambda r: r.get("date") or "", reverse=True)
    meta = {
        "ok": True,
        "checked": len(todo),
        "fetched": len(live_by_id),
        "updated": updated,
        "discovered": len(refs),
        "elapsed_sec": round(time.monotonic() - t0, 2),
    }
    logger.info(
        "doviz news publish enrich: checked=%s fetched=%s updated=%s in %.1fs",
        meta["checked"],
        meta["fetched"],
        updated,
        meta["elapsed_sec"],
    )
    return out, meta
