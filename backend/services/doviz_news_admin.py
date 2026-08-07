"""Doviz.com admin — aktif haber listesi (login + pagination).

https://www.doviz.com/admin/news?page=1&type=N&status=1&is_advertorial=0&sort=id_desc
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import urlencode, urljoin

import requests

from backend.services.doviz_notification_admin import (
    admin_base_url,
    admin_credentials_configured,
    admin_http_proxy,
    login_admin_session,
)

LOGGER = logging.getLogger(__name__)

# 2024 ilk içerik — hesaplar ve scrape yalnızca bu id ve sonrası
DOVIZ_NEWS_MIN_ID = 719818

NEWS_PATH = "/admin/news"
NEWS_QUERY = {
    "type": "N",
    "status": "1",
    "is_advertorial": "0",
    "sort": "id_desc",
}
DEFAULT_MAX_PAGES = 320  # ~264 sayfa yeterli; tampon
PAGE_SIZE_HINT = 100

_TAG_RE = re.compile(r"<[^>]+>", re.I)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_WS_RE = re.compile(r"\s+")
_DATE_FMTS = ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y")


def _cell_text(raw: str) -> str:
    s = unescape(_TAG_RE.sub(" ", raw or ""))
    return _WS_RE.sub(" ", s).strip()


def _parse_dt(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _display_source(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s, flags=re.I).rstrip("/")
    return s


def _norm_source(raw: str) -> str:
    s = _display_source(raw)
    return s.lower() if s else ""


def news_list_url(page: int = 1) -> str:
    base = urljoin(admin_base_url() + "/", NEWS_PATH.lstrip("/"))
    q = dict(NEWS_QUERY)
    q["page"] = str(max(1, int(page)))
    return f"{base}?{urlencode(q)}"


def parse_news_admin_html(html: str) -> list[dict[str, Any]]:
    """Admin tablo satırları → doviz_news_sheet satır şekli."""
    out: list[dict[str, Any]] = []
    for block in _TR_RE.findall(html or ""):
        tds = [_cell_text(x) for x in _TD_RE.findall(block)]
        if len(tds) < 5:
            continue
        news_id = tds[0].strip()
        if not news_id.isdigit():
            continue
        active_raw = tds[1].strip() if len(tds) > 1 else ""
        title = tds[2].strip() if len(tds) > 2 else ""
        source_raw = tds[3].strip() if len(tds) > 3 else ""
        if source_raw in ("-", "—", "Kendi içeriği", "kendi içeriği"):
            source_raw = ""
        date_raw = tds[4].strip() if len(tds) > 4 else ""
        category = tds[5].strip() if len(tds) > 5 else "Diğer"
        if not title:
            continue
        active = active_raw in ("✅", "1", "true", "True", "yes", "aktif", "Aktif") or "✓" in active_raw
        # status=1 listesinde çoğu aktif; emoji yoksa da aktif say
        if not active_raw:
            active = True
        dt = _parse_dt(date_raw)
        out.append(
            {
                "id": news_id,
                "active": active if active_raw else True,
                "title": title,
                "source": _display_source(source_raw),
                "source_key": _norm_source(source_raw),
                "is_own": not bool(source_raw),
                "category": category or "Diğer",
                "date": dt.isoformat(sep=" ") if dt else None,
                "date_day": dt.strftime("%Y-%m-%d") if dt else None,
                "hour": dt.hour if dt else None,
                "weekday": dt.weekday() if dt else None,
                "iso_week": f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}" if dt else None,
            }
        )
    return out


def news_id_in_scope(news_id: Any) -> bool:
    """True if id >= 2024 ilk içerik (719818)."""
    try:
        return int(str(news_id).strip()) >= DOVIZ_NEWS_MIN_ID
    except (TypeError, ValueError):
        return False


def fetch_active_news_rows_from_admin(
    *,
    sess: requests.Session | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: int = 45,
    min_id: int = DOVIZ_NEWS_MIN_ID,
) -> dict[str, Any]:
    """Aktif haber sayfalarını dolaşır; min_id altını almaz (id_desc erken keser)."""
    if not admin_credentials_configured() and sess is None:
        raise ValueError("DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD gerekli")

    t0 = datetime.utcnow()
    own_session = sess is None
    if sess is None:
        sess = login_admin_session()

    by_id: dict[str, dict[str, Any]] = {}
    pages_ok = 0
    empty_streak = 0
    last_page = 0
    skipped_old = 0
    hit_floor = False

    try:
        for page in range(1, max(1, int(max_pages)) + 1):
            url = news_list_url(page)
            try:
                resp = sess.get(url, timeout=timeout, allow_redirects=True)
            except requests.RequestException as exc:
                LOGGER.warning("Admin news page %s failed: %s", page, exc)
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            if resp.status_code >= 400 or "/admin/login" in str(resp.url).lower():
                LOGGER.warning(
                    "Admin news page %s HTTP %s url=%s",
                    page,
                    resp.status_code,
                    resp.url,
                )
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            rows = parse_news_admin_html(resp.text or "")
            if not rows:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            empty_streak = 0
            pages_ok += 1
            last_page = page
            page_ids: list[int] = []
            for row in rows:
                try:
                    nid = int(str(row["id"]).strip())
                except (TypeError, ValueError):
                    continue
                page_ids.append(nid)
                if nid >= int(min_id):
                    by_id[str(nid)] = row
                else:
                    skipped_old += 1
            if page % 50 == 0:
                LOGGER.info(
                    "Admin news scrape progress page=%s rows=%s skipped_old=%s",
                    page,
                    len(by_id),
                    skipped_old,
                )
            # id_desc: sayfadaki en küçük id eşikten düşükse daha eski sayfalar gelir — dur
            if page_ids and min(page_ids) < int(min_id):
                hit_floor = True
                LOGGER.info(
                    "Admin news scrape floor at page=%s min_id_on_page=%s threshold=%s kept=%s",
                    page,
                    min(page_ids),
                    min_id,
                    len(by_id),
                )
                break
    finally:
        if own_session:
            try:
                sess.close()
            except Exception:
                pass

    rows = list(by_id.values())
    rows.sort(key=lambda r: r.get("date") or "", reverse=True)
    return {
        "ok": True,
        "rows": rows,
        "parsed": len(rows),
        "pages": pages_ok,
        "last_page": last_page,
        "min_id": int(min_id),
        "skipped_old": skipped_old,
        "hit_floor": hit_floor,
        "elapsed_sec": round((datetime.utcnow() - t0).total_seconds(), 2),
        "source": "doviz_admin_news",
        "source_url": news_list_url(1),
        "proxy": bool(admin_http_proxy()),
    }
