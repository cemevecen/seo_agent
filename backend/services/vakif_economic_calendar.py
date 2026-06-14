"""Vakıf Yatırım (vkyanaliz.com) haftalık piyasa takvimi ve strateji bültenleri."""

from __future__ import annotations

import logging
import re
import time
from html import unescape
from typing import Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

VKY_BASE = "https://www.vkyanaliz.com"
VAKIF_SOURCE_URL = (
    "https://www.vakifbank.com.tr/tr/bireysel/yatirim/arastirmalar-ve-raporlar/piyasa-raporlari"
)

_REPORT_PATHS: dict[str, tuple[str, str]] = {
    "weekly_calendar": ("/piyasalara-bakis/haftalik-piyasa-takvimi", "Haftalık Piyasa Takvimi"),
    "daily_strategy": ("/piyasalara-bakis/gunluk-strateji-bulteni", "Günlük Strateji Bülteni"),
    "weekly_strategy": ("/piyasalara-bakis/haftalik-strateji-bulteni", "Haftalık Strateji Bülteni"),
    "technical_strategy": ("/yatirim-danismanligi/teknik-strateji-bulteni", "Teknik Strateji Bülteni"),
}

_CACHE: dict[str, Any] = {"data": None, "ts": 0.0}
_CACHE_TTL_SEC = 3600
_REQUEST_TIMEOUT = 20
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SEOAgent/1.0; +https://projectcontrol.up.railway.app)"}


def _fetch_html(path: str) -> str:
    url = urljoin(VKY_BASE + "/", path.lstrip("/"))
    resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _first_article_html(page_html: str) -> str:
    m = re.search(r"<article[^>]*>(.*?)</article>", page_html, re.I | re.S)
    return m.group(1) if m else page_html


def _strip_tags(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _parse_title_date(article_html: str, fallback_label: str) -> tuple[str, str]:
    m = re.search(r"<h3[^>]*>(.*?)</h3>", article_html, re.I | re.S)
    raw = _strip_tags(m.group(1)) if m else fallback_label
    if " / " in raw:
        label, date_label = raw.split(" / ", 1)
        return label.strip(), date_label.strip()
    return fallback_label, raw


def _parse_excerpt(article_html: str, *, skip_labels: frozenset[str]) -> str:
    for m in re.finditer(r"<p[^>]*>(.*?)</p>", article_html, re.I | re.S):
        text = _strip_tags(m.group(1))
        if not text or len(text) < 40:
            continue
        if text in skip_labels:
            continue
        return text[:420]
    return ""


def _parse_week_range(article_html: str) -> str:
    text = _strip_tags(article_html)
    m = re.search(r"Haftanın Gündem Konuları\s*\(([^)]+)\)", text, re.I)
    return m.group(1).strip() if m else ""


def _parse_region_agenda(article_html: str) -> list[dict[str, Any]]:
    regions: list[dict[str, Any]] = []
    for m in re.finditer(r'<li[^>]*class="MsoNormal"[^>]*>(.*?)</li>', article_html, re.I | re.S):
        block = m.group(1)
        strong_m = re.search(r"<strong[^>]*>(.*?)</strong>", block, re.I | re.S)
        if not strong_m:
            continue
        region = _strip_tags(strong_m.group(1)).rstrip("–").rstrip("-").strip()
        events_raw = _strip_tags(block.replace(strong_m.group(0), "", 1))
        events = [e.strip() for e in events_raw.split(",") if e.strip()]
        if region and events:
            regions.append({"region": region, "events": events})
    if regions:
        return regions
    # Yedek: sade strong + metin
    for m in re.finditer(r"<li[^>]*>\s*<strong>([^<]+)</strong>\s*(.*?)\s*</li>", article_html, re.I | re.S):
        region = _strip_tags(m.group(1)).rstrip("–").rstrip("-").strip()
        events_raw = _strip_tags(m.group(2))
        events = [e.strip() for e in events_raw.split(",") if e.strip()]
        if region and events:
            regions.append({"region": region, "events": events})
    return regions


def _abs_href(href: str) -> str:
    return urljoin(VKY_BASE + "/", href.lstrip("/"))


def _parse_first_pdf(article_html: str, slug_hint: str) -> str:
    m = re.search(rf'href="(Files/docs/{re.escape(slug_hint)}[^"]+\.pdf)"', article_html, re.I)
    return _abs_href(m.group(1)) if m else ""


def _parse_detail_url(page_html: str, slug_hint: str) -> str:
    m = re.search(rf'href="(bulten/{slug_hint}/[^"]+)"', page_html, re.I)
    return _abs_href(m.group(1)) if m else ""


def _regions_to_items(regions: list[dict[str, Any]], *, pdf_url: str, detail_url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    href = detail_url or pdf_url or VAKIF_SOURCE_URL
    for block in regions:
        region = block["region"]
        for event in block["events"]:
            items.append(
                {
                    "title": event,
                    "subtitle": region,
                    "badge": region,
                    "href": href,
                }
            )
    return items


def _parse_bulletin(key: str, path: str, label: str, page_html: str) -> dict[str, Any]:
    article = _first_article_html(page_html)
    title, date_label = _parse_title_date(article, label)
    slug = path.rsplit("/", 1)[-1]
    pdf_url = _parse_first_pdf(article, slug) or _parse_first_pdf(page_html, slug)
    detail_url = _parse_detail_url(page_html, slug)
    excerpt = _parse_excerpt(article, skip_labels=frozenset({label, "Piyasalara Bakış"}))
    return {
        "key": key,
        "label": label,
        "title": title,
        "date_label": date_label,
        "excerpt": excerpt,
        "pdf_url": pdf_url,
        "page_url": urljoin(VKY_BASE + "/", path.lstrip("/")),
        "detail_url": detail_url,
    }


def fetch_vakif_economic_calendar(*, force: bool = False) -> dict[str, Any]:
    """Vakıf Yatırım haftalık takvim + strateji bültenlerini döner (1s TTL cache)."""
    now = time.monotonic()
    if not force and _CACHE["data"] is not None and (now - float(_CACHE["ts"])) < _CACHE_TTL_SEC:
        return _CACHE["data"]

    payload: dict[str, Any] = {
        "source_url": VAKIF_SOURCE_URL,
        "provider": "Vakıf Yatırım / vkyanaliz.com",
        "weekly": None,
        "bulletins": [],
        "error": None,
    }

    try:
        cal_path, cal_label = _REPORT_PATHS["weekly_calendar"]
        cal_page = _fetch_html(cal_path)
        cal_article = _first_article_html(cal_page)
        _, published_label = _parse_title_date(cal_article, cal_label)
        week_range = _parse_week_range(cal_article)
        regions = _parse_region_agenda(cal_article)
        cal_slug = cal_path.rsplit("/", 1)[-1]
        pdf_url = _parse_first_pdf(cal_article, cal_slug) or _parse_first_pdf(cal_page, cal_slug)
        detail_url = _parse_detail_url(cal_page, cal_slug)

        payload["weekly"] = {
            "title": cal_label,
            "published_label": published_label,
            "week_range": week_range,
            "regions": regions,
            "pdf_url": pdf_url,
            "detail_url": detail_url,
            "items": _regions_to_items(regions, pdf_url=pdf_url, detail_url=detail_url),
        }

        for key in ("daily_strategy", "weekly_strategy", "technical_strategy"):
            path, label = _REPORT_PATHS[key]
            try:
                page_html = _fetch_html(path)
                payload["bulletins"].append(_parse_bulletin(key, path, label, page_html))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Vakıf bülten okunamadı (%s): %s", key, exc)

    except Exception as exc:  # noqa: BLE001
        logger.warning("Vakıf ekonomik takvim alınamadı: %s", exc)
        payload["error"] = str(exc)

    _CACHE["data"] = payload
    _CACHE["ts"] = now
    return payload
