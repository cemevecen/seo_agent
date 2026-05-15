"""
boxofficeturkiye.com veri çekici — haftalık Türkiye gişe listesi + vizyon takvimi.
robots.txt: Allow: / (tüm botlara açık)
HTML tabanlı, JavaScript render yok.
"""
from __future__ import annotations

import html
import logging
import re
import time
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

BOT_BASE    = "https://boxofficeturkiye.com"
BOT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SEOAgent/1.0; +https://doviz.com)",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


def _current_week_url() -> str:
    today = date.today()
    year, week, _ = today.isocalendar()
    return f"{BOT_BASE}/hafta/detay/{year}-{week:02d}"


def _prev_week_url() -> str:
    today = date.today()
    year, week, _ = today.isocalendar()
    week -= 1
    if week == 0:
        year -= 1
        week = 52
    return f"{BOT_BASE}/hafta/detay/{year}-{week:02d}"


def _parse_number(s: str) -> int:
    return int(re.sub(r"[^\d]", "", s or "0") or 0)


def _extract_film_links(raw_html: str) -> list[tuple[str, str, str]]:
    """HTML'den (slug, bot_id, title) üçlülerini çıkarır — tekrarları atar."""
    pattern = re.compile(
        r'href="/film/([^"]+)--(\d+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    seen: set[str] = set()
    results = []
    for slug, bot_id, raw_title in pattern.findall(raw_html):
        if bot_id in seen:
            continue
        title = html.unescape(raw_title.strip())
        if title:
            seen.add(bot_id)
            results.append((slug, bot_id, title))
    return results


def fetch_current_boxoffice() -> list[dict[str, Any]]:
    """
    Haftalık gişe listesi + vizyon takvimini birleştirir.
    Gişe listesi: bu haftanın seyirci/hasılat verileri (60-70 film).
    Vizyon takvimi: gelecek haftaların vizyon programı (40-50 film).
    İkisi birleşince ~100+ film kümesi elde edilir.
    """
    all_films: dict[str, dict] = {}

    # 1. Haftalık gişe listesi (mevcut haftadan başla, gerekirse önceki haftaya geç)
    for url_fn in (_current_week_url, _prev_week_url):
        url = url_fn()
        try:
            resp = requests.get(url, headers=BOT_HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 5000:
                films = _parse_weekly_list(resp.text, url)
                if films:
                    logger.info("BOT haftalık liste: %s → %d film", url, len(films))
                    for f in films:
                        all_films[f["bot_id"]] = f
                    break
        except Exception as exc:
            logger.warning("BOT haftalık fetch hatası (%s): %s", url, exc)

    # 2. Vizyon takvimi (gelecek haftaların filmleri)
    takvim_url = f"{BOT_BASE}/takvim"
    try:
        resp = requests.get(takvim_url, headers=BOT_HEADERS, timeout=15)
        if resp.status_code == 200:
            for slug, bot_id, title in _extract_film_links(resp.text):
                if bot_id not in all_films:
                    all_films[bot_id] = {
                        "title":           title,
                        "bot_id":          bot_id,
                        "slug":            slug,
                        "weekly_audience": 0,
                        "total_audience":  0,
                        "weekly_revenue":  0,
                        "total_revenue":   0,
                        "detail_url":      f"{BOT_BASE}/film/{slug}--{bot_id}",
                        "source_url":      takvim_url,
                    }
            logger.info("BOT vizyon takvimi: %s → toplam %d film", takvim_url, len(all_films))
    except Exception as exc:
        logger.warning("BOT takvim fetch hatası: %s", exc)

    return list(all_films.values())


def _parse_weekly_list(raw_html: str, source_url: str) -> list[dict[str, Any]]:
    """Haftalık gişe sayfasından film + seyirci/hasılat verilerini çıkarır."""
    links = _extract_film_links(raw_html)

    td_pattern = re.compile(r'<td[^>]*>\s*([\d.,₺\s]+?)\s*</td>', re.IGNORECASE)
    numbers_raw = [m.group(1).strip() for m in td_pattern.finditer(raw_html)]
    numbers = [_parse_number(n) for n in numbers_raw if re.search(r'\d{3,}', n)]

    films = []
    num_idx = 0
    for slug, bot_id, title in links:
        chunk = numbers[num_idx:num_idx + 4] if num_idx + 4 <= len(numbers) else [0, 0, 0, 0]
        num_idx += 4
        films.append({
            "title":           title,
            "bot_id":          bot_id,
            "slug":            slug,
            "weekly_audience": chunk[0] if chunk else 0,
            "total_audience":  chunk[1] if len(chunk) > 1 else 0,
            "weekly_revenue":  chunk[2] if len(chunk) > 2 else 0,
            "total_revenue":   chunk[3] if len(chunk) > 3 else 0,
            "detail_url":      f"{BOT_BASE}/film/{slug}--{bot_id}",
            "source_url":      source_url,
        })
    return films


def find_missing_from_tmdb(
    boxoffice_films: list[dict],
    existing_tmdb_ids: set[int],
    tmdb_search_fn: Any,
) -> list[dict[str, Any]]:
    """
    Gişe listesindeki filmleri TMDB'de arar.
    existing_tmdb_ids: zaten vizyon listesinde olan TMDB ID'leri.
    Döndürülen liste: TMDB ID'si bulunmuş ama vizyon listesinde olmayan filmler.
    """
    missing: list[dict] = []

    for film in boxoffice_films:
        title = film["title"]
        try:
            result = tmdb_search_fn(title)
            time.sleep(0.1)
        except Exception as exc:
            logger.warning("TMDB arama hatası [%s]: %s", title, exc)
            continue

        if not result:
            continue

        tmdb_id = result.get("id")
        if tmdb_id and tmdb_id not in existing_tmdb_ids:
            # Gişe verisiyle zenginleştir
            result["weekly_audience"] = film["weekly_audience"]
            result["total_audience"]  = film["total_audience"]
            result["weekly_revenue"]  = film["weekly_revenue"]
            result["total_revenue"]   = film["total_revenue"]
            result["bot_detail_url"]  = film["detail_url"]
            result["source"]          = "boxoffice_turkey"
            missing.append(result)
            existing_tmdb_ids.add(tmdb_id)

    logger.info("BOT→TMDB: %d filmden %d tanesi vizyon listesinde yok",
                len(boxoffice_films), len(missing))
    return missing
