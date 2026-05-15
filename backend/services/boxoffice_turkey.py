"""
boxofficeturkiye.com veri çekici — haftalık Türkiye gişe listesi.
robots.txt: Allow: / (tüm botlara açık)
HTML tabanlı, JavaScript render yok.
"""
from __future__ import annotations

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
    """Bu haftanın detay URL'si: /hafta/detay/2026-19 formatı."""
    today = date.today()
    year, week, _ = today.isocalendar()
    return f"{BOT_BASE}/hafta/detay/{year}-{week:02d}"


def _prev_week_url() -> str:
    """Geçen haftanın detay URL'si (pazartesi günü yeni hafta henüz yayınlanmamış olabilir)."""
    today = date.today()
    year, week, _ = today.isocalendar()
    week -= 1
    if week == 0:
        year -= 1
        week = 52
    return f"{BOT_BASE}/hafta/detay/{year}-{week:02d}"


def _parse_number(s: str) -> int:
    """'58.801' veya '₺16.995.056' → integer."""
    return int(re.sub(r"[^\d]", "", s or "0") or 0)


def fetch_current_boxoffice() -> list[dict[str, Any]]:
    """
    Türkiye'nin güncel haftalık gişe listesini çeker.
    Format: [{title, bot_id, weekly_audience, weekly_revenue, total_audience, total_revenue, detail_url}]
    """
    for url_fn in (_current_week_url, _prev_week_url):
        url = url_fn()
        try:
            resp = requests.get(url, headers=BOT_HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 5000:
                films = _parse_film_list(resp.text, url)
                if films:
                    logger.info("BOT gişe listesi çekildi: %s → %d film", url, len(films))
                    return films
        except Exception as exc:
            logger.warning("BOT gişe fetch hatası (%s): %s", url, exc)

    return []


def _parse_film_list(html: str, source_url: str) -> list[dict[str, Any]]:
    """HTML'den film listesini ayrıştırır (BeautifulSoup kullanmadan)."""
    films: list[dict] = []

    # Film linkleri: href="/film/slug--id">Film Adı</a>
    link_pattern = re.compile(
        r'href="/film/([^"]+)--(\d+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )

    # Satır bazlı yaklaşım: <tr> bloklarını bul, içinden film + sayıları çıkar
    # Basit yaklaşım: tüm film linklerini çek, sıra numarasına göre sayılarla eşleştir
    links = link_pattern.findall(html)

    # Sayılar: tablodaki <td> içindeki rakamları sırayla çek
    td_pattern = re.compile(r'<td[^>]*>\s*([\d.,₺\s]+?)\s*</td>', re.IGNORECASE)
    numbers_raw = [m.group(1).strip() for m in td_pattern.finditer(html)]
    numbers = [_parse_number(n) for n in numbers_raw if re.search(r'\d{3,}', n)]

    # Her film için 4 sayı: hafta_seyirci, toplam_seyirci, hafta_hasılat, toplam_hasılat
    # (ya da tersi — sıralamanın kesin formatını bilemiyoruz, iki kombinasyonu dene)
    seen_ids: set[str] = set()
    num_idx = 0

    for slug, bot_id, raw_title in links:
        title = raw_title.strip()
        if not title or bot_id in seen_ids:
            continue
        seen_ids.add(bot_id)

        # Sonraki 4 sayıyı al
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
