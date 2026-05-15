"""OMDB (Open Movie Database) entegrasyonu — IMDb/RT/Metacritic zenginleştirme."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

import requests
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import OmdbEnrichment

logger = logging.getLogger(__name__)

OMDB_BASE   = "https://www.omdbapi.com/"
DAILY_LIMIT = 999
RETRY_DAYS  = 7   # bulunamayan filmler N gün sonra tekrar denenir


def _api_key() -> str:
    key = (settings.omdb_api_key or "").strip()
    if not key:
        raise RuntimeError("OMDB_API_KEY tanımlı değil. Railway Variables'a ekleyin.")
    return key


def _fetch_omdb(title: str, year: str | None = None) -> dict | None:
    """Başlık + yıl ile OMDB'den film verisi çeker."""
    params: dict = {"t": title, "apikey": _api_key(), "plot": "short"}
    if year:
        params["y"] = year
    try:
        resp = requests.get(OMDB_BASE, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") == "True":
            return data
    except Exception as exc:
        logger.warning("OMDB fetch hatası [%s]: %s", title, exc)
    return None


def _safe(data: dict, key: str) -> str | None:
    v = data.get(key, "")
    return v if v and v != "N/A" else None


def _rt_score(ratings: list) -> str | None:
    for r in ratings:
        if "Rotten Tomatoes" in r.get("Source", ""):
            v = r.get("Value", "")
            return v if v and v != "N/A" else None
    return None


def upsert_omdb(db: Session, tmdb_id: int, title: str, year: str | None = None) -> OmdbEnrichment:
    """
    Tek film için OMDB verisi çekip DB'ye kaydeder/günceller.
    Zaten güncel kayıt varsa fetch yapmaz.
    """
    row = db.query(OmdbEnrichment).filter(OmdbEnrichment.tmdb_id == tmdb_id).first()

    # Güncelse atla
    if row:
        age_days = (datetime.utcnow() - row.fetched_at).days
        if row.found and age_days < 30:
            return row
        if not row.found and age_days < RETRY_DAYS:
            return row

    data = _fetch_omdb(title, year)

    if row is None:
        row = OmdbEnrichment(tmdb_id=tmdb_id)
        db.add(row)

    if data:
        ratings = data.get("Ratings") or []
        row.imdb_id     = _safe(data, "imdbID")
        row.imdb_rating = _safe(data, "imdbRating")
        row.imdb_votes  = _safe(data, "imdbVotes")
        row.rt_score    = _rt_score(ratings)
        row.metacritic  = _safe(data, "Metascore")
        row.age_rating  = _safe(data, "Rated")
        row.box_office  = _safe(data, "BoxOffice")
        row.awards      = _safe(data, "Awards")
        row.found       = True
    else:
        row.found = False

    row.fetched_at = datetime.utcnow()

    try:
        db.commit()
    except Exception:
        db.rollback()

    return row


def run_daily_omdb_enrichment(db: Session) -> dict:
    """
    Günlük zamanlanmış iş:
    - Mevcut TMDB vizyon listesindeki filmleri al
    - OMDB verisi olmayan veya eskimiş olanları filtrele
    - En yeniden başlayarak günlük 999 çekimi gerçekleştir
    """
    from backend.services.tmdb import fetch_combined_upcoming

    logger.info("OMDB günlük zenginleştirme başladı (limit=%d)", DAILY_LIMIT)

    try:
        data = fetch_combined_upcoming(months_ahead=8)
    except Exception as exc:
        logger.error("TMDB veri çekimi başarısız: %s", exc)
        return {"status": "error", "message": str(exc)}

    # Tüm benzersiz filmler (theatrical + streaming + turkish + tv)
    all_movies: list[dict] = []
    seen_ids: set[int] = set()
    for lst in (
        data.get("theatrical", []),
        data.get("streaming", []),
        data.get("turkish_only", []),
    ):
        for m in lst:
            mid = m.get("id")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                all_movies.append(m)

    # En yeniden eskiye sırala
    all_movies.sort(key=lambda x: x.get("release_date") or "0000", reverse=True)

    # Halihazırda güncel kayıtları DB'den çek
    retry_cutoff = datetime.utcnow() - timedelta(days=RETRY_DAYS)
    existing = {
        row.tmdb_id
        for row in db.query(OmdbEnrichment.tmdb_id, OmdbEnrichment.found, OmdbEnrichment.fetched_at)
        .filter(
            (OmdbEnrichment.found == True) |
            (OmdbEnrichment.fetched_at > retry_cutoff)
        )
        .all()
        if hasattr(row, "tmdb_id")
    }

    # Zenginleştirilecek filmler
    to_fetch = [m for m in all_movies if m.get("id") not in existing]

    fetched = 0
    errors  = 0

    for movie in to_fetch[:DAILY_LIMIT]:
        if fetched >= DAILY_LIMIT:
            break
        tmdb_id = movie["id"]
        title   = movie.get("title") or movie.get("original_title") or ""
        release = movie.get("release_date") or ""
        year    = release[:4] if release else None

        try:
            upsert_omdb(db, tmdb_id, title, year)
            fetched += 1
            time.sleep(0.12)   # ~8 req/sn → günde 999 kotasına saygılı
        except Exception as exc:
            logger.warning("OMDB upsert hatası [%d %s]: %s", tmdb_id, title, exc)
            errors += 1

    logger.info("OMDB zenginleştirme tamamlandı: fetched=%d errors=%d", fetched, errors)
    return {"fetched": fetched, "errors": errors, "skipped": len(all_movies) - fetched - errors}


def get_enrichment_map(db: Session, tmdb_ids: list[int]) -> dict[int, OmdbEnrichment]:
    """Verilen TMDB ID listesi için DB'deki zenginleştirme verisini dict olarak döner."""
    if not tmdb_ids:
        return {}
    rows = (
        db.query(OmdbEnrichment)
        .filter(OmdbEnrichment.tmdb_id.in_(tmdb_ids), OmdbEnrichment.found == True)
        .all()
    )
    return {r.tmdb_id: r for r in rows}
