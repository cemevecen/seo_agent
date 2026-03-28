"""Google Search Console collector'ı."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

from google.auth.transport.requests import Request as GoogleAuthRequest
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site, SiteCredential
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.search_console_auth import SEARCH_CONSOLE_SCOPES, get_search_console_credentials_record, load_google_credentials
from backend.services.metric_store import save_metrics
from backend.services.quota_guard import consume_api_quota
from backend.services.warehouse import (
    finish_collector_run,
    get_latest_search_console_rows,
    save_search_console_query_rows,
    start_collector_run,
)

LOGGER = logging.getLogger(__name__)


def _normalize_site_host(domain: str) -> str:
    raw = (domain or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return urlparse(raw).netloc.lower().strip("/")
    return raw.lower().strip("/")


def _property_candidates(domain: str) -> list[str]:
    host = _normalize_site_host(domain)
    if not host:
        return []

    naked = host[4:] if host.startswith("www.") else host
    candidates: list[str] = []
    for candidate in (
        f"sc-domain:{host}",
        f"sc-domain:{naked}",
        f"https://{host}/",
        f"http://{host}/",
    ):
        if candidate not in candidates:
            candidates.append(candidate)

    if host.startswith("www."):
        extra = (f"https://{naked}/", f"http://{naked}/")
    else:
        extra = (f"https://www.{host}/", f"http://www.{host}/")

    for candidate in extra:
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _resolve_search_console_property(service, site: Site) -> str:
    candidates = _property_candidates(site.domain)
    entries = service.sites().list().execute().get("siteEntry", [])
    available = {
        entry.get("siteUrl")
        for entry in entries
        if entry.get("siteUrl") and entry.get("permissionLevel") not in {"siteUnverifiedUser", "siteRestrictedUser"}
    }

    for candidate in candidates:
        if candidate in available:
            return candidate

    raise ValueError(
        f"Erisilebilir Search Console property bulunamadi. Adaylar: {', '.join(candidates)}"
    )


def _get_mock_queries_for_domain(domain: str) -> list[dict]:
    """Domain'e göre dinamik test querylerini döndürür (TOP 50). Device ayrılımı ile (desktop/mobile)."""
    # Domain'i normalize et (www'siz, lowercase)
    domain_key = domain.lower().replace("www.", "").split(".")[0]
    
    # Domain key'ini kısalt (sinemalar → sinema)
    if domain_key.startswith("sinema"):
        domain_key = "sinema"
    
    # Kategori-spesifik queryler (Top 50 keywords)
    query_sets = {
        "sinema": [
            # Top tier (80-100 clicks)
            {"query": "sinema seans saatleri", "clicks_desktop": 85.0, "clicks_mobile": 55.0, "impressions_desktop": 1800.0, "impressions_mobile": 1200.0, "position_desktop": 2.5, "position_mobile": 3.2},
            {"query": "film izle", "clicks_desktop": 80.0, "clicks_mobile": 55.0, "impressions_desktop": 1900.0, "impressions_mobile": 1300.0, "position_desktop": 2.1, "position_mobile": 2.6},
            # High tier (50-80 clicks)
            {"query": "yakındaki sinemalar", "clicks_desktop": 65.0, "clicks_mobile": 42.0, "impressions_desktop": 1500.0, "impressions_mobile": 1000.0, "position_desktop": 3.2, "position_mobile": 4.1},
            {"query": "yeni filmler", "clicks_desktop": 60.0, "clicks_mobile": 40.0, "impressions_desktop": 1400.0, "impressions_mobile": 900.0, "position_desktop": 2.9, "position_mobile": 3.5},
            {"query": "film önerileri", "clicks_desktop": 55.0, "clicks_mobile": 38.0, "impressions_desktop": 1300.0, "impressions_mobile": 850.0, "position_desktop": 3.1, "position_mobile": 3.8},
            {"query": "sinema bilet fiyatları", "clicks_desktop": 52.0, "clicks_mobile": 35.0, "impressions_desktop": 1200.0, "impressions_mobile": 800.0, "position_desktop": 4.0, "position_mobile": 5.0},
            # Medium tier (30-50 clicks)
            {"query": "çocuk filmleri", "clicks_desktop": 48.0, "clicks_mobile": 32.0, "impressions_desktop": 1100.0, "impressions_mobile": 700.0, "position_desktop": 4.6, "position_mobile": 5.2},
            {"query": "korku filmleri", "clicks_desktop": 47.0, "clicks_mobile": 31.0, "impressions_desktop": 1050.0, "impressions_mobile": 700.0, "position_desktop": 3.7, "position_mobile": 4.3},
            {"query": "aksiyon filmleri", "clicks_desktop": 45.0, "clicks_mobile": 30.0, "impressions_desktop": 1000.0, "impressions_mobile": 650.0, "position_desktop": 3.3, "position_mobile": 3.9},
            {"query": "romantik filmler", "clicks_desktop": 43.0, "clicks_mobile": 29.0, "impressions_desktop": 950.0, "impressions_mobile": 620.0, "position_desktop": 4.9, "position_mobile": 5.5},
            {"query": "komedi filmleri", "clicks_desktop": 42.0, "clicks_mobile": 28.0, "impressions_desktop": 930.0, "impressions_mobile": 600.0, "position_desktop": 4.3, "position_mobile": 4.9},
            {"query": "bilim kurgu filmleri", "clicks_desktop": 40.0, "clicks_mobile": 27.0, "impressions_desktop": 900.0, "impressions_mobile": 580.0, "position_desktop": 4.7, "position_mobile": 5.3},
            {"query": "film uyarlaması", "clicks_desktop": 38.0, "clicks_mobile": 25.0, "impressions_desktop": 850.0, "impressions_mobile": 550.0, "position_desktop": 4.9, "position_mobile": 5.8},
            {"query": "film review", "clicks_desktop": 36.0, "clicks_mobile": 24.0, "impressions_desktop": 800.0, "impressions_mobile": 520.0, "position_desktop": 5.1, "position_mobile": 5.9},
            {"query": "sinema kartı", "clicks_desktop": 32.0, "clicks_mobile": 21.0, "impressions_desktop": 700.0, "impressions_mobile": 450.0, "position_desktop": 5.6, "position_mobile": 6.2},
            # Lower tier (20-30 clicks)
            {"query": "imax sinema", "clicks_desktop": 28.0, "clicks_mobile": 18.0, "impressions_desktop": 600.0, "impressions_mobile": 380.0, "position_desktop": 6.0, "position_mobile": 6.6},
            {"query": "3d sinema", "clicks_desktop": 26.0, "clicks_mobile": 17.0, "impressions_desktop": 550.0, "impressions_mobile": 350.0, "position_desktop": 6.3, "position_mobile": 6.9},
            {"query": "dram filmleri", "clicks_desktop": 25.0, "clicks_mobile": 17.0, "impressions_desktop": 530.0, "impressions_mobile": 340.0, "position_desktop": 5.4, "position_mobile": 6.0},
            {"query": "gerilim filmleri", "clicks_desktop": 24.0, "clicks_mobile": 16.0, "impressions_desktop": 510.0, "impressions_mobile": 330.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "animasyon filmleri", "clicks_desktop": 22.0, "clicks_mobile": 15.0, "impressions_desktop": 470.0, "impressions_mobile": 310.0, "position_desktop": 5.9, "position_mobile": 6.5},
            # Additional keywords for top 50
            {"query": "sinema seansları", "clicks_desktop": 21.0, "clicks_mobile": 14.0, "impressions_desktop": 450.0, "impressions_mobile": 290.0, "position_desktop": 4.1, "position_mobile": 4.7},
            {"query": "film fragmanı", "clicks_desktop": 20.0, "clicks_mobile": 13.0, "impressions_desktop": 430.0, "impressions_mobile": 280.0, "position_desktop": 4.2, "position_mobile": 4.8},
            {"query": "sinema hakkında", "clicks_desktop": 19.0, "clicks_mobile": 13.0, "impressions_desktop": 410.0, "impressions_mobile": 270.0, "position_desktop": 4.3, "position_mobile": 4.9},
            {"query": "müzik filmleri", "clicks_desktop": 18.0, "clicks_mobile": 12.0, "impressions_desktop": 390.0, "impressions_mobile": 250.0, "position_desktop": 4.4, "position_mobile": 5.0},
            {"query": "biyografi filmleri", "clicks_desktop": 17.0, "clicks_mobile": 11.0, "impressions_desktop": 370.0, "impressions_mobile": 240.0, "position_desktop": 4.5, "position_mobile": 5.1},
            # Continue to reach 50
            {"query": "western filmleri", "clicks_desktop": 16.0, "clicks_mobile": 11.0, "impressions_desktop": 350.0, "impressions_mobile": 230.0, "position_desktop": 5.5, "position_mobile": 6.1},
            {"query": "polisiye filmleri", "clicks_desktop": 15.0, "clicks_mobile": 10.0, "impressions_desktop": 330.0, "impressions_mobile": 220.0, "position_desktop": 5.6, "position_mobile": 6.2},
            {"query": "bilim kurgu dizisi", "clicks_desktop": 14.0, "clicks_mobile": 9.0, "impressions_desktop": 310.0, "impressions_mobile": 210.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "filmler izle", "clicks_desktop": 13.0, "clicks_mobile": 9.0, "impressions_desktop": 290.0, "impressions_mobile": 200.0, "position_desktop": 5.8, "position_mobile": 6.4},
            {"query": "sinemanın tarihi", "clicks_desktop": 12.0, "clicks_mobile": 8.0, "impressions_desktop": 270.0, "impressions_mobile": 190.0, "position_desktop": 5.9, "position_mobile": 6.5},
            {"query": "kısa filmler", "clicks_desktop": 11.0, "clicks_mobile": 8.0, "impressions_desktop": 250.0, "impressions_mobile": 180.0, "position_desktop": 6.0, "position_mobile": 6.6},
            {"query": "belgesel filmler", "clicks_desktop": 10.0, "clicks_mobile": 7.0, "impressions_desktop": 230.0, "impressions_mobile": 170.0, "position_desktop": 6.1, "position_mobile": 6.7},
            {"query": "fantezi filmleri", "clicks_desktop": 9.0, "clicks_mobile": 6.0, "impressions_desktop": 210.0, "impressions_mobile": 160.0, "position_desktop": 6.2, "position_mobile": 6.8},
            {"query": "macera filmleri", "clicks_desktop": 8.0, "clicks_mobile": 5.0, "impressions_desktop": 190.0, "impressions_mobile": 150.0, "position_desktop": 6.3, "position_mobile": 6.9},
            {"query": "gizem filmleri", "clicks_desktop": 7.0, "clicks_mobile": 5.0, "impressions_desktop": 170.0, "impressions_mobile": 140.0, "position_desktop": 6.4, "position_mobile": 7.0},
            {"query": "tarih filmleri", "clicks_desktop": 6.0, "clicks_mobile": 4.0, "impressions_desktop": 150.0, "impressions_mobile": 130.0, "position_desktop": 6.5, "position_mobile": 7.1},
        ],
        "doviz": [
            # Top tier (80-120 clicks)
            {"query": "doviz kuru", "clicks_desktop": 120.0, "clicks_mobile": 80.0, "impressions_desktop": 2500.0, "impressions_mobile": 1600.0, "position_desktop": 3.0, "position_mobile": 3.5},
            {"query": "altin fiyatlari", "clicks_desktop": 95.0, "clicks_mobile": 63.0, "impressions_desktop": 2000.0, "impressions_mobile": 1300.0, "position_desktop": 4.5, "position_mobile": 5.1},
            {"query": "bitcoin fiyati", "clicks_desktop": 88.0, "clicks_mobile": 58.0, "impressions_desktop": 1850.0, "impressions_mobile": 1250.0, "position_desktop": 3.3, "position_mobile": 3.9},
            # High tier (50-80 clicks)
            {"query": "dolar ne kadar", "clicks_desktop": 78.0, "clicks_mobile": 52.0, "impressions_desktop": 1650.0, "impressions_mobile": 1100.0, "position_desktop": 7.6, "position_mobile": 8.2},
            {"query": "euro kuru", "clicks_desktop": 70.0, "clicks_mobile": 46.0, "impressions_desktop": 1500.0, "impressions_mobile": 1000.0, "position_desktop": 4.0, "position_mobile": 4.6},
            {"query": "petrol fiyati", "clicks_desktop": 68.0, "clicks_mobile": 45.0, "impressions_desktop": 1450.0, "impressions_mobile": 950.0, "position_desktop": 3.9, "position_mobile": 4.5},
            {"query": "kripto para", "clicks_desktop": 65.0, "clicks_mobile": 43.0, "impressions_desktop": 1400.0, "impressions_mobile": 900.0, "position_desktop": 4.1, "position_mobile": 4.7},
            {"query": "borsa istanbul", "clicks_desktop": 62.0, "clicks_mobile": 41.0, "impressions_desktop": 1320.0, "impressions_mobile": 880.0, "position_desktop": 4.9, "position_mobile": 5.5},
            {"query": "forex trading", "clicks_desktop": 60.0, "clicks_mobile": 40.0, "impressions_desktop": 1280.0, "impressions_mobile": 850.0, "position_desktop": 4.6, "position_mobile": 5.2},
            {"query": "merkez bankasi", "clicks_desktop": 55.0, "clicks_mobile": 36.0, "impressions_desktop": 1170.0, "impressions_mobile": 780.0, "position_desktop": 5.6, "position_mobile": 6.2},
            # Medium tier (30-50 clicks)
            {"query": "piyasa analizi", "clicks_desktop": 52.0, "clicks_mobile": 34.0, "impressions_desktop": 1100.0, "impressions_mobile": 730.0, "position_desktop": 4.3, "position_mobile": 4.9},
            {"query": "gumruk vergileri", "clicks_desktop": 48.0, "clicks_mobile": 32.0, "impressions_desktop": 1020.0, "impressions_mobile": 680.0, "position_desktop": 6.0, "position_mobile": 6.5},
            {"query": "hazine bonosu", "clicks_desktop": 45.0, "clicks_mobile": 30.0, "impressions_desktop": 950.0, "impressions_mobile": 630.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "yatirim stratejisi", "clicks_desktop": 42.0, "clicks_mobile": 28.0, "impressions_desktop": 890.0, "impressions_mobile": 590.0, "position_desktop": 5.3, "position_mobile": 5.9},
            {"query": "emtia fiyatlari", "clicks_desktop": 40.0, "clicks_mobile": 26.0, "impressions_desktop": 850.0, "impressions_mobile": 560.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "piyasa haluketi", "clicks_desktop": 38.0, "clicks_mobile": 25.0, "impressions_desktop": 810.0, "impressions_mobile": 540.0, "position_desktop": 4.8, "position_mobile": 5.4},
            # Continue to reach 50
            {"query": "gumus fiyati", "clicks_desktop": 35.0, "clicks_mobile": 23.0, "impressions_desktop": 750.0, "impressions_mobile": 500.0, "position_desktop": 5.0, "position_mobile": 5.6},
            {"query": "bakir fiyati", "clicks_desktop": 32.0, "clicks_mobile": 21.0, "impressions_desktop": 690.0, "impressions_mobile": 460.0, "position_desktop": 5.2, "position_mobile": 5.8},
            {"query": "bist 100", "clicks_desktop": 30.0, "clicks_mobile": 20.0, "impressions_desktop": 650.0, "impressions_mobile": 430.0, "position_desktop": 4.5, "position_mobile": 5.1},
            {"query": "dolar tl", "clicks_desktop": 28.0, "clicks_mobile": 18.0, "impressions_desktop": 610.0, "impressions_mobile": 410.0, "position_desktop": 5.8, "position_mobile": 6.4},
            {"query": "ruble kuru", "clicks_desktop": 26.0, "clicks_mobile": 17.0, "impressions_desktop": 570.0, "impressions_mobile": 390.0, "position_desktop": 6.0, "position_mobile": 6.6},
            {"query": "ethereum fiyati", "clicks_desktop": 24.0, "clicks_mobile": 16.0, "impressions_desktop": 530.0, "impressions_mobile": 370.0, "position_desktop": 4.2, "position_mobile": 4.8},
            {"query": "ltc fiyati", "clicks_desktop": 22.0, "clicks_mobile": 14.0, "impressions_desktop": 490.0, "impressions_mobile": 350.0, "position_desktop": 4.4, "position_mobile": 5.0},
            {"query": "ripple fiyati", "clicks_desktop": 20.0, "clicks_mobile": 13.0, "impressions_desktop": 450.0, "impressions_mobile": 330.0, "position_desktop": 4.6, "position_mobile": 5.2},
            {"query": "bnb fiyati", "clicks_desktop": 18.0, "clicks_mobile": 12.0, "impressions_desktop": 410.0, "impressions_mobile": 310.0, "position_desktop": 4.8, "position_mobile": 5.4},
            {"query": "ada fiyati", "clicks_desktop": 16.0, "clicks_mobile": 11.0, "impressions_desktop": 370.0, "impressions_mobile": 290.0, "position_desktop": 5.0, "position_mobile": 5.6},
            {"query": "doge fiyati", "clicks_desktop": 14.0, "clicks_mobile": 9.0, "impressions_desktop": 330.0, "impressions_mobile": 270.0, "position_desktop": 5.2, "position_mobile": 5.8},
            {"query": "polkadot fiyati", "clicks_desktop": 12.0, "clicks_mobile": 8.0, "impressions_desktop": 290.0, "impressions_mobile": 250.0, "position_desktop": 5.4, "position_mobile": 6.0},
            {"query": "solana fiyati", "clicks_desktop": 10.0, "clicks_mobile": 7.0, "impressions_desktop": 250.0, "impressions_mobile": 230.0, "position_desktop": 5.6, "position_mobile": 6.2},
            {"query": "xrp fiyati", "clicks_desktop": 8.0, "clicks_mobile": 5.0, "impressions_desktop": 210.0, "impressions_mobile": 210.0, "position_desktop": 5.8, "position_mobile": 6.4},
        ]
    }
    
    # Domain'e ait queryleri bul, yoksa varsayılan (doviz) seçimini yap
    base_queries = query_sets.get(domain_key, query_sets.get("doviz", []))
    
    # Eğer hiç eşleşme yoksa, generic query'ler oluştur
    if not base_queries:
        base_queries = query_sets["doviz"]
    
    return base_queries


def _mock_search_console_response(domain: str = "") -> dict:
    """
    Mock Search Console yanıtı - web ve mobile ayrılımı ile.
    Realistic position, impression, CTR drops'ı simüle et.
    """
    base_queries = _get_mock_queries_for_domain(domain)
    
    # Desktop ve mobile verilerini özellikle oluştur
    current_queries = []
    for q in base_queries:
        # Desktop row
        current_queries.append({
            "keys": [q["query"]],
            "clicks": q["clicks_desktop"],
            "impressions": q["impressions_desktop"],
            "ctr": (q["clicks_desktop"] / q["impressions_desktop"]) if q["impressions_desktop"] > 0 else 0,
            "position": q["position_desktop"],
            "device": "DESKTOP"
        })
        # Mobile row
        current_queries.append({
            "keys": [q["query"]],
            "clicks": q["clicks_mobile"],
            "impressions": q["impressions_mobile"],
            "ctr": (q["clicks_mobile"] / q["impressions_mobile"]) if q["impressions_mobile"] > 0 else 0,
            "position": q["position_mobile"],
            "device": "MOBILE"
        })
    
    # Dünkü pozisyonlar - realistic drops'ları simüle et
    # Top keywords'ler için position drops, impression drops
    position_deltas = {}
    impression_changes = {}  # multiplier (0.85 = 15% drop)
    
    for idx in range(0, len(current_queries), 2):
        query_idx = idx // 2
        
        # Top 10 keywords'ler için significant drops simüle et (position WORSENS = increases)
        if query_idx < 10:
            # Position: Position gets WORSE (higher number)
            # previous = current - delta, so negative delta means previous was lower (better)
            # This means current is worse than previous
            desktop_pos_delta = -(0.8 + (query_idx % 3) * 0.3)  # -0.8 to -1.4 (position got worse)
            mobile_pos_delta = -(0.5 + (query_idx % 3) * 0.25)  # -0.5 to -1.0
            
            # Impressions: 10-20% drop
            impression_mult = 0.85 - (query_idx % 4) * 0.02
        else:
            # Diğer keywords'ler için minimal changes
            desktop_pos_delta = -0.15  # Slight worsening
            mobile_pos_delta = -0.1
            impression_mult = 0.95
        
        position_deltas[idx] = desktop_pos_delta
        position_deltas[idx + 1] = mobile_pos_delta
        impression_changes[idx] = impression_mult
        impression_changes[idx + 1] = impression_mult
    
    previous_queries = []
    for idx, row in enumerate(current_queries):
        delta = position_deltas.get(idx, -0.15)
        impression_mult = impression_changes.get(idx, 0.95)
        
        # subtraction: previous = current - delta
        # if delta is negative, previous = current - (-value) = current + value (higher, worse)
        # This correctly shows previous was better (lower number)
        prev_row = {
            "keys": row["keys"],
            "position": float(row.get("position", 0)) - delta,
            "impressions": row.get("impressions", 0) / impression_mult,
            "ctr": row.get("ctr", 0) / impression_mult,  # CTR goes down with impressions
            "device": row["device"]
        }
        previous_queries.append(prev_row)
    
    return {
        "rows": current_queries,
        "previous_day": previous_queries,
    }


def _normalize_search_console_rows(rows: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    for row in rows or []:
        keys = row.get("keys") or []
        query = str(keys[0] if len(keys) > 0 else row.get("query") or "")
        device = str(keys[1] if len(keys) > 1 else row.get("device") or "ALL").upper()
        clicks = float(row.get("clicks") or 0.0)
        impressions = float(row.get("impressions") or 0.0)
        ctr = float(row.get("ctr") or 0.0)
        if impressions > 0 and ctr <= 0 and clicks > 0:
            ctr = clicks / impressions
        normalized.append(
            {
                "query": query,
                "device": device,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": ctr,
                "position": float(row.get("position") or 0.0),
            }
        )
    return normalized


def _fetch_search_console_rows(service, site_url: str, start_date: date, end_date: date) -> list[dict]:
    page_size = max(100, min(int(settings.search_console_row_batch_size), int(settings.search_console_max_rows)))
    max_rows = max(page_size, int(settings.search_console_max_rows))
    all_rows: list[dict] = []
    start_row = 0

    while start_row < max_rows:
        response = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": ["query", "device"],
                    "rowLimit": page_size,
                    "startRow": start_row,
                },
            )
            .execute()
        )
        rows = response.get("rows", []) or []
        normalized = _normalize_search_console_rows(rows)
        all_rows.extend(normalized)
        if len(rows) < page_size:
            break
        start_row += page_size

    return all_rows[:max_rows]


def _load_search_console_data(site: Site, credential: SiteCredential | None) -> dict:
    # Credential varsa Search Console API cevabı üretir, yoksa bos/failure doner.
    if credential is None:
        return {"rows": [], "previous_day": [], "source": "failed", "error": "Search Console baglantisi yok."}

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return {"rows": [], "previous_day": [], "source": "failed", "error": "Google Search Console istemcisi yuklu degil."}

    try:
        credential_data = load_google_credentials(credential)
        if credential.credential_type == "search_console_oauth":
            credentials = credential_data
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(GoogleAuthRequest())
        else:
            credentials = service_account.Credentials.from_service_account_info(
                credential_data,
                scopes=SEARCH_CONSOLE_SCOPES,
            )
        service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
        site_url = _resolve_search_console_property(service, site)
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=27)
        previous_date = end_date - timedelta(days=1)

        current_rows = _fetch_search_console_rows(service, site_url, start_date, end_date)
        previous_rows = _fetch_search_console_rows(service, site_url, previous_date, previous_date)
        return {
            "rows": current_rows,
            "previous_day": previous_rows,
            "source": "live",
            "error": None,
            "site_url": site_url,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "previous_date": previous_date.isoformat(),
        }
    except Exception as exc:
        LOGGER.warning("Search Console failed for %s due to credential/API error: %s", site.domain, exc)
        return {"rows": [], "previous_day": [], "source": "failed", "error": str(exc)}


def collect_search_console_metrics(db: Session, site: Site) -> dict:
    """Son 28 gün query/ranking özetini çıkarır ve veritabanına kaydeder."""
    decision = consume_api_quota(db, site, provider="search_console", units=2)
    if not decision.allowed:
        return {
            "site_id": site.id,
            "rows": [],
            "blocked": True,
            "reason": decision.reason,
            "summary": {},
        }

    credential = get_search_console_credentials_record(db, site.id)
    collected_at = datetime.utcnow()
    collector_run = start_collector_run(
        db,
        site_id=site.id,
        provider="search_console",
        strategy="all",
        target_url=site.domain,
        requested_at=collected_at,
    )
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])
    previous_rows = payload.get("previous_day", [])
    source = payload.get("source", "failed")
    error = payload.get("error")
    site_url = payload.get("site_url", "")

    if source != "live":
        finish_collector_run(
            db,
            collector_run,
            status="failed",
            finished_at=datetime.utcnow(),
            error_message=str(error or "Search Console canli veri alinamadi."),
            summary={"source": source},
            row_count=0,
        )
        return {
            "site_id": site.id,
            "rows": [],
            "summary": {},
            "source": source,
            "error": error,
        }
    previous_map = {
        (str(row.get("query") or ""), str(row.get("device") or "ALL").upper()): float(row.get("position", 0))
        for row in previous_rows
    }

    total_clicks = sum(float(row.get("clicks", 0)) for row in rows)
    total_impressions = sum(float(row.get("impressions", 0)) for row in rows)
    avg_ctr = (total_clicks / total_impressions * 100.0) if total_impressions > 0 else 0.0
    avg_position = sum(float(row.get("position", 0)) for row in rows) / len(rows) if rows else 0.0
    dropped_queries = 0
    max_drop = 0.0
    for row in rows:
        query = str(row.get("query") or "")
        device = str(row.get("device") or "ALL").upper()
        current_position = float(row.get("position", 0))
        previous_position = previous_map.get((query, device))
        if previous_position is None:
            continue
        drop = current_position - previous_position
        if drop > 0.5:
            dropped_queries += 1
            max_drop = max(max_drop, drop)

    metrics = {
        "search_console_clicks_28d": total_clicks,
        "search_console_impressions_28d": total_impressions,
        "search_console_avg_ctr_28d": avg_ctr,
        "search_console_avg_position_28d": avg_position,
        "search_console_dropped_queries": float(dropped_queries),
        "search_console_biggest_drop": max_drop,
    }
    save_metrics(db, site.id, metrics, collected_at)
    current_row_count = save_search_console_query_rows(
        db,
        site_id=site.id,
        property_url=site_url,
        data_scope="current_28d",
        rows=rows,
        collected_at=collected_at,
        start_date=str(payload.get("start_date") or ""),
        end_date=str(payload.get("end_date") or ""),
        collector_run_id=collector_run.id,
    )
    previous_row_count = save_search_console_query_rows(
        db,
        site_id=site.id,
        property_url=site_url,
        data_scope="previous_day",
        rows=previous_rows,
        collected_at=collected_at,
        start_date=str(payload.get("previous_date") or ""),
        end_date=str(payload.get("previous_date") or ""),
        collector_run_id=collector_run.id,
    )
    finish_collector_run(
        db,
        collector_run,
        status="success",
        finished_at=collected_at,
        summary={
            "source": "live",
            "property_url": site_url,
            "current_rows": current_row_count,
            "previous_rows": previous_row_count,
        },
        row_count=current_row_count + previous_row_count,
    )
    evaluate_site_alerts(db, site)
    return {"site_id": site.id, "rows": rows, "summary": metrics, "source": "live", "error": None}


def get_top_queries(db: Session, site: Site, limit: int = 10, device: str = "all") -> list[dict]:
    """Site detay ekranı için en iyi sorgu satırlarını döndürür - Device segmentasyonu ile.
    
    Args:
        db: Database session
        site: Site object
        limit: Unique query count (not row count). Each query returns 2 rows (DESKTOP + MOBILE).
        device: Filter by device - "all", "DESKTOP", or "MOBILE"
    
    Returns:
        List of query rows. If device is "all", includes both DESKTOP and MOBILE.
        If device is "DESKTOP" or "MOBILE", includes only that device.
    """
    rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="current_28d")
    previous_day = get_latest_search_console_rows(db, site_id=site.id, data_scope="previous_day")
    if not rows:
        credential = get_search_console_credentials_record(db, site.id)
        payload = _load_search_console_data(site, credential)
        rows = payload.get("rows", [])
        previous_day = payload.get("previous_day", [])
    
    # Device-specific previous map: (query, device) -> position
    previous_map = {
        (str(row.get("query") or row.get("keys", [""])[0]), str(row.get("device", "DESKTOP")).upper()): float(row.get("position", 0))
        for row in previous_day
    }
    
    # Normalize device parameter
    device = (device or "all").upper().strip()
    if device not in ["ALL", "DESKTOP", "MOBILE"]:
        device = "ALL"
    
    # Group rows by query name to get unique queries
    queries_dict = {}
    for row in rows:
        query_name = str(row.get("query") or row.get("keys", [""])[0])
        row_device = (row.get("device", "DESKTOP") or "DESKTOP").upper().strip()
        
        if query_name not in queries_dict:
            queries_dict[query_name] = {}
        
        current_position = float(row.get("position", 0))
        previous_position = previous_map.get((query_name, row_device), current_position)
        delta = current_position - previous_position
        
        queries_dict[query_name][row_device] = {
            "query": query_name,
            "clicks": float(row.get("clicks", 0)),
            "impressions": float(row.get("impressions", 0)),
            "ctr": float(row.get("ctr", 0)) * 100.0 if "ctr" in row else (
                (float(row.get("clicks", 0)) / float(row.get("impressions", 0)) * 100.0)
                if float(row.get("impressions", 0)) > 0
                else 0.0
            ),
            "position": current_position,
            "previous_position": previous_position,
            "delta": delta,
            "device": row_device,
        }
    
    # Flatten back to list, limiting to unique query count and applying device filter
    result = []
    query_count = 0
    for query_name in queries_dict:
        if query_count >= limit:
            break
        
        # Filter by device
        if device == "ALL":
            # Include both DESKTOP and MOBILE rows for this query (if both exist)
            for dev in ["DESKTOP", "MOBILE"]:
                if dev in queries_dict[query_name]:
                    result.append(queries_dict[query_name][dev])
        else:
            # Include only the selected device
            if device in queries_dict[query_name]:
                result.append(queries_dict[query_name][device])
        
        query_count += 1
    
    return result
