"""Google Search Console collector'ı."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta

from google.auth.transport.requests import Request as GoogleAuthRequest
from sqlalchemy.orm import Session

from backend.models import Site, SiteCredential
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.search_console_auth import SEARCH_CONSOLE_SCOPES, get_search_console_credentials_record, load_google_credentials
from backend.services.metric_store import save_metrics
from backend.services.quota_guard import consume_api_quota

LOGGER = logging.getLogger(__name__)


def _get_mock_queries_for_domain(domain: str) -> list[dict]:
    """Domain'e göre dinamik test querylerini döndürür. Device ayrılımı ile (desktop/mobile)."""
    # Domain'i normalize et (www'siz, lowercase)
    domain_key = domain.lower().replace("www.", "").split(".")[0]
    
    # Kategori-spesifik queryler
    query_sets = {
        "sinema": [
            {"query": "sinema seans saatleri", "clicks_desktop": 50.0, "clicks_mobile": 35.0, "impressions_desktop": 1200.0, "impressions_mobile": 900.0, "position_desktop": 2.5, "position_mobile": 3.2},
            {"query": "yakındaki sinemalar", "clicks_desktop": 38.0, "clicks_mobile": 24.0, "impressions_desktop": 950.0, "impressions_mobile": 650.0, "position_desktop": 3.2, "position_mobile": 4.1},
            {"query": "film uyarlaması", "clicks_desktop": 28.0, "clicks_mobile": 20.0, "impressions_desktop": 700.0, "impressions_mobile": 500.0, "position_desktop": 4.9, "position_mobile": 5.8},
            {"query": "yeni filmler", "clicks_desktop": 32.0, "clicks_mobile": 23.0, "impressions_desktop": 800.0, "impressions_mobile": 600.0, "position_desktop": 2.9, "position_mobile": 3.5},
            {"query": "sinema bilet fiyatları", "clicks_desktop": 25.0, "clicks_mobile": 17.0, "impressions_desktop": 550.0, "impressions_mobile": 400.0, "position_desktop": 4.0, "position_mobile": 5.0},
            {"query": "çocuk filmleri", "clicks_desktop": 22.0, "clicks_mobile": 16.0, "impressions_desktop": 500.0, "impressions_mobile": 380.0, "position_desktop": 4.6, "position_mobile": 5.2},
            {"query": "korku filmleri", "clicks_desktop": 27.0, "clicks_mobile": 18.0, "impressions_desktop": 650.0, "impressions_mobile": 450.0, "position_desktop": 3.7, "position_mobile": 4.3},
            {"query": "aksiyon filmleri", "clicks_desktop": 31.0, "clicks_mobile": 21.0, "impressions_desktop": 750.0, "impressions_mobile": 500.0, "position_desktop": 3.3, "position_mobile": 3.9},
            {"query": "romantik filmler", "clicks_desktop": 20.0, "clicks_mobile": 15.0, "impressions_desktop": 430.0, "impressions_mobile": 320.0, "position_desktop": 4.9, "position_mobile": 5.5},
            {"query": "komedi filmleri", "clicks_desktop": 24.0, "clicks_mobile": 17.0, "impressions_desktop": 540.0, "impressions_mobile": 380.0, "position_desktop": 4.3, "position_mobile": 4.9},
            {"query": "bilim kurgu filmleri", "clicks_desktop": 23.0, "clicks_mobile": 16.0, "impressions_desktop": 500.0, "impressions_mobile": 350.0, "position_desktop": 4.7, "position_mobile": 5.3},
            {"query": "film izle", "clicks_desktop": 46.0, "clicks_mobile": 32.0, "impressions_desktop": 1200.0, "impressions_mobile": 750.0, "position_desktop": 2.1, "position_mobile": 2.6},
            {"query": "sinema kartı", "clicks_desktop": 16.0, "clicks_mobile": 12.0, "impressions_desktop": 360.0, "impressions_mobile": 260.0, "position_desktop": 5.6, "position_mobile": 6.2},
            {"query": "imax sinema", "clicks_desktop": 13.0, "clicks_mobile": 9.0, "impressions_desktop": 280.0, "impressions_mobile": 200.0, "position_desktop": 6.0, "position_mobile": 6.6},
            {"query": "3d sinema", "clicks_desktop": 11.0, "clicks_mobile": 8.0, "impressions_desktop": 240.0, "impressions_mobile": 180.0, "position_desktop": 6.3, "position_mobile": 6.9},
        ],
        "doviz": [
            {"query": "doviz kuru", "clicks_desktop": 72.0, "clicks_mobile": 48.0, "impressions_desktop": 1500.0, "impressions_mobile": 1000.0, "position_desktop": 3.0, "position_mobile": 3.5},
            {"query": "altin fiyatlari", "clicks_desktop": 48.0, "clicks_mobile": 32.0, "impressions_desktop": 1100.0, "impressions_mobile": 700.0, "position_desktop": 4.5, "position_mobile": 5.1},
            {"query": "dolar ne kadar", "clicks_desktop": 39.0, "clicks_mobile": 26.0, "impressions_desktop": 900.0, "impressions_mobile": 600.0, "position_desktop": 7.6, "position_mobile": 8.2},
            {"query": "euro kuru", "clicks_desktop": 35.0, "clicks_mobile": 23.0, "impressions_desktop": 800.0, "impressions_mobile": 550.0, "position_desktop": 4.0, "position_mobile": 4.6},
            {"query": "bitcoin fiyati", "clicks_desktop": 45.0, "clicks_mobile": 30.0, "impressions_desktop": 1000.0, "impressions_mobile": 700.0, "position_desktop": 3.3, "position_mobile": 3.9},
            {"query": "borsa istanbul", "clicks_desktop": 31.0, "clicks_mobile": 21.0, "impressions_desktop": 720.0, "impressions_mobile": 480.0, "position_desktop": 4.9, "position_mobile": 5.5},
            {"query": "merkez bankasi", "clicks_desktop": 27.0, "clicks_mobile": 18.0, "impressions_desktop": 630.0, "impressions_mobile": 420.0, "position_desktop": 5.6, "position_mobile": 6.2},
            {"query": "gumruk vergileri", "clicks_desktop": 22.0, "clicks_mobile": 16.0, "impressions_desktop": 540.0, "impressions_mobile": 360.0, "position_desktop": 6.0, "position_mobile": 6.5},
            {"query": "hazine bonosu", "clicks_desktop": 19.0, "clicks_mobile": 13.0, "impressions_desktop": 450.0, "impressions_mobile": 300.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "piyasa analizi", "clicks_desktop": 29.0, "clicks_mobile": 19.0, "impressions_desktop": 660.0, "impressions_mobile": 440.0, "position_desktop": 4.3, "position_mobile": 4.9},
            {"query": "kripto para", "clicks_desktop": 37.0, "clicks_mobile": 25.0, "impressions_desktop": 840.0, "impressions_mobile": 560.0, "position_desktop": 4.1, "position_mobile": 4.7},
            {"query": "forex trading", "clicks_desktop": 33.0, "clicks_mobile": 22.0, "impressions_desktop": 750.0, "impressions_mobile": 500.0, "position_desktop": 4.6, "position_mobile": 5.2},
            {"query": "yatirim stratejisi", "clicks_desktop": 25.0, "clicks_mobile": 17.0, "impressions_desktop": 570.0, "impressions_mobile": 380.0, "position_desktop": 5.3, "position_mobile": 5.9},
            {"query": "emtia fiyatlari", "clicks_desktop": 22.0, "clicks_mobile": 15.0, "impressions_desktop": 510.0, "impressions_mobile": 340.0, "position_desktop": 5.7, "position_mobile": 6.3},
            {"query": "petrol fiyati", "clicks_desktop": 41.0, "clicks_mobile": 27.0, "impressions_desktop": 930.0, "impressions_mobile": 620.0, "position_desktop": 3.9, "position_mobile": 4.5},
        ]
    }
    
    # Domain'e ait queryleri bul, yoksa varsayılan (doviz) seçimini yap
    base_queries = query_sets.get(domain_key, query_sets.get("doviz", []))
    
    # Eğer hiç eşleşme yoksa, generic query'ler oluştur (herhangi bir domain için)
    if not base_queries:
        base_queries = query_sets["doviz"]  # Varsayılan olarak doviz
    
    return base_queries


def _mock_search_console_response(domain: str = "") -> dict:
    """
    Mock Search Console yanıtı - web ve mobile ayrılımı ile.
    Her query, desktop ve mobile verisi ile döner.
    Device filterlemesi ve tüm domainler için dinamik.
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
    
    # Dünkü pozisyonlar - dinamik delta değerleri oluştur
    # Her device çifti için farklı delta değerleri
    position_deltas = {}
    for idx in range(0, len(current_queries), 2):
        # Desktop için negatif delta (iyileşme) veya pozitif (kötüleşme)
        desktop_delta = -0.3 - (idx // 6) * 0.1  # Vary by group of 6
        # Mobile için farklı değer
        mobile_delta = 0.2 + (idx // 6) * 0.05
        position_deltas[idx] = desktop_delta
        position_deltas[idx + 1] = mobile_delta
    
    previous_queries = []
    for idx, row in enumerate(current_queries):
        delta = position_deltas.get(idx, -0.3)
        prev_row = {
            "keys": row["keys"],
            "position": float(row.get("position", 0)) - delta,
            "device": row["device"]
        }
        previous_queries.append(prev_row)
    
    return {
        "rows": current_queries,
        "previous_day": previous_queries,
    }


def _load_search_console_data(site: Site, credential: SiteCredential | None) -> dict:
    # Credential yoksa mock, varsa Search Console API cevabı üretir.
    if credential is None:
        return _mock_search_console_response(site.domain)

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return _mock_search_console_response()

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
        site_url = site.domain if site.domain.startswith("http") else f"sc-domain:{site.domain}"
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=27)
        previous_date = end_date - timedelta(days=1)

        current = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": ["query"],
                    "rowLimit": 50,
                },
            )
            .execute()
        )
        previous = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={
                    "startDate": previous_date.isoformat(),
                    "endDate": previous_date.isoformat(),
                    "dimensions": ["query"],
                    "rowLimit": 50,
                },
            )
            .execute()
        )
        return {"rows": current.get("rows", []), "previous_day": previous.get("rows", [])}
    except Exception as exc:
        LOGGER.warning("Search Console fallback to mock for %s due to credential/API error: %s", site.domain, exc)
        return _mock_search_console_response(site.domain)


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
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])
    previous_rows = payload.get("previous_day", [])
    previous_map = {row.get("keys", [""])[0]: float(row.get("position", 0)) for row in previous_rows}

    total_clicks = sum(float(row.get("clicks", 0)) for row in rows)
    total_impressions = sum(float(row.get("impressions", 0)) for row in rows)
    avg_ctr = (total_clicks / total_impressions * 100.0) if total_impressions > 0 else 0.0
    avg_position = sum(float(row.get("position", 0)) for row in rows) / len(rows) if rows else 0.0
    dropped_queries = 0
    max_drop = 0.0
    for row in rows:
        query = row.get("keys", [""])[0]
        current_position = float(row.get("position", 0))
        previous_position = previous_map.get(query)
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
    save_metrics(db, site.id, metrics, datetime.utcnow())
    evaluate_site_alerts(db, site)
    return {"site_id": site.id, "rows": rows, "summary": metrics}


def get_top_queries(db: Session, site: Site, limit: int = 10) -> list[dict]:
    """Site detay ekranı için en iyi sorgu satırlarını döndürür - Device segmentasyonu ile."""
    credential = get_search_console_credentials_record(db, site.id)
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])
    previous_day = payload.get("previous_day", [])
    
    # Device-specific previous map: (query, device) -> position
    previous_map = {
        (row.get("keys", [""])[0], row.get("device", "DESKTOP").upper()): float(row.get("position", 0))
        for row in previous_day
    }
    
    result = []
    for idx, row in enumerate(rows):
        query = row.get("keys", [""])[0]
        device = (row.get("device", "DESKTOP") or "DESKTOP").upper().strip()
        current_position = float(row.get("position", 0))
        previous_position = previous_map.get((query, device), current_position)
        delta = current_position - previous_position
        
        result.append({
            "query": query,
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
            "device": device,
        })
    
    return result