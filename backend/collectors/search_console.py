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
    """Domain'e göre standart test querylerini döndürür. Bu queryler tüm zamanlar aynı kalır."""
    if "sinema" in domain.lower():
        return [
            {"keys": ["sinema seans saatleri"], "clicks": 85.0, "impressions": 2100.0, "ctr": 0.040, "position": 2.8},
            {"keys": ["yakındaki sinemalar"], "clicks": 62.0, "impressions": 1600.0, "ctr": 0.039, "position": 3.5},
            {"keys": ["film uyarlaması"], "clicks": 48.0, "impressions": 1200.0, "ctr": 0.040, "position": 5.2},
            {"keys": ["yeni filmler"], "clicks": 55.0, "impressions": 1400.0, "ctr": 0.039, "position": 3.1},
            {"keys": ["sinema bilet fiyatları"], "clicks": 42.0, "impressions": 950.0, "ctr": 0.044, "position": 4.2},
            {"keys": ["çocuk filmleri"], "clicks": 38.0, "impressions": 880.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["korku filmleri"], "clicks": 45.0, "impressions": 1100.0, "ctr": 0.041, "position": 3.9},
            {"keys": ["aksiyon filmleri"], "clicks": 52.0, "impressions": 1250.0, "ctr": 0.042, "position": 3.5},
            {"keys": ["romantik filmler"], "clicks": 35.0, "impressions": 750.0, "ctr": 0.047, "position": 5.1},
            {"keys": ["komedi filmleri"], "clicks": 41.0, "impressions": 920.0, "ctr": 0.045, "position": 4.5},
            {"keys": ["bilim kurgu filmleri"], "clicks": 39.0, "impressions": 850.0, "ctr": 0.046, "position": 4.9},
            {"keys": ["film izle"], "clicks": 78.0, "impressions": 1950.0, "ctr": 0.040, "position": 2.3},
            {"keys": ["sinema kartı"], "clicks": 28.0, "impressions": 620.0, "ctr": 0.045, "position": 5.8},
            {"keys": ["imax sinema"], "clicks": 22.0, "impressions": 480.0, "ctr": 0.046, "position": 6.2},
            {"keys": ["3d sinema"], "clicks": 19.0, "impressions": 420.0, "ctr": 0.045, "position": 6.5},
        ]
    # Varsayılan doviz.com queryler
    return [
        {"keys": ["doviz kuru"], "clicks": 120.0, "impressions": 2500.0, "ctr": 0.048, "position": 3.2},
        {"keys": ["altin fiyatlari"], "clicks": 80.0, "impressions": 1800.0, "ctr": 0.044, "position": 4.7},
        {"keys": ["dolar ne kadar"], "clicks": 65.0, "impressions": 1500.0, "ctr": 0.043, "position": 7.8},
        {"keys": ["euro kuru"], "clicks": 58.0, "impressions": 1350.0, "ctr": 0.043, "position": 4.2},
        {"keys": ["bitcoin fiyati"], "clicks": 75.0, "impressions": 1700.0, "ctr": 0.044, "position": 3.5},
        {"keys": ["borsa istanbul"], "clicks": 52.0, "impressions": 1200.0, "ctr": 0.043, "position": 5.1},
        {"keys": ["merkez bankasi"], "clicks": 45.0, "impressions": 1050.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["gumruk vergileri"], "clicks": 38.0, "impressions": 900.0, "ctr": 0.042, "position": 6.2},
        {"keys": ["hazine bonosu"], "clicks": 32.0, "impressions": 750.0, "ctr": 0.043, "position": 5.9},
        {"keys": ["piyasa analizi"], "clicks": 48.0, "impressions": 1100.0, "ctr": 0.044, "position": 4.5},
        {"keys": ["kripto para"], "clicks": 62.0, "impressions": 1400.0, "ctr": 0.044, "position": 4.3},
        {"keys": ["forex trading"], "clicks": 55.0, "impressions": 1250.0, "ctr": 0.044, "position": 4.8},
        {"keys": ["yatirim stratejisi"], "clicks": 42.0, "impressions": 950.0, "ctr": 0.044, "position": 5.5},
        {"keys": ["emtia fiyatlari"], "clicks": 37.0, "impressions": 850.0, "ctr": 0.044, "position": 5.9},
        {"keys": ["petrol fiyati"], "clicks": 68.0, "impressions": 1550.0, "ctr": 0.044, "position": 4.1},
    ]


def _mock_search_console_response(domain: str = "") -> dict:
    """
    Mock Search Console yanıtı - bugünün querylerini ve dünün pozisyonlarını döndürür.
    Sistem hangi queryler dönerse, o queryler için dünkü pozisyonları karşılaştırır.
    """
    # Standart queryler (hep aynı)
    current_queries = _get_mock_queries_for_domain(domain)
    
    # Dünkü pozisyonlar - aynı queryler için slightly farklı pozisyonlar (simüle)
    # Her query için position biraz değişiyor (bazı iyileşti, bazı kötüleşti)
    position_deltas = {
        0: -0.3,   # İyileşti
        1: -0.4,   
        2: -0.6,   
        3: +0.3,   # Kötüleşti
        4: -0.3,   
        5: -0.4,   
        6: +0.4,   
        7: -0.3,   
        8: -0.3,   
        9: -0.3,   
        10: -0.2,  
        11: +0.3,  
        12: -0.3,  
        13: -0.3,  
        14: -0.5,  
    }
    
    previous_queries = []
    for idx, row in enumerate(current_queries):
        delta = position_deltas.get(idx, -0.3)
        prev_row = {
            "keys": row["keys"],
            "position": float(row.get("position", 0)) - delta  # Dünkü position
        }
        previous_queries.append(prev_row)
    
    return {
        "rows": current_queries,
        "previous_day": previous_queries,
    }
    
    # Varsayılan doviz.com mock data  - 50 query
    finance_queries = [
        {"keys": ["doviz kuru"], "clicks": 120.0, "impressions": 2500.0, "ctr": 0.048, "position": 3.2},
        {"keys": ["altin fiyatlari"], "clicks": 80.0, "impressions": 1800.0, "ctr": 0.044, "position": 4.7},
        {"keys": ["dolar ne kadar"], "clicks": 65.0, "impressions": 1500.0, "ctr": 0.043, "position": 7.8},
        {"keys": ["euro kuru"], "clicks": 58.0, "impressions": 1350.0, "ctr": 0.043, "position": 4.2},
        {"keys": ["bitcoin fiyati"], "clicks": 75.0, "impressions": 1700.0, "ctr": 0.044, "position": 3.5},
        {"keys": ["borsa istanbul"], "clicks": 52.0, "impressions": 1200.0, "ctr": 0.043, "position": 5.1},
        {"keys": ["merkez bankasi"], "clicks": 45.0, "impressions": 1050.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["gumruk vergileri"], "clicks": 38.0, "impressions": 900.0, "ctr": 0.042, "position": 6.2},
        {"keys": ["hazine bonosu"], "clicks": 32.0, "impressions": 750.0, "ctr": 0.043, "position": 5.9},
        {"keys": ["piyasa analizi"], "clicks": 48.0, "impressions": 1100.0, "ctr": 0.044, "position": 4.5},
        {"keys": ["kripto para"], "clicks": 62.0, "impressions": 1400.0, "ctr": 0.044, "position": 4.3},
        {"keys": ["forex trading"], "clicks": 55.0, "impressions": 1250.0, "ctr": 0.044, "position": 4.8},
        {"keys": ["yatirim stratejisi"], "clicks": 42.0, "impressions": 950.0, "ctr": 0.044, "position": 5.5},
        {"keys": ["emtia fiyatlari"], "clicks": 37.0, "impressions": 850.0, "ctr": 0.044, "position": 5.9},
        {"keys": ["petrol fiyati"], "clicks": 68.0, "impressions": 1550.0, "ctr": 0.044, "position": 4.1},
    ]


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
    """Site detay ekranı için en iyi sorgu satırlarını döndürür."""
    credential = get_search_console_credentials_record(db, site.id)
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])[:limit]
    previous_map = {
        row.get("keys", [""])[0]: float(row.get("position", 0)) for row in payload.get("previous_day", [])
    }
    return [
        {
            "query": row.get("keys", [""])[0],
            "clicks": float(row.get("clicks", 0)),
            "impressions": float(row.get("impressions", 0)),
            "ctr": float(row.get("ctr", 0)) * 100.0 if "ctr" in row else (
                (float(row.get("clicks", 0)) / float(row.get("impressions", 0)) * 100.0)
                if float(row.get("impressions", 0)) > 0
                else 0.0
            ),
            "position": float(row.get("position", 0)),
            "previous_position": previous_map.get(row.get("keys", [""])[0]),
            "delta": float(row.get("position", 0)) - previous_map.get(row.get("keys", [""])[0], float(row.get("position", 0))),
        }
        for row in rows
    ]