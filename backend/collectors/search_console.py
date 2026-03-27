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


def _mock_search_console_response(domain: str = "") -> dict:
    # Credential yokken test edilebilir kalsın diye örnek veri döndürür. Domain'e göre site-specific sorguları döner.
    if "sinema" in domain.lower():
        cinema_queries = [
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
            {"keys": ["sinema seansları"], "clicks": 65.0, "impressions": 1650.0, "ctr": 0.039, "position": 3.2},
            {"keys": ["film önerileri"], "clicks": 33.0, "impressions": 750.0, "ctr": 0.044, "position": 4.6},
            {"keys": ["en iyi filmler"], "clicks": 47.0, "impressions": 1120.0, "ctr": 0.042, "position": 3.8},
            {"keys": ["oscar kazanan filmler"], "clicks": 25.0, "impressions": 580.0, "ctr": 0.043, "position": 5.4},
            {"keys": ["türk filmleri"], "clicks": 31.0, "impressions": 710.0, "ctr": 0.044, "position": 4.7},
            {"keys": ["hollywood filmleri"], "clicks": 44.0, "impressions": 1050.0, "ctr": 0.042, "position": 4.0},
            {"keys": ["sinema oyuncuları"], "clicks": 20.0, "impressions": 450.0, "ctr": 0.044, "position": 5.9},
            {"keys": ["film yönetmenleri"], "clicks": 17.0, "impressions": 390.0, "ctr": 0.044, "position": 6.1},
            {"keys": ["sinema haberleri"], "clicks": 27.0, "impressions": 620.0, "ctr": 0.044, "position": 5.2},
            {"keys": ["film fragmanları"], "clicks": 36.0, "impressions": 820.0, "ctr": 0.044, "position": 4.4},
            {"keys": ["film rezensyonları"], "clicks": 24.0, "impressions": 560.0, "ctr": 0.043, "position": 5.5},
            {"keys": ["imdb filmler"], "clicks": 19.0, "impressions": 430.0, "ctr": 0.044, "position": 6.0},
            {"keys": ["netflix filmler"], "clicks": 58.0, "impressions": 1400.0, "ctr": 0.041, "position": 3.3},
            {"keys": ["amazon prime filmler"], "clicks": 32.0, "impressions": 740.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["online film izle"], "clicks": 51.0, "impressions": 1200.0, "ctr": 0.042, "position": 3.6},
            {"keys": ["sinema biletiyle"], "clicks": 15.0, "impressions": 350.0, "ctr": 0.043, "position": 6.3},
            {"keys": ["film talepleri"], "clicks": 12.0, "impressions": 280.0, "ctr": 0.043, "position": 6.5},
            {"keys": ["sinema promosyonları"], "clicks": 18.0, "impressions": 410.0, "ctr": 0.044, "position": 5.9},
            {"keys": ["film tahlili"], "clicks": 21.0, "impressions": 480.0, "ctr": 0.044, "position": 5.8},
            {"keys": ["sinema deneyimi"], "clicks": 14.0, "impressions": 320.0, "ctr": 0.044, "position": 6.2},
            {"keys": ["film kategorileri"], "clicks": 26.0, "impressions": 600.0, "ctr": 0.043, "position": 5.3},
            {"keys": ["sinema stillleri"], "clicks": 11.0, "impressions": 250.0, "ctr": 0.044, "position": 6.6},
            {"keys": ["animasyon filmleri"], "clicks": 46.0, "impressions": 1080.0, "ctr": 0.043, "position": 3.9},
            {"keys": ["belgesel filmler"], "clicks": 23.0, "impressions": 530.0, "ctr": 0.043, "position": 5.6},
            {"keys": ["müzikli filmler"], "clicks": 29.0, "impressions": 670.0, "ctr": 0.043, "position": 4.9},
            {"keys": ["drama filmleri"], "clicks": 34.0, "impressions": 780.0, "ctr": 0.044, "position": 4.5},
            {"keys": ["gerilim filmleri"], "clicks": 37.0, "impressions": 850.0, "ctr": 0.044, "position": 4.3},
            {"keys": ["aile filmleri"], "clicks": 30.0, "impressions": 690.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["macera filmleri"], "clicks": 40.0, "impressions": 920.0, "ctr": 0.043, "position": 4.2},
            {"keys": ["superkahaman filmleri"], "clicks": 53.0, "impressions": 1300.0, "ctr": 0.041, "position": 3.4},
            {"keys": ["fantazi filmleri"], "clicks": 43.0, "impressions": 1000.0, "ctr": 0.043, "position": 4.1},
            {"keys": ["tarihsel filmler"], "clicks": 16.0, "impressions": 370.0, "ctr": 0.043, "position": 6.1},
            {"keys": ["psikiyatrik filmler"], "clicks": 13.0, "impressions": 300.0, "ctr": 0.043, "position": 6.4},
            {"keys": ["suç filmleri"], "clicks": 50.0, "impressions": 1180.0, "ctr": 0.042, "position": 3.7},
            {"keys": ["bilim kurgu klasikleri"], "clicks": 9.0, "impressions": 210.0, "ctr": 0.043, "position": 6.7},
        ]
        return {
            "rows": cinema_queries,
            "previous_day": [{"keys": q["keys"], "position": q["position"] - 0.4} for q in cinema_queries[:50]],
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
        {"keys": ["donem sonu muhasebesi"], "clicks": 28.0, "impressions": 650.0, "ctr": 0.043, "position": 6.1},
        {"keys": ["vergi orani"], "clicks": 51.0, "impressions": 1180.0, "ctr": 0.043, "position": 5.2},
        {"keys": ["faiz oranlari"], "clicks": 64.0, "impressions": 1480.0, "ctr": 0.043, "position": 4.0},
        {"keys": ["ekonomik rapor"], "clicks": 33.0, "impressions": 760.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["merkez bankasi karar"], "clicks": 47.0, "impressions": 1080.0, "ctr": 0.043, "position": 4.6},
        {"keys": ["enflasyon orani"], "clicks": 59.0, "impressions": 1350.0, "ctr": 0.044, "position": 4.2},
        {"keys": ["isizlik rakameri"], "clicks": 35.0, "impressions": 810.0, "ctr": 0.043, "position": 5.7},
        {"keys": ["gdp orani"], "clicks": 29.0, "impressions": 670.0, "ctr": 0.043, "position": 5.9},
        {"keys": ["dex index"], "clicks": 53.0, "impressions": 1210.0, "ctr": 0.044, "position": 4.7},
        {"keys": ["pay alimi"], "clicks": 44.0, "impressions": 1010.0, "ctr": 0.044, "position": 4.9},
        {"keys": ["sahibi olma"], "clicks": 39.0, "impressions": 900.0, "ctr": 0.043, "position": 5.4},
        {"keys": ["gayrimenkul yatirimi"], "clicks": 41.0, "impressions": 950.0, "ctr": 0.043, "position": 5.3},
        {"keys": ["emeklilik fonu"], "clicks": 36.0, "impressions": 830.0, "ctr": 0.043, "position": 5.6},
        {"keys": ["banka faizi"], "clicks": 54.0, "impressions": 1240.0, "ctr": 0.044, "position": 4.6},
        {"keys": ["kredi kartı orani"], "clicks": 46.0, "impressions": 1060.0, "ctr": 0.043, "position": 5.0},
        {"keys": ["ipotek kredisi"], "clicks": 40.0, "impressions": 920.0, "ctr": 0.043, "position": 5.2},
        {"keys": ["sigortai plani"], "clicks": 26.0, "impressions": 600.0, "ctr": 0.043, "position": 6.2},
        {"keys": ["investisyon fonu"], "clicks": 31.0, "impressions": 710.0, "ctr": 0.044, "position": 5.8},
        {"keys": ["dijital para"], "clicks": 60.0, "impressions": 1380.0, "ctr": 0.044, "position": 4.1},
        {"keys": ["blokchain teknolojisi"], "clicks": 34.0, "impressions": 780.0, "ctr": 0.044, "position": 5.7},
        {"keys": ["akaryakit fiyati"], "clicks": 57.0, "impressions": 1300.0, "ctr": 0.044, "position": 4.3},
        {"keys": ["doviztl artis"], "clicks": 25.0, "impressions": 580.0, "ctr": 0.043, "position": 6.3},
        {"keys": ["doviz degerleri"], "clicks": 70.0, "impressions": 1600.0, "ctr": 0.044, "position": 3.9},
        {"keys": ["kac tl"], "clicks": 72.0, "impressions": 1650.0, "ctr": 0.044, "position": 3.8},
        {"keys": ["dis ticaret"], "clicks": 27.0, "impressions": 620.0, "ctr": 0.044, "position": 6.0},
        {"keys": ["gumruk uzlastirmasi"], "clicks": 21.0, "impressions": 480.0, "ctr": 0.044, "position": 6.4},
        {"keys": ["uluslararasi ticaret"], "clicks": 23.0, "impressions": 530.0, "ctr": 0.043, "position": 6.1},
        {"keys": ["yatirim tesvikleri"], "clicks": 19.0, "impressions": 440.0, "ctr": 0.043, "position": 6.5},
        {"keys": ["ihracat destegi"], "clicks": 22.0, "impressions": 500.0, "ctr": 0.044, "position": 6.2},
        {"keys": ["maliye bakanligi"], "clicks": 30.0, "impressions": 690.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["vergi dairesi"], "clicks": 24.0, "impressions": 550.0, "ctr": 0.044, "position": 6.1},
        {"keys": ["gumruk müdürlüğü"], "clicks": 20.0, "impressions": 460.0, "ctr": 0.043, "position": 6.3},
        {"keys": ["enerji ticareti"], "clicks": 49.0, "impressions": 1120.0, "ctr": 0.044, "position": 5.1},
        {"keys": ["elektrik fiyati"], "clicks": 63.0, "impressions": 1450.0, "ctr": 0.043, "position": 4.0},
        {"keys": ["dogalgaz fiyati"], "clicks": 61.0, "impressions": 1400.0, "ctr": 0.044, "position": 4.1},
    ]
    return {
        "rows": finance_queries,
        "previous_day": [{"keys": q["keys"], "position": q["position"] - 0.3} for q in finance_queries[:50]],
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