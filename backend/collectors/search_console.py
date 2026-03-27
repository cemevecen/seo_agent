"""Google Search Console collector'ı."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session

from backend.models import Site, SiteCredential
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.crypto import decrypt_text
from backend.services.metric_store import save_metrics


def _mock_search_console_response() -> dict:
    # Credential yokken test edilebilir kalsın diye örnek veri döndürür.
    return {
        "rows": [
            {"keys": ["doviz kuru"], "clicks": 120.0, "impressions": 2500.0, "ctr": 0.048, "position": 3.2},
            {"keys": ["altin fiyatlari"], "clicks": 80.0, "impressions": 1800.0, "ctr": 0.044, "position": 4.7},
            {"keys": ["dolar ne kadar"], "clicks": 65.0, "impressions": 1500.0, "ctr": 0.043, "position": 7.8},
        ],
        "previous_day": [
            {"keys": ["doviz kuru"], "position": 2.8},
            {"keys": ["altin fiyatlari"], "position": 3.9},
            {"keys": ["dolar ne kadar"], "position": 5.1},
        ],
    }


def _load_search_console_data(site: Site, credential: SiteCredential | None) -> dict:
    # Credential yoksa mock, varsa Search Console API cevabı üretir.
    if credential is None:
        return _mock_search_console_response()

    service_info = json.loads(decrypt_text(credential.encrypted_data))
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return _mock_search_console_response()

    credentials = service_account.Credentials.from_service_account_info(
        service_info,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
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


def collect_search_console_metrics(db: Session, site: Site) -> dict:
    """Son 28 gün query/ranking özetini çıkarır ve veritabanına kaydeder."""
    credential = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site.id, SiteCredential.credential_type == "search_console")
        .first()
    )
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
    credential = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site.id, SiteCredential.credential_type == "search_console")
        .first()
    )
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