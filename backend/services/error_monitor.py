"""
Site hata izleme — GA4, Search Console ve sunucu loglarından 404/500 toplar.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _ga4_run_report(credentials_json: str, property_id: str, body: dict) -> dict:
    """GA4 Analytics Data API v1beta runReport çağrısı."""
    import google.oauth2.service_account as _svc
    from googleapiclient.discovery import build as _build
    import json as _json

    creds_dict = _json.loads(credentials_json)
    creds = _svc.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    svc = _build("analyticsdata", "v1beta", credentials=creds, cache_discovery=False)
    return (
        svc.properties()
        .runReport(property=f"properties/{property_id}", body=body)
        .execute()
    )


def fetch_ga4_error_pages(
    credentials_json: str,
    property_id: str,
    days: int = 7,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    GA4'ten hata sayfalarını çeker.
    404 sayfaları: pagePath'te '404' geçenler VEYA pageTitle'da hata kelimeleri.
    """
    start = f"{days}daysAgo"

    body = {
        "dateRanges": [{"startDate": start, "endDate": "today"}],
        "dimensions": [
            {"name": "pagePath"},
            {"name": "pageTitle"},
            {"name": "date"},
        ],
        "metrics": [
            {"name": "screenPageViews"},
            {"name": "totalUsers"},
        ],
        "dimensionFilter": {
            "orGroup": {
                "expressions": [
                    {"filter": {"fieldName": "pagePath",  "stringFilter": {"matchType": "CONTAINS", "value": "/404",         "caseSensitive": False}}},
                    {"filter": {"fieldName": "pagePath",  "stringFilter": {"matchType": "CONTAINS", "value": "404",          "caseSensitive": False}}},
                    {"filter": {"fieldName": "pageTitle", "stringFilter": {"matchType": "CONTAINS", "value": "404",          "caseSensitive": False}}},
                    {"filter": {"fieldName": "pageTitle", "stringFilter": {"matchType": "CONTAINS", "value": "bulunamadı",   "caseSensitive": False}}},
                    {"filter": {"fieldName": "pageTitle", "stringFilter": {"matchType": "CONTAINS", "value": "not found",    "caseSensitive": False}}},
                    {"filter": {"fieldName": "pageTitle", "stringFilter": {"matchType": "CONTAINS", "value": "sayfa yok",    "caseSensitive": False}}},
                ],
            }
        },
        "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
        "limit": limit,
    }

    try:
        resp = _ga4_run_report(credentials_json, property_id, body)
    except Exception as exc:
        logger.warning("GA4 error pages fetch hatası [property=%s]: %s", property_id, exc)
        return []

    rows = resp.get("rows") or []
    dim_headers = [h["name"] for h in (resp.get("dimensionHeaders") or [])]
    met_headers = [h["name"] for h in (resp.get("metricHeaders") or [])]

    results = []
    for row in rows:
        dims = {dim_headers[i]: v["value"] for i, v in enumerate(row.get("dimensionValues", []))}
        mets = {met_headers[i]: v["value"] for i, v in enumerate(row.get("metricValues", []))}
        results.append({
            "url":         dims.get("pagePath", ""),
            "page_title":  dims.get("pageTitle", ""),
            "date":        dims.get("date", ""),
            "pageviews":   int(mets.get("screenPageViews", 0)),
            "users":       int(mets.get("totalUsers", 0)),
            "status_code": 404,
            "source":      "ga4",
            "error_type":  "not_found",
        })
    return results


def save_error_logs(
    db: Session,
    site_id: int,
    errors: list[dict[str, Any]],
    source: str,
) -> int:
    """Hataları DB'ye upsert eder. Aynı URL+source varsa hit_count ve last_seen günceller."""
    from backend.models import SiteErrorLog

    saved = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for e in errors:
        url = (e.get("url") or "")[:2048]
        if not url:
            continue
        status_code = int(e.get("status_code", 404))

        existing = (
            db.query(SiteErrorLog)
            .filter(
                SiteErrorLog.site_id == site_id,
                SiteErrorLog.url == url,
                SiteErrorLog.source == source,
                SiteErrorLog.status_code == status_code,
            )
            .first()
        )
        if existing:
            existing.hit_count = max(existing.hit_count, int(e.get("users", 1)))
            existing.last_seen = now
            extra = {"page_title": e.get("page_title", "")}
            existing.extra_json = json.dumps(extra, ensure_ascii=False)
        else:
            row = SiteErrorLog(
                site_id=site_id,
                url=url,
                status_code=status_code,
                source=source,
                error_type=e.get("error_type", "not_found"),
                hit_count=int(e.get("users", 1)),
                first_seen=now,
                last_seen=now,
                extra_json=json.dumps({"page_title": e.get("page_title", "")}, ensure_ascii=False),
            )
            db.add(row)
        saved += 1

    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("SiteErrorLog kayıt hatası")
        return 0

    return saved


def get_error_summary(
    db: Session,
    site_id: int,
    days: int = 7,
) -> dict[str, Any]:
    """Son N günün hata özetini döner."""
    from backend.models import SiteErrorLog
    from sqlalchemy import func

    cutoff = datetime.utcnow() - timedelta(days=days)

    rows = (
        db.query(SiteErrorLog)
        .filter(
            SiteErrorLog.site_id == site_id,
            SiteErrorLog.last_seen >= cutoff,
        )
        .order_by(SiteErrorLog.hit_count.desc())
        .limit(500)
        .all()
    )

    total_404 = sum(1 for r in rows if r.status_code == 404)
    total_5xx = sum(1 for r in rows if r.status_code >= 500)
    total_users = sum(r.hit_count for r in rows)

    by_source: dict[str, int] = {}
    for r in rows:
        by_source[r.source] = by_source.get(r.source, 0) + 1

    error_list = []
    for r in rows:
        extra = {}
        if r.extra_json:
            try:
                extra = json.loads(r.extra_json)
            except Exception:
                pass
        error_list.append({
            "id":          r.id,
            "url":         r.url,
            "status_code": r.status_code,
            "source":      r.source,
            "error_type":  r.error_type,
            "hit_count":   r.hit_count,
            "first_seen":  r.first_seen.isoformat() if r.first_seen else "",
            "last_seen":   r.last_seen.isoformat() if r.last_seen else "",
            "page_title":  extra.get("page_title", ""),
        })

    return {
        "total_404":   total_404,
        "total_5xx":   total_5xx,
        "total_users": total_users,
        "by_source":   by_source,
        "errors":      error_list,
        "site_id":     site_id,
        "days":        days,
    }


def run_error_detection_for_site(db: Session, site_id: int, days: int = 7) -> dict:
    """Tek site için GA4 hata tespiti çalıştırır ve DB'ye kaydeder."""
    from backend.models import Site
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties

    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        return {"status": "error", "message": "site not found"}

    record = get_ga4_credentials_record(db, site.id)
    if not record or not record.credentials_json:
        return {"status": "skip", "message": "GA4 credentials yok"}

    properties = load_ga4_properties(record)
    property_id = properties.get("web") or properties.get("mweb")
    if not property_id:
        return {"status": "skip", "message": "web property yok"}

    errors = fetch_ga4_error_pages(record.credentials_json, property_id, days=days)
    saved = save_error_logs(db, site.id, errors, source="ga4")

    logger.info("Hata tespiti: site=%s, %d hata bulundu, %d kaydedildi", site.domain, len(errors), saved)
    return {"status": "ok", "found": len(errors), "saved": saved, "domain": site.domain}


def run_error_detection_all_sites(db: Session, days: int = 1) -> list[dict]:
    """Tüm GA4-bağlı siteler için hata tespiti."""
    from backend.models import Site
    from backend.services.ga4_auth import get_ga4_credentials_record

    sites = db.query(Site).all()
    results = []
    for site in sites:
        try:
            record = get_ga4_credentials_record(db, site.id)
            if not record or not record.credentials_json:
                continue
            result = run_error_detection_for_site(db, site.id, days=days)
            results.append(result)
        except Exception as exc:
            logger.warning("Hata tespiti başarısız [site=%s]: %s", site.domain, exc)
    return results
