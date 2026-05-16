"""
Site hata izleme — GA4 Analytics Data API ile 404/hata sayfası tespiti.
Credential pattern: ga4_realtime.py ile aynı (global service account).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _build_ga4_client():
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.oauth2 import service_account
    from backend.services.ga4_auth import GA4_SCOPES, load_ga4_service_account_info

    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_ga4_error_pages(
    property_id: str,
    days: int = 7,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    GA4 Analytics Data API ile hata sayfalarını çeker.
    pagePath'te '404' geçen VEYA pageTitle'da hata kelimeleri içeren sayfalar.
    """
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Filter, FilterExpression,
        FilterExpressionList, Metric, OrderBy, RunReportRequest,
    )

    pid = property_id.strip()
    if not pid.startswith("properties/"):
        pid = f"properties/{pid}"

    # 404 sayfalarını yakala: path'te /404 VEYA title'da hata kelimeleri
    error_filters = [
        FilterExpression(filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="/404", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="404", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="bulunamadı", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="not found", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="sayfa bulunamadı", case_sensitive=False,
            ),
        )),
    ]

    request = RunReportRequest(
        property=pid,
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),  # tam URL — query string dahil
            Dimension(name="pageTitle"),
            Dimension(name="pageReferrer"),              # nereden geldi — hangi link kırdı
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
        ],
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(expressions=error_filters)
        ),
        order_bys=[OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"),
            desc=True,
        )],
        limit=limit,
    )

    try:
        client = _build_ga4_client()
        response = client.run_report(request)
    except Exception as exc:
        logger.warning("GA4 error pages fetch hatası [property=%s]: %s", property_id, exc)
        return []

    # URL bazında grupla — aynı URL farklı referrer'lardan gelebilir
    grouped: dict[str, dict] = {}
    for row in response.rows:
        dims = [dv.value for dv in row.dimension_values]
        mets = [mv.value for mv in row.metric_values]
        page_path  = dims[0] if len(dims) > 0 else ""
        page_title = dims[1] if len(dims) > 1 else ""
        referrer   = (dims[2] if len(dims) > 2 else "").strip()
        pageviews  = int(mets[0]) if len(mets) > 0 else 0
        users      = int(mets[1]) if len(mets) > 1 else 0

        if not page_path:
            continue

        if page_path not in grouped:
            grouped[page_path] = {
                "url":         page_path,
                "page_title":  page_title,
                "referrers":   [],
                "pageviews":   0,
                "users":       0,
                "status_code": 404,
                "source":      "ga4",
                "error_type":  "not_found",
            }
        grouped[page_path]["pageviews"] += pageviews
        grouped[page_path]["users"]     += users
        if referrer and referrer not in grouped[page_path]["referrers"]:
            grouped[page_path]["referrers"].append(referrer)

    results = sorted(grouped.values(), key=lambda x: x["users"], reverse=True)
    logger.info("GA4 hata sayfaları: property=%s, %d benzersiz URL, %d satır döndü",
                property_id, len(results), len(response.rows))
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
    now = datetime.utcnow()

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
        # Mevcut referrer listesiyle birleştir (yeni çekimde yeni kaynaklar eklensin)
        new_refs = e.get("referrers") or ([e.get("referrer")] if e.get("referrer") else [])
        if existing:
            old_extra = {}
            try:
                old_extra = json.loads(existing.extra_json or "{}")
            except Exception:
                pass
            old_refs = old_extra.get("referrers") or []
            merged = old_refs + [r for r in new_refs if r and r not in old_refs]
        else:
            merged = [r for r in new_refs if r]
        extra = json.dumps({
            "page_title": e.get("page_title", ""),
            "referrers":  merged[:20],  # max 20 referrer sakla
        }, ensure_ascii=False)
        if existing:
            existing.hit_count = int(e.get("users", 1))  # GA4 artık date'siz, tam toplam
            existing.last_seen  = now
            existing.extra_json = extra
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
                extra_json=extra,
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
            "referrers":   extra.get("referrers") or [],
        })

    return {
        "total_404":   total_404,
        "total_5xx":   total_5xx,
        "total_users": total_users,
        "by_source":   {"ga4": total_404 + total_5xx},
        "errors":      error_list,
        "site_id":     site_id,
        "days":        days,
    }


def run_error_detection_for_site(db: Session, site_id: int, days: int = 7) -> dict:
    """Tek site için GA4 hata tespiti çalıştırır ve DB'ye kaydeder."""
    from backend.models import Site
    from backend.services.ga4_auth import (
        get_ga4_credentials_record, load_ga4_properties, ga4_is_configured,
    )

    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        return {"status": "error", "message": "site not found"}

    if not ga4_is_configured():
        return {"status": "skip", "message": "GA4 service account yapılandırılmamış"}

    record = get_ga4_credentials_record(db, site.id)
    if not record:
        return {"status": "skip", "message": "GA4 property kaydı yok"}

    properties = load_ga4_properties(record)
    property_id = properties.get("web") or properties.get("mweb") or next(iter(properties.values()), "")
    if not property_id:
        return {"status": "skip", "message": "web property ID yok"}

    errors = fetch_ga4_error_pages(property_id, days=days)
    saved = save_error_logs(db, site.id, errors, source="ga4")

    logger.info(
        "Hata tespiti: site=%s property=%s, %d hata bulundu, %d kaydedildi",
        site.domain, property_id, len(errors), saved,
    )
    return {
        "status":  "ok",
        "found":   len(errors),
        "saved":   saved,
        "domain":  site.domain,
    }


def run_error_detection_all_sites(db: Session, days: int = 1) -> list[dict]:
    """Tüm GA4-bağlı siteler için hata tespiti."""
    from backend.models import Site
    from backend.services.ga4_auth import get_ga4_credentials_record, ga4_is_configured

    if not ga4_is_configured():
        logger.warning("GA4 service account yapılandırılmamış — hata tespiti atlanıyor")
        return []

    sites = db.query(Site).all()
    results = []
    for site in sites:
        try:
            record = get_ga4_credentials_record(db, site.id)
            if not record:
                continue
            result = run_error_detection_for_site(db, site.id, days=days)
            results.append(result)
        except Exception as exc:
            logger.warning("Hata tespiti başarısız [site=%s]: %s", site.domain, exc)
    return results
