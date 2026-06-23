"""SEO meta tag taraması — GA4 top URL listesi + sayfa crawl (manuel ve zamanlanmış job)."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from backend.collectors.site_audit import _fetch_url_audit
from backend.models import Site, UrlAuditRecord
from backend.services.ga4_page_urls import (
    is_seo_audit_crawl_url,
    repair_seo_audit_url,
    seo_audit_url_from_ga4,
)
from backend.services.meta_audit import purge_invalid_m_doviz_audit_urls
from backend.services.warehouse import finish_collector_run, start_collector_run

logger = logging.getLogger(__name__)

_GA4_TOP_LIMIT = 250


def _collect_audit_urls(site_id: int, site_domain: str, progress: dict) -> list[str]:
    seen_urls: set[str] = set()
    urls: list[str] = []

    def _add(u: str) -> None:
        u = repair_seo_audit_url(u.split("?")[0].rstrip("/"))
        if u and is_seo_audit_crawl_url(u) and u not in seen_urls:
            seen_urls.add(u)
            urls.append(u)

    base = f"https://{site_domain}"

    if "doviz" in (site_domain or "").lower():
        from backend.database import SessionLocal

        with SessionLocal() as db:
            n = purge_invalid_m_doviz_audit_urls(db, site_id)
            if n:
                logger.info("SEO audit: %d hatalı m.doviz URL temizlendi", n)

    progress["current"] = "GA4'ten trafik verileri çekiliyor…"
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, OrderBy, RunReportRequest
        from google.oauth2 import service_account

        from backend.services.ga4_auth import (
            GA4_SCOPES,
            ga4_is_configured,
            get_ga4_credentials_record,
            load_ga4_properties,
            load_ga4_service_account_info,
        )

        if ga4_is_configured():
            from backend.database import SessionLocal

            with SessionLocal() as db:
                record = get_ga4_credentials_record(db, site_id)
                properties = load_ga4_properties(record)

            info = load_ga4_service_account_info()
            creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
            client = BetaAnalyticsDataClient(credentials=creds)

            for profile_key in ("web", "mweb"):
                prop_id = properties.get(profile_key, "")
                if not prop_id:
                    continue
                pid = prop_id if prop_id.startswith("properties/") else f"properties/{prop_id}"
                progress["current"] = f"GA4 top sayfalar çekiliyor: {profile_key}…"
                try:
                    resp = client.run_report(
                        RunReportRequest(
                            property=pid,
                            date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
                            dimensions=[
                                Dimension(name="hostName"),
                                Dimension(name="pagePath"),
                            ],
                            metrics=[Metric(name="sessions")],
                            order_bys=[
                                OrderBy(
                                    metric=OrderBy.MetricOrderBy(metric_name="sessions"),
                                    desc=True,
                                )
                            ],
                            limit=_GA4_TOP_LIMIT,
                        )
                    )
                    for row in resp.rows:
                        dims = row.dimension_values
                        hostname = dims[0].value if dims else ""
                        path = dims[1].value if len(dims) > 1 else ""
                        full_url = seo_audit_url_from_ga4(
                            hostname, path, ga4_profile=profile_key,
                        )
                        if full_url:
                            _add(full_url)
                    logger.info(
                        "SEO audit GA4: site=%s profile=%s rows=%d urls=%d",
                        site_domain,
                        profile_key,
                        len(resp.rows or []),
                        len(urls),
                    )
                except Exception as exc:
                    logger.warning("SEO audit GA4 hatası [%s/%s]: %s", site_domain, profile_key, exc)
    except Exception as exc:
        logger.warning("SEO audit GA4 genel hata [%s]: %s", site_domain, exc)

    has_akaryakit = any("akaryakit" in u for u in urls)
    if not has_akaryakit and "doviz.com" in (site_domain or ""):
        for u in [
            f"{base}/akaryakit-fiyatlari",
            f"{base}/akaryakit-fiyatlari/istanbul-avrupa",
            f"{base}/akaryakit-fiyatlari/istanbul-anadolu",
            f"{base}/akaryakit-fiyatlari/ankara",
            f"{base}/akaryakit-fiyatlari/izmir",
            f"{base}/akaryakit-fiyatlari/adana",
            f"{base}/akaryakit-fiyatlari/bursa",
            f"{base}/akaryakit-fiyatlari/antalya",
        ]:
            _add(u)
        logger.info("SEO audit: akaryakit fallback eklendi [%s]", site_domain)

    www_base = base if (site_domain or "").startswith("www.") else f"https://www.doviz.com"
    has_fuel_hub = any("yakit-sarj" in u or "ev-sarj-fiyatlari" in u for u in urls)
    if not has_fuel_hub and "doviz.com" in (site_domain or ""):
        for u in (f"{www_base}/yakit-sarj", f"{www_base}/ev-sarj-fiyatlari"):
            _add(u)
        logger.info("SEO audit: yakit-sarj / ev-sarj fallback eklendi [%s]", site_domain)

    return urls


def _persist_audit_result(db, site_id: int, url: str, result: dict, *, collected_at: datetime, sitemap_source: str) -> None:
    result.setdefault("sitemap_source", sitemap_source)
    result.setdefault("sitemap_lastmod", "")
    checks = result.get("checks") or {}
    db.query(UrlAuditRecord).filter(
        UrlAuditRecord.site_id == site_id,
        UrlAuditRecord.url == result.get("url", url),
    ).delete(synchronize_session=False)
    db.add(
        UrlAuditRecord(
            site_id=site_id,
            url=result.get("url", url),
            final_url=result.get("final_url", url),
            status_code=int(result.get("status_code") or 0),
            content_type=str(result.get("content_type") or ""),
            sitemap_source=str(result.get("sitemap_source") or ""),
            sitemap_lastmod=str(result.get("sitemap_lastmod") or ""),
            has_title=bool(result.get("has_title", checks.get("title"))),
            title=str(result.get("title") or ""),
            title_length=int(result.get("title_length") or len(result.get("title") or "")),
            has_meta_description=bool(result.get("has_meta_description", checks.get("desc"))),
            meta_description=str(result.get("meta_description") or ""),
            meta_description_length=int(result.get("meta_description_length") or len(result.get("meta_description") or "")),
            has_h1=bool(result.get("has_h1", checks.get("h1"))),
            h1=str(result.get("h1") or ""),
            h1_count=int(result.get("h1_count") or 0),
            h2_count=int(result.get("h2_count") or 0),
            has_canonical=bool(result.get("has_canonical", checks.get("canonical"))),
            canonical_url=str(result.get("canonical_url") or ""),
            canonical_matches_final=bool(result.get("canonical_matches_final", checks.get("canonical_matches_final"))),
            has_schema=bool(result.get("has_schema", checks.get("schema"))),
            is_noindex=bool(result.get("is_noindex", not checks.get("indexable", True))),
            meta_robots=str(result.get("meta_robots") or ""),
            has_og_title=bool(result.get("has_og_title", checks.get("og_title"))),
            has_og_description=bool(result.get("has_og_description", checks.get("og_description"))),
            issue_count=int(result.get("issue_count") or 0),
            checks_json=json.dumps(checks, ensure_ascii=True),
            seo_score=str(result.get("seo_score") or "poor"),
            collected_at=collected_at,
        )
    )


def execute_seo_audit_for_site(
    db,
    site: Site,
    *,
    trigger_source: str = "manual",
    progress: dict | None = None,
    sitemap_source: str = "ga4",
) -> dict[str, Any]:
    """Tek site için SEO audit crawl. progress dict polling için güncellenir."""
    from backend.database import SessionLocal

    site_id = site.id
    site_domain = site.domain or ""
    prog = progress if progress is not None else {
        "running": True,
        "total": 0,
        "done": 0,
        "ok": 0,
        "error": 0,
        "current": "Başlıyor…",
    }
    prog["running"] = True

    run = start_collector_run(
        db,
        site_id=site_id,
        provider="seo_audit",
        strategy=str(trigger_source or "manual"),
        target_url=f"https://{site_domain}",
        trigger_source=trigger_source,
    )
    db.commit()

    deleted_old = 0
    try:
        urls = _collect_audit_urls(site_id, site_domain, prog)
        if not urls:
            prog["current"] = "GA4'ten URL çekilemedi"
            finish_collector_run(
                db,
                run,
                status="failed",
                error_message="URL listesi boş",
                summary={"url_count": 0, "trigger_source": trigger_source},
            )
            db.commit()
            return {"status": "empty", "ok": 0, "error": 0, "total": 0, "deleted": 0}

        prog["total"] = len(urls)
        prog["current"] = f"{len(urls)} URL bulundu, tarama başlıyor…"
        collected_at = datetime.utcnow()

        for url in urls:
            prog["current"] = url
            try:
                result = _fetch_url_audit(url, timeout_seconds=8)
                with SessionLocal() as wdb:
                    _persist_audit_result(
                        wdb,
                        site_id,
                        url,
                        result,
                        collected_at=collected_at,
                        sitemap_source=sitemap_source,
                    )
                    wdb.commit()
                prog["ok"] += 1
            except Exception as exc:
                logger.debug("SEO audit URL hatası [%s]: %s", url, exc)
                prog["error"] += 1
            finally:
                prog["done"] += 1

        with SessionLocal() as wdb:
            deleted_old = (
                wdb.query(UrlAuditRecord)
                .filter(
                    UrlAuditRecord.site_id == site_id,
                    UrlAuditRecord.collected_at < collected_at,
                )
                .delete(synchronize_session=False)
            )
            wdb.commit()

        prog["current"] = f"Tamamlandı — {prog['ok']} URL başarılı, {prog['error']} hata"
        finish_collector_run(
            db,
            run,
            status="success",
            summary={
                "url_count": len(urls),
                "ok": prog["ok"],
                "error": prog["error"],
                "deleted_old": deleted_old,
                "trigger_source": trigger_source,
            },
            row_count=int(prog["ok"]),
        )
        db.commit()
        logger.info(
            "SEO audit tamamlandı: site=%s trigger=%s %d/%d URL, %d eski kayıt silindi",
            site_domain,
            trigger_source,
            prog["ok"],
            prog["total"],
            deleted_old,
        )
        return {
            "status": "ok",
            "ok": prog["ok"],
            "error": prog["error"],
            "total": prog["total"],
            "deleted": deleted_old,
        }
    except Exception as exc:
        logger.exception("SEO audit hatası site=%s: %s", site_domain, exc)
        prog["current"] = "Hata oluştu"
        finish_collector_run(
            db,
            run,
            status="failed",
            error_message=str(exc)[:500],
            summary={"trigger_source": trigger_source},
        )
        db.commit()
        return {"status": "error", "message": str(exc), "ok": prog.get("ok", 0), "error": prog.get("error", 0)}
    finally:
        prog["running"] = False
