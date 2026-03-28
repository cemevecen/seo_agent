"""Search Console URL Inspection API collector."""

from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import urlparse

from google.auth.transport.requests import Request as GoogleAuthRequest
from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.search_console_auth import (
    SEARCH_CONSOLE_SCOPES,
    get_search_console_credentials_record,
    load_google_credentials,
)
from backend.services.warehouse import (
    finish_collector_run,
    get_latest_url_inspection_snapshot,
    save_url_inspection_snapshot,
    start_collector_run,
)
from backend.collectors.search_console import _resolve_search_console_property


def _normalize_url(domain: str) -> str:
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    return f"https://{domain}"


def _inspection_candidates(domain: str) -> list[str]:
    base = _normalize_url(domain).rstrip("/")
    host = urlparse(base).netloc
    candidates = [base + "/"]
    if host.startswith("www."):
        naked = host[4:]
        candidates.append(f"https://{naked}/")
    else:
        candidates.append(f"https://www.{host}/")
    deduped: list[str] = []
    for item in candidates:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _extract_summary(payload: dict, inspection_url: str, property_url: str) -> dict:
    result = (payload.get("inspectionResult") or {})
    index_status = result.get("indexStatusResult") or {}
    rich_result = result.get("richResultsResult") or {}
    mobile_usability = result.get("mobileUsabilityResult") or {}
    return {
        "inspection_url": inspection_url,
        "property_url": property_url,
        "verdict": index_status.get("verdict") or "",
        "coverage_state": index_status.get("coverageState") or "",
        "indexing_state": index_status.get("indexingState") or "",
        "page_fetch_state": index_status.get("pageFetchState") or "",
        "robots_txt_state": index_status.get("robotsTxtState") or "",
        "google_canonical": index_status.get("googleCanonical") or "",
        "user_canonical": index_status.get("userCanonical") or "",
        "last_crawl_time": index_status.get("lastCrawlTime") or "",
        "referring_urls": index_status.get("referringUrls") or [],
        "rich_results_verdict": rich_result.get("verdict") or "",
        "mobile_usability_verdict": mobile_usability.get("verdict") or "",
    }


def _load_service_and_property(db: Session, site: Site):
    credential = get_search_console_credentials_record(db, site.id)
    if credential is None:
        raise RuntimeError("Search Console baglantisi yok.")

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError("Google Search Console istemcisi yuklu degil.") from exc

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
    property_url = _resolve_search_console_property(service, site)
    return service, property_url


def collect_url_inspection(db: Session, site: Site) -> dict:
    collected_at = datetime.utcnow()
    run = start_collector_run(
        db,
        site_id=site.id,
        provider="url_inspection",
        strategy="homepage",
        target_url=_normalize_url(site.domain),
        requested_at=collected_at,
    )
    try:
        service, property_url = _load_service_and_property(db, site)
        last_error: Exception | None = None
        for inspection_url in _inspection_candidates(site.domain):
            try:
                payload = (
                    service.urlInspection()
                    .index()
                    .inspect(
                        body={
                            "inspectionUrl": inspection_url,
                            "siteUrl": property_url,
                            "languageCode": "tr-TR",
                        }
                    )
                    .execute()
                )
                summary = _extract_summary(payload, inspection_url, property_url)
                save_url_inspection_snapshot(
                    db,
                    site_id=site.id,
                    inspection_url=inspection_url,
                    property_url=property_url,
                    payload=payload,
                    summary=summary,
                    collected_at=collected_at,
                    collector_run_id=run.id,
                )
                finish_collector_run(
                    db,
                    run,
                    status="success",
                    finished_at=collected_at,
                    summary={"inspection_url": inspection_url, "verdict": summary.get("verdict")},
                    row_count=1,
                )
                return {"state": "live", "summary": summary}
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
        raise RuntimeError(str(last_error or "URL Inspection sonucu alinamadi."))
    except Exception as exc:  # noqa: BLE001
        fallback = get_latest_url_inspection_snapshot(db, site_id=site.id)
        finish_collector_run(
            db,
            run,
            status="failed" if fallback is None else "stale",
            finished_at=datetime.utcnow(),
            error_message=str(exc),
            summary={"fallback_used": fallback is not None},
            row_count=0,
        )
        return {
            "state": "failed" if fallback is None else "stale",
            "summary": fallback,
            "error": str(exc),
        }
