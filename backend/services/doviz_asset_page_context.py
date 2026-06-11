"""Döviz varlık paneli — /doviz-varliklar ve /errors şablon bağlamı."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from backend.services.doviz_asset_csv_manifest import (
    csv_run_summary,
    get_csv_scan_progress,
    get_latest_csv_run,
    manifest_upload_info,
)
from backend.services.doviz_asset_monitor import get_latest_run


def build_doviz_asset_monitor_context(db: Session) -> dict[str, Any]:
    latest = get_latest_run(db, run_kind="catalog")
    csv_latest = get_latest_csv_run(db)
    manifest = manifest_upload_info(db)
    payload = (latest or {}).get("payload") or {}
    csv_payload = (csv_latest or {}).get("payload") or {}
    csv_summary = (csv_latest or {}).get("summary") or csv_run_summary(csv_payload)
    csv_progress = get_csv_scan_progress()
    issue_state = payload.get("issue_state") or {}
    open_issues = sorted(issue_state.values(), key=lambda x: str(x.get("first_seen_at") or ""))
    csv_failures = csv_summary.get("failures_preview") or csv_payload.get("failures") or []
    return {
        "run": latest,
        "csv_run": csv_latest,
        "csv_summary": csv_summary,
        "csv_progress": csv_progress,
        "manifest": manifest,
        "scan_at_tr": payload.get("scan_at_tr") or (latest or {}).get("collected_at_tr"),
        "csv_scan_at_tr": csv_summary.get("scan_at_tr")
        or csv_payload.get("scan_at_tr")
        or (csv_latest or {}).get("collected_at_tr"),
        "alerts": payload.get("alerts") or [],
        "missing": payload.get("prices_missing") or [],
        "catalog_removed": payload.get("catalog_removed") or [],
        "open_issues": open_issues,
        "csv_failures": csv_failures[:100],
        "csv_failure_total": csv_summary.get("failure_total") or len(csv_failures),
    }
