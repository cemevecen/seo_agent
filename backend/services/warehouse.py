"""Collector verilerini buyuk hacimde saklamak icin warehouse yardimcilari."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.models import (
    CollectorRun,
    CruxHistorySnapshot,
    LighthouseAuditRecord,
    PageSpeedPayloadSnapshot,
    SearchConsoleQuerySnapshot,
    UrlInspectionSnapshot,
)


def start_collector_run(
    db: Session,
    *,
    site_id: int,
    provider: str,
    strategy: str = "all",
    target_url: str = "",
    requested_at: datetime | None = None,
) -> CollectorRun:
    run = CollectorRun(
        site_id=site_id,
        provider=provider,
        strategy=strategy,
        status="started",
        target_url=target_url,
        summary_json="{}",
        requested_at=requested_at or datetime.utcnow(),
    )
    db.add(run)
    db.flush()
    return run


def finish_collector_run(
    db: Session,
    run: CollectorRun,
    *,
    status: str,
    finished_at: datetime | None = None,
    error_message: str = "",
    summary: dict | None = None,
    row_count: int = 0,
) -> None:
    run.status = status
    run.finished_at = finished_at or datetime.utcnow()
    run.error_message = error_message or ""
    run.row_count = int(row_count or 0)
    run.summary_json = json.dumps(summary or {}, ensure_ascii=True)
    db.add(run)


def save_pagespeed_payload_snapshot(
    db: Session,
    *,
    site_id: int,
    strategy: str,
    payload: dict,
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> None:
    db.add(
        PageSpeedPayloadSnapshot(
            site_id=site_id,
            collector_run_id=collector_run_id,
            strategy=strategy,
            payload_json=json.dumps(payload, ensure_ascii=True),
            collected_at=collected_at,
        )
    )


def save_lighthouse_audit_records(
    db: Session,
    *,
    site_id: int,
    strategy: str,
    analysis: dict,
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> int:
    sections = (analysis or {}).get("sections") or {}
    count = 0
    for category_key, category_sections in sections.items():
        for section in category_sections or []:
            section_key = str(section.get("key") or "")
            section_title_en = str(section.get("title_en") or "")
            section_title_tr = str(section.get("title_tr") or "")
            for item in section.get("items") or []:
                db.add(
                    LighthouseAuditRecord(
                        site_id=site_id,
                        collector_run_id=collector_run_id,
                        strategy=strategy,
                        category=str(category_key),
                        section_key=section_key,
                        section_title_en=section_title_en,
                        section_title_tr=section_title_tr,
                        audit_id=str(item.get("id") or ""),
                        audit_state=str(item.get("state") or ""),
                        priority=str(item.get("priority") or "MEDIUM"),
                        score=float(item.get("score") or 0.0),
                        score_display_mode=str(item.get("score_display_mode") or ""),
                        title_en=str(item.get("title_en") or item.get("title") or ""),
                        title_tr=str(item.get("title_tr") or ""),
                        display_value=str(item.get("display_value") or ""),
                        problem_en=str(item.get("problem_en") or item.get("problem") or ""),
                        problem_tr=str(item.get("problem_tr") or ""),
                        impact_en=str(item.get("impact_en") or item.get("impact") or ""),
                        impact_tr=str(item.get("impact_tr") or ""),
                        examples_json=json.dumps(item.get("examples") or [], ensure_ascii=True),
                        solution_json=json.dumps(item.get("solution") or [], ensure_ascii=True),
                        expected_result_en=str(item.get("expected_result_en") or item.get("expected_result") or ""),
                        expected_result_tr=str(item.get("expected_result_tr") or ""),
                        collected_at=collected_at,
                    )
                )
                count += 1
    return count


def save_search_console_query_rows(
    db: Session,
    *,
    site_id: int,
    property_url: str,
    data_scope: str,
    rows: list[dict],
    collected_at: datetime,
    start_date: str,
    end_date: str,
    collector_run_id: int | None = None,
) -> int:
    count = 0
    for row in rows:
        db.add(
            SearchConsoleQuerySnapshot(
                site_id=site_id,
                collector_run_id=collector_run_id,
                property_url=property_url,
                data_scope=data_scope,
                query=str(row.get("query") or ""),
                device=str(row.get("device") or "ALL"),
                clicks=float(row.get("clicks") or 0.0),
                impressions=float(row.get("impressions") or 0.0),
                ctr=float(row.get("ctr") or 0.0),
                position=float(row.get("position") or 0.0),
                start_date=start_date,
                end_date=end_date,
                collected_at=collected_at,
            )
        )
        count += 1
    return count


def get_latest_search_console_rows(
    db: Session,
    *,
    site_id: int,
    data_scope: str = "current_28d",
) -> list[dict]:
    latest_timestamp = (
        db.query(func.max(SearchConsoleQuerySnapshot.collected_at))
        .filter(
            SearchConsoleQuerySnapshot.site_id == site_id,
            SearchConsoleQuerySnapshot.data_scope == data_scope,
        )
        .scalar()
    )
    if latest_timestamp is None:
        return []

    rows = (
        db.query(SearchConsoleQuerySnapshot)
        .filter(
            SearchConsoleQuerySnapshot.site_id == site_id,
            SearchConsoleQuerySnapshot.data_scope == data_scope,
            SearchConsoleQuerySnapshot.collected_at == latest_timestamp,
        )
        .order_by(SearchConsoleQuerySnapshot.clicks.desc(), SearchConsoleQuerySnapshot.impressions.desc())
        .all()
    )
    return [
        {
            "query": row.query,
            "device": row.device,
            "clicks": float(row.clicks),
            "impressions": float(row.impressions),
            "ctr": float(row.ctr),
            "position": float(row.position),
            "start_date": row.start_date,
            "end_date": row.end_date,
        }
        for row in rows
    ]


def get_site_warehouse_summary(db: Session, *, site_id: int) -> dict:
    latest_runs = (
        db.query(CollectorRun)
        .filter(CollectorRun.site_id == site_id)
        .order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc())
        .limit(10)
        .all()
    )
    return {
        "collector_runs": int(
            db.query(func.count(CollectorRun.id)).filter(CollectorRun.site_id == site_id).scalar() or 0
        ),
        "pagespeed_payload_snapshots": int(
            db.query(func.count(PageSpeedPayloadSnapshot.id))
            .filter(PageSpeedPayloadSnapshot.site_id == site_id)
            .scalar()
            or 0
        ),
        "lighthouse_audit_records": int(
            db.query(func.count(LighthouseAuditRecord.id))
            .filter(LighthouseAuditRecord.site_id == site_id)
            .scalar()
            or 0
        ),
        "search_console_query_snapshots": int(
            db.query(func.count(SearchConsoleQuerySnapshot.id))
            .filter(SearchConsoleQuerySnapshot.site_id == site_id)
            .scalar()
            or 0
        ),
        "crux_history_snapshots": int(
            db.query(func.count(CruxHistorySnapshot.id))
            .filter(CruxHistorySnapshot.site_id == site_id)
            .scalar()
            or 0
        ),
        "url_inspection_snapshots": int(
            db.query(func.count(UrlInspectionSnapshot.id))
            .filter(UrlInspectionSnapshot.site_id == site_id)
            .scalar()
            or 0
        ),
        "latest_runs": [
            {
                "provider": run.provider,
                "strategy": run.strategy,
                "status": run.status,
                "row_count": run.row_count,
                "requested_at": run.requested_at.isoformat() if run.requested_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                "target_url": run.target_url,
                "error_message": run.error_message,
                "summary": json.loads(run.summary_json or "{}"),
            }
            for run in latest_runs
        ],
    }


def save_crux_history_snapshot(
    db: Session,
    *,
    site_id: int,
    form_factor: str,
    target_url: str,
    payload: dict,
    summary: dict,
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> None:
    db.add(
        CruxHistorySnapshot(
            site_id=site_id,
            collector_run_id=collector_run_id,
            form_factor=form_factor,
            target_url=target_url,
            summary_json=json.dumps(summary or {}, ensure_ascii=True),
            payload_json=json.dumps(payload or {}, ensure_ascii=True),
            collected_at=collected_at,
        )
    )


def save_url_inspection_snapshot(
    db: Session,
    *,
    site_id: int,
    inspection_url: str,
    property_url: str,
    payload: dict,
    summary: dict,
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> None:
    db.add(
        UrlInspectionSnapshot(
            site_id=site_id,
            collector_run_id=collector_run_id,
            inspection_url=inspection_url,
            property_url=property_url,
            verdict=str(summary.get("verdict") or ""),
            coverage_state=str(summary.get("coverage_state") or ""),
            indexing_state=str(summary.get("indexing_state") or ""),
            page_fetch_state=str(summary.get("page_fetch_state") or ""),
            robots_txt_state=str(summary.get("robots_txt_state") or ""),
            google_canonical=str(summary.get("google_canonical") or ""),
            user_canonical=str(summary.get("user_canonical") or ""),
            last_crawl_time=str(summary.get("last_crawl_time") or ""),
            summary_json=json.dumps(summary or {}, ensure_ascii=True),
            payload_json=json.dumps(payload or {}, ensure_ascii=True),
            collected_at=collected_at,
        )
    )


def get_latest_crux_snapshot(db: Session, *, site_id: int, form_factor: str) -> dict | None:
    row = (
        db.query(CruxHistorySnapshot)
        .filter(CruxHistorySnapshot.site_id == site_id, CruxHistorySnapshot.form_factor == form_factor)
        .order_by(CruxHistorySnapshot.collected_at.desc(), CruxHistorySnapshot.id.desc())
        .first()
    )
    if row is None:
        return None
    return {
        "form_factor": row.form_factor,
        "target_url": row.target_url,
        "summary": json.loads(row.summary_json or "{}"),
        "collected_at": row.collected_at.isoformat() if row.collected_at else None,
    }


def get_latest_url_inspection_snapshot(db: Session, *, site_id: int) -> dict | None:
    row = (
        db.query(UrlInspectionSnapshot)
        .filter(UrlInspectionSnapshot.site_id == site_id)
        .order_by(UrlInspectionSnapshot.collected_at.desc(), UrlInspectionSnapshot.id.desc())
        .first()
    )
    if row is None:
        return None
    return {
        "inspection_url": row.inspection_url,
        "property_url": row.property_url,
        "summary": json.loads(row.summary_json or "{}"),
        "collected_at": row.collected_at.isoformat() if row.collected_at else None,
    }
