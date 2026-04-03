"""Collector verilerini buyuk hacimde saklamak icin warehouse yardimcilari."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.models import (
    CollectorRun,
    CruxHistorySnapshot,
    Ga4ReportSnapshot,
    LighthouseAuditRecord,
    PageSpeedPayloadSnapshot,
    SearchConsoleQuerySnapshot,
    UrlAuditRecord,
    UrlInspectionSnapshot,
)
from backend.services.timezone_utils import format_local_datetime


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
                property_url=str(row.get("property_url") or property_url),
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
            "property_url": row.property_url,
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


def get_latest_search_console_rows_batch(
    db: Session,
    *,
    site_id: int,
    scopes: list[str],
) -> dict[str, list[dict]]:
    """Birden fazla scope için SC satırlarını 2 sorguda toplu çeker (N×2 yerine)."""
    if not scopes:
        return {}

    # 1. Her scope için en son collected_at'ı tek sorguda al
    from sqlalchemy import case, literal_column, tuple_ as sq_tuple
    rows_max = (
        db.query(
            SearchConsoleQuerySnapshot.data_scope,
            func.max(SearchConsoleQuerySnapshot.collected_at).label("max_ts"),
        )
        .filter(
            SearchConsoleQuerySnapshot.site_id == site_id,
            SearchConsoleQuerySnapshot.data_scope.in_(scopes),
        )
        .group_by(SearchConsoleQuerySnapshot.data_scope)
        .all()
    )
    scope_to_ts: dict[str, object] = {row.data_scope: row.max_ts for row in rows_max if row.max_ts is not None}
    if not scope_to_ts:
        return {scope: [] for scope in scopes}

    # 2. Tüm scope+timestamp çiftleri için tek SELECT
    from sqlalchemy import and_, or_
    conditions = [
        and_(
            SearchConsoleQuerySnapshot.data_scope == scope,
            SearchConsoleQuerySnapshot.collected_at == ts,
        )
        for scope, ts in scope_to_ts.items()
    ]
    all_rows = (
        db.query(SearchConsoleQuerySnapshot)
        .filter(
            SearchConsoleQuerySnapshot.site_id == site_id,
            or_(*conditions),
        )
        .order_by(SearchConsoleQuerySnapshot.clicks.desc(), SearchConsoleQuerySnapshot.impressions.desc())
        .all()
    )

    result: dict[str, list[dict]] = {scope: [] for scope in scopes}
    for row in all_rows:
        result.setdefault(row.data_scope, []).append(
            {
                "query": row.query,
                "property_url": row.property_url,
                "device": row.device,
                "clicks": float(row.clicks),
                "impressions": float(row.impressions),
                "ctr": float(row.ctr),
                "position": float(row.position),
                "start_date": row.start_date,
                "end_date": row.end_date,
            }
        )
    return result


def get_latest_sc_rows_multi_site(
    db: Session,
    *,
    site_ids: list[int],
    scopes: list[str],
) -> "dict[int, dict[str, list[dict]]]":
    """Multiple sites × scopes için SC satırlarını 2 sorguda toplu çeker (N×site × 2×scope yerine)."""
    if not site_ids or not scopes:
        return {sid: {scope: [] for scope in scopes} for sid in site_ids}

    from sqlalchemy import and_, or_

    # 1. Her (site_id, scope) için max collected_at — tek sorgu
    rows_max = (
        db.query(
            SearchConsoleQuerySnapshot.site_id,
            SearchConsoleQuerySnapshot.data_scope,
            func.max(SearchConsoleQuerySnapshot.collected_at).label("max_ts"),
        )
        .filter(
            SearchConsoleQuerySnapshot.site_id.in_(site_ids),
            SearchConsoleQuerySnapshot.data_scope.in_(scopes),
        )
        .group_by(SearchConsoleQuerySnapshot.site_id, SearchConsoleQuerySnapshot.data_scope)
        .all()
    )
    site_scope_ts: dict[tuple, object] = {}
    for row in rows_max:
        if row.max_ts is not None:
            site_scope_ts[(row.site_id, row.data_scope)] = row.max_ts

    if not site_scope_ts:
        return {sid: {scope: [] for scope in scopes} for sid in site_ids}

    # 2. Tüm eşleşen satırları tek SELECT ile çek
    conditions = [
        and_(
            SearchConsoleQuerySnapshot.site_id == sid,
            SearchConsoleQuerySnapshot.data_scope == scope,
            SearchConsoleQuerySnapshot.collected_at == ts,
        )
        for (sid, scope), ts in site_scope_ts.items()
    ]
    all_rows = (
        db.query(SearchConsoleQuerySnapshot)
        .filter(or_(*conditions))
        .order_by(
            SearchConsoleQuerySnapshot.site_id,
            SearchConsoleQuerySnapshot.data_scope,
            SearchConsoleQuerySnapshot.clicks.desc(),
            SearchConsoleQuerySnapshot.impressions.desc(),
        )
        .all()
    )

    result: dict[int, dict[str, list[dict]]] = {
        sid: {scope: [] for scope in scopes} for sid in site_ids
    }
    for row in all_rows:
        if row.site_id in result and row.data_scope in result[row.site_id]:
            result[row.site_id][row.data_scope].append(
                {
                    "query": row.query,
                    "property_url": row.property_url,
                    "device": row.device,
                    "clicks": float(row.clicks),
                    "impressions": float(row.impressions),
                    "ctr": float(row.ctr),
                    "position": float(row.position),
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                }
            )
    return result


def save_ga4_report_snapshot(
    db: Session,
    *,
    site_id: int,
    profile: str,
    period_days: int,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
    payload: dict,
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> Ga4ReportSnapshot:
    row = Ga4ReportSnapshot(
        site_id=site_id,
        collector_run_id=collector_run_id,
        profile=str(profile).strip().lower(),
        period_days=int(period_days),
        last_start=last_start,
        last_end=last_end,
        prev_start=prev_start,
        prev_end=prev_end,
        payload_json=json.dumps(payload or {}, ensure_ascii=True),
        collected_at=collected_at,
    )
    db.add(row)
    return row


def save_url_audit_records(
    db: Session,
    *,
    site_id: int,
    rows: list[dict],
    collected_at: datetime,
    collector_run_id: int | None = None,
) -> int:
    count = 0
    for row in rows:
        db.add(
            UrlAuditRecord(
                site_id=site_id,
                collector_run_id=collector_run_id,
                url=str(row.get("url") or ""),
                final_url=str(row.get("final_url") or ""),
                status_code=int(row.get("status_code") or 0),
                content_type=str(row.get("content_type") or ""),
                sitemap_source=str(row.get("sitemap_source") or ""),
                sitemap_lastmod=str(row.get("sitemap_lastmod") or ""),
                has_title=bool(row.get("has_title")),
                title=str(row.get("title") or ""),
                title_length=int(row.get("title_length") or 0),
                has_meta_description=bool(row.get("has_meta_description")),
                meta_description=str(row.get("meta_description") or ""),
                meta_description_length=int(row.get("meta_description_length") or 0),
                has_h1=bool(row.get("has_h1")),
                h1=str(row.get("h1") or ""),
                h1_count=int(row.get("h1_count") or 0),
                has_canonical=bool(row.get("has_canonical")),
                canonical_url=str(row.get("canonical_url") or ""),
                canonical_matches_final=bool(row.get("canonical_matches_final")),
                has_schema=bool(row.get("has_schema")),
                is_noindex=bool(row.get("is_noindex")),
                meta_robots=str(row.get("meta_robots") or ""),
                has_og_title=bool(row.get("has_og_title")),
                has_og_description=bool(row.get("has_og_description")),
                search_clicks=float(row.get("search_clicks") or 0.0),
                search_impressions=float(row.get("search_impressions") or 0.0),
                search_ctr=float(row.get("search_ctr") or 0.0),
                search_console_seen=bool(row.get("search_console_seen")),
                indexed_via=str(row.get("indexed_via") or "none"),
                inspection_verdict=str(row.get("inspection_verdict") or ""),
                issue_count=int(row.get("issue_count") or 0),
                checks_json=json.dumps(row.get("checks") or {}, ensure_ascii=True),
                seo_score=str(row.get("seo_score") or "poor"),
                collected_at=collected_at,
            )
        )
        count += 1
    return count


def get_latest_ga4_report_snapshot(
    db: Session,
    *,
    site_id: int,
    profile: str,
    period_days: int = 30,
) -> dict | None:
    row = (
        db.query(Ga4ReportSnapshot)
        .filter(
            Ga4ReportSnapshot.site_id == site_id,
            Ga4ReportSnapshot.profile == str(profile).strip().lower(),
            Ga4ReportSnapshot.period_days == int(period_days),
        )
        .order_by(Ga4ReportSnapshot.collected_at.desc(), Ga4ReportSnapshot.id.desc())
        .first()
    )
    if row is None:
        return None
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    return {
        "profile": row.profile,
        "period_days": row.period_days,
        "last_start": row.last_start,
        "last_end": row.last_end,
        "prev_start": row.prev_start,
        "prev_end": row.prev_end,
        "collected_at": row.collected_at.isoformat(),
        "payload": payload,
    }


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
        "ga4_report_snapshots": int(
            db.query(func.count(Ga4ReportSnapshot.id)).filter(Ga4ReportSnapshot.site_id == site_id).scalar() or 0
        ),
        "url_audit_records": int(
            db.query(func.count(UrlAuditRecord.id)).filter(UrlAuditRecord.site_id == site_id).scalar() or 0
        ),
        "latest_runs": [
            {
                "provider": run.provider,
                "strategy": run.strategy,
                "status": run.status,
                "row_count": run.row_count,
                "requested_at": format_local_datetime(run.requested_at),
                "finished_at": format_local_datetime(run.finished_at),
                "target_url": run.target_url,
                "error_message": run.error_message,
                "summary": json.loads(run.summary_json or "{}"),
            }
            for run in latest_runs
        ],
    }


def get_latest_url_audit_snapshot(db: Session, *, site_id: int, row_limit: int = 250) -> dict | None:
    latest_run = (
        db.query(CollectorRun)
        .filter(
            CollectorRun.site_id == site_id,
            CollectorRun.provider == "site_audit",
            CollectorRun.status == "success",
        )
        .order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc())
        .first()
    )
    if latest_run is None:
        return None

    rows = (
        db.query(UrlAuditRecord)
        .filter(
            UrlAuditRecord.site_id == site_id,
            UrlAuditRecord.collector_run_id == latest_run.id,
        )
        .order_by(
            UrlAuditRecord.issue_count.desc(),
            UrlAuditRecord.status_code.asc(),
            UrlAuditRecord.url.asc(),
        )
        .limit(max(1, int(row_limit)))
        .all()
    )
    try:
        summary = json.loads(latest_run.summary_json or "{}")
    except json.JSONDecodeError:
        summary = {}

    order = {"poor": 0, "needs_improvement": 1, "good": 2}
    prepared_rows = []
    for row in rows:
        try:
            checks = json.loads(row.checks_json or "{}")
        except json.JSONDecodeError:
            checks = {}
        prepared_rows.append(
            {
                "url": row.url,
                "final_url": row.final_url,
                "status_code": int(row.status_code or 0),
                "content_type": row.content_type,
                "sitemap_source": row.sitemap_source,
                "sitemap_lastmod": row.sitemap_lastmod,
                "has_title": bool(row.has_title),
                "title": row.title,
                "title_length": int(row.title_length or 0),
                "has_meta_description": bool(row.has_meta_description),
                "meta_description": row.meta_description,
                "meta_description_length": int(row.meta_description_length or 0),
                "has_h1": bool(row.has_h1),
                "h1": row.h1,
                "h1_count": int(row.h1_count or 0),
                "has_canonical": bool(row.has_canonical),
                "canonical_url": row.canonical_url,
                "canonical_matches_final": bool(row.canonical_matches_final),
                "has_schema": bool(row.has_schema),
                "is_noindex": bool(row.is_noindex),
                "meta_robots": row.meta_robots,
                "has_og_title": bool(row.has_og_title),
                "has_og_description": bool(row.has_og_description),
                "search_clicks": float(row.search_clicks or 0.0),
                "search_impressions": float(row.search_impressions or 0.0),
                "search_ctr": float(row.search_ctr or 0.0),
                "search_console_seen": bool(row.search_console_seen),
                "indexed_via": row.indexed_via,
                "inspection_verdict": row.inspection_verdict,
                "issue_count": int(row.issue_count or 0),
                "seo_score": row.seo_score,
                "checks": checks,
                "score_rank": order.get(row.seo_score, 9),
            }
        )

    prepared_rows.sort(
        key=lambda item: (
            item["score_rank"],
            -(item["search_clicks"] or 0.0),
            -(item["search_impressions"] or 0.0),
            -item["issue_count"],
            item["url"],
        )
    )
    return {
        "collected_at": latest_run.finished_at.isoformat() if latest_run.finished_at else None,
        "requested_at": latest_run.requested_at.isoformat() if latest_run.requested_at else None,
        "summary": summary,
        "rows": prepared_rows,
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
        "payload": json.loads(row.payload_json or "{}"),
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
