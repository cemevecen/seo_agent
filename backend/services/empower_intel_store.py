"""Empower Intelligence scrape → warehouse upsert (project × platform × date)."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import EmpowerIntelDailyRow

LOGGER = logging.getLogger(__name__)

PLATFORMS = frozenset({"web", "mweb", "ios", "android"})


def _parse_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    s = str(raw).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _norm_platform(raw: str) -> str:
    p = (raw or "").strip().lower()
    aliases = {
        "mobile web": "mweb",
        "mobile_web": "mweb",
        "mobileweb": "mweb",
        "mw": "mweb",
        "desktop": "web",
        "www": "web",
    }
    p = aliases.get(p, p)
    if p not in PLATFORMS:
        raise ValueError(f"Geçersiz platform: {raw}")
    return p


def upsert_rows(
    db: Session,
    *,
    project: str,
    platform: str,
    rows: list[dict[str, Any]],
    source: str = "scrape",
    scraped_at: datetime | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """Satırları (project, platform, report_date) ile upsert — duplicate yok."""
    proj = (project or "doviz").strip().lower() or "doviz"
    plat = _norm_platform(platform)
    when = scraped_at or datetime.utcnow()
    inserted = 0
    updated = 0
    skipped = 0

    for raw in rows:
        if not isinstance(raw, dict):
            skipped += 1
            continue
        rd = _parse_date(raw.get("report_date") or raw.get("date") or raw.get("day"))
        if not rd:
            skipped += 1
            continue
        metrics = raw.get("metrics")
        if not isinstance(metrics, dict):
            metrics = {k: v for k, v in raw.items() if k not in {"report_date", "date", "day", "platform", "project", "metrics"}}
        payload = json.dumps(metrics, ensure_ascii=False, default=str)

        existing = db.execute(
            select(EmpowerIntelDailyRow).where(
                EmpowerIntelDailyRow.project == proj,
                EmpowerIntelDailyRow.platform == plat,
                EmpowerIntelDailyRow.report_date == rd,
            )
        ).scalar_one_or_none()

        if existing is None:
            db.add(
                EmpowerIntelDailyRow(
                    project=proj,
                    platform=plat,
                    report_date=rd,
                    metrics_json=payload,
                    source=(source or "scrape")[:64],
                    scraped_at=when,
                    updated_at=when,
                )
            )
            inserted += 1
        else:
            existing.metrics_json = payload
            existing.source = (source or "scrape")[:64]
            existing.scraped_at = when
            existing.updated_at = when
            updated += 1

    if commit:
        db.commit()
    else:
        db.flush()

    return {
        "ok": True,
        "project": proj,
        "platform": plat,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "row_count": inserted + updated,
        "message": f"{proj}/{plat}: +{inserted} ~{updated}",
    }


def ingest_payload(db: Session, body: dict[str, Any]) -> dict[str, Any]:
    """Bridge ingest: {project, scraped_at, platforms:[{platform, rows:[...]}]}."""
    project = str(body.get("project") or "doviz").strip().lower() or "doviz"
    source = str(body.get("source") or "empower_intel_bridge")[:64]
    scraped_raw = str(body.get("scraped_at") or "").strip()
    scraped_at = datetime.utcnow()
    if scraped_raw:
        try:
            scraped_at = datetime.fromisoformat(scraped_raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass

    platforms = body.get("platforms") or body.get("snapshots") or []
    if not isinstance(platforms, list) or not platforms:
        # flat rows with platform field
        flat = body.get("rows") or []
        if isinstance(flat, list) and flat:
            by_plat: dict[str, list[dict[str, Any]]] = {}
            for r in flat:
                if not isinstance(r, dict):
                    continue
                try:
                    p = _norm_platform(str(r.get("platform") or ""))
                except ValueError:
                    continue
                by_plat.setdefault(p, []).append(r)
            platforms = [{"platform": p, "rows": rs} for p, rs in by_plat.items()]

    if not platforms:
        return {"ok": False, "message": "platforms/rows boş", "row_count": 0}

    totals = {"inserted": 0, "updated": 0, "skipped": 0, "row_count": 0}
    details: list[dict[str, Any]] = []
    for block in platforms:
        if not isinstance(block, dict):
            continue
        plat = str(block.get("platform") or "").strip()
        rows = block.get("rows") or []
        if not plat or not isinstance(rows, list):
            continue
        try:
            res = upsert_rows(
                db,
                project=project,
                platform=plat,
                rows=rows,
                source=source,
                scraped_at=scraped_at,
                commit=False,
            )
        except ValueError as exc:
            details.append({"platform": plat, "ok": False, "message": str(exc)})
            continue
        for k in ("inserted", "updated", "skipped", "row_count"):
            totals[k] += int(res.get(k) or 0)
        details.append(res)

    db.commit()
    return {
        "ok": True,
        "project": project,
        "inserted": totals["inserted"],
        "updated": totals["updated"],
        "skipped": totals["skipped"],
        "row_count": totals["row_count"],
        "platforms": details,
        "message": (
            f"{project}: +{totals['inserted']} ~{totals['updated']} "
            f"({len(details)} platform)"
        ),
    }


def query_rows(
    db: Session,
    *,
    project: str = "doviz",
    platform: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    proj = (project or "doviz").strip().lower() or "doviz"
    q = select(EmpowerIntelDailyRow).where(EmpowerIntelDailyRow.project == proj)
    if platform:
        q = q.where(EmpowerIntelDailyRow.platform == _norm_platform(platform))
    sd = _parse_date(start) if start else None
    ed = _parse_date(end) if end else None
    if sd:
        q = q.where(EmpowerIntelDailyRow.report_date >= sd)
    if ed:
        q = q.where(EmpowerIntelDailyRow.report_date <= ed)
    q = q.order_by(EmpowerIntelDailyRow.platform, EmpowerIntelDailyRow.report_date)
    rows = db.execute(q.limit(max(1, min(int(limit or 5000), 50000)))).scalars().all()
    out = []
    for r in rows:
        try:
            metrics = json.loads(r.metrics_json or "{}")
        except json.JSONDecodeError:
            metrics = {}
        out.append(
            {
                "project": r.project,
                "platform": r.platform,
                "report_date": r.report_date.isoformat(),
                "metrics": metrics,
                "source": r.source,
                "scraped_at": r.scraped_at.isoformat() if r.scraped_at else "",
                "updated_at": r.updated_at.isoformat() if r.updated_at else "",
            }
        )
    return {"ok": True, "project": proj, "count": len(out), "rows": out}


def summary(db: Session, *, project: str = "doviz") -> dict[str, Any]:
    proj = (project or "doviz").strip().lower() or "doviz"
    rows = (
        db.execute(
            select(EmpowerIntelDailyRow).where(EmpowerIntelDailyRow.project == proj)
        )
        .scalars()
        .all()
    )
    by_plat: dict[str, dict[str, Any]] = {}
    latest_scrape = ""
    for r in rows:
        p = r.platform
        bucket = by_plat.setdefault(
            p,
            {
                "platform": p,
                "row_count": 0,
                "min_date": None,
                "max_date": None,
                "metric_keys": set(),
                "updated_at": "",
            },
        )
        bucket["row_count"] += 1
        d = r.report_date.isoformat()
        if bucket["min_date"] is None or d < bucket["min_date"]:
            bucket["min_date"] = d
        if bucket["max_date"] is None or d > bucket["max_date"]:
            bucket["max_date"] = d
        try:
            metrics = json.loads(r.metrics_json or "{}")
        except json.JSONDecodeError:
            metrics = {}
        if isinstance(metrics, dict):
            bucket["metric_keys"].update(str(k) for k in metrics.keys())
        ua = r.updated_at.isoformat() if r.updated_at else ""
        if ua and ua > str(bucket["updated_at"] or ""):
            bucket["updated_at"] = ua
        sa = r.scraped_at.isoformat() if r.scraped_at else ""
        if sa and sa > latest_scrape:
            latest_scrape = sa

    platforms = []
    for p in ("web", "mweb", "ios", "android"):
        b = by_plat.get(p)
        if not b:
            platforms.append(
                {
                    "platform": p,
                    "row_count": 0,
                    "min_date": "",
                    "max_date": "",
                    "metric_count": 0,
                    "metric_keys": [],
                    "updated_at": "",
                }
            )
            continue
        keys = sorted(b["metric_keys"])
        platforms.append(
            {
                "platform": p,
                "row_count": int(b["row_count"]),
                "min_date": b["min_date"] or "",
                "max_date": b["max_date"] or "",
                "metric_count": len(keys),
                "metric_keys": keys,
                "updated_at": b["updated_at"] or "",
            }
        )
    return {
        "ok": True,
        "project": proj,
        "total_rows": len(rows),
        "latest_scrape": latest_scrape,
        "platforms": platforms,
    }

