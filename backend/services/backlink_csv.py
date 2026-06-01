"""Search Console Links CSV import (Latest / More sample / Top linking sites)."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import requests
from sqlalchemy.orm import Session

from backend.models import BacklinkDomainAction, BacklinkImport, BacklinkRow, Site
from backend.services.backlink_risk import (
    ACTION_DISAVOW,
    ACTION_IGNORE,
    ACTION_MONITOR,
    ACTION_REVIEW,
    assess_linking_url,
    normalize_domain,
)

LOGGER = logging.getLogger(__name__)

REPORT_TYPES = ("latest_links", "more_sample", "top_linking_sites")

_HEADER_ALIASES: dict[str, list[str]] = {
    "source_url": [
        "bağlantı verilen sayfa",
        "baglanti verilen sayfa",
        "linking page",
        "source page",
        "referring page",
        "referrer page",
        "page url",
        "url",
        "site",
    ],
    "target_url": [
        "hedef sayfa",
        "target page",
        "target url",
        "linked page",
    ],
    "anchor_text": [
        "bağlantı metni",
        "baglanti metni",
        "link text",
        "anchor text",
        "anchor",
    ],
    "last_crawled": [
        "son tarama",
        "last crawled",
        "last crawl date",
    ],
    "linking_site": [
        "bağlantı veren site",
        "linking site",
        "site",
        "domain",
    ],
}


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("\ufeff", "")


def _build_header_map(headers: list[str]) -> dict[str, int]:
    norm_headers = [_norm(h) for h in headers]
    out: dict[str, int] = {}
    for std_key, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            alias_norm = _norm(alias)
            for i, h in enumerate(norm_headers):
                if h == alias_norm:
                    out[std_key] = i
                    break
            if std_key in out:
                break
    return out


def _looks_like_url(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return False
    if re.match(r"^https?://", v, re.I):
        return True
    if "." in v and " " not in v and len(v) < 500:
        return bool(re.search(r"[a-z0-9-]+\.[a-z]{2,}", v, re.I))
    return False


def _cell(row: list[str], idx: int | None) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def parse_csv_text(text: str, *, report_type: str) -> list[dict[str, Any]]:
    """CSV metnini satır dict listesine çevirir."""
    raw = (text or "").strip()
    if not raw:
        return []
    sample = raw[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(raw), dialect=dialect)
    rows = list(reader)
    if not rows:
        return []

    header_idx = 0
    header_map = _build_header_map(rows[0])
    if not header_map.get("source_url") and not header_map.get("linking_site"):
        for i, row in enumerate(rows[:5]):
            if any(_looks_like_url(c) for c in row):
                header_idx = i
                break
        data_rows = rows[header_idx:]
    else:
        data_rows = rows[1:]

    out: list[dict[str, Any]] = []
    for row in data_rows:
        if not row or not any((c or "").strip() for c in row):
            continue
        if not header_map and report_type == "top_linking_sites":
            domain_cell = _cell(row, 0)
            if domain_cell:
                src = domain_cell if _looks_like_url(domain_cell) else f"http://{domain_cell}/"
                out.append(
                    {
                        "source_url": src,
                        "target_url": "",
                        "anchor_text": "",
                        "last_crawled": "",
                    }
                )
            continue

        src = _cell(row, header_map.get("source_url"))
        if not src and header_map.get("linking_site") is not None:
            dom = _cell(row, header_map.get("linking_site"))
            if dom:
                src = dom if _looks_like_url(dom) else f"http://{dom}/"
        if not src:
            for c in row:
                if _looks_like_url(c):
                    src = c.strip()
                    break
        if not src:
            continue
        out.append(
            {
                "source_url": src,
                "target_url": _cell(row, header_map.get("target_url")),
                "anchor_text": _cell(row, header_map.get("anchor_text")),
                "last_crawled": _cell(row, header_map.get("last_crawled")),
            }
        )
    return out


def fetch_public_sheet_csv(url: str, *, timeout: int = 25) -> str:
    """Yayınlanmış Google Sheets CSV export URL'sinden metin çeker."""
    u = (url or "").strip()
    if not u:
        raise ValueError("Sheets URL boş.")
    if "docs.google.com/spreadsheets" in u and "export" not in u:
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", u)
        if m:
            u = f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=csv"
    resp = requests.get(u, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def import_backlink_csv(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    csv_text: str,
    source_filename: str = "",
    source_kind: str = "csv_upload",
) -> dict[str, Any]:
    rt = (report_type or "latest_links").strip().lower()
    if rt not in REPORT_TYPES:
        raise ValueError(f"Geçersiz report_type: {rt}")

    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise ValueError("Site bulunamadı.")

    parsed = parse_csv_text(csv_text, report_type=rt)
    if not parsed:
        raise ValueError("CSV'de geçerli bağlantı satırı bulunamadı.")

    imp = BacklinkImport(
        site_id=site.id,
        report_type=rt,
        source_filename=(source_filename or "")[:255],
        source_kind=(source_kind or "csv_upload")[:32],
        row_count=0,
        created_at=datetime.utcnow(),
    )
    db.add(imp)
    db.flush()

    row_models: list[BacklinkRow] = []
    for item in parsed:
        src = item["source_url"]
        risk = assess_linking_url(
            src,
            anchor_text=item.get("anchor_text") or "",
            target_url=item.get("target_url") or "",
        )
        row_models.append(
            BacklinkRow(
                import_id=imp.id,
                site_id=site.id,
                source_url=src[:2048],
                target_url=(item.get("target_url") or "")[:2048],
                domain=(risk.get("domain") or normalize_domain(src))[:255],
                anchor_text=(item.get("anchor_text") or "")[:512],
                last_crawled=(item.get("last_crawled") or "")[:64],
                risk_score=int(risk.get("risk_score") or 0),
                risk_flags_json=json.dumps(risk.get("risk_flags") or [], ensure_ascii=False),
                recommended_action=str(risk.get("recommended_action") or ACTION_MONITOR),
            )
        )
    db.bulk_save_objects(row_models)
    imp.row_count = len(row_models)
    db.commit()
    db.refresh(imp)

    summary = build_dashboard(db, site_id=site.id, report_type=rt)
    summary["import"] = {
        "id": imp.id,
        "row_count": imp.row_count,
        "created_at": imp.created_at.isoformat() if imp.created_at else None,
        "source_filename": imp.source_filename,
    }
    return summary


def _domain_actions_map(db: Session, site_id: int) -> dict[str, str]:
    rows = db.query(BacklinkDomainAction).filter(BacklinkDomainAction.site_id == site_id).all()
    return {(r.domain or "").lower(): (r.action or "") for r in rows if r.domain}


def _effective_action(recommended: str, override: str | None) -> str:
    o = (override or "").strip().lower()
    if o in (ACTION_IGNORE, ACTION_MONITOR, ACTION_REVIEW, ACTION_DISAVOW):
        return o
    return (recommended or ACTION_MONITOR).strip().lower()


def build_dashboard(db: Session, *, site_id: int, report_type: str = "latest_links") -> dict[str, Any]:
    rt = (report_type or "latest_links").strip().lower()
    imports = (
        db.query(BacklinkImport)
        .filter(BacklinkImport.site_id == site_id, BacklinkImport.report_type == rt)
        .order_by(BacklinkImport.created_at.desc())
        .limit(10)
        .all()
    )
    actions = _domain_actions_map(db, site_id)
    latest = imports[0] if imports else None
    previous = imports[1] if len(imports) > 1 else None

    domain_stats: dict[str, dict[str, Any]] = {}
    diff = {"new_domains": [], "lost_domains": [], "has_previous": bool(previous)}

    if latest:
        rows = db.query(BacklinkRow).filter(BacklinkRow.import_id == latest.id).all()
        for r in rows:
            dom = (r.domain or "").lower()
            if not dom:
                continue
            bucket = domain_stats.setdefault(
                dom,
                {
                    "domain": dom,
                    "link_count": 0,
                    "max_risk_score": 0,
                    "risk_flags": set(),
                    "recommended_action": ACTION_MONITOR,
                    "sample_urls": [],
                },
            )
            bucket["link_count"] += 1
            bucket["max_risk_score"] = max(bucket["max_risk_score"], int(r.risk_score or 0))
            try:
                flags = json.loads(r.risk_flags_json or "[]")
            except json.JSONDecodeError:
                flags = []
            for f in flags:
                bucket["risk_flags"].add(str(f))
            rec = r.recommended_action or ACTION_MONITOR
            if _action_rank(rec) > _action_rank(bucket["recommended_action"]):
                bucket["recommended_action"] = rec
            if len(bucket["sample_urls"]) < 3:
                bucket["sample_urls"].append(r.source_url)

        if previous:
            prev_domains = {
                (d or "").lower()
                for (d,) in db.query(BacklinkRow.domain)
                .filter(BacklinkRow.import_id == previous.id)
                .distinct()
                .all()
                if d
            }
            cur_domains = set(domain_stats.keys())
            diff["new_domains"] = sorted(cur_domains - prev_domains)[:200]
            diff["lost_domains"] = sorted(prev_domains - cur_domains)[:200]

    domains_out: list[dict[str, Any]] = []
    for dom, b in domain_stats.items():
        override = actions.get(dom)
        eff = _effective_action(b["recommended_action"], override)
        domains_out.append(
            {
                "domain": dom,
                "link_count": b["link_count"],
                "max_risk_score": b["max_risk_score"],
                "risk_flags": sorted(b["risk_flags"]),
                "recommended_action": b["recommended_action"],
                "effective_action": eff,
                "operator_action": override,
                "sample_urls": b["sample_urls"],
            }
        )
    domains_out.sort(key=lambda x: (-x["max_risk_score"], -x["link_count"], x["domain"]))

    action_counts = {ACTION_IGNORE: 0, ACTION_MONITOR: 0, ACTION_REVIEW: 0, ACTION_DISAVOW: 0}
    for d in domains_out:
        action_counts[d["effective_action"]] = action_counts.get(d["effective_action"], 0) + 1

    return {
        "site_id": site_id,
        "report_type": rt,
        "imports": [
            {
                "id": i.id,
                "row_count": i.row_count,
                "source_filename": i.source_filename,
                "source_kind": i.source_kind,
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in imports
        ],
        "latest_import_id": latest.id if latest else None,
        "previous_import_id": previous.id if previous else None,
        "diff": diff,
        "action_counts": action_counts,
        "domains": domains_out[:500],
        "domain_total": len(domains_out),
    }


def _action_rank(action: str) -> int:
    order = {ACTION_IGNORE: 0, ACTION_MONITOR: 1, ACTION_REVIEW: 2, ACTION_DISAVOW: 3}
    return order.get((action or "").lower(), 1)


def set_domain_action(db: Session, *, site_id: int, domain: str, action: str) -> dict[str, Any]:
    dom = normalize_domain(domain) or (domain or "").strip().lower()
    if not dom:
        raise ValueError("Domain boş.")
    act = (action or "").strip().lower()
    if act not in (ACTION_IGNORE, ACTION_MONITOR, ACTION_REVIEW, ACTION_DISAVOW):
        raise ValueError("Geçersiz aksiyon.")
    row = (
        db.query(BacklinkDomainAction)
        .filter(BacklinkDomainAction.site_id == site_id, BacklinkDomainAction.domain == dom)
        .first()
    )
    if row is None:
        row = BacklinkDomainAction(site_id=site_id, domain=dom, action=act, updated_at=datetime.utcnow())
        db.add(row)
    else:
        row.action = act
        row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "site_id": site_id, "domain": dom, "action": act}


def build_disavow_text(db: Session, *, site_id: int, report_type: str = "latest_links") -> str:
    dash = build_dashboard(db, site_id=site_id, report_type=report_type)
    lines = ["# ProjectControl — GSC backlink disavow taslağı", f"# report_type={report_type}", ""]
    for d in dash.get("domains") or []:
        if d.get("effective_action") == ACTION_DISAVOW:
            lines.append(f"domain:{d['domain']}")
    if len(lines) <= 3:
        lines.append("# (disavow adayı domain yok)")
    return "\n".join(lines) + "\n"
