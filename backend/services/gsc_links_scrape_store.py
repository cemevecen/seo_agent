"""GSC Links scrape → BacklinkImport snapshot store."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session

from backend.models import BacklinkImport, BacklinkRow, Site
from backend.services import backlink_csv
from backend.services.backlink_risk import (
    ACTION_IGNORE,
    ACTION_MONITOR,
    assess_linking_url,
    normalize_domain,
)
from backend.services.ga4_page_urls import ga4_site_host

LOGGER = logging.getLogger(__name__)

LINK_TYPE_TO_REPORT: dict[str, str] = {
    "EXTERNAL": "external",
    "DOMAIN": "domain",
    "ANCHOR_TEXT": "anchor_text",
    "INTERNAL": "internal",
}

REPORT_TO_LINK_TYPE: dict[str, str] = {v: k for k, v in LINK_TYPE_TO_REPORT.items()}

# Panel site.domain → GSC resource listesi
SITE_GSC_PROPERTIES: dict[str, list[dict[str, str]]] = {
    "www.doviz.com": [
        {"resource_id": "sc-domain:doviz.com", "label": "doviz.com (domain)"},
        {"resource_id": "sc-domain:m.doviz.com", "label": "m.doviz.com"},
    ],
    "doviz.com": [
        {"resource_id": "sc-domain:doviz.com", "label": "doviz.com (domain)"},
        {"resource_id": "sc-domain:m.doviz.com", "label": "m.doviz.com"},
    ],
    "www.sinemalar.com": [
        {"resource_id": "https://www.sinemalar.com/", "label": "www.sinemalar.com"},
        {"resource_id": "https://m.sinemalar.com/", "label": "m.sinemalar.com"},
    ],
    "sinemalar.com": [
        {"resource_id": "https://www.sinemalar.com/", "label": "www.sinemalar.com"},
        {"resource_id": "https://m.sinemalar.com/", "label": "m.sinemalar.com"},
    ],
}


def resolve_site_domain(db: Session, site_domain: str) -> Site | None:
    want = (site_domain or "").strip().lower()
    if not want:
        return None
    naked = want[4:] if want.startswith("www.") else want
    candidates = [want, naked, f"www.{naked}"]
    sites = db.query(Site).filter(Site.is_active.is_(True)).all()
    for s in sites:
        d = (s.domain or "").strip().lower()
        if d in candidates:
            return s
        host = ga4_site_host(s.domain) or ""
        if host in candidates or host.endswith("." + naked) or naked.endswith("." + host):
            if naked in {"doviz.com", "sinemalar.com"} and naked in (host, d, d.replace("www.", "")):
                return s
    # fallback: domain contains key
    for key in ("doviz.com", "sinemalar.com"):
        if key in want:
            for s in sites:
                d = (s.domain or "").lower()
                if key in d and "canli" not in d:
                    return s
    return None


def properties_for_site(site: Site) -> list[dict[str, str]]:
    d = (site.domain or "").strip().lower()
    if d in SITE_GSC_PROPERTIES:
        return list(SITE_GSC_PROPERTIES[d])
    naked = d[4:] if d.startswith("www.") else d
    if naked in SITE_GSC_PROPERTIES:
        return list(SITE_GSC_PROPERTIES[naked])
    if "doviz.com" in d:
        return list(SITE_GSC_PROPERTIES["www.doviz.com"])
    if "sinemalar.com" in d:
        return list(SITE_GSC_PROPERTIES["www.sinemalar.com"])
    return []


def _rows_from_external(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        tgt = (r.get("target_url") or "").strip()
        if not tgt:
            continue
        inc = int(r.get("incoming_links") or 0)
        sites = int(r.get("linking_sites") or 0)
        out.append(
            {
                "source_url": tgt,
                "target_url": tgt,
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{inc}:{sites}",
                "last_crawled": "",
                "incoming_links": inc,
                "linking_sites": sites,
                "is_top_target_aggregate": True,
            }
        )
    return out


def _rows_from_internal(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        tgt = (r.get("target_url") or "").strip()
        if not tgt:
            continue
        inc = int(r.get("incoming_links") or 0)
        out.append(
            {
                "source_url": tgt,
                "target_url": tgt,
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{inc}:0",
                "last_crawled": "",
                "incoming_links": inc,
                "linking_sites": 0,
                "is_top_target_aggregate": True,
            }
        )
    return out


def _rows_from_domain(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        dom = (r.get("linking_site") or "").strip().lower()
        if not dom:
            continue
        pages = int(r.get("linking_pages") or 0)
        targets = int(r.get("target_pages") or 0)
        src = dom if dom.startswith("http") else f"http://{dom}/"
        out.append(
            {
                "source_url": src,
                "target_url": "",
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{pages}:{targets}",
                "last_crawled": "",
                "incoming_links": pages,
                "linking_sites": targets,
                "is_linking_site_aggregate": True,
            }
        )
    return out


def _rows_from_anchor(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        text = (r.get("anchor_text") or "").strip()
        if not text:
            continue
        rank = int(r.get("rank") or 0)
        out.append(
            {
                "source_url": f"gsc-anchor://{quote(text, safe='')}",
                "target_url": "",
                "anchor_text": text[:512],
                "last_crawled": str(rank),
                "is_anchor_rank": True,
            }
        )
    return out


def ingest_snapshot(
    db: Session,
    *,
    site: Site,
    resource_id: str,
    link_type: str,
    rows: list[dict[str, Any]],
    kpis: dict[str, Any] | None = None,
    property_label: str = "",
    scraped_at: str = "",
) -> dict[str, Any]:
    lt = (link_type or "").strip().upper()
    report_type = LINK_TYPE_TO_REPORT.get(lt)
    if not report_type:
        return {"ok": False, "message": f"Geçersiz link_type: {link_type}"}

    if lt == "EXTERNAL":
        parsed = _rows_from_external(rows)
    elif lt == "INTERNAL":
        parsed = _rows_from_internal(rows)
    elif lt == "DOMAIN":
        parsed = _rows_from_domain(rows)
    else:
        parsed = _rows_from_anchor(rows)

    if not parsed:
        return {"ok": False, "message": "Snapshot satırı yok", "report_type": report_type}

    meta = {
        "kpis": kpis or {},
        "property_label": property_label,
        "scraped_at": scraped_at,
        "link_type": lt,
        "resource_id": resource_id,
        "row_count_raw": len(rows),
    }

    is_target_agg = lt in {"EXTERNAL", "INTERNAL"}
    is_domain_agg = lt == "DOMAIN"
    is_anchor = lt == "ANCHOR_TEXT"

    imp = BacklinkImport(
        site_id=site.id,
        report_type=report_type,
        source_filename=f"gsc_scrape|{resource_id}|{lt}"[:255],
        source_kind="gsc_scrape",
        row_count=0,
        created_at=datetime.utcnow(),
        gsc_resource_id=(resource_id or "")[:255],
        meta_json=json.dumps(meta, ensure_ascii=False),
    )
    db.add(imp)
    db.flush()

    row_models: list[BacklinkRow] = []
    batch_seen: set[str] = set()

    for item in parsed:
        if is_target_agg:
            tgt = (item.get("target_url") or item.get("source_url") or "").strip()
            if not tgt:
                continue
            # Scrape property scope — site host filter daha gevşek (m./kur. dahil)
            if not backlink_csv.target_url_belongs_to_site(tgt, site.domain or ""):
                # m.doviz property can still list www targets; keep if same registrable
                host = ""
                try:
                    from urllib.parse import urlparse

                    host = (urlparse(tgt).hostname or "").lower()
                except Exception:
                    host = ""
                site_host = (ga4_site_host(site.domain) or "").lower()
                naked = site_host[4:] if site_host.startswith("www.") else site_host
                if not host or not naked or naked not in host:
                    continue
            tkey = backlink_csv._canonical_target_key(tgt, site.domain or "")  # noqa: SLF001
            if tkey in batch_seen:
                continue
            batch_seen.add(tkey)
            from urllib.parse import urlparse

            host_dom = urlparse(tgt if tgt.startswith("http") else f"https://{tgt}").hostname or ""
            dom = (normalize_domain(host_dom or tgt) or ga4_site_host(site.domain) or "target")[:255]
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=tgt[:2048],
                    target_url=tgt[:2048],
                    domain=dom.lower(),
                    anchor_text=(item.get("anchor_text") or "")[:512],
                    last_crawled="",
                    risk_score=0,
                    risk_flags_json="[]",
                    recommended_action=ACTION_IGNORE,
                )
            )
            continue

        if is_domain_agg:
            src = item["source_url"]
            risk = assess_linking_url(src, anchor_text="", target_url="")
            dom = ((risk.get("domain") or normalize_domain(src)) or "").lower()[:255]
            if not dom or dom in batch_seen:
                continue
            batch_seen.add(dom)
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=src[:2048],
                    target_url="",
                    domain=dom,
                    anchor_text=(item.get("anchor_text") or "")[:512],
                    last_crawled="",
                    risk_score=int(risk.get("risk_score") or 0),
                    risk_flags_json=json.dumps(risk.get("risk_flags") or [], ensure_ascii=False),
                    recommended_action=str(risk.get("recommended_action") or ACTION_MONITOR),
                )
            )
            continue

        if is_anchor:
            text = (item.get("anchor_text") or "").strip()
            if not text:
                continue
            key = text.lower()
            if key in batch_seen:
                continue
            batch_seen.add(key)
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=(item.get("source_url") or f"gsc-anchor://{quote(text)}")[:2048],
                    target_url="",
                    domain="gsc-anchor",
                    anchor_text=text[:512],
                    last_crawled=(item.get("last_crawled") or "")[:64],
                    risk_score=0,
                    risk_flags_json="[]",
                    recommended_action=ACTION_IGNORE,
                )
            )

    if row_models:
        db.bulk_save_objects(row_models)
    imp.row_count = len(row_models)
    db.commit()
    db.refresh(imp)
    return {
        "ok": True,
        "import_id": imp.id,
        "site_id": site.id,
        "report_type": report_type,
        "gsc_resource_id": resource_id,
        "row_count": imp.row_count,
        "link_type": lt,
    }


def ingest_gsc_links_payload(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    snapshots = payload.get("snapshots") or []
    if not isinstance(snapshots, list) or not snapshots:
        return {"ok": False, "message": "snapshots boş"}

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for snap in snapshots:
        if not isinstance(snap, dict):
            continue
        site_domain = (snap.get("site_domain") or "").strip()
        site = resolve_site_domain(db, site_domain)
        if site is None:
            errors.append(f"Site bulunamadı: {site_domain}")
            continue
        if not snap.get("ok") and not (snap.get("rows") or []):
            errors.append(f"{snap.get('property_label') or site_domain} · {snap.get('link_type')}: boş")
            continue
        try:
            one = ingest_snapshot(
                db,
                site=site,
                resource_id=str(snap.get("resource_id") or ""),
                link_type=str(snap.get("link_type") or ""),
                rows=list(snap.get("rows") or []),
                kpis=dict(snap.get("kpis") or {}),
                property_label=str(snap.get("property_label") or ""),
                scraped_at=str(snap.get("scraped_at") or payload.get("scraped_at") or ""),
            )
            results.append(one)
            if not one.get("ok"):
                errors.append(one.get("message") or "ingest fail")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("gsc links ingest snapshot failed")
            errors.append(str(exc))
            db.rollback()

    ok_n = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_n > 0,
        "message": f"GSC Links ingest · {ok_n}/{len(snapshots)} snapshot"
        + (f" · {len(errors)} hata" if errors else ""),
        "imported": results,
        "errors": errors[:20],
        "source": payload.get("source") or "gsc_links_bridge",
    }


def build_change_window(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    gsc_resource_id: str = "",
    window: str = "daily",
) -> dict[str, Any]:
    """Günlük (~24s) veya haftalık (~7g) snapshot farkı + KPI serisi."""
    rt = (report_type or "external").strip().lower()
    # legacy aliases
    if rt == "top_target_pages":
        rt = "external"
    if rt == "top_target_pages_internal":
        rt = "internal"

    hours = 24 if (window or "daily").lower() in {"daily", "day", "1d"} else 24 * 7
    now = datetime.utcnow()
    q = db.query(BacklinkImport).filter(
        BacklinkImport.site_id == site_id,
        BacklinkImport.report_type == rt,
    )
    rid = (gsc_resource_id or "").strip()
    if rid:
        q = q.filter(BacklinkImport.gsc_resource_id == rid)
    imports = q.order_by(BacklinkImport.created_at.desc()).limit(40).all()
    if not imports:
        return {
            "ok": True,
            "window": window,
            "has_baseline": False,
            "message": "Henüz scrape snapshot yok",
            "series": [],
            "diff": {},
        }

    latest = imports[0]
    baseline = None
    target_ts = now - timedelta(hours=hours)
    # en yakın geçmiş snapshot (target_ts civarı veya daha eski)
    older = [i for i in imports[1:] if i.created_at and i.created_at <= target_ts + timedelta(hours=6)]
    if older:
        baseline = min(older, key=lambda i: abs((i.created_at - target_ts).total_seconds()))
    elif len(imports) > 1:
        baseline = imports[-1] if hours >= 24 * 6 else imports[1]

    def _keys(imp: BacklinkImport) -> dict[str, Any]:
        rows = db.query(BacklinkRow).filter(BacklinkRow.import_id == imp.id).all()
        if rt in {"external", "internal"}:
            return {
                r.source_url: {
                    "key": r.source_url,
                    "label": r.source_url,
                    "count": _agg_incoming(r.anchor_text),
                    "sites": _agg_sites(r.anchor_text),
                }
                for r in rows
                if r.source_url
            }
        if rt == "domain":
            return {
                (r.domain or "").lower(): {
                    "key": (r.domain or "").lower(),
                    "label": r.domain,
                    "count": _agg_incoming(r.anchor_text),
                    "sites": _agg_sites(r.anchor_text),
                }
                for r in rows
                if r.domain
            }
        # anchor
        return {
            (r.anchor_text or "").lower(): {
                "key": (r.anchor_text or "").lower(),
                "label": r.anchor_text,
                "rank": int(r.last_crawled or 0) if str(r.last_crawled or "").isdigit() else 0,
                "count": 0,
            }
            for r in rows
            if r.anchor_text
        }

    latest_map = _keys(latest)
    base_map = _keys(baseline) if baseline else {}
    new_keys = sorted(set(latest_map) - set(base_map))
    lost_keys = sorted(set(base_map) - set(latest_map))
    changed: list[dict[str, Any]] = []
    for k in sorted(set(latest_map) & set(base_map)):
        a = latest_map[k]
        b = base_map[k]
        if rt == "anchor_text":
            if int(a.get("rank") or 0) != int(b.get("rank") or 0):
                changed.append(
                    {
                        "key": k,
                        "label": a.get("label"),
                        "rank_from": b.get("rank"),
                        "rank_to": a.get("rank"),
                        "delta_rank": int(b.get("rank") or 0) - int(a.get("rank") or 0),
                    }
                )
        else:
            dcount = int(a.get("count") or 0) - int(b.get("count") or 0)
            if dcount:
                changed.append(
                    {
                        "key": k,
                        "label": a.get("label"),
                        "count_from": b.get("count"),
                        "count_to": a.get("count"),
                        "delta": dcount,
                    }
                )
    changed.sort(key=lambda x: abs(int(x.get("delta") or x.get("delta_rank") or 0)), reverse=True)

    series = []
    for imp in reversed(imports[:14]):
        meta = {}
        try:
            meta = json.loads(imp.meta_json or "{}")
        except json.JSONDecodeError:
            meta = {}
        kpis = meta.get("kpis") or {}
        series.append(
            {
                "import_id": imp.id,
                "created_at": imp.created_at.isoformat() if imp.created_at else None,
                "row_count": imp.row_count,
                "total_links": kpis.get("total_links"),
                "gsc_resource_id": imp.gsc_resource_id or "",
            }
        )

    latest_meta = {}
    try:
        latest_meta = json.loads(latest.meta_json or "{}")
    except json.JSONDecodeError:
        latest_meta = {}

    return {
        "ok": True,
        "window": "daily" if hours <= 24 else "weekly",
        "window_hours": hours,
        "has_baseline": bool(baseline),
        "report_type": rt,
        "gsc_resource_id": rid or (latest.gsc_resource_id or ""),
        "latest": {
            "import_id": latest.id,
            "created_at": latest.created_at.isoformat() if latest.created_at else None,
            "row_count": latest.row_count,
            "kpis": (latest_meta.get("kpis") or {}),
            "label": latest.source_filename,
        },
        "baseline": (
            {
                "import_id": baseline.id,
                "created_at": baseline.created_at.isoformat() if baseline.created_at else None,
                "row_count": baseline.row_count,
                "label": baseline.source_filename,
            }
            if baseline
            else None
        ),
        "diff": {
            "new_count": len(new_keys),
            "lost_count": len(lost_keys),
            "changed_count": len(changed),
            "new": [latest_map[k] for k in new_keys[:200]],
            "lost": [base_map[k] for k in lost_keys[:200]],
            "changed": changed[:200],
        },
        "series": series,
    }


def _agg_incoming(anchor: str) -> int:
    a, _ = backlink_csv._parse_gsc_agg_anchor(anchor)  # noqa: SLF001
    return int(a or 0)


def _agg_sites(anchor: str) -> int:
    _, b = backlink_csv._parse_gsc_agg_anchor(anchor)  # noqa: SLF001
    return int(b or 0)
