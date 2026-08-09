"""GSC CWV + AMP scrape ingest, history, regression e-posta."""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session

from backend.models import GscCwvReportSnapshot, Site
from backend.services.ga4_page_urls import ga4_site_host
from backend.services.warehouse import finish_collector_run, start_collector_run

LOGGER = logging.getLogger(__name__)
SOURCE = "gsc_cwv_scrape"

# Eşikler — Good → kötüleşme / sayım düşüşü
GOOD_DROP_PCT = 5.0          # good URL sayısı % düşüşü
POOR_INCREASE_ABS = 10       # poor mutlak artış
NI_INCREASE_PCT = 8.0        # needs improvement % artış
EXAMPLE_REGRESSION = True    # good örnek URL poor/NI'ye düşerse mail


def resolve_site(db: Session, site_domain: str) -> Site | None:
    want = (site_domain or "").strip().lower()
    if not want:
        return None
    naked = want[4:] if want.startswith("www.") else want
    candidates = {want, naked, f"www.{naked}"}
    sites = db.query(Site).filter(Site.is_active.is_(True)).all()
    for s in sites:
        d = (s.domain or "").strip().lower()
        if d in candidates:
            return s
        host = (ga4_site_host(s.domain) or "").lower()
        if host in candidates:
            return s
    for key in ("doviz.com", "sinemalar.com"):
        if key in want:
            for s in sites:
                d = (s.domain or "").lower()
                if key in d and "canli" not in d:
                    return s
    return None


def _url_set_from_device(dev: dict[str, Any], *, bucket: str) -> set[str]:
    urls: set[str] = set()
    if bucket == "good":
        for r in dev.get("good_urls") or []:
            u = (r.get("url") or "").strip()
            if u:
                urls.add(u)
        return urls
    for d in dev.get("issue_drilldowns") or []:
        st = (d.get("status") or "").lower()
        if bucket == "poor" and st != "poor":
            continue
        if bucket == "needs_improvement" and st not in ("needs_improvement", "ni", "warning"):
            continue
        for r in d.get("url_rows") or []:
            u = (r.get("url") or "").strip()
            if u:
                urls.add(u)
    return urls


def _collect_problem_urls(payload: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("mobile", "desktop"):
        out |= _url_set_from_device(payload.get(key) or {}, bucket="poor")
        out |= _url_set_from_device(payload.get(key) or {}, bucket="needs_improvement")
    for iss in (payload.get("amp") or {}).get("issues") or []:
        for r in iss.get("url_rows") or []:
            u = (r.get("url") or "").strip()
            if u:
                out.add(u)
    return out


def detect_regressions(
    prev: dict[str, Any] | None,
    curr: dict[str, Any],
) -> list[dict[str, Any]]:
    """Eşik + örnek URL regresyonları."""
    if not prev:
        return []
    alerts: list[dict[str, Any]] = []
    for device in ("mobile", "desktop"):
        p = (prev.get(device) or {}).get("kpis") or {}
        c = (curr.get(device) or {}).get("kpis") or {}
        pg, cg = int(p.get("good") or 0), int(c.get("good") or 0)
        pp, cp = int(p.get("poor") or 0), int(c.get("poor") or 0)
        pn, cn = int(p.get("needs_improvement") or 0), int(c.get("needs_improvement") or 0)
        if pg > 0 and cg < pg:
            drop_pct = (pg - cg) / pg * 100.0
            if drop_pct >= GOOD_DROP_PCT:
                alerts.append(
                    {
                        "kind": "good_drop",
                        "device": device,
                        "message": (
                            f"{device}: Good URL {pg:,} → {cg:,} (−{drop_pct:.1f}% ≥ {GOOD_DROP_PCT}%)"
                        ),
                        "prev_good": pg,
                        "curr_good": cg,
                    }
                )
        if cp - pp >= POOR_INCREASE_ABS:
            alerts.append(
                {
                    "kind": "poor_increase",
                    "device": device,
                    "message": f"{device}: Poor URL {pp:,} → {cp:,} (+{cp - pp})",
                }
            )
        if pn > 0 and cn > pn:
            ni_pct = (cn - pn) / pn * 100.0
            if ni_pct >= NI_INCREASE_PCT:
                alerts.append(
                    {
                        "kind": "ni_increase",
                        "device": device,
                        "message": f"{device}: Needs improvement {pn:,} → {cn:,} (+{ni_pct:.1f}%)",
                    }
                )
        if EXAMPLE_REGRESSION:
            prev_good = _url_set_from_device(prev.get(device) or {}, bucket="good")
            curr_bad = _url_set_from_device(curr.get(device) or {}, bucket="poor") | _url_set_from_device(
                curr.get(device) or {}, bucket="needs_improvement"
            )
            moved = sorted(prev_good & curr_bad)
            if moved:
                alerts.append(
                    {
                        "kind": "url_regression",
                        "device": device,
                        "message": (
                            f"{device}: {len(moved)} örnek URL Good listesinden Poor/NI tarafına geçti"
                        ),
                        "urls": moved[:40],
                    }
                )
    return alerts


def _send_regression_email(site: Site, payload: dict[str, Any], alerts: list[dict[str, Any]]) -> bool:
    if not alerts:
        return False
    try:
        from backend.config import settings
        from backend.services.mailer import send_email
    except Exception:
        return False
    if not getattr(settings, "outbound_email_enabled", False):
        LOGGER.info("CWV alert mail atlandı (outbound_email_enabled=false)")
        return False
    to_addr = (
        getattr(settings, "operations_mail_to", None)
        or getattr(settings, "mail_to", None)
        or ""
    )
    domain = site.domain or payload.get("site_domain") or ""
    rows = "".join(
        f"<li style='margin:6px 0'><b>{html.escape(str(a.get('kind') or ''))}</b> — "
        f"{html.escape(str(a.get('message') or ''))}"
        + (
            "<ul>"
            + "".join(
                f"<li style='font-size:12px;word-break:break-all'>{html.escape(str(u))}</li>"
                for u in (a.get("urls") or [])[:15]
            )
            + "</ul>"
            if a.get("urls")
            else ""
        )
        + "</li>"
        for a in alerts
    )
    totals = payload.get("totals") or {}
    safe_domain = html.escape(str(domain))
    body_html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:680px">
      <p style="font-size:16px;font-weight:700;color:#b45309;margin:0 0 8px">Web Vitals eşik uyarısı · {safe_domain}</p>
      <p style="font-size:13px;color:#64748b;margin:0 0 16px">
        Scrape sonrası regresyon · Poor {int(totals.get('poor') or 0)} ·
        NI {int(totals.get('needs_improvement') or 0)} · Good {int(totals.get('good') or 0)}
      </p>
      <ul style="padding-left:18px;color:#334155;font-size:13px">{rows}</ul>
      <p style="font-size:12px;color:#94a3b8;margin-top:18px">
        Panel: <a href="https://projectcontrol.up.railway.app/web-vitals?site_id={int(site.id)}">/web-vitals</a>
      </p>
    </div>
    """
    subject = f"⚠ Web Vitals regresyon · {domain} · {len(alerts)} uyarı"
    try:
        send_email(subject, body_html, recipients=[to_addr] if to_addr else None)
        LOGGER.info("CWV regression mail gönderildi site=%s alerts=%d", domain, len(alerts))
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("CWV mail fail: %s", exc)
        return False


def latest_snapshot(db: Session, site_id: int) -> GscCwvReportSnapshot | None:
    return (
        db.query(GscCwvReportSnapshot)
        .filter(GscCwvReportSnapshot.site_id == site_id)
        .order_by(GscCwvReportSnapshot.collected_at.desc(), GscCwvReportSnapshot.id.desc())
        .first()
    )


def history_points(db: Session, site_id: int, *, limit: int = 60) -> list[dict[str, Any]]:
    """Son N scrape noktasını kronolojik (eski→yeni) döner."""
    lim = max(5, min(200, int(limit or 60)))
    rows = (
        db.query(GscCwvReportSnapshot)
        .filter(GscCwvReportSnapshot.site_id == site_id)
        .order_by(
            GscCwvReportSnapshot.collected_at.desc(),
            GscCwvReportSnapshot.id.desc(),
        )
        .limit(lim)
        .all()
    )
    rows = list(reversed(rows))
    out = []
    for r in rows:
        out.append(
            {
                "collected_at": r.collected_at.isoformat() if r.collected_at else "",
                "poor": int(r.poor_count or 0),
                "needs_improvement": int(r.ni_count or 0),
                "good": int(r.good_count or 0),
                "amp_url_count": int(r.amp_url_count or 0),
            }
        )
    return out


def _dedupe_drilldowns(payload: dict[str, Any]) -> dict[str, Any]:
    """Aynı item_key iki kez gelmesin (click + fallback)."""
    for key in ("mobile", "desktop"):
        dev = payload.get(key)
        if not isinstance(dev, dict):
            continue
        seen: set[str] = set()
        clean: list[dict[str, Any]] = []
        for d in dev.get("issue_drilldowns") or []:
            if not isinstance(d, dict):
                continue
            ik = str(d.get("item_key") or "").strip()
            title = str(d.get("title") or "").strip().lower()
            sig = ik or f"t:{title}"
            if sig in seen:
                continue
            seen.add(sig)
            clean.append(d)
        dev["issue_drilldowns"] = clean
    amp = payload.get("amp")
    if isinstance(amp, dict):
        seen_a: set[str] = set()
        clean_a: list[dict[str, Any]] = []
        for d in amp.get("issues") or []:
            if not isinstance(d, dict):
                continue
            ik = str(d.get("item_key") or "").strip()
            if ik and ik in seen_a:
                continue
            if ik:
                seen_a.add(ik)
            clean_a.append(d)
        amp["issues"] = clean_a
        amp["url_row_count"] = sum(int(i.get("url_row_count") or 0) for i in clean_a)
    return payload


def ingest_gsc_cwv_payload(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    snaps = list(payload.get("snapshots") or [])
    if not snaps:
        return {"ok": False, "message": "snapshots boş"}

    saved = 0
    mailed = 0
    errors: list[str] = []
    for snap in snaps:
        if not isinstance(snap, dict) or snap.get("error"):
            errors.append(str((snap or {}).get("error") or "bad snapshot"))
            continue
        site = resolve_site(db, str(snap.get("site_domain") or ""))
        if site is None:
            errors.append(f"site yok: {snap.get('site_domain')}")
            continue

        prev_row = latest_snapshot(db, site.id)
        prev_payload = None
        if prev_row and prev_row.payload_json:
            try:
                prev_payload = json.loads(prev_row.payload_json)
            except Exception:
                prev_payload = None

        snap = _dedupe_drilldowns(dict(snap))
        totals = snap.get("totals") or {}
        poor = int(totals.get("poor") or 0)
        ni = int(totals.get("needs_improvement") or 0)
        good = int(totals.get("good") or 0)
        amp_n = int((snap.get("amp") or {}).get("url_row_count") or 0)

        run = start_collector_run(
            db,
            site_id=site.id,
            provider="gsc_cwv",
            strategy="scrape",
            target_url=f"https://search.google.com/search-console/core-web-vitals?resource_id={quote(str(snap.get('resource_id') or ''), safe='')}",
            trigger_source=SOURCE,
        )
        row = GscCwvReportSnapshot(
            site_id=site.id,
            resource_id=str(snap.get("resource_id") or ""),
            source=SOURCE,
            payload_json=json.dumps(snap, ensure_ascii=False),
            poor_count=poor,
            ni_count=ni,
            good_count=good,
            amp_url_count=amp_n,
            collected_at=datetime.utcnow(),
        )
        db.add(row)
        finish_collector_run(
            db,
            run,
            status="success",
            row_count=poor + ni + good + amp_n,
            summary={"poor": poor, "ni": ni, "good": good, "amp": amp_n},
        )
        db.commit()
        saved += 1

        alerts = detect_regressions(prev_payload, snap)
        if alerts and _send_regression_email(site, snap, alerts):
            mailed += 1

    ok = saved > 0
    return {
        "ok": ok,
        "saved": saved,
        "mailed": mailed,
        "errors": errors,
        "message": f"GSC CWV ingest · {saved} snapshot" + (f" · {mailed} mail" if mailed else ""),
        "source": SOURCE,
    }


def build_panel_context(db: Session, site: Site) -> dict[str, Any]:
    row = latest_snapshot(db, site.id)
    payload: dict[str, Any] = {}
    if row and row.payload_json:
        try:
            payload = json.loads(row.payload_json)
        except Exception:
            payload = {}
    hist = history_points(db, site.id)
    rid = payload.get("resource_id") or (
        "sc-domain:doviz.com" if "doviz" in (site.domain or "") else f"https://{site.domain}/"
    )
    return {
        "payload": payload,
        "history": hist,
        "collected_at": row.collected_at.isoformat() if row and row.collected_at else "",
        "gsc_links": {
            "main": f"https://search.google.com/u/0/search-console/core-web-vitals?resource_id={quote(str(rid), safe='')}",
            "mobile_summary": f"https://search.google.com/u/0/search-console/core-web-vitals/summary?resource_id={quote(str(rid), safe='')}&device=2",
            "desktop_summary": f"https://search.google.com/u/0/search-console/core-web-vitals/summary?resource_id={quote(str(rid), safe='')}&device=1",
            "amp": f"https://search.google.com/u/0/search-console/amp?resource_id={quote(str(rid), safe='')}",
        },
        "thresholds": {
            "good_drop_pct": GOOD_DROP_PCT,
            "poor_increase_abs": POOR_INCREASE_ABS,
            "ni_increase_pct": NI_INCREASE_PCT,
        },
    }
