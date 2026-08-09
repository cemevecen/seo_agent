"""pagespeed.web.dev scrape snapshot — site bazlı, API/mock ile karışmaz."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.metric_store import save_metrics
from backend.services.warehouse import (
    finish_collector_run,
    save_crux_history_snapshot,
    save_lighthouse_audit_records,
    save_pagespeed_payload_snapshot,
    start_collector_run,
)

LOGGER = logging.getLogger(__name__)

SOURCE = "pagespeed_web_scrape"

_METRIC_KEYS = (
    ("largest_contentful_paint", "LCP", "lcp"),
    ("interaction_to_next_paint", "INP", "inp"),
    ("cumulative_layout_shift", "CLS", "cls"),
    ("first_contentful_paint", "FCP", "fcp"),
    ("experimental_time_to_first_byte", "TTFB", "ttfb"),
)


def _site_by_domain(db: Session, domain: str) -> Site | None:
    d = (domain or "").strip().lower().removeprefix("https://").removeprefix("http://").strip("/")
    if not d:
        return None
    variants = {d, d.removeprefix("www."), f"www.{d.removeprefix('www.')}"}
    return db.query(Site).filter(Site.domain.in_(list(variants))).first()


def _percentile_ms(metrics: dict[str, Any], key: str) -> float | None:
    block = metrics.get(key) if isinstance(metrics, dict) else None
    if not isinstance(block, dict):
        return None
    pct = block.get("percentile")
    if pct is None:
        return None
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return None
    # CLS is unitless * 100 in PSI field sometimes
    if key == "cumulative_layout_shift":
        return v / 100.0 if v > 1.5 else v
    return v


def _hist_shares(metrics: dict[str, Any], key: str) -> tuple[float | None, float | None, float | None]:
    block = metrics.get(key) if isinstance(metrics, dict) else None
    if not isinstance(block, dict):
        return None, None, None
    hist = block.get("distributions") or []
    if not isinstance(hist, list) or len(hist) < 3:
        return None, None, None

    def _p(i: int) -> float | None:
        try:
            return round(float(hist[i].get("proportion") or 0) * 100.0, 1)
        except (TypeError, ValueError, AttributeError, IndexError):
            return None

    return _p(0), _p(1), _p(2)


def _field_metric_card(metrics: dict[str, Any], api_key: str, label: str) -> dict[str, Any]:
    p75 = _percentile_ms(metrics, api_key)
    good, ni, poor = _hist_shares(metrics, api_key)
    return {
        "key": api_key,
        "label": label,
        "latest": p75,
        "good_share": good,
        "ni_share": ni,
        "poor_share": poor,
        "chart": {
            "labels": [],
            "values": [p75] if p75 is not None else [],
            "good_share": [good] if good is not None else [],
            "ni_share": [ni] if ni is not None else [],
            "poor_share": [poor] if poor is not None else [],
        },
    }


def field_series_from_psi_payload(psi: dict[str, Any], *, prefer_origin: bool = False) -> dict[str, dict]:
    """PSI loadingExperience / originLoadingExperience → data-explorer crux kartları."""
    loading = psi.get("originLoadingExperience") if prefer_origin else psi.get("loadingExperience")
    if not isinstance(loading, dict) or not loading.get("metrics"):
        loading = psi.get("loadingExperience") or psi.get("originLoadingExperience") or {}
    metrics = loading.get("metrics") if isinstance(loading, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    out: dict[str, dict] = {}
    for api_key, label, short in _METRIC_KEYS:
        # INP may appear under experimental_ key in older payloads
        use_key = api_key
        if api_key == "interaction_to_next_paint" and api_key not in metrics:
            use_key = "experimental_interaction_to_next_paint"
        card = _field_metric_card(metrics, use_key, label)
        out[short] = card
    return out


def _overall_from_psi(psi: dict[str, Any]) -> str:
    for key in ("loadingExperience", "originLoadingExperience"):
        block = psi.get(key) if isinstance(psi.get(key), dict) else {}
        cat = str(block.get("overall_category") or "").upper()
        if cat:
            return cat
    return ""


def build_crux_compat_payload(psi: dict[str, Any], *, form_factor: str) -> dict[str, Any]:
    """Scrape PSI field metriklerini CrUX history UI formatına çevir (tek nokta — sentetik değil)."""
    loading = psi.get("loadingExperience") if isinstance(psi.get("loadingExperience"), dict) else {}
    origin = psi.get("originLoadingExperience") if isinstance(psi.get("originLoadingExperience"), dict) else {}
    metrics = (loading.get("metrics") or origin.get("metrics") or {}) if isinstance(loading or origin, dict) else {}
    if not metrics and isinstance(origin, dict):
        metrics = origin.get("metrics") or {}

    # Minimal current-style record so _format_crux_series / charts can read percentiles
    record_metrics: dict[str, Any] = {}
    for api_key, _label, _short in _METRIC_KEYS:
        use_key = api_key
        if api_key not in metrics and api_key == "interaction_to_next_paint":
            use_key = "experimental_interaction_to_next_paint"
        block = metrics.get(use_key) if isinstance(metrics, dict) else None
        if not isinstance(block, dict):
            continue
        p75 = block.get("percentile")
        dists = block.get("distributions") or []
        densities = []
        for d in dists[:3]:
            try:
                densities.append(float(d.get("proportion") or 0))
            except (TypeError, ValueError, AttributeError):
                densities.append(0.0)
        while len(densities) < 3:
            densities.append(0.0)
        record_metrics[api_key] = {
            "percentilesTimeseries": {"p75s": [p75]},
            "histogramTimeseries": [
                {"densities": [densities[0]]},
                {"densities": [densities[1]]},
                {"densities": [densities[2]]},
            ],
        }

    today = datetime.utcnow()
    period = {
        "firstDate": {"year": today.year, "month": today.month, "day": max(1, today.day - 27)},
        "lastDate": {"year": today.year, "month": today.month, "day": today.day},
    }
    return {
        "source": SOURCE,
        "form_factor": form_factor,
        "history": {
            "record": {
                "metrics": record_metrics,
                "collectionPeriods": [period],
            }
        },
        "current": {
            "record": {
                "metrics": {
                    k: {
                        "percentiles": {"p75": (metrics.get(k) or {}).get("percentile")},
                        "histogram": (metrics.get(k) or {}).get("distributions") or [],
                    }
                    for k in list(metrics.keys())
                }
            }
        },
        "psi_overall_category": _overall_from_psi(psi),
        "id": psi.get("id"),
        "finalUrl": ((psi.get("lighthouseResult") or {}).get("finalUrl") or psi.get("id")),
    }


def ingest_pagespeed_web_scrape(
    db: Session,
    *,
    domain: str,
    form_factor: str,
    psi_payload: dict[str, Any],
    analysis_url: str = "",
    scope_note: str = "",
) -> dict[str, Any]:
    """pagespeed.web.dev scrape sonucunu warehouse'a yazar (API mock ile karışmaz)."""
    site = _site_by_domain(db, domain)
    if site is None:
        return {"ok": False, "message": f"Site bulunamadı: {domain}"}

    ff = "mobile" if str(form_factor).lower().startswith("m") else "desktop"
    if not isinstance(psi_payload, dict) or not psi_payload:
        return {"ok": False, "message": "psi_payload boş"}

    wrapped = {
        "source": SOURCE,
        "analysis_url": analysis_url,
        "scope_note": scope_note,
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "form_factor": ff,
        "domain": site.domain,
        "psi": psi_payload,
        "mock": False,
    }

    run = start_collector_run(
        db,
        site_id=site.id,
        provider=SOURCE,
        strategy=ff,
        target_url=analysis_url or str(psi_payload.get("id") or site.domain),
        trigger_source="pagespeed_web_scrape",
    )
    now = datetime.utcnow()
    try:
        save_pagespeed_payload_snapshot(
            db,
            site_id=site.id,
            strategy=ff,
            payload=wrapped,
            collected_at=now,
            collector_run_id=run.id if run else None,
        )
        crux_payload = build_crux_compat_payload(psi_payload, form_factor=ff)
        save_crux_history_snapshot(
            db,
            site_id=site.id,
            form_factor=ff,
            target_url=str(psi_payload.get("id") or analysis_url or site.domain),
            payload=crux_payload,
            summary={
                "source": SOURCE,
                "overall_category": _overall_from_psi(psi_payload),
                "analysis_url": analysis_url,
            },
            collected_at=now,
            collector_run_id=run.id if run else None,
        )

        # Lab skor + audit section — pagespeed collector ile aynı warehouse yolları
        lh = psi_payload.get("lighthouseResult") if isinstance(psi_payload.get("lighthouseResult"), dict) else {}
        if lh.get("categories") or lh.get("audits"):
            from backend.collectors.pagespeed import (
                _build_lighthouse_analysis,
                _extract_lighthouse_metrics,
                _flatten_strategy_metrics,
                _save_pagespeed_audit_snapshot,
            )

            try:
                metrics_payload, analysis = (
                    _extract_lighthouse_metrics(psi_payload),
                    _build_lighthouse_analysis(psi_payload, ff),
                )
            except Exception:
                metrics_payload, analysis = {}, {}
            if analysis:
                analysis["source"] = SOURCE
                _save_pagespeed_audit_snapshot(db, site.id, ff, analysis, now)
                try:
                    save_lighthouse_audit_records(
                        db,
                        site_id=site.id,
                        strategy=ff,
                        analysis=analysis,
                        collected_at=now,
                        collector_run_id=run.id if run else None,
                    )
                except Exception:
                    LOGGER.exception("lighthouse audit rows skipped")
            if metrics_payload:
                flat = _flatten_strategy_metrics(ff, metrics_payload)
                if flat:
                    save_metrics(db, site.id, flat, collected_at=now)

        finish_collector_run(
            db,
            run,
            status="success",
            row_count=1,
            summary={"source": SOURCE, "overall": _overall_from_psi(psi_payload)},
        )
        db.commit()
        return {
            "ok": True,
            "site_id": site.id,
            "domain": site.domain,
            "form_factor": ff,
            "overall_category": _overall_from_psi(psi_payload),
            "source": SOURCE,
            "has_lab": bool(lh.get("categories")),
        }
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("pagespeed web scrape ingest failed")
        try:
            finish_collector_run(db, run, status="error", error_message=str(exc)[:400])
            db.commit()
        except Exception:
            db.rollback()
        return {"ok": False, "message": str(exc)[:240]}


def latest_scrape_psi(db: Session, site_id: int, form_factor: str) -> dict[str, Any] | None:
    """En son pagespeed_web_scrape payload'ını döndür."""
    from backend.models import PageSpeedPayloadSnapshot

    ff = "mobile" if str(form_factor).lower().startswith("m") else "desktop"
    rows = (
        db.query(PageSpeedPayloadSnapshot)
        .filter(
            PageSpeedPayloadSnapshot.site_id == site_id,
            PageSpeedPayloadSnapshot.strategy == ff,
        )
        .order_by(PageSpeedPayloadSnapshot.collected_at.desc(), PageSpeedPayloadSnapshot.id.desc())
        .limit(8)
        .all()
    )
    for row in rows:
        try:
            data = json.loads(row.payload_json or "{}")
        except Exception:
            continue
        if isinstance(data, dict) and data.get("source") == SOURCE and isinstance(data.get("psi"), dict):
            return data
        # Ham PSI (eski API) — scrape değil
    return None
