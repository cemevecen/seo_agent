"""CrUX History API collector."""

from __future__ import annotations

import json
import math
from datetime import datetime
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.warehouse import (
    finish_collector_run,
    get_latest_crux_snapshot,
    save_crux_history_snapshot,
    start_collector_run,
)

CRUX_HISTORY_ENDPOINT = "https://chromeuxreport.googleapis.com/v1/records:queryHistoryRecord"

FORM_FACTORS = {
    "mobile": "PHONE",
    "desktop": "DESKTOP",
}

METRIC_LABELS = {
    "largest_contentful_paint": "LCP",
    "interaction_to_next_paint": "INP",
    "cumulative_layout_shift": "CLS",
    "first_contentful_paint": "FCP",
    "experimental_time_to_first_byte": "TTFB",
}


def _normalize_url(domain: str) -> str:
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    return f"https://{domain}"


def _candidate_identifiers(domain: str) -> list[dict[str, str]]:
    normalized_url = _normalize_url(domain).rstrip("/")
    parsed = urlparse(normalized_url)
    host = parsed.netloc.lower()
    candidates: list[dict[str, str]] = [{"type": "url", "value": normalized_url + "/"}]

    if host.startswith("www."):
        naked = host[4:]
        url_candidates = [f"https://{host}/", f"https://{naked}/"]
        origin_candidates = [f"https://{host}", f"https://{naked}"]
    else:
        url_candidates = [f"https://{host}/", f"https://www.{host}/"]
        origin_candidates = [f"https://{host}", f"https://www.{host}"]

    for value in url_candidates:
        record = {"type": "url", "value": value}
        if record not in candidates:
            candidates.append(record)
    for value in origin_candidates:
        record = {"type": "origin", "value": value}
        if record not in candidates:
            candidates.append(record)
    return candidates


def _extract_crux_points(record: dict) -> dict[str, list[dict]]:
    metrics = (record.get("metrics") or {})
    periods = record.get("collectionPeriods") or record.get("collectionPeriod") or []
    if isinstance(periods, dict):
        periods = [periods]

    labels: list[str] = []
    for period in periods:
        if not isinstance(period, dict):
            labels.append("")
            continue
        last_date = period.get("lastDate") or {}
        year = last_date.get("year")
        month = last_date.get("month")
        day = last_date.get("day")
        labels.append(f"{year:04d}-{month:02d}-{day:02d}" if year and month and day else "")

    def _safe_number(raw_value):
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return None
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    series: dict[str, list[dict]] = {}
    for metric_key, short_label in METRIC_LABELS.items():
        metric_payload = metrics.get(metric_key) or {}
        percentile_payload = metric_payload.get("percentilesTimeseries") or {}
        values = percentile_payload.get("p75s") or percentile_payload.get("p75") or []
        if not isinstance(values, list):
            values = []
        histogram_payload = metric_payload.get("histogramTimeseries") or []
        good_share = None
        if isinstance(histogram_payload, list) and histogram_payload:
            first_bin = histogram_payload[0] or {}
            densities = first_bin.get("densities") or []
            if densities and isinstance(densities, list):
                density_value = _safe_number(densities[-1])
                good_share = density_value * 100.0 if density_value is not None else None

        points: list[dict] = []
        for idx, raw_value in enumerate(values):
            value = _safe_number(raw_value)
            if value is None:
                continue
            points.append(
                {
                    "label": labels[idx] if idx < len(labels) else str(idx + 1),
                    "value": value,
                }
            )
        series[metric_key] = {
            "label": short_label,
            "points": points,
            "latest": points[-1]["value"] if points else None,
            "good_share": good_share,
        }
    return series


def _fetch_crux_history(domain: str, form_factor: str) -> tuple[dict, dict]:
    api_key = settings.google_api_key.strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY eksik.")

    last_error: Exception | None = None
    for identifier in _candidate_identifiers(domain):
        body_payload = {
            identifier["type"]: identifier["value"],
            "formFactor": form_factor,
            "metrics": list(METRIC_LABELS.keys()),
        }
        body = json.dumps(body_payload).encode("utf-8")
        request = Request(
            f"{CRUX_HISTORY_ENDPOINT}?key={api_key}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=settings.pagespeed_request_timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            record = payload.get("record") or {}
            return payload, {
                "form_factor": form_factor,
                "target_url": identifier["value"],
                "identifier_type": identifier["type"],
                "series": _extract_crux_points(record),
            }
        except HTTPError as exc:
            if exc.code == 404:
                last_error = exc
                continue
            raise
        except (URLError, TimeoutError) as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("CrUX history verisi alinamadi.")


def collect_crux_history(db: Session, site: Site) -> dict:
    target_url = _normalize_url(site.domain)
    collected_at = datetime.utcnow()
    output: dict[str, dict] = {}

    for local_key, api_form_factor in FORM_FACTORS.items():
        run = start_collector_run(
            db,
            site_id=site.id,
            provider="crux_history",
            strategy=local_key,
            target_url=target_url,
            requested_at=collected_at,
        )
        try:
            raw_payload, summary = _fetch_crux_history(site.domain, api_form_factor)
            save_crux_history_snapshot(
                db,
                site_id=site.id,
                form_factor=local_key,
                target_url=str(summary.get("target_url") or target_url),
                payload=raw_payload,
                summary=summary,
                collected_at=collected_at,
                collector_run_id=run.id,
            )
            finish_collector_run(
                db,
                run,
                status="success",
                finished_at=collected_at,
                summary={"series_keys": sorted((summary.get("series") or {}).keys())},
                row_count=sum(len((metric.get("points") or [])) for metric in (summary.get("series") or {}).values()),
            )
            output[local_key] = {"state": "live", "summary": summary}
        except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
            fallback = get_latest_crux_snapshot(db, site_id=site.id, form_factor=local_key)
            finish_collector_run(
                db,
                run,
                status="failed" if fallback is None else "stale",
                finished_at=datetime.utcnow(),
                error_message=str(exc),
                summary={"fallback_used": fallback is not None},
                row_count=0,
            )
            output[local_key] = {
                "state": "failed" if fallback is None else "stale",
                "summary": fallback,
                "error": str(exc),
            }
    return output
