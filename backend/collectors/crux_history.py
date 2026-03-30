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
CRUX_CURRENT_ENDPOINT = "https://chromeuxreport.googleapis.com/v1/records:queryRecord"

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


def _candidate_identifiers(domain: str, form_factor: str | None = None) -> list[dict[str, str]]:
    normalized_url = _normalize_url(domain).rstrip("/")
    parsed = urlparse(normalized_url)
    host = parsed.netloc.lower()
    candidates: list[dict[str, str]] = [{"type": "url", "value": normalized_url + "/"}]
    mobile_host = ""
    if form_factor == "PHONE":
        if host.startswith("www."):
            mobile_host = f"m.{host[4:]}"
        elif not host.startswith("m."):
            mobile_host = f"m.{host}"

    if host.startswith("www."):
        naked = host[4:]
        url_candidates = [f"https://{host}/"]
        if mobile_host:
            url_candidates.append(f"https://{mobile_host}/")
            origin_candidates = [f"https://{mobile_host}", f"https://{host}"]
        else:
            origin_candidates = [f"https://{host}"]
        url_candidates.append(f"https://{naked}/")
        origin_candidates.append(f"https://{naked}")
    else:
        url_candidates = [f"https://{host}/"]
        if mobile_host:
            url_candidates.append(f"https://{mobile_host}/")
            origin_candidates = [f"https://{mobile_host}", f"https://{host}"]
        else:
            origin_candidates = [f"https://{host}"]
        url_candidates.append(f"https://www.{host}/")
        origin_candidates.append(f"https://www.{host}")

    for value in url_candidates:
        record = {"type": "url", "value": value}
        if record not in candidates:
            candidates.append(record)
    for value in origin_candidates:
        record = {"type": "origin", "value": value}
        if record not in candidates:
            candidates.append(record)
    return candidates


def _safe_number(raw_value):
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _format_collection_period(period: dict | None) -> dict[str, str] | None:
    if not isinstance(period, dict):
        return None

    def _format_date(value: dict | None) -> str | None:
        if not isinstance(value, dict):
            return None
        year = value.get("year")
        month = value.get("month")
        day = value.get("day")
        if not (year and month and day):
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"

    first_date = _format_date(period.get("firstDate"))
    last_date = _format_date(period.get("lastDate"))
    if not first_date and not last_date:
        return None
    return {
        "first_date": first_date or "",
        "last_date": last_date or "",
    }


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


def _extract_crux_current(record: dict) -> dict[str, dict]:
    metrics = (record.get("metrics") or {})
    current: dict[str, dict] = {}
    for metric_key, short_label in METRIC_LABELS.items():
        metric_payload = metrics.get(metric_key) or {}
        percentiles = metric_payload.get("percentiles") or {}
        histogram = metric_payload.get("histogram") or []
        good_share = None
        if isinstance(histogram, list) and histogram:
            density_value = _safe_number((histogram[0] or {}).get("density"))
            good_share = density_value * 100.0 if density_value is not None else None
        current[metric_key] = {
            "label": short_label,
            "latest": _safe_number(percentiles.get("p75")),
            "good_share": good_share,
        }
    return current


def _request_crux_record(endpoint: str, body_payload: dict, *, request_timeout: int | None = None) -> dict:
    api_key = settings.google_api_key.strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY eksik.")

    body = json.dumps(body_payload).encode("utf-8")
    request = Request(
        f"{endpoint}?key={api_key}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    effective_timeout = int(request_timeout or settings.pagespeed_request_timeout)
    with urlopen(request, timeout=effective_timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_crux_history(
    domain: str,
    form_factor: str,
    *,
    request_timeout: int | None = None,
    max_identifier_attempts: int | None = None,
) -> tuple[dict, dict]:
    last_error: Exception | None = None
    for idx, identifier in enumerate(_candidate_identifiers(domain, form_factor)):
        if max_identifier_attempts and idx >= max_identifier_attempts:
            break
        body_payload = {identifier["type"]: identifier["value"], "formFactor": form_factor, "metrics": list(METRIC_LABELS.keys())}
        try:
            payload = _request_crux_record(CRUX_HISTORY_ENDPOINT, body_payload, request_timeout=request_timeout)
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


def _fetch_crux_current(
    domain: str,
    form_factor: str,
    *,
    request_timeout: int | None = None,
    max_identifier_attempts: int | None = None,
) -> tuple[dict, dict]:
    last_error: Exception | None = None
    for idx, identifier in enumerate(_candidate_identifiers(domain, form_factor)):
        if max_identifier_attempts and idx >= max_identifier_attempts:
            break
        body_payload = {identifier["type"]: identifier["value"], "formFactor": form_factor, "metrics": list(METRIC_LABELS.keys())}
        try:
            payload = _request_crux_record(CRUX_CURRENT_ENDPOINT, body_payload, request_timeout=request_timeout)
            record = payload.get("record") or {}
            return payload, {
                "form_factor": form_factor,
                "target_url": identifier["value"],
                "identifier_type": identifier["type"],
                "collection_period": _format_collection_period(record.get("collectionPeriod")),
                "current": _extract_crux_current(record),
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
    raise RuntimeError("CrUX guncel veri kaydi alinamadi.")


def collect_crux_history(
    db: Session,
    site: Site,
    *,
    request_timeout: int | None = None,
    max_identifier_attempts: int | None = None,
) -> dict:
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
            raw_history_payload, history_summary = _fetch_crux_history(
                site.domain,
                api_form_factor,
                request_timeout=request_timeout,
                max_identifier_attempts=max_identifier_attempts,
            )
            raw_current_payload = {}
            current_summary: dict[str, object] = {}
            current_error = ""
            try:
                raw_current_payload, current_summary = _fetch_crux_current(
                    site.domain,
                    api_form_factor,
                    request_timeout=request_timeout,
                    max_identifier_attempts=max_identifier_attempts,
                )
            except (HTTPError, URLError, TimeoutError, RuntimeError) as exc:
                current_error = str(exc)

            summary = {
                "form_factor": local_key,
                "target_url": str(current_summary.get("target_url") or history_summary.get("target_url") or target_url),
                "identifier_type": str(current_summary.get("identifier_type") or history_summary.get("identifier_type") or ""),
                "history_target_url": str(history_summary.get("target_url") or target_url),
                "history_identifier_type": str(history_summary.get("identifier_type") or ""),
                "current_target_url": str(current_summary.get("target_url") or ""),
                "current_identifier_type": str(current_summary.get("identifier_type") or ""),
                "current_collection_period": current_summary.get("collection_period"),
                "current": current_summary.get("current") or {},
                "series": history_summary.get("series") or {},
                "current_error": current_error,
            }
            save_crux_history_snapshot(
                db,
                site_id=site.id,
                form_factor=local_key,
                target_url=str(summary.get("target_url") or target_url),
                payload={"history": raw_history_payload, "current": raw_current_payload},
                summary=summary,
                collected_at=collected_at,
                collector_run_id=run.id,
            )
            finish_collector_run(
                db,
                run,
                status="success",
                finished_at=collected_at,
                summary={
                    "series_keys": sorted((summary.get("series") or {}).keys()),
                    "current_keys": sorted((summary.get("current") or {}).keys()),
                    "current_error": current_error,
                },
                row_count=(
                    sum(len((metric.get("points") or [])) for metric in (summary.get("series") or {}).values())
                    + len(summary.get("current") or {})
                ),
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
