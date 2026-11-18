"""Ana sayfa — izlenen Döviz URL’lerinin GA4 oturum trafiği (1g / 7g / 30g vs önceki dönem)."""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any
from urllib.parse import urlparse

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)
from sqlalchemy.orm import Session

from backend.collectors.ga4 import _calendar_windows, _client
from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties

LOGGER = logging.getLogger(__name__)

# Kullanıcının istediği sabit liste (sıra korunur).
WATCH_URLS: list[dict[str, str]] = [
    {"url": "https://kur.doviz.com/", "label": "Kur"},
    {"url": "https://altin.doviz.com/", "label": "Altın"},
    {"url": "https://www.doviz.com/emtia", "label": "Emtia"},
    {"url": "https://www.doviz.com/pariteler", "label": "Pariteler"},
    {"url": "https://www.doviz.com/kripto-paralar", "label": "Kripto"},
    {"url": "https://borsa.doviz.com/", "label": "Borsa"},
    {"url": "https://borsa.doviz.com/halka-arz", "label": "Halka arz"},
    {"url": "https://www.doviz.com/yakit-sarj", "label": "Yakıt / şarj"},
    {"url": "https://haber.doviz.com/", "label": "Haber"},
    {"url": "https://www.doviz.com/kredi-karsilastirma", "label": "Kredi karşılaştırma"},
]

_PERIODS = (1, 7, 30)
_CACHE_TTL_SEC = 600.0
_cache: dict[str, Any] = {"at": 0.0, "payload": None}


def _parse_url(url: str) -> tuple[str, str]:
    raw = (url or "").strip()
    if not raw:
        return "", "/"
    if "://" not in raw:
        raw = "https://" + raw
    p = urlparse(raw)
    host = (p.hostname or "").strip().lower()
    path = (p.path or "/").strip() or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return host, path


def _path_variants(path: str) -> list[str]:
    p = (path or "/").strip() or "/"
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    if p == "/":
        return ["/"]
    return [p, p + "/"]


def _hosts_for(host: str) -> list[str]:
    """www için m. varyantını da dahil et; alt alan adlarında yalnız o host."""
    h = (host or "").strip().lower()
    if not h:
        return []
    if h.startswith("www."):
        bare = h[4:]
        return [h, f"m.{bare}"]
    return [h]


def _fmt_range(start: str, end: str) -> str:
    def _d(iso: str) -> str:
        try:
            y, m, d = iso[:10].split("-")
            return f"{d}.{m}"
        except Exception:
            return iso[:10]

    if start[:10] == end[:10]:
        try:
            y, m, d = start[:10].split("-")
            return f"{d}.{m}.{y}"
        except Exception:
            return start[:10]
    return f"{_d(start)}–{_d(end)}"


def _pct_delta(cur: float, prev: float) -> tuple[str, str, float]:
    try:
        c = float(cur)
        p = float(prev)
    except (TypeError, ValueError):
        return ("—", "flat", 0.0)
    if p <= 0:
        if c > 0:
            return ("+∞", "up-strong", 999.0)
        return ("0%", "flat", 0.0)
    pct = (c - p) / p * 100.0
    ap = abs(pct)
    if ap < 0.05:
        tone = "flat"
    elif pct > 0:
        tone = "up-strong" if ap >= 20 else ("up" if ap >= 8 else "up-mild")
    else:
        tone = "down-strong" if ap >= 20 else ("down" if ap >= 8 else "down-mild")
    sign = "+" if pct > 0 else ""
    return (f"{sign}{pct:.1f}%", tone, round(pct, 2))


def _fmt_int(n: float) -> str:
    try:
        v = int(round(float(n)))
    except (TypeError, ValueError):
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"{v / 1_000:.1f}K".replace(".0K", "K")
    return str(v)


def _watch_targets() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in WATCH_URLS:
        host, path = _parse_url(item["url"])
        out.append(
            {
                "url": item["url"],
                "label": item["label"],
                "host": host,
                "path": path,
                "hosts": _hosts_for(host),
                "paths": _path_variants(path),
            }
        )
    return out


def _build_url_filter(targets: list[dict[str, Any]]) -> FilterExpression:
    exprs: list[FilterExpression] = []
    for t in targets:
        host_exprs = [
            FilterExpression(
                filter=Filter(
                    field_name="hostName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value=h,
                        case_sensitive=False,
                    ),
                )
            )
            for h in t["hosts"]
        ]
        path_exprs = [
            FilterExpression(
                filter=Filter(
                    field_name="pagePath",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.EXACT,
                        value=p,
                        case_sensitive=False,
                    ),
                )
            )
            for p in t["paths"]
        ]
        host_group = (
            host_exprs[0]
            if len(host_exprs) == 1
            else FilterExpression(or_group=FilterExpressionList(expressions=host_exprs))
        )
        path_group = (
            path_exprs[0]
            if len(path_exprs) == 1
            else FilterExpression(or_group=FilterExpressionList(expressions=path_exprs))
        )
        exprs.append(
            FilterExpression(
                and_group=FilterExpressionList(expressions=[host_group, path_group])
            )
        )
    if len(exprs) == 1:
        return exprs[0]
    return FilterExpression(or_group=FilterExpressionList(expressions=exprs))


def _norm_path(path: str) -> str:
    p = (path or "/").strip() or "/"
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    return p


def _match_key(host: str, path: str, targets: list[dict[str, Any]]) -> str | None:
    h = (host or "").strip().lower()
    p = _norm_path(path)
    for t in targets:
        if h in t["hosts"] and p == t["path"]:
            return t["url"]
    return None


def _run_range_map(
    client,
    property_id: str,
    *,
    start: str,
    end: str,
    targets: list[dict[str, Any]],
    dim_filter: FilterExpression,
) -> dict[str, float]:
    """url -> sessions for a single date range."""
    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="hostName"),
            Dimension(name="pagePath"),
        ],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimension_filter=dim_filter,
        limit=max(50, len(targets) * 8),
    )
    try:
        resp = client.run_report(req, timeout=90.0)
    except Exception:
        LOGGER.warning(
            "home watch-urls GA4 range %s..%s failed", start, end, exc_info=True
        )
        return {}
    out: dict[str, float] = {t["url"]: 0.0 for t in targets}
    for row in resp.rows or []:
        if len(row.dimension_values) < 2:
            continue
        host = str(row.dimension_values[0].value or "")
        path = str(row.dimension_values[1].value or "")
        key = _match_key(host, path, targets)
        if not key:
            continue
        val = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        out[key] = out.get(key, 0.0) + val
    return out


def _run_period(
    client,
    property_id: str,
    *,
    days: int,
    targets: list[dict[str, Any]],
    dim_filter: FilterExpression,
) -> dict[str, dict[str, float]]:
    """url -> {last, prev} sessions."""
    (last_start, last_end), (prev_start, prev_end) = _calendar_windows(days)
    last_map = _run_range_map(
        client,
        property_id,
        start=last_start,
        end=last_end,
        targets=targets,
        dim_filter=dim_filter,
    )
    prev_map = _run_range_map(
        client,
        property_id,
        start=prev_start,
        end=prev_end,
        targets=targets,
        dim_filter=dim_filter,
    )
    return {
        t["url"]: {
            "last": float(last_map.get(t["url"], 0.0)),
            "prev": float(prev_map.get(t["url"], 0.0)),
        }
        for t in targets
    }


def _normalize_property_id(raw: str) -> str:
    prop = str(raw or "").strip()
    if prop.startswith("properties/"):
        prop = prop.split("/", 1)[1]
    return prop


def _resolve_web_mweb_properties(db: Session, site_id: int = 1) -> list[str]:
    """Döviz web + mweb property id’leri (teksel, sırayla)."""
    record = get_ga4_credentials_record(db, site_id)
    props = load_ga4_properties(record) or {}
    out: list[str] = []
    for key in ("web", "mweb"):
        pid = _normalize_property_id(str(props.get(key) or ""))
        if pid and pid not in out:
            out.append(pid)
    if not out:
        for v in props.values():
            pid = _normalize_property_id(str(v or ""))
            if pid and pid not in out:
                out.append(pid)
                break
    return out


def _merge_period_buckets(
    *maps: dict[str, dict[str, float]],
    targets: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    out = {t["url"]: {"last": 0.0, "prev": 0.0} for t in targets}
    for m in maps:
        for url, bucket in (m or {}).items():
            if url not in out:
                continue
            out[url]["last"] += float((bucket or {}).get("last") or 0.0)
            out[url]["prev"] += float((bucket or {}).get("prev") or 0.0)
    return out


def build_home_watch_urls_context(db: Session, *, force: bool = False) -> dict[str, Any]:
    now = time.time()
    if (
        not force
        and _cache.get("payload") is not None
        and (now - float(_cache.get("at") or 0.0)) < _CACHE_TTL_SEC
    ):
        return dict(_cache["payload"])

    targets = _watch_targets()
    property_ids = _resolve_web_mweb_properties(db, 1)
    period_meta: dict[int, dict[str, str]] = {}
    for days in _PERIODS:
        (ls, le), (ps, pe) = _calendar_windows(days)
        period_meta[days] = {
            "current_label": _fmt_range(ls, le),
            "previous_label": _fmt_range(ps, pe),
        }

    by_period: dict[int, dict[str, dict[str, float]]] = {d: {} for d in _PERIODS}
    error = None
    if not property_ids:
        error = "GA4 web property bulunamadı"
    else:
        try:
            client = _client()
            dim_filter = _build_url_filter(targets)
            for days in _PERIODS:
                parts = [
                    _run_period(
                        client,
                        pid,
                        days=days,
                        targets=targets,
                        dim_filter=dim_filter,
                    )
                    for pid in property_ids
                ]
                by_period[days] = _merge_period_buckets(*parts, targets=targets)
        except Exception as exc:
            LOGGER.exception("home watch-urls fetch failed")
            error = str(exc)

    rows: list[dict[str, Any]] = []
    for t in targets:
        url = t["url"]
        periods_out: list[dict[str, Any]] = []
        for days in _PERIODS:
            bucket = (by_period.get(days) or {}).get(url) or {}
            last_v = float(bucket.get("last") or 0.0)
            prev_v = float(bucket.get("prev") or 0.0)
            delta_fmt, tone, delta_pct = _pct_delta(last_v, prev_v)
            meta = period_meta[days]
            periods_out.append(
                {
                    "days": days,
                    "label": f"{days}g" if days > 1 else "1g",
                    "title": (
                        "Son 1 gün"
                        if days == 1
                        else ("Son 1 hafta" if days == 7 else "Son 1 ay")
                    ),
                    "last": last_v,
                    "prev": prev_v,
                    "last_fmt": _fmt_int(last_v),
                    "prev_fmt": _fmt_int(prev_v),
                    "delta_fmt": delta_fmt,
                    "delta_tone": tone,
                    "delta_pct": delta_pct,
                    "current_label": meta["current_label"],
                    "previous_label": meta["previous_label"],
                    "has_data": (last_v > 0 or prev_v > 0),
                }
            )
        rows.append(
            {
                "url": url,
                "label": t["label"],
                "host": t["host"],
                "path": t["path"],
                "periods": periods_out,
            }
        )

    payload = {
        "rows": rows,
        "error": error,
        "metric_label": "sessions",
        "synced_at": date.today().isoformat(),
        "period_meta": {
            str(d): period_meta[d] for d in _PERIODS
        },
    }
    _cache["at"] = now
    _cache["payload"] = payload
    return dict(payload)
