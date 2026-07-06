"""GA4 Realtime — iOS/Android Haber detay (screen_view / news_detail_opened).

GA4 Realtime API event-scoped customEvent boyutlarını desteklemez; önce customEvent
denenir, başarısız olursa eventName filtresi + unifiedScreenName kırılımına düşülür.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    MinuteRange,
    OrderBy,
    RunRealtimeReportRequest,
)

from backend.services.ga4_app_event_config import app_event_detail_config
from backend.services.ga4_app_event_enrich import (
    build_news_article_lookup,
    enrich_app_event_detail_sections,
    section_enriches_news,
    section_uses_article_lookup,
)
from backend.services.ga4_realtime import (
    _build_client,
    _is_realtime_noise_title,
    _normalize_ga4_property_id,
    _realtime_row_dimensions,
    _screen_unified_news_article,
)

logger = logging.getLogger(__name__)

_COMBINED_SEP = " · "
_JUNK_VALUES = frozenset({"", "(not set)", "(other)", "not set", "(data not available)", "(blank)"})
_REALTIME_DETAIL_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 45.0
_CACHE_VER = 1


def _param_key(name: str | None) -> str:
    return re.sub(r"[\s_]", "", str(name or "").strip().lower())


def _realtime_event_name_filter(event_name: str) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value=str(event_name or "").strip(),
            ),
        )
    )


def _join_dim_values(parts: list[str]) -> str:
    clean = [str(p or "").strip() for p in parts if str(p or "").strip()]
    if not clean:
        return "(not set)"
    if len(clean) == 1:
        return clean[0]
    return _COMBINED_SEP.join(p if p else "(not set)" for p in clean)


def _realtime_dimension_candidates(
    param_key: str,
    alt_params: list[str] | None,
    *,
    property_id: str,
) -> list[str]:
    from backend.collectors.ga4 import _event_dimension_candidates

    return _event_dimension_candidates(param_key, alt_params, property_id=property_id)


def _run_realtime_dim_report(
    property_id: str,
    *,
    dimension_names: list[str],
    event_name: str | None = None,
    window_minutes: int = 30,
    limit: int = 100,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, float]:
    """Boyut kırılımı → eventCount haritası."""
    if not dimension_names:
        return {}
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    w = max(1, min(int(window_minutes), 29))
    fetch_cap = max(1, min(int(limit), 250))

    minute_ranges = [MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0)]
    dims = [Dimension(name=d) for d in dimension_names if str(d or "").strip()]
    if not dims:
        return {}

    req_kwargs: dict[str, Any] = {
        "property": f"properties/{property_id}",
        "dimensions": dims,
        "metrics": [Metric(name="eventCount")],
        "minute_ranges": minute_ranges,
        "limit": fetch_cap,
    }
    if event_name:
        req_kwargs["dimension_filter"] = _realtime_event_name_filter(event_name)
    try:
        req_kwargs["order_bys"] = [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)
        ]
        response = client.run_realtime_report(RunRealtimeReportRequest(**req_kwargs))
    except Exception:
        req_kwargs.pop("order_bys", None)
        response = client.run_realtime_report(RunRealtimeReportRequest(**req_kwargs))

    dim_headers = [h.name for h in response.dimension_headers]
    dim_count = len(dimension_names)
    out: dict[str, float] = {}

    for row in response.rows or []:
        dm = _realtime_row_dimensions(row, dim_headers)
        parts: list[str] = []
        for dname in dimension_names:
            val = str(dm.get(dname, "") or "").strip()
            if val.lower() in ("current", "previous"):
                val = ""
            if not val:
                for k, v in dm.items():
                    if k in dimension_names:
                        continue
                    vs = str(v or "").strip()
                    if vs and vs.lower() not in ("current", "previous"):
                        val = vs
                        break
            parts.append(val)
        if len(parts) > dim_count:
            parts = parts[:dim_count]
        key = _join_dim_values(parts)
        if key.lower() in _JUNK_VALUES or _is_realtime_noise_title(key):
            continue
        val = 0.0
        if row.metric_values:
            try:
                val = float(row.metric_values[0].value or 0.0)
            except (ValueError, TypeError):
                val = 0.0
        if val <= 0:
            continue
        out[key] = out.get(key, 0.0) + val

    return out


def _counts_to_rows(counts: dict[str, float]) -> list[dict[str, Any]]:
    rows = [{"value": k, "count": v} for k, v in counts.items() if v > 0]
    rows.sort(key=lambda r: (-float(r.get("count") or 0), str(r.get("value") or "").lower()))
    return rows


def _filter_unified_screen_rows(
    rows: list[dict[str, Any]],
    *,
    news_only: bool,
    site_domain: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        val = str(row.get("value") or "").strip()
        if not val or val.lower() in _JUNK_VALUES:
            continue
        is_news = _screen_unified_news_article(val, site_domain=site_domain)
        if news_only and not is_news:
            continue
        if not news_only and is_news:
            continue
        out.append(row)
    return out


def _fetch_realtime_section_rows(
    property_id: str,
    *,
    event_name: str,
    param: str,
    param2: str | None,
    alt_params: list[str] | None,
    alt_params_2: list[str] | None,
    site_domain: str,
    window_minutes: int,
    limit: int,
    client: BetaAnalyticsDataClient | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Bölüm satırları ve isteğe bağlı realtime notu."""
    pk = _param_key(param)
    p2 = str(param2 or "").strip() or None

    if pk == "from":
        for dim in _realtime_dimension_candidates(param, alt_params, property_id=property_id):
            try:
                counts = _run_realtime_dim_report(
                    property_id,
                    dimension_names=[dim],
                    event_name=event_name,
                    window_minutes=window_minutes,
                    limit=limit,
                    client=client,
                )
                if counts:
                    return _counts_to_rows(counts)[:limit], None
            except Exception as exc:
                logger.debug("Realtime custom dim failed [%s %s]: %s", event_name, dim, exc)
        return [], "Realtime API event parametresi (from) desteklemiyor; GA4 arayüzünden izleyin."

    dim_candidates: list[list[str]] = []
    if p2:
        for d1 in _realtime_dimension_candidates(param, alt_params, property_id=property_id):
            for d2 in _realtime_dimension_candidates(p2, alt_params_2, property_id=property_id):
                dim_candidates.append([d1, d2])
    else:
        for d in _realtime_dimension_candidates(param, alt_params, property_id=property_id):
            dim_candidates.append([d])

    for dims in dim_candidates:
        if not dims:
            continue
        try:
            counts = _run_realtime_dim_report(
                property_id,
                dimension_names=dims,
                event_name=event_name,
                window_minutes=window_minutes,
                limit=limit * 4,
                client=client,
            )
            if counts:
                return _counts_to_rows(counts)[:limit], None
        except Exception as exc:
            logger.debug("Realtime param dims failed %s: %s", dims, exc)

    if pk == "unifiedscreenname" or section_enriches_news(param, p2):
        try:
            counts = _run_realtime_dim_report(
                property_id,
                dimension_names=["unifiedScreenName"],
                event_name=event_name,
                window_minutes=window_minutes,
                limit=limit * 6,
                client=client,
            )
            rows = _counts_to_rows(counts)
            news_only = section_enriches_news(param, p2)
            rows = _filter_unified_screen_rows(rows, news_only=news_only, site_domain=site_domain)
            if rows:
                note = None
                if section_enriches_news(param, p2):
                    note = (
                        "Realtime API customEvent parametrelerini desteklemez; "
                        "eventName + unifiedScreenName (haber başlığı) ile yaklaşık liste."
                    )
                elif pk == "unifiedscreenname":
                    note = (
                        "Realtime API customEvent desteklemez; "
                        "eventName + unifiedScreenName ekran kırılımı kullanıldı."
                    )
                return rows[:limit], note
        except Exception as exc:
            logger.debug("Realtime unifiedScreenName fallback failed: %s", exc)

    return [], "Bu parametre için realtime veri alınamadı."


def fetch_realtime_app_event_detail(
    property_id: str,
    profile: str,
    *,
    site_domain: str = "",
    lookup_property_ids: list[str] | None = None,
    window_minutes: int = 30,
    limit: int = 25,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Profil için Haber detay bölümlerini realtime çeker ve URL ile zenginleştirir."""
    cfg = app_event_detail_config(profile)
    if not cfg:
        return {
            "profile": profile,
            "event_name": "",
            "title": "",
            "sections": [],
            "window_minutes": window_minutes,
            "fetched_at": None,
        }

    cache_key = (
        _CACHE_VER,
        str(property_id),
        str(profile).lower(),
        int(window_minutes),
        int(limit),
        str(site_domain or "").lower(),
    )
    cached = _REALTIME_DETAIL_CACHE.get(cache_key)
    if cached and (time.monotonic() - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    event_name = str(cfg.get("event_name") or "")
    specs = [s for s in (cfg.get("sections") or []) if str(s.get("param") or "").strip()]
    w = max(1, min(int(window_minutes), 29))
    safe_limit = max(5, min(int(limit), 50))

    def _one_section(spec: dict[str, Any]) -> dict[str, Any]:
        param = str(spec.get("param") or "").strip()
        param2 = str(spec.get("param2") or "").strip() or None
        label = str(spec.get("label") or param)
        try:
            rows, note = _fetch_realtime_section_rows(
                property_id,
                event_name=event_name,
                param=param,
                param2=param2,
                alt_params=list(spec.get("alt_params") or []),
                alt_params_2=list(spec.get("alt_params_2") or []),
                site_domain=site_domain,
                window_minutes=w,
                limit=safe_limit,
                client=client,
            )
            return {
                "label": label,
                "event_name": event_name,
                "param": param,
                "param2": param2,
                "rows": rows,
                "error": None,
                "realtime_note": note,
            }
        except Exception as exc:
            logger.warning("Realtime app event section failed [%s %s]: %s", profile, param, exc)
            return {
                "label": label,
                "event_name": event_name,
                "param": param,
                "param2": param2,
                "rows": [],
                "error": str(exc),
                "realtime_note": None,
            }

    workers = min(4, max(1, len(specs)))
    ordered: list[dict[str, Any] | None] = [None] * len(specs)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rt-evdetail") as pool:
        futures = {pool.submit(_one_section, spec): i for i, spec in enumerate(specs)}
        for fut in futures:
            ordered[futures[fut]] = fut.result()
    raw_sections = [s for s in ordered if s is not None]

    lookup_pids = [str(p or "").strip() for p in (lookup_property_ids or []) if str(p or "").strip()]
    needs_lookup = any(section_uses_article_lookup(s.get("param"), s.get("param2")) for s in raw_sections)
    lookup = {"by_id": {}, "by_title": {}}
    if needs_lookup and lookup_pids:
        lookup = build_news_article_lookup(lookup_pids, days=90, lookup_days=90)

    sections = enrich_app_event_detail_sections(
        raw_sections,
        property_ids=lookup_pids,
        days=90,
        site_domain=site_domain or None,
        lookup=lookup,
    )

    from datetime import datetime, timezone

    result = {
        "profile": profile,
        "event_name": event_name,
        "title": str(cfg.get("title") or ""),
        "sections": sections,
        "window_minutes": w,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "metric_scope": "ga4_realtime_30m",
    }
    _REALTIME_DETAIL_CACHE[cache_key] = (time.monotonic(), result)
    return result
