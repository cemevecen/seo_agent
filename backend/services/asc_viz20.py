"""iOS viz20 — ASC + GA4 + Virgül interaktif grafikler (Android play_viz20 paralel)."""

from __future__ import annotations

import statistics
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.services.asc_metrics_warehouse import metric_catalog, query_asc_metric
from backend.services.firebase_console_store import firebase_console_payload

GA4_METRICS = [
    "sessions",
    "users",
    "engaged_sessions",
    "new_users",
    "avg_session",
    "page_views",
]

VIRGUL_METRICS = [
    "net_revenue",
    "ad_request",
    "impression",
    "click",
    "ad_request_ecpm",
    "ad_ecpm",
    "viewability_pct",
    "ctr_pct",
    "coverage_pct",
]

ASC_METRICS = [m["value"] for m in metric_catalog()]

VIZ_META: list[dict[str, Any]] = [
    {
        "id": "treemap",
        "title": "Treemap",
        "detail": (
            "Crash issue başlıklarını olay hacmine göre alan payı olarak gösterir. "
            "Veri: Firebase iOS Crashlytics scrape (Mac bridge)."
        ),
        "controls": ["etype", "limit"],
    },
    {
        "id": "combo",
        "title": "Dual-axis combo",
        "detail": (
            "Sol eksende GA4 metrik (varsayılan sessions), sağ eksende Virgül metrik "
            "(varsayılan net revenue TL). Günlük seriler üst üste bindirilir."
        ),
        "controls": ["start", "end", "metric_left", "metric_right"],
    },
    {
        "id": "horizon",
        "title": "Horizon chart",
        "detail": (
            "Aynı grafikte 4–6 metrik (ASC + GA4 + Virgül) günlük seri olarak bindirilir; "
            "her seri kendi maksimumuna göre 0–1 normalize edilir (en fazla 6 metrik)."
        ),
        "controls": ["start", "end", "metrics"],
    },
    {
        "id": "control",
        "title": "Control chart (SPC)",
        "detail": (
            "Shewhart kontrol grafiği: seçili ASC metriğinin günlük serisi, "
            " süreç ortalaması ve ±3σ kontrol limitleri (UCL/LCL)."
        ),
        "controls": ["start", "end", "metric"],
    },
    {
        "id": "timeline",
        "title": "Release / Metrik etkisi",
        "detail": (
            "Seçili metriğin günlük serisi üzerinde yüksek değer günleri kırmızı dikey çizgi ile vurgulanır; "
            "iOS release günleri yeşil çizgi ve üst elmas ile işaretlenir."
        ),
        "controls": ["start", "end", "metric"],
    },
]

VIZ_IDS = frozenset(v["id"] for v in VIZ_META)


def _table(columns: list[str], rows: list[list[Any]]) -> dict[str, Any]:
    return {"columns": columns, "rows": rows}


def _default_range(days: int = 28) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=max(1, days) - 1)
    return start.isoformat(), end.isoformat()


def _parse_iso_date(raw: str | None) -> date | None:
    s = str(raw or "").strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _series_vals(data: dict[str, Any]) -> list[float]:
    out: list[float] = []
    for r in data.get("series") or []:
        try:
            out.append(float(r.get("value") or 0))
        except (TypeError, ValueError):
            out.append(0.0)
    return out


def _series_map(data: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for r in data.get("series") or []:
        try:
            out[str(r.get("key"))] = float(r.get("value") or 0)
        except (TypeError, ValueError):
            continue
    return out


def _asc_q(*, metric: str, start: str, end: str) -> dict[str, Any]:
    return query_asc_metric(start=start, end=end, metric=metric)


def _firebase_ios_issues(db: Session | None, *, etype: str = "CRASH", limit: int = 15) -> list[dict[str, Any]]:
    if db is None:
        return []
    snap = firebase_console_payload(db) or {}
    platforms = snap.get("platforms") if isinstance(snap.get("platforms"), dict) else {}
    ios = platforms.get("ios") if isinstance(platforms.get("ios"), dict) else {}
    if etype == "ANR":
        pool = ios.get("anr_issues") if isinstance(ios.get("anr_issues"), list) else []
    else:
        pool = ios.get("issues") if isinstance(ios.get("issues"), list) else []
    out: list[dict[str, Any]] = []
    for iss in pool:
        if not isinstance(iss, dict):
            continue
        title = str(iss.get("title") or iss.get("issue_id") or "Issue")[:80]
        ev = iss.get("events") or iss.get("event_count") or iss.get("impacted_users") or 0
        try:
            val = float(ev)
        except (TypeError, ValueError):
            val = 1.0
        out.append({"title": title, "events": val, "issue_type": etype})
    out.sort(key=lambda x: -float(x.get("events") or 0))
    return out[: max(3, min(int(limit or 15), 50))]


def _timeline_ios_releases(start: str, end: str) -> list[dict[str, Any]]:
    start_d = _parse_iso_date(start)
    end_d = _parse_iso_date(end)
    if not start_d or not end_d:
        return []
    ios: list[dict[str, Any]] = []
    try:
        from backend.services.app_release_sheet import fetch_releases_from_sheet

        ios, _android = fetch_releases_from_sheet("doviz", since=start_d, use_cache=True)
    except Exception:
        ios = []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ios:
        if not isinstance(item, dict):
            continue
        day = str(item.get("released_at") or "")[:10]
        if not day or day < start_d.isoformat() or day > end_d.isoformat():
            continue
        if day in seen:
            continue
        seen.add(day)
        version = str(item.get("version") or "").strip()
        build = str(item.get("build") or "").strip()
        rows.append(
            {
                "date": day,
                "version": version or None,
                "version_code": build or None,
            }
        )
    return rows


def build_asc_viz20_meta() -> dict[str, Any]:
    return {
        "ok": True,
        "viz": VIZ_META,
        "play_metrics": ASC_METRICS,
        "ga4_metrics": GA4_METRICS,
        "virgul_metrics": VIRGUL_METRICS,
        "dims": ["overview"],
        "default_range": {"start": _default_range()[0], "end": _default_range()[1]},
    }


def build_asc_viz20_data(
    db: Session,
    *,
    viz_id: str,
    start: str | None = None,
    end: str | None = None,
    metric: str | None = None,
    metric_left: str | None = None,
    metric_right: str | None = None,
    metrics: str | None = None,
    etype: str = "CRASH",
    limit: int = 15,
) -> dict[str, Any]:
    vid = (viz_id or "").strip().lower()
    if vid not in VIZ_IDS:
        return {
            "ok": False,
            "viz": vid,
            "message": f"Bilinmeyen veya kaldırılmış grafik: {vid}",
            "chart": {},
            "table": _table([], []),
        }
    if not start or not end:
        start, end = _default_range(28)
    m = (metric or "crashes").strip()
    lim = max(3, min(int(limit or 15), 50))

    if vid == "treemap":
        issues = _firebase_ios_issues(db, etype=etype, limit=lim)
        labels_t: list[str] = []
        parents: list[str] = []
        values_t: list[float] = []
        rows_t: list[list[Any]] = []
        for iss in issues:
            title = str(iss.get("title") or "Issue")[:80]
            val = float(iss.get("events") or 1)
            labels_t.append(title)
            parents.append(etype)
            values_t.append(val)
            rows_t.append([title, val, iss.get("issue_type") or etype])
        if not labels_t:
            return {
                "ok": False,
                "viz": vid,
                "message": "Issue verisi yok — Firebase iOS scrape gerekli",
                "chart": {},
                "table": _table([], []),
            }
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "treemap",
                "labels": [etype] + labels_t,
                "parents": [""] + [etype] * len(labels_t),
                "values": [sum(values_t)] + values_t,
            },
            "table": _table(["Issue", "Events", "Type"], rows_t),
            "params": {"etype": etype},
        }

    if vid == "combo":
        from backend.api.play_analytics import (
            get_play_ga4_overlay_series,
            get_play_virgul_overlay_series,
        )

        ml = (metric_left or "ga4:sessions").strip()
        mr = (metric_right or "virgul:net_revenue").strip()
        ga4_key = ml.replace("ga4:", "") if ml.startswith("ga4:") else "sessions"
        vir_key = mr.replace("virgul:", "") if mr.startswith("virgul:") else "net_revenue"
        ga4 = get_play_ga4_overlay_series(
            db, start=start, end=end, metric=ga4_key, profile="ios", project="doviz"
        )
        vir = get_play_virgul_overlay_series(
            db, start=start, end=end, metric=vir_key, branch="ios", project="doviz"
        )
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "combo",
                "left": {"label": ga4.get("label") or ml, "series": ga4.get("series") or []},
                "right": {"label": vir.get("label") or mr, "series": vir.get("series") or []},
            },
            "table": _table(
                ["Date", ga4.get("label") or ml, vir.get("label") or mr],
                [
                    [
                        r.get("key"),
                        _series_map(ga4).get(str(r.get("key")), 0),
                        _series_map(vir).get(str(r.get("key")), 0),
                    ]
                    for r in (ga4.get("series") or [])[:60]
                ],
            ),
            "params": {"start": start, "end": end, "metric_left": ml, "metric_right": mr},
        }

    if vid == "horizon":
        metric_list = [x.strip() for x in (metrics or "crashes,ga4:sessions,active_devices").split(",") if x.strip()][:6]
        traces_h: list[dict[str, Any]] = []
        trows_h: list[list[Any]] = []
        for mk in metric_list:
            if mk.startswith("xdata:"):
                from backend.services.empower_intel_store import query_series

                payload = query_series(
                    db,
                    project="doviz",
                    platform="ios",
                    metric=mk,
                    start=start,
                    end=end,
                )
                series = payload.get("series") or []
                label = payload.get("label") or mk
            elif mk.startswith("ga4:"):
                from backend.api.play_analytics import get_play_ga4_overlay_series

                gk = mk.replace("ga4:", "")
                payload = get_play_ga4_overlay_series(
                    db, start=start, end=end, metric=gk, profile="ios", project="doviz"
                )
                series = payload.get("series") or []
                label = payload.get("label") or mk
            elif mk.startswith("virgul:"):
                from backend.api.play_analytics import get_play_virgul_overlay_series

                vk = mk.replace("virgul:", "")
                payload = get_play_virgul_overlay_series(
                    db, start=start, end=end, metric=vk, branch="ios", project="doviz"
                )
                series = payload.get("series") or []
                label = payload.get("label") or mk
            else:
                data = _asc_q(metric=mk, start=start, end=end)
                series = data.get("series") or []
                label = mk
            xs = [str(r.get("key")) for r in series]
            ys = [float(r.get("value") or 0) for r in series]
            if ys:
                mx = max(ys) or 1.0
                ys = [round(y / mx, 4) for y in ys]
            traces_h.append({"name": label, "x": xs, "y": ys})
            trows_h.append([label, round(sum(ys), 2), len(xs)])
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "horizon", "traces": traces_h},
            "table": _table(["Metric", "Norm sum", "Points"], trows_h),
            "params": {"start": start, "end": end, "metrics": ",".join(metric_list)},
        }

    if vid == "control":
        data = _asc_q(metric=m, start=start, end=end)
        ys = _series_vals(data)
        xs = [str(r.get("key")) for r in (data.get("series") or [])]
        mean = statistics.mean(ys) if ys else 0.0
        stdev = statistics.pstdev(ys) if len(ys) > 1 else 0.0
        ucl = mean + 3 * stdev
        lcl = max(0.0, mean - 3 * stdev)
        alarms = [i for i, y in enumerate(ys) if y > ucl or y < lcl]
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "control",
                "x": xs,
                "y": ys,
                "mean": mean,
                "ucl": ucl,
                "lcl": lcl,
                "alarms": alarms,
            },
            "table": _table(
                ["Date", "Value", "Alarm"],
                [[xs[i], ys[i], "yes" if i in alarms else ""] for i in range(len(xs))],
            ),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "timeline":
        metric_data = _asc_q(metric=m or "crashes", start=start, end=end)
        releases = _timeline_ios_releases(start or "", end or "")
        spikes = sorted(metric_data.get("series") or [], key=lambda r: -float(r.get("value") or 0))[:5]
        rel_rows = [
            [r.get("date"), r.get("version") or "—", r.get("version_code") or "—"]
            for r in releases
        ]
        series_rows = [[r.get("key"), r.get("value")] for r in (metric_data.get("series") or [])[:30]]
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "timeline",
                "releases": releases,
                "spikes": [{"date": r.get("key"), "value": r.get("value")} for r in spikes],
                "series": metric_data.get("series") or [],
            },
            "table": _table(["Tarih", "Sürüm", "Build"], rel_rows)
            if rel_rows
            else _table(["Date", "Value"], series_rows),
            "params": {"start": start, "end": end, "metric": m or "crashes"},
        }

    return {"ok": False, "viz": vid, "message": f"Bilinmeyen viz: {vid}", "chart": {}, "table": _table([], [])}
