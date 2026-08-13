"""Android viz20 — seçili interaktif grafikler için veri toplayıcı."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.api.play_analytics import resolve_play_analytics_query
from backend.services.play_console_store import play_console_payload
from backend.services.play_scrape_warehouse import load_scrape_facts, scrape_metric_keys
from backend.services.stability_free import build_stability_free_payload

PLAY_METRICS = scrape_metric_keys()

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

DIMS = ["overview", "country", "app_version", "device", "os_version"]

VIZ_META: list[dict[str, Any]] = [
    {
        "id": "treemap",
        "title": "Treemap",
        "detail": (
            "Issue başlıklarını olay hacmine göre alan payı olarak gösterir. "
            "Kategori seçimi yok — seçili issue tipindeki (Crash veya ANR) tüm satırlar "
            "birleştirilir. Veri: Play Console vitals scrape + Firebase Android fallback."
        ),
        "controls": ["etype", "limit"],
    },
    {
        "id": "combo",
        "title": "Dual-axis combo",
        "detail": (
            "Sol eksende GA4 metrik (varsayılan sessions), sağ eksende Virgül metrik "
            "(varsayılan net revenue TL). Günlük seriler üst üste bindirilir; "
            "tarih aralığı Preset ile ana grafikle aynı mantıkta seçilir."
        ),
        "controls": ["start", "end", "metric_left", "metric_right"],
    },
    {
        "id": "horizon",
        "title": "Horizon chart",
        "detail": (
            "Aynı grafikte 4–6 metrik (Play + GA4 + Virgül) günlük/haftalık seri olarak bindirilir; "
            "her seri kendi maksimumuna göre 0–1 normalize edilir. "
            "Metrikler ana grafikteki gibi çoklu seçim listesinden işaretlenir (en fazla 6)."
        ),
        "controls": ["start", "end", "metrics"],
    },
    {
        "id": "control",
        "title": "Control chart (SPC)",
        "detail": (
            "Shewhart kontrol grafiği: seçili Play metriğinin günlük serisi, "
            " süreç ortalaması ve ±3σ kontrol limitleri (UCL/LCL). "
            "Limit dışı noktalar alarm olarak işaretlenir."
        ),
        "controls": ["start", "end", "metric"],
    },
    {
        "id": "timeline",
        "title": "Release / Metrik etkisi",
        "detail": (
            "Seçili metriğin günlük serisi üzerinde yüksek değer günleri kırmızı dikey çizgi ile vurgulanır; "
            "Android release günleri yeşil çizgi ve üst elmas ile işaretlenir. "
            "Grafik üzerine gelince sürüm adı ve versiyon kodu gösterilir."
        ),
        "controls": ["start", "end", "metric"],
    },
]

VIZ_IDS = frozenset(v["id"] for v in VIZ_META)


def _table(columns: list[str], rows: list[list[Any]]) -> dict[str, Any]:
    return {"columns": columns, "rows": rows}


def _parse_iso_date(raw: str | None) -> date | None:
    s = str(raw or "").strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _version_code_for_name(name: str, vitals: dict[str, Any]) -> str:
    ver = str(name or "").strip()
    if not ver:
        return ""
    name_map = vitals.get("version_name_map") if isinstance(vitals.get("version_name_map"), dict) else {}
    for code, nm in name_map.items():
        s = str(nm or "").strip()
        if not s:
            continue
        if s == ver or ver in s or f"({ver})" in s:
            return str(code).strip()
    for row in vitals.get("versions") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("name") or "").strip() == ver:
            return str(row.get("code") or "").strip()
    return ""


def _timeline_releases(db: Session | None, start: str, end: str) -> list[dict[str, Any]]:
    """Android release tarihleri — Google Sheets (önbellek); vitals sürüm kodu eşlemesi."""
    start_d = _parse_iso_date(start)
    end_d = _parse_iso_date(end)
    if not start_d or not end_d:
        return []
    since_d = start_d
    vitals: dict[str, Any] = {}
    if db is not None:
        snap = play_console_payload(db) or {}
        panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
        vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}

    android: list[dict[str, Any]] = []
    try:
        # Yalnızca Sheets — fetch_version_releases_for_product app_intel taramasına düşer (30sn+).
        from backend.services.app_release_sheet import fetch_releases_from_sheet

        _, android = fetch_releases_from_sheet("doviz", since=since_d, use_cache=True)
    except Exception:
        android = []

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in android:
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
        version_code = build.split("/")[0].strip() if build else ""
        if not version_code:
            version_code = _version_code_for_name(version, vitals)
        rows.append(
            {
                "date": day,
                "version": version or None,
                "version_code": version_code or None,
                "source": item.get("source"),
            }
        )
    rows.sort(key=lambda r: str(r.get("date") or ""))
    return rows[:24]


def _q(
    *,
    metric: str,
    start: str | None,
    end: str | None,
    breakdown: str = "date",
    dim: str = "overview",
    segment: str | None = None,
    compare: str = "",
    facts: list[dict[str, Any]] | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return resolve_play_analytics_query(
        start=start,
        end=end,
        metric=metric,
        breakdown=breakdown,
        dim=dim,
        segment=segment,
        compare=compare,
        facts=facts,
        meta=meta,
    )


def _series_vals(data: dict[str, Any]) -> list[float]:
    out: list[float] = []
    for r in data.get("series") or []:
        try:
            out.append(float(r.get("value") or 0))
        except (TypeError, ValueError):
            continue
    return out


def _series_map(data: dict[str, Any]) -> dict[str, float]:
    m: dict[str, float] = {}
    for r in data.get("series") or []:
        k = str(r.get("key") or "")
        if not k:
            continue
        try:
            m[k] = float(r.get("value") or 0)
        except (TypeError, ValueError):
            continue
    return m


def _total(data: dict[str, Any]) -> float:
    try:
        return float(data.get("total") or 0)
    except (TypeError, ValueError):
        return sum(_series_vals(data))


def _default_range(days: int = 28) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


def _shift_half(start_s: str, end_s: str) -> tuple[str, str]:
    s = date.fromisoformat(start_s[:10])
    e = date.fromisoformat(end_s[:10])
    mid = s + (e - s) // 2
    prev_end = mid - timedelta(days=1)
    return s.isoformat(), prev_end.isoformat()


def _top_segments(
    *,
    metric: str,
    dim: str,
    start: str,
    end: str,
    limit: int = 8,
    facts: list[dict[str, Any]] | None = None,
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    data = _q(
        metric=metric,
        start=start,
        end=end,
        breakdown="segment",
        dim=dim,
        facts=facts,
        meta=meta,
    )
    rows = list(data.get("series") or [])
    rows.sort(key=lambda r: (-float(r.get("value") or 0), str(r.get("key") or "")))
    return rows[:limit]


def _vitals_issues(db: Session, etype: str = "CRASH") -> list[dict[str, Any]]:
    snap = play_console_payload(db) or {}
    panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
    vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
    et = (etype or "CRASH").upper()
    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    block = crashes.get(et) if isinstance(crashes.get(et), dict) else {}
    issues: list[dict[str, Any]] = []
    for cat in block.get("categories") or []:
        if not isinstance(cat, dict):
            continue
        for iss in cat.get("issues") or []:
            if isinstance(iss, dict):
                issues.append(iss)
    return issues


def build_viz20_meta() -> dict[str, Any]:
    return {
        "ok": True,
        "viz": VIZ_META,
        "play_metrics": PLAY_METRICS,
        "ga4_metrics": GA4_METRICS,
        "virgul_metrics": VIRGUL_METRICS,
        "dims": DIMS,
        "default_range": {"start": _default_range()[0], "end": _default_range()[1]},
    }


def build_viz20_data(
    db: Session,
    *,
    viz_id: str,
    start: str | None = None,
    end: str | None = None,
    metric: str | None = None,
    dim: str | None = None,
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
    facts, meta = load_scrape_facts()
    m = (metric or "crashes").strip()
    d = (dim or "country").strip()
    lim = max(3, min(int(limit or 15), 50))

    if vid == "funnel":
        steps = [
            ("Store visitors", "ar2_visitors"),
            ("Store acquisitions", "ar2_acquisitions"),
            ("DAU", "dau"),
        ]
        labels: list[str] = []
        values: list[float] = []
        rows: list[list[Any]] = []
        base = None
        for label, mk in steps:
            data = _q(metric=mk, start=start, end=end, facts=facts, meta=meta)
            val = _total(data)
            labels.append(label)
            values.append(round(val, 2))
            drop = None
            if base and base > 0:
                drop = round((1 - val / base) * 100, 1)
            rows.append([label, val, drop if drop is not None else "—"])
            if base is None:
                base = val if val > 0 else None
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "funnel", "labels": labels, "values": values},
            "table": _table(["Step", "Total", "Drop %"], rows),
            "params": {"start": start, "end": end},
        }

    if vid == "waterfall":
        cur = _q(
            metric=m,
            start=start,
            end=end,
            breakdown="week",
            compare="previous_period",
            facts=facts,
            meta=meta,
        )
        cur_map = _series_map(cur)
        cmp_block = cur.get("compare") if isinstance(cur.get("compare"), dict) else {}
        prev_map = _series_map({"series": cmp_block.get("series") or []})
        keys = sorted(set(cur_map) | set(prev_map))
        if not keys:
            keys = sorted(cur_map.keys())
        labels_w: list[str] = []
        values_w: list[float] = []
        measure: list[str] = []
        running = 0.0
        for i, k in enumerate(keys):
            pv = prev_map.get(k, 0.0)
            cv = cur_map.get(k, 0.0)
            if i == 0:
                labels_w.append(k)
                values_w.append(round(pv, 2))
                measure.append("absolute")
                running = pv
            delta = cv - pv
            labels_w.append(f"Δ {k}")
            values_w.append(round(delta, 2))
            measure.append("relative")
            running += delta
        labels_w.append("Total")
        values_w.append(round(running, 2))
        measure.append("total")
        seg_rows = _top_segments(metric=m, dim=d, start=start, end=end, limit=5, facts=facts, meta=meta)
        seg_table = [[r.get("key"), r.get("value")] for r in seg_rows]
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "waterfall",
                "labels": labels_w,
                "values": values_w,
                "measure": measure,
            },
            "table": _table(["Segment", "Value"], seg_table),
            "params": {"start": start, "end": end, "metric": m, "dim": d},
        }

    if vid == "heatmap":
        data = _q(metric=m, start=start, end=end, facts=facts, meta=meta)
        grid: dict[tuple[int, int], float] = defaultdict(float)
        weekday_labels = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]
        for r in data.get("series") or []:
            ds = str(r.get("key") or "")[:10]
            try:
                dt = date.fromisoformat(ds)
            except ValueError:
                continue
            wd = dt.weekday()
            iso = dt.isocalendar()
            wk = iso.week + iso.year * 100
            try:
                grid[(wd, wk)] += float(r.get("value") or 0)
            except (TypeError, ValueError):
                continue
        if not grid:
            return {"ok": False, "viz": vid, "message": "Heatmap için veri yok", "chart": {}, "table": _table([], [])}
        weeks = sorted({k[1] for k in grid})
        week_labels = [str(w % 100) for w in weeks[-8:]]
        z: list[list[float]] = []
        trows: list[list[Any]] = []
        for wd in range(7):
            row: list[float] = []
            for wk in weeks[-8:]:
                v = grid.get((wd, wk), 0.0)
                row.append(round(v, 2))
            z.append(row)
            trows.append([weekday_labels[wd]] + row)
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "heatmap",
                "x": week_labels,
                "y": weekday_labels,
                "z": z,
            },
            "table": _table(["Day"] + week_labels, trows),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "cohort":
        acq = _q(metric="device_acquisition", start=start, end=end, breakdown="week", facts=facts, meta=meta)
        dau = _q(metric="dau", start=start, end=end, breakdown="week", facts=facts, meta=meta)
        acq_map = _series_map(acq)
        dau_map = _series_map(dau)
        cohorts = sorted(acq_map.keys())[-6:]
        max_w = 5
        x_labels = ["Cohort"] + [f"W{i}" for i in range(max_w + 1)]
        z_c: list[list[float | None]] = []
        trows_c: list[list[Any]] = []
        for ck in cohorts:
            base = acq_map.get(ck, 0.0)
            row_pct: list[float | None] = []
            trow: list[Any] = [ck, "100%"]
            row_pct.append(100.0 if base > 0 else None)
            for wi in range(1, max_w + 1):
                # proxy: DAU week offset / acquisition cohort
                keys = sorted(dau_map.keys())
                try:
                    idx = keys.index(ck)
                except ValueError:
                    idx = -1
                tgt = keys[idx + wi] if idx >= 0 and idx + wi < len(keys) else None
                if base > 0 and tgt:
                    pct = min(100.0, round(dau_map.get(tgt, 0) / base * 100, 1))
                else:
                    pct = None
                row_pct.append(pct)
                trow.append(f"{pct}%" if pct is not None else "—")
            z_c.append(row_pct)
            trows_c.append(trow)
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "heatmap", "x": x_labels[1:], "y": cohorts, "z": [r[1:] for r in z_c], "cohort": True},
            "table": _table(x_labels, trows_c),
            "params": {"start": start, "end": end},
        }

    if vid == "treemap":
        issues = _vitals_issues(db, etype=etype)[:lim]
        labels_t: list[str] = []
        parents: list[str] = []
        values_t: list[float] = []
        rows_t: list[list[Any]] = []
        for iss in issues:
            title = str(iss.get("title") or iss.get("issue_id") or "Issue")[:80]
            ev = iss.get("events") or iss.get("event_count") or iss.get("impacted_users") or 0
            try:
                val = float(ev)
            except (TypeError, ValueError):
                val = 1.0
            labels_t.append(title)
            parents.append(etype)
            values_t.append(val)
            rows_t.append([title, val, iss.get("issue_type") or etype])
        if not labels_t:
            return {"ok": False, "viz": vid, "message": "Issue verisi yok — vitals sync gerekli", "chart": {}, "table": _table([], [])}
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "treemap", "labels": [etype] + labels_t, "parents": [""] + [etype] * len(labels_t), "values": [sum(values_t)] + values_t},
            "table": _table(["Issue", "Events", "Type"], rows_t),
            "params": {"etype": etype},
        }

    if vid == "bump":
        data = _q(metric=m, start=start, end=end, breakdown="month", dim=d, facts=facts, meta=meta)
        # rank top segments each month from facts
        month_seg: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for f in facts or []:
            if str(f.get("dim") or "") != d:
                continue
            ds = str(f.get("date") or "")
            if not ds or ds.startswith("i"):
                continue
            try:
                dt = date.fromisoformat(ds[:10])
            except ValueError:
                continue
            if dt.isoformat() < start or dt.isoformat() > end:
                continue
            if str(f.get("metric") or "") != m:
                continue
            mk = f"{dt.year}-{dt.month:02d}"
            seg = str(f.get("segment") or "UNKNOWN")
            try:
                month_seg[mk][seg] += float(f.get("value") or 0)
            except (TypeError, ValueError):
                continue
        months = sorted(month_seg.keys())[-8:]
        seg_tot: dict[str, float] = defaultdict(float)
        for mk in months:
            for seg, val in month_seg[mk].items():
                seg_tot[seg] += val
        top_segs = sorted(seg_tot, key=lambda s: -seg_tot[s])[:5]
        traces: list[dict[str, Any]] = []
        trows_b: list[list[Any]] = []
        for seg in top_segs:
            ranks: list[int | None] = []
            for mk in months:
                ordered = sorted(month_seg[mk].items(), key=lambda x: -x[1])
                rank = next((i + 1 for i, (s, _) in enumerate(ordered) if s == seg), None)
                ranks.append(rank)
            traces.append({"name": seg, "x": months, "y": ranks})
            trows_b.append([seg] + [r if r is not None else "—" for r in ranks])
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "bump", "traces": traces},
            "table": _table(["Segment"] + months, trows_b),
            "params": {"start": start, "end": end, "metric": m, "dim": d},
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
            db, start=start, end=end, metric=ga4_key, profile="android", project="doviz"
        )
        vir = get_play_virgul_overlay_series(
            db, start=start, end=end, metric=vir_key, branch="android", project="doviz"
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
                    [r.get("key"), _series_map(ga4).get(str(r.get("key")), 0), _series_map(vir).get(str(r.get("key")), 0)]
                    for r in (ga4.get("series") or [])[:60]
                ],
            ),
            "params": {"start": start, "end": end, "metric_left": ml, "metric_right": mr},
        }

    if vid == "stacked100":
        segs = _top_segments(metric=m, dim=d, start=start, end=end, limit=6, facts=facts, meta=meta)
        weeks = sorted(
            {
                str(r.get("key"))
                for r in (_q(metric=m, start=start, end=end, breakdown="week", facts=facts, meta=meta).get("series") or [])
            }
        )[-10:]
        traces_s: list[dict[str, Any]] = []
        trows_s: list[list[Any]] = []
        for seg_row in segs:
            seg = str(seg_row.get("key") or "")
            data = _q(
                metric=m,
                start=start,
                end=end,
                breakdown="week",
                dim=d,
                segment=seg,
                facts=facts,
                meta=meta,
            )
            sm = _series_map(data)
            y = [sm.get(w, 0.0) for w in weeks]
            traces_s.append({"name": seg, "x": weeks, "y": y})
            trows_s.append([seg] + [round(v, 2) for v in y])
        # normalize per week to 100%
        if traces_s and weeks:
            for wi, wk in enumerate(weeks):
                tot = sum(tr["y"][wi] for tr in traces_s)
                if tot > 0:
                    for tr in traces_s:
                        tr["y"][wi] = round(tr["y"][wi] / tot * 100, 2)
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "stacked100", "traces": traces_s},
            "table": _table(["Segment"] + weeks, trows_s),
            "params": {"start": start, "end": end, "metric": m, "dim": d},
        }

    if vid == "boxplot":
        segs = _top_segments(metric=m, dim="app_version", start=start, end=end, limit=6, facts=facts, meta=meta)
        box_traces: list[dict[str, Any]] = []
        trows_box: list[list[Any]] = []
        for seg_row in segs:
            ver = str(seg_row.get("key") or "")
            data = _q(
                metric=m,
                start=start,
                end=end,
                breakdown="date",
                dim="app_version",
                segment=ver,
                facts=facts,
                meta=meta,
            )
            vals = _series_vals(data)
            if vals:
                box_traces.append({"name": ver, "y": vals})
                trows_box.append([ver, round(statistics.mean(vals), 4), round(min(vals), 4), round(max(vals), 4)])
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "boxplot", "traces": box_traces},
            "table": _table(["Version", "Mean", "Min", "Max"], trows_box),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "scatter":
        snap = play_console_payload(db) or {}
        package = (snap.get("package_name") or "com.Doviz").strip()
        panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
        vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
        stab = build_stability_free_payload(package_name=package, product_id="doviz", vitals=vitals)
        pts: list[dict[str, Any]] = []
        rows_sc: list[list[Any]] = []
        for v in stab.get("play_versions") or []:
            if not isinstance(v, dict):
                continue
            anr = v.get("anr_rate_pct")
            crash = v.get("crash_rate_pct")
            users = v.get("user_count") or v.get("users") or 100
            if anr is None and crash is None:
                cf = v.get("crash_free_pct")
                if cf is not None:
                    crash = max(0.0, 100.0 - float(cf))
            if anr is None or crash is None:
                continue
            try:
                pts.append(
                    {
                        "name": v.get("version_name") or v.get("version_code"),
                        "x": float(anr),
                        "y": float(crash),
                        "size": max(5.0, float(users)),
                    }
                )
                rows_sc.append([v.get("version_name"), anr, crash, users])
            except (TypeError, ValueError):
                continue
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "scatter", "points": pts},
            "table": _table(["Version", "ANR %", "Crash %", "Users"], rows_sc),
            "params": {},
        }

    if vid == "calendar":
        data = _q(metric=m, start=start, end=end, facts=facts, meta=meta)
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "calendar", "series": data.get("series") or []},
            "table": _table(["Date", "Value"], [[r.get("key"), r.get("value")] for r in (data.get("series") or [])]),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "sankey":
        steps = [
            ("Store", "ar2_visitors"),
            ("Install", "ar2_acquisitions"),
            ("DAU", "dau"),
            ("Active devices", "active_devices"),
        ]
        nodes: list[str] = []
        links: list[dict[str, Any]] = []
        prev_val: float | None = None
        prev_label: str | None = None
        for label, mk in steps:
            data = _q(metric=mk, start=start, end=end, facts=facts, meta=meta)
            val = _total(data)
            nodes.append(label)
            if prev_val is not None and prev_label:
                links.append({"source": prev_label, "target": label, "value": round(min(prev_val, val), 2)})
            prev_label = label
            prev_val = val
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "sankey", "nodes": nodes, "links": links},
            "table": _table(["Node", "Total"], [[label, _total(_q(metric=mk, start=start, end=end, facts=facts, meta=meta))] for label, mk in steps]),
            "params": {"start": start, "end": end},
        }

    if vid == "horizon":
        metric_list = [x.strip() for x in (metrics or "anrs,crashes,dau,ga4:sessions").split(",") if x.strip()][:6]
        traces_h: list[dict[str, Any]] = []
        trows_h: list[list[Any]] = []
        for mk in metric_list:
            if mk.startswith("ga4:"):
                from backend.api.play_analytics import get_play_ga4_overlay_series

                gk = mk.replace("ga4:", "")
                payload = get_play_ga4_overlay_series(
                    db, start=start, end=end, metric=gk, profile="android", project="doviz"
                )
                series = payload.get("series") or []
                label = payload.get("label") or mk
            else:
                data = _q(metric=mk, start=start, end=end, facts=facts, meta=meta)
                series = data.get("series") or []
                label = mk
            xs = [str(r.get("key")) for r in series]
            ys = _series_vals({"series": series})
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

    if vid == "marimekko":
        devs = _top_segments(metric=m, dim="device", start=start, end=end, limit=5, facts=facts, meta=meta)
        cols: list[dict[str, Any]] = []
        trows_m: list[list[Any]] = []
        for dev_row in devs:
            dev = str(dev_row.get("key") or "")
            width = float(dev_row.get("value") or 0)
            os_data = _q(
                metric=m,
                start=start,
                end=end,
                breakdown="segment",
                dim="os_version",
                segment=None,
                facts=facts,
                meta=meta,
            )
            segs = (os_data.get("series") or [])[:4]
            total_os = sum(float(s.get("value") or 0) for s in segs) or 1.0
            segments = [
                {"label": str(s.get("key")), "share": float(s.get("value") or 0) / total_os}
                for s in segs
            ]
            cols.append({"label": dev, "width": width, "segments": segments})
            trows_m.append([dev, width, ", ".join(f"{s['label']}:{round(s['share']*100)}%" for s in segments)])
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "marimekko", "columns": cols},
            "table": _table(["Device", "Traffic", "OS mix"], trows_m),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "control":
        data = _q(metric=m, start=start, end=end, facts=facts, meta=meta)
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
            "table": _table(["Date", "Value", "Alarm"], [[xs[i], ys[i], "yes" if i in alarms else ""] for i in range(len(xs))]),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "pareto":
        issues = _vitals_issues(db, etype=etype)
        issues.sort(key=lambda i: -float(i.get("events") or i.get("event_count") or 0))
        issues = issues[:lim]
        labels_p = [str(i.get("title") or "?")[:50] for i in issues]
        values_p = [float(i.get("events") or i.get("event_count") or 0) for i in issues]
        total_ev = sum(values_p) or 1.0
        cum: list[float] = []
        run = 0.0
        for v in values_p:
            run += v
            cum.append(round(run / total_ev * 100, 1))
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "pareto", "labels": labels_p, "values": values_p, "cumulative": cum},
            "table": _table(["Issue", "Events", "Cum %"], [[l, v, c] for l, v, c in zip(labels_p, values_p, cum)]),
            "params": {"etype": etype, "limit": lim},
        }

    if vid == "multiples":
        dims_m = ["device", "os_version", "country", "app_version"]
        panels_m: list[dict[str, Any]] = []
        trows_mp: list[list[Any]] = []
        for dm in dims_m:
            data = _q(metric=m, start=start, end=end, breakdown="week", dim=dm, facts=facts, meta=meta)
            if str(data.get("dim") or dm) != dm and data.get("series"):
                data = _q(metric=m, start=start, end=end, breakdown="week", dim="overview", facts=facts, meta=meta)
            seg = _top_segments(metric=m, dim=dm, start=start, end=end, limit=1, facts=facts, meta=meta)
            if seg:
                data = _q(
                    metric=m,
                    start=start,
                    end=end,
                    breakdown="week",
                    dim=dm,
                    segment=str(seg[0].get("key")),
                    facts=facts,
                    meta=meta,
                )
            panels_m.append(
                {
                    "title": dm,
                    "x": [str(r.get("key")) for r in (data.get("series") or [])],
                    "y": _series_vals(data),
                }
            )
            trows_mp.append([dm, _total(data)])
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "multiples", "panels": panels_m},
            "table": _table(["Breakdown", "Total"], trows_mp),
            "params": {"start": start, "end": end, "metric": m},
        }

    if vid == "timeline":
        crash_data = _q(metric=m or "crashes", start=start, end=end, facts=facts, meta=meta)
        releases = _timeline_releases(db, start or "", end or "")
        spikes = sorted(crash_data.get("series") or [], key=lambda r: -float(r.get("value") or 0))[:5]
        rel_rows = [
            [r.get("date"), r.get("version") or "—", r.get("version_code") or "—"]
            for r in releases
        ]
        series_rows = [[r.get("key"), r.get("value")] for r in (crash_data.get("series") or [])[:30]]
        return {
            "ok": True,
            "viz": vid,
            "chart": {
                "type": "timeline",
                "releases": releases,
                "spikes": [{"date": r.get("key"), "value": r.get("value")} for r in spikes],
                "series": crash_data.get("series") or [],
            },
            "table": _table(
                ["Tarih", "Sürüm", "Versiyon kodu"],
                rel_rows,
            )
            if rel_rows
            else _table(["Date", "Value"], series_rows),
            "params": {"start": start, "end": end, "metric": m or "crashes"},
        }

    if vid == "matrix":
        metric_list = [x.strip() for x in (metrics or "anrs,crashes,dau,revenue").split(",") if x.strip()][:8]
        weeks = sorted(
            {
                str(r.get("key"))
                for r in (_q(metric=metric_list[0], start=start, end=end, breakdown="week", facts=facts, meta=meta).get("series") or [])
            }
        )[-8:]
        z_m: list[list[float]] = []
        trows_mx: list[list[Any]] = []
        for mk in metric_list:
            data = _q(metric=mk, start=start, end=end, breakdown="week", facts=facts, meta=meta)
            sm = _series_map(data)
            row = [round(sm.get(w, 0.0), 2) for w in weeks]
            z_m.append(row)
            trows_mx.append([mk] + row)
        return {
            "ok": True,
            "viz": vid,
            "chart": {"type": "heatmap", "x": weeks, "y": metric_list, "z": z_m, "matrix": True},
            "table": _table(["Metric"] + weeks, trows_mx),
            "params": {"start": start, "end": end, "metrics": ",".join(metric_list)},
        }

    return {"ok": False, "viz": viz_id, "message": f"Bilinmeyen viz: {viz_id}", "chart": {}, "table": _table([], [])}
