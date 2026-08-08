"""Play Console scrape explorer — snapshot içindeki explorer_facts sorgusu.

GCS CSV yerine (veya yanında) Mac bridge scrape kataloğundan
cihaz edinme / DAU / gelir / çökme vb. kırılımlı veriyi sunar.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from backend.services import play_console_store

# UI metric key → scrape metric_key
_SCRAPE_METRICS = (
    "device_acquisition",
    "user_lost",
    "active_devices",
    "dau",
    "ar2_acquisitions",
    "rating",
    "active_users",
    "crashes",
    "revenue",
    "ar2_visitors",
    # GCS uyumu
    "installs",
    "uninstalls",
    "active",
    "net",
    "anrs",
)

_METRIC_ALIASES = {
    "active": "active_devices",
    "installs": "device_acquisition",
}


def scrape_metric_keys() -> list[str]:
    return list(_SCRAPE_METRICS)


def _load_facts() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from backend.database import SessionLocal

    with SessionLocal() as db:
        payload = play_console_store.play_console_payload(db)
    panels = payload.get("panels") if isinstance(payload, dict) else {}
    if not isinstance(panels, dict):
        panels = {}
    facts = panels.get("explorer_facts") or []
    if not isinstance(facts, list):
        facts = []
    meta = {
        "synced_at": payload.get("updated_at") or payload.get("background_synced_at"),
        "stats_views": panels.get("stats_views") or [],
        "explorer_fact_count": panels.get("explorer_fact_count") or len(facts),
        "message": payload.get("message") if isinstance(payload, dict) else None,
    }
    return [f for f in facts if isinstance(f, dict)], meta


def _resolve_metric(metric: str) -> str:
    m = (metric or "").strip()
    return _METRIC_ALIASES.get(m, m)


def query_scrape_analytics(
    *,
    start: str | None = None,
    end: str | None = None,
    metric: str = "active_devices",
    breakdown: str = "date",
    dim: str = "overview",
    segment: str | None = None,
    compare: str | None = "previous_period",
) -> dict[str, Any]:
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else (end_d - timedelta(days=27))
    start_s, end_s = start_d.isoformat(), end_d.isoformat()
    metric_key = _resolve_metric(metric)
    breakdown = breakdown if breakdown in ("date", "week", "month", "segment") else "date"
    dim = dim if dim else "overview"

    facts, meta = _load_facts()
    if not facts:
        return {
            "ok": False,
            "source": "scrape",
            "configured": True,
            "message": (
                "Scrape explorer_facts boş — Mac’te "
                "`play_console_scrape.py --sync --ingest` çalıştır "
                "(tüm statistics URL kataloğu)."
            ),
            "series": [],
            "total": 0,
            "row_count": 0,
            "facets": {"metrics": scrape_metric_keys(), "segments": []},
            "stats_views": meta.get("stats_views") or [],
        }

    # Metrik filtre
    cur = [f for f in facts if str(f.get("metric") or "") == metric_key]
    if not cur:
        available = sorted({str(f.get("metric")) for f in facts if f.get("metric")})
        return {
            "ok": False,
            "source": "scrape",
            "configured": True,
            "message": f"Metrik `{metric_key}` scrape’te yok. Mevcut: {', '.join(available[:12])}",
            "series": [],
            "total": 0,
            "row_count": 0,
            "facets": {"metrics": available or scrape_metric_keys(), "segments": []},
            "stats_views": meta.get("stats_views") or [],
        }

    # Boyut: overview kartları her zaman; dim=country ise country fact’leri
    if dim in ("", "overview", "all"):
        dim_facts = [f for f in cur if str(f.get("dim") or "overview") in ("overview", "")]
        if not dim_facts:
            dim_facts = cur
    else:
        dim_facts = [f for f in cur if str(f.get("dim") or "") == dim]
        if not dim_facts:
            dim_facts = [f for f in cur if str(f.get("dim") or "") == "overview"]

    if segment and segment not in ("", "all", "ALL", "OVERALL"):
        dim_facts = [
            f
            for f in dim_facts
            if str(f.get("segment") or "").upper() == segment.upper()
            or str(f.get("segment") or "") == segment
        ]

    # Tarihli fact’ler
    dated = []
    undated = []
    for f in dim_facts:
        ds = f.get("date")
        if ds and isinstance(ds, str) and len(ds) >= 8 and not str(ds).startswith("i"):
            if start_s <= ds <= end_s:
                dated.append(f)
        else:
            undated.append(f)

    use = dated if dated else undated

    buckets: dict[str, float] = defaultdict(float)
    for f in use:
        if breakdown == "segment":
            key = str(f.get("segment") or f.get("label") or "UNKNOWN")
        elif breakdown in ("week", "month") and f.get("date") and not str(f["date"]).startswith("i"):
            try:
                d = date.fromisoformat(str(f["date"])[:10])
                if breakdown == "week":
                    iso = d.isocalendar()
                    key = f"{iso.year}-W{iso.week:02d}"
                else:
                    key = f"{d.year}-{d.month:02d}"
            except ValueError:
                key = str(f.get("date") or f.get("label") or "?")
        else:
            key = str(f.get("date") or f.get("segment") or f.get("label") or "TOTAL")
        try:
            buckets[key] += float(f.get("value") or 0)
        except (TypeError, ValueError):
            continue

    series = [{"key": k, "value": round(v, 4)} for k, v in buckets.items()]
    if breakdown in ("date", "week", "month"):
        series.sort(key=lambda r: r["key"])
    else:
        series.sort(key=lambda r: (-r["value"], r["key"]))
    series = series[:80]
    total = sum(r["value"] for r in series)

    compare_payload = None
    if compare == "previous_period" and undated and not dated:
        # Snapshot kartlarında önceki dönem delta metni varsa kullanma — total only
        compare_payload = None
    elif compare == "previous_period" and dated:
        span = (end_d - start_d).days + 1
        pe = start_d - timedelta(days=1)
        ps = pe - timedelta(days=span - 1)
        prev = [
            f
            for f in dim_facts
            if f.get("date")
            and isinstance(f.get("date"), str)
            and not str(f["date"]).startswith("i")
            and ps.isoformat() <= str(f["date"]) <= pe.isoformat()
        ]
        pb: dict[str, float] = defaultdict(float)
        for f in prev:
            key = str(f.get("date") or "?")
            try:
                pb[key] += float(f.get("value") or 0)
            except (TypeError, ValueError):
                pass
        prev_series = [{"key": k, "value": round(v, 4)} for k, v in sorted(pb.items())]
        prev_total = sum(r["value"] for r in prev_series)
        delta_pct = None
        if prev_total:
            delta_pct = round((total - prev_total) / abs(prev_total) * 100.0, 2)
        compare_payload = {
            "mode": "previous_period",
            "start": ps.isoformat(),
            "end": pe.isoformat(),
            "total": prev_total,
            "delta_pct": delta_pct,
            "series": prev_series,
        }

    segs = sorted(
        {
            str(f.get("segment"))
            for f in cur
            if f.get("segment") and str(f.get("segment")) not in ("", "OVERALL")
        }
    )[:80]

    return {
        "ok": bool(series),
        "source": "scrape",
        "configured": True,
        "bucket": False,
        "message": (
            f"Scrape · {len(use)} fact · metric={metric_key} · "
            f"views={len(meta.get('stats_views') or [])}"
        ),
        "start": start_s,
        "end": end_s,
        "metric": metric_key,
        "breakdown": breakdown,
        "dim": dim,
        "segment": segment or "all",
        "total": total,
        "series": series,
        "compare": compare_payload,
        "facets": {
            "metrics": scrape_metric_keys(),
            "dims": ["overview", "country", "os_version"],
            "breakdowns": ["date", "week", "month", "segment"],
            "segments": segs,
        },
        "row_count": len(use),
        "stats_views": meta.get("stats_views") or [],
        "synced_at": meta.get("synced_at"),
        "date_min": None,
        "date_max": None,
    }
