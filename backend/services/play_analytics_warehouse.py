"""Play Console analytics — GCS CSV warehouse + tarih/kırılım sorguları.

Kaynak: gs://GP_REPORTS_BUCKET/stats/installs/installs_<pkg>_YYYYMM_<dim>.csv
(ve crashes overview). Virgül tarzı: start/end + breakdown + compare.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from backend.services import gp_client

LOGGER = logging.getLogger(__name__)

_CACHE: dict[str, Any] = {}
_CACHE_TTL = 60 * 20  # 20 dk

_DIM_SUFFIXES = (
    "overview",
    "country",
    "os_version",
    "app_version",
    "device",
    "language",
    "carrier",
)

_DIM_COL_CANDIDATES: dict[str, tuple[str, ...]] = {
    "country": ("Country", "Country Code", "country"),
    "os_version": ("Android OS Version", "OS Version", "os_version"),
    "app_version": ("App Version Name", "App Version Code", "Version", "app_version"),
    "device": ("Device", "device"),
    "language": ("Language", "language"),
    "carrier": ("Carrier", "carrier"),
}

_METRIC_COLS: dict[str, tuple[str, ...]] = {
    "installs": ("Daily User Installs", "Daily Device Installs", "Install events"),
    "uninstalls": ("Daily User Uninstalls", "Daily Device Uninstalls"),
    "active": ("Active Device Installs", "Installs on active devices"),
    "update": ("Daily Device Upgrades", "Daily User Upgrades"),
}


def _pkg() -> str:
    return (gp_client._env("GP_PACKAGE_NAME") or "com.Doviz").strip()  # noqa: SLF001


def _parse_date(s: str) -> str | None:
    t = (s or "").strip()
    if not t:
        return None
    # YYYY-MM-DD or YYYYMMDD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", t):
        return t
    if re.fullmatch(r"\d{8}", t):
        return f"{t[:4]}-{t[4:6]}-{t[6:8]}"
    return None


def _decode_csv(raw: bytes) -> str:
    try:
        return raw.decode("utf-16")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def _pick_col(row: dict[str, str], candidates: tuple[str, ...]) -> str:
    keys = {k.strip(): k for k in row.keys() if k}
    lower = {k.strip().lower(): k for k in row.keys() if k}
    for c in candidates:
        if c in keys:
            return keys[c]
        if c.lower() in lower:
            return lower[c.lower()]
    return ""


def _int_cell(row: dict[str, str], candidates: tuple[str, ...]) -> int:
    col = _pick_col(row, candidates)
    if not col:
        return 0
    try:
        return int(float(str(row.get(col) or "0").replace(",", "").strip() or "0"))
    except (TypeError, ValueError):
        return 0


def _load_install_facts(package_name: str) -> dict[str, Any]:
    """Tüm installs_* CSV'lerini oku → facts listesi."""
    bucket_name = gp_client._env("GP_REPORTS_BUCKET")  # noqa: SLF001
    if not bucket_name or not gp_client.is_configured():
        return {
            "ok": False,
            "configured": bool(gp_client.is_configured()),
            "bucket": bool(bucket_name),
            "facts": [],
            "message": (
                "GP_SERVICE_ACCOUNT_JSON veya GP_REPORTS_BUCKET eksik — "
                "interaktif kırılım için Railway env gerekli."
            ),
        }

    now = time.time()
    cached = _CACHE.get(package_name)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    client = gp_client._get_storage_client()  # noqa: SLF001
    if client is None:
        return {
            "ok": False,
            "configured": True,
            "bucket": True,
            "facts": [],
            "message": "GCS client oluşturulamadı (google-cloud-storage / credentials).",
        }

    try:
        bucket = client.bucket(bucket_name)
        prefix = f"stats/installs/installs_{package_name}_"
        blobs = list(bucket.list_blobs(prefix=prefix))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Play analytics list_blobs: %s", exc)
        return {
            "ok": False,
            "configured": True,
            "bucket": True,
            "facts": [],
            "message": f"Bucket listelenemedi: {exc}",
        }

    facts: list[dict[str, Any]] = []
    files_read = 0
    for blob in blobs:
        name = blob.name or ""
        if not name.endswith(".csv"):
            continue
        # installs_com.Doviz_202608_country.csv
        m = re.search(r"_(\d{6})_([a-z0-9_]+)\.csv$", name)
        if not m:
            continue
        dim = m.group(2)
        if dim not in _DIM_SUFFIXES:
            continue
        try:
            raw = blob.download_as_bytes()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("CSV download %s: %s", name, exc)
            continue
        text = _decode_csv(raw)
        reader = csv.DictReader(io.StringIO(text))
        files_read += 1
        for row in reader:
            if not isinstance(row, dict):
                continue
            ds = _parse_date(str(row.get("Date") or row.get("date") or ""))
            if not ds:
                continue
            segment = "OVERALL"
            if dim != "overview":
                col = _pick_col(row, _DIM_COL_CANDIDATES.get(dim, ()))
                segment = str(row.get(col) or "").strip() or "UNKNOWN"
            installs = _int_cell(row, _METRIC_COLS["installs"])
            uninstalls = _int_cell(row, _METRIC_COLS["uninstalls"])
            active = _int_cell(row, _METRIC_COLS["active"])
            facts.append(
                {
                    "date": ds,
                    "dim": dim,
                    "segment": segment,
                    "installs": installs,
                    "uninstalls": uninstalls,
                    "active": active,
                    "net": installs - uninstalls,
                }
            )

    # Crashes overview (opsiyonel)
    try:
        cprefix = f"stats/crashes/crashes_{package_name}_"
        for blob in bucket.list_blobs(prefix=cprefix):
            name = blob.name or ""
            if not name.endswith("_overview.csv"):
                continue
            raw = blob.download_as_bytes()
            text = _decode_csv(raw)
            reader = csv.DictReader(io.StringIO(text))
            files_read += 1
            for row in reader:
                if not isinstance(row, dict):
                    continue
                ds = _parse_date(str(row.get("Date") or ""))
                if not ds:
                    continue
                crashes = _int_cell(row, ("Crashes", "Daily Crashes", "crash"))
                anrs = _int_cell(row, ("ANRs", "Daily ANRs", "anr"))
                facts.append(
                    {
                        "date": ds,
                        "dim": "overview",
                        "segment": "OVERALL",
                        "installs": 0,
                        "uninstalls": 0,
                        "active": 0,
                        "net": 0,
                        "crashes": crashes,
                        "anrs": anrs,
                    }
                )
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("crashes CSV skip: %s", exc)

    data = {
        "ok": bool(facts),
        "configured": True,
        "bucket": True,
        "package_name": package_name,
        "facts": facts,
        "files_read": files_read,
        "message": (
            f"{len(facts)} satır · {files_read} CSV"
            if facts
            else "Bucket’ta installs CSV bulunamadı (prefix/izin kontrol et)."
        ),
        "loaded_at": datetime.utcnow().isoformat() + "Z",
    }
    _CACHE[package_name] = {"ts": now, "data": data}
    return data


def _period_key(ds: str, grain: str) -> str:
    try:
        d = date.fromisoformat(ds)
    except ValueError:
        return ds
    if grain == "week":
        iso = d.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    if grain == "month":
        return f"{d.year}-{d.month:02d}"
    return ds  # day


def _filter_facts(
    facts: list[dict[str, Any]],
    *,
    start: str,
    end: str,
    dim: str,
    segment: str | None,
) -> list[dict[str, Any]]:
    out = []
    for f in facts:
        if f.get("dim") != dim:
            continue
        ds = f.get("date") or ""
        if ds < start or ds > end:
            continue
        if segment and segment not in ("", "all", "ALL", "OVERALL"):
            if str(f.get("segment") or "").upper() != segment.upper() and str(f.get("segment")) != segment:
                # allow exact match case-sensitive too
                if str(f.get("segment")) != segment:
                    continue
        out.append(f)
    return out


def _aggregate(
    facts: list[dict[str, Any]],
    *,
    breakdown: str,
    metric: str,
    limit: int = 60,
) -> list[dict[str, Any]]:
    """breakdown: date|week|month|segment"""
    buckets: dict[str, float] = defaultdict(float)
    for f in facts:
        if breakdown in ("date", "week", "month"):
            key = _period_key(str(f.get("date")), "day" if breakdown == "date" else breakdown)
        else:
            key = str(f.get("segment") or "UNKNOWN")
        val = float(f.get(metric) or 0)
        # active: son gün değeri için max; diğerleri sum
        if metric == "active":
            buckets[key] = max(buckets[key], val)
        else:
            buckets[key] += val

    rows = [{"key": k, "value": round(v, 4)} for k, v in buckets.items()]
    if breakdown in ("date", "week", "month"):
        rows.sort(key=lambda r: r["key"])
    else:
        rows.sort(key=lambda r: (-r["value"], r["key"]))
    return rows[:limit]


def _prev_range(start: str, end: str) -> tuple[str, str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    span = (e - s).days + 1
    pe = s - timedelta(days=1)
    ps = pe - timedelta(days=span - 1)
    return ps.isoformat(), pe.isoformat()


def query_play_analytics(
    *,
    start: str | None = None,
    end: str | None = None,
    metric: str = "installs",
    breakdown: str = "date",
    dim: str = "overview",
    segment: str | None = None,
    compare: str | None = "previous_period",
    package_name: str | None = None,
) -> dict[str, Any]:
    pkg = (package_name or _pkg()).strip() or "com.Doviz"
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else (end_d - timedelta(days=27))
    start_s, end_s = start_d.isoformat(), end_d.isoformat()

    metric = metric if metric in ("installs", "uninstalls", "active", "net", "crashes", "anrs") else "installs"
    breakdown = breakdown if breakdown in ("date", "week", "month", "segment") else "date"
    dim = dim if dim in _DIM_SUFFIXES else "overview"
    if breakdown == "segment" and dim == "overview":
        dim = "country"

    warehouse = _load_install_facts(pkg)
    facts = warehouse.get("facts") or []
    cur_facts = _filter_facts(facts, start=start_s, end=end_s, dim=dim, segment=segment)
    # crashes/anrs only on overview rows that have those keys — if metric crashes use overview
    if metric in ("crashes", "anrs"):
        dim = "overview"
        cur_facts = _filter_facts(facts, start=start_s, end=end_s, dim="overview", segment=None)
        cur_facts = [f for f in cur_facts if metric in f]

    series = _aggregate(cur_facts, breakdown=breakdown, metric=metric)
    total = sum(r["value"] for r in series) if metric != "active" else (series[-1]["value"] if series else 0)

    compare_payload = None
    if compare == "previous_period":
        ps, pe = _prev_range(start_s, end_s)
        prev_facts = _filter_facts(facts, start=ps, end=pe, dim=dim if metric not in ("crashes", "anrs") else "overview", segment=segment)
        if metric in ("crashes", "anrs"):
            prev_facts = [f for f in prev_facts if metric in f]
        prev_series = _aggregate(prev_facts, breakdown=breakdown, metric=metric)
        prev_total = sum(r["value"] for r in prev_series) if metric != "active" else (prev_series[-1]["value"] if prev_series else 0)
        delta_pct = None
        if prev_total:
            delta_pct = round((total - prev_total) / abs(prev_total) * 100.0, 2)
        compare_payload = {
            "mode": "previous_period",
            "start": ps,
            "end": pe,
            "total": prev_total,
            "delta_pct": delta_pct,
            "series": prev_series,
        }

    # facets: available segments for current dim
    seg_set = sorted(
        {
            str(f.get("segment"))
            for f in facts
            if f.get("dim") == dim and start_s <= str(f.get("date")) <= end_s
        }
    )[:80]

    return {
        "ok": bool(warehouse.get("ok")) or bool(series),
        "configured": warehouse.get("configured"),
        "bucket": warehouse.get("bucket"),
        "message": warehouse.get("message"),
        "package_name": pkg,
        "start": start_s,
        "end": end_s,
        "metric": metric,
        "breakdown": breakdown,
        "dim": dim,
        "segment": segment or "all",
        "total": total,
        "series": series,
        "compare": compare_payload,
        "facets": {
            "dims": list(_DIM_SUFFIXES),
            "metrics": ["installs", "uninstalls", "net", "active", "crashes", "anrs"],
            "breakdowns": ["date", "week", "month", "segment"],
            "segments": seg_set,
        },
        "files_read": warehouse.get("files_read"),
        "loaded_at": warehouse.get("loaded_at"),
        "row_count": len(cur_facts),
    }


def play_analytics_status() -> dict[str, Any]:
    pkg = _pkg()
    wh = _load_install_facts(pkg)
    dates = sorted({f["date"] for f in (wh.get("facts") or [])})
    return {
        "ok": wh.get("ok"),
        "configured": wh.get("configured"),
        "bucket": wh.get("bucket"),
        "package_name": pkg,
        "message": wh.get("message"),
        "files_read": wh.get("files_read"),
        "fact_count": len(wh.get("facts") or []),
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "loaded_at": wh.get("loaded_at"),
    }
