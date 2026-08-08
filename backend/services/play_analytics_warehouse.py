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


def _recent_yyyymm(months: int = 4) -> set[str]:
    today = date.today()
    out: set[str] = set()
    y, m = today.year, today.month
    for _ in range(max(1, months)):
        out.add(f"{y}{m:02d}")
        m -= 1
        if m <= 0:
            m = 12
            y -= 1
    return out


def _load_install_facts(
    package_name: str,
    *,
    dims: set[str] | None = None,
    months: int = 4,
) -> dict[str, Any]:
    """Tüm installs_* CSV'lerini oku → facts listesi.

    dims: yalnızca bu boyutlar (+ overview her zaman). None = hepsi.
    months: son N ay dosyası (hız için).
    """
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

    want_dims = set(dims) if dims else set(_DIM_SUFFIXES)
    want_dims.add("overview")
    cache_key = f"{package_name}|{','.join(sorted(want_dims))}|m{months}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["data"]
    # Hata cache (403 vs) kısa TTL — UI kilitlenmesin
    err_cached = _CACHE.get(cache_key + "|err")
    if err_cached and (now - err_cached["ts"]) < 120:
        return err_cached["data"]

    client = gp_client._get_storage_client()  # noqa: SLF001
    if client is None:
        return {
            "ok": False,
            "configured": True,
            "bucket": True,
            "facts": [],
            "message": "GCS client oluşturulamadı (google-cloud-storage / credentials).",
        }

    months_ok = _recent_yyyymm(months)
    prefix = f"stats/installs/installs_{package_name}_"
    sample_names: list[str] = []
    raw_count = 0
    try:
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=300))
        raw_count = len(blobs)
        sample_names = [(b.name or "") for b in blobs[:15]]

        # Prefix boşsa: paket adı / path keşfi
        if not blobs:
            alt_samples: list[str] = []
            try:
                for b in bucket.list_blobs(prefix="stats/installs/", max_results=80):
                    n = b.name or ""
                    alt_samples.append(n)
                    # installs_<pkg>_YYYYMM_dim.csv — pkg case-insensitive eşleş
                    low = n.lower()
                    pkg_low = package_name.lower()
                    if f"installs_{pkg_low}_" in low or (
                        "installs_" in low and "doviz" in low
                    ):
                        blobs.append(b)
                if not sample_names:
                    sample_names = alt_samples[:20]
                raw_count = max(raw_count, len(alt_samples))
            except Exception as exc2:  # noqa: BLE001
                LOGGER.warning("Play analytics installs/ list: %s", exc2)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Play analytics list_blobs: %s", exc)
        msg = str(exc)
        if "403" in msg or "Insufficient Permission" in msg or "Permission" in msg:
            msg = (
                "GCS 403 — Play Console → Users: service account’a "
                "“View app information and download bulk reports” ver. "
                f"({exc})"
            )
        data = {
            "ok": False,
            "configured": True,
            "bucket": True,
            "facts": [],
            "message": msg,
            "debug": {"prefix": prefix, "bucket": bucket_name},
        }
        _CACHE[cache_key + "|err"] = {"ts": now, "data": data}
        return data

    # Önce overview, sonra istenen dim — ay filtresi (önce gevşek: ay yoksa yine al)
    selected = []
    skipped_month = 0
    skipped_dim = 0
    skipped_name = 0
    for blob in blobs:
        name = blob.name or ""
        if not name.endswith(".csv"):
            continue
        # .../installs_com.Doviz_202608_overview.csv
        m = re.search(r"installs_[^/]+_(\d{6})_([a-z0-9_]+)\.csv$", name, re.I)
        if not m:
            skipped_name += 1
            continue
        yyyymm, dim = m.group(1), m.group(2).lower()
        if dim not in want_dims:
            skipped_dim += 1
            continue
        if yyyymm not in months_ok:
            skipped_month += 1
            continue
        selected.append((0 if dim == "overview" else 1, name, dim, blob))

    # Ay filtresi her şeyi elediysse: son bulunan dosyalardan al (max 12)
    if not selected and blobs:
        for blob in blobs:
            name = blob.name or ""
            m = re.search(r"installs_[^/]+_(\d{6})_([a-z0-9_]+)\.csv$", name, re.I)
            if not m:
                continue
            dim = m.group(2).lower()
            if dim not in want_dims:
                continue
            selected.append((0 if dim == "overview" else 1, name, dim, blob))
        selected = selected[:12]

    selected.sort(key=lambda x: (x[0], x[1]))
    selected = selected[:24]

    facts: list[dict[str, Any]] = []
    files_read = 0
    parse_errors = 0
    for _, name, dim, blob in selected:
        try:
            raw = blob.download_as_bytes(timeout=30)
        except TypeError:
            try:
                raw = blob.download_as_bytes()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("CSV download %s: %s", name, exc)
                continue
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

    # Crashes overview (opsiyonel, hızlı)
    if "overview" in want_dims:
        try:
            cprefix = f"stats/crashes/crashes_{package_name}_"
            for blob in bucket.list_blobs(prefix=cprefix, max_results=20):
                name = blob.name or ""
                m = re.search(r"_(\d{6})_overview\.csv$", name)
                if not m or m.group(1) not in months_ok:
                    continue
                try:
                    raw = blob.download_as_bytes()
                except Exception:
                    continue
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

    if not facts:
        if raw_count == 0 and not sample_names:
            msg = (
                f"Bucket’ta installs CSV yok · prefix=`{prefix}`. "
                "Play Console → İndirme raporları / Download reports → İstatistikler "
                "en az bir kez açılsın (GCS’e yazılsın). "
                "GP_PACKAGE_NAME paket adıyla dosya adı birebir olmalı."
            )
        elif raw_count == 0 and sample_names:
            msg = (
                f"Paket prefix’inde dosya yok (`{prefix}`). "
                f"Bucket installs örnekleri: {', '.join(sample_names[:5])}"
            )
        elif not selected:
            msg = (
                f"{raw_count} blob görüldü, filtreye uymadı "
                f"(ay={skipped_month}, dim={skipped_dim}, ad={skipped_name}). "
                f"Örnek: {', '.join(sample_names[:5]) or '—'}"
            )
        else:
            msg = (
                f"{files_read} CSV indirildi ama satır yok (tarih/kolon parse). "
                f"Örnek dosya: {selected[0][1] if selected else '—'}"
            )
    else:
        msg = f"{len(facts)} satır · {files_read} CSV"

    data = {
        "ok": bool(facts),
        "configured": True,
        "bucket": True,
        "package_name": package_name,
        "facts": facts,
        "files_read": files_read,
        "message": msg,
        "debug": {
            "prefix": prefix,
            "bucket": bucket_name,
            "raw_blob_count": raw_count,
            "selected_count": len(selected),
            "months": sorted(months_ok),
            "want_dims": sorted(want_dims),
            "samples": sample_names[:12],
            "skipped": {
                "month": skipped_month,
                "dim": skipped_dim,
                "name": skipped_name,
            },
        },
        "loaded_at": datetime.utcnow().isoformat() + "Z",
    }
    # Boş sonucu uzun cache’leme — keşif sonrası tekrar denenebilsin
    ttl_key = cache_key if facts else cache_key + "|empty"
    _CACHE[ttl_key if facts else cache_key] = {"ts": now - (_CACHE_TTL - 180) if not facts else now, "data": data}
    if facts:
        _CACHE[cache_key] = {"ts": now, "data": data}
    else:
        _CACHE[cache_key] = {"ts": now - (_CACHE_TTL - 120), "data": data}  # ~2 dk sonra yenile
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

    warehouse = _load_install_facts(
        pkg,
        dims={dim, "overview"},
        months=14 if (date.fromisoformat(end_s) - date.fromisoformat(start_s)).days > 100 else 8,
    )
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
        "debug": warehouse.get("debug"),
    }


def play_analytics_status() -> dict[str, Any]:
    pkg = _pkg()
    wh = _load_install_facts(pkg, dims={"overview"}, months=2)
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
