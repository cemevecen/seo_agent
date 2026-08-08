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
    "installs": (
        "Daily User Installs",
        "Daily Device Installs",
        "Install events",
        "User Installs",
        "Device Installs",
    ),
    "uninstalls": (
        "Daily User Uninstalls",
        "Daily Device Uninstalls",
        "User Uninstalls",
        "Device Uninstalls",
    ),
    "active": (
        "Installs on active devices",
        "Active Device Installs",
        "Active Devices",
        "Installs on Active Devices",
    ),
    "update": ("Daily Device Upgrades", "Daily User Upgrades"),
}


def _pkg() -> str:
    return (gp_client._env("GP_PACKAGE_NAME") or "com.Doviz").strip()  # noqa: SLF001


def _parse_date(s: str, *, file_yyyymm: str | None = None) -> str | None:
    """Play CSV Date = YYYY-MM-DD. YYYYMMDD’yi yalnızca dosya ayıyla uyumluysa kabul et
    (aksi halde App Version Code gibi 20120403 değerleri sahte tarihe döner)."""
    t = _clean_text(s)
    if not t:
        return None
    ds: str | None = None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", t):
        ds = t
    elif re.fullmatch(r"\d{8}", t) and file_yyyymm and t.startswith(file_yyyymm):
        ds = f"{t[:4]}-{t[4:6]}-{t[6:8]}"
    if not ds:
        return None
    try:
        d = date.fromisoformat(ds)
    except ValueError:
        return None
    # Play raporları son yıllar; 2011–2012 version-code sahteciliğini ele
    if d.year < date.today().year - 4:
        return None
    if file_yyyymm and re.fullmatch(r"\d{6}", file_yyyymm):
        fy, fm = int(file_yyyymm[:4]), int(file_yyyymm[4:6])
        # Dosya ayı ±1 dışında ise kolon kayması
        if abs((d.year - fy) * 12 + (d.month - fm)) > 1:
            return None
    return ds


def _decode_csv(raw: bytes) -> str:
    if not raw:
        return ""
    text = ""
    # Play GCS raporları UTF-16 (çoğunlukla LE + BOM).
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16")
    elif len(raw) >= 4 and raw[1] == 0 and raw[3] == 0:
        try:
            text = raw.decode("utf-16-le")
        except UnicodeDecodeError:
            text = ""
    if not text:
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            try:
                text = raw.decode("utf-16")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", errors="replace")
    # Yanlış 8-bit decode sonrası NUL’lar: D\\x00a\\x00t\\x00e → Date
    if "\x00" in text:
        text = text.replace("\x00", "")
    return text


def _clean_text(s: str | None) -> str:
    return (s or "").replace("\x00", "").replace("\ufeff", "").strip()


def _norm_header(k: str | None) -> str:
    return _clean_text(k)


def _row_get(row: dict[str, str], *names: str) -> str:
    """BOM / NUL / boşluk farkını yok sayarak hücre oku."""
    if not row:
        return ""
    wanted = {n.lower() for n in names}
    for k, v in row.items():
        if _norm_header(k).lower() in wanted:
            return _clean_text(v if v is None else str(v))
    return ""


def _pick_col(row: dict[str, str], candidates: tuple[str, ...]) -> str:
    keys = {_norm_header(k): k for k in row.keys() if k}
    lower = {_norm_header(k).lower(): k for k in row.keys() if k}
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
        raw = _clean_text(str(row.get(col) or "0")).replace(",", "")
        return int(float(raw or "0"))
    except (TypeError, ValueError):
        return 0


def _fact_date_bounds(facts: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    dates = sorted({str(f.get("date")) for f in facts if f.get("date")})
    if not dates:
        return None, None
    return dates[0], dates[-1]


def _sample_headers(row: dict[str, str] | None) -> list[str]:
    if not row:
        return []
    return [_norm_header(k) for k in row.keys()][:20]


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
    cache_key = f"v3|{package_name}|{','.join(sorted(want_dims))}|m{months}"
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
    # GCS listesi alfabetik: …_201105_… önce gelir; max_results=300 eski
    # dosyalarda kalır, 2026 hiç görülmez → ay ay prefix çek.
    pkg_variants: list[str] = []
    for p in (package_name, package_name.lower()):
        if p and p not in pkg_variants:
            pkg_variants.append(p)
    prefix = f"stats/installs/installs_{package_name}_"
    sample_names: list[str] = []
    raw_count = 0
    blobs: list[Any] = []
    seen_names: set[str] = set()

    def _add_blob(b: Any) -> None:
        n = b.name or ""
        if not n or n in seen_names:
            return
        seen_names.add(n)
        blobs.append(b)

    try:
        bucket = client.bucket(bucket_name)
        for yyyymm in sorted(months_ok, reverse=True):
            for pkg in pkg_variants:
                month_prefix = f"stats/installs/installs_{pkg}_{yyyymm}_"
                try:
                    for b in bucket.list_blobs(prefix=month_prefix, max_results=40):
                        _add_blob(b)
                except Exception as exc_m:  # noqa: BLE001
                    LOGGER.debug("list month %s: %s", month_prefix, exc_m)

        raw_count = len(blobs)
        sample_names = sorted({(b.name or "") for b in blobs})[:20]

        if not blobs:
            alt_samples: list[str] = []
            newest_yyyymm: str | None = None
            for pkg in pkg_variants:
                broad = f"stats/installs/installs_{pkg}_"
                try:
                    found_months: set[str] = set()
                    for i, b in enumerate(bucket.list_blobs(prefix=broad)):
                        n = b.name or ""
                        if i < 30:
                            alt_samples.append(n)
                        m = re.search(r"_(\d{6})_[a-z0-9_]+\.csv$", n, re.I)
                        if m:
                            found_months.add(m.group(1))
                        if len(found_months) >= 48 and i > 800:
                            break
                        if i >= 8000:
                            break
                    if found_months:
                        newest_yyyymm = max(found_months)
                        for ym in sorted(found_months, reverse=True)[:10]:
                            for b in bucket.list_blobs(
                                prefix=f"stats/installs/installs_{pkg}_{ym}_",
                                max_results=40,
                            ):
                                _add_blob(b)
                except Exception as exc2:  # noqa: BLE001
                    LOGGER.warning("Play analytics broad list %s: %s", broad, exc2)

            if newest_yyyymm:
                sample_names = [f"(newest_month={newest_yyyymm})"] + sorted(
                    {(b.name or "") for b in blobs}
                )[:15]
            elif not sample_names:
                sample_names = alt_samples[:20]
            raw_count = max(raw_count, len(blobs), len(alt_samples))

            if not blobs:
                try:
                    for b in bucket.list_blobs(prefix="stats/installs/", max_results=120):
                        n = b.name or ""
                        alt_samples.append(n)
                        low = n.lower()
                        if "installs_" in low and "doviz" in low:
                            _add_blob(b)
                    if not sample_names:
                        sample_names = alt_samples[:20]
                    raw_count = max(raw_count, len(alt_samples))
                except Exception as exc3:  # noqa: BLE001
                    LOGGER.warning("Play analytics installs/ list: %s", exc3)
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

    # Önce overview, sonra istenen dim — ay filtresi
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
        selected.append((0 if dim == "overview" else 1, name, dim, yyyymm, blob))

    # Ay filtresi her şeyi elediysse: en yeni yyyymm dosyalarından al (2011 yok)
    if not selected and blobs:
        ranked: list[tuple[str, int, str, str, Any]] = []
        for blob in blobs:
            name = blob.name or ""
            m = re.search(r"installs_[^/]+_(\d{6})_([a-z0-9_]+)\.csv$", name, re.I)
            if not m:
                continue
            yyyymm, dim = m.group(1), m.group(2).lower()
            if dim not in want_dims:
                continue
            try:
                if int(yyyymm[:4]) < date.today().year - 4:
                    continue
            except ValueError:
                continue
            ranked.append((yyyymm, 0 if dim == "overview" else 1, name, dim, blob))
        ranked.sort(key=lambda x: (x[0], -x[1]), reverse=True)
        for yyyymm, pri, name, dim, blob in ranked[:12]:
            selected.append((pri, name, dim, yyyymm, blob))

    selected.sort(key=lambda x: (x[0], x[1]))
    selected = selected[:24]

    facts: list[dict[str, Any]] = []
    files_read = 0
    parse_errors = 0
    header_samples: list[str] = []
    row_samples: list[dict[str, str]] = []
    for _, name, dim, yyyymm, blob in selected:
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
            if not header_samples:
                header_samples = _sample_headers(row)
            # Sadece Date — App Version Code (20120403) tarih sanılmasın
            ds = _parse_date(_row_get(row, "Date", "date"), file_yyyymm=yyyymm)
            if not ds:
                parse_errors += 1
                if len(row_samples) < 2:
                    row_samples.append(
                        {
                            _norm_header(k): _clean_text(str(v or ""))[:48]
                            for k, v in list(row.items())[:8]
                        }
                    )
                continue
            segment = "OVERALL"
            if dim != "overview":
                col = _pick_col(row, _DIM_COL_CANDIDATES.get(dim, ()))
                segment = _clean_text(str(row.get(col) or "")) or "UNKNOWN"
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
                file_ym = m.group(1)
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
                    ds = _parse_date(_row_get(row, "Date", "date"), file_yyyymm=file_ym)
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
            oldest = ""
            if selected:
                oldest = selected[0][1]
            elif sample_names:
                oldest = sample_names[0]
            msg = (
                f"{files_read} CSV indirildi ama geçerli tarih satırı yok "
                f"(parse_err={parse_errors}). "
                f"Başlıklar: {', '.join(header_samples) or '—'}. "
                f"Örnek dosya: {oldest or '—'}. "
                "Not: bucket’ta yalnızca eski aylar (2011…) varsa Play Console’da "
                "Download reports / bulut depolama export’unun güncel olduğundan emin ol."
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
            "headers": header_samples,
            "row_samples": row_samples,
            "parse_errors": parse_errors,
            "skipped": {
                "month": skipped_month,
                "dim": skipped_dim,
                "name": skipped_name,
            },
        },
        "date_min": _fact_date_bounds(facts)[0],
        "date_max": _fact_date_bounds(facts)[1],
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
        if breakdown != "date":
            rows = rows[:limit]
    else:
        rows.sort(key=lambda r: (-r["value"], r["key"]))
        rows = rows[:limit]
    return rows


def _densify_date_series(
    series: list[dict[str, Any]],
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    by_key = {str(r["key"]): float(r.get("value") or 0) for r in series}
    out: list[dict[str, Any]] = []
    cur = start
    while cur <= end:
        k = cur.isoformat()
        out.append({"key": k, "value": round(by_key.get(k, 0.0), 4)})
        cur += timedelta(days=1)
    return out


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
    requested_start, requested_end = start_s, end_s
    auto_shifted = False

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
    date_min, date_max = warehouse.get("date_min"), warehouse.get("date_max")
    if not date_min or not date_max:
        date_min, date_max = _fact_date_bounds(facts)

    # Crash CSV satırları installs=0 ile overview'a karışmasın
    metric_facts = facts
    if metric in ("installs", "uninstalls", "active", "net"):
        metric_facts = [f for f in facts if "crashes" not in f and "anrs" not in f]
        dmin2, dmax2 = _fact_date_bounds(metric_facts)
        if dmin2 and dmax2:
            date_min, date_max = dmin2, dmax2
    elif metric in ("crashes", "anrs"):
        metric_facts = [f for f in facts if metric in f]
        dmin2, dmax2 = _fact_date_bounds(metric_facts)
        if dmin2 and dmax2:
            date_min, date_max = dmin2, dmax2

    # Play raporları 3–7 gün gecikmeli: seçili aralıkta satır yoksa son mevcut güne kaydır
    span_days = (end_d - start_d).days + 1
    cur_facts = _filter_facts(metric_facts, start=start_s, end=end_s, dim=dim, segment=segment)
    if metric in ("crashes", "anrs"):
        dim = "overview"
        cur_facts = _filter_facts(metric_facts, start=start_s, end=end_s, dim="overview", segment=None)

    if not cur_facts and date_max:
        try:
            dmax = date.fromisoformat(str(date_max))
            end_d = dmax
            start_d = end_d - timedelta(days=max(span_days, 1) - 1)
            if date_min:
                dmin = date.fromisoformat(str(date_min))
                if start_d < dmin:
                    start_d = dmin
            start_s, end_s = start_d.isoformat(), end_d.isoformat()
            auto_shifted = (start_s != requested_start) or (end_s != requested_end)
            cur_facts = _filter_facts(metric_facts, start=start_s, end=end_s, dim=dim, segment=segment)
            if metric in ("crashes", "anrs"):
                cur_facts = _filter_facts(metric_facts, start=start_s, end=end_s, dim="overview", segment=None)
        except ValueError:
            pass

    series = _aggregate(cur_facts, breakdown=breakdown, metric=metric)
    if breakdown == "date":
        series = _densify_date_series(series, start=start_d, end=end_d)
    total = sum(r["value"] for r in series) if metric != "active" else (series[-1]["value"] if series else 0)

    compare_payload = None
    if compare == "previous_period":
        ps, pe = _prev_range(start_s, end_s)
        prev_facts = _filter_facts(
            metric_facts,
            start=ps,
            end=pe,
            dim=dim if metric not in ("crashes", "anrs") else "overview",
            segment=segment,
        )
        prev_series = _aggregate(prev_facts, breakdown=breakdown, metric=metric)
        if breakdown == "date":
            prev_series = _densify_date_series(
                prev_series,
                start=date.fromisoformat(ps),
                end=date.fromisoformat(pe),
            )
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
            for f in metric_facts
            if f.get("dim") == dim and start_s <= str(f.get("date")) <= end_s
        }
    )[:80]

    msg = warehouse.get("message") or ""
    if auto_shifted:
        msg = (
            f"Play CSV gecikmeli — aralık {requested_start}…{requested_end} boştu; "
            f"mevcut veriye kaydırıldı ({start_s}…{end_s}). "
            f"Bucket: {date_min or '—'} → {date_max or '—'}. · {msg}"
        )
    elif not cur_facts and metric_facts:
        msg = (
            f"Seçili aralıkta satır yok ({requested_start}…{requested_end}). "
            f"Bucket tarihleri: {date_min or '—'} → {date_max or '—'}. · {msg}"
        )
    elif not metric_facts and facts:
        msg = (
            f"Metrik `{metric}` için satır yok (CSV kolon/parse). "
            f"Ham warehouse: {len(facts)} satır. · {msg}"
        )

    return {
        "ok": bool(warehouse.get("ok")) or bool(series),
        "configured": warehouse.get("configured"),
        "bucket": warehouse.get("bucket"),
        "message": msg,
        "package_name": pkg,
        "start": start_s,
        "end": end_s,
        "requested_start": requested_start,
        "requested_end": requested_end,
        "auto_shifted": auto_shifted,
        "date_min": date_min,
        "date_max": date_max,
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
