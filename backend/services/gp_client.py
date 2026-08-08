"""
Google Play Developer API ve Play Developer Reporting API istemcisi.

Service account JSON ile kimlik doğrulama yapar; androidpublisher v3 ve
playdeveloperreporting v1beta1 API'lerini kullanır.

Gerekli ortam değişkenleri:
    GP_SERVICE_ACCOUNT_JSON  — Service account JSON'ının TAM içeriği (tek satır ya da multi-line)
                               veya Railway secret olarak ayarlanabilir.
    GP_PACKAGE_NAME          — Opsiyonel; varsayılan APP_PRODUCTS'tan alınır.

Google Play Console'da service account'a şu roller verilmeli:
  - "View app information" (minimum)
  - "View app information and download bulk reports" (GCS installs/crashes CSV)
  - "View financial data" (gelir için)
  - "Release to production" veya en azından production track okuma (staged rollout yüzdesi için)

Google Play Reporting API → https://developers.google.com/play/developer/reporting/reference/rest
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_GP_SCOPES = [
    "https://www.googleapis.com/auth/androidpublisher",
    "https://www.googleapis.com/auth/playdeveloperreporting",
    # Play rapor bucket (pubsite_prod_rev_*) CSV okuma — yoksa list_blobs 403 Insufficient Permission
    "https://www.googleapis.com/auth/devstorage.read_only",
]


# ─── Yapılandırma ────────────────────────────────────────────────────────────

def _env(name: str) -> str | None:
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    return v or None


def is_configured() -> bool:
    return bool(_env("GP_SERVICE_ACCOUNT_JSON"))


def _load_credentials():
    """google.oauth2.service_account.Credentials döndür."""
    raw = _env("GP_SERVICE_ACCOUNT_JSON") or ""
    # Railway'de tek satır JSON ya da \\n ile escape edilmiş olabilir
    if "\\n" in raw and "\n" not in raw:
        raw = raw.replace("\\n", "\n")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("GP service account JSON parse hatası: %s", exc)
        return None
    try:
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info(info, scopes=_GP_SCOPES)
    except Exception as exc:
        logger.error("GP credentials oluşturulamadı: %s", exc)
        return None


# ─── Android Publisher API (reviews, install stats) ─────────────────────────

def _publisher_service():
    creds = _load_credentials()
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        return build("androidpublisher", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:
        logger.error("GP androidpublisher service hatası: %s", exc)
        return None


def fetch_app_details(package_name: str) -> dict[str, Any] | None:
    """Uygulama başlığı, kategori, içerik derecelendirmesi gibi metadata."""
    svc = _publisher_service()
    if svc is None:
        return None
    try:
        res = svc.edits().insert(body={}, packageName=package_name).execute()
        edit_id = res["id"]
        details = svc.edits().details().get(packageName=package_name, editId=edit_id).execute()
        # editleri commit etme — sadece okuma
        svc.edits().delete(packageName=package_name, editId=edit_id).execute()
        return details
    except Exception as exc:
        logger.warning("GP app details hatası (%s): %s", package_name, exc)
        return None


def fetch_reviews(package_name: str, *, lang: str = "tr", max_results: int = 100) -> list[dict[str, Any]]:
    """Son yorumları çek."""
    svc = _publisher_service()
    if svc is None:
        return []
    try:
        resp = svc.reviews().list(
            packageName=package_name,
            translationLanguage=lang,
            maxResults=max_results,
        ).execute()
        return resp.get("reviews") or []
    except Exception as exc:
        logger.warning("GP reviews hatası (%s): %s", package_name, exc)
        return []


# ─── Play Developer Reporting API ───────────────────────────────────────────
# Bu API daha yeni; Google Play Console Statistics'e karşılık gelir.
# Desteklenen metrikler: crashRate, anrRate, stuckBackgroundWakelockRate
# Ayrıca: errorCountMetricSet, excessiveWakeupRateMetricSet, vs.

def _reporting_service():
    creds = _load_credentials()
    if creds is None:
        return None
    try:
        from googleapiclient.discovery import build
        return build(
            "playdeveloperreporting",
            "v1beta1",
            credentials=creds,
            cache_discovery=False,
        )
    except Exception as exc:
        logger.error("GP playdeveloperreporting service hatası: %s", exc)
        return None


def _date_to_gp(d: date) -> dict:
    """Play Reporting API DateTime formatı.

    Google Play Reporting API "UTC"yi IANA zone olarak kabul etmiyor; metric
    set'lerin default zaman dilimi America/Los_Angeles (Google Play merkezi).
    """
    return {
        "year": d.year,
        "month": d.month,
        "day": d.day,
        "timeZone": {"id": "America/Los_Angeles"},
    }


def fetch_crash_rate(package_name: str, *, days: int = 30) -> dict[str, Any] | None:
    """Günlük çökme oranı trendi (crashRateMetricSet)."""
    svc = _reporting_service()
    if svc is None:
        return None
    # Vitals freshness genelde 2-3 gün geride; güvenlik için 3 gün geri kayalım.
    end = date.today() - timedelta(days=3)
    start = end - timedelta(days=days - 1)
    name = f"apps/{package_name}/crashRateMetricSet"
    body = {
        "timelineSpec": {
            "aggregationPeriod": "DAILY",
            "startTime": _date_to_gp(start),
            "endTime": _date_to_gp(end + timedelta(days=1)),  # endTime exclusive
        },
        "metrics": ["crashRate7dUserWeighted", "crashRate28dUserWeighted"],
        "pageSize": 1000,
    }
    try:
        resp = svc.vitals().crashrate().query(name=name, body=body).execute()
        return resp
    except Exception as exc:
        logger.warning("GP crash rate hatası (%s): %s", package_name, exc)
        return None


def fetch_anr_rate(package_name: str, *, days: int = 30) -> dict[str, Any] | None:
    """ANR oranı trendi."""
    svc = _reporting_service()
    if svc is None:
        return None
    # Vitals freshness genelde 2-3 gün geride; güvenlik için 3 gün geri kayalım.
    end = date.today() - timedelta(days=3)
    start = end - timedelta(days=days - 1)
    name = f"apps/{package_name}/anrRateMetricSet"
    body = {
        "timelineSpec": {
            "aggregationPeriod": "DAILY",
            "startTime": _date_to_gp(start),
            "endTime": _date_to_gp(end + timedelta(days=1)),
        },
        "metrics": ["anrRate7dUserWeighted", "anrRate28dUserWeighted"],
        "pageSize": 1000,
    }
    try:
        resp = svc.vitals().anrrate().query(name=name, body=body).execute()
        return resp
    except Exception as exc:
        logger.warning("GP ANR rate hatası (%s): %s", package_name, exc)
        return None


def _reporting_ui_dim(api_dim: str) -> str:
    return {
        "versionCode": "app_version",
        "deviceModel": "device",
        "apiLevel": "os_version",
        "countryCode": "country",
    }.get(api_dim, api_dim)


def _parse_gp_decimal(raw: Any) -> float | None:
    """google.type.Decimal çoğu zaman {"value": "1.23"} dict gelir."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        raw = raw.get("value")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _dim_segment(row: dict[str, Any], dim: str) -> str:
    for d in row.get("dimensions") or []:
        if not isinstance(d, dict) or d.get("dimension") != dim:
            continue
        return str(
            d.get("stringValue")
            or d.get("int64Value")
            or d.get("value")
            or "UNKNOWN"
        )
    return "UNKNOWN"


def _row_date(row: dict[str, Any]) -> str | None:
    st = row.get("startTime") or {}
    try:
        return f"{int(st['year']):04d}-{int(st['month']):02d}-{int(st['day']):02d}"
    except (KeyError, TypeError, ValueError):
        return None


def _parse_freshness_exclusive(meta: dict[str, Any] | None) -> date | None:
    """Metric set get() → DAILY latestEndTime (TimelineSpec endTime için exclusive)."""
    if not isinstance(meta, dict):
        return None
    for f in (meta.get("freshnessInfo") or {}).get("freshnesses") or []:
        if not isinstance(f, dict):
            continue
        if str(f.get("aggregationPeriod") or "") != "DAILY":
            continue
        et = f.get("latestEndTime") or {}
        try:
            return date(int(et["year"]), int(et["month"]), int(et["day"]))
        except (KeyError, TypeError, ValueError):
            continue
    return None


def _freshness_from_error(exc: BaseException) -> date | None:
    """'at most the current freshness 2026-08-06 00:00' → date."""
    m = re.search(
        r"freshness\s+(\d{4})-(\d{2})-(\d{2})",
        str(exc),
        flags=re.I,
    )
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _clip_reporting_range(
    start: date | None,
    end: date | None,
    *,
    freshness_exclusive: date | None,
) -> tuple[date, date, date]:
    """Inclusive start/end + exclusive endTime; freshness dışına çıkma.

    Returns: (start_inclusive, end_inclusive, end_exclusive)
    """
    end_req = end or (date.today() - timedelta(days=3))
    start_d = start or date(2025, 1, 1)
    # İstenen exclusive uç
    end_excl = end_req + timedelta(days=1)
    # Güvenlik: bugünden ileri gitme
    end_excl = min(end_excl, date.today() + timedelta(days=1))
    if freshness_exclusive is not None:
        end_excl = min(end_excl, freshness_exclusive)
    end_incl = end_excl - timedelta(days=1)
    if start_d > end_incl:
        start_d = end_incl - timedelta(days=27)
    return start_d, end_incl, end_excl


def _get_metric_freshness(svc: Any, *, kind: str, package_name: str) -> date | None:
    """kind: anr | crash | errors"""
    try:
        if kind == "anr":
            meta = (
                svc.vitals()
                .anrrate()
                .get(name=f"apps/{package_name}/anrRateMetricSet")
                .execute()
            )
        elif kind == "crash":
            meta = (
                svc.vitals()
                .crashrate()
                .get(name=f"apps/{package_name}/crashRateMetricSet")
                .execute()
            )
        else:
            try:
                meta = (
                    svc.vitals()
                    .errors()
                    .counts()
                    .get(name=f"apps/{package_name}/errorCountMetricSet")
                    .execute()
                )
            except AttributeError:
                meta = (
                    svc.vitals()
                    .errorcounts()
                    .get(name=f"apps/{package_name}/errorCountMetricSet")
                    .execute()
                )
        return _parse_freshness_exclusive(meta)
    except Exception as exc:  # noqa: BLE001
        logger.info("GP freshness get (%s/%s): %s", kind, package_name, exc)
        return None


def fetch_error_counts_by_dimension(
    package_name: str,
    *,
    report_type: str,
    dimension: str = "versionCode",
    start: date | None = None,
    end: date | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Mutlak ANR/çökme sayıları — errorCountMetricSet (reportType zorunlu)."""
    svc = _reporting_service()
    if svc is None:
        return [], "Reporting service yok (GP_SERVICE_ACCOUNT_JSON?)"
    dim = dimension if dimension in ("versionCode", "deviceModel", "apiLevel") else "versionCode"
    ui_dim = _reporting_ui_dim(dim)
    metric_key = "anrs" if report_type == "APPLICATION_NOT_RESPONDING" else "crashes"
    fresh = _get_metric_freshness(svc, kind="errors", package_name=package_name)
    start_d, _, end_excl = _clip_reporting_range(start, end, freshness_exclusive=fresh)
    name = f"apps/{package_name}/errorCountMetricSet"

    def _collect(start_inclusive: date, end_exclusive: date) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "timelineSpec": {
                "aggregationPeriod": "DAILY",
                "startTime": _date_to_gp(start_inclusive),
                "endTime": _date_to_gp(end_exclusive),
            },
            "dimensions": ["reportType", dim],
            "metrics": ["errorReportCount", "distinctUsers"],
            "filter": f'reportType = "{report_type}"',
            "pageSize": 100000,
        }
        out: list[dict[str, Any]] = []
        page_token = None
        while True:
            if page_token:
                body["pageToken"] = page_token
            try:
                resp = svc.vitals().errors().counts().query(name=name, body=body).execute()
            except AttributeError:
                resp = svc.vitals().errorcounts().query(name=name, body=body).execute()
            for row in resp.get("rows") or []:
                if not isinstance(row, dict):
                    continue
                ds = _row_date(row)
                if not ds:
                    continue
                seg = _dim_segment(row, dim)
                if not seg or seg in ("UNKNOWN", report_type):
                    continue
                metrics = {
                    (m.get("metric") or ""): m
                    for m in (row.get("metrics") or [])
                    if isinstance(m, dict)
                }
                count = _parse_gp_decimal(
                    (metrics.get("errorReportCount") or {}).get("decimalValue")
                    or (metrics.get("errorReportCount") or {}).get("value")
                )
                users = _parse_gp_decimal(
                    (metrics.get("distinctUsers") or {}).get("decimalValue")
                    or (metrics.get("distinctUsers") or {}).get("value")
                )
                if count is None and users is None:
                    continue
                out.append(
                    {
                        "metric": metric_key,
                        "view_id": f"{metric_key}_errors_{dim}",
                        "dim": ui_dim,
                        "segment": seg,
                        "date": ds,
                        "value": float(count if count is not None else users or 0.0),
                        "distinct_users": users,
                        "label": f"{metric_key}:{dim}:{seg}",
                        "source": "reporting_api_errors",
                    }
                )
            page_token = resp.get("nextPageToken")
            if not page_token or len(out) >= 200000:
                break
        return out

    try:
        return _collect(start_d, end_excl), None
    except Exception as exc:  # noqa: BLE001
        fresh2 = _freshness_from_error(exc) or fresh
        if fresh2 is not None:
            try:
                start2, _, end2 = _clip_reporting_range(
                    start, end, freshness_exclusive=fresh2
                )
                return _collect(start2, end2), None
            except Exception as exc2:  # noqa: BLE001
                logger.warning("GP errorCount retry (%s): %s", package_name, exc2)
                return [], f"errorCountMetricSet hata: {exc2}"
        logger.warning("GP errorCount (%s/%s/%s): %s", package_name, report_type, dim, exc)
        return [], f"errorCountMetricSet hata: {exc}"


def _query_rate_by_dimension(
    *,
    package_name: str,
    metric_key: str,
    dim: str,
    start: date | None,
    end: date | None,
    rate_kind: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """anrRate / crashRate boyut sorgusu — freshness clip + retry."""
    svc = _reporting_service()
    if svc is None:
        return [], "Reporting service yok"
    ui_dim = _reporting_ui_dim(dim)
    fresh = _get_metric_freshness(svc, kind=rate_kind, package_name=package_name)
    start_d, _, end_excl = _clip_reporting_range(start, end, freshness_exclusive=fresh)
    if rate_kind == "anr":
        name = f"apps/{package_name}/anrRateMetricSet"
        metric_names = ["anrRate", "anrRate7dUserWeighted", "distinctUsers"]
        query_fn = lambda body: svc.vitals().anrrate().query(name=name, body=body).execute()
    else:
        name = f"apps/{package_name}/crashRateMetricSet"
        metric_names = ["crashRate", "crashRate7dUserWeighted", "distinctUsers"]
        query_fn = lambda body: svc.vitals().crashrate().query(name=name, body=body).execute()

    def _collect(start_inclusive: date, end_exclusive: date) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "timelineSpec": {
                "aggregationPeriod": "DAILY",
                "startTime": _date_to_gp(start_inclusive),
                "endTime": _date_to_gp(end_exclusive),
            },
            "dimensions": [dim],
            "metrics": metric_names,
            "pageSize": 100000,
        }
        out: list[dict[str, Any]] = []
        page_token = None
        while True:
            if page_token:
                body["pageToken"] = page_token
            resp = query_fn(body)
            for row in resp.get("rows") or []:
                if not isinstance(row, dict):
                    continue
                ds = _row_date(row)
                if not ds:
                    continue
                seg = _dim_segment(row, dim)
                metrics = {
                    (m.get("metric") or ""): m
                    for m in (row.get("metrics") or [])
                    if isinstance(m, dict)
                }
                rate = None
                for key in metric_names[:2]:
                    rate = _parse_gp_decimal(
                        (metrics.get(key) or {}).get("decimalValue")
                        or (metrics.get(key) or {}).get("value")
                    )
                    if rate is not None:
                        break
                users = _parse_gp_decimal(
                    (metrics.get("distinctUsers") or {}).get("decimalValue")
                    or (metrics.get("distinctUsers") or {}).get("value")
                )
                approx = (
                    round(rate * users, 4)
                    if rate is not None and users is not None
                    else rate
                )
                if approx is None:
                    continue
                out.append(
                    {
                        "metric": metric_key,
                        "view_id": f"{metric_key}_reporting_{dim}",
                        "dim": ui_dim,
                        "segment": seg,
                        "date": ds,
                        "value": float(approx),
                        "distinct_users": users,
                        "label": f"{metric_key}:{dim}:{seg}",
                        "source": "reporting_api",
                    }
                )
            page_token = resp.get("nextPageToken")
            if not page_token or len(out) >= 200000:
                break
        return out

    try:
        return _collect(start_d, end_excl), None
    except Exception as exc:  # noqa: BLE001
        fresh2 = _freshness_from_error(exc) or fresh
        if fresh2 is not None:
            try:
                start2, _, end2 = _clip_reporting_range(
                    start, end, freshness_exclusive=fresh2
                )
                return _collect(start2, end2), None
            except Exception as exc2:  # noqa: BLE001
                return [], str(exc2)
        return [], str(exc)


def fetch_anr_by_dimension(
    package_name: str,
    *,
    dimension: str = "versionCode",
    start: date | None = None,
    end: date | None = None,
) -> list[dict[str, Any]]:
    """ANR kırılımı: önce mutlak errorCount, yoksa anrRate × kullanıcı."""
    dim = (
        dimension
        if dimension in ("versionCode", "deviceModel", "apiLevel", "countryCode")
        else "versionCode"
    )
    err_count = None
    if dim != "countryCode":
        rows, err_count = fetch_error_counts_by_dimension(
            package_name,
            report_type="APPLICATION_NOT_RESPONDING",
            dimension=dim,
            start=start,
            end=end,
        )
        if rows:
            fetch_anr_by_dimension.last_error = None  # type: ignore[attr-defined]
            return rows
        if err_count:
            logger.info("ANR errorCount fallback rate: %s", err_count)

    rows, rate_err = _query_rate_by_dimension(
        package_name=package_name,
        metric_key="anrs",
        dim=dim,
        start=start,
        end=end,
        rate_kind="anr",
    )
    if rows:
        fetch_anr_by_dimension.last_error = None  # type: ignore[attr-defined]
    else:
        fetch_anr_by_dimension.last_error = (  # type: ignore[attr-defined]
            rate_err or err_count or "yanıt boş"
        )
    return rows


def fetch_crash_by_dimension(
    package_name: str,
    *,
    dimension: str = "versionCode",
    start: date | None = None,
    end: date | None = None,
) -> list[dict[str, Any]]:
    """Crash kırılımı: önce errorCount, yoksa crashRate."""
    dim = (
        dimension
        if dimension in ("versionCode", "deviceModel", "apiLevel", "countryCode")
        else "versionCode"
    )
    err_count = None
    if dim != "countryCode":
        rows, err_count = fetch_error_counts_by_dimension(
            package_name,
            report_type="CRASH",
            dimension=dim,
            start=start,
            end=end,
        )
        if rows:
            fetch_crash_by_dimension.last_error = None  # type: ignore[attr-defined]
            return rows
        if err_count:
            logger.info("Crash errorCount fallback rate: %s", err_count)

    rows, rate_err = _query_rate_by_dimension(
        package_name=package_name,
        metric_key="crashes",
        dim=dim,
        start=start,
        end=end,
        rate_kind="crash",
    )
    if rows:
        fetch_crash_by_dimension.last_error = None  # type: ignore[attr-defined]
    else:
        fetch_crash_by_dimension.last_error = (  # type: ignore[attr-defined]
            rate_err or err_count or "yanıt boş"
        )
    return rows


def fetch_slow_render_rate(package_name: str, *, days: int = 30) -> dict[str, Any] | None:
    """Yavaş render oranı (slowRenderingRateMetricSet)."""
    svc = _reporting_service()
    if svc is None:
        return None
    # Vitals freshness genelde 2-3 gün geride; güvenlik için 3 gün geri kayalım.
    end = date.today() - timedelta(days=3)
    start = end - timedelta(days=days - 1)
    name = f"apps/{package_name}/slowRenderingRateMetricSet"
    body = {
        "timelineSpec": {
            "aggregationPeriod": "DAILY",
            "startTime": _date_to_gp(start),
            "endTime": _date_to_gp(end + timedelta(days=1)),
        },
        "metrics": ["slowRenderingRate7dUserWeighted"],
        "pageSize": 1000,
    }
    try:
        resp = svc.vitals().slowrenderingrate().query(name=name, body=body).execute()
        return resp
    except Exception as exc:
        logger.warning("GP slow render hatası (%s): %s", package_name, exc)
        return None


# ─── Cloud Storage rapor bucket (kurulum/kaldırma CSV'leri) ────────────────
# Play Console her uygulama için günlük CSV raporları
# gs://pubsite_prod_rev_<ID>/stats/installs/installs_<package>_YYYYMM_*.csv
# adresine otomatik gönderiyor. Bunları okuyup günlük seri çıkarıyoruz.

_install_cache: dict[str, Any] = {}  # {package: {ts, data}}
_INSTALL_CACHE_TTL = 60 * 30  # 30 dk cache


def _get_storage_client():
    creds = _load_credentials()
    if creds is None:
        return None
    try:
        from google.cloud import storage
        return storage.Client(credentials=creds, project=creds.project_id)
    except ImportError:
        logger.error("google-cloud-storage paketi yüklü değil.")
        return None
    except Exception as exc:
        logger.error("GP storage client hatası: %s", exc)
        return None


def fetch_install_stats(
    package_name: str,
    *,
    days: int = 30,
) -> dict[str, Any] | None:
    """Play Console install CSV'lerini okuyup günlük kurulum/kaldırma serisi çıkar.

    CSV yolu: stats/installs/installs_<package>_YYYYMM_overview.csv
    Kolonlar: Date, Package Name, Daily Device Installs, Daily Device Uninstalls,
              Daily User Installs, Daily User Uninstalls, Active Device Installs, ...
    """
    bucket_name = _env("GP_REPORTS_BUCKET")
    if not bucket_name:
        return None

    # Cache check
    now = time.time()
    cached = _install_cache.get(package_name)
    if cached and (now - cached["ts"]) < _INSTALL_CACHE_TTL:
        return _filter_install_data(cached["data"], days)

    client = _get_storage_client()
    if client is None:
        return None

    try:
        bucket = client.bucket(bucket_name)
    except Exception as exc:
        logger.error("GP bucket erişim hatası (%s): %s", bucket_name, exc)
        return None

    # CSV'leri listele — list_blobs daha sağlam; ay ay probe yapmak
    # blob.exists() ile yetki sorunlarında yanıltıcı False döndürebiliyor.
    prefix = f"stats/installs/installs_{package_name}_"
    try:
        blob_iter = bucket.list_blobs(prefix=prefix)
        candidates = [b for b in blob_iter if b.name.endswith("_overview.csv")]
    except Exception as exc:
        logger.warning("GP bucket listeleme hatası (%s, prefix=%s): %s",
                       bucket_name, prefix, exc)
        return None

    if not candidates:
        logger.warning("GP bucket'ta CSV bulunamadı: prefix=%s", prefix)
        return None

    daily_rows: dict[str, dict[str, float]] = {}
    for blob in candidates:
        try:
            raw = blob.download_as_bytes()
        except Exception as exc:
            logger.warning("GP CSV indirme hatası (%s): %s", blob.name, exc)
            continue

        # Play CSV'leri UTF-16 LE BOM ile geliyor
        try:
            text = raw.decode("utf-16")
        except UnicodeDecodeError:
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue

        reader = csv.DictReader(io.StringIO(text))
        for r in reader:
            ds = (r.get("Date") or "").strip()
            if not ds:
                continue
            try:
                device_installs = int(r.get("Daily Device Installs") or 0)
                device_uninstalls = int(r.get("Daily Device Uninstalls") or 0)
                user_installs = int(r.get("Daily User Installs") or 0)
                user_uninstalls = int(r.get("Daily User Uninstalls") or 0)
                active = int(r.get("Active Device Installs") or 0)
            except (TypeError, ValueError):
                continue
            daily_rows[ds] = {
                "installs": user_installs or device_installs,
                "uninstalls": user_uninstalls or device_uninstalls,
                "active": active,
            }

    if not daily_rows:
        logger.warning("GP install CSV bulunamadı: package=%s bucket=%s",
                       package_name, bucket_name)
        return None

    full_data = {
        "daily": daily_rows,
        "dates_sorted": sorted(daily_rows.keys()),
    }
    _install_cache[package_name] = {"ts": now, "data": full_data}
    return _filter_install_data(full_data, days)


def _filter_install_data(full: dict, days: int) -> dict:
    """Tam datasetten son N gün serisi çıkar."""
    dates_sorted = full["dates_sorted"]
    if not dates_sorted:
        return {"installs_series": [], "uninstalls_series": [],
                "total_installs": 0, "total_uninstalls": 0,
                "active_latest": 0, "dates": []}
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    recent_dates = [d for d in dates_sorted if d >= cutoff]
    if not recent_dates:
        recent_dates = dates_sorted[-days:] if len(dates_sorted) >= days else dates_sorted

    installs_series = [full["daily"][d]["installs"] for d in recent_dates]
    uninstalls_series = [full["daily"][d]["uninstalls"] for d in recent_dates]
    total_installs = sum(installs_series)
    total_uninstalls = sum(uninstalls_series)
    active_latest = full["daily"][recent_dates[-1]].get("active", 0)

    return {
        "installs_series": installs_series,
        "uninstalls_series": uninstalls_series,
        "total_installs": total_installs,
        "total_uninstalls": total_uninstalls,
        "active_latest": active_latest,
        "dates": recent_dates,
    }


# ─── Üst seviye: Play Store Analytics özeti ─────────────────────────────────

def _extract_metric_rows(resp: dict | None, metric_key: str) -> list[dict]:
    """Reporting API response'undan günlük satırları çıkar."""
    if not resp:
        return []
    rows = resp.get("rows") or []
    out = []
    for row in rows:
        # API "startTime" (DateTime) dönüyor; metrics ise liste değil dict liste karışık olabilir
        date_info = row.get("startTime") or row.get("startDate") or {}
        try:
            d = date(int(date_info["year"]), int(date_info["month"]), int(date_info["day"]))
        except (KeyError, TypeError, ValueError):
            continue
        # metrics: liste formatında [{metric: "...", decimalValue: {...}}, ...]
        # ya da dict formatında {metric_key: {decimalValue: "..."}}
        val = 0.0
        raw_metrics = row.get("metrics")
        if isinstance(raw_metrics, list):
            for m in raw_metrics:
                if (m.get("metric") or "") == metric_key:
                    dv = m.get("decimalValue") or {}
                    val = float(dv.get("value") or dv if isinstance(dv, (int, float, str)) else 0)
                    if isinstance(dv, dict):
                        val = float(dv.get("value") or 0)
                    break
        elif isinstance(raw_metrics, dict):
            val_obj = raw_metrics.get(metric_key) or {}
            v = val_obj.get("decimalValue") or val_obj.get("int64Value") or 0
            if isinstance(v, dict):
                v = v.get("value") or 0
            try:
                val = float(v)
            except (TypeError, ValueError):
                val = 0.0
        out.append({"date": d.isoformat(), "value": val})
    return sorted(out, key=lambda r: r["date"])


def build_gp_analytics_payload(
    package_name: str,
    *,
    days: int = 30,
) -> dict[str, Any] | None:
    """Google Play vitals (crash rate, ANR rate) + install/uninstall CSV serisi."""
    if not is_configured():
        return None

    crash_resp = fetch_crash_rate(package_name, days=days)
    anr_resp = fetch_anr_rate(package_name, days=days)

    crash_rows = _extract_metric_rows(crash_resp, "crashRate7dUserWeighted")
    anr_rows = _extract_metric_rows(anr_resp, "anrRate7dUserWeighted")

    crash_series = [r["value"] for r in crash_rows]
    anr_series = [r["value"] for r in anr_rows]

    latest_crash = crash_series[-1] if crash_series else None
    latest_anr = anr_series[-1] if anr_series else None

    install_stats = fetch_install_stats(package_name, days=days)

    return {
        "source": "live",
        "crash_rate_series": crash_series,
        "crash_rate_latest": latest_crash,
        "anr_rate_series": anr_series,
        "anr_rate_latest": latest_anr,
        "dates": [r["date"] for r in crash_rows],
        "install_stats": install_stats,
    }
