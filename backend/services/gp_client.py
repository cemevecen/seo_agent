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
  - "View financial data" (gelir için)

Google Play Reporting API → https://developers.google.com/play/developer/reporting/reference/rest
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)

_GP_SCOPES = [
    "https://www.googleapis.com/auth/androidpublisher",
    "https://www.googleapis.com/auth/playdeveloperreporting",
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

    # Son 13 ay için aylık dosyaları listele (365 gün için yeterli)
    today = date.today()
    months_to_fetch = set()
    for off in range(0, 390, 28):
        d = today - timedelta(days=off)
        months_to_fetch.add(f"{d.year:04d}{d.month:02d}")

    daily_rows: dict[str, dict[str, float]] = {}
    for ym in sorted(months_to_fetch):
        blob_path = f"stats/installs/installs_{package_name}_{ym}_overview.csv"
        try:
            blob = bucket.blob(blob_path)
            if not blob.exists():
                continue
            raw = blob.download_as_bytes()
        except Exception as exc:
            logger.debug("GP CSV yok ya da erişilemiyor (%s): %s", blob_path, exc)
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
