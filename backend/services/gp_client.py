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

import json
import logging
import os
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
    end = date.today() - timedelta(days=1)
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
    end = date.today() - timedelta(days=1)
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
    end = date.today() - timedelta(days=1)
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
    """Google Play vitals (crash rate, ANR rate) + scraper rating verisini birleştirir."""
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

    return {
        "source": "live",
        "crash_rate_series": crash_series,
        "crash_rate_latest": latest_crash,
        "anr_rate_series": anr_series,
        "anr_rate_latest": latest_anr,
        "dates": [r["date"] for r in crash_rows],
    }
