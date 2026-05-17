"""Firebase Crashlytics → BigQuery üzerinden hata özeti.

iOS: doviz-ios projesi, CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON credential.
Android: doviz-android projesi, CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON credential.
Her platform kendi BigQuery client'ını kullanır.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from backend.config import settings
from backend.services.app_intel import APP_PRODUCTS

logger = logging.getLogger(__name__)

BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/bigquery.readonly",)

_SAFE_TABLE_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Sabit proje ve dataset bilgileri
_PLATFORM_PROJECTS = {
    "ios": "doviz-ios",
    "android": "doviz-android",
}
_DATASET = "firebase_crashlytics"


def _load_platform_credentials(platform: str) -> dict | None:
    """Platform için service account JSON'ını env var'dan yükle."""
    if platform == "ios":
        raw = (settings.crashlytics_ios_service_account_json or "").strip()
    else:
        raw = (settings.crashlytics_android_service_account_json or "").strip()

    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Crashlytics %s service account JSON ayrıştırılamadı: %s", platform, exc)
        return None


def crashlytics_platform_ready(platform: str) -> bool:
    return bool(_load_platform_credentials(platform))


def crashlytics_bigquery_ready() -> bool:
    return crashlytics_platform_ready("ios") or crashlytics_platform_ready("android")


def _get_bq_client(platform: str):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = _load_platform_credentials(platform)
    if not info:
        raise ValueError(
            f"Crashlytics {platform} service account tanımlı değil. "
            f"Railway'de CRASHLYTICS_{platform.upper()}_SERVICE_ACCOUNT_JSON ekleyin."
        )
    creds = service_account.Credentials.from_service_account_info(info, scopes=BIGQUERY_SCOPES)
    project = _PLATFORM_PROJECTS[platform]
    return bigquery.Client(credentials=creds, project=project)


def _bundle_to_table_id(bundle_id: str) -> str:
    return bundle_id.strip().replace(".", "_")


def _run_top_issues_query(*, platform: str, full_table: str, days: int) -> tuple[list[dict[str, Any]], str | None]:
    from google.api_core import exceptions as gexc

    days = max(1, min(int(days), 90))
    sql = f"""
SELECT
  COALESCE(issue.issue_id, '') AS issue_id,
  COALESCE(issue.title, '') AS issue_title,
  COALESCE(error_type, '') AS error_type,
  COALESCE(platform, '') AS platform,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM `{full_table}`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
GROUP BY issue.issue_id, issue.title, error_type, platform
ORDER BY event_count DESC
LIMIT 25
"""
    try:
        client = _get_bq_client(platform)
        job = client.query(sql)
        rows: list[dict[str, Any]] = []
        for r in job.result():
            rows.append({
                "issue_id": r.get("issue_id") or "",
                "issue_title": r.get("issue_title") or "",
                "error_type": r.get("error_type") or "",
                "platform": r.get("platform") or "",
                "event_count": int(r.get("event_count") or 0),
                "affected_users": int(r.get("affected_users") or 0),
            })
        return rows, None
    except gexc.NotFound as exc:
        logger.warning("Crashlytics BQ table not found: %s — %s", full_table, exc)
        return [], (
            f"Tablo bulunamadı: `{full_table}`. "
            "Firebase'de Crashlytics→BigQuery aktarımı başladıktan 24 saat sonra tablolar oluşur."
        )
    except gexc.Forbidden as exc:
        logger.warning("Crashlytics BQ forbidden: %s — %s", full_table, exc)
        return [], (
            "BigQuery erişimi reddedildi. Service account'a bu projede "
            "'BigQuery Data Viewer' ve 'BigQuery Job User' rollerini verin."
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Crashlytics BQ query failed for %s", full_table)
        msg = str(exc).strip() or exc.__class__.__name__
        return [], f"BigQuery hatası: {msg}"


def _run_crash_free_rate(*, platform: str, full_table: str, days: int) -> dict[str, Any] | None:
    """Son N günde crash-free sessions oranı."""
    from google.api_core import exceptions as gexc

    days = max(1, min(int(days), 90))
    sql = f"""
SELECT
  COUNT(DISTINCT installation_uuid) AS total_users,
  COUNT(DISTINCT IF(error_type = 'FATAL', installation_uuid, NULL)) AS crashed_users
FROM `{full_table}`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
"""
    try:
        client = _get_bq_client(platform)
        job = client.query(sql)
        for r in job.result():
            total = int(r.get("total_users") or 0)
            crashed = int(r.get("crashed_users") or 0)
            if total > 0:
                rate = round((1 - crashed / total) * 100, 2)
            else:
                rate = None
            return {"total_users": total, "crashed_users": crashed, "crash_free_pct": rate}
    except (gexc.NotFound, gexc.Forbidden, Exception):
        pass
    return None


def build_crashlytics_payload(product_id: str, days: int = 7) -> dict[str, Any]:
    """Seçili ürün için Android + iOS Crashlytics tablolarından üst issue listesi."""
    pid = (product_id or "").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product"}

    if not crashlytics_bigquery_ready():
        return {
            "ok": False,
            "configured": False,
            "message": (
                "Crashlytics service account tanımlı değil. "
                "Railway'de CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON ve "
                "CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON ekleyin."
            ),
        }

    meta = APP_PRODUCTS[pid]
    android_pkg = (meta.get("android_package") or "").strip()
    ios_bundle = (meta.get("ios_bundle_id") or "").strip()

    platforms: dict[str, Any] = {}
    for platform, bundle, kind in (
        ("android", android_pkg, "Android"),
        ("ios", ios_bundle, "iOS"),
    ):
        if not crashlytics_platform_ready(platform):
            platforms[platform] = {
                "label": kind,
                "skipped": True,
                "reason": f"{platform} service account tanımlı değil.",
                "issues": [],
            }
            continue

        if not bundle:
            platforms[platform] = {
                "label": kind,
                "skipped": True,
                "reason": f"{platform} bundle/package APP_PRODUCTS'ta tanımlı değil.",
                "issues": [],
            }
            continue

        table_id = _bundle_to_table_id(bundle)
        project = _PLATFORM_PROJECTS[platform]
        full_table = f"{project}.{_DATASET}.{table_id}"

        issues, err = _run_top_issues_query(platform=platform, full_table=full_table, days=days)
        crash_free = _run_crash_free_rate(platform=platform, full_table=full_table, days=days)

        entry: dict[str, Any] = {
            "label": kind,
            "bundle_or_package": bundle,
            "table_id": table_id,
            "full_table": full_table,
            "issues": issues,
            "crash_free": crash_free,
        }
        if err:
            entry["error"] = err
        platforms[platform] = entry

    fatal_total = 0
    anr_total = 0
    nonfatal_total = 0
    affected_users_total = 0
    for p in platforms.values():
        if p.get("skipped") or p.get("error"):
            continue
        for row in p.get("issues") or []:
            et = (row.get("error_type") or "").upper()
            n = int(row.get("event_count") or 0)
            u = int(row.get("affected_users") or 0)
            affected_users_total += u
            if et == "FATAL":
                fatal_total += n
            elif et == "ANR":
                anr_total += n
            elif et == "NON_FATAL":
                nonfatal_total += n

    return {
        "ok": True,
        "configured": True,
        "product": pid,
        "period_days": max(1, min(int(days), 90)),
        "platforms": platforms,
        "totals": {
            "fatal_events": fatal_total,
            "anr_events": anr_total,
            "non_fatal_events": nonfatal_total,
            "affected_users": affected_users_total,
        },
    }
