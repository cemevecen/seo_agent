"""Firebase Crashlytics → BigQuery üzerinden hata özeti."""

from __future__ import annotations

import logging
import re
from typing import Any

from backend.config import settings
from backend.services.app_intel import APP_PRODUCTS
from backend.services.ga4_auth import ga4_is_configured, load_ga4_service_account_info

logger = logging.getLogger(__name__)

BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/bigquery.readonly",)

_SAFE_TABLE_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def crashlytics_bigquery_ready() -> bool:
    return ga4_is_configured()


def _resolve_project_id() -> str:
    env = (settings.firebase_crashlytics_bigquery_project or "").strip()
    if env:
        return env
    try:
        info = load_ga4_service_account_info()
    except ValueError:
        return ""
    return (info.get("project_id") or "").strip()


def _dataset_id() -> str:
    d = (settings.firebase_crashlytics_bigquery_dataset or "").strip()
    return d or "firebase_crashlytics"


def _bundle_to_table_id(bundle_id: str) -> str:
    return bundle_id.strip().replace(".", "_")


def _get_bq_client():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(dict(info), scopes=BIGQUERY_SCOPES)
    project = _resolve_project_id()
    if not project:
        raise ValueError(
            "BigQuery proje id bulunamadı. FIREBASE_CRASHLYTICS_BIGQUERY_PROJECT veya geçerli GA4 service account project_id girin."
        )
    return bigquery.Client(credentials=creds, project=project)


def _run_top_issues_query(*, full_table: str, days: int) -> tuple[list[dict[str, Any]], str | None]:
    from google.api_core import exceptions as gexc
    from google.cloud import bigquery

    days = max(1, min(int(days), 90))
    sql = f"""
SELECT
  COALESCE(issue.issue_id, '') AS issue_id,
  COALESCE(issue.title, '') AS issue_title,
  COALESCE(error_type, '') AS error_type,
  COALESCE(platform, '') AS platform,
  COUNT(*) AS event_count
FROM `{full_table}`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
GROUP BY issue.issue_id, issue.title, error_type, platform
ORDER BY event_count DESC
LIMIT 25
"""
    try:
        client = _get_bq_client()
        job = client.query(sql)
        rows: list[dict[str, Any]] = []
        for r in job.result():
            rows.append(
                {
                    "issue_id": r.get("issue_id") or "",
                    "issue_title": r.get("issue_title") or "",
                    "error_type": r.get("error_type") or "",
                    "platform": r.get("platform") or "",
                    "event_count": int(r.get("event_count") or 0),
                }
            )
        return rows, None
    except gexc.NotFound as exc:
        logger.warning("Crashlytics BQ table not found: %s — %s", full_table, exc)
        return [], (
            f"Tablo bulunamadı: `{full_table}`. Firebase’de Crashlytics→BigQuery aktarımını ve "
            "tablo adının bundle id’nin noktaları alt çizgiyle eşleştiğini doğrulayın."
        )
    except gexc.Forbidden as exc:
        logger.warning("Crashlytics BQ forbidden: %s — %s", full_table, exc)
        return [], (
            "BigQuery erişimi reddedildi. Service account’a bu projede "
            "“BigQuery Data Viewer” ve “BigQuery Job User” rollerini verin."
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Crashlytics BQ query failed for %s", full_table)
        msg = str(exc).strip() or exc.__class__.__name__
        if "issue" in msg.lower() and "unrecognized name" in msg.lower():
            return [], (
                "Sorgu şeması uyuşmadı (issue alanı). BigQuery’de Crashlytics tablosunun güncel şemasını kontrol edin. "
                f"Teknik: {msg}"
            )
        return [], f"BigQuery hatası: {msg}"


def build_crashlytics_payload(product_id: str, days: int = 7) -> dict[str, Any]:
    """Seçili ürün için Android + iOS Crashlytics tablolarından üst issue listesi."""
    pid = (product_id or "").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product"}

    if not crashlytics_bigquery_ready():
        return {
            "ok": False,
            "configured": False,
            "message": "GA4 service account tanımlı değil; BigQuery okuması için aynı kimlik kullanılır (.env: GA4_SERVICE_ACCOUNT_FILE veya JSON).",
        }

    project = _resolve_project_id()
    if not project:
        return {
            "ok": False,
            "configured": False,
            "message": "GCP proje id çözülemedi. FIREBASE_CRASHLYTICS_BIGQUERY_PROJECT veya geçerli service account project_id ekleyin.",
        }

    dataset = _dataset_id()
    meta = APP_PRODUCTS[pid]
    android_pkg = (meta.get("android_package") or "").strip()
    ios_bundle = (meta.get("ios_bundle_id") or "").strip()

    platforms: dict[str, Any] = {}
    for label, bundle, kind in (
        ("android", android_pkg, "Android"),
        ("ios", ios_bundle, "iOS"),
    ):
        if not bundle:
            platforms[label] = {
                "label": kind,
                "skipped": True,
                "reason": f"{label} bundle / package tanımlı değil (APP_PRODUCTS).",
                "full_table": None,
                "issues": [],
            }
            continue

        table_id = _bundle_to_table_id(bundle)
        if not _SAFE_TABLE_ID.match(table_id):
            platforms[label] = {
                "label": kind,
                "skipped": True,
                "reason": "Geçersiz tablo adı türetimi.",
                "full_table": None,
                "issues": [],
            }
            continue

        full_table = f"{project}.{dataset}.{table_id}"
        issues, err = _run_top_issues_query(full_table=full_table, days=days)
        entry: dict[str, Any] = {
            "label": kind,
            "bundle_or_package": bundle,
            "table_id": table_id,
            "full_table": full_table,
            "issues": issues,
        }
        if err:
            entry["error"] = err
        platforms[label] = entry

    fatal_total = 0
    anr_total = 0
    nonfatal_total = 0
    for p in platforms.values():
        if p.get("skipped") or p.get("error"):
            continue
        for row in p.get("issues") or []:
            et = (row.get("error_type") or "").upper()
            n = int(row.get("event_count") or 0)
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
        "bigquery_project": project,
        "bigquery_dataset": dataset,
        "platforms": platforms,
        "totals": {
            "fatal_events": fatal_total,
            "anr_events": anr_total,
            "non_fatal_events": nonfatal_total,
        },
    }
