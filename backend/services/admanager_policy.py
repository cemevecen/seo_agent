"""Google Ad Manager Policy Center servis katmanı.

Kimlik doğrulama: service account JSON → ADMANAGER_SERVICE_ACCOUNT_JSON env var
API: Google Ad Manager REST API v1 (google-api-python-client üzerinden)
Network code: settings.admanager_network_code (varsayılan: 21728129623)

Akış:
  1. Rapor oluştur (create)
  2. Raporu çalıştır (run) → operation adı al
  3. Operation tamamlanana kadar poll et
  4. Sonuçları indir (fetchReportResultRows)
  5. DB'ye kaydet (upsert by url+issue_type)
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from backend.config import settings

logger = logging.getLogger(__name__)

ADMANAGER_SCOPES = ["https://www.googleapis.com/auth/admanager"]
NETWORK_CODE = settings.admanager_network_code

# Sinemalar.com admin URL şablonu: URL path'inden content ID çıkarıp admin linki üret
# Örn: /mobileweb/movieCast/837 → admin.sinemalar.com/contents/837
def _admin_link(url: str) -> str | None:
    import re
    m = re.search(r"/(\d+)(?:/amp)?/?$", url)
    if not m:
        return None
    cid = m.group(1)
    return f"https://www.sinemalar.com/admin/contents/{cid}/edit"


def is_configured() -> bool:
    return bool((settings.admanager_service_account_json or "").strip())


def _get_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = (settings.admanager_service_account_json or "").strip()
    if not raw:
        raise ValueError("ADMANAGER_SERVICE_ACCOUNT_JSON tanımlı değil.")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=ADMANAGER_SCOPES)
    return build("admanager", "v1", credentials=creds, cache_discovery=False)


def _date_str(d: date) -> str:
    return d.isoformat()


def fetch_policy_violations(days: int = 7) -> tuple[list[dict], str | None]:
    """Ad Manager API'den policy ihlallerini çek.

    Döner: (rows, error_message | None)
    Her row: {"url", "issue_type", "category", "ad_requests_7d", "enforcement",
               "first_reported", "last_reported"}
    """
    if not is_configured():
        return [], "ADMANAGER_SERVICE_ACCOUNT_JSON tanımlı değil."

    try:
        svc = _get_service()
        network_name = f"networks/{NETWORK_CODE}"

        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

        # Rapor tanımı — Policy Center boyutları
        report_body = {
            "displayName": f"policy_violations_{end_date.isoformat()}",
            "reportDefinition": {
                "dimensions": ["URL_CHANNEL", "POLICY_VIOLATION_TYPE", "POLICY_VIOLATION_ENFORCEMENT_STATUS"],
                "metrics": ["AD_REQUESTS"],
                "dateRange": {
                    "startDate": {
                        "year": start_date.year,
                        "month": start_date.month,
                        "day": start_date.day,
                    },
                    "endDate": {
                        "year": end_date.year,
                        "month": end_date.month,
                        "day": end_date.day,
                    },
                },
            },
        }

        # 1) Rapor oluştur
        report = svc.networks().reports().create(
            parent=network_name, body=report_body
        ).execute()
        report_name = report["name"]
        logger.info("Ad Manager rapor oluşturuldu: %s", report_name)

        # 2) Raporu çalıştır
        op = svc.networks().reports().run(name=report_name, body={}).execute()
        op_name = op.get("name", "")
        logger.info("Ad Manager rapor çalıştırıldı, operation: %s", op_name)

        # 3) Operation tamamlanana kadar poll et (max 120 saniye)
        _poll_operation(svc, op_name, timeout_s=120)

        # 4) Sonuçları al
        rows = _fetch_all_rows(svc, report_name)
        logger.info("Ad Manager rapor %d satır döndü", len(rows))
        return rows, None

    except Exception as exc:
        msg = str(exc)
        logger.error("Ad Manager policy fetch hatası: %s", msg)
        # API boyut adları yanlışsa anlamlı hata ver
        if "400" in msg or "INVALID_ARGUMENT" in msg:
            return [], f"Ad Manager API rapor parametresi hatası: {msg[:300]}"
        if "403" in msg or "PERMISSION_DENIED" in msg:
            return [], f"Ad Manager API erişim reddedildi. Service account'un network'e API erişimi var mı? Detay: {msg[:200]}"
        if "404" in msg:
            return [], f"Ad Manager API endpoint bulunamadı. Network code doğru mu? ({NETWORK_CODE})"
        return [], f"Ad Manager API hatası: {msg[:300]}"


def _poll_operation(svc, op_name: str, timeout_s: int = 120) -> None:
    """Operation done olana kadar bekle."""
    if not op_name:
        time.sleep(5)
        return
    deadline = time.time() + timeout_s
    interval = 3
    while time.time() < deadline:
        try:
            op = svc.networks().reports().operations().get(name=op_name).execute()
            if op.get("done"):
                if "error" in op:
                    raise RuntimeError(f"Operation hata: {op['error']}")
                return
        except Exception as exc:
            if "operations" in str(exc).lower() or "not found" in str(exc).lower():
                # Operation API desteklenmiyorsa bekleyip devam et
                time.sleep(interval)
                return
            raise
        time.sleep(interval)
        interval = min(interval * 1.5, 15)
    raise TimeoutError(f"Ad Manager raporu {timeout_s}s içinde tamamlanmadı.")


def _fetch_all_rows(svc, report_name: str) -> list[dict]:
    """Tüm rapor satırlarını sayfalayarak çek."""
    results = []
    page_token = None
    while True:
        kwargs: dict[str, Any] = {"name": report_name}
        if page_token:
            kwargs["pageToken"] = page_token
        resp = svc.networks().reports().fetchReportResultRows(**kwargs).execute()
        for row in resp.get("rows", []):
            parsed = _parse_row(row)
            if parsed:
                results.append(parsed)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def _parse_row(row: dict) -> dict | None:
    """API satırını standart dict'e dönüştür."""
    cells = row.get("dimensionValues", [])
    metrics = row.get("metricValues", [])

    def cell(i: int) -> str:
        if i < len(cells):
            v = cells[i]
            return str(v.get("value") or v.get("displayValue") or "").strip()
        return ""

    url = cell(0)
    issue_type = cell(1)
    enforcement = cell(2)

    if not url or not issue_type:
        return None

    ad_requests = 0
    if metrics:
        try:
            ad_requests = int(float(metrics[0].get("value", 0)))
        except (TypeError, ValueError):
            pass

    return {
        "url": url,
        "issue_type": issue_type,
        "category": _categorize(issue_type),
        "ad_requests_7d": ad_requests,
        "enforcement": enforcement,
        "first_reported": None,
        "last_reported": date.today(),
    }


def _categorize(issue_type: str) -> str:
    """İhlal tipine göre kategori ata."""
    t = issue_type.lower()
    if any(k in t for k in ("sexual", "adult", "cinsel", "yetişkin")):
        return "Yetişkinlere özel"
    if any(k in t for k in ("shocking", "şok", "violence", "şiddet")):
        return "Şok edici içerik"
    if any(k in t for k in ("publisher", "yayıncı", "no content")):
        return "Yayıncı içeriği yok"
    if any(k in t for k in ("malware", "phishing", "güvenlik")):
        return "Güvenlik"
    return "Politika sorunu"


# ── Veritabanı işlemleri ──────────────────────────────────────────────────────

def sync_to_db(db, rows: list[dict]) -> int:
    """Çekilen satırları DB'ye upsert et. Güncellenen kayıt sayısını döner."""
    from backend.models import AdPolicyViolation

    if not rows:
        return 0

    now = datetime.utcnow()
    count = 0
    for r in rows:
        existing = (
            db.query(AdPolicyViolation)
            .filter(
                AdPolicyViolation.url == r["url"],
                AdPolicyViolation.issue_type == r["issue_type"],
            )
            .first()
        )
        if existing:
            existing.ad_requests_7d = r["ad_requests_7d"]
            existing.enforcement = r["enforcement"]
            existing.last_reported = r["last_reported"] or date.today()
            existing.fetched_at = now
        else:
            db.add(AdPolicyViolation(
                url=r["url"],
                issue_type=r["issue_type"],
                category=r["category"],
                ad_requests_7d=r["ad_requests_7d"],
                enforcement=r["enforcement"],
                first_reported=r["first_reported"] or date.today(),
                last_reported=r["last_reported"] or date.today(),
                our_status="new",
                our_notes="",
                fetched_at=now,
                updated_at=now,
            ))
            count += 1
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise
    return count


def get_violations(db, *, status: str | None = None, issue_type: str | None = None,
                   order_by: str = "ad_requests") -> list[dict]:
    """DB'den ihlalleri filtreli çek."""
    from backend.models import AdPolicyViolation
    from sqlalchemy import desc

    q = db.query(AdPolicyViolation)
    if status and status != "all":
        q = q.filter(AdPolicyViolation.our_status == status)
    if issue_type and issue_type != "all":
        q = q.filter(AdPolicyViolation.issue_type == issue_type)

    if order_by == "date":
        q = q.order_by(desc(AdPolicyViolation.last_reported), desc(AdPolicyViolation.ad_requests_7d))
    else:
        q = q.order_by(desc(AdPolicyViolation.ad_requests_7d))

    rows = q.limit(1000).all()
    return [_violation_to_dict(r) for r in rows]


def get_stats(db) -> dict:
    """Özet istatistikler."""
    from backend.models import AdPolicyViolation
    from sqlalchemy import func

    total = db.query(func.count(AdPolicyViolation.id)).scalar() or 0
    new_count = db.query(func.count(AdPolicyViolation.id)).filter(
        AdPolicyViolation.our_status == "new"
    ).scalar() or 0
    total_requests = db.query(func.sum(AdPolicyViolation.ad_requests_7d)).scalar() or 0
    last_fetch = db.query(func.max(AdPolicyViolation.fetched_at)).scalar()

    by_category = {}
    for row in db.query(AdPolicyViolation.category, func.count(AdPolicyViolation.id)).group_by(
        AdPolicyViolation.category
    ).all():
        by_category[row[0] or "Diğer"] = row[1]

    by_status = {}
    for row in db.query(AdPolicyViolation.our_status, func.count(AdPolicyViolation.id)).group_by(
        AdPolicyViolation.our_status
    ).all():
        by_status[row[0]] = row[1]

    return {
        "total": total,
        "new": new_count,
        "total_ad_requests_7d": int(total_requests),
        "last_fetch": last_fetch.isoformat() if last_fetch else None,
        "by_category": by_category,
        "by_status": by_status,
    }


def _violation_to_dict(r) -> dict:
    return {
        "id": r.id,
        "url": r.url,
        "issue_type": r.issue_type,
        "category": r.category,
        "ad_requests_7d": r.ad_requests_7d,
        "enforcement": r.enforcement,
        "first_reported": r.first_reported.isoformat() if r.first_reported else None,
        "last_reported": r.last_reported.isoformat() if r.last_reported else None,
        "our_status": r.our_status,
        "our_notes": r.our_notes,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        "admin_link": _admin_link(r.url),
    }
