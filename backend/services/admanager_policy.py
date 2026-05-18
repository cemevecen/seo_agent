"""Google Ad Manager Policy Center servis katmanı.

Kimlik doğrulama: service account JSON → ADMANAGER_SERVICE_ACCOUNT_JSON env var
API: Google Ad Manager SOAP API v202502 — PublisherQueryLanguageService
Network code: settings.admanager_network_code (varsayılan: 21728129623)

REST API v1'de PolicyViolation boyutu mevcut değil; Policy Center verisi
yalnızca SOAP PQL ile çekilebilir.
"""
from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

from backend.config import settings

logger = logging.getLogger(__name__)

ADMANAGER_SCOPES = ["https://www.googleapis.com/auth/admanager"]
NETWORK_CODE = settings.admanager_network_code

_PQL_ENDPOINT = "https://ads.google.com/apis/ads/publisher/v202605/PublisherQueryLanguageService"
_PQL_NS = "https://www.google.com/apis/ads/publisher/v202605"

# Sinemalar.com admin URL şablonu
def _admin_link(url: str) -> str | None:
    import re
    m = re.search(r"/(\d+)(?:/amp)?/?$", url)
    if not m:
        return None
    return f"https://www.sinemalar.com/admin/contents/{m.group(1)}/edit"


def is_configured() -> bool:
    has_oauth = bool(
        (settings.admanager_oauth_refresh_token or "").strip()
        and (settings.admanager_oauth_client_id or "").strip()
        and (settings.admanager_oauth_client_secret or "").strip()
    )
    has_sa = bool((settings.admanager_service_account_json or "").strip())
    return has_oauth or has_sa


def _get_credentials():
    # OAuth2 refresh token öncelikli
    rt = (settings.admanager_oauth_refresh_token or "").strip()
    cid = (settings.admanager_oauth_client_id or "").strip()
    csecret = (settings.admanager_oauth_client_secret or "").strip()
    if rt and cid and csecret:
        from google.oauth2.credentials import Credentials
        return Credentials(
            token=None,
            refresh_token=rt,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cid,
            client_secret=csecret,
            scopes=ADMANAGER_SCOPES,
        )
    # Fallback: service account
    from google.oauth2 import service_account
    raw = (settings.admanager_service_account_json or "").strip()
    if not raw:
        raise ValueError("Ne OAuth2 token ne de ADMANAGER_SERVICE_ACCOUNT_JSON tanımlı.")
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=ADMANAGER_SCOPES)


def _pql_select(query: str, offset: int = 0) -> str:
    """SOAP PQL select isteği XML'i oluştur."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Header>
    <RequestHeader xmlns="{_PQL_NS}">
      <networkCode>{NETWORK_CODE}</networkCode>
      <applicationName>seo_agent</applicationName>
    </RequestHeader>
  </soapenv:Header>
  <soapenv:Body>
    <select xmlns="{_PQL_NS}">
      <selectStatement>
        <query>{query} OFFSET {offset}</query>
      </selectStatement>
    </select>
  </soapenv:Body>
</soapenv:Envelope>"""


def _parse_pql_response(xml_text: str) -> tuple[list[list[str]], list[str], int]:
    """SOAP PQL yanıtını ayrıştır. (rows, columns, totalSize) döner."""
    root = ET.fromstring(xml_text)

    # Namespace'i XML'den otomatik bul
    # Body → selectResponse → rval
    body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        # Fault kontrolü
        fault = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")
        if fault is not None:
            fstr = fault.find("faultstring")
            raise RuntimeError(f"SOAP Fault: {fstr.text if fstr is not None else ET.tostring(fault)}")
        raise RuntimeError(f"SOAP Body bulunamadı: {xml_text[:500]}")

    # Tüm alt elementleri düz gez
    ns_map = {"ns": _PQL_NS}

    total_el = root.find(f".//{{{_PQL_NS}}}totalResultSetSize")
    total = int(total_el.text) if total_el is not None else 0

    # Kolon isimleri
    columns: list[str] = []
    for col in root.findall(f".//{{{_PQL_NS}}}columnTypes"):
        label = col.find(f"{{{_PQL_NS}}}labelName")
        if label is not None:
            columns.append(label.text or "")

    # Satırlar
    rows: list[list[str]] = []
    for row_el in root.findall(f".//{{{_PQL_NS}}}rows"):
        cells: list[str] = []
        for val_el in row_el.findall(f"{{{_PQL_NS}}}values"):
            v = val_el.find(f"{{{_PQL_NS}}}value")
            cells.append((v.text or "").strip() if v is not None else "")
        rows.append(cells)

    return rows, columns, total


def fetch_policy_violations(days: int = 7) -> tuple[list[dict], str | None]:
    """Ad Manager SOAP PQL'den policy ihlallerini çek.

    Döner: (rows, error_message | None)
    """
    if not is_configured():
        return [], "ADMANAGER_SERVICE_ACCOUNT_JSON tanımlı değil."

    try:
        from google.auth.transport.requests import AuthorizedSession
        creds = _get_credentials()
        session = AuthorizedSession(creds)

        # Tanısal sorgular — PolicyViolation erişimini anlamak için
        for test_q in [
            "SELECT Name FROM Publisher_Query_Language_Tables LIMIT 100",
            "SELECT TableName, ColumnName, Selectable, Filterable FROM Publisher_Query_Language_Columns WHERE TableName = 'PolicyViolation' LIMIT 50",
            "SELECT Url, ViolationType FROM PolicyViolation LIMIT 1",
        ]:
            try:
                t_xml = _pql_select(test_q, 0)
                t_resp = session.post(_PQL_ENDPOINT, data=t_xml.encode("utf-8"),
                                      headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""},
                                      timeout=30)
                logger.warning("PQL diag [%s] status=%d body=%s", test_q[:80], t_resp.status_code, t_resp.text[:800])
            except Exception as te:
                logger.warning("PQL diag [%s] exception=%s", test_q[:80], te)

        query = "SELECT Url, ViolationType, EnforcementStatus FROM PolicyViolation LIMIT 500"

        all_rows: list[dict] = []
        offset = 0
        page_size = 500

        while True:
            soap_xml = _pql_select(query.replace("LIMIT 500", f"LIMIT {page_size}"), offset)
            resp = session.post(
                _PQL_ENDPOINT,
                data=soap_xml.encode("utf-8"),
                headers={
                    "Content-Type": "text/xml; charset=utf-8",
                    "SOAPAction": "",
                },
                timeout=60,
            )

            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:400]}")

            rows, columns, total = _parse_pql_response(resp.text)
            logger.info("PQL sayfa offset=%d → %d satır / toplam %d", offset, len(rows), total)

            if not columns:
                # Kolon ismi gelmemişse varsayılan sıra kullan
                columns = ["Url", "ViolationType", "EnforcementStatus", "WeeklyAdRequestCount"]

            for cells in rows:
                parsed = _parse_pql_row(cells, columns)
                if parsed:
                    all_rows.append(parsed)

            offset += len(rows)
            if offset >= total or not rows:
                break

        logger.info("Ad Manager Policy ihlali toplam %d satır", len(all_rows))
        return all_rows, None

    except Exception as exc:
        msg = str(exc)
        logger.error("Ad Manager policy fetch hatası: %s", msg)
        if "403" in msg or "PERMISSION_DENIED" in msg or "Forbidden" in msg:
            return [], f"Ad Manager erişim reddedildi. Service account'un network'te 'Reporter-Service-Account' rolü var mı? Detay: {msg[:300]}"
        if "404" in msg:
            return [], f"Ad Manager SOAP endpoint bulunamadı. Network code doğru mu? ({NETWORK_CODE})"
        if "UNEXECUTABLE" in msg:
            return [], (
                "PolicyViolation PQL tablosu bu ağda çalıştırılamıyor (UNEXECUTABLE). "
                "Bu tablo yalnızca Ad Manager 360 ağlarında ve Policy Center erişimi olan hesaplarda "
                "kullanılabilir. Ad Manager → Raporlama → Policy Center'a erişebiliyorsanız "
                "CSV olarak indirip manuel yükleyebilirsiniz. Detay: " + msg[:300]
            )
        if "PolicyViolation" in msg or "no such table" in msg.lower():
            return [], f"PQL sorgu hatası (tablo/kolon adı yanlış olabilir). Detay: {msg[:400]}"
        return [], f"Ad Manager API hatası: {msg[:400]}"


def _parse_pql_row(cells: list[str], columns: list[str]) -> dict | None:
    col_map = {c.lower(): i for i, c in enumerate(columns)}

    def get(name: str) -> str:
        i = col_map.get(name.lower())
        return cells[i].strip() if i is not None and i < len(cells) else ""

    url = get("Url") or get("url")
    issue_type = get("ViolationType") or get("violationtype")

    if not url or not issue_type:
        return None

    ad_requests = 0
    raw_req = get("WeeklyAdRequestCount") or get("weeklyadrequestcount")
    try:
        ad_requests = int(float(raw_req))
    except (TypeError, ValueError):
        pass

    enforcement = get("EnforcementStatus") or get("enforcementstatus")

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

    return [_violation_to_dict(r) for r in q.limit(1000).all()]


def get_stats(db) -> dict:
    from backend.models import AdPolicyViolation
    from sqlalchemy import func

    total = db.query(func.count(AdPolicyViolation.id)).scalar() or 0
    new_count = db.query(func.count(AdPolicyViolation.id)).filter(
        AdPolicyViolation.our_status == "new"
    ).scalar() or 0
    total_requests = db.query(func.sum(AdPolicyViolation.ad_requests_7d)).scalar() or 0
    last_fetch = db.query(func.max(AdPolicyViolation.fetched_at)).scalar()

    by_category: dict[str, int] = {}
    for row in db.query(AdPolicyViolation.category, func.count(AdPolicyViolation.id)).group_by(
        AdPolicyViolation.category
    ).all():
        by_category[row[0] or "Diğer"] = row[1]

    by_status: dict[str, int] = {}
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
