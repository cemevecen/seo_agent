"""GA4 (Google Analytics Data API) credential yardımcıları."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import SiteCredential
from backend.services.crypto import decrypt_text, encrypt_text

GA4_CREDENTIAL_TYPE = "ga4"

GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
GA4_DEFAULT_PROFILES = ("web", "mweb", "android", "ios")


def _load_service_account_payload() -> dict | None:
    raw_json = (settings.ga4_service_account_json or "").strip()
    raw_file = (settings.ga4_service_account_file or "").strip()

    if raw_json:
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError:
            # JSON değilse dosya yolu gibi davran
            raw_file = raw_json

    if raw_file:
        path = Path(raw_file).expanduser()
        if path.exists():
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                return None
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return None
            return payload

    return None


def ga4_is_configured() -> bool:
    return _load_service_account_payload() is not None


def get_ga4_credentials_record(db: Session, site_id: int) -> SiteCredential | None:
    return (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == GA4_CREDENTIAL_TYPE)
        .order_by(SiteCredential.id.desc())
        .first()
    )


def load_ga4_properties(record: SiteCredential | None) -> dict[str, str]:
    if record is None:
        return {}
    payload = json.loads(decrypt_text(record.encrypted_data))
    if isinstance(payload, dict) and isinstance(payload.get("properties"), dict):
        result: dict[str, str] = {}
        for key, value in (payload.get("properties") or {}).items():
            k = str(key or "").strip().lower()
            v = str(value or "").strip()
            if k and v:
                result[k] = v
        return result
    # Backward-compat: eski tek property kaydı
    single = str((payload or {}).get("property_id") or "").strip() if isinstance(payload, dict) else ""
    return {"web": single} if single else {}


def upsert_ga4_properties(db: Session, site_id: int, properties: dict[str, str]) -> SiteCredential:
    cleaned: dict[str, str] = {}
    for key, value in (properties or {}).items():
        k = str(key or "").strip().lower()
        v = str(value or "").strip()
        if not k:
            continue
        if v:
            cleaned[k] = v
        else:
            # boş geldiyse silmek için yok say
            continue

    encrypted = encrypt_text(json.dumps({"properties": cleaned}, ensure_ascii=False))
    record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == GA4_CREDENTIAL_TYPE)
        .first()
    )
    if record is None:
        record = SiteCredential(site_id=site_id, credential_type=GA4_CREDENTIAL_TYPE, encrypted_data=encrypted)
        db.add(record)
    else:
        record.encrypted_data = encrypted
    db.commit()
    db.refresh(record)
    return record


def get_ga4_connection_status(db: Session, site_id: int) -> dict[str, str | bool]:
    record = get_ga4_credentials_record(db, site_id)
    properties = load_ga4_properties(record)
    if not properties:
        return {"connected": False, "method": "none", "label": "Property yok", "properties": {}}
    if not ga4_is_configured():
        return {
            "connected": False,
            "method": "service_account",
            "label": "Service account dosyası yok/okunamadı",
            "properties": properties,
        }
    return {"connected": True, "method": "service_account", "label": "GA4 bağlı", "properties": properties}


def load_ga4_service_account_info() -> dict:
    payload = _load_service_account_payload()
    if payload is None:
        raise ValueError("GA4 service account tanımı bulunamadı. .env için GA4_SERVICE_ACCOUNT_JSON veya GA4_SERVICE_ACCOUNT_FILE ekleyin.")
    return payload

