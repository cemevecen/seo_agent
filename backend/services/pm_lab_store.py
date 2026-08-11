"""Owner PM lab workspace — bölüm bazlı JSON birleştirme."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import OwnerPmLabWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1

SECTION_DEFS: list[dict[str, Any]] = [
    {
        "id": "serp",
        "no": 2,
        "title": "SERP fotoğraf — ilk iki sayfa",
        "hint": "gümüş, gram altın, bitcoin, harem altın, dolar, altın fiyatı",
    },
    {
        "id": "competitors",
        "no": 3,
        "title": "Rakip ana sayfa fiyatları",
        "hint": "bigpara, uzmanpara, tradingview, canlıdöviz, investing",
    },
    {
        "id": "ads_transparency",
        "no": 7,
        "title": "Google Ads Transparency",
        "hint": "doviz.com reklam vitrini",
    },
    {
        "id": "sikayet",
        "no": 9,
        "title": "Şikayetvar / Ekşi",
        "hint": "Döviz marka şikayet ve başlıklar",
    },
    {
        "id": "app_rank",
        "no": 10,
        "title": "Uygulama sırası",
        "hint": "data.ai / Sensor Tower / mağaza detay sırası",
    },
    {
        "id": "firebase_perf",
        "no": 11,
        "title": "Firebase Performance",
        "hint": "Android + iOS Performance (Crashlytics değil)",
    },
    {
        "id": "store_charts",
        "no": 12,
        "title": "Play / App Store kategori listeleri",
        "hint": "FINANCE ücretsiz listeleri ve Döviz konumu",
    },
    {
        "id": "apple_search_ads",
        "no": 14,
        "title": "Apple Search Ads",
        "hint": "kampanya özeti — oturum gerekir",
    },
    {
        "id": "gsc_index",
        "no": 15,
        "title": "GSC tarama + indekslenmeyen",
        "hint": "Crawl stats ve neden indekslenmedi",
    },
    {
        "id": "google_news",
        "no": 17,
        "title": "Google News vitrin",
        "hint": "aynı kelimeler · haber sonuçları",
    },
]


def _get_or_create(db: Session) -> OwnerPmLabWorkspace:
    row = db.get(OwnerPmLabWorkspace, _WORKSPACE_ID)
    if row is None:
        row = OwnerPmLabWorkspace(id=_WORKSPACE_ID, payload_json="{}")
        db.add(row)
        db.flush()
    return row


def _loads(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw or "") if raw else {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def load_payload(db: Session) -> dict[str, Any]:
    row = _get_or_create(db)
    data = _loads(row.payload_json)
    data.setdefault("sections", {})
    if not isinstance(data["sections"], dict):
        data["sections"] = {}
    data["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
    data["sync_ok"] = bool(row.sync_ok)
    data["sync_message"] = row.sync_message or ""
    data["source"] = row.source or ""
    return data


def get_shot_bytes(db: Session, section: str, name: str) -> bytes | None:
    payload = load_payload(db)
    sections = payload.get("sections") or {}
    block = sections.get(section) if isinstance(sections, dict) else None
    if not isinstance(block, dict):
        return None
    shots = block.get("shots") if isinstance(block.get("shots"), dict) else {}
    raw = shots.get(name)
    if not isinstance(raw, str) or not raw.strip():
        return None
    import base64

    try:
        return base64.b64decode(raw)
    except Exception:
        return None


def ingest_pm_lab_payload(db: Session, body: dict[str, Any]) -> dict[str, Any]:
    row = _get_or_create(db)
    existing = _loads(row.payload_json)
    existing.setdefault("sections", {})
    if not isinstance(existing["sections"], dict):
        existing["sections"] = {}

    incoming = body.get("sections") if isinstance(body.get("sections"), dict) else {}
    replace = bool(body.get("replace"))
    if replace:
        existing["sections"] = dict(incoming)
    else:
        for key, val in incoming.items():
            if not isinstance(key, str) or not key:
                continue
            if isinstance(val, dict):
                prev = existing["sections"].get(key)
                if isinstance(prev, dict):
                    merged = dict(prev)
                    merged.update(val)
                    if isinstance(prev.get("shots"), dict) and isinstance(val.get("shots"), dict):
                        shots = dict(prev["shots"])
                        shots.update(val["shots"])
                        merged["shots"] = shots
                    existing["sections"][key] = merged
                else:
                    existing["sections"][key] = val
            else:
                existing["sections"][key] = val

    now = datetime.utcnow()
    existing["scraped_at"] = str(body.get("scraped_at") or now.isoformat())
    row.payload_json = json.dumps(existing, ensure_ascii=False)
    row.source = str(body.get("source") or "pm_lab_scrape")[:64]
    row.sync_ok = bool(body.get("sync_ok", True))
    row.sync_message = str(body.get("sync_message") or "")[:512]
    row.updated_at = now
    db.commit()
    return {
        "ok": True,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "section_count": len(existing.get("sections") or {}),
        "keys": sorted((existing.get("sections") or {}).keys()),
    }


def page_context(db: Session) -> dict[str, Any]:
    payload = load_payload(db)
    raw_sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    cards: list[dict[str, Any]] = []
    for spec in SECTION_DEFS:
        block = raw_sections.get(spec["id"])
        if not isinstance(block, dict):
            block = {}
        shots = block.get("shots") if isinstance(block.get("shots"), dict) else {}
        cards.append(
            {
                **spec,
                "ok": block.get("ok"),
                "message": block.get("message") or "",
                "scraped_at": block.get("scraped_at") or "",
                "summary": block.get("summary") or "",
                "data": {k: v for k, v in block.items() if k != "shots"},
                "shot_names": sorted(str(k) for k in shots.keys()),
            }
        )
    return {
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "source": payload.get("source") or "",
        "cards": cards,
    }
