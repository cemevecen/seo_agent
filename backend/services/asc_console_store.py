"""App Store Connect scrape snapshot — tek paylaşımlı workspace (id=1)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import AscConsoleWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1


def _get_or_create(db: Session) -> AscConsoleWorkspace:
    row = db.get(AscConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        row = AscConsoleWorkspace(id=_WORKSPACE_ID)
        db.add(row)
        db.flush()
    return row


def _pack_metrics_blob(
    metrics: list[dict[str, Any]] | None,
    panels: dict[str, Any] | None,
) -> str:
    return json.dumps(
        {
            "version": 2,
            "items": metrics if isinstance(metrics, list) else [],
            "panels": panels if isinstance(panels, dict) else {},
        },
        ensure_ascii=False,
    )


def _unpack_metrics_blob(raw: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        data = json.loads(raw or "") if raw else []
    except Exception:
        return [], {}
    if isinstance(data, list):
        return data, {}
    if isinstance(data, dict) and int(data.get("version") or 0) >= 2:
        items = data.get("items") if isinstance(data.get("items"), list) else []
        panels = data.get("panels") if isinstance(data.get("panels"), dict) else {}
        return items, panels
    return [], {}


def _normalize_store_reviews(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """App Store public / hafif normalize — DOM junk yok."""
    rows = raw if isinstance(raw, list) else []
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        author = str(r.get("author") or "Anonim").strip()[:80] or "Anonim"
        body = str(r.get("body") or r.get("raw") or "").strip()[:800]
        stars = r.get("stars")
        if not body and not stars:
            continue
        date = str(r.get("date") or "").strip()[:64]
        date_iso = str(r.get("date_iso") or "").strip()
        rid = str(r.get("review_id") or "").strip()
        key = rid.lower() if rid else (author.lower() + "|" + body[:60].lower() + "|" + date[:16])
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(
            {
                "author": author,
                "date": date,
                "date_iso": date_iso,
                "device": str(r.get("device") or "")[:120],
                "body": body or (f"({stars})" if stars else ""),
                "stars": stars,
                "review_id": rid[:80],
                "app_version": str(r.get("app_version") or "")[:40],
                "source": str(r.get("source") or "app_store_public")[:40],
                "locale": str(r.get("locale") or "")[:16],
                "reply": str(r.get("reply") or "")[:800],
            }
        )
    cleaned.sort(key=lambda x: str(x.get("date_iso") or ""), reverse=True)
    return cleaned


def ingest_asc_console_payload(
    db: Session,
    *,
    metrics: list[dict[str, Any]] | None = None,
    panels: dict[str, Any] | None = None,
    reviews: list[dict[str, Any]] | None = None,
    raw_network: list[dict[str, Any]] | None = None,
    source: str = "asc_console_bridge",
    source_url: str | None = None,
    bundle_id: str | None = None,
    app_id: str | None = None,
    sync_ok: bool = True,
    sync_message: str | None = None,
    sync_mode: str = "analytics_scrape",
    merge_reviews: bool = False,
) -> dict[str, Any]:
    row = _get_or_create(db)

    # Yorum-only sync: metrics/panels dokunma
    if merge_reviews or str(sync_mode or "").startswith("reviews"):
        if not isinstance(reviews, list) or not reviews:
            raise ValueError("merge_reviews: reviews listesi gerekli")
        cleaned_reviews = _normalize_store_reviews(reviews)
        now = datetime.utcnow()
        row.reviews_json = json.dumps(cleaned_reviews, ensure_ascii=False)
        row.source = (source or "app_store_public")[:64]
        row.source_url = (source_url or "")[:512]
        if bundle_id:
            row.bundle_id = bundle_id[:128]
        if app_id:
            row.app_id = app_id[:64]
        row.sync_ok = bool(sync_ok)
        row.sync_message = (sync_message or "")[:512]
        row.sync_mode = (sync_mode or "reviews_store")[:32]
        row.updated_at = now
        if sync_ok:
            row.background_synced_at = now
        db.commit()
        db.refresh(row)
        return {
            "ok": True,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            "review_count": len(cleaned_reviews),
            "fact_count": 0,
            "message": row.sync_message,
            "sync_ok": row.sync_ok,
        }

    incoming_panels = panels if isinstance(panels, dict) else {}
    facts = [
        f
        for f in (incoming_panels.get("explorer_facts") or [])
        if isinstance(f, dict) and f.get("metric")
    ]
    # Metrik bazında birleştir — kısmi scrape diğer metrikleri silmesin
    _, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for f in existing_panels.get("explorer_facts") or []:
        if not isinstance(f, dict) or not f.get("metric"):
            continue
        key = (
            str(f.get("metric")),
            str(f.get("date") or "")[:10],
            str(f.get("dim") or "overview"),
        )
        by_key[key] = f
    touched_metrics = {str(f.get("metric")) for f in facts}
    # Aynı scrape turunda gelen metriklerin eski tarihlerini temizle
    if touched_metrics:
        by_key = {
            k: v
            for k, v in by_key.items()
            if k[0] not in touched_metrics
        }
    for f in facts:
        key = (
            str(f.get("metric")),
            str(f.get("date") or "")[:10],
            str(f.get("dim") or "overview"),
        )
        by_key[key] = f
    merged_facts = list(by_key.values())
    merged_panels = dict(existing_panels) if isinstance(existing_panels, dict) else {}
    merged_panels["version"] = 1
    merged_panels["explorer_facts"] = merged_facts[:50000]
    merged_panels["explorer_fact_count"] = len(merged_facts)
    for k in ("pages", "measure_keys", "scrape_meta", "ratings"):
        if incoming_panels.get(k) is not None:
            merged_panels[k] = incoming_panels[k]

    now = datetime.utcnow()
    row.metrics_json = _pack_metrics_blob(metrics or [], merged_panels)
    if reviews is not None:
        row.reviews_json = json.dumps(_normalize_store_reviews(reviews), ensure_ascii=False)
    if raw_network is not None:
        row.raw_network_json = json.dumps(
            (raw_network or [])[:40], ensure_ascii=False
        )
    row.source = (source or "asc_console_bridge")[:64]
    row.source_url = (source_url or "")[:512]
    if bundle_id:
        row.bundle_id = bundle_id[:128]
    if app_id:
        row.app_id = app_id[:64]
    row.sync_ok = bool(sync_ok)
    row.sync_message = (sync_message or "")[:512]
    row.sync_mode = (sync_mode or "analytics_scrape")[:32]
    row.updated_at = now
    if sync_ok:
        row.background_synced_at = now
    db.commit()
    db.refresh(row)
    LOGGER.info(
        "ASC console ingest · facts=%d · ok=%s · %s",
        len(merged_facts),
        sync_ok,
        row.sync_message,
    )
    return {
        "ok": True,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "fact_count": len(merged_facts),
        "review_count": len(json.loads(row.reviews_json or "[]") or []),
        "message": row.sync_message,
        "sync_ok": row.sync_ok,
    }


def asc_console_payload(db: Session) -> dict[str, Any]:
    row = db.get(AscConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        return {
            "ok": True,
            "empty": True,
            "metrics": [],
            "panels": {"version": 1, "explorer_facts": []},
            "reviews": [],
            "review_count": 0,
            "message": "No App Store Connect data yet — run Update page or wait for the next automatic scan.",
        }
    metrics, panels = _unpack_metrics_blob(row.metrics_json or "[]")
    try:
        raw_network = json.loads(row.raw_network_json or "[]")
    except Exception:
        raw_network = []
    try:
        reviews = json.loads(getattr(row, "reviews_json", None) or "[]")
    except Exception:
        reviews = []
    if not isinstance(reviews, list):
        reviews = []
    facts = panels.get("explorer_facts") if isinstance(panels, dict) else []
    return {
        "ok": bool(row.sync_ok),
        "empty": not facts and not reviews,
        "metrics": metrics,
        "panels": panels,
        "reviews": reviews,
        "review_count": len(reviews),
        "raw_network": raw_network if isinstance(raw_network, list) else [],
        "bundle_id": row.bundle_id or "com.nokta.Finans.Takip",
        "app_id": row.app_id or "465599322",
        "source": row.source,
        "source_url": row.source_url,
        "sync_ok": row.sync_ok,
        "sync_message": row.sync_message,
        "sync_mode": row.sync_mode,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "background_synced_at": (
            row.background_synced_at.isoformat() if row.background_synced_at else None
        ),
        "message": row.sync_message or None,
        "explorer_fact_count": len(facts) if isinstance(facts, list) else 0,
    }


def load_asc_scrape_facts() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from backend.database import SessionLocal

    with SessionLocal() as db:
        payload = asc_console_payload(db)
    panels = payload.get("panels") if isinstance(payload, dict) else {}
    if not isinstance(panels, dict):
        panels = {}
    facts = panels.get("explorer_facts") or []
    if not isinstance(facts, list):
        facts = []
    meta = {
        "synced_at": payload.get("updated_at") or payload.get("background_synced_at"),
        "explorer_fact_count": payload.get("explorer_fact_count") or len(facts),
        "message": payload.get("message"),
        "bundle_id": payload.get("bundle_id"),
        "app_id": payload.get("app_id"),
        "sync_ok": payload.get("sync_ok"),
        "ratings": panels.get("ratings") if isinstance(panels.get("ratings"), dict) else None,
        "measure_keys": panels.get("measure_keys") if isinstance(panels.get("measure_keys"), list) else None,
    }
    return [f for f in facts if isinstance(f, dict)], meta
