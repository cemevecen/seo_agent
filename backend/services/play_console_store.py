"""Play Console scrape snapshot — tek paylaşımlı workspace (id=1)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import PlayConsoleWorkspace
from backend.services.play_console_normalize import normalize_play_snapshot

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1


def _get_or_create(db: Session) -> PlayConsoleWorkspace:
    row = db.get(PlayConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        row = PlayConsoleWorkspace(id=_WORKSPACE_ID)
        db.add(row)
        db.flush()
    return row


def ingest_play_console_payload(
    db: Session,
    *,
    metrics: list[dict[str, Any]] | None = None,
    reviews: list[dict[str, Any]] | None = None,
    rating_summary: dict[str, Any] | None = None,
    raw_network: list[dict[str, Any]] | None = None,
    source: str = "play_console_bridge",
    source_url: str | None = None,
    package_name: str | None = None,
    app_id: str | None = None,
    sync_ok: bool = True,
    sync_message: str | None = None,
    sync_mode: str = "dashboard_reviews",
) -> dict[str, Any]:
    cleaned = normalize_play_snapshot(
        metrics=metrics,
        reviews=reviews,
        rating_summary=rating_summary,
    )
    metrics = cleaned["metrics"]
    reviews = cleaned["reviews"]
    rating_summary = cleaned["rating_summary"]
    row = _get_or_create(db)
    if metrics is not None:
        row.metrics_json = json.dumps(metrics, ensure_ascii=False)
    if reviews is not None:
        row.reviews_json = json.dumps(reviews, ensure_ascii=False)
    if rating_summary is not None:
        row.rating_summary_json = json.dumps(rating_summary, ensure_ascii=False)
    if raw_network is not None:
        # Ağ yakalaması büyük olabilir — son 40 yanıtla sınırla
        trimmed = raw_network[-40:] if len(raw_network) > 40 else raw_network
        row.raw_network_json = json.dumps(trimmed, ensure_ascii=False)
    if source:
        row.source = str(source)[:64]
    if source_url is not None:
        row.source_url = str(source_url)[:512]
    if package_name:
        row.package_name = str(package_name)[:128]
    if app_id:
        row.app_id = str(app_id)[:64]
    row.sync_ok = bool(sync_ok)
    row.sync_message = str(sync_message or "")[:512]
    row.sync_mode = str(sync_mode or "")[:32]
    now = datetime.utcnow()
    row.updated_at = now
    row.background_synced_at = now
    db.commit()
    db.refresh(row)
    return {
        "ok": True,
        "synced": True,
        "metric_count": len(metrics or []),
        "review_count": len(reviews or []),
        "updated_at": row.updated_at.isoformat() + "Z",
        "package_name": row.package_name,
    }


def play_console_payload(db: Session) -> dict[str, Any]:
    row = db.get(PlayConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        return {
            "ok": True,
            "empty": True,
            "metrics": [],
            "reviews": [],
            "rating_summary": {},
            "message": "Henüz Play Console scrape yok — Mac bridge login + sync gerekli.",
        }

    def _loads(raw: str, default: Any) -> Any:
        try:
            return json.loads(raw or "") if raw else default
        except Exception:
            return default

    metrics = _loads(row.metrics_json, [])
    reviews = _loads(row.reviews_json, [])
    rating_summary = _loads(row.rating_summary_json, {})
    cleaned = normalize_play_snapshot(
        metrics=metrics if isinstance(metrics, list) else [],
        reviews=reviews if isinstance(reviews, list) else [],
        rating_summary=rating_summary if isinstance(rating_summary, dict) else {},
    )
    metrics = cleaned["metrics"]
    reviews = cleaned["reviews"]
    rating_summary = cleaned["rating_summary"]
    return {
        "ok": bool(row.sync_ok),
        "empty": not metrics and not reviews,
        "metrics": metrics,
        "reviews": reviews,
        "rating_summary": rating_summary,
        "package_name": row.package_name or "com.Doviz",
        "app_id": row.app_id or "",
        "source": row.source or "",
        "source_url": row.source_url or "",
        "sync_ok": bool(row.sync_ok),
        "sync_message": row.sync_message or "",
        "sync_mode": row.sync_mode or "",
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
        "background_synced_at": (
            row.background_synced_at.isoformat() + "Z" if row.background_synced_at else None
        ),
        "metric_count": len(metrics),
        "review_count": len(reviews),
    }
