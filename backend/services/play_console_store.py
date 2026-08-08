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


def _pack_metrics_blob(
    metrics: list[dict[str, Any]] | None,
    panels: dict[str, Any] | None,
) -> str:
    """metrics_json: v2 envelope {version, items, panels} — migration yok."""
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
    if isinstance(data, dict) and isinstance(data.get("metrics"), list):
        return data["metrics"], data.get("panels") if isinstance(data.get("panels"), dict) else {}
    return [], {}


def ingest_play_console_payload(
    db: Session,
    *,
    metrics: list[dict[str, Any]] | None = None,
    panels: dict[str, Any] | None = None,
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
    merge_vitals: bool = False,
    merge_reviews: bool = False,
) -> dict[str, Any]:
    row = _get_or_create(db)

    # vitals-only: sunucu tarafında birleştir — mevcut explorer/kartları silme
    if merge_vitals or str(sync_mode or "").startswith("vitals"):
        existing_metrics, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
        incoming = panels if isinstance(panels, dict) else {}
        vitals = incoming.get("vitals") if isinstance(incoming.get("vitals"), dict) else None
        if not vitals:
            raise ValueError("merge_vitals: panels.vitals gerekli")
        merged_panels = dict(existing_panels) if isinstance(existing_panels, dict) else {}
        merged_panels["vitals"] = vitals
        pages = (
            dict(merged_panels.get("pages") or {})
            if isinstance(merged_panels.get("pages"), dict)
            else {}
        )
        for pk, pv in (incoming.get("pages") or {}).items():
            if isinstance(pv, dict):
                pages[str(pk)] = pv
        merged_panels["pages"] = pages
        if incoming.get("vitals_category_count") is not None:
            merged_panels["vitals_category_count"] = incoming.get("vitals_category_count")
        if incoming.get("vitals_overview_row_count") is not None:
            merged_panels["vitals_overview_row_count"] = incoming.get(
                "vitals_overview_row_count"
            )
        if isinstance(incoming.get("version_name_map"), dict):
            base_map = (
                merged_panels.get("version_name_map")
                if isinstance(merged_panels.get("version_name_map"), dict)
                else {}
            )
            merged_panels["version_name_map"] = {
                **{str(k): str(v) for k, v in base_map.items() if str(k) and str(v)},
                **{
                    str(k): str(v)
                    for k, v in incoming["version_name_map"].items()
                    if str(k).strip() and str(v).strip()
                },
            }
        # vitals bundle içindeki map de birleştir
        vitals_map = (
            vitals.get("version_name_map")
            if isinstance(vitals.get("version_name_map"), dict)
            else {}
        )
        if vitals_map:
            base_map = (
                merged_panels.get("version_name_map")
                if isinstance(merged_panels.get("version_name_map"), dict)
                else {}
            )
            merged_panels["version_name_map"] = {
                **{str(k): str(v) for k, v in base_map.items() if str(k) and str(v)},
                **{
                    str(k): str(v)
                    for k, v in vitals_map.items()
                    if str(k).strip() and str(v).strip()
                },
            }
        # Mevcut metrics/reviews/rating korunur (gönderilse bile boş listeyle ezilmez)
        try:
            existing_reviews = json.loads(row.reviews_json or "[]")
        except Exception:
            existing_reviews = []
        try:
            existing_rating = json.loads(row.rating_summary_json or "{}")
        except Exception:
            existing_rating = {}
        metrics = existing_metrics
        panels = merged_panels
        reviews = existing_reviews if isinstance(existing_reviews, list) else []
        rating_summary = existing_rating if isinstance(existing_rating, dict) else {}

    # reviews-only: mevcut panels/metrics korunur, sadece yorum (+opsiyonel rating) güncellenir
    elif merge_reviews or str(sync_mode or "").startswith("reviews"):
        existing_metrics, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
        metrics = existing_metrics
        panels = existing_panels if isinstance(existing_panels, dict) else {}
        if not isinstance(reviews, list) or not reviews:
            raise ValueError("merge_reviews: reviews listesi gerekli")
        if not isinstance(rating_summary, dict) or not rating_summary:
            try:
                rating_summary = json.loads(row.rating_summary_json or "{}")
            except Exception:
                rating_summary = {}

    cleaned = normalize_play_snapshot(
        metrics=metrics,
        panels=panels,
        reviews=reviews,
        rating_summary=rating_summary,
    )
    metrics = cleaned["metrics"]
    panels = cleaned["panels"]
    reviews = cleaned["reviews"]
    rating_summary = cleaned["rating_summary"]
    if metrics is not None or panels is not None:
        row.metrics_json = _pack_metrics_blob(metrics, panels)
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
        "tpg_count": len((panels or {}).get("tpg") or []),
        "breakdown_count": len((panels or {}).get("breakdowns") or []),
        "monetize_count": len((panels or {}).get("monetize") or []),
        "grow_count": len((panels or {}).get("grow") or []),
        "monitor_count": len((panels or {}).get("monitor") or []),
        "release_count": len((panels or {}).get("release") or []),
        "statistics_count": len((panels or {}).get("statistics") or []),
        "review_count": len(reviews or []),
        "explorer_fact_count": len((panels or {}).get("explorer_facts") or []),
        "stats_view_count": len((panels or {}).get("stats_views") or []),
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
            "panels": {"version": 2, "tpg": [], "breakdowns": [], "series": []},
            "reviews": [],
            "rating_summary": {},
            "message": "Henüz Play Console scrape yok — Mac bridge login + sync gerekli.",
        }

    def _loads(raw: str, default: Any) -> Any:
        try:
            return json.loads(raw or "") if raw else default
        except Exception:
            return default

    metrics, panels = _unpack_metrics_blob(row.metrics_json or "[]")
    reviews = _loads(row.reviews_json, [])
    rating_summary = _loads(row.rating_summary_json, {})
    cleaned = normalize_play_snapshot(
        metrics=metrics if isinstance(metrics, list) else [],
        panels=panels if isinstance(panels, dict) else {},
        reviews=reviews if isinstance(reviews, list) else [],
        rating_summary=rating_summary if isinstance(rating_summary, dict) else {},
    )
    metrics = cleaned["metrics"]
    panels = cleaned["panels"]
    reviews = cleaned["reviews"]
    rating_summary = cleaned["rating_summary"]
    return {
        "ok": bool(row.sync_ok),
        "empty": not metrics and not reviews,
        "metrics": metrics,
        "panels": panels,
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
        "tpg_count": len(panels.get("tpg") or []),
        "breakdown_count": len(panels.get("breakdowns") or []),
        "monetize_count": len(panels.get("monetize") or []),
        "grow_count": len(panels.get("grow") or []),
        "monitor_count": len(panels.get("monitor") or []),
        "release_count": len(panels.get("release") or []),
        "statistics_count": len(panels.get("statistics") or []),
        "review_count": len(reviews),
    }
