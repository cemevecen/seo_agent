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


def _vitals_has_usable_data(vitals: Any) -> bool:
    """Overview satırı veya en az bir sorun satırı / pozitif issue_count var mı?"""
    if not isinstance(vitals, dict) or vitals.get("error"):
        return False
    ov = vitals.get("metrics_overview") if isinstance(vitals.get("metrics_overview"), dict) else {}
    if len(ov.get("rows") or []) > 0:
        return True
    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    for et in ("CRASH", "ANR"):
        block = crashes.get(et) if isinstance(crashes.get(et), dict) else {}
        for cat in block.get("categories") or []:
            if not isinstance(cat, dict):
                continue
            if cat.get("issues"):
                return True
            raw_n = cat.get("issue_count") or cat.get("issue_row_count")
            try:
                if int(str(raw_n).replace(".", "").replace(",", "").split()[0]) > 0:
                    return True
            except (TypeError, ValueError, IndexError):
                pass
    byv = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    for payload in byv.values():
        if not isinstance(payload, dict):
            continue
        nested = {"crashes": payload.get("crashes") or payload, "metrics_overview": {}}
        # Avoid infinite recursion via by_version: only inspect crashes map
        cr = nested.get("crashes") if isinstance(nested.get("crashes"), dict) else {}
        for et in ("CRASH", "ANR"):
            block = cr.get(et) if isinstance(cr.get(et), dict) else {}
            for cat in block.get("categories") or []:
                if not isinstance(cat, dict):
                    continue
                if cat.get("issues"):
                    return True
                raw_n = cat.get("issue_count") or cat.get("issue_row_count")
                try:
                    if int(str(raw_n).replace(".", "").replace(",", "").split()[0]) > 0:
                        return True
                except (TypeError, ValueError, IndexError):
                    pass
            if block.get("summary_rate"):
                return True
    return False


def _preserve_existing_vitals_if_incoming_empty(
    panels: dict[str, Any] | None,
    existing_panels: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(panels, dict):
        return panels
    incoming = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else None
    existing = (
        existing_panels.get("vitals")
        if isinstance(existing_panels, dict) and isinstance(existing_panels.get("vitals"), dict)
        else None
    )
    if incoming is None:
        return panels
    if _vitals_has_usable_data(incoming):
        return panels
    if not _vitals_has_usable_data(existing):
        return panels
    LOGGER.warning(
        "Boş/hatalı vitals ingest engellendi — mevcut Android Vitals korundu "
        "(incoming error=%s)",
        str((incoming or {}).get("error") or "")[:120],
    )
    out = dict(panels)
    out["vitals"] = existing
    if isinstance(existing_panels, dict):
        if existing_panels.get("vitals_category_count") is not None:
            out["vitals_category_count"] = existing_panels.get("vitals_category_count")
        if existing_panels.get("vitals_overview_row_count") is not None:
            out["vitals_overview_row_count"] = existing_panels.get(
                "vitals_overview_row_count"
            )
    return out


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
    merge_ratings_counts: bool = False,
) -> dict[str, Any]:
    row = _get_or_create(db)

    # ratings_counts-only: explorer_facts içine ratings_count satırlarını birleştir
    if merge_ratings_counts or str(sync_mode or "").startswith("ratings_count"):
        existing_metrics, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
        incoming = panels if isinstance(panels, dict) else {}
        new_facts = [
            f
            for f in (incoming.get("explorer_facts") or [])
            if isinstance(f, dict) and str(f.get("metric")) == "ratings_count"
        ]
        if not new_facts:
            raise ValueError("merge_ratings_counts: ratings_count fact gerekli")
        merged_panels = dict(existing_panels) if isinstance(existing_panels, dict) else {}
        # Tarih bazında birleştir — kısa CSV eski uzun geçmişi silmesin
        by_date: dict[str, dict[str, Any]] = {}
        other_facts: list[dict[str, Any]] = []
        for f in merged_panels.get("explorer_facts") or []:
            if not isinstance(f, dict):
                continue
            if str(f.get("metric")) != "ratings_count":
                other_facts.append(f)
                continue
            ds = str(f.get("date") or "")[:10]
            if ds:
                by_date[ds] = f
        for f in new_facts:
            ds = str(f.get("date") or "")[:10]
            if not ds:
                continue
            prev = by_date.get(ds)
            new_stars = f.get("stars") if isinstance(f.get("stars"), dict) else {}
            prev_stars = (
                prev.get("stars") if isinstance(prev, dict) and isinstance(prev.get("stars"), dict) else {}
            )
            # Yıldızlı CSV’yi yıldızsız kısa satırla ezme
            if prev and prev_stars and any(prev_stars.values()) and not any(
                (new_stars or {}).values()
            ):
                continue
            by_date[ds] = f
        merged_panels["explorer_facts"] = other_facts + list(by_date.values())
        merged_panels["explorer_fact_count"] = len(merged_panels["explorer_facts"])
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

    # vitals-only: sunucu tarafında birleştir — mevcut explorer/kartları silme
    elif merge_vitals or str(sync_mode or "").startswith("vitals"):
        existing_metrics, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
        incoming = panels if isinstance(panels, dict) else {}
        vitals = incoming.get("vitals") if isinstance(incoming.get("vitals"), dict) else None
        if not vitals:
            raise ValueError("merge_vitals: panels.vitals gerekli")
        if not _vitals_has_usable_data(vitals):
            existing_v = (
                existing_panels.get("vitals")
                if isinstance(existing_panels.get("vitals"), dict)
                else None
            )
            if _vitals_has_usable_data(existing_v):
                raise ValueError(
                    "merge_vitals: boş vitals — mevcut Android Vitals korundu; yeniden tarama gerekir"
                )
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

    # Tam dashboard / DOM scrape: kısa yorum listesi uzun Play Store sync'i ezmesin
    elif reviews is not None:
        try:
            existing_reviews = json.loads(row.reviews_json or "[]")
        except Exception:
            existing_reviews = []
        if isinstance(existing_reviews, list) and isinstance(reviews, list):
            existing_n = len(existing_reviews)
            incoming_n = len(reviews)
            existing_public = sum(
                1
                for x in existing_reviews
                if isinstance(x, dict) and "play_store" in str(x.get("source") or "")
            )
            # DOM viewport (~6–20) veya boş liste, dolu public sync'in üstüne yazmasın
            if existing_n >= 40 and incoming_n < max(25, existing_n // 3) and (
                existing_public >= max(10, existing_n // 4) or existing_n >= 80
            ):
                reviews = existing_reviews

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

    # Boş/başarısız scrape explorer_facts'i silmesin / boş snapshot yazmasın
    if not (
        merge_vitals
        or merge_reviews
        or merge_ratings_counts
        or str(sync_mode or "").startswith(("vitals", "reviews", "ratings_count"))
    ):
        incoming_facts = (panels or {}).get("explorer_facts") if isinstance(panels, dict) else None
        incoming_n = len(incoming_facts) if isinstance(incoming_facts, list) else 0
        try:
            _em, existing_panels = _unpack_metrics_blob(row.metrics_json or "[]")
        except Exception:
            existing_panels = {}
        existing_facts = (
            (existing_panels or {}).get("explorer_facts")
            if isinstance(existing_panels, dict)
            else None
        )
        existing_n = len(existing_facts) if isinstance(existing_facts, list) else 0
        if incoming_n == 0:
            raise ValueError(
                "Boş explorer_facts ingest reddedildi"
                + (f" (mevcut {existing_n} fact korunuyor)" if existing_n else "")
                + ". Play Console stats views başarısız — yeniden sync gerekir."
            )
        # Boş vitals full sync ile dolu vitals'i ezmesin
        panels = _preserve_existing_vitals_if_incoming_empty(panels, existing_panels)

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
        "store_listings_count": len((panels or {}).get("store_listings") or []),
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
            "message": "No Play Console data yet — run Update page or wait for the next automatic scan.",
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
        "store_listings_count": len(panels.get("store_listings") or []),
        "monitor_count": len(panels.get("monitor") or []),
        "release_count": len(panels.get("release") or []),
        "statistics_count": len(panels.get("statistics") or []),
        "review_count": len(reviews),
    }
