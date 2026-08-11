"""Owner PM lab workspace — bölüm bazlı JSON + SERP/News geçmişi."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from backend.models import OwnerPmLabWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1
_RUNS_KEEP = 48
_INTERVAL_HOURS = 3

SECTION_DEFS: list[dict[str, Any]] = [
    {
        "id": "serp",
        "no": 2,
        "title": "SERP — ilk 4 sayfa",
        "hint": "sekme sekme kelimeler · sıra / domain / meta · iniş-çıkış",
    },
    {
        "id": "competitors",
        "no": 3,
        "title": "Rakip ana sayfa fiyatları",
        "hint": "satır: varlık · sütun: site",
    },
    {
        "id": "sikayet",
        "no": 9,
        "title": "Şikayetvar / Ekşi",
        "hint": "x.com · doviz.com · sinemalar.com · son 10",
    },
    {
        "id": "store_charts",
        "no": 12,
        "title": "Play / App Store kategori listeleri",
        "hint": "Finans ücretsiz · ilk 200 · bilinen isimler",
    },
    {
        "id": "google_news",
        "no": 17,
        "title": "Google News vitrin",
        "hint": "ilk 25 · kaynak metrikleri · 3 saatte bir",
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


def _strip_shots(block: Any) -> Any:
    if not isinstance(block, dict):
        return block
    out = {k: v for k, v in block.items() if k != "shots"}
    return out


def _kw_rank_index(keywords: list[Any]) -> dict[str, dict[str, int]]:
    index: dict[str, dict[str, int]] = {}
    for kw in keywords:
        if not isinstance(kw, dict):
            continue
        name = str(kw.get("keyword") or "").strip()
        if not name:
            continue
        ranks: dict[str, int] = {}
        rows = kw.get("rows") if isinstance(kw.get("rows"), list) else []
        if not rows:
            for page in kw.get("pages") or []:
                if not isinstance(page, dict):
                    continue
                for row in page.get("organic") or []:
                    if not isinstance(row, dict):
                        continue
                    host = str(row.get("domain") or "").strip().lower()
                    try:
                        rank = int(row.get("rank") or 0)
                    except (TypeError, ValueError):
                        rank = 0
                    if host and rank:
                        ranks[host] = rank
        for row in rows:
            if not isinstance(row, dict):
                continue
            host = str(row.get("domain") or "").strip().lower()
            try:
                rank = int(row.get("rank") or 0)
            except (TypeError, ValueError):
                rank = 0
            if host and rank:
                ranks[host] = rank
        index[name] = ranks
    return index


def _enrich_serp(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    out = _strip_shots(incoming)
    prev_idx = _kw_rank_index(prev.get("keywords") or [])
    keywords = out.get("keywords") if isinstance(out.get("keywords"), list) else []
    for kw in keywords:
        if not isinstance(kw, dict):
            continue
        name = str(kw.get("keyword") or "")
        old_map = prev_idx.get(name) or {}
        rows = kw.get("rows") if isinstance(kw.get("rows"), list) else []
        current: set[str] = set()
        entered: list[dict[str, Any]] = []
        climbed: list[dict[str, Any]] = []
        fell: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            host = str(row.get("domain") or "").strip().lower()
            current.add(host)
            try:
                rank = int(row.get("rank") or 0)
            except (TypeError, ValueError):
                rank = 0
            old = old_map.get(host)
            if old is None:
                row["delta"] = "new"
                row["delta_n"] = None
                row["prev_rank"] = None
                entered.append({"domain": host, "rank": rank, "title": row.get("title") or ""})
            else:
                row["prev_rank"] = old
                row["delta_n"] = old - rank
                if rank < old:
                    row["delta"] = "up"
                    climbed.append({"domain": host, "rank": rank, "prev_rank": old, "delta_n": old - rank})
                elif rank > old:
                    row["delta"] = "down"
                    fell.append({"domain": host, "rank": rank, "prev_rank": old, "delta_n": old - rank})
                else:
                    row["delta"] = "same"
        dropped = []
        for host, old_rank in old_map.items():
            if host not in current:
                dropped.append({"domain": host, "prev_rank": old_rank})
        kw["entered"] = entered
        kw["dropped"] = dropped
        kw["climbed"] = climbed
        kw["fell"] = fell
        kw["moves"] = {
            "entered": len(entered),
            "dropped": len(dropped),
            "up": len(climbed),
            "down": len(fell),
        }

    runs = list(prev.get("runs") or []) if isinstance(prev.get("runs"), list) else []
    snap = {
        "at": out.get("scraped_at") or datetime.utcnow().isoformat(),
        "ok": bool(out.get("ok")),
        "summary": out.get("summary") or "",
        "moves": {
            "entered": sum(int((kw.get("moves") or {}).get("entered") or 0) for kw in keywords if isinstance(kw, dict)),
            "dropped": sum(int((kw.get("moves") or {}).get("dropped") or 0) for kw in keywords if isinstance(kw, dict)),
            "up": sum(int((kw.get("moves") or {}).get("up") or 0) for kw in keywords if isinstance(kw, dict)),
            "down": sum(int((kw.get("moves") or {}).get("down") or 0) for kw in keywords if isinstance(kw, dict)),
        },
        "keyword_count": len(keywords),
        "row_count": sum(len(kw.get("rows") or []) for kw in keywords if isinstance(kw, dict)),
    }
    runs.append(snap)
    out["runs"] = runs[-_RUNS_KEEP:]
    return out


def _enrich_news(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    out = _strip_shots(incoming)
    keywords = out.get("keywords") if isinstance(out.get("keywords"), list) else []
    source_counts: dict[str, int] = {}
    total = 0
    for kw in keywords:
        if not isinstance(kw, dict):
            continue
        for art in kw.get("articles") or []:
            if not isinstance(art, dict):
                continue
            src = str(art.get("source") or "—").strip() or "—"
            source_counts[src] = source_counts.get(src, 0) + 1
            total += 1
    ranked = sorted(source_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    out["source_counts"] = [{"source": k, "count": v} for k, v in ranked]
    out["article_total"] = total

    prev_runs = list(prev.get("runs") or []) if isinstance(prev.get("runs"), list) else []
    hist_totals: dict[str, list[int]] = {}
    for run in prev_runs:
        if not isinstance(run, dict):
            continue
        for row in run.get("source_counts") or []:
            if not isinstance(row, dict):
                continue
            hist_totals.setdefault(str(row.get("source") or "—"), []).append(int(row.get("count") or 0))
    averages = []
    for src, _cnt in ranked:
        series = hist_totals.get(src) or []
        series.append(source_counts.get(src, 0))
        avg = round(sum(series) / len(series), 2) if series else 0
        averages.append({"source": src, "count": source_counts.get(src, 0), "avg": avg, "runs": len(series)})
    out["source_averages"] = averages

    prev_runs.append(
        {
            "at": out.get("scraped_at") or datetime.utcnow().isoformat(),
            "ok": bool(out.get("ok")),
            "article_total": total,
            "source_counts": out["source_counts"][:20],
        }
    )
    out["runs"] = prev_runs[-_RUNS_KEEP:]
    return out


def _enrich_generic(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    out = _strip_shots(incoming)
    runs = list(prev.get("runs") or []) if isinstance(prev.get("runs"), list) else []
    runs.append(
        {
            "at": out.get("scraped_at") or datetime.utcnow().isoformat(),
            "ok": bool(out.get("ok")),
            "summary": out.get("summary") or "",
        }
    )
    out["runs"] = runs[-_RUNS_KEEP:]
    if isinstance(prev.get("matrix"), list) and not out.get("prev_matrix"):
        out["prev_matrix"] = prev.get("matrix")
    return out


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
    del db, section, name
    return None


def ingest_pm_lab_payload(db: Session, body: dict[str, Any]) -> dict[str, Any]:
    row = _get_or_create(db)
    existing = _loads(row.payload_json)
    existing.setdefault("sections", {})
    if not isinstance(existing["sections"], dict):
        existing["sections"] = {}

    incoming = body.get("sections") if isinstance(body.get("sections"), dict) else {}
    replace = bool(body.get("replace"))
    keep_ids = {spec["id"] for spec in SECTION_DEFS}
    if replace:
        existing["sections"] = {}

    for key, val in incoming.items():
        if not isinstance(key, str) or key not in keep_ids:
            continue
        prev = existing["sections"].get(key)
        if not isinstance(prev, dict):
            prev = {}
        if not isinstance(val, dict):
            existing["sections"][key] = val
            continue
        if key == "serp":
            existing["sections"][key] = _enrich_serp(prev, val)
        elif key == "google_news":
            existing["sections"][key] = _enrich_news(prev, val)
        else:
            existing["sections"][key] = _enrich_generic(prev, val)

    for dead in list(existing["sections"].keys()):
        if dead not in keep_ids:
            existing["sections"].pop(dead, None)

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


def _next_run_iso(scraped_at: str | None) -> str:
    raw = (scraped_at or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    nxt = dt + timedelta(hours=_INTERVAL_HOURS)
    return nxt.isoformat()


def page_context(db: Session) -> dict[str, Any]:
    payload = load_payload(db)
    raw_sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    cards: list[dict[str, Any]] = []
    boot_sections: dict[str, Any] = {}
    for spec in SECTION_DEFS:
        block = raw_sections.get(spec["id"])
        if not isinstance(block, dict):
            block = {}
        data = _strip_shots(block)
        boot_sections[spec["id"]] = data
        cards.append(
            {
                **spec,
                "ok": data.get("ok"),
                "message": data.get("message") or "",
                "scraped_at": data.get("scraped_at") or "",
                "summary": data.get("summary") or "",
                "data": data,
                "shot_names": [],
            }
        )
    boot = {
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "next_at": _next_run_iso(str(payload.get("scraped_at") or "")),
        "interval_hours": _INTERVAL_HOURS,
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "sections": boot_sections,
        "defs": SECTION_DEFS,
    }
    return {
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "source": payload.get("source") or "",
        "cards": cards,
        "boot_json": json.dumps(boot, ensure_ascii=False),
    }
