"""Owner PM lab workspace — bölüm bazlı JSON + SERP/News geçmişi."""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from backend.models import OwnerPmLabWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1
_RUNS_KEEP = 48
_INTERVAL_HOURS = 3
COMPETITORS_INTERVAL_MIN = 10

SECTION_DEFS: list[dict[str, Any]] = [
    {
        "id": "serp",
        "no": 2,
        "title": "SERP — ilk 4 sayfa",
        "hint": "",
    },
    {
        "id": "competitors",
        "no": 3,
        "title": "Rakip kur fiyatları karşılaştırma",
        "hint": "",
    },
    {
        "id": "sikayet",
        "no": 9,
        "title": "x - ekşi - şikayetvar",
        "hint": "",
    },
    {
        "id": "store_charts",
        "no": 12,
        "title": "Play / App Store kategori listeleri",
        "hint": "",
    },
    {
        "id": "google_news",
        "no": 17,
        "title": "Google News vitrin",
        "hint": "",
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


def _store_plat(chart: dict[str, Any] | None) -> str:
    cid = str((chart or {}).get("id") or "").lower()
    if cid in ("android", "play"):
        return "android"
    return "ios"


def _collect_store_icon_map(payload: dict[str, Any] | None) -> dict[str, str]:
    """platform:app_id → ikon URL. Bir kez çekilir, sonraki taramada sistem tanır."""
    out: dict[str, str] = {}
    if not isinstance(payload, dict):
        return out
    raw = payload.get("icon_map")
    if isinstance(raw, dict):
        for k, v in raw.items():
            ks, vs = str(k or "").strip(), str(v or "").strip()
            if ks and vs:
                out[ks] = vs
    for chart in payload.get("charts") or []:
        if not isinstance(chart, dict):
            continue
        plat = _store_plat(chart)
        for app in chart.get("apps") or []:
            if not isinstance(app, dict):
                continue
            aid = str(app.get("id") or "").strip()
            icon = str(app.get("icon") or "").strip()
            if aid and icon:
                out[f"{plat}:{aid}"] = icon
    return out


def _apply_store_icon_map(charts: list[Any] | None, icon_map: dict[str, str]) -> None:
    for chart in charts or []:
        if not isinstance(chart, dict):
            continue
        plat = _store_plat(chart)
        for app in chart.get("apps") or []:
            if not isinstance(app, dict):
                continue
            aid = str(app.get("id") or "").strip()
            if not aid:
                continue
            icon = str(app.get("icon") or "").strip()
            if icon:
                icon_map[f"{plat}:{aid}"] = icon
                continue
            remembered = icon_map.get(f"{plat}:{aid}") or ""
            if remembered:
                app["icon"] = remembered


def _store_chart_comparable(old_apps: list[Any], new_apps: list[Any]) -> bool:
    """Skip Δ when the previous snapshot is a different slice (e.g. missing first 25)."""
    old_ids = [
        str(a.get("id") or "")
        for a in (old_apps or [])[:15]
        if isinstance(a, dict) and a.get("id")
    ]
    new_ids = [
        str(a.get("id") or "")
        for a in (new_apps or [])[:15]
        if isinstance(a, dict) and a.get("id")
    ]
    if len(old_ids) < 5 or len(new_ids) < 5:
        return bool(old_ids) and bool(new_ids) and len(set(old_ids) & set(new_ids)) >= 1
    return len(set(old_ids) & set(new_ids)) >= 3


def _enrich_store_charts(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    out = _strip_shots(incoming)
    icon_map = _collect_store_icon_map(prev)
    icon_map.update(_collect_store_icon_map(out))
    _apply_store_icon_map(out.get("charts") if isinstance(out.get("charts"), list) else [], icon_map)
    out["icon_map"] = icon_map
    prev_charts = {
        str(c.get("id") or ""): c
        for c in (prev.get("charts") or [])
        if isinstance(c, dict) and c.get("id")
    }
    for chart in out.get("charts") or []:
        if not isinstance(chart, dict):
            continue
        old_apps = (prev_charts.get(str(chart.get("id") or "")) or {}).get("apps") or []
        new_apps = chart.get("apps") or []
        if old_apps and new_apps and not _store_chart_comparable(old_apps, new_apps):
            for app in new_apps:
                if isinstance(app, dict):
                    app["prev_rank"] = None
                    app["delta"] = None
                    app["delta_n"] = None
            chart["dropped"] = []
            chart["moves"] = {"up": 0, "down": 0, "new": 0, "same": 0, "dropped": 0, "reset": True}
            continue
        old_map: dict[str, dict[str, Any]] = {}
        for app in old_apps:
            if not isinstance(app, dict) or not app.get("id"):
                continue
            try:
                rank = int(app.get("rank") or 0)
            except (TypeError, ValueError):
                rank = 0
            if rank:
                old_map[str(app["id"])] = {"rank": rank, "name": app.get("name") or ""}
        current_ids: set[str] = set()
        up = down = new = same = 0
        for app in chart.get("apps") or []:
            if not isinstance(app, dict):
                continue
            aid = str(app.get("id") or "")
            current_ids.add(aid)
            try:
                rank = int(app.get("rank") or 0)
            except (TypeError, ValueError):
                rank = 0
            old = old_map.get(aid)
            prev_rank = (old or {}).get("rank")
            app["prev_rank"] = prev_rank
            if prev_rank is None:
                app["delta"] = "new"
                app["delta_n"] = None
                new += 1
            else:
                app["delta_n"] = int(prev_rank) - rank
                if rank < prev_rank:
                    app["delta"] = "up"
                    up += 1
                elif rank > prev_rank:
                    app["delta"] = "down"
                    down += 1
                else:
                    app["delta"] = "same"
                    same += 1
        dropped = [
            {"id": k, "name": v.get("name") or k, "prev_rank": v.get("rank")}
            for k, v in old_map.items()
            if k not in current_ids
        ]
        dropped.sort(key=lambda r: int(r.get("prev_rank") or 0))
        matched_dn = [
            int(app.get("delta_n"))
            for app in chart.get("apps") or []
            if isinstance(app, dict) and app.get("delta") in ("up", "down") and app.get("delta_n") is not None
        ]
        mode_n = 0
        mode = 0
        if matched_dn:
            mode, mode_n = Counter(matched_dn).most_common(1)[0]
        if abs(mode) >= 20 and mode_n >= max(15, len(matched_dn) // 2):
            for app in chart.get("apps") or []:
                if isinstance(app, dict):
                    app["prev_rank"] = None
                    app["delta"] = None
                    app["delta_n"] = None
            chart["dropped"] = []
            chart["moves"] = {"up": 0, "down": 0, "new": 0, "same": 0, "dropped": 0, "reset": True}
            continue
        chart["dropped"] = dropped[:40]
        chart["moves"] = {"up": up, "down": down, "new": new, "same": same, "dropped": len(dropped)}
    runs = list(prev.get("runs") or []) if isinstance(prev.get("runs"), list) else []
    runs.append(
        {
            "at": out.get("scraped_at") or datetime.utcnow().isoformat(),
            "ok": bool(out.get("ok")),
            "summary": out.get("summary") or "",
            "moves": {
                "up": sum(int(((c.get("moves") or {}).get("up") or 0)) for c in out.get("charts") or [] if isinstance(c, dict)),
                "down": sum(int(((c.get("moves") or {}).get("down") or 0)) for c in out.get("charts") or [] if isinstance(c, dict)),
            },
        }
    )
    out["runs"] = runs[-_RUNS_KEEP:]
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
        elif key == "store_charts":
            existing["sections"][key] = _enrich_store_charts(prev, val)
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


def _next_run_iso(
    scraped_at: str | None,
    *,
    minutes: int | None = None,
    hours: int | None = None,
) -> str:
    raw = (scraped_at or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if minutes is not None:
        nxt = dt + timedelta(minutes=minutes)
    else:
        nxt = dt + timedelta(hours=hours if hours is not None else _INTERVAL_HOURS)
    return nxt.isoformat()


def format_pm_lab_when(iso: str | None) -> str:
    raw = str(iso or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw[:16].replace("T", " ")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        dt = dt.astimezone(ZoneInfo("Europe/Istanbul"))
    except Exception:
        pass
    return dt.strftime("%d.%m.%Y %H:%M")


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
        scraped_at = str(data.get("scraped_at") or payload.get("scraped_at") or "")
        cards.append(
            {
                **spec,
                "ok": data.get("ok"),
                "message": data.get("message") or "",
                "scraped_at": scraped_at,
                "scraped_at_label": format_pm_lab_when(scraped_at),
                "summary": data.get("summary") or "",
                "data": data,
                "shot_names": [],
            }
        )
    comp_block = raw_sections.get("competitors") if isinstance(raw_sections.get("competitors"), dict) else {}
    boot = {
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "next_at": _next_run_iso(
            str(comp_block.get("scraped_at") or payload.get("scraped_at") or ""),
            minutes=COMPETITORS_INTERVAL_MIN,
        ),
        "interval_hours": _INTERVAL_HOURS,
        "interval_minutes": COMPETITORS_INTERVAL_MIN,
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "sections": boot_sections,
        "defs": SECTION_DEFS,
    }
    return {
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "scraped_at_label": format_pm_lab_when(str(payload.get("scraped_at") or payload.get("updated_at") or "")),
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "source": payload.get("source") or "",
        "cards": cards,
        "boot_json": json.dumps(boot, ensure_ascii=False),
    }
