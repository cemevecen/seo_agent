"""Owner PM lab workspace — bölüm bazlı JSON + SERP/News geçmişi."""

from __future__ import annotations

import json
import locale
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from copy import deepcopy
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from backend.models import OwnerPmLabWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1
_RUNS_KEEP = 48
_INTERVAL_HOURS = 3
COMPETITORS_INTERVAL_MIN = 10

SERP_KEYWORDS_RAW: tuple[str, ...] = (
    "gram gümüş",
    "usd",
    "altın",
    "altın fiyatları",
    "çeyrek altın",
    "gram altın",
    "gram altın fiyatı",
    "harem çeyrek altın",
    "harem gram altın",
    "harem dolar",
    "kapalıçarşı gram altın",
    "bitcoin",
    "kripto para",
    "brent petrol",
    "benzin fiyatı",
    "motorin fiyatı",
    "döviz",
    "döviz çevirici",
    "dolar",
    "ons altın",
)


def _tr_collate_key(text: str) -> str:
    try:
        return locale.strxfrm(text)
    except Exception:
        return text.casefold()


try:
    locale.setlocale(locale.LC_COLLATE, "tr_TR.UTF-8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_COLLATE, "tr_TR")
    except locale.Error:
        pass

SERP_KEYWORDS: tuple[str, ...] = tuple(sorted(SERP_KEYWORDS_RAW, key=_tr_collate_key))

SERP_BATCH_SIZE = 5
SERP_BATCH_COUNT = max(1, (len(SERP_KEYWORDS) + SERP_BATCH_SIZE - 1) // SERP_BATCH_SIZE)


def serp_keyword_batches() -> tuple[tuple[str, ...], ...]:
    out: list[tuple[str, ...]] = []
    items = list(SERP_KEYWORDS)
    for i in range(0, len(items), SERP_BATCH_SIZE):
        out.append(tuple(items[i : i + SERP_BATCH_SIZE]))
    return tuple(out)


def serp_keywords_for_batch(batch_index: int) -> tuple[str, ...]:
    batches = serp_keyword_batches()
    if not batches:
        return ()
    idx = int(batch_index) % len(batches)
    return batches[idx]

SECTION_DEFS: list[dict[str, Any]] = [
    {
        "id": "serp",
        "no": 2,
        "title": "SERP — first 4 pages",
        "hint": "",
    },
    {
        "id": "competitors",
        "no": 3,
        "title": "Competitor FX price comparison",
        "hint": "",
    },
    {
        "id": "store_charts",
        "no": 12,
        "title": "Play / App Store category charts",
        "hint": "",
        "pm_lab_page": False,
    },
    {
        "id": "google_news",
        "no": 17,
        "title": "Google News showcase",
        "hint": "",
        "pm_lab_page": False,
    },
]


def _pm_lab_page_specs() -> list[dict[str, Any]]:
    return [{k: v for k, v in spec.items() if k != "pm_lab_page"} for spec in SECTION_DEFS if spec.get("pm_lab_page", True)]


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


def _prev_keywords_map(prev: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for kw in prev.get("keywords") or []:
        if not isinstance(kw, dict):
            continue
        name = str(kw.get("keyword") or "").strip()
        if name:
            out[name] = kw
    return out


def _serp_keywords_row_count(keywords: list[Any]) -> int:
    return sum(len(kw.get("rows") or []) for kw in keywords if isinstance(kw, dict))


def _serp_has_rows(keywords: list[Any]) -> bool:
    return _serp_keywords_row_count(keywords) > 0


def _serp_last_good_source(prev: dict[str, Any]) -> dict[str, Any] | None:
    lg = prev.get("last_good")
    if isinstance(lg, dict) and _serp_has_rows(lg.get("keywords") or []):
        return lg
    kws = prev.get("keywords") if isinstance(prev.get("keywords"), list) else []
    if not _serp_has_rows(kws):
        return None
    clean = [deepcopy(kw) for kw in kws if isinstance(kw, dict)]
    return {
        "keywords": clean,
        "row_count": _serp_keywords_row_count(clean),
        "scraped_at": prev.get("scraped_at"),
    }


def _serp_restore_last_good(block: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    out = dict(block)
    restored = [deepcopy(kw) for kw in (source.get("keywords") or []) if isinstance(kw, dict)]
    for kw in restored:
        kw["rows_stale"] = True
        kw.pop("entered", None)
        kw.pop("dropped", None)
        kw.pop("climbed", None)
        kw.pop("fell", None)
        kw["moves"] = {"entered": 0, "dropped": 0, "up": 0, "down": 0}
    out["keywords"] = restored
    out["rows_stale"] = True
    out["row_count"] = int(source.get("row_count") or _serp_keywords_row_count(restored))
    out["ok"] = bool(out["row_count"])
    note = "Son tarama boş; önceki SERP listesi gösteriliyor."
    out["message"] = note
    return out


def _serp_apply_last_good_if_empty(block: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(block, dict):
        return block
    kws = block.get("keywords") if isinstance(block.get("keywords"), list) else []
    if _serp_has_rows(kws):
        return block
    source = _serp_last_good_source(block)
    if not source:
        return block
    return _serp_restore_last_good(block, source)


def _serp_apply_keyword_deltas(
    prev_idx: dict[str, dict[str, int]],
    prev_kws: dict[str, dict[str, Any]],
    keywords: list[Any],
) -> None:
    for kw in keywords:
        if not isinstance(kw, dict):
            continue
        name = str(kw.get("keyword") or "")
        old_map = prev_idx.get(name) or {}
        rows = [r for r in (kw.get("rows") if isinstance(kw.get("rows"), list) else []) if isinstance(r, dict)]
        if not rows:
            prev_kw = prev_kws.get(name) or {}
            prev_rows = [dict(r) for r in (prev_kw.get("rows") or []) if isinstance(r, dict)]
            if prev_rows:
                kw["rows"] = prev_rows
                kw["rows_stale"] = True
                kw["our_rank"] = prev_kw.get("our_rank")
                kw["row_count"] = len(prev_rows)
            else:
                kw["rows"] = []
                kw["row_count"] = 0
            kw["entered"] = []
            kw["dropped"] = []
            kw["climbed"] = []
            kw["fell"] = []
            kw["moves"] = {"entered": 0, "dropped": 0, "up": 0, "down": 0}
            continue
        kw.pop("rows_stale", None)
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


def _published_serp_base(prev: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(prev)
    for key in ("serp_refresh_pending", "refresh_in_progress", "refresh_progress"):
        out.pop(key, None)
    return out


def _merge_pending_serp_keywords(pending: dict[str, Any]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    batches = pending.get("batches") if isinstance(pending.get("batches"), dict) else {}
    for batch in batches.values():
        if not isinstance(batch, dict):
            continue
        for kw in batch.get("keywords") or []:
            if not isinstance(kw, dict):
                continue
            name = str(kw.get("keyword") or "").strip()
            if name:
                by_name[name] = deepcopy(kw)
    merged: list[dict[str, Any]] = []
    for name in SERP_KEYWORDS:
        if name in by_name:
            merged.append(by_name[name])
    for name, kw in by_name.items():
        if name not in SERP_KEYWORDS:
            merged.append(kw)
    return merged


def _enrich_serp_finalize(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    out = _strip_shots(incoming)
    prev_published = _published_serp_base(prev)
    prev_idx = _kw_rank_index(prev_published.get("keywords") or [])
    prev_kws = _prev_keywords_map(prev_published)
    keywords = out.get("keywords") if isinstance(out.get("keywords"), list) else []
    _serp_apply_keyword_deltas(prev_idx, prev_kws, keywords)
    out["keywords"] = keywords

    runs = list(prev_published.get("runs") or []) if isinstance(prev_published.get("runs"), list) else []
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
    out["row_count"] = snap["row_count"]

    fresh_rows = sum(
        len(kw.get("rows") or [])
        for kw in keywords
        if isinstance(kw, dict) and not kw.get("rows_stale")
    )
    if fresh_rows > 0:
        clean_keywords = [deepcopy(kw) for kw in keywords if isinstance(kw, dict)]
        for kw in clean_keywords:
            kw.pop("rows_stale", None)
        out["last_good"] = {
            "keywords": clean_keywords,
            "row_count": fresh_rows,
            "scraped_at": out.get("scraped_at"),
        }
        out.pop("rows_stale", None)
    elif not _serp_has_rows(keywords):
        out["message"] = out.get("message") or "SERP boş — Google headless engeli; Mac bridge headed tarama gerekir."
        last_good = _serp_last_good_source(prev_published)
        if last_good:
            out = _serp_restore_last_good(out, last_good)
            out["last_good"] = deepcopy(last_good)
            snap = dict(snap)
            snap["row_count"] = out["row_count"]
            snap["ok"] = bool(out["row_count"])
            if out["runs"]:
                out["runs"][-1] = snap
        elif isinstance(prev_published.get("last_good"), dict):
            out["last_good"] = deepcopy(prev_published["last_good"])
    elif isinstance(prev_published.get("last_good"), dict):
        out["last_good"] = deepcopy(prev_published["last_good"])

    if any(isinstance(kw, dict) and kw.get("rows_stale") for kw in (out.get("keywords") or [])):
        out["rows_stale"] = True

    out.pop("serp_refresh_pending", None)
    out.pop("refresh_in_progress", None)
    out.pop("refresh_progress", None)
    return out


def _enrich_serp_batch_cycle(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    batch_index = int(incoming.get("batch_index") or 0)
    batch_total = max(1, int(incoming.get("batch_total") or SERP_BATCH_COUNT))
    published = _published_serp_base(prev)

    pending_raw = prev.get("serp_refresh_pending")
    cycle_active = isinstance(pending_raw, dict) and bool(prev.get("refresh_in_progress"))
    if batch_index == 0 and not cycle_active:
        pending = {
            "started_at": str(incoming.get("scraped_at") or datetime.utcnow().isoformat()),
            "batch_total": batch_total,
            "batches": {},
        }
    elif isinstance(pending_raw, dict):
        pending = deepcopy(pending_raw)
    else:
        pending = {
            "started_at": str(incoming.get("scraped_at") or datetime.utcnow().isoformat()),
            "batch_total": batch_total,
            "batches": {},
        }

    raw_kws = [deepcopy(kw) for kw in (incoming.get("keywords") or []) if isinstance(kw, dict)]
    pending.setdefault("batches", {})
    if isinstance(pending["batches"], dict):
        pending["batches"][str(batch_index)] = {
            "keywords": raw_kws,
            "scraped_at": incoming.get("scraped_at"),
            "ok": bool(incoming.get("ok")),
            "blocked": bool(incoming.get("blocked")),
        }

    have = set(pending.get("batches") or {})
    need = {str(i) for i in range(batch_total)}
    if have >= need:
        merged = _merge_pending_serp_keywords(pending)
        finalized_incoming = {
            **incoming,
            "keywords": merged,
            "scraped_at": str(incoming.get("scraped_at") or datetime.utcnow().isoformat()),
            "summary": f"{batch_total} batch · {len(merged)} kelime · tamamlandı",
            "ok": _serp_has_rows(merged),
            "batch_index": None,
        }
        return _enrich_serp_finalize(prev, finalized_incoming)

    published["serp_refresh_pending"] = pending
    published["refresh_in_progress"] = True
    published["refresh_progress"] = f"{len(have)}/{batch_total}"
    published["message"] = (
        f"SERP yenileniyor ({len(have)}/{batch_total}) — tablo önceki tam taramayı gösteriyor."
    )
    return published


def _enrich_serp(prev: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    if incoming.get("batch_index") is not None:
        return _enrich_serp_batch_cycle(prev, incoming)

    out = _strip_shots(incoming)
    prev_idx = _kw_rank_index(prev.get("keywords") or [])
    prev_kws = _prev_keywords_map(prev)
    keywords = out.get("keywords") if isinstance(out.get("keywords"), list) else []
    _serp_apply_keyword_deltas(prev_idx, prev_kws, keywords)
    out["keywords"] = keywords

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
    out["row_count"] = snap["row_count"]

    fresh_rows = sum(
        len(kw.get("rows") or [])
        for kw in keywords
        if isinstance(kw, dict) and not kw.get("rows_stale")
    )
    if fresh_rows > 0:
        clean_keywords = [deepcopy(kw) for kw in keywords if isinstance(kw, dict)]
        for kw in clean_keywords:
            kw.pop("rows_stale", None)
        out["last_good"] = {
            "keywords": clean_keywords,
            "row_count": fresh_rows,
            "scraped_at": out.get("scraped_at"),
        }
        out.pop("rows_stale", None)
    elif not _serp_has_rows(keywords):
        out["message"] = out.get("message") or "SERP boş — Google headless engeli; Mac bridge headed tarama gerekir."
        last_good = _serp_last_good_source(prev)
        if last_good:
            out = _serp_restore_last_good(out, last_good)
            out["last_good"] = deepcopy(last_good)
            snap = dict(snap)
            snap["row_count"] = out["row_count"]
            snap["ok"] = bool(out["row_count"])
            if out["runs"]:
                out["runs"][-1] = snap
        elif isinstance(prev.get("last_good"), dict):
            out["last_good"] = deepcopy(prev["last_good"])
    elif isinstance(prev.get("last_good"), dict):
        out["last_good"] = deepcopy(prev["last_good"])

    if any(isinstance(kw, dict) and kw.get("rows_stale") for kw in (out.get("keywords") or [])):
        out["rows_stale"] = True

    out.pop("serp_refresh_pending", None)
    out.pop("refresh_in_progress", None)
    out.pop("refresh_progress", None)
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


def _itunes_artwork(ids: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    clean = [str(i).strip() for i in ids if str(i or "").strip().isdigit()]
    for i in range(0, len(clean), 50):
        chunk = clean[i : i + 50]
        url = f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country=tr"
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=18) as resp:
                info = json.loads(resp.read().decode("utf-8", errors="replace") or "{}")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
            continue
        for row in info.get("results") or []:
            if not isinstance(row, dict):
                continue
            aid = str(row.get("trackId") or "").strip()
            icon = str(row.get("artworkUrl100") or row.get("artworkUrl60") or "").strip()
            if aid and icon:
                out[aid] = icon.replace("100x100bb", "128x128bb")
    return out


def _play_icon_one(pkg: str) -> tuple[str, str]:
    try:
        from google_play_scraper import app as gp_app

        meta = gp_app(pkg, lang="tr", country="tr")
        icon = str((meta or {}).get("icon") or "").strip()
        return pkg, icon
    except Exception:
        return pkg, ""


def _play_artwork(packages: list[str], *, budget_s: float = 8.0) -> dict[str, str]:
    out: dict[str, str] = {}
    pkgs = [str(p).strip() for p in packages if str(p or "").strip()]
    if not pkgs:
        return out
    deadline = time.monotonic() + max(1.5, budget_s)
    pool = ThreadPoolExecutor(max_workers=8)
    try:
        futs = [pool.submit(_play_icon_one, p) for p in pkgs]
        for fut in as_completed(futs):
            if time.monotonic() > deadline:
                break
            try:
                pkg, icon = fut.result()
            except Exception:
                continue
            if pkg and icon:
                out[pkg] = icon
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return out


def _hydrate_store_icons(
    store: dict[str, Any],
    *,
    fetch: bool | None = None,
    platforms: tuple[str, ...] = ("ios", "android"),
    android_budget_s: float = 10.0,
) -> int:
    """Eksik mağaza ikonlarını bir kez doldur (iTunes / Play) ve icon_map'e yaz."""
    if not isinstance(store, dict):
        return 0
    icon_map = _collect_store_icon_map(store)
    _apply_store_icon_map(store.get("charts") if isinstance(store.get("charts"), list) else [], icon_map)
    if fetch is None:
        fetch = not bool(os.environ.get("PYTEST_CURRENT_TEST"))
    missing_ios: list[str] = []
    missing_and: list[str] = []
    want = {str(p) for p in platforms}
    for chart in store.get("charts") or []:
        if not isinstance(chart, dict):
            continue
        plat = _store_plat(chart)
        for app in chart.get("apps") or []:
            if not isinstance(app, dict) or str(app.get("icon") or "").strip():
                continue
            aid = str(app.get("id") or "").strip()
            if not aid:
                continue
            if plat == "ios":
                missing_ios.append(aid)
            else:
                missing_and.append(aid)
    added = 0
    if fetch and missing_ios and "ios" in want:
        for aid, icon in _itunes_artwork(missing_ios).items():
            key = f"ios:{aid}"
            if icon and key not in icon_map:
                icon_map[key] = icon
                added += 1
    if fetch and missing_and and "android" in want:
        for pkg, icon in _play_artwork(missing_and, budget_s=android_budget_s).items():
            key = f"android:{pkg}"
            if icon and key not in icon_map:
                icon_map[key] = icon
                added += 1
    if added:
        _apply_store_icon_map(store.get("charts") if isinstance(store.get("charts"), list) else [], icon_map)
        store["icon_map"] = icon_map
    return added


_HYDRATE_BG_LOCK = threading.Lock()
_HYDRATE_BG_RUNNING = False


def _android_icons_missing(store: dict[str, Any]) -> int:
    n = 0
    for chart in store.get("charts") or []:
        if not isinstance(chart, dict) or _store_plat(chart) != "android":
            continue
        for app in chart.get("apps") or []:
            if isinstance(app, dict) and not str(app.get("icon") or "").strip() and str(app.get("id") or "").strip():
                n += 1
    return n


def _bg_hydrate_android_icons() -> None:
    global _HYDRATE_BG_RUNNING
    try:
        from backend.database import SessionLocal

        with SessionLocal() as db:
            payload = load_payload(db)
            store = (payload.get("sections") or {}).get("store_charts")
            if not isinstance(store, dict):
                return
            n = _hydrate_store_icons(store, platforms=("android",), android_budget_s=22.0)
            if n:
                _persist_payload(db, payload)
                LOGGER.info("pm-lab store android icons hydrated: %s", n)
    except Exception:
        LOGGER.exception("pm-lab store android icon hydrate")
    finally:
        with _HYDRATE_BG_LOCK:
            _HYDRATE_BG_RUNNING = False


def _kick_android_icon_hydrate(store: dict[str, Any]) -> None:
    global _HYDRATE_BG_RUNNING
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return
    if not _android_icons_missing(store):
        return
    with _HYDRATE_BG_LOCK:
        if _HYDRATE_BG_RUNNING:
            return
        _HYDRATE_BG_RUNNING = True
    threading.Thread(target=_bg_hydrate_android_icons, name="pml-android-icons", daemon=True).start()


def _persist_payload(db: Session, payload: dict[str, Any]) -> None:
    row = _get_or_create(db)
    row.payload_json = json.dumps(payload, ensure_ascii=False)
    db.commit()


PM_LAB_REFRESH_JOBS = ("serp", "competitors", "store_charts", "google_news")


def _refresh_job_ids(raw: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, dict):
            job = str(item.get("job") or "").strip()
        else:
            job = str(item or "").strip()
        if job in PM_LAB_REFRESH_JOBS and job not in out:
            out.append(job)
    return out


REFRESH_QUEUE_TTL_SEC = 90
REFRESH_RUNNING_TTL_SEC = 20 * 60
SERP_CYCLE_RESUME_SEC = int(os.environ.get("PM_LAB_SERP_CYCLE_RESUME_SEC") or str(75 * 60))
SERP_CYCLE_STALE_SEC = int(os.environ.get("PM_LAB_SERP_CYCLE_STALE_SEC") or str(3 * 3600))


def serp_missing_batch_indices(pending: dict[str, Any], batch_total: int) -> list[int]:
    batches = pending.get("batches") if isinstance(pending.get("batches"), dict) else {}
    have = {int(k) for k in batches if str(k).isdigit()}
    total = max(1, int(batch_total))
    return [i for i in range(total) if i not in have]


def serp_cycle_meta(serp: dict[str, Any]) -> dict[str, Any]:
    """Bridge resume + UI: yarım SERP döngüsü meta."""
    if not isinstance(serp, dict) or not serp.get("refresh_in_progress"):
        return {"missing_batches": [], "stale": False, "started_at": ""}
    pending = serp.get("serp_refresh_pending")
    if not isinstance(pending, dict):
        return {"missing_batches": [], "stale": False, "started_at": ""}
    batch_total = max(1, int(pending.get("batch_total") or SERP_BATCH_COUNT))
    missing = serp_missing_batch_indices(pending, batch_total)
    started_raw = str(pending.get("started_at") or "")
    started = _parse_iso_dt(started_raw)
    resume = False
    stale = False
    if started is not None:
        age = (datetime.now(timezone.utc) - started).total_seconds()
        resume = age >= SERP_CYCLE_RESUME_SEC
        stale = age >= SERP_CYCLE_STALE_SEC
    return {
        "missing_batches": missing,
        "resume": resume,
        "stale": stale,
        "started_at": started_raw,
        "batch_total": batch_total,
    }


def _prune_stale_serp_cycle(prev: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Yarım kalan SERP döngüsü çok uzun sürdüyse kısmi veriyle kilidi kaldır."""
    meta = serp_cycle_meta(prev)
    if not meta["missing_batches"] or not meta["stale"]:
        return prev, False
    pending_raw = prev.get("serp_refresh_pending")
    if not isinstance(pending_raw, dict):
        return prev, False
    batch_total = int(meta["batch_total"])
    missing = meta["missing_batches"]
    merged = _merge_pending_serp_keywords(pending_raw)
    if not merged:
        out = _published_serp_base(prev)
        out["message"] = "SERP tarama yarım kaldı — yeniden başlatılıyor."
        return out, True
    finalized = _enrich_serp_finalize(
        prev,
        {
            "ok": _serp_has_rows(merged),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "keywords": merged,
            "summary": f"{batch_total} batch · {len(merged)} kelime · kısmi ({len(missing)} batch eksik)",
            "message": (
                f"SERP kısmi tamamlandı — {len(missing)} batch zaman aşımı; "
                "Mac bridge kalan batch'leri otomatik sürdürecek."
            ),
            "rows_stale": True,
        },
    )
    finalized["rows_stale"] = True
    return finalized, True


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _prune_refresh_state(data: dict[str, Any]) -> bool:
    """Drop stale Refresh queue / running flags so the UI cannot stick on Queued."""
    changed = False
    now = datetime.now(timezone.utc)
    raw = data.get("refresh_queue")
    kept: list[dict[str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                job = str(item.get("job") or "").strip()
                ts = str(item.get("requested_at") or "")
            else:
                job = str(item or "").strip()
                ts = ""
            if job not in PM_LAB_REFRESH_JOBS:
                changed = True
                continue
            requested = _parse_iso_dt(ts)
            if requested is not None and (now - requested).total_seconds() > REFRESH_QUEUE_TTL_SEC:
                changed = True
                continue
            kept.append({"job": job, "requested_at": ts or now.isoformat()})
        if kept != raw:
            data["refresh_queue"] = kept
            changed = True
    running = str(data.get("refresh_running") or "").strip()
    if running:
        started = _parse_iso_dt(str(data.get("refresh_running_at") or ""))
        if started is None:
            data["refresh_running_at"] = now.isoformat()
            changed = True
        elif (now - started).total_seconds() > REFRESH_RUNNING_TTL_SEC:
            data["refresh_running"] = ""
            data["refresh_running_at"] = ""
            changed = True
    return changed


def enqueue_pm_lab_refresh(db: Session, job: str) -> dict[str, Any]:
    job = str(job or "").strip()
    if job not in PM_LAB_REFRESH_JOBS:
        raise ValueError("unknown job")
    row = _get_or_create(db)
    data = _loads(row.payload_json)
    _prune_refresh_state(data)
    queued = _refresh_job_ids(data.get("refresh_queue"))
    if job not in queued:
        queued.append(job)
    now = datetime.now(timezone.utc).isoformat()
    data["refresh_queue"] = [{"job": j, "requested_at": now} for j in queued]
    row.payload_json = json.dumps(data, ensure_ascii=False)
    db.commit()
    return {"ok": True, "job": job, "queued": queued}


def claim_pm_lab_refresh(db: Session) -> str | None:
    row = _get_or_create(db)
    data = _loads(row.payload_json)
    pruned = _prune_refresh_state(data)
    queued = _refresh_job_ids(data.get("refresh_queue"))
    if not queued:
        if pruned:
            row.payload_json = json.dumps(data, ensure_ascii=False)
            db.commit()
        return None
    job = queued.pop(0)
    now = datetime.now(timezone.utc).isoformat()
    data["refresh_queue"] = [{"job": j, "requested_at": now} for j in queued]
    data["refresh_running"] = job
    data["refresh_running_at"] = now
    row.payload_json = json.dumps(data, ensure_ascii=False)
    db.commit()
    return job


def pm_lab_refresh_status(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    return {
        "queued": _refresh_job_ids(data.get("refresh_queue")),
        "running": str(data.get("refresh_running") or "").strip(),
    }


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
    changed = _prune_refresh_state(data)
    sections = data.get("sections")
    if isinstance(sections, dict):
        serp = sections.get("serp")
        if isinstance(serp, dict):
            pruned, serp_changed = _prune_stale_serp_cycle(serp)
            if serp_changed:
                sections["serp"] = pruned
                changed = True
    if changed:
        row.payload_json = json.dumps(data, ensure_ascii=False)
        db.commit()
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
    running = str(existing.get("refresh_running") or "").strip()
    if running and running in incoming:
        keep_running = False
        if running == "serp":
            serp = existing["sections"].get("serp")
            if isinstance(serp, dict) and serp.get("refresh_in_progress"):
                keep_running = True
        if not keep_running:
            existing["refresh_running"] = ""
            existing["refresh_running_at"] = ""
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
    store = raw_sections.get("store_charts")
    if isinstance(store, dict):
        try:
            n = _hydrate_store_icons(store)
            if n:
                _persist_payload(db, payload)
        except Exception:
            LOGGER.exception("pm-lab store icon hydrate")
        _kick_android_icon_hydrate(store)
    cards: list[dict[str, Any]] = []
    boot_sections: dict[str, Any] = {}
    for spec in SECTION_DEFS:
        block = raw_sections.get(spec["id"])
        if not isinstance(block, dict):
            block = {}
        if spec["id"] == "serp":
            block = _serp_apply_last_good_if_empty(block)
        data = _strip_shots(block)
        boot_sections[spec["id"]] = data
        if not spec.get("pm_lab_page", True):
            continue
        scraped_at = str(data.get("scraped_at") or payload.get("scraped_at") or "")
        cards.append(
            {
                **{k: v for k, v in spec.items() if k != "pm_lab_page"},
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
        "defs": _pm_lab_page_specs(),
        "serp_tab_keywords": list(SERP_KEYWORDS),
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
