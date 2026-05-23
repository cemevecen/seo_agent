"""Crashlytics derin analiz — BQ export alanları (threads, breadcrumbs, vb.)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _ts_iso(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def parse_breadcrumbs(raw: list | None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for bc in raw or []:
        if not isinstance(bc, dict):
            continue
        params = bc.get("params") or []
        parts = []
        for p in params:
            if isinstance(p, dict) and p.get("key"):
                parts.append(f"{p.get('key')}={p.get('value', '')}")
        out.append({
            "name": str(bc.get("name") or ""),
            "timestamp": _ts_iso(bc.get("timestamp")) or "",
            "params": ", ".join(parts),
        })
    return out


def parse_custom_keys(raw: list | None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for ck in raw or []:
        if isinstance(ck, dict) and ck.get("key"):
            out.append({"key": str(ck["key"]), "value": str(ck.get("value") or "")})
    return out


def parse_exceptions(raw: list | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ex in raw or []:
        if not isinstance(ex, dict):
            continue
        frames = []
        for f in ex.get("frames") or []:
            if not isinstance(f, dict):
                continue
            frames.append({
                "file": f.get("file") or "",
                "symbol": f.get("symbol") or "",
                "line": f.get("line"),
            })
        out.append({
            "type": ex.get("type") or "",
            "exception_message": ex.get("exception_message") or ex.get("message") or "",
            "nested": bool(ex.get("nested")),
            "frames": frames[:40],
        })
    return out


def parse_threads(raw: list | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in raw or []:
        if not isinstance(t, dict):
            continue
        frames = []
        for f in t.get("frames") or []:
            if not isinstance(f, dict):
                continue
            frames.append({
                "file": f.get("file") or "",
                "symbol": f.get("symbol") or "",
                "line": f.get("line"),
                "blamed": bool(f.get("blamed")),
            })
        out.append({
            "thread_name": t.get("thread_name") or "",
            "title": t.get("title") or "",
            "subtitle": t.get("subtitle") or "",
            "blamed": bool(t.get("blamed")),
            "crashed": bool(t.get("crashed")),
            "frames": frames[:50],
        })
    return out


def enrich_issue_row(row: dict[str, Any], *, days: int) -> dict[str, Any]:
    """Console benzeri rozetler: yeni, tekrarlayan, erken oturum yaklaşımı."""
    events = int(row.get("event_count") or 0)
    users = int(row.get("affected_users") or 0)
    ratio = round(events / users, 2) if users > 0 else float(events)
    row = dict(row)
    row["events_per_user"] = ratio

    first_seen = row.get("first_seen")
    last_seen = row.get("last_seen")
    badges: list[str] = []

    now = datetime.now(timezone.utc)
    if first_seen:
        try:
            fs = first_seen if hasattr(first_seen, "tzinfo") else datetime.fromisoformat(str(first_seen).replace("Z", "+00:00"))
            if fs.tzinfo is None:
                fs = fs.replace(tzinfo=timezone.utc)
            age_days = (now - fs).total_seconds() / 86400
            if age_days <= 3:
                badges.append("new")
            if age_days <= 1 and events >= 5:
                badges.append("early")
        except Exception:
            pass

    if users > 0 and ratio >= 3:
        badges.append("repetitive")

    if first_seen and last_seen:
        try:
            ls = last_seen if hasattr(last_seen, "tzinfo") else datetime.fromisoformat(str(last_seen).replace("Z", "+00:00"))
            if ls.tzinfo is None:
                ls = ls.replace(tzinfo=timezone.utc)
            if (now - ls).total_seconds() < 86400 and events >= 10:
                badges.append("spiking")
        except Exception:
            pass

    row["badges"] = badges
    return row


def merge_breakdown_rows(
    rows_list: list[list[dict]], key_field: str, count_field: str = "event_count", *, limit: int = 20
) -> list[dict]:
    merged: dict[str, dict] = {}
    total = 0
    for rows in rows_list:
        for r in rows:
            key = str(r.get(key_field) or "bilinmiyor")
            cnt = int(r.get(count_field) or 0)
            total += cnt
            if key in merged:
                merged[key][count_field] += cnt
            else:
                merged[key] = {key_field: key, count_field: cnt}
    result = sorted(merged.values(), key=lambda x: -x[count_field])
    for r in result:
        r["pct"] = round(r[count_field] / total * 100, 1) if total > 0 else 0.0
    return result[:limit]


def summarize_issue_tr(
    *,
    issue_title: str,
    error_type: str,
    total_events: int,
    affected_users: int,
    blame_frames: list[dict],
    trend: list[dict],
    process_states: list[dict] | None = None,
) -> str:
    """Issue için kısa Türkçe AI özet (Groq)."""
    from backend.config import settings
    from backend.services.inbox_llm import _groq_plain_text

    model = (getattr(settings, "groq_model", None) or "llama-3.3-70b-versatile").strip()

    frames_txt = "\n".join(
        f"- {f.get('file', '')}:{f.get('line', '')} {f.get('symbol', '')} ({f.get('occurrences', 0)}x)"
        for f in (blame_frames or [])[:5]
    )
    trend_txt = ", ".join(f"{t.get('date')}: {t.get('count')}" for t in (trend or [])[-7:])
    state_txt = ", ".join(f"{s.get('state')}: {s.get('event_count')}" for s in (process_states or [])[:4])

    system = (
        "Sen kıdemli mobil güvenilirlik mühendisisin. Firebase Crashlytics issue verisini "
        "Türkçe, 8-12 cümlelik net bir özet halinde yaz. Markdown kullan: ## Özet, ## Olası neden, "
        "## Aciliyet, ## Önerilen aksiyon. Teknik terimleri backtick ile vurgula."
    )
    user = (
        f"Issue: {issue_title}\n"
        f"Tür: {error_type}\n"
        f"Olay: {total_events}, Kullanıcı: {affected_users}\n"
        f"Stack (blame):\n{frames_txt or '(yok)'}\n"
        f"Trend: {trend_txt or '(yok)'}\n"
        f"Process state: {state_txt or '(yok)'}"
    )
    try:
        return _groq_plain_text(system, user, model=model)
    except Exception as exc:
        logger.warning("Issue AI özet hatası: %s", exc)
        raise
