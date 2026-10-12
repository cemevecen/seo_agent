"""Google Search Status Dashboard — core / spam update incidents.

Kaynak: https://status.search.google.com/incidents.json
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import httpx

LOGGER = logging.getLogger(__name__)

_INCIDENTS_URL = "https://status.search.google.com/incidents.json"
_CACHE_TTL_SEC = 3600.0
_cache: tuple[float, list[dict[str, Any]]] | None = None

_CORE_RE = re.compile(r"\bcore\s+update\b", re.I)
_SPAM_RE = re.compile(r"\bspam\s+update\b", re.I)
_DISCOVER_RE = re.compile(r"\bdiscover\b", re.I)


def _parse_iso_day(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        return None


def _classify(desc: str) -> str | None:
    text = desc or ""
    if _CORE_RE.search(text):
        return "core"
    if _SPAM_RE.search(text):
        return "spam"
    if _DISCOVER_RE.search(text) and "update" in text.lower():
        return "discover"
    return None


def _normalize_incident(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if str(raw.get("service_name") or "").strip().lower() != "ranking":
        return None
    desc = str(raw.get("external_desc") or "").strip()
    kind = _classify(desc)
    if not kind:
        return None
    begin = _parse_iso_day(raw.get("begin") or raw.get("created"))
    end = _parse_iso_day(raw.get("end")) or begin
    if not begin:
        return None
    uri = str(raw.get("uri") or "").strip()
    if uri and not uri.startswith("http"):
        uri = "https://status.search.google.com/" + uri.lstrip("/")
    return {
        "id": str(raw.get("id") or "")[:64],
        "kind": kind,
        "label": desc[:160],
        "start": begin,
        "end": end,
        "uri": uri[:256],
    }


def fetch_ranking_updates(*, force: bool = False) -> list[dict[str, Any]]:
    global _cache
    now = time.monotonic()
    if not force and _cache and (now - _cache[0]) < _CACHE_TTL_SEC:
        return list(_cache[1])
    try:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            resp = client.get(
                _INCIDENTS_URL,
                headers={"User-Agent": "seo-agent/1.0 (+google-search-status)"},
            )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("google search status fetch failed: %s", exc)
        if _cache:
            return list(_cache[1])
        return []
    rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            norm = _normalize_incident(item)
            if norm:
                rows.append(norm)
    rows.sort(key=lambda r: r.get("start") or "", reverse=True)
    _cache = (now, rows)
    return list(rows)


def updates_in_range(
    *,
    start: str | None = None,
    end: str | None = None,
    kinds: tuple[str, ...] = ("core", "spam"),
) -> dict[str, Any]:
    start_d = date.fromisoformat(start) if start else None
    end_d = date.fromisoformat(end) if end else None
    if start_d and end_d and start_d > end_d:
        start_d, end_d = end_d, start_d
    want = {k.lower() for k in kinds if k}
    out: list[dict[str, Any]] = []
    for row in fetch_ranking_updates():
        if want and str(row.get("kind") or "") not in want:
            continue
        rs = date.fromisoformat(row["start"])
        re_ = date.fromisoformat(row["end"] or row["start"])
        if start_d and re_ < start_d:
            continue
        if end_d and rs > end_d:
            continue
        out.append(row)
    out.sort(key=lambda r: r.get("start") or "")
    return {
        "ok": True,
        "source": "google_search_status",
        "url": _INCIDENTS_URL,
        "start": start_d.isoformat() if start_d else None,
        "end": end_d.isoformat() if end_d else None,
        "count": len(out),
        "updates": out,
    }
