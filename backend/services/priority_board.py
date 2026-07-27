"""Ana sayfa — git.nokta panosu (GitLab /boards ile aynı kaynak).

Open / Doing / Testing / Closed maddeleri git.nokta.com issue’larından gelir
(GITLAB_PRIVATE_TOKEN + VPN / relay). Railway proje maddeleri değil.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from backend.services.gitlab_board import fetch_all_issues_async, get_gitlab_token

LOGGER = logging.getLogger(__name__)

PRIORITY_BOARD_COLUMNS: list[dict[str, str]] = [
    {"id": "open", "label": "Open", "hint": "Son güncellenen 3"},
    {"id": "doing", "label": "Doing", "hint": "Aktif · son 3"},
    {"id": "testing", "label": "Testing", "hint": "Doğrulama · son 3"},
    {"id": "closed", "label": "Closed", "hint": "Son kapanan 3"},
]

# /boards sekmesi ile aynı proje yolları
_GITLAB_HOME_PROJECTS: list[dict[str, str]] = [
    {"path": "nokta/doviz", "product": "doviz", "source": "web", "source_label": "Web"},
    {"path": "ios/doviz", "product": "doviz", "source": "ios", "source_label": "iOS"},
    {"path": "android/doviz", "product": "doviz", "source": "android", "source_label": "Android"},
    {"path": "nokta/sinemalar", "product": "sinemalar", "source": "web", "source_label": "Web"},
]

_PRODUCT_META: dict[str, dict[str, str]] = {
    "doviz": {
        "id": "doviz",
        "label": "Döviz",
        "subtitle": "Web · iOS · Android",
        "accent": "sky",
    },
    "sinemalar": {
        "id": "sinemalar",
        "label": "Sinemalar",
        "subtitle": "Web",
        "accent": "violet",
    },
}

_TZ_IST = ZoneInfo("Europe/Istanbul")


def _issue_label_names(issue: dict[str, Any]) -> list[str]:
    raw = issue.get("labels") or []
    out: list[str] = []
    for lab in raw:
        if isinstance(lab, str):
            out.append(lab)
        elif isinstance(lab, dict) and lab.get("name"):
            out.append(str(lab["name"]))
    return out


def _has_label(issue: dict[str, Any], name: str) -> bool:
    target = name.strip().lower()
    return any(n.strip().lower() == target for n in _issue_label_names(issue))


def _parse_iso(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _issue_sort_ts(issue: dict[str, Any], *, prefer_closed: bool = False) -> float:
    if prefer_closed:
        dt = _parse_iso(issue.get("closed_at")) or _parse_iso(issue.get("updated_at"))
    else:
        dt = _parse_iso(issue.get("updated_at")) or _parse_iso(issue.get("created_at"))
    if dt is None:
        return 0.0
    return dt.timestamp()


def _fmt_issue_date(issue: dict[str, Any], *, prefer_closed: bool = False) -> str:
    if prefer_closed:
        dt = _parse_iso(issue.get("closed_at")) or _parse_iso(issue.get("updated_at"))
    else:
        dt = _parse_iso(issue.get("updated_at")) or _parse_iso(issue.get("created_at"))
    if dt is None:
        return ""
    try:
        return dt.astimezone(_TZ_IST).strftime("%d.%m.%Y")
    except Exception:
        return dt.strftime("%d.%m.%Y")


def _classify_bucket(issue: dict[str, Any]) -> str:
    """Boards UI ile uyumlu: Closed / Doing / Testing / Open (backlog)."""
    state = str(issue.get("state") or "").lower()
    if state == "closed":
        return "closed"
    if _has_label(issue, "Doing"):
        return "doing"
    if _has_label(issue, "Testing"):
        return "testing"
    return "open"


def _normalize_entry(
    issue: dict[str, Any],
    *,
    status: str,
    source_label: str,
    project_path: str,
) -> dict[str, Any]:
    prefer_closed = status == "closed"
    iid = issue.get("iid")
    title = str(issue.get("title") or "").strip() or f"Issue #{iid}"
    date_label = _fmt_issue_date(issue, prefer_closed=prefer_closed)
    return {
        "id": f"{project_path}#{iid}",
        "iid": iid,
        "status": status,
        "title": title,
        "note": date_label,
        "date_label": date_label,
        "web_url": issue.get("web_url") or "",
        "source_label": source_label,
        "project_path": project_path,
        "updated_at": issue.get("updated_at") or issue.get("closed_at") or "",
        "_sort": _issue_sort_ts(issue, prefer_closed=prefer_closed),
    }


async def _fetch_project_issues(project: dict[str, str]) -> tuple[str, list[dict[str, Any]], str | None]:
    path = project["path"]
    try:
        opened, _ = await fetch_all_issues_async(
            path,
            "opened",
            order_by="updated_at",
            sort="desc",
            max_pages=1,
        )
        closed, _ = await fetch_all_issues_async(
            path,
            "closed",
            order_by="updated_at",
            sort="desc",
            max_pages=1,
        )
        tagged: list[dict[str, Any]] = []
        for issue in opened + closed:
            row = dict(issue)
            row["_pc_source_label"] = project["source_label"]
            row["_pc_project_path"] = path
            row["_pc_product"] = project["product"]
            tagged.append(row)
        return path, tagged, None
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("git.nokta home board fetch failed for %s: %s", path, exc)
        return path, [], str(exc)


def _top_entries(
    issues: list[dict[str, Any]],
    *,
    status: str,
    limit: int,
) -> list[dict[str, Any]]:
    bucket = [i for i in issues if _classify_bucket(i) == status]
    prefer_closed = status == "closed"
    bucket.sort(key=lambda i: _issue_sort_ts(i, prefer_closed=prefer_closed), reverse=True)
    out: list[dict[str, Any]] = []
    for issue in bucket[:limit]:
        out.append(
            _normalize_entry(
                issue,
                status=status,
                source_label=str(issue.get("_pc_source_label") or "GitLab"),
                project_path=str(issue.get("_pc_project_path") or ""),
            )
        )
    return out


async def build_git_nokta_home_board(*, limit: int = 3) -> dict[str, Any]:
    """Döviz + Sinemalar için Open/Doing/Testing/Closed — her kolonda en son `limit` madde."""
    if not get_gitlab_token():
        return {
            "ok": False,
            "error": "GITLAB_PRIVATE_TOKEN tanımlı değil — git.nokta maddeleri yüklenemedi.",
            "sections": _empty_sections(),
        }

    results = await asyncio.gather(
        *[_fetch_project_issues(p) for p in _GITLAB_HOME_PROJECTS],
        return_exceptions=False,
    )
    by_product: dict[str, list[dict[str, Any]]] = {"doviz": [], "sinemalar": []}
    errors: list[str] = []
    for path, issues, err in results:
        if err:
            errors.append(f"{path}: {err}")
        for issue in issues:
            prod = str(issue.get("_pc_product") or "")
            if prod in by_product:
                by_product[prod].append(issue)

    sections: list[dict[str, Any]] = []
    for product in ("doviz", "sinemalar"):
        meta = _PRODUCT_META[product]
        issues = by_product[product]
        columns = []
        for col in PRIORITY_BOARD_COLUMNS:
            entries = _top_entries(issues, status=col["id"], limit=limit)
            columns.append(
                {
                    "id": col["id"],
                    "label": col["label"],
                    "hint": col["hint"],
                    "count": len(entries),
                    "entries": entries,
                }
            )
        sections.append(
            {
                "id": meta["id"],
                "label": meta["label"],
                "subtitle": meta["subtitle"],
                "accent": meta["accent"],
                "columns": columns,
            }
        )

    ok = any(
        sum(c["count"] for c in s["columns"]) > 0 for s in sections
    ) or not errors
    return {
        "ok": ok or not errors,
        "error": "; ".join(errors[:3]) if errors and not any(
            sum(c["count"] for c in s["columns"]) > 0 for s in sections
        ) else None,
        "sections": sections,
    }


def _empty_sections() -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for product in ("doviz", "sinemalar"):
        meta = _PRODUCT_META[product]
        columns = [
            {
                "id": col["id"],
                "label": col["label"],
                "hint": col["hint"],
                "count": 0,
                "entries": [],
            }
            for col in PRIORITY_BOARD_COLUMNS
        ]
        sections.append({**meta, "columns": columns})
    return sections


# Geriye uyumluluk — sync çağrılar boş/static yerine boş şablon döner
def get_priority_board_sections() -> list[dict[str, Any]]:
    return _empty_sections()
