"""GitLab board yıldızları — ana sayfa git.nokta chip'leri."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import GitlabBoardStar
from backend.services.gitlab_board import (
    fetch_issues_by_iids,
    get_board_column_orders,
    save_board_column_order,
)

LOGGER = logging.getLogger(__name__)

# boards sekmeleri → ana sayfa chip
PROJECT_CHIP_MAP: dict[str, dict[str, str]] = {
    "nokta/doviz": {"product": "doviz", "platform": "web", "source_label": "Web"},
    "ios/doviz": {"product": "doviz", "platform": "ios", "source_label": "iOS"},
    "android/doviz": {"product": "doviz", "platform": "android", "source_label": "Android"},
    "nokta/sinemalar": {"product": "sinemalar", "platform": "web", "source_label": "Web"},
}

HOME_CHIPS: dict[str, list[dict[str, str]]] = {
    "doviz": [
        {"id": "web", "label": "Web"},
        {"id": "ios", "label": "iOS"},
        {"id": "android", "label": "Android"},
    ],
    "sinemalar": [
        {"id": "web", "label": "Web"},
    ],
}

HOME_ORDER_PRODUCTS: tuple[str, ...] = ("doviz", "sinemalar")


def home_order_project_key(product: str, platform: str) -> str:
    return f"home_git_nokta::{(product or '').strip().lower()}::{(platform or '').strip().lower()}"


def resolve_chip_for_project(project_path: str) -> dict[str, str] | None:
    return PROJECT_CHIP_MAP.get((project_path or "").strip())


def _label_names(labels: Any) -> list[str]:
    if isinstance(labels, str):
        try:
            labels = json.loads(labels)
        except Exception:
            return []
    if not isinstance(labels, list):
        return []
    out: list[str] = []
    for lab in labels:
        if isinstance(lab, str):
            out.append(lab)
        elif isinstance(lab, dict) and lab.get("name"):
            out.append(str(lab["name"]))
    return out


def classify_board_list(state: str, labels: Any) -> str:
    st = (state or "").lower()
    if st == "closed":
        return "closed"
    names = {n.strip().lower() for n in _label_names(labels)}
    if "doing" in names:
        return "doing"
    if "testing" in names:
        return "testing"
    return "open"


def star_to_dict(row: GitlabBoardStar) -> dict[str, Any]:
    labels = _label_names(row.labels_json)
    # Her zaman state+label'dan türet — stale board_list (Doing/Testing) Closed'ı ezmesin
    board_list = classify_board_list(row.state, labels)
    return {
        "id": row.id,
        "project_path": row.project_path,
        "issue_iid": row.issue_iid,
        "product": row.product,
        "platform": row.platform,
        "title": row.title,
        "web_url": row.web_url,
        "state": row.state,
        "labels": labels,
        "board_list": board_list,
        "starred_at": row.starred_at.isoformat() if row.starred_at else None,
        "source_label": (PROJECT_CHIP_MAP.get(row.project_path) or {}).get("source_label")
        or row.platform.upper(),
    }


def refresh_stars_from_gitlab(
    db: Session,
    *,
    product: str | None = None,
    platform: str | None = None,
    project_path: str | None = None,
    per_project_timeout_sec: float = 4.0,
) -> dict[str, Any]:
    """Yıldız satırlarını GitLab'daki güncel state/label ile senkronize et.

    Ana sayfa board_list'i yıldız anındaki snapshot'tı; Closed'a taşınan maddeler
    Doing/Testing'te kalıyordu. Kısa timeout — GitLab erişilemezse asılı kalmaz.
    """
    q = db.query(GitlabBoardStar)
    if product:
        q = q.filter(GitlabBoardStar.product == product)
    if platform:
        q = q.filter(GitlabBoardStar.platform == platform)
    if project_path:
        q = q.filter(GitlabBoardStar.project_path == project_path)
    rows = q.all()
    if not rows:
        return {"ok": True, "updated": 0, "checked": 0, "errors": []}

    by_project: dict[str, list[GitlabBoardStar]] = {}
    for row in rows:
        by_project.setdefault(row.project_path, []).append(row)

    updated = 0
    errors: list[str] = []
    for path, project_rows in by_project.items():
        iids = [int(r.issue_iid) for r in project_rows]
        try:
            issues = fetch_issues_by_iids(
                path, iids, timeout_sec=per_project_timeout_sec
            )
        except Exception as exc:
            msg = f"{path}: {exc}"
            LOGGER.warning("Star refresh failed: %s", msg)
            errors.append(msg)
            continue
        missing: list[int] = []
        for row in project_rows:
            issue = issues.get(int(row.issue_iid))
            if not issue:
                missing.append(int(row.issue_iid))
                continue
            state = str(issue.get("state") or row.state or "opened")
            labels = _label_names(issue.get("labels"))
            board_list = classify_board_list(state, labels)
            title = str(issue.get("title") or row.title or "")[:512]
            web_url = str(issue.get("web_url") or row.web_url or "")[:1024]
            old_labels = _label_names(row.labels_json)
            if (
                (row.state or "") == state[:16]
                and (row.board_list or "") == board_list
                and (row.title or "") == title
                and old_labels == labels
                and (row.web_url or "") == web_url
            ):
                continue
            row.state = state[:16]
            row.labels_json = json.dumps(labels, ensure_ascii=False)
            row.board_list = board_list
            row.title = title
            row.web_url = web_url
            updated += 1
        if missing:
            sample = ",".join(str(x) for x in missing[:8])
            more = f"+{len(missing) - 8}" if len(missing) > 8 else ""
            msg = f"{path}: {len(missing)} issue GitLab'da bulunamadı (#{sample}{more})"
            LOGGER.warning("Star refresh incomplete: %s", msg)
            errors.append(msg)
    if updated:
        db.commit()
    return {"ok": True, "updated": updated, "checked": len(rows), "errors": errors}


def list_stars(
    db: Session,
    *,
    product: str | None = None,
    platform: str | None = None,
    project_path: str | None = None,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    if refresh:
        refresh_stars_from_gitlab(
            db, product=product, platform=platform, project_path=project_path
        )
    q = db.query(GitlabBoardStar)
    if product:
        q = q.filter(GitlabBoardStar.product == product)
    if platform:
        q = q.filter(GitlabBoardStar.platform == platform)
    if project_path:
        q = q.filter(GitlabBoardStar.project_path == project_path)
    rows = q.order_by(GitlabBoardStar.starred_at.desc()).all()
    return [star_to_dict(r) for r in rows]


def list_home_star_orders(db: Session) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for product, chips in HOME_CHIPS.items():
        for chip in chips:
            project_key = home_order_project_key(product, chip["id"])
            orders = get_board_column_orders(db, project_key)
            for list_key, iids in orders.items():
                out[f"{product}:{chip['id']}:{list_key}"] = iids
    return out


def save_home_star_order(
    db: Session,
    *,
    product: str,
    platform: str,
    board_list: str,
    issue_iids: list[int],
) -> None:
    save_board_column_order(
        db,
        home_order_project_key(product, platform),
        (board_list or "").strip().lower(),
        issue_iids,
    )


def list_starred_iids(db: Session, project_path: str) -> list[int]:
    rows = (
        db.query(GitlabBoardStar.issue_iid)
        .filter(GitlabBoardStar.project_path == project_path)
        .all()
    )
    return [int(r[0]) for r in rows]


def upsert_star(
    db: Session,
    *,
    project_path: str,
    issue_iid: int,
    title: str = "",
    web_url: str = "",
    state: str = "opened",
    labels: list[str] | None = None,
    platform_override: str | None = None,
    bump_starred_at: bool = True,
) -> dict[str, Any]:
    meta = resolve_chip_for_project(project_path)
    if not meta:
        raise ValueError(f"Bilinmeyen board projesi: {project_path}")
    platform = (platform_override or meta["platform"]).strip().lower()
    product = meta["product"]
    labels = labels or []
    board_list = classify_board_list(state, labels)
    row = (
        db.query(GitlabBoardStar)
        .filter(
            GitlabBoardStar.project_path == project_path,
            GitlabBoardStar.issue_iid == int(issue_iid),
        )
        .first()
    )
    if row is None:
        row = GitlabBoardStar(
            project_path=project_path,
            issue_iid=int(issue_iid),
            product=product,
            platform=platform,
            title=(title or "")[:512],
            web_url=(web_url or "")[:1024],
            state=(state or "opened")[:16],
            labels_json=json.dumps(labels, ensure_ascii=False),
            board_list=board_list,
            starred_at=datetime.utcnow(),
        )
        db.add(row)
    else:
        row.product = product
        row.platform = platform
        row.title = (title or row.title or "")[:512]
        row.web_url = (web_url or row.web_url or "")[:1024]
        row.state = (state or row.state or "opened")[:16]
        row.labels_json = json.dumps(labels, ensure_ascii=False)
        row.board_list = board_list
        if bump_starred_at or row.starred_at is None:
            row.starred_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return star_to_dict(row)


def remove_star(db: Session, *, project_path: str, issue_iid: int) -> bool:
    row = (
        db.query(GitlabBoardStar)
        .filter(
            GitlabBoardStar.project_path == project_path,
            GitlabBoardStar.issue_iid == int(issue_iid),
        )
        .first()
    )
    if not row:
        return False
    db.delete(row)
    db.commit()
    return True
