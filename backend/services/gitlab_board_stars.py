"""GitLab board yıldızları — ana sayfa git.nokta chip'leri."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import GitlabBoardStar

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
        "board_list": row.board_list or classify_board_list(row.state, labels),
        "starred_at": row.starred_at.isoformat() if row.starred_at else None,
        "source_label": (PROJECT_CHIP_MAP.get(row.project_path) or {}).get("source_label")
        or row.platform.upper(),
    }


def list_stars(
    db: Session,
    *,
    product: str | None = None,
    platform: str | None = None,
    project_path: str | None = None,
) -> list[dict[str, Any]]:
    q = db.query(GitlabBoardStar)
    if product:
        q = q.filter(GitlabBoardStar.product == product)
    if platform:
        q = q.filter(GitlabBoardStar.platform == platform)
    if project_path:
        q = q.filter(GitlabBoardStar.project_path == project_path)
    rows = q.order_by(GitlabBoardStar.starred_at.desc()).all()
    return [star_to_dict(r) for r in rows]


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
