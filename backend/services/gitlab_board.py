import asyncio
import logging
import os
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backend.models import GitlabBoardIssueOrder, GitlabBoardProjectSettings

LOGGER = logging.getLogger(__name__)

GITLAB_URL = "https://git.nokta.com/api/v4"

BOARD_SORT_MODES: dict[str, dict[str, str]] = {
    "manual": {"label": "Manuel (sürükle-bırak)", "gitlab_order_by": "relative_position"},
    "relative_position": {"label": "GitLab board sırası", "gitlab_order_by": "relative_position"},
    "updated_at_desc": {"label": "Son güncelleme (yeni üstte)", "gitlab_order_by": "updated_at"},
    "updated_at_asc": {"label": "Son güncelleme (eski üstte)", "gitlab_order_by": "updated_at"},
    "created_at_desc": {"label": "Oluşturulma (yeni üstte)", "gitlab_order_by": "created_at"},
    "created_at_asc": {"label": "Oluşturulma (eski üstte)", "gitlab_order_by": "created_at"},
    "weight_desc": {"label": "Ağırlık / öncelik (yüksek üstte)", "gitlab_order_by": "weight"},
}

DEFAULT_BOARD_SORT_MODE = "manual"


def get_gitlab_token() -> str:
    return os.environ.get("GITLAB_PRIVATE_TOKEN") or ""


def _headers() -> dict[str, str]:
    return {"PRIVATE-TOKEN": get_gitlab_token()}


def _encoded_path(project_path: str) -> str:
    return project_path.replace("/", "%2F")


async def fetch_project_board_async(project_path: str) -> dict[str, Any]:
    """GitLab projesinin ilk board'unu ve issue'larını eşzamanlı (async) çeker."""
    token = get_gitlab_token()
    if not token:
        return {"error": "Token bulunamadı. Lütfen .env dosyanıza GITLAB_PRIVATE_TOKEN ekleyin."}

    headers = _headers()
    encoded_path = _encoded_path(project_path)
    boards_url = f"{GITLAB_URL}/projects/{encoded_path}/boards"
    issues_url = f"{GITLAB_URL}/projects/{encoded_path}/issues?state=opened&per_page=100"

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            boards_task = client.get(boards_url, headers=headers)
            issues_task = client.get(issues_url, headers=headers)

            resp_boards, resp_issues = await asyncio.gather(boards_task, issues_task)

            if resp_boards.status_code != 200:
                return {"error": f"Board alınamadı: {resp_boards.status_code} - {resp_boards.text}"}

            boards = resp_boards.json()
            if not boards:
                return {"error": "Projede aktif board bulunamadı"}

            main_board = boards[0]
            issues = resp_issues.json() if resp_issues.status_code == 200 else []

            return {
                "board": main_board,
                "issues": issues,
                "project_path": project_path,
            }
        except Exception as e:
            return {"error": str(e)}


def get_board_column_orders(db: Session, project_path: str) -> dict[str, list[int]]:
    """Proje için kayıtlı sütun sıralarını {list_key: [iid, ...]} olarak döner."""
    rows = (
        db.query(GitlabBoardIssueOrder)
        .filter(GitlabBoardIssueOrder.project_path == project_path)
        .order_by(GitlabBoardIssueOrder.list_key.asc(), GitlabBoardIssueOrder.sort_index.asc())
        .all()
    )
    out: dict[str, list[int]] = {}
    for row in rows:
        out.setdefault(row.list_key, []).append(int(row.issue_iid))
    return out


def save_board_column_order(
    db: Session,
    project_path: str,
    list_key: str,
    issue_iids: list[int],
) -> None:
    """Bir sütunun tam sırasını kaydeder (üstten alta)."""
    project_path = (project_path or "").strip()
    list_key = (list_key or "").strip()
    if not project_path or not list_key:
        return

    seen: set[int] = set()
    ordered: list[int] = []
    for raw in issue_iids:
        try:
            iid = int(raw)
        except (TypeError, ValueError):
            continue
        if iid in seen:
            continue
        seen.add(iid)
        ordered.append(iid)

    db.query(GitlabBoardIssueOrder).filter(
        GitlabBoardIssueOrder.project_path == project_path,
        GitlabBoardIssueOrder.list_key == list_key,
    ).delete(synchronize_session=False)

    now = datetime.utcnow()
    for idx, iid in enumerate(ordered):
        db.add(
            GitlabBoardIssueOrder(
                project_path=project_path,
                list_key=list_key,
                issue_iid=iid,
                sort_index=idx,
                updated_at=now,
            )
        )
    db.commit()


def normalize_board_sort_mode(sort_mode: str | None) -> str:
    mode = (sort_mode or "").strip()
    if mode in BOARD_SORT_MODES:
        return mode
    return DEFAULT_BOARD_SORT_MODE


def get_board_project_settings(db: Session, project_path: str) -> dict[str, Any]:
    project_path = (project_path or "").strip()
    row = (
        db.query(GitlabBoardProjectSettings)
        .filter(GitlabBoardProjectSettings.project_path == project_path)
        .first()
    )
    mode = normalize_board_sort_mode(row.sort_mode if row else DEFAULT_BOARD_SORT_MODE)
    return {
        "project_path": project_path,
        "sort_mode": mode,
        "sort_modes": [
            {"id": key, "label": meta["label"]}
            for key, meta in BOARD_SORT_MODES.items()
        ],
    }


def save_board_project_settings(db: Session, project_path: str, sort_mode: str) -> dict[str, Any]:
    project_path = (project_path or "").strip()
    mode = normalize_board_sort_mode(sort_mode)
    if not project_path:
        return {"project_path": "", "sort_mode": mode}
    row = (
        db.query(GitlabBoardProjectSettings)
        .filter(GitlabBoardProjectSettings.project_path == project_path)
        .first()
    )
    now = datetime.utcnow()
    if row:
        row.sort_mode = mode
        row.updated_at = now
    else:
        db.add(
            GitlabBoardProjectSettings(
                project_path=project_path,
                sort_mode=mode,
                updated_at=now,
            )
        )
    db.commit()
    return {"project_path": project_path, "sort_mode": mode}


async def sync_open_issues_order_to_gitlab(
    project_path: str,
    ordered: list[dict[str, Any]],
) -> dict[str, Any]:
    """Açık issue listesini GitLab relative_position ile hizalar (soldan sağa, üstten alta)."""
    if not ordered:
        return {"ok": True, "synced": 0, "failed": 0, "total": 0, "skipped": 0}

    synced = 0
    failed = 0
    skipped = 0
    prev_id: int | None = None

    async with httpx.AsyncClient(timeout=20.0) as client:
        token = get_gitlab_token()
        if not token:
            return {"ok": False, "synced": 0, "failed": len(ordered), "total": len(ordered), "skipped": 0}
        headers = _headers()
        encoded_path = _encoded_path(project_path)

        for item in ordered:
            try:
                iid = int(item.get("iid"))
                global_id = int(item.get("id"))
            except (TypeError, ValueError):
                skipped += 1
                continue
            if prev_id is None:
                prev_id = global_id
                continue

            url = f"{GITLAB_URL}/projects/{encoded_path}/issues/{iid}/reorder"
            try:
                resp = await client.put(
                    url,
                    headers=headers,
                    params={"move_after_id": prev_id},
                )
                if resp.status_code in (200, 201, 204):
                    synced += 1
                else:
                    failed += 1
                    LOGGER.warning(
                        "GitLab sync-sort failed [%s#%s] %s: %s",
                        project_path,
                        iid,
                        resp.status_code,
                        resp.text[:200],
                    )
            except Exception as exc:
                failed += 1
                LOGGER.warning("GitLab sync-sort error [%s#%s]: %s", project_path, iid, exc)
            prev_id = global_id

    return {
        "ok": failed == 0,
        "synced": synced,
        "failed": failed,
        "skipped": skipped,
        "total": len(ordered),
    }


async def reorder_issue_async(
    project_path: str,
    issue_iid: int,
    *,
    move_after_id: int | None = None,
    move_before_id: int | None = None,
) -> dict[str, Any] | None:
    """GitLab issue reorder API — relative_position kalıcılığı."""
    token = get_gitlab_token()
    if not token:
        return None

    headers = _headers()
    encoded_path = _encoded_path(project_path)
    params: dict[str, int] = {}
    if move_after_id is not None:
        params["move_after_id"] = int(move_after_id)
    if move_before_id is not None:
        params["move_before_id"] = int(move_before_id)

    url = f"{GITLAB_URL}/projects/{encoded_path}/issues/{issue_iid}/reorder"
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.put(url, headers=headers, params=params or None)
            if resp.status_code not in (200, 201, 204):
                LOGGER.warning(
                    "GitLab reorder failed [%s#%s] %s: %s",
                    project_path,
                    issue_iid,
                    resp.status_code,
                    resp.text[:200],
                )
                return None
            if resp.text and resp.text.strip():
                data = resp.json()
                if isinstance(data, dict):
                    return data
            fetch_url = f"{GITLAB_URL}/projects/{encoded_path}/issues/{issue_iid}"
            ref = await client.get(fetch_url, headers=headers)
            if ref.status_code == 200:
                return ref.json()
            return None
        except Exception as exc:
            LOGGER.exception("GitLab reorder error [%s#%s]: %s", project_path, issue_iid, exc)
            return None


async def update_issue_async(
    project_path: str,
    issue_iid: int,
    *,
    add_labels: list[str] | None = None,
    remove_labels: list[str] | None = None,
    state_event: str | None = None,
) -> dict[str, Any] | None:
    """Issue etiket / durum güncellemesi."""
    token = get_gitlab_token()
    if not token:
        return None

    headers = _headers()
    encoded_path = _encoded_path(project_path)
    url = f"{GITLAB_URL}/projects/{encoded_path}/issues/{issue_iid}"

    data: dict[str, Any] = {}
    if add_labels:
        data["add_labels"] = ",".join(add_labels)
    if remove_labels:
        data["remove_labels"] = ",".join(remove_labels)
    if state_event in ("close", "reopen"):
        data["state_event"] = state_event

    if not data:
        return None

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.put(url, headers=headers, json=data)
            if resp.status_code != 200:
                LOGGER.warning(
                    "GitLab issue update failed [%s#%s] %s: %s",
                    project_path,
                    issue_iid,
                    resp.status_code,
                    resp.text[:200],
                )
                return None
            return resp.json()
        except Exception as exc:
            LOGGER.exception("GitLab issue update error [%s#%s]: %s", project_path, issue_iid, exc)
            return None


async def move_issue_async(
    project_path: str,
    issue_iid: int,
    add_labels: list[str],
    remove_labels: list[str],
) -> bool:
    """Issue'nun etiketlerini güncelleyerek board üzerinde sütun değiştirir."""
    updated = await update_issue_async(
        project_path,
        issue_iid,
        add_labels=add_labels,
        remove_labels=remove_labels,
    )
    return updated is not None
