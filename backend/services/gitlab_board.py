import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
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


def _issues_updated_after_iso() -> str:
    one_year = datetime.now(timezone.utc) - timedelta(days=365)
    return one_year.strftime("%Y-%m-%dT%H:%M:%SZ")


def _gitlab_connect_error_message(exc: Exception) -> str:
    msg = str(exc).strip() or exc.__class__.__name__
    low = msg.lower()
    if "connect" in low or "timeout" in low or "name or service" in low:
        return (
            "GitLab'e (git.nokta.com) sunucudan ulaşılamadı. "
            "Railway ortamında GITLAB_PRIVATE_TOKEN tanımlı olmalı; "
            "şirket ağı/VPN gerekiyorsa sunucunun da erişebildiğinden emin olun."
        )
    return f"GitLab isteği başarısız: {msg}"


async def fetch_gitlab_version_async() -> dict[str, Any]:
    token = get_gitlab_token()
    if not token:
        return {"ok": False, "error": "GITLAB_PRIVATE_TOKEN tanımlı değil."}
    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            resp = await client.get(f"{GITLAB_URL}/version", headers=_headers())
            if resp.status_code in (200, 401):
                return {"ok": True, "status": resp.status_code}
            return {"ok": False, "error": f"GitLab version HTTP {resp.status_code}"}
        except Exception as exc:
            return {"ok": False, "error": _gitlab_connect_error_message(exc)}


async def fetch_all_issues_async(
    project_path: str,
    state: str,
    *,
    order_by: str = "updated_at",
    sort: str = "desc",
    updated_after: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """GitLab issue listesi — sayfalı."""
    token = get_gitlab_token()
    if not token:
        raise ValueError("GITLAB_PRIVATE_TOKEN tanımlı değil.")

    encoded_path = _encoded_path(project_path)
    updated_after = updated_after or _issues_updated_after_iso()
    headers = _headers()
    all_issues: list[dict[str, Any]] = []
    page = 1

    async def _paginate(c: httpx.AsyncClient) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        p = 1
        while True:
            url = (
                f"{GITLAB_URL}/projects/{encoded_path}/issues"
                f"?state={state}&updated_after={updated_after}"
                f"&order_by={order_by}&sort={sort}&per_page=100&page={p}"
            )
            resp = await c.get(url, headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"Issues ({state}) HTTP {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Issues ({state}) beklenmeyen yanıt")
            out.extend(data)
            if len(data) < 100:
                break
            p += 1
        return out

    if client is not None:
        return await _paginate(client)

    async with httpx.AsyncClient(timeout=60.0) as c:
        return await _paginate(c)


async def fetch_board_project_bundle_async(
    project_path: str,
    *,
    opened_order_by: str = "relative_position",
    opened_sort: str = "asc",
) -> dict[str, Any]:
    """Boards UI: board + açık/kapalı issue listeleri (sunucu proxy)."""
    token = get_gitlab_token()
    if not token:
        return {"error": "GITLAB_PRIVATE_TOKEN tanımlı değil. Railway / .env kontrol edin."}

    encoded_path = _encoded_path(project_path)
    boards_url = f"{GITLAB_URL}/projects/{encoded_path}/boards"
    headers = _headers()

    async with httpx.AsyncClient(timeout=90.0) as client:
        try:
            resp_boards = await client.get(boards_url, headers=headers)
            if resp_boards.status_code != 200:
                return {
                    "error": f"Board alınamadı: HTTP {resp_boards.status_code}",
                    "detail": resp_boards.text[:300],
                }
            boards = resp_boards.json()
            if not boards:
                return {"error": "Projede aktif board bulunamadı."}

            opened_task = fetch_all_issues_async(
                project_path,
                "opened",
                order_by=opened_order_by,
                sort=opened_sort,
                client=client,
            )
            closed_task = fetch_all_issues_async(
                project_path,
                "closed",
                order_by="updated_at",
                sort="desc",
                client=client,
            )
            opened_issues, closed_issues = await asyncio.gather(opened_task, closed_task)

            return {
                "board": boards[0],
                "opened_issues": opened_issues,
                "closed_issues": closed_issues,
                "project_path": project_path,
            }
        except Exception as exc:
            LOGGER.exception("GitLab board bundle failed [%s]: %s", project_path, exc)
            return {"error": _gitlab_connect_error_message(exc)}


async def create_issue_async(
    project_path: str,
    title: str,
    *,
    labels: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    token = get_gitlab_token()
    if not token:
        return None, "GITLAB_PRIVATE_TOKEN tanımlı değil."

    encoded_path = _encoded_path(project_path)
    url = f"{GITLAB_URL}/projects/{encoded_path}/issues"
    payload: dict[str, Any] = {"title": (title or "").strip()}
    if not payload["title"]:
        return None, "Başlık boş"
    if labels:
        payload["labels"] = labels

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.post(url, headers=_headers(), json=payload)
            if resp.status_code not in (200, 201):
                detail = resp.text[:300]
                try:
                    body = resp.json()
                    if isinstance(body, dict):
                        detail = str(body.get("message") or body.get("error") or detail)
                except Exception:
                    pass
                return None, detail or f"GitLab HTTP {resp.status_code}"
            return resp.json(), None
        except Exception as exc:
            LOGGER.exception("GitLab create issue [%s]: %s", project_path, exc)
            return None, _gitlab_connect_error_message(exc)


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


async def sync_column_order_to_gitlab(
    project_path: str,
    target_ordered: list[dict[str, Any]],
    *,
    current_ordered: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    """Tek board sütunundaki issue sırasını GitLab relative_position ile hizalar."""
    if len(target_ordered) <= 1:
        return {"synced": 0, "failed": 0, "skipped": len(target_ordered)}

    current = current_ordered or []
    target_iids = [int(x.get("iid")) for x in target_ordered if x.get("iid") is not None]
    current_iids = [int(x.get("iid")) for x in current if x.get("iid") is not None]
    start_at = 0
    while start_at < len(target_iids) and start_at < len(current_iids) and target_iids[start_at] == current_iids[start_at]:
        start_at += 1
    if start_at >= len(target_ordered):
        return {"synced": 0, "failed": 0, "skipped": len(target_ordered)}

    synced = 0
    failed = 0
    for i in range(start_at, len(target_ordered)):
        item = target_ordered[i]
        try:
            iid = int(item.get("iid"))
        except (TypeError, ValueError):
            continue
        move_after_id: int | None = None
        move_before_id: int | None = None
        if i == 0:
            if current and current[0].get("id") is not None:
                try:
                    move_before_id = int(current[0]["id"])
                except (TypeError, ValueError):
                    pass
        else:
            prev = target_ordered[i - 1]
            if prev.get("id") is not None:
                try:
                    move_after_id = int(prev["id"])
                except (TypeError, ValueError):
                    pass
        if move_after_id is None and move_before_id is None:
            continue
        updated = await reorder_issue_async(
            project_path,
            iid,
            move_after_id=move_after_id,
            move_before_id=move_before_id,
        )
        if updated is not None:
            synced += 1
        else:
            failed += 1
    return {"synced": synced, "failed": failed, "skipped": start_at}


async def sync_columns_order_to_gitlab(
    project_path: str,
    columns: list[dict[str, Any]],
) -> dict[str, Any]:
    """Board sütunlarını ayrı ayrı GitLab sırasına yazar."""
    total_synced = 0
    total_failed = 0
    column_results: list[dict[str, Any]] = []
    for col in columns or []:
        target = col.get("ordered") or []
        current = col.get("current") or []
        list_key = str(col.get("list_key") or "")
        result = await sync_column_order_to_gitlab(
            project_path,
            target,
            current_ordered=current,
        )
        total_synced += int(result.get("synced") or 0)
        total_failed += int(result.get("failed") or 0)
        column_results.append({"list_key": list_key, **result})
    return {
        "ok": total_failed == 0,
        "synced": total_synced,
        "failed": total_failed,
        "columns": column_results,
    }


async def sync_open_issues_order_to_gitlab(
    project_path: str,
    ordered: list[dict[str, Any]],
) -> dict[str, Any]:
    """Geriye dönük: tek düz liste — tüm sütunları tek kolon gibi senkronlar."""
    result = await sync_column_order_to_gitlab(project_path, ordered)
    return {
        "ok": result.get("failed", 0) == 0,
        "synced": result.get("synced", 0),
        "failed": result.get("failed", 0),
        "skipped": result.get("skipped", 0),
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


def normalize_board_move_labels(
    *,
    from_label: str,
    to_label: str,
    remove_labels: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    skip = {"", "null", "__open__", "__closed__"}
    add = [to_label] if to_label and to_label not in skip else []
    if remove_labels:
        remove = [str(x).strip() for x in remove_labels if str(x).strip() and str(x).strip() not in skip]
        remove = [x for x in remove if x not in add]
    else:
        remove = [from_label] if from_label and from_label not in skip else []
    return add, remove


async def update_issue_async(
    project_path: str,
    issue_iid: int,
    *,
    add_labels: list[str] | None = None,
    remove_labels: list[str] | None = None,
    state_event: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Issue etiket / durum güncellemesi. (issue, hata_mesajı) döner."""
    token = get_gitlab_token()
    if not token:
        return None, "GITLAB_PRIVATE_TOKEN tanımlı değil"

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
        return None, "Güncellenecek alan yok"

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.put(url, headers=headers, data=data)
            if resp.status_code != 200:
                detail = resp.text[:300]
                try:
                    payload = resp.json()
                    if isinstance(payload, dict):
                        detail = str(payload.get("message") or payload.get("error") or detail)
                except Exception:
                    pass
                LOGGER.warning(
                    "GitLab issue update failed [%s#%s] %s: %s",
                    project_path,
                    issue_iid,
                    resp.status_code,
                    detail,
                )
                return None, detail or f"GitLab HTTP {resp.status_code}"
            return resp.json(), None
        except Exception as exc:
            LOGGER.exception("GitLab issue update error [%s#%s]: %s", project_path, issue_iid, exc)
            return None, str(exc)


async def move_issue_async(
    project_path: str,
    issue_iid: int,
    add_labels: list[str],
    remove_labels: list[str],
) -> bool:
    """Issue'nun etiketlerini güncelleyerek board üzerinde sütun değiştirir."""
    updated, _err = await update_issue_async(
        project_path,
        issue_iid,
        add_labels=add_labels,
        remove_labels=remove_labels,
    )
    return updated is not None
