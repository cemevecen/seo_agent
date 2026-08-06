"""Ana sayfa Google Drive klasör işlemleri (liste / yükle / sil / içerik)."""

from __future__ import annotations

import io
import logging
import mimetypes
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from sqlalchemy.orm import Session

from backend.services import home_drive_auth

LOGGER = logging.getLogger(__name__)

_IMAGE_MIMES = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/heic",
        "image/heif",
        "image/bmp",
    }
)
_MAX_UPLOAD_BYTES = 12 * 1024 * 1024  # 12 MB


def _drive_service(db: Session):
    creds = home_drive_auth.ensure_fresh_credentials(db)
    if creds is None:
        raise RuntimeError("Google Drive bağlı değil. Önce hesabı bağlayın.")
    return build("drive", "v3", credentials=creds, cache_discovery=False), creds


def list_folder_images(db: Session, *, limit: int = 60) -> list[dict[str, Any]]:
    folder_id = home_drive_auth.home_drive_folder_id()
    if not folder_id:
        raise RuntimeError("HOME_DRIVE_FOLDER_ID tanımlı değil.")
    service, _ = _drive_service(db)
    q = (
        f"'{folder_id}' in parents and trashed=false "
        "and (mimeType contains 'image/' or mimeType = 'application/octet-stream')"
    )
    resp = (
        service.files()
        .list(
            q=q,
            spaces="drive",
            fields="files(id,name,mimeType,createdTime,modifiedTime,size,webViewLink,thumbnailLink)",
            orderBy="createdTime desc",
            pageSize=max(1, min(int(limit or 60), 100)),
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    out: list[dict[str, Any]] = []
    for f in resp.get("files") or []:
        mime = str(f.get("mimeType") or "")
        if mime and not mime.startswith("image/") and mime != "application/octet-stream":
            continue
        fid = str(f.get("id") or "").strip()
        if not fid:
            continue
        out.append(
            {
                "id": fid,
                "name": str(f.get("name") or "image"),
                "mime_type": mime or "image/jpeg",
                "created_time": str(f.get("createdTime") or ""),
                "size": int(f.get("size") or 0),
                "web_view_link": str(f.get("webViewLink") or ""),
                "thumb_url": f"/api/home/drive/files/{fid}/content",
            }
        )
    return out


def upload_image(
    db: Session,
    *,
    filename: str,
    content: bytes,
    content_type: str | None = None,
) -> dict[str, Any]:
    folder_id = home_drive_auth.home_drive_folder_id()
    if not folder_id:
        raise RuntimeError("HOME_DRIVE_FOLDER_ID tanımlı değil.")
    if not content:
        raise ValueError("Boş dosya yüklenemez.")
    if len(content) > _MAX_UPLOAD_BYTES:
        raise ValueError("Dosya 12 MB sınırını aşıyor.")
    mime = (content_type or "").split(";")[0].strip().lower()
    if not mime or mime == "application/octet-stream":
        guessed, _ = mimetypes.guess_type(filename or "")
        mime = (guessed or "image/jpeg").lower()
    if mime not in _IMAGE_MIMES and not mime.startswith("image/"):
        raise ValueError("Yalnızca görsel dosyaları yüklenebilir.")
    safe_name = (filename or "image").strip() or "image"
    if len(safe_name) > 180:
        safe_name = safe_name[:180]

    service, _ = _drive_service(db)
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime, resumable=False)
    created = (
        service.files()
        .create(
            body={"name": safe_name, "parents": [folder_id]},
            media_body=media,
            fields="id,name,mimeType,createdTime,size,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    fid = str(created.get("id") or "").strip()
    return {
        "id": fid,
        "name": str(created.get("name") or safe_name),
        "mime_type": str(created.get("mimeType") or mime),
        "created_time": str(created.get("createdTime") or ""),
        "size": int(created.get("size") or len(content)),
        "web_view_link": str(created.get("webViewLink") or ""),
        "thumb_url": f"/api/home/drive/files/{fid}/content" if fid else "",
    }


def delete_file(db: Session, file_id: str) -> None:
    """Hedef klasördeki dosyayı sil; kalıcı silme yetkisi yoksa çöpe at."""
    fid = (file_id or "").strip()
    if not fid:
        raise ValueError("Dosya id eksik.")
    folder_id = home_drive_auth.home_drive_folder_id()
    service, _ = _drive_service(db)
    meta = (
        service.files()
        .get(fileId=fid, fields="id,parents,trashed", supportsAllDrives=True)
        .execute()
    )
    if meta.get("trashed"):
        return
    parents = list(meta.get("parents") or [])
    if folder_id and folder_id not in parents:
        raise PermissionError("Dosya hedef Drive klasöründe değil.")
    try:
        service.files().delete(fileId=fid, supportsAllDrives=True).execute()
        return
    except Exception as exc:  # noqa: BLE001
        LOGGER.info("Drive permanent delete failed for %s (%s); trying trash", fid, exc)
    service.files().update(
        fileId=fid,
        body={"trashed": True},
        supportsAllDrives=True,
    ).execute()


def delete_files(db: Session, file_ids: list[str]) -> dict[str, Any]:
    """Birden fazla dosyayı sil. Dönüş: deleted / failed / errors."""
    seen: set[str] = set()
    deleted: list[str] = []
    failed: list[dict[str, str]] = []
    for raw in file_ids or []:
        fid = (raw or "").strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        try:
            delete_file(db, fid)
            deleted.append(fid)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Drive delete failed for %s: %s", fid, exc)
            failed.append({"id": fid, "error": str(exc)[:160]})
    return {
        "deleted": deleted,
        "failed": failed,
        "deleted_count": len(deleted),
        "failed_count": len(failed),
    }


def download_file_bytes(db: Session, file_id: str) -> tuple[bytes, str, str]:
    fid = (file_id or "").strip()
    if not fid:
        raise ValueError("Dosya id eksik.")
    folder_id = home_drive_auth.home_drive_folder_id()
    service, _ = _drive_service(db)
    meta = (
        service.files()
        .get(
            fileId=fid,
            fields="id,name,mimeType,parents,trashed",
            supportsAllDrives=True,
        )
        .execute()
    )
    if meta.get("trashed"):
        raise FileNotFoundError("Dosya çöp kutusunda.")
    parents = list(meta.get("parents") or [])
    if folder_id and folder_id not in parents:
        raise PermissionError("Dosya hedef Drive klasöründe değil.")
    request = service.files().get_media(fileId=fid, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    mime = str(meta.get("mimeType") or "application/octet-stream")
    name = str(meta.get("name") or "image")
    return buf.getvalue(), mime, name
