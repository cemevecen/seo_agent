"""Ana sayfa Google Drive klasör işlemleri (liste / yükle / sil / içerik)."""

from __future__ import annotations

import io
import json
import logging
import mimetypes
from typing import Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from sqlalchemy.orm import Session

from backend.services import home_drive_auth

LOGGER = logging.getLogger(__name__)

# Ana sayfa container anahtarları — yükleme sırasında önerilir / badge bağlanır
HOME_DRIVE_CONTAINERS: tuple[dict[str, str], ...] = (
    {"key": "realtime", "label": "active users"},
    {"key": "ga4-doviz", "label": "doviz · ga4"},
    {"key": "ga4-sinemalar", "label": "sinemalar · ga4"},
    {"key": "sc-doviz", "label": "doviz · search console"},
    {"key": "sc-sinemalar", "label": "sinemalar · search console"},
    {"key": "position-doviz", "label": "doviz · position drops"},
    {"key": "position-sinemalar", "label": "sinemalar · position drops"},
    {"key": "notification-week", "label": "Notification · 7g"},
    {"key": "crashlytics", "label": "Mobil mağaza / Firebase"},
    {"key": "priority-board", "label": "git.nokta"},
)
_HOME_DRIVE_CONTAINER_BY_KEY = {c["key"]: c["label"] for c in HOME_DRIVE_CONTAINERS}


def list_home_drive_containers() -> list[dict[str, str]]:
    return [dict(c) for c in HOME_DRIVE_CONTAINERS]


def resolve_home_drive_container(raw_key: str | None) -> tuple[str, str]:
    key = str(raw_key or "").strip()
    if not key:
        raise ValueError("Container seçimi gerekli.")
    label = _HOME_DRIVE_CONTAINER_BY_KEY.get(key)
    if not label:
        raise ValueError("Geçersiz container seçimi.")
    return key, label


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
_VIDEO_MIMES = frozenset(
    {
        "video/mp4",
        "video/webm",
        "video/quicktime",
        "video/x-msvideo",
        "video/x-matroska",
    }
)
_MAX_IMAGE_BYTES = 12 * 1024 * 1024  # 12 MB
_MAX_VIDEO_BYTES = 100 * 1024 * 1024  # 100 MB
_PANEL_APP_PROP = "seo_panel_home"


def _drive_service(db: Session):
    creds = home_drive_auth.ensure_fresh_credentials(db)
    if creds is None:
        raise RuntimeError("Google Drive bağlı değil. Önce hesabı bağlayın.")
    return build("drive", "v3", credentials=creds, cache_discovery=False), creds


def _http_error_reason(exc: HttpError) -> tuple[int, str, str]:
    """Return (status, reason, message) from a Drive HttpError."""
    status = 0
    try:
        status = int(getattr(exc, "status_code", 0) or 0)
    except Exception:  # noqa: BLE001
        status = 0
    if not status:
        try:
            status = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
        except Exception:  # noqa: BLE001
            status = 0
    reason = ""
    message = str(exc)
    try:
        raw = exc.content.decode("utf-8") if isinstance(exc.content, (bytes, bytearray)) else str(exc.content or "")
        data = json.loads(raw) if raw else {}
        err = data.get("error") if isinstance(data, dict) else {}
        if isinstance(err, dict):
            message = str(err.get("message") or message)
            errors = err.get("errors") or []
            if errors and isinstance(errors[0], dict):
                reason = str(errors[0].get("reason") or "")
    except Exception:  # noqa: BLE001
        pass
    return status, reason, message


def _friendly_drive_error(exc: BaseException) -> str:
    if isinstance(exc, HttpError):
        status, reason, message = _http_error_reason(exc)
        low = f"{reason} {message}".lower()
        if reason == "accessNotConfigured" or "access not configured" in low or "has not been used" in low:
            return (
                "Google Cloud projesinde Drive API etkin değil. "
                "APIs & Services → Library → Google Drive API → Enable."
            )
        if reason in ("storageQuotaExceeded", "quotaExceeded") or "storage quota" in low:
            return "Drive depolama kotası dolu veya bu hesaba yazılamıyor."
        if (
            reason
            in (
                "insufficientPermissions",
                "insufficientFilePermissions",
                "forbidden",
            )
            or "insufficient" in low
        ):
            return (
                "Bu klasöre yazma yetkin yok. Klasörü bu Google hesabıyla "
                "Düzenleyici olarak paylaş, sonra bağlantıyı kesip yeniden bağla."
            )
        if reason == "notFound" or "file not found" in low:
            return "Hedef Drive klasörü bulunamadı (HOME_DRIVE_FOLDER_ID yanlış veya erişilemiyor)."
        if reason in ("authError", "unauthorized") or status == 401:
            return "Drive oturumu geçersiz. Bağlantıyı kesip yeniden bağla."
        if reason == "domainPolicy" or ("domain" in low and "polic" in low):
            return "Kurumsal politika bu uygulamaya Drive yüklemeyi engelliyor."
        short = (message or str(exc)).strip()
        if len(short) > 220:
            short = short[:220] + "…"
        return f"Drive hatası ({status or '?'}{('/' + reason) if reason else ''}): {short}"
    return str(exc)[:220]


def _assert_folder_writable(service, folder_id: str) -> None:
    try:
        meta = (
            service.files()
            .get(
                fileId=folder_id,
                fields="id,name,mimeType,driveId,capabilities(canAddChildren)",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as exc:
        raise RuntimeError(_friendly_drive_error(exc)) from exc
    mime = str(meta.get("mimeType") or "")
    if mime and mime != "application/vnd.google-apps.folder":
        raise RuntimeError("HOME_DRIVE_FOLDER_ID bir klasör değil.")
    caps = meta.get("capabilities") or {}
    if caps.get("canAddChildren") is False:
        raise PermissionError(
            "Bu klasöre dosya ekleme yetkin yok. Klasörü Düzenleyici olarak paylaş "
            "veya Shared Drive’da Content manager rolü ver."
        )


def _file_dict(
    *,
    fid: str,
    name: str,
    mime: str,
    size: int,
    web_view_link: str,
    created_time: str = "",
    container_key: str = "",
    container_label: str = "",
) -> dict[str, Any]:
    kind = "video" if (mime or "").startswith("video/") else "image"
    return {
        "id": fid,
        "name": name or ("video" if kind == "video" else "image"),
        "mime_type": mime or ("video/mp4" if kind == "video" else "image/jpeg"),
        "kind": kind,
        "created_time": created_time,
        "size": int(size or 0),
        "web_view_link": web_view_link or "",
        "thumb_url": f"/api/home/drive/files/{fid}/content" if fid and kind == "image" else "",
        "container_key": container_key or "",
        "container_label": container_label or "",
    }


def _register_upload(
    db: Session,
    *,
    drive_file_id: str,
    name: str,
    mime_type: str,
    size_bytes: int,
    web_view_link: str,
    container_key: str = "",
    container_label: str = "",
) -> None:
    from backend.models import HomeDriveUpload

    fid = (drive_file_id or "").strip()
    if not fid:
        return
    row = db.query(HomeDriveUpload).filter(HomeDriveUpload.drive_file_id == fid).first()
    if row is None:
        row = HomeDriveUpload(drive_file_id=fid)
        db.add(row)
    row.name = (name or "")[:255]
    row.mime_type = (mime_type or "")[:120]
    row.size_bytes = int(size_bytes or 0)
    row.web_view_link = web_view_link or ""
    row.container_key = (container_key or "")[:64]
    row.container_label = (container_label or "")[:120]
    db.commit()


def _unregister_uploads(db: Session, file_ids: list[str]) -> None:
    from backend.models import HomeDriveUpload

    ids = [str(x).strip() for x in (file_ids or []) if str(x).strip()]
    if not ids:
        return
    db.query(HomeDriveUpload).filter(HomeDriveUpload.drive_file_id.in_(ids)).delete(
        synchronize_session=False
    )
    db.commit()


def list_folder_images(db: Session, *, limit: int = 60) -> list[dict[str, Any]]:
    """Geriye uyum — panel galerisi yalnızca manuel yüklemeleri döner."""
    return list_panel_uploads(db, limit=limit)


def list_panel_uploads(db: Session, *, limit: int = 60) -> list[dict[str, Any]]:
    """Yalnızca bu panelden yüklenen imaj/videolar (DB kaydı)."""
    from backend.models import HomeDriveUpload

    cap = max(1, min(int(limit or 60), 100))
    rows = (
        db.query(HomeDriveUpload)
        .order_by(HomeDriveUpload.uploaded_at.desc(), HomeDriveUpload.id.desc())
        .limit(cap)
        .all()
    )
    out: list[dict[str, Any]] = []
    missing: list[str] = []
    service = None
    for row in rows:
        fid = (row.drive_file_id or "").strip()
        if not fid:
            continue
        # Drive’da silinmişse kaydı temizle
        try:
            if service is None:
                service, _ = _drive_service(db)
            meta = (
                service.files()
                .get(
                    fileId=fid,
                    fields="id,trashed,name,mimeType,size,webViewLink,createdTime",
                    supportsAllDrives=True,
                )
                .execute()
            )
            if meta.get("trashed"):
                missing.append(fid)
                continue
            out.append(
                _file_dict(
                    fid=fid,
                    name=str(meta.get("name") or row.name or ""),
                    mime=str(meta.get("mimeType") or row.mime_type or ""),
                    size=int(meta.get("size") or row.size_bytes or 0),
                    web_view_link=str(meta.get("webViewLink") or row.web_view_link or ""),
                    created_time=str(meta.get("createdTime") or ""),
                    container_key=str(getattr(row, "container_key", "") or ""),
                    container_label=str(getattr(row, "container_label", "") or ""),
                )
            )
        except HttpError as exc:
            status, reason, _ = _http_error_reason(exc)
            if status == 404 or reason == "notFound":
                missing.append(fid)
                continue
            # Geçici Drive hatasında DB kaydıyla göster
            out.append(
                _file_dict(
                    fid=fid,
                    name=row.name,
                    mime=row.mime_type,
                    size=row.size_bytes,
                    web_view_link=row.web_view_link,
                    created_time=row.uploaded_at.isoformat() if row.uploaded_at else "",
                    container_key=str(getattr(row, "container_key", "") or ""),
                    container_label=str(getattr(row, "container_label", "") or ""),
                )
            )
        except Exception:  # noqa: BLE001
            out.append(
                _file_dict(
                    fid=fid,
                    name=row.name,
                    mime=row.mime_type,
                    size=row.size_bytes,
                    web_view_link=row.web_view_link,
                    created_time=row.uploaded_at.isoformat() if row.uploaded_at else "",
                    container_key=str(getattr(row, "container_key", "") or ""),
                    container_label=str(getattr(row, "container_label", "") or ""),
                )
            )
    if missing:
        _unregister_uploads(db, missing)
    return out


def list_container_badges(db: Session) -> dict[str, dict[str, Any]]:
    """Aktif panel yüklemelerinden container → badge + tüm dosyalar.

    Badge köşesinde en yeni dosya; ``files`` dizisi aynı container’a bağlı
    tüm (silinmemiş) yüklemeleri içerir. Silinenler listeden düşer.
    """
    badges: dict[str, dict[str, Any]] = {}
    for item in list_panel_uploads(db, limit=100):
        key = str(item.get("container_key") or "").strip()
        if not key:
            continue
        file_entry = {
            "file_id": item.get("id") or "",
            "kind": item.get("kind") or "image",
            "name": item.get("name") or "",
            "thumb_url": item.get("thumb_url") or "",
            "web_view_link": item.get("web_view_link") or "",
        }
        if key not in badges:
            label = str(item.get("container_label") or "") or _HOME_DRIVE_CONTAINER_BY_KEY.get(
                key, key
            )
            badges[key] = {
                "container_key": key,
                "container_label": label,
                "file_id": file_entry["file_id"],
                "kind": file_entry["kind"],
                "name": file_entry["name"],
                "thumb_url": file_entry["thumb_url"],
                "web_view_link": file_entry["web_view_link"],
                "files": [file_entry],
                "file_count": 1,
            }
        else:
            badges[key]["files"].append(file_entry)
            badges[key]["file_count"] = len(badges[key]["files"])
    return badges


def upload_image(
    db: Session,
    *,
    filename: str,
    content: bytes,
    content_type: str | None = None,
    container_key: str | None = None,
) -> dict[str, Any]:
    return upload_media(
        db,
        filename=filename,
        content=content,
        content_type=content_type,
        container_key=container_key,
    )


def upload_media(
    db: Session,
    *,
    filename: str,
    content: bytes,
    content_type: str | None = None,
    container_key: str | None = None,
) -> dict[str, Any]:
    ckey, clabel = resolve_home_drive_container(container_key)
    folder_id = home_drive_auth.home_drive_folder_id()
    if not folder_id:
        raise RuntimeError("HOME_DRIVE_FOLDER_ID tanımlı değil.")
    if not content:
        raise ValueError("Boş dosya yüklenemez.")
    mime = (content_type or "").split(";")[0].strip().lower()
    if not mime or mime == "application/octet-stream":
        guessed, _ = mimetypes.guess_type(filename or "")
        mime = (guessed or "image/jpeg").lower()
    is_image = mime in _IMAGE_MIMES or mime.startswith("image/")
    is_video = mime in _VIDEO_MIMES or mime.startswith("video/")
    if not is_image and not is_video:
        raise ValueError("Yalnızca görsel veya video yüklenebilir.")
    max_bytes = _MAX_VIDEO_BYTES if is_video else _MAX_IMAGE_BYTES
    if len(content) > max_bytes:
        raise ValueError(
            f"Dosya {('100 MB' if is_video else '12 MB')} sınırını aşıyor."
        )
    safe_name = (filename or ("video" if is_video else "image")).strip() or (
        "video" if is_video else "image"
    )
    if len(safe_name) > 180:
        safe_name = safe_name[:180]

    service, _ = _drive_service(db)
    _assert_folder_writable(service, folder_id)
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime, resumable=True)
    try:
        created = (
            service.files()
            .create(
                body={
                    "name": safe_name,
                    "parents": [folder_id],
                    "appProperties": {
                        "source": _PANEL_APP_PROP,
                        "containerKey": ckey,
                    },
                },
                media_body=media,
                fields="id,name,mimeType,createdTime,size,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as exc:
        raise RuntimeError(_friendly_drive_error(exc)) from exc
    fid = str(created.get("id") or "").strip()
    item = _file_dict(
        fid=fid,
        name=str(created.get("name") or safe_name),
        mime=str(created.get("mimeType") or mime),
        size=int(created.get("size") or len(content)),
        web_view_link=str(created.get("webViewLink") or ""),
        created_time=str(created.get("createdTime") or ""),
        container_key=ckey,
        container_label=clabel,
    )
    _register_upload(
        db,
        drive_file_id=fid,
        name=item["name"],
        mime_type=item["mime_type"],
        size_bytes=item["size"],
        web_view_link=item["web_view_link"],
        container_key=ckey,
        container_label=clabel,
    )
    return item


def delete_file(db: Session, file_id: str) -> None:
    """Hedef klasördeki panel yüklemesini sil; kalıcı silme yetkisi yoksa çöpe at."""
    from backend.models import HomeDriveUpload

    fid = (file_id or "").strip()
    if not fid:
        raise ValueError("Dosya id eksik.")
    # Galeriden yalnızca panel kayıtlarını silmeye izin ver
    tracked = (
        db.query(HomeDriveUpload).filter(HomeDriveUpload.drive_file_id == fid).first()
    )
    if tracked is None:
        raise PermissionError("Bu dosya panel yüklemeleri arasında değil.")
    folder_id = home_drive_auth.home_drive_folder_id()
    service, _ = _drive_service(db)
    try:
        meta = (
            service.files()
            .get(fileId=fid, fields="id,parents,trashed", supportsAllDrives=True)
            .execute()
        )
    except HttpError as exc:
        status, reason, _ = _http_error_reason(exc)
        if status == 404 or reason == "notFound":
            _unregister_uploads(db, [fid])
            return
        raise RuntimeError(_friendly_drive_error(exc)) from exc
    if meta.get("trashed"):
        _unregister_uploads(db, [fid])
        return
    parents = list(meta.get("parents") or [])
    if folder_id and folder_id not in parents:
        # Klasör dışı ama panel kaydı varsa kaydı temizle
        _unregister_uploads(db, [fid])
        raise PermissionError("Dosya hedef Drive klasöründe değil.")
    try:
        service.files().delete(fileId=fid, supportsAllDrives=True).execute()
        _unregister_uploads(db, [fid])
        return
    except Exception as exc:  # noqa: BLE001
        LOGGER.info("Drive permanent delete failed for %s (%s); trying trash", fid, exc)
    try:
        service.files().update(
            fileId=fid,
            body={"trashed": True},
            supportsAllDrives=True,
        ).execute()
        _unregister_uploads(db, [fid])
    except HttpError as exc:
        raise RuntimeError(_friendly_drive_error(exc)) from exc


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
    from backend.models import HomeDriveUpload

    fid = (file_id or "").strip()
    if not fid:
        raise ValueError("Dosya id eksik.")
    tracked = (
        db.query(HomeDriveUpload).filter(HomeDriveUpload.drive_file_id == fid).first()
    )
    if tracked is None:
        raise PermissionError("Bu dosya panel yüklemeleri arasında değil.")
    folder_id = home_drive_auth.home_drive_folder_id()
    service, _ = _drive_service(db)
    try:
        meta = (
            service.files()
            .get(
                fileId=fid,
                fields="id,name,mimeType,parents,trashed",
                supportsAllDrives=True,
            )
            .execute()
        )
    except HttpError as exc:
        raise RuntimeError(_friendly_drive_error(exc)) from exc
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
