"""Home Drive auth helpers + delete batch."""

from unittest.mock import MagicMock, patch

from backend.services.home_drive import delete_files
from backend.services.home_drive_auth import (
    HOME_DRIVE_SCOPES,
    home_drive_folder_id,
    home_drive_oauth_is_configured,
)


def test_home_drive_defaults():
    assert home_drive_oauth_is_configured() in (True, False)
    assert "drive" in " ".join(HOME_DRIVE_SCOPES)
    assert home_drive_folder_id() == "14_VrPCB5H0b2aD8K8mxfHYZvVkPVqZQW"


def test_delete_files_batch_skips_blank_and_dedupes():
    db = MagicMock()
    with patch("backend.services.home_drive.delete_file") as del_fn:
        del_fn.side_effect = [None, RuntimeError("boom"), None]
        result = delete_files(db, ["a", "", "a", "b", "c", None])  # type: ignore[list-item]
    assert result["deleted"] == ["a", "c"]
    assert result["deleted_count"] == 2
    assert result["failed_count"] == 1
    assert result["failed"][0]["id"] == "b"
    assert del_fn.call_count == 3


def test_friendly_drive_error_maps_insufficient_permissions():
    from backend.services.home_drive import _friendly_drive_error
    from googleapiclient.errors import HttpError

    resp = MagicMock()
    resp.status = 403
    content = b'{"error":{"message":"Insufficient permissions","errors":[{"reason":"insufficientPermissions"}]}}'
    exc = HttpError(resp, content)
    msg = _friendly_drive_error(exc)
    assert "yazma yetkin" in msg.lower() or "yetkin yok" in msg.lower()


def test_file_dict_marks_video_kind():
    from backend.services.home_drive import _file_dict

    img = _file_dict(fid="1", name="a.png", mime="image/png", size=10, web_view_link="")
    vid = _file_dict(fid="2", name="b.mp4", mime="video/mp4", size=20, web_view_link="https://x")
    assert img["kind"] == "image"
    assert img["thumb_url"].endswith("/1/content")
    assert vid["kind"] == "video"
    assert vid["thumb_url"] == ""


def test_resolve_home_drive_container():
    from backend.services.home_drive import list_home_drive_containers, resolve_home_drive_container
    import pytest

    keys = {c["key"] for c in list_home_drive_containers()}
    assert "ga4-doviz" in keys
    assert "sc-sinemalar" in keys
    key, label = resolve_home_drive_container("ga4-doviz")
    assert key == "ga4-doviz"
    assert "ga4" in label.lower()
    with pytest.raises(ValueError):
        resolve_home_drive_container("")
    with pytest.raises(ValueError):
        resolve_home_drive_container("not-a-real-container")


def test_list_container_badges_groups_files(monkeypatch):
    from backend.services import home_drive

    monkeypatch.setattr(
        home_drive,
        "list_panel_uploads",
        lambda db, limit=100: [
            {
                "id": "a1",
                "name": "one.png",
                "kind": "image",
                "thumb_url": "/t/a1",
                "web_view_link": "",
                "container_key": "ga4-doviz",
                "container_label": "doviz · ga4",
            },
            {
                "id": "a2",
                "name": "two.png",
                "kind": "image",
                "thumb_url": "/t/a2",
                "web_view_link": "",
                "container_key": "ga4-doviz",
                "container_label": "doviz · ga4",
            },
            {
                "id": "b1",
                "name": "clip.mp4",
                "kind": "video",
                "thumb_url": "",
                "web_view_link": "https://x",
                "container_key": "sc-doviz",
                "container_label": "doviz · search console",
            },
        ],
    )
    badges = home_drive.list_container_badges(db=None)
    assert badges["ga4-doviz"]["file_count"] == 2
    assert len(badges["ga4-doviz"]["files"]) == 2
    assert badges["ga4-doviz"]["file_id"] == "a1"
    assert badges["sc-doviz"]["file_count"] == 1
    assert badges["sc-doviz"]["kind"] == "video"


def test_resolve_home_drive_container_includes_sc_doviz():
    from backend.services.home_drive import list_home_drive_containers, resolve_home_drive_container

    keys = {c["key"] for c in list_home_drive_containers()}
    assert "sc-doviz" in keys
    key, label = resolve_home_drive_container("sc-doviz")
    assert key == "sc-doviz"
    assert "search console" in label.lower()


def test_upload_media_rejects_empty_and_bad_container():
    from backend.services import home_drive
    import pytest

    db = MagicMock()
    with pytest.raises(ValueError, match="Container"):
        home_drive.upload_media(db, filename="a.png", content=b"x", container_key="")
    with pytest.raises(ValueError, match="Geçersiz"):
        home_drive.upload_media(db, filename="a.png", content=b"x", container_key="nope")
    with pytest.raises(ValueError, match="Boş"):
        home_drive.upload_media(db, filename="a.png", content=b"", container_key="sc-doviz")


def test_upload_media_rejects_oversize_image(monkeypatch):
    from backend.services import home_drive
    import pytest

    monkeypatch.setattr(home_drive.home_drive_auth, "home_drive_folder_id", lambda: "folder-1")
    db = MagicMock()
    big = b"0" * (12 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="12 MB"):
        home_drive.upload_media(
            db,
            filename="big.png",
            content=big,
            content_type="image/png",
            container_key="sc-doviz",
        )


def test_upload_media_single_and_multi_container_keys(monkeypatch):
    """Tek ve çoklu yükleme aynı upload_media yolunu kullanır; container_key zorunlu."""
    from backend.services import home_drive

    monkeypatch.setattr(home_drive.home_drive_auth, "home_drive_folder_id", lambda: "folder-1")

    created = {"id": "file-99", "name": "shot.png", "mimeType": "image/png", "size": "4", "webViewLink": ""}
    files_api = MagicMock()
    files_api.create.return_value.execute.return_value = created
    service = MagicMock()
    service.files.return_value = files_api

    monkeypatch.setattr(home_drive, "_drive_service", lambda db: (service, None))
    monkeypatch.setattr(home_drive, "_assert_folder_writable", lambda service, folder_id: None)
    registered = []

    def _reg(db, **kwargs):
        registered.append(kwargs)

    monkeypatch.setattr(home_drive, "_register_upload", _reg)

    db = MagicMock()
    for key in ("sc-doviz", "ga4-doviz"):
        registered.clear()
        out = home_drive.upload_media(
            db,
            filename="shot.png",
            content=b"\x89PNG",
            content_type="image/png",
            container_key=key,
        )
        assert out["id"] == "file-99"
        assert out["container_key"] == key
        assert registered and registered[0]["container_key"] == key
        body = files_api.create.call_args.kwargs.get("body") or files_api.create.call_args[1].get("body")
        if body is None and files_api.create.call_args:
            # positional/kwargs vary by mock usage
            kwargs = files_api.create.call_args.kwargs or {}
            body = kwargs.get("body") or (files_api.create.call_args[0][0] if files_api.create.call_args[0] else None)
        assert body["appProperties"]["containerKey"] == key


def _unwrap_endpoint(fn):
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


def test_home_drive_upload_api_accepts_sequential_files(monkeypatch):
    """UI çoklu yüklemeyi ardışık POST ile yapar — iki çağrı da ok dönmeli."""
    import asyncio
    import json

    from backend.api import home_drive as api

    db = MagicMock()
    monkeypatch.setattr(
        api.home_drive_auth,
        "get_home_drive_credential_row",
        lambda db: MagicMock(),
    )

    uploaded = []

    def fake_upload(db, **kwargs):
        uploaded.append(kwargs)
        return {
            "id": f"id-{len(uploaded)}",
            "name": kwargs.get("filename") or "x.png",
            "kind": "image",
            "thumb_url": "/t",
            "container_key": kwargs.get("container_key"),
            "container_label": "doviz · search console",
        }

    monkeypatch.setattr(api.home_drive, "upload_media", fake_upload)
    endpoint = _unwrap_endpoint(api.home_drive_upload)

    class FakeUpload:
        def __init__(self, name, data, content_type="image/png"):
            self.filename = name
            self.content_type = content_type
            self._data = data

        async def read(self):
            return self._data

    async def run():
        r1 = await endpoint(
            request=MagicMock(),
            db=db,
            file=FakeUpload("a.png", b"png-bytes"),
            container_key="sc-doviz",
        )
        r2 = await endpoint(
            request=MagicMock(),
            db=db,
            file=FakeUpload("b.png", b"png-bytes-2"),
            container_key="sc-doviz",
        )
        return r1, r2

    r1, r2 = asyncio.run(run())
    assert r1.status_code == 200
    assert r2.status_code == 200
    j1 = json.loads(r1.body.decode())
    j2 = json.loads(r2.body.decode())
    assert j1["ok"] is True and j1["file"]["container_key"] == "sc-doviz"
    assert j2["ok"] is True
    assert len(uploaded) == 2
    assert [u["filename"] for u in uploaded] == ["a.png", "b.png"]


def test_home_drive_upload_api_rejects_when_disconnected(monkeypatch):
    import asyncio
    import json

    from backend.api import home_drive as api

    monkeypatch.setattr(api.home_drive_auth, "get_home_drive_credential_row", lambda db: None)
    endpoint = _unwrap_endpoint(api.home_drive_upload)

    class FakeUpload:
        filename = "a.png"
        content_type = "image/png"

        async def read(self):
            return b"x"

    async def run():
        return await endpoint(
            request=MagicMock(),
            db=MagicMock(),
            file=FakeUpload(),
            container_key="sc-doviz",
        )

    resp = asyncio.run(run())
    assert resp.status_code == 401
    j = json.loads(resp.body.decode())
    assert j["ok"] is False

