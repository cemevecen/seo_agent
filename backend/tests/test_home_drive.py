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
