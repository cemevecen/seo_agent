"""Home Drive auth helpers."""

from backend.services.home_drive_auth import (
    HOME_DRIVE_SCOPES,
    home_drive_folder_id,
    home_drive_oauth_is_configured,
)


def test_home_drive_defaults():
    assert home_drive_oauth_is_configured() in (True, False)
    assert "drive" in " ".join(HOME_DRIVE_SCOPES)
    assert home_drive_folder_id() == "14_VrPCB5H0b2aD8K8mxfHYZvVkPVqZQW"
