from backend.config import host_requires_panel_auth


def test_railway_host_requires_auth():
    assert host_requires_panel_auth("projectcontrol.up.railway.app") is True


def test_localhost_not_public():
    assert host_requires_panel_auth("127.0.0.1") is False
    assert host_requires_panel_auth("localhost") is False
