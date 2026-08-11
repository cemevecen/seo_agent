"""Doviz admin notifications/stats HTML → CSV parse."""

from unittest.mock import MagicMock, patch

import requests

from backend.services.doviz_notification_admin import login_admin_session, parse_stats_html_to_csv
from backend.services.notification_analytics_store import parse_csv_text


SAMPLE_HTML = """
<html><body>
<table class="tablesorter">
<thead>
<tr>
  <th>ID</th><th>Text</th>
  <th>Android App Impression</th><th>Android App Click</th><th>Android App CTR</th>
  <th>iOS App Click</th>
  <th>Desktop Impression</th><th>Desktop Click</th><th>Desktop CTR</th>
  <th>Mobileweb Impression</th><th>Mobileweb Click</th><th>Mobileweb CTR</th>
  <th>Date</th>
</tr>
</thead>
<tbody>
<tr>
  <td>2988029</td>
  <td>Piyasaların gözü istihdam verisinde</td>
  <td>85.815</td><td>2.316</td><td>%2.699</td>
  <td>3.186</td>
  <td>7.688</td><td>184</td><td>%2.393</td>
  <td>5.738</td><td>138</td><td>%2.405</td>
  <td>07.08.2026 10:15</td>
</tr>
<tr>
  <td>150389</td>
  <td>Dolar, FED Başkanı&#39;nın Açıklaması!</td>
  <td>2</td><td>0</td><td>%0</td>
  <td>0</td>
  <td>0</td><td>0</td><td>-</td>
  <td>0</td><td>0</td><td>-</td>
  <td>17.11.2016 17:21</td>
</tr>
</tbody>
</table>
</body></html>
"""


def test_parse_stats_html_to_rows():
    csv_text = parse_stats_html_to_csv(SAMPLE_HTML)
    rows = parse_csv_text(csv_text)
    assert len(rows) == 2
    assert rows[0]["id"] == "2988029"
    assert "istihdam" in rows[0]["text"]
    assert rows[0]["platforms"]["android"]["impression"] == 85815.0
    assert rows[0]["platforms"]["android"]["click"] == 2316.0
    assert rows[0]["platforms"]["ios"]["click"] == 3186.0
    assert rows[0]["date"].startswith("2026-08-07")
    assert rows[1]["id"] == "150389"


def test_admin_base_url_strips_login_and_rejects_railway():
    from backend.services.doviz_notification_admin import admin_base_url, login_url_candidates

    class FullLoginUrl:
        doviz_admin_base_url = "https://www.doviz.com/admin/login"

    class StatsUrl:
        doviz_admin_base_url = "https://www.doviz.com/admin/notifications/stats"

    class RailwayLogin:
        doviz_admin_base_url = "https://projectcontrol.up.railway.app/admin/login"

    with patch("backend.services.doviz_notification_admin.settings", FullLoginUrl):
        assert admin_base_url() == "https://www.doviz.com"
        assert login_url_candidates()[0] == "https://www.doviz.com/admin/login"

    with patch("backend.services.doviz_notification_admin.settings", StatsUrl):
        assert admin_base_url() == "https://www.doviz.com"

    with patch("backend.services.doviz_notification_admin.settings", RailwayLogin):
        assert admin_base_url() == "https://www.doviz.com"
        assert "admin/login/admin/login" not in " ".join(login_url_candidates())


def test_is_admin_vpn_unreachable_error():
    from backend.services.doviz_notification_admin import is_admin_vpn_unreachable_error

    assert is_admin_vpn_unreachable_error("Admin login sayfası açılamadı (son HTTP 404). VPN")
    assert is_admin_vpn_unreachable_error("Connection timed out")
    assert not is_admin_vpn_unreachable_error("Hatalı e-mail veya şifre")


def test_login_ignores_404_landing_when_stats_ok():
    """Başarılı giriş sonrası dashboard 404 olsa bile stats doğrulanınca OK."""

    class FakeSettings:
        doviz_admin_email = "a@b.com"
        doviz_admin_password = "secret"
        doviz_admin_base_url = "https://www.doviz.com"

    def fake_request(method, url, **kw):
        r = MagicMock(spec=requests.Response)
        r.headers = {}
        r.url = url
        allow = kw.get("allow_redirects", True)
        if method == "GET" and url.rstrip("/").endswith("doviz.com"):
            r.status_code = 200
            r.text = "home"
            r.url = url
        elif method == "GET" and "login" in url:
            r.status_code = 200
            r.text = (
                '<form method="post" class="login">'
                '<input name="email" type="text">'
                '<input name="password" type="password">'
                "</form>"
            )
            r.url = "https://www.doviz.com/admin/login"
        elif method == "POST" and "login" in url and not allow:
            r.status_code = 302
            r.headers = {"Location": "/admin/dashboard"}
            r.text = ""
        elif method == "GET" and "dashboard" in url:
            r.status_code = 404
            r.text = "Not Found"
            r.url = "https://www.doviz.com/admin/dashboard"
        elif method == "GET" and "stats" in url:
            r.status_code = 200
            r.text = "<html><table><tr><th>id</th></tr></table></html>"
            r.url = "https://www.doviz.com/admin/notifications/stats"
        else:
            r.status_code = 200
            r.text = "ok"
        return r

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.cookies = {"session": "1"}

        def get(self, url, **kw):
            return fake_request("GET", url, **kw)

        def post(self, url, **kw):
            return fake_request("POST", url, **kw)

    with (
        patch("backend.services.doviz_notification_admin.settings", FakeSettings),
        patch("backend.services.doviz_notification_admin.requests.Session", FakeSession),
    ):
        sess = login_admin_session()
        assert sess is not None
        assert sess.cookies.get("session") == "1"


def test_admin_cookie_jar_roundtrip(tmp_path, monkeypatch):
    from backend.services.doviz_notification_admin import (
        _load_admin_cookies,
        _save_admin_cookies,
    )

    jar = tmp_path / "admin-cookies.json"
    monkeypatch.setenv("DOVIZ_ADMIN_COOKIE_JAR", str(jar))
    sess = requests.Session()
    sess.cookies.set("doviz_session", "abc", domain="www.doviz.com", path="/")
    _save_admin_cookies(sess)
    assert jar.is_file()
    sess2 = requests.Session()
    assert _load_admin_cookies(sess2) is True
    assert sess2.cookies.get("doviz_session") == "abc"
