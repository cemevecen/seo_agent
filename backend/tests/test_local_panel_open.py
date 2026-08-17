"""Yerel geliştirme kapısı: 127.0.0.1 panelinde girişsiz erişim.

Panel girişi «yalnızca Google» ve OAuth redirect_uri canlıya sabit olduğu için
127.0.0.1:8012 panelinde oturum açmak mümkün değil. Bu kapı yalnızca yerelde,
yalnızca açık bayrakla ve yalnızca loopback TCP eşiyle açılır.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend import main as app_main


class _Req:
    def __init__(self, host: str | None = "127.0.0.1", headers: dict | None = None) -> None:
        self.client = SimpleNamespace(host=host) if host is not None else None
        self.headers = headers or {}


@pytest.fixture()
def local_flag(monkeypatch):
    monkeypatch.setenv("LOCAL_PANEL_NO_AUTH", "1")
    monkeypatch.setattr(
        "backend.services.app_member_auth.is_railway_runtime", lambda: False, raising=False
    )


def test_loopback_client_gets_in_when_flag_is_on(local_flag):
    assert app_main._local_panel_open(_Req("127.0.0.1")) is True
    assert app_main._local_panel_open(_Req("::1")) is True


def test_flag_off_keeps_the_panel_closed(monkeypatch):
    monkeypatch.delenv("LOCAL_PANEL_NO_AUTH", raising=False)
    monkeypatch.setattr(
        "backend.services.app_member_auth.is_railway_runtime", lambda: False, raising=False
    )
    assert app_main._local_panel_open(_Req("127.0.0.1")) is False


def test_remote_client_never_gets_in(local_flag):
    assert app_main._local_panel_open(_Req("192.168.1.20")) is False
    assert app_main._local_panel_open(_Req("10.0.0.5")) is False
    assert app_main._local_panel_open(_Req(None)) is False


def test_forged_forwarded_header_cannot_open_the_door(local_flag):
    """X-Forwarded-For'a bakılmıyor — sahte başlıkla kapı açılmamalı."""
    req = _Req("203.0.113.9", headers={"x-forwarded-for": "127.0.0.1", "host": "127.0.0.1:8012"})
    assert app_main._local_panel_open(req) is False


def test_railway_runtime_never_opens_even_with_flag(monkeypatch):
    monkeypatch.setenv("LOCAL_PANEL_NO_AUTH", "1")
    monkeypatch.setattr(
        "backend.services.app_member_auth.is_railway_runtime", lambda: True, raising=False
    )
    assert app_main._local_panel_open(_Req("127.0.0.1")) is False


def test_middleware_consults_the_gate_before_auth():
    """Sözleşme: kapı, oturum kontrolünden önce değerlendirilmeli."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
    # Kapı iki yerde sorulur; burada özellikle middleware'deki çağrı sınanıyor.
    middleware = src.split("async def ip_allowlist_middleware", 1)[1]
    chunk = middleware.split("if _local_panel_open(request):", 1)[1][:400]
    assert "return await call_next(request)" in chunk
    assert chunk.index("return await call_next(request)") < chunk.index("password_ready")


def test_route_level_guard_also_honours_the_gate():
    """«/» dashboard'ı middleware dışı ikinci kapıyı kullanıyor — o da geçidi sormalı."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
    body = src.split("def _ensure_panel_session(request: Request)", 1)[1].split("\ndef ", 1)[0]
    assert "_local_panel_open(request)" in body
    # Oturum kontrolünden önce sorulmalı, sonra değil
    assert body.index("_local_panel_open(request)") < body.index("panel_session_granted")
