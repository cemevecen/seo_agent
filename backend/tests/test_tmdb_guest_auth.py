"""TMDB misafir linki doğrulama."""

import hmac

from backend.services import tmdb_guest_auth as tga


def test_access_query_matches(monkeypatch):
    monkeypatch.setattr(tga.settings, "tmdb_guest_access_token", "secret-guest-token", raising=False)
    assert tga.access_query_matches("secret-guest-token")
    assert not tga.access_query_matches("wrong")


def test_guest_cookie_roundtrip(monkeypatch):
    monkeypatch.setattr(tga.settings, "tmdb_guest_access_token", "abc", raising=False)
    monkeypatch.setattr(tga.settings, "secret_key", "unit-test-secret", raising=False)
    expected = tga.guest_cookie_value()
    assert expected
    assert hmac.compare_digest(expected, tga.guest_cookie_value())
