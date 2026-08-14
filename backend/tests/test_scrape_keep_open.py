"""SCRAPE_KEEP_OPEN / per-scrape KEEP_OPEN helpers."""

from __future__ import annotations

import os

from backend.services.scrape_browser import scrape_keep_window_open


def test_scrape_keep_window_open_defaults_on(monkeypatch):
    monkeypatch.delenv("SCRAPE_KEEP_OPEN", raising=False)
    monkeypatch.delenv("PLAY_CONSOLE_KEEP_OPEN", raising=False)
    assert scrape_keep_window_open() is True
    assert scrape_keep_window_open(env_key="PLAY_CONSOLE_KEEP_OPEN") is True


def test_scrape_keep_window_open_global_off(monkeypatch):
    monkeypatch.setenv("SCRAPE_KEEP_OPEN", "0")
    monkeypatch.delenv("GSC_LINKS_KEEP_OPEN", raising=False)
    assert scrape_keep_window_open() is False
    assert scrape_keep_window_open(env_key="GSC_LINKS_KEEP_OPEN") is False


def test_scrape_keep_window_open_per_scrape_override(monkeypatch):
    monkeypatch.setenv("SCRAPE_KEEP_OPEN", "1")
    monkeypatch.setenv("FIREBASE_CONSOLE_KEEP_OPEN", "0")
    assert scrape_keep_window_open(env_key="FIREBASE_CONSOLE_KEEP_OPEN") is False
    monkeypatch.setenv("FIREBASE_CONSOLE_KEEP_OPEN", "1")
    monkeypatch.setenv("SCRAPE_KEEP_OPEN", "0")
    # per-scrape=1 wins over global=0
    assert scrape_keep_window_open(env_key="FIREBASE_CONSOLE_KEEP_OPEN") is True
