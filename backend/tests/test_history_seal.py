"""History seal: mühürlü gövde + dünün dilimi."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

from backend.services.history_seal import (
    DEFAULT_HISTORY_SEAL,
    DEFAULT_HISTORY_START,
    filter_facts_no_today,
    history_seal,
    history_start,
    is_pipeline_sealed,
    missing_days,
    never_store_today,
    play_qs_date_range,
    scheduled_fetch_window,
)


def test_defaults():
    assert history_start() == DEFAULT_HISTORY_START
    assert history_seal() == DEFAULT_HISTORY_SEAL


def test_sealed_by_default(monkeypatch):
    monkeypatch.delenv("HISTORY_SEALED", raising=False)
    monkeypatch.delenv("PLAY_HISTORY_SEALED", raising=False)
    with patch("backend.services.history_seal.load_seal_meta", return_value={}):
        assert is_pipeline_sealed("play") is True


def test_sealed_opt_out(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "0")
    assert is_pipeline_sealed("play") is False


def test_yesterday_only_when_sealed(monkeypatch):
    monkeypatch.delenv("HISTORY_FORCE_FULL", raising=False)
    monkeypatch.delenv("PLAY_FORCE_FULL", raising=False)
    yday = date(2026, 8, 14)
    today = date(2026, 8, 15)
    with (
        patch("backend.services.history_seal.calendar_yesterday", return_value=yday),
        patch("backend.services.history_seal.calendar_today", return_value=today),
        patch("backend.services.history_seal.is_pipeline_sealed", return_value=True),
        patch("backend.services.history_seal.force_full_history", return_value=False),
        patch(
            "backend.services.history_seal.pipeline_seal_through",
            return_value=date(2026, 8, 13),
        ),
    ):
        win = scheduled_fetch_window("play")
    assert win["mode"] == "yesterday_only"
    assert win["start"] == yday
    assert win["end"] == today  # scrape ~1.5g
    assert win["store_end"] == yday  # kaydet yalnız dün
    assert win["days"] == 2


def test_force_full_window(monkeypatch):
    monkeypatch.setenv("HISTORY_FORCE_FULL", "1")
    yday = date(2026, 8, 14)
    with (
        patch("backend.services.history_seal.calendar_yesterday", return_value=yday),
        patch("backend.services.history_seal.calendar_today", return_value=date(2026, 8, 15)),
        patch(
            "backend.services.history_seal.pipeline_seal_through",
            return_value=date(2026, 8, 13),
        ),
    ):
        win = scheduled_fetch_window("asc", force_full=True)
    assert win["mode"] == "backfill_full"
    assert win["start"] == DEFAULT_HISTORY_START
    assert win["end"] == date(2026, 8, 13)
    assert win["days"] == (win["end"] - win["start"]).days + 1


def test_never_store_today():
    today = date(2026, 8, 15)
    with patch("backend.services.history_seal.calendar_today", return_value=today):
        assert never_store_today(today) is True
        assert never_store_today(today.isoformat()) is True
        assert never_store_today(today - timedelta(days=1)) is False
        assert never_store_today(None) is True


def test_filter_facts_no_today():
    today = date(2026, 8, 15)
    facts = [
        {"metric": "a", "date": "2026-08-14"},
        {"metric": "a", "date": "2026-08-15"},
        {"metric": "b", "date": "2026-08-13"},
    ]
    with patch("backend.services.history_seal.calendar_today", return_value=today):
        out = filter_facts_no_today(facts)
    assert len(out) == 2
    assert all(f["date"] != "2026-08-15" for f in out)


def test_missing_days():
    today = date(2026, 8, 15)
    with patch("backend.services.history_seal.calendar_today", return_value=today):
        miss = missing_days(
            ["2026-08-10", "2026-08-12"],
            start=date(2026, 8, 10),
            end=date(2026, 8, 14),
        )
    assert miss == [date(2026, 8, 11), date(2026, 8, 13), date(2026, 8, 14)]


def test_play_qs_date_range():
    assert play_qs_date_range(date(2025, 1, 1), date(2026, 8, 13)) == "2025_1_1-2026_8_13"
    assert play_qs_date_range(date(2026, 8, 14), date(2026, 8, 14)) == "2026_8_14-2026_8_14"


def test_gap_fill_mode():
    yday = date(2026, 8, 16)
    seal = date(2026, 8, 13)
    with (
        patch("backend.services.history_seal.calendar_yesterday", return_value=yday),
        patch("backend.services.history_seal.calendar_today", return_value=date(2026, 8, 17)),
        patch("backend.services.history_seal.is_pipeline_sealed", return_value=True),
        patch("backend.services.history_seal.force_full_history", return_value=False),
        patch("backend.services.history_seal.pipeline_seal_through", return_value=seal),
    ):
        win = scheduled_fetch_window(
            "play",
            known_dates=["2026-08-14"],  # 15–16 missing
        )
    assert win["mode"] == "gap_fill"
    assert win["start"] == date(2026, 8, 15)
    assert win["end"] == date(2026, 8, 16)
