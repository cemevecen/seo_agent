"""Konsol kapsaması — «elimde hangi günler var» ve eksik gün tespiti.

Bu uç `history_seal.gap_fill` modunu besler; boşluk tespiti buradan gelen
`known_dates` olmadan hiç çalışmıyordu.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from backend.services import console_coverage as CC

ROOT = Path(__file__).resolve().parents[2]


# ── Tarih toplama: iki farklı JSON şekli ────────────────────────────────────

def test_collects_dates_from_asc_explorer_facts():
    blob = {
        "version": 2,
        "items": [],
        "panels": {
            "explorer_facts": [
                {"metric": "impressions", "date": "2026-08-15", "dim": "overview"},
                {"metric": "downloads", "date": "2026-08-15", "dim": "overview"},
                {"metric": "impressions", "date": "2026-08-16", "dim": "overview"},
            ]
        },
    }
    counts: dict[date, int] = {}
    CC._collect_dates(blob, counts)
    assert counts == {date(2026, 8, 15): 2, date(2026, 8, 16): 1}


def test_collects_dates_from_firebase_nested_series():
    blob = {
        "android": {"series": [{"day": "2026-08-14", "v": 1}, {"day": "2026-08-15", "v": 2}]},
        "ios": {"sessions_series": [{"report_date": "2026-08-15", "v": 3}]},
    }
    counts: dict[date, int] = {}
    CC._collect_dates(blob, counts)
    assert set(counts) == {date(2026, 8, 14), date(2026, 8, 15)}
    assert counts[date(2026, 8, 15)] == 2  # iki farklı seriden


def test_shape_change_does_not_silently_empty_coverage():
    """Özyinelemeli gezinme sayesinde yapı değişse de tarihler bulunur."""
    counts: dict[date, int] = {}
    CC._collect_dates({"yeni": {"kutu": [{"date": "2026-08-17"}]}}, counts)
    assert counts == {date(2026, 8, 17): 1}


def test_bad_dates_are_ignored():
    counts: dict[date, int] = {}
    CC._collect_dates(
        [{"date": "dun"}, {"date": ""}, {"date": None}, {"date": "2026-13-40"}], counts
    )
    assert counts == {}


def test_recursion_is_bounded():
    node: dict = {"date": "2026-08-10"}
    for _ in range(40):
        node = {"n": node}
    counts: dict[date, int] = {}
    CC._collect_dates(node, counts)  # patlamamalı
    assert isinstance(counts, dict)


def test_broken_json_is_not_fatal():
    assert CC._load_blob("{bozuk") is None
    assert CC._load_blob("") is None


# ── Eksik gün hesabı ────────────────────────────────────────────────────────

def test_missing_between_finds_the_hole():
    known = {date(2026, 8, 10), date(2026, 8, 12)}
    missing = CC.missing_between(known, date(2026, 8, 10), date(2026, 8, 13))
    assert missing == [date(2026, 8, 11), date(2026, 8, 13)]


def test_missing_between_handles_inverted_range():
    assert CC.missing_between(set(), date(2026, 8, 13), date(2026, 8, 10)) == []


def test_payload_reports_gap_and_window():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()
    counts = {yday: 3}
    out = CC._coverage_payload("asc", counts, start=(yday - timedelta(days=2)).isoformat(), end=yday.isoformat())
    assert out["ok"] is True
    assert out["known_count"] == 1
    assert out["has_gap"] is True
    assert out["missing_count"] == 2
    assert yday.isoformat() not in out["missing"]
    assert out["counts"][yday.isoformat()] == 3


def test_payload_has_no_gap_when_complete():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()
    counts = {yday - timedelta(days=i): 1 for i in range(3)}
    out = CC._coverage_payload("asc", counts, start=(yday - timedelta(days=2)).isoformat(), end=yday.isoformat())
    assert out["has_gap"] is False and out["missing"] == []


def test_default_window_matches_gap_fill_range():
    """Panel ile scraper aynı aralığı konuşmalı: mühür+1 → dün."""
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    out = CC._coverage_payload("asc", {}, start=None, end=None)
    assert out["end"] == calendar_yesterday().isoformat()
    assert out["start"] == (pipeline_seal_through("asc") + timedelta(days=1)).isoformat()


# ── Uçlar ───────────────────────────────────────────────────────────────────

def test_both_endpoints_exist_and_are_token_protected():
    asc = (ROOT / "backend/api/asc_console.py").read_text(encoding="utf-8")
    fb = (ROOT / "backend/api/firebase_console.py").read_text(encoding="utf-8")
    for src, path in ((asc, "/asc-console/coverage"), (fb, "/firebase-console/coverage")):
        assert f'@router.get("{path}")' in src
        block = src.split(f'@router.get("{path}")', 1)[1]
        assert "_check_ingest_token" in block


def test_bridge_can_reach_coverage_without_panel_cookie():
    main = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    prefixes = main.split("public_prefixes = (", 1)[1].split("\n    )", 1)[0]
    assert '"/api/asc-console/coverage"' in prefixes
    assert '"/api/firebase-console/coverage"' in prefixes


def test_coverage_registry_covers_both_pipelines():
    assert set(CC.COVERAGE_BY_PIPELINE) == {"asc", "firebase"}


def test_coverage_reads_from_db_without_crashing_when_empty():
    class _Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return None

    class _DB:
        def query(self, *a, **k):
            return _Q()

    out = CC.asc_coverage(_DB())
    assert out["ok"] is True and out["known_count"] == 0
    assert out["has_gap"] in (True, False)


def test_coverage_uses_real_stored_json():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()

    class _Row:
        metrics_json = json.dumps({
            "version": 2, "items": [],
            "panels": {"explorer_facts": [{"metric": "m", "date": yday.isoformat()}]},
        })

    class _Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return _Row()

    class _DB:
        def query(self, *a, **k):
            return _Q()

    out = CC.asc_coverage(_DB(), start=yday.isoformat(), end=yday.isoformat())
    assert out["known_count"] == 1 and out["has_gap"] is False
