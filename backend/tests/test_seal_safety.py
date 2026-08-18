"""Seal safety: mühürlü gövde silinmez; 1.5g scrape → yalnız dün kaydedilir."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from backend.database import Base
from backend.models import (
    AscConsoleWorkspace,
    EmpowerIntelDailyRow,
    FirebaseConsoleWorkspace,
    PlayConsoleWorkspace,
)
from backend.services.asc_console_store import _pack_metrics_blob as asc_pack
from backend.services.asc_console_store import ingest_asc_console_payload
from backend.services.empower_intel_store import upsert_rows
from backend.services.firebase_console_store import _pack_blob as fb_pack
from backend.services.firebase_console_store import ingest_firebase_console_payload
from backend.services.history_seal import (
    DEFAULT_HISTORY_SEAL,
    DEFAULT_HISTORY_START,
    calendar_yesterday,
    scheduled_fetch_window,
)
from backend.services.play_console_store import _pack_metrics_blob as play_pack
from backend.services.play_console_store import ingest_play_console_payload


ROOT = Path(__file__).resolve().parents[2]
PLAY_CACHE = Path.home() / ".seo-agent" / "play-console-last-full.json"
EMPOWER_CACHE = Path.home() / ".seo-agent" / "cache" / "empower-intel-last.json"


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def test_defaults_are_exact_seal_range():
    assert DEFAULT_HISTORY_START == date(2025, 1, 1)
    assert DEFAULT_HISTORY_SEAL == date(2026, 8, 13)


def test_manual_auto_window_fetches_1_5_days_stores_yesterday_only():
    today = date(2026, 8, 15)
    yday = date(2026, 8, 14)
    with (
        patch("backend.services.history_seal.calendar_today", return_value=today),
        patch("backend.services.history_seal.calendar_yesterday", return_value=yday),
        patch("backend.services.history_seal.is_pipeline_sealed", return_value=True),
        patch("backend.services.history_seal.force_full_history", return_value=False),
        patch(
            "backend.services.history_seal.pipeline_seal_through",
            return_value=date(2026, 8, 13),
        ),
    ):
        for pipe in (
            "play",
            "asc",
            "notification",
            "empower",
            "sinemalar_moderation",
            "firebase",
        ):
            win = scheduled_fetch_window(pipe)
            assert win["start"] == yday, pipe
            assert win["end"] == today, pipe
            assert win["store_end"] == yday, pipe


def test_play_ingest_keeps_sealed_body_and_drops_today(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    sealed = [
        {"metric": "installs", "date": "2025-01-01", "dim": "overview", "value": 1},
        {"metric": "installs", "date": "2026-08-13", "dim": "overview", "value": 13},
    ]
    row = PlayConsoleWorkspace(id=1)
    row.metrics_json = play_pack([], {"version": 2, "explorer_facts": sealed})
    row.reviews_json = "[]"
    row.rating_summary_json = "{}"
    db.add(row)
    db.commit()

    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        ingest_play_console_payload(
            db,
            metrics=[],
            panels={
                "explorer_facts": [
                    {"metric": "installs", "date": "2026-08-14", "dim": "overview", "value": 14},
                    {"metric": "installs", "date": "2026-08-15", "dim": "overview", "value": 99},
                ]
            },
            reviews=[],
            rating_summary={},
            sync_ok=True,
            sync_mode="dashboard",
        )

    from backend.services.play_console_store import _unpack_metrics_blob

    _, panels = _unpack_metrics_blob(db.get(PlayConsoleWorkspace, 1).metrics_json)
    by = {
        f["date"]: f["value"]
        for f in panels["explorer_facts"]
        if f.get("metric") == "installs"
    }
    assert by["2025-01-01"] == 1
    assert by["2026-08-13"] == 13
    assert by["2026-08-14"] == 14
    assert "2026-08-15" not in by


def test_asc_ingest_keeps_sealed_body_and_drops_today(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    row = AscConsoleWorkspace(id=1)
    row.metrics_json = asc_pack(
        [],
        {
            "explorer_facts": [
                {"metric": "units", "date": "2025-01-01", "dim": "overview", "value": 1},
                {"metric": "units", "date": "2026-08-13", "dim": "overview", "value": 13},
            ]
        },
    )
    db.add(row)
    db.commit()

    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        ingest_asc_console_payload(
            db,
            panels={
                "explorer_facts": [
                    {"metric": "units", "date": "2026-08-14", "dim": "overview", "value": 14},
                    {"metric": "units", "date": "2026-08-15", "dim": "overview", "value": 99},
                ]
            },
            sync_ok=True,
        )

    from backend.services.asc_console_store import _unpack_metrics_blob

    _, panels = _unpack_metrics_blob(db.get(AscConsoleWorkspace, 1).metrics_json)
    by = {f["date"]: f["value"] for f in panels["explorer_facts"] if f["metric"] == "units"}
    assert by["2025-01-01"] == 1
    assert by["2026-08-13"] == 13
    assert by["2026-08-14"] == 14
    assert "2026-08-15" not in by


def test_firebase_short_scrape_does_not_wipe_series(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    row = FirebaseConsoleWorkspace(id=1)
    row.metrics_json = fb_pack(
        [],
        {
            "platforms": {
                "android": {
                    "ok": True,
                    "series": [
                        {"date": "2025-01-01", "v": 1},
                        {"date": "2026-08-13", "v": 13},
                    ],
                }
            },
            "explorer_facts": [
                {"metric": "crash_free", "date": "2025-01-01", "platform": "android", "value": 99},
            ],
        },
    )
    db.add(row)
    db.commit()

    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        ingest_firebase_console_payload(
            db,
            {
                "sync_ok": True,
                "scrape_days": 1,
                "panels": {
                    "platforms": {
                        "android": {
                            "ok": True,
                            "series": [
                                {"date": "2026-08-14", "v": 14},
                                {"date": "2026-08-15", "v": 99},
                            ],
                        }
                    },
                    "explorer_facts": [
                        {
                            "metric": "crash_free",
                            "date": "2026-08-14",
                            "platform": "android",
                            "value": 98,
                        },
                        {
                            "metric": "crash_free",
                            "date": "2026-08-15",
                            "platform": "android",
                            "value": 97,
                        },
                    ],
                },
            },
        )

    from backend.services.firebase_console_store import _unpack_blob

    _, panels = _unpack_blob(db.get(FirebaseConsoleWorkspace, 1).metrics_json)
    ser = panels["platforms"]["android"]["series"]
    dates = [x["date"] for x in ser]
    assert "2025-01-01" in dates
    assert "2026-08-13" in dates
    assert "2026-08-14" in dates
    assert "2026-08-15" not in dates
    facts = panels["explorer_facts"]
    fdates = [f["date"] for f in facts]
    assert "2025-01-01" in fdates
    assert "2026-08-14" in fdates
    assert "2026-08-15" not in fdates


def test_empower_upsert_skips_today_keeps_history(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    upsert_rows(
        db,
        project="doviz",
        platform="android",
        rows=[
            {"report_date": "2025-01-01", "metrics": {"a": 1}},
            {"report_date": "2026-08-13", "metrics": {"a": 13}},
        ],
    )
    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        upsert_rows(
            db,
            project="doviz",
            platform="android",
            rows=[
                {"report_date": "2026-08-14", "metrics": {"a": 14}},
                {"report_date": "2026-08-15", "metrics": {"a": 99}},
            ],
        )
    rows = db.execute(select(EmpowerIntelDailyRow)).scalars().all()
    by = {r.report_date.isoformat(): json.loads(r.metrics_json)["a"] for r in rows}
    assert by["2025-01-01"] == 1
    assert by["2026-08-13"] == 13
    assert by["2026-08-14"] == 14
    assert "2026-08-15" not in by


def _cache_dates(payload: dict) -> list[str]:
    """Döküm içindeki benzersiz gün listesi (iki farklı şekil de desteklenir)."""
    out: set[str] = set()
    for fact in (payload.get("panels") or {}).get("explorer_facts") or []:
        if isinstance(fact, dict) and fact.get("date"):
            out.add(str(fact["date"])[:10])
    for platform in payload.get("platforms") or []:
        if not isinstance(platform, dict):
            continue
        for row in platform.get("rows") or []:
            if isinstance(row, dict) and (row.get("report_date") or row.get("date")):
                out.add(str(row.get("report_date") or row.get("date"))[:10])
    return sorted(out)


def _assert_dump_invariants(path, label: str) -> None:
    """Yerel scrape dökümü için gerçek değişmezler.

    Bu dosyalar «son tarama» çıktısıdır (her turda üzerine yazılır, geri
    okunmaz). Eskiden burada sabit tarih aralığı sınanıyordu; kısa bir tarama
    dosyayı daraltınca test kendiliğinden kırılıyordu — dosyanın taşımadığı bir
    özellik sınanıyordu. Mühür güvenliğinin kendisi zaten upsert testlerinde.
    """
    if not path.is_file():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    dates = _cache_dates(payload)
    if not dates:
        return
    yesterday = calendar_yesterday().isoformat()
    # Bugün hiçbir zaman tam gün değildir; döküme kalıcı gün olarak girmemeli
    assert dates[-1] <= yesterday, f"{label}: bugün/gelecek tarih var → {dates[-1]}"
    # Aralık kendi içinde sürekli olmalı: kısa tarama bile delikli seri üretmemeli
    span_start = date.fromisoformat(dates[0])
    span_end = date.fromisoformat(dates[-1])
    have = set(dates)
    missing = []
    cur = span_start
    while cur <= span_end:
        if cur.isoformat() not in have:
            missing.append(cur.isoformat())
        cur = date.fromordinal(cur.toordinal() + 1)
    assert missing == [], f"{label}: döküm içinde gün boşluğu {missing[:5]}"


def test_local_play_cache_dump_is_consistent():
    _assert_dump_invariants(PLAY_CACHE, "play")


def test_local_empower_cache_dump_is_consistent():
    _assert_dump_invariants(EMPOWER_CACHE, "empower")
