"""Tests for Android viz20 data builders."""

from __future__ import annotations

from backend.services.play_viz20 import VIZ_IDS, build_viz20_data, build_viz20_meta


def test_build_viz20_meta_has_five_charts():
    meta = build_viz20_meta()
    assert meta["ok"] is True
    assert len(meta["viz"]) == 5
    ids = {v["id"] for v in meta["viz"]}
    assert ids == VIZ_IDS
    assert ids == {"treemap", "combo", "horizon", "control", "timeline"}
    for v in meta["viz"]:
        assert "detail" in v
        assert "n" not in v


def test_build_viz20_timeline_uses_sheet_only(monkeypatch):
    slow_called: list[int] = []

    def _slow(*_a, **_k):
        slow_called.append(1)
        return {"android": [], "ios": []}

    monkeypatch.setattr(
        "backend.services.store_version_releases.fetch_version_releases_for_product",
        _slow,
    )
    monkeypatch.setattr(
        "backend.services.app_release_sheet.fetch_releases_from_sheet",
        lambda *_a, **_k: (
            [],
            [
                {
                    "version": "9.5.10",
                    "released_at": "2026-08-01T12:00:00Z",
                    "build": "290/9.5.10",
                    "source": "google_sheets",
                }
            ],
        ),
    )
    monkeypatch.setattr(
        "backend.services.play_viz20.load_scrape_facts",
        lambda: ([], {}),
    )
    monkeypatch.setattr(
        "backend.services.play_viz20._q",
        lambda **_kw: {"series": [{"key": "2026-08-01", "value": 3}], "total": 3},
    )
    monkeypatch.setattr(
        "backend.services.play_viz20.play_console_payload",
        lambda _db: {"panels": {"vitals": {"version_name_map": {"290": "9.5.10"}}}},
    )
    out = build_viz20_data(None, viz_id="timeline", start="2026-07-15", end="2026-08-13")
    assert out["ok"] is True
    assert not slow_called
    rel = out["chart"]["releases"]
    assert len(rel) == 1
    assert rel[0]["version_code"] == "290"
    assert rel[0]["date"] == "2026-08-01"


def test_build_viz20_data_rejects_removed_chart():
    out = build_viz20_data(None, viz_id="funnel")  # type: ignore[arg-type]
    assert out["ok"] is False
    assert "kaldırılmış" in out["message"]
