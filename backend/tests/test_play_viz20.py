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


def test_build_viz20_data_rejects_removed_chart():
    out = build_viz20_data(None, viz_id="funnel")  # type: ignore[arg-type]
    assert out["ok"] is False
    assert "kaldırılmış" in out["message"]
