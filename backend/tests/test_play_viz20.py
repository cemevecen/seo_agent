"""Tests for Android viz20 data builders."""

from __future__ import annotations

from backend.services.play_viz20 import build_viz20_meta


def test_build_viz20_meta_has_20_charts():
    meta = build_viz20_meta()
    assert meta["ok"] is True
    assert len(meta["viz"]) == 20
    ids = {v["id"] for v in meta["viz"]}
    assert "funnel" in ids
    assert "marimekko" in ids
    assert "matrix" in ids
