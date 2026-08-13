"""Tests for iOS asc viz20 data builders."""

from backend.services.asc_viz20 import VIZ_IDS, build_asc_viz20_meta


def test_build_asc_viz20_meta_has_five_charts():
    meta = build_asc_viz20_meta()
    assert meta["ok"] is True
    ids = {v["id"] for v in meta["viz"]}
    assert ids == VIZ_IDS
    assert ids == {"treemap", "combo", "horizon", "control", "timeline"}
