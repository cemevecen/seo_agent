"""Regression: ayrı grafik (GA4 / peer) eksen sync'te tanımsız opts kullanılamaz."""

from pathlib import Path

AD_HTML = Path(__file__).resolve().parents[2] / "templates" / "ad.html"


def test_drill_stack_xaxis_sync_does_not_reference_bare_opts():
    text = AD_HTML.read_text(encoding="utf-8")
    start = text.index("function drillStackXaxisSync")
    end = text.index("function plotAxis", start)
    body = text[start:end]
    assert "function drillStackXaxisSync" in body
    assert "syncOpts" in body
    # Eski bug: tanımsız `opts` → ReferenceError → "Web ayrı grafik hatası"
    assert "opts && opts.traces" not in body
    assert "syncOpts && syncOpts.traces" in body


def test_peer_slice_helpers_present_for_area_mapping():
    text = AD_HTML.read_text(encoding="utf-8")
    assert "function peerMappedArea" in text
    assert "function peerSliceForFilter" in text
    assert "peerSliceForFilter(slice)" in text
