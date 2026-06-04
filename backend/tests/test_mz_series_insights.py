from backend.services.mz_series_insights import SeriesPoint, analyze_series


def _pts(vals):
    return [SeriesPoint(label=f"d{i}", value=v) for i, v in enumerate(vals)]


def test_flat_series_no_anomalies():
    out = analyze_series(_pts([100.0] * 12), is_rate_pct=True)
    assert not out["peaks"]
    assert not out["valleys"]
    assert not out["spikes_down"]


def test_coverage_style_dips_detected():
    base = [100.0] * 20
    base[10] = 42.0
    base[11] = 38.0
    base[12] = 100.0
    out = analyze_series(_pts(base), is_rate_pct=True)
    assert out["valleys"] or out["spikes_down"]
    labels = {v["label"] for v in out["valleys"] + out["spikes_down"]}
    assert "d10" in labels or "d11" in labels


def test_spike_up_detected():
    vals = [10.0, 10.0, 10.0, 50.0, 12.0, 11.0]
    out = analyze_series(_pts(vals), is_rate_pct=False, min_points=4)
    assert out["spikes_up"]
    assert out["spikes_up"][0]["delta"] >= 30.0


def test_local_peak_detected():
    vals = [5.0, 8.0, 20.0, 9.0, 6.0, 5.0, 4.0]
    out = analyze_series(_pts(vals), min_points=4)
    assert any(p["label"] == "d2" for p in out["peaks"])


def test_short_series_returns_empty_buckets():
    out = analyze_series(_pts([1.0, 2.0]), min_points=5)
    assert out["peaks"] == []
    assert out["range_min"] is None
