"""Notification analytics alert evaluation."""

from datetime import date, timedelta

from backend.services.notification_analytics_alerts import (
    _period_stats,
    _median,
    _daily_ctr_values,
    evaluate_notification_analytics_alerts,
)


def _sample_row(day: str, android_click: float) -> dict:
    return {
        "id": "1",
        "text": "Test",
        "date": day + "T09:00:00",
        "platforms": {
            "android": {"click": android_click, "impression": android_click * 10, "ctr": 10},
            "ios": {"click": 10, "ctr": 5},
            "desktop": {"click": 5, "impression": 50, "ctr": 10},
            "mobileweb": {"click": 3, "impression": 30, "ctr": 10},
        },
    }


def test_period_stats_sums_clicks():
    rows = [_sample_row("2026-06-01", 100), _sample_row("2026-06-02", 200)]
    stats = _period_stats(rows)
    assert stats["clicks"] == 336.0


def test_median():
    assert _median([1, 2, 3, 4, 5]) == 3
    assert _median([1, 2, 3, 4]) == 2.5


def test_daily_ctr_values():
    rows = [_sample_row("2026-06-01", 100), _sample_row("2026-06-01", 50)]
    vals = _daily_ctr_values(rows)
    assert len(vals) == 1
    assert vals[0] > 0


def test_evaluate_with_mock_db(monkeypatch):
    ref = date(2026, 6, 10)
    cur_rows = [_sample_row((ref - timedelta(days=i)).isoformat(), 50) for i in range(7)]
    prev_rows = [_sample_row((ref - timedelta(days=i)).isoformat(), 500) for i in range(7, 14)]

    class FakeWs:
        rows_json = "[]"

    def fake_get_workspace(db):
        return FakeWs()

    def fake_load_rows(row):
        return cur_rows + prev_rows

    def fake_filter(rows, *, start=None, end=None):
        out = []
        for r in rows:
            d = str(r.get("date") or "")[:10]
            if start and d < start[:10]:
                continue
            if end and d > end[:10]:
                continue
            out.append(r)
        return out

    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._get_workspace",
        fake_get_workspace,
    )
    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._load_rows",
        fake_load_rows,
    )
    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts.filter_rows_by_date",
        fake_filter,
    )

    out = evaluate_notification_analytics_alerts(None, send_email=False, reference_day=ref)
    assert out["ok"] is True
    assert any(a["id"] == "click_drop" for a in out["alerts"])
