"""Home notification week-compare summary."""

from datetime import date, timedelta

from backend.services.notification_analytics_alerts import (
    _period_stats,
    _top_titles_by_clicks,
    build_notification_week_compare,
)


def _row(day: str, *, text: str, desktop_c: float, desktop_i: float, ios_c: float = 0) -> dict:
    return {
        "id": "1",
        "text": text,
        "date": day + "T09:00:00",
        "platforms": {
            "desktop": {"click": desktop_c, "impression": desktop_i, "ctr": 10},
            "mobileweb": {"click": 0, "impression": 0, "ctr": 0},
            "android": {"click": 0, "impression": 0, "ctr": 0},
            "ios": {"click": ios_c, "ctr": 5},
        },
    }


def test_period_stats_includes_platform_impressions():
    rows = [_row("2026-07-20", text="A", desktop_c=10, desktop_i=100, ios_c=5)]
    stats = _period_stats(rows)
    assert stats["platform_clicks"]["desktop"] == 10
    assert stats["platform_clicks"]["ios"] == 5
    assert stats["platform_impressions"]["desktop"] == 100
    assert stats["platform_impressions"]["ios"] == 0
    assert stats["clicks"] == 15
    assert stats["impressions"] == 100


def test_top_titles_by_clicks():
    rows = [
        _row("2026-07-20", text="Büyük", desktop_c=50, desktop_i=500),
        _row("2026-07-21", text="Küçük", desktop_c=10, desktop_i=100),
        _row("2026-07-22", text="Büyük", desktop_c=20, desktop_i=200),
    ]
    top = _top_titles_by_clicks(rows, limit=5)
    assert top[0]["text"] == "Büyük"
    assert top[0]["clicks"] == 70
    assert top[1]["text"] == "Küçük"


def test_build_notification_week_compare(monkeypatch):
    ref = date(2026, 7, 27)
    cur_rows = [
        _row((ref - timedelta(days=i)).isoformat(), text=f"Cur{i}", desktop_c=100, desktop_i=1000, ios_c=20)
        for i in range(7)
    ]
    prev_rows = [
        _row((ref - timedelta(days=i)).isoformat(), text=f"Prev{i}", desktop_c=50, desktop_i=500, ios_c=10)
        for i in range(7, 14)
    ]

    class FakeWs:
        rows_json = "[]"

    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._get_workspace",
        lambda db: FakeWs(),
    )
    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._load_rows",
        lambda row: cur_rows + prev_rows,
    )

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
        "backend.services.notification_analytics_alerts.filter_rows_by_date",
        fake_filter,
    )

    result = build_notification_week_compare(None, reference_day=ref, top_n=3)
    assert result["empty"] is False
    assert result["windows"]["current"]["end"] == "2026-07-27"
    assert result["windows"]["previous"]["end"] == "2026-07-20"
    assert result["totals"]["clicks_cur"] > result["totals"]["clicks_prev"]
    by_key = {p["key"]: p for p in result["platforms"]}
    assert by_key["desktop"]["has_impressions"] is True
    assert by_key["ios"]["has_impressions"] is False
    assert by_key["ios"]["impressions_cur"] is None
    assert len(result["top_titles"]) <= 3
    assert result["top_titles"][0]["clicks"] > 0
