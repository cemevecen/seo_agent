"""Home notification week-compare summary."""

from datetime import date, timedelta

from backend.services.notification_analytics_alerts import (
    _period_stats,
    _top_sends_by_clicks,
    build_notification_week_compare,
)


def _row(
    day: str,
    *,
    text: str,
    nid: str = "1",
    desktop_c: float,
    desktop_i: float,
    mobileweb_c: float = 0,
    android_c: float = 0,
    ios_c: float = 0,
) -> dict:
    return {
        "id": nid,
        "text": text,
        "date": day + "T09:00:00",
        "platforms": {
            "desktop": {"click": desktop_c, "impression": desktop_i, "ctr": 10},
            "mobileweb": {"click": mobileweb_c, "impression": 0, "ctr": 0},
            "android": {"click": android_c, "impression": 0, "ctr": 0},
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


def test_top_sends_by_clicks_includes_platforms_and_send_day():
    rows = [
        _row(
            "2026-07-20",
            text="Büyük",
            nid="100",
            desktop_c=50,
            desktop_i=500,
            mobileweb_c=10,
            android_c=5,
            ios_c=5,
        ),
        _row(
            "2026-07-21",
            text="Küçük",
            nid="200",
            desktop_c=10,
            desktop_i=100,
            ios_c=1,
        ),
        _row(
            "2026-07-22",
            text="Büyük",
            nid="100",
            desktop_c=20,
            desktop_i=200,
        ),
    ]
    top = _top_sends_by_clicks(rows, limit=5)
    assert top[0]["text"] == "Büyük"
    assert top[0]["id"] == "100"
    assert top[0]["send_day"] == "2026-07-20"
    assert top[0]["clicks"] == 70
    assert top[0]["desktop"] == 50
    assert top[0]["mobileweb"] == 10
    assert top[0]["android"] == 5
    assert top[0]["ios"] == 5
    assert top[1]["text"] == "Büyük"
    assert top[1]["send_day"] == "2026-07-22"
    assert top[1]["clicks"] == 20


def test_build_notification_week_compare(monkeypatch):
    ref = date(2026, 7, 27)
    cur_rows = [
        _row(
            (ref - timedelta(days=i)).isoformat(),
            text=f"Cur{i}",
            nid=str(1000 + i),
            desktop_c=100,
            desktop_i=1000,
            ios_c=20,
        )
        for i in range(7)
    ]
    prev_rows = [
        _row(
            (ref - timedelta(days=i)).isoformat(),
            text=f"Prev{i}",
            nid=str(2000 + i),
            desktop_c=50,
            desktop_i=500,
            ios_c=10,
        )
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
    assert result["windows"]["including_today"]["end"] == date.today().isoformat()
    assert result["totals"]["clicks_cur"] > result["totals"]["clicks_prev"]
    by_key = {p["key"]: p for p in result["platforms"]}
    assert by_key["desktop"]["has_impressions"] is True
    assert by_key["ios"]["has_impressions"] is False
    assert by_key["ios"]["impressions_cur"] is None
    assert len(result["top_titles"]) <= 3
    assert len(result["top_titles_previous"]) <= 3
    assert "top_titles_including_today" in result
    first = result["top_titles"][0]
    assert first["clicks"] > 0
    assert "desktop" in first and "send_day" in first and "id" in first
    prev_first = result["top_titles_previous"][0]
    assert prev_first["clicks"] > 0
    assert str(prev_first["id"]).startswith("2000") or prev_first["text"].startswith("Prev")


def test_including_today_window_covers_as_of_day(monkeypatch):
    ref = date(2026, 8, 6)  # yesterday-style KPI end
    today = date(2026, 8, 7)
    rows = [
        _row(
            "2026-08-07",
            text="Bugün",
            nid="9001",
            desktop_c=999,
            desktop_i=1000,
            ios_c=1,
        ),
        _row(
            "2026-08-06",
            text="Dün",
            nid="9002",
            desktop_c=10,
            desktop_i=100,
        ),
        _row(
            "2026-07-31",
            text="Hafta başı",
            nid="9003",
            desktop_c=50,
            desktop_i=500,
        ),
        _row(
            "2026-07-30",
            text="Dışarıda",
            nid="9004",
            desktop_c=5000,
            desktop_i=5000,
        ),
    ]

    class FakeWs:
        rows_json = "[]"

    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._get_workspace",
        lambda db: FakeWs(),
    )
    monkeypatch.setattr(
        "backend.services.notification_analytics_alerts._load_rows",
        lambda row: rows,
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

    result = build_notification_week_compare(
        None, reference_day=ref, top_n=10, as_of_day=today
    )
    assert result["windows"]["current"]["end"] == "2026-08-06"
    assert result["windows"]["including_today"]["start"] == "2026-08-01"
    assert result["windows"]["including_today"]["end"] == "2026-08-07"
    incl = result["top_titles_including_today"]
    assert incl[0]["id"] == "9001"
    assert incl[0]["text"] == "Bugün"
    ids = {x["id"] for x in incl}
    assert "9004" not in ids  # 30.07 outside today-inclusive 7d
    week = {x["id"] for x in result["top_titles"]}
    assert "9001" not in week  # today excluded from last-week KPI list
    assert "9002" in week
