from backend.services.doviz_asset_monitor import html_has_gold_price_rows


def test_html_has_gold_price_rows_positive():
    html = """
    <table>
    <tr><th>Alış</th><th>Satış</th></tr>
    <tr><td><a href="#">Gram</a></td><td>2.450,12</td><td>2.480,00</td></tr>
  </table>
    """
    assert html_has_gold_price_rows(html) is True


def test_html_has_gold_price_rows_empty_table():
    html = """
    <h1>Kuveyt Türk</h1>
    <table><tr><th>Alış</th><th>Satış</th></tr></table>
    """
    assert html_has_gold_price_rows(html) is False


def test_catalog_excludes_merkez_bankasi():
    from backend.services.doviz_asset_monitor import _excluded_slugs

    assert "merkez-bankasi" in _excluded_slugs()


def test_build_issue_state_preserves_first_seen():
    from backend.services.doviz_asset_monitor import _build_issue_state

    first = "2026-06-01T10:00:00Z"
    scan = "2026-06-10T12:00:00Z"
    prev = {
        "kuveyt-turk|m.doviz.com": {
            "first_seen_at": first,
            "first_seen_tr": "01.06.2026 13:00",
        }
    }
    missing = [{"slug": "kuveyt-turk", "host": "m.doviz.com", "http_status": 200}]
    state = _build_issue_state(
        scan_iso=scan,
        prices_missing=missing,
        catalog_removed=[],
        prev_issue_state=prev,
    )
    row = state["kuveyt-turk|m.doviz.com"]
    assert row["first_seen_at"] == first
    assert row["last_seen_at"] == scan
    assert row["first_seen_tr"] == "01.06.2026 13:00"


def test_format_ts_tr_empty():
    from backend.services.doviz_asset_monitor import format_ts_tr

    assert format_ts_tr(None) == "—"
    assert format_ts_tr("2026-06-10T10:30:00Z")[:10] == "10.06.2026"


def test_should_send_asset_email_only_on_new_alerts():
    from backend.services.doviz_asset_monitor import _should_send_asset_email

    assert _should_send_asset_email([], {}) is False
    assert _should_send_asset_email([], {"last_email_at": "2020-01-01T00:00:00Z"}) is False


def test_should_send_asset_email_respects_hourly_cooldown(monkeypatch):
    from datetime import datetime, timezone

    from backend.services import doviz_asset_monitor as mod

    monkeypatch.setattr(mod.settings, "doviz_asset_monitor_email_enabled", True)
    monkeypatch.setattr(mod.settings, "outbound_email_enabled", True)
    monkeypatch.setattr(mod.settings, "doviz_asset_monitor_email_cooldown_hours", 1.0)

    recent = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    alerts = [{"slug": "x", "kind": "prices_empty"}]
    assert mod._should_send_asset_email(alerts, {"last_email_at": recent}) is False
    assert mod._should_send_asset_email(alerts, {"last_email_at": "2020-01-01T00:00:00Z"}) is True
