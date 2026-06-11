from backend.services.doviz_asset_csv_manifest import (
    _build_csv_issue_state,
    _failures_for_email,
    _increment_email_counts,
    normalize_manifest_url,
    parse_urls_from_csv_text,
)


def test_parse_urls_from_csv_text():
    text = """
    https://kur.doviz.com/akbank/euro
    garbage
    https://kur.doviz.com/akbank/euro
    https://altin.doviz.com/akbank/gram-altin
    """
    urls = parse_urls_from_csv_text(text)
    assert len(urls) == 2
    assert "https://kur.doviz.com/akbank/euro" in urls


def test_normalize_rejects_non_doviz():
    assert normalize_manifest_url("https://example.com/x") is None


def test_email_cap_two_per_url():
    scan = "2026-06-10T12:00:00Z"
    failures = [
        {
            "url": "https://kur.doviz.com/x/y",
            "kind": "prices_empty",
            "http_status": 200,
            "message": "m",
        }
    ]
    prev = {
        "url:https://kur.doviz.com/x/y": {
            "first_seen_at": "2026-06-09T10:00:00Z",
            "email_notify_count": 2,
        }
    }
    state = _build_csv_issue_state(scan_iso=scan, failures=failures, prev_issue_state=prev)
    assert state["url:https://kur.doviz.com/x/y"]["email_notify_count"] == 2
    mail = _failures_for_email(failures, state)
    assert mail == []

    prev["url:https://kur.doviz.com/x/y"]["email_notify_count"] = 1
    state = _build_csv_issue_state(scan_iso=scan, failures=failures, prev_issue_state=prev)
    mail = _failures_for_email(failures, state)
    assert len(mail) == 1
    _increment_email_counts(state, mail)
    assert state["url:https://kur.doviz.com/x/y"]["email_notify_count"] == 2
