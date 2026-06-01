"""Backlink risk + CSV parse smoke tests."""

from backend.services.backlink_csv import parse_csv_text
from backend.services.backlink_risk import ACTION_DISAVOW, assess_linking_url, normalize_domain


def test_normalize_domain_ip():
    assert normalize_domain("http://100.24.25.201/spam") == "100.24.25.201"


def test_assess_spam_url_disavow():
    out = assess_linking_url("http://100.24.25.201/porn-free-download")
    assert out["risk_score"] >= 70
    assert out["recommended_action"] == ACTION_DISAVOW
    assert "ip_host" in out["risk_flags"]


def test_parse_csv_turkish_headers():
    csv = (
        "Bağlantı verilen sayfa,Son tarama\n"
        "http://evil.example/spam,2026-05-07\n"
        "http://news.example/haber,2026-05-07\n"
    )
    rows = parse_csv_text(csv, report_type="latest_links")
    assert len(rows) == 2
    assert rows[0]["source_url"].startswith("http://evil")
