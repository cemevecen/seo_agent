"""Backlink risk + CSV parse smoke tests."""

from backend.services.backlink_csv import parse_csv_text
from backend.services.backlink_risk import (
    ACTION_DISAVOW,
    ACTION_IGNORE,
    assess_linking_url,
    finalize_domain_risk_summary,
    is_trusted_media_domain,
    normalize_domain,
)


def test_normalize_domain_ip():
    assert normalize_domain("http://100.24.25.201/spam") == "100.24.25.201"


def test_assess_spam_url_disavow():
    out = assess_linking_url("http://100.24.25.201/porn-free-download")
    assert out["risk_score"] >= 70
    assert out["recommended_action"] == ACTION_DISAVOW
    assert "ip_host" in out["risk_flags"]


def test_trusted_media_low_risk():
    assert is_trusted_media_domain("www.hurriyet.com.tr")
    out = assess_linking_url(
        "https://www.hurriyet.com.tr/ekonomi/ornek-haber-basligi-123456",
        target_url="https://doviz.com/altin",
    )
    assert out["recommended_action"] == ACTION_IGNORE
    assert out["risk_score"] < 25
    assert "trusted_media" in out["risk_flags"]


def test_domain_majority_not_single_outlier():
    bucket = {
        "domain": "news.example.com",
        "link_count": 10,
        "max_risk_score": 80,
        "min_risk_score": 5,
        "low_risk_links": 9,
        "action_counts": {"ignore": 9, "monitor": 0, "review": 0, "disavow": 1},
        "recommended_action": "disavow",
        "risk_flags": set(),
        "sample_urls": [],
        "sample_links": [],
    }
    finalize_domain_risk_summary(bucket)
    assert bucket["domain_category"] == "mostly_clean"
    assert bucket["recommended_action"] == ACTION_IGNORE


def test_parse_csv_turkish_headers():
    csv = (
        "Bağlantı verilen sayfa,Son tarama\n"
        "http://evil.example/spam,2026-05-07\n"
        "http://news.example/haber,2026-05-07\n"
    )
    rows = parse_csv_text(csv, report_type="latest_links")
    assert len(rows) == 2
    assert rows[0]["source_url"].startswith("http://evil")
