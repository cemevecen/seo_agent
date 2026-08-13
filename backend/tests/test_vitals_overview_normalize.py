"""Vitals metrics overview — tam scrape normalize."""

from __future__ import annotations

from backend.services.play_console_normalize import _normalize_vitals


def test_normalize_vitals_overview_sections_and_recommendations():
    raw = {
        "version": 3,
        "days": 28,
        "crashes": {},
        "metrics_overview": {
            "url": "https://play.google.com/console/app/vitals/metrics/overview",
            "page_title": "Android vitals'a genel bakış",
            "peer_group": "Benzerler grubu: Özel benzer grubu",
            "rows": [
                {
                    "key": "anr",
                    "metric": "Kullanıcı tarafından algılanan ANR oranı",
                    "value_28d": "%0,07",
                    "vs_previous_28d": "-%0,01",
                    "vs_peers_median": "+%0,04",
                }
            ],
            "recommendations": [
                {"title": "Edge-to-edge may not display for all users", "version": "290 (9.5.10)"},
            ],
            "summary_cards": [
                {"key": "anr", "metric": "Kullanıcı tarafından algılanan ANR oranı", "value": "%0,07", "delta": "-%0,01"},
            ],
            "sections": [
                {
                    "id": "stability",
                    "title": "Kararlılık",
                    "rows": [
                        {
                            "key": "anr",
                            "metric": "Kullanıcı tarafından algılanan ANR oranı",
                            "value_28d": "%0,07",
                            "vs_previous_28d": "-%0,01",
                            "vs_peers_median": "+%0,04",
                        }
                    ],
                },
                {
                    "id": "memory",
                    "title": "Bellek",
                    "rows": [
                        {
                            "metric": "Bellek kullanımı (anonim RSS ve takas)",
                            "p50": "110 MB",
                            "vs_previous_p50": "-1 MB",
                            "p90": "201 MB",
                            "vs_previous_p90": "0 MB",
                        }
                    ],
                },
            ],
        },
    }
    out = _normalize_vitals(raw)
    ov = out["metrics_overview"]
    assert ov["page_title"] == "Android vitals'a genel bakış"
    assert ov["section_count"] == 2
    assert ov["recommendation_count"] == 1
    assert ov["summary_cards"][0]["value"] == "%0,07"
    mem = next(s for s in ov["sections"] if s["id"] == "memory")
    assert mem["rows"][0]["p50"] == "110 MB"
