"""Play vitals tarama → ANR-free (Reporting API yok)."""

from __future__ import annotations

from backend.services.play_console_normalize import _normalize_vitals
from backend.services.stability_free import (
    free_rates_from_vitals_overview,
    play_latest_anr_from_vitals,
    play_versions_anr_from_vitals,
)


def _vitals_fixture() -> dict:
    return {
        "version_code": "290",
        "versions": [{"code": "290", "name": "9.5.10"}, {"code": "289", "name": "9.5.9"}],
        "version_name_map": {"290": "9.5.10", "289": "9.5.9"},
        "metrics_overview": {
            "rows": [
                {
                    "key": "anr",
                    "metric": "Kullanıcı tarafından algılanan ANR oranı",
                    "value_28d": "%0,04",
                }
            ]
        },
        "metrics_overview_by_version": {
            "290": {
                "rows": [
                    {
                        "key": "anr",
                        "metric": "Kullanıcı tarafından algılanan ANR oranı",
                        "value_28d": "%0,03",
                    }
                ]
            }
        },
        "by_version": {
            "289": {
                "crashes": {
                    "ANR": {
                        "summary_rate": "%0,05",
                        "categories": [],
                    }
                }
            }
        },
        "crashes": {
            "ANR": {
                "version_code": "290",
                "summary_rate": "%0,03",
                "categories": [],
            }
        },
    }


def test_overall_anr_free_from_overview():
    rates = free_rates_from_vitals_overview(_vitals_fixture())
    assert rates["anr_rate_pct"] == 0.04
    assert rates["anr_free_pct"] == 99.96
    assert rates["anr_free_fmt"] == "99,96%"


def test_latest_anr_prefers_version_overview():
    latest = play_latest_anr_from_vitals(_vitals_fixture())
    assert latest is not None
    assert latest["version_code"] == "290"
    assert latest["version_name"] == "9.5.10"
    assert latest["anr_rate_pct"] == 0.03
    assert latest["anr_free_pct"] == 99.97
    assert latest["anr_free_fmt"] == "99,97%"
    assert latest["source"] == "play_vitals_overview"
    assert latest["period"] == "28d"


def test_latest_anr_prefers_7d_scrape_block():
    vitals = dict(_vitals_fixture())
    vitals["anr_latest_7d"] = {
        "days": 7,
        "version_code": "290",
        "block": {
            "summary_rate": "%0,02",
            "categories": [],
        },
    }
    latest = play_latest_anr_from_vitals(vitals)
    assert latest is not None
    assert latest["version_code"] == "290"
    assert latest["anr_rate_pct"] == 0.02
    assert latest["anr_free_fmt"] == "99,98%"
    assert latest["period"] == "7d"
    assert latest["source"] == "play_vitals_scrape_7d"


def test_latest_anr_from_summary_rate_cards():
    vitals = {
        "version_code": "290",
        "versions": [{"code": "290", "name": "9.5.10"}],
        "version_name_map": {"290": "9.5.10"},
        "by_version": {
            "290": {
                "crashes": {
                    "ANR": {
                        "categories": [
                            {
                                "id": "general",
                                "cards": [
                                    {
                                        "title": "Kullanıcı tarafından algılanan ANR oranı",
                                        "value": "%0,02",
                                    },
                                    {"title": "Etkilenen kullanıcılar", "value": "1.234"},
                                ],
                            }
                        ]
                    }
                }
            }
        },
    }
    latest = play_latest_anr_from_vitals(vitals)
    assert latest is not None
    assert latest["anr_rate_pct"] == 0.02
    assert latest["anr_free_fmt"] == "99,98%"
    assert latest["version_code"] == "290"


def test_latest_does_not_fall_back_to_older_version():
    vitals = {
        "version_code": "290",
        "versions": [{"code": "290", "name": "9.5.10"}, {"code": "289", "name": "9.5.9"}],
        "by_version": {
            "289": {
                "crashes": {"ANR": {"summary_rate": "%0,05", "categories": []}}
            }
        },
    }
    assert play_latest_anr_from_vitals(vitals) is None


def test_play_versions_sorted_newest_first():
    rows = play_versions_anr_from_vitals(_vitals_fixture())
    assert [r["version_code"] for r in rows] == ["290", "289"]
    assert rows[1]["anr_rate_pct"] == 0.05


def test_normalize_keeps_summary_rate_and_version_overview():
    raw = {
        "version_code": "290",
        "versions": [{"code": "290", "name": "9.5.10"}],
        "version_name_map": {"290": "9.5.10"},
        "crashes": {
            "ANR": {
                "summary_rate": "%0,03",
                "categories": [{"id": "general", "label": "Genel", "cards": [], "issues": []}],
            }
        },
        "by_version": {
            "290": {
                "crashes": {
                    "ANR": {
                        "summary_rate": "%0,03",
                        "categories": [
                            {
                                "id": "general",
                                "label": "Genel",
                                "cards": [
                                    {
                                        "title": "Kullanıcı tarafından algılanan ANR oranı",
                                        "value": "%0,03",
                                    }
                                ],
                                "issues": [],
                            }
                        ],
                    }
                }
            }
        },
        "metrics_overview": {
            "rows": [
                {
                    "key": "anr",
                    "metric": "Kullanıcı tarafından algılanan ANR oranı",
                    "value_28d": "%0,04",
                }
            ]
        },
        "metrics_overview_by_version": {
            "290": {
                "url": "https://example/overview?versionCode=290",
                "rows": [
                    {
                        "key": "anr",
                        "metric": "Kullanıcı tarafından algılanan ANR oranı",
                        "value_28d": "%0,03",
                    }
                ],
            }
        },
    }
    out = _normalize_vitals(raw)
    assert out["crashes"]["ANR"]["summary_rate"] == "%0,03"
    assert out["by_version"]["290"]["crashes"]["ANR"]["summary_rate"] == "%0,03"
    assert out["metrics_overview_by_version"]["290"]["rows"][0]["value_28d"] == "%0,03"
    latest = play_latest_anr_from_vitals(out)
    assert latest is not None
    assert latest["anr_free_fmt"] == "99,97%"
