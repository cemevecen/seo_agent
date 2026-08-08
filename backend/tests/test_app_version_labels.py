"""App version code + name labeling (Play Console chip format)."""

from __future__ import annotations

from backend.services.gp_client import (
    app_version_code_key,
    format_app_version_label,
    relabel_app_version_analytics,
    segments_match_app_version,
)


def test_format_app_version_label():
    m = {"290": "9.5.10", "289": "9.5.9"}
    assert format_app_version_label("290", m) == "290 (9.5.10)"
    assert format_app_version_label("290 (9.5.10)", m) == "290 (9.5.10)"
    assert format_app_version_label("9.5.10", m) == "290 (9.5.10)"
    assert format_app_version_label("288", m) == "288"


def test_app_version_code_key_and_match():
    assert app_version_code_key("290 (9.5.10)") == "290"
    assert app_version_code_key("290") == "290"
    assert segments_match_app_version("290", "290 (9.5.10)")
    assert segments_match_app_version("290 (9.5.10)", "290")
    assert not segments_match_app_version("289", "290")


def test_relabel_payload():
    payload = {
        "dim": "app_version",
        "segment": "290",
        "series": [{"key": "290", "value": 1}],
        "compare": {"series": [{"key": "289", "value": 2}]},
        "facets": {"segments": ["290", "289"]},
    }
    out = relabel_app_version_analytics(
        payload,
        package_name="com.Doviz",
        dim="app_version",
    )
    # Map boş olabilir (credentials yok) — o zaman değişmez; mock map ile doğrula
    from backend.services import gp_client

    gp_client._VERSION_NAME_CACHE["com.Doviz"] = (
        __import__("time").time(),
        {"290": "9.5.10", "289": "9.5.9"},
    )
    out = relabel_app_version_analytics(
        payload,
        package_name="com.Doviz",
        dim="app_version",
    )
    assert out["series"][0]["key"] == "290 (9.5.10)"
    assert out["compare"]["series"][0]["key"] == "289 (9.5.9)"
    assert out["facets"]["segments"][0] == "290 (9.5.10)"
    assert out["segment"] == "290 (9.5.10)"
