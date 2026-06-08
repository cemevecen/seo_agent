"""TMDB platform streaming — sağlayıcı slug ve filtre listesi."""

from unittest.mock import patch

from backend.services.tmdb import (
    TR_STREAMING_PROVIDERS,
    _fetch_tr_ott_provider_names,
    _provider_slugs,
    streaming_provider_filters,
)


def test_streaming_provider_filters_major_ott():
    labels = {f["label"] for f in streaming_provider_filters()}
    slugs = {f["slug"] for f in streaming_provider_filters()}
    assert "Netflix" in labels
    assert "Prime" in labels
    assert "Disney+" in labels
    assert "Max (HBO)" in labels
    assert "Apple TV+" in labels
    assert "TV+" in labels
    assert "netflix" in slugs
    assert "tvplus" in slugs


def test_provider_slugs_pipe_join():
    assert _provider_slugs(["Netflix", "Max"]) == "netflix|max"
    assert _provider_slugs([]) == ""


def test_tr_streaming_providers_include_tv_plus():
    ids = {int(p["id"]) for p in TR_STREAMING_PROVIDERS}
    assert 1904 in ids  # TV+
    assert 1899 in ids  # Max


def test_fetch_tr_ott_provider_names_flatrate_only():
    payload = {
        "results": {
            "TR": {
                "flatrate": [
                    {"provider_id": 8, "provider_name": "Netflix"},
                    {"provider_id": 99999, "provider_name": "Other"},
                ],
                "rent": [{"provider_id": 119, "provider_name": "Prime Video"}],
            }
        }
    }
    with patch("backend.services.tmdb._get", return_value=payload):
        names = _fetch_tr_ott_provider_names("movie", 42)
    assert names == ["Netflix"]
