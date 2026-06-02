"""TMDB platform streaming — sağlayıcı slug ve filtre listesi."""

from backend.services.tmdb import (
    TR_STREAMING_PROVIDERS,
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
