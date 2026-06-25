"""TMDB Türkiye vizyon tarihi çıkarımı (release_dates)."""

from backend.services.tmdb import (
    _earliest_tr_release_by_types,
    _tr_movie_release_dates,
    _RELEASE_TYPE_DIGITAL,
    _RELEASE_TYPE_THEATRICAL,
)


def test_tr_theatrical_earliest():
    payload = {
        "results": [
            {
                "iso_3166_1": "TR",
                "release_dates": [
                    {"type": 3, "release_date": "2026-05-22T00:00:00.000Z"},
                    {"type": 3, "release_date": "2026-05-15T00:00:00.000Z"},
                ],
            },
            {
                "iso_3166_1": "US",
                "release_dates": [
                    {"type": 3, "release_date": "2026-05-01T00:00:00.000Z"},
                ],
            },
        ]
    }
    assert _earliest_tr_release_by_types(payload, _RELEASE_TYPE_THEATRICAL) == "2026-05-15"


def test_tr_digital_separate_from_theatrical():
    payload = {
        "results": [
            {
                "iso_3166_1": "TR",
                "release_dates": [
                    {"type": 3, "release_date": "2026-06-01T00:00:00.000Z"},
                    {"type": 4, "release_date": "2026-05-20T00:00:00.000Z"},
                ],
            }
        ]
    }
    theat, digital = _tr_movie_release_dates(payload)
    assert theat == "2026-06-01"
    assert digital == "2026-05-20"


def test_no_tr_returns_none():
    payload = {"results": [{"iso_3166_1": "US", "release_dates": [{"type": 3, "release_date": "2026-01-01T00:00:00.000Z"}]}]}
    assert _earliest_tr_release_by_types(payload, _RELEASE_TYPE_THEATRICAL) is None
    assert _tr_movie_release_dates(payload) == (None, None)
