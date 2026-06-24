"""Sinemalar.com arama HTML ayrıştırma ve eşleştirme."""

from backend.services import sinemalar_match as sm


SAMPLE_HTML = """
<a href="https://www.sinemalar.com/film/10132/dune">
    <img alt="Dune (2021) afişi" src="x">
    <a class="item-title link" href="https://www.sinemalar.com/film/10132/dune">Dune</a>
</a>
<a href="https://www.sinemalar.com/dizi/25191/breaking-bad">
    <a class="item-title link" href="https://www.sinemalar.com/dizi/25191/breaking-bad">Breaking Bad</a>
</a>
"""


def test_parse_search_html():
    hits = sm._parse_search_html(SAMPLE_HTML)
    assert len(hits) == 2
    assert hits[0]["kind"] == "film"
    assert hits[0]["sinemalar_id"] == 10132
    assert hits[0]["year"] == "2021"
    assert hits[1]["kind"] == "dizi"


def test_pick_best_movie_year():
    hits = sm._parse_search_html(SAMPLE_HTML)
    match = sm._pick_best(
        title="Dune",
        original_title="Dune: Part One",
        year="2021",
        media_type="movie",
        hits=hits,
    )
    assert match is not None
    assert match["sinemalar_id"] == 10132


def test_pick_best_tv_prefers_dizi():
    hits = sm._parse_search_html(SAMPLE_HTML)
    match = sm._pick_best(
        title="Breaking Bad",
        original_title="",
        year="",
        media_type="tv",
        hits=hits,
    )
    assert match is not None
    assert match["kind"] == "dizi"


DETAIL_VIZYON_HTML = """
<div><b>Vizyon Tarihi:</b>
01 Mart 2024
</div>
"""

DETAIL_EMPTY_VIZYON_HTML = """
<div><b>Vizyon Tarihi:</b>
</div>
"""


def test_parse_release_from_detail():
    out = sm._parse_release_from_detail_html(DETAIL_VIZYON_HTML, kind="film")
    assert out["sinemalar_has_release_date"] is True
    assert out["sinemalar_release_label"] == "01 Mart 2024"
    assert out["sinemalar_release_date"] == "2024-03-01"

    empty = sm._parse_release_from_detail_html(DETAIL_EMPTY_VIZYON_HTML, kind="film")
    assert empty["sinemalar_has_release_date"] is False


def test_lookup_uses_mocked_http(monkeypatch):
    sm._cache.clear()

    def fake_get(url, **kwargs):
        class R:
            status_code = 200

            def raise_for_status(self):
                pass

            @property
            def text(self):
                if "/ara?" in url:
                    return SAMPLE_HTML
                return DETAIL_VIZYON_HTML

        return R()

    monkeypatch.setattr(sm.requests, "get", fake_get)
    out = sm.lookup(title="Dune", release_date="2021-10-01", media_type="movie")
    assert out["sinemalar_found"] is True
    assert "film/10132" in (out["sinemalar_url"] or "")
    assert out["sinemalar_has_release_date"] is True
    assert out["sinemalar_release_date"] == "2024-03-01"

    out2 = sm.lookup(title="Dune", release_date="2021-10-01", media_type="movie")
    assert out2["sinemalar_found"] is True
