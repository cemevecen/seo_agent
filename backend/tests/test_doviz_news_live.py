"""Tests for haber.doviz.com live gap fill."""

from __future__ import annotations

from backend.services.doviz_news_live import (
    _first_news_article_ld,
    _parse_iso_dt,
    discover_live_article_refs,
    fetch_article_row,
    merge_sheet_with_live,
)


def test_parse_iso_dt_with_offset():
    dt = _parse_iso_dt("2026-08-07T14:54:00+03:00")
    assert dt is not None
    assert dt.hour == 14
    assert dt.minute == 54


def test_first_news_article_ld_extracts_created():
    html = """
    <script type="application/ld+json">
    {"@type":"NewsArticle","headline":"Test başlık","articleSection":"Gündem Haberleri",
     "dateCreated":"2026-08-07T14:54:00+03:00","datePublished":"2026-08-07T16:32:35+03:00"}
    </script>
    """
    ld = _first_news_article_ld(html)
    assert ld is not None
    assert ld["headline"] == "Test başlık"
    assert ld["dateCreated"].startswith("2026-08-07T14:54")


def test_merge_sheet_with_live_adds_missing_and_updates_newer_date():
    sheet = [
        {
            "id": "910272",
            "title": "Sheet eski",
            "date": "2026-08-07 08:47:00",
            "category": "Gündem Haberleri",
        }
    ]
    live = [
        {
            "id": "910272",
            "title": "Should keep title",
            "date": "2026-08-07 09:00:00",
            "date_day": "2026-08-07",
            "hour": 9,
            "category": "Gündem Haberleri",
        },
        {
            "id": "910489",
            "title": "Özgür Özel Le Monde",
            "date": "2026-08-07 14:54:00",
            "category": "Gündem Haberleri",
        },
    ]
    merged = merge_sheet_with_live(sheet, live)
    assert len(merged) == 2
    assert merged[0]["id"] == "910489"
    assert merged[0]["title"] == "Özgür Özel Le Monde"
    # Aynı ID: yayın saati daha yeni → tarih güncellenir, başlık sheet’ten kalır
    old = next(r for r in merged if r["id"] == "910272")
    assert old["title"] == "Sheet eski"
    assert old["date"] == "2026-08-07 09:00:00"


def test_discover_live_article_refs_from_html(monkeypatch):
    html = """
    <a href="https://haber.doviz.com/gundem-haberleri/ozgur-ozel/910489">x</a>
    <a href="/borsa-haberleri/thy/910469">y</a>
    <a href="https://haber.doviz.com/gundem-haberleri/ozgur-ozel/910489">dup</a>
    """

    class FakeResp:
        def raise_for_status(self):
            return None

        @property
        def text(self):
            return html

    class FakeSess:
        def get(self, url, timeout=18):
            return FakeResp()

    refs = discover_live_article_refs(limit=10, session=FakeSess())  # type: ignore[arg-type]
    ids = [r["id"] for r in refs]
    assert ids[0] == "910489"
    assert "910469" in ids
    assert ids.count("910489") == 1


def test_fetch_article_row_uses_date_published(monkeypatch):
    html = """
    <script type="application/ld+json">
    {"@type":"NewsArticle","headline":"Özgür Özel'den Le Monde",
     "articleSection":"Gündem Haberleri",
     "dateCreated":"2026-08-07T14:54:00+03:00",
     "datePublished":"2026-08-07T16:32:35+03:00"}
    </script>
    """

    class FakeResp:
        def raise_for_status(self):
            return None

        @property
        def text(self):
            return html

    class FakeSess:
        def get(self, url, timeout=18):
            return FakeResp()

    row = fetch_article_row(
        {"id": "910489", "url": "https://haber.doviz.com/gundem-haberleri/x/910489"},
        session=FakeSess(),  # type: ignore[arg-type]
    )
    assert row is not None
    assert row["id"] == "910489"
    # Yayına alma saati (datePublished), taslak/created değil
    assert row["date"] == "2026-08-07 16:32:35"
    assert row["category"] == "Gündem Haberleri"
    assert row.get("_live") is True
