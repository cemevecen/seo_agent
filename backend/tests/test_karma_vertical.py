"""Trend dikey filtre — doviz vs sinemalar içerik ayrımı."""

from types import SimpleNamespace

from backend.karma.vertical import ContentVertical, intel_row_matches_vertical


def _row(**kwargs):
    defaults = {
        "headline": "",
        "topic": "",
        "category": "",
        "content": "",
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_finance_includes_politics_and_world():
    assert intel_row_matches_vertical(
        _row(headline="Meclis'te yeni ekonomi paketi görüşülüyor", category="Politika & Ekonomi"),
        ContentVertical.FINANCE,
    )
    assert intel_row_matches_vertical(
        _row(headline="NATO zirvesinde ticaret yaptırımları gündemde", category="Dünya"),
        ContentVertical.FINANCE,
    )


def test_finance_includes_weather():
    assert intel_row_matches_vertical(
        _row(headline="MGM: İstanbul'da kar yağışı bekleniyor", category="Meteoroloji"),
        ContentVertical.FINANCE,
    )


def test_finance_excludes_pure_entertainment():
    assert not intel_row_matches_vertical(
        _row(headline="Netflix'te yeni dizi fragmanı yayınlandı", category="Sinema & Eğlence"),
        ContentVertical.FINANCE,
    )
    assert not intel_row_matches_vertical(
        _row(headline="Oscar adayları açıklandı — vizyon takvimi", category="Genel"),
        ContentVertical.FINANCE,
    )


def test_entertainment_includes_platform_and_vizyon():
    assert intel_row_matches_vertical(
        _row(headline="Exxen'de yeni dizi sezonu başlıyor", category="Sinema & Eğlence"),
        ContentVertical.ENTERTAINMENT,
    )
    assert intel_row_matches_vertical(
        _row(headline="Vizyon takvimi: Marvel filmi sinemalarda", category="Genel"),
        ContentVertical.ENTERTAINMENT,
    )


def test_entertainment_excludes_sports_and_politics():
    for headline in (
        "Trabzonspor'dan Zhegrova hamlesi",
        "Galatasaray Başkanı Dursun Özbek açıklama yaptı",
        "Fenerbahçe Amrabat transferini duyurdu",
        "Transfer sezonu kapandı: Beşiktaş Lauriente'yi kadrosuna kattı",
        "36. NATO Zirvesi'ne Ankara ev sahipliği yapacak",
    ):
        assert not intel_row_matches_vertical(_row(headline=headline, category="Türkiye"), ContentVertical.ENTERTAINMENT)


def test_entertainment_includes_imdb_and_film():
    assert intel_row_matches_vertical(
        _row(headline="IMDb'ye göre zirvede tek başına! Yeşilçam'ın en yüksek puanlı filmi belli oldu"),
        ContentVertical.ENTERTAINMENT,
    )


def test_entertainment_excludes_pure_finance_and_weather():
    assert not intel_row_matches_vertical(
        _row(headline="TCMB faiz kararı açıklandı", category="Finans & Borsa"),
        ContentVertical.ENTERTAINMENT,
    )
    assert not intel_row_matches_vertical(
        _row(headline="Hava durumu: yağmur bekleniyor", category="Meteoroloji"),
        ContentVertical.ENTERTAINMENT,
    )
