"""Android kategori sırası — kaynak filtreleme (detay sayfası geçerli)."""

from backend.services.app_intel import (
    _android_cached_category_rank_is_obsolete,
    _android_category_rank_is_displayable,
    _android_histogram_overall,
    _extract_android_packages,
    _android_pkg_index,
)


def test_details_page_rank_is_displayable_and_not_obsolete():
    cr = {"rank": 161, "chart": "details_page", "category_name": "Finans"}
    assert _android_category_rank_is_displayable(cr)
    assert not _android_cached_category_rank_is_obsolete(cr)


def test_store_search_rank_rejected():
    cr = {"rank": 1, "chart": "store_search_package"}
    assert not _android_category_rank_is_displayable(cr)
    assert _android_cached_category_rank_is_obsolete(cr)


def test_chart_api_rank_is_displayable():
    cr = {
        "rank": 138,
        "chart": "category_top",
        "chart_label": "Ücretsiz",
        "rank_basis": "batchexecute_api",
    }
    assert _android_category_rank_is_displayable(cr)
    assert not _android_cached_category_rank_is_obsolete(cr)


def test_extract_android_packages_and_index():
    sample = r'[["com.ziraat.ziraatmobil"],[\"com.Doviz\"],["com.haremaltin.android.haremaltin"]]'
    # mixed escapes
    text = '[[\\"com.ziraat.ziraatmobil\\"],[\\"com.garanti.cepsubesi\\"],[\\"com.Doviz\\"]]'
    pkgs = _extract_android_packages(text)
    assert pkgs[0] == "com.ziraat.ziraatmobil"
    assert _android_pkg_index(pkgs, "com.Doviz") == 2
    assert sample  # silence unused in some linters


def test_android_histogram_overall_dict_and_list():
    assert _android_histogram_overall({"histogram": {"1": 40, "2": 9, "3": 53, "4": 149, "5": 1800}}) == {
        "1": 40,
        "2": 9,
        "3": 53,
        "4": 149,
        "5": 1800,
    }
    assert _android_histogram_overall({"histogram": [40, 9, 53, 149, 1800]}) == {
        "1": 40,
        "2": 9,
        "3": 53,
        "4": 149,
        "5": 1800,
    }
    assert _android_histogram_overall({"histogram": {}}) is None
