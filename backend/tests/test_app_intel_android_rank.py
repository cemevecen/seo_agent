"""Android kategori sırası — kaynak filtreleme (detay sayfası geçerli)."""

from backend.services.app_intel import (
    _android_cached_category_rank_is_obsolete,
    _android_category_rank_is_displayable,
)


def test_details_page_rank_is_displayable_and_not_obsolete():
    cr = {"rank": 161, "chart": "details_page", "category_name": "Finans"}
    assert _android_category_rank_is_displayable(cr)
    assert not _android_cached_category_rank_is_obsolete(cr)


def test_store_search_rank_rejected():
    cr = {"rank": 1, "chart": "store_search_package"}
    assert not _android_category_rank_is_displayable(cr)
    assert _android_cached_category_rank_is_obsolete(cr)
