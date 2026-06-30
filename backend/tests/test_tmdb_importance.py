"""TMDB takvim — planlama önemi (yüksek/orta/düşük) katmanları."""

from backend.services.tmdb import _importance_tier


def test_franchise_upcoming_high_by_popularity():
    # Gelecek vizyon: düşük trend ama bilinen IP (ör. animasyon serisi)
    assert _importance_tier({"popularity": 42, "vote_count": 6, "is_turkish": False}) == "high"


def test_established_title_high_by_votes():
    assert _importance_tier({"popularity": 12, "vote_count": 200, "is_turkish": False}) == "high"


def test_turkish_medium_even_with_low_pop():
    assert _importance_tier({"popularity": 3, "vote_count": 0, "is_turkish": True}) == "medium"


def test_niche_low():
    assert _importance_tier({"popularity": 2.5, "vote_count": 3, "is_turkish": False}) == "low"


def test_old_threshold_would_miss_franchise():
    """Eski eşik pop<80 → düşük; yeni kural pop≥35 → yüksek."""
    m = {"popularity": 55, "vote_count": 10, "is_turkish": False}
    assert _importance_tier(m) == "high"
