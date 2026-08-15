# -*- coding: utf-8 -*-
"""TMDB standalone vs /sinemalar?tab=movie erisim."""

from backend.services.app_member_auth import TMDB_ONLY_MEMBER_EMAILS, is_tmdb_only_member_email


def test_tmdb_only_emails_match_product_owners():
    assert "berendemirci@gmail.com" in TMDB_ONLY_MEMBER_EMAILS
    assert "gozdeunaldi@nokta.com" in TMDB_ONLY_MEMBER_EMAILS
    assert is_tmdb_only_member_email("BerenDemirci@gmail.com")
    assert is_tmdb_only_member_email("gozdeunaldi@nokta.com")
    assert not is_tmdb_only_member_email("cemevecen@nokta.com")
    assert not is_tmdb_only_member_email("onurtorun@nokta.com")


def test_shared_tmdb_partial_and_sinemalar_tab():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    partial = (root / "templates/partials/tmdb_upcoming_content.html").read_text(encoding="utf-8")
    wrap = (root / "templates/tmdb_upcoming.html").read_text(encoding="utf-8")
    policy = (root / "templates/partials/policy_content.html").read_text(encoding="utf-8")
    base = (root / "templates/base.html").read_text(encoding="utf-8")

    assert "partials/tmdb_upcoming_content.html" in wrap
    assert "tmdb-card" in partial or "film_card" in partial
    assert 'tab=movie' in policy
    assert "tmdb_upcoming_content.html" in policy
    # Header movie only for tmdb-only members, before settings
    assert "is_tmdb_only_member(request)" in base
    movie_idx = base.find('href="/tmdb-upcoming" data-nav-match="/tmdb-upcoming"')
    settings_idx = base.find('href="/settings"')
    assert movie_idx > 0 and settings_idx > movie_idx
