"""TMDB-only Google üyeleri — yol ve e-posta allowlist."""

from backend.services import app_member_auth as ama
from backend.services import tmdb_guest_auth as tga


def test_tmdb_only_email_in_allowlist():
    assert ama.is_tmdb_only_member_email("gozdeunaldi@nokta.com")
    assert ama.is_tmdb_only_member_email("GozdeUnaldi@nokta.com")
    assert not ama.is_tmdb_only_member_email("other@nokta.com")


def test_tmdb_only_member_path_allowed():
    assert tga.tmdb_only_member_path_allowed("/tmdb-upcoming")
    assert tga.tmdb_only_member_path_allowed("/static/js/foo.js")
    assert tga.tmdb_only_member_path_allowed("/api/tmdb-upcoming/sinemalar-lookup")
    assert tga.tmdb_only_member_path_allowed("/auth/logout")
    assert not tga.tmdb_only_member_path_allowed("/realtime")
    assert not tga.tmdb_only_member_path_allowed("/api/panel/online-users")
