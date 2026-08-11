"""Mobil web: kaydırma tuzağı, responsive buton, soluk dark palet."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = (ROOT / "templates/base.html").read_text(encoding="utf-8")


def test_shell_uses_overflow_x_clip_not_hidden():
    assert 'id="seo-mobile-web"' in BASE
    assert "overflow-x: clip" in BASE
    assert 'id="app-shell"' in BASE
    assert "overflow-x-clip" in BASE
    assert 'id="app-shell" class="relative flex min-h-screen min-h-[100dvh] w-full max-w-[100vw] flex-col overflow-x-hidden"' not in BASE


def test_mobile_buttons_wrap_outside_horizontal_scroll():
    assert "white-space: normal" in BASE.split('id="seo-mobile-web"', 1)[1].split("</style>", 1)[0]
    assert "touch-action: pan-x pan-y" in BASE
    assert "viewport-fit=cover" in BASE


def test_dark_mode_uses_muted_not_neon():
    matte = BASE.split('id="seo-dark-matte"', 1)[1].split("</style>", 1)[0]
    assert "#6f9b86" in BASE
    assert "#b57a82" in BASE
    assert "box-shadow: none !important" in matte
    assert "#10b981" not in BASE.split('id="seo-dark-delta-tones"', 1)[1].split("</style>", 1)[0]
    assert "#34d399" not in BASE.split("review-star-badge[data-stars=\"5\"]", 1)[1].split("data-stars=\"0\"", 1)[0]


def test_app_refresh_does_not_hijack_nested_scroll():
    js = (ROOT / "static/js/app-refresh.js").read_text(encoding="utf-8")
    assert "isNestedVerticalScroller" in js
    assert "ev.cancelable" in js
    assert "app-refresh.js?v=2" in BASE
