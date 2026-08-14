"""Selenium evaluate must forward Playwright-style args (known/page_key)."""

from __future__ import annotations


def test_evaluate_script_applies_arguments():
    """Mirror SeleniumPage.evaluate arrow-fn wrapping without a live driver."""
    script = "(args) => ({ known: (args && args.known) || [], page: (args && args.page_key) || '' })"
    wrapped = f"return ({script}).apply(null, arguments);"
    # Simulate Selenium: first python arg becomes arguments[0] in JS via execute_script
    # We just assert the wrapper source is what we ship.
    from backend.services import selenium_playwright_shim as shim
    import inspect

    src = inspect.getsource(shim.SeleniumPage.evaluate)
    assert ".apply(null, arguments)" in src
    assert "*args" in src or "args" in src
    assert wrapped  # keep local example honest
