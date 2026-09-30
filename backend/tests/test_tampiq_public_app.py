"""Isolated TAMPIQ public Support/Privacy ASGI app — no ProjectControl coupling."""
from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from backend.tampiq_public_app import app, public_site_context


ROOT = Path(__file__).resolve().parents[2]
PC_TEMPLATE_DIRS = [
    ROOT / "templates",
]


def test_public_pages_ok_no_auth():
    client = TestClient(app)
    for path in ("/", "/support", "/privacy", "/health"):
        res = client.get(path)
        assert res.status_code == 200, path


def test_support_and_privacy_render_branding_and_email():
    client = TestClient(app)
    ctx = public_site_context()
    support = client.get("/support")
    privacy = client.get("/privacy")
    assert ctx["app_name"] in support.text
    assert ctx["support_email"] in support.text
    assert "Privacy Policy" in privacy.text or "Privacy" in privacy.text
    assert ctx["privacy_email"] in privacy.text
    # No ProjectControl chrome
    for body in (support.text, privacy.text):
        assert "ProjectControl" not in body
        assert "projectcontrol" not in body.lower()
        assert "Dashboard" not in body
        assert "Login" not in body


def test_openapi_disabled():
    client = TestClient(app)
    assert client.get("/openapi.json").status_code == 404
    assert client.get("/docs").status_code in {404, 302}


def test_projectcontrol_templates_have_zero_tampiq_discovery():
    """Nav/home/sidebar/footer must not mention TAMPIQ or public support URLs."""
    banned = (
        "tampiq",
        "TAMPIQ",
        "/_public/tampiq",
        "tampiq-support",
        "tampiq_public",
    )
    skip_roots = {
        ROOT / "templates" / "tampiq",
    }
    hits: list[str] = []
    for base in PC_TEMPLATE_DIRS:
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if any(str(path).startswith(str(skip)) for skip in skip_roots):
                continue
            if path.suffix.lower() not in {".html", ".js", ".css", ".md"}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for token in banned:
                if token in text:
                    hits.append(f"{path.relative_to(ROOT)}:{token}")
    assert hits == [], f"ProjectControl UI discovery leaks: {hits}"


def test_main_module_does_not_mount_tampiq_routes():
    main_src = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert "tampiq_public" not in main_src
    assert "/support" not in main_src or "tampiq" not in main_src.lower()
    # Hard guarantee: no dedicated public tampiq paths in ProjectControl entry
    for needle in ('"/support"', "'/support'", "/privacy"):
        # /privacy alone might appear elsewhere; require tampiq context
        pass
    assert "TAMPIQ" not in main_src
    assert "tampiq" not in main_src.lower()
