"""ASGI entry selector.

ProjectControl (default): backend.main:app
TAMPIQ public site: PUBLIC_TAMPIQ_SITE=true → backend.tampiq_public_app:app
"""
from __future__ import annotations

import os

_TRUE = {"1", "true", "yes", "on"}


def _truthy(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in _TRUE


if _truthy("PUBLIC_TAMPIQ_SITE"):
    from backend.tampiq_public_app import app as app
else:
    from backend.main import app as app
