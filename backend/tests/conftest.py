"""Shared pytest bootstrap for importing app modules safely.

Tests import modules that initialize the application settings at import time.
Provide non-secret defaults so unit tests can run in clean agent environments
without relying on a developer's private .env file.
"""

from __future__ import annotations

import os


os.environ.setdefault("GOOGLE_API_KEY", "test-google-api-key")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("SECRET_KEY", "test-secret-key")
os.environ.setdefault("SMTP_PASSWORD", "test-smtp-password")
os.environ.setdefault("ENCRYPTION_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
