#!/usr/bin/env python3
"""Regression tests for forced manual refresh flows."""

import sys
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.database import Base
from backend.main import _refresh_site_detail_measurements
from backend.models import Site


class ManualRefreshForceBypassesCooldownTest(unittest.TestCase):
    def setUp(self) -> None:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
        )
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
        self.db = SessionLocal()
        self.site = Site(domain="example.com", display_name="Example", is_active=True)
        self.db.add(self.site)
        self.db.commit()
        self.db.refresh(self.site)

    def tearDown(self) -> None:
        self.db.close()

    def test_force_refresh_calls_search_console_even_when_cooldown_is_active(self) -> None:
        with patch("backend.main._latest_collector_run_recent", return_value=True), \
             patch("backend.main._metrics_fresh_within", return_value=True), \
             patch("backend.main.collect_search_console_metrics", return_value={"state": "live"}) as collect_search_console_metrics:
            result = _refresh_site_detail_measurements(
                self.db,
                self.site,
                include_pagespeed=False,
                include_crawler=False,
                include_search_console=True,
                force=True,
            )

        collect_search_console_metrics.assert_called_once_with(self.db, self.site)
        self.assertEqual(result["search_console"]["state"], "live")


if __name__ == "__main__":
    unittest.main()
