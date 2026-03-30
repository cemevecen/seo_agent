#!/usr/bin/env python3
"""Regression test for Search Console refresh alert visibility."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent-publish")

from backend.collectors.search_console import collect_search_console_metrics
from backend.database import Base
from backend.models import Site
from backend.services.warehouse import get_latest_search_console_rows


class SearchConsoleRefreshAlertSyncTest(unittest.TestCase):
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

    def test_refresh_commits_new_query_rows_before_alert_evaluation(self) -> None:
        payload = {
            "source": "live",
            "error": None,
            "rows": [
                {"query": "brand query", "device": "DESKTOP", "clicks": 40, "impressions": 400, "ctr": 10.0, "position": 2.0},
            ],
            "current_7d_rows": [
                {"query": "brand query", "device": "DESKTOP", "clicks": 40, "impressions": 400, "ctr": 10.0, "position": 3.5},
            ],
            "previous_rows": [
                {"query": "brand query", "device": "DESKTOP", "clicks": 10, "impressions": 100, "ctr": 10.0, "position": 1.8},
            ],
            "previous_7d_rows": [
                {"query": "brand query", "device": "DESKTOP", "clicks": 60, "impressions": 600, "ctr": 10.0, "position": 1.5},
            ],
            "trend_7d_rows": [],
            "site_url": "sc-domain:example.com",
            "start_date": "2026-03-01",
            "end_date": "2026-03-28",
            "previous_date": "2026-03-27",
            "current_7d_start": "2026-03-22",
            "current_7d_end": "2026-03-28",
            "previous_7d_start": "2026-03-15",
            "previous_7d_end": "2026-03-21",
        }
        observed = {}

        def fake_evaluate(db, site, *, send_notifications=True):
            rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="current_7d")
            observed["queries"] = [row["query"] for row in rows]
            observed["positions"] = [row["position"] for row in rows]
            return []

        with patch("backend.collectors.search_console.consume_api_quota", return_value=SimpleNamespace(allowed=True, reason="")), \
             patch("backend.collectors.search_console.get_search_console_credentials_record", return_value=None), \
             patch("backend.collectors.search_console._load_search_console_data", return_value=payload), \
             patch("backend.collectors.search_console.evaluate_site_alerts", side_effect=fake_evaluate):
            result = collect_search_console_metrics(self.db, self.site)

        self.assertEqual(result["source"], "live")
        self.assertEqual(observed["queries"], ["brand query"])
        self.assertEqual(observed["positions"], [3.5])


if __name__ == "__main__":
    unittest.main()
