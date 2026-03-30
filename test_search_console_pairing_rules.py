#!/usr/bin/env python3
"""Regression tests for Search Console query pairing rules."""

import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.database import Base
from backend.models import Alert, AlertLog, Site
from backend.services.alert_engine import _get_top_50_keywords_with_changes, get_recent_alerts


class SearchConsolePairingRulesTest(unittest.TestCase):
    def test_queries_without_previous_period_pair_are_excluded(self) -> None:
        site = SimpleNamespace(id=42)
        current_rows = [
            {"query": "paired query", "device": "MOBILE", "clicks": 40, "impressions": 400, "ctr": 0.10, "position": 4.0},
            {"query": "current only", "device": "MOBILE", "clicks": 30, "impressions": 300, "ctr": 0.10, "position": 3.0},
        ]
        previous_rows = [
            {"query": "paired query", "device": "MOBILE", "clicks": 60, "impressions": 600, "ctr": 0.10, "position": 2.0},
            {"query": "previous only", "device": "MOBILE", "clicks": 90, "impressions": 900, "ctr": 0.10, "position": 1.0},
        ]

        def fake_rows(_db, *, site_id, data_scope):
            self.assertEqual(site_id, 42)
            if data_scope == "current_7d":
                return current_rows
            if data_scope == "previous_7d":
                return previous_rows
            return []

        with patch("backend.services.alert_engine.get_latest_search_console_rows", side_effect=fake_rows):
            result = _get_top_50_keywords_with_changes(None, site)

        self.assertEqual([item["query"] for item in result["top_50"]], ["paired query"])
        self.assertEqual(result["dropped_queries"], [])

    def test_recent_alerts_hides_legacy_dropped_query_logs(self) -> None:
        engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        Base.metadata.create_all(bind=engine)
        db = SessionLocal()
        try:
            site = Site(domain="example.com", display_name="Example", is_active=True)
            db.add(site)
            db.commit()
            db.refresh(site)

            dropped_alert = Alert(site_id=site.id, alert_type="search_console_dropped_queries", threshold=1.0, is_active=True)
            ctr_alert = Alert(site_id=site.id, alert_type="search_console_ctr_drop", threshold=5.0, is_active=True)
            db.add_all([dropped_alert, ctr_alert])
            db.commit()
            db.refresh(dropped_alert)
            db.refresh(ctr_alert)

            db.add_all(
                [
                    AlertLog(
                        alert_id=dropped_alert.id,
                        domain=site.domain,
                        triggered_at=datetime.utcnow(),
                        message="[NEGATIVE] search_console_dropped_queries: 'ghost query'. Position: 2.0->N/A [M]",
                        sent_mail=False,
                    ),
                    AlertLog(
                        alert_id=ctr_alert.id,
                        domain=site.domain,
                        triggered_at=datetime.utcnow(),
                        message="[NEGATIVE] example.com CTR düşüşü - 'paired query' (100 clicks): CTR 10.000 → 8.000 (-20.0%)",
                        sent_mail=False,
                    ),
                ]
            )
            db.commit()

            visible = get_recent_alerts(db, limit=10)
        finally:
            db.close()

        self.assertEqual(len(visible), 1)
        self.assertEqual(visible[0]["alert_type"], "search_console_ctr_drop")


if __name__ == "__main__":
    unittest.main()
