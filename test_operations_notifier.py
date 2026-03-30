#!/usr/bin/env python3
"""Regression tests for operations notifier."""

import sys
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.database import Base
from backend.models import Site, SiteCredential
from backend.services import operations_notifier


class OperationsNotifierTest(unittest.TestCase):
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

    def test_missed_schedule_email_is_sent_once_per_day(self) -> None:
        self.db.add(
            SiteCredential(
                site_id=self.site.id,
                credential_type="search_console",
                encrypted_data="{}",
            )
        )
        self.db.commit()

        fixed_now = datetime(2026, 3, 30, 6, 0, tzinfo=ZoneInfo("Europe/Istanbul"))

        with patch.object(operations_notifier.settings, "scheduled_refresh_monitor_enabled", True), \
             patch.object(operations_notifier.settings, "scheduled_refresh_monitor_grace_minutes", 30), \
             patch.object(operations_notifier.settings, "search_console_scheduled_refresh_enabled", True), \
             patch.object(operations_notifier.settings, "search_console_scheduled_refresh_hour", 4), \
             patch.object(operations_notifier.settings, "search_console_scheduled_refresh_minute", 0), \
             patch.object(operations_notifier.settings, "alerts_scheduled_refresh_enabled", False), \
             patch.object(operations_notifier.settings, "scheduled_refresh_enabled", False), \
             patch("backend.services.operations_notifier.now_local", return_value=fixed_now), \
             patch("backend.services.operations_notifier.send_email", return_value=True) as send_email:
            first_subjects = operations_notifier.notify_missed_scheduled_refreshes(self.db)
            second_subjects = operations_notifier.notify_missed_scheduled_refreshes(self.db)

        self.assertEqual(len(first_subjects), 1)
        self.assertEqual(second_subjects, [])
        self.assertEqual(send_email.call_count, 1)


if __name__ == "__main__":
    unittest.main()
