#!/usr/bin/env python3
"""Regression tests for operations notifier email wording."""

import sys
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.models import CollectorRun
from backend.services.operations_notifier import _missed_run_reason


class OperationsNotifierMessagingTest(unittest.TestCase):
    def test_missing_run_reason_is_explanatory_when_no_run_exists(self) -> None:
        reason = _missed_run_reason(None)
        self.assertIn("hicbir calisma kaydi olusmadi", reason)
        self.assertIn("scheduler", reason)

    def test_missing_run_reason_mentions_last_status_when_run_exists(self) -> None:
        run = CollectorRun(
            provider="url_inspection",
            strategy="homepage",
            status="failed",
            requested_at=datetime(2026, 3, 30, 8, 0, tzinfo=ZoneInfo("UTC")).replace(tzinfo=None),
        )
        reason = _missed_run_reason(run)
        self.assertIn("Son gorulen durum: failed", reason)


if __name__ == "__main__":
    unittest.main()
