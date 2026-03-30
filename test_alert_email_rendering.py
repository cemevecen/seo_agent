#!/usr/bin/env python3
"""Regression tests for richer alert email parsing and rendering."""

import sys
import unittest

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.services.alert_engine import _alert_email_row, _parse_alert_message


class AlertEmailRenderingTest(unittest.TestCase):
    def test_ctr_alert_is_expanded_into_explicit_columns(self) -> None:
        parsed = _parse_alert_message(
            "[NEGATIVE] doviz.com CTR düşüşü - 'altin fiyatlari' (4998 clicks): CTR 22.400 → 20.097 (-10.3%)",
            domain="doviz.com",
        )
        row = _alert_email_row(parsed)

        self.assertEqual(parsed["metric_type"], "CTR")
        self.assertEqual(parsed["before"], "22.400%")
        self.assertEqual(parsed["after"], "20.097%")
        self.assertEqual(parsed["delta"], "-10.3%")
        self.assertIn("4,998 click", parsed["extra"])
        self.assertEqual(len(row), 7)

    def test_threshold_alert_shows_threshold_and_current_value(self) -> None:
        parsed = _parse_alert_message(
            "doviz.com için Mobile PageSpeed kritik seviyede. Mevcut değer: 47.00, eşik: 50.00.",
            domain="doviz.com",
        )

        self.assertEqual(parsed["metric_type"], "Mobile PageSpeed kritik seviyede")
        self.assertEqual(parsed["before"], "Esik 50.00")
        self.assertEqual(parsed["after"], "47.00")
        self.assertEqual(parsed["delta"], "-3.00")


if __name__ == "__main__":
    unittest.main()
