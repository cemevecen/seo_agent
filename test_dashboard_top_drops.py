#!/usr/bin/env python3
"""Regression tests for dashboard drop styling semantics."""

import sys
import unittest

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.main import _build_dashboard_top_drops


class DashboardTopDropsTest(unittest.TestCase):
    def test_position_drop_uses_negative_palette(self) -> None:
        site_cards = [
            {
                "domain": "example.com",
                "top_queries": [
                    {
                        "query": "altin fiyatlari",
                        "clicks_current": 100800.0,
                        "clicks_previous": 100800.0,
                        "clicks_diff": 0.0,
                        "position_current": 8.07,
                        "position_previous": 7.19,
                        "position_diff": 0.88,
                    }
                ],
            }
        ]

        items = _build_dashboard_top_drops(site_cards, limit=6)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["reason"], "Pozisyon düşüşü")
        self.assertIn("rose", items[0]["classes"]["badge"])


if __name__ == "__main__":
    unittest.main()
