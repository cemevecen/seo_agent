#!/usr/bin/env python3
"""Regression tests for unified position semantics."""

import sys
import unittest

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.api.alerts import _position_change_state


class PositionSemanticsTest(unittest.TestCase):
    def test_smaller_position_is_improvement(self) -> None:
        self.assertEqual(_position_change_state(-1.1), "improved")

    def test_larger_position_is_worsened(self) -> None:
        self.assertEqual(_position_change_state(2.0), "worsened")

    def test_equal_position_is_neutral(self) -> None:
        self.assertEqual(_position_change_state(0.0), "neutral")


if __name__ == "__main__":
    unittest.main()
