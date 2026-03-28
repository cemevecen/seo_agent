#!/usr/bin/env python3
"""Test message parsing with new format."""

import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.api.alerts import _extract_query_details_from_message

# Test cases with new format
test_messages = [
    "[NEGATIVE] search_console_position_change: 'döviz'. Position: 1.0->1.0",
    "[POSITIVE] search_console_position_change: 'harem altın'. Position: 2.1->2.0",
    "[NEGATIVE] search_console_dropped_queries: 'romantik filmler'. Position: 5.0->N/A",
    "[POSITIVE] search_console_position_change: 'altın fiyatları'. Position: 8.7->8.0",
]

print("=== Testing message parsing with new format ===\n")

for msg in test_messages:
    print(f"Message: {msg}")
    result = _extract_query_details_from_message(msg)
    print(f"Result: {result}")
    print()
