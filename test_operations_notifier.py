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

    def test_trigger_email_formats_numbers_and_adds_comparison_table(self) -> None:
        result = {
            "summary": {
                "search_console_clicks_28d": 1300254.0,
                "search_console_impressions_28d": 27339832.0,
                "search_console_avg_ctr_28d": 4.7558960859,
                "search_console_avg_position_28d": 4.5017189867,
            },
            "comparison": {
                "current_7d_summary": {
                    "clicks": 1300254.0,
                    "impressions": 27339832.0,
                    "ctr": 4.7558960859,
                    "position": 4.5017189867,
                },
                "previous_7d_summary": {
                    "clicks": 1200000.0,
                    "impressions": 26000000.0,
                    "ctr": 4.9989,
                    "position": 4.91,
                },
            },
            "source": "live",
        }

        with patch("backend.services.operations_notifier.send_email", return_value=True) as send_email:
            sent = operations_notifier.notify_system_trigger(
                trigger_source="manual",
                system_key="search_console",
                site=self.site,
                result=result,
                action_label="Search Console verisini yenile",
            )

        self.assertTrue(sent)
        self.assertEqual(send_email.call_count, 1)
        html = send_email.call_args.args[1]
        self.assertIn("1.300.254", html)
        self.assertIn("27.339.832", html)
        self.assertIn("%4,76", html)
        self.assertIn("4,5", html)
        self.assertIn("Karşılaştırmalı Veri", html)
        self.assertIn("Önceki 7 Gün", html)
        self.assertIn("Son 7 Gün", html)
        self.assertIn("-0,24 puan (düşüş)", html)

    def test_crawler_audit_emails_include_final_url_and_source_pages(self) -> None:
        result = {
            "collector_run_id": 77,
            "summary": {
                "link_audit": {
                    "source_pages": 40,
                    "audited_urls": 250,
                    "redirect_301_links": 3,
                    "redirect_302_links": 1,
                    "redirect_chains": 1,
                    "broken_links": 2,
                    "max_hops": 2,
                    "source_strategy": "Search Console öncelikli URL listesi",
                    "broken_samples": [
                        {
                            "url": "https://example.com/eski",
                            "final_url": "https://example.com/eski",
                            "final_status": 404,
                            "source_urls": ["https://example.com/kaynak-a", "https://example.com/kaynak-b"],
                            "source_count": 2,
                        }
                    ],
                    "redirect_samples": [
                        {
                            "url": "https://example.com/yonlen",
                            "final_url": "https://example.com/yeni",
                            "final_status": 200,
                            "chain": "301 -> 302 -> 200",
                            "source_urls": ["https://example.com/kaynak-c"],
                            "source_count": 1,
                            "issue_label": "Redirect zinciri, 302, 301",
                        }
                    ],
                }
            },
        }

        with patch("backend.services.operations_notifier.send_email", return_value=True) as send_email:
            subjects = operations_notifier.notify_crawler_audit_emails(
                db=self.db,
                site=self.site,
                result=result,
                trigger_source="system",
            )

        self.assertEqual(len(subjects), 1)
        self.assertEqual(send_email.call_count, 1)
        combined_html = "\n".join(call.args[1] for call in send_email.call_args_list)
        self.assertIn("Final URL", combined_html)
        self.assertIn("https://example.com/yeni", combined_html)
        self.assertIn("https://example.com/kaynak-a", combined_html)
        self.assertIn("301 / 302", combined_html)


if __name__ == "__main__":
    unittest.main()
