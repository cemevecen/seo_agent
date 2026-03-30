#!/usr/bin/env python3
"""Regression tests for sitewide crawler link audit metrics."""

import sys
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, "/Users/cemevecen/Desktop/seo_agent/seo-agent")

from backend.collectors import crawler
from backend.database import Base
from backend.models import CollectorRun, Metric, Site


class CrawlerLinkAuditTest(unittest.TestCase):
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

    def test_extract_internal_links_filters_external_and_duplicates(self) -> None:
        html = """
        <a href="/usd">USD</a>
        <a href="https://www.example.com/altin">Altın</a>
        <a href="mailto:test@example.com">Mail</a>
        <a href="/usd#yorum">USD tekrar</a>
        <a href="https://external.com/news">External</a>
        """
        links = crawler._extract_internal_links(html, "https://example.com")
        self.assertEqual(
            links,
            [
                "https://example.com/usd",
                "https://www.example.com/altin",
            ],
        )

    def test_collect_crawler_metrics_saves_broken_and_redirect_counts(self) -> None:
        homepage_html = """
        <a href="/usd">USD</a>
        <a href="/altin">Altın</a>
        <a href="/borsa">Borsa</a>
        """

        def fake_fetch(url: str):
            if url.endswith("/robots.txt"):
                return 200, "User-agent: *\nDisallow:"
            if url.endswith("/sitemap.xml"):
                return 200, "<urlset><url><loc>https://example.com/</loc></url></urlset>"
            if url.endswith("/kaynak-1"):
                return 200, '<a href="/usd">USD</a><a href="/altin">Altın</a>'
            if url.endswith("/kaynak-2"):
                return 200, '<a href="/altin">Altın</a><a href="/borsa">Borsa</a>'
            return 200, homepage_html

        def fake_probe(url: str):
            if url.endswith("/usd"):
                return {
                    "url": url,
                    "final_url": url,
                    "final_status": 404,
                    "history": [],
                    "hops": 0,
                    "redirect": False,
                    "redirect_chain": False,
                    "broken": True,
                }
            if url.endswith("/altin"):
                return {
                    "url": url,
                    "final_url": "https://example.com/altin/guncel",
                    "final_status": 200,
                    "history": [
                        {"status": 301, "url": url},
                        {"status": 302, "url": "https://example.com/altin-yeni"},
                    ],
                    "hops": 2,
                    "redirect": True,
                    "redirect_chain": True,
                    "broken": False,
                }
            return {
                "url": url,
                "final_url": url,
                "final_status": 200,
                "history": [],
                "hops": 0,
                "redirect": False,
                "redirect_chain": False,
                "broken": False,
            }

        with patch("backend.collectors.crawler._fetch_text", side_effect=fake_fetch), \
             patch("backend.collectors.crawler._seed_source_pages", return_value=(["https://example.com/kaynak-1", "https://example.com/kaynak-2"], "Search Console öncelikli URL listesi")), \
             patch("backend.collectors.crawler._probe_internal_link", side_effect=fake_probe), \
             patch("backend.collectors.crawler.evaluate_site_alerts"):
            result = crawler.collect_crawler_metrics(self.db, self.site)

        latest_metrics = {
            row.metric_type: row.value
            for row in self.db.query(Metric).filter(Metric.site_id == self.site.id).all()
        }
        run = (
            self.db.query(CollectorRun)
            .filter(CollectorRun.site_id == self.site.id, CollectorRun.provider == "crawler")
            .order_by(CollectorRun.id.desc())
            .first()
        )

        self.assertEqual(latest_metrics["crawler_source_pages_count"], 2.0)
        self.assertEqual(latest_metrics["crawler_audited_urls_count"], 5.0)
        self.assertEqual(latest_metrics["crawler_broken_links_count"], 1.0)
        self.assertEqual(latest_metrics["crawler_redirect_links_count"], 1.0)
        self.assertEqual(latest_metrics["crawler_redirect_301_count"], 1.0)
        self.assertEqual(latest_metrics["crawler_redirect_302_count"], 1.0)
        self.assertEqual(latest_metrics["crawler_redirect_chain_count"], 1.0)
        self.assertEqual(latest_metrics["crawler_redirect_max_hops"], 2.0)
        self.assertEqual(result["summary"]["link_audit"]["broken_links"], 1)
        self.assertEqual(result["summary"]["link_audit"]["source_pages"], 2)
        self.assertEqual(result["summary"]["link_audit"]["audited_urls"], 5)
        self.assertEqual(result["summary"]["link_audit"]["redirect_chains"], 1)
        self.assertEqual(result["summary"]["link_audit"]["source_strategy"], "Search Console öncelikli URL listesi")
        self.assertIsNotNone(run)
        self.assertIn("link_audit", run.summary_json)


if __name__ == "__main__":
    unittest.main()
