"""News intelligence RSS/Atom parse testleri."""

from datetime import timedelta

import xml.etree.ElementTree as ET

from backend.services import news_intelligence as ni


def test_feed_item_nodes_rss():
    root = ET.fromstring(
        """<rss><channel><item><title>A</title><link>http://a</link></item></channel></rss>"""
    )
    nodes, fmt = ni.feed_item_nodes(root)
    assert fmt == "rss"
    assert len(nodes) == 1


def test_feed_item_nodes_atom():
    root = ET.fromstring(
        """<feed xmlns="http://www.w3.org/2005/Atom">
        <entry><title>B</title><link href="http://b"/></entry></feed>"""
    )
    nodes, fmt = ni.feed_item_nodes(root)
    assert fmt == "atom"
    assert len(nodes) == 1


def test_extract_atom_fields():
    root = ET.fromstring(
        """<feed xmlns="http://www.w3.org/2005/Atom">
        <title>NTV</title>
        <entry>
          <title>Son dakika haber</title>
          <link href="https://www.ntv.com.tr/haber"/>
          <published>2026-05-23T19:39:15+03:00</published>
        </entry></feed>"""
    )
    entry, fmt = ni.feed_item_nodes(root)
    assert fmt == "atom"
    title, link, pub, _desc, src, _src_url = ni.extract_item_fields(
        entry[0], "atom", ch_title="NTV", ch_link="https://www.ntv.com.tr"
    )
    assert title == "Son dakika haber"
    assert link == "https://www.ntv.com.tr/haber"
    assert "2026" in pub
    assert src == "NTV"


def test_news_dedup_key_same_headline():
    a = ni.news_dedup_key("Doviz.com", "Jeopolitik gerilimler kripto varlıkları vurdu")
    b = ni.news_dedup_key("Doviz.com", "  Jeopolitik   gerilimler kripto varlıkları vurdu  ")
    assert a == b


def test_dedupe_news_rows_keeps_newest_first():
    class Row:
        def __init__(self, source_name, headline):
            self.source_name = source_name
            self.headline = headline

    rows = [
        Row("Doviz.com", "Aynı haber"),
        Row("Doviz.com", "Aynı haber"),
        Row("Doviz.com", "Başka haber"),
    ]
    out = ni.dedupe_news_rows(rows)
    assert len(out) == 2
    assert out[0].headline == "Aynı haber"
    assert out[1].headline == "Başka haber"


def test_extract_item_image_url_from_description():
    root = ET.fromstring(
        """<rss><channel><item>
        <title>X</title><link>http://a</link>
        <description><![CDATA[<p>Özet</p><img src="https://cdn.example.com/cover.jpg" alt="">]]></description>
        </item></channel></rss>"""
    )
    item = root.find(".//item")
    url = ni._extract_item_image_url(item, "rss", item.findtext("description") or "")
    assert url == "https://cdn.example.com/cover.jpg"


def test_retention_hours_is_twelve():
    assert ni.RETENTION_HOURS == 12


def test_parse_pub_date_aware_vs_naive_cutoff():
    published = ni.parse_pub_date("2026-06-03T19:39:15+03:00")
    assert published is not None
    assert published.tzinfo is None
    cutoff = ni._utc_naive_now() - timedelta(hours=ni.RETENTION_HOURS)
    assert published >= cutoff  # offset-naive vs aware karşılaştırma patlamamalı
