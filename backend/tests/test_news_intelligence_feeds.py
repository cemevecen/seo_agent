"""News intelligence RSS/Atom parse testleri."""

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
