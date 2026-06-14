from backend.services.vakif_economic_calendar import (
    _parse_region_agenda,
    _parse_week_range,
    _regions_to_items,
)

SAMPLE_ARTICLE = """
<p><strong>Haftanın Gündem Konuları (15 – 19 Haziran 2026)</strong></p>
<ul>
<li class="MsoNormal"><strong><span>Türkiye– </span></strong><span>Sanayi Üretimi, Bütçe Dengesi</span></li>
<li class="MsoNormal"><strong><span>ABD–</span></strong><span> Fed Faiz Kararı Toplantısı</span></li>
</ul>
"""


def test_parse_week_range():
    assert _parse_week_range(SAMPLE_ARTICLE) == "15 – 19 Haziran 2026"


def test_parse_region_agenda():
    regions = _parse_region_agenda(SAMPLE_ARTICLE)
    assert len(regions) == 2
    assert regions[0]["region"] == "Türkiye"
    assert "Sanayi Üretimi" in regions[0]["events"]
    assert regions[1]["region"] == "ABD"


def test_regions_to_items():
    regions = [{"region": "Türkiye", "events": ["TCMB", "Enflasyon"]}]
    items = _regions_to_items(regions, pdf_url="https://x/pdf", detail_url="")
    assert len(items) == 2
    assert items[0]["title"] == "TCMB"
    assert items[0]["badge"] == "Türkiye"
