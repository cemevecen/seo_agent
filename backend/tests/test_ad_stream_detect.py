"""Reklam raporu dosya adı → proje/dal eşlemesi."""

from backend.services.ad_analytics_store import detect_stream, _report_period_rank


def test_detect_doviz_streams():
    assert detect_stream("dovizcom1_Report_01.01.2025_31.12.2025.xlsx").key == "doviz:desktop"
    assert detect_stream("dovizcom2_Report_01.01.2026_03.06.2026.xlsx").key == "doviz:desktop"
    assert detect_stream("m.dovizcom1_Report_2025.xlsx").key == "doviz:mweb"
    assert detect_stream("doviz_ios_1_Report_2025.xlsx").key == "doviz:ios"
    assert detect_stream("doviz_android_2_Report_2026.xlsx").key == "doviz:android"


def test_detect_sinemalar_streams():
    assert detect_stream("sinemalardesktop_1_Report_2025.xlsx").key == "sinemalar:desktop"
    assert detect_stream("m.sinemalar_2_Report_2026.xlsx").key == "sinemalar:mweb"


def test_period_rank():
    assert _report_period_rank("dovizcom1_Report_2025.xlsx") == 1
    assert _report_period_rank("dovizcom2_Report_2026.xlsx") == 2
