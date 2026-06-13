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


def test_detect_compact_stream_names():
    assert detect_stream("dovizweb1.xlsx").key == "doviz:desktop"
    assert detect_stream("dovizweb2_Report.xlsx").key == "doviz:desktop"
    assert detect_stream("dovizmweb1.xlsx").key == "doviz:mweb"
    assert detect_stream("dovizios2.xlsx").key == "doviz:ios"
    assert detect_stream("dovizandroid1.xlsx").key == "doviz:android"
    assert detect_stream("sinemalarweb1.xlsx").key == "sinemalar:desktop"
    assert detect_stream("sinemalarmweb2.xlsx").key == "sinemalar:mweb"


def test_period_rank():
    assert _report_period_rank("dovizcom1_Report_2025.xlsx") == 1
    assert _report_period_rank("dovizcom2_Report_2026.xlsx") == 2
    assert _report_period_rank("dovizweb1.xlsx") == 1
    assert _report_period_rank("dovizweb2.xlsx") == 2
    assert _report_period_rank("dovizmweb1.xlsx") == 1
    assert _report_period_rank("sinemalarweb2.xlsx") == 2
    assert _report_period_rank("dovizweb3.xlsx") == 3
    assert _report_period_rank("dovizweb6.xlsx") == 6
    assert _report_period_rank("dovizios12.xlsx") == 12
    assert _report_period_rank("sinemalarmweb4.xlsx") == 4


def test_bulk_sort_period_sequence():
    from backend.services.ad_analytics_store import _bulk_sort_key

    files = [
        (b"", "dovizweb5.xlsx"),
        (b"", "dovizweb1.xlsx"),
        (b"", "dovizweb3.xlsx"),
        (b"", "dovizweb2.xlsx"),
    ]
    names = [name for _, name in sorted(files, key=_bulk_sort_key)]
    assert names == [
        "dovizweb1.xlsx",
        "dovizweb2.xlsx",
        "dovizweb3.xlsx",
        "dovizweb5.xlsx",
    ]


def test_user_upload_filenames():
    """Kullanıcının Desktop/reklam klasöründeki kompakt adlar."""
    files = {
        "dovizweb1.xlsx": "doviz:desktop",
        "dovizweb2.xlsx": "doviz:desktop",
        "dovizweb3.xlsx": "doviz:desktop",
        "dovizmweb1.xlsx": "doviz:mweb",
        "dovizmweb2.xlsx": "doviz:mweb",
        "dovizmweb3.xlsx": "doviz:mweb",
        "dovizios1.xlsx": "doviz:ios",
        "dovizios2.xlsx": "doviz:ios",
        "dovizios3.xlsx": "doviz:ios",
        "dovizandroid1.xlsx": "doviz:android",
        "dovizandroid2.xlsx": "doviz:android",
        "dovizandroid3.xlsx": "doviz:android",
        "sinemalarweb1.xlsx": "sinemalar:desktop",
        "sinemalarweb2.xlsx": "sinemalar:desktop",
        "sinemalarweb3.xlsx": "sinemalar:desktop",
        "sinemalarmweb1.xlsx": "sinemalar:mweb",
        "sinemalarmweb2.xlsx": "sinemalar:mweb",
        "sinemalarmweb3.xlsx": "sinemalar:mweb",
    }
    for name, expected in files.items():
        stream = detect_stream(name)
        assert stream is not None, name
        assert stream.key == expected, name
