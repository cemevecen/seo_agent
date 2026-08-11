from datetime import date

from backend.services.market_sheets_sync import (
    parse_archive_payload,
    parse_historical_table_matrix,
    parse_market_sheet_csv,
    _parse_tr_date_cell,
    _parse_tr_number,
)


SAMPLE_CSV = ''',,
Tarih,Açılış,Kapanış
1 Ocak 2025,"2.983,35","2.984,70"
2 Ocak 2025,"2.984,48","3.021,24"
'''


def test_parse_tr_number():
    assert _parse_tr_number("2.983,35") == 2983.35
    assert _parse_tr_number("35,368") == 35.368


def test_parse_tr_date():
    assert _parse_tr_date_cell("1 Ocak 2025") == date(2025, 1, 1)
    assert _parse_tr_date_cell("02 Oca 2025") == date(2025, 1, 2)
    assert _parse_tr_date_cell("03 Şub 2025") == date(2025, 2, 3)


BRENT_CSV = """Tarih,Açılış,Kapanış
02 Oca 2025,"74,93","75,93"
03 Oca 2025,"75,98","76,51"
"""


def test_parse_brent_sheet_csv():
    rows = parse_market_sheet_csv(BRENT_CSV)
    assert len(rows) == 2
    assert rows[0]["report_date"] == date(2025, 1, 2)
    assert rows[0]["close_price"] == 75.93


def test_parse_market_sheet_csv_sample():
    rows = parse_market_sheet_csv(SAMPLE_CSV)
    assert len(rows) == 2
    assert rows[0]["report_date"] == date(2025, 1, 1)
    assert rows[0]["close_price"] == 2984.70
    assert rows[0]["open_price"] == 2983.35
    assert rows[1]["close_price"] == 3021.24


def test_parse_tr_number_thousands_dot():
    assert _parse_tr_number("$63.924") == 63924
    assert _parse_tr_number("13.811,60") == 13811.60


def test_parse_historical_table_son_deger():
    rows = parse_historical_table_matrix(
        ["Tarih", "Son Değer"],
        [["11 Ağustos 2026", "$63.924"], ["10 Ağustos 2026", "64.180,50"]],
    )
    assert len(rows) == 2
    assert rows[0]["report_date"] == date(2026, 8, 11)
    assert rows[0]["close_price"] == 63924
    assert rows[1]["close_price"] == 64180.50


def test_parse_historical_table_open_close():
    rows = parse_historical_table_matrix(
        ["Tarih", "Açılış", "Kapanış"],
        [["11 Ağustos 2026", "6.732,45", "6.711,16"]],
    )
    assert len(rows) == 1
    assert rows[0]["open_price"] == 6732.45
    assert rows[0]["close_price"] == 6711.16


def test_parse_historical_table_alis_satis():
    rows = parse_historical_table_matrix(
        ["Tarih", "Alış", "Satış"],
        [["11 Ağustos 2026", "10.572,98", "10.816,63"]],
    )
    assert len(rows) == 1
    assert rows[0]["open_price"] == 10572.98
    assert rows[0]["close_price"] == 10816.63


def test_parse_tr_date_english_month():
    assert _parse_tr_date_cell("11 August 2026") == date(2026, 8, 11)
    assert _parse_tr_date_cell("10 Aug 2026") == date(2026, 8, 10)


def test_parse_archive_payload():
    payload = {
        "data": {
            "archive": {
                "a": {"update_date": 1735689600, "open": 2983.35, "close": 2984.70},
            }
        }
    }
    rows = parse_archive_payload(payload)
    assert len(rows) == 1
    assert rows[0]["close_price"] == 2984.70
    assert rows[0]["open_price"] == 2983.35


def test_parse_archive_payload_ask_as_close():
    payload = {
        "data": {
            "archive": {
                "a": {"update_date": 1735689600, "ask": 10816.63},
            }
        }
    }
    rows = parse_archive_payload(payload)
    assert len(rows) == 1
    assert rows[0]["close_price"] == 10816.63
