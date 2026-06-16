from datetime import date

from backend.services.market_sheets_sync import parse_market_sheet_csv, _parse_tr_date_cell, _parse_tr_number


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
    assert _parse_tr_date_cell("02 Oca 2025") is None or _parse_tr_date_cell("2 Ocak 2025") == date(2025, 1, 2)


def test_parse_market_sheet_csv_sample():
    rows = parse_market_sheet_csv(SAMPLE_CSV)
    assert len(rows) == 2
    assert rows[0]["report_date"] == date(2025, 1, 1)
    assert rows[0]["close_price"] == 2984.70
    assert rows[1]["close_price"] == 3021.24
