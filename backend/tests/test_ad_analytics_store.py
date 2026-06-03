import io
from datetime import date
from pathlib import Path

from openpyxl import Workbook

from backend.database import SessionLocal, init_db
from backend.models import AdReportRow
from backend.services import ad_analytics_store as store

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "ad_sample.csv"


def test_xlsx_header_after_title_rows():
    wb = Workbook()
    ws = wb.active
    ws.append(["Rapor özeti"])
    ws.append(["Dönem", "2025"])
    ws.append([])
    ws.append(
        [
            "Ad Unit",
            "Date",
            "Income Type",
            "Net Revenue",
        ]
    )
    ws.append(["web_unit_1", 45658, "Open Auction", 10.5])
    buf = io.BytesIO()
    wb.save(buf)
    rows = store.parse_xlsx_bytes(buf.getvalue(), filename="dovizcom1_Report_2025.xlsx")
    assert len(rows) == 1
    assert rows[0]["ad_unit"] == "web_unit_1"


def test_xlsx_turkish_headers():
    wb = Workbook()
    ws = wb.active
    ws.append(["Reklam birimi", "Tarih", "Gelir tipi", "Net gelir"])
    ws.append(["web_test", 45658, "Mediation", 3.0])
    buf = io.BytesIO()
    wb.save(buf)
    rows = store.parse_xlsx_bytes(buf.getvalue(), filename="dovizcom2_Report_2026.xlsx")
    assert len(rows) == 1


def test_channel_and_surface_from_filename_and_ad_unit():
    rows = store.parse_csv_text(
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
        "m_doviz_kripto_320x50,45658,45658,Open Auction,1,1,1,0,0,0,0,0,0,1\n"
        "web_doviz_kripto_970x90,45658,45658,Mediation,1,1,1,0,0,0,0,0,0,2\n",
        filename="dovizcom2_Report_2026.xlsx",
    )
    assert len(rows) == 2
    assert rows[0]["channel"] == "dovizcom"
    assert rows[0]["surface"] == "mweb"
    assert rows[1]["surface"] == "web"


def test_parse_csv_and_import():
    init_db()
    text = FIXTURE.read_text(encoding="utf-8")
    rows = store.parse_csv_text(text, filename="doviz_android_test.csv")
    assert len(rows) >= 3
    assert rows[0]["ad_unit"] == "test_sticky_unit"
    assert isinstance(rows[0]["report_date"], date)
    with SessionLocal() as db:
        store.reset_all(db)
        out = store.import_rows(db, rows)
        assert out["inserted"] >= 3
        assert rows[0].get("project") == "doviz"
        assert rows[0].get("branch") == "android"
        summ = store.query_summary(db)
        assert summ["kpis"]["net_revenue"] > 20
        assert len(summ["by_income_type"]) >= 2
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()
