import io
import json
from datetime import date, timedelta
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


def test_resolve_compare_range_previous_period():
    start, end = store.resolve_compare_range("2026-01-10", "2026-01-16", "previous_period")
    assert start == "2026-01-03"
    assert end == "2026-01-09"


def test_resolve_compare_range_previous_year():
    start, end = store.resolve_compare_range("2025-03-01", "2025-03-07", "previous_year")
    assert start == "2024-03-01"
    assert end == "2024-03-07"


def test_resolve_compare_range_custom():
    start, end = store.resolve_compare_range(
        "2026-01-01",
        "2026-01-31",
        "custom",
        "2025-06-01",
        "2025-06-30",
    )
    assert start == "2025-06-01"
    assert end == "2025-06-30"


def test_compute_kpi_deltas():
    deltas = store.compute_kpi_deltas(
        {"net_revenue": 150.0, "impression": 1000},
        {"net_revenue": 100.0, "impression": 800},
    )
    assert deltas["net_revenue"]["pct"] == 50.0
    assert deltas["impression"]["abs"] == 200


def _excel_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days


def test_query_summary_with_compare():
    init_db()
    text = (
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
    )
    d1 = date(2026, 1, 5)
    d2 = date(2026, 1, 12)
    d0 = date(2025, 12, 29)
    text += f"unit_a,1,{_excel_serial(d1)},Open Auction,10,10,10,1,0,0,0,0,0,50\n"
    text += f"unit_a,1,{_excel_serial(d2)},Open Auction,10,10,10,1,0,0,0,0,0,30\n"
    text += f"unit_a,1,{_excel_serial(d0)},Open Auction,10,10,10,1,0,0,0,0,0,20\n"
    rows = store.parse_csv_text(text, filename="dovizcom1_Report_2026.xlsx")
    with SessionLocal() as db:
        store.reset_all(db)
        store.import_rows(db, rows)
        summ = store.query_summary(
            db,
            start=d1.isoformat(),
            end=d2.isoformat(),
            compare_mode="previous_period",
        )
        assert "compare" in summ
        assert summ["compare"]["deltas"]["net_revenue"]["compare"] == 20.0
        assert summ["compare"]["deltas"]["net_revenue"]["current"] == 80.0
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()


def test_empower_metrics_from_extra_header():
    wb = Workbook()
    ws = wb.active
    ws.append(
        [
            "Ad Unit",
            "Date",
            "Income Type",
            "Impression",
            "Net Revenue",
            "Empower Pageviews",
        ]
    )
    d = date(2026, 2, 1)
    serial = (d - date(1899, 12, 30)).days
    ws.append(["web_x", serial, "Open Auction", 100, 50.0, 1200])
    buf = io.BytesIO()
    wb.save(buf)
    rows = store.parse_xlsx_bytes(buf.getvalue(), filename="dovizcom1_Report_2026.xlsx")
    assert len(rows) == 1
    extras = json.loads(rows[0]["extra_metrics"])
    assert extras.get("empower_pageview") == 1200.0


def test_kpi_available_omits_missing_empower():
    init_db()
    text = (
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
        "unit_a,1,45658,Open Auction,10,8,10,1,0,0,0,0,0,50\n"
    )
    rows = store.parse_csv_text(text, filename="dovizcom1_Report_2026.xlsx")
    with SessionLocal() as db:
        store.reset_all(db)
        store.import_rows(db, rows)
        summ = store.query_summary(db)
        assert "empower_pageview" not in summ["kpi_available"]
        assert "net_revenue" in summ["kpi_available"]
        assert "impression" in summ["kpi_available"]
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()


def test_viewability_coverage_percent_scale():
    init_db()
    text = (
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
        "unit_a,1,45658,Open Auction,100,80,100,1,0,0,0,80,90,50\n"
    )
    rows = store.parse_csv_text(text, filename="dovizcom1_Report_2026.xlsx")
    with SessionLocal() as db:
        store.reset_all(db)
        store.import_rows(db, rows)
        summ = store.query_summary(db)
        assert summ["kpis"]["coverage_pct"] == 80.0
        assert summ["kpis"]["viewability_pct"] == 90.0
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()


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


def test_build_upload_batch_summary():
    per_file = [
        {"filename": "a.xlsx", "parsed": 100, "inserted": 100, "stream_key": "doviz:web"},
        {"filename": "b.xlsx", "error": "bozuk"},
        {"filename": "c.xlsx", "parsed": 0, "parse_error": "başlık yok"},
        {"filename": "d.xlsx", "parsed": 50, "warning": "dal?", "stream_key": None},
    ]
    s = store.build_upload_batch_summary(per_file)
    assert s["file_count"] == 4
    assert s["ok_count"] == 2
    assert s["failed_count"] == 1
    assert s["empty_count"] == 1
    assert s["has_errors"] is True
    assert s["integrated_rows"] == 150
