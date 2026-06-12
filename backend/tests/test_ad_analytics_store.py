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


def test_incremental_append_upserts_same_day():
    init_db()
    d = date(2026, 6, 10)
    serial = _excel_serial(d)
    base = (
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
        f"web_unit_1,1,{serial},Open Auction,1,1,1,0,0,0,0,0,0,10\n"
    )
    with SessionLocal() as db:
        store.reset_all(db)
        store.import_append_to_stream(
            db,
            base.encode("utf-8"),
            stream_key="doviz:desktop",
            original_filename="gunluk.csv",
        )
        store.import_append_to_stream(
            db,
            base.replace(",10\n", ",15\n").encode("utf-8"),
            stream_key="doviz:desktop",
            original_filename="gunluk2.csv",
        )
        n = db.query(AdReportRow).filter(AdReportRow.report_date == d).count()
        rev = db.query(AdReportRow).filter(AdReportRow.report_date == d).one().net_revenue
    assert n == 1
    assert rev == 15.0


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


def test_revenue_week_anomaly_requires_14_days():
    days = [{"date": f"2026-01-{i:02d}", "net_revenue": 10.0} for i in range(1, 11)]
    out = store._revenue_week_anomaly(days)
    assert out["ok"] is False


def test_revenue_week_anomaly_delta():
    days = [{"date": f"2026-01-{i:02d}", "net_revenue": 10.0 if i <= 7 else (5.0 if i <= 14 else 1.0)} for i in range(1, 21)]
    out = store._revenue_week_anomaly(days)
    assert out["ok"] is True
    assert out["last7_revenue"] == 7.0
    assert out["prev7_revenue"] == 70.0


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


def test_align_by_date_series_calendar_not_index():
    """Karşı dönem daha az satır içerse indeks hizalaması sıfıra düşürür; takvim eşlemesi korur."""
    primary = [
        {"date": "2025-06-01", "net_revenue": 10},
        {"date": "2025-06-02", "net_revenue": 20},
        {"date": "2025-06-03", "net_revenue": 30},
        {"date": "2025-06-04", "net_revenue": 40},
    ]
    compare = [
        {"date": "2024-06-01", "net_revenue": 1},
        {"date": "2024-06-02", "net_revenue": 2},
    ]
    aligned = store.align_by_date_series(
        primary,
        compare,
        "net_revenue",
        mode="previous_year",
        primary_start="2025-06-01",
        compare_start="2024-06-01",
    )
    assert len(aligned) == 4
    assert aligned[0]["compare"] == 1.0
    assert aligned[1]["compare"] == 2.0
    assert aligned[2]["compare"] is None
    assert aligned[3]["compare"] is None
    assert aligned[2]["compare_date"] == "2024-06-03"
    assert aligned[3]["compare_date"] == "2024-06-04"


def test_compute_kpi_deltas():
    deltas = store.compute_kpi_deltas(
        {"net_revenue": 150.0, "impression": 1000},
        {"net_revenue": 100.0, "impression": 800},
    )
    assert deltas["net_revenue"]["pct"] == 50.0
    assert deltas["impression"]["abs"] == 200

    zero_base = store._kpi_delta(50.0, 0.0)
    assert zero_base["pct"] is None
    assert zero_base["abs"] == 50.0


def _excel_serial(d: date) -> int:
    return (d - date(1899, 12, 30)).days


def test_compare_ad_units_param_uses_primary_top_units():
    primary = [{"ad_unit": "unit_a"}, {"ad_unit": "unit_b"}]
    assert store._compare_ad_units_param(primary, None) == "unit_a,unit_b"
    assert store._compare_ad_units_param(primary, "unit_x") == "unit_x"


def test_merge_breakdown_str_keys_and_delta_abs():
    primary = [{"ad_unit": "unit_a", "net_revenue": 100.0, "impression": 10}]
    compare = [{"ad_unit": "unit_a", "net_revenue": 50.0, "impression": 5}]
    merged = store._merge_breakdown(primary, compare, "ad_unit")
    assert merged[0]["net_revenue_compare"] == 50.0
    assert merged[0]["net_revenue_delta_pct"] == 100.0
    assert merged[0]["net_revenue_delta_abs"] == 50.0


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


def test_aggregate_ctr_sub_percent_precision():
    init_db()
    text = (
        "Ad Unit,Month,Date,Income Type,Ad Request,Matched Request,Impression,Click,"
        "Ad Request Ecpm,Ad Impression Ecpm,CTR,Coverage,Viewability,Net Revenue\n"
        "unit_a,1,45658,Open Auction,1000,800,193290271,858,0,0,0,25.2,0,50\n"
    )
    rows = store.parse_csv_text(text, filename="dovizcom1_Report_2026.xlsx")
    with SessionLocal() as db:
        store.reset_all(db)
        store.import_rows(db, rows)
        summ = store.query_summary(db)
        assert summ["kpis"]["click"] == 858
        expected_ctr = 858 / 193_290_271 * 100.0
        assert summ["kpis"]["ctr_pct"] == round(expected_ctr, 6)
        assert summ["kpis"]["ctr_pct"] > 0
        assert summ["kpis"]["coverage_pct"] == 25.2
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
    assert s["has_warnings"] is True
    assert s["integrated_rows"] == 150


def test_build_heatmap_calendar():
    days = [
        {"date": "2026-06-01", "net_revenue": 10.0},
        {"date": "2026-06-02", "net_revenue": 20.0},
    ]
    hm = store._build_heatmap_calendar(days)
    assert len(hm) == 2
    assert hm[0]["dow_label"] in store._DOW_LABELS


def test_facets_returns_bounds_and_row_count():
    init_db()
    with SessionLocal() as db:
        out = store.facets(db)
    assert "min_date" in out
    assert "max_date" in out
    assert isinstance(out["total_rows"], int)
    assert isinstance(out["streams"], list)
