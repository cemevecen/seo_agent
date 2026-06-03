from datetime import date
from pathlib import Path

from backend.database import SessionLocal, init_db
from backend.models import AdReportRow
from backend.services import ad_analytics_store as store

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "ad_sample.csv"


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
        assert out["inserted"] == 3
        summ = store.query_summary(db)
        assert summ["kpis"]["net_revenue"] > 20
        assert len(summ["by_income_type"]) >= 2
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()
