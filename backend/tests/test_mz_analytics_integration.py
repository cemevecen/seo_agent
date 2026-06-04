"""Mx analitiği — yeni özet alanları, filtreler ve karşılaştırma koordinasyonu."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from backend.database import SessionLocal, init_db
from backend.models import AdReportRow
from backend.services import ad_analytics_store as store

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "ad_sample.csv"


def _seed_db(db) -> None:
    store.reset_all(db)
    text = FIXTURE.read_text(encoding="utf-8")
    rows = store.parse_csv_text(text, filename="doviz_android_test.csv")
    assert len(rows) >= 3
    store.import_rows(db, rows)


def test_summary_extensions_present_and_filtered():
    init_db()
    with SessionLocal() as db:
        _seed_db(db)
        full = store.query_summary(db)
        assert len(full["by_date"]) >= 1
        assert len(full["by_platform"]) >= 1
        assert len(full["by_date_income_type"]) >= 1
        assert len(full["heatmap_calendar"]) >= 1
        assert full["funnel"]["impression"] > 0
        assert len(full["by_ad_unit_scatter"]) >= 1
        assert len(full["by_month"]) >= 1

        med = store.query_summary(db, income_types="Mediation")
        assert full["kpis"]["net_revenue"] > med["kpis"]["net_revenue"]
        assert all(
            r["income_type"] == "Mediation" for r in med["by_income_type"]
        )

        search = store.query_summary(db, search="sticky")
        assert search["kpis"]["net_revenue"] > 0
        units = {u["ad_unit"] for u in search["by_ad_unit"]}
        assert all("sticky" in (u or "").lower() for u in units)

        plat = store.query_summary(db, platforms="app")
        if plat["by_platform"]:
            assert all(
                (p["platform"] or "") in ("app", "") for p in plat["by_platform"]
            )

        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()


def test_compare_leaders_platform_coordinated():
    init_db()
    with SessionLocal() as db:
        _seed_db(db)
        bounds = store.date_bounds(db)
        summ = store.query_summary(
            db,
            start=bounds["min_date"],
            end=bounds["max_date"],
            compare_mode="previous_period",
            income_types="Open Auction",
        )
        cmp = summ.get("compare")
        assert cmp is not None
        assert "leaders_losers" in cmp
        assert "gainers" in cmp["leaders_losers"]
        assert "by_platform" in cmp
        assert cmp["deltas"]["net_revenue"]["current"] >= 0
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()


def test_table_breakdown_sort_with_filters():
    init_db()
    with SessionLocal() as db:
        _seed_db(db)
        tab = store.query_table(
            db,
            breakdown="ad_unit,income_type",
            limit=50,
            search="sticky",
        )
        assert tab["total"] >= 1
        assert "ad_unit" in tab["breakdown"]
        for row in tab["rows"]:
            assert "sticky" in (row.get("ad_unit") or "").lower()
        tab2 = store.query_table(db, breakdown="date", limit=10)
        dates = [r.get("date") for r in tab2["rows"] if r.get("date")]
        assert dates == sorted(dates, reverse=True)
        db.execute(__import__("sqlalchemy").delete(AdReportRow))
        db.commit()
