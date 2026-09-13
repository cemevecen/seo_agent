"""Play reports CSV → Android overview facts."""

from datetime import date

from backend.services.play_reports_backfill import (
    facts_from_crashes_csv,
    facts_from_installs_csv,
    facts_from_ratings_csv,
    facts_from_store_country_csv,
)

START = date(2026, 7, 1)
END = date(2026, 7, 31)


def test_installs_csv_maps_daily_columns():
    text = (
        "Date,Package name,Daily Device Installs,Daily Device Uninstalls,"
        "Daily Device Upgrades,Total User Installs,Daily User Installs,"
        "Daily User Uninstalls,Active Device Installs\n"
        "2026-07-17,com.Doviz,211,0,0,0,125,245,1\n"
        "2026-06-01,com.Doviz,9,0,0,0,9,9,1\n"
    )
    facts = facts_from_installs_csv(text, start=START, end=END)
    by = {(f["metric"], f["date"]): f["value"] for f in facts}
    assert by[("device_acquisition", "2026-07-17")] == 211
    assert by[("user_acquisition", "2026-07-17")] == 125
    assert by[("user_lost", "2026-07-17")] == 245
    assert ("device_acquisition", "2026-06-01") not in by
    assert all(f["value_kind"] == "daily" and f["dim"] == "overview" for f in facts)


def test_crashes_and_rating_and_store_sum():
    crashes = facts_from_crashes_csv(
        "Date,Package Name,Daily Crashes,Daily ANRs\n2026-07-16,com.Doviz,18,79\n",
        start=START,
        end=END,
    )
    by = {(f["metric"], f["date"]): f["value"] for f in crashes}
    assert by[("crashes", "2026-07-16")] == 18
    assert by[("anrs", "2026-07-16")] == 79

    rating = facts_from_ratings_csv(
        "Date,Package Name,Daily Average Rating,Total Average Rating\n"
        "2026-07-16,com.Doviz,5.0,4.59\n",
        start=START,
        end=END,
    )
    assert rating[0]["metric"] == "rating"
    assert rating[0]["value"] == 4.59

    store = facts_from_store_country_csv(
        "Date,Package name,Country / region,Store listing acquisitions,"
        "Store listing visitors,Store listing conversion rate\n"
        "2026-07-16,com.Doviz,TR,100,400,0.25\n"
        "2026-07-16,com.Doviz,DE,7,90,0.07\n",
        start=START,
        end=END,
    )
    bys = {(f["metric"], f["date"]): f["value"] for f in store}
    assert bys[("ar2_acquisitions", "2026-07-16")] == 107
    assert bys[("ar2_visitors", "2026-07-16")] == 490
