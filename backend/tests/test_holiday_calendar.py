from backend.services.holiday_calendar import normalize_rows, parse_date_tr


def test_parse_date_tr():
    assert parse_date_tr("22.11.2026") == "2026-11-22"
    assert parse_date_tr("") is None
    assert parse_date_tr("bad") is None


def test_normalize_rows_inherits_blank_date():
    rows = normalize_rows(
        [
            {
                "date_tr": "22.11.2026",
                "country": "Lübnan",
                "exchange": "Beyrut Borsası",
                "holiday": "Kurtuluş Günü",
            },
            {
                "date_tr": "",
                "country": "Japonya",
                "exchange": "Tokyo Borsası",
                "holiday": "İşçi Şükran Günü",
            },
        ]
    )
    assert len(rows) == 2
    assert {r["date_iso"] for r in rows} == {"2026-11-22"}
    assert {r["date_tr"] for r in rows} == {"22.11.2026"}
    by_country = {r["country"]: r for r in rows}
    assert "Japonya" in by_country
    assert "Lübnan" in by_country
    assert by_country["Japonya"]["holiday"] == "İşçi Şükran Günü"
