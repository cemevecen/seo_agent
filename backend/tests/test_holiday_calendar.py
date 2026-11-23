from backend.services.holiday_calendar import (
    flag_emoji,
    flag_img_url,
    normalize_rows,
    parse_date_tr,
    resolve_country_iso,
)


def test_parse_date_tr():
    assert parse_date_tr("22.11.2026") == "2026-11-22"
    assert parse_date_tr("") is None
    assert parse_date_tr("bad") is None


def test_resolve_country_iso_and_flags():
    assert resolve_country_iso(country_flag="USA") == "us"
    assert resolve_country_iso(country="Lübnan", country_flag="Lebanon") == "lb"
    assert resolve_country_iso(country="Birleşik Arap Emirlikleri", country_flag="Dubai") == "ae"
    assert flag_emoji("us") == "🇺🇸"
    assert flag_img_url("jp") == "https://flagcdn.com/16x12/jp.png"


def test_normalize_rows_inherits_blank_date():
    rows = normalize_rows(
        [
            {
                "date_tr": "22.11.2026",
                "country": "Lübnan",
                "country_flag": "Lebanon",
                "exchange": "Beyrut Borsası",
                "holiday": "Kurtuluş Günü",
            },
            {
                "date_tr": "",
                "country": "Japonya",
                "country_flag": "Japan",
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
    assert by_country["Lübnan"]["country_code"] == "lb"
    assert by_country["Japonya"]["flag_img"].endswith("/jp.png")
    assert by_country["Lübnan"]["flag_emoji"] == "🇱🇧"
