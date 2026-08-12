"""revenue_targets_sheet — CSV parse ve proje filtreleri."""

from backend.services.revenue_targets_sheet import (
    parse_revenue_targets_csv,
    revenue_targets_payload,
)

SAMPLE_CSV = """,Proje,Hedef,Hedef (%80),Kazanç,Hedef Tamamlama  Oranı,Günlük Kazanç,Kalan
Şubat 2023,Doviz.com,  550.000   ,  440.000   ,  343.681   ,"62,49%",  12.274   ,  206.319
,Sinemalar.com,  45.000   ,  36.000   ,  25.677   ,"57,06%",  917   ,  19.323
Mart 2023,Doviz.com,  1.200.000   ,  960.000   ,  1.153.360   ,"96,11%",  37.205   ,  46.640
,Sinemalar.com,  80.000   ,  64.000   ,  73.726   ,"92,16%",  2.378   ,  6.274
Ağustos 2026,Doviz.com,  2.000.000   ,  1.600.000   ,  800.000   ,"40,00%",  66.667   ,  1.200.000
,Sinemalar.com,  150.000   ,  120.000   ,  60.000   ,"40,00%",  5.000   ,  90.000
"""


def test_parse_revenue_targets_csv():
    from backend.services.revenue_targets_sheet import parse_revenue_targets_csv

    rows = parse_revenue_targets_csv(SAMPLE_CSV)
    assert len(rows) == 6
    feb_doviz = rows[0]
    assert feb_doviz["period_key"] == "2023-02"
    assert feb_doviz["project"] == "doviz"
    assert feb_doviz["hedef"] == 550_000.0
    assert feb_doviz["kazanc"] == 343_681.0
    assert feb_doviz["tamamlama_orani"] == 62.49
    assert rows[1]["project"] == "sinemalar"
    assert rows[1]["period_key"] == "2023-02"
    assert rows[2]["period_key"] == "2023-03"


def test_revenue_targets_payload_filter(monkeypatch):
    from backend.services.revenue_targets_sheet import (
        parse_revenue_targets_csv,
        revenue_targets_payload,
    )

    calls: list[bool] = []

    def _fetch(**kwargs):
        calls.append(bool(kwargs.get("force")))
        return parse_revenue_targets_csv(SAMPLE_CSV)

    monkeypatch.setattr(
        "backend.services.revenue_targets_sheet.fetch_revenue_targets_rows",
        _fetch,
    )
    all_rows = revenue_targets_payload()["rows"]
    assert len(all_rows) == 6
    doviz = revenue_targets_payload(project="doviz")["rows"]
    assert len(doviz) == 3
    assert all(r["project"] == "doviz" for r in doviz)
    y2023 = revenue_targets_payload(year=2023)["rows"]
    assert len(y2023) == 4
    revenue_targets_payload(force=True)
    assert calls[-1] is True
    cur = revenue_targets_payload()["current_month"]
    assert cur["doviz"] is not None
    assert cur["sinemalar"] is not None
    assert cur["doviz"]["period_key"] == "2026-08"
    assert cur["doviz"]["target_100"] == 2_000_000.0
    assert cur["doviz"]["remaining"] == 1_200_000.0


def test_fetch_falls_back_when_pending_sheet_private(monkeypatch):
    from backend.services import revenue_targets_sheet as mod

    calls: list[str] = []

    def _fake_fetch(url: str, **_kwargs):
        calls.append(url)
        if "1ITl0rUl" in url or "11IWNTk3" in url:
            raise ValueError("Sayfa erişilemedi (HTTP 401)")
        return SAMPLE_CSV

    monkeypatch.setattr(mod, "fetch_public_sheet_csv", _fake_fetch)
    monkeypatch.setattr(mod, "load_ingested_revenue_targets", lambda **_k: None)
    monkeypatch.setattr(mod, "_CACHE", None)
    rows = mod.fetch_revenue_targets_rows(force=True)
    assert len(rows) == 6
    assert any("1ITl0rUl" in u for u in calls)
    assert any("1ulWizYIfbdeUERkEwqEi70abtSkXJt7oYtHnn07OyuA" in u for u in calls)
    assert mod._CACHE and "1ulWizYIfbdeUERkEwqEi70abtSkXJt7oYtHnn07OyuA" in str(
        mod._CACHE.get("source_url")
    )
    assert mod._CACHE.get("warning")


def test_enrich_month_target_kpi_needed_daily():
    from datetime import date

    from backend.services.revenue_targets_sheet import enrich_month_target_kpi

    row = {
        "project": "doviz",
        "project_label": "Doviz.com",
        "period": "Ağustos 2026",
        "period_key": "2026-08",
        "year": 2026,
        "month": 8,
        "hedef": 310_000.0,
        "hedef_80": 248_000.0,
        "kazanc": 100_000.0,
        "kalan": 210_000.0,
        "gunluk_kazanc": 10_000.0,
        "tamamlama_orani": 32.26,
    }
    kpi = enrich_month_target_kpi(row, today=date(2026, 8, 12))
    assert kpi is not None
    assert kpi["days_elapsed"] == 12
    assert kpi["days_remaining"] == 20  # 31-12+1
    assert abs(kpi["needed_daily"] - (210_000 / 20)) < 0.01
    assert kpi["completion_pct_80"] is not None
    assert abs(kpi["remaining_80"] - max(0.0, 248_000.0 - 100_000.0)) < 0.01
    assert abs(kpi["needed_daily_80"] - ((248_000.0 - 100_000.0) / 20)) < 0.01
    assert abs(kpi["needed_daily_100"] - (210_000 / 20)) < 0.01
