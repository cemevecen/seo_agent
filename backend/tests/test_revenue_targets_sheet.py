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
"""


def test_parse_revenue_targets_csv():
    rows = parse_revenue_targets_csv(SAMPLE_CSV)
    assert len(rows) == 4
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
    monkeypatch.setattr(
        "backend.services.revenue_targets_sheet.fetch_revenue_targets_rows",
        lambda **_: parse_revenue_targets_csv(SAMPLE_CSV),
    )
    all_rows = revenue_targets_payload()["rows"]
    assert len(all_rows) == 4
    doviz = revenue_targets_payload(project="doviz")["rows"]
    assert len(doviz) == 2
    assert all(r["project"] == "doviz" for r in doviz)
    y2023 = revenue_targets_payload(year=2023)["rows"]
    assert len(y2023) == 4
