"""Play 'Veri tablosu' — tarih etiketi ve gün başına değer eşleşmesi.

ANR/çökme serisinde eksik gün ve yılın (2026) değer sanılması sorunlarını korur.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
)
assert _SPEC and _SPEC.loader
mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mod)


def test_day_label_accepts_dotted_month_abbreviation():
    assert mod._parse_tr_day_label("3 Eyl 2026") == "2026-09-03"
    assert mod._parse_tr_day_label("3 Eyl. 2026") == "2026-09-03"
    assert mod._parse_tr_day_label("31 Ara. 2026") == "2026-12-31"
    assert mod._parse_tr_day_label("13 Eylül 2026") == "2026-09-13"


def test_day_label_rejects_bare_year_and_yearless_day():
    # Yıl tek başına satırda kalırsa tarih sayılmamalı (değer olarak sızmasın)
    assert mod._parse_tr_day_label("2026") is None
    assert mod._parse_tr_day_label("3 Eyl") is None


def test_data_table_maps_each_day_to_its_own_value():
    text = "\n".join(
        [
            "Tüm ülkeler / bölgeler",
            "3 Eyl 2026",
            "3",
            "2 Eyl 2026",
            "8",
            "1 Eyl 2026",
            "9",
        ]
    )
    facts = mod._parse_stats_data_table(
        text, metric_key="crashes", view_id="crashes_date", segments=["OVERALL"]
    )
    by_date = {f["date"]: f["value"] for f in facts}
    assert by_date == {"2026-09-03": 3.0, "2026-09-02": 8.0, "2026-09-01": 9.0}
    # Yıl hiçbir günün değeri olmamalı
    assert 2026.0 not in set(by_date.values())
