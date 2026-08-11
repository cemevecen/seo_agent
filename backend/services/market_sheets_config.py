"""Piyasa günlük kapanış serileri — doviz.com tarihsel tablo taraması."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSheetSeries:
    key: str
    label: str
    source_url: str
    unit: str = ""
    # Eski Google Sheets alanı; tarama sonrası kullanılmaz.
    sheet_url: str = ""


# 01.01.2025+ Tablo / Verileri Getir — Mac köprüsü günde bir (00:05 TR).
MARKET_SHEET_SERIES: tuple[MarketSheetSeries, ...] = (
    MarketSheetSeries(
        key="gram_altin",
        label="Gram altın",
        source_url="https://altin.doviz.com/gram-altin",
        unit="TL/gr",
    ),
    MarketSheetSeries(
        key="usd_try",
        label="USD/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/amerikan-dolari",
        unit="TL",
    ),
    MarketSheetSeries(
        key="eur_try",
        label="EUR/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/euro",
        unit="TL",
    ),
    MarketSheetSeries(
        key="bist100",
        label="BIST 100",
        source_url="https://borsa.doviz.com/endeksler/xu100-bist-100",
        unit="puan",
    ),
    MarketSheetSeries(
        key="bitcoin",
        label="Bitcoin",
        source_url="https://www.doviz.com/kripto-paralar/bitcoin/tarihsel-veri",
        unit="USD",
    ),
    MarketSheetSeries(
        key="gram_gumus",
        label="Gram gümüş",
        source_url="https://altin.doviz.com/gumus",
        unit="TL/gr",
    ),
    MarketSheetSeries(
        key="brent",
        label="Brent petrol",
        source_url="https://www.doviz.com/emtia/brent-petrol",
        unit="USD/varil",
    ),
    MarketSheetSeries(
        key="ceyrek_altin",
        label="Çeyrek altın",
        source_url="https://altin.doviz.com/ceyrek-altin",
        unit="TL",
    ),
)

SERIES_BY_KEY = {s.key: s for s in MARKET_SHEET_SERIES}

TARAMA_SOURCE_ID = "doviz.com"
TARAMA_START_DATE = "2025-01-01"
