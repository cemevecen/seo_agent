"""Google Sheets — günlük piyasa kapanış serileri (herkese açık tablolar)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSheetSeries:
    key: str
    label: str
    sheet_url: str
    unit: str = ""


# Varsayılan: kullanıcının paylaştığı herkese açık Sheets (yalnızca sheet_id gerekir).
MARKET_SHEET_SERIES: tuple[MarketSheetSeries, ...] = (
    MarketSheetSeries(
        key="gram_altin",
        label="Gram altın",
        sheet_url="https://docs.google.com/spreadsheets/d/16dpDktX7BYtpti2m6-rjV4jHZs6OXm1CMHBqeZj8D0A/edit#gid=0",
        unit="TL/gr",
    ),
    MarketSheetSeries(
        key="usd_try",
        label="USD/TRY",
        sheet_url="https://docs.google.com/spreadsheets/d/1PTxaphp51rhtt2wmbFROhTOXOaS633LFYs5UmcJ7KTc/edit#gid=0",
        unit="TL",
    ),
    MarketSheetSeries(
        key="eur_try",
        label="EUR/TRY",
        sheet_url="https://docs.google.com/spreadsheets/d/1xg4ziFry5OM5P7eSdLStxj2FSba6gULuHBH9UnxI8cE/edit#gid=0",
        unit="TL",
    ),
    MarketSheetSeries(
        key="bist100",
        label="BIST 100",
        sheet_url="https://docs.google.com/spreadsheets/d/1Y3kdq_9PNxgzV2WiHyffjM1BiKg7NOK8H3_rdXzK1PM/edit#gid=0",
        unit="puan",
    ),
    MarketSheetSeries(
        key="gram_gumus",
        label="Gram gümüş",
        sheet_url="https://docs.google.com/spreadsheets/d/188SFsvBp9EWCf9KZtoSPFXiLlfs4_UhrRnRW-16m7hk/edit#gid=0",
        unit="TL/gr",
    ),
    MarketSheetSeries(
        key="brent",
        label="Brent petrol",
        sheet_url="https://docs.google.com/spreadsheets/d/15kFLzHOiRtq_5ZbA122g3R7969onrbZ_SzACWxiDmCU/edit#gid=0",
        unit="USD/varil",
    ),
)

SERIES_BY_KEY = {s.key: s for s in MARKET_SHEET_SERIES}
