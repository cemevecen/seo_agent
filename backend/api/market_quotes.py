"""Piyasa kapanış serileri — tarama ingest ve grafik overlay API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.market_sheets_config import MARKET_SHEET_SERIES, SERIES_BY_KEY
from backend.services.market_sheets_sync import ingest_market_tarama_payload, query_overlay

router = APIRouter(tags=["market-quotes"])


def _check_ingest_token(
    authorization: str | None,
    x_notification_ingest_token: str | None,
) -> None:
    expected = (settings.notification_ingest_token or "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="NOTIFICATION_INGEST_TOKEN tanımlı değil (Railway Variables).",
        )
    got = (x_notification_ingest_token or "").strip()
    if not got and authorization:
        raw = authorization.strip()
        if raw.lower().startswith("bearer "):
            got = raw[7:].strip()
        else:
            got = raw
    if not got or got != expected:
        raise HTTPException(status_code=401, detail="Geçersiz ingest token.")


class MarketQuoteSeriesIn(BaseModel):
    key: str = ""
    series_key: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)


class MarketQuotesIngestBody(BaseModel):
    series: list[MarketQuoteSeriesIn] = Field(default_factory=list)
    source: str = ""


@router.post("/market-quotes/sync")
def post_market_quotes_sync():
    """Eski tablo senkronu kapalı; tarama ingest kullanın."""
    return {
        "ok": False,
        "disabled": True,
        "message": "Piyasa serileri tarama ile güncellenir.",
    }


@router.post("/market-quotes/ingest")
def post_market_quotes_ingest(
    body: MarketQuotesIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    items = [s.model_dump() for s in body.series]
    result = ingest_market_tarama_payload(db, items, commit=True)
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        record_scrape_ingest(
            db,
            source="market_tarama",
            target="doviz.com",
            status="success" if result.get("ok") else "error",
            row_count=int(result.get("rows_upserted") or 0),
            message=str(result.get("message") or f"{result.get('ok_count')}/{result.get('series_count')} seri"),
            detail={"ok_count": result.get("ok_count"), "results": result.get("results")},
            commit=True,
        )
    except Exception:
        pass
    if not result.get("ok_count"):
        raise HTTPException(status_code=400, detail=result.get("message") or "Piyasa tarama ingest boş")
    return result


@router.get("/market-quotes/overlay")
def get_market_quotes_overlay(
    start: str | None = Query(None),
    end: str | None = Query(None),
    series: str | None = Query(None, description="Virgülle: gram_altin,usd_try,..."),
    db: Session = Depends(get_db),
):
    keys = None
    if series:
        keys = [k.strip() for k in series.split(",") if k.strip()]
        bad = [k for k in keys if k not in SERIES_BY_KEY]
        if bad:
            raise HTTPException(status_code=400, detail=f"Bilinmeyen seri: {', '.join(bad)}")
    return query_overlay(db, start=start, end=end, series_keys=keys)


@router.get("/market-quotes/meta")
def get_market_quotes_meta():
    return {
        "series": [
            {
                "key": s.key,
                "label": s.label,
                "unit": s.unit,
                "source_url": s.source_url,
            }
            for s in MARKET_SHEET_SERIES
        ],
        "source": "tarama",
    }
