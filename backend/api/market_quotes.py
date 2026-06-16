"""Piyasa kapanış serileri — Sheets senkron ve grafik overlay API."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services.market_sheets_config import SERIES_BY_KEY
from backend.services.market_sheets_sync import query_overlay, sync_all_market_sheets

router = APIRouter(tags=["market-quotes"])


@router.post("/market-quotes/sync")
def post_market_quotes_sync():
    """Tüm Google Sheets tablolarını çekip DB'ye yazar (haftalık güncelleme sonrası)."""
    try:
        return sync_all_market_sheets()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
            {"key": s.key, "label": s.label, "unit": s.unit}
            for s in SERIES_BY_KEY.values()
        ],
    }
