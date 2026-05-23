"""Mağaza katalog araması + yorum analizi API — /app sayfası Vivindis-tarzı panel için."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from backend.services import store_catalog_search
from backend.services import store_review_analysis as sra

router = APIRouter(prefix="/store", tags=["store-catalog"])
logger = logging.getLogger(__name__)


class StoreSearchResultItem(BaseModel):
    id: str
    name: str
    developer: str | None = None
    icon: str | None = None
    rating: float | None = None
    review_count: int | None = None
    platform: Literal["google_play", "app_store"]
    store_url: str


class StoreSearchResponse(BaseModel):
    results: list[StoreSearchResultItem]
    has_more: bool = False
    offset: int = 0


_PLATFORM_RE = re.compile(r"^(google_play|app_store|both)$")


@router.get("/search", response_model=StoreSearchResponse)
async def search_stores(
    q: Annotated[str, Query(min_length=2, max_length=200)],
    platform: Annotated[str, Query()],
    lang: Annotated[str, Query(min_length=2, max_length=8)] = "tr",
    country: Annotated[str, Query(min_length=2, max_length=8)] = "tr",
    num: Annotated[int, Query(ge=1, le=50)] = 20,
    offset: Annotated[int, Query(ge=0, le=500)] = 0,
) -> StoreSearchResponse:
    if not _PLATFORM_RE.match(platform):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="platform google_play, app_store veya both olmalı.",
        )
    query = q.strip()
    if len(query) < 2:
        raise HTTPException(status_code=400, detail="Sorgu çok kısa.")

    try:
        rows, has_more, off = await store_catalog_search.search_catalog(
            query, platform, lang, country, num, offset,
        )
    except Exception as exc:
        logger.warning("store_catalog_search failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Mağaza araması başarısız.",
        ) from exc

    items = [StoreSearchResultItem.model_validate(r) for r in rows]
    return StoreSearchResponse(results=items, has_more=has_more, offset=off)


# ─── Yorum analizi endpoint'i ──────────────────────────────────────────────────

_PLATFORM_SINGLE_RE = re.compile(r"^(google_play|app_store)$")


class ReviewAnalysisRequest(BaseModel):
    app_id: str
    platform: str
    days: int = 30
    lang: str = "tr"
    country: str = "tr"
    limit: int = 300


class ReviewAnalysisResponse(BaseModel):
    app_id: str
    platform: str
    days: int
    reviews_fetched: int
    app_meta: dict[str, Any] = {}
    analysis: dict[str, Any]
    reviews: list[dict[str, Any]] = []


@router.post("/review-analysis", response_model=ReviewAnalysisResponse)
async def store_review_analysis(body: ReviewAnalysisRequest) -> ReviewAnalysisResponse:
    """Belirtilen mağaza uygulamasının yorumlarını çeker ve heuristic analiz yapar."""
    app_id = (body.app_id or "").strip()
    if not app_id:
        raise HTTPException(status_code=400, detail="app_id boş olamaz.")
    if not _PLATFORM_SINGLE_RE.match(body.platform):
        raise HTTPException(status_code=400, detail="platform google_play veya app_store olmalı.")
    days = max(1, min(body.days, 730))
    limit = max(50, min(body.limit, 1000))

    try:
        result = await run_in_threadpool(
            sra.analyze_store_app,
            app_id,
            body.platform,
            days=days,
            lang=body.lang,
            country=body.country,
            limit=limit,
        )
    except Exception as exc:
        logger.warning("store review analysis failed (%s/%s): %s", body.platform, app_id, exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Yorum analizi başarısız: {exc}") from exc

    return ReviewAnalysisResponse(
        app_id=app_id,
        platform=body.platform,
        days=days,
        reviews_fetched=result["reviews_fetched"],
        app_meta=result.get("app_meta") or {},
        analysis=result["analysis"],
        reviews=result.get("reviews") or [],
    )
