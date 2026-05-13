"""Mağaza katalog araması API — Realtime sayfasındaki Vivindis-tarzı panel için."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from backend.services import store_catalog_search

router = APIRouter(prefix="/store", tags=["store-catalog"])


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
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Mağaza araması başarısız.",
        ) from exc

    items = [StoreSearchResultItem.model_validate(r) for r in rows]
    return StoreSearchResponse(results=items, has_more=has_more, offset=off)
