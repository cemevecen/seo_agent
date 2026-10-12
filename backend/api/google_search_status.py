"""Google Search Status — core/spam update markers for charts."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from backend.services import google_search_status as gss

router = APIRouter(tags=["google-search-status"])


@router.get("/google-search-status/updates")
def google_search_status_updates(
    start: str | None = Query(None, description="YYYY-MM-DD"),
    end: str | None = Query(None, description="YYYY-MM-DD"),
    kinds: str = Query("core,spam", description="Virgülle: core,spam,discover"),
) -> dict[str, Any]:
    kind_tuple = tuple(
        k.strip().lower() for k in str(kinds or "").split(",") if k.strip()
    ) or ("core", "spam")
    return gss.updates_in_range(start=start, end=end, kinds=kind_tuple)
