"""pagespeed.web.dev scrape ingest API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.pagespeed_web_scrape_store import ingest_pagespeed_web_scrape

router = APIRouter(tags=["pagespeed-web"])


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


class PagespeedWebIngestBody(BaseModel):
    domain: str
    form_factor: str = "mobile"
    analysis_url: str = ""
    scope_note: str = ""
    psi_payload: dict[str, Any] = Field(default_factory=dict)


@router.post("/pagespeed-web/ingest")
def pagespeed_web_ingest(
    body: PagespeedWebIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = ingest_pagespeed_web_scrape(
        db,
        domain=body.domain,
        form_factor=body.form_factor,
        psi_payload=body.psi_payload,
        analysis_url=body.analysis_url,
        scope_note=body.scope_note,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result
