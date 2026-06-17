"""
GitLab API relay — şirket ağında çalışır; Railway (SEO Agent) dışarıdan bu URL'ye bağlanır.

Ortam:
  GITLAB_UPSTREAM_URL=https://git.nokta.com/api/v4
  GITLAB_PRIVATE_TOKEN=...
  GITLAB_RELAY_SECRET=...   (SEO Agent Railway'de aynı değer + GITLAB_API_BASE_URL=https://relay-host/api/v4)
  PORT=8090
"""

from __future__ import annotations

import os

import httpx
from fastapi import FastAPI, Request, Response

UPSTREAM = (os.environ.get("GITLAB_UPSTREAM_URL") or "https://git.nokta.com/api/v4").rstrip("/")
TOKEN = (os.environ.get("GITLAB_PRIVATE_TOKEN") or "").strip()
RELAY_KEY = (os.environ.get("GITLAB_RELAY_SECRET") or "").strip()

app = FastAPI(title="GitLab API Relay", docs_url=None, redoc_url=None)

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _check_relay_key(request: Request) -> bool:
    if not RELAY_KEY:
        return True
    got = (request.headers.get("x-gitlab-relay-key") or "").strip()
    return got == RELAY_KEY


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.api_route("/api/v4/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def gitlab_proxy(path: str, request: Request) -> Response:
    if not TOKEN:
        return Response(status_code=503, content="GITLAB_PRIVATE_TOKEN not configured")
    if not _check_relay_key(request):
        return Response(status_code=401, content="invalid relay key")

    query = request.url.query
    url = f"{UPSTREAM}/{path}" + (f"?{query}" if query else "")

    forward_headers: dict[str, str] = {}
    for key, value in request.headers.items():
        low = key.lower()
        if low in _HOP_BY_HOP or low == "private-token":
            continue
        if low in ("content-type", "accept"):
            forward_headers[key] = value
    forward_headers["PRIVATE-TOKEN"] = TOKEN

    body = await request.body()
    timeout = httpx.Timeout(15.0, read=90.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        upstream = await client.request(request.method, url, headers=forward_headers, content=body or None)

    resp_headers = {
        k: v
        for k, v in upstream.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "content-encoding"
    }
    return Response(content=upstream.content, status_code=upstream.status_code, headers=resp_headers)
