"""Dis sitelere giden nazik HTTP istekleri icin ortak yardimcilar."""

from __future__ import annotations

import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from backend.config import settings

_STATE_LOCK = threading.Lock()
_HOST_NEXT_ALLOWED_AT: dict[str, float] = {}
_CACHE: dict[str, tuple[float, int, str]] = {}


def fetch_text(
    url: str,
    *,
    timeout_seconds: int | None = None,
    cache_ttl_seconds: int | None = None,
    min_interval_seconds: float | None = None,
) -> tuple[int, str]:
    """Metin cevabini cache ve host bazli pacing ile getirir."""
    ttl = max(0, int(cache_ttl_seconds if cache_ttl_seconds is not None else settings.outbound_cache_ttl_seconds))
    now = time.monotonic()

    if ttl > 0:
        with _STATE_LOCK:
            cached = _CACHE.get(url)
            if cached and cached[0] > now:
                return cached[1], cached[2]

    host = (urlparse(url).netloc or "").lower()
    delay = max(0.0, float(min_interval_seconds if min_interval_seconds is not None else settings.outbound_min_interval_seconds))
    if host and delay > 0:
        with _STATE_LOCK:
            next_allowed_at = _HOST_NEXT_ALLOWED_AT.get(host, 0.0)
            wait_seconds = max(0.0, next_allowed_at - now)
            _HOST_NEXT_ALLOWED_AT[host] = max(now, next_allowed_at) + delay
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    request = Request(
        url=url,
        headers={
            "User-Agent": settings.outbound_user_agent,
            "Accept": "text/html,application/xml,text/plain;q=0.9,*/*;q=0.8",
        },
    )
    timeout = max(1, int(timeout_seconds if timeout_seconds is not None else settings.crawler_request_timeout_seconds))

    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            status = getattr(response, "status", 200)
            body = response.read().decode(charset, errors="ignore")
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        status = exc.code
        body = exc.read().decode(charset, errors="ignore")
    except URLError:
        status = 0
        body = ""

    if ttl > 0:
        with _STATE_LOCK:
            _CACHE[url] = (time.monotonic() + ttl, status, body)
    return status, body
