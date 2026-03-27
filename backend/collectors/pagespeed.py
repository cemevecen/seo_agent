"""PageSpeed Insights collector'ı."""

from __future__ import annotations

import json
import logging
import socket
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.alert_engine import emit_custom_alert, evaluate_site_alerts
from backend.services.metric_store import get_latest_metrics, save_metrics
from backend.services.quota_guard import consume_api_quota

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
LOGGER = logging.getLogger(__name__)

STRATEGY_METRIC_MAP = {
    "mobile": {
        "performance_score": "pagespeed_mobile_score",
        "lcp": "pagespeed_mobile_lcp",
        "cls": "pagespeed_mobile_cls",
        "inp": "pagespeed_mobile_inp",
    },
    "desktop": {
        "performance_score": "pagespeed_desktop_score",
        "lcp": "pagespeed_desktop_lcp",
        "cls": "pagespeed_desktop_cls",
        "inp": "pagespeed_desktop_inp",
    },
}


def _normalize_url(domain: str) -> str:
    # API çağrıları için çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    return f"https://{domain}"


def _extract_lighthouse_metrics(payload: dict) -> dict[str, float]:
    # Lighthouse sonucundan gerekli temel performans alanlarını ayıklar.
    lighthouse = payload.get("lighthouseResult", {})
    categories = lighthouse.get("categories", {})
    audits = lighthouse.get("audits", {})
    performance_score = categories.get("performance", {}).get("score") or 0
    lcp = (audits.get("largest-contentful-paint") or {}).get("numericValue") or 0
    cls = (audits.get("cumulative-layout-shift") or {}).get("numericValue") or 0
    inp = (
        (audits.get("interaction-to-next-paint") or {}).get("numericValue")
        or (audits.get("experimental-interaction-to-next-paint") or {}).get("numericValue")
        or 0
    )
    return {
        "performance_score": float(performance_score) * 100,
        "lcp": float(lcp),
        "cls": float(cls),
        "inp": float(inp),
    }


def _fetch_pagespeed(url: str, strategy: str) -> dict[str, float]:
    # API key yoksa deterministic mock veri döndürür, varsa gerçek API çağrısı yapar.
    api_key = settings.google_api_key.strip()
    if not api_key or api_key.startswith("local-"):
        if strategy == "mobile":
            return {"performance_score": 72.0, "lcp": 2850.0, "cls": 0.08, "inp": 180.0}
        return {"performance_score": 89.0, "lcp": 1650.0, "cls": 0.03, "inp": 110.0}

    query = urlencode({"url": url, "strategy": strategy, "key": api_key, "category": "performance"})
    with urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=settings.pagespeed_request_timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _extract_lighthouse_metrics(payload)


def _fetch_pagespeed_with_retries(url: str, strategy: str) -> dict[str, float]:
    # Geçici ağ hatalarında yeniden deneyip kalıcı hataları açıklayıcı şekilde döndürür.
    attempts = max(1, settings.pagespeed_max_retries + 1)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return _fetch_pagespeed(url, strategy)
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="ignore")[:300]
            if exc.code not in {408, 429, 500, 502, 503, 504}:
                raise RuntimeError(f"{strategy} istegi reddedildi ({exc.code}). {details}".strip()) from exc
            last_error = RuntimeError(f"{strategy} istegi gecici olarak basarisiz oldu ({exc.code}). {details}".strip())
        except (TimeoutError, socket.timeout, URLError) as exc:
            last_error = exc

        LOGGER.warning("PageSpeed %s denemesi %s/%s basarisiz oldu: %s", strategy, attempt, attempts, last_error)
        if attempt < attempts:
            time.sleep(max(0.0, settings.pagespeed_retry_backoff_seconds) * attempt)

    raise RuntimeError(f"{strategy} PageSpeed verisi alinamadi: {last_error}") from last_error


def _load_latest_strategy_metrics(db: Session, site_id: int, strategy: str) -> dict[str, float] | None:
    # Strateji icin daha once kaydedilmis son metrikleri fallback olarak yukler.
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site_id)}
    metric_names = STRATEGY_METRIC_MAP[strategy]
    if any(latest.get(metric_name) is None for metric_name in metric_names.values()):
        return None
    return {
        key: float(latest[metric_name].value)
        for key, metric_name in metric_names.items()
    }


def _flatten_strategy_metrics(strategy: str, payload: dict[str, float]) -> dict[str, float]:
    return {
        STRATEGY_METRIC_MAP[strategy][key]: value
        for key, value in payload.items()
    }


def collect_pagespeed_metrics(db: Session, site: Site) -> dict:
    """Mobile ve desktop performans verilerini toplayıp Metric tablosuna kaydeder."""
    decision = consume_api_quota(db, site, provider="pagespeed", units=2)
    if not decision.allowed:
        return {
            "site_id": site.id,
            "blocked": True,
            "reason": decision.reason,
        }

    target_url = _normalize_url(site.domain)
    collected_at = datetime.utcnow()
    metrics: dict[str, float] = {}
    strategy_payloads: dict[str, dict[str, float] | None] = {"mobile": None, "desktop": None}
    strategy_status: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}

    for strategy in ("mobile", "desktop"):
        try:
            payload = _fetch_pagespeed_with_retries(target_url, strategy)
            strategy_payloads[strategy] = payload
            strategy_status[strategy] = {"state": "fresh", "message": "Canli veri guncellendi."}
            metrics.update(_flatten_strategy_metrics(strategy, payload))
        except RuntimeError as exc:
            fallback = _load_latest_strategy_metrics(db, site.id, strategy)
            errors[strategy] = str(exc)
            if fallback is not None:
                strategy_payloads[strategy] = fallback
                strategy_status[strategy] = {
                    "state": "stale",
                    "message": "Canli istek basarisiz oldu, son basarili olcum gosteriliyor.",
                }
                emit_custom_alert(
                    db,
                    site,
                    f"pagespeed_{strategy}_fetch_error",
                    f"{site.domain} icin {strategy} PageSpeed istegi basarisiz oldu. Son basarili olcum korunuyor. Hata: {exc}",
                    dedupe_hours=3,
                )
            else:
                strategy_status[strategy] = {
                    "state": "failed",
                    "message": "Canli veri alinamadi ve gosterilecek onceki olcum bulunmuyor.",
                }
                emit_custom_alert(
                    db,
                    site,
                    f"pagespeed_{strategy}_fetch_error",
                    f"{site.domain} icin {strategy} PageSpeed istegi basarisiz oldu ve onceki olcum bulunmuyor. Hata: {exc}",
                    dedupe_hours=3,
                )
            LOGGER.warning("PageSpeed %s fallback durumuna gecti for %s: %s", strategy, site.domain, exc)

    if metrics:
        save_metrics(db, site.id, metrics, collected_at)
        evaluate_site_alerts(db, site)

    return {
        "site_id": site.id,
        "url": target_url,
        "mobile": strategy_payloads["mobile"],
        "desktop": strategy_payloads["desktop"],
        "status": strategy_status,
        "errors": errors,
        "saved_metric_count": len(metrics),
    }