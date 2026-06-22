"""GA4 Realtime — günlük property token kotası (429) devre kesici."""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from backend.config import settings

LOGGER = logging.getLogger(__name__)
_LOCK = threading.RLock()
_PAUSE_UNTIL: dict[str, float] = {}
_LAST_WARN_LOG: dict[str, float] = {}
_WARN_LOG_INTERVAL_SEC = 3600.0

_PROPERTY_ID_RE = re.compile(r"properties/(\d+)")


class Ga4RealtimeQuotaPausedError(RuntimeError):
    """Property günlük Realtime kotası dolu; API çağrısı atlandı."""


def normalize_property_id(property_id: str) -> str:
    raw = str(property_id or "").strip()
    m = _PROPERTY_ID_RE.search(raw)
    if m:
        return m.group(1)
    return raw.lstrip("properties/").strip()


def is_daily_quota_exhausted_message(message: str | None) -> bool:
    text = str(message or "").lower()
    if "exhausted property tokens" in text:
        return True
    if "429" in text and ("quota" in text or "token" in text):
        return True
    return False


def _pause_until_ts() -> float:
    """Kota dolduğunda bir sonraki «gündüz job» başlangıcına kadar bekle (varsayılan 06:00 TR)."""
    tz_name = (getattr(settings, "report_calendar_timezone", None) or "Europe/Istanbul").strip()
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    resume_hour = int(getattr(settings, "ga4_realtime_quota_resume_hour", 6))
    resume_hour = max(0, min(23, resume_hour))
    target = now.replace(hour=resume_hour, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target.timestamp()


def pause_property(property_id: str) -> float:
    pid = normalize_property_id(property_id)
    if not pid:
        return 0.0
    until = _pause_until_ts()
    with _LOCK:
        prev = _PAUSE_UNTIL.get(pid)
        if prev is None or until > prev:
            _PAUSE_UNTIL[pid] = until
    return until


def is_property_paused(property_id: str) -> bool:
    pid = normalize_property_id(property_id)
    if not pid:
        return False
    now = time.time()
    with _LOCK:
        until = _PAUSE_UNTIL.get(pid)
        if until is None:
            return False
        if now >= until:
            _PAUSE_UNTIL.pop(pid, None)
            return False
        return True


def paused_property_resume_times() -> dict[str, float]:
    """Property ID → unix resume time (kota devre kesici; bellek içi, restart sonrası sıfırlanır)."""
    now = time.time()
    with _LOCK:
        active = {pid: until for pid, until in _PAUSE_UNTIL.items() if until > now}
        for pid in list(_PAUSE_UNTIL):
            if _PAUSE_UNTIL[pid] <= now:
                _PAUSE_UNTIL.pop(pid, None)
        return dict(active)


def assert_property_realtime_allowed(property_id: str) -> None:
    pid = normalize_property_id(property_id)
    if is_property_paused(pid):
        raise Ga4RealtimeQuotaPausedError(
            f"GA4 Realtime günlük kota aşıldı (property {pid}); "
            f"otomatik yenileme saat {int(getattr(settings, 'ga4_realtime_quota_resume_hour', 6)):02d}:00 "
            f"({getattr(settings, 'report_calendar_timezone', 'Europe/Istanbul')}) sonrası denenecek."
        )


def note_realtime_quota_error(
    property_id: str,
    exc: BaseException,
    *,
    domain: str = "",
    logger: logging.Logger | None = None,
) -> bool:
    """429 / exhausted property tokens → pause + seyrek log. True ise kota işaretlendi."""
    if not is_daily_quota_exhausted_message(str(exc)):
        return False
    pid = normalize_property_id(property_id)
    pause_property(pid)
    log = logger or LOGGER
    now = time.time()
    with _LOCK:
        last = _LAST_WARN_LOG.get(pid, 0.0)
        if now - last >= _WARN_LOG_INTERVAL_SEC:
            _LAST_WARN_LOG[pid] = now
            log.warning(
                "GA4 Realtime günlük kota doldu — property %s%s API çağrıları duraklatıldı (sonraki pencere: sabah job). Detay: %s",
                pid,
                f" [{domain}]" if domain else "",
                str(exc)[:200],
            )
        else:
            log.debug("GA4 Realtime kota (property %s): %s", pid, str(exc)[:120])
    return True


def light_realtime_domains() -> frozenset[str]:
    raw = (getattr(settings, "ga4_realtime_light_domains", "") or "").strip()
    if not raw:
        return frozenset()
    parts = {p.strip().lower().lstrip(".") for p in raw.replace(";", ",").split(",") if p.strip()}
    return frozenset(parts)


def domain_is_light_realtime(domain: str | None) -> bool:
    allow = light_realtime_domains()
    if not allow:
        return False
    d = str(domain or "").strip().lower()
    d = d.replace("https://", "").replace("http://", "").split("/")[0]
    if d.startswith("www."):
        d = d[4:]
    if d in allow:
        return True
    return any(d.endswith(f".{a}") or d == a for a in allow)


def scheduler_profiles_for_site(domain: str | None, properties: dict[str, str]) -> tuple[str, ...]:
    """Zamanlayıcı: hafif domainlerde yalnızca web (+ ayrı mweb property varsa mweb)."""
    base = ("web", "mweb", "ios", "android")
    if not domain_is_light_realtime(domain):
        return base
    web_pid = (properties.get("web") or "").strip()
    mweb_pid = (properties.get("mweb") or "").strip()
    out: list[str] = ["web"]
    if mweb_pid and mweb_pid != web_pid:
        out.append("mweb")
    return tuple(out)
