"""GA4 Data API kota ölçümü.

GA4 kotası istek sayısıyla değil **token** ile ölçülür; bir isteğin maliyeti
boyut sayısı, tarih aralığı uzunluğu ve dönen satır sayısıyla büyür. Kaç token
kaldığını yalnızca API söyleyebilir: istekte ``return_property_quota`` açılırsa
yanıt ``property_quota`` alanını taşır.

Bu modül client'ı ince bir proxy ile sarar; böylece tek yerden hem bayrak açılır
hem de her yanıttaki kota örneklenir. Ölçüm hiçbir koşulda çağrıyı bozmaz —
kota okunamazsa sessizce atlanır.

Realtime ayrı bir kota kovası kullandığı için ``kind`` ile ayrı tutulur.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any

LOGGER = logging.getLogger(__name__)

# Her yanıt için satır yazmak gece işinde yüzlerce insert demek; property+kind
# başına bu aralıkta en fazla bir örnek saklanır.
SAMPLE_MIN_INTERVAL_SEC = 300.0
# Kalan token bu orandan fazla düşerse aralığı beklemeden yaz (ani tüketim izi).
SAMPLE_DROP_TRIGGER = 0.10
RETENTION_DAYS = 30
_PRUNE_MIN_INTERVAL_SEC = 3600.0

_LOCK = threading.RLock()
_LAST_SAMPLE: dict[str, tuple[float, int]] = {}  # key -> (ts, tokens_per_day_remaining)
_LATEST: dict[str, dict[str, Any]] = {}  # key -> son okunan kota (bellekte, anlık görünüm)
_LAST_PRUNE_TS = 0.0

_QUOTA_FIELDS = (
    "tokens_per_day",
    "tokens_per_hour",
    "tokens_per_project_per_hour",
    "concurrent_requests",
)


def _property_id_from_request(request: Any) -> str:
    raw = str(getattr(request, "property", "") or "")
    return raw.rsplit("/", 1)[-1].strip() if raw else ""


def _extract_quota(response: Any) -> dict[str, int] | None:
    """`PropertyQuota` → düz sözlük. Alan yoksa None."""
    quota = getattr(response, "property_quota", None)
    if quota is None:
        return None
    out: dict[str, int] = {}
    found = False
    for field in _QUOTA_FIELDS:
        status = getattr(quota, field, None)
        if status is None:
            continue
        consumed = getattr(status, "consumed", None)
        remaining = getattr(status, "remaining", None)
        if consumed is None and remaining is None:
            continue
        found = True
        out[f"{field}_consumed"] = int(consumed or 0)
        out[f"{field}_remaining"] = int(remaining or 0)
    return out if found else None


def _should_persist(key: str, quota: dict[str, int]) -> bool:
    """Aralık dolduysa ya da kalan token belirgin düştüyse yaz."""
    now = time.time()
    remaining = int(quota.get("tokens_per_day_remaining") or 0)
    prev = _LAST_SAMPLE.get(key)
    if prev is None:
        return True
    prev_ts, prev_remaining = prev
    if (now - prev_ts) >= SAMPLE_MIN_INTERVAL_SEC:
        return True
    if prev_remaining > 0:
        drop = (prev_remaining - remaining) / float(prev_remaining)
        if drop >= SAMPLE_DROP_TRIGGER:
            return True
    return False


def _prune(db: Any) -> None:
    global _LAST_PRUNE_TS
    now = time.time()
    if (now - _LAST_PRUNE_TS) < _PRUNE_MIN_INTERVAL_SEC:
        return
    _LAST_PRUNE_TS = now
    from backend.models import Ga4QuotaSample

    cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    db.query(Ga4QuotaSample).filter(Ga4QuotaSample.recorded_at < cutoff).delete(
        synchronize_session=False
    )


def record_quota(response: Any, *, property_id: str, kind: str = "core") -> None:
    """Yanıttaki kotayı belleğe al, gerekiyorsa veritabanına örnekle."""
    if not property_id:
        return
    try:
        quota = _extract_quota(response)
        if not quota:
            return
        key = f"{kind}:{property_id}"
        snapshot = dict(quota)
        snapshot.update(
            {"property_id": property_id, "kind": kind, "recorded_at": datetime.utcnow()}
        )
        with _LOCK:
            _LATEST[key] = snapshot
            if not _should_persist(key, quota):
                return
            _LAST_SAMPLE[key] = (
                time.time(),
                int(quota.get("tokens_per_day_remaining") or 0),
            )

        from backend.database import SessionLocal
        from backend.models import Ga4QuotaSample

        with SessionLocal() as db:
            db.add(
                Ga4QuotaSample(
                    property_id=property_id,
                    kind=kind,
                    **{f: int(quota.get(f) or 0) for f in _sample_columns()},
                )
            )
            _prune(db)
            db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("GA4 kota örneklemesi atlandı: %s", exc)


def _sample_columns() -> tuple[str, ...]:
    return tuple(f"{f}_{suffix}" for f in _QUOTA_FIELDS for suffix in ("consumed", "remaining"))


class QuotaTrackingClient:
    """`BetaAnalyticsDataClient` proxy'si: kota bayrağını açar, yanıtı örnekler.

    Sarmalanan client'ın diğer tüm üyeleri olduğu gibi geçer; yalnızca rapor
    çağrıları araya alınır. Ölçüm hatası çağrıyı etkilemez.
    """

    def __init__(self, inner: Any, *, kind: str = "core") -> None:
        self._inner = inner
        self._kind = kind

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    @staticmethod
    def _enable_quota(request: Any) -> None:
        try:
            request.return_property_quota = True
        except Exception:  # noqa: BLE001
            pass

    def _call(self, method_name: str, kind: str, *args: Any, **kwargs: Any) -> Any:
        request = kwargs.get("request") if "request" in kwargs else (args[0] if args else None)
        if request is not None:
            self._enable_quota(request)
        response = getattr(self._inner, method_name)(*args, **kwargs)
        if request is not None:
            record_quota(
                response, property_id=_property_id_from_request(request), kind=kind
            )
        return response

    def run_report(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("run_report", self._kind, *args, **kwargs)

    def run_realtime_report(self, *args: Any, **kwargs: Any) -> Any:
        return self._call("run_realtime_report", "realtime", *args, **kwargs)


def track(client: Any, *, kind: str = "core") -> Any:
    """Client'ı kota ölçen proxy ile sar (iki kez sarmaz)."""
    if client is None or isinstance(client, QuotaTrackingClient):
        return client
    return QuotaTrackingClient(client, kind=kind)


def latest_snapshot() -> list[dict[str, Any]]:
    """Bellekteki en son okunan kotalar (property × kind)."""
    with _LOCK:
        return sorted(
            (dict(v) for v in _LATEST.values()),
            key=lambda r: (str(r.get("kind")), str(r.get("property_id"))),
        )


def quota_summary(db: Any, *, hours: int = 24) -> dict[str, Any]:
    """Panel için: son okunan kota + verilen pencerede görülen en düşük kalan."""
    from sqlalchemy import func

    from backend.models import Ga4QuotaSample

    since = datetime.utcnow() - timedelta(hours=max(1, int(hours)))
    rows: list[dict[str, Any]] = []
    try:
        agg = (
            db.query(
                Ga4QuotaSample.property_id,
                Ga4QuotaSample.kind,
                func.min(Ga4QuotaSample.tokens_per_day_remaining),
                func.min(Ga4QuotaSample.tokens_per_hour_remaining),
                func.max(Ga4QuotaSample.tokens_per_day_consumed),
                func.count(Ga4QuotaSample.id),
                func.max(Ga4QuotaSample.recorded_at),
            )
            .filter(Ga4QuotaSample.recorded_at >= since)
            .group_by(Ga4QuotaSample.property_id, Ga4QuotaSample.kind)
            .all()
        )
        for pid, kind, min_day, min_hour, max_consumed, n, last_at in agg:
            rows.append(
                {
                    "property_id": str(pid),
                    "kind": str(kind),
                    "tokens_per_day_remaining_min": int(min_day or 0),
                    "tokens_per_hour_remaining_min": int(min_hour or 0),
                    "tokens_per_day_consumed_max": int(max_consumed or 0),
                    "samples": int(n or 0),
                    "last_recorded_at": last_at.isoformat() if last_at else None,
                }
            )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("GA4 kota özeti okunamadı: %s", exc)

    return {
        "ok": True,
        "window_hours": int(hours),
        "latest": latest_snapshot(),
        "window": sorted(rows, key=lambda r: (r["kind"], r["property_id"])),
        "note": (
            "Kota token cinsindendir; maliyet boyut sayısı, tarih aralığı ve satır "
            "sayısıyla büyür. Realtime ayrı kovadır."
        ),
    }
