"""x-ga4 — GA4 Data API'nin panelde kullanılmayan alanları.

Property'de mevcut olan ama hiç sorgulanmayan boyut/metrikleri tek sayfada
toplar. **Yalnızca GA4 Data API** kullanılır; başka servis, export veya kazıma
yoktur.

Bloklar:
  1. Kullanıcı & kararlılık — active1/7/28DayUsers, crashFreeUsersRate
  2. Varlık ilgisi        — customEvent:asset_key
  3. Uygulama davranışı   — customEvent:from / search_text / menu_item / sections_*
  4. İçerik derinliği     — pagePath × userEngagementDuration + newUsers
  5. Saatlik ritim        — hour × activeUsers
  6. Kitle                — brandingInterest, yaş/cinsiyet, audienceName

Her blok bağımsız çalışır: biri hata alırsa diğerleri etkilenmez, hata sayfada
görünür. Sessiz boş blok bırakılmaz.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

LOGGER = logging.getLogger(__name__)

_DEFAULT_SITE_ID = 1
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 300.0
_MAX_WORKERS = 6

# GA4 «değersiz» boyut değerleri — filtrelenmezse (not set) satırı listeyi yutuyor
_EMPTY_VALUES = ("(not set)", "", "(none)")

PROFILES: tuple[str, ...] = ("web", "mweb", "android", "ios")
APP_PROFILES: tuple[str, ...] = ("android", "ios")


def _types() -> Any:
    from google.analytics.data_v1beta import types

    return types


def _exclude_empty(dimension: str) -> Any:
    t = _types()
    return t.FilterExpression(
        not_expression=t.FilterExpression(
            filter=t.Filter(
                field_name=dimension,
                in_list_filter=t.Filter.InListFilter(values=list(_EMPTY_VALUES)),
            )
        )
    )


def _run(
    client: Any,
    property_id: str,
    *,
    dimensions: list[str],
    metrics: list[str],
    start: str,
    end: str,
    limit: int = 25,
    dimension_filter: Any = None,
    order_metric: str | None = None,
) -> list[dict[str, Any]]:
    """Tek RunReport → sözlük listesi. Boyut adları anahtar olur."""
    t = _types()
    kwargs: dict[str, Any] = {
        "property": f"properties/{property_id}",
        "dimensions": [t.Dimension(name=d) for d in dimensions],
        "metrics": [t.Metric(name=m) for m in metrics],
        "date_ranges": [t.DateRange(start_date=start, end_date=end)],
        "limit": max(1, min(int(limit), 250)),
    }
    if dimension_filter is not None:
        kwargs["dimension_filter"] = dimension_filter
    if order_metric:
        kwargs["order_bys"] = [
            t.OrderBy(metric=t.OrderBy.MetricOrderBy(metric_name=order_metric), desc=True)
        ]
    resp = client.run_report(t.RunReportRequest(**kwargs))
    out: list[dict[str, Any]] = []
    for row in resp.rows or []:
        item: dict[str, Any] = {}
        for i, dim in enumerate(dimensions):
            item[dim] = row.dimension_values[i].value if i < len(row.dimension_values) else ""
        for i, met in enumerate(metrics):
            raw = row.metric_values[i].value if i < len(row.metric_values) else "0"
            try:
                item[met] = float(raw or 0)
            except (TypeError, ValueError):
                item[met] = 0.0
        out.append(item)
    return out


def _block(name: str, fn: Any) -> dict[str, Any]:
    """Blok gövdesini hata yutmadan ama sayfayı düşürmeden çalıştır."""
    try:
        return {"ok": True, "error": None, **fn()}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("x-ga4 blok başarısız [%s]: %s", name, exc)
        return {"ok": False, "error": str(exc)[:220]}


# ── 1. Kullanıcı & kararlılık ───────────────────────────────────────────────

USER_METRICS = ("active1DayUsers", "active7DayUsers", "active28DayUsers")
STABILITY_METRICS = ("crashFreeUsersRate", "crashAffectedUsers")


def _user_stability(client: Any, properties: dict[str, str]) -> dict[str, Any]:
    """DAU / WAU / MAU + crash-free — Firebase konsolu yerine doğrudan GA4."""
    rows = []
    for pf in PROFILES:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        entry: dict[str, Any] = {"profile": pf}
        try:
            data = _run(
                client, pid,
                dimensions=[], metrics=list(USER_METRICS),
                start="yesterday", end="yesterday", limit=1,
            )
            entry.update(data[0] if data else {})
        except Exception as exc:  # noqa: BLE001
            entry["users_error"] = str(exc)[:140]
        # Crash-free yalnızca uygulamalarda anlamlı; web/mWeb'de GA4 sabit 1.0
        # döndürüyor ve bu "ölçüldü de çökme yok" izlenimi veriyor — yazılmaz.
        if pf in APP_PROFILES:
            try:
                data = _run(
                    client, pid,
                    dimensions=[], metrics=list(STABILITY_METRICS),
                    start="yesterday", end="yesterday", limit=1,
                )
                entry.update(data[0] if data else {})
            except Exception as exc:  # noqa: BLE001
                entry["stability_error"] = str(exc)[:140]
        rows.append(entry)
    return {"rows": rows}


# ── 2. Varlık ilgisi ────────────────────────────────────────────────────────

def _asset_interest(
    client: Any, properties: dict[str, str], start: str, end: str, limit: int
) -> dict[str, Any]:
    """`customEvent:asset_key` — hangi varlığa bakılıyor (işin merkezi)."""
    dim = "customEvent:asset_key"
    per_profile: dict[str, Any] = {}
    combined: dict[str, float] = {}
    for pf in PROFILES:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        try:
            rows = _run(
                client, pid,
                dimensions=[dim], metrics=["eventCount"],
                start=start, end=end, limit=limit,
                dimension_filter=_exclude_empty(dim), order_metric="eventCount",
            )
            per_profile[pf] = [
                {"asset": r[dim], "events": r["eventCount"]} for r in rows
            ]
            for r in rows:
                combined[r[dim]] = combined.get(r[dim], 0.0) + r["eventCount"]
        except Exception as exc:  # noqa: BLE001
            per_profile[pf] = {"error": str(exc)[:140]}
    top = sorted(combined.items(), key=lambda kv: -kv[1])[:limit]
    return {
        "per_profile": per_profile,
        "combined": [{"asset": k, "events": v} for k, v in top],
    }


# ── 3. Uygulama / site davranışı ────────────────────────────────────────────

# (profil, boyut, başlık) — boyut o property'de yoksa blok kendi hatasını yazar
BEHAVIOR_DIMENSIONS: tuple[tuple[str, str, str], ...] = (
    ("ios", "customEvent:from", "Habere nereden gelindi (iOS)"),
    ("ios", "customEvent:search_text", "Uygulama içi arama (iOS)"),
    ("ios", "customEvent:sections_enabled", "Açılan bölümler (iOS)"),
    ("ios", "customEvent:sections_disabled", "Kapatılan bölümler (iOS)"),
    ("web", "customEvent:menu_item", "Menü kullanımı (web)"),
)


def _behavior(
    client: Any, properties: dict[str, str], start: str, end: str, limit: int
) -> dict[str, Any]:
    groups = []
    for pf, dim, label in BEHAVIOR_DIMENSIONS:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        group: dict[str, Any] = {"profile": pf, "dimension": dim, "label": label}
        try:
            rows = _run(
                client, pid,
                dimensions=[dim], metrics=["eventCount"],
                start=start, end=end, limit=limit,
                dimension_filter=_exclude_empty(dim), order_metric="eventCount",
            )
            group["rows"] = [{"value": r[dim], "events": r["eventCount"]} for r in rows]
        except Exception as exc:  # noqa: BLE001
            group["error"] = str(exc)[:140]
            group["rows"] = []
        groups.append(group)
    return {"groups": groups}


# ── 4. İçerik derinliği ─────────────────────────────────────────────────────

DEPTH_METRICS = ("screenPageViews", "userEngagementDuration", "newUsers")


def _content_depth(
    client: Any, properties: dict[str, str], start: str, end: str, limit: int
) -> dict[str, Any]:
    """Sayfa başına gerçek okuma süresi ve yeni kullanıcı payı.

    `scrolledUsers` bu property'de veri döndürmüyor (scroll ölçümü boş), bu
    yüzden okuma derinliği `userEngagementDuration` üzerinden hesaplanır.
    """
    rows_out: list[dict[str, Any]] = []
    for pf in ("web", "mweb"):
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        rows = _run(
            client, pid,
            dimensions=["pagePath"], metrics=list(DEPTH_METRICS),
            start=start, end=end, limit=limit, order_metric="screenPageViews",
        )
        for r in rows:
            views = float(r.get("screenPageViews") or 0)
            engagement = float(r.get("userEngagementDuration") or 0)
            rows_out.append(
                {
                    "profile": pf,
                    "page": r.get("pagePath") or "",
                    "views": views,
                    "engagement_seconds": engagement,
                    "seconds_per_view": round(engagement / views, 1) if views else 0.0,
                    "new_users": float(r.get("newUsers") or 0),
                }
            )
    rows_out.sort(key=lambda r: -r["views"])
    return {"rows": rows_out[: limit * 2]}


# ── 5. Saatlik ritim ────────────────────────────────────────────────────────

def _hourly(
    client: Any, properties: dict[str, str], start: str, end: str
) -> dict[str, Any]:
    """Saat × aktif kullanıcı — yayın saati kararı için.

    GA4 yüksek kardinalitede `(other)` kovası döndürebiliyor; gizlenmez, ayrı
    satır olarak raporlanır.
    """
    series: dict[str, Any] = {}
    for pf in ("web", "mweb", "android", "ios"):
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        try:
            rows = _run(
                client, pid,
                dimensions=["hour"], metrics=["activeUsers", "sessions"],
                start=start, end=end, limit=30,
            )
        except Exception as exc:  # noqa: BLE001
            series[pf] = {"error": str(exc)[:140]}
            continue
        hours = []
        other = 0.0
        for r in rows:
            label = str(r.get("hour") or "")
            if label.isdigit():
                hours.append({"hour": int(label), "users": r["activeUsers"], "sessions": r["sessions"]})
            else:
                other += float(r.get("activeUsers") or 0)
        hours.sort(key=lambda h: h["hour"])
        series[pf] = {"hours": hours, "other_users": other}
    return {"series": series}


# ── 6. Kitle ────────────────────────────────────────────────────────────────

def _audience(
    client: Any, properties: dict[str, str], start: str, end: str, limit: int
) -> dict[str, Any]:
    pid = str(properties.get("web") or "").strip()
    if not pid:
        return {"interests": [], "demographics": [], "audiences": []}

    def _safe(dims: list[str], key: str) -> list[dict[str, Any]]:
        try:
            rows = _run(
                client, pid, dimensions=dims, metrics=["activeUsers"],
                start=start, end=end, limit=limit, order_metric="activeUsers",
            )
            return [
                {"label": " · ".join(str(r.get(d) or "") for d in dims), "users": r["activeUsers"]}
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("x-ga4 kitle bloğu [%s]: %s", key, exc)
            return []

    return {
        "interests": _safe(["brandingInterest"], "brandingInterest"),
        "demographics": _safe(["userAgeBracket", "userGender"], "demographics"),
        "audiences": _safe(["audienceName"], "audienceName"),
    }


# ── Toplayıcı ───────────────────────────────────────────────────────────────

def build_x_ga4_report(
    db: Any,
    *,
    site_id: int = _DEFAULT_SITE_ID,
    days: int = 7,
    limit: int = 15,
    force: bool = False,
) -> dict[str, Any]:
    """Altı bloğu paralel çeker. Kota maliyeti ölçüldü: blok başına birkaç token."""
    from backend.services.ga4_auth import get_ga4_connection_status

    safe_days = max(1, min(int(days or 7), 90))
    safe_limit = max(5, min(int(limit or 15), 50))
    cache_key = f"{site_id}|{safe_days}|{safe_limit}"
    if not force:
        hit = _CACHE.get(cache_key)
        if hit and (time.time() - hit[0]) < _CACHE_TTL_SEC:
            return {**hit[1], "cached": True}

    status = get_ga4_connection_status(db, site_id)
    if not status.get("connected"):
        return {
            "ok": False,
            "error": str(status.get("label") or "GA4 bağlı değil"),
            "blocks": {},
        }
    properties = (status.get("properties") or {}) if isinstance(status, dict) else {}
    if not properties:
        return {"ok": False, "error": "GA4 property tanımlı değil", "blocks": {}}

    from backend.collectors.ga4 import _client

    client = _client()
    start = f"{safe_days}daysAgo" if safe_days > 1 else "yesterday"
    end = "yesterday"

    jobs = {
        "user_stability": lambda: _block("user_stability", lambda: _user_stability(client, properties)),
        "assets": lambda: _block("assets", lambda: _asset_interest(client, properties, start, end, safe_limit)),
        "behavior": lambda: _block("behavior", lambda: _behavior(client, properties, start, end, safe_limit)),
        "content_depth": lambda: _block("content_depth", lambda: _content_depth(client, properties, start, end, safe_limit)),
        "hourly": lambda: _block("hourly", lambda: _hourly(client, properties, start, end)),
        "audience": lambda: _block("audience", lambda: _audience(client, properties, start, end, safe_limit)),
    }

    names = list(jobs.keys())
    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, len(names))) as pool:
        results = list(pool.map(lambda n: jobs[n](), names))

    blocks = dict(zip(names, results))
    out = {
        "ok": True,
        "error": None,
        "cached": False,
        "window": {"start": start, "end": end, "days": safe_days},
        "profiles": [p for p in PROFILES if str(properties.get(p) or "").strip()],
        "blocks": blocks,
        "note": "Tüm veriler GA4 Data API'den gelir; başka kaynak kullanılmaz.",
    }
    _CACHE[cache_key] = (time.time(), out)
    return out
