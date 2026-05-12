"""GA4 Realtime API — pencereli karşılaştırma ve alarm değerlendirmesi.

Son 30 dakikayı iki pencereye böler (ör. 0-9 dk vs 10-19 dk) ve
activeUsers/screenPageViews değişimini izleyerek alarm tetikler.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Metric,
    MinuteRange,
    RunRealtimeReportRequest,
)
from google.oauth2 import service_account
from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.ga4_auth import (
    GA4_SCOPES,
    get_ga4_credentials_record,
    load_ga4_properties,
    load_ga4_service_account_info,
)

logger = logging.getLogger(__name__)

# ── Alarm eşikleri ────────────────────────────────────────────────────────────
# Yüzdesel düşüş/artış eşikleri (ayarlar sayfasından override edilebilir)
ALARM_RULES: dict[str, dict[str, Any]] = {
    "traffic_drop": {
        "label": "Traffic düşüşü",
        "metric": "activeUsers",
        "direction": "drop",
        "threshold_pct": 40,
        "min_baseline": 5,
        "severity": "critical",
    },
    "traffic_spike": {
        "label": "Traffic artışı",
        "metric": "activeUsers",
        "direction": "spike",
        "threshold_pct": 80,
        "min_baseline": 5,
        "severity": "warning",
    },
    "pageview_drop": {
        "label": "Sayfa görüntüleme düşüşü",
        "metric": "screenPageViews",
        "direction": "drop",
        "threshold_pct": 50,
        "min_baseline": 10,
        "severity": "warning",
    },
}

# Varsayılan pencere boyutu (dakika)
DEFAULT_WINDOW_MINUTES = 10


def _build_client() -> BetaAnalyticsDataClient:
    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_realtime_comparison(
    property_id: str,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    *,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """İki minuteRange penceresi ile Realtime API çağrısı yapar.

    Pencere A (current): son `window_minutes` dakika (0 → window-1)
    Pencere B (previous): onun önceki `window_minutes` dakikası

    Returns dict: {current: {...}, previous: {...}, comparison: {...}, fetched_at: ...}
    """
    if client is None:
        client = _build_client()

    w = max(1, min(window_minutes, 15))
    current_end = 0
    current_start = w - 1
    previous_end = w
    previous_start = 2 * w - 1

    metrics = [
        Metric(name="activeUsers"),
        Metric(name="screenPageViews"),
        Metric(name="eventCount"),
        Metric(name="conversions"),
    ]

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        metrics=metrics,
        minute_ranges=[
            MinuteRange(
                name="current",
                start_minutes_ago=current_start,
                end_minutes_ago=current_end,
            ),
            MinuteRange(
                name="previous",
                start_minutes_ago=previous_start,
                end_minutes_ago=previous_end,
            ),
        ],
    )

    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    windows: dict[str, dict[str, float]] = {"current": {}, "previous": {}}

    for row in response.rows:
        range_name = ""
        for dv in row.dimension_values:
            val = dv.value
            if val in ("current", "previous"):
                range_name = val
                break

        key = range_name if range_name in ("current", "previous") else "current"
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                windows[key][mname] = windows[key].get(mname, 0) + float(mv.value)
            except (ValueError, TypeError):
                pass

    comparison = _build_comparison(windows["current"], windows["previous"])

    return {
        "property_id": property_id,
        "window_minutes": w,
        "current": windows["current"],
        "previous": windows["previous"],
        "comparison": comparison,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
    }


def _build_comparison(current: dict[str, float], previous: dict[str, float]) -> dict[str, dict[str, Any]]:
    """Her metrik için yüzdesel değişim ve yön hesaplar."""
    result: dict[str, dict[str, Any]] = {}
    all_keys = set(list(current.keys()) + list(previous.keys()))
    for key in sorted(all_keys):
        cur = current.get(key, 0.0)
        prev = previous.get(key, 0.0)
        if prev > 0:
            pct_change = ((cur - prev) / prev) * 100.0
        elif cur > 0:
            pct_change = 100.0
        else:
            pct_change = 0.0
        result[key] = {
            "current": cur,
            "previous": prev,
            "change_pct": round(pct_change, 1),
            "direction": "up" if pct_change > 0 else ("down" if pct_change < 0 else "flat"),
        }
    return result


def evaluate_alarms(
    comparison: dict[str, dict[str, Any]],
    *,
    rules: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Karşılaştırma sonuçlarına alarm kurallarını uygular.

    Returns list of triggered alarms: [{rule_id, label, metric, ...}, ...]
    """
    if rules is None:
        rules = ALARM_RULES

    triggered: list[dict[str, Any]] = []

    for rule_id, rule in rules.items():
        metric_name = rule["metric"]
        comp = comparison.get(metric_name)
        if comp is None:
            continue

        prev_val = comp["previous"]
        cur_val = comp["current"]
        change_pct = comp["change_pct"]

        if prev_val < rule.get("min_baseline", 0):
            continue

        threshold = rule["threshold_pct"]
        direction = rule["direction"]

        fire = False
        if direction == "drop" and change_pct <= -threshold:
            fire = True
        elif direction == "spike" and change_pct >= threshold:
            fire = True

        if fire:
            triggered.append({
                "rule_id": rule_id,
                "label": rule["label"],
                "metric": metric_name,
                "severity": rule.get("severity", "warning"),
                "current_value": cur_val,
                "previous_value": prev_val,
                "change_pct": change_pct,
                "threshold_pct": threshold,
                "message": (
                    f"{rule['label']}: {metric_name} "
                    f"{prev_val:.0f} → {cur_val:.0f} ({change_pct:+.1f}%)"
                ),
            })

    return triggered


def check_site_realtime(
    db: Session,
    site: Site,
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    profile: str = "web",
) -> dict[str, Any]:
    """Tek bir site+profil için realtime kontrol çalıştırır.

    1. GA4 property_id bulunur
    2. Realtime API çağrılır
    3. Alarm kuralları değerlendirilir
    4. Sonuç DB'ye kaydedilir

    Returns full result dict.
    """
    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")

    if not property_id:
        return {
            "site_id": site.id,
            "domain": site.domain,
            "error": "no_ga4_property",
            "message": f"Site {site.domain} için GA4 property ({profile}) tanımlı değil.",
        }

    try:
        result = fetch_realtime_comparison(property_id, window_minutes)
    except Exception as exc:
        logger.warning("GA4 Realtime API hatası [%s / %s]: %s", site.domain, property_id, exc)
        return {
            "site_id": site.id,
            "domain": site.domain,
            "profile": profile,
            "error": "api_error",
            "message": str(exc),
        }

    alarms = evaluate_alarms(result["comparison"])

    result["site_id"] = site.id
    result["domain"] = site.domain
    result["profile"] = profile
    result["alarms"] = alarms
    result["alarm_count"] = len(alarms)

    _save_snapshot(db, site.id, profile, result)

    if alarms:
        _save_alarm_logs(db, site.id, alarms)
        logger.warning(
            "GA4 Realtime ALARM [%s]: %d kural tetiklendi — %s",
            site.domain,
            len(alarms),
            "; ".join(a["message"] for a in alarms),
        )

    return result


def _save_snapshot(db: Session, site_id: int, profile: str, result: dict[str, Any]) -> None:
    """Realtime kontrol sonucunu DB'ye kaydeder."""
    import json as _json

    from backend.models import RealtimeSnapshot

    snapshot = RealtimeSnapshot(
        site_id=site_id,
        profile=profile,
        active_users_current=result.get("current", {}).get("activeUsers", 0),
        active_users_previous=result.get("previous", {}).get("activeUsers", 0),
        pageviews_current=result.get("current", {}).get("screenPageViews", 0),
        pageviews_previous=result.get("previous", {}).get("screenPageViews", 0),
        window_minutes=result.get("window_minutes", DEFAULT_WINDOW_MINUTES),
        alarm_count=len(result.get("alarms", [])),
        payload_json=_json.dumps(result, default=str, ensure_ascii=False),
    )
    db.add(snapshot)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("RealtimeSnapshot kayıt hatası (site_id=%s)", site_id)


def _save_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]]) -> None:
    """Tetiklenen alarmları DB'ye kaydeder."""
    from backend.models import RealtimeAlarmLog

    for alarm in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=alarm["rule_id"],
            metric=alarm["metric"],
            severity=alarm.get("severity", "warning"),
            current_value=alarm["current_value"],
            previous_value=alarm["previous_value"],
            change_pct=alarm["change_pct"],
            message=alarm["message"],
        )
        db.add(log)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("RealtimeAlarmLog kayıt hatası (site_id=%s)", site_id)


def get_recent_snapshots(
    db: Session,
    site_id: int,
    *,
    profile: str = "web",
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Son N snapshot kaydını döner (mini trend grafiği için)."""
    import json as _json

    from backend.models import RealtimeSnapshot

    rows = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
        .order_by(RealtimeSnapshot.collected_at.desc())
        .limit(limit)
        .all()
    )
    result = []
    for row in reversed(rows):
        result.append({
            "collected_at": row.collected_at.isoformat() if row.collected_at else None,
            "active_users": row.active_users_current,
            "active_users_prev": row.active_users_previous,
            "pageviews": row.pageviews_current,
            "pageviews_prev": row.pageviews_previous,
            "alarm_count": row.alarm_count,
            "window_minutes": row.window_minutes,
        })
    return result


def get_recent_alarms(
    db: Session,
    site_id: int,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Son N alarm kaydını döner."""
    from backend.models import RealtimeAlarmLog

    rows = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(RealtimeAlarmLog.triggered_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": row.id,
            "rule_id": row.rule_id,
            "metric": row.metric,
            "severity": row.severity,
            "current_value": row.current_value,
            "previous_value": row.previous_value,
            "change_pct": row.change_pct,
            "message": row.message,
            "triggered_at": row.triggered_at.isoformat() if row.triggered_at else None,
        }
        for row in rows
    ]


def run_all_sites_realtime_check(db: Session, *, window_minutes: int = DEFAULT_WINDOW_MINUTES) -> list[dict[str, Any]]:
    """Tüm aktif siteleri kontrol eder — scheduler job'ından çağrılır."""
    from backend.models import Site as SiteModel

    sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()
    results = []
    for site in sites:
        try:
            r = check_site_realtime(db, site, window_minutes=window_minutes)
            results.append(r)
        except Exception as exc:
            logger.exception("Realtime check başarısız [%s]: %s", site.domain, exc)
            results.append({
                "site_id": site.id,
                "domain": site.domain,
                "error": "check_failed",
                "message": str(exc),
            })
    return results
