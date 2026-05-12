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

# Sayfa bazlı alarm eşikleri
PAGE_ALARM_RULES: dict[str, dict[str, Any]] = {
    "page_traffic_drop": {
        "label": "Sayfa trafik düşüşü",
        "direction": "drop",
        "threshold_pct": 50,
        "min_users": 20,
        "severity": "warning",
    },
    "page_traffic_spike": {
        "label": "Sayfa trafik artışı",
        "direction": "spike",
        "threshold_pct": 100,
        "min_users": 20,
        "severity": "warning",
    },
    "page_disappeared": {
        "label": "Sayfa top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 30,
        "severity": "critical",
    },
    "page_new_entry": {
        "label": "Yeni sayfa top listeye girdi",
        "direction": "new_entry",
        "min_users": 50,
        "severity": "info",
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

    # Realtime API max 2 MinuteRange destekler ve max 29 dk geriye gider.
    # 30 dk'yı iki 15'lik yarıya böleriz: current (0-14) + previous (15-29).
    # Toplam 30 dk değeri için ayrı tek range'li bir çağrı yapılır.
    half = 15

    metrics = [
        Metric(name="activeUsers"),
        Metric(name="screenPageViews"),
        Metric(name="eventCount"),
        Metric(name="conversions"),
    ]

    # Çağrı 1: Karşılaştırma (2 range)
    req_compare = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=half - 1, end_minutes_ago=0),
            MinuteRange(name="previous", start_minutes_ago=2 * half - 1, end_minutes_ago=half),
        ],
    )

    # Çağrı 2: Toplam 30 dk (1 range)
    total_start = min(max(1, window_minutes), 30) - 1
    req_total = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="total", start_minutes_ago=total_start, end_minutes_ago=0),
        ],
    )

    t0 = time.monotonic()
    resp_compare = client.run_realtime_report(req_compare)
    resp_total = client.run_realtime_report(req_total)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in resp_compare.metric_headers]
    windows: dict[str, dict[str, float]] = {"current": {}, "previous": {}}

    for row in resp_compare.rows:
        range_name = ""
        for dv in row.dimension_values:
            val = dv.value
            if val in ("current", "previous"):
                range_name = val
                break
        key = range_name if range_name in windows else "current"
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                windows[key][mname] = windows[key].get(mname, 0) + float(mv.value)
            except (ValueError, TypeError):
                pass

    total: dict[str, float] = {}
    total_metric_names = [m.name for m in resp_total.metric_headers]
    for row in resp_total.rows:
        for i, mv in enumerate(row.metric_values):
            mname = total_metric_names[i] if i < len(total_metric_names) else f"metric_{i}"
            try:
                total[mname] = total.get(mname, 0) + float(mv.value)
            except (ValueError, TypeError):
                pass

    comparison = _build_comparison(windows["current"], windows["previous"])

    return {
        "property_id": property_id,
        "window_minutes": total_start + 1,
        "total": total,
        "current": windows["current"],
        "previous": windows["previous"],
        "comparison": comparison,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
    }


def fetch_realtime_top_pages(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 10,
    dimension: str = "unifiedScreenName",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API ile son N dakikadaki top sayfaları/linkleri çeker.

    dimension parametresi:
      - "unifiedScreenName" → sayfa başlıkları (Top Sayfalar)
      - "pagePath"          → URL path'leri  (Top Linkler)
    """
    if client is None:
        client = _build_client()

    w = max(1, min(window_minutes, 30))

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dimension)],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="screenPageViews"),
        ],
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
        ],
    )

    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    pages: list[dict[str, Any]] = []

    for row in response.rows:
        page_path = ""
        for dv in row.dimension_values:
            if dv.value not in ("current", "previous"):
                page_path = dv.value
                break
        metrics_dict: dict[str, float] = {}
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                metrics_dict[mname] = float(mv.value)
            except (ValueError, TypeError):
                metrics_dict[mname] = 0.0
        pages.append({"page": page_path, **metrics_dict})

    pages.sort(key=lambda p: p.get("activeUsers", 0), reverse=True)

    return {
        "property_id": property_id,
        "window_minutes": w,
        "pages": pages[:limit],
        "total_pages": len(pages),
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

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    for a in alarms:
        a["domain"] = site.domain
        a["profile"] = profile
        a["profile_label"] = profile_label
        a["message"] = (
            f"{a['label']}: {site.domain} {profile_label} — "
            f"{a['metric']} {a['previous_value']:.0f} → {a['current_value']:.0f} ({a['change_pct']:+.1f}%)"
        )

    result["site_id"] = site.id
    result["domain"] = site.domain
    result["profile"] = profile
    result["alarms"] = alarms
    result["alarm_count"] = len(alarms)

    _save_snapshot(db, site.id, profile, result)

    if alarms:
        _save_alarm_logs(db, site.id, alarms)
        _send_site_alarm_emails(site.domain, profile, alarms)
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

    total = result.get("total") or result.get("current") or {}
    prev_half = result.get("previous") or {}
    snapshot = RealtimeSnapshot(
        site_id=site_id,
        profile=profile,
        active_users_current=total.get("activeUsers", 0),
        active_users_previous=prev_half.get("activeUsers", 0),
        pageviews_current=total.get("screenPageViews", 0),
        pageviews_previous=prev_half.get("screenPageViews", 0),
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


def _rt_send_email(subject: str, html_body: str) -> bool:
    """Realtime alarmları için e-posta gönderir.
    outbound_email_enabled'dan bağımsız çalışır — kendi flag'i ga4_realtime_page_alert_email."""
    import smtplib as _smtplib
    from email.message import EmailMessage as _EM

    from backend.config import settings

    required = [settings.smtp_host, settings.smtp_user, settings.smtp_password, settings.mail_from]
    if not all(v and v.strip() and not v.startswith("local-") for v in required):
        return False
    recipients = [r.strip() for r in settings.mail_to.split(",") if r.strip()]
    if not recipients:
        return False

    msg = _EM()
    msg["Subject"] = subject
    msg["From"] = settings.mail_from
    msg["To"] = ", ".join(recipients)
    msg.set_content("Realtime alarm — plain text fallback.")
    msg.add_alternative(html_body, subtype="html")
    try:
        with _smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=45) as smtp:
            smtp.starttls()
            smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(msg)
        logger.info("Realtime alarm e-posta gönderildi: %s", subject[:80])
        return True
    except Exception:
        logger.exception("Realtime alarm e-posta gönderilemedi: %s", subject[:80])
        return False


def _send_site_alarm_emails(domain: str, profile: str, alarms: list[dict[str, Any]]) -> None:
    """Genel site alarmları (trafik düşüşü/artışı) için e-posta gönderir."""
    from backend.config import settings
    if not settings.ga4_realtime_page_alert_email:
        return

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)

    for alarm in alarms:
        metric = alarm.get("metric", "activeUsers")
        cur = alarm.get("current_value", 0)
        prev = alarm.get("previous_value", 0)
        pct = alarm.get("change_pct", 0)
        label = alarm.get("label", "Alarm")

        if pct < 0:
            subject = f"📉 {label}: {domain} {profile_label} — {metric} {prev:.0f} → {cur:.0f} ({pct:+.1f}%)"
        else:
            subject = f"📈 {label}: {domain} {profile_label} — {metric} {prev:.0f} → {cur:.0f} ({pct:+.1f}%)"

        color = "#dc2626" if pct < 0 else "#16a34a"
        html_body = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px;">
            <h2 style="color: #1e293b; margin-bottom: 8px;">{alarm['message']}</h2>
            <table style="border-collapse: collapse; width: 100%; margin: 16px 0;">
                <tr style="background: #f1f5f9;">
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Site</td>
                    <td style="padding: 8px 12px;">{domain}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Profil</td>
                    <td style="padding: 8px 12px;">{profile_label}</td>
                </tr>
                <tr style="background: #f1f5f9;">
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Metrik</td>
                    <td style="padding: 8px 12px;">{metric}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Değer</td>
                    <td style="padding: 8px 12px;">
                        <strong>{cur:.0f}</strong>
                        <span style="color: #64748b;">(önceki: {prev:.0f})</span>
                        <span style="color: {color}; font-weight: 700;"> {pct:+.1f}%</span>
                    </td>
                </tr>
            </table>
            <p style="color: #94a3b8; font-size: 12px; margin-top: 24px;">
                SEO Agent Realtime Site Alarmı — otomatik gönderilmiştir.
            </p>
        </div>
        """
        _rt_send_email(subject, html_body)


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


# ── Sayfa bazlı alarm sistemi ────────────────────────────────────────────────

def save_page_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    pages: list[dict[str, Any]],
) -> None:
    """Top sayfa sonuçlarını DB'ye kaydeder."""
    from backend.models import RealtimePageSnapshot

    for i, page in enumerate(pages[:25]):
        snap = RealtimePageSnapshot(
            site_id=site_id,
            profile=profile,
            page_path=str(page.get("page", ""))[:500],
            active_users=page.get("activeUsers", 0),
            pageviews=page.get("screenPageViews", 0),
            rank=i + 1,
        )
        db.add(snap)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("RealtimePageSnapshot kayıt hatası (site_id=%s)", site_id)


def get_previous_page_snapshots(
    db: Session,
    site_id: int,
    profile: str,
) -> list[dict[str, Any]]:
    """Son kayıtlı sayfa snapshot'ını döner (karşılaştırma için)."""
    from backend.models import RealtimePageSnapshot
    from sqlalchemy import func as sqlfunc

    latest_time = (
        db.query(sqlfunc.max(RealtimePageSnapshot.collected_at))
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
        )
        .scalar()
    )
    if not latest_time:
        return []

    rows = (
        db.query(RealtimePageSnapshot)
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
            RealtimePageSnapshot.collected_at == latest_time,
        )
        .order_by(RealtimePageSnapshot.rank)
        .all()
    )
    return [
        {
            "page": row.page_path,
            "activeUsers": row.active_users,
            "screenPageViews": row.pageviews,
            "rank": row.rank,
        }
        for row in rows
    ]


def evaluate_page_alarms(
    current_pages: list[dict[str, Any]],
    previous_pages: list[dict[str, Any]],
    *,
    site_domain: str = "",
    profile: str = "web",
) -> list[dict[str, Any]]:
    """Sayfa bazlı alarm kurallarını değerlendirir.

    Kontrol edilen durumlar:
    - Sayfa trafik düşüşü (>%50)
    - Sayfa trafik artışı (>%100)
    - Sayfa top listeden düştü (önceki listede var, şimdikinde yok)
    - Yeni sayfa top listeye girdi (öncekinde yok, şimdikinde var)
    """
    if not previous_pages:
        return []

    triggered: list[dict[str, Any]] = []
    plabel = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    tag = f"{site_domain} {plabel}" if site_domain else plabel

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

    for page_path, curr in curr_map.items():
        curr_users = curr.get("activeUsers", 0)
        prev = prev_map.get(page_path)

        if prev:
            prev_users = prev.get("activeUsers", 0)

            rule = PAGE_ALARM_RULES["page_traffic_drop"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct <= -rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "page_traffic_drop",
                        "severity": rule["severity"],
                        "page": page_path,
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"📉 [{tag}] {page_path[:60]} — {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })

            rule = PAGE_ALARM_RULES["page_traffic_spike"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct >= rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "page_traffic_spike",
                        "severity": rule["severity"],
                        "page": page_path,
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"📈 [{tag}] {page_path[:60]} — {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })
        else:
            rule = PAGE_ALARM_RULES["page_new_entry"]
            if curr_users >= rule["min_users"]:
                triggered.append({
                    "rule_id": "page_new_entry",
                    "severity": rule["severity"],
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": curr_users,
                    "previous_users": 0,
                    "change_pct": 100.0,
                    "message": f"🆕 [{tag}] {page_path[:60]} — top listeye girdi ({curr_users:.0f} kullanıcı)",
                })

    rule = PAGE_ALARM_RULES["page_disappeared"]
    for page_path, prev in prev_map.items():
        if page_path not in curr_map:
            prev_users = prev.get("activeUsers", 0)
            if prev_users >= rule["min_prev_users"]:
                triggered.append({
                    "rule_id": "page_disappeared",
                    "severity": rule["severity"],
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": 0,
                    "previous_users": prev_users,
                    "change_pct": -100.0,
                    "message": f"⚠️ [{tag}] {page_path[:60]} — listeden düştü (önceki: {prev_users:.0f})",
                })

    return triggered


def check_page_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 30,
) -> list[dict[str, Any]]:
    """Tek site+profil için sayfa bazlı alarm kontrolü yapar.

    1. Önceki snapshot'ı DB'den al
    2. Yeni top sayfaları API'den çek
    3. Karşılaştır, alarmları değerlendir
    4. Yeni snapshot'ı DB'ye kaydet
    5. Alarmları DB'ye kaydet ve mail gönder
    """
    from backend.config import settings

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return []

    previous_pages = get_previous_page_snapshots(db, site.id, profile)

    try:
        result = fetch_realtime_top_pages(property_id, window_minutes=window_minutes, limit=25)
    except Exception as exc:
        logger.warning("Sayfa alarm: top pages API hatası [%s/%s]: %s", site.domain, profile, exc)
        return []

    current_pages = result.get("pages", [])
    save_page_snapshots(db, site.id, profile, current_pages)

    if not previous_pages:
        return []

    alarms = evaluate_page_alarms(
        current_pages, previous_pages,
        site_domain=site.domain, profile=profile,
    )

    if alarms:
        _save_page_alarm_logs(db, site.id, alarms)
        if settings.ga4_realtime_page_alert_email:
            _send_page_alarm_email(site.domain, profile, alarms)

    return alarms


def _save_page_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]]) -> None:
    """Sayfa bazlı alarmları RealtimeAlarmLog'a kaydeder."""
    from backend.models import RealtimeAlarmLog

    for a in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=a["rule_id"],
            metric="page:" + a.get("page", "")[:200],
            severity=a.get("severity", "warning"),
            current_value=a.get("current_users", 0),
            previous_value=a.get("previous_users", 0),
            change_pct=a.get("change_pct", 0),
            message=a["message"],
        )
        db.add(log)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Sayfa alarm log kayıt hatası (site_id=%s)", site_id)


def _send_page_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]]) -> None:
    """Sayfa bazlı alarmlar için e-posta gönderir."""
    from backend.config import settings
    if not settings.ga4_realtime_page_alert_email:
        return

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)

    for alarm in alarms:
        page = alarm.get("page", "bilinmiyor")
        page_short = page[:80] if len(page) > 80 else page
        curr = alarm.get("current_users", 0)
        prev = alarm.get("previous_users", 0)
        pct = alarm.get("change_pct", 0)
        rule_id = alarm.get("rule_id", "")

        if rule_id == "page_traffic_drop":
            subject = f"📉 Trafik düşüşü: {page_short} ({domain} {profile_label}) — {prev:.0f} → {curr:.0f}"
        elif rule_id == "page_traffic_spike":
            subject = f"📈 Trafik artışı: {page_short} ({domain} {profile_label}) — {prev:.0f} → {curr:.0f}"
        elif rule_id == "page_disappeared":
            subject = f"⚠️ Sayfa düştü: {page_short} ({domain} {profile_label}) — önceki {prev:.0f} kullanıcı"
        elif rule_id == "page_new_entry":
            subject = f"🆕 Yeni sayfa: {page_short} ({domain} {profile_label}) — {curr:.0f} kullanıcı"
        else:
            subject = f"Realtime sayfa alarmı: {page_short} ({domain})"

        html_body = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 600px;">
            <h2 style="color: #1e293b; margin-bottom: 8px;">{alarm['message']}</h2>
            <table style="border-collapse: collapse; width: 100%; margin: 16px 0;">
                <tr style="background: #f1f5f9;">
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Site</td>
                    <td style="padding: 8px 12px;">{domain}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Profil</td>
                    <td style="padding: 8px 12px;">{profile_label}</td>
                </tr>
                <tr style="background: #f1f5f9;">
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Sayfa</td>
                    <td style="padding: 8px 12px; word-break: break-all;">{page}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 12px; font-weight: 600; color: #475569;">Aktif Kullanıcı</td>
                    <td style="padding: 8px 12px;">
                        <strong>{curr:.0f}</strong>
                        <span style="color: #64748b;">(önceki: {prev:.0f})</span>
                        <span style="color: {'#dc2626' if pct < 0 else '#16a34a'}; font-weight: 700;">
                            {pct:+.1f}%
                        </span>
                    </td>
                </tr>
            </table>
            <p style="color: #94a3b8; font-size: 12px; margin-top: 24px;">
                SEO Agent Realtime Sayfa Alarmı — otomatik gönderilmiştir.
            </p>
        </div>
        """
        _rt_send_email(subject, html_body)


def run_page_alarm_check_all_sites(db: Session, *, window_minutes: int = 30) -> list[dict[str, Any]]:
    """Tüm aktif siteler ve profilleri için sayfa bazlı alarm kontrolü."""
    from backend.models import Site as SiteModel

    all_alarms: list[dict[str, Any]] = []
    sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()

    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        for profile in ("web", "mweb", "ios", "android"):
            prop_id = str(properties.get(profile, "")).strip()
            if not prop_id:
                continue
            try:
                alarms = check_page_alarms_for_site(
                    db, site, profile=profile, window_minutes=window_minutes,
                )
                all_alarms.extend(alarms)
            except Exception as exc:
                logger.exception("Sayfa alarm check hatası [%s/%s]: %s", site.domain, profile, exc)

    return all_alarms
