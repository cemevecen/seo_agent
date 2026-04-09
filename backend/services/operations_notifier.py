"""Operasyon e-postalari ve zamanlanmis guncelleme izleme yardimcilari."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from html import escape
from urllib.parse import quote

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import CollectorRun, NotificationDeliveryLog, Site
from backend.services.email_templates import data_table, note_box, render_email_shell, section, stat_cards, summary_table
from backend.services.mailer import send_email
from backend.services.search_console_auth import get_search_console_connection_status
from backend.services.timezone_utils import (
    format_local_datetime,
    local_schedule_datetime,
    local_schedule_to_utc_naive,
    now_local,
)

# Ortam değişkeninden oku; ayarlarda tanımlıysa onu kullan, yoksa env'e bak.
DEFAULT_OPERATIONS_RECIPIENT: str = os.getenv("OPERATIONS_EMAIL", "cemevecen@nokta.com")

SUMMARY_LABELS = {
    "search_console_clicks_28d": "Search Console tıklama 28G",
    "search_console_impressions_28d": "Search Console gösterim 28G",
    "search_console_avg_ctr_28d": "Search Console ort. CTR 28G",
    "search_console_avg_position_28d": "Search Console ort. pozisyon 28G",
    "search_console_dropped_queries": "Düşen sorgu sayısı",
    "search_console_biggest_drop": "En büyük düşüş",
    "source_pages": "Kaynak sayfa",
    "audited_urls": "Taranan URL",
    "redirect_301_links": "301 yönlendirme",
    "redirect_302_links": "302 yönlendirme",
    "redirect_chains": "Redirect zinciri",
    "broken_links": "Kırık URL",
    "max_hops": "Maks. hop",
}

COMPARISON_FIELDS = [
    ("clicks", "Tıklama"),
    ("impressions", "Gösterim"),
    ("ctr", "Ortalama CTR"),
    ("position", "Ortalama pozisyon"),
]

SYSTEM_LABELS = {
    "pagespeed": "PageSpeed",
    "crawler": "Crawler",
    "search_console": "Search Console",
    "search_console_alerts": "Alert Refresh",
    "ga4": "GA4",
    "crux_history": "CrUX History",
    "url_inspection": "URL Inspection",
    "app_intel": "App Mağaza Analitiği",
}

TRIGGER_SOURCE_LABELS = {
    "manual": "manuel",
    "system": "sistem",
}


@dataclass(frozen=True)
class ScheduledSystemSpec:
    notification_name: str
    label: str
    provider: str
    strategy: str
    schedule_hour: int
    schedule_minute: int
    enabled: bool
    connected_only: bool = False


def operations_recipients() -> list[str]:
    raw = str(settings.operations_mail_to or DEFAULT_OPERATIONS_RECIPIENT).strip()
    recipients = [item.strip() for item in raw.split(",") if item.strip()]
    return recipients or [DEFAULT_OPERATIONS_RECIPIENT]


def _safe_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_tr_number(value, *, decimals: int = 0) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "-"
    rendered = f"{numeric:,.{decimals}f}"
    rendered = rendered.replace(",", "__TMP__").replace(".", ",").replace("__TMP__", ".")
    if decimals > 0:
        rendered = rendered.rstrip("0").rstrip(",")
    return rendered


def _format_summary_value(key: str, value) -> str:
    if isinstance(value, bool):
        return "Evet" if value else "Hayır"
    if isinstance(value, dict):
        return f"{len(value)} alan"
    if isinstance(value, list):
        return f"{len(value)} kayıt"
    numeric = _safe_float(value)
    if numeric is not None:
        lowered = key.lower()
        if "ctr" in lowered:
            return f"%{_format_tr_number(numeric, decimals=2)}"
        if "position" in lowered or "drop" in lowered:
            return _format_tr_number(numeric, decimals=2)
        return _format_tr_number(numeric, decimals=0)
    return str(value)


def _humanize_summary_label(key: str) -> str:
    return SUMMARY_LABELS.get(key, str(key).replace("_", " ").strip().title())


def _summary_detail_rows(summary: dict) -> list[list[str]]:
    rows: list[list[str]] = []
    for key, value in summary.items():
        rows.append([_humanize_summary_label(str(key)), _format_summary_value(str(key), value)])
    return rows


def _format_comparison_value(field: str, value) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "-"
    if field == "ctr":
        return f"%{_format_tr_number(numeric, decimals=2)}"
    if field == "position":
        return _format_tr_number(numeric, decimals=2)
    return _format_tr_number(numeric, decimals=0)


def _format_comparison_delta(field: str, previous, current) -> str:
    previous_numeric = _safe_float(previous)
    current_numeric = _safe_float(current)
    if previous_numeric is None or current_numeric is None:
        return "-"
    delta = current_numeric - previous_numeric
    if field == "ctr":
        direction = "artış" if delta > 0 else "düşüş" if delta < 0 else "değişmedi"
        return f"{delta:+.2f} ({direction})".replace(".", ",")
    if field == "position":
        if delta < 0:
            note = "iyileşme"
        elif delta > 0:
            note = "kötüleşme"
        else:
            note = "değişmedi"
        return f"{delta:+.2f} ({note})".replace(".", ",")
    return f"{delta:+,.0f}".replace(",", ".")


def _fmt_date_tr(iso: str | None) -> str:
    if not iso:
        return "-"
    try:
        d = date.fromisoformat(str(iso)[:10])
        return d.strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        return str(iso)


def _gsc_performance_url(property_url: str) -> str:
    pu = (property_url or "").strip()
    if not pu:
        return ""
    return f"https://search.google.com/search-console/performance/search-analytics?resource_id={quote(pu, safe='')}&hl=tr"


def _same_weekday_sc_comparison_rows(result: dict | None) -> list[list[str]]:
    """Son gün vs bir önceki haftanın aynı günü (Search Console günlük özet)."""
    if not isinstance(result, dict):
        return []
    comp = result.get("comparison")
    if not isinstance(comp, dict):
        return []
    sw = comp.get("same_weekday_day")
    if not isinstance(sw, dict):
        return []
    cur = sw.get("current_day_summary") or {}
    prev = sw.get("previous_week_same_weekday_summary") or {}
    if not isinstance(cur, dict) or not isinstance(prev, dict):
        return []
    rows: list[list[str]] = []
    for field, label in COMPARISON_FIELDS:
        before = prev.get(field)
        after = cur.get(field)
        if before is None and after is None:
            continue
        rows.append(
            [
                label,
                _format_comparison_value(field, before),
                _format_comparison_value(field, after),
                _format_comparison_delta(field, before, after),
            ]
        )
    return rows


def _same_weekday_sc_links_paragraph(result: dict | None) -> str:
    if not isinstance(result, dict):
        return ""
    comp = result.get("comparison")
    if not isinstance(comp, dict):
        return ""
    sw = comp.get("same_weekday_day")
    if not isinstance(sw, dict):
        return ""
    pu = str(sw.get("property_url") or "").strip()
    if not pu:
        return ""
    href = _gsc_performance_url(pu)
    if not href:
        return ""
    ref_d = _fmt_date_tr(str(sw.get("reference_date") or ""))
    prev_d = _fmt_date_tr(str(sw.get("previous_week_date") or ""))
    wd = escape(str(sw.get("weekday_label_tr") or ""))
    return (
        f'<p style="margin:0;font-size:13px;line-height:1.6;color:#475569;">'
        f"Performans raporu: <a href=\"{escape(href, quote=True)}\" target=\"_blank\" rel=\"noopener noreferrer\" "
        f'style="color:#1d4ed8;">Search Console — {wd}</a> '
        f"(karşılaştırma: <strong>{prev_d}</strong> → <strong>{ref_d}</strong>).</p>"
    )


def _comparison_rows(result: dict | None) -> list[list[str]]:
    if not isinstance(result, dict):
        return []
    comparison = result.get("comparison")
    if not isinstance(comparison, dict):
        return []
    current = comparison.get("current_7d_summary") or {}
    previous = comparison.get("previous_7d_summary") or {}
    if not isinstance(current, dict) or not isinstance(previous, dict):
        return []
    rows: list[list[str]] = []
    for field, label in COMPARISON_FIELDS:
        before = previous.get(field)
        after = current.get(field)
        if before is None and after is None:
            continue
        rows.append(
            [
                label,
                _format_comparison_value(field, before),
                _format_comparison_value(field, after),
                _format_comparison_delta(field, before, after),
            ]
        )
    return rows


def _record_delivery(
    db: Session,
    *,
    notification_type: str,
    notification_key: str,
    subject: str,
    recipient: str,
) -> None:
    db.add(
        NotificationDeliveryLog(
            notification_type=notification_type,
            notification_key=notification_key,
            subject=subject,
            recipient=recipient,
        )
    )
    db.commit()


def _delivery_exists(db: Session, *, notification_type: str, notification_key: str) -> bool:
    return (
        db.query(NotificationDeliveryLog.id)
        .filter(
            NotificationDeliveryLog.notification_type == notification_type,
            NotificationDeliveryLog.notification_key == notification_key,
        )
        .first()
        is not None
    )


def _send_operations_email(subject: str, html_body: str, *, notification_key: str | None = None, db: Session | None = None) -> bool:
    recipients = operations_recipients()
    sent = send_email(subject, html_body, recipients=recipients)
    if sent and notification_key and db is not None:
        _record_delivery(
            db,
            notification_type="operations",
            notification_key=notification_key,
            subject=subject,
            recipient=",".join(recipients),
        )
    return sent


def _result_status_label(result: dict | None) -> str:
    if not isinstance(result, dict):
        return "tamamlandi"
    if result.get("blocked"):
        return "engellendi"
    if result.get("state"):
        state = str(result.get("state"))
        return "güncel değil" if state.lower() == "stale" else state
    if result.get("source"):
        source = str(result.get("source"))
        return "güncel değil" if source.lower() == "stale" else source
    if result.get("error") or result.get("errors"):
        return "başarısız"
    return "tamamlandi"


def _result_tone(result: dict | None) -> str:
    status = _result_status_label(result).lower()
    if status in {"failed", "blocked"}:
        return "rose"
    if status in {"warning", "stale", "güncel değil"}:
        return "amber"
    return "blue"


def _should_send_trigger_email(result: dict | None) -> bool:
    if not isinstance(result, dict):
        return True
    if result.get("blocked"):
        return False
    if str(result.get("state") or "").lower() == "skipped":
        return False
    return True


def _site_heading_row(domain: str) -> str:
    return (
        '<tr><td style="padding:22px 32px 10px 32px;background:#f8fafc;border-top:1px solid #e2e8f0;">'
        f'<p style="margin:0;font-size:17px;font-weight:800;color:#0f172a;letter-spacing:-0.02em;">{escape(domain)}</p>'
        "</td></tr>"
    )


def _site_detail_section_rows(result: dict | None) -> list[str]:
    """Tek site için tetik özeti blokları (önceki 'tetiklendi' mailinin içeriği; özet tablosu hariç)."""
    details: list[tuple[str, str]] = []
    detail_rows: list[list[str]] = []
    comparison_rows: list[list[str]] = []
    same_weekday_rows: list[list[str]] = []
    same_weekday_links_html: str = ""
    summary_cards: list[dict[str, str]] = []
    if isinstance(result, dict):
        if result.get("reason"):
            details.append(("Neden", str(result["reason"])))
        if result.get("error"):
            details.append(("Hata", str(result["error"])))
        if result.get("errors"):
            details.append(("Hata Özeti", str(result["errors"])))
        if isinstance(result.get("summary"), dict):
            detail_rows = _summary_detail_rows(result["summary"])
            summary_cards = _result_summary_cards(result["summary"])
        elif result.get("summary"):
            details.append(("Özet", str(result["summary"])))
        comparison_rows = _comparison_rows(result)
        same_weekday_rows = _same_weekday_sc_comparison_rows(result)
        same_weekday_links_html = _same_weekday_sc_links_paragraph(result)
        if result.get("state"):
            details.append(("Durum", str(result["state"])))
        elif result.get("source"):
            details.append(("Kaynak", str(result["source"])))

    rows: list[str] = []
    if summary_cards:
        rows.append(
            section(
                "Kritik Özet",
                stat_cards(summary_cards),
            )
        )
    if details:
        rows.append(
            section(
                "Calisma Detaylari",
                summary_table(details),
            )
        )
    if detail_rows:
        rows.append(
            section(
                "Metrik Dökümü",
                data_table(["Alan", "Değer"], detail_rows),
            )
        )
    if comparison_rows:
        rows.append(
            section(
                "Karşılaştırmalı Veri",
                data_table(["Alan", "Önceki 7 Gün", "Son 7 Gün", "Fark"], comparison_rows),
            )
        )
    if same_weekday_rows:
        sw_title = "Haftalık aynı gün (son gün vs önceki haftanın aynı günü)"
        sw_sub = (
            "Search Console’daki son tam gün ile bir önceki haftanın aynı hafta günü (ör. Çarşamba–Çarşamba) kıyaslanır."
        )
        sw_inner = data_table(
            ["Alan", "Önceki hafta aynı gün", "Son gün", "Fark"],
            same_weekday_rows,
        )
        if same_weekday_links_html:
            sw_inner = f'<div style="margin-bottom:12px;">{same_weekday_links_html}</div>{sw_inner}'
        rows.append(section(sw_title, sw_inner, subtitle=sw_sub))
    return rows


def _consolidated_tone(items: list[tuple[Site | None, dict | None]]) -> str:
    for _s, r in items:
        if _result_tone(r) == "rose":
            return "rose"
    for _s, r in items:
        if _result_tone(r) == "amber":
            return "amber"
    return "blue"


def send_consolidated_system_email(
    *,
    system_key: str,
    trigger_source: str,
    action_label: str,
    items: list[tuple[Site | None, dict | None]],
    db: Session | None = None,
    notification_key: str | None = None,
) -> bool:
    """
    Aynı sistem için tek konu başlıklı özet maili; eski 'tetiklendi' içeriği site bloklarının sonunda.
    """
    if not settings.operations_trigger_email_enabled:
        return False
    system_label = SYSTEM_LABELS.get(system_key, system_key.replace("_", " ").title())
    kept: list[tuple[Site | None, dict]] = []
    for site, result in items:
        if result is None:
            continue
        if not _should_send_trigger_email(result):
            continue
        kept.append((site, result))
    if not kept:
        return False
    ts_label = TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)
    subject = f"SEO Agent: {system_label} — {action_label} ({ts_label})"
    global_rows = [
        ("Mail tipi", "Operasyon özeti"),
        ("Tetik tipi", ts_label),
        ("Sistem", system_label),
        ("Aksiyon", action_label),
        ("Site sayısı", str(len(kept))),
        ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
    ]
    sections: list[str] = [section("Özet", summary_table(global_rows))]
    for site, result in kept:
        domain = site.domain if site is not None else "Tüm sistem"
        sections.append(_site_heading_row(domain))
        sections.extend(_site_detail_section_rows(result))
    body = render_email_shell(
        eyebrow="SEO Agent Operations",
        title=f"{system_label} — {action_label}",
        intro="",
        tone=_consolidated_tone(kept),
        status_label="Operasyon özeti",
        sections=sections,
    )
    return _send_operations_email(subject, body, notification_key=notification_key, db=db)


def _trigger_email_body(
    *,
    trigger_source: str,
    system_label: str,
    site: Site | None,
    result: dict | None,
    action_label: str,
) -> str:
    """Tek site tam HTML (test / nadir kullanım); üretimde send_consolidated_system_email tercih edilir."""
    summary_rows = [
        ("Mail tipi", "Bilgilendirme"),
        ("Tetik tipi", TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)),
        ("Sistem", system_label),
        ("Aksiyon", action_label),
        ("Site", site.domain if site is not None else "Tüm sistem"),
        ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
    ]
    sections = [
        section(
            "Ozet",
            summary_table(summary_rows),
        )
    ]
    sections.extend(_site_detail_section_rows(result))
    return render_email_shell(
        eyebrow="SEO Agent Operations",
        title=f"{system_label} — {action_label}",
        intro="",
        tone=_result_tone(result),
        status_label="Bilgilendirme",
        sections=sections,
    )


def _result_summary_cards(summary: dict) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    preferred = [
        ("search_console_clicks_28d", "28g Click", "Toplam click hacmi", "blue"),
        ("search_console_impressions_28d", "28g Impression", "Toplam impression hacmi", "amber"),
        ("search_console_avg_ctr_28d", "Ort. CTR", "Search Console ortalama CTR", "emerald"),
        ("search_console_avg_position_28d", "Ort. Pozisyon", "Dusuk deger daha iyi", "slate"),
        ("search_console_dropped_queries", "Dusen Sorgu", "Dikkat gerektiren sorgu sayisi", "rose"),
        ("search_console_biggest_drop", "En Buyuk Dusus", "Kritik pozisyon kaybi", "rose"),
    ]
    for key, label, caption, tone in preferred:
        if key not in summary:
            continue
        value = summary.get(key)
        rendered = _format_summary_value(key, value)
        cards.append({"label": label, "value": rendered, "caption": caption, "tone": tone})
        if len(cards) >= 4:
            break
    return cards


def notify_system_trigger(
    *,
    trigger_source: str,
    system_key: str,
    site: Site | None,
    result: dict | None = None,
    action_label: str = "",
) -> bool:
    if not _should_send_trigger_email(result):
        return False
    system_label = SYSTEM_LABELS.get(system_key, system_key.replace("_", " ").title())
    return send_consolidated_system_email(
        system_key=system_key,
        trigger_source=trigger_source,
        action_label=action_label or system_label,
        items=[(site, result)],
    )


def notify_result_map(
    *,
    trigger_source: str,
    site: Site | None,
    results: dict[str, dict] | None,
    action_label: str,
    system_key_map: dict[str, str] | None = None,
) -> None:
    key_map = system_key_map or {}
    for result_key, result_value in (results or {}).items():
        sys_key = key_map.get(result_key, result_key)
        payload = result_value if isinstance(result_value, dict) else {"summary": result_value}
        send_consolidated_system_email(
            system_key=sys_key,
            trigger_source=trigger_source,
            action_label=action_label,
            items=[(site, payload)],
        )


def _missed_run_reason(latest_run: CollectorRun | None) -> str:
    if latest_run is None:
        return (
            "Beklenen zaman penceresinde hicbir calisma kaydi olusmadi. "
            "Bu genelde scheduler'in calismadigi, uygulamanin o saatte kapali oldugu "
            "veya job'in hic baslamadigi anlamina gelir."
        )
    status = str(latest_run.status or "unknown").lower()
    if status == "started":
        return (
            f"Calisma kaydi baslamis gorunuyor ancak basariyla tamamlanmis kayit yok. "
            f"Son gorulen durum: {latest_run.status} ({format_local_datetime(latest_run.requested_at)})."
        )
    return (
        f"Beklenen zaman penceresinde basarili tamamlanan kayit yok. "
        f"Son gorulen durum: {latest_run.status} ({format_local_datetime(latest_run.requested_at)})."
    )


def _active_sites(db: Session, *, connected_only: bool) -> list[Site]:
    sites = (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.asc(), Site.id.asc())
        .all()
    )
    if not connected_only:
        return sites
    return [site for site in sites if get_search_console_connection_status(db, site.id).get("connected")]


def _latest_relevant_run(
    db: Session,
    *,
    site_id: int,
    provider: str,
    strategy: str,
    requested_after,
) -> CollectorRun | None:
    return (
        db.query(CollectorRun)
        .filter(
            CollectorRun.site_id == site_id,
            CollectorRun.provider == provider,
            CollectorRun.strategy == strategy,
            CollectorRun.requested_at >= requested_after,
        )
        .order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc())
        .first()
    )


def _scheduled_system_specs() -> list[ScheduledSystemSpec]:
    return [
        ScheduledSystemSpec(
            notification_name="search_console_daily",
            label="Search Console",
            provider="search_console",
            strategy="all",
            schedule_hour=int(settings.search_console_scheduled_refresh_hour),
            schedule_minute=int(settings.search_console_scheduled_refresh_minute),
            enabled=bool(settings.search_console_scheduled_refresh_enabled),
            connected_only=True,
        ),
        ScheduledSystemSpec(
            notification_name="search_console_alerts_daily",
            label="Alert Refresh",
            provider="search_console",
            strategy="alerts",
            schedule_hour=int(settings.alerts_scheduled_refresh_hour),
            schedule_minute=int(settings.alerts_scheduled_refresh_minute),
            enabled=bool(settings.alerts_scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="pagespeed_mobile_daily",
            label="PageSpeed Mobile",
            provider="pagespeed",
            strategy="mobile",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="pagespeed_desktop_daily",
            label="PageSpeed Desktop",
            provider="pagespeed",
            strategy="desktop",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="crawler_daily",
            label="Crawler",
            provider="crawler",
            strategy="sitewide",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="crux_mobile_daily",
            label="CrUX History Mobile",
            provider="crux_history",
            strategy="mobile",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="crux_desktop_daily",
            label="CrUX History Desktop",
            provider="crux_history",
            strategy="desktop",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=False,
        ),
        ScheduledSystemSpec(
            notification_name="url_inspection_daily",
            label="URL Inspection",
            provider="url_inspection",
            strategy="homepage",
            schedule_hour=int(settings.scheduled_refresh_hour),
            schedule_minute=int(settings.scheduled_refresh_minute),
            enabled=bool(settings.scheduled_refresh_enabled),
            connected_only=True,
        ),
    ]


def notify_missed_scheduled_refreshes(db: Session) -> list[str]:
    if not settings.scheduled_refresh_monitor_enabled:
        return []

    local_now = now_local()
    grace = timedelta(minutes=max(0, int(settings.scheduled_refresh_monitor_grace_minutes)))
    sent_subjects: list[str] = []

    for spec in _scheduled_system_specs():
        if not spec.enabled:
            continue

        scheduled_local = local_schedule_datetime(local_now.date(), spec.schedule_hour, spec.schedule_minute)
        if local_now < scheduled_local + grace:
            continue

        expected_sites = _active_sites(db, connected_only=spec.connected_only)
        if not expected_sites:
            continue

        requested_after = local_schedule_to_utc_naive(local_now.date(), spec.schedule_hour, spec.schedule_minute)
        missing_rows: list[list[str]] = []
        missing_count = 0

        for site in expected_sites:
            latest_run = _latest_relevant_run(
                db,
                site_id=site.id,
                provider=spec.provider,
                strategy=spec.strategy,
                requested_after=requested_after,
            )
            if latest_run is not None and str(latest_run.status).lower() == "success":
                continue

            missing_count += 1
            missing_rows.append(
                [
                    site.domain,
                    "Run bulunamadi" if latest_run is None else str(latest_run.status or "unknown"),
                    _missed_run_reason(latest_run),
                ]
            )

        if not missing_rows:
            continue

        notification_key = f"missed:{spec.notification_name}:{local_now.date().isoformat()}"
        if _delivery_exists(db, notification_type="operations", notification_key=notification_key):
            continue

        subject = f"SEO Agent UYARI: {spec.label} zamaninda guncellenmedi"
        body = render_email_shell(
            eyebrow="SEO Agent Operations",
            title=f"Uyari: {spec.label} zamaninda guncellenmedi",
            intro="Bu mail bir sorun bildirir. Beklenen zaman araliginda bu sistem icin basarili calisma kaydi bulunamadi.",
            tone="amber",
            status_label="Uyari",
            sections=[
                section(
                    "Zamanlama Ozeti",
                    summary_table(
                        [
                            ("Sistem", spec.label),
                            ("Beklenen saat", scheduled_local.strftime("%d.%m.%Y %H:%M") + " TSİ"),
                            ("Kontrol zamani", format_local_datetime(local_now)),
                            ("Etkilenen site", f"{missing_count}/{len(expected_sites)}"),
                        ]
                    ),
                    subtitle="Monitor job'i beklenen pencere sonrasinda son calisma kayitlarini kontrol etti.",
                ),
                section(
                    "Etkilenen Siteler",
                    data_table(
                        ["Site", "Son Durum", "Aciklama"],
                        missing_rows,
                    ),
                    subtitle="Tabloda, ilgili zaman penceresinde neden basarili run gorulmedigi anlatilir.",
                ),
                section(
                    "Aksiyon Notu",
                    note_box(
                        "Kontrol Listesi",
                        "Uygulamanın o saatte ayakta olduğunu, scheduler'in aktif olduğunu ve ilgili refresh job akışının hata vermediğini kontrol et. Run hiç oluşmadıysa job başlamamış olabilir; failed veya started görülüyorsa işlem tamamlanmadan kesilmiş olabilir.",
                        tone="amber",
                    ),
                ),
            ],
        )
        if _send_operations_email(subject, body, notification_key=notification_key, db=db):
            sent_subjects.append(subject)

    return sent_subjects


def _crawler_summary_from_result(result: dict | None) -> dict:
    if not isinstance(result, dict):
        return {}
    summary = result.get("summary") or {}
    if not isinstance(summary, dict):
        return {}
    return dict(summary.get("link_audit") or {})


def _format_source_urls(urls: list[str], *, total: int) -> str:
    if not urls:
        return "-"
    visible = [str(item) for item in urls[:3]]
    if total > len(visible):
        visible.append(f"+{total - len(visible)} sayfa")
    return "<br>".join(visible)


def _crawler_issue_rows(link_audit: dict) -> list[list[str]]:
    rows: list[list[str]] = []
    for sample in link_audit.get("broken_samples") or []:
        rows.append(
            [
                "Kırık",
                str(sample.get("url") or "-"),
                str(sample.get("final_url") or "-"),
                str(sample.get("final_status") or "erişilemedi"),
                _format_source_urls(list(sample.get("source_urls") or []), total=int(sample.get("source_count") or 0)),
            ]
        )
    for sample in link_audit.get("redirect_samples") or []:
        rows.append(
            [
                str(sample.get("issue_label") or "Redirect"),
                str(sample.get("url") or "-"),
                str(sample.get("final_url") or "-"),
                str(sample.get("chain") or sample.get("final_status") or "-"),
                _format_source_urls(list(sample.get("source_urls") or []), total=int(sample.get("source_count") or 0)),
            ]
        )
    return rows[:10]


def _crawler_summary_cards(link_audit: dict) -> list[dict[str, str]]:
    return [
        {
            "label": "Kaynak Sayfa",
            "value": _format_tr_number(link_audit.get("source_pages"), decimals=0),
            "caption": "İç linki çıkarılan başlangıç sayfaları",
            "tone": "blue",
        },
        {
            "label": "Taranan URL",
            "value": _format_tr_number(link_audit.get("audited_urls"), decimals=0),
            "caption": "Final durum kontrolü yapılan hedef URL",
            "tone": "slate",
        },
        {
            "label": "301 / 302",
            "value": f'{_format_tr_number(link_audit.get("redirect_301_links"), decimals=0)} / {_format_tr_number(link_audit.get("redirect_302_links"), decimals=0)}',
            "caption": "Dahili linklerde yönlendirme sayısı",
            "tone": "amber",
        },
        {
            "label": "Kırık / Zincir",
            "value": f'{_format_tr_number(link_audit.get("broken_links"), decimals=0)} / {_format_tr_number(link_audit.get("redirect_chains"), decimals=0)}',
            "caption": "Kritik teknik sorun sayısı",
            "tone": "rose" if int(link_audit.get("broken_links") or 0) > 0 or int(link_audit.get("redirect_chains") or 0) > 0 else "emerald",
        },
    ]


def _crawler_site_report_sections(site: Site, link_audit: dict) -> list[str]:
    issue_rows = _crawler_issue_rows(link_audit)
    summary_rows = [
        ("Site", site.domain),
        ("Kaynak seçimi", str(link_audit.get("source_strategy") or "URL listesi")),
        ("Kaynak sayfa", _format_tr_number(link_audit.get("source_pages"), decimals=0)),
        ("Taranan URL", _format_tr_number(link_audit.get("audited_urls"), decimals=0)),
        ("Maks. hop", _format_tr_number(link_audit.get("max_hops"), decimals=0)),
    ]
    return [
        section("Özet", summary_table(summary_rows), subtitle="Günlük koşuda kullanılan kapsam ve taranan URL sayısı."),
        section("Kritik Kartlar", stat_cards(_crawler_summary_cards(link_audit)), subtitle="En önemli crawler metrikleri."),
        section(
            "Sorunlu URL Örnekleri",
            data_table(["Durum", "Hedef URL", "Final URL", "Durum Zinciri", "Kaynak Sayfalar"], issue_rows)
            if issue_rows
            else note_box("Temiz Sonuç", "Bu günlük koşuda örnek tabloya girecek kırık veya yönlendirmeli URL bulunmadı.", tone="emerald"),
            subtitle="Tablo, tıklayıp kontrol etmen gereken örnek URL'leri gösterir.",
        ),
    ]


def _crawler_site_issue_note_section() -> str:
    """Tek mailde rapor tabloları tekrarlanmasın; eski ayrı uyarı mailindeki eylem özeti."""
    return section(
        "Uyarı — ne yapılmalı",
        note_box(
            "Önerilen aksiyon",
            "Kırık URL'leri kaldır veya doğru hedefe yönlendir. 301/302 kullanan dahili linkleri doğrudan final URL'ye çevir. Zincir görülen URL'leri tek adımlı hedefe indir.",
            tone="amber",
        ),
        subtitle="Aşağıda özet ve örnek tabloda bu site için tespit edilen sorunlar yer alır.",
    )


def notify_crawler_audit_emails_batch(
    db: Session,
    items: list[tuple[Site, dict | None]],
    trigger_source: str,
) -> list[str]:
    """Tüm dahili siteler için tek günlük crawler raporu (rapor + varsa uyarı blokları bir arada)."""
    parsed: list[tuple[Site, dict, dict]] = []
    for site, result in items:
        link_audit = _crawler_summary_from_result(result)
        rid = str((result or {}).get("collector_run_id") or "").strip()
        if not link_audit or not rid:
            continue
        parsed.append((site, result or {}, link_audit))

    if not parsed:
        return []

    ts_label = TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)
    batch_key = f"crawler-batch:{now_local().date().isoformat()}:{trigger_source}"
    if trigger_source == "system" and _delivery_exists(db, notification_type="operations", notification_key=batch_key):
        return []

    any_issue = False
    for _site, _res, la in parsed:
        if any(int(la.get(key) or 0) > 0 for key in ("broken_links", "redirect_chains", "redirect_301_links", "redirect_302_links")):
            any_issue = True
            break

    global_rows = [
        ("Mail tipi", "Crawler günlük özet"),
        ("Tetik tipi", ts_label),
        ("Site sayısı", str(len(parsed))),
        ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
    ]
    sections: list[str] = [
        section(
            "Genel",
            summary_table(global_rows),
            subtitle="Site içi taranan URL'lerin son durumu; kırık URL, yönlendirme ve redirect zinciri dağılımı.",
        ),
    ]
    for site, _res, link_audit in parsed:
        sections.append(_site_heading_row(site.domain))
        sections.extend(_crawler_site_report_sections(site, link_audit))
        has_issue = any(int(link_audit.get(key) or 0) > 0 for key in ("broken_links", "redirect_chains", "redirect_301_links", "redirect_302_links"))
        if has_issue:
            sections.append(_crawler_site_issue_note_section())

    subject = f"SEO Agent: Crawler — günlük özet ({ts_label})"
    title = "Crawler — günlük özet"
    intro = (
        "Aşağıda tüm dahili siteler için günlük crawler sonuçları yer alır. "
        "Uyarı gerektiren bulgular ilgili site bloklarında ek bölümlerle listelenir."
        if any_issue
        else "Aşağıda tüm dahili siteler için günlük crawler sonuçları yer alır."
    )
    body = render_email_shell(
        eyebrow="SEO Agent Operations",
        title=title,
        intro=intro,
        tone="amber" if any_issue else "blue",
        status_label="Günlük Rapor" if not any_issue else "Günlük Rapor / Uyarı",
        sections=sections,
    )
    nk = batch_key if trigger_source == "system" else None
    if _send_operations_email(subject, body, notification_key=nk, db=db):
        return [subject]
    return []


def notify_crawler_audit_emails(
    *,
    db: Session,
    site: Site,
    result: dict | None,
    trigger_source: str,
) -> list[str]:
    return notify_crawler_audit_emails_batch(db, [(site, result)], trigger_source)
