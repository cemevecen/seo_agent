"""Operasyon e-postalari ve zamanlanmis guncelleme izleme yardimcilari."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import CollectorRun, NotificationDeliveryLog, Site
from backend.services.email_templates import data_table, note_box, render_email_shell, section, summary_table
from backend.services.mailer import send_email
from backend.services.search_console_auth import get_search_console_connection_status
from backend.services.timezone_utils import (
    format_local_datetime,
    local_schedule_datetime,
    local_schedule_to_utc_naive,
    now_local,
)

DEFAULT_OPERATIONS_RECIPIENT = "cemevecen@nokta.com"

SYSTEM_LABELS = {
    "pagespeed": "PageSpeed",
    "crawler": "Crawler",
    "search_console": "Search Console",
    "search_console_alerts": "Alert Refresh",
    "crux_history": "CrUX History",
    "url_inspection": "URL Inspection",
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
        return "blocked"
    if result.get("state"):
        return str(result.get("state"))
    if result.get("source"):
        return str(result.get("source"))
    if result.get("error") or result.get("errors"):
        return "failed"
    return "completed"


def _result_tone(result: dict | None) -> str:
    status = _result_status_label(result).lower()
    if status in {"failed", "blocked"}:
        return "rose"
    if status in {"warning", "stale"}:
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


def _trigger_email_body(
    *,
    trigger_source: str,
    system_label: str,
    site: Site | None,
    result: dict | None,
    action_label: str,
) -> str:
    details: list[tuple[str, str]] = []
    if isinstance(result, dict):
        if result.get("reason"):
            details.append(("Neden", str(result["reason"])))
        if result.get("error"):
            details.append(("Hata", str(result["error"])))
        if result.get("errors"):
            details.append(("Hata Ozeti", str(result["errors"])))
        if result.get("summary"):
            details.append(("Ozet", str(result["summary"])))
        if result.get("state"):
            details.append(("Durum", str(result["state"])))
        elif result.get("source"):
            details.append(("Kaynak", str(result["source"])))

    summary_rows = [
        ("Mail tipi", "Bilgilendirme"),
        ("Tetik tipi", TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)),
        ("Sistem", system_label),
        ("Aksiyon", action_label),
        ("Site", site.domain if site is not None else "Tum sistem"),
        ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
    ]
    sections = [
        section(
            "Ozet",
            summary_table(summary_rows),
            subtitle="Bu mail, dis sisteme giden tetikleme akisinin calistigini bildirir.",
        )
    ]
    if details:
        sections.append(
            section(
                "Calisma Detaylari",
                summary_table(details),
                subtitle="Collector veya API sonucundan toplanan ozet alanlar.",
            )
        )
    sections.append(
        section(
            "Yorum",
            note_box(
                "Ne Anlama Gelir",
                "Bu mail bir bilgilendirme kaydidir. Manuel veya sistem tetigi ilgili entegrasyona ulastiysa gonderilir.",
                tone=_result_tone(result),
            ),
        )
    )
    return render_email_shell(
        eyebrow="SEO Agent Operations",
        title=f"{system_label} sistemi {TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)} tetiklendi",
        intro="Tetikleme akisi basladi veya tamamlandi. Asagidaki tablo hangi sistemin, hangi aksiyonla ve hangi sonuc ozetiyle calistigini gosterir.",
        tone=_result_tone(result),
        status_label="Bilgilendirme",
        sections=sections,
    )


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
    source_label = TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)
    subject = f"SEO Agent: {system_label} sistemi {source_label} tetiklendi"
    return _send_operations_email(
        subject,
        _trigger_email_body(
            trigger_source=trigger_source,
            system_label=system_label,
            site=site,
            result=result,
            action_label=action_label or system_label,
        ),
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
        notify_system_trigger(
            trigger_source=trigger_source,
            system_key=key_map.get(result_key, result_key),
            site=site,
            result=result_value if isinstance(result_value, dict) else {"summary": result_value},
            action_label=action_label,
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
            strategy="homepage",
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
                        "Uygulamanin o saatte ayakta oldugunu, scheduler'in aktif oldugunu ve ilgili refresh job akisinin hata vermedigini kontrol et. Run hic olusmadiysa job baslamamis olabilir; failed veya started goruluyorsa islem tamamlanmadan kesilmis olabilir.",
                        tone="amber",
                    ),
                ),
            ],
        )
        if _send_operations_email(subject, body, notification_key=notification_key, db=db):
            sent_subjects.append(subject)

    return sent_subjects
