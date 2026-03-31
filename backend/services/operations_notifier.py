"""Operasyon e-postalari ve zamanlanmis guncelleme izleme yardimcilari."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

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

DEFAULT_OPERATIONS_RECIPIENT = "cemevecen@nokta.com"

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
        return f"{delta:+.2f} puan ({direction})".replace(".", ",")
    if field == "position":
        if delta < 0:
            note = "iyileşme"
        elif delta > 0:
            note = "kötüleşme"
        else:
            note = "değişmedi"
        return f"{delta:+.2f} ({note})".replace(".", ",")
    return f"{delta:+,.0f}".replace(",", ".")


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


def _trigger_email_body(
    *,
    trigger_source: str,
    system_label: str,
    site: Site | None,
    result: dict | None,
    action_label: str,
) -> str:
    details: list[tuple[str, str]] = []
    detail_rows: list[list[str]] = []
    comparison_rows: list[list[str]] = []
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
        if result.get("state"):
            details.append(("Durum", str(result["state"])))
        elif result.get("source"):
            details.append(("Kaynak", str(result["source"])))

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
    if summary_cards:
        sections.append(
            section(
                "Kritik Özet",
                stat_cards(summary_cards),
            )
        )
    if details:
        sections.append(
            section(
                "Calisma Detaylari",
                summary_table(details),
            )
        )
    if detail_rows:
        sections.append(
            section(
                "Metrik Dökümü",
                data_table(["Alan", "Değer"], detail_rows),
            )
        )
    if comparison_rows:
        sections.append(
            section(
                "Karşılaştırmalı Veri",
                data_table(["Alan", "Önceki 7 Gün", "Son 7 Gün", "Fark"], comparison_rows),
            )
        )
    return render_email_shell(
        eyebrow="SEO Agent Operations",
        title=f"{system_label} sistemi {TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)} tetiklendi",
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


def notify_crawler_audit_emails(
    *,
    db: Session,
    site: Site,
    result: dict | None,
    trigger_source: str,
) -> list[str]:
    link_audit = _crawler_summary_from_result(result)
    collector_run_id = str((result or {}).get("collector_run_id") or "").strip()
    if not link_audit or not collector_run_id:
        return []

    subjects: list[str] = []
    issue_rows = _crawler_issue_rows(link_audit)
    summary_rows = [
        ("Site", site.domain),
        ("Tetik tipi", TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)),
        ("Kaynak seçimi", str(link_audit.get("source_strategy") or "URL listesi")),
        ("Kaynak sayfa", _format_tr_number(link_audit.get("source_pages"), decimals=0)),
        ("Taranan URL", _format_tr_number(link_audit.get("audited_urls"), decimals=0)),
        ("Maks. hop", _format_tr_number(link_audit.get("max_hops"), decimals=0)),
    ]

    if trigger_source == "system":
        report_key = f"crawler-report:{site.id}:{collector_run_id}"
        if not _delivery_exists(db, notification_type="operations", notification_key=report_key):
            report_subject = f"SEO Agent: {site.domain} günlük crawler raporu"
            report_body = render_email_shell(
                eyebrow="SEO Agent Operations",
                title=f"{site.domain} günlük crawler raporu",
                intro="Site içi taranan URL'lerin son durum özeti aşağıda yer alır. Bu rapor kırık URL, 301/302 yönlendirme ve redirect zinciri dağılımını tek bakışta gösterir.",
                tone="blue",
                status_label="Günlük Rapor",
                sections=[
                    section("Özet", summary_table(summary_rows), subtitle="Günlük koşuda kullanılan kapsam ve taranan URL sayısı."),
                    section("Kritik Kartlar", stat_cards(_crawler_summary_cards(link_audit)), subtitle="En önemli crawler metrikleri."),
                    section(
                        "Sorunlu URL Örnekleri",
                        data_table(["Durum", "Hedef URL", "Final URL", "Durum Zinciri", "Kaynak Sayfalar"], issue_rows)
                        if issue_rows
                        else note_box("Temiz Sonuç", "Bu günlük koşuda örnek tabloya girecek kırık veya yönlendirmeli URL bulunmadı.", tone="emerald"),
                        subtitle="Tablo, tıklayıp kontrol etmen gereken örnek URL'leri gösterir.",
                    ),
                ],
            )
            if _send_operations_email(report_subject, report_body, notification_key=report_key, db=db):
                subjects.append(report_subject)

    has_issue = any(int(link_audit.get(key) or 0) > 0 for key in ("broken_links", "redirect_chains", "redirect_301_links", "redirect_302_links"))
    if has_issue:
        issue_key = f"crawler-issue:{site.id}:{collector_run_id}"
        if not _delivery_exists(db, notification_type="operations", notification_key=issue_key):
            issue_subject = f"SEO Agent UYARI: {site.domain} crawler sorunları bulundu"
            issue_body = render_email_shell(
                eyebrow="SEO Agent Operations",
                title=f"Uyarı: {site.domain} crawler sorunları bulundu",
                intro="Bu mail bir sorun bildirir. İç linklerde kırık hedef, yönlendirme veya redirect zinciri tespit edildi.",
                tone="amber",
                status_label="Uyarı",
                sections=[
                    section("Özet", summary_table(summary_rows), subtitle="Sorun çıkan koşunun kısa özeti."),
                    section("Kritik Kartlar", stat_cards(_crawler_summary_cards(link_audit)), subtitle="Hangi problem tipinin öne çıktığı burada görülür."),
                    section(
                        "Sorunlu URL Tablosu",
                        data_table(["Durum", "Hedef URL", "Final URL", "Durum Zinciri", "Kaynak Sayfalar"], issue_rows),
                        subtitle="Final URL ve bu linkin hangi kaynak sayfalarda bulunduğu birlikte gösterilir.",
                    ),
                    section(
                        "Yorum",
                        note_box(
                            "Ne Yapılmalı",
                            "Kırık URL'leri kaldır veya doğru hedefe yönlendir. 301/302 kullanan dahili linkleri doğrudan final URL'ye çevir. Zincir görülen URL'leri tek adımlı hedefe indir.",
                            tone="amber",
                        ),
                    ),
                ],
            )
            if _send_operations_email(issue_subject, issue_body, notification_key=issue_key, db=db):
                subjects.append(issue_subject)

    return subjects
