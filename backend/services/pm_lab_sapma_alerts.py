"""Rakip fiyat sapması — sarı eşiği aşınca operasyon maili."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from backend.config import settings
from backend.services.email_templates import note_box, render_email_shell, section
from backend.services.mailer import send_email
from backend.services.operations_notifier import _delivery_exists, _record_delivery, operations_recipients

LOGGER = logging.getLogger(__name__)

_SCRAPE = None
_ALERT_TYPE = "pm_lab_sapma"
_COOLDOWN_HOURS = 2


def _scrape_mod() -> Any:
    global _SCRAPE
    if _SCRAPE is not None:
        return _SCRAPE
    import importlib.util

    path = Path(__file__).resolve().parents[2] / "scripts" / "pm_lab_scrape.py"
    spec = importlib.util.spec_from_file_location("_pm_lab_scrape_sapma", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("pm_lab_scrape yüklenemedi")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _SCRAPE = mod
    return mod


def _hour_bucket(hours: int = _COOLDOWN_HOURS) -> str:
    now = datetime.now(timezone.utc)
    slot = now.hour // max(1, hours)
    return f"{now.strftime('%Y%m%d')}-{slot}"


def _render_html(item: dict[str, Any]) -> str:
    tone = "rose" if item.get("band") == "hot" else "amber"
    status = "kritik sapma" if item.get("band") == "hot" else "uyarı eşiği"
    kind = "Foreks" if item.get("kind") == "foreks" else "diğer sitelerin ortalaması"
    warn = str(item.get("warn") or "").replace(".", ",")
    detail = (
        f"{item.get('asset') or ''} · {item.get('pct_text') or ''}\n"
        f"Döviz vs {kind}. Hacim eşiği ±{warn}%."
    )
    return render_email_shell(
        eyebrow="PM lab",
        title=str(item.get("subject") or "Doviz sapma"),
        intro="Kotasyon sapması uyarı eşiğini geçti.",
        tone=tone,
        status_label=status,
        sections=[section("Sapma", note_box("Ölçüm", detail, tone=tone))],
    )


def notify_competitor_sapma(db: Session, section_data: dict[str, Any] | None) -> list[str]:
    """Sarı/kırmızı sapmalar için mail. Konu: Doviz - Sapma - varlık - değer."""
    if not isinstance(section_data, dict):
        return []
    if not getattr(settings, "outbound_email_enabled", False):
        return []
    matrix = section_data.get("matrix")
    if not isinstance(matrix, list) or not matrix:
        return []
    try:
        alerts = _scrape_mod().collect_sapma_alerts(matrix)
    except Exception:
        LOGGER.exception("sapma alarm listesi üretilemedi")
        return []
    recipients = operations_recipients()
    if not recipients:
        return []
    sent: list[str] = []
    bucket = _hour_bucket()
    for item in alerts:
        subject = str(item.get("subject") or "").strip()[:255]
        if not subject:
            continue
        key = f"{item.get('kind')}:{item.get('asset_id')}:{bucket}"
        if _delivery_exists(db, notification_type=_ALERT_TYPE, notification_key=key):
            continue
        html = _render_html(item)
        ok = send_email(subject, html, recipients=recipients)
        if not ok:
            continue
        _record_delivery(
            db,
            notification_type=_ALERT_TYPE,
            notification_key=key,
            subject=subject,
            recipient=",".join(recipients),
        )
        sent.append(subject)
    return sent
