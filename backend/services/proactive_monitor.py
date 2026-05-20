"""Proaktif izleme — Railway, DB ve GA4 anomalilerini tespit eder, alert oluşturur."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import httpx

from backend.config import settings

LOGGER = logging.getLogger(__name__)


def run_proactive_checks() -> None:
    """Her 30 dakikada bir çalışır. Anomali varsa ai_talk_alerts'e yazar."""
    LOGGER.info("Proaktif izleme başladı.")
    try:
        _check_railway()
    except Exception as e:
        LOGGER.warning("Railway kontrol hatası: %s", e)
    try:
        _check_db_size()
    except Exception as e:
        LOGGER.warning("DB boyut kontrol hatası: %s", e)
    try:
        _check_ga4_quota_errors()
    except Exception as e:
        LOGGER.warning("GA4 quota kontrol hatası: %s", e)
    LOGGER.info("Proaktif izleme tamamlandı.")


def _check_railway() -> None:
    from backend.services.agent_tools import railway_get_deployments, create_alert
    result = railway_get_deployments(limit=3)
    deploys = result.get("deployments") or []
    failed = [d for d in deploys if d.get("status") in ("FAILED", "CRASHED", "ERROR")]
    if not failed:
        return
    latest = failed[0]
    create_alert(
        alert_type="railway_deployment",
        severity="critical",
        title=f"Railway deploy başarısız: {latest.get('service', '?')}",
        summary=(
            f"son deployment başarısız oldu. "
            f"servis: {latest.get('service', '?')}, "
            f"durum: {latest.get('status', '?')}, "
            f"commit: {latest.get('commit', '')[:60]}"
        ),
        detail=latest,
    )


def _check_db_size() -> None:
    from backend.services.agent_tools import db_table_stats, create_alert
    result = db_table_stats()
    tables = result.get("tables") or []
    # Çok büyük tablo tespiti: 1GB+ olan tablolar
    big_tables = [t for t in tables if _parse_size_mb(t.get("size", "0")) > 1000]
    if not big_tables:
        return
    names = ", ".join(t["table"] for t in big_tables[:3])
    create_alert(
        alert_type="db_size",
        severity="warning",
        title=f"büyük tablo tespit edildi: {names}",
        summary=f"{len(big_tables)} tablo 1GB'ı geçti: {names}. temizlik veya partitioning düşünülebilir.",
        detail={"big_tables": big_tables[:5]},
    )


def _check_ga4_quota_errors() -> None:
    """Son 30 dakikadaki Railway loglarında GA4 429 hatası ara."""
    from backend.services.agent_tools import create_alert
    # Log çekme mekanizması yoksa bu kontrolü atla
    # Basit yaklaşım: bu fonksiyon şimdilik sadece yer tutucu
    # Gerçek implementasyon Railway log streaming API gerektirir
    pass


def _parse_size_mb(size_str: str) -> float:
    """'1.2 GB', '500 MB', '2 kB' → MB cinsine çevirir."""
    try:
        parts = size_str.strip().split()
        if len(parts) != 2:
            return 0
        val = float(parts[0].replace(",", "."))
        unit = parts[1].upper()
        if "GB" in unit:
            return val * 1024
        if "MB" in unit:
            return val
        if "KB" in unit:
            return val / 1024
        return val
    except Exception:
        return 0
