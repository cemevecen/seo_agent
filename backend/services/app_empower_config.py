"""Empower (uygulama) günlük metrik serileri — grafik overlay."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppEmpowerSeries:
    key: str
    label: str
    unit: str


APP_EMPOWER_SERIES: tuple[AppEmpowerSeries, ...] = (
    AppEmpowerSeries("sessions", "Sessions", ""),
    AppEmpowerSeries("dau_7d", "DAU (7 gün)", ""),
    AppEmpowerSeries("crash_affected_users", "Crash etkilenen", ""),
    AppEmpowerSeries("avg_session_duration", "Ort. oturum süresi", "sn"),
    AppEmpowerSeries("engagement_rate", "Engagement rate", "%"),
    AppEmpowerSeries("arpdau_usd", "ARPDAU", "USD"),
)

SERIES_BY_KEY: dict[str, AppEmpowerSeries] = {s.key: s for s in APP_EMPOWER_SERIES}
