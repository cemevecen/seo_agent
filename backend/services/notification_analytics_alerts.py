"""Notification Analytics — eşik alarmları (click düşüşü, CTR medyan altı)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from html import escape
from typing import Any

from sqlalchemy.orm import Session

from backend.services.notification_analytics_store import (
    WORKSPACE_ID,
    _get_workspace,
    _load_rows,
    _row_day_key,
    filter_rows_by_date,
)
from backend.services.operations_notifier import _delivery_exists, _send_operations_email, operations_recipients

LOGGER = logging.getLogger(__name__)

NOTIFICATION_TYPE = "notification_analytics"
CLICK_DROP_PCT = 30.0
WINDOW_DAYS = 7
MEDIAN_LOOKBACK_DAYS = 30


def _parse_day(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def _row_clicks(row: dict) -> float:
    total = 0.0
    for key in ("desktop", "mobileweb", "android", "ios"):
        plat = (row.get("platforms") or {}).get(key) or {}
        try:
            total += float(plat.get("click") or 0)
        except (TypeError, ValueError):
            pass
    return total


def _row_impressions(row: dict) -> float:
    total = 0.0
    for key in ("desktop", "mobileweb", "android"):
        plat = (row.get("platforms") or {}).get(key) or {}
        try:
            total += float(plat.get("impression") or 0)
        except (TypeError, ValueError):
            pass
    return total


def _period_stats(rows: list[dict]) -> dict[str, Any]:
    clicks = 0.0
    impressions = 0.0
    platform: dict[str, float] = {"desktop": 0.0, "mobileweb": 0.0, "android": 0.0, "ios": 0.0}
    for row in rows:
        clicks += _row_clicks(row)
        impressions += _row_impressions(row)
        for key in platform:
            plat = (row.get("platforms") or {}).get(key) or {}
            try:
                platform[key] += float(plat.get("click") or 0)
            except (TypeError, ValueError):
                pass
    ctr = (clicks / impressions * 100.0) if impressions > 0 else 0.0
    return {
        "rows": len(rows),
        "clicks": round(clicks, 2),
        "impressions": round(impressions, 2),
        "ctr": round(ctr, 4),
        "platform_clicks": {k: round(v, 2) for k, v in platform.items()},
    }


def _daily_ctr_values(rows: list[dict]) -> list[float]:
    by_day: dict[str, dict[str, float]] = {}
    for row in rows:
        d = _row_day_key(row.get("date"))
        if not d:
            continue
        if d not in by_day:
            by_day[d] = {"clicks": 0.0, "impressions": 0.0}
        by_day[d]["clicks"] += _row_clicks(row)
        by_day[d]["impressions"] += _row_impressions(row)
    out: list[float] = []
    for agg in by_day.values():
        if agg["impressions"] > 0:
            out.append(agg["clicks"] / agg["impressions"] * 100.0)
    return out


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    mid = len(s) // 2
    if len(s) % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def evaluate_notification_analytics_alerts(
    db: Session,
    *,
    send_email: bool = False,
    reference_day: date | None = None,
) -> dict[str, Any]:
    """Son 7 gün vs önceki 7 gün click; CTR medyan altı kontrolü."""
    ref = reference_day or date.today()
    cur_end = ref
    cur_start = ref - timedelta(days=WINDOW_DAYS - 1)
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=WINDOW_DAYS - 1)
    median_start = ref - timedelta(days=MEDIAN_LOOKBACK_DAYS - 1)

    row = _get_workspace(db)
    all_rows = _load_rows(row)
    cur_rows = filter_rows_by_date(
        all_rows,
        start=cur_start.isoformat(),
        end=cur_end.isoformat(),
    )
    prev_rows = filter_rows_by_date(
        all_rows,
        start=prev_start.isoformat(),
        end=prev_end.isoformat(),
    )
    median_rows = filter_rows_by_date(
        all_rows,
        start=median_start.isoformat(),
        end=cur_end.isoformat(),
    )

    cur = _period_stats(cur_rows)
    prev = _period_stats(prev_rows)
    click_delta_pct: float | None = None
    if prev["clicks"] > 0:
        click_delta_pct = round((cur["clicks"] - prev["clicks"]) / prev["clicks"] * 100.0, 2)
    elif cur["clicks"] > 0:
        click_delta_pct = None

    median_ctr = _median(_daily_ctr_values(median_rows))
    cur_daily_ctrs = _daily_ctr_values(cur_rows)
    cur_ctr_avg = sum(cur_daily_ctrs) / len(cur_daily_ctrs) if cur_daily_ctrs else 0.0

    alerts: list[dict[str, Any]] = []

    if prev["clicks"] > 0 and click_delta_pct is not None and click_delta_pct <= -CLICK_DROP_PCT:
        alerts.append(
            {
                "id": "click_drop",
                "severity": "critical",
                "title": f"Notification click %{abs(click_delta_pct):.1f} düştü",
                "summary": (
                    f"Son {WINDOW_DAYS} gün: {int(cur['clicks']):,} click · "
                    f"Önceki {WINDOW_DAYS} gün: {int(prev['clicks']):,} click "
                    f"({click_delta_pct:+.1f}%)"
                ).replace(",", "."),
                "metric": {"current": cur["clicks"], "previous": prev["clicks"], "delta_pct": click_delta_pct},
            }
        )

    if median_ctr > 0 and cur_ctr_avg > 0 and cur_ctr_avg < median_ctr:
        alerts.append(
            {
                "id": "ctr_below_median",
                "severity": "warning",
                "title": "Notification CTR medyanın altında",
                "summary": (
                    f"Son {WINDOW_DAYS} gün ort. CTR %{cur_ctr_avg:.2f} · "
                    f"{MEDIAN_LOOKBACK_DAYS} gün medyan %{median_ctr:.2f}"
                ),
                "metric": {"current_ctr": round(cur_ctr_avg, 4), "median_ctr": round(median_ctr, 4)},
            }
        )

    sent: list[str] = []
    if send_email and alerts:
        day_key = ref.isoformat()
        batch_key = f"nt-analytics:{day_key}"
        if not _delivery_exists(db, notification_type=NOTIFICATION_TYPE, notification_key=batch_key):
            subject = f"[Notification Analytics] {len(alerts)} alarm — {day_key}"
            lines = [
                "<h2>Notification Analytics alarmları</h2>",
                f"<p>Dönem: son {WINDOW_DAYS} gün ({cur_start} – {cur_end})</p>",
                "<ul>",
            ]
            for a in alerts:
                lines.append(f"<li><b>{escape(a['title'])}</b> — {escape(a['summary'])}</li>")
            lines.append("</ul>")
            lines.append(
                '<p><a href="https://projectcontrol.up.railway.app/notification">Paneli aç</a></p>'
            )
            body = "\n".join(lines)
            if _send_operations_email(subject, body, notification_key=batch_key, db=db):
                sent.append(batch_key)
                db.commit()
            try:
                from backend.services.agent_tools import create_alert

                for a in alerts:
                    create_alert(
                        alert_type=f"notification_analytics_{a['id']}",
                        severity=a["severity"],
                        title=a["title"],
                        summary=a["summary"],
                        detail=a.get("metric") or {},
                    )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("create_alert failed: %s", exc)
        else:
            LOGGER.info("Notification analytics alert already sent for %s", day_key)

    return {
        "ok": True,
        "workspace_id": WORKSPACE_ID,
        "reference_day": ref.isoformat(),
        "windows": {
            "current": {"start": cur_start.isoformat(), "end": cur_end.isoformat()},
            "previous": {"start": prev_start.isoformat(), "end": prev_end.isoformat()},
        },
        "current": cur,
        "previous": prev,
        "click_delta_pct": click_delta_pct,
        "median_ctr": round(median_ctr, 4),
        "current_ctr_avg": round(cur_ctr_avg, 4),
        "alerts": alerts,
        "email_sent": sent,
        "recipients": operations_recipients() if send_email else [],
    }
