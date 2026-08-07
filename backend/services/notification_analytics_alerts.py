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


_PLATFORM_KEYS = ("desktop", "mobileweb", "android", "ios")
_PLATFORM_WITH_IMPRESSIONS = ("desktop", "mobileweb", "android")


def _period_stats(rows: list[dict]) -> dict[str, Any]:
    clicks = 0.0
    impressions = 0.0
    platform: dict[str, float] = {k: 0.0 for k in _PLATFORM_KEYS}
    platform_impr: dict[str, float] = {k: 0.0 for k in _PLATFORM_KEYS}
    for row in rows:
        clicks += _row_clicks(row)
        impressions += _row_impressions(row)
        plats = row.get("platforms") or {}
        for key in _PLATFORM_KEYS:
            plat = plats.get(key) or {}
            try:
                platform[key] += float(plat.get("click") or 0)
            except (TypeError, ValueError):
                pass
            if key in _PLATFORM_WITH_IMPRESSIONS:
                try:
                    platform_impr[key] += float(plat.get("impression") or 0)
                except (TypeError, ValueError):
                    pass
    ctr = (clicks / impressions * 100.0) if impressions > 0 else 0.0
    return {
        "rows": len(rows),
        "clicks": round(clicks, 2),
        "impressions": round(impressions, 2),
        "ctr": round(ctr, 4),
        "platform_clicks": {k: round(v, 2) for k, v in platform.items()},
        "platform_impressions": {k: round(v, 2) for k, v in platform_impr.items()},
    }


def _platform_click(row: dict, key: str) -> float:
    plat = (row.get("platforms") or {}).get(key) or {}
    try:
        return float(plat.get("click") or 0)
    except (TypeError, ValueError):
        return 0.0


def _top_sends_by_clicks(rows: list[dict], *, limit: int = 5) -> list[dict[str, Any]]:
    """Her gönderim (id + tarih) için platform click kırılımı; toplam click'e göre Top N."""
    items: list[dict[str, Any]] = []
    for row in rows:
        text = str(row.get("text") or "").strip()
        desktop = _platform_click(row, "desktop")
        mobileweb = _platform_click(row, "mobileweb")
        android = _platform_click(row, "android")
        ios = _platform_click(row, "ios")
        total = desktop + mobileweb + android + ios
        if not text or total <= 0:
            continue
        send_day = _row_day_key(row.get("date")) or str(row.get("date") or "")[:10]
        items.append(
            {
                "id": str(row.get("id") or "").strip() or "—",
                "text": text,
                "send_day": send_day or "—",
                "clicks": round(total, 2),
                "desktop": round(desktop, 2),
                "mobileweb": round(mobileweb, 2),
                "android": round(android, 2),
                "ios": round(ios, 2),
            }
        )
    items.sort(key=lambda x: (-float(x["clicks"]), str(x["text"]).lower()))
    return items[: max(0, int(limit))]


def _week_windows(reference_day: date) -> tuple[date, date, date, date]:
    cur_end = reference_day
    cur_start = reference_day - timedelta(days=WINDOW_DAYS - 1)
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=WINDOW_DAYS - 1)
    return cur_start, cur_end, prev_start, prev_end


def build_notification_week_compare(
    db: Session,
    *,
    reference_day: date | None = None,
    top_n: int = 5,
    as_of_day: date | None = None,
) -> dict[str, Any]:
    """Son 7 tam gün vs önceki 7 gün — platform click/impression + top başlıklar (Döviz).

    reference_day pencerenin son günüdür (dahil). Ana sayfa dünü verir ki
    bugünün eksik verisi kıyasa karışmasın.

    as_of_day (varsayılan: bugün) ile bugünü kapsayan son 7 gün Top N
    (top_titles_including_today) ayrıca üretilir — KPI penceresini değiştirmez.
    """
    ref = reference_day or date.today()
    today = as_of_day or date.today()
    cur_start, cur_end, prev_start, prev_end = _week_windows(ref)
    incl_end = today
    incl_start = today - timedelta(days=WINDOW_DAYS - 1)
    all_rows = _load_rows(_get_workspace(db))
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
    incl_rows = filter_rows_by_date(
        all_rows,
        start=incl_start.isoformat(),
        end=incl_end.isoformat(),
    )
    cur = _period_stats(cur_rows)
    prev = _period_stats(prev_rows)
    platforms: list[dict[str, Any]] = []
    labels = {
        "desktop": "Web",
        "mobileweb": "Mobil Web",
        "ios": "iOS",
        "android": "Android",
    }
    for key in ("desktop", "mobileweb", "ios", "android"):
        has_impr = key in _PLATFORM_WITH_IMPRESSIONS
        platforms.append(
            {
                "key": key,
                "label": labels[key],
                "clicks_cur": cur["platform_clicks"].get(key, 0.0),
                "clicks_prev": prev["platform_clicks"].get(key, 0.0),
                "impressions_cur": cur["platform_impressions"].get(key, 0.0) if has_impr else None,
                "impressions_prev": prev["platform_impressions"].get(key, 0.0) if has_impr else None,
                "has_impressions": has_impr,
            }
        )
    return {
        "reference_day": ref.isoformat(),
        "as_of_day": today.isoformat(),
        "windows": {
            "current": {"start": cur_start.isoformat(), "end": cur_end.isoformat()},
            "previous": {"start": prev_start.isoformat(), "end": prev_end.isoformat()},
            "including_today": {
                "start": incl_start.isoformat(),
                "end": incl_end.isoformat(),
            },
        },
        "totals": {
            "clicks_cur": cur["clicks"],
            "clicks_prev": prev["clicks"],
            "impressions_cur": cur["impressions"],
            "impressions_prev": prev["impressions"],
            "rows_cur": cur["rows"],
            "rows_prev": prev["rows"],
        },
        "platforms": platforms,
        "top_titles": _top_sends_by_clicks(cur_rows, limit=top_n),
        "top_titles_previous": _top_sends_by_clicks(prev_rows, limit=top_n),
        "top_titles_including_today": _top_sends_by_clicks(incl_rows, limit=top_n),
        "empty": cur["rows"] == 0 and prev["rows"] == 0,
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
    cur_start, cur_end, prev_start, prev_end = _week_windows(ref)
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
