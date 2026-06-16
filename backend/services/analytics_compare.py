"""GA4 / Search Console — /ad ile aynı karşılaştırma modları (günlük veriden yeniden özet)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from backend.services.ad_analytics_store import resolve_compare_range


def parse_compare_options(
    *,
    enabled: bool = False,
    mode: str | None = None,
    custom_start: str | None = None,
    custom_end: str | None = None,
) -> dict[str, Any]:
    on = bool(enabled) and bool(mode)
    m = (mode or "").strip() or "previous_period"
    if m not in ("previous_period", "previous_year", "custom"):
        m = "previous_period"
    return {
        "enabled": on,
        "mode": m if on else None,
        "custom_start": (custom_start or "")[:10] or None,
        "custom_end": (custom_end or "")[:10] or None,
    }


def _parse_iso(d: str | None) -> date | None:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d)[:10])
    except (ValueError, TypeError):
        return None


def _in_range(d: str | None, start: date, end: date) -> bool:
    dd = _parse_iso(d)
    if not dd:
        return False
    return start <= dd <= end


def _ga4_pct_change(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0 if current == 0 else 100.0
    return ((current - previous) / abs(previous)) * 100.0


def _sum_ga4_daily(daily: dict[str, Any], start: date, end: date) -> dict[str, float]:
    dates = daily.get("dates") or []
    sessions = daily.get("sessions") or []
    users = daily.get("activeUsers") or daily.get("totalUsers") or []
    engaged = daily.get("engagedSessions") or []
    pageviews = daily.get("screenPageViews") or daily.get("pageviews") or []

    tot_sess = tot_users = tot_eng = tot_pv = 0.0
    for i, raw in enumerate(dates):
        if not _in_range(str(raw), start, end):
            continue
        s = float(sessions[i] if i < len(sessions) else 0) or 0.0
        u = float(users[i] if i < len(users) else 0) or 0.0
        e = float(engaged[i] if i < len(engaged) else 0) or 0.0
        pv = float(pageviews[i] if i < len(pageviews) else 0) or 0.0
        tot_sess += s
        tot_users += u
        tot_eng += e
        tot_pv += pv
    eng_rate_pct = (tot_eng / tot_sess * 100.0) if tot_sess > 0 else 0.0
    return {
        "sessions": tot_sess,
        "users": tot_users,
        "engaged": tot_eng,
        "pageviews": tot_pv,
        "engagement_rate_pct": eng_rate_pct,
    }


def apply_ga4_period_compare(
    period: dict[str, Any],
    *,
    compare: dict[str, Any],
    daily_long: dict[str, Any] | None,
) -> dict[str, Any]:
    """7/30/90 KPI önceki dönemini seçilen moda göre günceller (daily seri gerekir)."""
    if not compare.get("enabled"):
        return period
    mode = compare.get("mode") or "previous_period"
    if mode == "previous_period":
        period["compare_mode"] = mode
        return period
    if period.get("trend_only") or int(period.get("period_days") or 0) == 1:
        return period
    daily = daily_long if isinstance(daily_long, dict) else {}
    if not (daily.get("dates") or []):
        period["compare_mode"] = mode
        period["compare_note"] = "Uzun günlük seri yok; varsayılan önceki dönem kullanıldı."
        return period

    ranges = period.get("ranges") or {}
    ps = ranges.get("last_start") or ""
    pe = ranges.get("last_end") or ""
    cs, ce = resolve_compare_range(
        ps,
        pe,
        mode,
        compare.get("custom_start"),
        compare.get("custom_end"),
    )
    if not cs or not ce:
        return period
    c_start = _parse_iso(cs)
    c_end = _parse_iso(ce)
    if not c_start or not c_end:
        return period

    if _ga4_daily_coverage(daily, c_start, c_end) < (c_end - c_start).days + 1:
        out = dict(period)
        out["compare_mode"] = mode
        out["compare_data_unavailable"] = True
        out["compare_note"] = "Seçilen karşılaştırma aralığı için günlük GA4 verisi yok."
        out["ranges"] = {**ranges, "prev_start": cs, "prev_end": ce}
        out["sessions_pct_change"] = None
        out["users_pct_change"] = None
        out["engaged_pct_change"] = None
        out["pageviews_pct_change"] = None
        out["engagement_rate_pct_change"] = None
        out["wow_change_pct"] = None
        return out

    agg = _sum_ga4_daily(daily, c_start, c_end)
    out = dict(period)
    out["compare_data_unavailable"] = False
    out["ranges"] = {
        **ranges,
        "prev_start": cs,
        "prev_end": ce,
    }
    out["prev_total"] = agg["sessions"]
    out["users_prev"] = agg["users"]
    out["engaged_prev"] = agg["engaged"]
    out["pageviews_prev"] = agg["pageviews"]
    out["engagement_rate_prev_pct"] = agg["engagement_rate_pct"]
    out["sessions_pct_change"] = _ga4_pct_change(float(out.get("last_total") or 0), agg["sessions"])
    out["users_pct_change"] = _ga4_pct_change(float(out.get("users_last") or 0), agg["users"])
    out["engaged_pct_change"] = _ga4_pct_change(float(out.get("engaged_last") or 0), agg["engaged"])
    out["pageviews_pct_change"] = _ga4_pct_change(float(out.get("pageviews_last") or 0), agg["pageviews"])
    out["engagement_rate_pct_change"] = _ga4_pct_change(
        float(out.get("engagement_rate_last_pct") or 0),
        agg["engagement_rate_pct"],
    )
    out["wow_change_pct"] = out["sessions_pct_change"]
    out["compare_mode"] = mode
    out["compare_label_prev"] = f"{cs} – {ce}"
    return out


def _sc_summarize_daily_rows(rows: list[dict[str, Any]], device_code: str, start: date, end: date) -> dict[str, float]:
    clicks = impressions = 0.0
    pos_weight = 0.0
    for row in rows:
        dev = str(row.get("device") or "ALL").upper()
        if dev != device_code:
            continue
        if not _in_range(str(row.get("date") or ""), start, end):
            continue
        c = float(row.get("clicks") or 0)
        im = float(row.get("impressions") or 0)
        p = float(row.get("position") or 0)
        clicks += c
        impressions += im
        if im > 0:
            pos_weight += p * im
    ctr = (clicks / impressions * 100.0) if impressions > 0 else 0.0
    position = (pos_weight / impressions) if impressions > 0 else 0.0
    return {
        "clicks": clicks,
        "impressions": impressions,
        "ctr": ctr,
        "position": position,
    }


def _sc_pct_change(cur: float, prev: float) -> float | None:
    if prev == 0:
        return 0.0 if cur == 0 else 100.0
    return ((cur - prev) / abs(prev)) * 100.0


def _sc_daily_coverage(
    rows: list[dict[str, Any]],
    device_code: str,
    start: date,
    end: date,
) -> int:
    """Karşılaştırma aralığında bu cihaz için kaç güne ait günlük satır var."""
    days: set[str] = set()
    dev = str(device_code or "").upper()
    for row in rows:
        if str(row.get("device") or "ALL").upper() != dev:
            continue
        d = _parse_iso(str(row.get("date") or ""))
        if d and start <= d <= end:
            days.add(d.isoformat())
    return len(days)


def resolve_sc_summary_period_range(
    summary_payload: dict[str, Any],
    period_key: str,
    scope_fallback: tuple[str, str],
) -> tuple[str, str]:
    """7/30/90 güncel dönem ISO aralığı — sorgu satırı min/max yerine collector özeti."""
    keys_by_period = {
        "7": ("current_7d_start", "current_7d_end"),
        "30": ("current_30d_start", "current_30d_end"),
        "90": ("current_90d_start", "current_90d_end"),
    }
    pair = keys_by_period.get(str(period_key))
    if pair:
        s = str(summary_payload.get(pair[0]) or "").strip()[:10]
        e = str(summary_payload.get(pair[1]) or "").strip()[:10]
        if s and e:
            return s, e
    rows = list(summary_payload.get("trend_28d_rows") or []) + list(
        summary_payload.get("trend_12m_rows") or []
    )
    dates = sorted({str(r.get("date") or "").strip()[:10] for r in rows if r.get("date")})
    if not dates:
        return scope_fallback
    try:
        pd = int(period_key)
        end_d = date.fromisoformat(dates[-1])
        start_d = end_d - timedelta(days=pd - 1)
        return start_d.isoformat(), end_d.isoformat()
    except (ValueError, TypeError, OSError):
        return scope_fallback


def _ga4_daily_coverage(daily: dict[str, Any], start: date, end: date) -> int:
    days: set[str] = set()
    for raw in daily.get("dates") or []:
        d = _parse_iso(str(raw))
        if d and start <= d <= end:
            days.add(d.isoformat())
    return len(days)


def apply_sc_period_view_compare(
    view: dict[str, Any],
    *,
    period_key: str,
    primary_start: str,
    primary_end: str,
    compare: dict[str, Any],
    daily_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not compare.get("enabled") or period_key == "12m":
        return view
    mode = compare.get("mode") or "previous_period"
    if mode == "previous_period":
        view["compare_mode"] = mode
        return view

    cs, ce = resolve_compare_range(
        primary_start,
        primary_end,
        mode,
        compare.get("custom_start"),
        compare.get("custom_end"),
    )
    if not cs or not ce:
        return view
    c_start = _parse_iso(cs)
    c_end = _parse_iso(ce)
    p_start = _parse_iso(primary_start)
    p_end = _parse_iso(primary_end)
    if not c_start or not c_end or not p_start or not p_end:
        return view

    device_code = str(view.get("device_code") or "DESKTOP").upper()
    out = dict(view)
    out["compare_mode"] = mode
    out["compare_prev_start"] = cs
    out["compare_prev_end"] = ce
    out["compare_primary_start"] = primary_start[:10]
    out["compare_primary_end"] = primary_end[:10]

    if not daily_rows:
        out["compare_data_unavailable"] = True
        out["compare_unavailable_note"] = (
            "Günlük trend verisi yok. Search Console kartını yenileyin."
        )
        out["clicks_pct_change"] = None
        out["impressions_pct_change"] = None
        out["ctr_pct_change"] = None
        out["position_delta"] = None
        return out

    cov = _sc_daily_coverage(daily_rows, device_code, c_start, c_end)
    if cov == 0:
        out["compare_data_unavailable"] = True
        out["compare_unavailable_note"] = (
            "Seçilen karşılaştırma tarihleri önbellekte yok "
            "(geçen yıl için 12 aylık günlük seri gerekir). Siteyi yenileyin."
        )
        out["clicks_pct_change"] = None
        out["impressions_pct_change"] = None
        out["ctr_pct_change"] = None
        out["position_delta"] = None
        return out

    cur = _sc_summarize_daily_rows(daily_rows, device_code, p_start, p_end)
    prev = _sc_summarize_daily_rows(daily_rows, device_code, c_start, c_end)

    out["compare_data_unavailable"] = False
    out["summary_previous"] = {
        "clicks": prev["clicks"],
        "impressions": prev["impressions"],
        "ctr": prev["ctr"],
        "position": prev["position"],
    }
    out["summary_current"] = {
        "clicks": cur["clicks"],
        "impressions": cur["impressions"],
        "ctr": cur["ctr"],
        "position": cur["position"],
    }
    prev_has_signal = (
        prev["clicks"] > 0 or prev["impressions"] > 0 or prev["ctr"] > 0 or prev["position"] > 0
    )
    if not prev_has_signal and cov > 0:
        out["compare_data_unavailable"] = True
        out["compare_unavailable_note"] = "Karşılaştırma döneminde ölçülebilir veri yok."
        out["clicks_pct_change"] = None
        out["impressions_pct_change"] = None
        out["ctr_pct_change"] = None
        out["position_delta"] = None
        return out

    out["clicks_pct_change"] = _sc_pct_change(cur["clicks"], prev["clicks"])
    out["impressions_pct_change"] = _sc_pct_change(cur["impressions"], prev["impressions"])
    out["ctr_pct_change"] = _sc_pct_change(cur["ctr"], prev["ctr"])
    out["position_delta"] = prev["position"] - cur["position"]
    return out


def apply_search_console_report_compare(
    report: dict[str, Any],
    *,
    compare: dict[str, Any],
    summary_payload: dict[str, Any],
    period_primary_ranges: dict[str, tuple[str | None, str | None]],
    format_prev_label: Any,
) -> dict[str, Any]:
    """7/30/90 KPI ve tablo önceki dönem etiketlerini karşılaştırma moduna göre günceller."""
    if not compare.get("enabled"):
        return report
    mode = compare.get("mode") or "previous_period"
    if mode == "previous_period":
        report["compare_mode"] = mode
        return report

    daily_rows = list(summary_payload.get("trend_28d_rows") or []) + list(
        summary_payload.get("trend_12m_rows") or []
    )
    periods = report.get("periods") or {}
    for period_key, pr in period_primary_ranges.items():
        ps, pe = (pr[0] or "").strip(), (pr[1] or "").strip()
        if not ps or not pe or period_key not in periods:
            continue
        views = periods[period_key].get("views") or {}
        for device_key, view in list(views.items()):
            if not isinstance(view, dict):
                continue
            updated = apply_sc_period_view_compare(
                view,
                period_key=period_key,
                primary_start=ps,
                primary_end=pe,
                compare=compare,
                daily_rows=daily_rows,
            )
            cps = str(updated.get("compare_prev_start") or "").strip()[:10]
            cpe = str(updated.get("compare_prev_end") or "").strip()[:10]
            pps = str(updated.get("compare_primary_start") or ps).strip()[:10]
            ppe = str(updated.get("compare_primary_end") or pe).strip()[:10]
            if callable(format_prev_label):
                if cps and cpe:
                    updated["table_label_previous"] = format_prev_label(cps, cpe)
                    updated["range_prev"] = updated["table_label_previous"]
                if pps and ppe:
                    updated["range_last"] = format_prev_label(pps, ppe)
            views[device_key] = updated
        mv = views.get("mobile") or {}
        if mv.get("range_prev") or mv.get("table_label_previous"):
            periods[period_key]["label_previous"] = mv.get("table_label_previous") or mv.get("range_prev")
            subtitle = (
                f"Güncel dönem: {mv.get('range_last') or '—'} · "
                f"Karşılaştırma: {mv.get('range_prev') or mv.get('table_label_previous') or '—'}"
            )
            if mv.get("compare_data_unavailable") and mv.get("compare_unavailable_note"):
                subtitle += f" · {mv['compare_unavailable_note']}"
            periods[period_key]["subtitle"] = subtitle
        periods[period_key]["views"] = views

    report["periods"] = periods
    report["compare_mode"] = mode
    pk = "7"
    if pk in periods:
        legacy = periods[pk].get("views") or {}
        if legacy:
            report["views"] = legacy
    return report


def compare_mode_label_tr(mode: str | None) -> str:
    if mode == "previous_year":
        return "Geçen yıl (aynı tarihler)"
    if mode == "custom":
        return "Özel karşılaştırma aralığı"
    return "Önceki dönem (aynı uzunluk)"
