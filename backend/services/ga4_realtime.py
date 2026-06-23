"""GA4 Realtime API — pencereli karşılaştırma ve alarm değerlendirmesi.

Son 30 dakikayı iki pencereye böler (ör. 0-9 dk vs 10-19 dk) ve
activeUsers/screenPageViews değişimini izleyerek alarm tetikler.
"""

from __future__ import annotations

import copy
import hashlib
import html
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    MinuteRange,
    OrderBy,
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


def _normalize_ga4_property_id(property_id: str) -> str:
    """Ayarlar bazen `properties/123` veya çift önek içerir; Realtime isteği yalnız sayısal id ister."""
    pid = str(property_id or "").strip().strip("/")
    while pid.lower().startswith("properties/"):
        pid = pid.split("/", 1)[1].strip().strip("/")
    return pid


# Realtime mail ve döngü sıralaması: doviz önce, sinemalar sonra; her site içinde profil sırası
_DOMAIN_PRIORITY: dict[str, int] = {
    "doviz.com": 0,
    "sinemalar.com": 1,
}
_PROFILE_PRIORITY: dict[str, int] = {
    "web": 0,
    "mweb": 1,
    "ios": 2,
    "android": 3,
}


def _domain_sort_key(domain: str) -> tuple[int, str]:
    d = (domain or "").lower().lstrip("www.").strip()
    return (_DOMAIN_PRIORITY.get(d, 99), d)


def _site_profile_sort_key(site_domain: str, profile: str) -> tuple[int, int, str]:
    d = (site_domain or "").lower().lstrip("www.").strip()
    return (
        _DOMAIN_PRIORITY.get(d, 99),
        _PROFILE_PRIORITY.get(profile, 99),
        d,
    )


def _sort_sites(sites: list) -> list:
    return sorted(sites, key=lambda s: _domain_sort_key(s.domain))


def _realtime_email_thread_key(domain: str, profile: str) -> str:
    """Site + profil için sabit Gmail iş parçacığı anahtarı (mailer `thread_kind` ile birlikte)."""
    dom = (domain or "").strip().lower()
    if dom.startswith("www."):
        dom = dom[4:]
    dom = re.sub(r"[^a-z0-9.-]", "", dom) or "site"
    prof = re.sub(r"[^a-z0-9]", "", (profile or "web").strip().lower()) or "web"
    return f"{dom}.{prof}"



    """Gmail iş parçacığı / References için site+profil anahtarı (ASCII, kısa)."""
    d = (domain or "").strip().lower()
    p = (profile or "").strip().lower()
    return hashlib.sha256(f"{d}|{p}".encode()).hexdigest()[:24]


def _email_site_short_label(domain: str, *, max_len: int = 36) -> str:
    """Konu satırı için kısa site adı — www. ve TLD (.com/.net vb.) atılır."""
    import re as _re
    d = (domain or "").strip().lower().rstrip(".")
    if d.startswith("www."):
        d = d[4:]
    # TLD'yi kaldır: .com .net .org .com.tr .co.uk vb.
    d = _re.sub(r"\.[a-z]{2,6}(\.[a-z]{2})?$", "", d)
    if not d:
        return "site"
    if len(d) > max_len:
        return d[: max_len - 1] + "…"
    return d


def _email_profile_abbr(profile: str) -> str:
    return {"web": "web", "mweb": "mweb", "android": "android", "ios": "ios"}.get(profile, profile or "web")


def _email_metric_chip(metric: str) -> str:
    """E-posta gövdesi/konu — kısa metrik kodu."""
    m = (metric or "").strip()
    return {"activeUsers": "kul", "screenPageViews": "gör"}.get(m, m)


def _email_metric_plain_tr(metric: str) -> str:
    return _email_metric_chip(metric)


def _html_email_metric_row(prev: int, cur: int, pct: float, *, large: bool = False) -> str:
    """Önceki/şimdiki yarı etiketi olmadan: 1067 → 2427 +127.5%"""
    is_drop = pct < 0
    pct_c = "#dc2626" if is_drop else "#16a34a"
    fs = "26px" if large else "22px"
    if prev > 0:
        return (
            f'<div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;">'
            f'<span style="font-size:{fs};font-weight:900;color:#475569;">{prev:,}</span>'
            f'<span style="font-size:16px;color:#94a3b8;">→</span>'
            f'<span style="font-size:{fs};font-weight:900;color:#0f172a;">{cur:,}</span>'
            f'<span style="font-size:16px;font-weight:800;color:{pct_c};">{pct:+.1f}%</span>'
            f'</div>'
        )
    return f'<span style="font-size:{fs};font-weight:900;color:#0f172a;">{cur:,}</span>'


def _html_email_section_header(domain: str, profile: str) -> str:
    """Kısa site adı + profil — www.doviz.com yok."""
    short = html.escape(_email_site_short_label(domain))
    prof = html.escape(_email_profile_abbr(profile))
    suffix = f" [{prof}]" if prof not in ("web", "") else ""
    return f'<p style="font-size:14px;font-weight:700;margin:0 0 10px;">{short}{suffix}</p>'


def _email_metric_subject_slug(metric: str, rule_id: str) -> str:
    """Konu satırı: kullanıcı isteği gibi kısa İngilizce küçük harf."""
    m = (metric or "").strip()
    if m == "activeUsers":
        return "active users"
    if m == "screenPageViews":
        return "page views"
    if rule_id.startswith("news_"):
        return "haber"
    if rule_id.startswith("page_"):
        return "sayfa"
    return (m or "metric").replace("_", " ").lower()


def _email_rt_verb_and_display_pct(rule_id: str, change_pct: float) -> tuple[str, float]:
    """Konu için fiil + gösterilecek mutlak yüzde (yuvarlak)."""
    rid = rule_id or ""
    if rid in ("page_disappeared", "news_disappeared"):
        return "düşüş", abs(float(change_pct))
    if rid in ("page_new_entry", "news_new_entry"):
        return "artış", abs(float(change_pct))
    if float(change_pct) < 0:
        return "düşüş", abs(float(change_pct))
    return "artış", float(change_pct)


def _email_pick_primary_alarm(alarms: list[dict[str, Any]]) -> dict[str, Any]:
    """Konu özeti: en büyük mutlak yüzde değişimi olan alarm."""
    if len(alarms) == 1:
        return alarms[0]
    return max(alarms, key=lambda a: abs(float(a.get("change_pct", 0.0))))


def _email_site_alarm_subject(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    """sinemalar.com — +312 kul. · -89 gör. [mweb]"""
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    suffix = f" [{p}]" if p not in ("web", "") else ""
    chips = []
    for a in alarms[:3]:
        cur = int(a.get("current_value", 0))
        prev = int(a.get("previous_value", 0))
        delta = cur - prev
        sign = "+" if delta >= 0 else ""
        metric_short = {"activeUsers": "kul", "screenPageViews": "gör"}.get(str(a.get("metric", "")), "")
        chips.append(f"{sign}{delta} {metric_short}".strip())
    rest = f" +{len(alarms) - 3}" if len(alarms) > 3 else ""
    return f"{short} — {' · '.join(chips)}{rest}{suffix}"


def _email_page_alarm_subject(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    """sinemalar.com — Altın haberi +57 · Dolar +32 [mweb]"""
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    suffix = f" [{p}]" if p not in ("web", "") else ""
    chips = []
    for a in alarms[:10]:
        t = _rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=20)
        c = int(a.get("current_users", 0))
        pv = int(a.get("previous_users", 0))
        rid = str(a.get("rule_id", ""))
        if rid == "page_disappeared":
            chips.append(f"{t} ↓{pv}")
        elif rid == "page_new_entry":
            chips.append(f"{t} ↑{c}")
        else:
            d = c - pv
            chips.append(f"{t} {'+' if d >= 0 else ''}{d}")
    rest = f" +{len(alarms) - 10}" if len(alarms) > 10 else ""
    return f"{short} — {' · '.join(chips)}{rest}{suffix}"


def _sort_news_alarms(alarms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Alarmları öncelik+kullanıcı sayısına göre sırala (yeni girişler önce, en yüksek trafik üstte)."""
    def _key(a):
        rid = str(a.get("rule_id", ""))
        # Yeni giriş > spike > drop > peak_drop > disappeared
        priority = {"news_new_entry": 0, "news_traffic_spike": 1, "news_traffic_drop": 2, "news_peak_drop": 2, "news_disappeared": 3}.get(rid, 9)
        curr = int(a.get("current_users", 0))
        prev = int(a.get("previous_users", 0))
        users = max(curr, prev)
        return (priority, -users)
    return sorted(alarms, key=_key)


def _email_news_alarm_subject(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    """Konu: en yüksek trafikli ilk 10 haber alarmının özeti."""
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    suffix = f" [{p}]" if p not in ("web", "") else ""

    def _alarm_chip(a: dict[str, Any]) -> str:
        title = _rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=22)
        rid = str(a.get("rule_id", ""))
        curr = int(a.get("current_users", 0))
        prev = int(a.get("previous_users", 0))
        if rid == "news_new_entry":
            return f"{title} ↑{curr}"
        if rid == "news_disappeared":
            return f"{title} ↓{prev}"
        if rid == "news_peak_drop":
            peak = int(a.get("peak_users", prev))
            return f"{title} ↓{curr}"
        delta = curr - prev
        sign = "+" if delta >= 0 else ""
        return f"{title} {sign}{delta}"

    # Negatifleri (drop/disappeared/peak_drop) ayır, ikisinin de konuda kesinlikle görünmesini garantile
    neg_alarms = [a for a in alarms if str(a.get("rule_id", "")) in NEGATIVE_ALARM_RULE_IDS]
    pos_alarms = [a for a in alarms if str(a.get("rule_id", "")) not in NEGATIVE_ALARM_RULE_IDS]
    pos_sorted = _sort_news_alarms(pos_alarms)
    neg_sorted = _sort_news_alarms(neg_alarms)
    # Pozitif slot: max 8, Negatif slot: max 7 — toplam 15
    chosen = pos_sorted[:8] + neg_sorted[:7]
    sorted_chosen = _sort_news_alarms(chosen)
    chips = [_alarm_chip(a) for a in sorted_chosen]
    remainder = max(0, len(alarms) - len(chosen))
    rest = f" +{remainder}" if remainder > 0 else ""
    return f"{short} — {' · '.join(chips)}{rest}{suffix}"


def _html_site_alarm_body(
    domain: str,
    profile: str,
    alarms: list[dict[str, Any]],
    *,
    detail_top_html: str = "",
) -> str:
    """Site metrik alarmları — yalnızca sayılar."""
    prof_tag = html.escape(_email_profile_abbr(profile))
    prof_suffix = f" [{prof_tag}]" if prof_tag not in ("web", "") else ""
    cards: list[str] = []
    pre_parts: list[str] = []
    for alarm in alarms:
        metric_key = str(alarm.get("metric", "activeUsers"))
        metric_chip = html.escape(_email_metric_plain_tr(metric_key))
        cur = int(alarm.get("current_value", 0))
        prev = int(alarm.get("previous_value", 0))
        pct = float(alarm.get("change_pct", 0.0))
        is_drop = pct < 0
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        metric_row = _html_email_metric_row(prev, cur, pct, large=True)
        pre_parts.append(f"{metric_chip} {prev:,}→{cur:,} {pct:+.0f}%")
        cards.append(
            f'<div style="margin:8px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<div style="font-size:11px;color:#64748b;margin-bottom:6px;font-weight:700;">{metric_chip}{prof_suffix}</div>'
            f'{metric_row}'
            f'</div>'
        )

    driver_html = ""
    first_alarm = alarms[0] if alarms else {}
    drivers = first_alarm.get("drivers", [])
    if drivers:
        site_delta = first_alarm.get("current_value", 0) - first_alarm.get("previous_value", 0)
        driver_html = _html_driver_analysis_section(drivers, site_delta)

    pre = _preheader(" · ".join(pre_parts[:4]))

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            {pre}
            {_html_email_section_header(domain, profile)}
            {''.join(cards)}
            {driver_html}
            {detail_top_html}
        </div>
        """


def _html_page_alarm_body(
    domain: str,
    profile: str,
    alarms: list[dict[str, Any]],
    *,
    detail_top_html: str = "",
) -> str:
    cards: list[str] = []
    pre_parts: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "")
        title = _rt_alarm_screen_title_one_line(page, max_len=70) or page
        title_e = html.escape(title)
        row_url = _alarm_row_public_url(domain, "page:" + str(page))
        if row_url:
            ru = html.escape(row_url, quote=True)
            title_html = (
                f'<a href="{ru}" style="color:#0f172a;text-decoration:none;border-bottom:1px solid rgba(15,23,42,0.18);">{title_e}</a>'
            )
        else:
            title_html = title_e
        curr = int(alarm.get("current_users", 0))
        prev = int(alarm.get("previous_users", 0))
        pct = float(alarm.get("change_pct", 0.0))
        rid = str(alarm.get("rule_id", ""))
        is_drop = pct < 0 or rid == "page_disappeared"
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        pct_c = "#dc2626" if is_drop else "#16a34a"

        if rid == "page_disappeared":
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:{pct_c};">{prev:,}</span>'
                f'<span style="font-size:16px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{pct_c};">0</span>'
            )
            pre_parts.append(f"{title[:18]} {prev:,}→0")
        elif rid == "page_new_entry":
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:{pct_c};">0</span>'
                f'<span style="font-size:16px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{pct_c};">{curr:,}</span>'
            )
            pre_parts.append(f"{title[:18]} 0→{curr:,}")
        elif prev > 0:
            metric_html = _html_email_metric_row(prev, curr, pct)
            pre_parts.append(f"{title[:18]} {prev:,}→{curr:,} {pct:+.0f}%")
        else:
            metric_html = f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr:,}</span>'
            pre_parts.append(f"{title[:18]} {curr:,}")

        paths_html = ""
        page_paths = alarm.get("page_paths") or []
        if page_paths:
            path_items = "".join(
                f'<div style="font-size:11px;color:#2563eb;font-family:monospace;margin-top:2px;">'
                f'<a href="https://{domain}{html.escape(p)}" style="color:#2563eb;">{html.escape(p)}</a>'
                f'</div>'
                for p in page_paths[:10]
            )
            paths_html = f'<div style="margin-top:4px;">{path_items}</div>'
            if len(page_paths) > 10:
                paths_html += f'<div style="font-size:11px;color:#94a3b8;margin-top:2px;">+{len(page_paths)-10} URL</div>'

        cards.append(
            f'<div style="margin:8px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:14px;font-weight:800;line-height:1.3;">{title_html}</p>'
            f'{paths_html}'
            f'<div style="display:flex;align-items:baseline;gap:4px;flex-wrap:wrap;margin-top:6px;">{metric_html}</div>'
            f'</div>'
        )

    pre = _preheader(" · ".join(pre_parts[:6]))

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            {pre}
            {_html_email_section_header(domain, profile)}
            {''.join(cards)}
            {detail_top_html}
        </div>
        """


def _preheader(text: str) -> str:
    """Email preview alanı için görünmez preheader — telefon/watch lock screen'de ilk görünen metin."""
    t = html.escape(text)
    # Trailing whitespace: email client'ların body'den fazla metin çekmesini engeller
    filler = "&nbsp;" * 80
    return (
        f'<span style="display:none;font-size:1px;color:#fafafa;'
        f'max-height:0;line-height:0;overflow:hidden;mso-hide:all;">'
        f'{t}{filler}</span>'
    )


def _html_news_alarm_body(
    domain: str,
    profile: str,
    alarms: list[dict[str, Any]],
    site_kpi: dict | None = None,
    *,
    detail_top_html: str = "",
) -> str:
    neg_alarms = [a for a in alarms if str(a.get("rule_id", "")) in NEGATIVE_ALARM_RULE_IDS]
    pos_alarms = [a for a in alarms if str(a.get("rule_id", "")) not in NEGATIVE_ALARM_RULE_IDS]
    pos_sorted = _sort_news_alarms(pos_alarms)[:8]
    neg_sorted = _sort_news_alarms(neg_alarms)[:7]
    alarms = _sort_news_alarms(pos_sorted + neg_sorted)
    cards: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "")
        title = _rt_alarm_screen_title_one_line(page, max_len=80)
        title_e = html.escape(title or page)
        row_url = _alarm_row_public_url(domain, "news:" + str(page))
        if row_url:
            ru = html.escape(row_url, quote=True)
            title_html = (
                f'<a href="{ru}" style="color:#0f172a;text-decoration:none;border-bottom:1px solid rgba(15,23,42,0.18);">{title_e}</a>'
            )
        else:
            title_html = title_e
        curr = int(alarm.get("current_users", 0))
        prev = int(alarm.get("previous_users", 0))
        rid = str(alarm.get("rule_id", ""))
        is_drop = rid in ("news_traffic_drop", "news_disappeared", "news_peak_drop")
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        num_c = "#dc2626" if is_drop else "#16a34a"

        if rid == "news_new_entry":
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">0</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{curr:,}</span>'
            )
        elif rid == "news_disappeared":
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{prev:,}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">0</span>'
            )
        elif rid == "news_peak_drop":
            peak = int(alarm.get("peak_users", prev))
            drop_pct = int(alarm.get("drop_pct", 0))
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{peak:,}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{curr:,}</span>'
                f'<span style="font-size:14px;font-weight:800;color:{num_c};margin-left:8px;">−{drop_pct}%</span>'
            )
        else:
            delta = curr - prev
            sign = "+" if delta >= 0 else ""
            pct = float(alarm.get("change_pct", 0.0))
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{prev:,}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr:,}</span>'
                f'<span style="font-size:16px;font-weight:800;color:{num_c};margin-left:8px;">{sign}{delta} ({pct:+.0f}%)</span>'
            )

        cards.append(
            f'<div style="margin:10px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:15px;font-weight:800;line-height:1.3;">{title_html}</p>'
            f'<div style="display:flex;align-items:baseline;gap:4px;flex-wrap:wrap;">{metric_html}</div>'
            f'</div>'
        )

    kpi_html = ""
    kpi = site_kpi or {}
    if kpi.get("current") is not None:
        cur_kpi  = int(kpi.get("current", 0))
        prev_kpi = int(kpi.get("previous", 0))
        pct_kpi  = float(kpi.get("change_pct", 0.0))
        is_drop_kpi = pct_kpi < 0
        kpi_color = "#dc2626" if is_drop_kpi else "#16a34a"
        kpi_bg    = "#fef2f2" if is_drop_kpi else "#f0fdf4"
        kpi_border= "#dc2626" if is_drop_kpi else "#16a34a"
        if prev_kpi > 0:
            kpi_html = (
                f'<div style="margin:0 0 14px;padding:10px 14px;border-radius:8px;'
                f'border-left:4px solid {kpi_border};background:{kpi_bg};">'
                f'<div style="font-size:11px;color:#64748b;margin-bottom:4px;font-weight:700;">site kul</div>'
                f'{_html_email_metric_row(prev_kpi, cur_kpi, pct_kpi)}'
                f'</div>'
            )
        else:
            kpi_html = (
                f'<div style="margin:0 0 14px;padding:8px 14px;border-radius:8px;'
                f'background:#f8fafc;border-left:4px solid #e2e8f0;">'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{cur_kpi:,}</span>'
                f'</div>'
            )

    pre_parts: list[str] = []
    if kpi.get("current"):
        cur_k = int(kpi["current"])
        pct_k = float(kpi.get("change_pct", 0))
        pre_parts.append(f"site {cur_k:,} {pct_k:+.0f}%")
    for a in alarms[:10]:
        title = _rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=18)
        curr  = int(a.get("current_users", 0))
        prev_a = int(a.get("previous_users", 0))
        if title and title != "—":
            pre_parts.append(f"{title}: {prev_a:,}→{curr:,}")
    preheader_str = " · ".join(pre_parts) if pre_parts else f"{len(alarms)} haber"

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            {_preheader(preheader_str)}
            {_html_email_section_header(domain, profile)}
            {kpi_html}
            {''.join(cards)}
            {detail_top_html}
        </div>
        """


def _utc_db_datetime_iso_z(dt: datetime | None) -> str | None:
    """UTC olarak saklanan (çoğunlukla naive) DB zamanını `...Z` ile JSON'a yazar.

    Aksi halde `2026-05-13T07:10:00` tarayıcıda *yerel* saat sanılıp Plotly ekseni kayar.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        u = dt.replace(tzinfo=timezone.utc)
    else:
        u = dt.astimezone(timezone.utc)
    return u.isoformat().replace("+00:00", "Z")


def _realtime_row_dimensions(row: Any, dim_headers: list[str]) -> dict[str, str]:
    """GA4 Realtime satırındaki boyutları isim → değer haritasına çevirir (sıra/API ek boyutları için)."""
    vals = [dv.value for dv in row.dimension_values]
    return {name: (vals[i] if i < len(vals) else "") for i, name in enumerate(dim_headers)}


# ── Alarm eşikleri ────────────────────────────────────────────────────────────
# Yüzdesel düşüş/artış eşikleri (ayarlar sayfasından override edilebilir)
ALARM_RULES: dict[str, dict[str, Any]] = {
    "traffic_drop": {
        "label": "Aktif kullanıcılar",
        "metric": "activeUsers",
        "direction": "drop",
        "threshold_pct": 40,
        # Önceki yarı-pencerede en az bu kadar aktif kullanıcı olmalı. Düşük tutulduğunda
        # gece/düşük trafik anlarındaki küçük dalgalanmalar sürekli alarm üretiyordu.
        "min_baseline": 40,
        "severity": "critical",
    },
    "traffic_spike": {
        "label": "Aktif kullanıcılar",
        "metric": "activeUsers",
        "direction": "spike",
        "threshold_pct": 40,
        "min_baseline": 40,
        "severity": "warning",
    },
    "pageview_drop": {
        "label": "Sayfa görüntülemeleri",
        "metric": "screenPageViews",
        "direction": "drop",
        "threshold_pct": 40,
        "min_baseline": 60,
        "severity": "warning",
    },
}

PAGE_ALARM_RULES: dict[str, dict[str, Any]] = {
    "page_traffic_drop": {
        "label": "Sayfa trafiği düşüşü",
        "direction": "drop",
        "threshold_pct": 45,
        "min_users": 22,
        "severity": "warning",
    },
    "page_traffic_spike": {
        "label": "Sayfa trafiği artışı",
        "direction": "spike",
        "threshold_pct": 65,
        "min_users": 18,
        "severity": "warning",
    },
    "page_disappeared": {
        "label": "Sayfa top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 20,
        "severity": "warning",
    },
    "page_new_entry": {
        "label": "Yeni sayfa top listeye girdi",
        "direction": "new_entry",
        "min_users": 30,
        "severity": "info",
    },
}

# Haberler (unifiedScreenName) — sayfa alarmlarından biraz daha sıkı eşikler (gürültü azaltma)
NEWS_ALARM_RULES: dict[str, dict[str, Any]] = {
    "news_traffic_drop": {
        "label": "Haber trafiği düşüşü",
        "direction": "drop",
        "threshold_pct": 40,
        "min_users": 20,
        "severity": "warning",
    },
    "news_traffic_spike": {
        "label": "Haber trafiği artışı",
        "direction": "spike",
        "threshold_pct": 60,
        "min_users": 20,
        "severity": "warning",
    },
    "news_disappeared": {
        "label": "Haber top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 25,
        "severity": "warning",
    },
    "news_new_entry": {
        "label": "Yeni haber başlığı top listeye girdi",
        "direction": "new_entry",
        "min_users": 40,
        "severity": "info",
    },
    "news_peak_drop": {
        "label": "Haber zirveden düştü",
        "direction": "peak_drop",
        "drop_pct": 55,       # zirveden en az %55 düşüş
        "min_peak_users": 30, # zirvede en az bu kadar kullanıcı olmalı
        "severity": "warning",
    },
}


# Negatif (düşüş yönlü) alarm kural ID'leri — cooldown ve render için ayrı sınıf.
NEGATIVE_ALARM_RULE_IDS: frozenset[str] = frozenset({
    "news_traffic_drop",
    "news_disappeared",
    "news_peak_drop",
    "page_traffic_drop",
    "page_disappeared",
    "app_event_drop",
    "app_event_peak_drop",
})


def _split_alarms_by_sentiment(alarms: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Alarmları (negatif, pozitif) olarak iki gruba böler.

    Cooldown'un negatif alarmları (düşüş, kaybolma, zirveden düşüş) gizlememesi için kullanılır.
    """
    negatives: list[dict[str, Any]] = []
    positives: list[dict[str, Any]] = []
    for a in alarms:
        rid = str(a.get("rule_id", ""))
        if rid in NEGATIVE_ALARM_RULE_IDS:
            negatives.append(a)
        else:
            positives.append(a)
    return negatives, positives


# Bir site+profil mailinde azami pozitif/negatif alarm (konsolide mailde ayrıca batch cap var).
ALARM_EMAIL_TOP_N = 10
REALTIME_DETAIL_TOP_N = 10
REALTIME_BUCKET_TOP_PAGES_N = 6

# 4 saatlik özet maili: doviz web/mweb/android/ios + sinemalar web/mweb (monetizasyon 6 akış)
REALTIME_DIGEST_AREAS: tuple[tuple[str, str], ...] = (
    ("doviz", "web"),
    ("doviz", "mweb"),
    ("doviz", "android"),
    ("doviz", "ios"),
    ("sinemalar", "web"),
    ("sinemalar", "mweb"),
)


def _alarm_user_volumes(alarm: dict[str, Any]) -> tuple[float, float, float]:
    prev = float(
        alarm.get("previous_users")
        or alarm.get("previous_value")
        or alarm.get("previous_count")
        or 0
    )
    curr = float(
        alarm.get("current_users")
        or alarm.get("current_value")
        or alarm.get("current_count")
        or 0
    )
    return prev, curr, abs(curr - prev)


def alarm_worthy_for_email(alarm: dict[str, Any]) -> bool:
    """Düşük hacimli yüzde oynamalarını postadan eler; spike/404/kritik korunur."""
    from backend.config import settings

    rid = str(alarm.get("rule_id") or "")
    sev = str(alarm.get("severity") or "").lower()
    if sev == "critical" or "404" in rid or rid.startswith("rt_404"):
        return True

    min_vol = int(getattr(settings, "ga4_realtime_email_min_users_for_mail", 30))
    min_delta = int(getattr(settings, "ga4_realtime_email_min_abs_user_delta", 12))
    prev, curr, delta = _alarm_user_volumes(alarm)
    peak = max(prev, curr)

    if rid in ("page_traffic_drop", "news_traffic_drop"):
        return peak >= min_vol and delta >= min_delta
    if rid in ("page_disappeared", "news_disappeared"):
        return prev >= min_vol
    if rid == "news_peak_drop":
        try:
            peak_u = float(alarm.get("peak_users") or alarm.get("min_peak_users") or 0)
        except (TypeError, ValueError):
            peak_u = 0.0
        return peak_u >= min_vol
    if rid in ("page_traffic_spike", "news_traffic_spike", "page_new_entry", "news_new_entry"):
        spike_floor = max(18, min_vol - 8)
        return curr >= spike_floor or (prev >= spike_floor and delta >= min_delta)
    if rid.startswith("app_event"):
        return peak >= 15
    return True


def filter_alarms_for_email(alarms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [a for a in alarms if alarm_worthy_for_email(a)]


def _cap_top_n_each_side(alarms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Alarmları artan/düşen olarak ayırıp her gruptan en çok değişeni 10'ar tane döner.

    Her grup change_pct mutlak değerine göre azalan sıralı. Önce pozitifler (en çok artan
    önce), sonra negatifler (en çok düşen önce). Maile gönderilecek listenin uzunluğunu
    sınırlamak için kullanılır.
    """
    negatives, positives = _split_alarms_by_sentiment(alarms)

    def _abs_pct(a: dict[str, Any]) -> float:
        try:
            return abs(float(a.get("change_pct", 0) or 0))
        except (TypeError, ValueError):
            return 0.0

    positives_sorted = sorted(positives, key=_abs_pct, reverse=True)[:ALARM_EMAIL_TOP_N]
    negatives_sorted = sorted(negatives, key=_abs_pct, reverse=True)[:ALARM_EMAIL_TOP_N]
    return positives_sorted + negatives_sorted

# sinemalar.com (www dahil): Realtime yüzde eşikleri — istek üzerine tüm threshold_pct = 50
_SINEMALAR_REALTIME_ALARM_PCT = 35


def _is_sinemalar_site_domain(domain: str | None) -> bool:
    d = (domain or "").strip().lower()
    if not d:
        return False
    if d.startswith("www."):
        d = d[4:]
    return d == "sinemalar.com" or d.endswith(".sinemalar.com")


def _realtime_rules_threshold_pct_for_domain(
    base_rules: dict[str, dict[str, Any]],
    site_domain: str | None,
) -> dict[str, dict[str, Any]]:
    """sinemalar için tüm ``threshold_pct`` = 50; döviz için 30.

    NOT: Mutlak tabanlar (``min_users``/``min_baseline`` vb.) ARTIK ölçeklenmez. Önceden bu
    değerler yüzde oranıyla küçültülüyordu (ör. doviz tabanı 3 → 1), bu da 1→2 kullanıcı gibi
    çok küçük değişikliklerin alarm tetiklemesine ve sürekli mail gelmesine yol açıyordu.
    Tabanlar kuralda tanımlandığı gibi korunur; yalnızca yüzde eşiği domaine göre ayarlanır.
    """
    d = (site_domain or "").strip().lower()
    if d.startswith("www."):
        d = d[4:]

    target_pct = None
    if d == "sinemalar.com" or d.endswith(".sinemalar.com"):
        target_pct = 50.0
    elif d == "doviz.com" or d.endswith(".doviz.com"):
        target_pct = 30.0

    if target_pct is None:
        return base_rules

    out = copy.deepcopy(base_rules)
    for _rid, rule in out.items():
        if "threshold_pct" in rule:
            rule["threshold_pct"] = target_pct
    return out


# Varsayılan pencere boyutu (dakika)
DEFAULT_WINDOW_MINUTES = 10

# DB snapshot trend — gerçek kayıtlar; sentetik interpolasyon yok
REALTIME_TREND_LIMIT_DEFAULT = 288  # ~4.8 saat @ 60 sn poll
REALTIME_TREND_LIMIT_MAX = 480  # ~8 saat @ 60 sn poll
REALTIME_TREND_HOURS_DEFAULT = 24.0  # realtime sayfa 24s sekmesi — yalnızca DB okur
REALTIME_TREND_HOURS_MAX = 48.0
REALTIME_TREND_ROWS_MAX = 1440  # 24s @ ~1 dk örnekleme üst sınırı
REALTIME_HOME_TREND_LIMIT = 120  # ana sayfa mini spark


def _build_client() -> BetaAnalyticsDataClient:
    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def _fetch_realtime_comparison_with_metrics(
    client: BetaAnalyticsDataClient,
    property_id: str,
    window_minutes: int,
    metrics: list[Metric],
    *,
    dimension_filter: FilterExpression | None = None,
) -> dict[str, Any]:
    """İki minuteRange + tek toplam aralık; metrik listesi dışarıdan (yeniden deneme için)."""
    half = 15
    prop = f"properties/{property_id}"

    req_compare = RunRealtimeReportRequest(
        property=prop,
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=half - 1, end_minutes_ago=0),
            MinuteRange(name="previous", start_minutes_ago=2 * half - 1, end_minutes_ago=half),
        ],
        dimension_filter=dimension_filter,
    )

    total_start = min(max(1, window_minutes), 30) - 1
    req_total = RunRealtimeReportRequest(
        property=prop,
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="total", start_minutes_ago=total_start, end_minutes_ago=0),
        ],
        dimension_filter=dimension_filter,
    )

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_c = pool.submit(client.run_realtime_report, req_compare)
        fut_t = pool.submit(client.run_realtime_report, req_total)
        resp_compare = fut_c.result()
        resp_total = fut_t.result()
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in resp_compare.metric_headers]
    dim_headers_cmp = [h.name for h in resp_compare.dimension_headers]
    windows: dict[str, dict[str, float]] = {"current": {}, "previous": {}}

    for row in resp_compare.rows:
        range_name = ""
        if dim_headers_cmp:
            dm = _realtime_row_dimensions(row, dim_headers_cmp)
            for v in dm.values():
                sv = str(v or "").strip().lower()
                if sv in ("current", "previous"):
                    range_name = sv
                    break
        if not range_name:
            for dv in row.dimension_values:
                val = str(dv.value or "").strip().lower()
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
    # Karşılaştırma satırı boş gelse bile toplam penceresi dolu olabiliyor; KPI alanları boş kalmasın.
    for m in metrics:
        name = m.name
        if name in comparison:
            continue
        tv = float(total.get(name, 0) or 0)
        comparison[name] = {
            "current": tv,
            "previous": tv,
            "change_pct": 0.0,
            "direction": "flat",
        }
    if not resp_compare.rows and total:
        logger.warning(
            "GA4 Realtime: karşılaştırma satırı boş, toplam metrik dolu (property=%s); KPI için comparison tamamlandı.",
            property_id,
        )

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


def fetch_realtime_comparison(
    property_id: str,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    *,
    client: BetaAnalyticsDataClient | None = None,
    dimension_filter: FilterExpression | None = None,
) -> dict[str, Any]:
    """İki minuteRange ile Realtime API karşılaştırması + tek aralık toplamı.

    GA Realtime en fazla ~29 dk geriye gider; karşılaştırma için sabit iki **15 dk**
    aralığı kullanılır: ``current`` (son 15 dk) ve ``previous`` (önceki 15 dk).
    ``window_minutes`` yalnızca ikinci istekteki **toplam** (tek minuteRange)
    uzunluğunu ``min(max(1, window_minutes), 30)`` ile sınırlar.

    Firebase / uygulama veri akışlarında ``conversions`` (ve bazen ``eventCount``)
    Realtime toplu istekte INVALID_ARGUMENT üretebilir; bu durumda metrik kümesi
    otomatik daraltılır.
    """
    if client is None:
        client = _build_client()

    pid = _normalize_ga4_property_id(property_id)
    from backend.services.ga4_realtime_quota import (
        assert_property_realtime_allowed,
        note_realtime_quota_error,
    )

    assert_property_realtime_allowed(pid)

    metric_sets: tuple[tuple[str, list[Metric]], ...] = (
        (
            "activeUsers+screenPageViews+eventCount+conversions",
            [
                Metric(name="activeUsers"),
                Metric(name="screenPageViews"),
                Metric(name="eventCount"),
                Metric(name="conversions"),
            ],
        ),
        (
            "activeUsers+screenPageViews+eventCount",
            [
                Metric(name="activeUsers"),
                Metric(name="screenPageViews"),
                Metric(name="eventCount"),
            ],
        ),
        (
            "activeUsers+screenPageViews",
            [
                Metric(name="activeUsers"),
                Metric(name="screenPageViews"),
            ],
        ),
    )

    last_exc: Exception | None = None
    for label, mset in metric_sets:
        try:
            out = _fetch_realtime_comparison_with_metrics(
                client, pid, window_minutes, mset, dimension_filter=dimension_filter
            )
            if label != metric_sets[0][0]:
                logger.warning(
                    "GA4 Realtime karşılaştırma — uygulama/web uyumu için metrik kümesi düşürüldü (%s, property=%s).",
                    label,
                    pid,
                )
            return out
        except Exception as exc:
            last_exc = exc
            logger.debug(
                "Realtime karşılaştırma metrik seti başarısız (%s, property=%s): %s",
                label,
                pid,
                exc,
            )
    if last_exc is not None:
        note_realtime_quota_error(pid, last_exc)
        raise last_exc
    raise RuntimeError("fetch_realtime_comparison: no metric set")


def fetch_realtime_top_pages(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 10,
    dimension: str = "unifiedScreenName",
    sort_by: str = "activeUsers",
    compare_previous: bool = False,
    include_page_path: bool = False,  # GA4 Realtime şemasında pagePath geçerli değil
    metrics: list[str] | None = None,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API ile son N dakikadaki top sayfaları çeker.

    compare_previous: True ise önceki pencereyle (window_minutes kadar öncesi) karşılaştırma metriklerini de döner.
    sort_by: "activeUsers" veya "screenPageViews" — sıralama kriteri.
    include_page_path: True ise pagePath ikinci dimension olarak eklenir (Realtime API'de desteklenmez — False bırakın).
    metrics: Özel metrik listesi (örn. yalnızca activeUsers); None ise varsayılan ikili.
    """
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    from backend.services.ga4_realtime_quota import assert_property_realtime_allowed

    assert_property_realtime_allowed(property_id)
    # GA4 Standard property realtime: max 29 dakika geri.
    # compare_previous=True iken iki pencere arka arkaya: 2*w-1 ≤ 29 olmalı → w ≤ 15.
    max_w = 15 if compare_previous else 29
    w = max(1, min(window_minutes, max_w))

    minute_ranges = [
        MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
    ]
    if compare_previous:
        minute_ranges.append(
            MinuteRange(name="previous", start_minutes_ago=2 * w - 1, end_minutes_ago=w)
        )

    # pagePath sadece web profillerinde anlamlı; app/unifiedScreenName ile birlikte kullanılabilir
    dims = [Dimension(name=dimension)]
    if include_page_path and dimension != "pagePath":
        dims.append(Dimension(name="pagePath"))

    metric_names_req = metrics or ["activeUsers", "screenPageViews"]
    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=dims,
        metrics=[Metric(name=m) for m in metric_names_req],
        minute_ranges=minute_ranges,
    )

    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]

    # Merging logic for comparison
    temp_map: dict[str, dict[str, Any]] = {}

    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        page_val = str(dm.get(dimension, "") or "").strip()

        # MinuteRange adını bul (GA4 bunu bazen boyuta bazen meta veriye koyar)
        range_name = "current"
        for k, v in dm.items():
            if str(v).lower() in ("current", "previous"):
                range_name = str(v).lower()
                break

        if not page_val or page_val.lower() in ("current", "previous"):
            # Sayfa adı/path'i başka bir kolona kaymış olabilir (GA4 Realtime quirk)
            for k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    page_val = vs
                    break

        if not page_val:
            continue
        if _is_realtime_noise_title(page_val):
            continue

        # pagePath ikinci dimension'sa topla
        path_val = ""
        if include_page_path and dimension != "pagePath":
            path_val = str(dm.get("pagePath", "") or "").strip()
            if path_val.lower() in ("current", "previous", ""):
                path_val = ""

        if page_val not in temp_map:
            temp_map[page_val] = {
                "page": page_val,
                "page_paths": [],  # URL listesi
                "activeUsers": 0.0,
                "screenPageViews": 0.0,
                "activeUsers_previous": 0.0,
                "screenPageViews_previous": 0.0,
            }

        entry = temp_map[page_val]

        # URL'yi ekle (tekrar etmesin)
        if path_val and path_val not in entry["page_paths"]:
            entry["page_paths"].append(path_val)

        suffix = "_previous" if range_name == "previous" else ""

        for i, mv in enumerate(row.metric_values):
            raw_mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            mname = raw_mname
            if mname in ("views", "eventCount"):
                mname = "screenPageViews"
            try:
                val = float(mv.value)
                key = mname + suffix
                # Aynı ekran adına birden fazla satır gelirse toplama — zirve al (çift sayım önlenir).
                prev_val = float(entry.get(key, 0.0) or 0.0)
                entry[key] = max(prev_val, val)
            except (ValueError, TypeError):
                pass

    pages = list(temp_map.values())
    pages.sort(key=lambda p: p.get(sort_by, 0), reverse=True)

    return {
        "property_id": property_id,
        "window_minutes": w,
        "pages": pages[:limit],
        "total_pages": len(pages),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
        "comparison_enabled": compare_previous,
    }


def _news_screen_exclude_prefixes_loaded() -> tuple[str, ...]:
    from backend.services.realtime_news_paths import news_screen_exclude_prefixes

    return news_screen_exclude_prefixes()


def _screen_unified_news_candidate(name: str, *, site_domain: str = "") -> bool:
    from backend.services.realtime_news_paths import unified_screen_news_candidate

    return unified_screen_news_candidate(name, site_domain=site_domain)


def _screen_unified_news_article(name: str, *, site_domain: str = "") -> bool:
    from backend.services.realtime_news_paths import unified_screen_news_article_title

    return unified_screen_news_article_title(name, site_domain=site_domain)


def _news_row_link(site_domain: str, unified: str) -> str:
    from backend.services.realtime_news_paths import realtime_news_page_link

    return realtime_news_page_link(unified, site_domain=site_domain)


def fetch_realtime_top_news_pages(
    property_id: str,
    *,
    site_domain: str = "",
    profile: str = "web",
    window_minutes: int = 30,
    limit: int = 12,
    sort_by: str = "activeUsers",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime «Haberler»: web/mweb path; ios/android unifiedScreenName (makale başlığı)."""
    import re

    from backend.services.realtime_news_paths import (
        is_news_detail_path,
        is_realtime_news_path,
        realtime_news_page_link,
        _normalize_news_tab_title,
    )

    profile_key = (profile or "web").strip().lower()
    _dynamic_screen = re.compile(r".*[:\-_]\d{3,}$")

    fetch_n = min(250, max(80, int(limit) * 25))
    breakdown_parts: list[str] = []
    bases: list[tuple[str, dict[str, Any]]] = []

    if profile_key in ("android", "ios"):
        try:
            candidate = fetch_realtime_top_pages_pick_best_screen_dimension(
                property_id,
                window_minutes=window_minutes,
                limit=fetch_n,
                sort_by=sort_by,
                client=client,
            )
            dim = str(candidate.get("breakdown") or "unifiedScreenName")
            bases.append((dim, candidate))
            breakdown_parts.append(dim)
        except Exception as exc:
            logger.debug("Realtime top-news app screen dim failed [%s]: %s", profile_key, exc)
    else:
        try:
            candidate = fetch_realtime_top_pages(
                property_id,
                window_minutes=window_minutes,
                limit=fetch_n,
                sort_by=sort_by,
                compare_previous=False,
                dimension="unifiedScreenName",
                include_page_path=False,
                client=client,
            )
            if candidate.get("pages"):
                bases.append(("unifiedScreenName", candidate))
                breakdown_parts.append("unifiedScreenName")
        except Exception as exc:
            logger.debug("Realtime top-news unifiedScreenName failed: %s", exc)

    if not bases:
        base = fetch_realtime_top_pages(
            property_id,
            window_minutes=window_minutes,
            limit=fetch_n,
            sort_by=sort_by,
            compare_previous=False,
            dimension="unifiedScreenName",
            include_page_path=False,
            client=client,
        )
        bases = [("unifiedScreenName", base)]
        breakdown_parts = ["unifiedScreenName"]

    merged: dict[str, dict[str, Any]] = {}
    window_minutes_out = window_minutes
    fetched_at = datetime.now(timezone.utc).isoformat()
    api_ms = 0

    def _merge_key(row: dict[str, Any]) -> str:
        link = (row.get("link_url") or "").strip().lower()
        if link:
            return f"url:{link}"
        pg = _normalize_news_tab_title(str(row.get("page") or "")).lower()
        return f"page:{pg}"

    for dim_used, base in bases:
        window_minutes_out = int(base.get("window_minutes") or window_minutes_out)
        fetched_at = base.get("fetched_at") or fetched_at
        api_ms = max(api_ms, int(base.get("api_ms") or 0))

        for p in base.get("pages") or []:
            key = str(p.get("page") or "").strip()
            if not key:
                continue
            if profile_key in ("android", "ios") and _dynamic_screen.match(key):
                continue
            if dim_used == "pagePath":
                if not is_realtime_news_path(key, site_domain=site_domain):
                    continue
                link = realtime_news_page_link(key, site_domain=site_domain)
                page_path = key if key.startswith("/") else ""
            else:
                if not _screen_unified_news_article(key, site_domain=site_domain):
                    continue
                link = realtime_news_page_link(key, site_domain=site_domain)
                page_path = key if key.startswith("/") else ""

            display_title = _normalize_news_tab_title(key) if not key.startswith("/") else key
            row = {
                "page": display_title,
                "page_path": page_path,
                "activeUsers": float(p.get("activeUsers") or 0),
                "screenPageViews": float(p.get("screenPageViews") or 0),
                "activeUsers_previous": None,
                "screenPageViews_previous": None,
                "link_url": link,
                "_is_detail": bool(
                    key.startswith("/") and is_news_detail_path(key)
                ),
            }
            mk = _merge_key(row)
            prev = merged.get(mk)
            if prev is None:
                merged[mk] = row
            else:
                for field in ("activeUsers", "screenPageViews"):
                    prev[field] = max(
                        float(prev.get(field) or 0),
                        float(row.get(field) or 0),
                    )
                if float(row.get(sort_by) or 0) > float(prev.get(sort_by) or 0):
                    prev["page"] = row["page"]
                    prev["link_url"] = row.get("link_url") or prev.get("link_url")

    out = sorted(
        merged.values(),
        key=lambda r: -float(r.get(sort_by) or 0),
    )
    for r in out:
        r.pop("_is_detail", None)
    cap = max(1, min(int(limit), 250))
    out = out[:cap]

    breakdown = "+".join(breakdown_parts) + "+news_path_rules" if breakdown_parts else "news_path_rules"

    return {
        "property_id": property_id,
        "window_minutes": window_minutes_out,
        "pages": out,
        "total_pages": len(out),
        "fetched_at": fetched_at,
        "api_ms": api_ms,
        "breakdown": breakdown,
        "comparison_enabled": False,
        "metric_scope": "ga4_realtime_30m",
    }


def _realtime_screen_label_quality(pages: list[dict[str, Any]]) -> tuple[int, int]:
    """(anlamlı etiketli satır, toplam). GA bazen '(not set)' / '(other)' döner — bunlar zayıf sayılır."""
    bare = frozenset(
        {
            "",
            "(not set)",
            "(other)",
            "not set",
            "(data not available)",
            "(blank)",
        },
    )
    total = len(pages)

    def _ok(page: str | None) -> bool:
        p = (page or "").strip().lower()
        return bool(p) and p not in bare

    labeled = sum(1 for p in pages if _ok(p.get("page")))
    return labeled, total


def _realtime_pages_metric_total(pages: list[dict[str, Any]]) -> float:
    """Toplam aktif kullanıcı + görüntüleme (boş metrik tespiti için)."""
    total = 0.0
    for p in pages:
        total += float(p.get("activeUsers") or 0) + float(p.get("screenPageViews") or 0)
    return total


def _realtime_pages_have_metrics(pages: list[dict[str, Any]]) -> bool:
    return _realtime_pages_metric_total(pages) > 0


def _realtime_pages_have_active_users(pages: list[dict[str, Any]]) -> bool:
    return any(float(p.get("activeUsers") or 0) > 0 for p in pages)


def _merge_realtime_page_active_users(
    pages: list[dict[str, Any]],
    au_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """activeUsers-only sorgudan gelen değerleri ekran satırlarına birleştirir."""
    au_map: dict[str, float] = {}
    for row in au_rows:
        key = str(row.get("page") or "").strip()
        if key:
            au_map[key] = max(au_map.get(key, 0.0), float(row.get("activeUsers") or 0))
    if not au_map:
        return pages
    out: list[dict[str, Any]] = []
    for p in pages:
        merged = dict(p)
        key = str(p.get("page") or "").strip()
        if key in au_map:
            merged["activeUsers"] = au_map[key]
        out.append(merged)
    return out


# Firebase / Android stream bazen unifiedScreenName boş kalır; Realtime şemasında denenecek sıra.
_REALTIME_APP_SCREEN_DIMENSIONS: tuple[str, ...] = (
    "unifiedScreenName",
    "screenName",
    "unifiedScreenClass",
    "pageTitle",
)


def fetch_realtime_top_pages_pick_best_screen_dimension(
    property_id: str,
    *,
    window_minutes: int,
    limit: int,
    sort_by: str,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Mobil stream için en anlamlı ekran boyutunu seçer (tek boyutta takılı kalmayı önler)."""
    if client is None:
        client = _build_client()
    best: dict[str, Any] | None = None
    best_key: tuple[int, int, int] = (-1, -1, -1)
    last_exc: Exception | None = None
    for dim in _REALTIME_APP_SCREEN_DIMENSIONS:
        try:
            res = fetch_realtime_top_pages(
                property_id,
                window_minutes=window_minutes,
                limit=limit,
                sort_by=sort_by,
                dimension=dim,
                include_page_path=False,   # app profillerde pagePath geçersiz
                client=client,
            )
        except Exception as exc:
            last_exc = exc
            logger.debug(
                "Realtime top pages dimension=%s property=%s: %s",
                dim,
                property_id,
                exc,
            )
            continue
        pages = res.get("pages") or []
        labeled, total = _realtime_screen_label_quality(pages)
        metrics_total = int(_realtime_pages_metric_total(pages))
        key = (labeled, metrics_total, total)
        if key > best_key:
            best_key = key
            best = res
            best["breakdown"] = dim
        if total > 0 and labeled >= 3 and labeled / total >= 0.45 and metrics_total > 0:
            res["breakdown"] = dim
            return res
    if best is not None:
        return best
    if last_exc is not None:
        raise last_exc
    return fetch_realtime_top_pages(
        property_id,
        window_minutes=window_minutes,
        limit=limit,
        sort_by=sort_by,
        dimension="unifiedScreenName",
        include_page_path=False,
        client=client,
    )


def fetch_realtime_top_event_names(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 10,
    sort_by: str = "activeUsers",
    compare_previous: bool = False,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Android/iOS akışlarında unifiedScreenName boşsa: eventName kırılımı.

    Dönüş şekli fetch_realtime_top_pages ile uyumlu: screenPageViews alanına eventCount yazılır.
    """
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    # GA4 Standard property realtime: max 29 dakika geri.
    # compare_previous=True iken iki pencere arka arkaya: 2*w-1 ≤ 29 olmalı → w ≤ 15.
    max_w = 15 if compare_previous else 29
    w = max(1, min(window_minutes, max_w))
    fetch_cap = min(250, max(limit * 8, 40))

    minute_ranges = [
        MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
    ]
    if compare_previous:
        minute_ranges.append(
            MinuteRange(name="previous", start_minutes_ago=2 * w - 1, end_minutes_ago=w)
        )

    # GA4 Realtime API: eventName + activeUsers + eventCount birlikte çalışmıyor ("cannot be queried together").
    # Önce eventCount ile dene; başarısız olursa activeUsers fallback.
    t0 = time.monotonic()
    try:
        request = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            minute_ranges=minute_ranges,
            limit=fetch_cap,
        )
        response = client.run_realtime_report(request)
    except Exception:
        request = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="activeUsers")],
            minute_ranges=minute_ranges,
            limit=fetch_cap,
        )
        response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]

    temp_map: dict[str, dict[str, Any]] = {}

    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        name = str(dm.get("eventName", "") or "").strip()
        
        range_name = "current"
        for k, v in dm.items():
            if str(v).lower() in ("current", "previous"):
                range_name = str(v).lower()
                break

        if not name or name.lower() in ("current", "previous"):
            for k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    name = vs
                    break
        if not name:
            continue

        if name not in temp_map:
            temp_map[name] = {
                "page": name,
                "activeUsers": 0.0,
                "screenPageViews": 0.0,
                "activeUsers_previous": 0.0,
                "screenPageViews_previous": 0.0,
            }
        
        entry = temp_map[name]
        suffix = "_previous" if range_name == "previous" else ""

        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            mapped_name = "screenPageViews" if mname == "eventCount" else mname
            try:
                val = float(mv.value)
                entry[mapped_name + suffix] = entry.get(mapped_name + suffix, 0.0) + val
            except (ValueError, TypeError):
                pass

    pages = list(temp_map.values())
    sort_key = "screenPageViews" if sort_by == "screenPageViews" else "activeUsers"
    pages.sort(key=lambda p: p.get(sort_key, 0), reverse=True)

    return {
        "property_id": property_id,
        "window_minutes": w,
        "pages": pages[:limit],
        "total_pages": len(pages),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
        "breakdown": "eventName",
        "comparison_enabled": compare_previous,
    }


def fetch_realtime_top_pages_with_app_fallback(
    property_id: str,
    *,
    profile: str,
    window_minutes: int = 30,
    limit: int = 10,
    sort_by: str = "activeUsers",
    compare_previous: bool = False,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Mobil: en iyi ekran boyutu; metrik yoksa tek metrik yeniden dene. eventName burada kullanılmaz."""
    if profile not in ("android", "ios"):
        base = fetch_realtime_top_pages(
            property_id,
            window_minutes=window_minutes,
            limit=limit,
            sort_by=sort_by,
            compare_previous=compare_previous,
            include_page_path=False,  # pagePath dim. bazı property'lerde API hatasına neden oluyor
            client=client,
        )
        base["breakdown"] = "unifiedScreenName"
        return base

    # Mobil için comparison desteği şu an pick_best içinde yoksa bile 
    # varsayılan unifiedScreenName üzerinden yürütüyoruz.
    base = fetch_realtime_top_pages_pick_best_screen_dimension(
        property_id,
        window_minutes=window_minutes,
        limit=limit,
        sort_by=sort_by,
        # pick_best şu an comparison desteklemiyorsa standarda dönecek
        client=client,
    )
    # Eğer comparison istendiyse ve base'de yoksa, zorla fetch et
    if compare_previous and not base.get("comparison_enabled"):
        dim = base.get("breakdown") or "unifiedScreenName"
        base = fetch_realtime_top_pages(
            property_id,
            window_minutes=window_minutes,
            limit=limit,
            sort_by=sort_by,
            dimension=dim,
            compare_previous=True,
            include_page_path=False,  # app property'lerde pagePath geçersiz
            client=client,
        )

    pages = base.get("pages") or []
    no_metrics = bool(pages) and not _realtime_pages_have_metrics(pages)

    if no_metrics:
        dim = base.get("breakdown") or "unifiedScreenName"
        for metric_set in (["activeUsers", "screenPageViews"], ["activeUsers"], ["screenPageViews"]):
            try:
                retry = fetch_realtime_top_pages(
                    property_id,
                    window_minutes=window_minutes,
                    limit=limit,
                    sort_by=sort_by,
                    dimension=dim,
                    compare_previous=False,
                    include_page_path=False,
                    metrics=metric_set,
                    client=client,
                )
                retry_pages = retry.get("pages") or []
                if _realtime_pages_have_metrics(retry_pages):
                    retry["breakdown"] = dim
                    retry["metrics_retry"] = metric_set
                    return retry
            except Exception as exc:
                logger.debug(
                    "Realtime app metrics retry %s [%s / %s]: %s",
                    metric_set,
                    property_id,
                    profile,
                    exc,
                )

    # Görüntüleme var ama aktif kullanıcı 0: Android'de ikili metrik sorgusu AU döndürmeyebilir.
    if pages and _realtime_pages_have_metrics(pages) and not _realtime_pages_have_active_users(pages):
        dim = base.get("breakdown") or "unifiedScreenName"
        try:
            au_only = fetch_realtime_top_pages(
                property_id,
                window_minutes=window_minutes,
                limit=max(limit, len(pages)),
                sort_by="activeUsers",
                dimension=dim,
                compare_previous=False,
                include_page_path=False,
                metrics=["activeUsers"],
                client=client,
            )
            au_pages = au_only.get("pages") or []
            if _realtime_pages_have_active_users(au_pages):
                merged_pages = _merge_realtime_page_active_users(pages, au_pages)
                base = dict(base)
                base["pages"] = merged_pages[:limit]
                base["active_users_merged"] = True
                pages = merged_pages
        except Exception as exc:
            logger.debug(
                "Realtime app activeUsers merge [%s / %s]: %s",
                property_id,
                profile,
                exc,
            )

    # Ekran listesine eventName karıştırma — etkinlikler /top-events endpoint'inde.
    return base


def fetch_realtime_top_events_fallback_active_users(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 200,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any] | None:
    """Bazı Android stream'lerde eventCount kırılımı boş döner; activeUsers ile etkinlik sırası dene."""
    if client is None:
        client = _build_client()
    property_id = _normalize_ga4_property_id(property_id)
    # GA4 Standard property realtime: max 29 dakika geri.
    w = max(1, min(window_minutes, 29))
    fetch_limit = max(1, min(limit, 250))
    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="activeUsers")],
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)],
        limit=fetch_limit,
    )
    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    dim_headers = [h.name for h in response.dimension_headers]
    metric_names = [m.name for m in response.metric_headers]
    events: list[dict[str, Any]] = []
    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        en = str(dm.get("eventName", "") or "").strip()
        if not en:
            for _k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    en = vs
                    break
        if not en:
            continue
        au = 0.0
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            if mname == "activeUsers":
                try:
                    au = float(mv.value)
                except (ValueError, TypeError):
                    au = 0.0
                break
        events.append({"eventName": en, "eventCount": au})
    events.sort(key=lambda e: e["eventCount"], reverse=True)
    if not events:
        return None
    tot = sum(e["eventCount"] for e in events)
    return {
        "property_id": property_id,
        "window_minutes": w,
        "events": events,
        "total_event_count": tot,
        "truncated": len(response.rows) >= fetch_limit,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
        "count_basis": "active_users",
    }


def fetch_realtime_top_events(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 200,
    compare_previous: bool = True,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API: etkinlik adına göre eventCount — mobil uygulama kartları için."""
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    # GA4 Standard property realtime: max 29 dakika geri.
    # compare_previous=True iken iki pencere arka arkaya: 2*w-1 ≤ 29 olmalı → w ≤ 15.
    max_w = 15 if compare_previous else 29
    w = max(1, min(window_minutes, max_w))
    fetch_limit = max(1, min(limit, 250))

    total_event_count = 0.0
    try:
        req_total = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            metrics=[Metric(name="eventCount")],
            minute_ranges=[
                MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
            ],
        )
        resp_total = client.run_realtime_report(req_total)
        if resp_total.rows:
            for mv in resp_total.rows[0].metric_values:
                try:
                    total_event_count += float(mv.value)
                except (ValueError, TypeError):
                    pass
    except Exception as exc:
        logger.debug(
            "Realtime toplam eventCount (metrics-only) alınamadı [%s]: %s",
            property_id,
            exc,
        )

    minute_ranges = [MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0)]
    if compare_previous:
        minute_ranges.append(MinuteRange(name="previous", start_minutes_ago=2 * w - 1, end_minutes_ago=w))

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        minute_ranges=minute_ranges,
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
        limit=fetch_limit,
    )

    t0 = time.monotonic()
    try:
        response = client.run_realtime_report(request)
    except Exception:
        # Fallback: sıralama olmadan dene
        request = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            minute_ranges=minute_ranges,
            limit=fetch_limit,
        )
        response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]

    # current/previous merge
    temp_map: dict[str, dict[str, Any]] = {}
    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        event_name = str(dm.get("eventName", "") or "").strip()
        # range adını tespit et
        range_name = "current"
        for k, v in dm.items():
            if str(v).lower() in ("current", "previous"):
                range_name = str(v).lower()
                break
        if not event_name or event_name.lower() in ("current", "previous"):
            for k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    event_name = vs
                    break
        if not event_name:
            continue
        if event_name not in temp_map:
            temp_map[event_name] = {"eventName": event_name, "eventCount": 0.0, "eventCount_previous": 0.0}
        ec = 0.0
        for i, mv in enumerate(row.metric_values):
            try:
                ec = float(mv.value)
            except (ValueError, TypeError):
                pass
        if range_name == "previous":
            temp_map[event_name]["eventCount_previous"] += ec
        else:
            temp_map[event_name]["eventCount"] += ec

    events: list[dict[str, Any]] = sorted(temp_map.values(), key=lambda e: e["eventCount"], reverse=True)

    events.sort(key=lambda e: e["eventCount"], reverse=True)

    needs_ev_fallback = (
        not events
        or not any((e.get("eventName") or "").strip() for e in events)
        or not any((e.get("eventCount") or 0) > 0 for e in events)
    )
    if needs_ev_fallback:
        try:
            alt_ev = fetch_realtime_top_events_fallback_active_users(
                property_id, window_minutes=w, limit=limit, client=client
            )
            if alt_ev and alt_ev.get("events"):
                return alt_ev
        except Exception as exc:
            logger.debug(
                "Realtime top-events activeUsers fallback [%s]: %s",
                property_id,
                exc,
            )

    if total_event_count <= 0:
        total_event_count = sum(e["eventCount"] for e in events)

    truncated = len(response.rows) >= fetch_limit

    return {
        "property_id": property_id,
        "window_minutes": w,
        "events": events,
        "total_event_count": total_event_count,
        "truncated": truncated,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
        "count_basis": "event_count",
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


def _is_realtime_noise_title(page: str | None) -> bool:
    p = str(page or "").strip().lower()
    return p in ("", "—", "(other)", "(not set)", "(blank)", "not set")


def evaluate_alarms(
    comparison: dict[str, dict[str, Any]],
    *,
    rules: dict[str, dict[str, Any]] | None = None,
    site_domain: str | None = None,
) -> list[dict[str, Any]]:
    """Karşılaştırma sonuçlarına alarm kurallarını uygular.

    Returns list of triggered alarms: [{rule_id, label, metric, ...}, ...]
    """
    base = rules if rules is not None else ALARM_RULES
    rules_eff = _realtime_rules_threshold_pct_for_domain(base, site_domain)

    triggered: list[dict[str, Any]] = []

    for rule_id, rule in rules_eff.items():
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
                    f"{metric_name} {prev_val:.0f} → {cur_val:.0f} ({change_pct:+.1f}%)"
                ),
            })

    return triggered


def _build_traffic_drivers_from_page_comparison(
    pages: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> dict[str, list[dict[str, Any]]]:
    """GA4 compare_previous sayfa listesinden artış/düşüş sürücülerini üretir."""
    if not pages:
        return {"increase": [], "decrease": []}

    candidates: list[dict[str, Any]] = []
    for page in pages:
        path = str(page.get("page") or "")
        if _is_realtime_noise_title(path):
            continue
        c = float(page.get("activeUsers") or 0)
        p = float(page.get("activeUsers_previous") or 0)
        diff = c - p
        if diff == 0:
            continue
        candidates.append({
            "page": path,
            "delta": diff,
            "current": c,
            "previous": p,
        })

    if not candidates:
        return {"increase": [], "decrease": []}

    total_pos = sum(d["delta"] for d in candidates if d["delta"] > 0) or 1.0
    total_neg_abs = abs(sum(d["delta"] for d in candidates if d["delta"] < 0)) or 1.0
    inc: list[dict[str, Any]] = []
    dec: list[dict[str, Any]] = []
    for d in candidates:
        if d["delta"] > 0:
            d["contribution_pct"] = (d["delta"] / total_pos) * 100
            inc.append(d)
        elif d["delta"] < 0:
            d["contribution_pct"] = (abs(d["delta"]) / total_neg_abs) * 100
            dec.append(d)

    inc.sort(key=lambda x: x["delta"], reverse=True)
    dec.sort(key=lambda x: x["delta"])
    return {"increase": inc[:limit], "decrease": dec[:limit]}


def _analyze_traffic_drivers(db: Session, site_id: int, profile: str, site_delta: float) -> dict[str, list[dict[str, Any]]]:
    """DB snapshot yedek yolu — canlı GA4 compare_previous kullanılamazsa devreye girer."""
    try:
        if abs(site_delta) < 5:
            return {"increase": [], "decrease": []}

        from backend.models import RealtimePageSnapshot
        from sqlalchemy import desc

        distinct_times = (
            db.query(RealtimePageSnapshot.collected_at)
            .filter(
                RealtimePageSnapshot.site_id == site_id,
                RealtimePageSnapshot.profile == profile,
            )
            .order_by(desc(RealtimePageSnapshot.collected_at))
            .distinct()
            .limit(2)
            .all()
        )

        if len(distinct_times) < 2:
            return {"increase": [], "decrease": []}

        curr_time, prev_time = distinct_times[0][0], distinct_times[1][0]

        def _rows_to_map(ts: Any) -> dict[str, float]:
            rows = (
                db.query(RealtimePageSnapshot)
                .filter(
                    RealtimePageSnapshot.site_id == site_id,
                    RealtimePageSnapshot.profile == profile,
                    RealtimePageSnapshot.collected_at == ts,
                )
                .all()
            )
            return {row.page_path: row.active_users for row in rows}

        curr_map = _rows_to_map(curr_time)
        prev_map = _rows_to_map(prev_time)

        if not curr_map or not prev_map:
            return {"increase": [], "decrease": []}

        raw_pages = [
            {
                "page": path,
                "activeUsers": curr_map.get(path, 0),
                "activeUsers_previous": prev_map.get(path, 0),
            }
            for path in set(curr_map) | set(prev_map)
        ]
        return _build_traffic_drivers_from_page_comparison(raw_pages)

    except Exception:
        logger.exception("_analyze_traffic_drivers hatası (site_id=%s)", site_id)
        return {"increase": [], "decrease": []}


def fetch_traffic_drivers(db: Session, site_id: int, profile: str) -> dict[str, Any]:
    """Değişim sekmesi: site toplamı + sayfa katkıları (GA4 non-overlapping pencereler)."""
    from backend.models import RealtimeSnapshot
    from sqlalchemy import desc

    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        return {"error": "site_not_found"}

    curr_snap = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
        .order_by(desc(RealtimeSnapshot.collected_at))
        .first()
    )
    if curr_snap is None:
        return {
            "site_delta": 0,
            "drivers": [],
            "drivers_increase": [],
            "drivers_decrease": [],
            "has_data": False,
        }

    # Aynı snapshot içindeki current vs previous — kayan pencere farkı değil
    site_delta = curr_snap.active_users_current - curr_snap.active_users_previous
    drivers = {"increase": [], "decrease": []}
    driver_source = "none"

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")

    if property_id:
        try:
            result = fetch_realtime_top_pages_with_app_fallback(
                property_id,
                profile=profile,
                window_minutes=15,
                limit=100,
                sort_by="activeUsers",
                compare_previous=True,
            )
            pages = result.get("pages") or []
            if result.get("comparison_enabled") and pages:
                save_page_snapshots(db, site.id, profile, pages)
                drivers = _build_traffic_drivers_from_page_comparison(pages)
                if drivers["increase"] or drivers["decrease"]:
                    driver_source = "live_api"
        except Exception as exc:
            from backend.services.ga4_realtime_quota import (
                Ga4RealtimeQuotaPausedError,
                note_realtime_quota_error,
            )

            if isinstance(exc, Ga4RealtimeQuotaPausedError):
                logger.debug("Traffic drivers atlandı (kota) [%s/%s]", site.domain, profile)
            elif note_realtime_quota_error(
                property_id or "",
                exc,
                domain=str(site.domain or ""),
                logger=logger,
            ):
                pass
            else:
                logger.warning(
                    "Traffic drivers canlı API hatası [%s/%s]: %s",
                    site.domain,
                    profile,
                    exc,
                )

    if not drivers["increase"] and not drivers["decrease"]:
        drivers = _analyze_traffic_drivers(db, site_id, profile, site_delta)
        if drivers["increase"] or drivers["decrease"]:
            driver_source = "db_snapshots"

    return {
        "has_data": True,
        "site_delta": site_delta,
        "current_total": curr_snap.active_users_current,
        "previous_total": curr_snap.active_users_previous,
        "collected_at": curr_snap.collected_at.isoformat() if curr_snap.collected_at else None,
        "driver_source": driver_source,
        "drivers": (drivers["decrease"] if site_delta < 0 else drivers["increase"]),
        "drivers_increase": drivers["increase"],
        "drivers_decrease": drivers["decrease"],
    }

def _html_driver_analysis_section(drivers: list[dict[str, Any]], site_delta: float) -> str:
    """Trafik değişim analizi için HTML bölümü üretir."""
    if not drivers:
        return ""

    is_drop = site_delta < 0
    title = "Düşüşün Kaynağı" if is_drop else "Artışın Kaynağı"
    header_color = "#7f1d1d" if is_drop else "#14532d"
    header_bg = "#fef2f2" if is_drop else "#f0fdf4"
    row_color = "#b91c1c" if is_drop else "#15803d"

    rows = []
    for d in drivers:
        path = d["page"]
        clean_title = _rt_alarm_screen_title_one_line(path, max_len=55)
        pct = d["contribution_pct"]
        # %100 üzerini gösterme (karşı yönde hareket eden sayfalar nedeniyle olabilir)
        pct_display = f"%{min(abs(pct), 999):.0f}"
        bar_width = min(abs(pct), 100)
        bar_color = "#fca5a5" if is_drop else "#86efac"

        rows.append(f"""
            <tr>
                <td style="padding:9px 0 5px;font-size:13px;color:#1e293b;vertical-align:top;">
                    {html.escape(clean_title)}
                    <div style="margin-top:3px;height:4px;border-radius:2px;background:#e2e8f0;width:100%;">
                        <div style="height:4px;border-radius:2px;background:{bar_color};width:{bar_width:.0f}%;"></div>
                    </div>
                </td>
                <td style="padding:9px 0 5px;text-align:right;font-size:13px;font-weight:700;color:{row_color};white-space:nowrap;vertical-align:top;padding-left:12px;">
                    {d['delta']:+.0f} <span style="font-weight:400;font-size:11px;color:#64748b;">({pct_display})</span>
                </td>
            </tr>
        """)

    return f"""
        <div style="margin-top:20px;border-radius:10px;overflow:hidden;border:1px solid #e2e8f0;">
            <div style="padding:10px 14px;background:{header_bg};border-bottom:1px solid #e2e8f0;">
                <span style="font-size:12px;font-weight:700;color:{header_color};text-transform:uppercase;letter-spacing:.04em;">
                    {title}
                </span>
                <span style="font-size:11px;color:#64748b;margin-left:8px;">Toplam değişim: {site_delta:+.0f} kullanıcı</span>
            </div>
            <div style="padding:4px 14px 10px;background:#fff;">
                <table style="width:100%;border-collapse:collapse;">
                    {''.join(rows)}
                </table>
            </div>
        </div>
    """


def _domain_shows_web_mweb_top_detail(domain: str) -> bool:
    d = (domain or "").lower()
    return "doviz" in d or "sinemalar" in d


def get_web_mweb_top_content_for_detail(
    db: Session,
    site_id: int,
    *,
    top_n: int = REALTIME_DETAIL_TOP_N,
    window_minutes: int = 15,
) -> dict[str, list[dict[str, Any]]]:
    """Döviz / Sinemalar detay: web + mweb top sayfa (snapshot penceresi)."""
    out: dict[str, list[dict[str, Any]]] = {"web": [], "mweb": []}
    for prof in ("web", "mweb"):
        bundle = aggregate_page_snapshots_over_window(
            db,
            site_id=site_id,
            profile=prof,
            window_minutes=window_minutes,
            limit=top_n,
            sort_by="activeUsers",
        )
        for p in bundle.get("pages") or []:
            title = _rt_alarm_screen_title_one_line(str(p.get("page") or ""), max_len=56)
            out[prof].append(
                {
                    "title": title,
                    "active_users": int(float(p.get("activeUsers") or 0)),
                    "pageviews": int(float(p.get("screenPageViews") or 0)),
                }
            )
    return out


def _html_web_mweb_top_content_block(top: dict[str, list[dict[str, Any]]]) -> str:
    if not (top.get("web") or top.get("mweb")):
        return ""
    parts = [
        '<div style="margin-top:16px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;background:#fff;">',
        '<div style="padding:8px 12px;background:#f8fafc;border-bottom:1px solid #e2e8f0;'
        'font-size:11px;font-weight:700;color:#64748b;letter-spacing:.03em;">'
        "TOP İÇERİK · Web &amp; Mweb</div>",
    ]
    for label, key in (("Web", "web"), ("Mweb", "mweb")):
        pages = top.get(key) or []
        if not pages:
            continue
        parts.append(
            f'<div style="padding:8px 12px 4px;font-size:11px;font-weight:800;color:#334155;">{label}</div>'
        )
        parts.append(
            '<ol style="margin:0 0 6px;padding:0 12px 8px 26px;font-size:12px;line-height:1.45;color:#1e293b;">'
        )
        for item in pages:
            parts.append(
                "<li style=\"margin:3px 0;\">"
                f'<span style="font-weight:600;">{html.escape(item["title"])}</span> '
                f'<span style="color:#64748b;">· {item["active_users"]:,} kul · '
                f'{item["pageviews"]:,} gör.</span></li>'
            )
        parts.append("</ol>")
    parts.append("</div>")
    return "".join(parts)


def _detail_top_html_for_alarm(db: Session | None, alarm: dict[str, Any]) -> str:
    domain = str(alarm.get("domain") or alarm.get("site_domain") or "")
    if not _domain_shows_web_mweb_top_detail(domain):
        return ""
    sid = alarm.get("site_id")
    if sid is None or db is None:
        return ""
    try:
        top = get_web_mweb_top_content_for_detail(db, int(sid))
    except Exception:
        logger.exception("detail top content failed site_id=%s", sid)
        return ""
    return _html_web_mweb_top_content_block(top)


def _realtime_alarm_detail_top_html(domain: str, alarms: list[dict[str, Any]]) -> str:
    if not alarms or not _domain_shows_web_mweb_top_detail(domain):
        return ""
    from backend.database import SessionLocal

    try:
        with SessionLocal() as db:
            return _detail_top_html_for_alarm(db, alarms[0])
    except Exception:
        logger.exception("Realtime batch email detail top skipped")
        return ""


def _digest_top_n() -> int:
    from backend.config import settings

    return int(getattr(settings, "ga4_realtime_email_digest_top_n", REALTIME_DETAIL_TOP_N))


def _digest_window_minutes() -> int:
    from backend.config import settings

    return int(getattr(settings, "ga4_realtime_email_batch_interval_minutes", 90))


def _digest_interval_short_label(minutes: int | None = None) -> str:
    """Konu satırı / blok başlığı: 90 → 1,5s · 120 → 2s · 45 → 45dk."""
    m = max(15, int(minutes if minutes is not None else _digest_window_minutes()))
    if m % 60 == 0:
        return f"{m // 60}s"
    whole, rem = divmod(m, 60)
    if rem == 30 and whole:
        return f"{whole},5s"
    if whole:
        return f"{whole}s {rem}dk"
    return f"{m}dk"


def _digest_interval_long_label(minutes: int | None = None) -> str:
    m = max(15, int(minutes if minutes is not None else _digest_window_minutes()))
    if m % 60 == 0:
        h = m // 60
        return f"{h} saatlik" if h > 1 else "1 saatlik"
    whole, rem = divmod(m, 60)
    if rem == 30 and whole:
        return f"{whole},5 saatlik"
    if whole:
        return f"{whole} saat {rem} dakikalık"
    return f"{m} dakikalık"


def _normalize_digest_domain(domain: str | None) -> str:
    d = (domain or "").strip().lower()
    if d.startswith("https://"):
        d = d[8:]
    elif d.startswith("http://"):
        d = d[7:]
    return d.split("/")[0].split("?")[0].rstrip(".")


def _site_for_digest_brand(sites: list, brand: str):
    from backend.services.ga4_digest_email import ga4_digest_bucket_for_domain

    key = (brand or "").strip().lower()
    want_bucket = "doviz" if key == "doviz" else "sinema" if key == "sinemalar" else None
    if not want_bucket:
        return None
    for site in sites:
        dom = _normalize_digest_domain(site.domain)
        if ga4_digest_bucket_for_domain(dom) == want_bucket:
            return site
    for site in sites:
        dom = (site.domain or "").lower()
        if key == "doviz" and "doviz" in dom and "sinema" not in dom:
            return site
        if key == "sinemalar" and "sinemalar" in dom:
            return site
    return None


def _latest_collected_at(
    db: Session,
    model: type,
    site_id: int,
    profile: str,
) -> datetime | None:
    from sqlalchemy import desc

    return (
        db.query(model.collected_at)
        .filter(model.site_id == site_id, model.profile == profile)
        .order_by(desc(model.collected_at))
        .limit(1)
        .scalar()
    )


def _digest_stamp_local(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "—"
    from zoneinfo import ZoneInfo

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone(ZoneInfo(tz_name))
    return local.strftime("%d.%m.%Y %H:%M")


def _build_realtime_digest_empty_diagnostic_html(
    db: Session,
    sites: list,
    *,
    interval_short: str,
    tz_name: str,
) -> str:
    """6 alanın tamamı boşken özet mailine tanı tablosu."""
    from backend.config import settings
    from backend.models import RealtimeSnapshot
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.ga4_realtime_quota import (
        is_property_paused,
        paused_property_resume_times,
    )

    host = (getattr(settings, "app_public_host", "") or "projectcontrol.up.railway.app").strip()
    host = host.replace("https://", "").replace("http://", "").split("/")[0]
    admin_job_url = f"https://{host}/api/admin/run-realtime-job-now"
    realtime_url = f"https://{host}/realtime"

    rows_html: list[str] = []
    paused_map = paused_property_resume_times()
    resume_hour = int(getattr(settings, "ga4_realtime_quota_resume_hour", 6))

    for brand, profile in REALTIME_DIGEST_AREAS:
        site = _site_for_digest_brand(sites, brand)
        if not site:
            rows_html.append(
                "<tr>"
                f'<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">{html.escape(brand)} / {html.escape(profile)}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;color:#dc2626;">Site eşleşmedi</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">—</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">—</td>'
                "</tr>"
            )
            continue

        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        pid = (properties.get(profile) or properties.get("web") or "").strip()
        last_kpi = _latest_collected_at(db, RealtimeSnapshot, site.id, profile)

        status_parts: list[str] = []
        if not pid:
            status_parts.append("GA4 property yok")
        elif is_property_paused(pid):
            status_parts.append("Kota duraklatıldı (429)")

        if last_kpi is None:
            status_parts.append("Hiç KPI snapshot yok")
        else:
            age_h = (datetime.now(timezone.utc) - (
                last_kpi.replace(tzinfo=timezone.utc)
                if last_kpi.tzinfo is None
                else last_kpi.astimezone(timezone.utc)
            )).total_seconds() / 3600.0
            if age_h > 3:
                status_parts.append(f"KPI eski ({age_h:.0f}s önce)")
        if not status_parts:
            status_parts.append("Pencerede sayfa/haber verisi yok (KPI var)")

        rows_html.append(
            "<tr>"
            f'<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">{html.escape(brand)} / {html.escape(profile)}</td>'
            f'<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">{html.escape(site.domain or "?")}</td>'
            f'<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;">{_digest_stamp_local(last_kpi, tz_name)}</td>'
            f'<td style="padding:6px 8px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#475569;">{html.escape("; ".join(status_parts))}</td>'
            "</tr>"
        )

    rt_enabled = bool(getattr(settings, "ga4_realtime_enabled", True))
    interval_min = int(getattr(settings, "ga4_realtime_interval_minutes", 10))
    paused_note = ""
    if paused_map:
        paused_note = (
            f'<p style="font-size:12px;color:#b45309;margin:12px 0 0;">'
            f"GA4 Realtime günlük kota uyarısı: {len(paused_map)} property API çağrıları duraklatılmış "
            f"(sunucu yeniden başlasa liste sıfırlanır; tipik devam: {resume_hour:02d}:00 {html.escape(tz_name)}).</p>"
        )

    active_domains = [
        _normalize_digest_domain(getattr(s, "domain", None) or "")
        for s in (sites or [])
        if getattr(s, "domain", None)
    ]
    inventory = (
        f'<p style="font-size:11px;color:#94a3b8;margin:0 0 10px;">'
        f"Aktif site kaydı: {len(sites or [])}"
        + (
            f" · domainler: {html.escape(', '.join(active_domains[:12]))}"
            + ("…" if len(active_domains) > 12 else "")
            if active_domains
            else " · <span style=\"color:#dc2626;\">hiç aktif site yok (DB)</span>"
        )
        + "</p>"
    )

    return (
        '<p style="font-size:13px;color:#64748b;margin:0 0 10px;">'
        f"Son <strong>{html.escape(interval_short)}</strong> penceresinde 6 alanın hiçbirinde "
        f"özetlenecek KPI veya sıralı sayfa/haber/event verisi yok. "
        f"<span style=\"color:#94a3b8;\">(Not: «{html.escape(interval_short)}» = "
        f"{html.escape(_digest_interval_long_label(_digest_window_minutes()))}, saniye değil.)</span></p>"
        f"{inventory}"
        f'<p style="font-size:12px;color:#64748b;margin:0 0 12px;">'
        f"Zamanlayıcı: ga4_realtime_enabled={'açık' if rt_enabled else 'kapalı'}, "
        f"toparlama aralığı ≈{interval_min} dk.</p>"
        '<table style="width:100%;border-collapse:collapse;font-size:12px;margin:0 0 14px;">'
        "<thead><tr style=\"background:#f1f5f9;\">"
        '<th style="text-align:left;padding:6px 8px;">Alan</th>'
        '<th style="text-align:left;padding:6px 8px;">Site</th>'
        '<th style="text-align:left;padding:6px 8px;">Son KPI</th>'
        '<th style="text-align:left;padding:6px 8px;">Durum</th>'
        "</tr></thead><tbody>"
        f"{''.join(rows_html)}"
        "</tbody></table>"
        f"{paused_note}"
        '<p style="font-size:12px;color:#475569;margin:14px 0 0;">'
        f'<a href="{html.escape(realtime_url, quote=True)}" style="color:#2563eb;">Realtime paneli</a>'
        f" · "
        f'<a href="{html.escape(admin_job_url, quote=True)}" style="color:#2563eb;">Manuel realtime job</a>'
        f" (admin oturumu gerekir)</p>"
    )


def _html_digest_top_list(
    title: str,
    rows: list[tuple[str, int, str | None]],
    *,
    value_label: str = "kul",
) -> str:
    if not rows:
        return ""
    items = []
    for idx, (label, val, href) in enumerate(rows, start=1):
        label_e = html.escape(label)
        if href:
            link = html.escape(href, quote=True)
            label_html = (
                f'<a href="{link}" style="color:#1e293b;text-decoration:none;border-bottom:1px solid rgba(30,41,59,.15);">'
                f"{label_e}</a>"
            )
        else:
            label_html = label_e
        items.append(
            f'<li style="margin:3px 0;">'
            f'<span style="color:#94a3b8;margin-right:6px;">{idx}.</span>'
            f"{label_html} "
            f'<span style="color:#64748b;">· {val:,} {value_label}</span></li>'
        )
    return (
        f'<div style="margin:10px 0 4px;font-size:11px;font-weight:800;color:#475569;">{html.escape(title)}</div>'
        f'<ol style="margin:0 0 8px;padding:0 0 0 22px;font-size:12px;line-height:1.45;">{"".join(items)}</ol>'
    )


def _digest_profile_block(
    db: Session,
    site: "Site",
    profile: str,
    *,
    top_n: int,
    window_minutes: int,
) -> str:
    from backend.models import RealtimeSnapshot
    from sqlalchemy import desc

    window_minutes = max(15, min(int(window_minutes), 24 * 60))
    window_label = _digest_interval_short_label(window_minutes)

    snap = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site.id, RealtimeSnapshot.profile == profile)
        .order_by(desc(RealtimeSnapshot.collected_at))
        .first()
    )

    header = _html_email_section_header(site.domain, profile)
    body_parts: list[str] = [header]

    if snap:
        au_cur = int(round(float(snap.active_users_current or 0)))
        au_prev = int(round(float(snap.active_users_previous or 0)))
        pv_cur = int(round(float(snap.pageviews_current or 0)))
        pv_prev = int(round(float(snap.pageviews_previous or 0)))
        delta = au_cur - au_prev
        delta_c = "#16a34a" if delta >= 0 else "#dc2626"
        delta_sign = f"{delta:+d}" if au_prev > 0 or delta != 0 else "—"
        body_parts.append(
            f'<div style="font-size:12px;color:#64748b;margin:0 0 8px;">'
            f"Aktif kullanıcı <strong style=\"color:#0f172a;\">{au_cur:,}</strong>"
            f' <span style="color:#94a3b8;">(önceki {au_prev:,}, </span>'
            f'<span style="color:{delta_c};font-weight:700;">{delta_sign}</span>'
            f'<span style="color:#94a3b8;">)</span>'
            f" · Görüntüleme <strong style=\"color:#0f172a;\">{pv_cur:,}</strong>"
            f' <span style="color:#94a3b8;">(önceki {pv_prev:,})</span></div>'
        )

    has_ranked_content = False

    if profile in ("web", "mweb"):
        page_bundle = aggregate_page_snapshots_over_window(
            db,
            site_id=site.id,
            profile=profile,
            window_minutes=window_minutes,
            limit=top_n,
            sort_by="activeUsers",
        )
        plist: list[tuple[str, int, str | None]] = []
        for row in page_bundle.get("pages") or []:
            path = str(row.get("page") or "").strip() or "?"
            title = _rt_alarm_screen_title_one_line(path, max_len=72)
            href = f"https://{site.domain}{path}" if path.startswith("/") else None
            plist.append((title, int(float(row.get("activeUsers") or 0)), href))
        if plist:
            has_ranked_content = True
            body_parts.append(
                _html_digest_top_list(f"Top sayfalar · son {window_label} zirve", plist)
            )

        news_bundle = aggregate_news_snapshots_over_window(
            db,
            site_id=site.id,
            profile=profile,
            site_domain=str(site.domain or ""),
            window_minutes=window_minutes,
            limit=top_n,
            sort_by="activeUsers",
        )
        nlist: list[tuple[str, int, str | None]] = []
        for row in news_bundle.get("pages") or []:
            title = _rt_alarm_screen_title_one_line(str(row.get("page") or ""), max_len=72)
            href = str(row.get("link_url") or "").strip() or None
            nlist.append((title, int(float(row.get("activeUsers") or 0)), href))
        if nlist:
            has_ranked_content = True
            body_parts.append(
                _html_digest_top_list(f"Top haberler · son {window_label} zirve", nlist)
            )

    if profile in ("android", "ios"):
        peaks = get_peak_app_event_snapshots(
            db, site.id, profile, window_minutes=window_minutes
        )
        elist = [
            (name, count, None)
            for name, count in sorted(peaks.items(), key=lambda item: item[1], reverse=True)[:top_n]
            if name
        ]
        if elist:
            has_ranked_content = True
            body_parts.append(
                _html_digest_top_list(f"Top eventler · son {window_label} zirve", elist, value_label="evt")
            )

    if not snap and not has_ranked_content:
        return ""

    if snap and not has_ranked_content:
        body_parts.append(
            f'<p style="font-size:12px;color:#94a3b8;margin:8px 0 0;">'
            f"Son {window_label} penceresinde sıralanacak sayfa/haber/event verisi yok.</p>"
        )

    return (
        f'<div style="margin:14px 0;padding:12px 14px;border:1px solid #e2e8f0;border-radius:8px;background:#fafafa;">'
        f'{"".join(body_parts)}</div>'
    )


def build_realtime_periodic_digest_html(db: Session, *, queued_alarm_sections: int = 0) -> str:
    """SEO Realtime periyodik özet maili — 6 alanda pencere içi top sayfa/haber/event."""
    from backend.models import Site as SiteModel
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from backend.config import settings

    top_n = _digest_top_n()
    window_minutes = _digest_window_minutes()
    interval_short = _digest_interval_short_label(window_minutes)
    interval_long = _digest_interval_long_label(window_minutes)
    tz_name = getattr(settings, "report_calendar_timezone", "Europe/Istanbul")
    now_local = datetime.now(ZoneInfo(tz_name))
    stamp = now_local.strftime("%d.%m.%Y %H:%M")

    sites = _sort_sites(db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all())
    area_blocks: list[str] = []
    for brand, profile in REALTIME_DIGEST_AREAS:
        site = _site_for_digest_brand(sites, brand)
        if not site:
            continue
        block = _digest_profile_block(
            db, site, profile, top_n=top_n, window_minutes=window_minutes
        )
        if block:
            area_blocks.append(block)

    if not area_blocks:
        inner = _build_realtime_digest_empty_diagnostic_html(
            db, sites, interval_short=interval_short, tz_name=tz_name
        )
    else:
        inner = "".join(area_blocks)

    queued_line = ""
    if queued_alarm_sections > 0:
        queued_line = (
            f'<p style="font-size:11px;color:#64748b;margin:0 0 14px;">'
            f"Bu dönemde kuyruğa alınan alarm bölümü: {queued_alarm_sections}</p>"
        )

    pre = _preheader(f"SEO Realtime {interval_short} özet · {stamp}")
    return (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;'
        f'max-width:620px;margin:0 auto;color:#0f172a;">'
        f"{pre}"
        f'<p style="font-size:15px;font-weight:800;margin:0 0 4px;">'
        f"SEO Realtime · {interval_long} özet</p>"
        f'<p style="font-size:12px;color:#64748b;margin:0 0 18px;">{html.escape(stamp)} · '
        f"6 alanda (döviz web/mweb/android/ios + sinemalar web/mweb) "
        f"son {interval_short} pencerede en çok {top_n} sayfa/haber/event · zirve aktif kullanıcı</p>"
        f"{queued_line}"
        f"{inner}"
        f"</div>"
    )


def realtime_periodic_digest_subject() -> str:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from backend.config import settings

    tz_name = getattr(settings, "report_calendar_timezone", "Europe/Istanbul")
    minutes = _digest_window_minutes()
    time_stamp = datetime.now(ZoneInfo(tz_name)).strftime("%H:%M")
    return f"SEO {minutes} - {time_stamp}"[:120]


def check_site_realtime(
    db: Session,
    site: Site,
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    profile: str = "web",
    skip_alarms: bool = False,
    skip_emails: bool = False,
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

    # Profil bazlı dimension filter oluştur
    dim_filter = None
    if profile == "web":
        # 'web' profili için filtreleme yapmıyoruz; varsayılan (tüm trafik) kalsın.
        dim_filter = None
    elif profile == "mweb":
        # Ayrı mweb property zaten mobil web stream; deviceCategory=mobile GA4 UI toplamından düşük kalır.
        dim_filter = None
    elif profile == "android":
        dim_filter = FilterExpression(
            filter=Filter(
                field_name="platform",
                string_filter=Filter.StringFilter(value="Android"),
            )
        )
    elif profile == "ios":
        dim_filter = FilterExpression(
            filter=Filter(
                field_name="platform",
                string_filter=Filter.StringFilter(value="iOS"),
            )
        )

    try:
        logger.info("GA4 Realtime Fetch: site=%s profile=%s property=%s", site.domain, profile, property_id)
        result = fetch_realtime_comparison(property_id, window_minutes, dimension_filter=dim_filter)
    except Exception as exc:
        from backend.services.ga4_realtime_quota import (
            Ga4RealtimeQuotaPausedError,
            note_realtime_quota_error,
        )

        if isinstance(exc, Ga4RealtimeQuotaPausedError):
            logger.debug("GA4 Realtime atlandı (kota) [%s / %s]", site.domain, property_id)
            return {
                "site_id": site.id,
                "domain": site.domain,
                "profile": profile,
                "error": "quota_paused",
                "message": str(exc),
            }
        if not note_realtime_quota_error(property_id, exc, domain=str(site.domain or ""), logger=logger):
            logger.warning("GA4 Realtime API hatası [%s / %s]: %s", site.domain, property_id, exc)
        return {
            "site_id": site.id,
            "domain": site.domain,
            "profile": profile,
            "error": "api_error",
            "message": str(exc),
        }

    if skip_alarms:
        alarms = []
    else:
        alarms = evaluate_alarms(result["comparison"], site_domain=site.domain)

        profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
        for a in alarms:
            a["domain"] = site.domain
            a["profile"] = profile
            a["profile_label"] = profile_label
            a["message"] = (
                f"{site.domain} {profile_label} — "
                f"{a['metric']} {a['previous_value']:.0f} → {a['current_value']:.0f} ({a['change_pct']:+.1f}%)"
            )

    result["site_id"] = site.id
    result["domain"] = site.domain
    result["profile"] = profile
    result["alarms"] = alarms
    result["alarm_count"] = len(alarms)

    _save_snapshot(db, site.id, profile, result)

    if alarms:
        _save_alarm_logs(db, site.id, alarms, profile=profile)

        # Sürücü Analizi (Korelasyon)
        # Sadece activeUsers alarmı varsa veya en büyük alarm oysa analiz yap
        active_users_alarm = next((a for a in alarms if a["metric"] == "activeUsers"), None)
        if active_users_alarm:
            delta = active_users_alarm["current_value"] - active_users_alarm["previous_value"]
            drivers = _analyze_traffic_drivers(db, site.id, profile, delta)
            # _analyze_traffic_drivers {"increase": [...], "decrease": [...]} döndürür;
            # mail body düz bir sürücü listesi bekler (delta işaretine göre seç).
            driver_list = drivers["decrease"] if delta < 0 else drivers["increase"]
            if driver_list:
                # İlk alarma sürücüleri ekle (mail body'de kullanmak için)
                alarms[0]["drivers"] = driver_list
                
        logger.info("GA4 Realtime: %d alarm bulundu (site=%s, profile=%s).", len(alarms), site.domain, profile)
        if not skip_emails:
            rule_ids = [a["rule_id"] for a in alarms]
            if _alarm_email_suppressed(db, site.id, rule_ids, profile=profile):
                logger.info("GA4 Realtime: E-posta cooldown aktif, gönderim atlandı (site=%s, profile=%s).", site.domain, profile)
            else:
                mail_alarms = filter_alarms_for_email(alarms)
                if mail_alarms:
                    st = _send_site_alarm_emails(site.domain, profile, mail_alarms)
                    _commit_realtime_email_mark(
                        db,
                        site.id,
                        [a["rule_id"] for a in mail_alarms],
                        profile=profile,
                        status=st,
                    )
        logger.warning(
            "GA4 Realtime ALARM [%s]: %d kural tetiklendi — %s",
            site.domain,
            len(alarms),
            "; ".join(a["message"] for a in alarms),
        )
    else:
        logger.debug("GA4 Realtime: Alarm tetiklenmedi (site=%s, profile=%s).", site.domain, profile)

    if profile in ("web", "mweb"):
        _maybe_save_page_snapshots_for_tooltip(db, site.id, profile, property_id)

    return result


CHART_SNAPSHOT_MIN_INTERVAL_SEC = 50
PAGE_SNAPSHOT_TOOLTIP_MIN_INTERVAL_SEC = 300


def _maybe_record_chart_snapshot(db: Session, site_id: int, profile: str, result: dict[str, Any]) -> None:
    """TTL cache'ten dönen KPI için de trend noktası yazar (sayfa ~60 sn poll, GA4 her seferinde değil)."""
    if result.get("error"):
        return
    from sqlalchemy import desc

    from backend.models import RealtimeSnapshot

    now = datetime.utcnow()
    last = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
        .order_by(desc(RealtimeSnapshot.collected_at))
        .first()
    )
    if last is not None and last.collected_at is not None:
        ca = last.collected_at
        if ca.tzinfo is not None:
            ca = ca.replace(tzinfo=None)
        if (now - ca).total_seconds() < CHART_SNAPSHOT_MIN_INTERVAL_SEC:
            return
    _save_snapshot(db, site_id, profile, result)


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


def _save_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]], profile: str = "web") -> None:
    """Tetiklenen alarmları DB'ye kaydeder."""
    from backend.models import RealtimeAlarmLog

    for alarm in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=alarm["rule_id"],
            metric=(f"{profile}:{alarm['metric']}")[:50],
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


def _alarm_email_suppressed(
    db: Session, site_id: int, rule_ids: list[str], profile: str | None = None
) -> bool:
    """Cooldown kontrolü: aynı site+profil için son N dakikada aynı kural tetiklendiyse True döner.

    profile verilirse yalnızca o profil için kontrol yapar — farklı profiller birbirini bastırmaz.
    """
    from backend.config import settings
    from backend.models import RealtimeAlarmLog

    cooldown = int(getattr(settings, "ga4_realtime_alarm_email_cooldown_minutes", 45))
    if cooldown <= 0 or not rule_ids:
        return False
    cutoff = datetime.utcnow() - timedelta(minutes=cooldown)
    q = (
        db.query(RealtimeAlarmLog.rule_id)
        .filter(
            RealtimeAlarmLog.site_id == site_id,
            RealtimeAlarmLog.rule_id.in_(rule_ids),
            RealtimeAlarmLog.email_sent_at.isnot(None),
            RealtimeAlarmLog.email_sent_at > cutoff,
        )
    )
    if profile:
        q = q.filter(RealtimeAlarmLog.metric.like(f"{profile}:%"))
    return q.first() is not None


def _mark_alarms_emailed(
    db: Session, site_id: int, rule_ids: list[str], *, profile: str | None = None,
    window_minutes: int = 5,
) -> None:
    """Son `window_minutes` dakika içinde kaydedilmiş ve mail atılan alarm loglarını işaretle."""
    from backend.models import RealtimeAlarmLog
    if not rule_ids:
        return
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=window_minutes)
    try:
        q = db.query(RealtimeAlarmLog).filter(
            RealtimeAlarmLog.site_id == site_id,
            RealtimeAlarmLog.rule_id.in_(rule_ids),
            RealtimeAlarmLog.triggered_at > cutoff,
            RealtimeAlarmLog.email_sent_at.is_(None),
        )
        if profile:
            q = q.filter(RealtimeAlarmLog.metric.like(f"{profile}:%"))
        q.update({RealtimeAlarmLog.email_sent_at: now}, synchronize_session=False)
        db.commit()
    except Exception as exc:
        logger.warning("_mark_alarms_emailed hatası: %s", exc)
        db.rollback()


def _realtime_email_dispatch_status(send_ok: bool) -> str:
    from backend.services.mailer import realtime_email_batch_is_collecting

    if not send_ok:
        return "failed"
    if realtime_email_batch_is_collecting():
        return "queued"
    return "sent"


def _commit_realtime_email_mark(
    db: Session,
    site_id: int,
    rule_ids: list[str],
    *,
    profile: str | None,
    status: str,
) -> None:
    from backend.services.mailer import realtime_email_batch_note_mark

    if status == "sent":
        _mark_alarms_emailed(db, site_id, rule_ids, profile=profile)
    elif status == "queued":
        realtime_email_batch_note_mark(site_id, rule_ids, profile=profile)


def apply_realtime_batch_email_marks(db: Session, marks: list[dict]) -> None:
    for m in marks:
        _mark_alarms_emailed(
            db,
            int(m["site_id"]),
            list(m.get("rule_ids") or []),
            profile=m.get("profile"),
        )


def _send_site_alarm_emails(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    """Genel site alarmları — batch veya anında gönderim. sent | queued | failed."""
    from backend.services.mailer import is_realtime_mail_ready, send_realtime_email

    if not alarms:
        return "failed"
    if not is_realtime_mail_ready():
        logger.warning(
            "Realtime site alarmı tetiklendi (site=%s, alarm_count=%d) ancak e-posta gönderilemedi. "
            "Lütfen şu ayarları kontrol edin: GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP/Gmail OAuth, MAIL_TO.",
            domain,
            len(alarms),
        )
        return "failed"

    logger.info("GA4 Realtime: E-posta hazırlanıyor (site=%s, profile=%s)...", domain, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    detail_top_html = _realtime_alarm_detail_top_html(domain, alarms)
    html_body = _html_site_alarm_body(domain, profile, alarms, detail_top_html=detail_top_html)
    subject = _email_site_alarm_subject(domain, profile, alarms)
    ok = send_realtime_email(subject, html_body, thread_kind="site", thread_key=thread_key)
    status = _realtime_email_dispatch_status(bool(ok))
    if status == "sent":
        logger.info("GA4 Realtime: E-posta başarıyla gönderildi (site=%s, profile=%s).", domain, profile)
    elif status == "queued":
        logger.info("GA4 Realtime: E-posta konsolide kuyruğa alındı (site=%s, profile=%s).", domain, profile)
    else:
        logger.error("GA4 Realtime: E-posta gönderimi başarısız (site=%s, profile=%s).", domain, profile)
    return status


def get_recent_snapshots(
    db: Session,
    site_id: int,
    *,
    profile: str = "web",
    limit: int | None = 30,
    hours: float | None = None,
) -> list[dict[str, Any]]:
    """Snapshot trendi — son N kayıt veya son X saat (GA4 çağrısı yok)."""
    from backend.models import RealtimeSnapshot

    prof = (profile or "web").strip()

    def _row_dict(row: RealtimeSnapshot) -> dict[str, Any]:
        return {
            "collected_at": _utc_db_datetime_iso_z(row.collected_at),
            "active_users": row.active_users_current,
            "active_users_prev": row.active_users_previous,
            "pageviews": row.pageviews_current,
            "pageviews_prev": row.pageviews_previous,
            "alarm_count": row.alarm_count,
            "window_minutes": row.window_minutes,
        }

    if hours is not None and hours > 0:
        h = min(float(hours), REALTIME_TREND_HOURS_MAX)
        cutoff = datetime.utcnow() - timedelta(hours=h)
        rows = (
            db.query(RealtimeSnapshot)
            .filter(
                RealtimeSnapshot.site_id == site_id,
                RealtimeSnapshot.profile == prof,
                RealtimeSnapshot.collected_at >= cutoff,
            )
            .order_by(RealtimeSnapshot.collected_at.asc())
            .limit(REALTIME_TREND_ROWS_MAX)
            .all()
        )
        return [_row_dict(row) for row in rows]

    lim = REALTIME_TREND_LIMIT_DEFAULT if limit is None else int(limit)
    lim = min(max(lim, 1), REALTIME_TREND_LIMIT_MAX)
    rows = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == prof)
        .order_by(RealtimeSnapshot.collected_at.desc())
        .limit(lim)
        .all()
    )
    return [_row_dict(row) for row in reversed(rows)]


_COMBINED_SNAPSHOT_BUCKET_MS = 15 * 60 * 1000
_COMBINED_SNAPSHOT_WEEK_MS = 7 * 24 * 60 * 60 * 1000


def _resolve_combined_snapshot_profiles(
    db: Session,
    site_id: int,
    *,
    cutoff: datetime,
) -> list[str]:
    from backend.models import RealtimeSnapshot

    found = {
        r[0]
        for r in db.query(RealtimeSnapshot.profile)
        .filter(
            RealtimeSnapshot.site_id == site_id,
            RealtimeSnapshot.collected_at >= cutoff,
        )
        .distinct()
        .all()
    }
    return [p for p in ("web", "mweb", "ios", "android") if p in found]


def _combined_site_snapshot_bucket_map(
    db: Session,
    site_id: int,
    profiles: list[str],
    *,
    cutoff: datetime,
    end: datetime,
    bucket_ms: int = _COMBINED_SNAPSHOT_BUCKET_MS,
) -> dict[int, dict[str, int]]:
    """15 dk duvar saati dilimlerinde profil toplamları (site geneli)."""
    from collections import defaultdict

    from backend.models import RealtimeSnapshot

    nested: dict[int, dict[str, dict[str, float]]] = defaultdict(dict)

    for prof in profiles:
        rows = (
            db.query(RealtimeSnapshot)
            .filter(
                RealtimeSnapshot.site_id == site_id,
                RealtimeSnapshot.profile == prof,
                RealtimeSnapshot.collected_at >= cutoff,
                RealtimeSnapshot.collected_at <= end,
            )
            .order_by(RealtimeSnapshot.collected_at.asc())
            .limit(REALTIME_TREND_ROWS_MAX)
            .all()
        )
        for row in rows:
            if not row.collected_at:
                continue
            ca = row.collected_at
            ts_ms = int(
                (ca.replace(tzinfo=timezone.utc) if ca.tzinfo is None else ca.astimezone(timezone.utc)).timestamp()
                * 1000
            )
            key = (ts_ms // bucket_ms) * bucket_ms
            au = float(row.active_users_current or 0)
            pv = float(row.pageviews_current or 0)
            slot = nested[key].get(prof)
            if slot is None or au >= slot["active_users"]:
                nested[key][prof] = {"active_users": au, "pageviews": pv}

    out: dict[int, dict[str, int]] = {}
    for key, prof_map in nested.items():
        total_au = sum(v["active_users"] for v in prof_map.values())
        total_pv = sum(v["pageviews"] for v in prof_map.values())
        out[key] = {
            "active_users": int(round(total_au)),
            "pageviews": int(round(total_pv)),
        }
    return out


def get_combined_site_snapshots(
    db: Session,
    site_id: int,
    *,
    hours: float = REALTIME_TREND_HOURS_DEFAULT,
    profiles: list[str] | None = None,
    include_prev_week: bool = True,
) -> list[dict[str, Any]]:
    """Tüm profillerin 15 dk dilimlerinde toplam aktif + görüntüleme trendi (yalnızca DB)."""
    h = min(float(hours), REALTIME_TREND_HOURS_MAX)
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=h)
    bucket_ms = _COMBINED_SNAPSHOT_BUCKET_MS

    profile_cutoff = cutoff - timedelta(days=7) if include_prev_week else cutoff
    if profiles is None:
        profiles = _resolve_combined_snapshot_profiles(db, site_id, cutoff=profile_cutoff)
    else:
        profiles = [str(p).strip() for p in profiles if str(p).strip()]

    current_map = _combined_site_snapshot_bucket_map(
        db, site_id, profiles, cutoff=cutoff, end=now, bucket_ms=bucket_ms
    )
    prev_map: dict[int, dict[str, int]] = {}
    if include_prev_week:
        week = timedelta(days=7)
        prev_map = _combined_site_snapshot_bucket_map(
            db,
            site_id,
            profiles,
            cutoff=cutoff - week,
            end=now - week,
            bucket_ms=bucket_ms,
        )

    out: list[dict[str, Any]] = []
    for key in sorted(current_map.keys()):
        cur = current_map[key]
        prev = prev_map.get(key - _COMBINED_SNAPSHOT_WEEK_MS) if prev_map else None
        dt = datetime.utcfromtimestamp(key / 1000.0)
        row: dict[str, Any] = {
            "collected_at": _utc_db_datetime_iso_z(dt),
            "active_users": cur["active_users"],
            "pageviews": cur["pageviews"],
        }
        if include_prev_week:
            row["active_users_prev_week"] = int(prev["active_users"]) if prev else None
            row["pageviews_prev_week"] = int(prev["pageviews"]) if prev else None
        out.append(row)
    return out


def get_combined_bucket_top_pages(
    db: Session,
    site_id: int,
    *,
    hours: float = REALTIME_TREND_HOURS_DEFAULT,
    top_n: int = 3,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Site geneli 24s grafik tooltip: her 15 dk diliminde web/mweb top sayfalar (DB snapshot)."""
    from collections import defaultdict

    from backend.models import RealtimePageSnapshot

    h = min(float(hours), REALTIME_TREND_HOURS_MAX)
    cutoff = datetime.utcnow() - timedelta(hours=h)
    bucket_ms = 15 * 60 * 1000
    cap = max(1, min(int(top_n), 10))
    profiles = ("web", "mweb")

    # bucket_ms -> profile -> page_path -> {au, pv} tepe
    nested: dict[int, dict[str, dict[str, dict[str, float]]]] = defaultdict(lambda: defaultdict(dict))

    rows = (
        db.query(RealtimePageSnapshot)
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile.in_(profiles),
            RealtimePageSnapshot.collected_at >= cutoff,
        )
        .order_by(RealtimePageSnapshot.collected_at.asc())
        .limit(500_000)
        .all()
    )
    for row in rows:
        if not row.collected_at:
            continue
        ca = row.collected_at
        ts_ms = int(
            (ca.replace(tzinfo=timezone.utc) if ca.tzinfo is None else ca.astimezone(timezone.utc)).timestamp()
            * 1000
        )
        key = (ts_ms // bucket_ms) * bucket_ms
        path = str(row.page_path or "").strip()
        if not path:
            continue
        prof = str(row.profile or "web").strip()
        if prof not in profiles:
            continue
        au = float(row.active_users or 0)
        pv = float(row.pageviews or 0)
        slot = nested[key][prof].get(path)
        if slot is None:
            nested[key][prof][path] = {"active_users": au, "pageviews": pv}
        else:
            slot["active_users"] = max(float(slot.get("active_users") or 0), au)
            slot["pageviews"] = max(float(slot.get("pageviews") or 0), pv)

    out: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for key, prof_map in nested.items():
        bucket_out: dict[str, list[dict[str, Any]]] = {}
        for prof in profiles:
            ranked: list[dict[str, Any]] = []
            for path, metrics in (prof_map.get(prof) or {}).items():
                label = path if len(path) <= 48 else path[:47] + "…"
                ranked.append(
                    {
                        "page_path": path,
                        "label": label,
                        "active_users": int(round(float(metrics.get("active_users") or 0))),
                        "pageviews": int(round(float(metrics.get("pageviews") or 0))),
                    }
                )
            ranked.sort(key=lambda x: (x["active_users"], x["pageviews"]), reverse=True)
            bucket_out[prof] = ranked[:cap]
            ranked_pv = sorted(ranked, key=lambda x: (x["pageviews"], x["active_users"]), reverse=True)
            bucket_out[prof + "_by_pv"] = ranked_pv[:cap]
        out[str(key)] = bucket_out
    return out


def fetch_realtime_profile_bundle(
    db: Session,
    site: Any,
    *,
    profile: str = "web",
    window_minutes: int | None = None,
    trend_limit: int = REALTIME_TREND_LIMIT_DEFAULT,
    trend_hours: float | None = None,
    skip_alarms: bool = True,
) -> dict[str, Any]:
    """Realtime sayfası ve ana sayfa KPI/spark için ortak veri yolu (TTL cache + trend)."""
    from backend.config import settings
    from backend.services.realtime_cache import get_or_call
    from backend.services.ga4_realtime_quota import domain_is_light_realtime

    w = window_minutes if window_minutes is not None else settings.ga4_realtime_window_minutes
    sid = int(site.id)
    prof = (profile or "web").strip()
    ttl = int(settings.ga4_realtime_kpi_cache_seconds)
    if domain_is_light_realtime(getattr(site, "domain", None)):
        ttl = max(ttl, 120)

    def _produce() -> dict[str, Any]:
        return check_site_realtime(db, site, window_minutes=w, profile=prof, skip_alarms=skip_alarms)

    result = dict(
        get_or_call(
            f"rt:kpi:{sid}:{prof}:{w}",
            ttl,
            _produce,
            is_error=lambda r: bool(r.get("error")),
            last_good_ttl=settings.ga4_realtime_last_good_seconds,
        )
    )
    _maybe_record_chart_snapshot(db, sid, prof, result)
    if not result.get("error") and prof in ("web", "mweb"):
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(prof) or properties.get("web")
        if property_id:
            _maybe_save_page_snapshots_for_tooltip(db, sid, prof, str(property_id))
    if trend_hours is not None and trend_hours > 0:
        result["trend"] = get_recent_snapshots(db, sid, profile=prof, hours=trend_hours)
    else:
        result["trend"] = get_recent_snapshots(db, sid, profile=prof, limit=trend_limit)
    return result


def bundle_from_snapshot_trend(trend: list[dict[str, Any]]) -> dict[str, Any] | None:
    """DB snapshot trend'inden anlık KPI (GA4 beklemeden ana sayfa ilk boyama)."""
    if not trend:
        return None
    last = trend[-1]
    cur = float(last.get("active_users") or 0)
    prev_raw = last.get("active_users_prev")
    prev = float(prev_raw if prev_raw is not None else cur)
    if prev <= 0:
        pct = 100.0 if cur > 0 else 0.0
    else:
        pct = round((cur - prev) / prev * 100.0, 1)
    return {
        "total": {"activeUsers": cur},
        "comparison": {
            "activeUsers": {"current": cur, "previous": prev, "change_pct": pct},
        },
        "cached": True,
        "stale": True,
        "from_snapshot": True,
    }


def _schedule_home_realtime_kpi_refresh(
    site_id: int,
    profile: str,
    *,
    window_minutes: int,
    trend_limit: int,
) -> None:
    """Soğuk cache'te snapshot gösterildikten sonra GA4 KPI cache'ini doldur."""
    import threading

    from backend.database import SessionLocal

    def _run() -> None:
        try:
            with SessionLocal() as db:
                site = db.query(Site).filter(Site.id == site_id).first()
                if site is None:
                    return
                fetch_realtime_profile_bundle(
                    db,
                    site,
                    profile=profile,
                    window_minutes=window_minutes,
                    trend_limit=trend_limit,
                    skip_alarms=True,
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Home realtime arka plan refresh başarısız (site=%s profile=%s).",
                site_id,
                profile,
            )

    threading.Thread(target=_run, daemon=True, name=f"home-rt-{site_id}-{profile}").start()


def fetch_home_realtime_profile_bundle(
    db: Session,
    site: Any,
    *,
    profile: str = "web",
    window_minutes: int | None = None,
    trend_limit: int = REALTIME_HOME_TREND_LIMIT,
) -> dict[str, Any]:
    """Ana sayfa realtime: bellek cache → snapshot → (gerekirse) GA4; miss'te snapshot + arka plan refresh."""
    from backend.config import settings
    from backend.services.realtime_cache import get_cached_only

    w = window_minutes if window_minutes is not None else settings.ga4_realtime_window_minutes
    sid = int(site.id)
    prof = (profile or "web").strip()
    trend = get_recent_snapshots(db, sid, profile=prof, limit=trend_limit)
    cache_key = f"rt:kpi:{sid}:{prof}:{w}"

    cached = get_cached_only(
        cache_key,
        settings.ga4_realtime_kpi_cache_seconds,
        last_good_ttl=settings.ga4_realtime_last_good_seconds,
    )
    if cached is not None:
        result = dict(cached)
        result["trend"] = trend
        return result

    snap_bundle = bundle_from_snapshot_trend(trend)
    if snap_bundle is not None:
        _schedule_home_realtime_kpi_refresh(
            sid, prof, window_minutes=w, trend_limit=trend_limit
        )
        snap_bundle["trend"] = trend
        return snap_bundle

    return fetch_realtime_profile_bundle(
        db,
        site,
        profile=prof,
        window_minutes=w,
        trend_limit=trend_limit,
        trend_hours=None,
        skip_alarms=True,
    )


def active_users_kpi_from_realtime_result(result: dict[str, Any]) -> tuple[float, str | None, str, float]:
    """realtime.html ``_renderKpis`` ile aynı activeUsers mantığı (toplam + comparison.change_pct)."""
    trend = result.get("trend") or []
    if result.get("error"):
        if trend:
            last = trend[-1]
            return float(last.get("active_users") or 0), None, "flat", 0.0
        return 0.0, None, "flat", 0.0

    comp_block = (result.get("comparison") or {}).get("activeUsers")
    total = result.get("total") or {}
    cur_win = result.get("current") or {}
    prev_win = result.get("previous") or {}

    comp: dict[str, Any] | None
    if comp_block is not None:
        comp = dict(comp_block)
    else:
        cw = cur_win.get("activeUsers")
        pw = prev_win.get("activeUsers")
        totv = total.get("activeUsers")
        if cw is not None or pw is not None:
            cu = float(cw or 0)
            pr = float(pw or 0)
            pct_raw = ((cu - pr) / pr * 100.0) if pr > 0 else (100.0 if cu > 0 else 0.0)
            comp = {"current": cu, "previous": pr, "change_pct": round(pct_raw, 1)}
        elif totv is not None:
            tnum = float(totv or 0)
            comp = {"current": tnum, "previous": tnum, "change_pct": 0.0}
        else:
            comp = None

    if total.get("activeUsers") is not None:
        main_val = float(total["activeUsers"])
    elif comp is not None:
        main_val = float(comp.get("current") or 0)
    elif trend:
        main_val = float(trend[-1].get("active_users") or 0)
    else:
        main_val = 0.0

    if comp is None or comp.get("change_pct") is None:
        return main_val, None, "flat", 0.0

    delta_pct = float(comp["change_pct"])
    tone = "up" if delta_pct > 0.5 else ("down" if delta_pct < -0.5 else "flat")
    sign = "+" if delta_pct > 0 else ""
    delta_fmt = f"{sign}{delta_pct:.1f}%"
    return main_val, delta_fmt, tone, delta_pct


def _alarm_row_public_url(domain: str, metric: str) -> str:
    """Sayfa/haber alarm satırı için tarayıcıda açılabilir mutlak URL (yalnız path tabanlı satırlar)."""
    from urllib.parse import urlparse

    host = (domain or "").strip()
    if host.lower().startswith(("http://", "https://")):
        host = (urlparse(host).netloc or host).strip()
    host = host.split("/")[0].strip()
    if not host:
        return ""

    m = (metric or "").strip()
    raw = ""
    if m.startswith("page:"):
        raw = m[5:].strip()
    elif m.startswith("news:"):
        raw = m[5:].strip()
    if not raw:
        return ""
    if raw.lower().startswith(("http://", "https://")):
        return raw[:2048]
    if raw.startswith("/"):
        return f"https://{host}{raw}"[:2048]
    return ""


def get_recent_alarms(
    db: Session,
    site_id: int,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Son N alarm kaydını döner."""
    from backend.models import RealtimeAlarmLog, Site

    site = db.query(Site).filter(Site.id == site_id).first()
    domain = (site.domain or "").strip() if site else ""

    rows = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(RealtimeAlarmLog.triggered_at.desc())
        .limit(limit)
        .all()
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        metric = row.metric
        row_url = _alarm_row_public_url(domain, metric)
        out.append(
            {
                "id": row.id,
                "site_id": site_id,
                "rule_id": row.rule_id,
                "metric": metric,
                "severity": row.severity,
                "current_value": row.current_value,
                "previous_value": row.previous_value,
                "change_pct": row.change_pct,
                "message": row.message,
                "triggered_at": _utc_db_datetime_iso_z(row.triggered_at),
                "domain": domain,
                "row_url": row_url or None,
            }
        )
    return out


def run_all_sites_realtime_check(
    db: Session,
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    skip_alarms: bool = False,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    """Tüm aktif siteleri kontrol eder — scheduler job'ından çağrılır.

    Her site için web ve (GA4 ayarlarında ayrı property ID ile tanımlıysa) mweb, ios,
    android profilleri ayrı ayrı kaydedilir; arka plan job'ı Realtime trendine yansır.
    """
    from backend.models import Site as SiteModel
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.ga4_realtime_quota import scheduler_profiles_for_site

    sites = _sort_sites(db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all())
    results: list[dict[str, Any]] = []
    profile_order = ("web", "mweb", "ios", "android")
    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        base_web = (properties.get("web") or "").strip()
        if not base_web and not any((properties.get(p) or "").strip() for p in profile_order):
            continue
        allowed_profiles = scheduler_profiles_for_site(site.domain, properties)
        for profile in profile_order:
            if profile not in allowed_profiles:
                continue
            pid = (properties.get(profile) or "").strip() or base_web
            if not pid:
                continue
            # Artık dimension filter kullandığımız için web fallback olsa dahi 
            # her profil için ayrı snapshot kaydediyoruz. 
            # API kota limitlerini izlemek gerekebilir.
            try:
                r = check_site_realtime(
                    db,
                    site,
                    window_minutes=window_minutes,
                    profile=profile,
                    skip_alarms=skip_alarms,
                    skip_emails=skip_emails,
                )
                results.append(r)
            except Exception as exc:
                logger.exception("Realtime check başarısız [%s / %s]: %s", site.domain, profile, exc)
                results.append({
                    "site_id": site.id,
                    "domain": site.domain,
                    "profile": profile,
                    "error": "check_failed",
                    "message": str(exc),
                })
    return results


# ── Sayfa bazlı alarm sistemi ────────────────────────────────────────────────


def _rt_alarm_screen_title_one_line(key: str, *, max_len: int = 100) -> str:
    """GA4 satır kimliği (path veya unifiedScreenName) — UI ve log için kısaltılmış tek başlık."""
    s = str(key or "").strip()
    if not s:
        return "—"
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def _maybe_save_page_snapshots_for_tooltip(
    db: Session,
    site_id: int,
    profile: str,
    property_id: str,
    *,
    min_interval_sec: int = PAGE_SNAPSHOT_TOOLTIP_MIN_INTERVAL_SEC,
) -> None:
    """Spark tooltip top içerik için web/mweb sayfa snapshot'ı (kota dostu throttle)."""
    if profile not in ("web", "mweb"):
        return
    from sqlalchemy import func as sqlfunc

    from backend.models import RealtimePageSnapshot

    latest = (
        db.query(sqlfunc.max(RealtimePageSnapshot.collected_at))
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
        )
        .scalar()
    )
    now = datetime.utcnow()
    if latest is not None:
        la = latest.replace(tzinfo=None) if latest.tzinfo else latest
        if (now - la).total_seconds() < max(60, int(min_interval_sec)):
            return
    try:
        result = fetch_realtime_top_pages(
            property_id,
            window_minutes=15,
            limit=100,
            compare_previous=False,
        )
        pages = result.get("pages") or []
        if pages:
            save_page_snapshots(db, site_id, profile, pages)
    except Exception as exc:
        from backend.services.ga4_realtime_quota import Ga4RealtimeQuotaPausedError

        if isinstance(exc, Ga4RealtimeQuotaPausedError):
            logger.debug("Tooltip page snapshot atlandı (kota) site=%s profile=%s", site_id, profile)
        else:
            logger.debug("Tooltip page snapshot atlandı site=%s profile=%s: %s", site_id, profile, exc)


def save_page_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    pages: list[dict[str, Any]],
) -> None:
    """Top sayfa sonuçlarını DB'ye kaydeder."""
    from backend.models import RealtimePageSnapshot

    for i, page in enumerate(pages[:100]):
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

    page_rules = _realtime_rules_threshold_pct_for_domain(PAGE_ALARM_RULES, site_domain)

    triggered: list[dict[str, Any]] = []

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

    # Floor-safety: top listenin alt sınırı; bu sınırın altındaki sayfalar liste dışı olabilir.
    floor_users_page = 0
    if curr_map:
        try:
            floor_users_page = int(min((c.get("activeUsers", 0) or 0) for c in curr_map.values()))
        except (ValueError, TypeError):
            floor_users_page = 0

    for page_path, curr in curr_map.items():
        title = _rt_alarm_screen_title_one_line(page_path)
        if not title or title == "—" or title.lower() in ("(other)", "(not set)", "(blank)", "not set"):
            continue
        curr_users = curr.get("activeUsers", 0)
        prev = prev_map.get(page_path)

        if prev:
            prev_users = prev.get("activeUsers", 0)

            rule = page_rules["page_traffic_drop"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct <= -rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "page_traffic_drop",
                        "severity": rule["severity"],
                        "page": page_path,
                        "page_paths": curr.get("page_paths", []),
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"{title} — trafik düştü: {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })

            rule = page_rules["page_traffic_spike"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct >= rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "page_traffic_spike",
                        "severity": rule["severity"],
                        "page": page_path,
                        "page_paths": curr.get("page_paths", []),
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"{title} — trafik arttı: {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })
        else:
            rule = page_rules["page_new_entry"]
            if curr_users >= rule["min_users"]:
                triggered.append({
                    "rule_id": "page_new_entry",
                    "severity": rule["severity"],
                    "page": page_path,
                    "page_paths": curr.get("page_paths", []),
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": curr_users,
                    "previous_users": 0,
                    "change_pct": 100.0,
                    "message": f"{title} — listeye girdi ({curr_users:.0f} kullanıcı)",
                })

    rule = page_rules["page_disappeared"]
    for page_path, prev in prev_map.items():
        if page_path not in curr_map:
            title = _rt_alarm_screen_title_one_line(page_path)
            if not title or title == "—" or title.lower() in ("(other)", "(not set)", "(blank)", "not set"):
                continue
            prev_users = prev.get("activeUsers", 0)
            if prev_users < rule["min_prev_users"]:
                continue
            # Floor-safety: sayfa top listeden düşmüş olabilir ama hâlâ kullanıcısı olabilir.
            if floor_users_page <= 2:
                assumed_curr = 0
                is_confident = True
            elif prev_users >= floor_users_page * 4:
                assumed_curr = max(0, floor_users_page - 1)
                is_confident = True
            else:
                # Belirsiz — alarm verme
                continue
            triggered.append({
                "rule_id": "page_disappeared",
                "severity": rule["severity"],
                "page": page_path,
                "page_paths": prev.get("page_paths", []),
                "profile": profile,
                "domain": site_domain,
                "current_users": assumed_curr,
                "previous_users": prev_users,
                "change_pct": -100.0 if assumed_curr == 0 else round(((assumed_curr - prev_users) / prev_users) * 100, 1),
                "message": f"{title} — listeden çıktı (önceki: {prev_users:.0f})",
            })

    return triggered


def check_page_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 30,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    """Tek site+profil için sayfa bazlı alarm kontrolü yapar.

    API'nin compare_previous=True modu kullanılır: current 15dk vs previous 15dk
    (GA4 Realtime'ın kendi karşılaştırması). DB snapshot'ı sadece fallback olarak
    kullanılır — örtüşen pencere sorununu önler.
    """
    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return []

    try:
        result = fetch_realtime_top_pages_with_app_fallback(
            property_id,
            profile=profile,
            window_minutes=window_minutes,
            limit=100,
            sort_by="activeUsers",
            compare_previous=True,
        )
    except Exception as exc:
        logger.warning("Sayfa alarm: top pages API hatası [%s/%s]: %s", site.domain, profile, exc)
        return []

    current_pages = result.get("pages", [])

    # Android/iOS profilleri: dinamik ID'li ekran isimlerini filtrele (notification_news_:1540 gibi)
    if profile in ("android", "ios"):
        import re as _re
        _dynamic = _re.compile(r".*[:\-_]\d{3,}$")
        current_pages = [p for p in current_pages if not _dynamic.match(p.get("page", "") or "")]

    save_page_snapshots(db, site.id, profile, current_pages)

    # API karşılaştırma verisi varsa kullan (daha doğru: non-overlapping 15min pencereler)
    # Yoksa DB snapshot fallback
    if result.get("comparison_enabled") and any(p.get("activeUsers_previous") for p in current_pages):
        synthetic_prev = [
            {
                "page": p["page"],
                "activeUsers": float(p.get("activeUsers_previous") or 0),
                "screenPageViews": float(p.get("screenPageViews_previous") or 0),
                "rank": i + 1,
            }
            for i, p in enumerate(current_pages)
        ]
        previous_pages = synthetic_prev
    else:
        previous_pages = get_previous_page_snapshots(db, site.id, profile)

    if not previous_pages:
        return []

    alarms = evaluate_page_alarms(
        current_pages, previous_pages,
        site_domain=site.domain, profile=profile,
    )

    if alarms:
        _save_page_alarm_logs(db, site.id, alarms, profile=profile)
        if not skip_emails:
            negatives, positives = _split_alarms_by_sentiment(alarms)
            to_send: list[dict[str, Any]] = []
            if positives:
                if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in positives], profile=profile):
                    logger.info("Sayfa pozitif cooldown — atlandı (site=%s, profile=%s).", site.domain, profile)
                else:
                    to_send.extend(positives)
            if negatives:
                if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in negatives], profile=profile):
                    logger.info("Sayfa negatif cooldown — atlandı (site=%s, profile=%s).", site.domain, profile)
                else:
                    to_send.extend(negatives)
            if to_send:
                to_send = filter_alarms_for_email(_cap_top_n_each_side(to_send))
            if to_send:
                st = _send_page_alarm_email(site.domain, profile, to_send)
                _commit_realtime_email_mark(
                    db,
                    site.id,
                    [a["rule_id"] for a in to_send],
                    profile=profile,
                    status=st,
                )

    return alarms


def _save_page_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]], profile: str = "web") -> None:
    """Sayfa bazlı alarmları RealtimeAlarmLog'a kaydeder."""
    from backend.models import RealtimeAlarmLog

    for a in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=a["rule_id"],
            metric=(f"{profile}:p:" + a.get("page", ""))[:50],
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


def _send_page_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    """Sayfa bazlı alarmlar — sent | queued | failed."""
    from backend.services.mailer import is_page_alarm_mail_ready, send_realtime_email

    if not alarms:
        return "failed"
    if not is_page_alarm_mail_ready():
        logger.warning(
            "Realtime sayfa alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP/Gmail OAuth ve MAIL_TO.",
            len(alarms),
        )
        return "failed"

    thread_key = _realtime_email_thread_key(domain, profile)
    detail_top_html = _realtime_alarm_detail_top_html(domain, alarms)
    html_body = _html_page_alarm_body(domain, profile, alarms, detail_top_html=detail_top_html)
    subject = _email_page_alarm_subject(domain, profile, alarms)
    ok = send_realtime_email(subject, html_body, thread_kind="page", thread_key=thread_key)
    return _realtime_email_dispatch_status(bool(ok))


def run_page_alarm_check_all_sites(
    db: Session,
    *,
    window_minutes: int = 30,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    """Tüm aktif siteler ve profilleri için sayfa bazlı alarm kontrolü."""
    from backend.models import Site as SiteModel

    all_alarms: list[dict[str, Any]] = []
    sites = _sort_sites(db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all())

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
                    skip_emails=skip_emails,
                )
                all_alarms.extend(alarms)
            except Exception as exc:
                logger.exception("Sayfa alarm check hatası [%s/%s]: %s", site.domain, profile, exc)

    return all_alarms


# ── Haberler (Realtime «Haberler» sekmesi) alarm + snapshot ─────────────────


def _news_snapshot_due(
    db: Session,
    site_id: int,
    profile: str,
    *,
    interval_minutes: int,
) -> bool:
    """Son haber snapshot'ından bu yana yeterli süre geçtiyse True."""
    from backend.models import RealtimeNewsSnapshot
    from sqlalchemy import func as sqlfunc

    latest_time = (
        db.query(sqlfunc.max(RealtimeNewsSnapshot.collected_at))
        .filter(
            RealtimeNewsSnapshot.site_id == site_id,
            RealtimeNewsSnapshot.profile == profile,
        )
        .scalar()
    )
    if latest_time is None:
        return True
    delta = datetime.utcnow() - latest_time
    return delta >= timedelta(minutes=interval_minutes)


def save_news_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    pages: list[dict[str, Any]],
) -> None:
    from backend.models import RealtimeNewsSnapshot

    for i, page in enumerate(pages[:100]):
        title = str(page.get("page") or "")[:500]
        snap = RealtimeNewsSnapshot(
            site_id=site_id,
            profile=profile,
            screen_title=title,
            active_users=float(page.get("activeUsers") or 0),
            pageviews=float(page.get("screenPageViews") or 0),
            rank=i + 1,
        )
        db.add(snap)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("RealtimeNewsSnapshot kayıt hatası (site_id=%s)", site_id)


def get_peak_news_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    window_minutes: int = 90,
) -> dict[str, int]:
    """Son `window_minutes` dakikadaki her haber başlığı için maksimum active_users değerini döner.

    Dönen dict: {screen_title: peak_active_users}
    """
    from backend.models import RealtimeNewsSnapshot
    from sqlalchemy import func as sqlfunc

    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
    rows = (
        db.query(
            RealtimeNewsSnapshot.screen_title,
            sqlfunc.max(RealtimeNewsSnapshot.active_users).label("peak_users"),
        )
        .filter(
            RealtimeNewsSnapshot.site_id == site_id,
            RealtimeNewsSnapshot.profile == profile,
            RealtimeNewsSnapshot.collected_at >= cutoff,
        )
        .group_by(RealtimeNewsSnapshot.screen_title)
        .all()
    )
    return {row.screen_title: int(row.peak_users or 0) for row in rows}


REALTIME_LIST_RANGE_MINUTES: dict[str, int] = {
    "15m": 15,
    "1h": 60,
    "24h": 24 * 60,
}

REALTIME_LIST_RANGE_LABELS: dict[str, str] = {
    "15m": "15 dk",
    "1h": "1 saat",
    "24h": "24 saat",
}


def parse_realtime_list_range(
    range_key: str | None,
    *,
    window: int | None = None,
) -> tuple[str, int, str]:
    """Sayfa/haber listesi penceresi: (mode, minutes, range_key). mode = ga4 | snapshots."""
    rk = (range_key or "").strip().lower()
    if rk in REALTIME_LIST_RANGE_MINUTES:
        minutes = REALTIME_LIST_RANGE_MINUTES[rk]
        mode = "ga4" if rk == "15m" else "snapshots"
        return mode, minutes, rk
    w = max(1, min(int(window or 30), 30))
    return "ga4", w, "15m"


def aggregate_page_snapshots_over_window(
    db: Session,
    *,
    site_id: int,
    profile: str,
    window_minutes: int,
    limit: int,
    sort_by: str = "activeUsers",
) -> dict[str, Any]:
    """DB snapshot’larından pencere içi zirve aktif kullanıcı / görüntüleme ile top sayfa."""
    from backend.models import RealtimePageSnapshot
    from sqlalchemy import func as sqlfunc

    minutes = max(15, min(int(window_minutes), 24 * 60))
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=minutes)
    prev_cutoff = cutoff - timedelta(minutes=minutes)

    curr_rows = (
        db.query(
            RealtimePageSnapshot.page_path,
            sqlfunc.max(RealtimePageSnapshot.active_users).label("peak_au"),
            sqlfunc.max(RealtimePageSnapshot.pageviews).label("peak_pv"),
        )
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
            RealtimePageSnapshot.collected_at >= cutoff,
        )
        .group_by(RealtimePageSnapshot.page_path)
        .all()
    )

    prev_map: dict[str, Any] = {}
    prev_rows = (
        db.query(
            RealtimePageSnapshot.page_path,
            sqlfunc.max(RealtimePageSnapshot.active_users).label("peak_au"),
            sqlfunc.max(RealtimePageSnapshot.pageviews).label("peak_pv"),
        )
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
            RealtimePageSnapshot.collected_at >= prev_cutoff,
            RealtimePageSnapshot.collected_at < cutoff,
        )
        .group_by(RealtimePageSnapshot.page_path)
        .all()
    )
    for row in prev_rows:
        prev_map[str(row.page_path or "")] = row

    sort_key = sort_by if sort_by in ("activeUsers", "screenPageViews") else "activeUsers"
    pages: list[dict[str, Any]] = []
    for row in curr_rows:
        path_str = str(row.page_path or "")
        prev = prev_map.get(path_str)
        pages.append(
            {
                "page": path_str,
                "page_paths": [path_str] if path_str.startswith("/") else [],
                "activeUsers": float(row.peak_au or 0),
                "screenPageViews": float(row.peak_pv or 0),
                "activeUsers_previous": float(prev.peak_au) if prev else None,
                "screenPageViews_previous": float(prev.peak_pv) if prev else None,
            }
        )
    pages.sort(key=lambda p: float(p.get(sort_key) or 0), reverse=True)
    cap = max(1, min(int(limit), 100))
    pages = pages[:cap]

    rk = next((k for k, v in REALTIME_LIST_RANGE_MINUTES.items() if v == minutes), "custom")
    return {
        "pages": pages,
        "window_minutes": minutes,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "comparison_enabled": bool(prev_map),
        "data_source": "snapshots",
        "metric_scope": f"snapshot_peak_{minutes}m",
        "range": rk,
        "range_label": REALTIME_LIST_RANGE_LABELS.get(rk, f"{minutes} dk"),
    }


def aggregate_news_snapshots_over_window(
    db: Session,
    *,
    site_id: int,
    profile: str,
    site_domain: str,
    window_minutes: int,
    limit: int,
    sort_by: str = "activeUsers",
) -> dict[str, Any]:
    """DB haber snapshot’larından pencere içi zirve metriklerle top haber listesi."""
    from backend.models import RealtimeNewsSnapshot
    from backend.services.realtime_news_paths import (
        is_realtime_news_path,
        realtime_news_page_link,
        unified_screen_news_article_title,
        _normalize_news_tab_title,
    )
    from sqlalchemy import func as sqlfunc

    minutes = max(15, min(int(window_minutes), 24 * 60))
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=minutes)
    prev_cutoff = cutoff - timedelta(minutes=minutes)

    def _peak_rows(since: datetime, until: datetime | None = None) -> dict[str, dict[str, float]]:
        q = db.query(
            RealtimeNewsSnapshot.screen_title,
            sqlfunc.max(RealtimeNewsSnapshot.active_users).label("peak_au"),
            sqlfunc.max(RealtimeNewsSnapshot.pageviews).label("peak_pv"),
        ).filter(
            RealtimeNewsSnapshot.site_id == site_id,
            RealtimeNewsSnapshot.profile == profile,
            RealtimeNewsSnapshot.collected_at >= since,
        )
        if until is not None:
            q = q.filter(RealtimeNewsSnapshot.collected_at < until)
        out: dict[str, dict[str, float]] = {}
        for row in q.group_by(RealtimeNewsSnapshot.screen_title).all():
            title = str(row.screen_title or "").strip()
            if not title:
                continue
            if title.startswith("/"):
                if not is_realtime_news_path(title, site_domain=site_domain):
                    continue
            else:
                art = unified_screen_news_article_title(title, site_domain=site_domain)
                if not art:
                    continue
            out[title] = {
                "activeUsers": float(row.peak_au or 0),
                "screenPageViews": float(row.peak_pv or 0),
            }
        return out

    curr_map = _peak_rows(cutoff)
    prev_map = _peak_rows(prev_cutoff, cutoff)

    sort_key = sort_by if sort_by in ("activeUsers", "screenPageViews") else "activeUsers"
    pages: list[dict[str, Any]] = []
    for raw_title, metrics in curr_map.items():
        title = raw_title
        if not title.startswith("/"):
            title = _normalize_news_tab_title(title)
        link = realtime_news_page_link(raw_title, site_domain=site_domain)
        prev = prev_map.get(raw_title) or {}
        pages.append(
            {
                "page": title,
                "page_path": raw_title if raw_title.startswith("/") else "",
                "activeUsers": metrics.get("activeUsers", 0),
                "screenPageViews": metrics.get("screenPageViews", 0),
                "activeUsers_previous": prev.get("activeUsers"),
                "screenPageViews_previous": prev.get("screenPageViews"),
                "link_url": link,
            }
        )
    pages.sort(key=lambda p: float(p.get(sort_key) or 0), reverse=True)
    cap = max(1, min(int(limit), 100))
    pages = pages[:cap]

    rk = next((k for k, v in REALTIME_LIST_RANGE_MINUTES.items() if v == minutes), "custom")
    return {
        "pages": pages,
        "window_minutes": minutes,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "comparison_enabled": bool(prev_map),
        "data_source": "snapshots",
        "metric_scope": f"snapshot_peak_{minutes}m",
        "range": rk,
        "range_label": REALTIME_LIST_RANGE_LABELS.get(rk, f"{minutes} dk"),
        "breakdown": "news_snapshots",
    }


def get_previous_news_snapshots(
    db: Session,
    site_id: int,
    profile: str,
) -> list[dict[str, Any]]:
    from backend.models import RealtimeNewsSnapshot
    from sqlalchemy import func as sqlfunc

    latest_time = (
        db.query(sqlfunc.max(RealtimeNewsSnapshot.collected_at))
        .filter(
            RealtimeNewsSnapshot.site_id == site_id,
            RealtimeNewsSnapshot.profile == profile,
        )
        .scalar()
    )
    if not latest_time:
        return []

    rows = (
        db.query(RealtimeNewsSnapshot)
        .filter(
            RealtimeNewsSnapshot.site_id == site_id,
            RealtimeNewsSnapshot.profile == profile,
            RealtimeNewsSnapshot.collected_at == latest_time,
        )
        .order_by(RealtimeNewsSnapshot.rank)
        .all()
    )
    return [
        {
            "page": row.screen_title,
            "activeUsers": row.active_users,
            "screenPageViews": row.pageviews,
            "rank": row.rank,
        }
        for row in rows
    ]


def evaluate_news_alarms(
    current_pages: list[dict[str, Any]],
    previous_pages: list[dict[str, Any]],
    *,
    site_domain: str = "",
    profile: str = "web",
    peak_pages: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    if not previous_pages and not peak_pages:
        return []

    news_rules = _realtime_rules_threshold_pct_for_domain(NEWS_ALARM_RULES, site_domain)

    triggered: list[dict[str, Any]] = []

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

    # Floor: mevcut listede görünen en düşük kullanıcı sayısı. Top-N listemizden düşmüş
    # ama gerçekte hâlâ N+1 sırasında olabilen sayfalar için tampon — "0'a düştü"
    # iddiasını sadece floor çok düşükse (≤2) güvenle yapabiliriz.
    floor_users = 0
    if curr_map:
        try:
            floor_users = int(min((c.get("activeUsers", 0) or 0) for c in curr_map.values()))
        except (ValueError, TypeError):
            floor_users = 0

    def _safe_curr_when_missing(prev_or_peak: float) -> tuple[int, bool]:
        """Top listeden düşen sayfa için 'gerçek değer 0' yerine konservatif tahmin.
        Dönen: (assumed_curr, is_confident_zero).
        floor_users ≤ 2: liste alt sınırı çok düşük → 0 varsaymak güvenli (is_confident=True)
        floor_users > 2: alt sınır anlamlı → curr ≈ floor_users-1 (kötümser) ve confidence=False
        Eğer prev/peak >= floor*4 ise floor'a göre kayıp yine de eşiği aşıyor olabilir → confident=True.
        """
        if floor_users <= 2:
            return 0, True
        if prev_or_peak >= floor_users * 4:
            # Çok büyük bir düşüş zaten — floor-1 olsa bile drop hâlâ belirgin
            return max(0, floor_users - 1), True
        # Belirsiz — alarm verme
        return max(0, floor_users - 1), False

    for page_path, curr in curr_map.items():
        title = _rt_alarm_screen_title_one_line(page_path)
        if not title or title == "—" or title.lower() in ("(other)", "(not set)", "(blank)", "not set"):
            continue
        curr_users = curr.get("activeUsers", 0)
        prev = prev_map.get(page_path)

        if prev:
            prev_users = prev.get("activeUsers", 0)

            rule = news_rules["news_traffic_drop"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct <= -rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "news_traffic_drop",
                        "severity": rule["severity"],
                        "page": page_path,
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"{title} — trafik düştü: {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })

            rule = news_rules["news_traffic_spike"]
            if prev_users >= rule["min_users"] and prev_users > 0:
                pct = ((curr_users - prev_users) / prev_users) * 100
                if pct >= rule["threshold_pct"]:
                    triggered.append({
                        "rule_id": "news_traffic_spike",
                        "severity": rule["severity"],
                        "page": page_path,
                        "profile": profile,
                        "domain": site_domain,
                        "current_users": curr_users,
                        "previous_users": prev_users,
                        "change_pct": round(pct, 1),
                        "message": f"{title} — trafik arttı: {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })
        else:
            rule = news_rules["news_new_entry"]
            if curr_users >= rule["min_users"]:
                triggered.append({
                    "rule_id": "news_new_entry",
                    "severity": rule["severity"],
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": curr_users,
                    "previous_users": 0,
                    "change_pct": 100.0,
                    "message": f"{title} — listeye girdi ({curr_users:.0f} kullanıcı)",
                })

    rule = news_rules["news_disappeared"]
    for page_path, prev in prev_map.items():
        if page_path not in curr_map:
            title = _rt_alarm_screen_title_one_line(page_path)
            if not title or title == "—" or title.lower() in ("(other)", "(not set)", "(blank)", "not set"):
                continue
            prev_users = prev.get("activeUsers", 0)
            if prev_users < rule["min_prev_users"]:
                continue
            # Floor-safety: sayfa top listeden düşmüş olabilir ama hâlâ kullanıcısı olabilir.
            assumed_curr, is_confident = _safe_curr_when_missing(prev_users)
            if not is_confident:
                # Belirsiz — alarm verme (false positive önleme)
                continue
            triggered.append({
                "rule_id": "news_disappeared",
                "severity": rule["severity"],
                "page": page_path,
                "profile": profile,
                "domain": site_domain,
                "current_users": assumed_curr,
                "previous_users": prev_users,
                "change_pct": -100.0 if assumed_curr == 0 else round(((assumed_curr - prev_users) / prev_users) * 100, 1),
                "message": f"{title} — listeden çıktı (önceki: {prev_users:.0f})",
            })

    # Zirve düşüş kontrolü — son 90 dak içinde zirve yapıp şimdi belirgin düşen haberler
    if peak_pages:
        peak_rule = news_rules.get("news_peak_drop", NEWS_ALARM_RULES["news_peak_drop"])
        drop_threshold_pct = float(peak_rule.get("drop_pct", 55))
        min_peak = float(peak_rule.get("min_peak_users", 30))
        # Zaten drop/disappeared olarak tetiklenmiş sayfaları atla (duplicate önleme)
        already_triggered = {a["page"] for a in triggered if a["rule_id"] in ("news_traffic_drop", "news_disappeared")}
        for page_path, peak_users in peak_pages.items():
            if page_path in already_triggered:
                continue
            if peak_users < min_peak:
                continue
            title = _rt_alarm_screen_title_one_line(page_path)
            if not title or title == "—" or title.lower() in ("(other)", "(not set)", "(blank)", "not set"):
                continue
            curr = curr_map.get(page_path)
            if curr is not None:
                curr_users = int(curr.get("activeUsers", 0))
            else:
                # Sayfa top listede yok — floor-safety ile konservatif tahmin yap
                assumed_curr, is_confident = _safe_curr_when_missing(peak_users)
                if not is_confident:
                    # Belirsiz — alarm verme (sayfa hâlâ floor düzeyinde olabilir)
                    continue
                curr_users = assumed_curr
            drop_pct = ((peak_users - curr_users) / peak_users) * 100
            if drop_pct >= drop_threshold_pct:
                triggered.append({
                    "rule_id": "news_peak_drop",
                    "severity": peak_rule.get("severity", "warning"),
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": curr_users,
                    "previous_users": peak_users,
                    "peak_users": peak_users,
                    "drop_pct": round(drop_pct),
                    "change_pct": -round(drop_pct, 1),
                    "message": f"{title} — düştü: {peak_users:.0f} → {curr_users:.0f} (−{drop_pct:.0f}%)",
                })

    return triggered


def _save_news_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]], profile: str = "web") -> None:
    from backend.models import RealtimeAlarmLog

    for a in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=a["rule_id"],
            metric=(f"{profile}:news:" + a.get("page", ""))[:50],
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
        logger.exception("Haber alarm log kayıt hatası (site_id=%s)", site_id)


def _get_site_kpi_summary(db: Session, site_id: int, profile: str) -> dict:
    """Son RealtimeSnapshot'tan genel trafik özetini döner (mail için)."""
    from backend.models import RealtimeSnapshot
    from sqlalchemy import desc as _desc
    try:
        snap = (
            db.query(RealtimeSnapshot)
            .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
            .order_by(_desc(RealtimeSnapshot.collected_at))
            .first()
        )
        if snap:
            cur  = snap.active_users_current  or 0
            prev = snap.active_users_previous or 0
            pct  = round((cur - prev) / prev * 100, 1) if prev > 0 else 0.0
            return {"current": int(cur), "previous": int(prev), "change_pct": pct}
    except Exception:
        pass
    return {}


def _send_news_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]], site_kpi: dict | None = None) -> str:
    """Haber trafiği alarmları — sent | queued | failed."""
    from backend.services.mailer import is_news_realtime_mail_ready, send_realtime_news_email

    if not alarms:
        return "failed"
    if not is_news_realtime_mail_ready():
        logger.warning(
            "Realtime haber alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_NEWS_ALERT_EMAIL, SMTP/Gmail OAuth ve MAIL_TO.",
            len(alarms),
        )
        return "failed"

    thread_key = _realtime_email_thread_key(domain, profile)
    detail_top_html = _realtime_alarm_detail_top_html(domain, alarms)
    html_body = _html_news_alarm_body(
        domain, profile, alarms, site_kpi=site_kpi or {}, detail_top_html=detail_top_html
    )
    subject = _email_news_alarm_subject(domain, profile, alarms)
    ok = send_realtime_news_email(subject, html_body, thread_kind="news", thread_key=thread_key)
    return _realtime_email_dispatch_status(bool(ok))


def check_news_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 15,
    interval_minutes: int = 15,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    if not _news_snapshot_due(db, site.id, profile, interval_minutes=interval_minutes):
        return []

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    # ios/android için web fallback YANLIŞ olur (app ≠ web property); sadece web/mweb için izinli.
    if profile in ("ios", "android"):
        property_id = properties.get(profile)
    else:
        property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return []

    previous_pages = get_previous_news_snapshots(db, site.id, profile)
    peak_pages = get_peak_news_snapshots(db, site.id, profile, window_minutes=90)

    try:
        result = fetch_realtime_top_news_pages(
            property_id,
            site_domain=(site.domain or "").strip(),
            profile=profile,
            window_minutes=window_minutes,
            limit=100,
            sort_by="activeUsers",
        )
    except Exception as exc:
        logger.warning("Haber alarm: top-news API hatası [%s/%s]: %s", site.domain, profile, exc)
        return []

    current_pages = result.get("pages", [])
    save_news_snapshots(db, site.id, profile, current_pages)

    alarms = evaluate_news_alarms(
        current_pages, previous_pages,
        site_domain=site.domain, profile=profile,
        peak_pages=peak_pages,
    )

    if alarms:
        _save_news_alarm_logs(db, site.id, alarms, profile=profile)
        if not skip_emails:
            # Negatif (düşüş) ve pozitif (artış) alarmları ayrı ayrı cooldown'la — negatifler bastırılmasın
            negatives, positives = _split_alarms_by_sentiment(alarms)
            to_send: list[dict[str, Any]] = []
            if positives:
                if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in positives], profile=profile):
                    logger.info("Haber pozitif alarmları cooldown — atlandı (site=%s, profile=%s).", site.domain, profile)
                else:
                    to_send.extend(positives)
            if negatives:
                if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in negatives], profile=profile):
                    logger.info("Haber negatif alarmları cooldown — atlandı (site=%s, profile=%s).", site.domain, profile)
                else:
                    to_send.extend(negatives)
            if to_send:
                to_send = filter_alarms_for_email(_cap_top_n_each_side(to_send))
            if to_send:
                site_kpi = _get_site_kpi_summary(db, site.id, profile)
                st = _send_news_alarm_email(site.domain, profile, to_send, site_kpi=site_kpi)
                _commit_realtime_email_mark(
                    db,
                    site.id,
                    [a["rule_id"] for a in to_send],
                    profile=profile,
                    status=st,
                )

    return alarms


def run_news_alarm_check_all_sites(db: Session, *, skip_emails: bool = False) -> list[dict[str, Any]]:
    """Tüm siteler için Haberler (web/mweb) alarm kontrolü — aralık ayarı config'ten."""
    from backend.config import settings
    from backend.models import Site as SiteModel

    all_alarms: list[dict[str, Any]] = []
    if not settings.ga4_realtime_news_alerts_enabled:
        return all_alarms

    interval = int(settings.ga4_realtime_news_alert_interval_minutes)
    window = int(settings.ga4_realtime_news_alert_window_minutes)
    sites = _sort_sites(db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all())

    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        for profile in ("web", "mweb", "ios", "android"):
            prop_id = str(properties.get(profile, "")).strip()
            if not prop_id:
                continue
            try:
                alarms = check_news_alarms_for_site(
                    db,
                    site,
                    profile=profile,
                    window_minutes=window,
                    interval_minutes=interval,
                    skip_emails=skip_emails,
                )
                all_alarms.extend(alarms)
            except Exception as exc:
                logger.exception("Haber alarm check hatası [%s/%s]: %s", site.domain, profile, exc)

    return all_alarms


# ── Özet Raporlama (Consolidated Summary) ──────────────────────────────────

def _html_realtime_summary_body(alarms: list[dict[str, Any]]) -> str:
    """Tüm alarmları içeren toplu özet HTML gövdesi."""
    import html

    # Alarmları site bazında grupla
    by_site: dict[str, list[dict[str, Any]]] = {}
    for a in alarms:
        dom = a.get("domain") or a.get("site_domain") or "Bilinmeyen Site"
        by_site.setdefault(dom, []).append(a)

    sections: list[str] = []
    for dom in sorted(by_site.keys(), key=_domain_sort_key):
        site_alarms = sorted(
            by_site[dom],
            key=lambda x: (
                _PROFILE_PRIORITY.get(x.get("profile", "web"), 99),
                -abs(float(x.get("change_pct", 0))),
            ),
        )
        rows: list[str] = []
        for a in site_alarms[:10]:
            profile = a.get("profile", "web")
            profile_abbr = _email_profile_abbr(profile)

            pct = float(a.get("change_pct", 0))
            color = "#dc2626" if pct < 0 else "#16a34a"

            raw_label = a.get("metric") or a.get("page") or ""
            metric_label = _email_metric_chip(str(raw_label)) if raw_label in ("activeUsers", "screenPageViews") else raw_label
            if len(str(metric_label)) > 45:
                metric_label = str(metric_label)[:42] + "..."

            cur = a.get("current_value") or a.get("current_users") or 0
            prev = a.get("previous_value") or a.get("previous_users") or 0

            rows.append(f"""
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:10px 8px;font-size:13px;color:#475569;width:48px;">{html.escape(profile_abbr)}</td>
                    <td style="padding:10px 8px;font-size:13px;color:#0f172a;font-weight:500;">{html.escape(str(metric_label))}</td>
                    <td style="padding:10px 8px;font-size:13px;color:#64748b;text-align:right;">{prev:,.0f} → {cur:,.0f}</td>
                    <td style="padding:10px 8px;font-size:14px;font-weight:700;color:{color};text-align:right;width:80px;">
                        {pct:+.1f}%
                    </td>
                </tr>
            """)

        short_dom = html.escape(_email_site_short_label(dom))
        sections.append(f"""
            <div style="margin-bottom:24px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
                <div style="background:#f8fafc;padding:10px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#334155;">
                    {short_dom}
                </div>
                <table style="width:100%;border-collapse:collapse;background:white;">
                    {''.join(rows)}
                </table>
            </div>
        """)

    return f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;margin:0 auto;padding:16px;">
        <p style="font-size:13px;font-weight:600;color:#64748b;margin:0 0 16px;">{len(alarms)} alarm</p>
        {''.join(sections)}
    </div>
    """

def send_realtime_summary_email(all_alarms: list[dict[str, Any]]) -> bool:
    """Tüm alarmları tek bir özet maili olarak gönderir."""
    from backend.services.mailer import send_realtime_email
    
    if not all_alarms:
        logger.info("send_realtime_summary_email: Gönderilecek alarm yok.")
        return False
    
    logger.info("send_realtime_summary_email: %d alarm için özet maili hazırlanıyor...", len(all_alarms))
    
    # Önem sırasına göre diz (en yüksek değişimler en üstte)
    sorted_alarms = sorted(all_alarms, key=lambda x: abs(float(x.get("change_pct", 0))), reverse=True)
    top_10 = sorted_alarms[:10]
    
    # Konu başlığını en önemli alarma göre seç
    primary = top_10[0]
    dom = primary.get("domain") or primary.get("site_domain") or "SEO"
    pct = abs(float(primary.get("change_pct", 0)))
    verb = "Düşüş" if float(primary.get("change_pct", 0)) < 0 else "Artış"
    
    subject = f"Realtime Özet: {dom} %{pct:.0f} {verb} ve {len(all_alarms)} Alarm"
    html_body = _html_realtime_summary_body(sorted_alarms)
    
    success = send_realtime_email(subject, html_body, thread_kind="summary", thread_key="daily_rt_summary", is_summary=True)
    if success:
        logger.info("send_realtime_summary_email: Özet maili başarıyla gönderildi.")
    else:
        logger.warning("send_realtime_summary_email: Özet maili gönderilemedi (mailer başarısız).")
    return success


# ── Realtime 404 Spike İzleme ─────────────────────────────────────────────────

# Yalnızca net hata sayfası başlıkları (geniş "bulunamadı" eşleşmesi yok)
_RT_404_TITLE_MARKERS: tuple[str, ...] = (
    "sayfa bulunamadı",
    "page not found",
    "404 error",
    "404 -",
    "error 404",
)

_RT_404_PATH_RE = re.compile(
    r"(?:^|[/?])(?:404|not[-_]?found|error[-_]?page)(?:[/?.]|$)|/404(?:/|$)",
    re.I,
)


def _is_rt_404_page(title: str, paths: list[str] | None = None) -> bool:
    """404/hata sayfası — net başlık veya path kalıbı."""
    t = (title or "").lower().strip()
    if t and any(marker in t for marker in _RT_404_TITLE_MARKERS):
        return True
    for raw in paths or []:
        p = (raw or "").strip()
        if p and _RT_404_PATH_RE.search(p):
            return True
    return False


def _evaluate_404_spike_severity(
    total: int,
    previous: int,
    *,
    warn_threshold: int,
    crit_threshold: int,
) -> str | None:
    """Mutlak kullanıcı sayısı değil, önceki pencereye göre ani artış (spike) aranır."""
    if total < warn_threshold:
        return None
    delta = total - previous
    min_delta = max(8, warn_threshold // 2)
    # Sürekli yüksek taban (384→380 gibi): spike değil, uyarı verme
    if previous >= warn_threshold and delta < min_delta:
        return None
    pct = (delta / previous * 100) if previous > 0 else (100.0 if total >= warn_threshold else 0.0)
    if total >= crit_threshold and (delta >= min_delta or previous == 0 or pct >= 25):
        return "critical"
    if total >= warn_threshold and (delta >= min_delta or previous == 0 or pct >= 15):
        return "warning"
    return None


def fetch_realtime_404_users(
    property_id: str,
    window_minutes: int = 15,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """GA4 Realtime — 404 sayfasındaki aktif kullanıcılar + önceki pencere karşılaştırması."""
    if client is None:
        client = _build_client()

    result = fetch_realtime_top_pages(
        property_id,
        window_minutes=window_minutes,
        limit=100,
        sort_by="activeUsers",
        dimension="unifiedScreenName",
        compare_previous=True,
        include_page_path=False,
        client=client,
    )

    pages_404: list[dict[str, Any]] = []
    total_users = 0
    total_prev = 0
    for p in result.get("pages") or []:
        title = str(p.get("page") or "")
        paths = [str(x) for x in (p.get("page_paths") or []) if x]
        if not _is_rt_404_page(title, paths):
            continue
        cur = int(p.get("activeUsers") or 0)
        prev = int(p.get("activeUsers_previous") or 0)
        total_users += cur
        total_prev += prev
        pages_404.append({
            "title": title,
            "activeUsers": cur,
            "activeUsers_previous": prev,
            "page_paths": paths[:5],
        })

    pages_404.sort(key=lambda x: x["activeUsers"], reverse=True)

    return {
        "total_404_users": total_users,
        "previous_404_users": total_prev,
        "delta_404_users": total_users - total_prev,
        "pages": pages_404,
        "window_minutes": window_minutes,
        "comparison_enabled": bool(result.get("comparison_enabled")),
    }




def check_realtime_404_for_site(
    db: Session,
    site: "Site",
    *,
    profile: str = "web",
    skip_emails: bool = False,
) -> dict[str, Any]:
    """Tek site+profil için realtime 404 spike kontrolü."""
    from backend.config import settings
    from backend.models import RealtimeAlarmLog

    if not getattr(settings, "ga4_realtime_404_enabled", True):
        return {}

    warn_threshold = int(getattr(settings, "ga4_realtime_404_warning_threshold", 10))
    crit_threshold = int(getattr(settings, "ga4_realtime_404_critical_threshold", 25))
    window         = int(getattr(settings, "ga4_realtime_404_window_minutes", 15))

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = (properties.get(profile) or "").strip()
    if not property_id:
        return {}  # Profil için property tanımlı değilse atla (web fallback yapmıyoruz — duplicate mail önlemi)

    try:
        data = fetch_realtime_404_users(property_id, window_minutes=window)
    except Exception as exc:
        logger.warning("Realtime 404 fetch hatası [%s/%s]: %s", site.domain, profile, exc)
        return {}

    total = data.get("total_404_users", 0)
    previous = int(data.get("previous_404_users") or 0)
    delta = int(data.get("delta_404_users") or (total - previous))
    pages = data.get("pages", [])

    if total == 0:
        return {"total_404_users": 0, "previous_404_users": previous, "delta_404_users": delta, "pages": [], "severity": None}

    severity = _evaluate_404_spike_severity(
        total, previous, warn_threshold=warn_threshold, crit_threshold=crit_threshold
    )

    result = {
        "total_404_users": total,
        "previous_404_users": previous,
        "delta_404_users": delta,
        "pages": pages,
        "severity": severity,
    }

    if severity is None:
        return result

    # DB'ye kaydet
    rule_id = f"rt_404_{severity}"
    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    change_pct = (delta / previous * 100) if previous > 0 else (100.0 if total > 0 else 0.0)
    log = RealtimeAlarmLog(
        site_id=site.id,
        rule_id=rule_id,
        metric=f"{profile}:active_404_users",
        severity=severity,
        current_value=float(total),
        previous_value=float(previous),
        change_pct=change_pct,
        message=(
            f"{site.domain} {profile_label} — 404 spike: {previous:.0f} → {total:.0f} kul. "
            f"({delta:+.0f}, {change_pct:+.0f}%)"
        ),
    )
    db.add(log)
    try:
        db.commit()
    except Exception:
        db.rollback()

    if skip_emails:
        return result

    return result


def run_404_spike_check_all_sites(db: Session, *, skip_emails: bool = False) -> list[dict]:
    """Tüm sitelerin web + mweb profillerinde 404 spike kontrolü."""
    from backend.models import Site as SiteModel
    from backend.services.ga4_auth import get_ga4_credentials_record

    results = []
    sites = _sort_sites(db.query(SiteModel).all())
    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        if not record:
            continue
        for profile in ("web", "mweb"):
            try:
                r = check_realtime_404_for_site(db, site, profile=profile, skip_emails=skip_emails)
                if r:
                    r["site_id"] = site.id
                    r["domain"]  = site.domain
                    r["profile"] = profile
                    results.append(r)
            except Exception as exc:
                logger.warning("404 spike check hatası [%s/%s]: %s", site.domain, profile, exc)
    return results


def send_realtime_email_for_alarm(alarm: dict[str, Any]) -> bool:
    """Tekil bir alarm için e-posta gönderir (hibrit gönderim mantığı için)."""
    from backend.database import SessionLocal
    from backend.services.mailer import send_realtime_email

    domain = alarm.get("domain") or alarm.get("site_domain") or "Site"
    profile = alarm.get("profile") or "web"
    detail_top_html = ""
    if _domain_shows_web_mweb_top_detail(str(domain)):
        try:
            with SessionLocal() as db:
                detail_top_html = _detail_top_html_for_alarm(db, alarm)
        except Exception:
            logger.exception("Realtime alarm detail top content skipped")
    rule_id = alarm.get("rule_id", "")
    if rule_id.startswith("news_"):
        subject = _email_news_alarm_subject(domain, profile, [alarm])
        html_body = _html_news_alarm_body(
            domain, profile, [alarm], site_kpi={}, detail_top_html=detail_top_html
        )
        thread_kind = "news"
    elif rule_id.startswith("page_"):
        subject = _email_page_alarm_subject(domain, profile, [alarm])
        html_body = _html_page_alarm_body(domain, profile, [alarm], detail_top_html=detail_top_html)
        thread_kind = "page"
    else:
        subject = _email_site_alarm_subject(domain, profile, [alarm])
        html_body = _html_site_alarm_body(domain, profile, [alarm], detail_top_html=detail_top_html)
        thread_kind = "site"
        
    thread_key = _realtime_email_thread_key(domain, profile)
    return send_realtime_email(subject, html_body, thread_kind=thread_kind, thread_key=thread_key)


# ── App Event Spike Alarm (Android / iOS) ───────────────────────────────────

APP_EVENT_SPIKE_THRESHOLD_PCT = 40
APP_EVENT_MIN_COUNT = 50  # min eventCount eşiği — düşük trafikli event'leri filtrele
APP_EVENT_PEAK_DROP_PCT = 55  # zirveden %55+ düşüş alarm tetikler
APP_EVENT_PEAK_MIN_COUNT = 80  # zirvede en az bu kadar event olmalı

# Gürültü oluşturan teknik event'ler
_APP_EVENT_BLACKLIST = {
    "session_start", "first_open", "user_engagement", "screen_view",
    "app_remove", "app_clear_data", "app_update", "os_update",
}


def save_app_event_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    events: list[dict[str, Any]],
) -> None:
    """Şu anki event sayımlarını DB'ye yazar (zirve karşılaştırması için)."""
    from backend.models import RealtimeAppEventSnapshot

    for e in events[:200]:
        name = str(e.get("eventName") or "").strip()[:200]
        if not name or name.lower() in _APP_EVENT_BLACKLIST:
            continue
        cnt = float(e.get("eventCount") or 0)
        if cnt <= 0:
            continue
        snap = RealtimeAppEventSnapshot(
            site_id=site_id,
            profile=profile,
            event_name=name,
            event_count=cnt,
        )
        db.add(snap)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("RealtimeAppEventSnapshot kayıt hatası (site_id=%s)", site_id)


def get_peak_app_event_snapshots(
    db: Session,
    site_id: int,
    profile: str,
    window_minutes: int = 90,
) -> dict[str, int]:
    """Son `window_minutes` dakikadaki her event için maksimum eventCount değerini döner."""
    from backend.models import RealtimeAppEventSnapshot
    from sqlalchemy import func as sqlfunc

    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
    rows = (
        db.query(
            RealtimeAppEventSnapshot.event_name,
            sqlfunc.max(RealtimeAppEventSnapshot.event_count).label("peak_count"),
        )
        .filter(
            RealtimeAppEventSnapshot.site_id == site_id,
            RealtimeAppEventSnapshot.profile == profile,
            RealtimeAppEventSnapshot.collected_at >= cutoff,
        )
        .group_by(RealtimeAppEventSnapshot.event_name)
        .all()
    )
    return {row.event_name: int(row.peak_count or 0) for row in rows}


def _html_app_event_alarm_body(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    neg_alarms = [a for a in alarms if str(a.get("rule_id", "")) in NEGATIVE_ALARM_RULE_IDS]
    pos_alarms = [a for a in alarms if str(a.get("rule_id", "")) not in NEGATIVE_ALARM_RULE_IDS]
    neg_sorted = sorted(neg_alarms, key=lambda a: int(a.get("current_count", a.get("peak_count", 0))), reverse=True)[:5]
    pos_sorted = sorted(pos_alarms, key=lambda a: int(a.get("current_count", 0)), reverse=True)[:5]
    alarms = sorted(pos_sorted + neg_sorted, key=lambda a: int(a.get("current_count", a.get("peak_count", 0))), reverse=True)

    cards = []
    for a in alarms:
        evt_name = str(a.get("event_name", ""))
        evt = html.escape(evt_name)
        curr = int(a.get("current_count", 0))
        prev = int(a.get("previous_count", 0))
        rid = str(a.get("rule_id", ""))
        is_peak_drop = rid == "app_event_peak_drop"
        is_drop = is_peak_drop or (curr - prev) < 0
        _neg_kw = ("failed", "error", "crash", "exception", "timeout", "denied")
        is_negative_event = any(kw in evt_name.lower() for kw in _neg_kw)
        is_bad = (not is_drop) if is_negative_event else is_drop
        border = "#dc2626" if is_bad else "#16a34a"
        bg = "#fef2f2" if is_bad else "#f0fdf4"
        num_c = "#dc2626" if is_bad else "#16a34a"

        if is_peak_drop:
            peak = int(a.get("peak_count", prev))
            drop_pct = int(a.get("drop_pct", 0))
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{peak:,}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{curr:,}</span>'
                f'<span style="font-size:13px;font-weight:800;color:{num_c};margin-left:8px;">−{drop_pct}%</span>'
            )
        else:
            delta = curr - prev
            sign = "+" if delta >= 0 else ""
            pct = float(a.get("change_pct", 0))
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{prev:,}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr:,}</span>'
                f'<span style="font-size:14px;font-weight:800;color:{num_c};margin-left:8px;">{sign}{delta} ({pct:+.0f}%)</span>'
            )

        cards.append(
            f'<div style="margin:10px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:15px;font-weight:800;color:#0f172a;font-family:monospace;">{evt}</p>'
            f'<div>{metric_html}</div></div>'
        )

    pre_parts = [f"{a.get('event_name','')[:18]}:{int(a.get('current_count',0))}" for a in alarms[:5]]
    preheader = " · ".join(pre_parts) or f"{len(alarms)} event"

    return (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:600px;color:#0f172a;">'
        f'{_preheader(preheader)}'
        f'{_html_email_section_header(domain, profile)}'
        f'{"".join(cards)}'
        f'</div>'
    )


def check_app_event_spike_for_site(
    db: Session,
    site: "Site",
    *,
    profile: str = "android",
    window_minutes: int = 30,
    skip_emails: bool = False,
) -> dict[str, Any]:
    """Tek site+profil için app event spike kontrolü (android/ios)."""
    if profile not in ("android", "ios"):
        return {}

    record = get_ga4_credentials_record(db, site.id)
    if not record:
        return {}
    properties = load_ga4_properties(record)
    prop_id = str(properties.get(profile, "")).strip()
    if not prop_id:
        return {}

    try:
        result = fetch_realtime_top_events(
            prop_id, window_minutes=window_minutes, limit=200, compare_previous=True,
        )
    except Exception as exc:
        logger.warning("App event çekim hatası [%s/%s]: %s", site.domain, profile, exc)
        return {}

    events = result.get("events", []) or []

    # Zirve karşılaştırması için DB snapshot'larından son 90 dk'nın zirve değerlerini al
    peak_events = get_peak_app_event_snapshots(db, site.id, profile, window_minutes=90)
    # Mevcut event'leri DB'ye yaz (sonraki check'lerde zirve karşılaştırması için)
    save_app_event_snapshots(db, site.id, profile, events)

    alarms = []
    seen_event_names: set[str] = set()
    for e in events:
        name = str(e.get("eventName") or "").strip()
        if not name or name.lower() in _APP_EVENT_BLACKLIST:
            continue
        curr = int(e.get("eventCount") or 0)
        prev = int(e.get("eventCount_previous") or 0)
        if curr < APP_EVENT_MIN_COUNT and prev < APP_EVENT_MIN_COUNT:
            continue
        if prev == 0:
            continue
        pct = (curr - prev) / prev * 100
        if abs(pct) < APP_EVENT_SPIKE_THRESHOLD_PCT:
            continue
        alarms.append({
            "event_name": name,
            "current_count": curr,
            "previous_count": prev,
            "change_pct": round(pct, 1),
            "rule_id": "app_event_spike" if pct > 0 else "app_event_drop",
        })
        seen_event_names.add(name)

    # Zirve düşüş kontrolü — son 90 dk'da zirve yapıp şimdi belirgin düşen event'ler
    curr_map: dict[str, int] = {
        str(e.get("eventName") or "").strip(): int(e.get("eventCount") or 0)
        for e in events
    }
    for event_name, peak_count in peak_events.items():
        if event_name in seen_event_names:
            continue  # spike/drop zaten tetiklendi, çift alarm verme
        if peak_count < APP_EVENT_PEAK_MIN_COUNT:
            continue
        if event_name.lower() in _APP_EVENT_BLACKLIST:
            continue
        curr_count = curr_map.get(event_name, 0)
        drop_pct = ((peak_count - curr_count) / peak_count) * 100
        if drop_pct >= APP_EVENT_PEAK_DROP_PCT:
            alarms.append({
                "event_name": event_name,
                "current_count": curr_count,
                "previous_count": peak_count,
                "peak_count": peak_count,
                "drop_pct": round(drop_pct),
                "change_pct": -round(drop_pct, 1),
                "rule_id": "app_event_peak_drop",
            })

    if not alarms:
        return {"alarms": []}

    # Cooldown — RealtimeAlarmLog üzerinden; negatif ve pozitif ayrı değerlendirilir
    if not skip_emails:
        negatives, positives = _split_alarms_by_sentiment(alarms)
        to_send: list[dict[str, Any]] = []
        if positives:
            if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in positives], profile=profile):
                logger.info("App event pozitif cooldown — atlandı (site=%s/%s).", site.domain, profile)
            else:
                to_send.extend(positives)
        if negatives:
            if _alarm_email_suppressed(db, site.id, [a["rule_id"] for a in negatives], profile=profile):
                logger.info("App event negatif cooldown — atlandı (site=%s/%s).", site.domain, profile)
            else:
                to_send.extend(negatives)

        if to_send:
            from backend.services.mailer import send_realtime_email

            to_send = filter_alarms_for_email(_cap_top_n_each_side(to_send))
        if to_send:
            html_body = _html_app_event_alarm_body(site.domain, profile, to_send)
            # Konuda negatifi de garantile: 2 pozitif + 1 negatif (varsa) — max 3 chip
            negs_to_send, poss_to_send = _split_alarms_by_sentiment(to_send)
            top_pos = sorted(poss_to_send, key=lambda a: abs(a.get("change_pct", 0)), reverse=True)[:2]
            top_neg = sorted(negs_to_send, key=lambda a: abs(a.get("change_pct", 0)), reverse=True)[:2]
            top_a = (top_pos + top_neg)[:3] if (top_pos or top_neg) else to_send[:3]
            chips = []
            for a in top_a:
                evt_short = a['event_name'][:14]
                if a.get("rule_id") == "app_event_peak_drop":
                    chips.append(f"{evt_short} ↓{int(a.get('current_count', 0))} (zirve {int(a.get('peak_count', 0))})")
                else:
                    chips.append(f"{evt_short} {('+' if a['change_pct']>=0 else '')}{a['change_pct']:.0f}%")
            subject = f"{_email_site_short_label(site.domain)} — {' · '.join(chips)} [{profile}]"
            thread_key = _realtime_email_thread_key(site.domain, profile)
            send_realtime_email(subject, html_body, thread_kind="app_event", thread_key=thread_key)

            # Cooldown için log — _save_alarm_logs beklenen tüm alanlarla çağrılmalı
            _save_alarm_logs(db, site.id, [
                {
                    "rule_id": a["rule_id"],
                    "message": a["event_name"],
                    "metric": "eventCount",
                    "current_value": float(a.get("current_count", 0)),
                    "previous_value": float(a.get("previous_count", a.get("peak_count", 0))),
                    "change_pct": float(a.get("change_pct", 0)),
                    "severity": "warning",
                }
                for a in to_send
            ], profile=profile)

    return {"alarms": alarms}


def run_app_event_spike_check_all_sites(db: Session, *, skip_emails: bool = False) -> list[dict]:
    """Tüm siteler için android+ios profillerinde event spike kontrolü."""
    from backend.models import Site as SiteModel
    results = []
    sites = _sort_sites(db.query(SiteModel).all())
    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        if not record:
            continue
        for profile in ("android", "ios"):
            try:
                r = check_app_event_spike_for_site(db, site, profile=profile, skip_emails=skip_emails)
                if r and r.get("alarms"):
                    r["site_id"] = site.id
                    r["domain"] = site.domain
                    r["profile"] = profile
                    results.append(r)
            except Exception as exc:
                logger.warning("App event check hatası [%s/%s]: %s", site.domain, profile, exc)
    return results
