"""GA4 Realtime API — pencereli karşılaştırma ve alarm değerlendirmesi.

Son 30 dakikayı iki pencereye böler (ör. 0-9 dk vs 10-19 dk) ve
activeUsers/screenPageViews değişimini izleyerek alarm tetikler.
"""

from __future__ import annotations

import copy
import hashlib
import html
import logging
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


def _realtime_email_thread_key(domain: str, profile: str) -> str:
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


def _email_metric_plain_tr(metric: str) -> str:
    """E-posta gövdesinde okunabilir metrik adı."""
    m = (metric or "").strip()
    return {
        "activeUsers": "Aktif kullanıcı (activeUsers)",
        "screenPageViews": "Sayfa görüntüleme (screenPageViews)",
    }.get(m, m)


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


def _html_site_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    """Site metrik alarmları — kompakt kart, preview'da okunabilir."""
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    cards: list[str] = []
    for alarm in alarms:
        metric_key = str(alarm.get("metric", "activeUsers"))
        metric_tr = html.escape(_email_metric_plain_tr(metric_key))
        cur = int(alarm.get("current_value", 0))
        prev = int(alarm.get("previous_value", 0))
        pct = float(alarm.get("change_pct", 0.0))
        delta = cur - prev
        is_drop = pct < 0
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        pct_c = "#dc2626" if is_drop else "#16a34a"
        sign = "+" if delta >= 0 else ""

        if prev > 0:
            metric_row = (
                f'<div style="display:flex;align-items:flex-end;gap:10px;flex-wrap:wrap;">'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Önceki yarı</div>'
                f'<span style="font-size:26px;font-weight:900;color:#475569;">{prev}</span>'
                f'</div>'
                f'<span style="font-size:20px;color:#94a3b8;padding-bottom:4px;">→</span>'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Şimdiki yarı</div>'
                f'<span style="font-size:26px;font-weight:900;color:#0f172a;">{cur}</span>'
                f'</div>'
                f'<span style="font-size:20px;font-weight:800;color:{pct_c};padding-bottom:4px;">{pct:+.1f}%</span>'
                f'</div>'
            )
        else:
            metric_row = (
                f'<div style="display:flex;align-items:baseline;gap:6px;">'
                f'<span style="font-size:28px;font-weight:900;color:#0f172a;">{cur}</span>'
                f'<span style="font-size:13px;color:#64748b;">aktif</span>'
                f'</div>'
            )

        cards.append(
            f'<div style="margin:8px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<div style="font-size:11px;color:#64748b;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em;font-weight:600;">{metric_tr} · {prof_e}</div>'
            f'{metric_row}'
            f'</div>'
        )

    # Özet — preview'da ilk satırda okunur
    summary_parts = []
    for a in alarms:
        cur = int(a.get("current_value", 0))
        prev = int(a.get("previous_value", 0))
        delta = cur - prev
        sign = "+" if delta >= 0 else ""
        metric_short = {"activeUsers": "kul", "screenPageViews": "gör"}.get(str(a.get("metric", "")), "")
        if prev > 0:
            pct_a = float(a.get("change_pct", 0.0))
            summary_parts.append(f"{prev}→{cur} ({pct_a:+.0f}%) {metric_short}".strip())
        else:
            summary_parts.append(f"{cur} {metric_short}".strip())
    summary_line = html.escape(" · ".join(summary_parts))

    # Sürücü Analizi (Eğer varsa)
    driver_html = ""
    first_alarm = alarms[0] if alarms else {}
    drivers = first_alarm.get("drivers", [])
    if drivers:
        site_delta = first_alarm.get("current_value", 0) - first_alarm.get("previous_value", 0)
        driver_html = _html_driver_analysis_section(drivers, site_delta)

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            <p style="font-size:15px;font-weight:700;margin:0 0 2px;">{dom_e} <span style="font-weight:400;color:#64748b;font-size:13px;">· {prof_e}</span></p>
            <p style="font-size:13px;font-weight:600;color:#475569;margin:0 0 10px;">{summary_line}</p>
            {''.join(cards)}
            {driver_html}
            <p style="color:#94a3b8;font-size:11px;margin-top:14px;">SEO Agent · GA4 Realtime (otomatik)</p>
        </div>
        """


def _html_page_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    dom_e = html.escape(domain)
    cards: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "")
        title = _rt_alarm_screen_title_one_line(page, max_len=70) or page
        title_e = html.escape(title)
        row_url = _alarm_row_public_url(domain, "page:" + str(page))
        # Başlık tıklanabilir olsun — row_url varsa <a> ile sar
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
        delta = curr - prev
        sign = "+" if delta >= 0 else ""

        if rid == "page_disappeared":
            metric_html = (f'<span style="font-size:24px;font-weight:900;color:{pct_c};">{prev}</span>'
                           f'<span style="font-size:13px;color:#64748b;margin-left:6px;">kul. vardı · listeden çıktı</span>')
        elif rid == "page_new_entry":
            metric_html = (f'<span style="font-size:24px;font-weight:900;color:{pct_c};">{curr}</span>'
                           f'<span style="font-size:13px;color:#64748b;margin-left:6px;">aktif kullanıcı · yeni giriş</span>')
        elif prev > 0:
            metric_html = (
                f'<div style="display:flex;align-items:flex-end;gap:8px;flex-wrap:wrap;">'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Önceki yarı</div>'
                f'<span style="font-size:22px;font-weight:900;color:#475569;">{prev}</span>'
                f'</div>'
                f'<span style="font-size:16px;color:#94a3b8;padding-bottom:3px;">→</span>'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Şimdiki yarı</div>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr}</span>'
                f'</div>'
                f'<span style="font-size:16px;font-weight:800;color:{pct_c};padding-bottom:3px;">{pct:+.1f}%</span>'
                f'</div>'
            )
        else:
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr}</span>'
                f'<span style="font-size:13px;color:#64748b;margin-left:6px;">aktif kullanıcı</span>'
            )

        # URL listesi — pagePath varsa başlığın altında göster
        paths_html = ""
        page_paths = alarm.get("page_paths") or []
        if page_paths:
            path_items = "".join(
                f'<div style="font-size:11px;color:#2563eb;font-family:monospace;margin-top:2px;">'
                f'<a href="https://{domain}{html.escape(p)}" style="color:#2563eb;">{html.escape(p)}</a>'
                f'</div>'
                for p in page_paths[:5]
            )
            paths_html = f'<div style="margin-top:4px;">{path_items}</div>'
            if len(page_paths) > 5:
                paths_html += f'<div style="font-size:11px;color:#94a3b8;margin-top:2px;">+{len(page_paths)-5} URL daha…</div>'

        cards.append(
            f'<div style="margin:8px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:14px;font-weight:800;line-height:1.3;">{title_html}</p>'
            f'{paths_html}'
            f'<div style="display:flex;align-items:baseline;gap:4px;flex-wrap:wrap;margin-top:6px;">{metric_html}</div>'
            f'</div>'
        )

    # Özet satırı — preview'da görünür
    chips = []
    for a in alarms[:10]:
        t = _rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=18)
        c = int(a.get("current_users", 0))
        p2 = int(a.get("previous_users", 0))
        d = c - p2
        chips.append(f"{t} {'+' if d >= 0 else ''}{d}")
    summary = html.escape(" · ".join(chips) + (f" +{len(alarms)-10}" if len(alarms) > 10 else ""))
    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            <p style="font-size:15px;font-weight:700;margin:0 0 2px;">{dom_e} <span style="font-weight:400;color:#64748b;font-size:13px;">· {html.escape(profile_label)}</span></p>
            <p style="font-size:13px;font-weight:600;color:#475569;margin:0 0 10px;">{summary}</p>
            {''.join(cards)}
            <p style="color:#94a3b8;font-size:11px;margin-top:14px;">SEO Agent · GA4 Realtime sayfa listesi (otomatik)</p>
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


def _html_news_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]], site_kpi: dict | None = None) -> str:
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    # Negatif (düşüş/kaybolma/zirveden düşüş) ve pozitif alarmları ayır — her ikisinin de
    # mailde kesinlikle yer alması için ayrı kotalarla seç: max 8 pozitif + max 7 negatif
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

        # Metrik satırı: her alarm tipine özel, anlamlı
        if rid == "news_new_entry":
            metric_html = (
                f'<span style="font-size:28px;font-weight:900;color:{num_c};">{curr}</span>'
                f'<span style="font-size:13px;color:#64748b;margin-left:6px;">aktif kullanıcı · yeni giriş</span>'
            )
        elif rid == "news_disappeared":
            metric_html = (
                f'<span style="font-size:28px;font-weight:900;color:{num_c};">{prev}</span>'
                f'<span style="font-size:13px;color:#64748b;margin-left:6px;">kul. vardı · listeden çıktı</span>'
            )
        elif rid == "news_peak_drop":
            peak = int(alarm.get("peak_users", prev))
            drop_pct = int(alarm.get("drop_pct", 0))
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{peak}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{curr}</span>'
                f'<span style="font-size:14px;font-weight:800;color:{num_c};margin-left:8px;">−{drop_pct}% · düştü</span>'
            )
        else:
            delta = curr - prev
            sign = "+" if delta >= 0 else ""
            direction_label = "arttı" if delta >= 0 else "düştü"
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{prev}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr}</span>'
                f'<span style="font-size:16px;font-weight:800;color:{num_c};margin-left:8px;">{sign}{delta} · {direction_label}</span>'
            )

        cards.append(
            f'<div style="margin:10px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:15px;font-weight:800;line-height:1.3;">{title_html}</p>'
            f'<div style="display:flex;align-items:baseline;gap:4px;flex-wrap:wrap;">{metric_html}</div>'
            f'</div>'
        )

    # Özet satırı — preview'da görünür (sıralı ilk 10)
    entries = [_rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=20) for a in alarms]
    summary_line = " · ".join(f for f in entries if f and f != "—")
    n = len(alarms)
    head = html.escape(f"{n} haber · {summary_line}")

    # Genel trafik özeti banner'ı
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
        pct_sign  = "" if pct_kpi < 0 else "+"
        if prev_kpi > 0:
            kpi_html = (
                f'<div style="margin:0 0 14px;padding:10px 14px;border-radius:8px;'
                f'border-left:4px solid {kpi_border};background:{kpi_bg};">'
                f'<div style="font-size:10px;color:#64748b;font-weight:600;text-transform:uppercase;'
                f'letter-spacing:.06em;margin-bottom:4px;">Genel Trafik · {prof_e}</div>'
                f'<div style="display:flex;align-items:flex-end;gap:8px;flex-wrap:wrap;">'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Önceki yarı</div>'
                f'<span style="font-size:22px;font-weight:900;color:#475569;">{prev_kpi:,}</span>'
                f'</div>'
                f'<span style="font-size:16px;color:#94a3b8;padding-bottom:2px;">→</span>'
                f'<div style="text-align:center;">'
                f'<div style="font-size:10px;color:#94a3b8;margin-bottom:2px;">Şimdiki yarı</div>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{cur_kpi:,}</span>'
                f'</div>'
                f'<span style="font-size:16px;font-weight:800;color:{kpi_color};padding-bottom:2px;">'
                f'{pct_sign}{pct_kpi:.1f}%</span>'
                f'</div>'
                f'</div>'
            )
        else:
            kpi_html = (
                f'<div style="margin:0 0 14px;padding:8px 14px;border-radius:8px;'
                f'background:#f8fafc;border-left:4px solid #e2e8f0;">'
                f'<span style="font-size:11px;color:#64748b;">Genel Trafik · {prof_e}: '
                f'<strong style="color:#0f172a;">{cur_kpi:,}</strong> aktif kullanıcı</span>'
                f'</div>'
            )

    # Preheader: KPI varsa önce trafik sayısı, sonra haber özeti
    kpi = site_kpi or {}
    pre_parts: list[str] = []
    if kpi.get("current"):
        cur_k = int(kpi["current"])
        pct_k = float(kpi.get("change_pct", 0))
        sign  = "+" if pct_k >= 0 else ""
        pre_parts.append(f"{cur_k:,} kul. {sign}{pct_k:.0f}%")
    for a in alarms[:10]:
        title = _rt_alarm_screen_title_one_line(str(a.get("page", "")), max_len=18)
        curr  = int(a.get("current_users", 0))
        if title and title != "—":
            pre_parts.append(f"{title}: {curr}")
    preheader_str = " · ".join(pre_parts) if pre_parts else f"{len(alarms)} haber alarmı"

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
            {_preheader(preheader_str)}
            <p style="font-size:15px;font-weight:700;margin:0 0 4px;">{dom_e} <span style="font-weight:400;color:#64748b;font-size:13px;">· {prof_e}</span></p>
            <p style="font-size:13px;font-weight:600;color:#475569;margin:0 0 12px;">{head}</p>
            {kpi_html}
            {''.join(cards)}
            <p style="color:#94a3b8;font-size:12px;margin-top:16px;">SEO Agent · GA4 Realtime haberler (otomatik)</p>
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
        "min_baseline": 3,
        "severity": "critical",
    },
    "traffic_spike": {
        "label": "Aktif kullanıcılar",
        "metric": "activeUsers",
        "direction": "spike",
        "threshold_pct": 40,
        "min_baseline": 3,
        "severity": "warning",
    },
    "pageview_drop": {
        "label": "Sayfa görüntülemeleri",
        "metric": "screenPageViews",
        "direction": "drop",
        "threshold_pct": 40,
        "min_baseline": 5,
        "severity": "warning",
    },
}

PAGE_ALARM_RULES: dict[str, dict[str, Any]] = {
    "page_traffic_drop": {
        "label": "Sayfa trafiği düşüşü",
        "direction": "drop",
        "threshold_pct": 40,
        "min_users": 15,
        "severity": "warning",
    },
    "page_traffic_spike": {
        "label": "Sayfa trafiği artışı",
        "direction": "spike",
        "threshold_pct": 60,
        "min_users": 15,
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


# Bir maile dahil edilecek azami pozitif/negatif alarm sayısı — istek üzerine 10+10.
ALARM_EMAIL_TOP_N = 10


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
    """sinemalar için tüm ``threshold_pct`` = 50; döviz için 30; mutlak tabanlar (min_*) de buna göre ölçeklenir."""
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
    scale = target_pct / 100.0
    for _rid, rule in out.items():
        if "threshold_pct" in rule:
            rule["threshold_pct"] = target_pct
        for key in ("min_users", "min_prev_users", "min_baseline", "min_peak_users"):
            if key in rule and isinstance(rule[key], (int, float)):
                rule[key] = max(1, int(round(float(rule[key]) * scale)))
    return out


# Varsayılan pencere boyutu (dakika)
DEFAULT_WINDOW_MINUTES = 10


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
    include_page_path: bool = True,   # pagePath ikinci dimension olarak ekle
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API ile son N dakikadaki top sayfaları çeker.

    compare_previous: True ise önceki pencereyle (window_minutes kadar öncesi) karşılaştırma metriklerini de döner.
    sort_by: "activeUsers" veya "screenPageViews" — sıralama kriteri.
    include_page_path: True ise pagePath ikinci dimension olarak eklenir; her sayfa için URL listesi de döner.
    """
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
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

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=dims,
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="screenPageViews"),
        ],
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
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                val = float(mv.value)
                entry[mname + suffix] = entry.get(mname + suffix, 0.0) + val
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


_DEFAULT_NEWS_SCREEN_EXCLUDE_PREFIXES: tuple[str, ...] = (
    "(other)",
    "(not set)",
    "canlı döviz",
    "canlı gram",
    "canlı euro",
    "canlı usd",
    "canlı sterlin",
    "güncel euro",
    "güncel altın fiyatları",
    "güncel altın",
    "anlık altın",
    "altın fiyatları",
    "serbest piyasa",
    "hisse senedi",
    "harem ",
    "döviz çevirici",
    "canlı dolar",
    "canlı borsa",
    "çerez politikası",
    "vizyondaki filmler",
    "türkiye'nin sinema",
    "en iyi animasyon",
    "en iyi türk",
    "en iyi korku",
    "en iyi romantik",
    "en iyi aksiyon",
    "tüm filmler",
    "tüm zamanların",
    "gündem haberleri",
)


def _news_screen_exclude_prefixes_loaded() -> tuple[str, ...]:
    from backend.config import settings

    raw = (getattr(settings, "ga4_realtime_news_screen_exclude_prefixes", None) or "").strip()
    if raw:
        return tuple(x.strip().lower() for x in raw.split(",") if x.strip())
    return _DEFAULT_NEWS_SCREEN_EXCLUDE_PREFIXES


def _screen_unified_news_candidate(name: str) -> bool:
    """Realtime'ta yalnızca unifiedScreenName (≈ başlık) olduğundan sezgisel haber adayı."""
    from backend.collectors.ga4 import _is_news_article_path

    n = (name or "").strip()
    if len(n) < 10:
        return False
    low = n.lower()
    if low.startswith("/") and _is_news_article_path(n):
        return True
    for pre in _news_screen_exclude_prefixes_loaded():
        if low.startswith(pre):
            return False
    return True


def _news_row_link(site_domain: str, unified: str) -> str:
    """Yalnızca makale path'i görünürse doğrudan site URL'si; aksi halde boş (harici arama yönlendirmesi yok)."""
    from backend.collectors.ga4 import _is_news_article_path

    d = (site_domain or "").strip().lower().replace("https://", "").replace("http://", "").strip("/")
    u = (unified or "").strip()
    if u.startswith("/") and d and _is_news_article_path(u):
        return "https://" + d + u
    return ""


def fetch_realtime_top_news_pages(
    property_id: str,
    *,
    site_domain: str = "",
    window_minutes: int = 30,
    limit: int = 12,
    sort_by: str = "activeUsers",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime «Haberler»: GA4 Realtime şemasında pagePath olmadığı için unifiedScreenName + sezgisel filtre.

    ``link_url`` yalnızca başlık site içi makale path'i ise doldurulur; aksi halde boştur.
    """
    fetch_n = min(250, max(80, int(limit) * 25))
    base = fetch_realtime_top_pages(
        property_id,
        window_minutes=window_minutes,
        limit=fetch_n,
        sort_by=sort_by,
        dimension="unifiedScreenName",
        compare_previous=False,
        include_page_path=False,
        client=client,
    )
    out: list[dict[str, Any]] = []
    for p in base.get("pages") or []:
        title = str(p.get("page") or "").strip()
        if not _screen_unified_news_candidate(title):
            continue
        out.append(
            {
                "page": title,
                "page_path": title if title.startswith("/") else "",
                "activeUsers": float(p.get("activeUsers") or 0),
                "screenPageViews": float(p.get("screenPageViews") or 0),
                "activeUsers_previous": float(p.get("activeUsers_previous") or 0),
                "screenPageViews_previous": float(p.get("screenPageViews_previous") or 0),
                "link_url": _news_row_link(site_domain, title),
            }
        )
        if len(out) >= max(1, min(int(limit), 250)):
            break

    return {
        "property_id": property_id,
        "window_minutes": base.get("window_minutes", window_minutes),
        "pages": out,
        "total_pages": len(out),
        "fetched_at": base.get("fetched_at") or datetime.now(timezone.utc).isoformat(),
        "api_ms": base.get("api_ms", 0),
        "breakdown": "unifiedScreenName+news_heuristic",
        "comparison_enabled": True,
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
    best_key: tuple[int, int] = (-1, -1)
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
        key = (labeled, total)
        if key > best_key:
            best_key = key
            best = res
            best["breakdown"] = dim
        if total > 0 and labeled >= 3 and labeled / total >= 0.45:
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
    """Mobil: önce en iyi ekran boyutu; etiketler zayıfsa eventName kırılımına düşer."""
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
    labeled, total = _realtime_screen_label_quality(pages)
    weak = labeled < 2 or (total > 0 and labeled / total < 0.35)
    if not weak:
        return base

    try:
        alt = fetch_realtime_top_event_names(
            property_id,
            window_minutes=window_minutes,
            limit=limit,
            sort_by=sort_by,
            compare_previous=compare_previous,
            client=client,
        )
        return alt
    except Exception as exc:
        logger.warning(
            "Realtime app fallback (eventName) başarısız [%s / %s]: %s",
            property_id,
            profile,
            exc,
        )
        return base

    alt_pages = alt.get("pages") or []
    if not alt_pages:
        return base

    alt["replaced_unified_screen"] = True
    alt["unified_screen_labeled_rows"] = labeled
    alt["unified_screen_total_rows"] = total
    return alt


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


def _analyze_traffic_drivers(db: Session, site_id: int, profile: str, site_delta: float) -> list[dict[str, Any]]:
    """Site genelindeki trafik değişimine hangi sayfaların ne kadar katkıda bulunduğunu analiz eder."""
    try:
        if abs(site_delta) < 5:
            return []

        from backend.models import RealtimePageSnapshot
        from sqlalchemy import desc

        # En son 2 farklı collected_at zaman damgasını bul
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
            return []

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
            return []

        drivers = []
        for path in set(curr_map) | set(prev_map):
            c = curr_map.get(path, 0)
            p = prev_map.get(path, 0)
            diff = c - p
            if diff != 0:
                contribution = (diff / site_delta * 100) if site_delta else 0
                drivers.append({
                    "page": path,
                    "delta": diff,
                    "contribution_pct": contribution,
                    "current": c,
                    "previous": p,
                })

        # Düşüşte en çok düşenler, artışta en çok artanlar önce
        drivers.sort(key=lambda x: x["delta"], reverse=site_delta > 0)
        return drivers[:5]

    except Exception:
        logger.exception("_analyze_traffic_drivers hatası (site_id=%s)", site_id)
        return []

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
        dim_filter = FilterExpression(
            filter=Filter(
                field_name="deviceCategory",
                string_filter=Filter.StringFilter(value="mobile"),
            )
        )
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
            if drivers:
                # İlk alarma sürücüleri ekle (mail body'de kullanmak için)
                alarms[0]["drivers"] = drivers
                
        logger.info("GA4 Realtime: %d alarm bulundu (site=%s, profile=%s).", len(alarms), site.domain, profile)
        if not skip_emails:
            rule_ids = [a["rule_id"] for a in alarms]
            if _alarm_email_suppressed(db, site.id, rule_ids, profile=profile):
                logger.info("GA4 Realtime: E-posta cooldown aktif, gönderim atlandı (site=%s, profile=%s).", site.domain, profile)
            else:
                if _send_site_alarm_emails(site.domain, profile, alarms):
                    _mark_alarms_emailed(db, site.id, rule_ids, profile=profile)
        logger.warning(
            "GA4 Realtime ALARM [%s]: %d kural tetiklendi — %s",
            site.domain,
            len(alarms),
            "; ".join(a["message"] for a in alarms),
        )
    else:
        logger.debug("GA4 Realtime: Alarm tetiklenmedi (site=%s, profile=%s).", site.domain, profile)

    return result


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


def _send_site_alarm_emails(domain: str, profile: str, alarms: list[dict[str, Any]]) -> bool:
    """Genel site alarmları — tek e-postada özet + Gmail iş parçacığı. True döner mail atıldıysa."""
    from backend.services.mailer import is_realtime_mail_ready, send_realtime_email

    if not alarms:
        return False
    if not is_realtime_mail_ready():
        logger.warning(
            "Realtime site alarmı tetiklendi (site=%s, alarm_count=%d) ancak e-posta gönderilemedi. "
            "Lütfen şu ayarları kontrol edin: GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP_HOST, MAIL_TO.",
            domain,
            len(alarms),
        )
        return False

    logger.info("GA4 Realtime: E-posta hazırlanıyor (site=%s, profile=%s)...", domain, profile)
    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    html_body = _html_site_alarm_body(domain, profile_label, alarms)
    subject = _email_site_alarm_subject(domain, profile, alarms)
    ok = send_realtime_email(subject, html_body, thread_kind="site", thread_key=thread_key)
    if ok:
        logger.info("GA4 Realtime: E-posta başarıyla gönderildi (site=%s, profile=%s).", domain, profile)
    else:
        logger.error("GA4 Realtime: E-posta gönderimi başarısız (site=%s, profile=%s).", domain, profile)
    return bool(ok)


def get_recent_snapshots(
    db: Session,
    site_id: int,
    *,
    profile: str = "web",
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Son N snapshot kaydını döner (mini trend grafiği için)."""
    from backend.models import RealtimeSnapshot

    prof = (profile or "web").strip()
    rows = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == prof)
        .order_by(RealtimeSnapshot.collected_at.desc())
        .limit(limit)
        .all()
    )
    result = []
    for row in reversed(rows):
        result.append({
            "collected_at": _utc_db_datetime_iso_z(row.collected_at),
            "active_users": row.active_users_current,
            "active_users_prev": row.active_users_previous,
            "pageviews": row.pageviews_current,
            "pageviews_prev": row.pageviews_previous,
            "alarm_count": row.alarm_count,
            "window_minutes": row.window_minutes,
        })
    return result


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

    sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()
    results: list[dict[str, Any]] = []
    profile_order = ("web", "mweb", "ios", "android")
    for site in sites:
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        base_web = (properties.get("web") or "").strip()
        if not base_web and not any((properties.get(p) or "").strip() for p in profile_order):
            continue
        for profile in profile_order:
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
                if _send_page_alarm_email(site.domain, profile, _cap_top_n_each_side(to_send)):
                    _mark_alarms_emailed(db, site.id, [a["rule_id"] for a in to_send], profile=profile)

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


def _send_page_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]]) -> bool:
    """Sayfa bazlı alarmlar — tek e-postada özet + Gmail iş parçacığı. True döner mail atıldıysa."""
    from backend.services.mailer import is_page_alarm_mail_ready, send_realtime_email

    if not alarms:
        return False
    if not is_page_alarm_mail_ready():
        logger.warning(
            "Realtime sayfa alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP ve MAIL_TO yapılandırmasını kontrol edin.",
            len(alarms),
        )
        return False

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    html_body = _html_page_alarm_body(domain, profile_label, alarms)
    subject = _email_page_alarm_subject(domain, profile, alarms)
    return bool(send_realtime_email(subject, html_body, thread_kind="page", thread_key=thread_key))


def run_page_alarm_check_all_sites(
    db: Session,
    *,
    window_minutes: int = 30,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    """Tüm aktif siteler ve profilleri için sayfa bazlı alarm kontrolü."""
    from backend.models import Site as SiteModel

    all_alarms: list[dict[str, Any]] = []
    sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()

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


def _send_news_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]], site_kpi: dict | None = None) -> bool:
    """Haber trafiği alarmları — tek e-postada özet + Gmail iş parçacığı. True döner mail atıldıysa."""
    from backend.services.mailer import is_news_realtime_mail_ready, send_realtime_news_email

    if not alarms:
        return False
    if not is_news_realtime_mail_ready():
        logger.warning(
            "Realtime haber alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_NEWS_ALERT_EMAIL, SMTP ve MAIL_TO.",
            len(alarms),
        )
        return False

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    html_body = _html_news_alarm_body(domain, profile_label, alarms, site_kpi=site_kpi or {})
    subject = _email_news_alarm_subject(domain, profile, alarms)
    return bool(send_realtime_news_email(subject, html_body, thread_kind="news", thread_key=thread_key))


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
                site_kpi = _get_site_kpi_summary(db, site.id, profile)
                if _send_news_alarm_email(site.domain, profile, _cap_top_n_each_side(to_send), site_kpi=site_kpi):
                    _mark_alarms_emailed(db, site.id, [a["rule_id"] for a in to_send], profile=profile)

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
    sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()

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
    # Siteleri alfabetik, içindeki alarmları ise önem sırasına göre diz
    for dom in sorted(by_site.keys()):
        site_alarms = sorted(by_site[dom], key=lambda x: abs(float(x.get("change_pct", 0))), reverse=True)
        rows: list[str] = []
        for a in site_alarms[:10]: # Her site için de bir sınır koyalım ki çok uzamasın
            profile = a.get("profile", "web")
            profile_abbr = {"web": "DK", "mweb": "MW", "android": "AN", "ios": "IOS"}.get(profile, profile.upper()[:3])
            
            pct = float(a.get("change_pct", 0))
            color = "#dc2626" if pct < 0 else "#16a34a"
            arrow = "↓" if pct < 0 else "↑"
            
            # KPI alarmları için 'metric', Sayfa/Haber alarmları için 'page' alanını kullan
            metric_label = a.get("metric") or a.get("page") or "Metrik"
            if len(str(metric_label)) > 45:
                metric_label = str(metric_label)[:42] + "..."
            
            cur = a.get("current_value") or a.get("current_users") or 0
            prev = a.get("previous_value") or a.get("previous_users") or 0
            
            rows.append(f"""
                <tr style="border-bottom:1px solid #f1f5f9;">
                    <td style="padding:10px 8px;font-size:13px;color:#475569;width:40px;">{profile_abbr}</td>
                    <td style="padding:10px 8px;font-size:13px;color:#0f172a;font-weight:500;">{html.escape(str(metric_label))}</td>
                    <td style="padding:10px 8px;font-size:13px;color:#64748b;text-align:right;">{prev:,.0f} → {cur:,.0f}</td>
                    <td style="padding:10px 8px;font-size:14px;font-weight:700;color:{color};text-align:right;width:80px;">
                        {arrow} {abs(pct):.1f}%
                    </td>
                </tr>
            """)

        sections.append(f"""
            <div style="margin-bottom:24px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
                <div style="background:#f8fafc;padding:10px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#334155;">
                    {html.escape(dom)}
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
        <p style="font-size:11px;color:#94a3b8;margin-top:16px;">SEO Agent · Realtime (otomatik)</p>
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

_RT_404_TITLE_PATTERNS: tuple[str, ...] = (
    "sayfa bulunamadı",
    "page not found",
    "bulunamadı",
    "not found",
    "404",
    "hata sayfası",
    "error page",
)


def _is_rt_404_title(title: str) -> bool:
    t = (title or "").lower().strip()
    return any(p in t for p in _RT_404_TITLE_PATTERNS)


def fetch_realtime_404_users(
    property_id: str,
    window_minutes: int = 15,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """GA4 Realtime'dan 404 sayfasındaki aktif kullanıcıları çeker."""
    if client is None:
        client = _build_client()

    result = fetch_realtime_top_pages(
        property_id,
        window_minutes=window_minutes,
        limit=50,
        sort_by="activeUsers",
        dimension="unifiedScreenName",
        compare_previous=False,
        include_page_path=False,
        client=client,
    )

    pages_404 = [
        p for p in (result.get("pages") or [])
        if _is_rt_404_title(str(p.get("page") or ""))
    ]
    total_users = sum(int(p.get("activeUsers") or 0) for p in pages_404)

    return {
        "total_404_users": total_users,
        "pages": [
            {
                "title":       str(p.get("page") or ""),
                "activeUsers": int(p.get("activeUsers") or 0),
            }
            for p in pages_404
        ],
        "window_minutes": window_minutes,
    }


def _html_404_spike_body(
    domain: str,
    profile_label: str,
    current_users: int,
    pages: list[dict],
    severity: str,
    threshold: int,
) -> str:
    dom_e  = html.escape(domain)
    prof_e = html.escape(profile_label)
    border = "#dc2626" if severity == "critical" else "#f59e0b"
    bg     = "#fef2f2" if severity == "critical" else "#fffbeb"
    badge_c= "#dc2626" if severity == "critical" else "#d97706"
    sev_tr = "KRİTİK" if severity == "critical" else "UYARI"

    page_rows = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
        f'border-bottom:1px solid rgba(0,0,0,0.06);font-size:13px;">'
        f'<span style="color:#475569">{html.escape(p["title"][:60])}</span>'
        f'<strong style="color:{badge_c};margin-left:12px">{p["activeUsers"]} kul.</strong>'
        f'</div>'
        for p in pages[:8]
    )

    # Preheader
    top_pages_str = " · ".join(
        f'{html.escape(p["title"][:20])}: {p["activeUsers"]}'
        for p in pages[:3]
    ) if pages else ""
    pre404 = f"{current_users} kul. 404'te ({sev_tr})" + (f" · {top_pages_str}" if top_pages_str else "")

    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;color:#0f172a;">
          {_preheader(pre404)}
          <p style="font-size:15px;font-weight:700;margin:0 0 2px">{dom_e}
            <span style="font-weight:400;color:#64748b;font-size:13px">· {prof_e}</span>
          </p>
          <div style="margin:10px 0;padding:14px 16px;border-radius:10px;border-left:4px solid {border};background:{bg}">
            <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:{badge_c};margin-bottom:8px">
              {sev_tr} · 404 Spike
            </div>
            <div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap">
              <span style="font-size:32px;font-weight:900;color:{badge_c}">{current_users}</span>
              <span style="font-size:14px;color:#475569">kullanıcı şu an 404 sayfasında</span>
            </div>
            <div style="font-size:12px;color:#64748b;margin-top:4px">
              Eşik: {threshold} · Pencere: son {15} dakika
            </div>
          </div>
          {('<div style="margin-top:8px">' + page_rows + '</div>') if pages else ''}
          <p style="color:#94a3b8;font-size:11px;margin-top:14px">SEO Agent · GA4 Realtime 404 (otomatik)</p>
        </div>"""


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
    property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return {}

    try:
        data = fetch_realtime_404_users(property_id, window_minutes=window)
    except Exception as exc:
        logger.warning("Realtime 404 fetch hatası [%s/%s]: %s", site.domain, profile, exc)
        return {}

    total = data.get("total_404_users", 0)
    pages = data.get("pages", [])

    if total == 0:
        return {"total_404_users": 0, "pages": [], "severity": None}

    severity = None
    if total >= crit_threshold:
        severity = "critical"
    elif total >= warn_threshold:
        severity = "warning"

    result = {"total_404_users": total, "pages": pages, "severity": severity}

    if severity is None:
        return result

    # DB'ye kaydet
    rule_id = f"rt_404_{severity}"
    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    log = RealtimeAlarmLog(
        site_id=site.id,
        rule_id=rule_id,
        metric=f"{profile}:active_404_users",
        severity=severity,
        current_value=float(total),
        previous_value=0.0,
        change_pct=0.0,
        message=f"{site.domain} {profile_label} — {total} kullanıcı 404 sayfasında",
    )
    db.add(log)
    try:
        db.commit()
    except Exception:
        db.rollback()

    if skip_emails:
        return result

    # Cooldown kontrolü
    if _alarm_email_suppressed(db, site.id, [rule_id, "rt_404_warning", "rt_404_critical"], profile=profile):
        logger.info("404 spike cooldown aktif, mail atlandı (site=%s, profile=%s).", site.domain, profile)
        return result

    # Mail gönder
    from backend.services.mailer import is_realtime_mail_ready, send_realtime_email
    if is_realtime_mail_ready():
        threshold = crit_threshold if severity == "critical" else warn_threshold
        html_body = _html_404_spike_body(
            site.domain, profile_label, total, pages, severity, threshold
        )
        subject = f"{'🚨 KRİTİK' if severity == 'critical' else '⚠️ UYARI'} · {site.domain} · {total} kul. 404 sayfasında"
        thread_key = _realtime_email_thread_key(site.domain, profile)
        send_realtime_email(subject, html_body, thread_kind="404spike", thread_key=thread_key)
        logger.warning("404 Spike [%s %s]: %d kullanıcı (%s)", site.domain, profile, total, severity)

    return result


def run_404_spike_check_all_sites(db: Session, *, skip_emails: bool = False) -> list[dict]:
    """Tüm sitelerin web + mweb profillerinde 404 spike kontrolü."""
    from backend.models import Site as SiteModel
    from backend.services.ga4_auth import get_ga4_credentials_record

    results = []
    sites = db.query(SiteModel).all()
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
    from backend.services.mailer import send_realtime_email
    
    domain = alarm.get("domain") or alarm.get("site_domain") or "Site"
    profile = alarm.get("profile") or "web"
    profile_label = alarm.get("profile_label") or profile
    
    # Konu başlığı üret
    rule_id = alarm.get("rule_id", "")
    if rule_id.startswith("news_"):
        subject = _email_news_alarm_subject(domain, profile, [alarm])
        html_body = _html_news_alarm_body(domain, profile_label, [alarm], site_kpi={})
        thread_kind = "news"
    elif rule_id.startswith("page_"):
        subject = _email_page_alarm_subject(domain, profile, [alarm])
        html_body = _html_page_alarm_body(domain, profile_label, [alarm])
        thread_kind = "page"
    else:
        subject = _email_site_alarm_subject(domain, profile, [alarm])
        html_body = _html_site_alarm_body(domain, profile_label, [alarm])
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


def _html_app_event_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    # Negatif (drop + peak_drop) ve pozitif (spike) ayrı kotalarla — her ikisi de görünsün
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
        # Olumsuz event'lerde (failed/error/crash) renk mantığı tersine döner:
        # artış = kötü (kırmızı), düşüş = iyi (yeşil)
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
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{peak}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:{num_c};">{curr}</span>'
                f'<span style="font-size:13px;font-weight:800;color:{num_c};margin-left:8px;">−{drop_pct}% · düştü</span>'
            )
        else:
            delta = curr - prev
            sign = "+" if delta >= 0 else ""
            pct = a.get("change_pct", 0)
            pct_sign = "+" if pct >= 0 else ""
            direction_label = ("arttı" if delta >= 0 else "düştü")
            metric_html = (
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{prev}</span>'
                f'<span style="font-size:18px;color:#94a3b8;margin:0 6px;">→</span>'
                f'<span style="font-size:22px;font-weight:900;color:#0f172a;">{curr}</span>'
                f'<span style="font-size:14px;font-weight:800;color:{num_c};margin-left:8px;">{sign}{delta} ({pct_sign}{pct:.0f}%) · {direction_label}</span>'
            )

        cards.append(
            f'<div style="margin:10px 0;padding:12px 14px;border-radius:8px;border-left:4px solid {border};background:{bg};">'
            f'<p style="margin:0 0 6px;font-size:15px;font-weight:800;color:#0f172a;font-family:monospace;">{evt}</p>'
            f'<div>{metric_html}</div></div>'
        )

    pre_parts = [f"{a.get('event_name','')[:18]}:{int(a.get('current_count',0))}" for a in alarms[:5]]
    preheader = " · ".join(pre_parts) or f"{len(alarms)} event alarmı"

    return (
        f'<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:600px;color:#0f172a;">'
        f'{_preheader(preheader)}'
        f'<p style="font-size:15px;font-weight:700;margin:0 0 4px;">{dom_e} <span style="font-weight:400;color:#64748b;font-size:13px;">· {prof_e}</span></p>'
        f'<p style="font-size:13px;font-weight:600;color:#475569;margin:0 0 12px;">{len(alarms)} event ani değişim</p>'
        f'{"".join(cards)}'
        f'<p style="color:#94a3b8;font-size:12px;margin-top:16px;">SEO Agent · Uygulama event alarmı (otomatik)</p>'
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
            # 10 en çok artan + 10 en çok düşen ile sınırla (istek üzerine).
            to_send = _cap_top_n_each_side(to_send)
            profile_label = {"android": "Android", "ios": "iOS"}.get(profile, profile)
            html_body = _html_app_event_alarm_body(site.domain, profile_label, to_send)
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
    sites = db.query(SiteModel).all()
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
