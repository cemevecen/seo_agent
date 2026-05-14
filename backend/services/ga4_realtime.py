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
    """Konu satırı için kısa site adı (www. atılır)."""
    d = (domain or "").strip().lower().rstrip(".")
    if d.startswith("www."):
        d = d[4:]
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
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    primary = _email_pick_primary_alarm(alarms)
    rid = str(primary.get("rule_id", ""))
    pct = float(primary.get("change_pct", 0.0))
    verb, disp = _email_rt_verb_and_display_pct(rid, pct)
    metric_slug = _email_metric_subject_slug(str(primary.get("metric", "")), rid)
    suffix = f" · {p}" if p != "web" else ""
    extra = f" (+{len(alarms) - 1})" if len(alarms) > 1 else ""
    return f"{short} - rt {verb} {disp:.0f}% {metric_slug}{extra}{suffix}"


def _email_page_alarm_subject(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    primary = _email_pick_primary_alarm(alarms)
    rid = str(primary.get("rule_id", ""))
    pct = float(primary.get("change_pct", 0.0))
    verb, disp = _email_rt_verb_and_display_pct(rid, pct)
    slug = {
        "page_traffic_drop": "active users",
        "page_traffic_spike": "active users",
        "page_disappeared": "sayfa trafiği",
        "page_new_entry": "yeni sayfa",
    }.get(rid, "sayfa")
    suffix = f" · {p}" if p != "web" else ""
    extra = f" (+{len(alarms) - 1})" if len(alarms) > 1 else ""
    return f"{short} - rt {verb} {disp:.0f}% {slug}{extra}{suffix}"


def _email_news_alarm_subject(domain: str, profile: str, alarms: list[dict[str, Any]]) -> str:
    short = _email_site_short_label(domain)
    p = _email_profile_abbr(profile)
    primary = _email_pick_primary_alarm(alarms)
    rid = str(primary.get("rule_id", ""))
    pct = float(primary.get("change_pct", 0.0))
    verb, disp = _email_rt_verb_and_display_pct(rid, pct)
    slug = {
        "news_traffic_drop": "active users",
        "news_traffic_spike": "active users",
        "news_disappeared": "haber trafiği",
        "news_new_entry": "yeni haber",
    }.get(rid, "haber")
    suffix = f" · {p}" if p != "web" else ""
    extra = f" (+{len(alarms) - 1})" if len(alarms) > 1 else ""
    return f"{short} - rt {verb} {disp:.0f}% {slug}{extra}{suffix}"


def _html_site_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    """Site metrik alarmları — anlaşılır kart düzeni."""
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    intro = (
        "GA4 Realtime iki yarı pencereyi karşılaştırır: <strong>önceki yarı</strong> ile <strong>şimdiki yarı</strong> "
        "aynı uzunluktadır; yüzde, önceki yarıya göre değişimi gösterir."
    )
    cards: list[str] = []
    for alarm in alarms:
        metric_key = str(alarm.get("metric", "activeUsers"))
        metric_tr = html.escape(_email_metric_plain_tr(metric_key))
        cur = alarm.get("current_value", 0)
        prev = alarm.get("previous_value", 0)
        pct = float(alarm.get("change_pct", 0.0))
        is_drop = pct < 0 or str(alarm.get("rule_id", "")) in ("page_disappeared", "news_disappeared")
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        title_c = "#991b1b" if is_drop else "#166534"
        pct_c = "#dc2626" if is_drop else "#16a34a"
        rule_id = str(alarm.get("rule_id", ""))
        if rule_id == "traffic_drop":
            explain = "Eşik aşıldı: aktif kullanıcı sayısı bir önceki yarıya göre belirgin düştü."
        elif rule_id == "pageview_drop":
            explain = "Eşik aşıldı: sayfa görüntülemeleri bir önceki yarıya göre belirgin düştü."
        elif rule_id == "traffic_spike":
            explain = "Eşik aşıldı: aktif kullanıcı bir önceki yarıya göre belirgin arttı."
        else:
            explain = "Kural tetiklendi; metrik ve yön aşağıda."

        cards.append(
            f"""
            <div style="margin:16px 0;padding:16px 18px;border-radius:10px;border-left:4px solid {border};
                        background:{bg};max-width:600px;">
                <div style="font-size:11px;letter-spacing:0.06em;font-weight:700;color:{title_c};text-transform:uppercase;">
                    {metric_tr} · {prof_e}
                </div>
                <div style="margin-top:10px;display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 14px;">
                    <span style="font-size:13px;color:#64748b;">Önceki yarı</span>
                    <span style="font-size:26px;font-weight:800;color:#0f172a;">{prev:.0f}</span>
                    <span style="font-size:20px;color:#94a3b8;">→</span>
                    <span style="font-size:13px;color:#64748b;">Şimdiki yarı</span>
                    <span style="font-size:26px;font-weight:800;color:#0f172a;">{cur:.0f}</span>
                    <span style="font-size:18px;font-weight:800;color:{pct_c};margin-left:4px;">{pct:+.1f}%</span>
                </div>
                <p style="margin:14px 0 0;font-size:14px;line-height:1.5;color:#334155;">{metric_tr}. {html.escape(explain)}</p>
            </div>
            """
        )

    n = len(alarms)
    head = html.escape("Birden fazla kural aynı anda tetiklendi." if n > 1 else "Realtime alarmı.")
    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:640px;color:#0f172a;">
            <p style="font-size:15px;font-weight:600;margin:0 0 8px;">{dom_e}</p>
            <p style="font-size:14px;line-height:1.55;color:#475569;margin:0 0 16px;">{intro}</p>
            <p style="font-size:13px;color:#64748b;margin:0 0 8px;">{head}</p>
            {''.join(cards)}
            <p style="color:#94a3b8;font-size:12px;margin-top:22px;">SEO Agent · GA4 Realtime (otomatik)</p>
        </div>
        """


def _html_page_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    intro = (
        "Aşağıdaki satırlar <strong>en çok trafik alan sayfa/ekran</strong> listesindeki bir satırı temsil eder. "
        "Sayılar, aynı Realtime penceresinde önceki ölçüme göre <strong>aktif kullanıcı</strong> değişimidir."
    )
    cards: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "")
        page_e = html.escape(page)
        row_url = _alarm_row_public_url(domain, "page:" + str(page))
        link_block = ""
        if row_url:
            ru = html.escape(row_url, quote=True)
            link_block = (
                f'<p style="margin:8px 0 0;font-size:13px;">'
                f'<a href="{ru}" target="_blank" rel="noopener noreferrer" style="color:#2563eb;font-weight:600;">Sayfayı aç</a>'
                f"</p>"
            )
        curr = alarm.get("current_users", 0)
        prev = alarm.get("previous_users", 0)
        pct = float(alarm.get("change_pct", 0.0))
        rid = str(alarm.get("rule_id", ""))
        is_drop = pct < 0 or rid == "page_disappeared"
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        pct_c = "#dc2626" if is_drop else "#16a34a"
        if rid == "page_disappeared":
            explain = "Bu başlık/URL bir önceki ölçümde listedeydi; şimdi eşik altında veya listeden çıktı."
        elif rid == "page_new_entry":
            explain = "Bu sayfa önceki ölçümde yoktu veya çok düşüktü; şimdi listede ve eşik üstünde."
        else:
            explain = "Önceki ölçüme göre bu satırdaki aktif kullanıcı değişimi kural eşiğini aştı."

        cards.append(
            f"""
            <div style="margin:16px 0;padding:16px 18px;border-radius:10px;border-left:4px solid {border};
                        background:{bg};max-width:600px;">
                <div style="font-size:11px;color:#64748b;margin-bottom:6px;">{html.escape(profile_label)}</div>
                <p style="margin:0 0 8px;font-size:17px;font-weight:800;color:#0f172a;word-break:break-word;line-height:1.35;">{page_e}</p>
                {link_block}
                <div style="display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 14px;">
                    <span style="font-size:13px;color:#64748b;">Önce</span>
                    <span style="font-size:24px;font-weight:800;color:#0f172a;">{prev:.0f}</span>
                    <span style="font-size:18px;color:#94a3b8;">→</span>
                    <span style="font-size:13px;color:#64748b;">Şimdi</span>
                    <span style="font-size:24px;font-weight:800;color:#0f172a;">{curr:.0f}</span>
                    <span style="font-size:17px;font-weight:800;color:{pct_c};">{pct:+.1f}%</span>
                </div>
                <p style="margin:12px 0 0;font-size:14px;line-height:1.5;color:#334155;">{html.escape(explain)}</p>
            </div>
            """
        )

    n = len(alarms)
    head = html.escape(f"{n} sayfa satırı tetiklendi." if n > 1 else "Sayfa alarmı.")
    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:640px;color:#0f172a;">
            <p style="font-size:15px;font-weight:600;margin:0 0 8px;">{dom_e}</p>
            <p style="font-size:14px;line-height:1.55;color:#475569;margin:0 0 16px;">{intro}</p>
            <p style="font-size:13px;color:#64748b;margin:0 0 8px;">{head}</p>
            {''.join(cards)}
            <p style="color:#94a3b8;font-size:12px;margin-top:22px;">SEO Agent · GA4 Realtime sayfa listesi (otomatik)</p>
        </div>
        """


def _html_news_alarm_body(domain: str, profile_label: str, alarms: list[dict[str, Any]]) -> str:
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    intro = (
        "«Haberler» listesi, site içi haber benzeri <strong>ekran adı</strong> satırlarından oluşur. "
        "Aşağıdaki sayılar ilgili başlık için <strong>aktif kullanıcı</strong> (önceki ölçüme göre)."
    )
    cards: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "")
        page_e = html.escape(page)
        row_url = _alarm_row_public_url(domain, "news:" + str(page))
        link_block = ""
        if row_url:
            ru = html.escape(row_url, quote=True)
            link_block = (
                f'<p style="margin:8px 0 0;font-size:13px;">'
                f'<a href="{ru}" target="_blank" rel="noopener noreferrer" style="color:#2563eb;font-weight:600;">Sayfayı aç</a>'
                f"</p>"
            )
        curr = alarm.get("current_users", 0)
        prev = alarm.get("previous_users", 0)
        pct = float(alarm.get("change_pct", 0.0))
        rid = str(alarm.get("rule_id", ""))
        is_drop = pct < 0 or rid == "news_disappeared"
        border = "#dc2626" if is_drop else "#16a34a"
        bg = "#fef2f2" if is_drop else "#f0fdf4"
        pct_c = "#dc2626" if is_drop else "#16a34a"
        if rid == "news_disappeared":
            explain = "Bu başlık listeden düştü veya trafik eşiğin altına indi."
        elif rid == "news_new_entry":
            explain = "Bu başlık yeni güçlü şekilde listeye girdi."
        else:
            explain = "Önceki ölçüme göre bu başlıkta aktif kullanıcı değişimi eşiği aştı."

        cards.append(
            f"""
            <div style="margin:16px 0;padding:16px 18px;border-radius:10px;border-left:4px solid {border};
                        background:{bg};max-width:600px;">
                <div style="font-size:11px;color:#64748b;margin-bottom:6px;">{prof_e}</div>
                <p style="margin:0 0 8px;font-size:17px;font-weight:800;color:#0f172a;word-break:break-word;line-height:1.35;">{page_e}</p>
                {link_block}
                <div style="display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 14px;">
                    <span style="font-size:13px;color:#64748b;">Önce</span>
                    <span style="font-size:24px;font-weight:800;color:#0f172a;">{prev:.0f}</span>
                    <span style="font-size:18px;color:#94a3b8;">→</span>
                    <span style="font-size:13px;color:#64748b;">Şimdi</span>
                    <span style="font-size:24px;font-weight:800;color:#0f172a;">{curr:.0f}</span>
                    <span style="font-size:17px;font-weight:800;color:{pct_c};">{pct:+.1f}%</span>
                </div>
                <p style="margin:12px 0 0;font-size:14px;line-height:1.5;color:#334155;">{html.escape(explain)}</p>
            </div>
            """
        )

    n = len(alarms)
    head = html.escape(f"{n} haber satırı tetiklendi." if n > 1 else "Haber alarmı.")
    return f"""
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:640px;color:#0f172a;">
            <p style="font-size:15px;font-weight:600;margin:0 0 8px;">{dom_e}</p>
            <p style="font-size:14px;line-height:1.55;color:#475569;margin:0 0 16px;">{intro}</p>
            <p style="font-size:13px;color:#64748b;margin:0 0 8px;">{head}</p>
            {''.join(cards)}
            <p style="color:#94a3b8;font-size:12px;margin-top:22px;">SEO Agent · GA4 Realtime haberler (otomatik)</p>
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
        "threshold_pct": 25,
        "min_baseline": 3,
        "severity": "critical",
    },
    "traffic_spike": {
        "label": "Aktif kullanıcılar",
        "metric": "activeUsers",
        "direction": "spike",
        "threshold_pct": 45,
        "min_baseline": 3,
        "severity": "warning",
    },
    "pageview_drop": {
        "label": "Sayfa görüntülemeleri",
        "metric": "screenPageViews",
        "direction": "drop",
        "threshold_pct": 30,
        "min_baseline": 5,
        "severity": "warning",
    },
}

# Sayfa bazlı alarm eşikleri
PAGE_ALARM_RULES: dict[str, dict[str, Any]] = {
    "page_traffic_drop": {
        "label": "Sayfa trafik düşüşü",
        "direction": "drop",
        "threshold_pct": 35,
        "min_users": 10,
        "severity": "warning",
    },
    "page_traffic_spike": {
        "label": "Sayfa trafik artışı",
        "direction": "spike",
        "threshold_pct": 60,
        "min_users": 10,
        "severity": "warning",
    },
    "page_disappeared": {
        "label": "Sayfa top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 15,
        "severity": "critical",
    },
    "page_new_entry": {
        "label": "Yeni sayfa top listeye girdi",
        "direction": "new_entry",
        "min_users": 25,
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
}

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
    """sinemalar için tüm ``threshold_pct`` = 50; mutlak tabanlar (min_*) de %50 ölçeklenir."""
    if not _is_sinemalar_site_domain(site_domain):
        return base_rules
    out = copy.deepcopy(base_rules)
    scale = _SINEMALAR_REALTIME_ALARM_PCT / 100.0
    for _rid, rule in out.items():
        if "threshold_pct" in rule:
            rule["threshold_pct"] = _SINEMALAR_REALTIME_ALARM_PCT
        for key in ("min_users", "min_prev_users", "min_baseline"):
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
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API ile son N dakikadaki top sayfaları çeker.

    compare_previous: True ise önceki pencereyle (window_minutes kadar öncesi) karşılaştırma metriklerini de döner.
    sort_by: "activeUsers" veya "screenPageViews" — sıralama kriteri.
    """
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    w = max(1, min(window_minutes, 30))

    minute_ranges = [
        MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
    ]
    if compare_previous:
        minute_ranges.append(
            MinuteRange(name="previous", start_minutes_ago=2 * w - 1, end_minutes_ago=w)
        )

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dimension)],
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

        if page_val not in temp_map:
            temp_map[page_val] = {
                "page": page_val,
                "activeUsers": 0.0,
                "screenPageViews": 0.0,
                "activeUsers_previous": 0.0,
                "screenPageViews_previous": 0.0,
            }

        entry = temp_map[page_val]
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
        compare_previous=True,
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
        if len(out) >= max(1, min(int(limit), 25)):
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
    w = max(1, min(window_minutes, 30))
    fetch_cap = min(250, max(limit * 8, 40))

    minute_ranges = [
        MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
    ]
    if compare_previous:
        minute_ranges.append(
            MinuteRange(name="previous", start_minutes_ago=2 * w - 1, end_minutes_ago=w)
        )

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="eventCount"),
        ],
        minute_ranges=minute_ranges,
        limit=fetch_cap,
    )

    t0 = time.monotonic()
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
    w = max(1, min(window_minutes, 30))
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
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API: etkinlik adına göre eventCount — mobil uygulama kartları için."""
    if client is None:
        client = _build_client()

    property_id = _normalize_ga4_property_id(property_id)
    w = max(1, min(window_minutes, 30))
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

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
        ],
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
        limit=fetch_limit,
    )

    t0 = time.monotonic()
    try:
        response = client.run_realtime_report(request)
    except Exception:
        request = RunRealtimeReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            minute_ranges=[
                MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
            ],
            limit=fetch_limit,
        )
        response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]
    events: list[dict[str, Any]] = []

    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        event_name = str(dm.get("eventName", "") or "").strip()
        if not event_name:
            for _k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    event_name = vs
                    break
        metrics_dict: dict[str, float] = {}
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                metrics_dict[mname] = float(mv.value)
            except (ValueError, TypeError):
                metrics_dict[mname] = 0.0
        ec = metrics_dict.get("eventCount", 0.0)
        events.append({"eventName": event_name, "eventCount": ec})

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
        _save_alarm_logs(db, site.id, alarms)
        logger.info("GA4 Realtime: %d alarm bulundu (site=%s, profile=%s).", len(alarms), site.domain, profile)
        if not skip_emails:
            _send_site_alarm_emails(site.domain, profile, alarms)
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


def _save_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]]) -> None:
    """Tetiklenen alarmları DB'ye kaydeder."""
    from backend.models import RealtimeAlarmLog

    for alarm in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=alarm["rule_id"],
            metric=alarm["metric"],
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


def _send_site_alarm_emails(domain: str, profile: str, alarms: list[dict[str, Any]]) -> None:
    """Genel site alarmları — tek e-postada özet + Gmail iş parçacığı (sabit konu + References)."""
    from backend.services.mailer import is_realtime_mail_ready, send_realtime_email

    if not alarms:
        return
    if not is_realtime_mail_ready():
        logger.warning(
            "Realtime site alarmı tetiklendi (site=%s, alarm_count=%d) ancak e-posta gönderilemedi. "
            "Lütfen şu ayarları kontrol edin: GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP_HOST, MAIL_TO.",
            domain,
            len(alarms),
        )
        return

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

    for i, page in enumerate(pages[:25]):
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
            if prev_users >= rule["min_prev_users"]:
                triggered.append({
                    "rule_id": "page_disappeared",
                    "severity": rule["severity"],
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": 0,
                    "previous_users": prev_users,
                    "change_pct": -100.0,
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

    1. Önceki snapshot'ı DB'den al
    2. Yeni top sayfaları API'den çek
    3. Karşılaştır, alarmları değerlendir
    4. Yeni snapshot'ı DB'ye kaydet
    5. Alarmları DB'ye kaydet ve mail gönder
    """
    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return []

    previous_pages = get_previous_page_snapshots(db, site.id, profile)

    try:
        result = fetch_realtime_top_pages_with_app_fallback(
            property_id,
            profile=profile,
            window_minutes=window_minutes,
            limit=25,
            sort_by="activeUsers",
        )
    except Exception as exc:
        logger.warning("Sayfa alarm: top pages API hatası [%s/%s]: %s", site.domain, profile, exc)
        return []

    current_pages = result.get("pages", [])
    save_page_snapshots(db, site.id, profile, current_pages)

    if not previous_pages:
        return []

    alarms = evaluate_page_alarms(
        current_pages, previous_pages,
        site_domain=site.domain, profile=profile,
    )

    if alarms:
        _save_page_alarm_logs(db, site.id, alarms)
        if not skip_emails:
            _send_page_alarm_email(site.domain, profile, alarms)

    return alarms


def _save_page_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]]) -> None:
    """Sayfa bazlı alarmları RealtimeAlarmLog'a kaydeder."""
    from backend.models import RealtimeAlarmLog

    for a in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=a["rule_id"],
            metric="page:" + a.get("page", "")[:200],
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


def _send_page_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]]) -> None:
    """Sayfa bazlı alarmlar — tek e-postada özet + Gmail iş parçacığı."""
    from backend.services.mailer import is_realtime_mail_ready, send_realtime_email

    if not alarms:
        return
    if not is_realtime_mail_ready():
        logger.warning(
            "Realtime sayfa alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP ve MAIL_TO yapılandırmasını kontrol edin.",
            len(alarms),
        )
        return

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    html_body = _html_page_alarm_body(domain, profile_label, alarms)
    subject = _email_page_alarm_subject(domain, profile, alarms)
    send_realtime_email(subject, html_body, thread_kind="page", thread_key=thread_key)


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

    for i, page in enumerate(pages[:25]):
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
) -> list[dict[str, Any]]:
    if not previous_pages:
        return []

    news_rules = _realtime_rules_threshold_pct_for_domain(NEWS_ALARM_RULES, site_domain)

    triggered: list[dict[str, Any]] = []

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

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
            if prev_users >= rule["min_prev_users"]:
                triggered.append({
                    "rule_id": "news_disappeared",
                    "severity": rule["severity"],
                    "page": page_path,
                    "profile": profile,
                    "domain": site_domain,
                    "current_users": 0,
                    "previous_users": prev_users,
                    "change_pct": -100.0,
                    "message": f"{title} — listeden çıktı (önceki: {prev_users:.0f})",
                })

    return triggered


def _save_news_alarm_logs(db: Session, site_id: int, alarms: list[dict[str, Any]]) -> None:
    from backend.models import RealtimeAlarmLog

    for a in alarms:
        log = RealtimeAlarmLog(
            site_id=site_id,
            rule_id=a["rule_id"],
            metric="news:" + a.get("page", "")[:200],
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


def _send_news_alarm_email(domain: str, profile: str, alarms: list[dict[str, Any]]) -> None:
    """Haber trafiği alarmları — tek e-postada özet + Gmail iş parçacığı (sabit konu + References)."""
    from backend.services.mailer import is_news_realtime_mail_ready, send_realtime_news_email

    if not alarms:
        return
    if not is_news_realtime_mail_ready():
        logger.warning(
            "Realtime haber alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_NEWS_ALERT_EMAIL, SMTP ve MAIL_TO.",
            len(alarms),
        )
        return

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    thread_key = _realtime_email_thread_key(domain, profile)
    html_body = _html_news_alarm_body(domain, profile_label, alarms)
    subject = _email_news_alarm_subject(domain, profile, alarms)
    send_realtime_news_email(subject, html_body, thread_kind="news", thread_key=thread_key)


def check_news_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 15,
    interval_minutes: int = 15,
    skip_emails: bool = False,
) -> list[dict[str, Any]]:
    if profile not in ("web", "mweb"):
        return []

    if not _news_snapshot_due(db, site.id, profile, interval_minutes=interval_minutes):
        return []

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    property_id = properties.get(profile) or properties.get("web")
    if not property_id:
        return []

    previous_pages = get_previous_news_snapshots(db, site.id, profile)

    try:
        result = fetch_realtime_top_news_pages(
            property_id,
            site_domain=(site.domain or "").strip(),
            window_minutes=window_minutes,
            limit=20,
            sort_by="activeUsers",
        )
    except Exception as exc:
        logger.warning("Haber alarm: top-news API hatası [%s/%s]: %s", site.domain, profile, exc)
        return []

    current_pages = result.get("pages", [])
    save_news_snapshots(db, site.id, profile, current_pages)

    if not previous_pages:
        return []

    alarms = evaluate_news_alarms(
        current_pages, previous_pages,
        site_domain=site.domain, profile=profile,
    )

    if alarms:
        _save_news_alarm_logs(db, site.id, alarms)
        if not skip_emails:
            _send_news_alarm_email(site.domain, profile, alarms)

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
        for profile in ("web", "mweb"):
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

    intro = (
        f"Bu e-posta son kontrol periyodundaki <strong>toplam {len(alarms)} adet</strong> "
        "Realtime alarmının özetidir. En yüksek değişim oranına sahip ilk 10 kayıt aşağıdadır."
    )

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
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;max-width:640px;color:#0f172a;margin:0 auto;padding:20px;">
        <h2 style="font-size:18px;margin-bottom:12px;color:#1e293b;">Realtime Alarm Özeti</h2>
        <p style="font-size:14px;line-height:1.5;color:#64748b;margin-bottom:24px;">{intro}</p>
        {''.join(sections)}
        <div style="margin-top:30px;padding-top:16px;border-top:1px solid #e2e8f0;font-size:12px;color:#94a3b8;text-align:center;">
            SEO Agent · Realtime Monitoring System
        </div>
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
