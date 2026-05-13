"""GA4 Realtime API — pencereli karşılaştırma ve alarm değerlendirmesi.

Son 30 dakikayı iki pencereye böler (ör. 0-9 dk vs 10-19 dk) ve
activeUsers/screenPageViews değişimini izleyerek alarm tetikler.
"""

from __future__ import annotations

import hashlib
import html
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
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


def _realtime_email_thread_key(domain: str, profile: str) -> str:
    """Gmail iş parçacığı / References için site+profil anahtarı (ASCII, kısa)."""
    d = (domain or "").strip().lower()
    p = (profile or "").strip().lower()
    return hashlib.sha256(f"{d}|{p}".encode()).hexdigest()[:24]


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
        "label": "Traffic düşüşü",
        "metric": "activeUsers",
        "direction": "drop",
        "threshold_pct": 40,
        "min_baseline": 5,
        "severity": "critical",
    },
    "traffic_spike": {
        "label": "Traffic artışı",
        "metric": "activeUsers",
        "direction": "spike",
        "threshold_pct": 80,
        "min_baseline": 5,
        "severity": "warning",
    },
    "pageview_drop": {
        "label": "Sayfa görüntüleme düşüşü",
        "metric": "screenPageViews",
        "direction": "drop",
        "threshold_pct": 50,
        "min_baseline": 10,
        "severity": "warning",
    },
}

# Sayfa bazlı alarm eşikleri
PAGE_ALARM_RULES: dict[str, dict[str, Any]] = {
    "page_traffic_drop": {
        "label": "Sayfa trafik düşüşü",
        "direction": "drop",
        "threshold_pct": 50,
        "min_users": 20,
        "severity": "warning",
    },
    "page_traffic_spike": {
        "label": "Sayfa trafik artışı",
        "direction": "spike",
        "threshold_pct": 100,
        "min_users": 20,
        "severity": "warning",
    },
    "page_disappeared": {
        "label": "Sayfa top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 30,
        "severity": "critical",
    },
    "page_new_entry": {
        "label": "Yeni sayfa top listeye girdi",
        "direction": "new_entry",
        "min_users": 50,
        "severity": "info",
    },
}

# Haberler (unifiedScreenName) — sayfa alarmlarından biraz daha sıkı eşikler (gürültü azaltma)
NEWS_ALARM_RULES: dict[str, dict[str, Any]] = {
    "news_traffic_drop": {
        "label": "Haber trafiği düşüşü",
        "direction": "drop",
        "threshold_pct": 55,
        "min_users": 40,
        "severity": "warning",
    },
    "news_traffic_spike": {
        "label": "Haber trafiği artışı",
        "direction": "spike",
        "threshold_pct": 90,
        "min_users": 40,
        "severity": "warning",
    },
    "news_disappeared": {
        "label": "Haber top listeden düştü",
        "direction": "disappeared",
        "min_prev_users": 50,
        "severity": "warning",
    },
    "news_new_entry": {
        "label": "Yeni haber başlığı top listeye girdi",
        "direction": "new_entry",
        "min_users": 75,
        "severity": "info",
    },
}

# Varsayılan pencere boyutu (dakika)
DEFAULT_WINDOW_MINUTES = 10


def _build_client() -> BetaAnalyticsDataClient:
    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_realtime_comparison(
    property_id: str,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    *,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """İki minuteRange ile Realtime API karşılaştırması + tek aralık toplamı.

    GA Realtime en fazla ~29 dk geriye gider; karşılaştırma için sabit iki **15 dk**
    aralığı kullanılır: ``current`` (son 15 dk) ve ``previous`` (önceki 15 dk).
    ``window_minutes`` yalnızca ikinci istekteki **toplam** (tek minuteRange)
    uzunluğunu ``min(max(1, window_minutes), 30)`` ile sınırlar.
    """
    if client is None:
        client = _build_client()

    # Realtime API max 2 MinuteRange destekler ve max 29 dk geriye gider.
    # 30 dk'yı iki 15'lik yarıya böleriz: current (0-14) + previous (15-29).
    # Toplam 30 dk değeri için ayrı tek range'li bir çağrı yapılır.
    half = 15

    metrics = [
        Metric(name="activeUsers"),
        Metric(name="screenPageViews"),
        Metric(name="eventCount"),
        Metric(name="conversions"),
    ]

    # Çağrı 1: Karşılaştırma (2 range)
    req_compare = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=half - 1, end_minutes_ago=0),
            MinuteRange(name="previous", start_minutes_ago=2 * half - 1, end_minutes_ago=half),
        ],
    )

    # Çağrı 2: Toplam 30 dk (1 range)
    total_start = min(max(1, window_minutes), 30) - 1
    req_total = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        metrics=metrics,
        minute_ranges=[
            MinuteRange(name="total", start_minutes_ago=total_start, end_minutes_ago=0),
        ],
    )

    t0 = time.monotonic()
    resp_compare = client.run_realtime_report(req_compare)
    resp_total = client.run_realtime_report(req_total)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in resp_compare.metric_headers]
    windows: dict[str, dict[str, float]] = {"current": {}, "previous": {}}

    for row in resp_compare.rows:
        range_name = ""
        for dv in row.dimension_values:
            val = dv.value
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


def fetch_realtime_top_pages(
    property_id: str,
    window_minutes: int = 30,
    *,
    limit: int = 10,
    dimension: str = "unifiedScreenName",
    sort_by: str = "activeUsers",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Realtime API ile son N dakikadaki top sayfaları çeker.

    sort_by: "activeUsers" veya "screenPageViews" — sıralama kriteri.
    """
    if client is None:
        client = _build_client()

    w = max(1, min(window_minutes, 30))

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name=dimension)],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="screenPageViews"),
        ],
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
        ],
    )

    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]
    pages: list[dict[str, Any]] = []

    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        page_path = str(dm.get(dimension, "") or "").strip()
        if not page_path or page_path.lower() in ("current", "previous"):
            for _k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    page_path = vs
                    break
        metrics_dict: dict[str, float] = {}
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                metrics_dict[mname] = float(mv.value)
            except (ValueError, TypeError):
                metrics_dict[mname] = 0.0
        pages.append({"page": page_path, **metrics_dict})

    pages.sort(key=lambda p: p.get(sort_by, 0), reverse=True)

    return {
        "property_id": property_id,
        "window_minutes": w,
        "pages": pages[:limit],
        "total_pages": len(pages),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
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
    """Path görünüyorsa doğrudan URL; değilse Google site: araması (Realtime path boyutu yok)."""
    from urllib.parse import quote

    from backend.collectors.ga4 import _is_news_article_path

    d = (site_domain or "").strip().lower().replace("https://", "").replace("http://", "").strip("/")
    u = (unified or "").strip()
    if u.startswith("/") and d and _is_news_article_path(u):
        return "https://" + d + u
    q = f"site:{d} {u}" if d else u
    return "https://www.google.com/search?q=" + quote(q, safe="")


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

    Tam URL çoğu satırda yok; ``link_url`` alanı ya doğrudan path (nadiren) ya da ``site:`` arama linkidir.
    """
    fetch_n = min(250, max(80, int(limit) * 25))
    base = fetch_realtime_top_pages(
        property_id,
        window_minutes=window_minutes,
        limit=fetch_n,
        sort_by=sort_by,
        dimension="unifiedScreenName",
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
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Android/iOS akışlarında unifiedScreenName boşsa: eventName kırılımı.

    Dönüş şekli fetch_realtime_top_pages ile uyumlu: screenPageViews alanına eventCount yazılır
    (şablondaki «görüntüleme» sekmesi event hacmine göre sıralanır).
    """
    if client is None:
        client = _build_client()

    w = max(1, min(window_minutes, 30))
    fetch_cap = min(250, max(limit * 8, 40))

    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="eventName")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="eventCount"),
        ],
        minute_ranges=[
            MinuteRange(name="current", start_minutes_ago=w - 1, end_minutes_ago=0),
        ],
        limit=fetch_cap,
    )

    t0 = time.monotonic()
    response = client.run_realtime_report(request)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    metric_names = [m.name for m in response.metric_headers]
    dim_headers = [h.name for h in response.dimension_headers]
    pages: list[dict[str, Any]] = []

    for row in response.rows:
        dm = _realtime_row_dimensions(row, dim_headers)
        name = str(dm.get("eventName", "") or "").strip()
        if not name:
            for _k, v in dm.items():
                vs = (v or "").strip()
                if vs and vs.lower() not in ("current", "previous"):
                    name = vs
                    break
        if not name:
            continue
        metrics_dict: dict[str, float] = {}
        for i, mv in enumerate(row.metric_values):
            mname = metric_names[i] if i < len(metric_names) else f"metric_{i}"
            try:
                metrics_dict[mname] = float(mv.value)
            except (ValueError, TypeError):
                metrics_dict[mname] = 0.0
        ec = metrics_dict.get("eventCount", 0.0)
        au = metrics_dict.get("activeUsers", 0.0)
        pages.append({"page": name, "activeUsers": au, "screenPageViews": ec})

    sort_key = "eventCount" if sort_by == "screenPageViews" else "activeUsers"
    pages.sort(key=lambda p: p.get(sort_key, 0), reverse=True)

    return {
        "property_id": property_id,
        "window_minutes": w,
        "pages": pages[:limit],
        "total_pages": len(pages),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "api_ms": elapsed_ms,
        "breakdown": "eventName",
    }


def fetch_realtime_top_pages_with_app_fallback(
    property_id: str,
    *,
    profile: str,
    window_minutes: int = 30,
    limit: int = 10,
    sort_by: str = "activeUsers",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Mobil: önce en iyi ekran boyutu; etiketler zayıfsa eventName kırılımına düşer."""
    if profile not in ("android", "ios"):
        base = fetch_realtime_top_pages(
            property_id,
            window_minutes=window_minutes,
            limit=limit,
            sort_by=sort_by,
            client=client,
        )
        base["breakdown"] = "unifiedScreenName"
        return base

    base = fetch_realtime_top_pages_pick_best_screen_dimension(
        property_id,
        window_minutes=window_minutes,
        limit=limit,
        sort_by=sort_by,
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
            client=client,
        )
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
) -> list[dict[str, Any]]:
    """Karşılaştırma sonuçlarına alarm kurallarını uygular.

    Returns list of triggered alarms: [{rule_id, label, metric, ...}, ...]
    """
    if rules is None:
        rules = ALARM_RULES

    triggered: list[dict[str, Any]] = []

    for rule_id, rule in rules.items():
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
                    f"{rule['label']}: {metric_name} "
                    f"{prev_val:.0f} → {cur_val:.0f} ({change_pct:+.1f}%)"
                ),
            })

    return triggered


def check_site_realtime(
    db: Session,
    site: Site,
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    profile: str = "web",
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

    try:
        result = fetch_realtime_comparison(property_id, window_minutes)
    except Exception as exc:
        logger.warning("GA4 Realtime API hatası [%s / %s]: %s", site.domain, property_id, exc)
        return {
            "site_id": site.id,
            "domain": site.domain,
            "profile": profile,
            "error": "api_error",
            "message": str(exc),
        }

    alarms = evaluate_alarms(result["comparison"])

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    for a in alarms:
        a["domain"] = site.domain
        a["profile"] = profile
        a["profile_label"] = profile_label
        a["message"] = (
            f"{a['label']}: {site.domain} {profile_label} — "
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
        _send_site_alarm_emails(site.domain, profile, alarms)
        logger.warning(
            "GA4 Realtime ALARM [%s]: %d kural tetiklendi — %s",
            site.domain,
            len(alarms),
            "; ".join(a["message"] for a in alarms),
        )

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
            "Realtime site alarmı tetiklendi (%d) ancak e-posta gönderilmedi — "
            "GA4_REALTIME_EMAIL_ENABLED, GA4_REALTIME_PAGE_ALERT_EMAIL, SMTP ve MAIL_TO yapılandırmasını kontrol edin.",
            len(alarms),
        )
        return

    profile_label = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    thread_key = _realtime_email_thread_key(domain, profile)
    rows: list[str] = []
    for alarm in alarms:
        metric = html.escape(str(alarm.get("metric", "activeUsers")))
        cur = alarm.get("current_value", 0)
        prev = alarm.get("previous_value", 0)
        pct = alarm.get("change_pct", 0)
        label = html.escape(str(alarm.get("label", "Alarm")))
        msg = html.escape(alarm.get("message", ""))
        color = "#dc2626" if pct < 0 else "#16a34a"
        rows.append(
            "<tr><td colspan=\"2\" style=\"padding:10px 12px;border-bottom:1px solid #e2e8f0;\">"
            f"<div style=\"font-weight:700;color:#0f172a;\">{label}</div>"
            f"<div style=\"font-size:13px;color:#334155;margin-top:4px;\">{msg}</div>"
            "<div style=\"margin-top:8px;font-size:13px;\">"
            f"Metrik: <span style=\"font-family:monospace;\">{metric}</span> — "
            f"<strong>{cur:.0f}</strong> "
            f"<span style=\"color:#64748b;\">(önceki {prev:.0f})</span> "
            f"<span style=\"color:{color};font-weight:700;\">{pct:+.1f}%</span>"
            "</div></td></tr>"
        )

    n = len(alarms)
    summary = f"{n} site metrik alarmı" if n > 1 else "Site metrik alarmı"
    summary_e = html.escape(summary)
    html_body = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 640px;">
            <p style="color:#64748b;font-size:14px;">{summary_e} — {dom_e} ({prof_e})</p>
            <table style="border-collapse:collapse;width:100%;margin:16px 0;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
            <tbody>
            {''.join(rows)}
            </tbody></table>
            <p style="color:#94a3b8;font-size:12px;margin-top:24px;">SEO Agent Realtime — aynı konu altında gruplanır (Gmail).</p>
        </div>
        """
    subject = f"Site metrikleri — {domain} ({profile_label})"
    send_realtime_email(subject, html_body, thread_kind="site", thread_key=thread_key)


def get_recent_snapshots(
    db: Session,
    site_id: int,
    *,
    profile: str = "web",
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Son N snapshot kaydını döner (mini trend grafiği için)."""
    import json as _json

    from backend.models import RealtimeSnapshot

    rows = (
        db.query(RealtimeSnapshot)
        .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
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


def get_recent_alarms(
    db: Session,
    site_id: int,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Son N alarm kaydını döner."""
    from backend.models import RealtimeAlarmLog

    rows = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(RealtimeAlarmLog.triggered_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": row.id,
            "rule_id": row.rule_id,
            "metric": row.metric,
            "severity": row.severity,
            "current_value": row.current_value,
            "previous_value": row.previous_value,
            "change_pct": row.change_pct,
            "message": row.message,
            "triggered_at": _utc_db_datetime_iso_z(row.triggered_at),
        }
        for row in rows
    ]


def run_all_sites_realtime_check(db: Session, *, window_minutes: int = DEFAULT_WINDOW_MINUTES) -> list[dict[str, Any]]:
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
            explicit = (properties.get(profile) or "").strip()
            # Realtime sayfası yalnızca GA'da ayrı property ID'si olan profilleri gösterir;
            # web dışında yalnızca web'e düşerek aynı veriyi iki kez çekmeyi atla.
            pid = explicit or base_web
            if not pid:
                continue
            if profile != "web" and not explicit:
                continue
            try:
                r = check_site_realtime(db, site, window_minutes=window_minutes, profile=profile)
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

    triggered: list[dict[str, Any]] = []
    plabel = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    tag = f"{site_domain} {plabel}" if site_domain else plabel

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

    for page_path, curr in curr_map.items():
        curr_users = curr.get("activeUsers", 0)
        prev = prev_map.get(page_path)

        if prev:
            prev_users = prev.get("activeUsers", 0)

            rule = PAGE_ALARM_RULES["page_traffic_drop"]
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
                        "message": f"📉 [{tag}] {page_path[:60]} — {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })

            rule = PAGE_ALARM_RULES["page_traffic_spike"]
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
                        "message": f"📈 [{tag}] {page_path[:60]} — {prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)",
                    })
        else:
            rule = PAGE_ALARM_RULES["page_new_entry"]
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
                    "message": f"🆕 [{tag}] {page_path[:60]} — top listeye girdi ({curr_users:.0f} kullanıcı)",
                })

    rule = PAGE_ALARM_RULES["page_disappeared"]
    for page_path, prev in prev_map.items():
        if page_path not in curr_map:
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
                    "message": f"⚠️ [{tag}] {page_path[:60]} — listeden düştü (önceki: {prev_users:.0f})",
                })

    return triggered


def check_page_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 30,
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
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    thread_key = _realtime_email_thread_key(domain, profile)
    rows: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "bilinmiyor")
        page_e = html.escape(page)
        msg_e = html.escape(alarm.get("message", ""))
        curr = alarm.get("current_users", 0)
        prev = alarm.get("previous_users", 0)
        pct = alarm.get("change_pct", 0)
        color = "#dc2626" if pct < 0 else "#16a34a"
        rows.append(
            "<tr><td colspan=\"2\" style=\"padding:10px 12px;border-bottom:1px solid #e2e8f0;\">"
            f"<div style=\"font-weight:700;color:#0f172a;\">{msg_e}</div>"
            f"<div style=\"font-size:12px;color:#475569;margin-top:4px;word-break:break-all;\">{page_e}</div>"
            "<div style=\"margin-top:6px;font-size:13px;\">"
            f"<strong>{curr:.0f}</strong> "
            f"<span style=\"color:#64748b;\">(önceki: {prev:.0f})</span> "
            f"<span style=\"color:{color};font-weight:700;\">{pct:+.1f}%</span>"
            "</div></td></tr>"
        )

    n = len(alarms)
    summary = f"{n} sayfa alarmı" if n > 1 else "Sayfa alarmı"
    summary_e = html.escape(summary)
    html_body = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 640px;">
            <p style="color:#64748b;font-size:14px;">{summary_e} — {dom_e} ({prof_e})</p>
            <table style="border-collapse:collapse;width:100%;margin:16px 0;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
            <tbody>
            {''.join(rows)}
            </tbody></table>
            <p style="color:#94a3b8;font-size:12px;margin-top:24px;">SEO Agent Realtime — aynı konu altında gruplanır (Gmail).</p>
        </div>
        """
    subject = f"Sayfa alarmları — {domain} ({profile_label})"
    send_realtime_email(subject, html_body, thread_kind="page", thread_key=thread_key)


def run_page_alarm_check_all_sites(db: Session, *, window_minutes: int = 30) -> list[dict[str, Any]]:
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

    triggered: list[dict[str, Any]] = []
    plabel = {"web": "Desktop", "mweb": "Mobile Web", "android": "Android", "ios": "iOS"}.get(profile, profile)
    tag = f"{site_domain} {plabel}" if site_domain else plabel

    prev_map: dict[str, dict[str, Any]] = {p["page"]: p for p in previous_pages}
    curr_map: dict[str, dict[str, Any]] = {p["page"]: p for p in current_pages}

    for page_path, curr in curr_map.items():
        curr_users = curr.get("activeUsers", 0)
        prev = prev_map.get(page_path)

        if prev:
            prev_users = prev.get("activeUsers", 0)

            rule = NEWS_ALARM_RULES["news_traffic_drop"]
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
                        "message": (
                            f"📉 Haberler [{tag}] {page_path[:60]} — "
                            f"{prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)"
                        ),
                    })

            rule = NEWS_ALARM_RULES["news_traffic_spike"]
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
                        "message": (
                            f"📈 Haberler [{tag}] {page_path[:60]} — "
                            f"{prev_users:.0f} → {curr_users:.0f} ({pct:+.1f}%)"
                        ),
                    })
        else:
            rule = NEWS_ALARM_RULES["news_new_entry"]
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
                    "message": (
                        f"🆕 Haberler [{tag}] {page_path[:60]} — "
                        f"listeye girdi ({curr_users:.0f} kullanıcı)"
                    ),
                })

    rule = NEWS_ALARM_RULES["news_disappeared"]
    for page_path, prev in prev_map.items():
        if page_path not in curr_map:
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
                    "message": (
                        f"⚠️ Haberler [{tag}] {page_path[:60]} — "
                        f"listeden düştü (önceki: {prev_users:.0f})"
                    ),
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
    dom_e = html.escape(domain)
    prof_e = html.escape(profile_label)
    thread_key = _realtime_email_thread_key(domain, profile)
    rows: list[str] = []
    for alarm in alarms:
        page = alarm.get("page", "bilinmiyor")
        page_e = html.escape(page)
        msg_e = html.escape(alarm.get("message", ""))
        curr = alarm.get("current_users", 0)
        prev = alarm.get("previous_users", 0)
        pct = alarm.get("change_pct", 0)
        color = "#dc2626" if pct < 0 else "#16a34a"
        rows.append(
            "<tr><td colspan=\"2\" style=\"padding:10px 12px;border-bottom:1px solid #e2e8f0;\">"
            f"<div style=\"font-weight:700;color:#0f172a;\">{msg_e}</div>"
            f"<div style=\"font-size:12px;color:#475569;margin-top:4px;word-break:break-all;\">{page_e}</div>"
            "<div style=\"margin-top:6px;font-size:13px;\">"
            f"<strong>{curr:.0f}</strong> "
            f"<span style=\"color:#64748b;\">(önceki: {prev:.0f})</span> "
            f"<span style=\"color:{color};font-weight:700;\">{pct:+.1f}%</span>"
            "</div></td></tr>"
        )

    n = len(alarms)
    summary = f"{n} haber trafiği alarmı" if n > 1 else "Haber trafiği alarmı"
    summary_e = html.escape(summary)
    html_body = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 640px;">
            <p style="color:#64748b;font-size:14px;">{summary_e} — {dom_e} ({prof_e})</p>
            <p style="color:#64748b;font-size:13px;margin-top:0;">GA4 Realtime «Haberler» (unifiedScreenName, sezgisel filtre).</p>
            <table style="border-collapse:collapse;width:100%;margin:16px 0;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
            <tbody>
            {''.join(rows)}
            </tbody></table>
            <p style="color:#94a3b8;font-size:12px;margin-top:24px;">SEO Agent Realtime — aynı konu altında gruplanır (Gmail).</p>
        </div>
        """
    subject = f"Haberler — {domain} ({profile_label})"
    send_realtime_news_email(subject, html_body, thread_kind="news", thread_key=thread_key)


def check_news_alarms_for_site(
    db: Session,
    site: Site,
    *,
    profile: str = "web",
    window_minutes: int = 15,
    interval_minutes: int = 15,
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
        _send_news_alarm_email(site.domain, profile, alarms)

    return alarms


def run_news_alarm_check_all_sites(db: Session) -> list[dict[str, Any]]:
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
                )
                all_alarms.extend(alarms)
            except Exception as exc:
                logger.exception("Haber alarm check hatası [%s/%s]: %s", site.domain, profile, exc)

    return all_alarms
