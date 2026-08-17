"""Doviz News yayın listesini GA4 + GSC trafik metrikleriyle zenginleştirir."""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.notification_content_traffic import (
    _aggregate_source_breakdown,
    _compute_day_phases,
    extract_article_id_from_path,
    normalize_article_id,
    page_url_matches_article_id,
)
from backend.services.timezone_utils import report_calendar_yesterday
from backend.services.warehouse import get_latest_search_console_rows

LOGGER = logging.getLogger(__name__)

_DEFAULT_SITE_ID = 1
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 300.0
_MAX_GA4_SPAN_DAYS = 90


def _parse_day(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _resolve_traffic_window(
    period_meta: dict[str, Any] | None,
    period_key: str | None,
) -> tuple[str, str, dict[str, Any]]:
    """Yayın dönemine göre GA4 penceresi; 'all' ve uzun aralıklar 90 güne kısaltılır."""
    meta = period_meta or {}
    yesterday = report_calendar_yesterday()
    start = _parse_day(meta.get("start"))
    end = _parse_day(meta.get("end"))
    note = None

    if not end:
        end = yesterday
    if not start:
        start = end - timedelta(days=6)

    if end > yesterday:
        end = yesterday
    if start > end:
        start = end

    span = (end - start).days
    note = None
    if (period_key or "") == "all":
        start = end - timedelta(days=27)
        note = "All: traffic last 28 days (GA4 quota)"
    elif span >= _MAX_GA4_SPAN_DAYS:
        start = end - timedelta(days=_MAX_GA4_SPAN_DAYS - 1)
        note = f"Trafik penceresi {_MAX_GA4_SPAN_DAYS} güne kısaltıldı"

    return (
        start.isoformat(),
        end.isoformat(),
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "note": note,
            "period_key": period_key,
        },
    )


def _gsc_scope_for_period(period_key: str | None, start: str, end: str) -> str:
    key = (period_key or "").strip().lower()
    if key in ("today", "yesterday", "last_7d", "prev_week"):
        return "current_7d_pages"
    s = _parse_day(start)
    e = _parse_day(end)
    if s and e and (e - s).days <= 10:
        return "current_7d_pages"
    return "current_30d_pages"


def _empty_traffic(*, error: str | None = None, window: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "window": window or {},
        "ga4": {
            "views": 0,
            "sessions": 0,
            "matched_articles": 0,
            "profiles": {},
        },
        "gsc": {
            "clicks": 0,
            "impressions": 0,
            "ctr": 0,
            "position": 0,
            "matched_pages": 0,
            "scope": None,
            "source": "none",
        },
        "own_vs_sourced": {
            "own_views": 0,
            "own_sessions": 0,
            "sourced_views": 0,
            "sourced_sessions": 0,
            "own_article_count": 0,
            "sourced_article_count": 0,
        },
        "source_breakdown": {"buckets": [], "channels": [], "source_medium": []},
        "day_phases": [],
        "by_article": {},
    }


def _merge_profile_pages(acc: dict[str, dict[str, float]], pages: list[dict[str, Any]]) -> None:
    for page in pages or []:
        aid = extract_article_id_from_path(str(page.get("page") or page.get("page_url") or ""))
        if not aid:
            continue
        slot = acc.setdefault(aid, {"views": 0.0, "sessions": 0.0})
        slot["views"] += float(page.get("views") or 0.0)
        slot["sessions"] += float(page.get("sessions") or 0.0)


def enrich_doviz_news_traffic(
    db: Session,
    *,
    rows: list[dict[str, Any]],
    period_meta: dict[str, Any] | None = None,
    period_key: str | None = None,
    site_id: int = _DEFAULT_SITE_ID,
    include_day_phases: bool = True,
) -> dict[str, Any]:
    """Seçili dönemdeki sheet satırlarını GA4/GSC ile eşle (batch, kota dostu)."""
    start, end, window = _resolve_traffic_window(period_meta, period_key)
    id_index: dict[str, dict[str, Any]] = {}
    for r in rows or []:
        aid = normalize_article_id(str(r.get("id") or ""))
        if not aid:
            continue
        # Aynı ID birden fazla satırda olursa ilkini tut (sheet sırası)
        id_index.setdefault(aid, r)

    if not id_index:
        out = _empty_traffic(error="Eşleşecek içerik ID yok", window=window)
        out["ok"] = True
        out["error"] = None
        return out

    cache_key = (
        f"{site_id}|{start}|{end}|{period_key}|"
        f"{len(id_index)}|{hash(frozenset(id_index.keys()))}"
    )
    cached = _CACHE.get(cache_key)
    if cached and (time.time() - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    ga4_status = get_ga4_connection_status(db, site_id)
    properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
    if not ga4_status.get("connected"):
        out = _empty_traffic(
            error=str(ga4_status.get("label") or "GA4 not connected"),
            window=window,
        )
        # GSC yine de denenebilir
        try:
            gsc_part = _enrich_gsc(db, site_id, id_index, period_key, start, end)
            out["gsc"] = gsc_part
            out["ok"] = True
            out["error"] = None if gsc_part.get("matched_pages") else out["error"]
            out["by_article"] = _merge_article_maps(id_index, {}, gsc_part.get("by_article") or {})
        except Exception as exc:
            LOGGER.warning("Doviz news GSC enrich (GA4 yok) başarısız: %s", exc)
        _CACHE[cache_key] = (time.time(), out)
        return out

    from backend.collectors.ga4 import (
        fetch_ga4_news_detail_pages_metrics,
        fetch_ga4_news_path_daily_metrics,
        fetch_ga4_news_traffic_sources,
    )

    by_article_ga4: dict[str, dict[str, float]] = {}
    profile_totals: dict[str, dict[str, float]] = {}
    source_raw = {"channels": [], "source_medium": []}
    daily_rows: list[dict[str, Any]] = []

    def _fetch_profile(pf: str) -> tuple[str, list[dict], dict[str, list[dict]], list[dict]]:
        prop = str(properties.get(pf) or "").strip()
        if not prop:
            return pf, [], {"channels": [], "source_medium": []}, []
        pages = fetch_ga4_news_detail_pages_metrics(
            property_id=prop, start=start, end=end, limit=2000
        )
        sources = fetch_ga4_news_traffic_sources(
            property_id=prop, start=start, end=end, limit=50
        )
        daily: list[dict] = []
        if include_day_phases and pf == "web":
            # Faz hesabı için tek property yeterli (web); mweb ile çift sayımı önler
            daily = fetch_ga4_news_path_daily_metrics(
                property_id=prop, start=start, end=end, limit=5000
            )
        return pf, pages, sources, daily

    profiles = [pf for pf in ("web", "mweb") if str(properties.get(pf) or "").strip()]
    if not profiles:
        out = _empty_traffic(error="GA4 web/mweb property missing", window=window)
        _CACHE[cache_key] = (time.time(), out)
        return out

    try:
        with ThreadPoolExecutor(max_workers=min(2, len(profiles))) as pool:
            results = list(pool.map(_fetch_profile, profiles))
    except Exception as exc:
        LOGGER.exception("Doviz news GA4 batch başarısız")
        out = _empty_traffic(error=str(exc) or "GA4 fetch failed", window=window)
        _CACHE[cache_key] = (time.time(), out)
        return out

    for pf, pages, sources, daily in results:
        matched_pages = []
        for page in pages or []:
            aid = extract_article_id_from_path(str(page.get("page") or ""))
            if aid and aid in id_index:
                matched_pages.append(page)
        _merge_profile_pages(by_article_ga4, matched_pages)
        pf_views = sum(float(p.get("views") or 0) for p in matched_pages)
        pf_sessions = sum(float(p.get("sessions") or 0) for p in matched_pages)
        profile_totals[pf] = {"views": round(pf_views, 1), "sessions": round(pf_sessions, 1)}
        # Kaynak kırılımı: web öncelikli; yoksa mweb
        if pf == "web" or (not source_raw["channels"] and not source_raw["source_medium"]):
            source_raw = sources or source_raw
        if daily:
            daily_rows = daily

    # Yalnızca dönemdeki ID'ler için kaynak kırılımını ölçeklemiyoruz —
    # haber path filtresi zaten haber detayına odaklanır; matched article oranı meta'da.
    source_breakdown = _aggregate_source_breakdown(
        source_raw.get("channels") or [],
        source_raw.get("source_medium") or [],
    )

    own_views = own_sessions = sourced_views = sourced_sessions = 0.0
    own_n = sourced_n = 0
    for aid, metrics in by_article_ga4.items():
        row = id_index.get(aid) or {}
        v = float(metrics.get("views") or 0)
        s = float(metrics.get("sessions") or 0)
        if row.get("is_own"):
            own_views += v
            own_sessions += s
            own_n += 1
        else:
            sourced_views += v
            sourced_sessions += s
            sourced_n += 1

    day_phases: list[dict[str, Any]] = []
    if include_day_phases and daily_rows:
        # Makale bazlı günlükleri yayın tarihine göre birleştir
        phase_acc = {
            "send_day": {"key": "send_day", "label": "Publish day", "sessions": 0.0, "views": 0.0},
            "day_1_3": {"key": "day_1_3", "label": "Days 1–3", "sessions": 0.0, "views": 0.0},
            "day_4_plus": {"key": "day_4_plus", "label": "Day 4+", "sessions": 0.0, "views": 0.0},
        }
        for drow in daily_rows:
            aid = extract_article_id_from_path(str(drow.get("page") or ""))
            if not aid or aid not in id_index:
                continue
            pub = str((id_index[aid].get("date_day") or "")[:10])
            if not pub:
                continue
            phases = _compute_day_phases(
                [{"date": drow.get("date"), "sessions": drow.get("sessions"), "views": drow.get("views")}],
                pub,
            )
            for p in phases:
                key = str(p.get("key") or "")
                if key in phase_acc:
                    phase_acc[key]["sessions"] += float(p.get("sessions") or 0)
                    phase_acc[key]["views"] += float(p.get("views") or 0)
                    # Etiketi yayın diline çek
                    if key == "send_day":
                        phase_acc[key]["label"] = "Publish day"
        day_phases = [
            {
                **p,
                "sessions": round(p["sessions"], 1),
                "views": round(p["views"], 1),
            }
            for p in (phase_acc["send_day"], phase_acc["day_1_3"], phase_acc["day_4_plus"])
            if p["sessions"] > 0 or p["views"] > 0
        ]

    gsc_part = _enrich_gsc(db, site_id, id_index, period_key, start, end)
    by_article = _merge_article_maps(id_index, by_article_ga4, gsc_part.get("by_article") or {})

    total_views = sum(float(m.get("views") or 0) for m in by_article_ga4.values())
    total_sessions = sum(float(m.get("sessions") or 0) for m in by_article_ga4.values())

    out: dict[str, Any] = {
        "ok": True,
        "error": None,
        "window": window,
        "ga4": {
            "views": round(total_views, 1),
            "sessions": round(total_sessions, 1),
            "matched_articles": len(by_article_ga4),
            "profiles": profile_totals,
            "published_with_id": len(id_index),
        },
        "gsc": {
            "clicks": gsc_part.get("clicks", 0),
            "impressions": gsc_part.get("impressions", 0),
            "ctr": gsc_part.get("ctr", 0),
            "position": gsc_part.get("position", 0),
            "matched_pages": gsc_part.get("matched_pages", 0),
            "scope": gsc_part.get("scope"),
            "source": gsc_part.get("source"),
            "note": gsc_part.get("note"),
        },
        "own_vs_sourced": {
            "own_views": round(own_views, 1),
            "own_sessions": round(own_sessions, 1),
            "sourced_views": round(sourced_views, 1),
            "sourced_sessions": round(sourced_sessions, 1),
            "own_article_count": own_n,
            "sourced_article_count": sourced_n,
        },
        "source_breakdown": source_breakdown,
        "day_phases": day_phases,
        "by_article": by_article,
        "notes": [
            n
            for n in (
                window.get("note"),
                "Kaynak kırılımı: dönemdeki tüm haber detay path’leri (GA4)",
                gsc_part.get("note"),
            )
            if n
        ],
    }
    _CACHE[cache_key] = (time.time(), out)
    return out


def _enrich_gsc(
    db: Session,
    site_id: int,
    id_index: dict[str, dict[str, Any]],
    period_key: str | None,
    start: str,
    end: str,
) -> dict[str, Any]:
    scope = _gsc_scope_for_period(period_key, start, end)
    try:
        rows = get_latest_search_console_rows(db, site_id=site_id, data_scope=scope)
    except Exception as exc:
        LOGGER.warning("GSC snapshot okunamadı scope=%s: %s", scope, exc)
        rows = []
    if not rows and scope != "current_30d_pages":
        try:
            rows = get_latest_search_console_rows(db, site_id=site_id, data_scope="current_30d_pages")
            scope = "current_30d_pages"
        except Exception:
            rows = []

    by_article: dict[str, dict[str, float]] = {}
    matched_pages = 0
    clicks = impressions = 0.0
    pos_weight = 0.0
    for r in rows or []:
        url = str(r.get("query") or "")
        if not url:
            continue
        hit_aid = extract_article_id_from_path(url)
        if not hit_aid or hit_aid not in id_index:
            # Fallback: bazı GSC satırlarında ID path dışında kalabilir
            hit_aid = None
            for aid in id_index:
                if page_url_matches_article_id(url, aid):
                    hit_aid = aid
                    break
        if not hit_aid:
            continue
        matched_pages += 1
        c = float(r.get("clicks") or 0)
        i = float(r.get("impressions") or 0)
        p = float(r.get("position") or 0)
        clicks += c
        impressions += i
        pos_weight += p * i
        slot = by_article.setdefault(
            hit_aid, {"gsc_clicks": 0.0, "gsc_impressions": 0.0, "gsc_position": 0.0, "_pos_w": 0.0}
        )
        slot["gsc_clicks"] += c
        slot["gsc_impressions"] += i
        slot["_pos_w"] += p * i

    for slot in by_article.values():
        impr = float(slot.get("gsc_impressions") or 0)
        slot["gsc_position"] = round(float(slot.pop("_pos_w", 0)) / impr, 2) if impr else 0.0
        slot["gsc_ctr"] = round(100.0 * float(slot["gsc_clicks"]) / impr, 2) if impr else 0.0

    ctr = round(100.0 * clicks / impressions, 2) if impressions else 0.0
    position = round(pos_weight / impressions, 2) if impressions else 0.0
    return {
        "clicks": round(clicks, 1),
        "impressions": round(impressions, 1),
        "ctr": ctr,
        "position": position,
        "matched_pages": matched_pages,
        "scope": scope,
        "source": "db",
        "note": "GSC depo snapshot (dönem yaklaşık)",
        "by_article": by_article,
    }


def _merge_article_maps(
    id_index: dict[str, dict[str, Any]],
    ga4_map: dict[str, dict[str, float]],
    gsc_map: dict[str, dict[str, float]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for aid in id_index:
        g = ga4_map.get(aid) or {}
        s = gsc_map.get(aid) or {}
        if not g and not s:
            continue
        out[aid] = {
            "views": round(float(g.get("views") or 0), 1),
            "sessions": round(float(g.get("sessions") or 0), 1),
            "gsc_clicks": round(float(s.get("gsc_clicks") or 0), 1),
            "gsc_impressions": round(float(s.get("gsc_impressions") or 0), 1),
            "gsc_ctr": float(s.get("gsc_ctr") or 0),
            "gsc_position": float(s.get("gsc_position") or 0),
        }
    return out


# ── Platform kırılımı (Android / iOS / Web / mWeb · 1 gün + 7 gün) ────────────
# Web/mWeb: haber detay sayfa yolundan ID çıkarılır.
# Android: `news_detail_opened` olayı · iOS: `screen_view` — ikisinde de `news_id`
# özel parametresi taşınıyor (backend/services/ga4_app_event_config.py).

PLATFORM_KEYS: tuple[str, ...] = ("android", "ios", "web", "mweb")
PLATFORM_WINDOWS: tuple[tuple[str, int], ...] = (("d1", 1), ("d7", 7))
_APP_PROFILE_EVENT = {"android": "news_detail_opened", "ios": "screen_view"}
_PLATFORM_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_PLATFORM_CACHE_TTL_SEC = 300.0


def _empty_platform_matrix(*, error: str | None = None) -> dict[str, Any]:
    return {
        "ok": error is None,
        "error": error,
        "platforms": list(PLATFORM_KEYS),
        "windows": [w for w, _ in PLATFORM_WINDOWS],
        "by_article": {},
        "totals": {},
        "matched": 0,
    }


def _platform_window_dates(days: int) -> tuple[str, str]:
    """GA4 collector'ıyla aynı takvim penceresi (dün bitişli N gün)."""
    end = report_calendar_yesterday()
    start = end - timedelta(days=max(1, int(days)) - 1)
    return start.isoformat(), end.isoformat()


_APP_ID_PARAM_ALTS = ["newsId", "news_ID", "newsid", "article_id", "content_id", "contentId"]
_APP_TITLE_PARAMS = ("news_title", "newsTitle")
_LEADING_ID_RE = re.compile(r"^\s*(\d{4,})")


def _norm_title_key(raw: str) -> str:
    """Başlık eşleşmesi için sadeleştirme — noktalama/boşluk/büyük-küçük farkı silinir."""
    low = unicodedata.normalize("NFKD", str(raw or "")).lower()
    return re.sub(r"[^a-z0-9ğüşıöç]+", "", low)[:90]


def _fetch_app_platform_counts(
    fetch_param_breakdown: Any,
    *,
    property_id: str,
    platform: str,
    days: int,
    id_index: dict[str, dict[str, Any]],
) -> tuple[dict[str, float], dict[str, Any]]:
    """Android/iOS haber açılışlarını ID'ye bağla — sırayla üç strateji.

    iOS'ta özel boyut adı property'ye göre değişebiliyor; tek bir isme güvenmek
    yerine ID → (ID+başlık) → başlık sırasıyla denenir ve hangisinin tuttuğu
    teşhis olarak döner.
    """
    event_name = _APP_PROFILE_EVENT[platform]
    title_index = {_norm_title_key(r.get("title") or ""): aid for aid, r in id_index.items()}
    title_index.pop("", None)
    diag: dict[str, Any] = {"strategy": None, "fetched": 0, "matched": 0, "tried": []}

    def _collect(rows: list[dict[str, Any]], *, by_title: bool) -> dict[str, float]:
        found: dict[str, float] = {}
        for row in rows or []:
            raw = str(row.get("value") or "")
            aid = None
            hit = _LEADING_ID_RE.match(raw)
            if hit:
                aid = normalize_article_id(hit.group(1))
            if not aid:
                aid = normalize_article_id(raw)
            if (not aid or aid not in id_index) and by_title:
                aid = title_index.get(_norm_title_key(raw.split("·")[-1]))
            if aid and aid in id_index:
                found[aid] = found.get(aid, 0.0) + float(row.get("count") or 0)
        return found

    attempts = (
        ("news_id", {"param_key": "news_id", "alt_params": list(_APP_ID_PARAM_ALTS)}, False),
        (
            "news_id+title",
            {
                "param_key": "news_id",
                "alt_params": list(_APP_ID_PARAM_ALTS),
                "param_key_2": _APP_TITLE_PARAMS[0],
                "alt_params_2": [_APP_TITLE_PARAMS[1]],
            },
            True,
        ),
        (
            "news_title",
            {"param_key": _APP_TITLE_PARAMS[0], "alt_params": [_APP_TITLE_PARAMS[1]]},
            True,
        ),
    )
    for name, kwargs, by_title in attempts:
        try:
            rows = fetch_param_breakdown(
                property_id=property_id,
                event_name=event_name,
                days=days,
                limit=500,
                **kwargs,
            )
        except Exception as exc:  # noqa: BLE001
            diag["tried"].append({"strategy": name, "error": str(exc)[:160]})
            continue
        found = _collect(rows, by_title=by_title)
        diag["tried"].append(
            {"strategy": name, "fetched": len(rows or []), "matched": len(found)}
        )
        if found:
            diag.update({"strategy": name, "fetched": len(rows or []), "matched": len(found)})
            return found, diag
    return {}, diag


def fetch_news_platform_breakdown(
    db: Session,
    *,
    rows: list[dict[str, Any]],
    site_id: int = _DEFAULT_SITE_ID,
) -> dict[str, Any]:
    """Haber ID → platform × dönem görüntüleme sayısı.

    Web/mWeb `screenPageViews`, Android/iOS ilgili olayın `eventCount` değeridir;
    ikisi de "haber kaç kez açıldı" sorusunu ölçer, sütunlar karşılaştırılabilir.
    """
    id_index: dict[str, dict[str, Any]] = {}
    for r in rows or []:
        aid = normalize_article_id(str(r.get("id") or ""))
        if aid:
            id_index.setdefault(aid, r)
    if not id_index:
        return _empty_platform_matrix()

    cache_key = f"{site_id}|{len(id_index)}|{hash(frozenset(id_index.keys()))}"
    hit = _PLATFORM_CACHE.get(cache_key)
    if hit and (time.time() - hit[0]) < _PLATFORM_CACHE_TTL_SEC:
        return hit[1]

    ga4_status = get_ga4_connection_status(db, site_id)
    if not ga4_status.get("connected"):
        return _empty_platform_matrix(error=str(ga4_status.get("label") or "GA4 not connected"))
    properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}

    from backend.collectors.ga4 import (
        fetch_ga4_event_param_breakdown,
        fetch_ga4_news_detail_pages_metrics,
    )

    diagnostics: dict[str, dict[str, Any]] = {}

    def _job(task: tuple[str, str, int]) -> tuple[str, str, dict[str, float]]:
        platform, win_key, days = task
        prop = str(properties.get(platform) or "").strip()
        if not prop:
            return platform, win_key, {}
        out: dict[str, float] = {}
        try:
            if platform in _APP_PROFILE_EVENT:
                out, diag = _fetch_app_platform_counts(
                    fetch_ga4_event_param_breakdown,
                    property_id=prop,
                    platform=platform,
                    days=days,
                    id_index=id_index,
                )
                diagnostics[f"{platform}:{win_key}"] = diag
            else:
                start, end = _platform_window_dates(days)
                pages = fetch_ga4_news_detail_pages_metrics(
                    property_id=prop, start=start, end=end, limit=2000
                )
                for page in pages or []:
                    aid = extract_article_id_from_path(str(page.get("page") or ""))
                    if aid and aid in id_index:
                        out[aid] = out.get(aid, 0.0) + float(page.get("views") or 0)
                diagnostics[f"{platform}:{win_key}"] = {
                    "fetched": len(pages or []),
                    "matched": len(out),
                    "strategy": "page_path",
                }
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Doviz news platform kırılımı [%s/%s]: %s", platform, win_key, exc)
            diagnostics[f"{platform}:{win_key}"] = {"error": str(exc)[:200]}
        return platform, win_key, out

    tasks = [
        (platform, win_key, days)
        for platform in PLATFORM_KEYS
        for win_key, days in PLATFORM_WINDOWS
        if str(properties.get(platform) or "").strip()
    ]
    if not tasks:
        return _empty_platform_matrix(error="GA4 property tanımlı değil")

    try:
        with ThreadPoolExecutor(max_workers=min(4, len(tasks))) as pool:
            results = list(pool.map(_job, tasks))
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Doviz news platform kırılımı başarısız")
        return _empty_platform_matrix(error=str(exc) or "GA4 fetch failed")

    by_article: dict[str, dict[str, dict[str, float]]] = {}
    totals: dict[str, dict[str, float]] = {}
    for platform, win_key, values in results:
        tot = 0.0
        for aid, val in values.items():
            if val <= 0:
                continue
            by_article.setdefault(aid, {}).setdefault(platform, {})[win_key] = round(val, 1)
            tot += val
        totals.setdefault(platform, {})[win_key] = round(tot, 1)

    out = {
        "ok": True,
        "error": None,
        "platforms": list(PLATFORM_KEYS),
        "windows": [w for w, _ in PLATFORM_WINDOWS],
        "by_article": by_article,
        "totals": totals,
        "matched": len(by_article),
        "diagnostics": diagnostics,
        "metric": "views",
        "note": "Web/mWeb screenPageViews · Android/iOS haber açılış olayı (news_id)",
    }
    _PLATFORM_CACHE[cache_key] = (time.time(), out)
    return out
