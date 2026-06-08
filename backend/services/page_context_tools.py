"""AI Talk — sayfa bağlamına göre veri çeken read-only araçlar."""
from __future__ import annotations

import json
import logging
from typing import Any

from backend.database import SessionLocal

LOGGER = logging.getLogger(__name__)


def _trim(obj: Any, *, max_str: int = 500, max_list: int = 20) -> Any:
    if isinstance(obj, str):
        return obj[:max_str]
    if isinstance(obj, list):
        return [_trim(x, max_str=max_str, max_list=max_list) for x in obj[:max_list]]
    if isinstance(obj, dict):
        return {k: _trim(v, max_str=max_str, max_list=max_list) for k, v in list(obj.items())[:40]}
    return obj


_PAGE_ANALYSIS_HINTS: dict[str, str] = {
    "ad": (
        "monetizasyon: page_fetch_mz_analytics ile KPI + by_date + kırılımları al; "
        "net_revenue/impression/eCPM/CTR/coverage birlikte yorumla; compare.deltas ve leaders_losers varsa kullan; "
        "custom.drill ile seçili birim/dilimi analize dahil et."
    ),
    "firebase": (
        "crashlytics: page_fetch_crashlytics_summary (product/platform/days = filters); "
        "daily_trend + crash_free + top_issues + cihaz/OS ile spike/kronik ayrımı yap; "
        "visible_text/dom_snapshot sadece ekrandaki sekme/filtre için, sayılar tool'dan."
    ),
    "home": "günün özeti: page_fetch_home_dashboard; anomali ve düşüşleri önceliklendir.",
    "ga4": "trafik: realtime veya site listesi; kaynak/sayfa kayması ve alarm varsa çıkarım.",
    "realtime": "anlık kullanıcı + alarm; baseline'dan sapmayı yorumla.",
    "app": (
        "App Store Connect özeti: page_fetch_asc_analytics (impression, conversion, redownload, sales); "
        "mağaza yorumları için page_fetch_app_intel."
    ),
    "errors": "404/5xx hacmi ve URL kalıpları; SEO/teknik kök neden hipotezi.",
    "inbox": "thread özeti + yanıt önerisi; iş etkisini kısaca değerlendir.",
    "intelligence": "haber kümesi; ortak tema ve operasyonel etki.",
}


def _analysis_hints_for_context(ctx: dict[str, Any]) -> str:
    page_id = (ctx.get("page_id") or "").strip().lower()
    custom = ctx.get("custom") if isinstance(ctx.get("custom"), dict) else {}
    if custom.get("page"):
        page_id = str(custom.get("page")).strip().lower()
    path = (ctx.get("path") or "").strip().lower()
    if not page_id and path.startswith("/ad"):
        page_id = "ad"
    if not page_id and path.startswith("/firebase"):
        page_id = "firebase"
    hint = _PAGE_ANALYSIS_HINTS.get(page_id, "")
    tool = ctx.get("suggested_tool")
    if tool and not hint:
        hint = f"önerilen araç: {tool}; sayıları çekip analitik iskelet (ölçülen→gözlem→çıkarım→risk→öneri) ile yanıtla."
    return hint


def format_page_context_for_prompt(ctx: dict[str, Any] | None) -> str:
    """System prompt'a eklenecek sayfa bağlamı metni."""
    if not ctx or not isinstance(ctx, dict):
        return ""
    hints = _analysis_hints_for_context(ctx)
    ctx_out = dict(ctx)
    if hints:
        ctx_out["analysis_hints"] = hints
    try:
        blob = json.dumps(_trim(ctx_out, max_str=2200, max_list=22), ensure_ascii=False, default=str)
    except Exception:
        blob = str(ctx_out)[:8000]
    if len(blob) > 12000:
        blob = blob[:12000] + "…"
    label = ctx.get("label") or ctx.get("page_id") or ctx.get("path") or "bilinmiyor"
    return (
        f"\n\n## aktif sayfa bağlamı (kullanıcı şu anda «{label}» ekranında)\n"
        "kullanıcı «bu sayfa», «ekranda görünen», «şu filtrelerle» dediğinde önce bu JSON'a bak; "
        "yetersizse ilgili page_fetch_* aracını çağır. "
        "yanıt: rakam + trend/yoğunluk çıkarımı + risk/fırsat + en fazla 3 öncelikli öneri.\n"
        f"```json\n{blob}\n```"
    )


def page_fetch_crashlytics_summary(
    product: str = "doviz",
    platform: str = "all",
    days: int = 7,
    limit_issues: int = 8,
) -> dict[str, Any]:
    """Firebase/Crashlytics özet — fatal/anr/non_fatal, crash-free, top issue'lar."""
    from backend.services import crashlytics_bq as cbq

    pid = (product or "doviz").strip().lower()
    plat = (platform or "all").strip().lower()
    days_i = max(1, min(int(days), 90))
    lim = max(1, min(int(limit_issues), 15))

    try:
        payload = cbq.build_full_payload(pid, days_i, plat)
    except Exception as exc:
        LOGGER.exception("page_fetch_crashlytics_summary")
        return {"error": str(exc)}

    if not payload or payload.get("error"):
        return {
            "ok": False,
            "product": pid,
            "message": payload.get("message") or payload.get("error") or "veri alınamadı",
        }

    totals = payload.get("totals") or {}
    issues_flat: list[dict] = []
    by_plat = payload.get("issues_by_platform") or {}
    for plat_key, rows in by_plat.items():
        for row in (rows or [])[:lim]:
            issues_flat.append(
                {
                    "platform": plat_key,
                    "title": (row.get("title") or row.get("issue_title") or "")[:200],
                    "event_count": row.get("event_count"),
                    "issue_id": row.get("issue_id") or row.get("id"),
                }
            )
    if not issues_flat:
        for row in (payload.get("issues") or [])[:lim]:
            issues_flat.append(
                {
                    "platform": row.get("platform"),
                    "title": (row.get("title") or row.get("issue_title") or "")[:200],
                    "event_count": row.get("event_count"),
                    "issue_id": row.get("issue_id") or row.get("id"),
                }
            )
    issues_flat.sort(key=lambda x: -(int(x.get("event_count") or 0)))
    issues_flat = issues_flat[:lim]

    trend = payload.get("trend") or []
    if not isinstance(trend, list):
        trend = []
    trend_tail = trend[-min(days_i, 21) :]

    devices = payload.get("device_breakdown") or []
    os_rows = payload.get("os_breakdown") or []
    versions = payload.get("versions") or payload.get("version_chips") or []

    return {
        "ok": True,
        "product": pid,
        "platform": plat,
        "days": days_i,
        "totals": totals,
        "crash_free_pct": payload.get("crash_free_pct"),
        "crash_free_sessions_pct": payload.get("crash_free_sessions_pct"),
        "data_days": payload.get("data_days"),
        "top_issues": issues_flat,
        "version_count": len(payload.get("version_chips") or versions or []),
        "daily_trend": trend_tail,
        "top_versions": (versions if isinstance(versions, list) else [])[:10],
        "top_devices": (devices if isinstance(devices, list) else [])[:8],
        "top_os": (os_rows if isinstance(os_rows, list) else [])[:8],
        "summary_by_platform": payload.get("summary_by_platform"),
    }


def page_fetch_mz_analytics(
    project: str | None = None,
    branch: str | None = None,
    start: str | None = None,
    end: str | None = None,
    income_types: str | None = None,
    platforms: str | None = None,
    channels: str | None = None,
    surfaces: str | None = None,
    sources: str | None = None,
    search: str | None = None,
    compare_mode: str | None = None,
    compare_start: str | None = None,
    compare_end: str | None = None,
    stream: str | None = None,
) -> dict[str, Any]:
    """Monetizasyon (/ad) özeti — KPI, günlük trend, gelir tipi ve birim kırılımları."""
    from backend.services import ad_analytics_store as store
    from backend.services.ad_analytics_store import AD_STREAMS

    proj = (project or "").strip() or None
    br = (branch or "").strip() or None
    sk = (stream or "").strip() or None

    if sk and (not proj or not br):
        for meta in AD_STREAMS:
            if meta.key == sk:
                proj = meta.project
                br = meta.branch
                break

    with SessionLocal() as db:
        if not proj or not br:
            fac = store.facets(db)
            for s in fac.get("streams") or []:
                if s.get("has_data"):
                    proj = s.get("project")
                    br = s.get("branch")
                    if sk and s.get("key") != sk:
                        continue
                    break

        if not proj or not br:
            return {"ok": False, "message": "Reklam verisi veya proje/dal seçimi yok."}

        raw = store.query_summary(
            db,
            start=start,
            end=end,
            income_types=income_types,
            platforms=platforms,
            channels=channels,
            surfaces=surfaces,
            sources=sources,
            search=search,
            project=proj,
            branch=br,
            compare_mode=compare_mode,
            compare_start=compare_start,
            compare_end=compare_end,
        )

    by_date = raw.get("by_date") or []
    if len(by_date) > 21:
        by_date = by_date[-21:]

    compare = raw.get("compare")
    compare_out = None
    if compare and isinstance(compare, dict):
        compare_out = {
            "mode": compare.get("mode"),
            "range": compare.get("range"),
            "deltas": compare.get("deltas"),
            "leaders_losers": compare.get("leaders_losers"),
        }

    return _trim(
        {
            "ok": True,
            "project": proj,
            "branch": br,
            "stream": sk,
            "range": raw.get("range"),
            "kpi_available": raw.get("kpi_available"),
            "kpis": raw.get("kpis"),
            "funnel": raw.get("funnel"),
            "by_date": by_date,
            "by_income_type": (raw.get("by_income_type") or [])[:15],
            "by_ad_unit": (raw.get("by_ad_unit") or [])[:12],
            "by_platform": (raw.get("by_platform") or [])[:10],
            "by_channel": (raw.get("by_channel") or [])[:10],
            "by_surface": (raw.get("by_surface") or [])[:10],
            "compare": compare_out,
        },
        max_str=800,
        max_list=22,
    )


def page_fetch_inbox_threads(route: str = "all", limit: int = 15) -> dict[str, Any]:
    """Inbox sekmesindeki thread listesi özeti."""
    from backend.models import SupportInboxThread
    from backend.services import inbox_sync

    r = (route or "all").strip().lower()
    if r not in inbox_sync.INBOX_TAB_ROUTE_TAGS:
        return {"error": f"Geçersiz route: {r}"}
    lim = max(1, min(int(limit), 50))
    with SessionLocal() as db:
        q = db.query(SupportInboxThread).order_by(SupportInboxThread.last_internal_ms.desc())
        if r == inbox_sync.INBOX_ROUTE_ANSWERED:
            q = q.filter(SupportInboxThread.answered_flag.is_(True))
        else:
            q = q.filter(SupportInboxThread.route_tag.in_(inbox_sync.INBOX_TAB_ROUTE_TAGS[r]))
            q = q.filter(SupportInboxThread.answered_flag.is_(False))
            if r == inbox_sync.INBOX_ROUTE_NSTAT:
                q = q.filter(SupportInboxThread.subject.ilike("%ziyaret edilen sayfalar%"))
        rows = q.limit(lim).all()
        unread = sum(1 for t in rows if t.gmail_unread)
        unanswered = sum(1 for t in rows if not t.answered_flag)
        items = [
            {
                "id": t.id,
                "subject": (t.subject or "")[:160],
                "route_tag": t.route_tag,
                "unread": bool(t.gmail_unread),
                "answered": bool(t.answered_flag),
                "snippet": (t.snippet or "")[:180],
                "has_summary": bool((t.ai_summary or "").strip()),
            }
            for t in rows
        ]
    return {
        "route": r,
        "count": len(items),
        "unread": unread,
        "unanswered": unanswered,
        "threads": items,
    }


def page_fetch_inbox_thread(thread_id: int) -> dict[str, Any]:
    """Tek inbox thread detayı + AI özet/taslak."""
    from backend.models import SupportInboxMessage, SupportInboxThread

    tid = int(thread_id)
    with SessionLocal() as db:
        t = db.query(SupportInboxThread).filter(SupportInboxThread.id == tid).first()
        if not t:
            return {"error": "thread bulunamadı"}
        msgs = (
            db.query(SupportInboxMessage)
            .filter(SupportInboxMessage.thread_id == t.id)
            .order_by(SupportInboxMessage.internal_ms.asc())
            .limit(30)
            .all()
        )
        return {
            "id": t.id,
            "subject": t.subject,
            "route_tag": t.route_tag,
            "unread": bool(t.gmail_unread),
            "answered": bool(t.answered_flag),
            "ai_summary": (t.ai_summary or "")[:4000] or None,
            "ai_draft_reply": (t.ai_draft_reply or "")[:4000] or None,
            "messages": [
                {
                    "from": m.from_addr,
                    "is_outbound": bool(m.is_outbound),
                    "snippet": (m.body_text or "")[:500],
                }
                for m in msgs[-12:]
            ],
        }


def page_fetch_news_intelligence(hours: int = 12, source: str = "", limit: int = 20) -> dict[str, Any]:
    """NEWS / intelligence son haberler."""
    from datetime import datetime, timedelta

    from backend.models import NewsIntelligenceItem
    from backend.services.news_intelligence import RETENTION_HOURS, dedupe_news_rows

    hrs = max(1, min(int(hours), RETENTION_HOURS))
    lim = max(1, min(int(limit), 40))
    cutoff = datetime.utcnow() - timedelta(hours=hrs)
    with SessionLocal() as db:
        q = (
            db.query(NewsIntelligenceItem)
            .filter(NewsIntelligenceItem.published_at >= cutoff)
            .filter(NewsIntelligenceItem.source_name.notin_(["Unknown", "Bilinmiyor", ""]))
            .order_by(NewsIntelligenceItem.published_at.desc())
        )
        src = (source or "").strip()
        if src:
            q = q.filter(NewsIntelligenceItem.source_name == src)
        rows = dedupe_news_rows(q.limit(lim * 2).all())[:lim]
        items = [
            {
                "headline": r.headline,
                "source_name": r.source_name,
                "category": r.category,
                "published_at": r.published_at.isoformat() if r.published_at else None,
            }
            for r in rows
        ]
        sources = sorted({r.source_name for r in rows if r.source_name})
    return {"hours": hrs, "source_filter": src or None, "count": len(items), "sources": sources, "items": items}


def page_fetch_asc_analytics(
    product: str = "doviz",
    period_days: int = 30,
    country: str = "all",
) -> dict[str, Any]:
    """App Store Connect Analytics + Sales özeti (sentetik değil; API)."""
    from backend.services import asc_analytics, asc_client
    from backend.services.app_intel import APP_PRODUCTS

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": f"bilinmeyen ürün: {pid}"}
    period = int(period_days)
    if period not in (0, 1, 7, 14, 30, 90, 365):
        period = 30
    cc = (country or "all").strip().lower()
    bundle = (APP_PRODUCTS[pid].get("ios_bundle_id") or "").strip()
    if not asc_client.is_configured():
        return {"ok": False, "message": "ASC_KEY_ID / ISSUER_ID / PRIVATE_KEY tanımlı değil."}

    analytics = asc_analytics.fetch_analytics_summary(bundle_id=bundle, days=period, country=cc)
    sales = None
    import os

    if (os.getenv("ASC_VENDOR_NUMBER") or "").strip():
        sales = asc_client.fetch_daily_sales_summary(
            bundle_id=bundle, days=period, country=cc,
        )

    from backend.services.store_rollout import fetch_store_rollout

    rollout = fetch_store_rollout(pid)

    return _trim(
        {
            "ok": True,
            "product": pid,
            "period_days": period,
            "country": cc,
            "analytics": analytics,
            "sales_summary": sales,
            "store_rollout": rollout,
        },
        max_str=900,
        max_list=25,
    )


def page_fetch_app_intel(product: str = "doviz", period_days: int = 30) -> dict[str, Any]:
    """App Store / Play intel özeti."""
    from backend.services.app_intel import APP_PRODUCTS, build_intel_payload

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": f"bilinmeyen ürün: {pid}"}
    period = int(period_days)
    if period not in (0, 7, 30, 90, 180, 365, 730):
        period = 30
    try:
        payload = build_intel_payload(pid, period, cache_only=True)
        if payload.get("error") == "no_cached_data":
            payload = build_intel_payload(pid, period, cache_only=False)
    except Exception as exc:
        return {"error": str(exc)}

    kpis = payload.get("kpis") or {}
    return _trim(
        {
            "product": pid,
            "period_days": period,
            "kpis": kpis,
            "store_rating": payload.get("store_rating"),
            "review_summary": payload.get("review_summary"),
            "asc_highlights": (payload.get("asc") or {}).get("highlights"),
            "error": payload.get("error"),
        },
        max_str=800,
        max_list=12,
    )


def page_fetch_errors_summary(site_id: int, days: int = 7) -> dict[str, Any]:
    """404/5xx hata özeti."""
    from backend.models import Site
    from backend.services.error_monitor import get_error_summary

    sid = int(site_id)
    d = max(1, min(int(days), 30))
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == sid).first()
        if not site:
            return {"error": "site bulunamadı"}
        summary = get_error_summary(db, sid, days=d)
        summary["domain"] = site.domain
        summary["display_name"] = site.display_name
    return _trim(summary, max_str=600, max_list=15)


def page_fetch_ga4_realtime(site_id: int, window: int = 10) -> dict[str, Any]:
    """GA4 realtime tek site özeti."""
    from backend.models import Site
    from backend.services.ga4_realtime import check_site_realtime, get_recent_alarms

    sid = int(site_id)
    w = max(5, min(int(window), 60))
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == sid).first()
        if not site:
            return {"error": "site bulunamadı"}
        result = check_site_realtime(db, site, window_minutes=w, profile="web", skip_alarms=True)
        alarms = get_recent_alarms(db, sid, limit=5)
    return _trim(
        {
            "site_id": sid,
            "domain": site.domain,
            "display_name": site.display_name,
            "window_minutes": w,
            "metrics": {
                k: result.get(k)
                for k in (
                    "active_users",
                    "delta_pct",
                    "baseline_avg",
                    "status",
                    "message",
                    "top_pages",
                    "top_sources",
                )
                if k in result
            },
            "recent_alarms": alarms,
        },
        max_str=500,
        max_list=10,
    )


def page_fetch_home_dashboard() -> dict[str, Any]:
    """Ana sayfa (Günün Özeti) — realtime, GA4, SC, pozisyon düşüşleri."""
    try:
        from backend.main import home_summary_payload

        with SessionLocal() as db:
            return home_summary_payload(db)
    except Exception as exc:
        LOGGER.exception("page_fetch_home_dashboard")
        return {"error": str(exc)}


def page_list_sites(limit: int = 30) -> dict[str, Any]:
    """GA4/errors sayfalarında site_id eşlemesi için site listesi."""
    from backend.models import Site

    lim = max(1, min(int(limit), 60))
    with SessionLocal() as db:
        rows = db.query(Site).order_by(Site.domain.asc()).limit(lim).all()
        items = [
            {"id": s.id, "domain": s.domain, "display_name": s.display_name or s.domain}
            for s in rows
        ]
    return {"sites": items, "count": len(items)}
