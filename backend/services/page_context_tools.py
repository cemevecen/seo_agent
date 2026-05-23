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


def format_page_context_for_prompt(ctx: dict[str, Any] | None) -> str:
    """System prompt'a eklenecek sayfa bağlamı metni."""
    if not ctx or not isinstance(ctx, dict):
        return ""
    try:
        blob = json.dumps(_trim(ctx, max_str=1200, max_list=15), ensure_ascii=False, default=str)
    except Exception:
        blob = str(ctx)[:8000]
    if len(blob) > 12000:
        blob = blob[:12000] + "…"
    label = ctx.get("label") or ctx.get("page_id") or ctx.get("path") or "bilinmiyor"
    return (
        f"\n\n## aktif sayfa bağlamı (kullanıcı şu anda «{label}» ekranında)\n"
        "kullanıcı «bu sayfa», «ekranda görünen», «şu filtrelerle» dediğinde önce bu JSON'a bak; "
        "yetersizse ilgili page_fetch_* aracını çağır.\n"
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
        "version_count": len(payload.get("version_chips") or []),
    }


def page_fetch_inbox_threads(route: str = "all", limit: int = 15) -> dict[str, Any]:
    """Inbox sekmesindeki thread listesi özeti."""
    from backend.models import SupportInboxThread
    from backend.services import inbox_sync

    r = (route or "all").strip().lower()
    if r not in inbox_sync.INBOX_TAB_ROUTE_TAGS:
        return {"error": f"Geçersiz route: {r}"}
    lim = max(1, min(int(limit), 50))
    with SessionLocal() as db:
        q = (
            db.query(SupportInboxThread)
            .filter(SupportInboxThread.route_tag.in_(inbox_sync.INBOX_TAB_ROUTE_TAGS[r]))
            .order_by(SupportInboxThread.last_internal_ms.desc())
        )
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
