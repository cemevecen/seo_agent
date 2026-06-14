from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from backend.karma.config import KARMA_BY_SLUG, karma_competitors_for_domain
from backend.models import NewsIntelligenceItem, RealtimeAlarmLog, Site


def _site_or_404(db: Session, site_id: int) -> Site:
    site = db.query(Site).filter(Site.id == site_id, Site.is_active.is_(True)).first()
    if not site:
        raise ValueError("Site not found")
    return site


def _base_payload(slug: str, site: Site) -> dict[str, Any]:
    item = KARMA_BY_SLUG[slug]
    domain = site.domain or ""
    return {
        "slug": slug,
        "title": item.title,
        "description": item.description,
        "group": item.group,
        "site": {"id": site.id, "domain": domain, "display_name": site.display_name or domain},
        "competitors": karma_competitors_for_domain(domain),
        "summary": "",
        "metrics": [],
        "sections": [],
        "actions": [],
    }


def _intel_recent(db: Session, hours: int = 12, limit: int = 80) -> list[NewsIntelligenceItem]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return (
        db.query(NewsIntelligenceItem)
        .filter(NewsIntelligenceItem.published_at >= cutoff)
        .order_by(desc(NewsIntelligenceItem.published_at))
        .limit(limit)
        .all()
    )


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[a-zA-ZçğıöşüÇĞİÖŞÜ0-9]{4,}", (text or "").lower())
    stop = {"için", "olarak", "daha", "sonra", "haber", "günü", "bugün", "yeni", "ile", "veya", "this", "that", "with"}
    return [w for w in words if w not in stop]


def karma_trend_radar(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("trend-radar", site)
    rows = _intel_recent(db)
    is_sinemalar = "sinemalar" in (site.domain or "").lower()

    topic_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    scored: list[tuple[float, NewsIntelligenceItem]] = []

    alarms = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(20)
        .all()
    )

    for row in rows:
        topic = (row.topic or row.category or "").strip()
        if topic:
            topic_counter[topic] += 1
        source_counter[row.source_name or ""] += 1
        score = 1.0
        if not row.is_in_our_site:
            score += 1.5
        if topic:
            score += min(topic_counter[topic], 5) * 0.3
        scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_topics = topic_counter.most_common(8)

    out["summary"] = (
        f"Son 12 saatte {len(rows)} haber sinyali; "
        f"{sum(1 for r in rows if not r.is_in_our_site)} tanesi sitede karşılığı zayıf veya yok."
    )
    out["metrics"] = [
        {"label": "Haber sinyali", "value": str(len(rows))},
        {"label": "Gap aday", "value": str(sum(1 for r in rows if not r.is_in_our_site))},
        {"label": "Realtime alarm", "value": str(len(alarms))},
        {"label": "Top topic", "value": top_topics[0][0] if top_topics else "—"},
    ]

    trend_items = []
    for score, row in scored[:15]:
        badges = []
        if not row.is_in_our_site:
            badges.append("gap")
        if row.topic:
            badges.append(row.topic)
        trend_items.append(
            {
                "title": row.headline,
                "subtitle": row.source_name,
                "badge": " · ".join(badges) if badges else f"skor {score:.1f}",
                "href": row.url,
                "meta": {"published": row.published_at.isoformat() if row.published_at else ""},
            }
        )

    alarm_items = [
        {
            "title": (a.message or a.rule_id or "Alarm")[:120],
            "subtitle": (a.metric or "") + " · " + (a.triggered_at.strftime("%H:%M") if a.triggered_at else ""),
            "badge": a.severity or "info",
            "href": "/realtime",
        }
        for a in alarms[:10]
    ]

    out["sections"] = [
        {"title": "Trend skoru (yüksek → düşük)", "items": trend_items},
        {"title": "Son realtime alarmlar", "items": alarm_items},
        {
            "title": "Topic yoğunluğu",
            "items": [{"title": t, "subtitle": f"{c} haber", "badge": "topic"} for t, c in top_topics],
        },
    ]
    if is_sinemalar:
        out["actions"] = [{"label": "Vizyon takvimi", "href": "/tmdb-upcoming"}]
    else:
        out["actions"] = [{"label": "Realtime", "href": "/realtime"}, {"label": "News", "href": "/intelligence"}]
    return out


def karma_query_haber(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("query-haber", site)
    queries = get_top_queries(db, site, limit=40, device="all")
    intel = _intel_recent(db, hours=24, limit=200)
    intel_text = " ".join((i.headline or "") + " " + (i.content or "")[:200] for i in intel).lower()

    items = []
    gap_count = 0
    for q in queries[:30]:
        query = str(q.get("query") or "")
        if not query:
            continue
        tokens = _tokenize(query)
        hit = any(t in intel_text for t in tokens[:3]) if tokens else False
        in_site = any(query.lower() in (i.headline or "").lower() for i in intel if i.is_in_our_site)
        covered = hit or in_site
        if not covered:
            gap_count += 1
        delta = float(q.get("delta") or 0)
        items.append(
            {
                "title": query,
                "subtitle": f"pos {float(q.get('position', 0)):.1f} · {int(q.get('impressions', 0))} imp",
                "badge": "haber var" if covered else "gap",
                "meta": {"clicks": q.get("clicks"), "delta": delta},
            }
        )

    items.sort(key=lambda x: (0 if x["badge"] == "gap" else 1, -float(x["meta"].get("delta") or 0)))
    out["summary"] = f"{gap_count} yükselen sorguda haber eşleşmesi zayıf veya yok."
    out["metrics"] = [
        {"label": "Sorgu", "value": str(len(items))},
        {"label": "Gap", "value": str(gap_count)},
        {"label": "Domain", "value": site.domain or "—"},
    ]
    out["sections"] = [{"title": "GSC sorgu → haber eşleşmesi", "items": items[:25]}]
    out["actions"] = [{"label": "Search Console", "href": "/search-console"}]
    return out


def karma_rakip_gap(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("rakip-gap", site)
    competitors = out["competitors"]
    rows = _intel_recent(db, hours=24, limit=150)

    comp_items: dict[str, list] = {c: [] for c in competitors}
    our_topics = Counter(_tokenize(" ".join(r.headline for r in rows if r.is_in_our_site)))

    for row in rows:
        src = (row.source_url or row.source_name or "").lower()
        for comp in competitors:
            comp_key = comp.replace("www.", "")
            if comp_key in src or comp_key in (row.source_name or "").lower():
                comp_items[comp].append(row)

    sections = []
    for comp in competitors:
        comp_rows = comp_items.get(comp) or []
        sections.append(
            {
                "title": comp,
                "items": [
                    {
                        "title": r.headline,
                        "subtitle": r.source_name,
                        "badge": "bizde yok" if not r.is_in_our_site else "bizde var",
                        "href": r.url,
                    }
                    for r in comp_rows[:8]
                ]
                or [{"title": "Son 24 saatte kaynak eşleşmesi yok", "subtitle": "Intelligence feed kontrol edin"}],
            }
        )

    missing_topics = []
    for row in rows:
        if row.is_in_our_site:
            continue
        for tok in _tokenize(row.headline)[:5]:
            if our_topics.get(tok, 0) == 0 and tok not in {m["title"] for m in missing_topics}:
                missing_topics.append({"title": tok, "subtitle": row.headline[:80], "badge": "topic gap"})
                if len(missing_topics) >= 12:
                    break

    out["summary"] = f"Rakipler: {', '.join(competitors)} — intelligence kaynaklarından gap taraması."
    out["metrics"] = [
        {"label": "Rakip", "value": str(len(competitors))},
        {"label": "Gap topic", "value": str(len(missing_topics))},
    ]
    out["sections"] = sections + [{"title": "Topic gap adayları", "items": missing_topics}]
    return out


def karma_seasonality(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("seasonality", site)
    is_sinemalar = "sinemalar" in (site.domain or "").lower()
    now = datetime.now(timezone.utc)

    if is_sinemalar:
        events = [
            ("Ocak", "Oscar sezonu, kış filmleri", "Yılın filmleri / ödül listeleri"),
            ("Şubat", "Sevgililer günü vizyon", "Romantik komedi / özel liste"),
            ("Mayıs", "Cannes", "Festival haberleri, fragman"),
            ("Temmuz-Ağu", "Blockbuster sezonu", "Vizyon takvimi yoğun içerik"),
            ("Aralık", "Yıl sonu top 10", "En çok izlenen / beğenilen"),
        ]
    else:
        events = [
            ("Her Çarşamba", "TCMB faiz", "Canlı sayfa + push hazırlığı"),
            ("Ay başı", "Enflasyon TÜFE/ÜFE", "Veri anı trafik spike"),
            ("Cuma", "ABD tarım dışı istihdam", "Dolar / altın canlı"),
            ("Bayram öncesi", "Altın / döviz talebi", "Rehber + hesaplama"),
            ("Seçim dönemi", "Politika / piyasa", "War room modu"),
        ]

    items = []
    for period, trigger, action in events:
        items.append({"title": period, "subtitle": trigger, "badge": action})

    out["summary"] = "Editoryal takvim — geçmiş spike pattern ile birleştirilmeye hazır."
    out["metrics"] = [
        {"label": "Ay", "value": now.strftime("%m")}, {"label": "Olay", "value": str(len(events))}
    ]
    out["sections"] = [{"title": "Mevsimsel hazırlık", "items": items}]
    out["actions"] = [{"label": "Alerts", "href": "/alerts"}]
    return out


def karma_anomaly_tree(db: Session, site_id: int) -> dict[str, Any]:
    from backend.services.ga4_realtime import fetch_traffic_drivers

    site = _site_or_404(db, site_id)
    out = _base_payload("anomaly-tree", site)
    alarms = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(15)
        .all()
    )

    tree_items = []
    for a in alarms:
        tree_items.append(
            {
                "title": f"Alarm · {(a.metric or 'site').split(':')[0]}",
                "subtitle": (a.message or a.rule_id or "")[:100],
                "badge": a.severity or "info",
                "href": "/realtime",
                "meta": {"time": a.triggered_at.isoformat() if a.triggered_at else ""},
            }
        )
        try:
            prof = (a.metric or "web:").split(":")[0] or "web"
            drivers = fetch_traffic_drivers(db, site_id, prof)
            for d in (drivers.get("drivers_increase") or [])[:3]:
                tree_items.append(
                    {
                        "title": "  ↳ artıran: " + (d.get("page") or d.get("path") or d.get("title") or "?")[:60],
                        "subtitle": str(d.get("delta_pct") or d.get("change_pct") or ""),
                        "badge": "driver+",
                    }
                )
            for d in (drivers.get("drivers_decrease") or [])[:2]:
                tree_items.append(
                    {
                        "title": "  ↳ düşüren: " + (d.get("page") or d.get("path") or d.get("title") or "?")[:60],
                        "subtitle": str(d.get("delta_pct") or d.get("change_pct") or ""),
                        "badge": "driver-",
                    }
                )
        except Exception:
            pass
        if len(tree_items) > 40:
            break

    out["summary"] = f"{len(alarms)} son alarm için kök neden ağacı (driver drill-down)."
    out["metrics"] = [{"label": "Alarm", "value": str(len(alarms))}]
    out["sections"] = [{"title": "Alarm → driver ağacı", "items": tree_items}]
    out["actions"] = [{"label": "Realtime", "href": "/realtime"}]
    return out


def karma_brief_generator(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("brief-generator", site)
    gaps = [r for r in _intel_recent(db) if not r.is_in_our_site][:10]
    items = []
    for r in gaps:
        brief = {
            "h1": r.headline[:90],
            "keywords": ", ".join(_tokenize(r.headline)[:6]),
            "angle": r.topic or r.category or "Genel",
            "internal_links": "Canlı döviz / ilgili kategori sayfaları",
        }
        items.append(
            {
                "title": r.headline,
                "subtitle": f"H1: {brief['h1']}",
                "badge": brief["angle"],
                "meta": brief,
            }
        )
    out["summary"] = "Gap haberlerinden otomatik brief taslağı."
    out["sections"] = [{"title": "Brief taslakları", "items": items}]
    out["actions"] = [{"label": "AI Talk", "href": "/ai"}]
    return out


def karma_headline_lab(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("headline-lab", site)
    rows = _intel_recent(db, limit=15)
    items = []
    for r in rows[:8]:
        base = r.headline.strip()
        variants = [
            base,
            base + " | Son durum",
            "SON DAKİKA: " + base if len(base) < 70 else base,
            base.replace("!", "").strip() + " — Detaylar",
        ]
        for i, v in enumerate(variants[:4]):
            score = max(40, 95 - i * 12 - abs(len(v) - 65) // 2)
            items.append({"title": v, "subtitle": f"Varyant {i + 1}", "badge": f"skor {score}"})
    out["summary"] = "Başlık varyantları (uzunluk + format heuristic skoru)."
    out["sections"] = [{"title": "Headline varyantları", "items": items}]
    return out


def karma_ic_link(db: Session, site_id: int) -> dict[str, Any]:
    from backend.models import RealtimePageSnapshot

    site = _site_or_404(db, site_id)
    out = _base_payload("ic-link", site)
    latest = (
        db.query(RealtimePageSnapshot.collected_at)
        .filter(RealtimePageSnapshot.site_id == site_id, RealtimePageSnapshot.profile == "web")
        .order_by(desc(RealtimePageSnapshot.collected_at))
        .limit(1)
        .scalar()
    )
    pages = []
    if latest:
        pages = (
            db.query(RealtimePageSnapshot)
            .filter(
                RealtimePageSnapshot.site_id == site_id,
                RealtimePageSnapshot.profile == "web",
                RealtimePageSnapshot.collected_at == latest,
            )
            .order_by(desc(RealtimePageSnapshot.active_users))
            .limit(15)
            .all()
        )
    items = []
    for i, p in enumerate(pages):
        path = p.page_path or "?"
        users = p.active_users or 0
        items.append(
            {
                "title": path,
                "subtitle": f"{users} aktif",
                "badge": "link kaynağı" if i < 5 else "link hedefi",
            }
        )
    out["summary"] = "Anlık top sayfalardan iç link kaynak/hedef önerisi."
    out["sections"] = [{"title": "Top sayfalar (iç link)", "items": items}]
    out["actions"] = [{"label": "Backlinks", "href": "/backlinks"}]
    return out


def karma_content_decay(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("content-decay", site)
    queries = get_top_queries(db, site, limit=50, device="all")
    decay = [q for q in queries if float(q.get("delta") or 0) < -1]
    decay.sort(key=lambda x: float(x.get("delta") or 0))
    items = [
        {
            "title": str(q.get("query") or ""),
            "subtitle": f"Δ pos {float(q.get('delta', 0)):.1f}",
            "badge": "güncelle",
            "meta": {"action": "refresh|merge|301"},
        }
        for q in decay[:20]
    ]
    out["summary"] = f"{len(decay)} sorguda pozisyon kaybı (decay adayı)."
    out["metrics"] = [{"label": "Decay", "value": str(len(decay))}]
    out["sections"] = [{"title": "Pozisyon düşen sorgular", "items": items}]
    out["actions"] = [{"label": "Alerts", "href": "/alerts"}]
    return out


def karma_topic_cluster(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("topic-cluster", site)
    rows = _intel_recent(db, hours=48)
    clusters: dict[str, int] = defaultdict(int)
    for r in rows:
        key = r.topic or r.category or "Genel"
        clusters[key] += 1
    items = [
        {"title": k, "subtitle": f"{v} haber", "badge": "güçlü" if v >= 5 else "zayıf"}
        for k, v in sorted(clusters.items(), key=lambda x: -x[1])
    ]
    out["summary"] = "Topic cluster yoğunluğu (intelligence feed)."
    out["sections"] = [{"title": "Cluster haritası", "items": items}]
    return out


def _push_platform_totals(row: dict) -> tuple[float, float, float]:
    """(clicks, impressions, ctr%) — tüm platformlar."""
    platforms = row.get("platforms") or {}
    clicks = 0.0
    impressions = 0.0
    for plat in platforms.values():
        if not isinstance(plat, dict):
            continue
        clicks += float(plat.get("click") or 0)
        impressions += float(plat.get("impression") or 0)
    ctr = (clicks / impressions * 100.0) if impressions > 0 else 0.0
    return clicks, impressions, ctr


def _article_id_from_push_text(text: str) -> str:
    m = re.search(r"/(\d{5,})", text or "")
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{6,})\b", text or "")
    return m.group(1) if m else ""


def karma_push_roi(db: Session, site_id: int) -> dict[str, Any]:
    from backend.services.notification_analytics_store import filter_rows_by_date, workspace_state
    from backend.services.notification_content_traffic import resolve_content_traffic

    site = _site_or_404(db, site_id)
    out = _base_payload("push-roi", site)
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=90)).strftime("%Y-%m-%d")

    ws = workspace_state(db, include_rows=True)
    rows = filter_rows_by_date(ws.get("rows") or [], start=start)
    if not rows:
        out["summary"] = "Notification Analytics'te kayıt yok — CSV yükleyin veya API'den çekin."
        out["sections"] = [
            {
                "title": "Push ROI",
                "items": [{"title": "Veri yok", "subtitle": "notification sayfasından veri aktarın", "badge": "—"}],
            }
        ]
        out["actions"] = [{"label": "Notification", "href": "/notification"}]
        return out

    scored: list[tuple[float, dict]] = []
    for row in rows:
        clicks, impressions, ctr = _push_platform_totals(row)
        if clicks <= 0 and impressions <= 0:
            continue
        score = clicks * 1.0 + ctr * 8.0 + min(impressions / 5000.0, 3.0)
        scored.append((score, {**row, "_clicks": clicks, "_impressions": impressions, "_ctr": ctr}))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = [r for _, r in scored[:20]]

    ga4_items: list[dict] = []
    for row in top[:5]:
        aid = _article_id_from_push_text(row.get("text") or "")
        if not aid:
            continue
        try:
            traffic = resolve_content_traffic(
                db,
                content_id=aid,
                headline=str(row.get("text") or ""),
                send_date=(row.get("date") or "")[:10],
                site_id=site_id,
                days=7,
                live=True,
            )
            ga4 = traffic.get("ga4") or {}
            totals = ga4.get("totals") or {}
            sessions = float(totals.get("sessions") or ga4.get("sessions") or 0)
            if sessions > 0:
                ga4_items.append(
                    {
                        "title": f"ID {aid} · {sessions:.0f} oturum (7g)",
                        "subtitle": (row.get("text") or "")[:80],
                        "badge": "GA4",
                    }
                )
        except Exception:
            continue

    items = []
    for row in top[:15]:
        day = (row.get("date") or "")[:10]
        items.append(
            {
                "title": (row.get("text") or "?")[:100],
                "subtitle": f"{day} · {row['_clicks']:.0f} click · {row['_impressions']:.0f} impr · CTR {row['_ctr']:.2f}%",
                "badge": "yüksek ROI" if row["_ctr"] >= 3 else "orta" if row["_ctr"] >= 1 else "düşük",
                "meta": {"push_id": row.get("id"), "article_id": _article_id_from_push_text(row.get("text") or "")},
            }
        )

    avg_ctr = sum(r["_ctr"] for r in top) / len(top) if top else 0.0
    out["summary"] = (
        f"Son 90 günde {len(rows)} push; en iyi {len(top)} konu ortalama CTR %{avg_ctr:.2f}. "
        f"{len(ga4_items)} tanesi GA4 oturumu ile eşleşti."
    )
    out["metrics"] = [
        {"label": "Push kaydı", "value": str(len(rows))},
        {"label": "Ölçülen", "value": str(len(scored))},
        {"label": "Ort. CTR", "value": f"{avg_ctr:.2f}%"},
    ]
    sections = [{"title": "Push konuları (click + CTR skoru)", "items": items}]
    if ga4_items:
        sections.append({"title": "Push → GA4 oturum (7g)", "items": ga4_items})
    out["sections"] = sections
    out["actions"] = [{"label": "Notification", "href": "/notification"}, {"label": "Firebase", "href": "/firebase"}]
    return out


def karma_serp_tracker(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("serp-tracker", site)
    queries = get_top_queries(db, site, limit=25, device="all")
    items = []
    for q in queries:
        pos = float(q.get("position") or 0)
        feat = "snippet aday" if pos <= 3 else "PAA aday" if pos <= 8 else "izle"
        items.append(
            {
                "title": str(q.get("query") or ""),
                "subtitle": f"poz {pos:.1f} · CTR {float(q.get('ctr', 0)) * 100:.1f}%",
                "badge": feat,
            }
        )
    out["summary"] = "Top sorgularda SERP feature fırsatı heuristic."
    out["sections"] = [{"title": "SERP izleme", "items": items}]
    out["actions"] = [{"label": "Search Console", "href": "/search-console"}]
    return out


def karma_programmatic_seo(db: Session, site_id: int) -> dict[str, Any]:
    from backend.models import LighthouseAuditRecord, UrlAuditRecord
    from backend.services.meta_audit import get_audit_issues, get_audit_summary

    site = _site_or_404(db, site_id)
    out = _base_payload("programmatic-seo", site)
    summary = get_audit_summary(db, site_id)
    total = int(summary.get("total_pages") or 0)
    issue_counts = summary.get("issue_counts") or {}
    score_counts = summary.get("score_counts") or {}

    thin_rows = (
        db.query(UrlAuditRecord)
        .filter(UrlAuditRecord.site_id == site_id, UrlAuditRecord.seo_score == "poor")
        .order_by(desc(UrlAuditRecord.search_impressions))
        .limit(8)
        .all()
    )
    dup_titles = int(summary.get("duplicate_title_groups") or 0)
    dup_descs = int(summary.get("duplicate_desc_groups") or 0)

    lh_fails = (
        db.query(LighthouseAuditRecord)
        .filter(
            LighthouseAuditRecord.site_id == site_id,
            LighthouseAuditRecord.audit_state.in_(("fail", "failed")),
        )
        .order_by(desc(LighthouseAuditRecord.collected_at))
        .limit(8)
        .all()
    )

    guard_items = [
        {
            "title": "Thin / poor SEO skoru",
            "subtitle": f"{score_counts.get('poor', 0)} URL · örnek {len(thin_rows)}",
            "badge": "risk" if score_counts.get("poor") else "ok",
        },
        {
            "title": "Duplicate title grupları",
            "subtitle": f"{dup_titles} grup tespit edildi",
            "badge": "risk" if dup_titles else "ok",
        },
        {
            "title": "Duplicate description",
            "subtitle": f"{dup_descs} grup",
            "badge": "risk" if dup_descs else "ok",
        },
        {
            "title": "Noindex sayfalar",
            "subtitle": f"{issue_counts.get('noindex', 0)} URL",
            "badge": "izle" if issue_counts.get("noindex") else "ok",
        },
    ]

    sample_issues = get_audit_issues(db, site_id, filter_key="poor", limit=10)
    issue_items = [
        {
            "title": (r.get("url") or "")[-80:],
            "subtitle": ", ".join(r.get("issues") or [])[:90] or "poor",
            "badge": r.get("seo_score") or "poor",
            "href": r.get("url"),
        }
        for r in sample_issues
    ]

    lh_items = [
        {
            "title": (a.title_tr or a.title_en or a.audit_id)[:90],
            "subtitle": f"{a.section_title_tr or a.section_key} · {a.strategy}",
            "badge": a.priority or a.audit_state,
        }
        for a in lh_fails
    ]

    thin_items = [
        {
            "title": (r.url or "")[-80:],
            "subtitle": f"imp {int(r.search_impressions)} · issues {r.issue_count}",
            "badge": "thin",
            "href": r.url,
        }
        for r in thin_rows
    ]

    out["summary"] = (
        f"SEO audit: {total} URL tarandı — {score_counts.get('poor', 0)} poor, "
        f"{dup_titles} duplicate title grubu, {len(lh_fails)} Lighthouse fail."
    )
    out["metrics"] = [
        {"label": "URL", "value": str(total)},
        {"label": "Poor", "value": str(score_counts.get("poor", 0))},
        {"label": "Dup title", "value": str(dup_titles)},
    ]
    out["sections"] = [
        {"title": "Programmatic guardrail", "items": guard_items},
        {"title": "Poor URL örnekleri", "items": issue_items or thin_items},
        {"title": "Lighthouse fail audit", "items": lh_items or [{"title": "Fail kaydı yok", "subtitle": "PSI temiz"}]},
    ]
    out["actions"] = [{"label": "SEO Audit", "href": "/seo-audit"}]
    return out


_COUNTRY_LABELS: dict[str, str] = {
    "TUR": "Türkiye",
    "USA": "ABD",
    "GBR": "GB",
    "DEU": "Almanya",
    "NLD": "Hollanda",
    "AZE": "Azerbaycan",
    "KAZ": "Kazakistan",
    "FRA": "Fransa",
    "RUS": "Rusya",
    "IRQ": "Irak",
    "SAU": "Suudi Arabistan",
    "ARE": "BAE",
}


def _is_latin_query(q: str) -> bool:
    q = (q or "").strip()
    if not q or re.search(r"[çğıöşüÇĞİÖŞÜ]", q):
        return False
    return bool(re.search(r"[a-zA-Z]{4,}", q))


def karma_international(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_countries, get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("international", site)
    is_sinemalar = "sinemalar" in (site.domain or "").lower()

    countries = get_top_countries(db, site, limit=12)
    queries = get_top_queries(db, site, limit=40, device="all")
    en_queries = [q for q in queries if _is_latin_query(str(q.get("query") or ""))]

    country_items = []
    total_imp = sum(c.get("impressions", 0) for c in countries) or 1.0
    for c in countries:
        cc = c.get("country") or "?"
        share = c.get("impressions", 0) / total_imp * 100.0
        label = _COUNTRY_LABELS.get(cc, cc)
        badge = "TR" if cc == "TUR" else "EN fırsat" if cc in {"USA", "GBR", "DEU", "NLD"} else "global"
        country_items.append(
            {
                "title": f"{label} ({cc})",
                "subtitle": f"{int(c.get('impressions', 0))} imp · {int(c.get('clicks', 0))} click · pay %{share:.1f}",
                "badge": badge,
            }
        )

    en_items = [
        {
            "title": str(q.get("query") or ""),
            "subtitle": f"poz {float(q.get('position', 0)):.1f} · {int(q.get('impressions', 0))} imp",
            "badge": "EN sorgu",
        }
        for q in sorted(en_queries, key=lambda x: -float(x.get("impressions") or 0))[:12]
    ]

    if not country_items:
        country_items = [
            {
                "title": "GSC country verisi alınamadı",
                "subtitle": "Search Console bağlantısı veya kota kontrol edin",
                "badge": "—",
            }
        ]

    if is_sinemalar and not en_items:
        en_items = [
            {"title": "movie / cast EN queries", "subtitle": "GSC Latin sorgu bekleniyor", "badge": "sinemalar"},
        ]

    tr_share = next((c.get("impressions", 0) for c in countries if c.get("country") == "TUR"), 0)
    tr_pct = tr_share / total_imp * 100.0 if countries else 0.0
    out["summary"] = (
        f"GSC ülke segmenti: {len(countries)} ülke · TR payı %{tr_pct:.0f}. "
        f"{len(en_items)} Latin/EN sorgu adayı."
    )
    out["metrics"] = [
        {"label": "Ülke", "value": str(len(countries))},
        {"label": "TR pay", "value": f"{tr_pct:.0f}%"},
        {"label": "EN sorgu", "value": str(len(en_items))},
    ]
    out["sections"] = [
        {"title": "Ülke kırılımı (28g GSC)", "items": country_items},
        {"title": "Lokalizasyon / EN sorgu adayları", "items": en_items},
    ]
    out["actions"] = [{"label": "Search Console", "href": "/search-console"}]
    return out


def karma_editorial_sla(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("editorial-sla", site)
    now = datetime.now(timezone.utc)
    rows = [r for r in _intel_recent(db) if not r.is_in_our_site]
    items = []
    for r in rows[:15]:
        age_h = 0.0
        if r.published_at:
            age_h = (now - r.published_at.replace(tzinfo=timezone.utc if r.published_at.tzinfo is None else r.published_at.tzinfo)).total_seconds() / 3600
        sla = "kritik" if age_h > 2 else "uyarı" if age_h > 1 else "ok"
        items.append(
            {
                "title": r.headline,
                "subtitle": f"{age_h:.1f}s önce · {r.source_name}",
                "badge": sla,
                "href": r.url,
            }
        )
    out["summary"] = "Trend haber yayın gecikmesi (site dışı gap = SLA risk)."
    out["sections"] = [{"title": "SLA takibi", "items": items}]
    out["actions"] = [{"label": "GitLab Boards", "href": "/boards"}]
    return out


def karma_war_room(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("war-room", site)
    alarms = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(5)
        .all()
    )
    intel = _intel_recent(db, limit=5)
    out["summary"] = "Kriz anı özet — realtime + haber + alarm."
    out["metrics"] = [{"label": "Aktif alarm", "value": str(len(alarms))}]
    out["sections"] = [
        {
            "title": "Alarmlar",
            "items": [
                {"title": a.message or a.rule_id or "?", "subtitle": a.metric or "", "badge": a.severity or ""}
                for a in alarms
            ],
        },
        {
            "title": "Son haberler",
            "items": [{"title": i.headline, "subtitle": i.source_name, "href": i.url} for i in intel],
        },
    ]
    out["actions"] = [
        {"label": "Realtime", "href": "/realtime"},
        {"label": "Boards", "href": "/boards"},
    ]
    return out


def karma_post_mortem(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("post-mortem", site)
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    alarms = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id, RealtimeAlarmLog.triggered_at >= cutoff)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(30)
        .all()
    )
    items = [
        {
            "title": (a.triggered_at.strftime("%d.%m %H:%M") if a.triggered_at else "?") + " · " + (a.metric or ""),
            "subtitle": (a.message or "")[:120],
            "badge": a.severity or "info",
        }
        for a in alarms
    ]
    out["summary"] = "Son 7 gün spike/alarm timeline — post-mortem taslağı."
    out["sections"] = [{"title": "Olay timeline", "items": items}]
    return out


def karma_morning_brief(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("morning-brief", site)
    gaps = sum(1 for r in _intel_recent(db) if not r.is_in_our_site)
    alarms = db.query(func.count(RealtimeAlarmLog.id)).filter(RealtimeAlarmLog.site_id == site_id).scalar() or 0
    out["summary"] = f"Editör: {gaps} gap haber · SEO: GSC kontrol · Teknik: {alarms} alarm kaydı."
    out["sections"] = [
        {
            "title": "Editör",
            "items": [{"title": f"{gaps} gap haber", "subtitle": "Intelligence", "badge": "aksiyon"}],
        },
        {
            "title": "SEO",
            "items": [{"title": "Pozisyon düşüşleri", "subtitle": "Alerts sayfası", "badge": "kontrol", "href": "/alerts"}],
        },
        {
            "title": "Teknik",
            "items": [{"title": "Realtime alarmlar", "subtitle": "Son 24s", "badge": "izle", "href": "/realtime"}],
        },
    ]
    out["actions"] = [{"label": "AI Brief", "href": "/ai"}]
    return out


def _crux_verdict(metric_key: str, value: float) -> str:
    thresholds = {
        "largest_contentful_paint": (2500, 4000),
        "interaction_to_next_paint": (200, 500),
        "cumulative_layout_shift": (0.1, 0.25),
        "first_contentful_paint": (1800, 3000),
        "experimental_time_to_first_byte": (800, 1800),
    }
    good, ni = thresholds.get(metric_key, (0, 0))
    if value <= good:
        return "good"
    if value <= ni:
        return "ni"
    return "poor"


def _fmt_cwv(metric_key: str, value: float) -> str:
    if metric_key == "cumulative_layout_shift":
        return f"{value:.2f}"
    if value >= 1000:
        return f"{value / 1000:.1f}s"
    return f"{int(round(value))}ms"


def karma_cwv_trafik(db: Session, site_id: int) -> dict[str, Any]:
    from backend.models import RealtimePageSnapshot
    from backend.services.warehouse import get_latest_crux_snapshot

    site = _site_or_404(db, site_id)
    out = _base_payload("cwv-trafik", site)
    domain = site.domain or ""

    mobile = get_latest_crux_snapshot(db, site_id=site_id, form_factor="mobile") or {}
    desktop = get_latest_crux_snapshot(db, site_id=site_id, form_factor="desktop") or {}
    mob_current = (mobile.get("summary") or {}).get("current") or {}
    desk_current = (desktop.get("summary") or {}).get("current") or {}

    metric_labels = {
        "largest_contentful_paint": "LCP",
        "interaction_to_next_paint": "INP",
        "cumulative_layout_shift": "CLS",
        "first_contentful_paint": "FCP",
        "experimental_time_to_first_byte": "TTFB",
    }

    cwv_items = []
    poor_count = 0
    for key, label in metric_labels.items():
        mob = mob_current.get(key) or {}
        desk = desk_current.get(key) or {}
        mob_val = mob.get("latest")
        desk_val = desk.get("latest")
        if mob_val is None and desk_val is None:
            continue
        primary = mob_val if mob_val is not None else desk_val
        verdict = _crux_verdict(key, float(primary or 0))
        if verdict == "poor":
            poor_count += 1
        cwv_items.append(
            {
                "title": f"{label} · mobil {_fmt_cwv(key, float(mob_val or 0))} / desktop {_fmt_cwv(key, float(desk_val or 0))}",
                "subtitle": f"CrUX p75 · good share mob %{float(mob.get('good_share') or 0):.0f}",
                "badge": verdict,
            }
        )

    latest_rt = (
        db.query(RealtimePageSnapshot.collected_at)
        .filter(RealtimePageSnapshot.site_id == site_id, RealtimePageSnapshot.profile == "web")
        .order_by(desc(RealtimePageSnapshot.collected_at))
        .limit(1)
        .scalar()
    )
    traffic_items = []
    total_active = 0
    if latest_rt:
        pages = (
            db.query(RealtimePageSnapshot)
            .filter(
                RealtimePageSnapshot.site_id == site_id,
                RealtimePageSnapshot.profile == "web",
                RealtimePageSnapshot.collected_at == latest_rt,
            )
            .order_by(desc(RealtimePageSnapshot.active_users))
            .limit(10)
            .all()
        )
        for p in pages:
            users = int(p.active_users or 0)
            total_active += users
            traffic_items.append(
                {
                    "title": p.page_path or "?",
                    "subtitle": f"{users} aktif kullanıcı (şimdi)",
                    "badge": "trafik",
                }
            )

    if not cwv_items:
        cwv_items = [{"title": "CrUX verisi yok", "subtitle": "Data Explorer yenilemesi gerekebilir", "badge": "—"}]

    mob_collected = (mobile.get("collected_at") or "")[:10]
    out["summary"] = (
        f"CrUX ({mob_collected or '—'}) · {poor_count} kötü metrik. "
        f"Anlık web trafik: {total_active} aktif kullanıcı (top sayfalar)."
    )
    out["metrics"] = [
        {"label": "Kötü CWV", "value": str(poor_count)},
        {"label": "Aktif", "value": str(total_active)},
        {"label": "Domain", "value": domain.split(".")[0] if domain else "—"},
    ]
    out["sections"] = [
        {"title": "Core Web Vitals (CrUX)", "items": cwv_items},
        {"title": "Anlık trafik (top sayfa)", "items": traffic_items},
    ]
    out["actions"] = [{"label": "Data Explorer", "href": f"/data-explorer/{domain}"}, {"label": "Realtime", "href": "/realtime"}]
    return out


def karma_ai_action(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries
    from backend.services.meta_audit import get_audit_issues

    site = _site_or_404(db, site_id)
    out = _base_payload("ai-action", site)
    domain = site.domain or ""

    gap = next((r for r in _intel_recent(db) if not r.is_in_our_site), None)
    alarm_row = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(1)
        .first()
    )
    decay_q = None
    for q in get_top_queries(db, site, limit=30, device="all"):
        if float(q.get("delta") or 0) < -1:
            decay_q = q
            break
    audit = (get_audit_issues(db, site_id, filter_key="poor", limit=1) or [None])[0]

    prompts: list[dict] = []

    if gap:
        prompts.append(
            {
                "title": "Gap haber brief",
                "subtitle": (gap.headline or "")[:80],
                "badge": "brief",
                "meta": {
                    "prompt": (
                        f"{domain} için şu gap habere editoryal brief yaz:\n"
                        f"Başlık: {gap.headline}\nKaynak: {gap.source_name}\n"
                        f"H1, anahtar kelimeler, iç link önerisi ve yayın aciliyeti ver."
                    )
                },
            }
        )

    if decay_q:
        qtext = str(decay_q.get("query") or "")
        prompts.append(
            {
                "title": "Decay sorgu aksiyonu",
                "subtitle": qtext[:80],
                "badge": "SEO",
                "meta": {
                    "prompt": (
                        f"GSC'te '{qtext}' sorgusu pozisyon kaybediyor (Δ {float(decay_q.get('delta', 0)):.1f}). "
                        f"Hangi sayfayı güncellemeli, merge mi 301 mi — kısa aksiyon planı yaz."
                    )
                },
            }
        )

    if alarm_row:
        prompts.append(
            {
                "title": "Realtime alarm analizi",
                "subtitle": (alarm_row.message or alarm_row.rule_id or "")[:80],
                "badge": "alarm",
                "meta": {
                    "prompt": (
                        f"Realtime alarm: {alarm_row.message or alarm_row.rule_id}. "
                        f"Metric: {alarm_row.metric}. Olası kök neden ve editör/teknik aksiyon listesi üret."
                    )
                },
            }
        )

    if audit:
        prompts.append(
            {
                "title": "SEO audit düzeltme",
                "subtitle": (audit.get("url") or "")[-70:],
                "badge": "teknik",
                "meta": {
                    "prompt": (
                        f"SEO audit poor URL: {audit.get('url')}\n"
                        f"Sorunlar: {', '.join(audit.get('issues') or [])}\n"
                        f"Öncelikli düzeltme adımlarını madde madde yaz."
                    )
                },
            }
        )

    prompts.append(
        {
            "title": "Sabah brifingi üret",
            "subtitle": f"{domain} — editör + SEO + teknik",
            "badge": "günlük",
            "meta": {
                "prompt": (
                    f"Bugün {domain} için kısa sabah brifingi: gap haberler, GSC düşüşleri, "
                    f"realtime alarmlar ve teknik öncelikler — 3 bölüm halinde."
                )
            },
        }
    )

    prompts.append(
        {
            "title": "GitLab issue taslağı",
            "subtitle": "Boards'a yapıştırılacak format",
            "badge": "issue",
            "href": "/boards",
            "meta": {
                "prompt": (
                    f"{domain} karma modülünden tespit edilen en kritik 1 SEO/içerik konusu için "
                    f"GitLab issue başlığı, açıklama ve kabul kriterleri yaz."
                )
            },
        }
    )

    out["summary"] = f"{len(prompts)} bağlama duyarlı AI Talk promptu — site verisinden üretildi."
    out["sections"] = [{"title": "Hızlı promptlar (AI Talk)", "items": prompts}]
    out["actions"] = [{"label": "AI Talk", "href": "/ai"}, {"label": "Boards", "href": "/boards"}]
    return out


_HANDLERS = {
    "trend-radar": karma_trend_radar,
    "query-haber": karma_query_haber,
    "rakip-gap": karma_rakip_gap,
    "seasonality": karma_seasonality,
    "anomaly-tree": karma_anomaly_tree,
    "brief-generator": karma_brief_generator,
    "headline-lab": karma_headline_lab,
    "ic-link": karma_ic_link,
    "content-decay": karma_content_decay,
    "topic-cluster": karma_topic_cluster,
    "push-roi": karma_push_roi,
    "serp-tracker": karma_serp_tracker,
    "programmatic-seo": karma_programmatic_seo,
    "international": karma_international,
    "editorial-sla": karma_editorial_sla,
    "war-room": karma_war_room,
    "post-mortem": karma_post_mortem,
    "morning-brief": karma_morning_brief,
    "cwv-trafik": karma_cwv_trafik,
    "ai-action": karma_ai_action,
}


def get_karma_data(db: Session, slug: str, site_id: int) -> dict[str, Any]:
    handler = _HANDLERS.get(slug)
    if not handler:
        raise ValueError("Unknown karma module")
    return handler(db, site_id)
