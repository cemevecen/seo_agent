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


def karma_push_roi(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("push-roi", site)
    out["summary"] = "Firebase push + GA4 event korelasyonu — notification modülü ile genişletilebilir."
    out["sections"] = [
        {
            "title": "Push konuları (placeholder)",
            "items": [
                {"title": "Faiz kararı canlı", "subtitle": "Tahmini yüksek geri dönüş", "badge": "ROI A"},
                {"title": "Kur alarmı", "subtitle": "Orta geri dönüş", "badge": "ROI B"},
            ],
        }
    ]
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
    site = _site_or_404(db, site_id)
    out = _base_payload("programmatic-seo", site)
    out["summary"] = "Şablon / otomatik sayfa kalite guardrail — SEO audit ile entegre."
    out["sections"] = [
        {
            "title": "Kontrol listesi",
            "items": [
                {"title": "Thin content", "subtitle": "Kelime sayısı < eşik", "badge": "risk"},
                {"title": "Duplicate title", "subtitle": "Aynı title hash", "badge": "risk"},
                {"title": "Crawl budget", "subtitle": "Faceted URL patlaması", "badge": "izle"},
            ],
        }
    ]
    out["actions"] = [{"label": "SEO Audit", "href": "/seo-audit"}]
    return out


def karma_international(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("international", site)
    out["summary"] = "TR/EN içerik fırsatı — GSC country segment (genişletilebilir)."
    out["sections"] = [
        {
            "title": "Lokalizasyon adayları",
            "items": [
                {"title": "Global finans terimleri", "subtitle": "EN arama hacmi yüksek", "badge": "EN"},
                {"title": "Film / oyuncu isimleri", "subtitle": "sinemalar EN", "badge": "EN"},
            ],
        }
    ]
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


def karma_cwv_trafik(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("cwv-trafik", site)
    domain = site.domain or ""
    out["summary"] = f"Lighthouse skorları vs trafik — {domain} data explorer."
    out["sections"] = [
        {
            "title": "CWV şablonları",
            "items": [
                {"title": "Ana sayfa", "subtitle": "LCP / INP / CLS", "badge": "speed"},
                {"title": "Haber detay", "subtitle": "Template LCP", "badge": "speed"},
            ],
        }
    ]
    out["actions"] = [{"label": "Speed / Explorer", "href": f"/data-explorer/{domain}"}]
    return out


def karma_ai_action(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("ai-action", site)
    out["summary"] = "AI Talk promptları → GitLab issue / brief / KPI takip köprüsü."
    out["sections"] = [
        {
            "title": "Hızlı promptlar",
            "items": [
                {"title": "Bu gap haber için brief yaz", "subtitle": "AI Talk", "badge": "prompt"},
                {"title": "Boards'a issue aç", "subtitle": "GitLab", "badge": "aksiyon", "href": "/boards"},
                {"title": "Realtime KPI izle", "subtitle": "Yayın sonrası", "badge": "takip", "href": "/realtime"},
            ],
        }
    ]
    out["actions"] = [{"label": "AI Talk", "href": "/ai"}]
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
