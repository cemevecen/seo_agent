from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from backend.karma.config import REFRESH_SEC, TREND_BY_SLUG, trend_competitors_for_domain
from backend.karma.vertical import (
    VERTICAL_LABELS,
    brief_deadline_label,
    brief_internal_links_hint,
    headline_variants,
    vertical_for_site,
    ContentVertical,
)
from backend.karma.realtime_helpers import (
    age_minutes,
    alarm_spike_patterns,
    alarms_recent,
    drivers_for_profiles,
    editorial_calendar_events,
    fmt_driver,
    fmt_local_time,
    gsc_rising_and_decay,
    intel_recent,
    match_query_intel,
    score_intel_row,
    site_pulse,
    tokenize,
    top_pages_rt,
    utcnow,
)
from backend.services.timezone_utils import now_local
from backend.models import NewsIntelligenceItem, RealtimeAlarmLog, Site


def _site_or_404(db: Session, site_id: int) -> Site:
    from backend.main import _is_external_site

    site = db.query(Site).filter(Site.id == site_id, Site.is_active.is_(True)).first()
    if not site or _is_external_site(db, site.id):
        raise ValueError("Site not found")
    return site


def _base_payload(slug: str, site: Site) -> dict[str, Any]:
    item = TREND_BY_SLUG[slug]
    domain = site.domain or ""
    vertical = vertical_for_site(site)
    now = utcnow()
    return {
        "slug": slug,
        "title": item.title,
        "description": item.description,
        "group": item.group,
        "site": {"id": site.id, "domain": domain, "display_name": site.display_name or domain},
        "vertical": vertical.value if vertical else None,
        "vertical_label": VERTICAL_LABELS.get(vertical, "") if vertical else "",
        "competitors": trend_competitors_for_domain(domain),
        "summary": "",
        "metrics": [],
        "sections": [],
        "actions": [],
        "live_at": now_local().isoformat(),
        "refresh_sec": REFRESH_SEC,
    }


def _alarm_item(a: RealtimeAlarmLog) -> dict[str, Any]:
    return {
        "title": (a.message or a.rule_id or "Alarm")[:120],
        "subtitle": f"{a.metric or ''} · {fmt_local_time(a.triggered_at, '%d.%m %H:%M')}",
        "badge": a.severity or "alarm",
        "href": "/realtime",
    }


def trend_trend_radar(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("trend-radar", site)
    vertical = vertical_for_site(site)
    now = utcnow()
    pulse = site_pulse(db, site_id)
    drivers = drivers_for_profiles(db, site_id)

    intel_30m = intel_recent(db, minutes=30, limit=80, site=site)
    intel_6h = intel_recent(db, hours=6, limit=250, site=site)
    topic_counter: Counter[str] = Counter()
    for row in intel_6h:
        t = (row.topic or row.category or "").strip()
        if t:
            topic_counter[t] += 1

    scored = [(score_intel_row(r, topic_counter, now=now, vertical=vertical), r) for r in intel_6h]
    scored.sort(key=lambda x: x[0], reverse=True)
    alarms = alarms_recent(db, site_id, hours=3, limit=25)

    critical = []
    for score, row in scored:
        age_m = age_minutes(row.published_at, now=now)
        if age_m > 90 and row.is_in_our_site:
            continue
        if not row.is_in_our_site or age_m <= 45:
            critical.append(
                {
                    "title": row.headline,
                    "subtitle": f"{age_m:.0f} dk · {row.source_name} · skor {score:.1f}",
                    "badge": "KRİTİK" if age_m <= 30 and not row.is_in_our_site else "gap" if not row.is_in_our_site else "trend",
                    "href": row.url,
                }
            )
        if len(critical) >= 12:
            break

    trend_items = [
        {
            "title": row.headline,
            "subtitle": f"{row.source_name} · {age_minutes(row.published_at, now=now):.0f} dk önce",
            "badge": f"skor {score:.1f}",
            "href": row.url,
        }
        for score, row in scored[:20]
    ]

    driver_items = []
    for prof, data in drivers.items():
        for d in (data.get("drivers_increase") or [])[:5]:
            driver_items.append(
                {"title": f"[{prof}] ↑ {fmt_driver(d)}", "subtitle": f"kaynak: {data.get('driver_source', 'live')}", "badge": "trafik+"}
            )
        for d in (data.get("drivers_decrease") or [])[:3]:
            driver_items.append(
                {"title": f"[{prof}] ↓ {fmt_driver(d)}", "subtitle": "düşüş driver", "badge": "trafik-"}
            )

    web = pulse.get("web") or {}
    mweb = pulse.get("mweb") or {}
    gaps_30m = sum(1 for r in intel_30m if not r.is_in_our_site)

    out["summary"] = (
        f"{out.get('vertical_label') or 'Trend'} · "
        f"Anlık {pulse.get('total_current', 0):.0f} aktif kullanıcı (Δ {pulse.get('total_delta', 0):+.0f}). "
        f"Son 30 dk: {len(intel_30m)} haber, {gaps_30m} gap. {len(alarms)} alarm (3s)."
    )
    out["metrics"] = [
        {"label": "Aktif (web)", "value": f"{web.get('current', 0):.0f}"},
        {"label": "Δ web", "value": f"{web.get('delta', 0):+.0f}"},
        {"label": "Gap 30dk", "value": str(gaps_30m)},
        {"label": "Alarm 3s", "value": str(len(alarms))},
    ]
    out["sections"] = [
        {"title": "🔴 Kritik — son 90 dk", "items": critical or [{"title": "Kritik gap yok", "subtitle": "Feed güncel", "badge": "ok"}]},
        {"title": "Trend fusion skoru", "items": trend_items},
        {"title": "Trafik driver (web/mweb)", "items": driver_items or [{"title": "Driver verisi yok", "subtitle": "Realtime kontrol edin", "badge": "—"}]},
        {"title": "Son alarmlar", "items": [_alarm_item(a) for a in alarms[:8]]},
        {
            "title": "Topic patlaması (6s)",
            "items": [{"title": t, "subtitle": f"{c} sinyal", "badge": "hot" if c >= 4 else "topic"} for t, c in topic_counter.most_common(10)],
        },
    ]
    out["actions"] = [{"label": "Realtime", "href": "/realtime"}, {"label": "News", "href": "/intelligence"}]
    return out


def trend_query_haber(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("query-haber", site)
    queries = get_top_queries(db, site, limit=60, device="all")
    intel = intel_recent(db, hours=6, limit=300, site=site)
    rising, _ = gsc_rising_and_decay(queries)

    items_gap = []
    items_ok = []
    for q in rising[:40]:
        query = str(q.get("query") or "")
        if not query:
            continue
        hit, in_site, best = match_query_intel(query, intel)
        delta = float(q.get("delta") or 0)
        imp = int(q.get("impressions") or 0)
        pos = float(q.get("position") or 0)
        age_str = ""
        if best and best.published_at:
            age_str = f" · haber {age_minutes(best.published_at):.0f} dk önce"
        row = {
            "title": query,
            "subtitle": f"↑ Δpos {delta:.1f} · poz {pos:.1f} · {imp} imp{age_str}",
            "badge": "KRİTİK gap" if not hit else ("bizde var" if in_site else "haber var"),
            "meta": {"delta": delta, "clicks": q.get("clicks")},
        }
        if not hit or not in_site:
            items_gap.append(row)
        else:
            items_ok.append(row)

    items_gap.sort(key=lambda x: -float(x["meta"].get("delta") or 0))
    rt_pages = top_pages_rt(db, site_id, "web", 8)
    page_items = [
        {"title": p["path"], "subtitle": f"{p['users']} aktif · canlı sayfa", "badge": "RT"}
        for p in rt_pages[:8]
    ]

    out["summary"] = f"{out.get('vertical_label') or ''} · {len(items_gap)} yükselen sorguda haber gap; {len(items_ok)} kapsanan."
    out["metrics"] = [
        {"label": "Yükselen", "value": str(len(rising))},
        {"label": "Gap", "value": str(len(items_gap))},
        {"label": "Kapsanan", "value": str(len(items_ok))},
    ]
    out["sections"] = [
        {"title": "Yükselen sorgu — haber gap (öncelik)", "items": items_gap[:25]},
        {"title": "Kapsanan yükselen sorgular", "items": items_ok[:12]},
        {"title": "Anlık top sayfalar (içerik fırsatı)", "items": page_items},
    ]
    out["actions"] = [{"label": "Search Console", "href": "/search-console"}, {"label": "News", "href": "/intelligence"}]
    return out


def trend_rakip_gap(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("rakip-gap", site)
    competitors = out["competitors"]
    rows_6h = intel_recent(db, hours=6, limit=300, site=site)
    rows_24h = intel_recent(db, hours=24, limit=400, site=site)
    now = utcnow()

    comp_items: dict[str, list[NewsIntelligenceItem]] = {c: [] for c in competitors}
    comp_velocity: Counter[str] = Counter()
    for row in rows_6h:
        src = (row.source_url or row.source_name or "").lower()
        for comp in competitors:
            key = comp.replace("www.", "")
            if key in src or key in (row.source_name or "").lower():
                comp_items[comp].append(row)
                comp_velocity[comp] += 1

    sections = []
    for comp in competitors:
        comp_rows = sorted(comp_items.get(comp) or [], key=lambda r: r.published_at or now, reverse=True)
        sections.append(
            {
                "title": f"{comp} · {comp_velocity.get(comp, 0)} haber (6s)",
                "items": [
                    {
                        "title": r.headline,
                        "subtitle": f"{age_minutes(r.published_at, now=now):.0f} dk · {r.source_name}",
                        "badge": "BİZDE YOK" if not r.is_in_our_site else "bizde",
                        "href": r.url,
                    }
                    for r in comp_rows[:10]
                ]
                or [{"title": "Son 6 saatte eşleşme yok", "subtitle": "Feed genişletiliyor", "badge": "—"}],
            }
        )

    our_tokens = Counter(tokenize(" ".join(r.headline for r in rows_24h if r.is_in_our_site)))
    missing = []
    for row in rows_6h:
        if row.is_in_our_site:
            continue
        for tok in tokenize(row.headline)[:6]:
            if our_tokens.get(tok, 0) == 0:
                missing.append(
                    {
                        "title": tok,
                        "subtitle": row.headline[:90],
                        "badge": f"{age_minutes(row.published_at, now=now):.0f} dk",
                        "href": row.url,
                    }
                )
                break
        if len(missing) >= 15:
            break

    gap_count = sum(1 for r in rows_6h if not r.is_in_our_site)
    label = (out.get("vertical_label") or "").strip()
    out["summary"] = label
    out["metrics"] = [
        {"label": "6s sinyal", "value": str(len(rows_6h))},
        {"label": "Gap", "value": str(gap_count)},
        {"label": "Rakip", "value": str(len(competitors))},
    ]
    out["sections"] = sections + [{"title": "Öne çıkan konular (6s)", "items": missing}]
    return out


def trend_seasonality(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("seasonality", site)
    domain = site.domain or ""
    vertical = vertical_for_site(site)
    now = utcnow()
    patterns = alarm_spike_patterns(db, site_id, days=30)
    events = editorial_calendar_events(domain)

    cal_items = [{"title": p, "subtitle": trig, "badge": act} for p, trig, act in events]
    hour_items = [
        {"title": f"Saat {h:02d}:00", "subtitle": f"{c} alarm (30g)", "badge": "spike saati"}
        for h, c in patterns.get("top_hours") or []
    ]
    day_items = [
        {"title": day, "subtitle": f"{c} alarm (30g)", "badge": "yoğun gün"}
        for day, c in patterns.get("top_days") or []
    ]

    upcoming = []
    month = now.month
    month_names = ["", "Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağu", "Eylül", "Ekim", "Kasım", "Aralık"]
    cur_month = month_names[month]
    for period, trig, act in events:
        if cur_month in period or period.startswith("Her") or "Ay" in period:
            upcoming.append({"title": period, "subtitle": trig, "badge": act})

    recent_alarms = alarms_recent(db, site_id, hours=48, limit=10)
    sections: list[dict[str, Any]] = []

    vakif_event_count = 0
    if vertical == ContentVertical.FINANCE:
        from backend.services.vakif_economic_calendar import fetch_vakif_economic_calendar

        vakif = fetch_vakif_economic_calendar()
        weekly = vakif.get("weekly") or {}
        week_items = weekly.get("items") or []
        vakif_event_count = len(week_items)
        week_label = weekly.get("week_range") or weekly.get("published_label") or "Bu hafta"
        week_title = f"Ekonomik takvim — {week_label}"
        if weekly.get("published_label"):
            week_title += f" · yayımlanma {weekly['published_label']}"

        sections.append(
            {
                "title": week_title,
                "items": week_items
                or [
                    {
                        "title": "Takvim verisi alınamadı",
                        "subtitle": vakif.get("error") or "Vakıf Yatırım kaynağı geçici olarak yanıt vermiyor",
                        "badge": "—",
                        "href": vakif.get("source_url"),
                    }
                ],
            }
        )

        bulletin_items = []
        for b in vakif.get("bulletins") or []:
            subtitle_parts = [p for p in (b.get("date_label"), b.get("excerpt")) if p]
            bulletin_items.append(
                {
                    "title": b.get("label") or b.get("title") or "Bülten",
                    "subtitle": " · ".join(subtitle_parts)[:220] if subtitle_parts else "Güncel strateji notu",
                    "badge": "bülten",
                    "href": b.get("pdf_url") or b.get("detail_url") or b.get("page_url"),
                }
            )
        if bulletin_items:
            sections.append({"title": "Strateji bültenleri (Vakıf Yatırım)", "items": bulletin_items})

    sections.extend(
        [
            {"title": "Yakın editoryal hazırlık", "items": upcoming or cal_items[:3]},
            {"title": "Spike saatleri (alarm geçmişi)", "items": hour_items or [{"title": "Veri birikiyor", "subtitle": "30g alarm yok", "badge": "—"}]},
            {"title": "Yoğun günler", "items": day_items},
            {"title": "Mevsimsel takvim", "items": cal_items},
            {"title": "Son 48s alarmlar (pattern doğrulama)", "items": [_alarm_item(a) for a in recent_alarms]},
        ]
    )

    summary_parts = []
    if out.get("vertical_label"):
        summary_parts.append(str(out["vertical_label"]))
    if vertical == ContentVertical.FINANCE and vakif_event_count:
        summary_parts.append(f"Vakıf takvim: {vakif_event_count} gündem maddesi")
    summary_parts.append(f"30g alarm pattern ({patterns.get('total', 0)} olay)")
    out["summary"] = " · ".join(summary_parts)
    out["metrics"] = [
        {"label": "Takvim", "value": str(vakif_event_count) if vertical == ContentVertical.FINANCE else "—"},
        {"label": "30g alarm", "value": str(patterns.get("total", 0))},
        {"label": "Ay", "value": cur_month},
    ]
    out["sections"] = sections
    actions = [{"label": "Alerts", "href": "/alerts"}, {"label": "Realtime", "href": "/realtime"}]
    if vertical == ContentVertical.FINANCE:
        actions.insert(0, {"label": "VakıfBank raporları", "href": "https://www.vakifbank.com.tr/tr/bireysel/yatirim/arastirmalar-ve-raporlar/piyasa-raporlari"})
    out["actions"] = actions
    return out


def trend_anomaly_tree(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("anomaly-tree", site)
    pulse = site_pulse(db, site_id)
    alarms = alarms_recent(db, site_id, hours=12, limit=20)
    drivers = drivers_for_profiles(db, site_id)

    tree_items: list[dict[str, Any]] = []
    tree_items.append(
        {
            "title": f"Site nabzı · {pulse.get('total_current', 0):.0f} aktif (Δ {pulse.get('total_delta', 0):+.0f})",
            "subtitle": "web + mweb toplam",
            "badge": "kök",
        }
    )

    for a in alarms:
        prof = (a.metric or "web:").split(":")[0] or "web"
        tree_items.append(
            {
                "title": f"⚡ {(a.message or a.rule_id or 'Alarm')[:100]}",
                "subtitle": f"{prof} · {fmt_local_time(a.triggered_at)}",
                "badge": a.severity or "alarm",
                "href": "/realtime",
            }
        )
        prof_drivers = drivers.get(prof) or {}
        for d in (prof_drivers.get("drivers_increase") or [])[:4]:
            tree_items.append({"title": "    ↳ ↑ " + fmt_driver(d), "subtitle": prof, "badge": "driver+"})
        for d in (prof_drivers.get("drivers_decrease") or [])[:3]:
            tree_items.append({"title": "    ↳ ↓ " + fmt_driver(d), "subtitle": prof, "badge": "driver-"})

    if len(tree_items) <= 1:
        for prof in ("web", "mweb"):
            pd = drivers.get(prof) or {}
            for d in (pd.get("drivers_increase") or [])[:5]:
                tree_items.append({"title": f"[{prof}] ↑ {fmt_driver(d)}", "subtitle": "canlı driver", "badge": "live"})

    out["summary"] = f"{len(alarms)} alarm (12s) + web/mweb driver ağacı. Anlık Δ {pulse.get('total_delta', 0):+.0f}."
    out["metrics"] = [
        {"label": "Alarm", "value": str(len(alarms))},
        {"label": "Aktif", "value": f"{pulse.get('total_current', 0):.0f}"},
        {"label": "Δ", "value": f"{pulse.get('total_delta', 0):+.0f}"},
    ]
    out["sections"] = [{"title": "Alarm → driver ağacı (live)", "items": tree_items[:50]}]
    out["actions"] = [{"label": "Realtime", "href": "/realtime"}]
    return out


def trend_brief_generator(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("brief-generator", site)
    vertical = vertical_for_site(site)
    now = utcnow()
    intel = intel_recent(db, hours=4, limit=150, site=site)
    topic_counter: Counter[str] = Counter()
    for r in intel:
        t = (r.topic or r.category or "").strip()
        if t:
            topic_counter[t] += 1

    gaps = [r for r in intel if not r.is_in_our_site]
    gaps_scored = sorted(gaps, key=lambda r: score_intel_row(r, topic_counter, now=now, vertical=vertical), reverse=True)

    queries = get_top_queries(db, site, limit=30, device="all")
    rising, _ = gsc_rising_and_decay(queries)
    query_map = {str(q.get("query") or "").lower(): q for q in rising[:15]}

    items = []
    for r in gaps_scored[:12]:
        age_m = age_minutes(r.published_at, now=now)
        kws = tokenize(r.headline)[:8]
        gsc_hint = ""
        for kw in kws:
            if kw in " ".join(query_map.keys()):
                gsc_hint = f"GSC yükselen: {kw}"
                break
        urgency = "ACİL" if age_m <= 30 else "yüksek" if age_m <= 90 else "normal"
        brief = {
            "h1": r.headline[:95],
            "keywords": ", ".join(kws[:6]),
            "angle": r.topic or r.category or ("Vizyon" if vertical else "Genel"),
            "urgency": urgency,
            "deadline": brief_deadline_label(urgency, age_m, vertical),
            "internal_links": brief_internal_links_hint(vertical),
            "gsc": gsc_hint,
        }
        items.append(
            {
                "title": r.headline,
                "subtitle": f"{urgency} · {age_m:.0f} dk · H1: {brief['h1'][:60]}…",
                "badge": brief["angle"],
                "href": r.url,
                "meta": brief,
            }
        )

    out["summary"] = f"{out.get('vertical_label') or ''} · {len(items)} acil brief — gap + GSC fırsat (4s)."
    out["metrics"] = [
        {"label": "Brief", "value": str(len(items))},
        {"label": "Acil", "value": str(sum(1 for i in items if i.get("meta", {}).get("urgency") == "ACİL"))},
    ]
    out["sections"] = [{"title": "Editoryal brief taslakları", "items": items}]
    out["actions"] = [{"label": "AI Talk", "href": "/ai"}, {"label": "News", "href": "/intelligence"}]
    return out


def trend_headline_lab(db: Session, site_id: int) -> dict[str, Any]:
    site = _site_or_404(db, site_id)
    out = _base_payload("headline-lab", site)
    vertical = vertical_for_site(site)
    now = utcnow()
    rows = intel_recent(db, hours=3, limit=40, site=site)
    topic_counter: Counter[str] = Counter()
    for r in rows:
        t = (r.topic or r.category or "").strip()
        if t:
            topic_counter[t] += 1
    top_rows = sorted(rows, key=lambda r: score_intel_row(r, topic_counter, now=now, vertical=vertical), reverse=True)[:10]

    items = []
    for r in top_rows:
        base = (r.headline or "").strip()
        if not base:
            continue
        age_m = age_minutes(r.published_at, now=now)
        variants = headline_variants(base, vertical, age_m=age_m)
        for i, v in enumerate(variants[:5]):
            score = max(35, 98 - i * 10 - abs(len(v) - 62) // 2 - (10 if "SON DAKİKA" in v and i > 0 else 0))
            items.append(
                {
                    "title": v[:120],
                    "subtitle": f"Kaynak: {r.source_name} · varyant {i + 1}",
                    "badge": f"skor {score}",
                    "href": r.url if i == 0 else None,
                }
            )

    out["summary"] = f"{out.get('vertical_label') or ''} · Son 3 saatin top {len(top_rows)} haberinden {len(items)} başlık varyantı."
    out["sections"] = [{"title": "Headline varyantları (CTR heuristic)", "items": items}]
    return out


def trend_ic_link(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("ic-link", site)
    web_pages = top_pages_rt(db, site_id, "web", 20)
    mweb_pages = top_pages_rt(db, site_id, "mweb", 12)
    queries = get_top_queries(db, site, limit=25, device="all")
    rising, _ = gsc_rising_and_decay(queries)

    sources = []
    for i, p in enumerate(web_pages[:8]):
        sources.append(
            {
                "title": p["path"],
                "subtitle": f"{p['users']} aktif · link KAYNAĞI",
                "badge": f"#{i + 1}",
            }
        )
    for i, p in enumerate(mweb_pages[:4]):
        sources.append(
            {
                "title": p["path"],
                "subtitle": f"{p['users']} aktif mweb · kaynak",
                "badge": "mweb",
            }
        )

    targets = []
    for q in rising[:10]:
        qstr = str(q.get("query") or "")
        targets.append(
            {
                "title": f"Hedef içerik: «{qstr}»",
                "subtitle": f"↑ Δpos {float(q.get('delta', 0)):.1f} · yükselen sorgu",
                "badge": "hedef",
            }
        )
    gaps = [r for r in intel_recent(db, hours=2, limit=30, site=site) if not r.is_in_our_site]
    for r in gaps[:5]:
        targets.append(
            {
                "title": (r.headline or "")[:90],
                "subtitle": "Gap haber → yeni URL hedefi",
                "badge": "gap",
                "href": r.url,
            }
        )

    pairs = []
    for si, src in enumerate(sources[:5]):
        if si < len(targets):
            tgt = targets[si]
            pairs.append(
                {
                    "title": f"{src['title'][:50]} → {tgt['title'][:50]}",
                    "subtitle": f"{src['subtitle']} → {tgt['subtitle'][:60]}",
                    "badge": "öneri",
                }
            )

    out["summary"] = f"{len(sources)} kaynak sayfa (RT) + {len(targets)} hedef (GSC/gap). {len(pairs)} eşleme önerisi."
    out["metrics"] = [
        {"label": "Kaynak", "value": str(len(sources))},
        {"label": "Hedef", "value": str(len(targets))},
        {"label": "Eşleme", "value": str(len(pairs))},
    ]
    out["sections"] = [
        {"title": "Link kaynakları (anlık trafik)", "items": sources},
        {"title": "Link hedefleri (GSC + gap)", "items": targets},
        {"title": "Kaynak → hedef önerileri", "items": pairs},
    ]
    out["actions"] = [{"label": "Realtime", "href": "/realtime"}, {"label": "Backlinks", "href": "/backlinks"}]
    return out


def trend_content_decay(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("content-decay", site)
    queries = get_top_queries(db, site, limit=80, device="all")
    _, decay = gsc_rising_and_decay(queries)
    drivers = drivers_for_profiles(db, site_id)
    declining_pages = set()
    for prof_data in drivers.values():
        for d in (prof_data.get("drivers_decrease") or [])[:15]:
            declining_pages.add(str(d.get("page") or d.get("path") or ""))

    items = []
    for q in decay[:25]:
        query = str(q.get("query") or "")
        delta = float(q.get("delta") or 0)
        imp = int(q.get("impressions") or 0)
        rt_hit = any(query.lower() in p.lower() for p in declining_pages if p)
        severity = "KRİTİK" if delta < -3 and imp > 500 else "yüksek" if delta < -2 else "izle"
        if rt_hit:
            severity = "KRİTİK+RT"
        items.append(
            {
                "title": query,
                "subtitle": f"Δpos {delta:.1f} · {imp} imp · {'RT düşüş var' if rt_hit else 'GSC only'}",
                "badge": severity,
                "meta": {"action": "refresh" if delta > -4 else "merge|301"},
            }
        )

    rt_decay_items = [
        {"title": p, "subtitle": "Anlık trafik düşüş driver", "badge": "RT decay"}
        for p in list(declining_pages)[:10]
        if p
    ]

    out["summary"] = f"{len(decay)} sorguda pozisyon kaybı; {len(rt_decay_items)} sayfa RT düşüşte. Birleşik decay skoru."
    out["metrics"] = [
        {"label": "GSC decay", "value": str(len(decay))},
        {"label": "RT düşüş", "value": str(len(rt_decay_items))},
        {"label": "Kritik", "value": str(sum(1 for i in items if "KRİTİK" in str(i.get("badge"))))},
    ]
    out["sections"] = [
        {"title": "Pozisyon düşen sorgular (GSC + RT)", "items": items},
        {"title": "Anlık trafik düşen sayfalar", "items": rt_decay_items},
    ]
    out["actions"] = [{"label": "Alerts", "href": "/alerts"}, {"label": "Search Console", "href": "/search-console"}]
    return out


def trend_topic_cluster(db: Session, site_id: int) -> dict[str, Any]:
    from backend.collectors.search_console import get_top_queries

    site = _site_or_404(db, site_id)
    out = _base_payload("topic-cluster", site)
    rows = intel_recent(db, hours=12, limit=400, site=site)
    queries = get_top_queries(db, site, limit=50, device="all")
    pages = top_pages_rt(db, site_id, "web", 30)

    intel_clusters: dict[str, int] = defaultdict(int)
    gap_clusters: dict[str, int] = defaultdict(int)
    for r in rows:
        key = r.topic or r.category or "Genel"
        intel_clusters[key] += 1
        if not r.is_in_our_site:
            gap_clusters[key] += 1

    gsc_clusters: dict[str, float] = defaultdict(float)
    for q in queries:
        tokens = tokenize(str(q.get("query") or ""))[:2]
        key = tokens[0] if tokens else "diğer"
        gsc_clusters[key] += float(q.get("impressions") or 0)

    path_clusters: Counter[str] = Counter()
    for p in pages:
        path = p.get("path") or "/"
        seg = path.strip("/").split("/")[0] if path != "/" else "home"
        path_clusters[seg] += p.get("users") or 0

    intel_items = [
        {
            "title": k,
            "subtitle": f"{v} haber · gap {gap_clusters.get(k, 0)}",
            "badge": "güçlü" if v >= 6 else "zayıf" if v >= 2 else "boşluk",
        }
        for k, v in sorted(intel_clusters.items(), key=lambda x: -x[1])[:15]
    ]
    gsc_items = [
        {"title": k, "subtitle": f"{int(v)} toplam imp", "badge": "GSC"}
        for k, v in sorted(gsc_clusters.items(), key=lambda x: -x[1])[:12]
        if k != "diğer"
    ]
    rt_items = [
        {"title": f"/{k}" if k != "home" else "/", "subtitle": f"{v} aktif kullanıcı", "badge": "RT"}
        for k, v in path_clusters.most_common(12)
    ]

    weak = [i for i in intel_items if i["badge"] == "boşluk" or "gap" in i["subtitle"] and int(i["subtitle"].split("gap ")[-1]) > 2]

    out["summary"] = f"{out.get('vertical_label') or ''} · 12s cluster: {len(intel_clusters)} topic, {len(gsc_clusters)} GSC token."
    out["metrics"] = [
        {"label": "Topic", "value": str(len(intel_clusters))},
        {"label": "Zayıf/gap", "value": str(len(weak))},
        {"label": "RT segment", "value": str(len(path_clusters))},
    ]
    out["sections"] = [
        {"title": "Haber topic cluster", "items": intel_items},
        {"title": "GSC query cluster (imp)", "items": gsc_items},
        {"title": "Realtime path cluster", "items": rt_items},
        {"title": "Otorite boşlukları", "items": weak[:10] or [{"title": "Belirgin boşluk yok", "subtitle": "Cluster dengeli", "badge": "ok"}]},
    ]
    return out


_HANDLERS = {
    "trend-radar": trend_trend_radar,
    "query-haber": trend_query_haber,
    "rakip-gap": trend_rakip_gap,
    "seasonality": trend_seasonality,
    "anomaly-tree": trend_anomaly_tree,
    "brief-generator": trend_brief_generator,
    "headline-lab": trend_headline_lab,
    "ic-link": trend_ic_link,
    "content-decay": trend_content_decay,
    "topic-cluster": trend_topic_cluster,
}


def get_trend_data(db: Session, slug: str, site_id: int) -> dict[str, Any]:
    handler = _HANDLERS.get(slug)
    if not handler:
        raise ValueError("Unknown trend module")
    return handler(db, site_id)


get_karma_data = get_trend_data
