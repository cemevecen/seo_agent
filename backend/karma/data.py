from __future__ import annotations

from datetime import date, timedelta
from collections import Counter
from datetime import timedelta
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from backend.karma.config import REFRESH_SEC, TREND_BY_SLUG
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
        "summary": "",
        "metrics": [],
        "sections": [],
        "actions": [],
        "live_at": now_local().isoformat(),
        "refresh_sec": REFRESH_SEC,
    }


def _alarm_item(a: RealtimeAlarmLog) -> dict[str, Any]:
    return {
        "title": (a.message or a.rule_id or "Alert")[:120],
        "subtitle": f"{a.metric or ''} · {fmt_local_time(a.triggered_at, '%d.%m %H:%M')}",
        "badge": a.severity or "alarm",
        "href": "/realtime",
    }


def _parse_iso_date(value: str | None) -> date | None:
    raw = (value or "")[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _entertainment_vizyon_sections(*, horizon_days: int = 21, limit: int = 10) -> list[dict[str, Any]]:
    """Sinemalar trend-radar: TMDB vizyon + platform takvimi."""
    try:
        from backend.services.tmdb import get_combined_upcoming

        data = get_combined_upcoming(months_ahead=2) or {}
    except Exception:
        return []

    today = date.today()
    horizon = today + timedelta(days=horizon_days)

    def in_window(item: dict) -> bool:
        rd = _parse_iso_date(item.get("release_date"))
        return rd is not None and today <= rd <= horizon

    theatrical = [m for m in (data.get("theatrical") or []) if in_window(m)]
    theatrical.sort(key=lambda m: m.get("release_date") or "9999")

    streaming = [m for m in (data.get("streaming") or []) if in_window(m)]
    streaming.sort(key=lambda m: (m.get("release_date") or "9999", -(m.get("popularity") or 0)))

    tv = [m for m in (data.get("tv_series") or []) if in_window(m)]
    tv.sort(key=lambda m: (m.get("release_date") or "9999", -(m.get("popularity") or 0)))

    sections: list[dict[str, Any]] = []

    if theatrical:
        sections.append(
            {
                "title": "🎬 Theatrical calendar",
                "items": [
                    {
                        "title": m.get("title") or "Film",
                        "subtitle": f"{(m.get('release_date') or '')[:10]} · pop {m.get('popularity', 0)}",
                        "badge": "theatrical",
                        "href": m.get("tmdb_url") or "",
                    }
                    for m in theatrical[:limit]
                ],
            }
        )

    if streaming:
        sections.append(
            {
                "title": "📺 Platform releases (film & TV)",
                "items": [
                    {
                        "title": m.get("title") or "Title",
                        "subtitle": (
                            f"{(m.get('release_date') or '')[:10]}"
                            f" · {', '.join((m.get('providers') or [])[:3]) or 'platform'}"
                        ),
                        "badge": (m.get("media_type") or "movie")[:6],
                        "href": m.get("tmdb_url") or "",
                    }
                    for m in streaming[:limit]
                ],
            }
        )

    if tv:
        sections.append(
            {
                "title": "📡 TV calendar",
                "items": [
                    {
                        "title": m.get("title") or "Series",
                        "subtitle": f"{(m.get('release_date') or '')[:10]} · pop {m.get('popularity', 0)}",
                        "badge": "tv",
                        "href": m.get("tmdb_url") or "",
                    }
                    for m in tv[:limit]
                ],
            }
        )

    return sections


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
                    "subtitle": f"{age_m:.0f} min · {row.source_name} · score {score:.1f}",
                    "badge": "CRITICAL" if age_m <= 30 and not row.is_in_our_site else "gap" if not row.is_in_our_site else "trend",
                    "href": row.url,
                }
            )
        if len(critical) >= 12:
            break

    trend_items = [
        {
            "title": row.headline,
            "subtitle": f"{row.source_name} · {age_minutes(row.published_at, now=now):.0f} min ago",
            "badge": f"score {score:.1f}",
            "href": row.url,
        }
        for score, row in scored[:20]
    ]

    driver_items = []
    for prof, data in drivers.items():
        for d in (data.get("drivers_increase") or [])[:5]:
            driver_items.append(
                {"title": f"[{prof}] ↑ {fmt_driver(d)}", "subtitle": f"source: {data.get('driver_source', 'live')}", "badge": "trafik+"}
            )
        for d in (data.get("drivers_decrease") or [])[:3]:
            driver_items.append(
                {"title": f"[{prof}] ↓ {fmt_driver(d)}", "subtitle": "drop driver", "badge": "trafik-"}
            )

    web = pulse.get("web") or {}
    mweb = pulse.get("mweb") or {}
    gaps_30m = sum(1 for r in intel_30m if not r.is_in_our_site)

    out["summary"] = (
        f"{out.get('vertical_label') or 'Trend'} · "
        f"Live {pulse.get('total_current', 0):.0f} active users (Δ {pulse.get('total_delta', 0):+.0f}). "
        f"Last 30 min: {len(intel_30m)} news, {gaps_30m} gaps. {len(alarms)} alerts (3h)."
    )
    out["metrics"] = [
        {"label": "Active (web)", "value": f"{web.get('current', 0):.0f}"},
        {"label": "Δ web", "value": f"{web.get('delta', 0):+.0f}"},
        {"label": "Gap 30m", "value": str(gaps_30m)},
        {"label": "Alerts 3h", "value": str(len(alarms))},
    ]
    out["sections"] = [
        {"title": "🔴 Critical — last 90 min", "items": critical or [{"title": "No critical gap", "subtitle": "Feed is current", "badge": "ok"}]},
        {"title": "Trend fusion score", "items": trend_items},
        {"title": "Traffic drivers (web/mweb)", "items": driver_items or [{"title": "No driver data", "subtitle": "Check Realtime", "badge": "—"}]},
        {"title": "Recent alerts", "items": [_alarm_item(a) for a in alarms[:8]]},
        {
            "title": "Topic spike (6h)",
            "items": [{"title": t, "subtitle": f"{c} signals", "badge": "hot" if c >= 4 else "topic"} for t, c in topic_counter.most_common(10)],
        },
    ]
    if vertical == ContentVertical.ENTERTAINMENT:
        out["sections"].extend(_entertainment_vizyon_sections())
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
            age_str = f" · news {age_minutes(best.published_at):.0f} min ago"
        row = {
            "title": query,
            "subtitle": f"↑ Δpos {delta:.1f} · pos {pos:.1f} · {imp} imp{age_str}",
            "badge": "CRITICAL gap" if not hit else ("on our site" if in_site else "news exists"),
            "meta": {"delta": delta, "clicks": q.get("clicks")},
        }
        if not hit or not in_site:
            items_gap.append(row)
        else:
            items_ok.append(row)

    items_gap.sort(key=lambda x: -float(x["meta"].get("delta") or 0))
    rt_pages = top_pages_rt(db, site_id, "web", 8)
    page_items = [
        {"title": p["path"], "subtitle": f"{p['users']} active · live page", "badge": "RT"}
        for p in rt_pages[:8]
    ]

    out["summary"] = f"{out.get('vertical_label') or ''} · {len(items_gap)} rising queries with news gap; {len(items_ok)} covered."
    out["metrics"] = [
        {"label": "Rising", "value": str(len(rising))},
        {"label": "Gap", "value": str(len(items_gap))},
        {"label": "Covered", "value": str(len(items_ok))},
    ]
    out["sections"] = [
        {"title": "Rising queries — news gap (priority)", "items": items_gap[:25]},
        {"title": "Covered rising queries", "items": items_ok[:12]},
        {"title": "Live top pages (content opportunity)", "items": page_items},
    ]
    out["actions"] = [{"label": "Search Console", "href": "/search-console"}, {"label": "News", "href": "/intelligence"}]
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
        {"title": f"{h:02d}:00", "subtitle": f"{c} alerts (30d)", "badge": "spike hour"}
        for h, c in patterns.get("top_hours") or []
    ]
    day_items = [
        {"title": day, "subtitle": f"{c} alerts (30d)", "badge": "busy day"}
        for day, c in patterns.get("top_days") or []
    ]

    upcoming = []
    month = now.month
    month_names_en = ["", "January", "February", "March", "April", "May", "June", "July", "Aug", "September", "October", "November", "December"]
    month_names_tr = ["", "Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağu", "Eylül", "Ekim", "Kasım", "Aralık"]
    cur_month = month_names_en[month]
    cur_month_tr = month_names_tr[month]
    for period, trig, act in events:
        if cur_month_tr in period or period.startswith("Her") or "Ay" in period:
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
        week_label = weekly.get("week_range") or weekly.get("published_label") or "This week"
        week_title = f"Economic calendar — {week_label}"
        if weekly.get("published_label"):
            week_title += f" · published {weekly['published_label']}"

        sections.append(
            {
                "title": week_title,
                "items": week_items
                or [
                    {
                        "title": "Could not load calendar",
                        "subtitle": vakif.get("error") or "Vakıf Yatırım source is temporarily unavailable",
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
                    "title": b.get("label") or b.get("title") or "Bulletin",
                    "subtitle": " · ".join(subtitle_parts)[:220] if subtitle_parts else "Latest strategy note",
                    "badge": "bulletin",
                    "href": b.get("pdf_url") or b.get("detail_url") or b.get("page_url"),
                }
            )
        if bulletin_items:
            sections.append({"title": "Strategy bulletins (Vakıf Yatırım)", "items": bulletin_items})

    sections.extend(
        [
            {"title": "Upcoming editorial prep", "items": upcoming or cal_items[:3]},
            {"title": "Spike hours (alert history)", "items": hour_items or [{"title": "Collecting data", "subtitle": "No alerts in 30d", "badge": "—"}]},
            {"title": "Busy days", "items": day_items},
            {"title": "Seasonal calendar", "items": cal_items},
            {"title": "Last 48h alerts (pattern check)", "items": [_alarm_item(a) for a in recent_alarms]},
        ]
    )

    summary_parts = []
    if out.get("vertical_label"):
        summary_parts.append(str(out["vertical_label"]))
    if vertical == ContentVertical.FINANCE and vakif_event_count:
        summary_parts.append(f"Vakıf calendar: {vakif_event_count} agenda items")
    summary_parts.append(f"30d alert pattern ({patterns.get('total', 0)} events)")
    out["summary"] = " · ".join(summary_parts)
    out["metrics"] = [
        {"label": "Calendar", "value": str(vakif_event_count) if vertical == ContentVertical.FINANCE else "—"},
        {"label": "30d alerts", "value": str(patterns.get("total", 0))},
        {"label": "Month", "value": cur_month},
    ]
    out["sections"] = sections
    actions = [{"label": "Alerts", "href": "/alerts"}, {"label": "Realtime", "href": "/realtime"}]
    if vertical == ContentVertical.FINANCE:
        actions.insert(0, {"label": "VakıfBank reports", "href": "https://www.vakifbank.com.tr/tr/bireysel/yatirim/arastirmalar-ve-raporlar/piyasa-raporlari"})
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
            "title": f"Site pulse · {pulse.get('total_current', 0):.0f} active (Δ {pulse.get('total_delta', 0):+.0f})",
            "subtitle": "web + mweb total",
            "badge": "root",
        }
    )

    for a in alarms:
        prof = (a.metric or "web:").split(":")[0] or "web"
        tree_items.append(
            {
                "title": f"⚡ {(a.message or a.rule_id or 'Alert')[:100]}",
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
                tree_items.append({"title": f"[{prof}] ↑ {fmt_driver(d)}", "subtitle": "live driver", "badge": "live"})

    out["summary"] = f"{len(alarms)} alerts (12h) + web/mweb driver tree. Live Δ {pulse.get('total_delta', 0):+.0f}."
    out["metrics"] = [
        {"label": "Alerts", "value": str(len(alarms))},
        {"label": "Active", "value": f"{pulse.get('total_current', 0):.0f}"},
        {"label": "Δ", "value": f"{pulse.get('total_delta', 0):+.0f}"},
    ]
    out["sections"] = [{"title": "Alert → driver tree (live)", "items": tree_items[:50]}]
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
                gsc_hint = f"GSC rising: {kw}"
                break
        urgency = "URGENT" if age_m <= 30 else "high" if age_m <= 90 else "normal"
        brief = {
            "h1": r.headline[:95],
            "keywords": ", ".join(kws[:6]),
            "angle": r.topic or r.category or ("Theatrical" if vertical else "General"),
            "urgency": urgency,
            "deadline": brief_deadline_label(urgency, age_m, vertical),
            "internal_links": brief_internal_links_hint(vertical),
            "gsc": gsc_hint,
        }
        items.append(
            {
                "title": r.headline,
                "subtitle": f"{urgency} · {age_m:.0f} min · H1: {brief['h1'][:60]}…",
                "badge": brief["angle"],
                "href": r.url,
                "meta": brief,
            }
        )

    out["summary"] = f"{out.get('vertical_label') or ''} · {len(items)} urgent briefs — gap + GSC opportunity (4h)."
    out["metrics"] = [
        {"label": "Brief", "value": str(len(items))},
        {"label": "Urgent", "value": str(sum(1 for i in items if i.get("meta", {}).get("urgency") == "URGENT"))},
    ]
    out["sections"] = [{"title": "Editorial brief drafts", "items": items}]
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
                    "subtitle": f"Source: {r.source_name} · variant {i + 1}",
                    "badge": f"score {score}",
                    "href": r.url if i == 0 else None,
                }
            )

    out["summary"] = f"{out.get('vertical_label') or ''} · Top {len(top_rows)} stories from last 3h → {len(items)} headline variants."
    out["sections"] = [{"title": "Headline variants (CTR heuristic)", "items": items}]
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
                "subtitle": f"{p['users']} active · link SOURCE",
                "badge": f"#{i + 1}",
            }
        )
    for i, p in enumerate(mweb_pages[:4]):
        sources.append(
            {
                "title": p["path"],
                "subtitle": f"{p['users']} active mweb · source",
                "badge": "mweb",
            }
        )

    targets = []
    for q in rising[:10]:
        qstr = str(q.get("query") or "")
        targets.append(
            {
                "title": f"Target content: «{qstr}»",
                "subtitle": f"↑ Δpos {float(q.get('delta', 0)):.1f} · rising query",
                "badge": "target",
            }
        )
    gaps = [r for r in intel_recent(db, hours=2, limit=30, site=site) if not r.is_in_our_site]
    for r in gaps[:5]:
        targets.append(
            {
                "title": (r.headline or "")[:90],
                "subtitle": "Gap story → new URL target",
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
                    "badge": "suggest",
                }
            )

    out["summary"] = f"{len(sources)} source pages (RT) + {len(targets)} targets (GSC/gap). {len(pairs)} match suggestions."
    out["metrics"] = [
        {"label": "Source", "value": str(len(sources))},
        {"label": "Target", "value": str(len(targets))},
        {"label": "Matches", "value": str(len(pairs))},
    ]
    out["sections"] = [
        {"title": "Link sources (live traffic)", "items": sources},
        {"title": "Link targets (GSC + gap)", "items": targets},
        {"title": "Source → target suggestions", "items": pairs},
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
        severity = "CRITICAL" if delta < -3 and imp > 500 else "high" if delta < -2 else "watch"
        if rt_hit:
            severity = "CRITICAL+RT"
        items.append(
            {
                "title": query,
                "subtitle": f"Δpos {delta:.1f} · {imp} imp · {'RT drop' if rt_hit else 'GSC only'}",
                "badge": severity,
                "meta": {"action": "refresh" if delta > -4 else "merge|301"},
            }
        )

    rt_decay_items = [
        {"title": p, "subtitle": "Live traffic drop driver", "badge": "RT decay"}
        for p in list(declining_pages)[:10]
        if p
    ]

    out["summary"] = f"{len(decay)} queries lost position; {len(rt_decay_items)} pages in RT drop. Combined decay score."
    out["metrics"] = [
        {"label": "GSC decay", "value": str(len(decay))},
        {"label": "RT drop", "value": str(len(rt_decay_items))},
        {"label": "Critical", "value": str(sum(1 for i in items if "CRITICAL" in str(i.get("badge"))))},
    ]
    out["sections"] = [
        {"title": "Queries losing position (GSC + RT)", "items": items},
        {"title": "Pages with live traffic drop", "items": rt_decay_items},
    ]
    out["actions"] = [{"label": "Alerts", "href": "/alerts"}, {"label": "Search Console", "href": "/search-console"}]
    return out


_HANDLERS = {
    "trend-radar": trend_trend_radar,
    "query-haber": trend_query_haber,
    "seasonality": trend_seasonality,
    "anomaly-tree": trend_anomaly_tree,
    "brief-generator": trend_brief_generator,
    "headline-lab": trend_headline_lab,
    "ic-link": trend_ic_link,
    "content-decay": trend_content_decay,
}


def get_trend_data(db: Session, slug: str, site_id: int) -> dict[str, Any]:
    handler = _HANDLERS.get(slug)
    if not handler:
        raise ValueError("Unknown trend module")
    return handler(db, site_id)


get_karma_data = get_trend_data
