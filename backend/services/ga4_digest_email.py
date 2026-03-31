"""GA4 haftalık (7g) karşılaştırma özet e-postaları: Döviz (4 alan) ve Sinemalar (2 alan), ayrı gönderim."""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.email_templates import render_ga4_digest_email
from backend.services.mailer import send_email
from backend.services.operations_notifier import TRIGGER_SOURCE_LABELS, operations_recipients
from backend.services.timezone_utils import format_local_datetime, now_local

GA4_DIGEST_DOVIZ_DOMAINS = frozenset({"doviz.com", "www.doviz.com", "m.doviz.com"})
GA4_DIGEST_SINEMA_DOMAINS = frozenset({"sinemalar.com", "www.sinemalar.com", "m.sinemalar.com"})

MIN_NOTES = 20
MAX_NOTES = 50

DOVIZ_AREA_TITLES = (
    "Alan 1 — Trafik ve oturum (7g / önceki 7g)",
    "Alan 2 — Kullanıcı ve yeni kullanıcı",
    "Alan 3 — Etkileşim, süre ve sayfa görüntüleme",
    "Alan 4 — Organik pay, kanal ve sayfa / kaynak hareketleri",
)

SINEMA_AREA_TITLES = (
    "Alan 1 — Trafik ve kullanıcı özeti",
    "Alan 2 — Etkileşim ve kaynak / içerik",
)

PROFILE_LABELS = {
    "web": "Web",
    "mweb": "Mobil web",
    "android": "Android",
    "ios": "iOS",
}


def ga4_digest_bucket_for_domain(domain: str | None) -> str | None:
    d = (domain or "").strip().lower()
    if d in GA4_DIGEST_DOVIZ_DOMAINS:
        return "doviz"
    if d in GA4_DIGEST_SINEMA_DOMAINS:
        return "sinema"
    return None


def _fmt_num(n: float, *, decimals: int = 0) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "-"
    s = f"{v:,.{decimals}f}"
    s = s.replace(",", "__TMP__").replace(".", ",").replace("__TMP__", ".")
    if decimals > 0:
        s = s.rstrip("0").rstrip(",")
    return s


def _fmt_pct(p: float) -> str:
    return f"%{_fmt_num(p, decimals=1)}"


def _import_profile_payload():
    from backend.main import _ga4_profile_payload_for_period
    from backend.services.ga4_auth import get_ga4_connection_status
    from backend.services.metric_store import get_latest_metrics

    return _ga4_profile_payload_for_period, get_ga4_connection_status, get_latest_metrics


def _score(abs_pct: float, volume: float) -> float:
    try:
        v = max(1.0, float(volume))
    except (TypeError, ValueError):
        v = 1.0
    try:
        p = abs(float(abs_pct))
    except (TypeError, ValueError):
        p = 0.0
    return p * math.log10(v + 10.0)


def _is_critical(metric: str, pct: float) -> bool:
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return False
    if abs(p) >= 15.0:
        return True
    if metric == "sessions" and p <= -10.0:
        return True
    return False


def _collect_notes_for_profile(
    *,
    domain: str,
    profile: str,
    pl: dict[str, Any],
    bucket: str,
) -> tuple[list[tuple[float, int, str, bool]], list[str]]:
    """(score, area_index, text, is_critical), critical_only_lines"""
    prof_label = PROFILE_LABELS.get(profile, profile)
    notes: list[tuple[float, int, str, bool]] = []
    critical_lines: list[str] = []

    def add(area_idx: int, metric: str, text: str, pct: float, vol: float) -> None:
        crit = _is_critical(metric, pct)
        if crit:
            text = f"KRİTİK — {text}"
        notes.append((_score(pct, vol), area_idx, text, crit))
        if crit:
            critical_lines.append(text)

    if not pl.get("has_period_data"):
        return notes, critical_lines

    lt = float(pl.get("last_total") or 0)
    pt = float(pl.get("prev_total") or 0)
    sp = float(pl.get("sessions_pct_change") or 0)
    if bucket == "doviz":
        add(0, "sessions", f"{domain} ({prof_label}): Oturumlar {_fmt_pct(sp)} değişti (son 7g {_fmt_num(lt)} vs önceki 7g {_fmt_num(pt)}).", sp, lt)
    else:
        add(0, "sessions", f"{domain} ({prof_label}): Oturumlar {_fmt_pct(sp)} — son 7g {_fmt_num(lt)} / önceki 7g {_fmt_num(pt)}.", sp, lt)

    ul = float(pl.get("users_last") or 0)
    up = float(pl.get("users_prev") or 0)
    uc = float(pl.get("users_pct_change") or 0)
    if bucket == "doviz":
        add(1, "users", f"{domain} ({prof_label}): Kullanıcılar {_fmt_pct(uc)} (son {_fmt_num(ul)} / önce {_fmt_num(up)}).", uc, ul)
    else:
        add(0, "users", f"{domain} ({prof_label}): Kullanıcılar {_fmt_pct(uc)} (son {_fmt_num(ul)} / önce {_fmt_num(up)}).", uc, ul)

    nl = float(pl.get("new_users_last") or 0)
    np = float(pl.get("new_users_prev") or 0)
    nc = float(pl.get("new_users_pct_change") or 0)
    if bucket == "doviz":
        add(1, "new_users", f"{domain} ({prof_label}): Yeni kullanıcılar {_fmt_pct(nc)} (son {_fmt_num(nl)} / önce {_fmt_num(np)}).", nc, nl)
    else:
        add(0, "new_users", f"{domain} ({prof_label}): Yeni kullanıcılar {_fmt_pct(nc)} (son {_fmt_num(nl)} / önce {_fmt_num(np)}).", nc, nl)

    el = float(pl.get("engaged_last") or 0)
    ep = float(pl.get("engaged_prev") or 0)
    ec = float(pl.get("engaged_pct_change") or 0)
    if bucket == "doviz":
        add(2, "engaged", f"{domain} ({prof_label}): Etkileşimli oturumlar {_fmt_pct(ec)} (son {_fmt_num(el)} / önce {_fmt_num(ep)}).", ec, el)
    else:
        add(1, "engaged", f"{domain} ({prof_label}): Etkileşimli oturumlar {_fmt_pct(ec)} (son {_fmt_num(el)} / önce {_fmt_num(ep)}).", ec, el)

    erl = float(pl.get("engagement_rate_last_pct") or 0)
    erp = float(pl.get("engagement_rate_prev_pct") or 0)
    erc = float(pl.get("engagement_rate_pct_change") or 0)
    if bucket == "doviz":
        add(2, "engagement_rate", f"{domain} ({prof_label}): Etkileşim oranı {_fmt_pct(erc)} (son {_fmt_pct(erl)} / önce {_fmt_pct(erp)}).", erc, el)
    else:
        add(1, "engagement_rate", f"{domain} ({prof_label}): Etkileşim oranı {_fmt_pct(erc)} (son {_fmt_pct(erl)} / önce {_fmt_pct(erp)}).", erc, el)

    asl = float(pl.get("avg_session_last_sec") or 0)
    asp = float(pl.get("avg_session_prev_sec") or 0)
    asc = float(pl.get("avg_session_pct_change") or 0)
    if bucket == "doviz":
        add(2, "avg_session", f"{domain} ({prof_label}): Ort. oturum süresi {_fmt_pct(asc)} (son {_fmt_num(asl, decimals=1)} sn / önce {_fmt_num(asp, decimals=1)} sn).", asc, el)
    else:
        add(1, "avg_session", f"{domain} ({prof_label}): Ort. oturum süresi {_fmt_pct(asc)} (son {_fmt_num(asl, decimals=1)} sn / önce {_fmt_num(asp, decimals=1)} sn).", asc, el)

    pvl = float(pl.get("pageviews_last") or 0)
    pvp = float(pl.get("pageviews_prev") or 0)
    pvc = float(pl.get("pageviews_pct_change") or 0)
    if bucket == "doviz":
        add(2, "pageviews", f"{domain} ({prof_label}): Sayfa görüntüleme {_fmt_pct(pvc)} (son {_fmt_num(pvl)} / önce {_fmt_num(pvp)}).", pvc, pvl)
    else:
        add(0, "pageviews", f"{domain} ({prof_label}): Sayfa görüntüleme {_fmt_pct(pvc)} (son {_fmt_num(pvl)} / önce {_fmt_num(pvp)}).", pvc, pvl)

    org = float(pl.get("organic_share_pct") or 0)
    if bucket == "doviz":
        add(3, "organic", f"{domain} ({prof_label}): Organik arama payı (son 7g oturum içinde) ~{_fmt_pct(org)}.", org, lt)

    for ch in (pl.get("top_channels") or [])[:6]:
        if not isinstance(ch, dict):
            continue
        label = str(ch.get("label") or "")
        val = float(ch.get("value") or 0)
        if label:
            if bucket == "doviz":
                notes.append((_score(0.0, val), 3, f"{domain} ({prof_label}): Kanal «{label}» son 7g {_fmt_num(val)} oturum.", False))
            else:
                notes.append((_score(0.0, val), 1, f"{domain} ({prof_label}): Kanal «{label}» son 7g {_fmt_num(val)} oturum.", False))

    for row in (pl.get("pages_no_news") or [])[:40]:
        if not isinstance(row, dict):
            continue
        page = str(row.get("page") or "")[:120]
        if not page:
            continue
        delta_pct = float(row.get("delta_pct") or 0)
        last_v = float(row.get("last_total") or 0)
        prev_v = float(row.get("prev_total") or 0)
        dp = float(delta_pct)
        crit = _is_critical("page", dp)
        t = f"{domain} ({prof_label}): Sayfa oturumu {_fmt_pct(dp)} — «{page}» (son {_fmt_num(last_v)} / önce {_fmt_num(prev_v)})."
        if crit:
            t = f"KRİTİK — {t}"
            critical_lines.append(t)
        ai = 3 if bucket == "doviz" else 1
        notes.append((_score(dp, last_v), ai, t, crit))

    for row in (pl.get("sources") or [])[:40]:
        if not isinstance(row, dict):
            continue
        sm = str(row.get("source_medium") or "")[:120]
        if not sm:
            continue
        delta_pct = float(row.get("delta_pct") or 0)
        last_v = float(row.get("last_total") or 0)
        prev_v = float(row.get("prev_total") or 0)
        dp = float(delta_pct)
        crit = _is_critical("source", dp)
        t = f"{domain} ({prof_label}): Kaynak/ortam {_fmt_pct(dp)} — «{sm}» (son {_fmt_num(last_v)} / önce {_fmt_num(prev_v)})."
        if crit:
            t = f"KRİTİK — {t}"
            critical_lines.append(t)
        ai = 3 if bucket == "doviz" else 1
        notes.append((_score(dp, last_v), ai, t, crit))

    return notes, critical_lines


def _build_bucket_digest(
    db: Session,
    *,
    bucket: str,
    site_ids: list[int],
) -> tuple[list[tuple[str, list[str]]], list[str], list[str]] | None:
    """Dönüş: (alan_blokları, kritik_satırlar, domain_listesi) veya None."""
    _ga4_profile_payload_for_period, get_ga4_connection_status, get_latest_metrics = _import_profile_payload()
    from backend.main import _external_site_ids

    external = _external_site_ids(db)
    all_notes: list[tuple[float, int, str, bool]] = []
    all_critical: list[str] = []
    domains_seen: list[str] = []

    for site_id in site_ids:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            continue
        if site.id in external:
            continue
        conn = get_ga4_connection_status(db, site.id)
        if not conn.get("connected"):
            continue
        latest = {m.metric_type: m.value for m in get_latest_metrics(db, site.id)}
        props = (conn.get("properties") or {}) if isinstance(conn, dict) else {}
        domains_seen.append(site.domain)
        for profile in ("web", "mweb", "android", "ios"):
            prop_id = str(props.get(profile) or "").strip()
            if not prop_id:
                continue
            pl = _ga4_profile_payload_for_period(
                db,
                site_id=site.id,
                profile=profile,
                period_days=7,
                latest=latest,
                prop_id=prop_id,
            )
            n, crit = _collect_notes_for_profile(
                domain=site.domain,
                profile=profile,
                pl=pl,
                bucket=bucket,
            )
            all_notes.extend(n)
            all_critical.extend(crit)

    if not all_notes:
        return None

    area_titles = DOVIZ_AREA_TITLES if bucket == "doviz" else SINEMA_AREA_TITLES
    n_areas = len(area_titles)
    best_by_text: dict[str, tuple[float, int, str]] = {}
    for score, area_idx, text, _crit in all_notes:
        if not text:
            continue
        ai = min(max(int(area_idx), 0), n_areas - 1)
        prev = best_by_text.get(text)
        if prev is None or float(score) > prev[0]:
            best_by_text[text] = (float(score), ai, text)
    ranked = sorted(best_by_text.values(), key=lambda x: -x[0])[:MAX_NOTES]

    buckets: list[list[str]] = [[] for _ in range(n_areas)]
    for _sc, area_idx, text in ranked:
        buckets[area_idx].append(text)

    if not any(buckets):
        return None

    uniq_crit: list[str] = []
    if all_critical:
        s2: set[str] = set()
        for c in sorted(all_critical, key=len, reverse=True):
            if c not in s2:
                s2.add(c)
                uniq_crit.append(c)

    area_blocks: list[tuple[str, list[str]]] = []
    for i, title in enumerate(area_titles):
        items = buckets[i] if i < len(buckets) else []
        area_blocks.append((title, items))

    return area_blocks, uniq_crit, domains_seen


def send_ga4_weekly_digest_emails(
    db: Session,
    *,
    trigger_source: str,
    action_label: str,
    only_buckets: frozenset[str] | None = None,
) -> list[str]:
    """
    Döviz ve Sinemalar için ayrı HTML e-posta gönderir.
    only_buckets: None ise ikisi; {'doviz'} veya {'sinema'} ile tek grup.
    """
    from backend.main import _external_site_ids

    external = _external_site_ids(db)
    sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
    doviz_ids: list[int] = []
    sinema_ids: list[int] = []
    for s in sites:
        if s.id in external:
            continue
        b = ga4_digest_bucket_for_domain(s.domain)
        if b == "doviz":
            doviz_ids.append(s.id)
        elif b == "sinema":
            sinema_ids.append(s.id)

    want = only_buckets or frozenset({"doviz", "sinema"})
    subjects: list[str] = []
    recipients = operations_recipients()
    if not recipients:
        return subjects

    ts_label = TRIGGER_SOURCE_LABELS.get(trigger_source, trigger_source)

    def send_one(bucket: str, site_ids: list[int], title_prefix: str) -> None:
        if bucket not in want or not site_ids:
            return
        built = _build_bucket_digest(db, bucket=bucket, site_ids=site_ids)
        if not built:
            return
        area_blocks, critical_lines, domains_seen = built
        summary_rows = [
            ("Mail tipi", "GA4 haftalık özet"),
            ("Tetik", ts_label),
            ("Aksiyon", action_label),
            ("Kapsam", title_prefix),
            ("Siteler", ", ".join(sorted(set(domains_seen))) or "-"),
            ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
        ]
        subject = f"SEO Agent GA4: {title_prefix} — haftalık özet ({ts_label})"
        html = render_ga4_digest_email(
            eyebrow="SEO Agent Operations",
            title=f"GA4 haftalık karşılaştırma — {title_prefix}",
            tone="blue",
            status_label="GA4 Özet",
            meta_rows=summary_rows,
            critical_lines=critical_lines[:24],
            area_blocks=area_blocks,
        )
        if send_email(subject, html, recipients=recipients):
            subjects.append(subject)

    send_one("doviz", doviz_ids, "Döviz (4 alan)")
    send_one("sinema", sinema_ids, "Sinemalar (2 alan)")
    return subjects
