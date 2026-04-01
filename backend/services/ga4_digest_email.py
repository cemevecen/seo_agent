"""GA4 haftalık (7g) karşılaştırma özet e-postaları: Döviz (4 alan) ve Sinemalar (2 alan), ayrı gönderim."""

from __future__ import annotations

import math
import re
from html import escape
from typing import Any

from sqlalchemy.orm import Session

from backend.locale import tr as tr_locale
from backend.models import Site
from backend.services.ga4_page_urls import enrich_ga4_page_rows, ga4_row_page_href, ga4_row_page_label
from backend.services.email_templates import (
    ga4_digest_same_weekday_section,
    render_email_shell,
    render_ga4_digest_email,
    section,
)
from backend.services.mailer import send_email
from backend.services.operations_notifier import TRIGGER_SOURCE_LABELS, operations_recipients
from backend.services.timezone_utils import format_local_datetime, now_local

GA4_DIGEST_DOVIZ_DOMAINS = frozenset({"doviz.com", "www.doviz.com", "m.doviz.com"})
GA4_DIGEST_SINEMA_DOMAINS = frozenset({"sinemalar.com", "www.sinemalar.com", "m.sinemalar.com"})

MIN_NOTES = 20
MAX_NOTES = 50

# Referral kaynağı: profil oturumlarına göre payı bundan küçükse özet/hesaplara alınmaz
GA4_DIGEST_REFERRAL_MIN_SESSION_SHARE = 0.01
# Sıralama skoru: düşüşler (negatif %) daha yüksek ağırlık
GA4_DIGEST_DECLINE_SCORE_MULT = 1.38

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

# Haber kategori URL'leri mail özetine alınmaz: ...-haberleri/ segmenti ve bilinen sabitler
_RE_DIGEST_HABERLERI = re.compile(r"/[^/]+-haberleri/", re.IGNORECASE)


def _exclude_digest_landing_path(path: str) -> bool:
    p = (path or "").strip()
    if not p:
        return True
    low = p.lower()
    if "/gundem-haberleri/" in low:
        return True
    if "/altin-ve-degerli-metal-haberleri/" in low:
        return True
    if _RE_DIGEST_HABERLERI.search(p):
        return True
    return False


def _exclude_digest_ga_dim_placeholder(s: str) -> bool:
    """GA boyut değerleri: (not set), (other) — mail ve seçimde yok."""
    t = (s or "").strip().lower()
    if "(not set)" in t or "(other)" in t:
        return True
    if "not%20set" in t or "(not%20set)" in t:
        return True
    return False


def balance_digest_entries_half_up_down(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Yüzde satırları: n adet varsa en fazla n//2 artış ve n//2 düşüş (|Δ%| büyükten).
    Satırlar artış / düşüş sırayla aralıklı (yeşil-kırmızı görünür); önce tüm artışlar bloklanmaz.
    pct=0 nötrler ve kanal satırları sonda."""

    def pct(e: dict[str, Any]) -> float:
        try:
            return float(e.get("pct_value") or 0)
        except (TypeError, ValueError):
            return 0.0

    if not entries:
        return []
    channels = [e for e in entries if e.get("kind") == "channel"]
    rest = [e for e in entries if e.get("kind") != "channel"]
    pos = [e for e in rest if pct(e) > 0]
    neg = [e for e in rest if pct(e) < 0]
    neutral = [e for e in rest if pct(e) == 0]
    pos.sort(key=lambda e: -abs(pct(e)))
    neg.sort(key=lambda e: -abs(pct(e)))

    n = len(rest)
    if n == 0:
        channels.sort(key=lambda e: -float(e.get("sessions_raw") or 0))
        return channels

    half = n // 2
    pos_part = pos[:half]
    neg_part = neg[:half]
    out: list[dict[str, Any]] = []
    for i in range(half):
        if i < len(pos_part):
            out.append(pos_part[i])
        if i < len(neg_part):
            out.append(neg_part[i])
    out.extend(neutral)
    channels.sort(key=lambda e: -float(e.get("sessions_raw") or 0))
    out.extend(channels)
    return out


def _digest_entry_signed_pct(entry: dict[str, Any]) -> float | None:
    """Kanal hariç işaretli yüzde; kanal veya anlamsız için None."""
    if str(entry.get("kind") or "") == "channel":
        return None
    try:
        return float(entry.get("pct_value") or 0)
    except (TypeError, ValueError):
        return None


def _select_ranked_notes_balanced(
    candidates: list[tuple[float, int, dict[str, Any]]],
    *,
    max_notes: int,
) -> list[tuple[float, int, dict[str, Any]]]:
    """MAX_NOTES havuzunu önce yarı artış / yarı düşüş (skor sırası) ile doldurur; böylece düşüşler listeden düşmez."""
    if not candidates:
        return []
    pos: list[tuple[float, int, dict[str, Any]]] = []
    neg: list[tuple[float, int, dict[str, Any]]] = []
    rest: list[tuple[float, int, dict[str, Any]]] = []
    for t in candidates:
        entry = t[2]
        sp = _digest_entry_signed_pct(entry)
        if sp is None:
            rest.append(t)
            continue
        if sp > 0:
            pos.append(t)
        elif sp < 0:
            neg.append(t)
        else:
            rest.append(t)
    pos.sort(key=lambda x: -float(x[0]))
    neg.sort(key=lambda x: -float(x[0]))
    half = max_notes // 2
    out = list(pos[:half]) + list(neg[:half])
    rem = max_notes - len(out)
    if rem > 0:
        pool = pos[half:] + neg[half:]
        pool.sort(key=lambda x: -float(x[0]))
        out.extend(pool[:rem])
    if len(out) < max_notes:
        rest.sort(key=lambda x: -float(x[0]))
        out.extend(rest[: max_notes - len(out)])
    return out[:max_notes]


def _digest_entry_key(d: dict[str, Any]) -> str:
    k = str(d.get("kind") or "")
    if k == "kpi":
        return f"kpi:{d.get('domain')}:{d.get('profile')}:{d.get('metric_label')}"
    if k == "page":
        return f"page:{d.get('domain')}:{d.get('profile')}:{d.get('path')}"
    if k == "source":
        return f"src:{d.get('domain')}:{d.get('profile')}:{d.get('source_medium')}"
    if k == "channel":
        return f"ch:{d.get('domain')}:{d.get('profile')}:{d.get('channel')}"
    return str(d)


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


def _score(pct: float, volume: float) -> float:
    try:
        v = max(1.0, float(volume))
    except (TypeError, ValueError):
        v = 1.0
    try:
        p = abs(float(pct))
    except (TypeError, ValueError):
        p = 0.0
    base = p * math.log10(v + 10.0)
    try:
        signed = float(pct)
    except (TypeError, ValueError):
        signed = 0.0
    if signed < 0:
        base *= GA4_DIGEST_DECLINE_SCORE_MULT
    return base


def _is_small_referral_row(source_medium: str, last_sessions: float, profile_sessions: float) -> bool:
    """Referral ortamı; son dönem oturum payı eşiğin altındaysa çıkar."""
    sm = (source_medium or "").lower()
    if "referral" not in sm:
        return False
    try:
        lt = float(profile_sessions)
        lv = float(last_sessions)
    except (TypeError, ValueError):
        return True
    if lt <= 0:
        return True
    return (lv / lt) < GA4_DIGEST_REFERRAL_MIN_SESSION_SHARE


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
) -> list[tuple[float, int, dict[str, Any]]]:
    """(score, area_index, digest_entry) — entry dict kind: kpi | page | source | channel."""
    prof_label = PROFILE_LABELS.get(profile, profile)
    notes: list[tuple[float, int, dict[str, Any]]] = []

    def add_kpi(
        area_idx: int,
        metric: str,
        metric_label: str,
        pct: float,
        vol: float,
        last_s: str,
        prev_s: str,
        *,
        last_raw: float | None = None,
        prev_raw: float | None = None,
    ) -> None:
        crit = _is_critical(metric, pct)
        notes.append(
            (
                _score(pct, vol),
                area_idx,
                {
                    "kind": "kpi",
                    "domain": domain,
                    "profile": prof_label,
                    "metric_label": metric_label,
                    "delta_pct": _fmt_pct(pct),
                    "pct_value": float(pct),
                    "last": last_s,
                    "prev": prev_s,
                    "last_raw": last_raw,
                    "prev_raw": prev_raw,
                    "critical": crit,
                },
            )
        )

    if not pl.get("has_period_data"):
        return notes

    lt = float(pl.get("last_total") or 0)
    pt = float(pl.get("prev_total") or 0)
    sp = float(pl.get("sessions_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(0, "sessions", "Oturumlar (toplam)", sp, lt, _fmt_num(lt), _fmt_num(pt), last_raw=lt, prev_raw=pt)
    else:
        add_kpi(0, "sessions", "Oturumlar (toplam)", sp, lt, _fmt_num(lt), _fmt_num(pt), last_raw=lt, prev_raw=pt)

    ul = float(pl.get("users_last") or 0)
    up = float(pl.get("users_prev") or 0)
    uc = float(pl.get("users_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(1, "users", "Kullanıcılar", uc, ul, _fmt_num(ul), _fmt_num(up), last_raw=ul, prev_raw=up)
    else:
        add_kpi(0, "users", "Kullanıcılar", uc, ul, _fmt_num(ul), _fmt_num(up), last_raw=ul, prev_raw=up)

    nl = float(pl.get("new_users_last") or 0)
    np = float(pl.get("new_users_prev") or 0)
    nc = float(pl.get("new_users_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(1, "new_users", "Yeni kullanıcılar", nc, nl, _fmt_num(nl), _fmt_num(np), last_raw=nl, prev_raw=np)
    else:
        add_kpi(0, "new_users", "Yeni kullanıcılar", nc, nl, _fmt_num(nl), _fmt_num(np), last_raw=nl, prev_raw=np)

    el = float(pl.get("engaged_last") or 0)
    ep = float(pl.get("engaged_prev") or 0)
    ec = float(pl.get("engaged_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(2, "engaged", "Etkileşimli oturumlar", ec, el, _fmt_num(el), _fmt_num(ep), last_raw=el, prev_raw=ep)
    else:
        add_kpi(1, "engaged", "Etkileşimli oturumlar", ec, el, _fmt_num(el), _fmt_num(ep), last_raw=el, prev_raw=ep)

    erl = float(pl.get("engagement_rate_last_pct") or 0)
    erp = float(pl.get("engagement_rate_prev_pct") or 0)
    erc = float(pl.get("engagement_rate_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(2, "engagement_rate", "Etkileşim oranı", erc, el, _fmt_pct(erl), _fmt_pct(erp), last_raw=erl, prev_raw=erp)
    else:
        add_kpi(1, "engagement_rate", "Etkileşim oranı", erc, el, _fmt_pct(erl), _fmt_pct(erp), last_raw=erl, prev_raw=erp)

    asl = float(pl.get("avg_session_last_sec") or 0)
    asp = float(pl.get("avg_session_prev_sec") or 0)
    asc = float(pl.get("avg_session_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(2, "avg_session", "Ort. oturum süresi (sn)", asc, el, _fmt_num(asl, decimals=1), _fmt_num(asp, decimals=1), last_raw=asl, prev_raw=asp)
    else:
        add_kpi(1, "avg_session", "Ort. oturum süresi (sn)", asc, el, _fmt_num(asl, decimals=1), _fmt_num(asp, decimals=1), last_raw=asl, prev_raw=asp)

    pvl = float(pl.get("pageviews_last") or 0)
    pvp = float(pl.get("pageviews_prev") or 0)
    pvc = float(pl.get("pageviews_pct_change") or 0)
    if bucket == "doviz":
        add_kpi(2, "pageviews", "Sayfa görüntüleme", pvc, pvl, _fmt_num(pvl), _fmt_num(pvp), last_raw=pvl, prev_raw=pvp)
    else:
        add_kpi(0, "pageviews", "Sayfa görüntüleme", pvc, pvl, _fmt_num(pvl), _fmt_num(pvp), last_raw=pvl, prev_raw=pvp)

    org = float(pl.get("organic_share_pct") or 0)
    if bucket == "doviz":
        add_kpi(3, "organic", "Organik arama payı (tahmini)", org, lt, _fmt_pct(org), "—", last_raw=org, prev_raw=None)

    for ch in (pl.get("top_channels") or [])[:6]:
        if not isinstance(ch, dict):
            continue
        label = str(ch.get("label") or "")
        val = float(ch.get("value") or 0)
        if label:
            ai = 3 if bucket == "doviz" else 1
            notes.append(
                (
                    _score(0.0, val),
                    ai,
                    {
                        "kind": "channel",
                        "domain": domain,
                        "profile": prof_label,
                        "channel": label,
                        "sessions": _fmt_num(val),
                        "sessions_raw": float(val),
                        "pct_value": 0.0,
                        "critical": False,
                    },
                )
            )

    for row in enrich_ga4_page_rows(pl.get("pages_no_news"))[:40]:
        if not isinstance(row, dict):
            continue
        page = str(row.get("page") or "")[:500]
        if not page or _exclude_digest_landing_path(page) or _exclude_digest_ga_dim_placeholder(page):
            continue
        last_v = float(row.get("last_total") or 0)
        prev_v = float(row.get("prev_total") or 0)
        dp = float(row.get("delta_pct") or 0)
        crit = _is_critical("page", dp)
        ai = 3 if bucket == "doviz" else 1
        page_row = {
            "page": page,
            "page_host": str(row.get("page_host") or "").strip(),
            "page_url": str(row.get("page_url") or "").strip(),
        }
        page_href = ga4_row_page_href(page_row, domain)
        page_label = ga4_row_page_label(page_row, domain)
        notes.append(
            (
                _score(dp, last_v),
                ai,
                {
                    "kind": "page",
                    "domain": domain,
                    "profile": prof_label,
                    "path": page,
                    "page_host": page_row["page_host"],
                    "page_url": page_href,
                    "page_label": page_label,
                    "delta_pct": _fmt_pct(dp),
                    "pct_value": float(dp),
                    "last": _fmt_num(last_v),
                    "prev": _fmt_num(prev_v),
                    "last_raw": last_v,
                    "prev_raw": prev_v,
                    "critical": crit,
                },
            )
        )

    for row in (pl.get("sources") or [])[:40]:
        if not isinstance(row, dict):
            continue
        sm = str(row.get("source_medium") or "")[:500]
        if not sm or _exclude_digest_ga_dim_placeholder(sm):
            continue
        last_v = float(row.get("last_total") or 0)
        if _is_small_referral_row(sm, last_v, lt):
            continue
        prev_v = float(row.get("prev_total") or 0)
        dp = float(row.get("delta_pct") or 0)
        crit = _is_critical("source", dp)
        ai = 3 if bucket == "doviz" else 1
        notes.append(
            (
                _score(dp, last_v),
                ai,
                {
                    "kind": "source",
                    "domain": domain,
                    "profile": prof_label,
                    "source_medium": sm,
                    "delta_pct": _fmt_pct(dp),
                    "pct_value": float(dp),
                    "last": _fmt_num(last_v),
                    "prev": _fmt_num(prev_v),
                    "last_raw": last_v,
                    "prev_raw": prev_v,
                    "critical": crit,
                },
            )
        )

    return notes


def _build_bucket_digest(
    db: Session,
    *,
    bucket: str,
    site_ids: list[int],
) -> tuple[
    list[tuple[str, list[dict[str, Any]]]], list[dict[str, Any]], list[str], str
] | None:
    """Dönüş: (alan_blokları, kritik_satırlar, domain_listesi, aynı_gün_html) veya None."""
    _ga4_profile_payload_for_period, get_ga4_connection_status, get_latest_metrics = _import_profile_payload()
    from backend.main import _external_site_ids

    external = _external_site_ids(db)
    all_notes: list[tuple[float, int, dict[str, Any]]] = []
    domains_seen: list[str] = []
    same_weekday_items: list[dict[str, Any]] = []

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
            sw = pl.get("same_weekday_kpi") or {}
            if isinstance(sw, dict) and sw.get("last") and sw.get("prev"):
                same_weekday_items.append(
                    {
                        "domain": site.domain,
                        "profile": PROFILE_LABELS.get(profile, profile),
                        "reference_date": sw.get("reference_date"),
                        "previous_week_date": sw.get("previous_week_date"),
                        "weekday_label_tr": sw.get("weekday_label_tr"),
                        "last": sw.get("last") or {},
                        "prev": sw.get("prev") or {},
                        "property_id": sw.get("property_id"),
                    }
                )
            all_notes.extend(
                _collect_notes_for_profile(
                    domain=site.domain,
                    profile=profile,
                    pl=pl,
                    bucket=bucket,
                )
            )

    if not all_notes and not same_weekday_items:
        return None

    area_titles = DOVIZ_AREA_TITLES if bucket == "doviz" else SINEMA_AREA_TITLES
    n_areas = len(area_titles)
    best_by_key: dict[str, tuple[float, int, dict[str, Any]]] = {}
    for score, area_idx, entry in all_notes:
        if not entry:
            continue
        ai = min(max(int(area_idx), 0), n_areas - 1)
        k = _digest_entry_key(entry)
        prev = best_by_key.get(k)
        if prev is None or float(score) > prev[0]:
            best_by_key[k] = (float(score), ai, entry)

    ranked = _select_ranked_notes_balanced(list(best_by_key.values()), max_notes=MAX_NOTES)

    buckets: list[list[dict[str, Any]]] = [[] for _ in range(n_areas)]
    for _sc, area_idx, entry in ranked:
        buckets[area_idx].append(entry)

    if not any(buckets) and not same_weekday_items:
        return None

    crit_rows = balance_digest_entries_half_up_down(
        [dict(e) for _, _, e in ranked if e.get("critical")]
    )

    area_blocks: list[tuple[str, list[dict[str, Any]]]] = []
    for i, title in enumerate(area_titles):
        raw = buckets[i] if i < len(buckets) else []
        items = balance_digest_entries_half_up_down(raw)
        area_blocks.append((title, items))

    sw_html = ga4_digest_same_weekday_section(same_weekday_items)
    return area_blocks, crit_rows, domains_seen, sw_html


def _ga4_collect_failures_footer_html(bucket: str, collect_failures: list[tuple[str, str]] | None) -> str:
    """Günlük GA4 toplama hatalarını özet tablo HTML (mail gövdesi sonuna)."""
    if not collect_failures:
        return ""
    rows = [(d, e) for d, e in collect_failures if ga4_digest_bucket_for_domain(d) == bucket]
    if not rows:
        return ""
    inner = "".join(
        "<tr>"
        f'<td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;font-size:13px;color:#0f172a;">{escape(d)}</td>'
        f'<td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;font-size:13px;color:#b91c1c;">{escape(e)}</td>'
        "</tr>"
        for d, e in rows
    )
    cap = (
        '<p style="margin:0 0 10px 0;font-size:12px;font-weight:800;letter-spacing:0.06em;color:#64748b;text-transform:uppercase;">'
        "Günlük yenileme — hata özeti</p>"
    )
    tbl = (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #fecaca;border-radius:12px;overflow:hidden;background:#fff7ed;">'
        "<thead><tr>"
        '<th style="padding:10px 12px;text-align:left;font-size:11px;font-weight:800;color:#9a3412;background:#ffedd5;">Site</th>'
        '<th style="padding:10px 12px;text-align:left;font-size:11px;font-weight:800;color:#9a3412;background:#ffedd5;">Hata</th>'
        "</tr></thead>"
        f"<tbody>{inner}</tbody></table>"
    )
    return cap + tbl


def send_ga4_weekly_digest_emails(
    db: Session,
    *,
    trigger_source: str,
    action_label: str,
    only_buckets: frozenset[str] | None = None,
    collect_failures: list[tuple[str, str]] | None = None,
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
        fail_footer = _ga4_collect_failures_footer_html(bucket, collect_failures)
        built = _build_bucket_digest(db, bucket=bucket, site_ids=site_ids)
        if not built:
            if not fail_footer:
                return
            summary_rows = [
                ("Mail tipi", "GA4 haftalık özet"),
                ("Tetik", ts_label),
                ("Aksiyon", action_label),
                ("Kapsam", title_prefix),
                ("Siteler", "-"),
                ("Zaman", format_local_datetime(now_local(), include_suffix=True)),
            ]
            subject = f"SEO Agent GA4: {title_prefix} — yenileme hataları ({ts_label})"
            html = render_email_shell(
                eyebrow=tr_locale.GA4_DIGEST_EYEBROW,
                title=f"GA4 yenileme hataları — {title_prefix}",
                intro="Özet veri oluşturulamadı; günlük toplama sırasında aşağıdaki hatalar kaydedildi.",
                tone="rose",
                status_label="Hata",
                sections=[section("Hatalar", fail_footer)],
            )
            if send_email(subject, html, recipients=recipients):
                subjects.append(subject)
            return
        area_blocks, critical_rows, domains_seen, same_weekday_html = built
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
            eyebrow=tr_locale.GA4_DIGEST_EYEBROW,
            title=f"GA4 haftalık karşılaştırma — {title_prefix}",
            tone="blue",
            status_label=tr_locale.GA4_DIGEST_STATUS_LABEL,
            meta_rows=summary_rows,
            critical_rows=critical_rows[:24],
            area_blocks=area_blocks,
            same_weekday_section_html=same_weekday_html,
            footer_extra_html=fail_footer,
        )
        if send_email(subject, html, recipients=recipients):
            subjects.append(subject)

    send_one("doviz", doviz_ids, "Döviz (4 alan)")
    send_one("sinema", sinema_ids, "Sinemalar (2 alan)")
    return subjects
