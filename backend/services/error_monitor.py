"""
Site hata izleme — GA4 Analytics Data API ile 404/hata sayfası tespiti.
Credential pattern: ga4_realtime.py ile aynı (global service account).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_REFERRER_SKIP = frozenset({"", "(not set)", "(none)", "(direct)"})
_MAX_REFERRERS = 20
_MAX_CHANNELS = 15


def _host_hint(domain: str | None) -> str:
    if not domain:
        return ""
    d = domain.strip().lower()
    for prefix in ("www.", "m."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d


def _referrer_is_internal(ref: str, site_host: str) -> bool:
    if not ref or not site_host:
        return False
    low = ref.lower()
    return site_host in low


def coerce_referrer_item(item: Any, site_host: str = "") -> dict[str, Any]:
    if isinstance(item, dict):
        ref = (item.get("ref") or item.get("url") or item.get("referrer") or "").strip()
        users = int(item.get("users") or 0)
        internal = bool(item.get("internal")) if "internal" in item else _referrer_is_internal(ref, site_host)
        return {"ref": ref, "users": users, "internal": internal}
    if isinstance(item, str):
        ref = item.strip()
        return {"ref": ref, "users": 0, "internal": _referrer_is_internal(ref, site_host)}
    return {"ref": "", "users": 0, "internal": False}


def normalize_referrers(raw: list | None, site_host: str = "") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw or []:
        coerced = coerce_referrer_item(item, site_host)
        if coerced["ref"] and coerced["ref"].lower() not in _REFERRER_SKIP:
            out.append(coerced)
    return out


def merge_referrer_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ref: dict[str, dict[str, Any]] = {}
    for it in items:
        ref = (it.get("ref") or "").strip()
        if not ref:
            continue
        if ref not in by_ref:
            by_ref[ref] = {"ref": ref, "users": 0, "internal": bool(it.get("internal"))}
        by_ref[ref]["users"] += int(it.get("users") or 0)
        by_ref[ref]["internal"] = by_ref[ref]["internal"] or bool(it.get("internal"))
    merged = sorted(by_ref.values(), key=lambda x: x["users"], reverse=True)
    return merged[:_MAX_REFERRERS]


def coerce_channel_item(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        ch = (item.get("channel") or item.get("label") or "").strip()
        return {"channel": ch, "users": int(item.get("users") or 0)}
    if isinstance(item, str):
        return {"channel": item.strip(), "users": 0}
    return {"channel": "", "users": 0}


def normalize_channels(raw: list | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw or []:
        coerced = coerce_channel_item(item)
        if coerced["channel"] and coerced["channel"].lower() not in _REFERRER_SKIP:
            out.append(coerced)
    return out


def merge_channel_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ch: dict[str, dict[str, Any]] = {}
    for it in items:
        ch = (it.get("channel") or "").strip()
        if not ch:
            continue
        if ch not in by_ch:
            by_ch[ch] = {"channel": ch, "users": 0}
        by_ch[ch]["users"] += int(it.get("users") or 0)
    merged = sorted(by_ch.values(), key=lambda x: x["users"], reverse=True)
    return merged[:_MAX_CHANNELS]


def _build_ga4_client():
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.oauth2 import service_account
    from backend.services.ga4_auth import GA4_SCOPES, load_ga4_service_account_info

    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_ga4_error_pages(
    property_id: str,
    days: int = 7,
    limit: int = 200,
    site_domain: str | None = None,
) -> list[dict[str, Any]]:
    """
    GA4 Analytics Data API ile hata sayfalarını çeker.
    pagePath'te '404' geçen VEYA pageTitle'da hata kelimeleri içeren sayfalar.
    """
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Filter, FilterExpression,
        FilterExpressionList, Metric, OrderBy, RunReportRequest,
    )

    pid = property_id.strip()
    if not pid.startswith("properties/"):
        pid = f"properties/{pid}"

    # 404 sayfalarını yakala: path'te /404 VEYA title'da hata kelimeleri
    error_filters = [
        FilterExpression(filter=Filter(
            field_name="pagePath",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="/404", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="404", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="bulunamadı", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="not found", case_sensitive=False,
            ),
        )),
        FilterExpression(filter=Filter(
            field_name="pageTitle",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value="sayfa bulunamadı", case_sensitive=False,
            ),
        )),
    ]

    # Sorgu 1: URL + başlık + kullanıcı sayısı
    req_main = RunReportRequest(
        property=pid,
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),
            Dimension(name="pageTitle"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
        ],
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(expressions=error_filters)
        ),
        order_bys=[OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="totalUsers"),
            desc=True,
        )],
        limit=limit,
    )

    try:
        client = _build_ga4_client()
        response = client.run_report(req_main)
    except Exception as exc:
        logger.warning("GA4 error pages fetch hatası [property=%s]: %s", property_id, exc)
        return []

    grouped: dict[str, dict] = {}
    for row in response.rows:
        dims = [dv.value for dv in row.dimension_values]
        mets = [mv.value for mv in row.metric_values]
        page_path  = dims[0] if len(dims) > 0 else ""
        page_title = dims[1] if len(dims) > 1 else ""
        pageviews  = int(mets[0]) if len(mets) > 0 else 0
        users      = int(mets[1]) if len(mets) > 1 else 0
        if not page_path:
            continue
        grouped[page_path] = {
            "url":         page_path,
            "page_title":  page_title,
            "referrers":   [],
            "channels":    [],
            "referrer_unknown_users": 0,
            "pageviews":   pageviews,
            "users":       users,
            "status_code": 404,
            "source":      "ga4",
            "error_type":  "not_found",
        }

    site_host = _host_hint(site_domain)

    if not grouped:
        return []

    # Sorgu 2: aynı filtre + pageReferrer — ayrı sorgu daha güvenilir
    req_ref = RunReportRequest(
        property=pid,
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),
            Dimension(name="pageReferrer"),
        ],
        metrics=[Metric(name="totalUsers")],
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(expressions=error_filters)
        ),
        order_bys=[OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="totalUsers"),
            desc=True,
        )],
        limit=limit * 5,
    )
    ref_agg: dict[str, dict[str, int]] = {}
    ref_unknown: dict[str, int] = {}
    try:
        ref_response = client.run_report(req_ref)
        for row in ref_response.rows:
            dims = [dv.value for dv in row.dimension_values]
            mets = [mv.value for mv in row.metric_values]
            page_path = dims[0] if len(dims) > 0 else ""
            referrer  = (dims[1] if len(dims) > 1 else "").strip()
            users = int(mets[0]) if len(mets) > 0 else 0
            if page_path not in grouped or users <= 0:
                continue
            if not referrer or referrer.lower() in _REFERRER_SKIP:
                ref_unknown[page_path] = ref_unknown.get(page_path, 0) + users
                continue
            bucket = ref_agg.setdefault(page_path, {})
            bucket[referrer] = bucket.get(referrer, 0) + users
    except Exception as exc:
        logger.debug("GA4 referrer sorgusu atlandı [property=%s]: %s", property_id, exc)

    for page_path, counts in ref_agg.items():
        if page_path not in grouped:
            continue
        items = [
            {
                "ref": ref,
                "users": u,
                "internal": _referrer_is_internal(ref, site_host),
            }
            for ref, u in sorted(counts.items(), key=lambda x: x[1], reverse=True)
        ]
        grouped[page_path]["referrers"] = items[:_MAX_REFERRERS]
        grouped[page_path]["referrer_unknown_users"] = ref_unknown.get(page_path, 0)

    for page_path, unknown in ref_unknown.items():
        if page_path in grouped:
            grouped[page_path]["referrer_unknown_users"] = unknown

    # Sorgu 3: oturum kanalı (referrer ölçülemeyen trafik için)
    req_ch = RunReportRequest(
        property=pid,
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
        dimensions=[
            Dimension(name="pagePathPlusQueryString"),
            Dimension(name="sessionDefaultChannelGroup"),
        ],
        metrics=[Metric(name="totalUsers")],
        dimension_filter=FilterExpression(
            or_group=FilterExpressionList(expressions=error_filters)
        ),
        order_bys=[OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="totalUsers"),
            desc=True,
        )],
        limit=limit * 10,
    )
    ch_agg: dict[str, dict[str, int]] = {}
    try:
        ch_response = client.run_report(req_ch)
        for row in ch_response.rows:
            dims = [dv.value for dv in row.dimension_values]
            mets = [mv.value for mv in row.metric_values]
            page_path = dims[0] if len(dims) > 0 else ""
            channel = (dims[1] if len(dims) > 1 else "").strip()
            users = int(mets[0]) if len(mets) > 0 else 0
            if page_path not in grouped or not channel or users <= 0:
                continue
            bucket = ch_agg.setdefault(page_path, {})
            bucket[channel] = bucket.get(channel, 0) + users
    except Exception as exc:
        logger.debug("GA4 kanal sorgusu atlandı [property=%s]: %s", property_id, exc)

    for page_path, counts in ch_agg.items():
        if page_path not in grouped:
            continue
        grouped[page_path]["channels"] = [
            {"channel": ch, "users": u}
            for ch, u in sorted(counts.items(), key=lambda x: x[1], reverse=True)
        ][: _MAX_CHANNELS]

    results = sorted(grouped.values(), key=lambda x: x["users"], reverse=True)
    logger.info("GA4 hata sayfaları: property=%s, %d benzersiz URL", property_id, len(results))
    return results


def save_error_logs(
    db: Session,
    site_id: int,
    errors: list[dict[str, Any]],
    source: str,
) -> int:
    """Hataları DB'ye kaydeder. Her çekimde önce eski kayıtlar temizlenir, taze yazılır."""
    from backend.models import SiteErrorLog

    # Eski kayıtları sil — GA4 artık gruplu sonuç döndürüyor, duplicate kalmasın
    try:
        db.query(SiteErrorLog).filter(
            SiteErrorLog.site_id == site_id,
            SiteErrorLog.source == source,
        ).delete(synchronize_session=False)
        db.flush()
    except Exception:
        db.rollback()

    saved = 0
    now = datetime.utcnow()

    for e in errors:
        url = (e.get("url") or "")[:2048]
        if not url:
            continue
        status_code = int(e.get("status_code", 404))

        existing = (
            db.query(SiteErrorLog)
            .filter(
                SiteErrorLog.site_id == site_id,
                SiteErrorLog.url == url,
                SiteErrorLog.source == source,
                SiteErrorLog.status_code == status_code,
            )
            .first()
        )
        new_refs = normalize_referrers(e.get("referrers") or [])
        if e.get("referrer"):
            new_refs = merge_referrer_items(
                new_refs + normalize_referrers([e.get("referrer")])
            )
        new_channels = normalize_channels(e.get("channels") or [])
        extra = json.dumps({
            "page_title": e.get("page_title", ""),
            "referrers": merge_referrer_items(new_refs),
            "channels": merge_channel_items(new_channels),
            "referrer_unknown_users": int(e.get("referrer_unknown_users") or 0),
        }, ensure_ascii=False)
        if existing:
            existing.hit_count = int(e.get("users", 1))  # GA4 artık date'siz, tam toplam
            existing.last_seen  = now
            existing.extra_json = extra
        else:
            row = SiteErrorLog(
                site_id=site_id,
                url=url,
                status_code=status_code,
                source=source,
                error_type=e.get("error_type", "not_found"),
                hit_count=int(e.get("users", 1)),
                first_seen=now,
                last_seen=now,
                extra_json=extra,
            )
            db.add(row)
        saved += 1

    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("SiteErrorLog kayıt hatası")
        return 0

    return saved


def format_error_sources_html(
    error: dict[str, Any],
    *,
    max_refs: int = 3,
    max_channels: int = 3,
) -> str:
    """404 mail ve benzeri için referrer + kanal satırları (HTML)."""
    from html import escape

    parts: list[str] = []
    referrers = error.get("referrers") or []
    if isinstance(referrers, list) and referrers and isinstance(referrers[0], str):
        referrers = normalize_referrers(referrers)
    for item in referrers[:max_refs]:
        ref = (item.get("ref") or "") if isinstance(item, dict) else str(item)
        users = int(item.get("users") or 0) if isinstance(item, dict) else 0
        if not ref:
            continue
        label = ref[:70] + ("…" if len(ref) > 70 else "")
        cnt = f" · {users} kul." if users else ""
        parts.append(
            f'<div style="font-size:10px;color:#64748b;font-family:monospace;margin-top:1px">'
            f"↳ {escape(label)}{escape(cnt)}</div>"
        )
    if len(referrers) > max_refs:
        parts.append(
            f'<div style="font-size:10px;color:#94a3b8">+{len(referrers) - max_refs} referrer daha</div>'
        )

    unknown = int(error.get("referrer_unknown_users") or 0)
    channels = error.get("channels") or []
    if unknown and not referrers:
        parts.append(
            f'<div style="font-size:10px;color:#94a3b8;margin-top:1px">'
            f"Referrer yok: {unknown} kul.</div>"
        )
    for ch in channels[:max_channels]:
        label = (ch.get("channel") or "") if isinstance(ch, dict) else str(ch)
        users = int(ch.get("users") or 0) if isinstance(ch, dict) else 0
        if not label:
            continue
        parts.append(
            f'<div style="font-size:10px;color:#475569;margin-top:1px">'
            f"◎ {escape(label)} · {users} kul.</div>"
        )
    if unknown and referrers:
        parts.append(
            f'<div style="font-size:10px;color:#94a3b8">+{unknown} kul. referrer ölçülmedi</div>'
        )
    if not parts:
        return ""
    return f'<div style="margin-top:2px">{"".join(parts)}</div>'


def _ga4_source_key(days: int) -> str:
    return f"ga4_{days}d"


def get_error_summary(
    db: Session,
    site_id: int,
    days: int = 7,
) -> dict[str, Any]:
    """Belirtilen periyot için DB'den hata özetini döner (GA4 çağrısı yapmaz)."""
    from backend.models import Site, SiteErrorLog

    site_host = ""
    site = db.query(Site).filter(Site.id == site_id).first()
    if site:
        site_host = _host_hint(site.domain)

    source_key = _ga4_source_key(days)

    rows = (
        db.query(SiteErrorLog)
        .filter(
            SiteErrorLog.site_id == site_id,
            SiteErrorLog.source == source_key,
        )
        .order_by(SiteErrorLog.hit_count.desc())
        .limit(500)
        .all()
    )

    total_404 = sum(1 for r in rows if r.status_code == 404)
    total_5xx = sum(1 for r in rows if r.status_code >= 500)
    total_users = sum(r.hit_count for r in rows)

    # Verinin kapsadığı tarih aralığı: çekim anından geriye days gün
    fetched_at = max((r.last_seen for r in rows if r.last_seen), default=None)
    if fetched_at:
        range_end = fetched_at.date()
        range_start = range_end - timedelta(days=days - 1)
        if range_start == range_end:
            fetched_at_str = range_end.strftime("%-d %b %Y")
        elif range_start.year == range_end.year:
            fetched_at_str = f"{range_start.strftime('%-d %b')} – {range_end.strftime('%-d %b %Y')}"
        else:
            fetched_at_str = f"{range_start.strftime('%-d %b %Y')} – {range_end.strftime('%-d %b %Y')}"
    else:
        fetched_at_str = ""

    error_list = []
    for r in rows:
        extra = {}
        if r.extra_json:
            try:
                extra = json.loads(r.extra_json)
            except Exception:
                pass
        error_list.append({
            "id":          r.id,
            "url":         r.url,
            "status_code": r.status_code,
            "source":      r.source,
            "error_type":  r.error_type,
            "hit_count":   r.hit_count,
            "first_seen":  r.first_seen.isoformat() if r.first_seen else "",
            "last_seen":   r.last_seen.isoformat() if r.last_seen else "",
            "page_title":  extra.get("page_title", ""),
            "referrers":   merge_referrer_items(
                normalize_referrers(extra.get("referrers"), site_host)
            ),
            "channels": merge_channel_items(
                normalize_channels(extra.get("channels"))
            ),
            "referrer_unknown_users": int(extra.get("referrer_unknown_users") or 0),
        })

    return {
        "total_404":   total_404,
        "total_5xx":   total_5xx,
        "total_users": total_users,
        "by_source":   {"ga4": total_404 + total_5xx},
        "errors":      error_list,
        "site_id":     site_id,
        "days":        days,
        "fetched_at":  fetched_at_str,
    }


def run_error_detection_for_site(db: Session, site_id: int, days: int = 7) -> dict:
    """Tek site için GA4 hata tespiti çalıştırır ve DB'ye kaydeder."""
    from backend.models import Site
    from backend.services.ga4_auth import (
        get_ga4_credentials_record, load_ga4_properties, ga4_is_configured,
    )

    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        return {"status": "error", "message": "site not found"}

    if not ga4_is_configured():
        return {"status": "skip", "message": "GA4 service account yapılandırılmamış"}

    record = get_ga4_credentials_record(db, site.id)
    if not record:
        return {"status": "skip", "message": "GA4 property kaydı yok"}

    properties = load_ga4_properties(record)
    property_id = properties.get("web") or properties.get("mweb") or next(iter(properties.values()), "")
    if not property_id:
        return {"status": "skip", "message": "web property ID yok"}

    errors = fetch_ga4_error_pages(property_id, days=days, site_domain=site.domain)
    source_key = _ga4_source_key(days)
    saved = save_error_logs(db, site.id, errors, source=source_key)

    logger.info(
        "Hata tespiti: site=%s property=%s days=%d, %d hata bulundu, %d kaydedildi",
        site.domain, property_id, days, len(errors), saved,
    )
    return {
        "status":  "ok",
        "found":   len(errors),
        "saved":   saved,
        "domain":  site.domain,
    }


_GA4_PERIODS = [1, 7, 14, 30]


def run_error_detection_all_sites(db: Session) -> list[dict]:
    """Tüm GA4-bağlı siteler için hata tespiti — 1/7/14/30 günlük periyotları önceden çeker."""
    from backend.models import Site
    from backend.services.ga4_auth import get_ga4_credentials_record, ga4_is_configured

    if not ga4_is_configured():
        logger.warning("GA4 service account yapılandırılmamış — hata tespiti atlanıyor")
        return []

    sites = db.query(Site).all()
    results = []
    for site in sites:
        try:
            record = get_ga4_credentials_record(db, site.id)
            if not record:
                continue
            for days in _GA4_PERIODS:
                try:
                    result = run_error_detection_for_site(db, site.id, days=days)
                    results.append(result)
                except Exception as exc:
                    logger.warning("Hata tespiti başarısız [site=%s days=%d]: %s", site.domain, days, exc)
        except Exception as exc:
            logger.warning("Hata tespiti başarısız [site=%s]: %s", site.domain, exc)
    return results
