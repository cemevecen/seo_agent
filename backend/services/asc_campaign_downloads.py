"""App Store Connect — kampanya bazlı Total Downloads (Analytics Reports CSV)."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date
from typing import Any

from backend.services import asc_analytics, asc_client

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 6 * 3600

# ASC UI’deki banner kampanya filtreleri ile uyumlu (alt string eşleşmesi).
DOVIZ_BANNER_CAMPAIGN_PATTERNS: tuple[str, ...] = (
    "app_download_currency_detail",
    "mdoviz_app_download_banner",
    "mdoviz app download banner",
    "mdoviz_app_download_banner_currency_detail",
    "mdoviz%20app%20download%20banner",
)


def _cache_get(key: str) -> dict[str, Any] | None:
    hit = _CACHE.get(key)
    if not hit:
        return None
    ts, data = hit
    if time.time() - ts > _CACHE_TTL:
        return None
    return data


def _cache_set(key: str, data: dict[str, Any]) -> None:
    _CACHE[key] = (time.time(), data)


def _norm_campaign(s: str) -> str:
    return (s or "").strip().lower().replace("%20", " ")


def campaign_name_matches(name: str, patterns: tuple[str, ...]) -> bool:
    n = _norm_campaign(name)
    if not n:
        return False
    for raw in patterns:
        p = _norm_campaign(raw)
        if not p:
            continue
        if p in n or n in p:
            return True
    return False


def _pick_campaign_column(headers: list[str]) -> str | None:
    return asc_analytics._pick_column(
        headers,
        "Campaign",
        "Campaign Name",
        "App Store Campaign",
        "Campaign ID",
        "Campaign Id",
    )


def _pick_downloads_column(headers: list[str]) -> str | None:
    return asc_analytics._pick_column(
        headers,
        "Total Downloads",
        "Downloads",
        "First-Time Downloads",
        "First Time Downloads",
    )


def _find_campaign_report_ids(reports: list[dict]) -> list[str]:
    ids: list[str] = []
    for r in reports:
        name = ((r.get("attributes") or {}).get("name") or "").lower()
        rid = r.get("id")
        if not rid:
            continue
        if "campaign" not in name:
            continue
        if any(k in name for k in ("download", "acquisition", "install", "detailed", "performance")):
            ids.append(rid)
    if ids:
        return ids
    for r in reports:
        name = ((r.get("attributes") or {}).get("name") or "").lower()
        if "campaign" in name and r.get("id"):
            ids.append(r["id"])
    return ids


def _parse_campaign_rows(
    rows: list[dict[str, str]],
    *,
    start: date,
    end: date,
    patterns: tuple[str, ...],
) -> tuple[dict[str, float], dict[str, dict[str, float]]]:
    """Günlük toplam (eşleşen kampanyalar) + kampanya→gün→indirme."""
    if not rows:
        return {}, {}
    headers = list(rows[0].keys())
    camp_col = _pick_campaign_column(headers)
    dl_col = _pick_downloads_column(headers)
    if not camp_col or not dl_col:
        return {}, {}

    total_by_date: dict[str, float] = defaultdict(float)
    by_camp_date: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for row in rows:
        camp = (row.get(camp_col) or "").strip()
        if not campaign_name_matches(camp, patterns):
            continue
        ds = asc_analytics._row_date(row, headers)
        if not ds:
            continue
        try:
            d = date.fromisoformat(ds[:10])
        except ValueError:
            continue
        if d < start or d > end:
            continue
        val = float(asc_analytics._parse_int(row.get(dl_col)))
        key = d.isoformat()
        total_by_date[key] += val
        by_camp_date[camp][key] += val

    return dict(total_by_date), {k: dict(v) for k, v in by_camp_date.items()}


def fetch_banner_campaign_downloads(
    *,
    bundle_id: str,
    start: date,
    end: date,
    campaign_patterns: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """
    ASC Analytics Reports — seçili banner kampanyalarına Total Downloads (günlük).
    App Store Connect UI kampanya filtresine en yakın API karşılığı.
    """
    patterns = campaign_patterns or DOVIZ_BANNER_CAMPAIGN_PATTERNS
    cache_key = f"asc_camp_dl:{bundle_id}:{start}:{end}:{hash(patterns)}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    if not asc_client.is_configured():
        out = {"ok": False, "message": "App Store Connect API yapılandırılmamış."}
        _cache_set(cache_key, out)
        return out

    app_id = asc_client.find_app_id_by_bundle(bundle_id)
    if not app_id:
        out = {"ok": False, "message": "Bundle App Store Connect’te bulunamadı."}
        _cache_set(cache_key, out)
        return out

    request_id = asc_analytics._ensure_ongoing_request(app_id)
    if not request_id:
        out = {
            "ok": False,
            "message": "Analytics ONGOING request yok (Admin / Sales and Reports rolü).",
        }
        _cache_set(cache_key, out)
        return out

    reports = asc_analytics._reports_for_request(request_id)
    report_ids = _find_campaign_report_ids(reports)
    if not report_ids:
        names = [((r.get("attributes") or {}).get("name") or "") for r in reports[:30]]
        out = {
            "ok": False,
            "message": "Kampanya indirme raporu listede yok.",
            "report_names_sample": names,
        }
        _cache_set(cache_key, out)
        return out

    total_by_date: dict[str, float] = defaultdict(float)
    by_camp_date: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    used_reports: list[str] = []

    for rid in report_ids[:5]:
        rname = ""
        for r in reports:
            if r.get("id") == rid:
                rname = (r.get("attributes") or {}).get("name") or rid
                break
        for inst_id in asc_analytics._latest_instance_ids(rid, max_instances=2):
            rows = asc_analytics._download_segment_rows(inst_id)
            if not rows:
                continue
            part_total, part_camp = _parse_campaign_rows(
                rows, start=start, end=end, patterns=patterns
            )
            if not part_total and not part_camp:
                continue
            used_reports.append(rname or rid)
            for d, v in part_total.items():
                total_by_date[d] += v
            for camp, days in part_camp.items():
                for d, v in days.items():
                    by_camp_date[camp][d] += v

    if not total_by_date:
        out = {
            "ok": False,
            "message": "Kampanya raporu var ancak seçili filtrelerle satır bulunamadı.",
            "campaign_patterns": list(patterns),
            "reports_tried": used_reports,
        }
        _cache_set(cache_key, out)
        return out

    from backend.services.ga4_app_attribution import _calendar_dates, _series_from_buckets

    campaigns_out: list[dict[str, Any]] = []
    for camp, day_map in sorted(
        by_camp_date.items(),
        key=lambda x: sum(x[1].values()),
        reverse=True,
    ):
        campaigns_out.append(
            {
                "campaign": camp,
                "total": int(round(sum(day_map.values()))),
                "daily": _series_from_buckets(day_map, start=start, end=end),
            }
        )

    out = {
        "ok": True,
        "source": "app_store_connect_campaigns",
        "note": "Total Downloads — ASC kampanya adı filtresi (UI’deki Campaign seçimi ile aynı mantık).",
        "campaign_patterns": list(patterns),
        "reports_used": list(dict.fromkeys(used_reports)),
        "combined_daily": _series_from_buckets(dict(total_by_date), start=start, end=end),
        "campaigns": campaigns_out,
    }
    _cache_set(cache_key, out)
    return out
