"""
Mağaza sürüm yayın tarihleri — grafik işaretleri ve özet listesi.

- iOS: App Store Connect appStoreVersions (earliestReleaseDate / createdDate)
- Android: Play yorumlarında sürümün ilk görüldüğü tarih (Play API tarih vermez)
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Any

from backend.services.app_release_sheet import fetch_releases_from_sheet
from backend.services import asc_client
from backend.services.app_intel import APP_PRODUCTS, get_raw_product_data
from backend.services.asc_analytics import _paginate_first_path

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 30 * 60

_DEFAULT_SINCE = date(2025, 1, 1)

_IOS_HISTORY_STATES = frozenset(
    {
        "READY_FOR_SALE",
        "REPLACED_WITH_NEW_VERSION",
        "REMOVED_FROM_SALE",
    }
)


def _utc_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _releases_from_reviews(reviews: list[dict[str, Any]], *, since: datetime) -> list[dict[str, Any]]:
    first: dict[str, datetime] = {}
    for r in reviews or []:
        ver = str(r.get("version") or "").strip()
        at = _utc_dt(r.get("at"))
        if not ver or at is None:
            continue
        if at < since:
            continue
        prev = first.get(ver)
        if prev is None or at < prev:
            first[ver] = at
    out = [
        {
            "version": ver,
            "released_at": _iso(at),
            "source": "reviews_first_seen",
        }
        for ver, at in first.items()
    ]
    out.sort(key=lambda x: x.get("released_at") or "")
    return out


def fetch_ios_version_releases(*, bundle_id: str, since: date) -> list[dict[str, Any]]:
    if not bundle_id or not asc_client.is_configured():
        return []

    since_dt = datetime(since.year, since.month, since.day, tzinfo=timezone.utc)
    app_id = asc_client.find_app_id_by_bundle(bundle_id)
    if not app_id:
        return []

    try:
        versions = _paginate_first_path(
            f"/v1/apps/{app_id}/appStoreVersions",
            {"limit": 50, "filter[platform]": "IOS"},
        )
    except Exception as exc:
        logger.warning("ASC version list failed (%s): %s", bundle_id, exc)
        return []

    by_ver: dict[str, dict[str, Any]] = {}
    for v in versions or []:
        attr = v.get("attributes") or {}
        state = (attr.get("appStoreState") or "").upper()
        if state and state not in _IOS_HISTORY_STATES:
            continue
        ver = (attr.get("versionString") or "").strip()
        if not ver:
            continue
        rd = _utc_dt(attr.get("earliestReleaseDate")) or _utc_dt(attr.get("createdDate"))
        if rd is None or rd < since_dt:
            continue
        rec = by_ver.get(ver)
        prev_at = _utc_dt(rec.get("released_at")) if rec else None
        if rec is None or (prev_at is not None and rd < prev_at):
            by_ver[ver] = {
                "version": ver,
                "released_at": _iso(rd),
                "source": "app_store_connect",
                "app_store_state": state or None,
            }

    out = list(by_ver.values())
    out.sort(key=lambda x: x.get("released_at") or "")
    return out


def fetch_version_releases_for_product(
    product_id: str,
    *,
    since: date | None = None,
    use_cache: bool = True,
) -> dict[str, Any]:
    pid = (product_id or "doviz").strip().lower()
    since_d = since or _DEFAULT_SINCE
    cache_key = f"vrel:{pid}:{since_d.isoformat()}"
    if use_cache:
        hit = _CACHE.get(cache_key)
        if hit and time.time() - hit[0] <= _CACHE_TTL:
            return hit[1]

    if pid not in APP_PRODUCTS:
        payload = {"error": "unknown_product", "product": pid}
        return payload

    product_meta = APP_PRODUCTS[pid]
    bundle = (product_meta.get("ios_bundle_id") or "").strip()
    since_dt = datetime(since_d.year, since_d.month, since_d.day, tzinfo=timezone.utc)

    sheet_ios, sheet_android = fetch_releases_from_sheet(pid, since=since_d, use_cache=use_cache)
    if sheet_ios or sheet_android:
        ios = sheet_ios
        android = sheet_android
        note_tr = "Kaynak: Google Sheets «sürüm güncellemeler» tablosu (resmi yayın tarihleri)."
    else:
        ios: list[dict[str, Any]] = []
        if bundle:
            ios = fetch_ios_version_releases(bundle_id=bundle, since=since_d)

        raw = get_raw_product_data(pid, force_refresh=False, cache_only=True)
        if raw.get("error"):
            raw = get_raw_product_data(pid, force_refresh=False, cache_only=False)

        android: list[dict[str, Any]] = []
        ios_fallback: list[dict[str, Any]] = []
        if not raw.get("error"):
            android = _releases_from_reviews(
                list((raw.get("android") or {}).get("reviews") or []),
                since=since_dt,
            )
            if not ios:
                ios_fallback = _releases_from_reviews(
                    list((raw.get("ios") or {}).get("reviews") or []),
                    since=since_dt,
                )
                for row in ios_fallback:
                    row["source"] = "reviews_first_seen"

        if not ios:
            ios = ios_fallback

        if not ios and not raw.get("error"):
            ios_meta = (raw.get("ios") or {}).get("meta")
            if isinstance(ios_meta, dict):
                ver = str(ios_meta.get("version") or "").strip()
                rd = _utc_dt(ios_meta.get("currentVersionReleaseDate"))
                if ver and rd and rd >= since_dt:
                    ios = [
                        {
                            "version": ver,
                            "released_at": _iso(rd),
                            "source": "itunes_lookup_current",
                        }
                    ]

        note_tr = (
            "iOS tarihleri App Store Connect sürüm kaydından; "
            "Android tarihleri mağaza yorumlarında sürümün ilk geçtiği güne göre tahmindir."
        )

    payload = {
        "product": pid,
        "product_label": product_meta.get("label") or pid,
        "since": since_d.isoformat(),
        "ios": ios,
        "android": android,
        "counts": {"ios": len(ios), "android": len(android)},
        "note_tr": note_tr,
    }
    _CACHE[cache_key] = (time.time(), payload)
    return payload
