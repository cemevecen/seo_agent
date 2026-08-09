"""Crash-free / ANR-free özet — Play scrape oranları + Reporting API + Crashlytics."""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_STABILITY_CACHE_TTL_S = 15 * 60
_STABILITY_CACHE_LOCK = threading.Lock()
_STABILITY_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_STABILITY_BUILD_LOCKS: dict[str, threading.Lock] = {}
_STABILITY_BUILD_LOCKS_GUARD = threading.Lock()


def _stability_build_lock(key: str) -> threading.Lock:
    with _STABILITY_BUILD_LOCKS_GUARD:
        lock = _STABILITY_BUILD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _STABILITY_BUILD_LOCKS[key] = lock
        return lock


def _stability_cache_get(key: str) -> dict[str, Any] | None:
    with _STABILITY_CACHE_LOCK:
        entry = _STABILITY_CACHE.get(key)
        if not entry:
            return None
        ts, payload = entry
        if time.time() - ts > _STABILITY_CACHE_TTL_S:
            return None
        return payload


def _stability_cache_set(key: str, payload: dict[str, Any]) -> None:
    with _STABILITY_CACHE_LOCK:
        _STABILITY_CACHE[key] = (time.time(), payload)
        if len(_STABILITY_CACHE) > 24:
            cutoff = time.time() - _STABILITY_CACHE_TTL_S
            for k, (ts, _) in list(_STABILITY_CACHE.items()):
                if ts < cutoff:
                    del _STABILITY_CACHE[k]


def _parse_tr_pct(raw: Any) -> float | None:
    """'%0,03' / '0.03%' / 0.03 → yüzde puanı (0.03)."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s or s in ("—", "-", "–"):
        return None
    s = s.replace("%", "").replace("\u00a0", " ").strip()
    s = s.replace(",", ".")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _free_from_rate_pct(rate_pct: float | None) -> float | None:
    if rate_pct is None:
        return None
    return round(100.0 - float(rate_pct), 4)


def _fmt_free(pct: float | None, *, digits: int = 2) -> str | None:
    if pct is None:
        return None
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return None
    # 99.995+ için 4 hane — aksi halde 100,00% yanıltır
    if v >= 99.995:
        return f"{v:.4f}".replace(".", ",") + "%"
    return f"{v:.{digits}f}".replace(".", ",") + "%"


def free_rates_from_vitals_overview(vitals: dict[str, Any] | None) -> dict[str, Any]:
    """Scrape metrics_overview satırlarından overall crash/ANR-free."""
    vitals = vitals if isinstance(vitals, dict) else {}
    ov = vitals.get("metrics_overview") if isinstance(vitals.get("metrics_overview"), dict) else {}
    rows = ov.get("rows") if isinstance(ov.get("rows"), list) else []
    crash_rate = None
    anr_rate = None
    crash_label = None
    anr_label = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").lower()
        metric = str(row.get("metric") or "")
        val = _parse_tr_pct(row.get("value_28d"))
        if key == "crash" or re.search(r"kilitlenme|crash", metric, re.I):
            crash_rate = val
            crash_label = metric or "Crash oranı"
        elif key == "anr" or re.search(r"\banr\b", metric, re.I):
            anr_rate = val
            anr_label = metric or "ANR oranı"
    crash_free = _free_from_rate_pct(crash_rate)
    anr_free = _free_from_rate_pct(anr_rate)
    return {
        "source": "play_vitals_overview",
        "period": "28d",
        "crash_rate_pct": crash_rate,
        "anr_rate_pct": anr_rate,
        "crash_free_pct": crash_free,
        "anr_free_pct": anr_free,
        "crash_free_fmt": _fmt_free(crash_free),
        "anr_free_fmt": _fmt_free(anr_free),
        "crash_metric": crash_label,
        "anr_metric": anr_label,
    }


def _cf_block(cf: dict[str, Any] | None, *, version: str | None = None) -> dict[str, Any] | None:
    if not isinstance(cf, dict):
        return None
    pct = cf.get("crash_free_sessions_pct")
    if pct is None:
        pct = cf.get("crash_free_pct")
    if pct is None:
        return None
    try:
        pct_f = float(pct)
    except (TypeError, ValueError):
        return None
    period_days = cf.get("period_days")
    try:
        period_days_i = int(period_days) if period_days is not None else None
    except (TypeError, ValueError):
        period_days_i = None
    period = cf.get("period")
    if not period and period_days_i:
        period = f"{period_days_i}d"
    if not period:
        period = "7d"
    return {
        "version": version or cf.get("version"),
        "crash_free_pct": pct_f,
        "crash_free_sessions_pct": cf.get("crash_free_sessions_pct"),
        "crash_free_users_pct": cf.get("crash_free_users_pct"),
        "crash_free_fmt": _fmt_free(pct_f),
        "method": cf.get("method"),
        "total_sessions": cf.get("total_sessions"),
        "crashed_sessions": cf.get("crashed_sessions"),
        "source": "crashlytics_bq",
        "period": period,
        "period_days": period_days_i,
        "since": cf.get("since"),
    }


def _latest_cf_since_release(
    cbq: Any,
    *,
    product_id: str,
    plat: str,
    version: str,
) -> dict[str, Any] | None:
    """Latest sürüm crash-free: yayın (ilk görülme) → bugün aralığı."""
    from backend.services.app_intel import APP_PRODUCTS

    meta = APP_PRODUCTS.get(product_id) or {}
    bundle = (
        (meta.get("android_package") or "")
        if plat == "android"
        else (meta.get("ios_bundle_id") or "")
    )
    tbl = None
    for p, t in cbq._platforms_for(product_id, plat):
        if p == plat:
            tbl = t
            break
    if not tbl or not bundle or not version:
        return None
    age_days, since_iso = cbq.version_release_span_days(
        plat, tbl, bundle=bundle, version=version
    )
    live = cbq.query_crash_free(plat, tbl, age_days, bundle=bundle, version=version)
    if not live:
        return None
    live = dict(live)
    live["period_days"] = age_days
    live["period"] = f"{age_days}d"
    if since_iso:
        live["since"] = since_iso
    return _cf_block(live, version=version)


def _ios_version_candidates(payload: dict[str, Any], product_id: str) -> list[str]:
    """Son 3 iOS sürümü — Crashlytics filtre listesi, yoksa mağaza yayınları."""
    filter_vers = (payload.get("filter_versions_by_platform") or {}).get("ios") or []
    out: list[str] = []
    for v in filter_vers:
        s = str(v or "").strip()
        if s and s not in out:
            out.append(s)
        if len(out) >= 3:
            return out[:3]
    # versions_by_platform satırları
    for key in ("versions_by_platform", "versions_7d_by_platform"):
        rows = (payload.get(key) or {}).get("ios") or []
        for r in rows:
            if not isinstance(r, dict):
                continue
            s = str(r.get("app_version") or "").strip()
            if s and s not in out:
                out.append(s)
            if len(out) >= 3:
                return out[:3]
    # Mağaza yayın tarihleri (en yeni önce)
    try:
        from backend.services.store_version_releases import fetch_version_releases_for_product

        rel = fetch_version_releases_for_product(product_id) or {}
        ios_rels = list(rel.get("ios") or [])
        ios_rels.sort(key=lambda x: str((x or {}).get("released_at") or ""), reverse=True)
        for row in ios_rels:
            s = str((row or {}).get("version") or "").strip()
            if s and s not in out:
                out.append(s)
            if len(out) >= 3:
                break
    except Exception:
        logger.debug("ios version candidates store releases failed", exc_info=True)
    return out[:3]


def _build_ios_version_rows(
    cbq: Any,
    *,
    product_id: str,
    payload: dict[str, Any],
    versions: list[str],
    force_refresh: bool,
) -> list[dict[str, Any]]:
    """Android vitals chip’leri gibi son 3 iOS sürümü + crash-free."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ver_stats: dict[str, dict[str, Any]] = {}
    for key in ("versions_by_platform", "versions_7d_by_platform"):
        for r in (payload.get(key) or {}).get("ios") or []:
            if not isinstance(r, dict):
                continue
            v = str(r.get("app_version") or "").strip()
            if v and v not in ver_stats:
                ver_stats[v] = r

    latest_stats = payload.get("latest_version_stats_by_platform") or {}
    ios_latest = latest_stats.get("ios") if isinstance(latest_stats.get("ios"), dict) else {}

    def _one(ver: str) -> dict[str, Any]:
        row = ver_stats.get(ver) or {}
        cf = None
        if ios_latest and str(ios_latest.get("version") or "").strip() == ver:
            cf = _cf_block(ios_latest.get("crash_free"), version=ver)
        if force_refresh or not cf:
            try:
                live = _latest_cf_since_release(
                    cbq, product_id=product_id, plat="ios", version=ver
                )
                if live:
                    cf = live
            except Exception as exc:  # noqa: BLE001
                logger.info("ios version CF (%s): %s", ver, exc)
        item: dict[str, Any] = {
            "version": ver,
            "label": f"v{ver}",
            "fatal": int(row.get("fatal_count") or row.get("fatal") or 0) or None,
            "anr": int(row.get("anr_count") or row.get("anr") or 0) or None,
            "total_events": int(row.get("total_events") or 0) or None,
            "affected_users": int(row.get("affected_users") or 0) or None,
        }
        if cf:
            item.update(
                {
                    "crash_free_pct": cf.get("crash_free_pct"),
                    "crash_free_sessions_pct": cf.get("crash_free_sessions_pct"),
                    "crash_free_users_pct": cf.get("crash_free_users_pct"),
                    "crash_free_fmt": cf.get("crash_free_fmt"),
                    "period": cf.get("period"),
                    "period_days": cf.get("period_days"),
                    "since": cf.get("since"),
                    "method": cf.get("method"),
                }
            )
        return item

    rows: list[dict[str, Any]] = []
    if not versions:
        return rows
    with ThreadPoolExecutor(max_workers=min(3, len(versions)), thread_name_prefix="ios-sf-ver") as pool:
        futs = {pool.submit(_one, v): v for v in versions}
        by_ver: dict[str, dict[str, Any]] = {}
        for fut in as_completed(futs):
            ver = futs[fut]
            try:
                by_ver[ver] = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.info("ios version row failed (%s): %s", ver, exc)
                by_ver[ver] = {"version": ver, "label": f"v{ver}"}
    for v in versions:
        if v in by_ver:
            rows.append(by_ver[v])
    return rows


def invalidate_stability_cache(product_id: str | None = None) -> None:
    """Bellek içi stability-free cache temizle (manuel yenile)."""
    with _STABILITY_CACHE_LOCK:
        if not product_id:
            _STABILITY_CACHE.clear()
            return
        pid = (product_id or "").strip().lower()
        for k in list(_STABILITY_CACHE.keys()):
            if k.startswith(f"{pid}:"):
                del _STABILITY_CACHE[k]


def build_stability_free_payload(
    *,
    package_name: str = "com.Doviz",
    product_id: str = "doviz",
    vitals: dict[str, Any] | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Android/iOS crash-free kartları için birleşik payload."""
    vitals = vitals if isinstance(vitals, dict) else {}
    cache_key = f"{product_id}:{package_name}:sf:v2"
    if force_refresh:
        invalidate_stability_cache(product_id)
    else:
        cached = _stability_cache_get(cache_key)
        if cached:
            return cached

    lock = _stability_build_lock(cache_key)
    with lock:
        if not force_refresh:
            cached = _stability_cache_get(cache_key)
            if cached:
                return cached
        return _build_stability_free_payload_locked(
            package_name=package_name,
            product_id=product_id,
            vitals=vitals,
            force_refresh=force_refresh,
            cache_key=cache_key,
        )


def _build_stability_free_payload_locked(
    *,
    package_name: str,
    product_id: str,
    vitals: dict[str, Any],
    force_refresh: bool,
    cache_key: str,
) -> dict[str, Any]:
    """stability-free build — caller holds singleflight lock."""
    from backend.services import crashlytics_bq as cbq
    from backend.services import gp_client

    play_overall = free_rates_from_vitals_overview(vitals)

    versions = vitals.get("versions") if isinstance(vitals.get("versions"), list) else []
    prefer_codes: list[str] = []
    latest_name = None
    for v in versions:
        if isinstance(v, dict) and v.get("code"):
            prefer_codes.append(str(v["code"]))
            if latest_name is None and v.get("name"):
                latest_name = str(v["name"])
    name_map = vitals.get("version_name_map") if isinstance(vitals.get("version_name_map"), dict) else {}
    if not prefer_codes and name_map:
        prefer_codes = sorted(
            (str(k) for k in name_map.keys() if str(k).isdigit()),
            key=lambda x: int(x),
            reverse=True,
        )

    play_latest = None
    play_versions: list[dict[str, Any]] = []
    play_err = None
    try:
        if gp_client.is_configured():
            rep = gp_client.fetch_version_stability_free(
                package_name,
                days=28,
                prefer_codes=prefer_codes[:5] or None,
            )
            play_err = rep.get("error")
            for row in rep.get("versions") or []:
                if not isinstance(row, dict):
                    continue
                code = str(row.get("version_code") or "")
                vname = row.get("version_name") or name_map.get(code) or latest_name
                item = {
                    **row,
                    "version_name": vname,
                    "crash_free_fmt": _fmt_free(row.get("crash_free_pct")),
                    "anr_free_fmt": _fmt_free(row.get("anr_free_pct")),
                    "label": f"v{vname}" if vname else f"code {code}",
                }
                play_versions.append(item)
            play_latest = play_versions[0] if play_versions else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("stability-free reporting: %s", exc)
        play_err = str(exc)[:160]

    crashlytics: dict[str, Any] = {"ok": False, "platforms": {}}
    try:
        payload = None
        if force_refresh:
            # Manuel yenile: BQ cache temizle + senkron rebuild
            try:
                cbq.invalidate_product_cache(product_id)
            except Exception:
                logger.debug("stability-free BQ cache clear failed", exc_info=True)
            try:
                payload = cbq.build_full_payload(
                    product_id, days=7, platform_filter="all"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("stability-free force BQ rebuild: %s", exc)
                payload = cbq.peek_cached_payload(product_id, days=7, platform_filter="all")
        else:
            payload = cbq.peek_cached_payload(product_id, days=7, platform_filter="all")
            if not payload:
                # Soğuk cache — senkron çekme pahalı; yalnız peek. İstersen prewarm.
                try:
                    cbq.prewarm_cache(product_id)
                except Exception:
                    pass
                payload = cbq.peek_cached_payload(product_id, days=7, platform_filter="all")
        if payload and payload.get("ok") is not False:
            cf_by = payload.get("crash_free_by_platform") or {}
            latest_stats = payload.get("latest_version_stats_by_platform") or {}
            plats: dict[str, Any] = {}
            for plat in ("android", "ios"):
                overall = _cf_block(cf_by.get(plat))
                scoped = latest_stats.get(plat) if isinstance(latest_stats.get(plat), dict) else {}
                ver = str((scoped or {}).get("version") or "").strip() or None
                latest_cf = None
                if ver:
                    latest_cf = _cf_block((scoped or {}).get("crash_free"), version=ver)
                # force veya cache boşsa since-release canlı CF
                if ver and (force_refresh or not latest_cf):
                    try:
                        live_cf = _latest_cf_since_release(
                            cbq, product_id=product_id, plat=plat, version=ver
                        )
                        if live_cf:
                            latest_cf = live_cf
                    except Exception as exc:  # noqa: BLE001
                        logger.info(
                            "stability-free since-release CF (%s/%s): %s", plat, ver, exc
                        )
                plats[plat] = {
                    "overall": overall,
                    "latest_version": ver,
                    "latest": latest_cf,
                    "fatal": (scoped or {}).get("fatal"),
                    "anr": (scoped or {}).get("anr"),
                }
                if plat == "ios":
                    try:
                        candidates = _ios_version_candidates(payload, product_id)
                        # latest yoksa listedeki ilkini latest yap
                        if not ver and candidates:
                            ver = candidates[0]
                            plats[plat]["latest_version"] = ver
                        ios_versions = _build_ios_version_rows(
                            cbq,
                            product_id=product_id,
                            payload=payload,
                            versions=candidates,
                            force_refresh=force_refresh,
                        )
                        plats[plat]["versions"] = ios_versions
                        # latest CF: chip listesindeki ilk sürümle hizala
                        if ios_versions:
                            top = ios_versions[0]
                            if top.get("crash_free_fmt") and (
                                force_refresh
                                or not latest_cf
                                or str(plats[plat].get("latest_version") or "")
                                == str(top.get("version") or "")
                            ):
                                if str(plats[plat].get("latest_version") or "") == str(
                                    top.get("version") or ""
                                ) or not latest_cf:
                                    plats[plat]["latest"] = _cf_block(top, version=top.get("version"))
                                    if top.get("fatal") is not None:
                                        plats[plat]["fatal"] = top.get("fatal")
                                    if top.get("anr") is not None:
                                        plats[plat]["anr"] = top.get("anr")
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("stability-free ios versions: %s", exc)
                        plats[plat]["versions"] = []
            crashlytics = {
                "ok": True,
                "days": payload.get("days") or 7,
                "fetched_at": payload.get("fetched_at"),
                "platforms": plats,
            }
        else:
            # BQ soğuk — en azından son 3 iOS sürüm adını mağazadan ver (chip listesi)
            try:
                candidates = _ios_version_candidates({}, product_id)
                if candidates:
                    crashlytics = {
                        "ok": True,
                        "days": 7,
                        "platforms": {
                            "ios": {
                                "overall": None,
                                "latest_version": candidates[0],
                                "latest": None,
                                "versions": [
                                    {"version": v, "label": f"v{v}"} for v in candidates
                                ],
                            }
                        },
                    }
            except Exception:
                logger.debug("stability-free ios cold versions failed", exc_info=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("stability-free crashlytics: %s", exc)
        crashlytics = {"ok": False, "error": str(exc)[:160], "platforms": {}}

    out = {
        "ok": True,
        "package_name": package_name,
        "product_id": product_id,
        "play_overall": play_overall,
        "play_latest": play_latest,
        "play_versions": play_versions[:8],
        "play_error": play_err,
        "crashlytics": crashlytics,
    }
    # Vitals’lı Android çağrıları da cache’e yazılsın (sonraki iOS/tekrar açılış)
    try:
        _stability_cache_set(cache_key, out)
    except Exception:
        pass
    return out
