"""Crash-free / ANR-free özet — Play scrape oranları + Reporting API + Crashlytics."""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


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
        "source": "play_scrape_vitals_overview",
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
        "period": "7d",
    }


def build_stability_free_payload(
    *,
    package_name: str = "com.Doviz",
    product_id: str = "doviz",
    vitals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Android/iOS crash-free kartları için birleşik payload."""
    from backend.services import crashlytics_bq as cbq
    from backend.services import gp_client

    vitals = vitals if isinstance(vitals, dict) else {}
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
                latest_cf = _cf_block((scoped or {}).get("crash_free"), version=ver)
                # Cache'de sürüm CF yoksa (eski cache) — canlı tek sorgu dene
                if ver and not latest_cf:
                    try:
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
                        if tbl and bundle:
                            live = cbq.query_crash_free(plat, tbl, 7, bundle=bundle, version=ver)
                            latest_cf = _cf_block(live, version=ver)
                    except Exception as exc:  # noqa: BLE001
                        logger.info("stability-free version CF (%s/%s): %s", plat, ver, exc)
                plats[plat] = {
                    "overall": overall,
                    "latest_version": ver,
                    "latest": latest_cf,
                    "fatal": (scoped or {}).get("fatal"),
                    "anr": (scoped or {}).get("anr"),
                }
            crashlytics = {
                "ok": True,
                "days": payload.get("days") or 7,
                "fetched_at": payload.get("fetched_at"),
                "platforms": plats,
            }
    except Exception as exc:  # noqa: BLE001
        logger.warning("stability-free crashlytics: %s", exc)
        crashlytics = {"ok": False, "error": str(exc)[:160], "platforms": {}}

    return {
        "ok": True,
        "package_name": package_name,
        "product_id": product_id,
        "play_overall": play_overall,
        "play_latest": play_latest,
        "play_versions": play_versions[:8],
        "play_error": play_err,
        "crashlytics": crashlytics,
    }
