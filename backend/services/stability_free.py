"""Crash-free / ANR-free özet.

Android ANR: Play vitals / Reporting.
Crash-free (son sürüm 24h + 7d): yalnızca S-Firebase Console scrape —
Play Reporting API ve BigQuery crash-free çekimi yok.
"""

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
    if v >= 99.995:
        return f"{v:.4f}".replace(".", ",") + "%"
    return f"{v:.{digits}f}".replace(".", ",") + "%"


def _fmt_rate_pct(rate: float | None) -> str | None:
    if rate is None:
        return None
    try:
        v = float(rate)
    except (TypeError, ValueError):
        return None
    if abs(v) < 0.1:
        return f"{v:.3f}".replace(".", ",") + "%"
    return f"{v:.2f}".replace(".", ",") + "%"


def _fmt_compact_n(n: Any) -> str | None:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return None
    if v < 0:
        return None
    if v >= 1_000_000:
        s = f"{v / 1_000_000:.1f}".replace(".", ",").rstrip("0").rstrip(",")
        return f"{s}M"
    if v >= 1_000:
        s = f"{v / 1_000:.1f}".replace(".", ",").rstrip("0").rstrip(",")
        return f"{s}k"
    return str(int(round(v)))


def _fmt_delta_pp(delta: float | None) -> str | None:
    if delta is None:
        return None
    try:
        v = float(delta)
    except (TypeError, ValueError):
        return None
    if abs(v) < 1e-9:
        return "Δ0"
    sign = "+" if v > 0 else ""
    if abs(v) < 0.01:
        return f"Δ{sign}{v:.3f}".replace(".", ",")
    if abs(v) < 1:
        return f"Δ{sign}{v:.2f}".replace(".", ",")
    return f"Δ{sign}{v:.1f}".replace(".", ",")


def _compact_extra(*bits: str | None) -> str:
    return " · ".join(b for b in bits if b)


def _series_users_and_delta(series: Any) -> tuple[int | None, float | None]:
    """Günlük seriden örneklem (users toplamı) + dönem Δ (son − ilk gün, pp)."""
    if not isinstance(series, list) or not series:
        return None, None
    rows: list[tuple[str, float, float]] = []
    for row in series:
        if not isinstance(row, dict):
            continue
        pct = row.get("crash_free_pct")
        if not isinstance(pct, (int, float)):
            continue
        day = str(row.get("date") or "")
        users = row.get("users")
        try:
            u = float(users) if users is not None else 0.0
        except (TypeError, ValueError):
            u = 0.0
        rows.append((day, float(pct), u))
    if not rows:
        return None, None
    rows.sort(key=lambda x: x[0])
    users_sum = int(round(sum(u for _, _, u in rows))) or None
    delta = None
    if len(rows) >= 2:
        delta = round(rows[-1][1] - rows[0][1], 4)
    return users_sum, delta


def _fb_window_kpi(
    win: dict[str, Any] | None, *, period: str, version: str | None = None
) -> dict[str, Any] | None:
    if not isinstance(win, dict):
        return None
    pct = win.get("crash_free_pct")
    fmt = win.get("crash_free_fmt") or _fmt_free(
        pct if isinstance(pct, (int, float)) else None
    )
    if fmt is None and pct is None:
        return None
    ver = version or win.get("version")
    sess_pct = win.get("crash_free_sessions_pct")
    sess_fmt = win.get("crash_free_sessions_fmt") or _fmt_free(
        sess_pct if isinstance(sess_pct, (int, float)) else None
    )
    users_sum, delta = _series_users_and_delta(win.get("series"))
    if users_sum is None:
        users_sum, _ = _series_users_and_delta(win.get("sessions_series"))
    sess_bit = None
    if sess_fmt and (
        not isinstance(pct, (int, float))
        or not isinstance(sess_pct, (int, float))
        or abs(float(sess_pct) - float(pct)) >= 0.00005
    ):
        sess_bit = f"s {sess_fmt}"
    extra = _compact_extra(
        sess_bit,
        _fmt_delta_pp(delta),
        _fmt_compact_n(users_sum),
    )
    return {
        "version": ver,
        "crash_free_pct": pct,
        "crash_free_fmt": fmt,
        "crash_free_sessions_pct": sess_pct,
        "crash_free_sessions_fmt": sess_fmt,
        "users": users_sum,
        "users_fmt": _fmt_compact_n(users_sum),
        "delta_pp": delta,
        "delta_fmt": _fmt_delta_pp(delta),
        "extra": extra or None,
        "period": period,
        "label": f"v{ver}" if ver and not str(ver).startswith("v") else ver,
        "source": "firebase_console_scrape",
        "method": "firebase_console_scrape",
    }


def firebase_console_stability_kpis() -> dict[str, Any]:
    """S-Firebase scrape → son sürüm 24h / 7d crash-free KPI'ları (tek kaynak)."""
    try:
        from backend.database import SessionLocal
        from backend.services.firebase_console_store import firebase_console_payload

        with SessionLocal() as db:
            snap = firebase_console_payload(db)
    except Exception:
        logger.debug("firebase console stability read failed", exc_info=True)
        return {"ok": False, "platforms": {}}

    platforms_in = snap.get("platforms") if isinstance(snap.get("platforms"), dict) else {}
    out_plats: dict[str, Any] = {}
    for plat in ("android", "ios"):
        block = platforms_in.get(plat) if isinstance(platforms_in.get(plat), dict) else {}
        if not block:
            continue
        windows = block.get("windows") if isinstance(block.get("windows"), dict) else {}
        ver = str(block.get("latest_version") or "").strip() or None
        w24 = (
            block.get("latest_24h")
            if isinstance(block.get("latest_24h"), dict)
            else windows.get("24h")
        )
        w7 = (
            block.get("latest_7d")
            if isinstance(block.get("latest_7d"), dict)
            else windows.get("7d")
        )
        w30 = windows.get("30d") if isinstance(windows.get("30d"), dict) else None
        kpi24 = _fb_window_kpi(w24 if isinstance(w24, dict) else None, period="24h", version=ver)
        kpi7 = _fb_window_kpi(w7 if isinstance(w7, dict) else None, period="7d", version=ver)
        kpi30 = _fb_window_kpi(w30, period="30d", version=ver)
        out_plats[plat] = {
            "latest_version": ver,
            "latest_24h": kpi24,
            "latest_7d": kpi7,
            "latest_30d": kpi30,
            "latest": kpi7 or kpi24,
            "anr_issues_count": len(block.get("anr_issues") or []) if plat == "android" else None,
            "issues_count": len(block.get("issues") or []),
        }
    return {
        "ok": bool(out_plats) and not snap.get("empty"),
        "updated_at": snap.get("updated_at") or snap.get("background_synced_at"),
        "sync_ok": snap.get("sync_ok"),
        "source": "firebase_console_scrape",
        "platforms": out_plats,
    }


def free_rates_from_vitals_overview(vitals: dict[str, Any] | None) -> dict[str, Any]:
    """Play vitals overview — ANR-free birincil; CF alanı yedek/debug (UI S-Firebase)."""
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
    anr_rate_fmt = _fmt_rate_pct(anr_rate)
    return {
        "source": "play_vitals_overview",
        "period": "28d",
        "crash_rate_pct": crash_rate,
        "anr_rate_pct": anr_rate,
        "anr_rate_fmt": anr_rate_fmt,
        "crash_free_pct": crash_free,
        "anr_free_pct": anr_free,
        "crash_free_fmt": _fmt_free(crash_free),
        "anr_free_fmt": _fmt_free(anr_free),
        "crash_metric": crash_label,
        "anr_metric": anr_label,
        "extra": _compact_extra(anr_rate_fmt) or None,
    }


def _ios_versions_from_asc_scrape(product_id: str) -> list[dict[str, Any]]:
    """ASC / mağaza — yalnızca sürüm chip listesi (crash-free BQ yok)."""
    out: list[dict[str, Any]] = []
    try:
        from backend.services.store_version_releases import fetch_version_releases_for_product

        rel = fetch_version_releases_for_product(product_id) or {}
        ios_rels = list(rel.get("ios") or [])
        ios_rels.sort(key=lambda x: str((x or {}).get("released_at") or ""), reverse=True)
        for row in ios_rels[:3]:
            ver = str((row or {}).get("version") or "").strip()
            if not ver:
                continue
            out.append({"version": ver, "label": f"v{ver}", "source": "asc_scrape"})
    except Exception:
        logger.debug("ios ASC version candidates failed", exc_info=True)
    return out


def _strip_play_latest_crash_free(play_latest: dict[str, Any] | None) -> dict[str, Any] | None:
    """Play Reporting crash-free'i düşür — CF yalnızca S-Firebase."""
    if not isinstance(play_latest, dict):
        return play_latest
    out = dict(play_latest)
    out.pop("crash_free_pct", None)
    out.pop("crash_free_fmt", None)
    out["crash_free_source"] = "disabled_use_firebase_console"
    return out


def build_stability_free_payload(
    *,
    package_name: str = "com.Doviz",
    product_id: str = "doviz",
    vitals: dict[str, Any] | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Android/iOS stability kartları — CF: S-Firebase; ANR: Play."""
    vitals = vitals if isinstance(vitals, dict) else {}
    cache_key = f"{product_id}:{package_name}:sf:v4-firebase-cf-extra"
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
    play_overall = free_rates_from_vitals_overview(vitals)
    # CF UI yalnızca S-Firebase — Play vitals crash-free alanını düşür (ANR kalsın)
    play_overall = {
        **play_overall,
        "crash_free_pct": None,
        "crash_free_fmt": None,
        "crash_free_source": "disabled_use_firebase_console",
    }

    play_latest = None
    play_versions: list[dict[str, Any]] = []
    play_err = None

    fb = firebase_console_stability_kpis()
    fb_plats = fb.get("platforms") if isinstance(fb.get("platforms"), dict) else {}

    plats: dict[str, Any] = {}
    for plat in ("android", "ios"):
        fp = fb_plats.get(plat) if isinstance(fb_plats.get(plat), dict) else {}
        ver = fp.get("latest_version")
        kpi7 = fp.get("latest_7d")
        kpi24 = fp.get("latest_24h")
        latest = fp.get("latest") or kpi7 or kpi24
        versions_out: list[dict[str, Any]] = []
        if plat == "ios":
            scrape_rows = _ios_versions_from_asc_scrape(product_id)
            for row in scrape_rows[:3]:
                item = dict(row)
                row_ver = str(item.get("version") or "")
                if ver and row_ver and row_ver in str(ver):
                    if isinstance(kpi7, dict) and kpi7.get("crash_free_fmt"):
                        item.update(
                            {
                                "crash_free_fmt": kpi7.get("crash_free_fmt"),
                                "crash_free_pct": kpi7.get("crash_free_pct"),
                                "period": "7d",
                                "source": "firebase_console_scrape",
                                "method": "firebase_console_scrape",
                            }
                        )
                versions_out.append(item)
            if not versions_out and ver:
                base: dict[str, Any] = {
                    "version": str(ver).split()[0],
                    "label": f"v{str(ver).split()[0]}",
                    "source": "firebase_console_scrape",
                }
                if isinstance(kpi7, dict):
                    base.update(
                        {
                            "crash_free_fmt": kpi7.get("crash_free_fmt"),
                            "crash_free_pct": kpi7.get("crash_free_pct"),
                            "period": "7d",
                        }
                    )
                versions_out = [base]
        plats[plat] = {
            "overall": None,
            "latest_version": ver,
            "latest": latest,
            "latest_24h": kpi24,
            "latest_7d": kpi7,
            "versions": versions_out,
            "source": "firebase_console_scrape",
        }

    crashlytics = {
        "ok": bool(fb.get("ok")),
        "days": 7,
        "source": "firebase_console_scrape",
        "fetched_at": fb.get("updated_at"),
        "platforms": plats,
        "bq_disabled_for_crash_free": True,
    }

    out = {
        "ok": True,
        "package_name": package_name,
        "product_id": product_id,
        "play_overall": play_overall,
        "play_latest": play_latest,
        "play_versions": play_versions[:8],
        "play_error": play_err,
        "crashlytics": crashlytics,
        "firebase_console": fb,
        "crash_free_source": "firebase_console_scrape",
    }
    try:
        _stability_cache_set(cache_key, out)
    except Exception:
        pass
    return out
