"""Firebase sekmesi — /android + /ios scrape / stability-free kaynakları.

BigQuery Crashlytics tam export yerine Play Console vitals scrape +
Reporting stability-free (+ iOS crash-free peek) kullanır; mevcut
Crashlytics HTMX şablonlarının beklediği payload şeklini üretir.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_TTL_S = 5 * 60
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def invalidate_firebase_store_cache(product_id: str | None = None) -> None:
    pid = (product_id or "").strip().lower()
    with _CACHE_LOCK:
        if not pid:
            _CACHE.clear()
            return
        for k in list(_CACHE):
            if k.startswith(f"{pid}:"):
                del _CACHE[k]


def _cache_get(key: str) -> dict[str, Any] | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        ts, payload = entry
        if time.time() - ts > _CACHE_TTL_S:
            return None
        return payload


def _cache_set(key: str, payload: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), payload)
        if len(_CACHE) > 32:
            cutoff = time.time() - _CACHE_TTL_S
            for k, (ts, _) in list(_CACHE.items()):
                if ts < cutoff:
                    del _CACHE[k]


def _parse_count(raw: Any) -> int:
    """'1.234' / '12,5 B' / '1.2K' → int (yaklaşık)."""
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return max(0, int(raw))
    s = str(raw).strip().replace("\u00a0", " ")
    if not s or s in ("—", "-", "–", "N/A"):
        return 0
    s_up = s.upper().replace(" ", "")
    mult = 1.0
    if s_up.endswith("B") or "BİN" in s.upper() or s_up.endswith("K"):
        mult = 1_000.0
        s_up = re.sub(r"(BİN|B|K)$", "", s_up, flags=re.I)
    elif s_up.endswith("M") or "MİLYON" in s.upper():
        mult = 1_000_000.0
        s_up = re.sub(r"(MİLYON|M)$", "", s_up, flags=re.I)
    s_up = s_up.replace("%", "")
    # TR: 1.234.567 or 1.234,5
    if re.search(r"\d+\.\d{3}", s_up) and "," not in s_up:
        s_up = s_up.replace(".", "")
    else:
        s_up = s_up.replace(".", "").replace(",", ".") if "," in s_up else s_up.replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s_up)
    if not m:
        return 0
    try:
        return max(0, int(float(m.group(0)) * mult))
    except ValueError:
        return 0


def _flatten_vitals_issues(
    crashes: dict[str, Any] | None,
    error_type: str,
    *,
    platform: str = "android",
    version_label: str | None = None,
) -> list[dict[str, Any]]:
    """Play vitals CRASH/ANR kategori issue listelerini Crashlytics satırına çevir."""
    crashes = crashes if isinstance(crashes, dict) else {}
    block = crashes.get(error_type) if isinstance(crashes.get(error_type), dict) else {}
    cats = block.get("categories") if isinstance(block.get("categories"), list) else []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    mapped_et = "FATAL" if error_type == "CRASH" else "ANR"
    for cat in cats:
        if not isinstance(cat, dict):
            continue
        for iss in cat.get("issues") or []:
            if not isinstance(iss, dict):
                continue
            iid = str(iss.get("issue_id") or "").strip()
            title = str(iss.get("title") or "").strip() or (f"Issue {iid[:12]}" if iid else "")
            if not title and not iid:
                continue
            key = iid or title
            if key in seen:
                continue
            seen.add(key)
            events = _parse_count(iss.get("events"))
            users = _parse_count(iss.get("users"))
            ver = (
                str(iss.get("affected_versions") or "").strip()
                or version_label
                or "—"
            )
            out.append(
                {
                    "issue_id": iid or key[:64],
                    "issue_title": title[:240],
                    "error_type": mapped_et,
                    "event_count": events,
                    "affected_users": users,
                    "latest_version": ver[:80],
                    "platform": platform,
                    "detail_url": str(iss.get("detail_url") or "")[:512] or None,
                    "source": "play_vitals_scrape",
                    "badges": list(iss.get("tags") or [])[:4],
                }
            )
    out.sort(key=lambda r: -int(r.get("event_count") or 0))
    return out


def _version_rows_from_vitals(vitals: dict[str, Any]) -> list[dict[str, Any]]:
    name_map = vitals.get("version_name_map") if isinstance(vitals.get("version_name_map"), dict) else {}
    by_v = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    versions_meta = vitals.get("versions") if isinstance(vitals.get("versions"), list) else []
    code_to_name: dict[str, str] = {str(k): str(v) for k, v in name_map.items()}
    for v in versions_meta:
        if isinstance(v, dict) and v.get("code"):
            code_to_name[str(v["code"])] = str(v.get("name") or v["code"])

    rows: list[dict[str, Any]] = []
    for code, payload in by_v.items():
        code_s = str(code or "").strip()
        if not code_s or code_s == "all" or not isinstance(payload, dict):
            continue
        cr = payload.get("crashes") if isinstance(payload.get("crashes"), dict) else {}
        label = code_to_name.get(code_s) or code_s
        fatal_issues = _flatten_vitals_issues(cr, "CRASH", version_label=label)
        anr_issues = _flatten_vitals_issues(cr, "ANR", version_label=label)
        fatal_n = sum(int(i.get("event_count") or 0) for i in fatal_issues) or len(fatal_issues)
        anr_n = sum(int(i.get("event_count") or 0) for i in anr_issues) or len(anr_issues)
        users = sum(int(i.get("affected_users") or 0) for i in fatal_issues + anr_issues)
        total = fatal_n + anr_n
        if total <= 0 and not fatal_issues and not anr_issues:
            continue
        rows.append(
            {
                "app_version": label,
                "version_code": code_s,
                "fatal_count": fatal_n,
                "anr_count": anr_n,
                "non_fatal_count": 0,
                "total_events": total,
                "affected_users": users,
            }
        )
    if not rows:
        # Sürüm listesi var ama by_version boş — meta'dan iskelet
        for v in versions_meta[:12]:
            if not isinstance(v, dict) or not v.get("code"):
                continue
            label = str(v.get("name") or v["code"])
            rows.append(
                {
                    "app_version": label,
                    "version_code": str(v["code"]),
                    "fatal_count": 0,
                    "anr_count": 0,
                    "non_fatal_count": 0,
                    "total_events": 0,
                    "affected_users": 0,
                }
            )
    rows.sort(key=lambda r: -int(r.get("total_events") or 0))
    return rows


def _cf_from_sf_block(block: dict[str, Any] | None, *, method: str) -> dict[str, Any] | None:
    if not isinstance(block, dict):
        return None
    pct = block.get("crash_free_pct")
    if pct is None:
        return None
    return {
        "crash_free_pct": pct,
        "crash_free_sessions_pct": block.get("crash_free_sessions_pct", pct),
        "crash_free_users_pct": block.get("crash_free_users_pct", pct),
        "method": method,
        "anr_free_pct": block.get("anr_free_pct"),
        "crash_free_fmt": block.get("crash_free_fmt"),
        "anr_free_fmt": block.get("anr_free_fmt"),
    }


def _filter_payload(
    data: dict[str, Any],
    *,
    versions: list[str] | None = None,
    error_type: str | None = None,
) -> dict[str, Any]:
    """Bellek içi sürüm / tür filtresi (BQ refetch yok)."""
    ver_set = {str(v).strip() for v in (versions or []) if str(v).strip()}
    et = (error_type or "").strip().upper() or None
    out = dict(data)

    def _match_ver(row: dict) -> bool:
        if not ver_set:
            return True
        lv = str(row.get("latest_version") or row.get("app_version") or "").strip()
        if lv in ver_set:
            return True
        # kısmi eşleşme (v1.2.3 vs 1.2.3)
        for v in ver_set:
            if v in lv or lv in v:
                return True
        return False

    def _filt_issues(rows: list) -> list:
        res = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            if et and str(r.get("error_type") or "").upper() != et:
                continue
            if not _match_ver(r):
                continue
            res.append(r)
        return res

    issues = _filt_issues(out.get("issues") or [])
    anr = _filt_issues(out.get("anr") or [])
    # ANR sekmesi her zaman ANR
    if et and et != "ANR":
        anr = []
    out["issues"] = issues
    out["anr"] = anr if not et or et == "ANR" else anr

    ibp = {}
    for plat, rows in (out.get("issues_by_platform") or {}).items():
        ibp[plat] = _filt_issues(rows)
    out["issues_by_platform"] = ibp
    abp = {}
    for plat, rows in (out.get("anr_by_platform") or {}).items():
        abp[plat] = _filt_issues(rows) if (not et or et == "ANR") else []
    out["anr_by_platform"] = abp

    if ver_set:
        vers = [
            r
            for r in (out.get("versions") or [])
            if isinstance(r, dict)
            and (
                str(r.get("app_version") or "") in ver_set
                or any(
                    v in str(r.get("app_version") or "") or str(r.get("app_version") or "") in v
                    for v in ver_set
                )
            )
        ]
        out["versions"] = vers
        vbp = {}
        for plat, rows in (out.get("versions_by_platform") or {}).items():
            vbp[plat] = [
                r
                for r in (rows or [])
                if isinstance(r, dict)
                and (
                    str(r.get("app_version") or "") in ver_set
                    or any(
                        v in str(r.get("app_version") or "")
                        or str(r.get("app_version") or "") in v
                        for v in ver_set
                    )
                )
            ]
        out["versions_by_platform"] = vbp

    # Totals yeniden
    fatal = sum(int(r.get("event_count") or 0) for r in issues if str(r.get("error_type") or "").upper() == "FATAL")
    anr_n = sum(int(r.get("event_count") or 0) for r in (out.get("anr") or []))
    users = sum(int(r.get("affected_users") or 0) for r in issues) + sum(
        int(r.get("affected_users") or 0) for r in (out.get("anr") or [])
    )
    out["totals"] = {
        "fatal": fatal if not et or et == "FATAL" else 0,
        "anr": anr_n if not et or et == "ANR" else 0,
        "non_fatal": 0,
        "affected_users": users,
    }
    out["active_filters"] = {"versions": list(ver_set), "error_type": et}
    return out


def get_vitals_issue_detail(issue_id: str) -> dict[str, Any] | None:
    """Play vitals scrape issue_details — modal için."""
    from backend.database import SessionLocal
    from backend.services.play_console_store import play_console_payload

    iid = (issue_id or "").strip()
    if not iid:
        return None
    try:
        with SessionLocal() as db:
            snap = play_console_payload(db) or {}
    except Exception:
        logger.debug("vitals issue detail snapshot failed", exc_info=True)
        return None
    panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
    vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    for et in ("CRASH", "ANR"):
        block = crashes.get(et) if isinstance(crashes.get(et), dict) else {}
        details = block.get("issue_details") if isinstance(block.get("issue_details"), dict) else {}
        if iid in details and isinstance(details[iid], dict):
            return {**details[iid], "error_type": "FATAL" if et == "CRASH" else "ANR"}
        # id prefix / suffix match
        for k, det in details.items():
            if str(k) == iid or str(k).endswith(iid) or iid.endswith(str(k)):
                if isinstance(det, dict):
                    return {**det, "error_type": "FATAL" if et == "CRASH" else "ANR"}
    # listeden bul
    for et in ("CRASH", "ANR"):
        for iss in _flatten_vitals_issues(crashes, et):
            if iss.get("issue_id") == iid:
                return {
                    "issue_id": iid,
                    "title": iss.get("issue_title"),
                    "url": iss.get("detail_url"),
                    "error_type": iss.get("error_type"),
                    "summary_cards": [
                        {"title": "Olay", "value": str(iss.get("event_count") or "—")},
                        {"title": "Kullanıcı", "value": str(iss.get("affected_users") or "—")},
                        {"title": "Sürüm", "value": str(iss.get("latest_version") or "—")},
                    ],
                    "insights": ["Kaynak: Play Console vitals scrape (/android)"],
                    "stack_trace": "",
                    "sections": [],
                }
    return None


def build_firebase_tab_payload(
    product_id: str = "doviz",
    days: int = 7,
    *,
    force_refresh: bool = False,
    versions: list[str] | None = None,
    error_type: str | None = None,
) -> dict[str, Any]:
    """Firebase HTMX partials için scrape/stability-free payload."""
    from backend.database import SessionLocal
    from backend.services.app_intel import APP_PRODUCTS
    from backend.services.crashlytics_detail import enrich_issue_row
    from backend.services.play_console_store import play_console_payload
    from backend.services.stability_free import build_stability_free_payload, invalidate_stability_cache

    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product", "configured": False}

    cache_key = f"{pid}:{int(days)}:store_tabs:v2"
    if force_refresh:
        invalidate_firebase_store_cache(pid)
        try:
            invalidate_stability_cache(pid)
        except Exception:
            pass
    else:
        cached = _cache_get(cache_key)
        if cached:
            data = cached
            if versions or error_type:
                return _filter_payload(data, versions=versions, error_type=error_type)
            return data

    package = "com.Doviz"
    vitals: dict[str, Any] = {}
    snap_at = None
    try:
        with SessionLocal() as db:
            snap = play_console_payload(db) or {}
        package = (snap.get("package_name") or package).strip() or package
        panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
        vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
        snap_at = snap.get("updated_at") or snap.get("fetched_at")
    except Exception as exc:  # noqa: BLE001
        logger.warning("firebase store-tabs snapshot: %s", exc)

    try:
        sf = build_stability_free_payload(
            package_name=package,
            product_id=pid,
            vitals=vitals,
            force_refresh=force_refresh,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("firebase store-tabs stability-free failed")
        return {
            "ok": False,
            "configured": True,
            "product": pid,
            "message": f"Android/iOS stability verisi alınamadı: {str(exc)[:120]}",
            "errors": [str(exc)[:200]],
        }

    play_overall = sf.get("play_overall") if isinstance(sf.get("play_overall"), dict) else {}
    play_latest = sf.get("play_latest") if isinstance(sf.get("play_latest"), dict) else {}
    cf_root = sf.get("crashlytics") if isinstance(sf.get("crashlytics"), dict) else {}
    cf_plats = cf_root.get("platforms") if isinstance(cf_root.get("platforms"), dict) else {}
    and_cf = cf_plats.get("android") if isinstance(cf_plats.get("android"), dict) else {}
    ios_cf = cf_plats.get("ios") if isinstance(cf_plats.get("ios"), dict) else {}

    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    fatal_issues_raw = _flatten_vitals_issues(crashes, "CRASH")
    anr_issues_raw = _flatten_vitals_issues(crashes, "ANR")

    period_days = 28
    for et in ("CRASH", "ANR"):
        block = crashes.get(et) if isinstance(crashes.get(et), dict) else {}
        if block.get("days"):
            try:
                period_days = int(block["days"])
            except (TypeError, ValueError):
                pass
            break
    ov = vitals.get("metrics_overview") if isinstance(vitals.get("metrics_overview"), dict) else {}
    if ov.get("period") == "28d":
        period_days = 28

    fatal_issues = [enrich_issue_row({**r}, days=period_days) for r in fatal_issues_raw]
    anr_issues = [enrich_issue_row({**r}, days=period_days) for r in anr_issues_raw]

    fatal_events = sum(int(r.get("event_count") or 0) for r in fatal_issues) or len(fatal_issues)
    anr_events = sum(int(r.get("event_count") or 0) for r in anr_issues) or len(anr_issues)
    affected = sum(int(r.get("affected_users") or 0) for r in fatal_issues + anr_issues)

    # Crash-free: Android = Play scrape; iOS = /ios stability-free (Crashlytics peek)
    android_cf = _cf_from_sf_block(
        {
            "crash_free_pct": play_overall.get("crash_free_pct"),
            "crash_free_sessions_pct": play_overall.get("crash_free_pct"),
            "crash_free_users_pct": play_overall.get("crash_free_pct"),
            "anr_free_pct": play_overall.get("anr_free_pct"),
            "crash_free_fmt": play_overall.get("crash_free_fmt"),
            "anr_free_fmt": play_overall.get("anr_free_fmt"),
        },
        method="play_scrape_vitals_overview",
    )
    if not android_cf and (and_cf.get("overall") or {}):
        android_cf = _cf_from_sf_block(and_cf.get("overall"), method="android_crashlytics_peek")

    ios_overall = ios_cf.get("overall") if isinstance(ios_cf.get("overall"), dict) else None
    ios_latest = ios_cf.get("latest") if isinstance(ios_cf.get("latest"), dict) else None
    ios_cf_block = _cf_from_sf_block(ios_latest or ios_overall, method="ios_stability_free")

    crash_free_by_plat: dict[str, Any] = {}
    if android_cf:
        crash_free_by_plat["android"] = android_cf
    if ios_cf_block:
        crash_free_by_plat["ios"] = ios_cf_block

    # iOS issue/sürüm — yalnızca /ios stability kaynağı (Crashlytics peek); Play vitals karışmaz
    ios_issues: list[dict[str, Any]] = []
    ios_anr: list[dict[str, Any]] = []
    ios_version_rows: list[dict[str, Any]] = []
    ios_filter_versions: list[str] = []
    ios_device: list[dict[str, Any]] = []
    ios_os: list[dict[str, Any]] = []
    ios_process: list[dict[str, Any]] = []
    ios_trend: list[dict[str, Any]] = []
    ios_fatal = int(ios_cf.get("fatal") or 0) if isinstance(ios_cf.get("fatal"), (int, float)) else 0
    ios_anr_n = int(ios_cf.get("anr") or 0) if isinstance(ios_cf.get("anr"), (int, float)) else 0
    ios_affected = 0

    for v in (ios_cf.get("versions") or [])[:3]:
        if not isinstance(v, dict) or not v.get("version"):
            continue
        ver = str(v["version"]).strip()
        ios_filter_versions.append(ver)
        fatal_c = int(v.get("fatal") or 0)
        total_c = int(v.get("total_events") or fatal_c or 0)
        users_c = int(v.get("affected_users") or 0)
        ios_version_rows.append(
            {
                "app_version": ver,
                "fatal_count": fatal_c,
                "anr_count": 0,
                "non_fatal_count": 0,
                "total_events": total_c,
                "affected_users": users_c,
            }
        )
    if ios_version_rows:
        ios_fatal = sum(int(r.get("fatal_count") or 0) for r in ios_version_rows)
        ios_affected = sum(int(r.get("affected_users") or 0) for r in ios_version_rows)

    try:
        from backend.services import crashlytics_bq as cbq

        bq = cbq.peek_cached_payload(pid, days=7, platform_filter="all")
        if bq and bq.get("ok") is not False:
            # Yalnızca iOS dilimleri — Android BQ satırları Play scrape'e karışmaz
            raw_ios_issues = (bq.get("issues_by_platform") or {}).get("ios") or []
            ios_issues = [
                enrich_issue_row({**r, "platform": "ios"}, days=int(bq.get("days") or 7))
                for r in raw_ios_issues
                if isinstance(r, dict)
            ]
            raw_ios_anr = (bq.get("anr_by_platform") or {}).get("ios") or []
            ios_anr = [
                enrich_issue_row({**r, "platform": "ios"}, days=int(bq.get("days") or 7))
                for r in raw_ios_anr
                if isinstance(r, dict)
            ]
            bq_ios_sum = (bq.get("summary_by_platform") or {}).get("ios") or {}
            if bq_ios_sum:
                ios_fatal = int(bq_ios_sum.get("fatal") or ios_fatal or 0)
                ios_anr_n = int(bq_ios_sum.get("anr") or ios_anr_n or 0)
                ios_affected = int(bq_ios_sum.get("affected_users") or ios_affected or 0)
            if not ios_cf_block:
                bq_cf = (bq.get("crash_free_by_platform") or {}).get("ios")
                ios_cf_block = _cf_from_sf_block(bq_cf, method="ios_crashlytics_peek")
                if ios_cf_block:
                    crash_free_by_plat["ios"] = ios_cf_block
            bq_ios_vers = (bq.get("versions_by_platform") or {}).get("ios") or []
            if bq_ios_vers and not ios_version_rows:
                ios_version_rows = [r for r in bq_ios_vers if isinstance(r, dict)][:12]
            bq_filt = (bq.get("filter_versions_by_platform") or {}).get("ios") or []
            if bq_filt:
                ios_filter_versions = [str(x).strip() for x in bq_filt if str(x).strip()][:12]
            ios_device = (bq.get("device_breakdown_by_platform") or {}).get("ios") or []
            ios_os = (bq.get("os_breakdown_by_platform") or {}).get("ios") or []
            ios_process = (bq.get("process_state_breakdown_by_platform") or {}).get("ios") or []
            ios_trend = (bq.get("trend_by_platform") or {}).get("ios") or []
            if not ios_affected and ios_issues:
                ios_affected = sum(int(r.get("affected_users") or 0) for r in ios_issues)
            if not ios_fatal and ios_issues:
                ios_fatal = sum(int(r.get("event_count") or 0) for r in ios_issues)
    except Exception:
        logger.debug("firebase ios BQ peek failed", exc_info=True)

    if not ios_filter_versions and ios_cf.get("latest_version"):
        ios_filter_versions = [str(ios_cf["latest_version"])]

    version_rows = _version_rows_from_vitals(vitals)
    filter_versions = [str(r.get("app_version") or "") for r in version_rows if r.get("app_version")]
    for pv in sf.get("play_versions") or []:
        if not isinstance(pv, dict):
            continue
        name = str(pv.get("version_name") or "").strip()
        if name and name not in filter_versions:
            filter_versions.append(name)

    summary_by_plat = {
        "android": {
            "fatal": fatal_events,
            "anr": anr_events,
            "non_fatal": 0,
            "affected_users": affected,
        },
        "ios": {
            "fatal": ios_fatal,
            "anr": ios_anr_n,
            "non_fatal": 0,
            "affected_users": ios_affected,
        },
    }

    # Platform birleşik CF yalnızca yan yana özet için; sütun diliminde kendi CF'si kullanılır
    crash_free_pct = None
    crash_free_sessions_pct = None
    crash_free_users_pct = None
    crash_free_method = None
    if android_cf and ios_cf_block:
        # İkisinin ortalamasını yazma — yanıltır; all görünümünde boş bırak
        crash_free_method = "per_platform"
    elif android_cf:
        crash_free_pct = android_cf.get("crash_free_pct")
        crash_free_sessions_pct = android_cf.get("crash_free_sessions_pct")
        crash_free_users_pct = android_cf.get("crash_free_users_pct")
        crash_free_method = android_cf.get("method")
    elif ios_cf_block:
        crash_free_pct = ios_cf_block.get("crash_free_pct")
        crash_free_sessions_pct = ios_cf_block.get("crash_free_sessions_pct")
        crash_free_users_pct = ios_cf_block.get("crash_free_users_pct")
        crash_free_method = ios_cf_block.get("method")

    result: dict[str, Any] = {
        "ok": True,
        "configured": True,
        "product": pid,
        "days": period_days,
        "data_days": period_days,
        "requested_days": days,
        "platform_filter": "all",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "snap_at": snap_at,
        "source": "android_ios_store_tabs",
        "totals": {
            "fatal": fatal_events + ios_fatal,
            "anr": anr_events + ios_anr_n,
            "non_fatal": 0,
            "affected_users": affected + ios_affected,
        },
        "crash_free_pct": crash_free_pct,
        "crash_free_sessions_pct": crash_free_sessions_pct,
        "crash_free_users_pct": crash_free_users_pct,
        "crash_free_method": crash_free_method,
        "crash_free_hints": [],
        "crash_free_by_platform": crash_free_by_plat,
        "summary_by_platform": summary_by_plat,
        # Birleşik listeler: dilimlemede platform kırılımı kullanılır
        "issues": fatal_issues + ios_issues,
        "issues_by_platform": {
            "android": fatal_issues,
            "ios": ios_issues,
        },
        "anr": anr_issues + ios_anr,
        "anr_by_platform": {
            "android": anr_issues,
            "ios": ios_anr,
        },
        "versions": version_rows + ios_version_rows,
        "versions_by_platform": {
            "android": version_rows,
            "ios": ios_version_rows,
        },
        "versions_7d_by_platform": {
            "android": version_rows,
            "ios": ios_version_rows,
        },
        "latest_version_stats_by_platform": {
            "android": {
                "version": (play_latest or {}).get("version_name")
                or (and_cf.get("latest_version") if and_cf else None),
                "fatal": fatal_events,
                "anr": anr_events,
                "crash_free": _cf_from_sf_block(play_latest, method="play_reporting")
                if play_latest
                else None,
            },
            "ios": {
                "version": ios_cf.get("latest_version")
                or (ios_filter_versions[0] if ios_filter_versions else None),
                "fatal": ios_fatal,
                "anr": ios_anr_n,
                "crash_free": ios_cf_block,
            },
        },
        "filter_versions_by_platform": {
            "android": filter_versions[:30],
            "ios": ios_filter_versions[:12],
        },
        "trend": ios_trend,
        "trend_by_platform": {"android": [], "ios": ios_trend},
        "version_trend": [],
        "version_trend_by_platform": {},
        "device_breakdown": ios_device,
        "device_breakdown_by_platform": {"android": [], "ios": ios_device},
        "os_breakdown": ios_os,
        "os_breakdown_by_platform": {"android": [], "ios": ios_os},
        "process_state_breakdown": ios_process,
        "process_state_breakdown_by_platform": {"android": [], "ios": ios_process},
        "storage_mb": {},
        # Soft uyarı yok — sütunlarda sarı banner / "çekilemedi" üretmesin
        "errors": [],
        "play_overall": play_overall,
        "play_latest": play_latest,
        "stability_free": {
            "ok": bool(sf.get("ok")),
            "play_error": sf.get("play_error"),
        },
    }

    try:
        _cache_set(cache_key, result)
    except Exception:
        pass

    if versions or error_type:
        return _filter_payload(result, versions=versions, error_type=error_type)
    return result
