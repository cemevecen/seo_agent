"""Firebase sekmesi — yalnızca Firebase Console scrape (S-Firebase ile aynı depo).

Kaynak: `firebase_console_workspace` ← Mac bridge `firebase_console_scrape.py`.
Play Console / Reporting API / BigQuery bu sekmenin ana verisi değildir.
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
_BUILD_LOCKS: dict[str, threading.Lock] = {}
_BUILD_LOCKS_GUARD = threading.Lock()
_REPORTING_BD_TTL_S = 15 * 60
_REPORTING_BD_CACHE: dict[str, tuple[float, tuple[list, list]]] = {}
_REPORTING_BD_LOCK = threading.Lock()


def _build_lock(key: str) -> threading.Lock:
    with _BUILD_LOCKS_GUARD:
        lock = _BUILD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _BUILD_LOCKS[key] = lock
        return lock


def invalidate_firebase_store_cache(product_id: str | None = None) -> None:
    pid = (product_id or "").strip().lower()
    with _CACHE_LOCK:
        if not pid:
            _CACHE.clear()
        else:
            for k in list(_CACHE):
                if k.startswith(f"{pid}:"):
                    del _CACHE[k]
    # Reporting kırılımı package bazlı; product invalidate’da temizle
    with _REPORTING_BD_LOCK:
        _REPORTING_BD_CACHE.clear()


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


def _sum_reporting_segments(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Reporting günlük satırlarını segment → toplam olay sayısına indirger."""
    out: dict[str, int] = {}
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        seg = str(r.get("segment") or "").strip()
        if not seg or seg.upper() in ("OVERALL", "ALL", "UNKNOWN", "TOTAL"):
            continue
        try:
            val = int(round(float(r.get("value") or 0)))
        except (TypeError, ValueError):
            val = 0
        if val <= 0:
            continue
        out[seg] = out.get(seg, 0) + val
    return out


def _split_device_segment(seg: str) -> tuple[str, str]:
    """Play deviceModel segment → (manufacturer, model)."""
    s = (seg or "").strip()
    if not s:
        return "", "bilinmiyor"
    for sep in (":", "/", "|"):
        if sep in s:
            left, right = s.split(sep, 1)
            left, right = left.strip(), right.strip()
            if left and right:
                return left, right
    return "", s


_API_LEVEL_NAMES: dict[int, str] = {
    28: "9",
    29: "10",
    30: "11",
    31: "12",
    32: "12L",
    33: "13",
    34: "14",
    35: "15",
    36: "16",
}


def _os_label_from_api_level(seg: str) -> str:
    s = (seg or "").strip()
    m = re.search(r"(\d{2,3})", s)
    if not m:
        return s or "bilinmiyor"
    try:
        api = int(m.group(1))
    except ValueError:
        return s
    name = _API_LEVEL_NAMES.get(api)
    if name:
        return f"Android {name} (API {api})"
    return f"API {api}"


def _android_breakdowns_from_scrape(
    *,
    days: int = 28,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Play Console explorer_facts — Android cihaz + OS kırılımı (Reporting yok)."""
    from datetime import date, timedelta

    from backend.services.android_device_names import enrich_device_row
    from backend.services.crashlytics_detail import merge_breakdown_rows
    from backend.services.play_scrape_warehouse import load_scrape_facts

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=max(int(days or 28), 7) - 1)
    facts, _meta = load_scrape_facts()
    if not facts:
        return [], []

    device_counts: dict[str, int] = {}
    os_counts: dict[str, int] = {}
    for f in facts:
        if not isinstance(f, dict):
            continue
        metric = str(f.get("metric") or "")
        if metric not in ("crashes", "anrs"):
            continue
        dim = str(f.get("dim") or "")
        seg = str(f.get("segment") or "").strip()
        if not seg or seg.upper() in ("OVERALL", "ALL", "UNKNOWN", "TOTAL"):
            continue
        ds = str(f.get("date") or "")
        if ds and len(ds) >= 8 and not ds.startswith("i"):
            try:
                d = date.fromisoformat(ds[:10])
                if d < start or d > end:
                    continue
            except ValueError:
                pass
        try:
            val = int(round(float(f.get("value") or 0)))
        except (TypeError, ValueError):
            val = 0
        if val <= 0:
            continue
        if dim == "device":
            device_counts[seg] = device_counts.get(seg, 0) + val
        elif dim == "os_version":
            os_counts[seg] = os_counts.get(seg, 0) + val

    device_labeled: list[dict[str, Any]] = []
    device_raw: dict[str, str | None] = {}
    for seg, n in device_counts.items():
        man, mod = _split_device_segment(seg)
        er = enrich_device_row(
            {"manufacturer": man, "model": mod, "event_count": n},
            platform="android",
        )
        device_labeled.append(
            {
                "label": er["label"],
                "manufacturer": er.get("manufacturer") or man,
                "model": er.get("model") or mod,
                "event_count": n,
            }
        )
        if er.get("label_raw"):
            device_raw[er["label"]] = er["label_raw"]

    android_devices = merge_breakdown_rows([device_labeled], "label", limit=20)
    for row in android_devices:
        if device_raw.get(row["label"]):
            row["label_raw"] = device_raw[row["label"]]

    os_rows = [
        {"os_version": _os_label_from_api_level(seg), "event_count": n}
        for seg, n in os_counts.items()
    ]
    android_os = merge_breakdown_rows([os_rows], "os_version", limit=20)
    return android_devices, android_os


def _android_version_trend_from_reporting(
    package_name: str,
    *,
    days: int = 28,
    limit_versions: int = 6,
    name_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Play Reporting — gün × app sürümü çökme serisi (versiyon trendi)."""
    from datetime import date, timedelta

    from backend.services import gp_client

    if not gp_client.is_configured():
        return []

    pkg = (package_name or "").strip() or "com.Doviz"
    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=max(int(days or 28), 7) - 1)
    try:
        rows = gp_client.fetch_crash_by_dimension(
            pkg, dimension="versionCode", start=start, end=end
        ) or []
    except Exception:
        logger.debug("firebase android version trend reporting failed", exc_info=True)
        return []

    # (date, version_label) → count; önce top sürümleri seç
    by_ver: dict[str, int] = {}
    daily: dict[tuple[str, str], int] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        seg = str(r.get("segment") or "").strip()
        if not seg or seg.upper() in ("OVERALL", "ALL", "UNKNOWN", "TOTAL"):
            continue
        ds = str(r.get("date") or "")[:10]
        if len(ds) < 8:
            continue
        try:
            val = int(round(float(r.get("value") or 0)))
        except (TypeError, ValueError):
            val = 0
        if val <= 0:
            continue
        label = gp_client.format_app_version_label(seg, name_map)
        by_ver[label] = by_ver.get(label, 0) + val
        key = (ds, label)
        daily[key] = daily.get(key, 0) + val

    if not daily:
        return []
    top = {
        v
        for v, _ in sorted(by_ver.items(), key=lambda x: x[1], reverse=True)[: max(1, limit_versions)]
    }
    out = [
        {"date": d, "app_version": ver, "event_count": n, "platform": "android"}
        for (d, ver), n in sorted(daily.items())
        if ver in top
    ]
    return out


def _android_version_trend_from_scrape(
    *,
    days: int = 28,
    limit_versions: int = 6,
) -> list[dict[str, Any]]:
    """explorer_facts dim=app_version × date — varsa versiyon trendi."""
    from datetime import date, timedelta

    from backend.services.play_scrape_warehouse import load_scrape_facts

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=max(int(days or 28), 7) - 1)
    facts, _meta = load_scrape_facts()
    if not facts:
        return []

    by_ver: dict[str, int] = {}
    daily: dict[tuple[str, str], int] = {}
    for f in facts:
        if not isinstance(f, dict):
            continue
        if str(f.get("metric") or "") not in ("crashes", "anrs"):
            continue
        if str(f.get("dim") or "") != "app_version":
            continue
        seg = str(f.get("segment") or "").strip()
        if not seg or seg.upper() in ("OVERALL", "ALL", "UNKNOWN", "TOTAL"):
            continue
        ds = str(f.get("date") or "")
        if not ds or len(ds) < 8 or ds.startswith("i"):
            continue
        try:
            d = date.fromisoformat(ds[:10])
            if d < start or d > end:
                continue
        except ValueError:
            continue
        try:
            val = int(round(float(f.get("value") or 0)))
        except (TypeError, ValueError):
            val = 0
        if val <= 0:
            continue
        by_ver[seg] = by_ver.get(seg, 0) + val
        key = (ds[:10], seg)
        daily[key] = daily.get(key, 0) + val

    if not daily:
        return []
    top = {
        v
        for v, _ in sorted(by_ver.items(), key=lambda x: x[1], reverse=True)[: max(1, limit_versions)]
    }
    return [
        {"date": d, "app_version": ver, "event_count": n, "platform": "android"}
        for (d, ver), n in sorted(daily.items())
        if ver in top
    ]


def _android_breakdowns_from_reporting(
    package_name: str,
    *,
    days: int = 28,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Reporting API — scrape boşsa cihaz/OS kırılımı."""
    from datetime import date, timedelta

    from backend.services import gp_client
    from backend.services.android_device_names import enrich_device_row
    from backend.services.crashlytics_detail import merge_breakdown_rows

    if not gp_client.is_configured():
        return [], []

    pkg = (package_name or "").strip() or "com.Doviz"
    cache_key = f"{pkg}:{int(days or 28)}"
    with _REPORTING_BD_LOCK:
        entry = _REPORTING_BD_CACHE.get(cache_key)
        if entry and time.time() - entry[0] < _REPORTING_BD_TTL_S:
            return entry[1]

    end = date.today() - timedelta(days=2)
    start = end - timedelta(days=max(int(days or 28), 7) - 1)

    device_counts: dict[str, int] = {}
    os_counts: dict[str, int] = {}
    for fetch in (gp_client.fetch_crash_by_dimension, gp_client.fetch_anr_by_dimension):
        try:
            rows = fetch(pkg, dimension="deviceModel", start=start, end=end) or []
            for seg, n in _sum_reporting_segments(rows).items():
                device_counts[seg] = device_counts.get(seg, 0) + n
        except Exception:
            logger.debug("firebase android device reporting failed", exc_info=True)
        try:
            rows = fetch(pkg, dimension="apiLevel", start=start, end=end) or []
            for seg, n in _sum_reporting_segments(rows).items():
                os_counts[seg] = os_counts.get(seg, 0) + n
        except Exception:
            logger.debug("firebase android os reporting failed", exc_info=True)

    device_labeled: list[dict[str, Any]] = []
    device_raw: dict[str, str | None] = {}
    for seg, n in device_counts.items():
        man, mod = _split_device_segment(seg)
        er = enrich_device_row(
            {"manufacturer": man, "model": mod, "event_count": n},
            platform="android",
        )
        device_labeled.append(
            {
                "label": er["label"],
                "manufacturer": er.get("manufacturer") or man,
                "model": er.get("model") or mod,
                "event_count": n,
            }
        )
        if er.get("label_raw"):
            device_raw[er["label"]] = er["label_raw"]

    android_devices = merge_breakdown_rows([device_labeled], "label", limit=20)
    for row in android_devices:
        if device_raw.get(row["label"]):
            row["label_raw"] = device_raw[row["label"]]

    os_rows = [
        {"os_version": _os_label_from_api_level(seg), "event_count": n}
        for seg, n in os_counts.items()
    ]
    android_os = merge_breakdown_rows([os_rows], "os_version", limit=20)
    out = (android_devices, android_os)
    with _REPORTING_BD_LOCK:
        _REPORTING_BD_CACHE[cache_key] = (time.time(), out)
        if len(_REPORTING_BD_CACHE) > 16:
            cutoff = time.time() - _REPORTING_BD_TTL_S
            for k, (ts, _) in list(_REPORTING_BD_CACHE.items()):
                if ts < cutoff:
                    del _REPORTING_BD_CACHE[k]
    return out


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
    """Geriye dönük: Play vitals detayı (artık birincil değil)."""
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
        for k, det in details.items():
            if str(k) == iid or str(k).endswith(iid) or iid.endswith(str(k)):
                if isinstance(det, dict):
                    return {**det, "error_type": "FATAL" if et == "CRASH" else "ANR"}
    for et in ("CRASH", "ANR"):
        for iss in _flatten_vitals_issues(crashes, et):
            if iss.get("issue_id") == iid:
                return {
                    "issue_id": iid,
                    "title": iss.get("issue_title"),
                    "url": iss.get("detail_url"),
                    "error_type": iss.get("error_type"),
                    "summary_cards": [
                        {"title": "Events", "value": str(iss.get("event_count") or "—")},
                        {"title": "Users", "value": str(iss.get("affected_users") or "—")},
                        {"title": "Version", "value": str(iss.get("latest_version") or "—")},
                    ],
                    "insights": ["Source: Play Console vitals (legacy)"],
                    "stack_trace": "",
                    "sections": [],
                }
    return None


def get_firebase_console_issue_detail(
    issue_id: str,
    *,
    platform: str | None = None,
) -> dict[str, Any] | None:
    """Firebase Console scrape içinden issue özeti (modal)."""
    from backend.database import SessionLocal
    from backend.services.firebase_console_store import firebase_console_payload

    iid = (issue_id or "").strip()
    if not iid:
        return None
    plat_f = (platform or "").strip().lower()
    try:
        with SessionLocal() as db:
            snap = firebase_console_payload(db) or {}
    except Exception:
        logger.debug("firebase console issue detail failed", exc_info=True)
        return None
    platforms = snap.get("platforms") if isinstance(snap.get("platforms"), dict) else {}
    plats = [plat_f] if plat_f in ("android", "ios") else ["android", "ios"]
    for plat in plats:
        block = platforms.get(plat) if isinstance(platforms.get(plat), dict) else {}
        if not block:
            continue
        pages = block.get("pages") if isinstance(block.get("pages"), dict) else {}
        detail_base = str(pages.get("crashlytics") or "").split("?")[0]
        pools: list[tuple[str, list]] = [
            ("FATAL", list(block.get("issues") or [])),
            ("ANR", list(block.get("anr_issues") or [])),
            ("NON_FATAL", list(block.get("nonfatal_issues") or [])),
        ]
        windows = block.get("windows") if isinstance(block.get("windows"), dict) else {}
        for win in windows.values():
            if isinstance(win, dict) and win.get("issues"):
                pools.append(("FATAL", list(win.get("issues") or [])))
        for et, rows in pools:
            for iss in rows:
                if not isinstance(iss, dict):
                    continue
                sid = str(iss.get("id") or iss.get("issue_id") or "").strip()
                if not sid or (sid != iid and not sid.endswith(iid) and not iid.endswith(sid)):
                    continue
                title = str(iss.get("title") or iss.get("issue_title") or sid)[:240]
                events = _parse_count(iss.get("event_count") or iss.get("events"))
                users = _parse_count(iss.get("affected_users") or iss.get("users"))
                ver = str(
                    iss.get("version")
                    or iss.get("app_version")
                    or iss.get("latest_version")
                    or block.get("latest_version")
                    or "—"
                )[:80]
                url = str(iss.get("url") or iss.get("detail_url") or "").strip()
                if not url and detail_base and sid:
                    url = f"{detail_base}/{sid}"
                mapped_et = str(iss.get("error_type") or et).upper()
                if mapped_et in ("CRASH", "FATAL"):
                    mapped_et = "FATAL"
                elif mapped_et in ("ANR",):
                    mapped_et = "ANR"
                elif "NON" in mapped_et:
                    mapped_et = "NON_FATAL"
                return {
                    "issue_id": sid,
                    "title": title,
                    "url": url or None,
                    "error_type": mapped_et,
                    "summary_cards": [
                        {"title": "Events", "value": str(events or "—")},
                        {"title": "Users", "value": str(users or "—")},
                        {"title": "Version", "value": ver},
                        {"title": "Platform", "value": plat},
                    ],
                    "insights": [
                        str(iss.get("detail") or iss.get("exception") or "").strip()
                        or "Source: Firebase Console scrape",
                        "Source: Firebase Console scrape (/firebase)",
                    ],
                    "stack_trace": str(iss.get("stack_trace") or "")[:8000],
                    "sections": [],
                    "platform": plat,
                }
    return None


def _map_scrape_issue(
    iss: dict[str, Any],
    *,
    platform: str,
    error_type: str,
    default_version: str | None = None,
    days: int = 7,
) -> dict[str, Any] | None:
    from backend.services.crashlytics_detail import enrich_issue_row

    if not isinstance(iss, dict):
        return None
    iid = str(iss.get("id") or iss.get("issue_id") or "").strip()
    title = str(iss.get("title") or iss.get("issue_title") or "").strip()
    if not title and not iid:
        return None
    if not title:
        title = f"Issue {iid[:12]}"
    events = _parse_count(iss.get("event_count") or iss.get("events"))
    users = _parse_count(iss.get("affected_users") or iss.get("users"))
    ver = (
        str(iss.get("version") or iss.get("app_version") or iss.get("latest_version") or "").strip()
        or (default_version or "—")
    )
    et = str(iss.get("error_type") or error_type or "FATAL").upper()
    if et in ("CRASH",):
        et = "FATAL"
    if "NON" in et:
        et = "NON_FATAL"
    row = {
        "issue_id": iid or title[:64],
        "issue_title": title[:240],
        "error_type": et,
        "event_count": events,
        "affected_users": users,
        "latest_version": ver[:80],
        "platform": platform,
        "detail_url": str(iss.get("url") or iss.get("detail_url") or "")[:512] or None,
        "source": "firebase_console_scrape",
        "badges": list(iss.get("badges") or iss.get("tags") or [])[:4],
        "exception": str(iss.get("exception") or iss.get("detail") or "")[:200] or None,
    }
    return enrich_issue_row(row, days=days)


def _versions_from_scrape_block(
    block: dict[str, Any],
    *,
    fatal_issues: list[dict[str, Any]],
    anr_issues: list[dict[str, Any]],
    nonfatal_issues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_v = block.get("by_version") if isinstance(block.get("by_version"), list) else []
    agg: dict[str, dict[str, Any]] = {}

    def _bucket(ver: str) -> dict[str, Any]:
        key = ver.strip() or "—"
        if key not in agg:
            agg[key] = {
                "app_version": key,
                "fatal_count": 0,
                "anr_count": 0,
                "non_fatal_count": 0,
                "total_events": 0,
                "affected_users": 0,
            }
        return agg[key]

    for iss in fatal_issues:
        b = _bucket(str(iss.get("latest_version") or "—"))
        b["fatal_count"] += int(iss.get("event_count") or 0) or 1
        b["affected_users"] += int(iss.get("affected_users") or 0)
    for iss in anr_issues:
        b = _bucket(str(iss.get("latest_version") or "—"))
        b["anr_count"] += int(iss.get("event_count") or 0) or 1
        b["affected_users"] += int(iss.get("affected_users") or 0)
    for iss in nonfatal_issues:
        b = _bucket(str(iss.get("latest_version") or "—"))
        b["non_fatal_count"] += int(iss.get("event_count") or 0) or 1
        b["affected_users"] += int(iss.get("affected_users") or 0)

    for row in by_v:
        if not isinstance(row, dict):
            continue
        ver = str(row.get("version") or row.get("label") or "").strip()
        if not ver:
            continue
        b = _bucket(ver)
        if row.get("build") and not b.get("version_code"):
            b["version_code"] = str(row.get("build"))
        # Scrape satırında event varsa kullan
        for src_key, dst in (
            ("fatal_count", "fatal_count"),
            ("anr_count", "anr_count"),
            ("non_fatal_count", "non_fatal_count"),
            ("event_count", "fatal_count"),
            ("events", "fatal_count"),
            ("affected_users", "affected_users"),
        ):
            n = _parse_count(row.get(src_key))
            if n > 0 and dst == "affected_users":
                b[dst] = max(int(b.get(dst) or 0), n)
            elif n > 0 and int(b.get(dst) or 0) <= 0:
                b[dst] = n

    out = list(agg.values())
    for r in out:
        r["total_events"] = int(r.get("fatal_count") or 0) + int(r.get("anr_count") or 0) + int(
            r.get("non_fatal_count") or 0
        )
    out.sort(key=lambda r: -int(r.get("total_events") or 0))
    return out


def _trend_from_cf_series(series: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Crash-free günlük seriden özet spark için trend satırları."""
    out: list[dict[str, Any]] = []
    for s in series or []:
        if not isinstance(s, dict):
            continue
        day = str(s.get("date") or s.get("day") or "")[:10]
        if len(day) < 10:
            continue
        cf = s.get("crash_free_pct")
        users = _parse_count(s.get("users"))
        try:
            cf_f = float(cf) if cf is not None else None
        except (TypeError, ValueError):
            cf_f = None
        if cf_f is None:
            fatal = 0
        else:
            # CF düştükçe “risk” artar; kullanıcı hacmiyle ölçekle
            risk = max(0.0, 100.0 - cf_f)
            fatal = int(round(risk * max(users, 1) / 100.0)) if users else int(round(risk * 10))
        out.append(
            {
                "date": day,
                "fatal": fatal,
                "anr": 0,
                "non_fatal": 0,
                "crash_free_pct": cf_f,
            }
        )
    return out


def _device_breakdown_from_scrape(block: dict[str, Any]) -> list[dict[str, Any]]:
    rows = block.get("by_device") if isinstance(block.get("by_device"), list) else []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        label = str(r.get("label") or r.get("device") or r.get("device_model") or "").strip()
        if not label:
            continue
        out.append(
            {
                "label": label,
                "manufacturer": str(r.get("manufacturer") or "")[:80] or None,
                "model": str(r.get("model") or label)[:120],
                "event_count": _parse_count(r.get("event_count") or r.get("events") or r.get("count")),
            }
        )
    out.sort(key=lambda x: -int(x.get("event_count") or 0))
    return out[:40]


def _os_breakdown_from_scrape(block: dict[str, Any]) -> list[dict[str, Any]]:
    rows = block.get("by_os") if isinstance(block.get("by_os"), list) else []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        label = str(r.get("os_version") or r.get("label") or r.get("os") or "").strip()
        if not label:
            continue
        out.append(
            {
                "os_version": label,
                "event_count": _parse_count(r.get("event_count") or r.get("events") or r.get("count")),
            }
        )
    out.sort(key=lambda x: -int(x.get("event_count") or 0))
    return out[:40]


def build_firebase_tab_payload(
    product_id: str = "doviz",
    days: int = 7,
    *,
    force_refresh: bool = False,
    versions: list[str] | None = None,
    error_type: str | None = None,
) -> dict[str, Any]:
    """Firebase HTMX partials — Firebase Console scrape payload."""
    pid = (product_id or "doviz").strip().lower()
    cache_key = f"{pid}:{int(days)}:firebase_console_scrape:v1"

    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached:
            if versions or error_type:
                return _filter_payload(cached, versions=versions, error_type=error_type)
            return cached

    # Soğuk cache stampede: aynı key için tek build
    lock = _build_lock(cache_key)
    with lock:
        if not force_refresh:
            cached = _cache_get(cache_key)
            if cached:
                if versions or error_type:
                    return _filter_payload(cached, versions=versions, error_type=error_type)
                return cached
        return _build_firebase_tab_payload_uncached(
            product_id=pid,
            days=days,
            force_refresh=force_refresh,
            versions=versions,
            error_type=error_type,
            cache_key=cache_key,
        )


def _build_firebase_tab_payload_uncached(
    *,
    product_id: str,
    days: int,
    force_refresh: bool,
    versions: list[str] | None,
    error_type: str | None,
    cache_key: str,
) -> dict[str, Any]:
    """Firebase HTMX — yalnızca Firebase Console scrape (lock altında)."""
    from backend.database import SessionLocal
    from backend.services.app_intel import APP_PRODUCTS
    from backend.services.firebase_console_store import query_firebase_console

    pid = product_id
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product", "configured": False}

    if force_refresh:
        invalidate_firebase_store_cache(pid)

    try:
        days_i = int(days or 7)
    except (TypeError, ValueError):
        days_i = 7
    days_i = min(max(days_i, 1), 90)

    try:
        with SessionLocal() as db:
            q = query_firebase_console(db, platform="all", days=days_i)
    except Exception as exc:  # noqa: BLE001
        logger.exception("firebase console query failed")
        return {
            "ok": False,
            "configured": True,
            "product": pid,
            "message": f"Firebase Console scrape okunamadı: {str(exc)[:140]}",
            "errors": [str(exc)[:200]],
            "source": "firebase_console_scrape",
        }

    platforms = q.get("platforms") if isinstance(q.get("platforms"), dict) else {}
    snap_at = q.get("updated_at") or q.get("background_synced_at") or q.get("fetched_at")
    empty = bool(q.get("empty")) or not platforms

    if empty:
        return {
            "ok": False,
            "configured": True,
            "product": pid,
            "days": days_i,
            "requested_days": days,
            "message": (
                "No Firebase data yet. "
                "Run Update page → Firebase or wait for the next automatic scan."
            ),
            "errors": [],
            "source": "firebase_console_scrape",
            "snap_at": snap_at,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    issues_by_plat: dict[str, list] = {"android": [], "ios": []}
    anr_by_plat: dict[str, list] = {"android": [], "ios": []}
    nf_by_plat: dict[str, list] = {"android": [], "ios": []}
    versions_by_plat: dict[str, list] = {"android": [], "ios": []}
    filter_versions_by_plat: dict[str, list] = {"android": [], "ios": []}
    crash_free_by_plat: dict[str, Any] = {}
    summary_by_plat: dict[str, Any] = {}
    trend_by_plat: dict[str, list] = {"android": [], "ios": []}
    version_trend_by_plat: dict[str, list] = {"android": [], "ios": []}
    device_by_plat: dict[str, list] = {"android": [], "ios": []}
    os_by_plat: dict[str, list] = {"android": [], "ios": []}
    latest_by_plat: dict[str, Any] = {}

    for plat in ("android", "ios"):
        block = platforms.get(plat) if isinstance(platforms.get(plat), dict) else {}
        if not block:
            summary_by_plat[plat] = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0}
            continue

        latest_ver = str(block.get("latest_version") or "").strip() or None
        anr_ids = {
            str(i.get("id") or i.get("issue_id") or "").strip()
            for i in (block.get("anr_issues") or [])
            if isinstance(i, dict)
        }
        nf_ids = {
            str(i.get("id") or i.get("issue_id") or "").strip()
            for i in (block.get("nonfatal_issues") or [])
            if isinstance(i, dict)
        }

        fatal_rows: list[dict[str, Any]] = []
        for iss in block.get("issues") or []:
            if not isinstance(iss, dict):
                continue
            sid = str(iss.get("id") or iss.get("issue_id") or "").strip()
            if sid and (sid in anr_ids or sid in nf_ids):
                continue
            # Eski scrape: ANR title karışmış olabilir
            title_l = str(iss.get("title") or "").lower()
            et_hint = str(iss.get("error_type") or iss.get("page") or "").lower()
            if "anr" in et_hint or title_l.startswith("anr"):
                continue
            mapped = _map_scrape_issue(
                iss,
                platform=plat,
                error_type="FATAL",
                default_version=latest_ver,
                days=days_i,
            )
            if mapped:
                fatal_rows.append(mapped)

        anr_rows: list[dict[str, Any]] = []
        for iss in block.get("anr_issues") or []:
            mapped = _map_scrape_issue(
                iss,
                platform=plat,
                error_type="ANR",
                default_version=latest_ver,
                days=days_i,
            )
            if mapped:
                anr_rows.append(mapped)

        nf_rows: list[dict[str, Any]] = []
        for iss in block.get("nonfatal_issues") or []:
            mapped = _map_scrape_issue(
                iss,
                platform=plat,
                error_type="NON_FATAL",
                default_version=latest_ver,
                days=days_i,
            )
            if mapped:
                nf_rows.append(mapped)

        # NON_FATAL filtresi için fatal listesine de ekle (tür filtresi)
        issues_combined = fatal_rows + nf_rows
        issues_by_plat[plat] = issues_combined
        anr_by_plat[plat] = anr_rows
        nf_by_plat[plat] = nf_rows

        ver_rows = _versions_from_scrape_block(
            block,
            fatal_issues=fatal_rows,
            anr_issues=anr_rows,
            nonfatal_issues=nf_rows,
        )
        versions_by_plat[plat] = ver_rows
        filter_versions_by_plat[plat] = [
            str(r.get("app_version") or "")
            for r in ver_rows
            if r.get("app_version") and r.get("app_version") != "—"
        ][:40]
        if latest_ver and latest_ver not in filter_versions_by_plat[plat]:
            filter_versions_by_plat[plat].insert(0, latest_ver)

        cf_pct = block.get("crash_free_pct")
        sess_pct = block.get("crash_free_sessions_pct")
        if cf_pct is None and isinstance(block.get("window"), dict):
            cf_pct = block["window"].get("crash_free_pct")
            sess_pct = block["window"].get("crash_free_sessions_pct", sess_pct)
        android_cf = _cf_from_sf_block(
            {
                "crash_free_pct": cf_pct,
                "crash_free_sessions_pct": sess_pct if sess_pct is not None else cf_pct,
                "crash_free_users_pct": cf_pct,
                "crash_free_fmt": block.get("crash_free_fmt"),
            },
            method="firebase_console_scrape",
        )
        if android_cf:
            crash_free_by_plat[plat] = android_cf

        fatal_n = sum(int(r.get("event_count") or 0) for r in fatal_rows) or len(fatal_rows)
        anr_n = sum(int(r.get("event_count") or 0) for r in anr_rows) or len(anr_rows)
        nf_n = sum(int(r.get("event_count") or 0) for r in nf_rows) or len(nf_rows)
        users_n = sum(int(r.get("affected_users") or 0) for r in fatal_rows + anr_rows + nf_rows)
        summary_by_plat[plat] = {
            "fatal": fatal_n,
            "anr": anr_n,
            "non_fatal": nf_n,
            "affected_users": users_n,
        }

        trend_by_plat[plat] = _trend_from_cf_series(block.get("series") or [])
        device_by_plat[plat] = _device_breakdown_from_scrape(block)
        os_by_plat[plat] = _os_breakdown_from_scrape(block)

        # Version × time: CF serisi + en güncel sürüm (scrape günlük issue kırılımı yok)
        vt: list[dict[str, Any]] = []
        top_ver = latest_ver or (filter_versions_by_plat[plat][0] if filter_versions_by_plat[plat] else None)
        if top_ver:
            for t in trend_by_plat[plat]:
                vt.append(
                    {
                        "date": t.get("date"),
                        "app_version": top_ver,
                        "event_count": int(t.get("fatal") or 0),
                    }
                )
        version_trend_by_plat[plat] = vt

        latest_by_plat[plat] = {
            "version": latest_ver,
            "fatal": fatal_n,
            "anr": anr_n,
            "crash_free": android_cf,
        }

    all_issues = issues_by_plat["android"] + issues_by_plat["ios"]
    all_anr = anr_by_plat["android"] + anr_by_plat["ios"]
    all_versions = versions_by_plat["android"] + versions_by_plat["ios"]
    all_trend = (trend_by_plat.get("android") or []) or (trend_by_plat.get("ios") or [])

    crash_free_pct = None
    crash_free_sessions_pct = None
    crash_free_users_pct = None
    crash_free_method = None
    and_cf = crash_free_by_plat.get("android")
    ios_cf = crash_free_by_plat.get("ios")
    if and_cf and ios_cf:
        crash_free_method = "per_platform"
    elif and_cf:
        crash_free_pct = and_cf.get("crash_free_pct")
        crash_free_sessions_pct = and_cf.get("crash_free_sessions_pct")
        crash_free_users_pct = and_cf.get("crash_free_users_pct")
        crash_free_method = and_cf.get("method")
    elif ios_cf:
        crash_free_pct = ios_cf.get("crash_free_pct")
        crash_free_sessions_pct = ios_cf.get("crash_free_sessions_pct")
        crash_free_users_pct = ios_cf.get("crash_free_users_pct")
        crash_free_method = ios_cf.get("method")

    totals = {
        "fatal": sum(int((summary_by_plat.get(p) or {}).get("fatal") or 0) for p in ("android", "ios")),
        "anr": sum(int((summary_by_plat.get(p) or {}).get("anr") or 0) for p in ("android", "ios")),
        "non_fatal": sum(
            int((summary_by_plat.get(p) or {}).get("non_fatal") or 0) for p in ("android", "ios")
        ),
        "affected_users": sum(
            int((summary_by_plat.get(p) or {}).get("affected_users") or 0) for p in ("android", "ios")
        ),
    }

    result: dict[str, Any] = {
        "ok": True,
        "configured": True,
        "product": pid,
        "days": days_i,
        "data_days": days_i,
        "requested_days": days,
        "platform_filter": "all",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "snap_at": snap_at,
        "source": "firebase_console_scrape",
        "totals": totals,
        "crash_free_pct": crash_free_pct,
        "crash_free_sessions_pct": crash_free_sessions_pct,
        "crash_free_users_pct": crash_free_users_pct,
        "crash_free_method": crash_free_method,
        "crash_free_hints": [],
        "crash_free_by_platform": crash_free_by_plat,
        "summary_by_platform": summary_by_plat,
        "issues": all_issues,
        "issues_by_platform": issues_by_plat,
        "anr": all_anr,
        "anr_by_platform": anr_by_plat,
        "versions": all_versions,
        "versions_by_platform": versions_by_plat,
        "versions_7d_by_platform": versions_by_plat,
        "latest_version_stats_by_platform": latest_by_plat,
        "filter_versions_by_platform": filter_versions_by_plat,
        "trend": all_trend,
        "trend_by_platform": trend_by_plat,
        "version_trend": (version_trend_by_plat.get("android") or [])
        + (version_trend_by_plat.get("ios") or []),
        "version_trend_by_platform": version_trend_by_plat,
        "device_breakdown": device_by_plat.get("android") or device_by_plat.get("ios") or [],
        "device_breakdown_by_platform": device_by_plat,
        "os_breakdown": os_by_plat.get("android") or os_by_plat.get("ios") or [],
        "os_breakdown_by_platform": os_by_plat,
        "process_state_breakdown": [],
        "process_state_breakdown_by_platform": {"android": [], "ios": []},
        "storage_mb": {},
        "errors": [],
        "filter_options": q.get("filter_options") or {},
    }

    try:
        _cache_set(cache_key, result)
    except Exception:
        pass

    if versions or error_type:
        return _filter_payload(result, versions=versions, error_type=error_type)
    return result
