"""Crashlytics payload helpers — Firebase Console scrape."""

from __future__ import annotations

import re
from typing import Any


def semver_sort_versions(versions: list[str]) -> list[str]:
    def key(v: str) -> tuple:
        parts = re.split(r"[.\-_]", v)
        nums: list[int] = []
        for p in parts:
            try:
                nums.append(int(p))
            except ValueError:
                nums.append(0)
        return tuple(nums)

    return sorted(set(versions), key=key, reverse=True)


def pick_higher_version(a: str | None, b: str | None) -> str:
    aa = (a or "").strip()
    bb = (b or "").strip()
    if not aa or aa == "—":
        return bb
    if not bb or bb == "—":
        return aa
    ranked = semver_sort_versions([aa, bb])
    return ranked[0] if ranked else aa


def merge_versions(results: list[tuple[str, list[dict]]]) -> list[dict]:
    merged: dict[str, dict] = {}
    for _plat, rows in results:
        for r in rows:
            v = r["app_version"]
            if v in merged:
                for k in ("fatal_count", "anr_count", "non_fatal_count", "total_events", "affected_users"):
                    merged[v][k] = merged[v].get(k, 0) + r.get(k, 0)
            else:
                merged[v] = {**r}
    return sorted(merged.values(), key=lambda x: -x["total_events"])


def slice_payload_for_platform(data: dict[str, Any], platform: str) -> dict[str, Any]:
    """platform=all cache'inden ios/android görünümünü bellek içi dilimle."""
    plat = (platform or "all").strip().lower()
    if plat == "all" or not data or not data.get("ok"):
        return data
    if plat not in ("ios", "android"):
        return data

    out = dict(data)
    out["platform_filter"] = plat

    summary = (data.get("summary_by_platform") or {}).get(plat) or {}
    out["totals"] = {
        "fatal": summary.get("fatal", 0),
        "anr": summary.get("anr", 0),
        "non_fatal": summary.get("non_fatal", 0),
        "affected_users": summary.get("affected_users", 0),
    }
    out["summary_by_platform"] = {plat: summary} if summary else {}

    cf = (data.get("crash_free_by_platform") or {}).get(plat)
    out["crash_free_by_platform"] = {plat: cf} if cf else {}
    if cf:
        out["crash_free_pct"] = cf.get("crash_free_pct")
        out["crash_free_sessions_pct"] = cf.get("crash_free_sessions_pct")
        out["crash_free_users_pct"] = cf.get("crash_free_users_pct")
    else:
        out["crash_free_pct"] = None
        out["crash_free_sessions_pct"] = None
        out["crash_free_users_pct"] = None
    out["crash_free_method"] = cf.get("method") if cf else None
    out["crash_free_hints"] = []
    out["errors"] = []

    out["trend"] = (data.get("trend_by_platform") or {}).get(plat) or []
    out["trend_by_platform"] = {plat: out["trend"]} if out["trend"] else {}

    out["issues"] = (data.get("issues_by_platform") or {}).get(plat) or []
    out["anr"] = (data.get("anr_by_platform") or {}).get(plat) or []

    ver_plat = (data.get("versions_by_platform") or {}).get(plat) or []
    out["versions"] = merge_versions([(plat, ver_plat)]) if ver_plat else []
    out["versions_by_platform"] = {plat: ver_plat} if ver_plat else {}
    ver7 = (data.get("versions_7d_by_platform") or {}).get(plat) or []
    out["versions_7d_by_platform"] = {plat: ver7} if ver7 else {}
    lv_stats = (data.get("latest_version_stats_by_platform") or {}).get(plat)
    out["latest_version_stats_by_platform"] = {plat: lv_stats} if lv_stats else {}

    vt = (data.get("version_trend_by_platform") or {}).get(plat) or []
    if not vt:
        vt = [r for r in (data.get("version_trend") or []) if (r.get("platform") or plat) == plat]
    if not vt:
        vt = [r for r in (data.get("version_trend") or []) if not r.get("platform")]
    out["version_trend"] = vt
    out["version_trend_by_platform"] = {plat: vt} if vt else {}

    out["device_breakdown"] = (data.get("device_breakdown_by_platform") or {}).get(plat) or []
    out["device_breakdown_by_platform"] = {plat: out["device_breakdown"]} if out["device_breakdown"] else {}
    out["os_breakdown"] = (data.get("os_breakdown_by_platform") or {}).get(plat) or []
    out["os_breakdown_by_platform"] = {plat: out["os_breakdown"]} if out["os_breakdown"] else {}
    out["process_state_breakdown"] = (data.get("process_state_breakdown_by_platform") or {}).get(plat) or []
    out["process_state_breakdown_by_platform"] = (
        {plat: out["process_state_breakdown"]} if out["process_state_breakdown"] else {}
    )
    out["filter_versions_by_platform"] = {
        plat: (data.get("filter_versions_by_platform") or {}).get(plat) or []
    }
    if plat == "ios":
        out.pop("play_overall", None)
        out.pop("play_latest", None)
    return out
