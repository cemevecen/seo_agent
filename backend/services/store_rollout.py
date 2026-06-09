"""
Mağaza sürüm yayın durumu — staged rollout / phased release (canlı API).

- Android: Play Console production track userFraction
- iOS: App Store Connect appStoreVersionPhasedRelease (7 günlük kademeli yayın)
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from backend.services import asc_client, gp_client
from backend.services.app_intel import APP_PRODUCTS
from backend.services.asc_analytics import _api_get, _paginate_first_path

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 15 * 60

# Apple kademeli yayın — kümülatif kullanıcı yüzdesi (gün 1–7)
_IOS_PHASED_CUMULATIVE_PCT: dict[int, float] = {
    1: 1.0,
    2: 2.0,
    3: 5.0,
    4: 10.0,
    5: 20.0,
    6: 50.0,
    7: 100.0,
}


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


def _android_rollout_error_message(exc: Exception) -> tuple[str, str | None]:
    """(kısa mesaj, opsiyonel etkinleştirme URL'si)."""
    raw = str(exc)
    if "SERVICE_DISABLED" in raw or (
        "androidpublisher.googleapis.com" in raw and "disabled" in raw.lower()
    ):
        url = ""
        if "activationUrl" in raw:
            m = re.search(r"https://console\.developers\.google\.com/apis/api/androidpublisher[^\s'\"]+", raw)
            if m:
                url = m.group(0)
        if not url:
            url = "https://console.developers.google.com/apis/api/androidpublisher.googleapis.com/overview"
        return (
            "Google Play Android Developer API bu GCP projesinde kapalı — "
            "API Library'den etkinleştirin, birkaç dakika bekleyin.",
            url,
        )
    if "403" in raw and "permission" in raw.lower():
        return ("Play Console API erişim izni yok (service account + uygulama yetkisi).", None)
    return (raw[:220], None)


def _ios_phased_pct(state: str | None, day: int | None) -> tuple[float, str]:
    st = (state or "").upper()
    if st in ("COMPLETE", "INACTIVE", ""):
        return 100.0, "full_release"
    if st == "PAUSED":
        d = max(1, min(int(day or 1), 7))
        return _IOS_PHASED_CUMULATIVE_PCT.get(d, 100.0), "paused"
    if st == "ACTIVE":
        d = max(1, min(int(day or 1), 7))
        return _IOS_PHASED_CUMULATIVE_PCT.get(d, 100.0), "phased_active"
    return 100.0, "unknown"


def fetch_ios_rollout(*, bundle_id: str) -> dict[str, Any]:
    if not asc_client.is_configured():
        return {"ok": False, "live": False, "message": "ASC API key tanımlı değil."}

    app_id = asc_client.find_app_id_by_bundle(bundle_id)
    if not app_id:
        return {"ok": False, "live": False, "message": "App Store Connect uygulaması bulunamadı."}

    versions = _paginate_first_path(
        f"/v1/apps/{app_id}/appStoreVersions",
        {"limit": 30, "filter[platform]": "IOS"},
    )
    if not versions:
        return {"ok": False, "live": False, "message": "App Store sürüm kaydı yok."}

    def _ver_key(v: dict) -> tuple:
        s = ((v.get("attributes") or {}).get("versionString") or "0").strip()
        parts = []
        for p in s.replace("-", ".").split("."):
            try:
                parts.append(int(p))
            except ValueError:
                parts.append(0)
        return tuple(parts)

    versions.sort(key=_ver_key, reverse=True)

    picked: dict | None = None
    for v in versions:
        attr = v.get("attributes") or {}
        state = (attr.get("appStoreState") or "").upper()
        if state in (
            "READY_FOR_SALE",
            "PENDING_DEVELOPER_RELEASE",
            "PROCESSING_FOR_APP_STORE",
            "PREPARE_FOR_SUBMISSION",
            "WAITING_FOR_REVIEW",
            "IN_REVIEW",
        ):
            picked = v
            break
    if not picked:
        picked = versions[0]

    vid = picked.get("id")
    attr = picked.get("attributes") or {}
    version_string = (attr.get("versionString") or "").strip() or "—"
    store_state = attr.get("appStoreState") or ""

    phased_state = None
    phased_day = None
    rollout_pct = 100.0
    rollout_mode = "full_release"
    phased_id = None

    if vid:
        detail = _api_get(
            f"/v1/appStoreVersions/{vid}",
            {"include": "appStoreVersionPhasedRelease"},
        )
        if detail:
            included = detail.get("included") or []
            for inc in included:
                if inc.get("type") == "appStoreVersionPhasedReleases":
                    pa = inc.get("attributes") or {}
                    phased_state = pa.get("phasedReleaseState")
                    phased_day = pa.get("currentDayNumber")
                    phased_id = inc.get("id")
                    break
            if phased_state is None:
                rel = (detail.get("data") or {}).get("relationships") or {}
                pr = (rel.get("appStoreVersionPhasedRelease") or {}).get("data")
                if pr and pr.get("id"):
                    pr_id = pr["id"]
                    pr_doc = _api_get(f"/v1/appStoreVersionPhasedReleases/{pr_id}")
                    if pr_doc and pr_doc.get("data"):
                        pa = pr_doc["data"].get("attributes") or {}
                        phased_state = pa.get("phasedReleaseState")
                        phased_day = pa.get("currentDayNumber")
                        phased_id = pr_id

    if phased_state:
        rollout_pct, rollout_mode = _ios_phased_pct(phased_state, phased_day)
    elif (store_state or "").upper() == "READY_FOR_SALE":
        rollout_pct, rollout_mode = 100.0, "full_release"
    elif store_state:
        rollout_pct, rollout_mode = 0.0, "not_live"

    return {
        "ok": True,
        "live": True,
        "platform": "ios",
        "version": version_string,
        "app_store_state": store_state,
        "rollout_pct": round(float(rollout_pct), 2),
        "rollout_mode": rollout_mode,
        "phased_release_state": phased_state,
        "phased_day": phased_day,
        "phased_release_id": phased_id,
        "source": "app_store_connect_api",
    }


def fetch_android_rollout(*, package_name: str) -> dict[str, Any]:
    if not gp_client.is_configured():
        return {"ok": False, "live": False, "message": "GP_SERVICE_ACCOUNT_JSON tanımlı değil."}

    svc = gp_client._publisher_service()  # noqa: SLF001
    if svc is None:
        return {"ok": False, "live": False, "message": "Play API bağlantısı kurulamadı."}

    edit_id = None
    try:
        edit = svc.edits().insert(body={}, packageName=package_name).execute()
        edit_id = edit.get("id")
        tracks_resp = svc.edits().tracks().list(packageName=package_name, editId=edit_id).execute()
        tracks = tracks_resp.get("tracks") or []
    except Exception as exc:
        msg, hint_url = _android_rollout_error_message(exc)
        if hint_url:
            logger.info(
                "GP tracks kullanılamıyor (%s): %s — %s",
                package_name,
                msg,
                hint_url,
            )
        else:
            logger.warning("GP tracks list hatası (%s): %s", package_name, exc)
        out: dict[str, Any] = {"ok": False, "live": False, "message": msg}
        if hint_url:
            out["api_enable_url"] = hint_url
        return out
    finally:
        if edit_id:
            try:
                svc.edits().delete(packageName=package_name, editId=edit_id).execute()
            except Exception:
                pass

    production = next((t for t in tracks if (t.get("track") or "").lower() == "production"), None)
    if not production:
        return {"ok": False, "live": False, "message": "Production track bulunamadı."}

    releases = list(production.get("releases") or [])
    if not releases:
        return {"ok": False, "live": False, "message": "Production sürümü yok."}

    def _release_rank(r: dict) -> tuple:
        codes = r.get("versionCodes") or []
        mx = max((int(c) for c in codes), default=0)
        return (mx, str(r.get("name") or ""))

    releases.sort(key=_release_rank, reverse=True)
    active = next((r for r in releases if (r.get("status") or "").lower() == "inprogress"), None)
    target = active or releases[0]
    status = (target.get("status") or "").lower()
    fraction = target.get("userFraction")

    if status == "inprogress" and fraction is not None:
        rollout_pct = round(float(fraction) * 100.0, 2)
        rollout_mode = "staged_rollout"
    elif status in ("completed", "halted"):
        rollout_pct = 100.0 if status == "completed" else round(float(fraction or 0) * 100.0, 2)
        rollout_mode = "completed" if status == "completed" else "halted"
    elif status == "draft":
        rollout_pct = 0.0
        rollout_mode = "draft"
    else:
        rollout_pct = 100.0
        rollout_mode = status or "unknown"

    version_name = (target.get("name") or "").strip()
    if not version_name and target.get("versionCodes"):
        version_name = "build " + ", ".join(str(c) for c in target["versionCodes"][:3])

    return {
        "ok": True,
        "live": True,
        "platform": "android",
        "version": version_name or "—",
        "track": "production",
        "release_status": target.get("status"),
        "rollout_pct": rollout_pct,
        "rollout_mode": rollout_mode,
        "user_fraction": fraction,
        "version_codes": target.get("versionCodes") or [],
        "source": "google_play_androidpublisher",
    }


def fetch_store_rollout(product_id: str) -> dict[str, Any]:
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": "unknown_product"}

    cache_key = f"rollout:{pid}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    meta = APP_PRODUCTS[pid]
    bundle = (meta.get("ios_bundle_id") or "").strip()
    package = (meta.get("android_package") or "").strip()

    ios = fetch_ios_rollout(bundle_id=bundle) if bundle else {"ok": False, "live": False, "message": "ios_bundle yok"}
    android = fetch_android_rollout(package_name=package) if package else {"ok": False, "live": False, "message": "package yok"}

    any_live = bool(ios.get("live") or android.get("live"))
    out = {
        "product": pid,
        "product_label": meta.get("label") or pid,
        "ios": ios,
        "android": android,
        "any_live": any_live,
        "source": "live" if any_live else "unavailable",
    }
    _cache_set(cache_key, out)
    return out
