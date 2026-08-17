"""Crash-free / ANR-free özet.

Android ANR: Play Console vitals tarama (overview + sürüm ANR sayfası).
Crash-free (son sürüm 24h + 7d): yalnızca S-Firebase Console tarama —
Play Reporting API yok.
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


def _fmt_free(pct: float | None, *, digits: int = 3) -> str | None:
    """Crash-free yüzdesi — 3 hane (99,994%). ANR tarafı digits=2 ile çağırır.

    Değerler 99,9x aralığında sıkıştığı için iki hane gerçek farkları gizliyordu.
    Yuvarlamanın %100 göstermesini engellemek için, istenen haneye göre hesaplanan
    eşiğin üstünde bir hane fazlasına çıkılır (2 hane → 99,995; 3 hane → 99,9995).
    """
    if pct is None:
        return None
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return None
    digits = max(0, int(digits))
    round_up_threshold = 100.0 - (0.5 * (10 ** -digits))
    if v >= round_up_threshold:
        return f"{v:.{digits + 1}f}".replace(".", ",") + "%"
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
    # Sayısal değer varsa gösterimi burada üretiyoruz: scrape'in kaydettiği hazır
    # metin iki haneli; hane sayısını tek yerden yönetelim ve eski kayıtlar için de
    # yeniden tarama beklemeden geçerli olsun.
    fmt = _fmt_free(pct) if isinstance(pct, (int, float)) else win.get("crash_free_fmt")
    if fmt is None and pct is None:
        return None
    ver = version or win.get("version")
    sess_pct = win.get("crash_free_sessions_pct")
    sess_fmt = (
        _fmt_free(sess_pct)
        if isinstance(sess_pct, (int, float))
        else win.get("crash_free_sessions_fmt")
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


# Bir pencere boşsa hangi sırayla yedeğe düşülecek — hücre "—" kalmasın.
_FB_WINDOW_FALLBACK: dict[str, tuple[str, ...]] = {
    "24h": ("24h", "7d", "30d", "90d"),
    "7d": ("7d", "30d", "90d", "24h"),
    "30d": ("30d", "90d", "7d", "24h"),
}
_FB_PERIOD_LABEL = {"24h": "24 saat", "7d": "7 gün", "30d": "30 gün", "90d": "90 gün"}


def _fb_window_by_key(block: dict[str, Any], windows: dict[str, Any], key: str) -> Any:
    if key == "24h":
        direct = block.get("latest_24h")
    elif key == "7d":
        direct = block.get("latest_7d")
    else:
        direct = None
    if isinstance(direct, dict) and direct.get("crash_free_pct") is not None:
        return direct
    win = windows.get(key)
    if isinstance(win, dict) and win.get("crash_free_pct") is not None:
        return win
    return direct if isinstance(direct, dict) else win


def _fb_window_from_series(block: dict[str, Any]) -> dict[str, Any] | None:
    """Son çare: günlük crash-free serisinin son dolu noktası."""
    series = block.get("series") if isinstance(block.get("series"), list) else []
    for item in reversed(series):
        if not isinstance(item, dict):
            continue
        pct = item.get("crash_free_pct")
        if pct is None:
            pct = item.get("value") if isinstance(item.get("value"), (int, float)) else None
        if isinstance(pct, (int, float)):
            return {
                "crash_free_pct": float(pct),
                "series": [item],
                "_carried_from_day": str(item.get("date") or item.get("day") or "")[:10],
            }
    return None


def _fb_kpi_with_fallback(
    block: dict[str, Any],
    windows: dict[str, Any],
    key: str,
    ver: str | None,
) -> dict[str, Any] | None:
    """İstenen pencere yoksa sırayla diğer pencerelere, sonra blok geneline ve seriye düş."""
    for candidate in _FB_WINDOW_FALLBACK.get(key, (key,)):
        win = _fb_window_by_key(block, windows, candidate)
        kpi = _fb_window_kpi(win if isinstance(win, dict) else None, period=key, version=ver)
        if kpi and kpi.get("crash_free_fmt"):
            if candidate != key:
                kpi["fallback_from"] = candidate
                kpi["extra"] = _compact_extra(
                    kpi.get("extra"), f"{_FB_PERIOD_LABEL.get(candidate, candidate)} verisi"
                )
            return kpi
    # Blok geneli (scrape'in kendi 7d→90d→24h yedeği)
    if block.get("crash_free_pct") is not None or block.get("crash_free_fmt"):
        kpi = _fb_window_kpi(
            {
                "crash_free_pct": block.get("crash_free_pct"),
                "crash_free_fmt": block.get("crash_free_fmt"),
                "crash_free_sessions_pct": block.get("crash_free_sessions_pct"),
                "crash_free_sessions_fmt": block.get("crash_free_sessions_fmt"),
                "series": block.get("series"),
            },
            period=key,
            version=ver,
        )
        if kpi and kpi.get("crash_free_fmt"):
            kpi["fallback_from"] = "latest"
            kpi["extra"] = _compact_extra(kpi.get("extra"), "son bilinen")
            return kpi
    carried = _fb_window_from_series(block)
    if carried:
        kpi = _fb_window_kpi(carried, period=key, version=ver)
        if kpi and kpi.get("crash_free_fmt"):
            day = carried.get("_carried_from_day") or ""
            kpi["fallback_from"] = "series"
            kpi["carried_from"] = day
            kpi["extra"] = _compact_extra(kpi.get("extra"), f"{day} verisi" if day else "son bilinen")
            return kpi
    return None


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
        # Pencere gelmediyse yedeğe düş — panelde "—" yerine son bilinen değer görünsün
        kpi24 = _fb_kpi_with_fallback(block, windows, "24h", ver)
        kpi7 = _fb_kpi_with_fallback(block, windows, "7d", ver)
        kpi30 = _fb_kpi_with_fallback(block, windows, "30d", ver)
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


def _anr_rate_from_cards(cards: Any) -> tuple[float | None, str | None]:
    """KPI kartlarından kullanıcı-algılanan ANR oranı (yüzde puanı)."""
    scored: list[tuple[int, float, str]] = []
    for card in cards or []:
        if not isinstance(card, dict):
            continue
        title = str(card.get("title") or "")
        value = str(card.get("value") or "").strip()
        if not value:
            continue
        if re.search(r"kilitlenme|crash", title, re.I) and not re.search(r"\banr\b", title, re.I):
            continue
        parsed = _parse_tr_pct(value)
        if parsed is None:
            continue
        has_pct = "%" in value
        has_rate_word = bool(re.search(r"oran|rate", title, re.I))
        has_anr = bool(re.search(r"\banr\b", title, re.I))
        if not has_pct and parsed >= 1:
            continue
        if not has_rate_word and not has_pct:
            continue
        score = 0
        if has_anr:
            score += 2
        if has_rate_word:
            score += 3
        if has_pct:
            score += 2
        if re.search(r"algılanan|perceived", title, re.I):
            score += 1
        scored.append((score, parsed, value))
    if not scored:
        return None, None
    scored.sort(key=lambda x: -x[0])
    return scored[0][1], scored[0][2]


def _anr_rate_from_crash_block(block: Any) -> tuple[float | None, str | None]:
    if not isinstance(block, dict):
        return None, None
    raw = block.get("summary_rate") or block.get("anr_rate")
    parsed = _parse_tr_pct(raw)
    if parsed is not None:
        return parsed, str(raw).strip()
    cards: list[Any] = []
    if isinstance(block.get("cards"), list):
        cards.extend(block["cards"])
    for cat in block.get("categories") or []:
        if isinstance(cat, dict) and isinstance(cat.get("cards"), list):
            cards.extend(cat["cards"])
    return _anr_rate_from_cards(cards)


def _anr_rate_from_overview_rows(rows: Any) -> tuple[float | None, str | None]:
    anr_rate = None
    anr_label = None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").lower()
        metric = str(row.get("metric") or "")
        if key != "anr" and not re.search(r"\banr\b", metric, re.I):
            continue
        val = _parse_tr_pct(row.get("value_28d"))
        if val is None:
            continue
        anr_rate = val
        anr_label = metric or "ANR oranı"
    return anr_rate, anr_label


def _latest_version_from_vitals(vitals: dict[str, Any]) -> tuple[str | None, str | None]:
    """(version_code, version_name) — en yeni tarama sürümü."""
    versions = vitals.get("versions") if isinstance(vitals.get("versions"), list) else []
    name_map = (
        vitals.get("version_name_map")
        if isinstance(vitals.get("version_name_map"), dict)
        else {}
    )
    code = None
    name = None
    if versions and isinstance(versions[0], dict):
        code = str(versions[0].get("code") or "").strip() or None
        name = str(versions[0].get("name") or "").strip() or None
    if not code:
        code = str(vitals.get("version_code") or "").strip() or None
    by_version = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    if not code and by_version:
        numeric = [k for k in by_version if str(k).isdigit()]
        if numeric:
            code = str(max(numeric, key=lambda x: int(x)))
    if code and not name:
        raw_name = name_map.get(code) or name_map.get(str(code))
        name = str(raw_name).strip() if raw_name else None
        if name:
            m = re.fullmatch(r"(\d{1,10})\s*\(([^)]+)\)", name)
            if m:
                name = m.group(2).strip() or name
    return code, name


def _anr_item_from_rate(
    *,
    code: str | None,
    name: str | None,
    anr_rate: float,
    source: str,
    period: str = "28d",
) -> dict[str, Any]:
    anr_free = _free_from_rate_pct(anr_rate)
    anr_rate_fmt = _fmt_rate_pct(anr_rate)
    return {
        "version_code": code,
        "version_name": name,
        "anr_rate_pct": anr_rate,
        "anr_free_pct": anr_free,
        "anr_free_fmt": _fmt_free(anr_free, digits=2),
        "anr_rate_fmt": anr_rate_fmt,
        "extra": _compact_extra(anr_rate_fmt) or None,
        "label": f"v{name}" if name else (f"code {code}" if code else "latest"),
        "period": period,
        "source": source,
        "crash_free_pct": None,
        "crash_free_fmt": None,
        "crash_free_source": "disabled_use_firebase_console",
    }


def play_versions_anr_from_vitals(vitals: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Sürüm bazlı ANR-free — Play vitals tarama (Reporting API yok)."""
    vitals = vitals if isinstance(vitals, dict) else {}
    name_map = (
        vitals.get("version_name_map")
        if isinstance(vitals.get("version_name_map"), dict)
        else {}
    )
    by_version = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    ov_by = (
        vitals.get("metrics_overview_by_version")
        if isinstance(vitals.get("metrics_overview_by_version"), dict)
        else {}
    )
    versions_meta = vitals.get("versions") if isinstance(vitals.get("versions"), list) else []
    name_by_code = {
        str(v.get("code")): str(v.get("name") or "").strip()
        for v in versions_meta
        if isinstance(v, dict) and v.get("code")
    }

    def _name_for(code: str) -> str | None:
        raw = name_by_code.get(code) or name_map.get(code) or name_map.get(str(code))
        if not raw:
            return None
        s = str(raw).strip()
        m = re.fullmatch(r"(\d{1,10})\s*\(([^)]+)\)", s)
        return (m.group(2).strip() if m else s) or None

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    codes: list[str] = []
    for v in versions_meta:
        if isinstance(v, dict) and v.get("code"):
            c = str(v["code"]).strip()
            if c and c not in seen:
                seen.add(c)
                codes.append(c)
    for k in by_version:
        c = str(k).strip()
        if c and c != "all" and c not in seen:
            seen.add(c)
            codes.append(c)
    for k in ov_by:
        c = str(k).strip()
        if c and c != "all" and c not in seen:
            seen.add(c)
            codes.append(c)

    codes.sort(key=lambda x: int(x) if x.isdigit() else -1, reverse=True)
    for code in codes:
        rate = None
        source = "play_vitals_scrape"
        ov = ov_by.get(code) if isinstance(ov_by.get(code), dict) else {}
        if ov:
            rate, _ = _anr_rate_from_overview_rows(ov.get("rows"))
            if rate is not None:
                source = "play_vitals_overview"
        payload = by_version.get(code) if isinstance(by_version.get(code), dict) else {}
        crashes = payload.get("crashes") if isinstance(payload.get("crashes"), dict) else payload
        anr_block = crashes.get("ANR") if isinstance(crashes, dict) else None
        if rate is None:
            rate, _ = _anr_rate_from_crash_block(anr_block)
        if rate is None:
            continue
        items.append(
            _anr_item_from_rate(
                code=code,
                name=_name_for(code),
                anr_rate=rate,
                source=source,
            )
        )
    return items


def _play_latest_anr_7d_from_vitals(
    vitals: dict[str, Any],
    *,
    code: str | None,
    name: str | None,
) -> dict[str, Any] | None:
    raw7 = vitals.get("anr_latest_7d")
    if not isinstance(raw7, dict):
        return None
    vc7 = str(raw7.get("version_code") or "").strip() or None
    block7 = raw7.get("block") if isinstance(raw7.get("block"), dict) else None
    if not block7:
        return None
    if code and vc7 and str(vc7) != str(code):
        return None
    rate, _ = _anr_rate_from_crash_block(block7)
    if rate is None:
        return None
    use_code = vc7 or code
    return _anr_item_from_rate(
        code=use_code,
        name=name,
        anr_rate=rate,
        source="play_vitals_scrape_7d",
        period="7d",
    )


def _play_latest_anr_7d_from_gp(
    package_name: str,
    *,
    code: str | None,
    name: str | None,
) -> dict[str, Any] | None:
    pkg = (package_name or "").strip()
    if not pkg:
        return None
    try:
        from backend.services.gp_client import fetch_version_anr_rate_7d

        row = fetch_version_anr_rate_7d(pkg, version_code=code)
    except Exception:
        logger.debug("GP 7d ANR fallback failed", exc_info=True)
        return None
    if not isinstance(row, dict):
        return None
    rate = row.get("anr_rate_pct")
    if rate is None:
        return None
    try:
        rate_f = float(rate)
    except (TypeError, ValueError):
        return None
    use_code = str(row.get("version_code") or code or "").strip() or code
    use_name = str(row.get("version_name") or name or "").strip() or name
    return _anr_item_from_rate(
        code=use_code,
        name=use_name,
        anr_rate=rate_f,
        source="reporting_api_7d",
        period="7d",
    )


def _play_latest_anr_28d_from_vitals(vitals: dict[str, Any] | None) -> dict[str, Any] | None:
    """Son sürüm ANR-free — vitals tarama (overview-by-version, ANR kartları, 28g)."""
    vitals = vitals if isinstance(vitals, dict) else {}
    items = play_versions_anr_from_vitals(vitals)
    code, name = _latest_version_from_vitals(vitals)
    if code:
        for item in items:
            if str(item.get("version_code") or "") == str(code):
                if name and not item.get("version_name"):
                    item = dict(item)
                    item["version_name"] = name
                    item["label"] = f"v{name}"
                return item
        # En yeni kod belli ama oranı yok — eski sürüme düşme
        top_anr = (
            (vitals.get("crashes") or {}).get("ANR")
            if isinstance(vitals.get("crashes"), dict)
            else None
        )
        top_code = (
            str(top_anr.get("version_code") or "").strip()
            if isinstance(top_anr, dict)
            else ""
        )
        if top_code == str(code) or not top_code:
            rate, _ = _anr_rate_from_crash_block(top_anr)
            if rate is not None:
                return _anr_item_from_rate(
                    code=code,
                    name=name,
                    anr_rate=rate,
                    source="play_vitals_scrape",
                    period="28d",
                )
        return None
    if items:
        return items[0]

    # Sürüm listesi yoksa birincil ANR bloğu (tarama en yeni sürümden başlar)
    top_anr = (vitals.get("crashes") or {}).get("ANR") if isinstance(vitals.get("crashes"), dict) else None
    rate, _ = _anr_rate_from_crash_block(top_anr)
    if rate is None:
        return None
    top_code = None
    if isinstance(top_anr, dict):
        top_code = str(top_anr.get("version_code") or "").strip() or None
    return _anr_item_from_rate(
        code=top_code or code,
        name=name,
        anr_rate=rate,
        source="play_vitals_scrape",
        period="28d",
    )


def play_latest_anr_from_vitals(
    vitals: dict[str, Any] | None,
    *,
    package_name: str | None = None,
) -> dict[str, Any] | None:
    """Son sürüm ANR-free — önce 7g vitals/API, yoksa 28g vitals."""
    vitals = vitals if isinstance(vitals, dict) else {}
    code, name = _latest_version_from_vitals(vitals)
    item7 = _play_latest_anr_7d_from_vitals(vitals, code=code, name=name)
    if item7:
        return item7
    item7 = _play_latest_anr_7d_from_gp(package_name or "", code=code, name=name)
    if item7:
        return item7
    return _play_latest_anr_28d_from_vitals(vitals)


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
    if anr_rate is None:
        all_block = (vitals.get("by_version") or {}).get("all") if isinstance(vitals.get("by_version"), dict) else None
        if isinstance(all_block, dict):
            crashes = all_block.get("crashes") if isinstance(all_block.get("crashes"), dict) else {}
            anr_rate, _ = _anr_rate_from_crash_block(crashes.get("ANR") if isinstance(crashes, dict) else None)
            if anr_rate is not None:
                anr_label = anr_label or "ANR oranı"
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
        "anr_free_fmt": _fmt_free(anr_free, digits=2),
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
    cache_key = f"{product_id}:{package_name}:sf:v6-play-anr-7d"
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

    play_versions = play_versions_anr_from_vitals(vitals)
    play_latest = play_latest_anr_from_vitals(vitals, package_name=package_name)
    if play_latest:
        play_latest = _strip_play_latest_crash_free(play_latest)
    play_err = None
    if not play_latest or not play_latest.get("anr_free_fmt"):
        play_err = "Son sürüm ANR oranı vitals taramasında yok"

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
