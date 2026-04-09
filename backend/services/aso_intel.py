from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from backend.services.app_intel import (
    APP_PRODUCTS,
    _fetch_google_bundle,
    _fetch_ios_lookup_meta,
    _filter_by_period_or_anchor,
    _satisfaction_split,
    build_intel_payload,
    get_cached_raw_product_data,
    get_raw_product_data,
    normalize_ios_app_id,
)
from backend.services.timezone_utils import inclusive_local_period_start_utc, report_calendar_today

_UTC = timezone.utc
_RANK_HISTORY_FILE = Path(__file__).resolve().parent / "aso_rank_history.json"
_RANK_LOCK = Lock()

_STOPWORDS = {
    "ve", "ile", "bir", "bu", "çok", "daha", "için", "gibi", "ama", "fakat", "çünkü", "de", "da", "ki",
    "the", "and", "for", "with", "that", "this", "from", "very", "you", "your",
    "app", "uygulama", "oldu", "olan", "olarak", "kadar", "gün", "sonra", "önce",
}


def _tokenize(text: str) -> list[str]:
    raw = re.findall(r"[a-zA-ZçğıöşüÇĞİÖŞÜ0-9]{3,}", (text or "").lower())
    out = [t for t in raw if t not in _STOPWORDS and not t.isdigit()]
    return out


def _dedupe_reviews(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda r: r.get("at") or datetime(1970, 1, 1, tzinfo=_UTC), reverse=True)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in ordered:
        txt = re.sub(r"\s+", " ", str(r.get("text") or "")).strip().lower()
        score = int(r.get("score") or 0)
        if txt:
            key = txt
        else:
            at = r.get("at")
            key = f"{at.isoformat() if isinstance(at, datetime) else at}\0{score}"
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "at": r.get("at"),
                "score": score,
                "text": str(r.get("text") or ""),
                "version": (str(r.get("version") or "").strip() or None),
            }
        )
    return out


def _period_filter(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return list(rows)
    start = inclusive_local_period_start_utc(n_calendar_days=days)
    if start is None:
        return list(rows)
    out: list[dict[str, Any]] = []
    for r in rows:
        at = r.get("at")
        if not isinstance(at, datetime):
            continue
        dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
        if dt >= start:
            out.append(r)
    return out


def _score_bucket(score: int) -> str:
    if score >= 4:
        return "positive"
    if score <= 2:
        return "negative"
    return "neutral"


def _extract_keywords(rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, float]] = {}
    for r in rows:
        s = int(r.get("score") or 0)
        for t in set(_tokenize(str(r.get("text") or ""))):
            rec = stats.setdefault(t, {"mentions": 0.0, "score_sum": 0.0})
            rec["mentions"] += 1.0
            rec["score_sum"] += float(s)
    out: list[dict[str, Any]] = []
    for kw, rec in stats.items():
        mentions = int(rec["mentions"])
        avg_score = round(rec["score_sum"] / max(1.0, rec["mentions"]), 2)
        intent = "fırsat" if avg_score >= 3.8 else ("risk" if avg_score <= 2.8 else "izle")
        out.append(
            {
                "keyword": kw,
                "mentions": mentions,
                "avg_score": avg_score,
                "intent": intent,
            }
        )
    out.sort(key=lambda x: (x["mentions"], x["avg_score"]), reverse=True)
    return out[:limit]


def _lang_mix(rows: list[dict[str, Any]]) -> dict[str, Any]:
    c = Counter()
    for r in rows:
        t = str(r.get("text") or "")
        if not t.strip():
            continue
        if re.search(r"[çğıöşüÇĞİÖŞÜ]", t):
            c["tr"] += 1
        elif re.search(r"\b(the|and|with|app|good|bad|update)\b", t.lower()):
            c["en"] += 1
        else:
            c["other"] += 1
    n = sum(c.values()) or 1
    return {
        "tr_pct": round(100.0 * c.get("tr", 0) / n, 1),
        "en_pct": round(100.0 * c.get("en", 0) / n, 1),
        "other_pct": round(100.0 * c.get("other", 0) / n, 1),
    }


def _release_impact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        at = r.get("at")
        s = int(r.get("score") or 0)
        if not isinstance(at, datetime):
            continue
        by_month[at.strftime("%Y-%m")].append(s)
    monthly = []
    for k in sorted(by_month.keys())[-6:]:
        vals = by_month[k]
        monthly.append({"month": k, "avg_score": round(sum(vals) / len(vals), 2), "count": len(vals)})
    cur_start = inclusive_local_period_start_utc(n_calendar_days=30)
    prev_start = inclusive_local_period_start_utc(n_calendar_days=60)
    prev_end_exclusive = inclusive_local_period_start_utc(n_calendar_days=30)
    recent: list[int] = []
    prev: list[int] = []
    for r in rows:
        at = r.get("at")
        if not isinstance(at, datetime):
            continue
        dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
        s = int(r.get("score") or 0)
        if cur_start and dt >= cur_start:
            recent.append(s)
        elif (
            prev_start
            and prev_end_exclusive
            and prev_start <= dt < prev_end_exclusive
        ):
            prev.append(s)
    recent_avg = round(sum(recent) / len(recent), 2) if recent else None
    prev_avg = round(sum(prev) / len(prev), 2) if prev else None
    delta = round((recent_avg or 0) - (prev_avg or 0), 2) if recent_avg is not None and prev_avg is not None else None
    return {"monthly": monthly, "recent30_avg": recent_avg, "prev30_avg": prev_avg, "delta": delta}


def _release_impact_by_version(rows: list[dict[str, Any]], *, min_live_days: int = 7) -> dict[str, Any] | None:
    by_ver: dict[str, dict[str, Any]] = {}
    for r in rows:
        ver = str(r.get("version") or "").strip()
        at = r.get("at")
        if not ver or not isinstance(at, datetime):
            continue
        rec = by_ver.setdefault(
            ver,
            {
                "version": ver,
                "first_at": at,
                "last_at": at,
                "count": 0,
                "score_sum": 0.0,
            },
        )
        if at < rec["first_at"]:
            rec["first_at"] = at
        if at > rec["last_at"]:
            rec["last_at"] = at
        rec["count"] = int(rec["count"]) + 1
        rec["score_sum"] = float(rec["score_sum"]) + float(int(r.get("score") or 0))

    versions: list[dict[str, Any]] = []
    for v in by_ver.values():
        cnt = int(v.get("count") or 0)
        if cnt <= 0:
            continue
        first_at = v.get("first_at")
        last_at = v.get("last_at")
        if not isinstance(first_at, datetime) or not isinstance(last_at, datetime):
            continue
        live_days = int((last_at - first_at).total_seconds() // 86400) + 1
        versions.append(
            {
                "version": v["version"],
                "first_at": first_at,
                "last_at": last_at,
                "live_days": live_days,
                "review_count": cnt,
                "avg_score": round(float(v["score_sum"]) / cnt, 2),
            }
        )
    if not versions:
        return None
    versions.sort(key=lambda x: x.get("first_at") or datetime(1970, 1, 1, tzinfo=_UTC))
    eligible = [v for v in versions if int(v.get("live_days") or 0) >= int(min_live_days)]
    if len(eligible) < 2:
        return None
    current = eligible[-1]
    previous = eligible[-2]
    cur_avg = float(current.get("avg_score"))
    prev_avg = float(previous.get("avg_score"))
    return {
        "mode": "release_compare",
        "min_live_days": int(min_live_days),
        "recent30_avg": round(cur_avg, 2),
        "prev30_avg": round(prev_avg, 2),
        "delta": round(cur_avg - prev_avg, 2),
        "recent_release": current,
        "prev_release": previous,
        "eligible_release_count": len(eligible),
    }


def _release_impact_by_release_window(
    rows: list[dict[str, Any]],
    *,
    current_version: str | None,
    current_release_date_iso: str | None,
    min_live_days: int = 7,
) -> dict[str, Any] | None:
    if not current_release_date_iso:
        return None
    try:
        rd = datetime.fromisoformat(str(current_release_date_iso).replace("Z", "+00:00"))
        if rd.tzinfo is None:
            rd = rd.replace(tzinfo=_UTC)
    except Exception:
        return None
    now = datetime.now(tz=_UTC)
    if int((now - rd).total_seconds() // 86400) + 1 < int(min_live_days):
        return None
    recent_start = rd
    recent_scores: list[int] = []
    prev_scores: list[int] = []
    prev_dates: list[datetime] = []
    for r in rows:
        at = r.get("at")
        if not isinstance(at, datetime):
            continue
        dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
        sc = int(r.get("score") or 0)
        if recent_start <= dt:
            recent_scores.append(sc)
        elif dt < rd:
            prev_scores.append(sc)
            prev_dates.append(dt)
    if not recent_scores or not prev_scores:
        return None
    prev_live_days = int((rd - min(prev_dates)).total_seconds() // 86400) + 1 if prev_dates else 0
    if prev_live_days < int(min_live_days):
        return None
    recent_avg = round(sum(recent_scores) / len(recent_scores), 2)
    prev_avg = round(sum(prev_scores) / len(prev_scores), 2)
    return {
        "mode": "release_window_compare",
        "min_live_days": int(min_live_days),
        "recent30_avg": recent_avg,
        "prev30_avg": prev_avg,
        "delta": round(recent_avg - prev_avg, 2),
        "recent_release": {
            "version": current_version,
            "first_at": recent_start,
            "last_at": now,
            "live_days": int((now - rd).total_seconds() // 86400) + 1,
            "review_count": len(recent_scores),
            "avg_score": recent_avg,
        },
        "prev_release": {
            "version": None,
            "first_at": (min(prev_dates) if prev_dates else None),
            "last_at": rd,
            "live_days": prev_live_days,
            "review_count": len(prev_scores),
            "avg_score": prev_avg,
        },
    }


def _release_impact_sections_from_raw(raw: dict[str, Any], *, store: dict[str, Any] | None = None) -> dict[str, Any]:
    """Dönem filtresi olmadan çekilen örnek yorumlardan 30g / önceki 30g skor eğilimi (Play + App Store)."""
    a_rows = _dedupe_reviews(list(raw.get("android", {}).get("reviews") or []))
    i_rows = _dedupe_reviews(list(raw.get("ios", {}).get("reviews") or []))
    a_rel = _release_impact_by_version(a_rows, min_live_days=7)
    i_rel = _release_impact_by_version(i_rows, min_live_days=7)
    if i_rel is None:
        ios_store = (store or {}).get("ios") or {}
        i_rel = _release_impact_by_release_window(
            i_rows,
            current_version=ios_store.get("version"),
            current_release_date_iso=(
                ios_store.get("current_version_release_date")
                or ios_store.get("currentVersionReleaseDate")
            ),
            min_live_days=7,
        )
    return {
        "android": (a_rel if a_rel is not None else _release_impact(a_rows)),
        "ios": (i_rel if i_rel is not None else _release_impact(i_rows)),
    }


def _release_impact_product_bundle(product_key: str, *, raw: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = raw or get_raw_product_data(product_key, force_refresh=False)
    if raw.get("error"):
        spec = APP_PRODUCTS.get(product_key) or {}
        return {
            "product_id": product_key,
            "label": spec.get("label", product_key),
            "platforms": {
                "android": {"monthly": [], "recent30_avg": None, "prev30_avg": None, "delta": None},
                "ios": {"monthly": [], "recent30_avg": None, "prev30_avg": None, "delta": None},
            },
            "store": {
                "android": {"version": None, "last_updated_at": None},
                "ios": {"version": None, "current_version_release_date": None},
            },
        }
    am = raw.get("android", {}).get("meta") or {}
    im = raw.get("ios", {}).get("meta") or {}
    store_block = {
        "android": {
            "version": am.get("play_version"),
            "last_updated_at": am.get("play_last_updated_at"),
        },
        "ios": {
            "version": im.get("version"),
            "current_version_release_date": im.get("currentVersionReleaseDate")
            or im.get("current_version_release_date"),
        },
    }
    return {
        "product_id": product_key,
        "label": raw.get("label") or product_key,
        "platforms": _release_impact_sections_from_raw(raw, store=store_block),
        "store": store_block,
    }


def _conversion_funnel(
    store_ratings: int | None,
    period_reviews: int,
    *,
    period_days: int,
    android_ratings: int | None,
    ios_ratings: int | None,
    android_score: float | None,
    ios_score: float | None,
) -> dict[str, Any]:
    ratings = max(0, int(store_ratings or 0))
    reviews = max(0, int(period_reviews or 0))

    def _pct(num: int | float, den: int | float) -> float | None:
        try:
            d = float(den)
            if d <= 0:
                return None
            return round((float(num) / d) * 100.0, 2)
        except Exception:
            return None

    pdays = max(1, int(period_days or 30))
    rating_to_review = _pct(reviews, ratings)
    reviews_daily = round(reviews / pdays, 2) if reviews > 0 else None
    android_r = int(android_ratings or 0)
    ios_r = int(ios_ratings or 0)
    mix_score = _mix_score(android_score, ios_score) if (android_score is not None or ios_score is not None) else None

    return {
        "ratings_total": ratings,
        "reviews_period": reviews,
        "kpis": {
            "reviews_to_ratings_pct": rating_to_review,
            "reviews_daily": reviews_daily,
            "android_ratings_share_pct": _pct(android_r, ratings),
            "ios_ratings_share_pct": _pct(ios_r, ratings),
        },
        "store_signals": {
            "period_days": pdays,
            "rating_mix_score": round(float(mix_score), 2) if mix_score is not None else None,
            "android": {
                "ratings": android_r,
                "score": float(android_score) if android_score is not None else None,
            },
            "ios": {
                "ratings": ios_r,
                "score": float(ios_score) if ios_score is not None else None,
            },
        },
    }


def _load_rank_history() -> dict[str, Any]:
    if not _RANK_HISTORY_FILE.exists():
        return {}
    try:
        return json.loads(_RANK_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_rank_history(data: dict[str, Any]) -> None:
    try:
        _RANK_HISTORY_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _update_rank_history(product_id: str, keyword_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = report_calendar_today().isoformat()
    rank_rows = []
    for i, kw in enumerate(keyword_rows[:15], start=1):
        rank = max(1, int(round(80 - min(60, kw["mentions"] * 2.3) - (kw["avg_score"] - 3.0) * 8)))
        rank_rows.append({"keyword": kw["keyword"], "rank": rank, "mentions": kw["mentions"]})
    with _RANK_LOCK:
        data = _load_rank_history()
        ph = data.setdefault(product_id, [])
        if not any(str(x.get("date")) == today for x in ph):
            ph.append({"date": today, "ranks": rank_rows})
            ph[:] = ph[-45:]
            _save_rank_history(data)
        history = data.get(product_id, [])
    trend: dict[str, list[dict[str, Any]]] = {}
    for d in history[-14:]:
        date = str(d.get("date"))
        for r in d.get("ranks") or []:
            k = str(r.get("keyword") or "")
            if not k:
                continue
            trend.setdefault(k, []).append({"date": date, "rank": int(r.get("rank") or 0)})
    result = []
    for kw in rank_rows[:10]:
        k = kw["keyword"]
        points = trend.get(k, [])
        delta7 = None
        if len(points) >= 2:
            delta7 = points[-1]["rank"] - points[max(0, len(points) - 7)]["rank"]
        result.append({"keyword": k, "current_rank": kw["rank"], "delta7": delta7, "series": points[-14:]})
    return result


def _mix_score(android_score: Any, ios_score: Any) -> float:
    vals: list[float] = []
    try:
        a = float(android_score) if android_score is not None else None
    except Exception:
        a = None
    try:
        i = float(ios_score) if ios_score is not None else None
    except Exception:
        i = None
    if a is not None and a > 0:
        vals.append(a)
    if i is not None and i > 0:
        vals.append(i)
    if not vals:
        return 0.0
    return round(sum(vals) / len(vals), 2)


def _safe_rank_fields(rank_obj: Any) -> dict[str, Any]:
    rank = rank_obj if isinstance(rank_obj, dict) else {}
    return {
        "rank": rank.get("rank"),
        "total": rank.get("total"),
        "chart": rank.get("chart"),
    }


def _category_rank_summary(period_days: int, *, active_product_id: str, active_window: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pid in APP_PRODUCTS.keys():
        if pid == active_product_id:
            android = (active_window or {}).get("android") or {}
            ios = (active_window or {}).get("ios") or {}
            out[pid] = {
                "android": {"category": android.get("store_category_name"), **_safe_rank_fields(android.get("store_category_rank"))},
                "ios": {"category": ios.get("store_category_name"), **_safe_rank_fields(ios.get("store_category_rank"))},
            }
            continue
        # Soğuk açılışta diğer ürünü dış kaynaktan fetch etmeyelim.
        cached = get_cached_raw_product_data(pid) or {}
        am = (cached.get("android") or {}).get("meta") or {}
        im = (cached.get("ios") or {}).get("meta") or {}
        out[pid] = {
            "android": {"category": am.get("genre"), **_safe_rank_fields(am.get("category_rank"))},
            "ios": {"category": im.get("primary_genre_name"), **_safe_rank_fields(im.get("category_rank"))},
        }
    return out


def _compare_entry(
    base_product_id: str,
    period_days: int,
    compare_product: str | None,
    compare_label: str | None,
    compare_android_package: str | None,
    compare_ios_app_id: str | None,
) -> dict[str, Any] | None:
    cp = (compare_product or "").strip().lower()
    if cp and cp in APP_PRODUCTS and cp != base_product_id:
        p = build_intel_payload(cp, period_days, force_refresh=False)
        w = p.get("active_window") or {}
        a = w.get("android") or {}
        i = w.get("ios") or {}
        return {
            "product_id": cp,
            "label": APP_PRODUCTS[cp].get("label"),
            "store_score_mix": _mix_score(a.get("store_score"), i.get("store_score")),
            "review_count_period": int(a.get("review_count_period") or 0) + int(i.get("review_count_period") or 0),
            "satisfaction_mix": round(
                (float((a.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5)
                + (float((i.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5),
                1,
            ),
            "source": "tracked_product",
        }
    pkg = (compare_android_package or "").strip()
    ios_id = normalize_ios_app_id(compare_ios_app_id)
    if not pkg and not ios_id:
        return None

    # Custom giriş aslında takipli bir ürüne denk geliyorsa tracked akışını kullan.
    for tp, spec in APP_PRODUCTS.items():
        if tp == base_product_id:
            continue
        if pkg and _pkg_eq(pkg, str(spec.get("android_package") or "")):
            p = build_intel_payload(tp, period_days, force_refresh=False)
            w = p.get("active_window") or {}
            a = w.get("android") or {}
            i = w.get("ios") or {}
            return {
                "product_id": tp,
                "label": (compare_label or "").strip() or spec.get("label"),
                "store_score_mix": _mix_score(a.get("store_score"), i.get("store_score")),
                "review_count_period": int(a.get("review_count_period") or 0) + int(i.get("review_count_period") or 0),
                "satisfaction_mix": round(
                    (float((a.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5)
                    + (float((i.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5),
                    1,
                ),
                "source": "tracked_product",
            }
        if ios_id and _ios_eq(ios_id, str(spec.get("ios_app_id") or "")):
            p = build_intel_payload(tp, period_days, force_refresh=False)
            w = p.get("active_window") or {}
            a = w.get("android") or {}
            i = w.get("ios") or {}
            return {
                "product_id": tp,
                "label": (compare_label or "").strip() or spec.get("label"),
                "store_score_mix": _mix_score(a.get("store_score"), i.get("store_score")),
                "review_count_period": int(a.get("review_count_period") or 0) + int(i.get("review_count_period") or 0),
                "satisfaction_mix": round(
                    (float((a.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5)
                    + (float((i.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5),
                    1,
                ),
                "source": "tracked_product",
            }

    g_meta, _rows, _err = (_fetch_google_bundle(pkg, max_reviews=200) if pkg else ({}, [], None))
    i_meta = _fetch_ios_lookup_meta(ios_id) if ios_id else {}
    return {
        "product_id": "custom",
        "label": (compare_label or "").strip() or "Karsilastirma uygulamasi",
        "store_score_mix": _mix_score(g_meta.get("score"), i_meta.get("score")),
        "review_count_period": 0,
        "satisfaction_mix": None,
        "source": "custom",
        "android_package": pkg or None,
        "ios_app_id": ios_id or None,
    }


def _parse_pair(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = [x.strip() for x in re.split(r"[,\n;|]+", raw) if x.strip()]
    out: list[str] = []
    for p in parts:
        if p not in out:
            out.append(p)
    return out[:4]


def _normalize_android_package(raw: str | None) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    m = re.search(r"[?&]id=([A-Za-z0-9._]+)", s)
    if m:
        s = m.group(1)
    s = s.strip(" ,;|")
    s = re.sub(r"\s+", "", s)
    return s


def _android_pkg_candidates(raw: str | None) -> list[str]:
    base = _normalize_android_package(raw)
    if not base:
        return []
    cands = [base]
    low = base.lower()
    if low.startswith("ct.") and "." in base:
        cands.append("com." + base.split(".", 1)[1])
    if low.startswith("co.") and "." in base:
        cands.append("com." + base.split(".", 1)[1])
    if not low.startswith("com.") and "." in base:
        cands.append("com." + base.split(".", 1)[1])
    out: list[str] = []
    seen: set[str] = set()
    for c in cands:
        k = c.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(c.strip())
    return out


def _fetch_google_bundle_resilient(pkg_raw: str | None, *, max_reviews: int) -> tuple[dict[str, Any], list[dict[str, Any]], str | None, str | None]:
    last_err: str | None = None
    for cand in _android_pkg_candidates(pkg_raw):
        meta, rows, err = _fetch_google_bundle(cand, max_reviews=max_reviews)
        has_signal = bool(meta.get("score") or meta.get("ratings") or rows)
        if has_signal:
            return meta, rows, err, cand
        if err:
            last_err = err
    return {}, [], last_err, None


def _play_store_icon(meta: dict[str, Any]) -> str | None:
    for key in ("icon", "iconUrl"):
        v = meta.get(key)
        if v:
            s = str(v).strip()
            if s:
                return s
    return None


def _pkg_eq(a: str | None, b: str | None) -> bool:
    na = _normalize_android_package(a).lower()
    nb = _normalize_android_package(b).lower()
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Prefix varyasyon toleransı (ct.xxx vs com.xxx gibi)
    if "." in na and "." in nb and na.split(".", 1)[1] == nb.split(".", 1)[1]:
        return True
    return False


def _ios_eq(a: str | None, b: str | None) -> bool:
    na, nb = normalize_ios_app_id(a), normalize_ios_app_id(b)
    return bool(na) and na == nb


def _benchmark_metrics_tracked(
    a: dict[str, Any],
    i: dict[str, Any],
    *,
    use_android: bool,
    use_ios: bool,
) -> tuple[float, int, int, float | None]:
    """(primary_score, ratings_total, review_count_period, satisfaction_pct)."""
    android_score = a.get("store_score")
    ios_score = i.get("store_score")
    mix = _mix_score(android_score, ios_score)
    if use_android and not use_ios:
        try:
            ps = float(android_score) if android_score is not None else mix
        except (TypeError, ValueError):
            ps = mix
        sm = float((a.get("satisfaction") or {}).get("memnun_oran") or 0)
        return (
            round(ps, 2),
            int(a.get("store_ratings") or 0),
            int(a.get("review_count_period") or 0),
            round(sm, 1),
        )
    if use_ios and not use_android:
        try:
            ps = float(ios_score) if ios_score is not None else mix
        except (TypeError, ValueError):
            ps = mix
        sm = float((i.get("satisfaction") or {}).get("memnun_oran") or 0)
        return (
            round(ps, 2),
            int(i.get("store_ratings_count") or 0),
            int(i.get("review_count_period") or 0),
            round(sm, 1),
        )
    # her iki mağaza da formda dolduruldu: ortalama skor + birleşik metrikler
    smix = round(
        (float((a.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5)
        + (float((i.get("satisfaction") or {}).get("memnun_oran") or 0) * 0.5),
        1,
    )
    return (
        round(mix, 2),
        int(a.get("store_ratings") or 0) + int(i.get("store_ratings_count") or 0),
        int(a.get("review_count_period") or 0) + int(i.get("review_count_period") or 0),
        smix,
    )


def _platform_eval_block(
    *,
    android_score: float | None,
    android_ratings: int | None,
    android_review_count_period: int | None,
    android_satisfaction: float | None,
    ios_score: float | None,
    ios_ratings: int | None,
    ios_review_count_period: int | None,
    ios_satisfaction: float | None,
) -> dict[str, Any]:
    return {
        "android": {
            "score": (float(android_score) if android_score is not None else None),
            "ratings": (int(android_ratings or 0)),
            "review_count_period": (int(android_review_count_period) if android_review_count_period is not None else None),
            "satisfaction": (round(float(android_satisfaction), 1) if android_satisfaction is not None else None),
        },
        "ios": {
            "score": (float(ios_score) if ios_score is not None else None),
            "ratings": (int(ios_ratings or 0)),
            "review_count_period": (int(ios_review_count_period) if ios_review_count_period is not None else None),
            "satisfaction": (round(float(ios_satisfaction), 1) if ios_satisfaction is not None else None),
        },
    }


def _entry_from_ids(
    period_days: int,
    *,
    android_pkg: str | None,
    ios_id: str | None,
    label: str | None = None,
    quick_mode: bool = False,
) -> dict[str, Any]:
    pkg = _normalize_android_package(android_pkg)
    iid = normalize_ios_app_id(ios_id)
    has_p = bool(pkg)
    has_i = bool(iid)
    # tracked app (package / id eşleşmesi büyük-küçük harf duyarsız paket)
    for pid, spec in APP_PRODUCTS.items():
        is_pkg_match = has_p and _pkg_eq(pkg, str(spec.get("android_package") or ""))
        is_ios_match = (not has_p) and has_i and _ios_eq(iid, str(spec.get("ios_app_id") or ""))
        if is_pkg_match or is_ios_match:
            if quick_mode:
                g_meta_q, q_rows, _ = _fetch_google_bundle(str(spec.get("android_package") or ""), max_reviews=40)
                i_meta_q = _fetch_ios_lookup_meta(str(spec.get("ios_app_id") or ""))
                a_sc_q = g_meta_q.get("score")
                i_sc_q = i_meta_q.get("score")
                use_a = has_p and not has_i
                use_i = has_i and not has_p
                if use_a:
                    score_disp = round(float(a_sc_q), 2) if a_sc_q is not None else round(_mix_score(a_sc_q, i_sc_q), 2)
                    score_basis = "play"
                    ratings_total = int(g_meta_q.get("ratings") or 0)
                elif use_i:
                    score_disp = round(float(i_sc_q), 2) if i_sc_q is not None else round(_mix_score(a_sc_q, i_sc_q), 2)
                    score_basis = "app_store"
                    ratings_total = int(i_meta_q.get("ratings_count") or 0)
                else:
                    score_disp = round(_mix_score(a_sc_q, i_sc_q), 2)
                    score_basis = "ortalama"
                    ratings_total = int(g_meta_q.get("ratings") or 0) + int(i_meta_q.get("ratings_count") or 0)
                pd = int(period_days) if int(period_days) > 0 else 30
                q_filtered, _, _ = _filter_by_period_or_anchor(q_rows, pd)
                q_rev_c = len(q_filtered) if q_filtered else None
                q_sat = None
                if q_filtered:
                    q_sat = round(float((_satisfaction_split(q_filtered).get("memnun_oran") or 0)), 1)
                return {
                    "product_id": pid,
                    "label": (label or "").strip() or spec.get("label") or pid,
                    "source": "tracked_product",
                    "android_package": spec.get("android_package"),
                    "ios_app_id": spec.get("ios_app_id"),
                    "icon_url": _play_store_icon(g_meta_q) or i_meta_q.get("icon"),
                    "store_score_mix": score_disp,
                    "android_score": (a_sc_q if use_a else None),
                    "ios_score": (i_sc_q if use_i else None),
                    "android_ratings": (int(g_meta_q.get("ratings") or 0) if use_a else 0),
                    "ios_ratings": (int(i_meta_q.get("ratings_count") or 0) if use_i else 0),
                    "score_basis": score_basis,
                    "ratings_total": ratings_total,
                    "review_count_period": q_rev_c,
                    "satisfaction_mix": q_sat,
                    "platform_eval": _platform_eval_block(
                        android_score=(a_sc_q if use_a else None),
                        android_ratings=(int(g_meta_q.get("ratings") or 0) if use_a else 0),
                        android_review_count_period=(q_rev_c if use_a else None),
                        android_satisfaction=(q_sat if use_a else None),
                        ios_score=(i_sc_q if use_i else None),
                        ios_ratings=(int(i_meta_q.get("ratings_count") or 0) if use_i else 0),
                        ios_review_count_period=(q_rev_c if use_i else None),
                        ios_satisfaction=(q_sat if use_i else None),
                    ),
                }
            p = build_intel_payload(pid, period_days, force_refresh=False)
            w = p.get("active_window") or {}
            a = w.get("android") or {}
            i = w.get("ios") or {}
            if has_p and not has_i:
                use_a, use_i = True, False
            elif has_i and not has_p:
                use_a, use_i = False, True
            else:
                use_a, use_i = True, True
            score_disp, rtot, rpc, sat = _benchmark_metrics_tracked(a, i, use_android=use_a, use_ios=use_i)
            a_sat = (a.get("satisfaction") or {}).get("memnun_oran")
            i_sat = (i.get("satisfaction") or {}).get("memnun_oran")
            return {
                "product_id": pid,
                "label": (label or "").strip() or spec.get("label") or pid,
                "source": "tracked_product",
                "android_package": spec.get("android_package"),
                "ios_app_id": spec.get("ios_app_id"),
                "icon_url": p.get("app_icon"),
                "store_score_mix": score_disp,
                "android_score": (a.get("store_score") if use_a else None),
                "ios_score": (i.get("store_score") if use_i else None),
                "android_ratings": (int(a.get("store_ratings") or 0) if use_a else 0),
                "ios_ratings": (int(i.get("store_ratings_count") or 0) if use_i else 0),
                "score_basis": "play" if use_a and not use_i else ("app_store" if use_i and not use_a else "ortalama"),
                "ratings_total": rtot,
                "review_count_period": rpc,
                "satisfaction_mix": sat,
                "platform_eval": _platform_eval_block(
                    android_score=(a.get("store_score") if use_a else None),
                    android_ratings=(int(a.get("store_ratings") or 0) if use_a else 0),
                    android_review_count_period=(int(a.get("review_count_period") or 0) if use_a else None),
                    android_satisfaction=((float(a_sat) if a_sat is not None else None) if use_a else None),
                    ios_score=(i.get("store_score") if use_i else None),
                    ios_ratings=(int(i.get("store_ratings_count") or 0) if use_i else 0),
                    ios_review_count_period=(int(i.get("review_count_period") or 0) if use_i else None),
                    ios_satisfaction=((float(i_sat) if i_sat is not None else None) if use_i else None),
                ),
            }
    # özel uygulama
    g_meta: dict[str, Any] = {}
    g_rows: list[dict[str, Any]] = []
    if pkg:
        # quick_mode: hafif örneklemle (yalnız gerçek yorum) dönem/memnuniyet sinyali üret.
        g_meta, g_rows, _, used_pkg = _fetch_google_bundle_resilient(pkg, max_reviews=(40 if quick_mode else 80))
        if used_pkg:
            pkg = used_pkg
    i_meta = _fetch_ios_lookup_meta(iid) if iid else {}
    a_sc = g_meta.get("score")
    i_sc = i_meta.get("score")
    if has_p and not has_i:
        try:
            score_disp = round(float(a_sc), 2) if a_sc is not None else round(_mix_score(a_sc, i_sc), 2)
        except (TypeError, ValueError):
            score_disp = round(_mix_score(a_sc, i_sc), 2)
        pd = int(period_days) if int(period_days) > 0 else 30
        filtered, _, _ = _filter_by_period_or_anchor(g_rows, pd)
        rev_c = len(filtered) if filtered else None
        sat_d = _satisfaction_split(filtered) if filtered else {}
        sat_pct = round(float(sat_d.get("memnun_oran") or 0), 1) if filtered else None
        no_store_signal = (a_sc is None) and (int(g_meta.get("ratings") or 0) == 0)
        return {
            "product_id": "custom",
            "label": (label or "").strip() or (pkg or iid or "Özel uygulama"),
            "source": "not_found" if no_store_signal else "custom",
            "android_package": pkg or None,
            "ios_app_id": iid or None,
            "icon_url": _play_store_icon(g_meta),
            "store_score_mix": (None if no_store_signal else score_disp),
            "android_score": a_sc,
            "ios_score": None,
            "android_ratings": int(g_meta.get("ratings") or 0),
            "ios_ratings": 0,
            "score_basis": "play",
            "ratings_total": (None if no_store_signal else int(g_meta.get("ratings") or 0)),
            "review_count_period": rev_c,
            "satisfaction_mix": sat_pct,
            "platform_eval": _platform_eval_block(
                android_score=a_sc,
                android_ratings=int(g_meta.get("ratings") or 0),
                android_review_count_period=rev_c,
                android_satisfaction=sat_pct,
                ios_score=None,
                ios_ratings=0,
                ios_review_count_period=None,
                ios_satisfaction=None,
            ),
            "note": ("paket_bulunamadi" if no_store_signal else None),
        }
    if has_i and not has_p:
        try:
            score_disp = round(float(i_sc), 2) if i_sc is not None else 0.0
        except (TypeError, ValueError):
            score_disp = 0.0
        no_store_signal = (i_sc is None) and (int(i_meta.get("ratings_count") or 0) == 0)
        return {
            "product_id": "custom",
            "label": (label or "").strip() or (pkg or iid or "Özel uygulama"),
            "source": "not_found" if no_store_signal else "custom",
            "android_package": pkg or None,
            "ios_app_id": iid or None,
            "icon_url": i_meta.get("icon"),
            "store_score_mix": (None if no_store_signal else score_disp),
            "android_score": None,
            "ios_score": i_sc,
            "android_ratings": 0,
            "ios_ratings": int(i_meta.get("ratings_count") or 0),
            "score_basis": "app_store",
            "ratings_total": (None if no_store_signal else int(i_meta.get("ratings_count") or 0)),
            "review_count_period": None,
            "satisfaction_mix": None,
            "platform_eval": _platform_eval_block(
                android_score=None,
                android_ratings=0,
                android_review_count_period=None,
                android_satisfaction=None,
                ios_score=i_sc,
                ios_ratings=int(i_meta.get("ratings_count") or 0),
                ios_review_count_period=None,
                ios_satisfaction=None,
            ),
            "note": ("paket_bulunamadi" if no_store_signal else None),
        }
    # her iki kimlik de verildi (özel çapraz): skor ortalaması; dönem yorumu yalnız Play örnekleminden
    pd = int(period_days) if int(period_days) > 0 else 30
    if g_rows:
        filtered, _, _ = _filter_by_period_or_anchor(g_rows, pd)
    else:
        filtered = []
    rev_c = len(filtered) if filtered else None
    sat_pct = None
    if filtered:
        sat_pct = round(float((_satisfaction_split(filtered).get("memnun_oran") or 0)), 1)
    ratings_total = int(g_meta.get("ratings") or 0) + int(i_meta.get("ratings_count") or 0)
    no_store_signal = (a_sc is None and i_sc is None and ratings_total == 0)
    return {
        "product_id": "custom",
        "label": (label or "").strip() or (pkg or iid or "Özel uygulama"),
        "source": "not_found" if no_store_signal else "custom",
        "android_package": pkg or None,
        "ios_app_id": iid or None,
        "icon_url": _play_store_icon(g_meta) or i_meta.get("icon"),
        "store_score_mix": (None if no_store_signal else round(_mix_score(a_sc, i_sc), 2)),
        "android_score": a_sc,
        "ios_score": i_sc,
        "android_ratings": int(g_meta.get("ratings") or 0),
        "ios_ratings": int(i_meta.get("ratings_count") or 0),
        "score_basis": "ortalama",
        "ratings_total": (None if no_store_signal else ratings_total),
        "review_count_period": rev_c,
        "satisfaction_mix": sat_pct,
        "platform_eval": _platform_eval_block(
            android_score=a_sc,
            android_ratings=int(g_meta.get("ratings") or 0),
            android_review_count_period=rev_c,
            android_satisfaction=sat_pct,
            ios_score=i_sc,
            ios_ratings=int(i_meta.get("ratings_count") or 0),
            ios_review_count_period=None,
            ios_satisfaction=None,
        ),
        "note": ("paket_bulunamadi" if no_store_signal else None),
    }


def build_competitor_pair_payload(
    *,
    period_days: int,
    android_packages: str | None,
    ios_app_ids: str | None,
    labels: str | None = None,
) -> dict[str, Any]:
    a_pkg = _parse_pair(android_packages)
    a_ios = _parse_pair(ios_app_ids)
    a_lbl = _parse_pair(labels)
    n_slots = max(len(a_pkg), len(a_ios), len(a_lbl))
    if n_slots < 2:
        return {"error": "pair_required"}
    n = min(4, n_slots)
    entries: list[dict[str, Any]] = []
    idx_inputs: list[tuple[int, str | None, str | None, str | None]] = []
    for i in range(n):
        pkg = a_pkg[i] if i < len(a_pkg) else None
        iid = a_ios[i] if i < len(a_ios) else None
        lbl = a_lbl[i] if i < len(a_lbl) else None
        if not (pkg or iid):
            continue
        idx_inputs.append((i, pkg, iid, lbl))
    if not idx_inputs:
        return {"error": "pair_required"}
    out_by_idx: dict[int, dict[str, Any]] = {}
    quick_mode = len(idx_inputs) > 2
    timeout_sec = 20 if quick_mode else 45
    pool = ThreadPoolExecutor(max_workers=min(4, len(idx_inputs)))
    try:
        futs = {
            pool.submit(_entry_from_ids, period_days, android_pkg=pkg, ios_id=iid, label=lbl, quick_mode=quick_mode): i
            for i, pkg, iid, lbl in idx_inputs
        }
        done, not_done = wait(set(futs.keys()), timeout=timeout_sec)
        for fut in done:
            idx = futs[fut]
            try:
                out_by_idx[idx] = fut.result()
            except Exception:
                out_by_idx[idx] = {}
        # Dış mağaza çağrısı aşırı uzarsa UI'nın kilitlenmemesi için placeholder dön.
        for fut in not_done:
            idx = futs[fut]
            _i, pkg, iid, lbl = next((x for x in idx_inputs if x[0] == idx), (idx, None, None, None))
            out_by_idx[idx] = {
                "product_id": "custom",
                "label": (lbl or "").strip() or (pkg or iid or f"Uygulama {idx+1}"),
                "source": "timeout",
                "android_package": pkg or None,
                "ios_app_id": iid or None,
                "icon_url": None,
                "store_score_mix": None,
                "android_score": None,
                "ios_score": None,
                "score_basis": "—",
                "ratings_total": None,
                "review_count_period": None,
                "satisfaction_mix": None,
                "platform_eval": _platform_eval_block(
                    android_score=None,
                    android_ratings=0,
                    android_review_count_period=None,
                    android_satisfaction=None,
                    ios_score=None,
                    ios_ratings=0,
                    ios_review_count_period=None,
                    ios_satisfaction=None,
                ),
                "note": "veri_zaman_asimi",
            }
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    entries = [out_by_idx[i] for i, *_ in idx_inputs if i in out_by_idx]
    if len(entries) < 2:
        return {"error": "pair_required"}
    ranked = sorted(entries, key=lambda e: float(e.get("store_score_mix") or 0), reverse=True)
    leader = ranked[0]
    leader_score = float(leader.get("store_score_mix") or 0)
    leader_ratings = int(leader.get("ratings_total") or 0)
    for e in entries:
        e["delta_from_leader_score"] = round(float(e.get("store_score_mix") or 0) - leader_score, 2)
        e["delta_from_leader_ratings"] = int(e.get("ratings_total") or 0) - leader_ratings
    out = {
        "period_days": period_days,
        "entries": entries,
        "comparison": {
            "leader_label": leader.get("label"),
            "leader_score": round(leader_score, 2),
            "leader_ratings_total": leader_ratings,
            "entry_count": len(entries),
        },
    }
    # Backward-compat (ilk iki kartı bekleyen eski istemciler için)
    if len(entries) >= 2:
        left, right = entries[0], entries[1]
        out["left"] = left
        out["right"] = right
        out["comparison"]["delta_score"] = round(float(left.get("store_score_mix") or 0) - float(right.get("store_score_mix") or 0), 2)
        out["comparison"]["delta_ratings_total"] = int(left.get("ratings_total") or 0) - int(right.get("ratings_total") or 0)
    return out


def build_aso_payload(
    product_id: str,
    period_days: int,
    *,
    force_refresh: bool = False,
    compare_product: str | None = None,
    compare_label: str | None = None,
    compare_android_package: str | None = None,
    compare_ios_app_id: str | None = None,
) -> dict[str, Any]:
    if product_id not in APP_PRODUCTS:
        return {"error": "unknown_product"}
    base = build_intel_payload(product_id, period_days, force_refresh=force_refresh)
    if base.get("error"):
        return base
    aw = base.get("active_window") or {}
    android = aw.get("android") or {}
    ios = aw.get("ios") or {}
    raw = get_raw_product_data(product_id, force_refresh=False)
    all_rows = _dedupe_reviews(_period_filter((raw.get("android", {}).get("reviews") or []) + (raw.get("ios", {}).get("reviews") or []), period_days))

    keywords = _extract_keywords(all_rows, limit=25)
    rank_tracking = _update_rank_history(product_id, keywords)
    lang_mix = _lang_mix(all_rows)
    by_product_release: dict[str, Any] = {
        product_id: _release_impact_product_bundle(product_id, raw=raw),
    }
    for pid in APP_PRODUCTS:
        if pid == product_id:
            continue
        cached_other = get_cached_raw_product_data(pid)
        if cached_other:
            by_product_release[pid] = _release_impact_product_bundle(pid, raw=cached_other)
    release = {"active_product_id": product_id, "by_product": by_product_release}

    category_counts = {x.get("label"): int(x.get("count") or 0) for x in (android.get("categories") or [])}
    neg_driver = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    top_kw = [k["keyword"] for k in keywords[:8]]
    title_kw = ", ".join(top_kw[:2]) if top_kw else "canlı kur"
    subtitle_kw = ", ".join(top_kw[2:5]) if len(top_kw) >= 3 else ", ".join(top_kw[:3])

    selected_comp = _compare_entry(
        product_id,
        period_days,
        compare_product,
        compare_label,
        compare_android_package,
        compare_ios_app_id,
    )
    # Soğuk açılışta gereksiz ikinci ürün fetch'ini tetiklememek için otomatik fallback kaldırıldı.
    competitors = [selected_comp] if selected_comp else []
    rank_summary = _category_rank_summary(period_days, active_product_id=product_id, active_window=aw)

    review_count_period = int(android.get("review_count_period") or 0) + int(ios.get("review_count_period") or 0)
    ratings_total = int(android.get("store_ratings") or 0) + int(ios.get("store_ratings_count") or 0)
    funnel = _conversion_funnel(
        ratings_total,
        review_count_period,
        period_days=period_days,
        android_ratings=int(android.get("store_ratings") or 0),
        ios_ratings=int(ios.get("store_ratings_count") or 0),
        android_score=(float(android.get("store_score")) if android.get("store_score") is not None else None),
        ios_score=(float(ios.get("store_score")) if ios.get("store_score") is not None else None),
    )

    alerts = []
    one_star = int((android.get("star_distribution_period") or {}).get("1") or 0) + int((ios.get("star_distribution_period") or {}).get("1") or 0)
    total_star = sum(int(v or 0) for v in (android.get("star_distribution_period") or {}).values()) + sum(
        int(v or 0) for v in (ios.get("star_distribution_period") or {}).values()
    )
    one_ratio = (one_star / total_star) if total_star else 0.0
    if one_ratio >= 0.22:
        alerts.append({"level": "high", "title": "1★ oranı yükseldi", "detail": f"Dönemde 1★ oranı %{round(one_ratio*100,1)}"})
    _pl_now = ((by_product_release.get(product_id) or {}).get("platforms") or {}).get("android") or {}
    _ios_now = ((by_product_release.get(product_id) or {}).get("platforms") or {}).get("ios") or {}
    _neg_delta = None
    if _pl_now.get("delta") is not None:
        _neg_delta = float(_pl_now["delta"])
    if _ios_now.get("delta") is not None:
        _d = float(_ios_now["delta"])
        _neg_delta = _d if _neg_delta is None else min(_neg_delta, _d)
    if _neg_delta is not None and _neg_delta <= -0.3:
        alerts.append(
            {
                "level": "high",
                "title": "Release etkisi negatif",
                "detail": f"Platform bazlı son 30 gün skor değişimi {_neg_delta}",
            }
        )
    if not alerts:
        alerts.append({"level": "ok", "title": "Kritik ASO alarmı yok", "detail": "Metrikler normal aralıkta."})

    creative_backlog = [
        {
            "hypothesis": "İlk screenshot'a canlı kur + alarm akışını ekle",
            "why": "Arama/keşif ve bildirim kategorileri yüksek hacimde.",
            "priority": "Yüksek",
        },
        {
            "hypothesis": "Dark mode screenshot varyantı test et",
            "why": "Gece kullanımı yüksek uygulamalarda conversion artışı sağlar.",
            "priority": "Orta",
        },
        {
            "hypothesis": "Kısa açıklamada iki ana keyword öne çıkar",
            "why": "Keyword coverage + CTR etkisi beklenir.",
            "priority": "Yüksek",
        },
    ]

    return {
        "product_id": product_id,
        "label": base.get("label"),
        "period_days": period_days,
        "generated_at": datetime.now(tz=_UTC).isoformat(),
        "top_rank_summary": {
            "doviz_finans_rank": rank_summary.get("doviz"),
            "sinemalar_kategori_rank": rank_summary.get("sinemalar"),
        },
        "keyword_intelligence": {
            "top_keywords": keywords[:15],
            "new_candidates": [k["keyword"] for k in keywords if k["intent"] == "fırsat"][:12],
            "risk_keywords": [k["keyword"] for k in keywords if k["intent"] == "risk"][:8],
        },
        "metadata_optimizer": {
            "title_idea": f"{base.get('label')} - {title_kw}",
            "subtitle_idea": subtitle_kw or "hızlı ve güvenli kullanım",
            "short_description_idea": f"{base.get('label')} ile {', '.join(top_kw[:4])} konularında hızlı deneyim.",
            "long_description_outline": [
                "1) Değer önerisi + ana fayda",
                "2) En çok geçen özellikler",
                "3) Güven/performans mesajı",
                "4) Güncel sürüm yenilikleri ve CTA",
            ],
        },
        "keyword_rank_tracking": rank_tracking,
        "conversion_funnel": funnel,
        "competitor_benchmark": competitors,
        "review_to_aso_loop": {
            "top_positive_themes": [k["keyword"] for k in keywords if k["avg_score"] >= 4.0][:8],
            "top_negative_themes": [k["keyword"] for k in keywords if k["avg_score"] <= 2.6][:8],
            "feature_request_categories": [x[0] for x in neg_driver],
        },
        "release_impact": release,
        "creative_test_backlog": creative_backlog,
        "localization_aso": {
            "language_mix": lang_mix,
            "suggested_locales": ["tr-TR", "en-US"] + (["de-DE"] if lang_mix["other_pct"] >= 10 else []),
            "notes": "TR metadata birincil, EN secondary metadata önerilir.",
        },
        "alerts": alerts,
    }


def aso_json_safe(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: aso_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [aso_json_safe(v) for v in obj]
    if isinstance(obj, float):
        return float(obj) if not math.isnan(obj) else None
    return obj
