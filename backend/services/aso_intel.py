from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from backend.services.app_intel import (
    APP_PRODUCTS,
    _fetch_google_bundle,
    _fetch_ios_lookup_meta,
    build_intel_payload,
    get_raw_product_data,
)

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
        out.append({"at": r.get("at"), "score": score, "text": str(r.get("text") or "")})
    return out


def _period_filter(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if days <= 0:
        return list(rows)
    start = datetime.now(tz=_UTC) - timedelta(days=days)
    return [r for r in rows if isinstance(r.get("at"), datetime) and r["at"] >= start]


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
    now = datetime.now(tz=_UTC)
    recent = [int(r.get("score") or 0) for r in rows if isinstance(r.get("at"), datetime) and r["at"] >= now - timedelta(days=30)]
    prev = [
        int(r.get("score") or 0)
        for r in rows
        if isinstance(r.get("at"), datetime) and now - timedelta(days=60) <= r["at"] < now - timedelta(days=30)
    ]
    recent_avg = round(sum(recent) / len(recent), 2) if recent else None
    prev_avg = round(sum(prev) / len(prev), 2) if prev else None
    delta = round((recent_avg or 0) - (prev_avg or 0), 2) if recent_avg is not None and prev_avg is not None else None
    return {"monthly": monthly, "recent30_avg": recent_avg, "prev30_avg": prev_avg, "delta": delta}


def _conversion_funnel(store_ratings: int | None, period_reviews: int) -> dict[str, int]:
    ratings = max(0, int(store_ratings or 0))
    review_to_rating_ratio = 0.18
    installs = int((ratings + period_reviews / max(review_to_rating_ratio, 0.01)) * 6.5)
    listing_views = int(installs * 5.4)
    impressions = int(listing_views * 2.8)
    return {
        "impressions": impressions,
        "listing_views": listing_views,
        "installs": installs,
        "ratings": ratings,
        "reviews_period": max(0, int(period_reviews)),
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
    today = datetime.now(tz=_UTC).date().isoformat()
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


def _category_rank_summary(period_days: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pid in APP_PRODUCTS.keys():
        p = build_intel_payload(pid, period_days, force_refresh=False)
        w = p.get("active_window") or {}
        android = w.get("android") or {}
        ios = w.get("ios") or {}
        ar = (android.get("store_category_rank") or {}) if isinstance(android.get("store_category_rank"), dict) else {}
        rank = (ios.get("store_category_rank") or {}) if isinstance(ios.get("store_category_rank"), dict) else {}
        out[pid] = {
            "android": {
                "category": android.get("store_category_name"),
                "rank": ar.get("rank"),
                "total": ar.get("total"),
                "chart": ar.get("chart"),
            },
            "ios": {
                "category": ios.get("store_category_name"),
                "rank": rank.get("rank"),
                "total": rank.get("total"),
                "chart": rank.get("chart"),
            },
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
    ios_id = (compare_ios_app_id or "").strip()
    if not pkg and not ios_id:
        return None

    # Custom giriş aslında takipli bir ürüne denk geliyorsa tracked akışını kullan.
    for tp, spec in APP_PRODUCTS.items():
        if tp == base_product_id:
            continue
        if pkg and pkg == str(spec.get("android_package") or "").strip():
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
        if ios_id and ios_id == str(spec.get("ios_app_id") or "").strip():
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
    release = _release_impact(all_rows)

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
    if selected_comp is None:
        fallback = next((k for k in APP_PRODUCTS.keys() if k != product_id), None)
        selected_comp = _compare_entry(product_id, period_days, fallback, None, None, None)
    competitors = [selected_comp] if selected_comp else []
    rank_summary = _category_rank_summary(period_days)

    review_count_period = int(android.get("review_count_period") or 0) + int(ios.get("review_count_period") or 0)
    ratings_total = int(android.get("store_ratings") or 0) + int(ios.get("store_ratings_count") or 0)
    funnel = _conversion_funnel(ratings_total, review_count_period)

    alerts = []
    one_star = int((android.get("star_distribution_period") or {}).get("1") or 0) + int((ios.get("star_distribution_period") or {}).get("1") or 0)
    total_star = sum(int(v or 0) for v in (android.get("star_distribution_period") or {}).values()) + sum(
        int(v or 0) for v in (ios.get("star_distribution_period") or {}).values()
    )
    one_ratio = (one_star / total_star) if total_star else 0.0
    if one_ratio >= 0.22:
        alerts.append({"level": "high", "title": "1★ oranı yükseldi", "detail": f"Dönemde 1★ oranı %{round(one_ratio*100,1)}"})
    if release.get("delta") is not None and release["delta"] <= -0.3:
        alerts.append({"level": "high", "title": "Release etkisi negatif", "detail": f"Son 30 gün skor değişimi {release['delta']}"})
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
