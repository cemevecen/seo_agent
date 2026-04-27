"""
App Store Connect Analytics benzeri özet — şu an deterministik demo veri.
Gerçek metrikler App Store Connect API (satış/analitik uçları) bağlandığında buraya bağlanır.
"""

from __future__ import annotations

import hashlib
import random
from typing import Any

from backend.services.app_intel import APP_PRODUCTS


def _seed_int(key: str) -> int:
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)


def _series(rng: random.Random, n: int, *, start: float, volatility: float) -> list[float]:
    n = max(2, min(int(n), 90))
    v = start
    out: list[float] = []
    for _ in range(n):
        v = max(0.0, v * (1.0 + rng.uniform(-volatility, volatility * 1.4)))
        out.append(round(v, 4))
    return out


def _delta_pct(rng: random.Random) -> float:
    return round(rng.uniform(-18.0, 110.0), 2)


def _fmt_compact(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1000:
        return f"{n / 1000:.1f}K".rstrip("0").rstrip(".")
    return f"{n:.0f}"


def build_asc_connect_preview_payload(product_id: str, period_days: int) -> dict[str, Any]:
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": "unknown_product"}
    if period_days not in (1, 7, 30):
        period_days = 30

    seed = _seed_int(f"app_asc_preview_v1|{pid}|{period_days}")
    rng = random.Random(seed)
    label = APP_PRODUCTS[pid]["label"]

    # Sparkline noktası: 1g → 6 segment, 7g → 7, 30g → 30
    line_points = 6 if period_days == 1 else period_days

    def big_metric(base: float, vol: float) -> dict[str, Any]:
        s = _series(rng, line_points, start=base, volatility=vol)
        prev = s[0] * (0.7 + rng.random() * 0.35) if s[0] else 1.0
        last = s[-1]
        d = ((last - prev) / prev * 100.0) if prev else 0.0
        return {
            "value": round(last, 2),
            "value_label": _fmt_compact(last) if last >= 1000 else f"{last:.2f}",
            "delta_pct": round(d, 2),
            "series": s,
        }

    # Ölçek: ürüne göre hafif fark
    mult = 1.15 if pid == "doviz" else 0.85

    crv = round(3.0 + rng.random() * 4.5, 2)
    acquisition = {
        "first_time_downloads": big_metric(24000 * mult, 0.045),
        "redownloads": big_metric(15000 * mult, 0.05),
        "conversion_rate": {
            "value": crv,
            "value_label": f"{crv:.2f}%",
            "delta_pct": _delta_pct(rng),
            "series": _series(rng, line_points, start=float(crv), volatility=0.04),
        },
        "impressions": big_metric(1_200_000 * mult, 0.03),
        "product_page_views": big_metric(75_000 * mult, 0.042),
        "updates": big_metric(3_200_000 * mult, 0.028),
    }
    for k in ("first_time_downloads", "redownloads", "impressions", "product_page_views", "updates"):
        acquisition[k]["value_label"] = _fmt_compact(acquisition[k]["value"])  # type: ignore[union-attr]

    pr = 800 + rng.random() * 4000
    pu = int(30 + rng.random() * 120)
    sales = {
        "proceeds": {
            "value": round(pr, 2),
            "value_label": f"${pr/1000:.2f}K" if pr >= 1000 else f"${pr:.2f}",
            "delta_pct": _delta_pct(rng),
            "series": _series(rng, line_points, start=max(0.5, pr / 3000.0), volatility=0.12),
        },
        "paying_users": {
            "value": pu,
            "value_label": str(pu),
            "delta_pct": _delta_pct(rng),
            "series": _series(rng, line_points, start=float(pu), volatility=0.08),
        },
        "in_app_purchases": big_metric(3500 * mult, 0.06),
    }
    sales["in_app_purchases"]["value_label"] = _fmt_compact(float(sales["in_app_purchases"]["value"]))  # type: ignore[union-attr]

    d1 = round(rng.random() * 0.5, 2)
    d7 = round(d1 + rng.random() * 0.25, 2)
    d35 = round(d7 + rng.random() * 0.2, 2)
    sales["d1_download_to_paid"] = d1
    sales["d7_download_to_paid"] = d7
    sales["d35_download_to_paid"] = d35

    subs_rng = random.Random(seed + 7)
    bar_n = line_points
    net_bars = []
    for i in range(bar_n):
        net_bars.append(round(subs_rng.uniform(-220, 180) * mult))
    net_sum = float(sum(net_bars))

    ap = int(1500 * mult + subs_rng.random() * 1200)
    pp = int(1500 * mult + subs_rng.random() * 800)
    mrrv = 400 + subs_rng.random() * 500
    subscriptions = {
        "active_plans": {
            "value": ap,
            "value_label": _fmt_compact(float(ap)),
            "delta_pct": round(subs_rng.uniform(-12, 8), 2),
        },
        "paid_plans": {
            "value": pp,
            "value_label": _fmt_compact(float(pp)),
            "delta_pct": round(subs_rng.uniform(-12, 8), 2),
        },
        "mrr": {
            "value": round(mrrv, 2),
            "value_label": f"${mrrv:.0f}",
            "delta_pct": round(subs_rng.uniform(30, 220), 2),
        },
        "net_paid_plans": {
            "value": int(net_sum),
            "value_label": str(int(net_sum)),
            "daily_bars": net_bars,
        },
        "plan_starts": int(250 + subs_rng.random() * 200),
        "churned": int(300 + subs_rng.random() * 300),
    }

    return {
        "source": "demo",
        "source_note": "Örnek trend verisi (App Store Connect API bağlantısı eklendiğinde canlı dolar).",
        "product": pid,
        "product_label": label,
        "period_days": period_days,
        "acquisition": acquisition,
        "sales": sales,
        "subscriptions": subscriptions,
    }
