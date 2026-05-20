"""
App Store Connect Analytics özet servisi.

Şu an: deterministik demo veri.
Yarın: gerçek ASC API bağlandığında payload yapısı aynı kalır; sadece
veri kaynağı bu modül içinde değişir.

Frontend için sağlanan alanlar:
  - filters / available_filters (period, country, source, device)
  - kpis (genel bakış)
  - acquisition / sales / subscriptions
  - engagement (sessions, active devices, crashes, retention)
  - ratings (ortalama, dağılım, toplam)
  - top_countries / top_sources / top_versions
  - trend_daily (genel günlük seri, ana grafik için)
"""

from __future__ import annotations

import hashlib
import logging
import random
from functools import lru_cache
from typing import Any

import httpx

from backend.services.app_intel import APP_PRODUCTS

logger = logging.getLogger(__name__)

# ─── iTunes Search API (public) ─────────────────────────────────────────────

@lru_cache(maxsize=32)
def _itunes_rating(app_id: str, country: str = "tr") -> dict[str, Any] | None:
    """iTunes Lookup API'dan gerçek puan ve toplam oy sayısını çeker.

    Cache'li (process ömrü boyunca); pratikte bir kez çekilir.
    """
    try:
        url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
        with httpx.Client(timeout=10) as cli:
            resp = cli.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        data = resp.json().get("results", [])
        if not data:
            return None
        r = data[0]
        avg = r.get("averageUserRating")
        cnt = r.get("userRatingCount")
        if avg is None:
            return None
        return {"average": round(float(avg), 2), "total": int(cnt or 0)}
    except Exception as exc:
        logger.debug("iTunes lookup hatası (app_id=%s): %s", app_id, exc)
        return None


def _best_itunes_rating(app_id: str) -> dict[str, Any] | None:
    """Birden fazla ülkeden sorgular; en yüksek toplam puan sayısını döner."""
    best: dict[str, Any] | None = None
    for cc in ("tr", "us", "de", "gb"):
        r = _itunes_rating(app_id, cc)
        if r and (best is None or r["total"] > best["total"]):
            best = r
    return best


def _estimate_rating_dist(total: int, avg: float, rng: random.Random) -> dict[str, int]:
    """Gerçek ortalamayı koruyacak şekilde yıldız dağılımı tahmin eder.

    Her yıldız en az toplam'ın %0.5'i kadar oy alır (0 gösterme sorunu giderildi).
    """
    # Gaussian-tabanlı ağırlıklar: ortalama yakınındaki yıldızlar dominant
    import math
    sigma = 1.2  # dağılım genişliği
    weights = [math.exp(-0.5 * ((s - avg) / sigma) ** 2) for s in range(1, 6)]

    # Küçük rastgele pertürbasyon (seed'li, tutarlı)
    perturb = [1.0 + rng.uniform(-0.08, 0.08) for _ in range(5)]
    weights = [weights[i] * perturb[i] for i in range(5)]

    # Minimum %0.5 floor: hiçbir yıldız sıfır göstermesin
    min_frac = 0.005
    sw = sum(weights)
    weights = [max(w / sw, min_frac) for w in weights]

    # Normalize et ve sayıya çevir
    sw2 = sum(weights)
    counts = [max(1, int(total * w / sw2)) for w in weights]

    # Toplam farkını dominant yıldıza ekle
    dominant = weights.index(max(weights))
    diff = total - sum(counts)
    counts[dominant] = max(1, counts[dominant] + diff)

    return {str(i + 1): counts[i] for i in range(5)}

# ─── Sabit filtre listeleri ──────────────────────────────────────────────────

_PERIODS: tuple[int, ...] = (0, 1, 7, 14, 30, 90, 365)

_COUNTRIES: list[dict[str, str]] = [
    {"code": "all", "name": "Tüm ülkeler"},
    {"code": "tr", "name": "Türkiye"},
    {"code": "us", "name": "ABD"},
    {"code": "de", "name": "Almanya"},
    {"code": "gb", "name": "Birleşik Krallık"},
    {"code": "fr", "name": "Fransa"},
    {"code": "nl", "name": "Hollanda"},
    {"code": "az", "name": "Azerbaycan"},
    {"code": "kz", "name": "Kazakistan"},
]

_SOURCES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm kaynaklar"},
    {"id": "search", "name": "App Store Arama"},
    {"id": "browse", "name": "App Store Keşfet"},
    {"id": "referrer_web", "name": "Web yönlendirmesi"},
    {"id": "referrer_app", "name": "Uygulama yönlendirmesi"},
    {"id": "institutional", "name": "Kurumsal satın alma"},
    {"id": "unavailable", "name": "Tespit edilemeyen"},
]

_DEVICES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm cihazlar"},
    {"id": "iphone", "name": "iPhone"},
    {"id": "ipad", "name": "iPad"},
    {"id": "ipod", "name": "iPod touch"},
]


# ─── Yardımcılar ─────────────────────────────────────────────────────────────

def _seed_int(key: str) -> int:
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)


def _series(rng: random.Random, n: int, *, start: float, volatility: float) -> list[float]:
    n = max(2, min(int(n), 365))
    v = start
    out: list[float] = []
    for _ in range(n):
        v = max(0.0, v * (1.0 + rng.uniform(-volatility, volatility * 1.4)))
        out.append(round(v, 4))
    return out


def _delta_pct(rng: random.Random, *, lo: float = -18.0, hi: float = 60.0) -> float:
    return round(rng.uniform(lo, hi), 2)


def _series_delta(series: list[float]) -> float | None:
    """İlk yarı ortalamasına göre ikinci yarının % değişimi."""
    n = len(series)
    if n < 4:
        return None
    mid = n // 2
    first_avg = sum(series[:mid]) / mid
    second_avg = sum(series[mid:]) / (n - mid)
    if first_avg == 0:
        return None
    return round((second_avg - first_avg) / first_avg * 100, 1)


def _fmt_compact(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1000:
        return f"{n / 1000:.1f}K".rstrip("0").rstrip(".")
    return f"{n:.0f}"


def _fmt_money(n: float) -> str:
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M".replace(".00M", "M")
    if n >= 1000:
        return f"${n / 1000:.1f}K".replace(".0K", "K")
    return f"${n:.2f}"


def _filter_multiplier(country: str, source: str, device: str) -> float:
    """Filtreye göre veri ölçeğini kıs/genişlet (demo)."""
    m = 1.0
    if country != "all":
        # Ülke seçilince ölçek küçülür (segment payı)
        country_shares = {
            "tr": 0.48, "us": 0.20, "de": 0.08, "gb": 0.05,
            "fr": 0.04, "nl": 0.03, "az": 0.02, "kz": 0.02,
        }
        m *= country_shares.get(country, 0.05)
    if source != "all":
        source_shares = {
            "search": 0.55, "browse": 0.18, "referrer_web": 0.10,
            "referrer_app": 0.08, "institutional": 0.03, "unavailable": 0.06,
        }
        m *= source_shares.get(source, 0.05)
    if device != "all":
        device_shares = {"iphone": 0.78, "ipad": 0.18, "ipod": 0.04}
        m *= device_shares.get(device, 0.02)
    return max(m, 0.001)


# ─── Tek metrik kartı üreticisi ──────────────────────────────────────────────

def _metric(rng: random.Random, *, base: float, vol: float, n_points: int,
            fmt: str = "compact", delta_lo: float = -18.0, delta_hi: float = 60.0) -> dict[str, Any]:
    s = _series(rng, n_points, start=base, volatility=vol)
    last = s[-1]
    prev = max(0.001, s[0] * (0.75 + rng.random() * 0.4))
    d = ((last - prev) / prev * 100.0) if prev else 0.0
    if fmt == "money":
        label = _fmt_money(last)
    elif fmt == "percent":
        label = f"{last:.2f}%"
    elif fmt == "raw":
        label = f"{last:.2f}"
    else:
        label = _fmt_compact(last)
    return {
        "value": round(last, 2),
        "value_label": label,
        "delta_pct": round(max(min(d, delta_hi), delta_lo), 2),
        "series": s,
    }


# ─── Ana payload üreticisi ───────────────────────────────────────────────────

def build_asc_connect_preview_payload(
    product_id: str,
    period_days: int,
    country: str = "all",
    source: str = "all",
    device: str = "all",
    progress_cb=None,  # Callable[[done: int, total: int], None] | None
) -> dict[str, Any]:
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": "unknown_product"}

    try:
        p = int(period_days)
    except (TypeError, ValueError):
        p = 30
    if p not in _PERIODS:
        p = 30
    # 0 = "tümü" — Apple DAILY max 365 gün
    effective_p = 365 if p == 0 else p

    cc = (country or "all").strip().lower()
    if cc not in {c["code"] for c in _COUNTRIES}:
        cc = "all"
    src = (source or "all").strip().lower()
    if src not in {s["id"] for s in _SOURCES}:
        src = "all"
    dev = (device or "all").strip().lower()
    if dev not in {d["id"] for d in _DEVICES}:
        dev = "all"

    seed = _seed_int(f"asc_v2|{pid}|{p}|{cc}|{src}|{dev}")
    # Rating seed period'dan bağımsız — App Store puanı kümülatif, period'a göre değişmez
    rating_seed = _seed_int(f"asc_rating|{pid}")
    rng = random.Random(seed)
    label = APP_PRODUCTS[pid]["label"]
    ios_app_id = APP_PRODUCTS[pid].get("ios_app_id", "")

    # Günlük noktalar (sparkline / trend için)
    line_points = max(6, effective_p) if effective_p > 1 else 6

    # Ürün ve filtre ölçeği
    base_mult = 1.15 if pid == "doviz" else 0.85
    fmult = _filter_multiplier(cc, src, dev)
    mult = base_mult * fmult * (effective_p / 30.0)  # period uzadıkça toplam büyür

    # ── Kazanım (Acquisition) ─────────────────────────────────────────────
    impressions = _metric(rng, base=1_200_000 * mult, vol=0.030, n_points=line_points)
    page_views = _metric(rng, base=75_000 * mult, vol=0.042, n_points=line_points)
    first_dl = _metric(rng, base=24_000 * mult, vol=0.045, n_points=line_points)
    redownloads = _metric(rng, base=15_000 * mult, vol=0.050, n_points=line_points)
    updates = _metric(rng, base=3_200_000 * mult, vol=0.028, n_points=line_points)
    conv_v = round(3.0 + rng.random() * 5.5, 2)
    conversion_rate = _metric(rng, base=float(conv_v), vol=0.04, n_points=line_points, fmt="percent")
    conversion_rate["value"] = conv_v
    conversion_rate["value_label"] = f"{conv_v:.2f}%"

    acquisition = {
        "impressions": impressions,
        "product_page_views": page_views,
        "first_time_downloads": first_dl,
        "redownloads": redownloads,
        "total_downloads": _metric(rng, base=(first_dl["value"] + redownloads["value"]),
                                   vol=0.035, n_points=line_points),
        "updates": updates,
        "conversion_rate": conversion_rate,
    }

    # ── Satış (Sales) ─────────────────────────────────────────────────────
    proceeds = _metric(rng, base=2400.0 * mult, vol=0.110, n_points=line_points, fmt="money")
    paying_users = _metric(rng, base=80.0 * mult, vol=0.080, n_points=line_points)
    iap = _metric(rng, base=3500.0 * mult, vol=0.060, n_points=line_points)
    d1 = round(rng.random() * 0.6, 2)
    d7 = round(d1 + rng.random() * 0.3, 2)
    d35 = round(d7 + rng.random() * 0.25, 2)
    refund_rate = round(0.5 + rng.random() * 2.5, 2)
    sales = {
        "proceeds": proceeds,
        "paying_users": paying_users,
        "in_app_purchases": iap,
        "d1_download_to_paid": d1,
        "d7_download_to_paid": d7,
        "d35_download_to_paid": d35,
        "refund_rate_pct": refund_rate,
        "arpu": {
            "value": round(proceeds["value"] / max(1, paying_users["value"]), 2),
            "value_label": _fmt_money(proceeds["value"] / max(1, paying_users["value"])),
            "delta_pct": round(_delta_pct(rng, lo=-15, hi=25), 2),
            "series": [],
        },
    }

    # ── Abonelikler (Subscriptions) ───────────────────────────────────────
    subs_rng = random.Random(seed + 7)
    bar_n = line_points
    net_bars = [round(subs_rng.uniform(-220, 180) * mult) for _ in range(bar_n)]
    net_sum = float(sum(net_bars))

    active_plans_v = int(1500 * mult + subs_rng.random() * 1200)
    paid_plans_v = int(1500 * mult + subs_rng.random() * 800)
    trial_v = int(300 * mult + subs_rng.random() * 400)
    mrr_v = 400 + subs_rng.random() * 500
    subscriptions = {
        "active_plans": {
            "value": active_plans_v,
            "value_label": _fmt_compact(float(active_plans_v)),
            "delta_pct": round(subs_rng.uniform(-12, 8), 2),
        },
        "paid_plans": {
            "value": paid_plans_v,
            "value_label": _fmt_compact(float(paid_plans_v)),
            "delta_pct": round(subs_rng.uniform(-12, 8), 2),
        },
        "free_trials": {
            "value": trial_v,
            "value_label": _fmt_compact(float(trial_v)),
            "delta_pct": round(subs_rng.uniform(-20, 30), 2),
        },
        "mrr": {
            "value": round(mrr_v, 2),
            "value_label": f"${mrr_v:.0f}",
            "delta_pct": round(subs_rng.uniform(10, 80), 2),
        },
        "net_paid_plans": {
            "value": int(net_sum),
            "value_label": str(int(net_sum)),
            "daily_bars": net_bars,
        },
        "plan_starts": int(250 + subs_rng.random() * 200),
        "churned": int(300 + subs_rng.random() * 300),
        "trial_conversion_pct": round(35 + subs_rng.random() * 45, 1),
    }

    # ── Etkileşim (Engagement) ────────────────────────────────────────────
    eng_rng = random.Random(seed + 11)
    sessions = _metric(eng_rng, base=180_000 * mult, vol=0.040, n_points=line_points)
    active_devices = _metric(eng_rng, base=42_000 * mult, vol=0.035, n_points=line_points)
    crashes = _metric(eng_rng, base=320 * mult, vol=0.080, n_points=line_points)
    crash_rate = round(0.05 + eng_rng.random() * 0.35, 3)
    engagement = {
        "sessions": sessions,
        "active_devices": active_devices,
        "crashes": crashes,
        "crash_rate_pct": crash_rate,
        "sessions_per_device": round(sessions["value"] / max(1, active_devices["value"]), 2),
        "retention_d1": round(28 + eng_rng.random() * 18, 1),
        "retention_d7": round(15 + eng_rng.random() * 12, 1),
        "retention_d28": round(6 + eng_rng.random() * 8, 1),
        "avg_session_seconds": int(60 + eng_rng.random() * 180),
    }

    # ── Puanlar (Ratings) ─────────────────────────────────────────────────
    # Rating period'a göre değişmez — kümülatif App Store puanı.
    # Önce iTunes Search API'dan gerçek veriyi dene (TR/US/DE/GB arasından en yüksek sayı).
    rt_rng = random.Random(rating_seed)
    live_rating = _best_itunes_rating(ios_app_id) if ios_app_id else None
    if live_rating:
        real_avg = live_rating["average"]
        real_total = live_rating["total"]
        rating_dist = _estimate_rating_dist(real_total, real_avg, rt_rng)
        ratings = {
            "average": real_avg,
            "total": real_total,
            "distribution": rating_dist,
            "delta_avg": round(rt_rng.uniform(-0.05, 0.1), 2),
            "source": "live",
        }
    else:
        total_ratings = int(8000 * base_mult + rt_rng.random() * 12_000)
        rating_dist = _estimate_rating_dist(total_ratings, 4.2, rt_rng)
        avg_val = sum(int(k) * v for k, v in rating_dist.items()) / max(1, sum(rating_dist.values()))
        ratings = {
            "average": round(avg_val, 2),
            "total": sum(rating_dist.values()),
            "distribution": rating_dist,
            "delta_avg": round(rt_rng.uniform(-0.2, 0.3), 2),
        }

    # ── Top tablolar ──────────────────────────────────────────────────────
    tc_rng = random.Random(seed + 17)
    top_countries: list[dict[str, Any]] = []
    cshare = {"tr": 0.48, "us": 0.20, "de": 0.08, "gb": 0.05, "fr": 0.04,
              "nl": 0.03, "az": 0.02, "kz": 0.02, "it": 0.025, "es": 0.02}
    for code, share in cshare.items():
        dl = int(first_dl["value"] * share * (0.9 + tc_rng.random() * 0.25))
        if dl <= 0:
            continue
        top_countries.append({
            "code": code,
            "name": next((c["name"] for c in _COUNTRIES if c["code"] == code), code.upper()),
            "downloads": dl,
            "share_pct": round(share * 100, 1),
            "delta_pct": round(tc_rng.uniform(-18, 35), 1),
            "proceeds": round(proceeds["value"] * share * (0.85 + tc_rng.random() * 0.3), 2),
        })
    top_countries.sort(key=lambda r: r["downloads"], reverse=True)

    top_sources: list[dict[str, Any]] = []
    sshare = {"search": 0.55, "browse": 0.18, "referrer_web": 0.10,
              "referrer_app": 0.08, "institutional": 0.03, "unavailable": 0.06}
    for sid, share in sshare.items():
        nm = next((s["name"] for s in _SOURCES if s["id"] == sid), sid)
        imp = int(impressions["value"] * share * (0.9 + tc_rng.random() * 0.25))
        pv = int(page_views["value"] * share * (0.9 + tc_rng.random() * 0.25))
        dl = int(first_dl["value"] * share * (0.85 + tc_rng.random() * 0.3))
        conv = round((dl / max(1, pv)) * 100, 2)
        top_sources.append({
            "id": sid,
            "name": nm,
            "impressions": imp,
            "product_page_views": pv,
            "downloads": dl,
            "conversion_pct": conv,
        })
    top_sources.sort(key=lambda r: r["downloads"], reverse=True)

    tv_rng = random.Random(seed + 23)
    base_ver = (3, 8)
    top_versions: list[dict[str, Any]] = []
    remaining = 1.0
    for i in range(5):
        v_major = base_ver[0] + (1 if i >= 3 else 0)
        v_minor = base_ver[1] - i
        if v_minor < 0:
            v_major -= 1
            v_minor = 9 + v_minor
        v_patch = tv_rng.randint(0, 4)
        share = remaining * (0.5 if i == 0 else (0.45 if i == 1 else tv_rng.uniform(0.05, 0.25)))
        share = min(share, remaining)
        remaining -= share
        dl = int(first_dl["value"] * share)
        top_versions.append({
            "version": f"{v_major}.{v_minor}.{v_patch}",
            "downloads": dl,
            "active_devices": int(active_devices["value"] * share * (0.85 + tv_rng.random() * 0.3)),
            "crash_rate_pct": round(0.05 + tv_rng.random() * 0.6, 2),
            "share_pct": round(share * 100, 1),
            "is_latest": i == 0,
        })

    # ── Genel günlük trend (overview ana grafik) ──────────────────────────
    # downloads + proceeds birlikte
    trend_daily = []
    dl_series = first_dl["series"]
    pr_series = proceeds["series"]
    for i, dl_v in enumerate(dl_series):
        trend_daily.append({
            "i": i,
            "downloads": round(dl_v),
            "proceeds": round(pr_series[i] if i < len(pr_series) else 0, 2),
        })

    # ── Genel bakış KPI'ları (kart sırasıyla) ─────────────────────────────
    kpis = {
        "impressions": impressions,
        "product_page_views": page_views,
        "total_downloads": acquisition["total_downloads"],
        "conversion_rate": conversion_rate,
        "proceeds": proceeds,
        "active_devices": active_devices,
    }

    payload = {
        "source": "demo",
        "source_note": (
            "Örnek veri — App Store Connect API bağlandığında bu özet canlı verilerle dolacak."
        ),
        "product": pid,
        "product_label": label,
        "period_days": effective_p,
        "filters": {"country": cc, "source": src, "device": dev},
        "available_filters": {
            "periods": list(_PERIODS),
            "countries": _COUNTRIES,
            "sources": _SOURCES,
            "devices": _DEVICES,
        },
        "kpis": kpis,
        "acquisition": acquisition,
        "sales": sales,
        "subscriptions": subscriptions,
        "engagement": engagement,
        "ratings": ratings,
        "top_countries": top_countries,
        "top_sources": top_sources,
        "top_versions": top_versions,
        "trend_daily": trend_daily,
    }

    # ── Gerçek App Store Connect verisi (varsa) ───────────────────────────
    try:
        from backend.services import asc_client
        if asc_client.is_configured():
            bundle = APP_PRODUCTS[pid].get("ios_bundle_id") or ""
            live = asc_client.fetch_daily_sales_summary(
                bundle_id=bundle, days=p, country=cc, device=dev,
                progress_cb=progress_cb,
            )
            if live:
                payload = _overlay_live_sales(payload, live)
            subs = asc_client.fetch_subscription_summary(days=p)
            if subs:
                payload = _overlay_live_subscriptions(payload, subs)
    except Exception as exc:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).warning("ASC live overlay başarısız: %s", exc)

    return payload


def _overlay_live_sales(payload: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
    """ASC sales raporundan gelen gerçek verilerle demo payload'ı güncelle."""
    first_dl = int(live.get("first_time_downloads") or 0)
    updates = int(live.get("updates") or 0)
    iap_units = int(live.get("iap_units") or 0)
    total_dl = int(live.get("total_downloads") or first_dl)
    proceeds_v = float(live.get("proceeds_usd") or 0)
    dl_series = list(live.get("dl_series") or [])
    pr_series = list(live.get("pr_series") or [])

    def _kpi(value: float, *, money: bool = False, series: list[float] | None = None) -> dict[str, Any]:
        s = [float(x) for x in (series or [])]
        return {
            "value": round(value, 2),
            "value_label": _fmt_money(value) if money else _fmt_compact(float(value)),
            "delta_pct": _series_delta(s),
            "series": s,
        }

    # Metrikler Sales Report'tan gelir (canlı)
    payload["acquisition"]["first_time_downloads"] = _kpi(first_dl, series=dl_series)
    payload["acquisition"]["total_downloads"] = _kpi(total_dl, series=dl_series)
    payload["acquisition"]["updates"] = _kpi(updates)
    if iap_units > 0:
        payload["sales"]["in_app_purchases"] = _kpi(float(iap_units))
    # Redownloads Sales Report'ta yok — kart mevcut ama değer yok
    payload["acquisition"]["redownloads"] = {
        "value": None, "value_label": "—", "delta_pct": None, "series": [],
        "is_unavailable": True,
    }
    payload["sales"]["proceeds"] = _kpi(proceeds_v, money=True, series=pr_series)
    payload["kpis"]["total_downloads"] = payload["acquisition"]["total_downloads"]
    payload["kpis"]["proceeds"] = payload["sales"]["proceeds"]

    # Demo'dan gelen impressions/page_views/conversion/active_devices kart gösterir ama
    # değeri "—" olarak işaretlenir ki gerçek veriyle karıştırılmasın.
    _unavail: dict[str, Any] = {"value": None, "value_label": "—", "delta_pct": None, "series": [], "is_demo": True}
    for key in ("impressions", "product_page_views", "conversion_rate"):
        payload["kpis"][key] = dict(_unavail)
        if key in payload.get("acquisition", {}):
            payload["acquisition"][key] = dict(_unavail)
    payload["kpis"]["active_devices"] = dict(_unavail)
    for _eng_key in ("active_devices", "sessions", "crashes"):
        if _eng_key in payload.get("engagement", {}):
            payload["engagement"][_eng_key] = dict(_unavail)
    # sessions kpi varsa da temizle
    if "sessions" in payload.get("kpis", {}):
        payload["kpis"]["sessions"] = dict(_unavail)

    # Günlük trend gerçek verilerden yeniden inşa
    if dl_series:
        payload["trend_daily"] = [
            {"i": i, "downloads": round(dl_series[i]),
             "proceeds": round(pr_series[i] if i < len(pr_series) else 0, 2)}
            for i in range(len(dl_series))
        ]

    # Ülke kırılımı — gerçekçi tablo
    country_agg = live.get("country_breakdown") or {}
    if country_agg:
        cc_total = sum(c["downloads"] for c in country_agg.values()) or 1
        new_top = []
        for code, vals in country_agg.items():
            new_top.append({
                "code": code.lower(),
                "name": code,
                "downloads": int(vals["downloads"]),
                "share_pct": round(vals["downloads"] / cc_total * 100, 1),
                "delta_pct": 0.0,
                "proceeds": round(float(vals.get("proceeds") or 0), 2),
            })
        new_top.sort(key=lambda r: r["downloads"], reverse=True)
        payload["top_countries"] = new_top[:15]

    # Versiyon kırılımı
    version_agg = live.get("version_breakdown") or {}
    if version_agg:
        v_total = sum(v["downloads"] for v in version_agg.values()) or 1
        new_v = []
        for ver, vals in version_agg.items():
            share = vals["downloads"] / v_total
            new_v.append({
                "version": ver,
                "downloads": int(vals["downloads"]),
                "active_devices": 0,
                "crash_rate_pct": 0.0,
                "share_pct": round(share * 100, 1),
                "is_latest": False,
            })
        new_v.sort(key=lambda r: r["downloads"], reverse=True)
        if new_v:
            new_v[0]["is_latest"] = True
        payload["top_versions"] = new_v[:10]

    payload["source"] = "live"
    payload["source_note"] = (
        "Canlı veri — App Store Connect Sales raporlarından (24-48 saat gecikme normaldir). "
        "Impression / dönüşüm / etkileşim alanları Analytics Reports API'ye bağlanana kadar demo kalıyor."
    )
    return payload


def _overlay_live_subscriptions(payload: dict[str, Any], subs: dict[str, Any]) -> dict[str, Any]:
    ap = int(subs.get("active_plans") or 0)
    pp = int(subs.get("paid_plans") or 0)
    ft = int(subs.get("free_trials") or 0)
    payload["subscriptions"]["active_plans"] = {
        "value": ap, "value_label": _fmt_compact(float(ap)), "delta_pct": 0.0,
    }
    payload["subscriptions"]["paid_plans"] = {
        "value": pp, "value_label": _fmt_compact(float(pp)), "delta_pct": 0.0,
    }
    payload["subscriptions"]["free_trials"] = {
        "value": ft, "value_label": _fmt_compact(float(ft)), "delta_pct": 0.0,
    }
    return payload
