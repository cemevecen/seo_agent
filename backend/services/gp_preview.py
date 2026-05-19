"""
Google Play Store Analytics özet servisi.

Google Play Console'un karşılığı (İndirmeler, Puanlama, Vitals).
Credential yoksa deterministik demo veri döner; varsa gerçek veriyle overlay.

Gerekli Railway env vars:
    GP_SERVICE_ACCOUNT_JSON  — Service account JSON içeriği
"""
from __future__ import annotations

import hashlib
import random
from typing import Any

from backend.services.app_intel import APP_PRODUCTS


# ─── Sabitler ────────────────────────────────────────────────────────────────

_PERIODS: tuple[int, ...] = (1, 7, 14, 30, 90, 365)

_COUNTRIES: list[dict[str, str]] = [
    {"code": "all", "name": "Tüm ülkeler"},
    {"code": "tr", "name": "Türkiye"},
    {"code": "us", "name": "ABD"},
    {"code": "de", "name": "Almanya"},
    {"code": "gb", "name": "Birleşik Krallık"},
    {"code": "in", "name": "Hindistan"},
    {"code": "br", "name": "Brezilya"},
    {"code": "ru", "name": "Rusya"},
    {"code": "az", "name": "Azerbaycan"},
    {"code": "kz", "name": "Kazakistan"},
]

_DEVICES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm cihazlar"},
    {"id": "phone", "name": "Telefon"},
    {"id": "tablet", "name": "Tablet"},
]

_ANDROID_SOURCES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm kaynaklar"},
    {"id": "google_search", "name": "Google Arama"},
    {"id": "play_search", "name": "Google Play Arama"},
    {"id": "third_party_referral", "name": "Üçüncü taraf yönlendirme"},
    {"id": "play_store_browse", "name": "Google Play Keşfet"},
    {"id": "direct", "name": "Doğrudan"},
]


# ─── Yardımcılar ─────────────────────────────────────────────────────────────

def _seed_int(key: str) -> int:
    return int(hashlib.sha256(key.encode()).hexdigest()[:12], 16)


def _series(rng: random.Random, n: int, *, start: float, vol: float) -> list[float]:
    n = max(2, min(n, 365))
    v = start
    out: list[float] = []
    for _ in range(n):
        v = max(0.0, v * (1.0 + rng.uniform(-vol, vol * 1.4)))
        out.append(round(v, 4))
    return out


def _fmt_compact(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1000:
        return f"{n / 1000:.1f}K".rstrip("0").rstrip(".")
    return f"{n:.0f}"


def _fmt_money(n: float) -> str:
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1000:
        return f"${n / 1000:.1f}K"
    return f"${n:.2f}"


def _kpi(rng: random.Random, *, base: float, vol: float, n_points: int,
         money: bool = False) -> dict[str, Any]:
    s = _series(rng, n_points, start=base, vol=vol)
    last = s[-1]
    prev = max(0.001, s[0] * (0.75 + rng.random() * 0.4))
    delta = round(((last - prev) / prev * 100.0), 2) if prev else 0.0
    delta = max(min(delta, 60.0), -25.0)
    return {
        "value": round(last, 2),
        "value_label": _fmt_money(last) if money else _fmt_compact(last),
        "delta_pct": delta,
        "series": s,
    }


# ─── Ana payload üreticisi ───────────────────────────────────────────────────

def build_gp_preview_payload(
    product_id: str,
    period_days: int,
    country: str = "all",
    device: str = "all",
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

    cc = (country or "all").strip().lower()
    dev = (device or "all").strip().lower()

    seed = _seed_int(f"gp_v1|{pid}|{p}|{cc}|{dev}")
    rng = random.Random(seed)
    label = APP_PRODUCTS[pid]["label"]
    n = max(6, p) if p > 1 else 6

    base_mult = 1.0 if pid == "doviz" else 0.75
    cc_mult = {"tr": 0.55, "us": 0.15, "de": 0.06, "gb": 0.05}.get(cc, 0.04 if cc != "all" else 1.0)
    mult = base_mult * cc_mult * (p / 30.0)

    # ── İndirmeler ────────────────────────────────────────────────────────
    installs = _kpi(rng, base=30_000 * mult, vol=0.05, n_points=n)
    uninstalls = _kpi(rng, base=8_000 * mult, vol=0.08, n_points=n)
    net_installs_v = max(0, installs["value"] - uninstalls["value"])
    installs_series = installs["series"]
    uninstalls_series = uninstalls["series"]

    # ── Puanlar ──────────────────────────────────────────────────────────
    rt_rng = random.Random(seed + 3)
    total_ratings = int(40_000 * base_mult + rt_rng.random() * 60_000)
    dist_w = [rt_rng.random() for _ in range(5)]
    dist_w[4] *= 10
    dist_w[3] *= 5
    dist_w[2] *= 2
    sw = sum(dist_w)
    rating_dist = {str(i + 1): int(total_ratings * (dist_w[i] / sw)) for i in range(5)}
    avg = sum(int(k) * v for k, v in rating_dist.items()) / max(1, sum(rating_dist.values()))

    # ── Vitals (Crash, ANR, Slow render) ─────────────────────────────────
    vi_rng = random.Random(seed + 7)
    crash_rate_v = round(0.01 + vi_rng.random() * 0.15, 3)
    anr_rate_v = round(0.005 + vi_rng.random() * 0.08, 3)
    slow_render_v = round(1.0 + vi_rng.random() * 5.0, 2)
    crash_series = [round(crash_rate_v * (0.9 + vi_rng.random() * 0.2), 4) for _ in range(n)]
    anr_series = [round(anr_rate_v * (0.9 + vi_rng.random() * 0.2), 4) for _ in range(n)]

    # ── Top ülkeler ───────────────────────────────────────────────────────
    tc_rng = random.Random(seed + 11)
    cshare = {"tr": 0.52, "us": 0.12, "de": 0.06, "gb": 0.05,
              "in": 0.04, "br": 0.03, "ru": 0.03, "az": 0.03, "kz": 0.02}
    top_countries = []
    for code, share in cshare.items():
        dl = int(installs["value"] * share * (0.85 + tc_rng.random() * 0.3))
        if dl <= 0:
            continue
        top_countries.append({
            "code": code,
            "name": next((c["name"] for c in _COUNTRIES if c["code"] == code), code.upper()),
            "installs": dl,
            "share_pct": round(share * 100, 1),
            "delta_pct": round(tc_rng.uniform(-15, 30), 1),
        })
    top_countries.sort(key=lambda r: r["installs"], reverse=True)

    # ── Top kaynaklar ─────────────────────────────────────────────────────
    src_rng = random.Random(seed + 13)
    sshare = {
        "google_search": 0.38, "play_search": 0.30,
        "play_store_browse": 0.14, "third_party_referral": 0.10, "direct": 0.08,
    }
    top_sources = []
    for sid, share in sshare.items():
        dl = int(installs["value"] * share * (0.85 + src_rng.random() * 0.3))
        nm = next((s["name"] for s in _ANDROID_SOURCES if s["id"] == sid), sid)
        top_sources.append({"id": sid, "name": nm, "installs": dl, "share_pct": round(share * 100, 1)})
    top_sources.sort(key=lambda r: r["installs"], reverse=True)

    # ── Günlük trend ──────────────────────────────────────────────────────
    trend_daily = [
        {"i": i, "installs": round(installs_series[i]), "uninstalls": round(uninstalls_series[i])}
        for i in range(len(installs_series))
    ]

    payload: dict[str, Any] = {
        "source": "demo",
        "source_note": "Örnek veri — Google Play service account bağlandığında canlı verilerle dolacak.",
        "product": pid,
        "product_label": label,
        "period_days": p,
        "filters": {"country": cc, "device": dev},
        "available_filters": {
            "periods": list(_PERIODS),
            "countries": _COUNTRIES,
            "devices": _DEVICES,
            "sources": _ANDROID_SOURCES,
        },
        "kpis": {
            "installs": installs,
            "uninstalls": uninstalls,
            "net_installs": {
                "value": net_installs_v,
                "value_label": _fmt_compact(float(net_installs_v)),
                "delta_pct": round(rng.uniform(-10, 30), 2),
                "series": [max(0.0, installs_series[i] - uninstalls_series[i]) for i in range(n)],
            },
        },
        "ratings": {
            "average": round(avg, 2),
            "total": sum(rating_dist.values()),
            "distribution": rating_dist,
            "delta_avg": round(rng.uniform(-0.1, 0.2), 2),
        },
        "vitals": {
            "crash_rate": crash_rate_v,
            "crash_rate_label": f"{crash_rate_v:.2f}%",
            "crash_series": crash_series,
            "anr_rate": anr_rate_v,
            "anr_rate_label": f"{anr_rate_v:.2f}%",
            "anr_series": anr_series,
            "slow_render_rate": slow_render_v,
            "slow_render_label": f"{slow_render_v:.1f}%",
        },
        "top_countries": top_countries,
        "top_sources": top_sources,
        "trend_daily": trend_daily,
    }

    # ── Gerçek GP verisi overlay (credential varsa) ───────────────────────
    try:
        from backend.services import gp_client
        if gp_client.is_configured():
            pkg = APP_PRODUCTS[pid].get("android_package") or ""
            live = gp_client.build_gp_analytics_payload(pkg, days=p)
            if live:
                payload = _overlay_live_vitals(payload, live)
    except Exception as exc:  # noqa: BLE001
        import logging as _log
        _log.getLogger(__name__).warning("GP live overlay başarısız: %s", exc)

    # ── Puanlama: google-play-scraper ile canlı (credential gerekmez) ─────
    try:
        pkg = APP_PRODUCTS[pid].get("android_package") or ""
        if pkg:
            payload = _overlay_live_ratings(payload, pkg)
    except Exception as exc:  # noqa: BLE001
        import logging as _log
        _log.getLogger(__name__).warning("GP puanlama overlay başarısız: %s", exc)

    return payload


def _overlay_live_ratings(payload: dict[str, Any], package_name: str) -> dict[str, Any]:
    """google-play-scraper meta'sından puan ortalaması + dağılım canlı."""
    try:
        from google_play_scraper import app as gp_app
    except ImportError:
        return payload
    try:
        meta = gp_app(package_name, lang="tr", country="tr")
    except Exception as exc:
        import logging as _log
        _log.getLogger(__name__).warning("GP store meta alınamadı (%s): %s", package_name, exc)
        return payload

    score = meta.get("score")
    total = meta.get("ratings")
    histogram = meta.get("histogram")  # 1-5 yıldız listesi: [<1*>, <2*>, ..., <5*>]
    if score is None or total is None or not histogram:
        return payload

    dist = {}
    if isinstance(histogram, (list, tuple)) and len(histogram) >= 5:
        for i in range(5):
            dist[str(i + 1)] = int(histogram[i] or 0)
    elif isinstance(histogram, dict):
        for k, v in histogram.items():
            dist[str(k)] = int(v or 0)

    if not dist:
        return payload

    payload["ratings"] = {
        "average": round(float(score), 2),
        "total": int(total),
        "distribution": dist,
        "delta_avg": 0.0,
    }
    # Ratings her durumda eklenebilir; source/note'u koruyalım
    cur = payload.get("source")
    if cur == "demo":
        payload["source"] = "live_partial"
        payload["source_note"] = (
            "Kısmi canlı veri — Puanlama gerçek. "
            "Vitals ve kurulum verileri için Play API yetkileri gerekli."
        )
    # cur == "live" veya "live_partial" ise note'u dokunmadan bırak (zaten ratings'i kapsıyor)
    return payload


def _overlay_live_vitals(payload: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
    """Google Play Vitals + install/uninstall (CSV) gerçek verisiyle overlay."""
    crash = live.get("crash_rate_latest")
    anr = live.get("anr_rate_latest")
    c_series = live.get("crash_rate_series") or []
    a_series = live.get("anr_rate_series") or []
    if crash is not None:
        payload["vitals"]["crash_rate"] = round(crash * 100, 3)
        payload["vitals"]["crash_rate_label"] = f"{crash * 100:.2f}%"
        payload["vitals"]["crash_series"] = [round(v * 100, 4) for v in c_series]
    if anr is not None:
        payload["vitals"]["anr_rate"] = round(anr * 100, 3)
        payload["vitals"]["anr_rate_label"] = f"{anr * 100:.2f}%"
        payload["vitals"]["anr_series"] = [round(v * 100, 4) for v in a_series]

    # Install CSV verisi
    inst = live.get("install_stats") or {}
    has_installs = bool(inst and inst.get("dates"))
    if has_installs:
        i_series = inst.get("installs_series") or []
        u_series = inst.get("uninstalls_series") or []
        total_i = int(inst.get("total_installs") or 0)
        total_u = int(inst.get("total_uninstalls") or 0)
        net = max(0, total_i - total_u)

        payload["kpis"]["installs"] = {
            "value": total_i,
            "value_label": _fmt_compact(float(total_i)),
            "delta_pct": 0.0,
            "series": [float(x) for x in i_series],
        }
        payload["kpis"]["uninstalls"] = {
            "value": total_u,
            "value_label": _fmt_compact(float(total_u)),
            "delta_pct": 0.0,
            "series": [float(x) for x in u_series],
        }
        payload["kpis"]["net_installs"] = {
            "value": net,
            "value_label": _fmt_compact(float(net)),
            "delta_pct": 0.0,
            "series": [max(0.0, i_series[k] - u_series[k]) for k in range(min(len(i_series), len(u_series)))],
        }
        # Günlük trend gerçek
        payload["trend_daily"] = [
            {"i": k, "installs": round(i_series[k]),
             "uninstalls": round(u_series[k] if k < len(u_series) else 0)}
            for k in range(len(i_series))
        ]

    # Source ve note: kaç bileşen canlı?
    has_vitals = (crash is not None) or (anr is not None)
    if has_vitals and has_installs:
        payload["source"] = "live"
        payload["source_note"] = (
            "Canlı veri — Vitals (çökme/ANR) ve kurulum istatistikleri Google Play "
            "Console raporlarından gerçek zamanlı çekiliyor (1-2 gün gecikme normaldir)."
        )
    elif has_vitals:
        payload["source"] = "live_partial"
        payload["source_note"] = (
            "Kısmi canlı veri — Android Vitals (çökme/ANR) gerçek. "
            "Kurulum sayıları için GP_REPORTS_BUCKET ortam değişkeni ve bucket "
            "okuma yetkisi onaylanmalı."
        )
    return payload
