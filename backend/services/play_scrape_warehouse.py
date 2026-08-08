"""Play Console scrape explorer — snapshot explorer_facts + Reporting API ANR kırılımları."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from backend.services import gp_client, play_console_store

_SCRAPE_METRICS = (
    "device_acquisition",
    "user_lost",
    "active_devices",
    "dau",
    "ar2_acquisitions",
    "rating",
    "active_users",
    "crashes",
    "anrs",
    "revenue",
    "ar2_visitors",
)

# Eski UI / API uyumu — GCS etiketleri scrape karşılıklarına map
_METRIC_ALIASES = {
    "active": "active_devices",
    "installs": "device_acquisition",
    "uninstalls": "user_lost",
}

# Günlük stok (toplamak yanlış): dönem kartında son gün / ortalama
_STOCK_LAST = frozenset({"active_devices", "active_users", "dau", "active"})
_STOCK_AVG = frozenset({"rating"})
# Play “CUMULATIVE” seriler — grafik/toplam için güne çevrilir
_CUMULATIVE = frozenset({"device_acquisition"})


def scrape_metric_keys() -> list[str]:
    return list(_SCRAPE_METRICS)


def _load_facts() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from backend.database import SessionLocal

    with SessionLocal() as db:
        payload = play_console_store.play_console_payload(db)
    panels = payload.get("panels") if isinstance(payload, dict) else {}
    if not isinstance(panels, dict):
        panels = {}
    facts = panels.get("explorer_facts") or []
    if not isinstance(facts, list):
        facts = []
    meta = {
        "synced_at": payload.get("updated_at") or payload.get("background_synced_at"),
        "stats_views": panels.get("stats_views") or [],
        "explorer_fact_count": panels.get("explorer_fact_count") or len(facts),
        "message": payload.get("message") if isinstance(payload, dict) else None,
        "package_name": (payload.get("package_name") if isinstance(payload, dict) else None) or "com.Doviz",
    }
    return [f for f in facts if isinstance(f, dict)], meta


def _resolve_metric(metric: str) -> str:
    m = (metric or "").strip()
    return _METRIC_ALIASES.get(m, m)


def _series_total(series: list[dict[str, Any]], metric_key: str, breakdown: str) -> tuple[float, str]:
    """Dönem özeti: olay metrikleri toplam; stok son gün; puan ortalama."""
    if not series:
        return 0.0, "sum"
    vals = [float(r.get("value") or 0) for r in series]
    if metric_key in _STOCK_AVG:
        return round(sum(vals) / len(vals), 4), "avg"
    if metric_key in _STOCK_LAST and breakdown in ("date", "week", "month"):
        return float(vals[-1]), "last"
    return round(sum(vals), 4), "sum"


def _decumulate_series(
    series: list[dict[str, Any]],
    *,
    seed_value: float | None = None,
) -> list[dict[str, Any]]:
    """Kümülatif günlük seriyi artışlara çevir (glitch/reset → 0)."""
    if not series:
        return []
    ordered = sorted(series, key=lambda r: str(r.get("key") or ""))
    out: list[dict[str, Any]] = []
    prev: float | None = seed_value
    for r in ordered:
        try:
            v = float(r.get("value") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if prev is None:
            daily = 0.0
            prev = v
        elif v + 1.0 >= prev:
            daily = max(0.0, v - prev)
            prev = v
        else:
            # Ani düşüş (eksik protobuf / glitch) — negatif üretme
            daily = 0.0
        out.append({**r, "value": round(daily, 4)})
    return out


def _normalize_revenue_facts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Play gelir protobuf: (1) mikro birim (~1e6), (2) eski çift-metrik scrape aynı güne 2 satır.

    Günlük REVENUE’yu tercih et: aynı tarih+segment için tek değer (mikroysa /1e6).
    """
    by: dict[tuple[Any, ...], dict[str, Any]] = {}
    for f in facts:
        if str(f.get("metric") or "") != "revenue":
            continue
        try:
            v = float(f.get("value") or 0)
        except (TypeError, ValueError):
            continue
        # Micros heuristic (Play financials sıkça 1e6 birim)
        if abs(v) >= 10_000:
            v = v / 1_000_000.0
        key = (
            str(f.get("date") or "")[:10],
            str(f.get("dim") or "overview"),
            str(f.get("segment") or "OVERALL"),
        )
        prev = by.get(key)
        row = {**f, "value": round(v, 4), "unit": "currency"}
        if prev is None:
            by[key] = row
            continue
        # Eski çift-metrik scrape: iki serinin ortalaması (yeniden scrape tek seri getirir)
        avg = (float(prev.get("value") or 0) + v) / 2.0
        by[key] = {**row, "value": round(avg, 4)}
    others = [f for f in facts if str(f.get("metric") or "") != "revenue"]
    return others + list(by.values())


_DIM_TO_REPORTING = {
    "app_version": "versionCode",
    "device": "deviceModel",
    "os_version": "apiLevel",
    "country": "countryCode",
}


def _enrich_reporting(
    facts: list[dict[str, Any]],
    *,
    metric_key: str,
    dim: str,
    start_d: date,
    end_d: date,
    package_name: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """ANR/crash sürüm / cihaz / OS / ülke kırılımını Reporting API’den ekle.

    Scrape yalnızca tarih×OVERALL ANR/çökme sayıları tutar; boyut kırılımı API’den gelir
    (oran × distinctUsers ≈ etkilenen kullanıcı).
    """
    api_dim = _DIM_TO_REPORTING.get(dim)
    if not api_dim or metric_key not in ("anrs", "crashes"):
        return facts, None
    existing = [
        f
        for f in facts
        if str(f.get("dim")) == dim
        and str(f.get("metric")) == metric_key
        and str(f.get("segment") or "") not in ("", "OVERALL", "all", "ALL")
    ]
    if len(existing) >= 8:
        return facts, None
    if not gp_client.is_configured():
        return facts, (
            "Sürüm/cihaz/OS ANR için Railway’de GP_SERVICE_ACCOUNT_JSON gerekir "
            "(Play Reporting API). Scrape yalnızca tarih bazlı ANR sayıları tutar."
        )
    try:
        if metric_key == "anrs":
            extra = gp_client.fetch_anr_by_dimension(
                package_name, dimension=api_dim, start=start_d, end=end_d
            )
            last_err = getattr(gp_client.fetch_anr_by_dimension, "last_error", None)
        else:
            extra = gp_client.fetch_crash_by_dimension(
                package_name, dimension=api_dim, start=start_d, end=end_d
            )
            last_err = getattr(gp_client.fetch_crash_by_dimension, "last_error", None)
    except Exception as exc:  # noqa: BLE001
        return facts, f"Reporting API hata: {exc}"
    if not extra:
        hint = last_err or "yanıt boş"
        return facts, (
            f"Reporting API boş ({api_dim}: {hint}). "
            "Play Console → Kullanıcılar ve izinler → service account’a "
            "«Uygulama bilgilerini görüntüleme» + Reporting erişimi ver; "
            "API’de playdeveloperreporting etkin olsun. "
            "Mac scrape ile OS/sürüm ANR sayfaları da eklenebilir."
        )
    # Aynı dim+metric eski satırları temizle (overview OVERALL kalsın)
    kept = [
        f
        for f in facts
        if not (
            str(f.get("metric")) == metric_key
            and str(f.get("dim")) == dim
            and str(f.get("source") or "").startswith("reporting_api")
        )
    ]
    src = str((extra[0] or {}).get("source") or "reporting_api")
    return kept + extra, f"Reporting API +{len(extra)} satır ({api_dim}→{dim}, {src})"


def _aggregate(
    use: list[dict[str, Any]],
    *,
    breakdown: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, float] = defaultdict(float)
    for f in use:
        if breakdown == "segment":
            key = str(f.get("segment") or f.get("label") or "UNKNOWN")
        elif breakdown in ("week", "month") and f.get("date") and not str(f["date"]).startswith("i"):
            try:
                d = date.fromisoformat(str(f["date"])[:10])
                if breakdown == "week":
                    iso = d.isocalendar()
                    key = f"{iso.year}-W{iso.week:02d}"
                else:
                    key = f"{d.year}-{d.month:02d}"
            except ValueError:
                key = str(f.get("date") or f.get("label") or "?")
        else:
            key = str(f.get("date") or f.get("segment") or f.get("label") or "TOTAL")
        try:
            buckets[key] += float(f.get("value") or 0)
        except (TypeError, ValueError):
            continue
    series = [{"key": k, "value": round(v, 4)} for k, v in buckets.items()]
    if breakdown in ("date", "week", "month"):
        series.sort(key=lambda r: r["key"])
        # Günlük seride kesme yok (2025→bugün); segment/hafta/ay için üst sınır
        if breakdown != "date":
            series = series[:180]
    else:
        series.sort(key=lambda r: (-r["value"], r["key"]))
        series = series[:120]
    return series


def _densify_date_series(
    series: list[dict[str, Any]],
    *,
    start: date,
    end: date,
    clip_to_data: bool = True,
) -> list[dict[str, Any]]:
    """Eksik günleri 0 ile doldur.

    clip_to_data=True: serideki son gerçek tarihten sonrasını ekleme
    (Play gecikmeli metriklerde son 1–3 günü sahte 0 yapmamak için).
    """
    by_key = {str(r["key"]): float(r.get("value") or 0) for r in series}
    data_dates = []
    for k in by_key:
        try:
            data_dates.append(date.fromisoformat(str(k)[:10]))
        except ValueError:
            continue
    eff_end = end
    eff_start = start
    if clip_to_data and data_dates:
        data_max = max(data_dates)
        data_min = min(data_dates)
        eff_end = min(end, data_max)
        # Seçili aralık tamamen veri dışında kalmasın diye başlangıcı da sıkıştır
        if eff_start > eff_end:
            return []
        if eff_start < data_min and start < data_min:
            # aralık içinde veri yoksa boş bırakma — sadece [start,eff_end] doldur
            pass
    out: list[dict[str, Any]] = []
    cur = eff_start
    while cur <= eff_end:
        k = cur.isoformat()
        out.append({"key": k, "value": round(by_key.get(k, 0.0), 4)})
        cur += timedelta(days=1)
    return out


def query_scrape_analytics(
    *,
    start: str | None = None,
    end: str | None = None,
    metric: str = "anrs",
    breakdown: str = "date",
    dim: str = "overview",
    segment: str | None = None,
    compare: str | None = "previous_period",
) -> dict[str, Any]:
    end_d = date.fromisoformat(end) if end else date.today()
    # Varsayılan: 2025-01-01
    start_d = date.fromisoformat(start) if start else date(2025, 1, 1)
    if start_d > end_d:
        start_d = end_d - timedelta(days=27)
    start_s, end_s = start_d.isoformat(), end_d.isoformat()
    metric_key = _resolve_metric(metric)
    breakdown = breakdown if breakdown in ("date", "week", "month", "segment") else "date"
    dim = dim if dim else "overview"
    # Sürüm/cihaz/OS/ülke seçiliyse günlük toplam yanıltıcı (segmentleri üst üste biner) → boyut kırılımı
    if dim in _DIM_TO_REPORTING and breakdown in ("date", "week", "month"):
        breakdown = "segment"

    facts, meta = _load_facts()
    if metric_key == "revenue":
        facts = _normalize_revenue_facts(facts)
    pkg = str(meta.get("package_name") or "com.Doviz")
    enrich_msg = None
    facts, enrich_msg = _enrich_reporting(
        facts,
        metric_key=metric_key,
        dim=dim,
        start_d=start_d,
        end_d=end_d,
        package_name=pkg,
    )

    if not facts:
        return {
            "ok": False,
            "source": "scrape",
            "configured": True,
            "message": (
                "Scrape explorer_facts boş — Mac’te "
                "`play_console_scrape.py --sync --ingest` çalıştır."
                + (f" · {enrich_msg}" if enrich_msg else "")
            ),
            "series": [],
            "total": 0,
            "row_count": 0,
            "facets": {
                "metrics": scrape_metric_keys(),
                "dims": ["overview", "country", "app_version", "device", "os_version"],
                "segments": [],
            },
            "stats_views": meta.get("stats_views") or [],
            "start": start_s,
            "end": end_s,
        }

    cur = [f for f in facts if str(f.get("metric") or "") == metric_key]
    if not cur:
        available = sorted({str(f.get("metric")) for f in facts if f.get("metric")})
        return {
            "ok": False,
            "source": "scrape",
            "configured": True,
            "message": (
                f"Metrik `{metric_key}` yok. Mevcut: {', '.join(available[:12])}"
                + (f" · {enrich_msg}" if enrich_msg else "")
            ),
            "series": [],
            "total": 0,
            "row_count": 0,
            "facets": {"metrics": available or scrape_metric_keys(), "segments": []},
            "stats_views": meta.get("stats_views") or [],
            "start": start_s,
            "end": end_s,
        }

    if dim in ("", "overview", "all"):
        dim_facts = [f for f in cur if str(f.get("dim") or "overview") in ("overview", "")]
        if not dim_facts:
            dim_facts = [f for f in cur if str(f.get("segment") or "") in ("OVERALL", "", "all")]
        # Ülke satırlarını genel toplama ekleme — 4–5× şişirme yapardı
        if not dim_facts:
            dim_facts = []
    else:
        dim_facts = [f for f in cur if str(f.get("dim") or "") == dim]
        if not dim_facts and dim in _DIM_TO_REPORTING:
            dim_facts = []

    if segment and segment not in ("", "all", "ALL", "OVERALL"):
        dim_facts = [
            f
            for f in dim_facts
            if str(f.get("segment") or "").upper() == segment.upper()
            or str(f.get("segment") or "") == segment
        ]

    dated = []
    undated = []
    for f in dim_facts:
        ds = f.get("date")
        if ds and isinstance(ds, str) and len(ds) >= 8 and not str(ds).startswith("i"):
            if start_s <= str(ds)[:10] <= end_s:
                dated.append(f)
        else:
            undated.append(f)
    use = dated if dated else undated

    # Reporting boyut satırlarında OVERALL’i gösterme
    if dim in _DIM_TO_REPORTING:
        use = [
            f
            for f in use
            if str(f.get("segment") or "") not in ("", "OVERALL", "all", "ALL")
        ]

    series = _aggregate(use, breakdown=breakdown)
    # segment breakdown + date grain: top segments by sum
    if breakdown == "segment" and not series and dated:
        series = _aggregate(
            [
                f
                for f in dated
                if str(f.get("segment") or "") not in ("", "OVERALL", "all", "ALL")
            ],
            breakdown="segment",
        )
    metric_dates = sorted(
        {
            str(f.get("date"))[:10]
            for f in dim_facts
            if f.get("date") and isinstance(f.get("date"), str) and not str(f["date"]).startswith("i")
        }
    )
    metric_date_max = None
    metric_date_min = None
    if metric_dates:
        try:
            metric_date_min = date.fromisoformat(metric_dates[0])
            metric_date_max = date.fromisoformat(metric_dates[-1])
        except ValueError:
            metric_date_min = metric_date_max = None

    lag_note = None
    effective_end = end_d
    if breakdown == "date" and dated and metric_date_max and end_d > metric_date_max:
        effective_end = metric_date_max
        lag_note = (
            f"Play gecikmesi: `{metric_key}` son veri {metric_date_max.isoformat()} "
            f"(seçili bitiş {end_s} — son {(end_d - metric_date_max).days} gün henüz yok)"
        )

    if breakdown == "date" and dated:
        series = _densify_date_series(series, start=start_d, end=effective_end, clip_to_data=True)
    if metric_key in _CUMULATIVE and breakdown == "date" and series:
        seed = None
        try:
            first_d = date.fromisoformat(str(series[0]["key"])[:10])
            seed_day = (first_d - timedelta(days=1)).isoformat()
            for f in dim_facts:
                if (
                    str(f.get("date") or "")[:10] == seed_day
                    and str(f.get("segment") or "OVERALL") in ("OVERALL", "", "all")
                ):
                    seed = float(f.get("value") or 0)
                    break
        except (TypeError, ValueError, KeyError, IndexError):
            seed = None
        series = _decumulate_series(series, seed_value=seed)
    total, total_mode = _series_total(series, metric_key, breakdown)

    compare_payload = None
    if compare == "previous_period" and dated:
        # Kıyası etkili (verisi olan) aralık uzunluğuna göre yap — sahte 0 günleri şişirmesin
        span = (effective_end - start_d).days + 1
        if span < 1:
            span = (end_d - start_d).days + 1
        pe = start_d - timedelta(days=1)
        ps = pe - timedelta(days=span - 1)
        prev = [
            f
            for f in dim_facts
            if f.get("date")
            and isinstance(f.get("date"), str)
            and not str(f["date"]).startswith("i")
            and ps.isoformat() <= str(f["date"])[:10] <= pe.isoformat()
        ]
        prev_series = _aggregate(prev, breakdown=breakdown)
        if breakdown == "date":
            prev_series = _densify_date_series(prev_series, start=ps, end=pe, clip_to_data=True)
        if metric_key in _CUMULATIVE and breakdown == "date" and prev_series:
            seed = None
            try:
                first_d = date.fromisoformat(str(prev_series[0]["key"])[:10])
                seed_day = (first_d - timedelta(days=1)).isoformat()
                for f in dim_facts:
                    if str(f.get("date") or "")[:10] == seed_day:
                        seed = float(f.get("value") or 0)
                        break
                # also search all metric facts before range
                if seed is None:
                    for f in cur:
                        if (
                            str(f.get("metric")) == metric_key
                            and str(f.get("date") or "")[:10] == seed_day
                            and str(f.get("segment") or "OVERALL") in ("OVERALL", "", "all")
                        ):
                            seed = float(f.get("value") or 0)
                            break
            except (TypeError, ValueError, KeyError, IndexError):
                seed = None
            prev_series = _decumulate_series(prev_series, seed_value=seed)
        prev_total, _ = _series_total(prev_series, metric_key, breakdown)
        delta_pct = None
        if prev_total:
            delta_pct = round((total - prev_total) / abs(prev_total) * 100.0, 2)
        compare_payload = {
            "mode": "previous_period",
            "start": ps.isoformat(),
            "end": pe.isoformat(),
            "total": prev_total,
            "delta_pct": delta_pct,
            "series": prev_series,
            "total_mode": total_mode,
        }

    segs = sorted(
        {
            str(f.get("segment"))
            for f in cur
            if f.get("segment") and str(f.get("segment")) not in ("", "OVERALL")
        }
    )[:120]

    dates = metric_dates or sorted(
        {
            str(f.get("date"))[:10]
            for f in cur
            if f.get("date") and not str(f.get("date")).startswith("i")
        }
    )

    msg = f"Scrape · {len(use)} fact · metric={metric_key}"
    if enrich_msg:
        msg += f" · {enrich_msg}"
    if dates:
        msg += f" · bucket {dates[0]}→{dates[-1]}"
    if lag_note:
        msg += f" · {lag_note}"
    if metric_key == "revenue":
        msg += " · birim: Play günlük gelir (mikro→para; eski çift seri tekilleştirildi)"
    if metric_key in _CUMULATIVE and breakdown == "date":
        msg += " · kümülatif→günlük artış"
    if not series and dim in _DIM_TO_REPORTING and metric_key in ("anrs", "crashes"):
        msg = (
            f"ANR/çökme `{dim}` kırılımı boş. "
            "Scrape yalnızca tarih bazlı sayıları tutar; sürüm/cihaz/OS/ülke "
            "Play Reporting API’den gelir. "
            + (enrich_msg or "Reporting yanıtı yok — GP_SERVICE_ACCOUNT_JSON / playdeveloperreporting yetkisini kontrol et.")
        )

    return {
        "ok": bool(series),
        "source": "scrape+reporting" if enrich_msg and "Reporting" in (enrich_msg or "") else "scrape",
        "configured": True,
        "bucket": False,
        "message": msg,
        "start": start_s,
        "end": end_s,
        "effective_end": effective_end.isoformat() if breakdown == "date" else end_s,
        "metric": metric_key,
        "breakdown": breakdown,
        "dim": dim,
        "segment": segment or "all",
        "total": total,
        "total_mode": total_mode,
        "series": series,
        "compare": compare_payload,
        "facets": {
            "metrics": scrape_metric_keys(),
            "dims": ["overview", "country", "app_version", "device", "os_version"],
            "breakdowns": ["date", "week", "month", "segment"],
            "segments": segs,
        },
        "row_count": len(use),
        "stats_views": meta.get("stats_views") or [],
        "synced_at": meta.get("synced_at"),
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "auto_shifted": bool(lag_note),
        "lag_days": (end_d - metric_date_max).days if lag_note and metric_date_max else 0,
    }
