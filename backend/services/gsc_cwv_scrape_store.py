"""GSC CWV + AMP scrape ingest, history, regression e-posta."""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session

from backend.models import GscCwvReportSnapshot, Site
from backend.services.ga4_page_urls import ga4_site_host
from backend.services.warehouse import finish_collector_run, start_collector_run

LOGGER = logging.getLogger(__name__)
SOURCE = "gsc_cwv_scrape"

# Eşikler — Good → kötüleşme / sayım düşüşü
GOOD_DROP_PCT = 5.0          # good URL sayısı % düşüşü
POOR_INCREASE_ABS = 10       # poor mutlak artış
NI_INCREASE_PCT = 8.0        # needs improvement % artış
EXAMPLE_REGRESSION = True    # good örnek URL poor/NI'ye düşerse mail

# GSC shell / Material ikon / nav chrome — scrape body_head kirlenmesi
_GSC_CHROME_MARKERS = (
    "breadcrumbs",
    "keyboard_arrow",
    "manual actions",
    "submit feedback",
    "about search console",
    "privacyterms",
    "privacy terms",
    "achievements",
    "link_2",
    "trophy",
    "layers ",
)
_AMP_TITLE_EXTRACT = (
    r"(Custom JavaScript is not allowed)",
    r"(Görüntü boyutu önerilen boyuttan daha küçük[^.\n]*)",
    r"(Image is smaller than recommended[^.!\n]*)",
    r"(Disallowed HTML tag[^.\n]*)",
    r"(Disallowed attribute[^.\n]*)",
    r"(The tag '[^']+' is disallowed)",
    r"(Missing mandatory attribute[^.\n]*)",
)


def _looks_like_gsc_chrome(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    hits = sum(1 for m in _GSC_CHROME_MARKERS if m in t)
    if hits >= 2:
        return True
    if len(t) > 140 and ("search console" in t or "settings" in t) and hits >= 1:
        return True
    if "breadcrumb" in t and ("settings" in t or "feedback" in t):
        return True
    return False


def clean_issue_title(raw: str, *, fallback: str = "") -> str:
    """AMP/CWV başlığından GSC chrome metnini ayıkla."""
    import re

    s = re.sub(r"\s+", " ", (raw or "").replace("\u00a0", " ")).strip(" .")
    if not s:
        return (fallback or "").strip()
    if not _looks_like_gsc_chrome(s) and len(s) <= 160:
        return s
    for pat in _AMP_TITLE_EXTRACT:
        m = re.search(pat, s, re.I)
        if m:
            return m.group(1).strip(" .")
    # "… AMP <issue>" sondaki kısa parça
    m = re.search(r"\bAMP\s+(.{8,100})$", s, re.I)
    if m and not _looks_like_gsc_chrome(m.group(1)):
        return m.group(1).strip(" .")
    return (fallback or "AMP sorunu").strip()


def clean_issue_causes(causes: list | None, *, title: str = "") -> list[str]:
    out: list[str] = []
    for c in causes or []:
        s = str(c or "").strip()
        if not s or _looks_like_gsc_chrome(s):
            continue
        if s.lower().startswith("gsc sorunu:") and _looks_like_gsc_chrome(s[11:]):
            continue
        if len(s) > 280:
            s = s[:277] + "…"
        out.append(s)
    return out


def _iso_year_ok(iso: str, year_now: int) -> bool:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", iso):
        return False
    y = int(iso[:4])
    return year_now - 3 <= y <= year_now + 1


def _resample_ints(arr: list[Any], n: int) -> list[int]:
    if n <= 0:
        return []
    vals = [int(x or 0) for x in (arr or [])]
    m = len(vals)
    if m == n:
        return vals
    if m <= 0:
        return [0] * n
    if m == 1:
        return [vals[0]] * n
    out: list[int] = []
    for i in range(n):
        j = int(round(i * (m - 1) / max(n - 1, 1)))
        out.append(vals[j])
    return out


def _series_value_len(ser: dict[str, Any] | None) -> int:
    if not isinstance(ser, dict):
        return 0
    return max(
        len(ser.get("dates") or []),
        len(ser.get("poor") or []),
        len(ser.get("needs_improvement") or []),
        len(ser.get("good") or []),
    )


def _filter_plausible_dates(ser: dict[str, Any], year_now: int) -> dict[str, Any] | None:
    dates = [str(d or "")[:10] for d in (ser.get("dates") or [])]
    keep = [i for i, d in enumerate(dates) if _iso_year_ok(d, year_now)]
    if len(keep) < 3:
        return None
    kept_dates = [dates[i] for i in keep]
    if kept_dates[0] > kept_dates[-1]:
        return None
    out = dict(ser)
    out["dates"] = kept_dates
    for metric in ("poor", "needs_improvement", "good"):
        arr = list(ser.get(metric) or [])
        if arr:
            out[metric] = [int(arr[i]) if i < len(arr) else 0 for i in keep]
    out["point_count"] = len(kept_dates)
    return out


def _fill_interior_zeros(arr: list[int]) -> list[int]:
    """SVG/tarama boşluklarındaki 0'ları komşu GSC noktalarıyla doğrusal doldur."""
    out = [int(x or 0) for x in (arr or [])]
    n = len(out)
    i = 0
    while i < n:
        if out[i] != 0:
            i += 1
            continue
        j = i
        while j < n and out[j] == 0:
            j += 1
        left = out[i - 1] if i > 0 else None
        right = out[j] if j < n else None
        if left is not None and right is not None and (left >= 40 or right >= 40):
            span = j - i + 1
            for k, idx in enumerate(range(i, j)):
                t = (k + 1) / span
                out[idx] = int(round(left + t * (right - left)))
        i = j
    return out


def _is_jagged(ser: dict[str, Any] | None) -> bool:
    """Sıfır-tepe testere (SVG boşluğu) — GSC günlük eğrisi böyle salınmaz."""
    if not isinstance(ser, dict):
        return False
    for metric in ("needs_improvement", "good", "poor"):
        arr = [int(x or 0) for x in (ser.get(metric) or [])]
        if _is_zero_cross_jagged(arr) or _is_sawtooth(arr):
            return True
    return False


def _is_zero_cross_jagged(arr: list[int]) -> bool:
    if len(arr) < 10:
        return False
    cross = 0
    for a, b in zip(arr, arr[1:]):
        if (a == 0) != (b == 0) and max(a, b) >= 100:
            cross += 1
    return cross >= 6


def _is_sawtooth(arr: list[int] | None) -> bool:
    """Sıfır olmayan 5k↔25k EKG — yığılmış çubuk SVG Y örneklemesi."""
    vals = [int(x or 0) for x in (arr or [])]
    if len(vals) < 12:
        return False
    med = sorted(vals)[len(vals) // 2]
    if med < 80:
        return False
    thresh = max(200, int(0.22 * med))
    flips = 0
    big = 0
    prev = 0
    for a, b in zip(vals, vals[1:]):
        d = b - a
        if abs(d) < thresh:
            continue
        big += 1
        sign = 1 if d > 0 else -1
        if prev and sign != prev:
            flips += 1
        prev = sign
    return flips >= 8 and big >= 10


def _desawtooth(arr: list[int]) -> list[int]:
    """Komşu ortalaması — çubuk tepesi/tabanı örneklemesini GSC eğrisine yaklaştırır."""
    out = [int(x or 0) for x in (arr or [])]
    if len(out) < 12 or not _is_sawtooth(out):
        return out
    for _ in range(5):
        nxt = list(out)
        if len(out) >= 2:
            nxt[0] = int(round((out[0] + out[1]) / 2.0))
            nxt[-1] = int(round((out[-2] + out[-1]) / 2.0))
        for i in range(1, len(out) - 1):
            nxt[i] = int(round((out[i - 1] + out[i] + out[i + 1]) / 3.0))
        out = nxt
        if not _is_sawtooth(out):
            break
    return out


def _suppress_false_poor_spikes(arr: list[int]) -> list[int]:
    """KPI 0 kötü iken SVG'nin kırmızı tepe-sıfır testeresini düzle."""
    out = [int(x or 0) for x in (arr or [])]
    if len(out) < 8:
        return out
    zeros = sum(1 for v in out if v == 0)
    if zeros < 0.35 * len(out):
        return out
    nz = [v for v in out if v > 0]
    if nz and sorted(nz)[len(nz) // 2] >= 80:
        return [0] * len(out)
    return out


def _fix_series_tail(
    ser: dict[str, Any] | None,
    kpis: dict[str, Any] | None = None,
    *,
    tail_days: int = 14,
) -> dict[str, Any] | None:
    """Son 2 hafta: SVG sağ kenarındaki 0'ları son gerçek GSC gününe çek; son gün = KPI."""
    if not isinstance(ser, dict):
        return ser
    dates = list(ser.get("dates") or [])
    n = len(dates)
    if n < 4:
        return ser
    tail_start = max(0, n - max(1, int(tail_days)))
    out = dict(ser)
    for metric in ("poor", "needs_improvement", "good"):
        arr = [int(x or 0) for x in (out.get(metric) or [])]
        if len(arr) != n:
            continue
        kpi_v: int | None = None
        if isinstance(kpis, dict):
            try:
                kpi_v = int(kpis.get(metric))
            except (TypeError, ValueError):
                kpi_v = None
        anchor_idx = tail_start - 1
        while anchor_idx >= 0 and arr[anchor_idx] <= 0:
            anchor_idx -= 1
        if anchor_idx < 0:
            if kpi_v is not None and arr:
                arr[-1] = kpi_v
            out[metric] = arr
            continue
        anchor = arr[anchor_idx]
        if anchor <= 0 and kpi_v is not None:
            anchor = kpi_v
        if anchor <= 0:
            continue
        for i in range(tail_start, n):
            if arr[i] <= 0:
                arr[i] = anchor
        if kpi_v is not None:
            arr[-1] = kpi_v
        out[metric] = arr
    return out


def _apply_kpis_to_series(ser: dict[str, Any], kpis: dict[str, Any] | None) -> dict[str, Any]:
    """Son noktayı GSC kartına kilitle. Tüm seriyi oranla çarpma — testereyi şişirir."""
    if not isinstance(kpis, dict):
        return ser
    out = dict(ser)
    for metric in ("poor", "needs_improvement", "good"):
        arr = [int(x or 0) for x in (out.get(metric) or [])]
        if not arr:
            continue
        try:
            kpi = int(kpis.get(metric))
        except (TypeError, ValueError):
            continue
        if kpi == 0:
            med = sorted(arr)[len(arr) // 2]
            if med <= 80 or sum(1 for v in arr if v == 0) >= 0.25 * len(arr):
                out[metric] = [0] * len(arr)
                continue
        last = arr[-1]
        if last > 50 and kpi > 50:
            ratio = kpi / last
            # Kayıtlı KPI bazen testerenin son dişi (18k); GSC kartı ~15k.
            # Uzaksa son noktayı zehirleme — düzeltme ortalaması kalsın.
            if abs(ratio - 1.0) > 0.20:
                out[metric] = arr
                continue
        arr[-1] = kpi
        out[metric] = arr
    return out


def _repair_gsc_values(ser: dict[str, Any] | None, kpis: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if not isinstance(ser, dict):
        return None
    out = dict(ser)
    out["needs_improvement"] = _desawtooth(_fill_interior_zeros(list(out.get("needs_improvement") or [])))
    out["good"] = _desawtooth(_fill_interior_zeros(list(out.get("good") or [])))
    poor = list(out.get("poor") or [])
    if poor:
        out["poor"] = _suppress_false_poor_spikes(poor)
        if out["poor"] == poor:
            out["poor"] = _fill_interior_zeros(poor)
        out["poor"] = _desawtooth(out["poor"])
    out = _fix_series_tail(out, kpis, tail_days=14)
    out = _apply_kpis_to_series(out, kpis)
    if _is_zero_cross_jagged(list(out.get("needs_improvement") or [])) or _is_zero_cross_jagged(
        list(out.get("good") or [])
    ):
        return None
    return out


def _bind_series_to_dates(ser: dict[str, Any] | None, dates: list[str]) -> dict[str, Any] | None:
    """Yanlış yıl etiketli GSC Y değerlerini kardeş cihazın 2026 eksenine oturt."""
    n = len(dates)
    if not isinstance(ser, dict) or n < 3:
        return None
    m = _series_value_len(ser)
    if m < 3:
        return None
    if abs(m - n) > max(8, int(0.12 * max(n, m))):
        return None
    if _is_jagged(ser):
        return None
    out = dict(ser)
    out["dates"] = list(dates)
    out["poor"] = _resample_ints(list(ser.get("poor") or []), n)
    out["needs_improvement"] = _resample_ints(list(ser.get("needs_improvement") or []), n)
    out["good"] = _resample_ints(list(ser.get("good") or []), n)
    out["point_count"] = n
    out["axis_source"] = "sibling"
    return out


def _sync_kpis_from_chart_last(payload: dict[str, Any], chart: dict[str, Any] | None) -> None:
    """Legend / kart = grafiğin son günü (GSC tooltip/kart ile hizalı onarım sonrası)."""
    if not isinstance(payload, dict) or not isinstance(chart, dict):
        return
    ov = payload.get("overview") if isinstance(payload.get("overview"), dict) else None
    for key in ("mobile", "desktop"):
        ser = chart.get(key) if isinstance(chart.get(key), dict) else None
        if not ser:
            continue
        last: dict[str, int] = {}
        for metric in ("poor", "needs_improvement", "good"):
            arr = list(ser.get(metric) or [])
            if arr:
                last[metric] = int(arr[-1] or 0)
        if not last:
            continue
        dev = payload.get(key) if isinstance(payload.get(key), dict) else None
        if isinstance(dev, dict):
            kpis = dict(dev.get("kpis") or {})
            kpis.update(last)
            dev["kpis"] = kpis
        if isinstance(ov, dict) and isinstance(ov.get(key), dict):
            ov[key] = {**ov[key], **last}


def _kpis_by_device(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    ov = payload.get("overview") if isinstance(payload.get("overview"), dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for key in ("mobile", "desktop"):
        dev = payload.get(key) if isinstance(payload.get(key), dict) else {}
        kpis = dev.get("kpis") if isinstance(dev.get("kpis"), dict) else None
        if not kpis:
            kpis = ov.get(key) if isinstance(ov.get(key), dict) else {}
        out[key] = kpis if isinstance(kpis, dict) else {}
    return out


def _series_quality(ser: Any, year_now: int, *, peer_len: int = 0) -> int:
    """Uzun 2026 GSC eğrisi > kardeş uzunluğuna uyan sapmış seri > kısa tarama penceresi."""
    if not isinstance(ser, dict):
        return 0
    if _is_jagged(ser):
        ser = _repair_gsc_values(ser)
        if not isinstance(ser, dict):
            return 0
    src = str(ser.get("source") or "")
    bonus = 80 if "tooltip" in src else 0
    dates = [str(d or "")[:10] for d in (ser.get("dates") or [])]
    ok = [d for d in dates if _iso_year_ok(d, year_now)]
    if len(ok) >= 3:
        try:
            span = (date.fromisoformat(ok[-1]) - date.fromisoformat(ok[0])).days
        except ValueError:
            span = len(ok)
        return max(span, 0) * 20 + len(ok) + bonus
    n = _series_value_len(ser)
    if n < 3:
        return 0
    if peer_len >= 3 and abs(n - peer_len) <= max(8, int(0.12 * peer_len)):
        return 200 + n + bonus
    return 0


def sanitize_chart_series(
    chart: dict[str, Any] | None,
    *,
    year_now: int | None = None,
    kpis_by_device: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Sapmış 2007 eksenini düşür; testere SVG'yi GSC eğrisine yaklaştır / at."""
    if not isinstance(chart, dict):
        return {}
    y_now = year_now or datetime.utcnow().year
    kpis_by_device = kpis_by_device if isinstance(kpis_by_device, dict) else {}
    out = dict(chart)
    filtered: dict[str, dict[str, Any] | None] = {}
    for key in ("mobile", "desktop"):
        ser = out.get(key)
        if not isinstance(ser, dict):
            filtered[key] = None
            continue
        filtered[key] = _filter_plausible_dates(ser, y_now)

    donor_dates: list[str] | None = None
    for min_n in (30, 3):
        for key in ("desktop", "mobile"):
            ser = filtered.get(key)
            dates = list((ser or {}).get("dates") or []) if isinstance(ser, dict) else []
            if len(dates) >= min_n:
                donor_dates = dates
                break
        if donor_dates:
            break

    for key in ("mobile", "desktop"):
        kpis = kpis_by_device.get(key) if isinstance(kpis_by_device.get(key), dict) else {}
        ser = filtered.get(key)
        if isinstance(ser, dict) and len(ser.get("dates") or []) >= 3:
            repaired = _repair_gsc_values(ser, kpis)
            out[key] = repaired
            continue
        raw = out.get(key) if isinstance(out.get(key), dict) else None
        if raw:
            raw = _repair_gsc_values(raw, kpis)
        bound = _bind_series_to_dates(raw, donor_dates) if donor_dates and raw else None
        if bound:
            bound = _repair_gsc_values(bound, kpis)
        out[key] = bound
    return out


def recover_chart_series(
    current: dict[str, Any] | None,
    older: list[Any] | None = None,
    *,
    year_now: int | None = None,
    kpis_by_device: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Eksik/kısa cihaz serisini önceki snapshot'tan tamamla, sonra sanitize et."""
    y_now = year_now or datetime.utcnow().year
    picked: dict[str, Any] = dict(current) if isinstance(current, dict) else {}
    pool = [picked]
    for cs in older or []:
        if isinstance(cs, dict):
            pool.append(cs)
    best_desk = picked.get("desktop")
    for cs in pool:
        cand = cs.get("desktop")
        if _series_quality(cand, y_now) > _series_quality(best_desk, y_now):
            best_desk = cand
    picked["desktop"] = best_desk
    peer_n = 0
    if isinstance(best_desk, dict):
        filt = _filter_plausible_dates(best_desk, y_now)
        peer_n = len((filt or {}).get("dates") or []) or _series_value_len(best_desk)
    best_mob = picked.get("mobile")
    for cs in pool:
        cand = cs.get("mobile")
        if _series_quality(cand, y_now, peer_len=peer_n) > _series_quality(
            best_mob, y_now, peer_len=peer_n
        ):
            best_mob = cand
    picked["mobile"] = best_mob
    return sanitize_chart_series(picked, year_now=y_now, kpis_by_device=kpis_by_device)


def sanitize_cwv_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Panel/ingest öncesi AMP+drilldown başlık ve nedenlerini temizle."""
    if not isinstance(payload, dict):
        return {}
    for key in ("mobile", "desktop"):
        dev = payload.get(key)
        if not isinstance(dev, dict):
            continue
        for bucket in ("issues", "issue_drilldowns"):
            rows = dev.get(bucket)
            if not isinstance(rows, list):
                continue
            for d in rows:
                if not isinstance(d, dict):
                    continue
                d["title"] = clean_issue_title(str(d.get("title") or ""), fallback=str(d.get("metric") or "CWV sorunu"))
                d["causes"] = clean_issue_causes(d.get("causes"), title=str(d.get("title") or ""))
    amp = payload.get("amp")
    if isinstance(amp, dict):
        clean_issues: list[dict[str, Any]] = []
        for d in amp.get("issues") or []:
            if not isinstance(d, dict):
                continue
            title = clean_issue_title(
                str(d.get("title") or ""),
                fallback="AMP sorunu",
            )
            d = {**d, "title": title, "causes": clean_issue_causes(d.get("causes"), title=title)}
            # overview_body gibi chrome metni panoda gösterme
            clean_issues.append(d)
        amp["issues"] = clean_issues
        amp.pop("overview_body", None)
        amp["url_row_count"] = sum(int(i.get("url_row_count") or 0) for i in clean_issues)
    return payload


def resolve_site(db: Session, site_domain: str) -> Site | None:
    want = (site_domain or "").strip().lower()
    if not want:
        return None
    naked = want[4:] if want.startswith("www.") else want
    candidates = {want, naked, f"www.{naked}"}
    sites = db.query(Site).filter(Site.is_active.is_(True)).all()
    for s in sites:
        d = (s.domain or "").strip().lower()
        if d in candidates:
            return s
        host = (ga4_site_host(s.domain) or "").lower()
        if host in candidates:
            return s
    for key in ("doviz.com", "sinemalar.com"):
        if key in want:
            for s in sites:
                d = (s.domain or "").lower()
                if key in d and "canli" not in d:
                    return s
    return None


def _url_set_from_device(dev: dict[str, Any], *, bucket: str) -> set[str]:
    urls: set[str] = set()
    if bucket == "good":
        for r in dev.get("good_urls") or []:
            u = (r.get("url") or "").strip()
            if u:
                urls.add(u)
        return urls
    for d in dev.get("issue_drilldowns") or []:
        st = (d.get("status") or "").lower()
        if bucket == "poor" and st != "poor":
            continue
        if bucket == "needs_improvement" and st not in ("needs_improvement", "ni", "warning"):
            continue
        for r in d.get("url_rows") or []:
            u = (r.get("url") or "").strip()
            if u:
                urls.add(u)
    return urls


def _collect_problem_urls(payload: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("mobile", "desktop"):
        out |= _url_set_from_device(payload.get(key) or {}, bucket="poor")
        out |= _url_set_from_device(payload.get(key) or {}, bucket="needs_improvement")
    for iss in (payload.get("amp") or {}).get("issues") or []:
        for r in iss.get("url_rows") or []:
            u = (r.get("url") or "").strip()
            if u:
                out.add(u)
    return out


def detect_regressions(
    prev: dict[str, Any] | None,
    curr: dict[str, Any],
) -> list[dict[str, Any]]:
    """Eşik + örnek URL regresyonları."""
    if not prev:
        return []
    alerts: list[dict[str, Any]] = []
    for device in ("mobile", "desktop"):
        p = (prev.get(device) or {}).get("kpis") or {}
        c = (curr.get(device) or {}).get("kpis") or {}
        pg, cg = int(p.get("good") or 0), int(c.get("good") or 0)
        pp, cp = int(p.get("poor") or 0), int(c.get("poor") or 0)
        pn, cn = int(p.get("needs_improvement") or 0), int(c.get("needs_improvement") or 0)
        if pg > 0 and cg < pg:
            drop_pct = (pg - cg) / pg * 100.0
            if drop_pct >= GOOD_DROP_PCT:
                alerts.append(
                    {
                        "kind": "good_drop",
                        "device": device,
                        "message": (
                            f"{device}: Good URL {pg:,} → {cg:,} (−{drop_pct:.1f}% ≥ {GOOD_DROP_PCT}%)"
                        ),
                        "prev_good": pg,
                        "curr_good": cg,
                    }
                )
        if cp - pp >= POOR_INCREASE_ABS:
            alerts.append(
                {
                    "kind": "poor_increase",
                    "device": device,
                    "message": f"{device}: Poor URL {pp:,} → {cp:,} (+{cp - pp})",
                }
            )
        if pn > 0 and cn > pn:
            ni_pct = (cn - pn) / pn * 100.0
            if ni_pct >= NI_INCREASE_PCT:
                alerts.append(
                    {
                        "kind": "ni_increase",
                        "device": device,
                        "message": f"{device}: Needs improvement {pn:,} → {cn:,} (+{ni_pct:.1f}%)",
                    }
                )
        if EXAMPLE_REGRESSION:
            prev_good = _url_set_from_device(prev.get(device) or {}, bucket="good")
            curr_bad = _url_set_from_device(curr.get(device) or {}, bucket="poor") | _url_set_from_device(
                curr.get(device) or {}, bucket="needs_improvement"
            )
            moved = sorted(prev_good & curr_bad)
            if moved:
                alerts.append(
                    {
                        "kind": "url_regression",
                        "device": device,
                        "message": (
                            f"{device}: {len(moved)} örnek URL Good listesinden Poor/NI tarafına geçti"
                        ),
                        "urls": moved[:40],
                    }
                )
    return alerts


def _send_regression_email(site: Site, payload: dict[str, Any], alerts: list[dict[str, Any]]) -> bool:
    if not alerts:
        return False
    try:
        from backend.config import settings
        from backend.services.mailer import send_email
    except Exception:
        return False
    if not getattr(settings, "outbound_email_enabled", False):
        LOGGER.info("CWV alert mail atlandı (outbound_email_enabled=false)")
        return False
    to_addr = (
        getattr(settings, "operations_mail_to", None)
        or getattr(settings, "mail_to", None)
        or ""
    )
    domain = site.domain or payload.get("site_domain") or ""
    rows = "".join(
        f"<li style='margin:6px 0'><b>{html.escape(str(a.get('kind') or ''))}</b> — "
        f"{html.escape(str(a.get('message') or ''))}"
        + (
            "<ul>"
            + "".join(
                f"<li style='font-size:12px;word-break:break-all'>{html.escape(str(u))}</li>"
                for u in (a.get("urls") or [])[:15]
            )
            + "</ul>"
            if a.get("urls")
            else ""
        )
        + "</li>"
        for a in alerts
    )
    totals = payload.get("totals") or {}
    safe_domain = html.escape(str(domain))
    body_html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:680px">
      <p style="font-size:16px;font-weight:700;color:#b45309;margin:0 0 8px">Web Vitals eşik uyarısı · {safe_domain}</p>
      <p style="font-size:13px;color:#64748b;margin:0 0 16px">
        Scrape sonrası regresyon · Poor {int(totals.get('poor') or 0)} ·
        NI {int(totals.get('needs_improvement') or 0)} · Good {int(totals.get('good') or 0)}
      </p>
      <ul style="padding-left:18px;color:#334155;font-size:13px">{rows}</ul>
      <p style="font-size:12px;color:#94a3b8;margin-top:18px">
        Panel: <a href="https://projectcontrol.up.railway.app/web-vitals?site_id={int(site.id)}">/web-vitals</a>
      </p>
    </div>
    """
    subject = f"⚠ Web Vitals regresyon · {domain} · {len(alerts)} uyarı"
    try:
        send_email(subject, body_html, recipients=[to_addr] if to_addr else None)
        LOGGER.info("CWV regression mail gönderildi site=%s alerts=%d", domain, len(alerts))
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("CWV mail fail: %s", exc)
        return False


def latest_snapshot(db: Session, site_id: int) -> GscCwvReportSnapshot | None:
    return (
        db.query(GscCwvReportSnapshot)
        .filter(GscCwvReportSnapshot.site_id == site_id)
        .order_by(GscCwvReportSnapshot.collected_at.desc(), GscCwvReportSnapshot.id.desc())
        .first()
    )


def history_points(db: Session, site_id: int, *, limit: int = 60) -> list[dict[str, Any]]:
    """Son N scrape noktasını kronolojik (eski→yeni) döner."""
    lim = max(5, min(200, int(limit or 60)))
    rows = (
        db.query(GscCwvReportSnapshot)
        .filter(GscCwvReportSnapshot.site_id == site_id)
        .order_by(
            GscCwvReportSnapshot.collected_at.desc(),
            GscCwvReportSnapshot.id.desc(),
        )
        .limit(lim)
        .all()
    )
    rows = list(reversed(rows))
    out = []
    for r in rows:
        mob: dict[str, Any] = {}
        desk: dict[str, Any] = {}
        if r.payload_json:
            try:
                payload = json.loads(r.payload_json)
                mob = ((payload.get("mobile") or {}).get("kpis") or (payload.get("overview") or {}).get("mobile") or {})
                desk = ((payload.get("desktop") or {}).get("kpis") or (payload.get("overview") or {}).get("desktop") or {})
            except Exception:
                mob, desk = {}, {}
        out.append(
            {
                "collected_at": r.collected_at.isoformat() if r.collected_at else "",
                "poor": int(r.poor_count or 0),
                "needs_improvement": int(r.ni_count or 0),
                "good": int(r.good_count or 0),
                "amp_url_count": int(r.amp_url_count or 0),
                "mobile": {
                    "poor": int(mob.get("poor") or 0),
                    "needs_improvement": int(mob.get("needs_improvement") or 0),
                    "good": int(mob.get("good") or 0),
                },
                "desktop": {
                    "poor": int(desk.get("poor") or 0),
                    "needs_improvement": int(desk.get("needs_improvement") or 0),
                    "good": int(desk.get("good") or 0),
                },
            }
        )
    return out


def _dedupe_drilldowns(payload: dict[str, Any]) -> dict[str, Any]:
    """Aynı item_key iki kez gelmesin (click + fallback)."""
    for key in ("mobile", "desktop"):
        dev = payload.get(key)
        if not isinstance(dev, dict):
            continue
        seen: set[str] = set()
        clean: list[dict[str, Any]] = []
        for d in dev.get("issue_drilldowns") or []:
            if not isinstance(d, dict):
                continue
            ik = str(d.get("item_key") or "").strip()
            title = str(d.get("title") or "").strip().lower()
            sig = ik or f"t:{title}"
            if sig in seen:
                continue
            seen.add(sig)
            clean.append(d)
        dev["issue_drilldowns"] = clean
    amp = payload.get("amp")
    if isinstance(amp, dict):
        seen_a: set[str] = set()
        clean_a: list[dict[str, Any]] = []
        for d in amp.get("issues") or []:
            if not isinstance(d, dict):
                continue
            ik = str(d.get("item_key") or "").strip()
            if ik and ik in seen_a:
                continue
            if ik:
                seen_a.add(ik)
            clean_a.append(d)
        amp["issues"] = clean_a
        amp["url_row_count"] = sum(int(i.get("url_row_count") or 0) for i in clean_a)
    return payload


def ingest_gsc_cwv_payload(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    snaps = list(payload.get("snapshots") or [])
    if not snaps:
        return {"ok": False, "message": "snapshots boş"}

    saved = 0
    mailed = 0
    errors: list[str] = []
    for snap in snaps:
        if not isinstance(snap, dict) or snap.get("error"):
            errors.append(str((snap or {}).get("error") or "bad snapshot"))
            continue
        site = resolve_site(db, str(snap.get("site_domain") or ""))
        if site is None:
            errors.append(f"site yok: {snap.get('site_domain')}")
            continue

        prev_row = latest_snapshot(db, site.id)
        prev_payload = None
        if prev_row and prev_row.payload_json:
            try:
                prev_payload = json.loads(prev_row.payload_json)
            except Exception:
                prev_payload = None

        snap = _dedupe_drilldowns(dict(snap))
        snap = sanitize_cwv_payload(snap)
        # Hızlı charts-only: önceki drilldown/AMP verisini koru, sadece grafik+KPI güncelle
        if snap.get("charts_only") and isinstance(prev_payload, dict):
            merged = dict(prev_payload)
            for k in ("overview", "totals", "last_updated", "scraped_at"):
                if snap.get(k) is not None:
                    merged[k] = snap.get(k)
            if snap.get("chart_series") is not None:
                new_cs = snap.get("chart_series") if isinstance(snap.get("chart_series"), dict) else {}
                old_cs = merged.get("chart_series") if isinstance(merged.get("chart_series"), dict) else {}
                keep = dict(old_cs)
                for key in ("mobile", "desktop"):
                    if new_cs.get(key):
                        keep[key] = new_cs.get(key)
                if new_cs.get("source"):
                    keep["source"] = new_cs.get("source")
                merged["chart_series"] = keep
            for dev in ("mobile", "desktop"):
                cur = snap.get(dev) if isinstance(snap.get(dev), dict) else {}
                old = merged.get(dev) if isinstance(merged.get(dev), dict) else {}
                merged[dev] = {
                    **old,
                    "kpis": (cur or {}).get("kpis") or old.get("kpis") or {},
                    "last_updated": (cur or {}).get("last_updated") or old.get("last_updated") or "",
                }
            # AMP charts_only'de atlanır — eskiyi tut
            if not (snap.get("amp") or {}).get("skipped"):
                merged["amp"] = snap.get("amp")
            snap = merged
        prev_cs = (
            prev_payload.get("chart_series")
            if isinstance(prev_payload, dict) and isinstance(prev_payload.get("chart_series"), dict)
            else None
        )
        snap["chart_series"] = recover_chart_series(
            snap.get("chart_series") if isinstance(snap.get("chart_series"), dict) else {},
            [prev_cs] if prev_cs else [],
            kpis_by_device=_kpis_by_device(snap),
        )
        _sync_kpis_from_chart_last(snap, snap.get("chart_series"))

        totals = snap.get("totals") or {}
        poor = int(totals.get("poor") or 0)
        ni = int(totals.get("needs_improvement") or 0)
        good = int(totals.get("good") or 0)
        amp_n = int((snap.get("amp") or {}).get("url_row_count") or 0)

        run = start_collector_run(
            db,
            site_id=site.id,
            provider="gsc_cwv",
            strategy="scrape",
            target_url=f"https://search.google.com/search-console/core-web-vitals?resource_id={quote(str(snap.get('resource_id') or ''), safe='')}",
            trigger_source=SOURCE,
        )
        row = GscCwvReportSnapshot(
            site_id=site.id,
            resource_id=str(snap.get("resource_id") or ""),
            source=SOURCE,
            payload_json=json.dumps(snap, ensure_ascii=False),
            poor_count=poor,
            ni_count=ni,
            good_count=good,
            amp_url_count=amp_n,
            collected_at=datetime.utcnow(),
        )
        db.add(row)
        finish_collector_run(
            db,
            run,
            status="success",
            row_count=poor + ni + good + amp_n,
            summary={"poor": poor, "ni": ni, "good": good, "amp": amp_n},
        )
        db.commit()
        saved += 1

        alerts = detect_regressions(prev_payload, snap)
        if alerts and _send_regression_email(site, snap, alerts):
            mailed += 1

    ok = saved > 0
    return {
        "ok": ok,
        "saved": saved,
        "mailed": mailed,
        "errors": errors,
        "message": f"GSC CWV ingest · {saved} snapshot" + (f" · {mailed} mail" if mailed else ""),
        "source": SOURCE,
    }


def _recent_chart_series(db: Session, site_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
    rows = (
        db.query(GscCwvReportSnapshot)
        .filter(GscCwvReportSnapshot.site_id == site_id)
        .order_by(
            GscCwvReportSnapshot.collected_at.desc(),
            GscCwvReportSnapshot.id.desc(),
        )
        .limit(max(3, min(40, int(limit or 20))))
        .all()
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        if not r.payload_json:
            continue
        try:
            payload = json.loads(r.payload_json)
        except Exception:
            continue
        cs = payload.get("chart_series") if isinstance(payload, dict) else None
        if isinstance(cs, dict):
            out.append(cs)
    return out


def build_panel_context(db: Session, site: Site) -> dict[str, Any]:
    row = latest_snapshot(db, site.id)
    payload: dict[str, Any] = {}
    if row and row.payload_json:
        try:
            payload = json.loads(row.payload_json)
        except Exception:
            payload = {}
    payload = sanitize_cwv_payload(payload if isinstance(payload, dict) else {})
    older = _recent_chart_series(db, site.id)
    payload["chart_series"] = recover_chart_series(
        payload.get("chart_series") if isinstance(payload.get("chart_series"), dict) else {},
        older,
        kpis_by_device=_kpis_by_device(payload),
    )
    _sync_kpis_from_chart_last(payload, payload.get("chart_series"))
    hist = history_points(db, site.id)
    rid = payload.get("resource_id") or (
        "sc-domain:doviz.com" if "doviz" in (site.domain or "") else f"https://{site.domain}/"
    )
    return {
        "payload": payload,
        "history": hist,
        "collected_at": row.collected_at.isoformat() if row and row.collected_at else "",
        "gsc_links": {
            "main": f"https://search.google.com/u/0/search-console/core-web-vitals?resource_id={quote(str(rid), safe='')}",
            "mobile_summary": f"https://search.google.com/u/0/search-console/core-web-vitals/summary?resource_id={quote(str(rid), safe='')}&device=2",
            "desktop_summary": f"https://search.google.com/u/0/search-console/core-web-vitals/summary?resource_id={quote(str(rid), safe='')}&device=1",
            "amp": f"https://search.google.com/u/0/search-console/amp?resource_id={quote(str(rid), safe='')}",
        },
        "thresholds": {
            "good_drop_pct": GOOD_DROP_PCT,
            "poor_increase_abs": POOR_INCREASE_ABS,
            "ni_increase_pct": NI_INCREASE_PCT,
        },
    }
