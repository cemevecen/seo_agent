#!/usr/bin/env python3
"""pagespeed.web.dev scrape → Project Control ingest.

Playwright ile pagespeed.web.dev analizini açar; batchexecute RPC’den
CrUX field + Lighthouse lab JSON çıkarır, Railway ingest’e yollar.

Örnek:
  .venv/bin/python scripts/pagespeed_web_scrape.py --sync --ingest
  .venv/bin/python scripts/pagespeed_web_scrape.py --domain www.doviz.com --form-factor desktop --ingest

Sentetik/mock üretmez. PSI yanıtı yoksa hata döner.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

DEFAULT_TARGETS: list[dict[str, str]] = [
    {"domain": "www.doviz.com", "url": "https://www.doviz.com"},
    {"domain": "www.sinemalar.com", "url": "https://www.sinemalar.com"},
]

INGEST_URL = (
    os.environ.get("PAGESPEED_WEB_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "pagespeed-web")
    or "https://projectcontrol.up.railway.app/api/pagespeed-web/ingest"
).strip()

_FIELD_KEYS = (
    "largest_contentful_paint",
    "interaction_to_next_paint",
    "cumulative_layout_shift",
    "first_contentful_paint",
    "experimental_time_to_first_byte",
)


def _ingest_token() -> str:
    return (
        os.environ.get("PAGESPEED_WEB_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("PLAY_CONSOLE_INGEST_TOKEN")
        or os.environ.get("BRIDGE_INGEST_TOKEN")
        or ""
    ).strip()


def _report_url(site_url: str, form_factor: str) -> str:
    return (
        "https://pagespeed.web.dev/report"
        f"?url={quote(site_url, safe='')}"
        f"&form_factor={form_factor}"
    )


def _parse_batchexecute(raw: str) -> list[Any]:
    body = raw
    if body.startswith(")]}'"):
        body = body.split("\n", 1)[1]
    body = body.lstrip("\n")
    pos = 0
    out: list[Any] = []
    while pos < len(body):
        while pos < len(body) and body[pos] in "\r\n":
            pos += 1
        if pos >= len(body):
            break
        m = re.match(r"(\d+)\n", body[pos:])
        if not m:
            break
        pos += m.end()
        n = int(m.group(1))
        piece = body[pos : pos + n]
        pos += n
        try:
            obj, _ = json.JSONDecoder().raw_decode(piece)
            out.append(obj)
        except Exception:
            continue
    return out


def _bound_value(node: Any) -> float | None:
    if not isinstance(node, list):
        return None
    # [null, 2500] or [null, null, "0.10"]
    if len(node) >= 3 and node[2] is not None:
        try:
            return float(node[2])
        except (TypeError, ValueError):
            return None
    if len(node) >= 2 and node[1] is not None:
        try:
            return float(node[1])
        except (TypeError, ValueError):
            return None
    return None


def _parse_crux_metric(block: Any) -> dict[str, Any] | None:
    """batchexecute CrUX metrik bloğu → PSI field metric dict."""
    if not isinstance(block, list) or len(block) < 2:
        return None
    data = block[1]
    if not isinstance(data, list) or not data:
        return None
    hist_wrap = data[0] if len(data) > 0 else None
    p75_wrap = data[1] if len(data) > 1 else None
    bins: list[Any] = []
    if isinstance(hist_wrap, list) and hist_wrap:
        inner = hist_wrap[0]
        if isinstance(inner, list):
            bins = inner
    distributions = []
    for b in bins[:3]:
        if not isinstance(b, list) or len(b) < 3:
            continue
        proportion = b[2]
        try:
            prop = float(proportion)
        except (TypeError, ValueError):
            continue
        distributions.append(
            {
                "min": _bound_value(b[0]),
                "max": _bound_value(b[1]) if len(b) > 1 else None,
                "proportion": prop,
            }
        )
    percentile = None
    if isinstance(p75_wrap, list) and p75_wrap:
        percentile = _bound_value(p75_wrap[0])
    if percentile is None and not distributions:
        return None
    # CLS p75 string "0.04" → PSI field often stores *100 as int 4
    out: dict[str, Any] = {}
    if percentile is not None:
        out["percentile"] = percentile
    if distributions:
        out["distributions"] = distributions
    return out


def _overall_from_metrics(metrics: dict[str, Any]) -> str:
    def p75(key: str) -> float | None:
        block = metrics.get(key)
        if not isinstance(block, dict):
            return None
        try:
            return float(block.get("percentile"))
        except (TypeError, ValueError):
            return None

    lcp = p75("largest_contentful_paint")
    inp = p75("interaction_to_next_paint")
    cls = p75("cumulative_layout_shift")
    if cls is not None and cls > 1.5:
        cls = cls / 100.0
    checks = []
    if lcp is not None:
        checks.append(lcp <= 2500)
    if inp is not None:
        checks.append(inp <= 200)
    if cls is not None:
        checks.append(cls <= 0.1)
    if not checks:
        return ""
    if all(checks):
        return "FAST"
    if any(not c for c in checks):
        # any fail → SLOW (PSI Failed); partial NI still Failed if any poor
        failed = False
        if lcp is not None and lcp > 4000:
            failed = True
        if inp is not None and inp > 500:
            failed = True
        if cls is not None and cls > 0.25:
            failed = True
        return "SLOW" if failed else "AVERAGE"
    return ""


def _is_mobile_host(url: str) -> bool:
    host = (url or "").lower()
    return "wwwm." in host or "://m." in host or host.startswith("m.") or "/m." in host


def _form_code(form_factor: str) -> int:
    return 1 if str(form_factor).lower().startswith("m") else 2


def _extract_field_experiences(inner: Any, *, form_factor: str) -> tuple[dict, dict]:
    """kind=2 CrUX satırlarından loadingExperience / originLoadingExperience."""
    want = _form_code(form_factor)
    url_metrics: dict[str, Any] = {}
    origin_metrics: dict[str, Any] = {}
    url_id = ""
    origin_id = ""

    rows = inner[0] if isinstance(inner, list) and inner and isinstance(inner[0], list) else []
    if not isinstance(rows, list):
        rows = []

    for entry in rows:
        if not isinstance(entry, list) or len(entry) < 6:
            continue
        form_c = entry[2]
        kind = entry[3]
        if form_c != want or kind != 2:
            continue
        url = str(entry[0] or "")
        payload = entry[5]
        if not isinstance(payload, list) or len(payload) < 2:
            continue
        metric_list = payload[1]
        if not isinstance(metric_list, list):
            continue
        metrics: dict[str, Any] = {}
        for item in metric_list:
            if not isinstance(item, list) or not item:
                continue
            key = item[0]
            if key not in _FIELD_KEYS:
                continue
            parsed = _parse_crux_metric(item[1] if len(item) > 1 else None)
            if parsed:
                # CLS percentile UI’da 0.08; batchexecute string/float — PSI API *100 kullanır
                if key == "cumulative_layout_shift" and "percentile" in parsed:
                    v = float(parsed["percentile"])
                    if v <= 1.5:
                        parsed = {**parsed, "percentile": round(v * 100)}
                metrics[key] = parsed
        if not metrics:
            continue
        if _is_mobile_host(url) or not origin_metrics:
            if _is_mobile_host(url):
                url_metrics = metrics
                url_id = url
            else:
                origin_metrics = metrics
                origin_id = url
        if not _is_mobile_host(url):
            origin_metrics = metrics
            origin_id = url

    # Tek kayıt varsa ikisine de koy
    if origin_metrics and not url_metrics:
        url_metrics = origin_metrics
        url_id = origin_id
    if url_metrics and not origin_metrics:
        origin_metrics = url_metrics
        origin_id = url_id

    def _exp(metrics: dict[str, Any], eid: str) -> dict[str, Any]:
        if not metrics:
            return {}
        return {
            "id": eid,
            "overall_category": _overall_from_metrics(metrics),
            "metrics": metrics,
        }

    return _exp(url_metrics, url_id), _exp(origin_metrics, origin_id)


def _walk_lighthouse(obj: Any, found: list[dict[str, Any]]) -> None:
    if isinstance(obj, str):
        s = obj.strip()
        if s.startswith("{") and '"fetchTime"' in s[:900] and ("audits" in s or "categories" in s):
            try:
                d = json.loads(s)
            except Exception:
                return
            if isinstance(d, dict) and (d.get("audits") or d.get("categories")):
                found.append(d)
        return
    if isinstance(obj, list):
        for v in obj:
            _walk_lighthouse(v, found)
        return
    if isinstance(obj, dict):
        if obj.get("audits") and obj.get("categories"):
            found.append(obj)
            return
        for v in obj.values():
            _walk_lighthouse(v, found)


def _lighthouse_form_factor(lh: dict[str, Any]) -> str:
    cfg = lh.get("configSettings") or {}
    em = str(cfg.get("emulatedFormFactor") or cfg.get("formFactor") or "").lower()
    if "desktop" in em:
        return "desktop"
    if "mobile" in em or "phone" in em:
        return "mobile"
    # Lighthouse 10+ may use screenEmulation.mobile
    se = cfg.get("screenEmulation") if isinstance(cfg.get("screenEmulation"), dict) else {}
    if se.get("mobile") is False:
        return "desktop"
    if se.get("mobile") is True:
        return "mobile"
    return ""


def _pick_lighthouse(candidates: list[dict[str, Any]], form_factor: str) -> dict[str, Any] | None:
    want = "mobile" if str(form_factor).lower().startswith("m") else "desktop"
    matched = [c for c in candidates if _lighthouse_form_factor(c) == want]
    if not matched:
        # Yanlış form_factor lab’ı panoya yazma — field CWV kalsın, lab boş kalsın
        return None

    def score(lh: dict[str, Any]) -> tuple:
        url = str(lh.get("finalUrl") or lh.get("requestedUrl") or "").lower()
        mobile_host = _is_mobile_host(url)
        if want == "mobile":
            return (0 if mobile_host else 1, -len(lh.get("audits") or {}))
        return (1 if mobile_host else 0, -len(lh.get("audits") or {}))

    return sorted(matched, key=score)[0]


def _psi_from_batchexecute_bodies(bodies: list[str], *, form_factor: str, page_url: str) -> dict[str, Any] | None:
    loading: dict[str, Any] = {}
    origin: dict[str, Any] = {}
    lighthouses: list[dict[str, Any]] = []

    for raw in bodies:
        for chunk in _parse_batchexecute(raw):
            if not isinstance(chunk, list) or not chunk:
                continue
            row = chunk[0]
            if not isinstance(row, list) or len(row) < 3:
                continue
            payload = row[2]
            inner: Any
            if isinstance(payload, str):
                try:
                    inner = json.loads(payload)
                except Exception:
                    continue
            else:
                inner = payload
            le, oe = _extract_field_experiences(inner, form_factor=form_factor)
            if le.get("metrics"):
                loading = le
            if oe.get("metrics"):
                origin = oe
            _walk_lighthouse(inner, lighthouses)

    lh = _pick_lighthouse(lighthouses, form_factor)
    if not loading.get("metrics") and not origin.get("metrics") and not lh:
        return None

    return {
        "id": page_url,
        "loadingExperience": loading or origin,
        "originLoadingExperience": origin or loading,
        "lighthouseResult": lh or {},
        "_extracted_from": "batchexecute",
    }


def _dom_field_fallback(page) -> dict[str, Any] | None:
    """DOM yedek — ağ yakalanamazsa (sentetik değil)."""
    try:
        data = page.evaluate(
            """() => {
              const text = document.body ? document.body.innerText : '';
              const out = { source: 'dom', metrics: {}, scores: {} };
              const patterns = [
                ['largest_contentful_paint', /Largest Contentful Paint \\(LCP\\)\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i],
                ['interaction_to_next_paint', /Interaction to Next Paint \\(INP\\)\\s*\\n\\s*([0-9]+)\\s*ms/i],
                ['cumulative_layout_shift', /Cumulative Layout Shift \\(CLS\\)\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)/i],
                ['first_contentful_paint', /First Contentful Paint \\(FCP\\)\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i],
                ['experimental_time_to_first_byte', /Time to First Byte \\(TTFB\\)\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i],
              ];
              for (const [key, re] of patterns) {
                const m = text.match(re);
                if (!m) continue;
                let v = parseFloat(String(m[1]).replace(',', '.'));
                if (!Number.isFinite(v)) continue;
                let percentile = v;
                if (key === 'interaction_to_next_paint') percentile = Math.round(v);
                else if (key.includes('layout')) percentile = Math.round(v * 100);
                else percentile = Math.round(v * 1000);
                out.metrics[key] = { percentile };
              }
              const passed = /Core Web Vitals Assessment:\\s*Passed/i.test(text);
              const failed = /Core Web Vitals Assessment:\\s*Failed/i.test(text);
              if (passed) out.overall_category = 'FAST';
              if (failed) out.overall_category = 'SLOW';
              // Lab skorları: "59\\nPerformance\\n80\\nAccessibility..."
              const sm = text.match(/(\\d{1,3})\\s*\\n\\s*Performance\\s*\\n\\s*(\\d{1,3})\\s*\\n\\s*Accessibility\\s*\\n\\s*(\\d{1,3})\\s*\\n\\s*Best Practices\\s*\\n\\s*(\\d{1,3})\\s*\\n\\s*SEO/i);
              if (sm) {
                out.scores = {
                  performance: parseInt(sm[1], 10) / 100,
                  accessibility: parseInt(sm[2], 10) / 100,
                  'best-practices': parseInt(sm[3], 10) / 100,
                  seo: parseInt(sm[4], 10) / 100,
                };
              }
              const lab = {};
              const labPatterns = [
                ['first-contentful-paint', /METRICS[\\s\\S]*?First Contentful Paint\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i, 's'],
                ['largest-contentful-paint', /METRICS[\\s\\S]*?Largest Contentful Paint\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i, 's'],
                ['total-blocking-time', /Total Blocking Time\\s*\\n\\s*([0-9]+(?:,[0-9]+)?)\\s*ms/i, 'ms'],
                ['cumulative-layout-shift', /METRICS[\\s\\S]*?Cumulative Layout Shift\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)/i, 'cls'],
                ['speed-index', /Speed Index\\s*\\n\\s*([0-9]+(?:[.,][0-9]+)?)\\s*s/i, 's'],
              ];
              for (const [id, re, kind] of labPatterns) {
                const m = text.match(re);
                if (!m) continue;
                let v = parseFloat(String(m[1]).replace(/,/g, ''));
                if (!Number.isFinite(v)) continue;
                let numeric = v;
                let display = m[0].split('\\n').pop();
                if (kind === 's') { numeric = v * 1000; display = v + ' s'; }
                else if (kind === 'ms') { display = Math.round(v) + ' ms'; }
                else { display = String(v); }
                lab[id] = { id, numericValue: numeric, displayValue: display, score: null };
              }
              out.lab_audits = lab;
              return out;
            }"""
        )
    except Exception:
        return None
    if not isinstance(data, dict) or not data.get("metrics"):
        return None
    metrics = data["metrics"]
    categories = {}
    for key, score in (data.get("scores") or {}).items():
        categories[key] = {"id": key, "score": score}
    audits = data.get("lab_audits") or {}
    return {
        "id": page.url,
        "loadingExperience": {
            "overall_category": data.get("overall_category") or _overall_from_metrics(metrics),
            "metrics": metrics,
        },
        "originLoadingExperience": {
            "overall_category": data.get("overall_category") or _overall_from_metrics(metrics),
            "metrics": metrics,
        },
        "lighthouseResult": {
            "categories": categories,
            "audits": audits,
            "fetchTime": datetime.now(timezone.utc).isoformat(),
            "requestedUrl": page.url,
            "finalUrl": page.url,
            "configSettings": {"emulatedFormFactor": "mobile"},
        }
        if categories or audits
        else {},
        "_extracted_from": "dom",
    }


def scrape_one(*, site_url: str, form_factor: str, headed: bool = False, timeout_ms: int = 180_000) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    analysis_url = _report_url(site_url, form_factor)
    batch_bodies: list[str] = []
    api_bodies: list[dict[str, Any]] = []
    final_page_url = analysis_url

    with sync_playwright() as p:
        from backend.services.scrape_browser import launch_ephemeral

        browser, context = launch_ephemeral(
            p, headed=headed, viewport={"width": 1440, "height": 1100}
        )
        page = context.new_page()

        def _on_response(resp) -> None:
            try:
                url = (resp.url or "").lower()
                if "batchexecute" in url:
                    txt = resp.text()
                    if txt and len(txt) > 200:
                        batch_bodies.append(txt)
                    return
                interesting = (
                    "pagespeedonline" in url
                    or "runpagespeed" in url
                    or ("googleapis.com" in url and ("pagespeed" in url or "chromeux" in url))
                )
                if not interesting:
                    return
                body = resp.json()
                if isinstance(body, dict):
                    api_bodies.append(body)
            except Exception:
                return

        page.on("response", _on_response)
        print(f"  · open {analysis_url}", flush=True)
        page.goto(analysis_url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_url("**/analysis/**", timeout=min(90_000, timeout_ms))
        except Exception:
            pass

        deadline = time.time() + (timeout_ms / 1000.0)
        psi: dict[str, Any] | None = None
        while time.time() < deadline:
            psi = _psi_from_batchexecute_bodies(batch_bodies, form_factor=form_factor, page_url=page.url)
            has_field = bool((psi or {}).get("loadingExperience", {}).get("metrics"))
            has_lab = bool((psi or {}).get("lighthouseResult", {}).get("categories"))
            lab_ff_ok = False
            if has_lab:
                lab_ff_ok = _lighthouse_form_factor((psi or {}).get("lighthouseResult") or {}) == (
                    "mobile" if form_factor.startswith("m") else "desktop"
                )
            try:
                cwv_ready = page.locator("text=Core Web Vitals Assessment").count() > 0
                lab_ready = page.locator("text=Diagnose performance issues").count() > 0
            except Exception:
                cwv_ready = False
                lab_ready = False
            if has_field and lab_ff_ok:
                break
            if has_field and lab_ready and not lab_ff_ok:
                # Lab geldi ama yanlış form — ikinci/büyük batchexecute’u bekle
                page.wait_for_timeout(5000)
                continue
            if has_field and cwv_ready and not lab_ready:
                # Field hazır, lab henüz yok
                page.wait_for_timeout(2500)
                continue
            if has_field and time.time() > deadline - 15:
                # Timeout’a yakın: yanlış lab varsa temizle, field ile bitir
                if psi and isinstance(psi.get("lighthouseResult"), dict):
                    got = _lighthouse_form_factor(psi["lighthouseResult"])
                    want_ff = "mobile" if form_factor.startswith("m") else "desktop"
                    if got and got != want_ff:
                        psi["lighthouseResult"] = {}
                break
            # klasik API yakalama
            for body in reversed(api_bodies):
                if body.get("lighthouseResult") or body.get("loadingExperience"):
                    cand = body
                    got = _lighthouse_form_factor(cand.get("lighthouseResult") or {})
                    want_ff = "mobile" if form_factor.startswith("m") else "desktop"
                    if cand.get("loadingExperience") and (not got or got == want_ff):
                        psi = cand
                        if got == want_ff:
                            break
            if psi and _lighthouse_form_factor((psi.get("lighthouseResult") or {})) == (
                "mobile" if form_factor.startswith("m") else "desktop"
            ):
                break
            page.wait_for_timeout(2000)

        if not psi or not (
            (psi.get("loadingExperience") or {}).get("metrics")
            or (psi.get("originLoadingExperience") or {}).get("metrics")
            or (psi.get("lighthouseResult") or {}).get("categories")
        ):
            page.wait_for_timeout(2000)
            psi = _dom_field_fallback(page)

        final_page_url = page.url
        browser.close()

    if not psi:
        return {
            "ok": False,
            "message": "pagespeed.web.dev PSI yanıtı yakalanamadı",
            "analysis_url": analysis_url,
            "captured": len(batch_bodies),
        }
    pid = str(psi.get("id") or "")
    resolved_analysis = pid if "/analysis/" in pid else (final_page_url or analysis_url)
    return {
        "ok": True,
        "analysis_url": resolved_analysis,
        "form_factor": form_factor,
        "site_url": site_url,
        "psi": psi,
        "captured_network": len(batch_bodies) + len(api_bodies),
        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def post_ingest(domain: str, form_factor: str, result: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli (.env)"}
    body = {
        "domain": domain,
        "form_factor": form_factor,
        "analysis_url": result.get("analysis_url") or "",
        "psi_payload": result.get("psi") or {},
        "scope_note": "pagespeed.web.dev tarama",
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Notification-Ingest-Token": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:240]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scrape pagespeed.web.dev into Project Control")
    parser.add_argument("--domain", default="", help="www.doviz.com | www.sinemalar.com")
    parser.add_argument("--url", default="", help="Analiz URL’si (yoksa domain’den)")
    parser.add_argument("--form-factor", default="all", choices=["mobile", "desktop", "all"])
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--sync", action="store_true", help="Tüm varsayılan hedefler")
    parser.add_argument("--timeout-ms", type=int, default=180000)
    args = parser.parse_args(argv)

    targets: list[dict[str, str]] = []
    if args.sync or not args.domain:
        targets = list(DEFAULT_TARGETS)
    else:
        url = args.url or f"https://{args.domain.removeprefix('https://').removeprefix('http://')}"
        targets = [{"domain": args.domain, "url": url}]

    factors = ["mobile", "desktop"] if args.form_factor == "all" else [args.form_factor]
    failures = 0
    for t in targets:
        for ff in factors:
            print(f"== {t['domain']} · {ff}", flush=True)
            result = scrape_one(
                site_url=t["url"],
                form_factor=ff,
                headed=bool(args.headed),
                timeout_ms=int(args.timeout_ms),
            )
            if not result.get("ok"):
                print(f"  ✗ {result.get('message')}", flush=True)
                failures += 1
                continue
            psi = result.get("psi") or {}
            le = (psi.get("loadingExperience") or {}).get("metrics") or {}
            lh = (psi.get("lighthouseResult") or {}).get("categories") or {}
            print(
                f"  ✓ PSI · via={psi.get('_extracted_from')} · field={len(le)} · "
                f"lab_cats={len(lh)} · id={(psi.get('id') or '')[:80]}",
                flush=True,
            )
            out = ROOT / "scratch" / f"pagespeed_web_{t['domain'].replace('.', '_')}_{ff}.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"  · wrote {out}", flush=True)
            if args.ingest or args.sync:
                ing = post_ingest(t["domain"], ff, result)
                print(f"  · ingest: {ing}", flush=True)
                if not ing.get("ok"):
                    failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
