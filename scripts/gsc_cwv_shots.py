#!/usr/bin/env python3
"""GSC Core Web Vitals — screenshot capture (Mac bridge).

Döviz + Sinemalar için overview + mobile/desktop summary PNG alır,
KPI özetini JSON ingest'e yazar, görselleri shots-ingest'e yükler.

  .venv/bin/python scripts/gsc_cwv_shots.py --sync --ingest --site doviz
  .venv/bin/python scripts/gsc_cwv_shots.py --sync --ingest

Env: NOTIFICATION_INGEST_TOKEN, GSC_CWV_SHOTS_INGEST_URL, GSC_CWV_INGEST_URL
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

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
        key, val = key.strip(), val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

SHOTS_DIR = Path.home() / ".seo-agent" / "cache" / "cwv-shots"
DEVICE_MOBILE = 2
DEVICE_DESKTOP = 1


def _shots_ingest_url() -> str:
    return (
        os.environ.get("GSC_CWV_SHOTS_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/gsc-cwv/shots-ingest"
    ).strip()


def _kpi_ingest_url() -> str:
    return (
        os.environ.get("GSC_CWV_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
    ).strip()


def _token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _png_of(page, path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), full_page=False, type="png")
    _trim_png_edges(path)
    return path.read_bytes()


def _trim_png_edges(path: Path, *, pad: int = 12, min_keep_w: float = 0.7, min_keep_h: float = 0.65) -> None:
    """Düz gri/beyaz kenar boşluğunu hafif kırp — aşırı trim kesik SS üretmesin."""
    try:
        from PIL import Image, ImageChops
    except Exception:
        return
    try:
        im = Image.open(path).convert("RGB")
    except Exception:
        return
    w, h = im.size
    if w < 80 or h < 80:
        return
    bg = Image.new("RGB", im.size, im.getpixel((2, 2)))
    diff = ImageChops.difference(im, bg)
    bbox = diff.getbbox()
    if not bbox:
        return
    l, t, r, b = bbox
    l = max(0, l - pad)
    t = max(0, t - pad)
    r = min(w, r + pad)
    b = min(h, b + pad)
    # Aşırı kırpma yok — tablo/kart kenarları kesilmesin
    if (r - l) < w * min_keep_w or (b - t) < h * min_keep_h:
        return
    # Neredeyse tüm alan zaten doluysa dokunma (gereksiz yeniden encode)
    if (r - l) >= w * 0.97 and (b - t) >= h * 0.97:
        return
    try:
        im.crop((l, t, r, b)).save(path, format="PNG", optimize=True)
    except Exception:
        pass


def _screenshot_locator(loc, path: Path, *, trim: bool = True) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        loc.screenshot(path=str(path), type="png")
    except Exception:
        return False
    if not path.is_file() or path.stat().st_size < 4_000:
        return False
    if trim:
        _trim_png_edges(path)
    return path.stat().st_size > 3_000


def _screenshot_main(page, path: Path) -> bytes:
    """Ana içerik — mümkünse grafik/kart bölgesi; GSC chrome'suz."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Önce stacked / line chart (svg|canvas) kartını dene
    if _shot_first_visible_chart(page, path):
        return path.read_bytes()
    selectors = (
        "[role='main']",
        "main",
        "[data-is-app-shell-content]",
        "#fcxPrimaryContent",
        ".i3WFpf",
    )
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() and loc.is_visible(timeout=1500):
                if _screenshot_locator(loc, path):
                    return path.read_bytes()
        except Exception:
            continue
    return _png_of(page, path)


def _clear_shot_marks(page) -> None:
    try:
        page.evaluate(
            """() => {
  document.querySelectorAll('[data-cwv-shot-target]').forEach((e) => {
    e.removeAttribute('data-cwv-shot-target');
  });
}"""
        )
    except Exception:
        pass


def _shot_marked_target(page, path: Path, *, trim: bool = True) -> bool:
    """data-cwv-shot-target=1 işaretli elementi screenshot et (Selenium/Playwright)."""
    try:
        loc = page.locator('[data-cwv-shot-target="1"]').first
        if not loc.count():
            return False
        try:
            loc.scroll_into_view_if_needed(timeout=2500)
        except Exception:
            pass
        time.sleep(0.25)
        ok = _screenshot_locator(loc, path, trim=trim)
        return ok
    except Exception:
        return False
    finally:
        _clear_shot_marks(page)


def _boost_trend_sparklines(page) -> None:
    """Trend mini-spark çizgilerini SS için kalınlaştır / büyüt."""
    try:
        page.evaluate(
            """() => {
  if (document.getElementById('cwv-spark-boost')) return;
  const style = document.createElement('style');
  style.id = 'cwv-spark-boost';
  style.textContent = `
    [data-cwv-shot-target="1"] svg,
    [data-cwv-issues-table] svg {
      overflow: visible !important;
    }
    [data-cwv-shot-target="1"] svg path,
    [data-cwv-shot-target="1"] svg polyline,
    [data-cwv-shot-target="1"] svg line,
    [data-cwv-issues-table] svg path,
    [data-cwv-issues-table] svg polyline,
    [data-cwv-issues-table] svg line {
      stroke-width: 2.75px !important;
      stroke-linecap: round !important;
      stroke-linejoin: round !important;
    }
    [data-cwv-shot-target="1"] td svg,
    [data-cwv-shot-target="1"] [role="cell"] svg,
    [data-cwv-issues-table] td svg,
    [data-cwv-issues-table] [role="cell"] svg {
      transform: scaleY(1.35) scaleX(1.08);
      transform-origin: center center;
    }
  `;
  document.documentElement.appendChild(style);
}"""
        )
    except Exception:
        pass


def _shot_issues_table(page, path: Path) -> bool:
    """Summary: yalnız «Why URLs aren't considered good» tablo kartı (üst grafik yok)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _clear_shot_marks(page)
    try:
        marked = page.evaluate(
            """() => {
  document.querySelectorAll('[data-cwv-shot-target],[data-cwv-issues-table]').forEach((e) => {
    e.removeAttribute('data-cwv-shot-target');
    e.removeAttribute('data-cwv-issues-table');
  });
  const norm = (s) => String(s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const needles = [
    "why urls aren't considered good",
    "why urls arent considered good",
    "urls with these issues don't provide",
    "url'ler neden iyi",
    "urller neden iyi",
    "iyi olarak değerlendirilmiyor",
    "iyi sayilmiyor",
    "iyi sayılmıyor",
  ];
  const nodes = Array.from(
    document.querySelectorAll('h1,h2,h3,h4,h5,div,span,section,article')
  );
  let heading = null;
  for (const el of nodes) {
    const t = norm(el.innerText || el.textContent || '');
    if (!t || t.length > 160) continue;
    if (needles.some((n) => t.includes(n))) {
      heading = el;
      break;
    }
  }
  if (!heading) {
    // Tablo başlık satırı: Severity + Issue + Trend + URLs
    const rows = Array.from(document.querySelectorAll('tr, [role="row"]'));
    for (const row of rows) {
      const t = norm(row.innerText || '');
      if (
        t.includes('severity') &&
        t.includes('issue') &&
        (t.includes('trend') || t.includes('validation')) &&
        t.includes('url')
      ) {
        heading = row;
        break;
      }
    }
  }
  if (!heading) return false;

  let pick = null;
  let node = heading;
  for (let i = 0; i < 12 && node; i++) {
    const r = node.getBoundingClientRect();
    const txt = norm(node.innerText || '');
    const hasTable =
      !!(node.querySelector && (
        node.querySelector('table') ||
        node.querySelector('[role="table"]') ||
        node.querySelector('[role="grid"]')
      ));
    const looksLikeCard =
      r.width >= 480 &&
      r.height >= 120 &&
      r.height <= 900 &&
      r.width <= Math.min(window.innerWidth - 8, 1600);
    const hasIssueRows =
      /lcp|cls|inp|fid|need improvement|poor|yetersiz|iyileş/i.test(txt);
    if (looksLikeCard && (hasTable || hasIssueRows) && txt.length < 8000) {
      // Üst stacked grafiği dahil eden çok yüksek kartları ele
      const chart = node.querySelector && node.querySelector('svg, canvas');
      let chartH = 0;
      if (chart) {
        const cr = chart.getBoundingClientRect();
        if (cr.height >= 140 && cr.width >= 280) chartH = cr.height;
      }
      // Büyük üst grafik varsa daha sıkı (tablo) çocuğa in
      if (chartH >= 140 && r.height > chartH + 160) {
        // continue walking — ama heading'e yakın bir kart tercih et
      } else {
        pick = node;
        break;
      }
    }
    node = node.parentElement;
  }

  if (!pick) {
    // Heading + tabloyu saran en dar kutu
    let box = heading.parentElement;
    for (let d = 0; d < 8 && box; d++) {
      const r = box.getBoundingClientRect();
      if (r.width >= 480 && r.height >= 140 && r.height <= 720) {
        pick = box;
        break;
      }
      box = box.parentElement;
    }
  }
  if (!pick) return false;

  // Üst grafik hâlâ içerideyse: tabloyu / satırları seç
  const bigChart = Array.from(pick.querySelectorAll('svg, canvas')).find((el) => {
    const r = el.getBoundingClientRect();
    return r.width >= 300 && r.height >= 150;
  });
  if (bigChart) {
    const table =
      pick.querySelector('table') ||
      pick.querySelector('[role="table"]') ||
      pick.querySelector('[role="grid"]');
    if (table) {
      let wrap = table;
      for (let i = 0; i < 5 && wrap.parentElement; i++) {
        const p = wrap.parentElement;
        const pr = p.getBoundingClientRect();
        const tr = table.getBoundingClientRect();
        if (pr.height <= tr.height + 120 && pr.width >= tr.width * 0.9) {
          wrap = p;
        } else break;
      }
      // Başlık metni wrap dışında kaldıysa heading'i de kapsayan kardeş üstü
      const headR = heading.getBoundingClientRect();
      const wrapR = wrap.getBoundingClientRect();
      if (headR.top < wrapR.top - 8) {
        let up = wrap.parentElement;
        for (let i = 0; i < 4 && up; i++) {
          const ur = up.getBoundingClientRect();
          if (ur.top <= headR.top + 4 && ur.height <= 820) {
            wrap = up;
            break;
          }
          up = up.parentElement;
        }
      }
      pick = wrap;
    }
  }

  pick.setAttribute('data-cwv-shot-target', '1');
  pick.setAttribute('data-cwv-issues-table', '1');
  try {
    pick.scrollIntoView({ block: 'center', inline: 'nearest' });
  } catch (_) {}
  return true;
}"""
        )
        if not marked:
            return False
        _boost_trend_sparklines(page)
        time.sleep(0.35)
        # Tablo için agresif kenar trim kapalı — kesik satır/spark olmasın
        return _shot_marked_target(page, path, trim=False)
    except Exception:
        _clear_shot_marks(page)
        return False


def _shot_first_visible_chart(page, path: Path) -> bool:
    """Sayfadaki ilk anlamlı SVG/canvas grafiğinin kartını yakala."""
    _clear_shot_marks(page)
    try:
        marked = page.evaluate(
            """() => {
  document.querySelectorAll('[data-cwv-shot-target]').forEach((e) => {
    e.removeAttribute('data-cwv-shot-target');
  });
  const charts = Array.from(document.querySelectorAll('svg, canvas')).filter((el) => {
    const r = el.getBoundingClientRect();
    return r.width >= 280 && r.height >= 120 && r.bottom > 0 && r.top < window.innerHeight;
  });
  if (!charts.length) return false;
  charts.sort((a, b) => {
    const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
    return (rb.width * rb.height) - (ra.width * ra.height);
  });
  let node = charts[0];
  let pick = charts[0];
  for (let i = 0; i < 8 && node; i++) {
    const r = node.getBoundingClientRect();
    if (r.width >= 320 && r.height >= 160 && r.height <= window.innerHeight * 0.85) {
      const txt = (node.innerText || '').length;
      if (txt < 4000) { pick = node; break; }
    }
    node = node.parentElement;
  }
  if (!pick) return false;
  pick.setAttribute('data-cwv-shot-target', '1');
  return true;
}"""
        )
        if not marked:
            return False
        return _shot_marked_target(page, path)
    except Exception:
        _clear_shot_marks(page)
        return False


def _load_cwv_scrape():
    import importlib.util

    path = ROOT / "scripts" / "gsc_cwv_scrape.py"
    spec = importlib.util.spec_from_file_location("gsc_cwv_scrape_mod", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _last_updated_from_body(body: str) -> str:
    import re

    text = body or ""
    m = re.search(
        r"(?:Last updated|Son güncelleme|Updated)\s*[:\-]?\s*([^\n\r·|]{4,40})",
        text,
        flags=re.I,
    )
    if m:
        return (m.group(1) or "").strip()[:80]
    return ""


def _enable_all_status_series(page) -> int:
    """Summary sayfasında Poor / NI / Good kutularının hepsini aç.

    GSC varsayılanı çoğu zaman yalnız «Poor» seçili bırakır; o zaman ana
    stacked grafik boş/yanlış görünür. Kutulara tıklayıp üç seriyi de aç.
    """
    try:
        n = int(
            page.evaluate(
                """() => {
  const series = [
    { re: /(^|\\n|\\s)(poor|yetersiz)\\b/i, key: 'poor' },
    { re: /need(s)?\\s*improvement|iyileştir|iyilestir/i, key: 'ni' },
    { re: /(^|\\n|\\s)(good|iyi)\\b/i, key: 'good' },
  ];
  const skipGoodish = (t, key) => {
    if (key !== 'good') return false;
    return /iyileştir|iyilestir|improvement|need/i.test(t);
  };
  const candidates = Array.from(
    document.querySelectorAll('[role="checkbox"], [aria-checked], button, [tabindex]')
  );
  // Kart gövdeleri: checkbox içeren üst kutular
  const cards = [];
  for (const el of candidates) {
    let node = el;
    for (let i = 0; i < 6 && node; i++) {
      const t = (node.innerText || '').replace(/\\s+/g, ' ').trim();
      if (t && t.length < 160 && /\\d/.test(t)) {
        cards.push(node);
        break;
      }
      node = node.parentElement;
    }
  }
  const uniq = Array.from(new Set(cards));
  let clicked = 0;
  const seen = new Set();
  for (const { re, key } of series) {
    if (seen.has(key)) continue;
    for (const card of uniq) {
      const t = (card.innerText || '').replace(/\\s+/g, ' ').trim();
      if (!re.test(t) || skipGoodish(t, key)) continue;
      const box =
        card.matches('[role="checkbox"], [aria-checked]')
          ? card
          : card.querySelector('[role="checkbox"], [aria-checked], input[type="checkbox"]');
      const aria = (box && box.getAttribute('aria-checked')) || '';
      const input = box && box.matches && box.matches('input')
        ? box
        : card.querySelector('input[type="checkbox"]');
      const on = aria === 'true' || !!(input && input.checked);
      if (on) {
        seen.add(key);
        break;
      }
      try {
        (box || card).click();
        clicked += 1;
        seen.add(key);
        break;
      } catch (_) {}
    }
  }
  return clicked;
}"""
            )
            or 0
        )
    except Exception:
        n = 0
    if n:
        time.sleep(1.2)
    # Playwright fallback — role/name ile
    import re as _re

    patterns = (
        _re.compile(r"poor|yetersiz", _re.I),
        _re.compile(r"need|improvement|iyileştir|iyilestir", _re.I),
        _re.compile(r"^good$|^iyi$|good urls|iyi url", _re.I),
    )
    for pat in patterns:
        try:
            loc = page.get_by_role("checkbox", name=pat)
            if loc.count() == 0:
                loc = page.locator("[role='checkbox']").filter(has_text=pat)
            if loc.count() == 0:
                continue
            target = loc.first
            aria = ""
            try:
                aria = (target.get_attribute("aria-checked") or "").lower()
            except Exception:
                aria = ""
            if aria == "true":
                continue
            target.click(timeout=2500)
            n += 1
            time.sleep(0.6)
        except Exception:
            continue
    if n:
        time.sleep(1.5)
    print(f"  · status series toggles · clicked≈{n}", flush=True)
    return n


def _shot_overview_device_card(page, label: str, path: Path) -> bool:
    """Overview'daki Mobile / Desktop çizgi grafik kartını sıkı kırp."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _clear_shot_marks(page)
    try:
        marked = page.evaluate(
            """(label) => {
  document.querySelectorAll('[data-cwv-shot-target]').forEach((e) => {
    e.removeAttribute('data-cwv-shot-target');
  });
  const norm = (s) => String(s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const want = norm(label);
  const nodes = Array.from(document.querySelectorAll('h1,h2,h3,h4,div,span,button'));
  let heading = null;
  for (const el of nodes) {
    const t = norm(el.innerText || el.textContent || '');
    if (!t || t.length > 40) continue;
    if (t === want || t.startsWith(want + ' ') || t === want + ' web') {
      heading = el;
      break;
    }
  }
  if (!heading) return false;
  let pick = null;
  let root = heading;
  for (let i = 0; i < 10 && root; i++) {
    const chart = root.querySelector && root.querySelector('svg, canvas');
    if (chart) {
      const cr = chart.getBoundingClientRect();
      if (cr.width >= 240 && cr.height >= 100) {
        let card = chart.parentElement;
        for (let d = 0; d < 6 && card; d++) {
          const r = card.getBoundingClientRect();
          if (r.width >= 300 && r.height >= 140 && r.height <= 720 && r.width <= 1400) {
            pick = card;
            break;
          }
          card = card.parentElement;
        }
        if (!pick) pick = chart;
        break;
      }
    }
    root = root.parentElement;
  }
  if (!pick) {
    let box = heading.parentElement;
    for (let d = 0; d < 5 && box; d++) {
      const r = box.getBoundingClientRect();
      if (r.width >= 300 && r.height >= 160 && r.height <= 640) { pick = box; break; }
      box = box.parentElement;
    }
  }
  if (!pick) return false;
  pick.setAttribute('data-cwv-shot-target', '1');
  return true;
}""",
            label,
        )
        if marked and _shot_marked_target(page, path):
            return True
    except Exception:
        _clear_shot_marks(page)
    # Eski xpath yolu — son çare, yükseklik sınırıyla
    for sel in (f"text={label}", f"text=/{label}/i"):
        try:
            heading = page.locator(sel).first
            if not heading.count():
                continue
            for depth in (3, 4, 5, 2):
                try:
                    box = heading.locator(
                        f"xpath=ancestor::*[self::div or self::section][{depth}]"
                    )
                    box.wait_for(state="visible", timeout=1500)
                    try:
                        box_h = box.bounding_box()
                        if box_h and box_h.get("height", 0) > 780:
                            continue
                    except Exception:
                        pass
                    if _screenshot_locator(box, path):
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def capture_property_shots(page, prop: dict[str, str], cwv_mod: Any) -> dict[str, Any]:
    _cwv_url = cwv_mod._cwv_url
    _ensure_signed_in = cwv_mod._ensure_signed_in
    _parse_overview_counts = cwv_mod._parse_overview_counts
    _extract_page_meta = cwv_mod._extract_page_meta

    rid = prop["resource_id"]
    site_key = prop.get("site_key") or "site"
    out_dir = SHOTS_DIR / site_key
    out_dir.mkdir(parents=True, exist_ok=True)
    shots: dict[str, Path] = {}

    print(f"CWV shots · {prop.get('label') or rid}", flush=True)
    try:
        page.set_viewport_size({"width": 1440, "height": 1100})
    except Exception:
        pass
    page.goto(_cwv_url(rid), wait_until="domcontentloaded", timeout=120_000)
    _ensure_signed_in(page, headed=True)
    time.sleep(3.5)
    try:
        page.evaluate("window.scrollTo(0, 80)")
    except Exception:
        pass
    time.sleep(1.0)

    meta = _extract_page_meta(page)
    body = str((meta or {}).get("body_head") or "")
    overview = _parse_overview_counts(body)
    last_upd = _last_updated_from_body(body)

    shots["full"] = out_dir / "full.png"
    # full: viewport (chrome'suz daha tutarlı) — main tüm sayfa yamukluğu üretmesin
    _png_of(page, shots["full"])
    print(f"  · full overview → {shots['full'].name}", flush=True)

    # Overview çizgi grafikleri (Mobil / Masaüstü kartları) — home panel ana görünüm
    for label_en, label_tr, variant in (
        ("Mobile", "Mobil", "mobile"),
        ("Desktop", "Masaüstü", "desktop"),
    ):
        card_path = out_dir / f"{variant}-overview.png"
        ok = _shot_overview_device_card(page, label_en, card_path) or _shot_overview_device_card(
            page, label_tr, card_path
        )
        if ok:
            shots[variant] = card_path
            print(f"  · {variant} overview card (line chart)", flush=True)
        else:
            print(f"  · {variant} overview card skip", flush=True)

    # Device summary — yalnız «Why URLs aren't considered good» tablo kartı
    # (üst stacked grafik yok; Trend spark kalınlaştırılır)
    for device, variant, label in (
        (DEVICE_MOBILE, "mobile", "Mobile web summary"),
        (DEVICE_DESKTOP, "desktop", "Desktop web summary"),
    ):
        path = out_dir / f"{variant}-summary.png"
        page.goto(
            _cwv_url(rid, "/summary", device=device),
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(3.5)
        # Tablo görünür olsun diye alta kaydır; status toggle şart değil
        try:
            page.evaluate(
                """() => {
  const t = Array.from(document.querySelectorAll('h1,h2,h3,h4,div,span'))
    .find((el) => /why urls aren|url.?ler neden iyi|considered good/i.test(el.innerText || ''));
  if (t) t.scrollIntoView({ block: 'center' });
  else window.scrollTo(0, Math.min(520, document.body.scrollHeight * 0.35));
}"""
            )
        except Exception:
            pass
        time.sleep(1.0)
        ok = _shot_issues_table(page, path)
        if not ok:
            # Son çare: eski grafik kırpığı (panel boş kalmasın)
            _enable_all_status_series(page)
            time.sleep(0.8)
            if not _shot_first_visible_chart(page, path):
                _screenshot_main(page, path)
            print(f"  · {label} → {path.name} (fallback chart)", flush=True)
        else:
            print(f"  · {label} → {path.name} (issues table)", flush=True)
        shots[f"{variant}_summary"] = path
        # Home Mobile/Desktop slotu: issues tablosu (üst overview çizgisi değil)
        if path.is_file() and path.stat().st_size > 4_000:
            home_path = out_dir / f"{variant}-overview.png"
            try:
                home_path.write_bytes(path.read_bytes())
            except Exception:
                home_path = path
            shots[variant] = home_path

    mobile_k = dict(overview.get("mobile") or {})
    desktop_k = dict(overview.get("desktop") or {})
    poor = int(mobile_k.get("poor") or 0) + int(desktop_k.get("poor") or 0)
    ni = int(mobile_k.get("needs_improvement") or 0) + int(
        desktop_k.get("needs_improvement") or 0
    )
    good = int(mobile_k.get("good") or 0) + int(desktop_k.get("good") or 0)

    return {
        "site_key": site_key,
        "site_domain": prop.get("site_domain") or "",
        "resource_id": rid,
        "label": prop.get("label") or rid,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": last_upd,
        "overview": overview,
        "chart_series": {},
        "mobile": {
            "kpis": mobile_k,
            "last_updated": last_upd,
            "issues": [],
            "issue_drilldowns": [],
            "good_urls": [],
        },
        "desktop": {
            "kpis": desktop_k,
            "last_updated": last_upd,
            "issues": [],
            "issue_drilldowns": [],
            "good_urls": [],
        },
        "amp": {"issues": [], "url_row_count": 0, "skipped": True},
        "totals": {"poor": poor, "needs_improvement": ni, "good": good},
        "source": "gsc_cwv_shots",
        "charts_only": True,
        "paths": {k: str(v) for k, v in shots.items()},
    }


def post_kpi_snapshot(capture: dict[str, Any]) -> dict[str, Any]:
    """KPI kartları için hafif JSON snapshot (AMP/issue drilldown yok)."""
    token = _token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    snap = {
        "site_key": capture.get("site_key") or "",
        "site_domain": capture.get("site_domain") or "",
        "resource_id": capture.get("resource_id") or "",
        "label": capture.get("label") or "",
        "scraped_at": capture.get("scraped_at") or "",
        "last_updated": capture.get("last_updated") or "",
        "overview": capture.get("overview") or {},
        "chart_series": capture.get("chart_series") or {},
        "mobile": capture.get("mobile") or {},
        "desktop": capture.get("desktop") or {},
        "amp": capture.get("amp") or {"issues": [], "url_row_count": 0, "skipped": True},
        "totals": capture.get("totals") or {},
        "source": "gsc_cwv_shots",
        "charts_only": True,
    }
    body_payload = {
        "source": "gsc_cwv_shots",
        "scraped_at": capture.get("scraped_at") or "",
        "snapshots": [snap],
    }
    try:
        resp = requests.post(
            _kpi_ingest_url(),
            headers={
                "X-Notification-Ingest-Token": token,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data=json.dumps(body_payload, ensure_ascii=False).encode("utf-8"),
            timeout=120,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"ok": False, "message": resp.text[:300]}
        body["http_status"] = resp.status_code
        body["ok"] = bool(body.get("ok")) and resp.status_code < 400
        return body
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:300]}


def post_shots(capture: dict[str, Any]) -> dict[str, Any]:
    token = _token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    paths = capture.get("paths") or {}
    open_handles = []
    try:
        mapping = (
            ("full", "full"),
            ("mobile", "mobile"),
            ("desktop", "desktop"),
            ("mobile_summary", "mobile_summary"),
            ("desktop_summary", "desktop_summary"),
            # geriye dönük: eski panel ekstra slotu
            ("mobile_summary", "extra"),
        )
        files = []
        data = [
            ("site_key", capture.get("site_key") or ""),
            ("site_domain", capture.get("site_domain") or ""),
            ("source", "gsc_cwv_shots"),
            ("scraped_at", capture.get("scraped_at") or ""),
        ]
        for key, variant in mapping:
            p = Path(str(paths.get(key) or ""))
            if not p.is_file():
                continue
            fh = p.open("rb")
            open_handles.append(fh)
            files.append(("files", (f"{variant}.png", fh, "image/png")))
            data.append(("variants", variant))
        if not files:
            return {"ok": False, "message": "Yüklenecek PNG yok"}
        resp = requests.post(
            _shots_ingest_url(),
            headers={"X-Notification-Ingest-Token": token},
            data=data,
            files=files,
            timeout=180,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"ok": False, "message": resp.text[:300]}
        body["http_status"] = resp.status_code
        body["ok"] = bool(body.get("ok")) and resp.status_code < 400
        return body
    finally:
        for fh in open_handles:
            try:
                fh.close()
            except Exception:
                pass


def run_shots(
    *,
    site_filter: str = "",
    ingest: bool = True,
    headed: bool | None = None,
) -> dict[str, Any]:
    cwv_mod = _load_cwv_scrape()
    PROPERTIES = cwv_mod.PROPERTIES
    GscNeedsSystemLogin = cwv_mod.GscNeedsSystemLogin
    system_firefox_login = cwv_mod._system_firefox_gsc_login
    ensure_session = cwv_mod._ensure_google_session_before_browser
    launch_ctx = cwv_mod._launch_context
    release_ctx = cwv_mod._release_context

    sk = (site_filter or "").strip().lower()
    if sk in ("all", "*", "both"):
        sk = ""
    props = PROPERTIES
    if sk:
        props = [p for p in PROPERTIES if p["site_key"] == sk or sk in (p["site_domain"] or "")]
    if not props:
        return {"ok": False, "message": f"site bulunamadı: {site_filter}"}

    if headed is None:
        headed = True

    captures: list[dict[str, Any]] = []
    login_retried = False

    # Nightly hiç açılmasın: oturum yoksa önce gerçek Firefox login
    pre = ensure_session(headed=headed)
    if isinstance(pre, dict) and not pre.get("ok"):
        return {
            "ok": False,
            "needs_login": True,
            "kind": "gsc_cwv_shots",
            "message": pre.get("message")
            or "GSC CWV login gerekli — gerçek Firefox.app ile giriş",
            "results": captures,
        }
    if isinstance(pre, dict) and pre.get("ok"):
        login_retried = True

    pw, ctx, selenium = launch_ctx(headed=headed)
    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        prop_i = 0
        while prop_i < len(props):
            prop = props[prop_i]
            try:
                cap = capture_property_shots(page, prop, cwv_mod)
                paths = cap.get("paths") or {}
                png_n = sum(1 for p in paths.values() if Path(str(p)).is_file() and Path(str(p)).stat().st_size > 1000)
                print(f"  · shot files · {cap.get('site_key')}: {png_n}/{len(paths)}", flush=True)
                if ingest:
                    cap["kpi_ingest"] = post_kpi_snapshot(cap)
                    print(
                        f"  · KPI ingest · {cap.get('site_key')}: "
                        f"{(cap.get('kpi_ingest') or {}).get('message') or (cap.get('kpi_ingest') or {}).get('ok')}",
                        flush=True,
                    )
                    cap["ingest"] = post_shots(cap)
                    print(
                        f"  · shots ingest · {cap.get('site_key')}: "
                        f"{(cap.get('ingest') or {}).get('message') or (cap.get('ingest') or {}).get('ok')}",
                        flush=True,
                    )
                captures.append(cap)
                prop_i += 1
            except GscNeedsSystemLogin as exc:
                if not headed or login_retried:
                    captures.append(
                        {
                            "site_key": prop.get("site_key"),
                            "ok": False,
                            "error": str(exc)[:300],
                            "needs_login": True,
                        }
                    )
                    print(f"CWV shots login gerekli: {exc}", flush=True)
                    break
                print(f"CWV shots → sistem Firefox login (Nightly yok): {exc}", flush=True)
                try:
                    release_ctx(pw, ctx, headed=headed, selenium=selenium)
                except Exception:
                    pass
                pw = ctx = None
                page = None
                selenium = False
                login_res = system_firefox_login()
                if not login_res.get("ok"):
                    return {
                        "ok": False,
                        "needs_login": True,
                        "kind": "gsc_cwv_shots",
                        "message": login_res.get("message")
                        or "GSC CWV login gerekli — gerçek Firefox.app ile giriş",
                        "results": captures,
                    }
                login_retried = True
                pw, ctx, selenium = launch_ctx(headed=headed)
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                print("GSC giriş OK — Selenium Firefox ile CWV shot devam.", flush=True)
            except Exception as exc:  # noqa: BLE001
                captures.append(
                    {
                        "site_key": prop.get("site_key"),
                        "ok": False,
                        "error": str(exc)[:300],
                    }
                )
                print(f"CWV shots hata: {exc}", flush=True)
                prop_i += 1
    finally:
        if pw is not None and ctx is not None:
            release_ctx(pw, ctx, headed=headed, selenium=selenium)

    ok_n = sum(
        1
        for c in captures
        if c.get("paths") and (not ingest or (c.get("ingest") or {}).get("ok"))
    )
    return {
        "ok": ok_n > 0,
        "kind": "gsc_cwv_shots",
        "captures": len(captures),
        "ok_captures": ok_n,
        "message": f"{ok_n}/{len(captures)} CWV shot set OK",
        "browser": "system_firefox_selenium" if headed else "playwright",
        "results": captures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GSC CWV screenshot capture")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--site", default="")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args(argv)
    if not args.sync:
        parser.print_help()
        return 2
    headed: bool | None = None
    if args.headed:
        headed = True
    elif args.headless:
        headed = False
    out = run_shots(site_filter=args.site, ingest=args.ingest, headed=headed)
    print(json.dumps({k: v for k, v in out.items() if k != "results"}, ensure_ascii=False))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
