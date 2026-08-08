#!/usr/bin/env python3
"""Google Play Console scrape (Mac bridge).

İlk giriş (headed — bir kez):
  .venv/bin/python scripts/play_console_scrape.py --login

Sync (varsayılan headed; Google headless’ta session düşürür):
  .venv/bin/python scripts/play_console_scrape.py --sync --ingest

Env:
  PLAY_CONSOLE_DEVELOPER_ID  (default 7587799419591090593)
  PLAY_CONSOLE_APP_ID        (default 4974102243818231576)
  PLAY_CONSOLE_PACKAGE       (default com.Doviz)
  PLAY_CONSOLE_PROFILE_DIR   (default ~/.seo-agent/play-console-profile)
  PLAY_CONSOLE_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    parsed: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip("'").strip('"')
        if k:
            parsed[k] = v
    for k, v in parsed.items():
        if k not in os.environ:
            os.environ[k] = v


_load_dotenv()

DEV_ID = (os.environ.get("PLAY_CONSOLE_DEVELOPER_ID") or "7587799419591090593").strip()
APP_ID = (os.environ.get("PLAY_CONSOLE_APP_ID") or "4974102243818231576").strip()
PACKAGE = (os.environ.get("PLAY_CONSOLE_PACKAGE") or "com.Doviz").strip()
PROFILE_DIR = Path(
    os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()
BASE_APP = f"https://play.google.com/console/u/0/developers/{DEV_ID}/app/{APP_ID}"
DASHBOARD_URL = (
    os.environ.get("PLAY_CONSOLE_DASHBOARD_URL") or f"{BASE_APP}/app-dashboard"
).strip()
REVIEWS_URL = (
    os.environ.get("PLAY_CONSOLE_REVIEWS_URL") or f"{BASE_APP}/user-feedback/reviews"
).strip()
MONETIZE_URL = (
    os.environ.get("PLAY_CONSOLE_MONETIZE_URL") or f"{BASE_APP}/monetize"
).strip()
STATISTICS_URL = (
    os.environ.get("PLAY_CONSOLE_STATISTICS_URL") or f"{BASE_APP}/statistics"
).strip()
INGEST_URL = (
    os.environ.get("PLAY_CONSOLE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/play-console/ingest"
).strip()

# Sayfa başına bilinen metrik başlıkları (TR Play Console)
_KNOWN_DASHBOARD = (
    "Toplam yükleme sayısı",
    "Kullanıcı kaybı",
    "Etkin cihazlar",
    "Kitle büyüme oranı",
    "Günlük etkin kullanıcı sayısı",
    "Mağaza girişi ziyaretçileri",
    "Mağaza girişi edinme sayısı",
    "Mağaza girişi dönüşüm oranı",
    "Kilitlenme oranı",
    "ANR oranı",
    "Google Play puanı",
    "Ortalama puan",
    "Uygulamayı yükleyen kullanıcı sayısı",
    "Cihaz edinme sayısı",
    "Cihaz ilk açılışları",
    "AEKS",
    "Gelir",
    "ÖYKBOG",
    "Alıcı Sayısı",
    "Yükleme tabanı",
    "Yeni cihaz edinme",
    "Yüklemeler",
)
_KNOWN_MONETIZE = (
    "Toplam gelir",
    "Gelir",
    "Aylık alıcılar",
    "Alıcı Sayısı",
    "Alıcı sayısı",
    "Aylık ÖYKBOG",
    "ÖYKBOG",
    "Aylık alıcı oranı",
    "Alıcı oranı",
    "Abonelikler",
    "Tek seferlik ürünler",
    "Yeni alıcılar",
    "Kümülatif alıcılar",
)


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _need_login(page_url: str, title: str, body_sample: str) -> bool:
    u = (page_url or "").lower()
    t = (title or "").lower()
    b = (body_sample or "").lower()
    if "accounts.google.com" in u or "signin" in u:
        return True
    if "sign in" in t or "oturum aç" in t:
        return True
    if "email or phone" in b or "e-posta veya telefon" in b:
        return True
    return False


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    pw = sync_playwright().start()
    context = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=not headed,
        viewport={"width": 1440, "height": 1100},
        locale="tr-TR",
        args=["--disable-blink-features=AutomationControlled"],
    )
    return pw, context


def run_login_interactive(timeout_sec: int = 600) -> dict[str, Any]:
    """Headed browser — kullanıcı cemevecen@nokta.com ile giriş yapar."""
    pw, context = _launch_context(headed=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        print(
            f"Tarayıcıda giriş yap (cemevecen@nokta.com). "
            f"Dashboard açılınca beklemeye gerek yok — {timeout_sec}s içinde otomatik kapanır.",
            flush=True,
        )
        deadline = time.time() + max(60, timeout_sec)
        while time.time() < deadline:
            url = page.url or ""
            title = ""
            try:
                title = page.title() or ""
            except Exception:
                pass
            if "play.google.com/console" in url and "accounts.google.com" not in url:
                # Cookie’lerin diske yazılması için biraz bekle
                time.sleep(5)
                try:
                    page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=60_000)
                    time.sleep(2)
                except Exception:
                    pass
                print(f"Login OK · {url}", flush=True)
                return {"ok": True, "url": url, "title": title, "profile": str(PROFILE_DIR)}
            time.sleep(2)
        return {
            "ok": False,
            "message": "Login zaman aşımı — tekrar --login dene",
            "url": page.url,
            "profile": str(PROFILE_DIR),
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _attach_network_capture(page, bag: list[dict[str, Any]]) -> None:
    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            if not any(
                x in url
                for x in (
                    "play.google.com",
                    "androidpublisher",
                    "playdeveloperreporting",
                    "googleapis.com",
                )
            ):
                return
            ctype = (resp.headers or {}).get("content-type", "")
            if "json" not in ctype and "javascript" not in ctype:
                return
            # Boyut sınırı
            body = None
            try:
                body = resp.json()
            except Exception:
                try:
                    text = resp.text()
                    if len(text) > 200_000:
                        text = text[:200_000]
                    body = {"_text": text}
                except Exception:
                    return
            bag.append(
                {
                    "url": url[:500],
                    "status": resp.status,
                    "body": body,
                }
            )
        except Exception:
            return

    page.on("response", on_response)


def _scroll_full_page(page) -> None:
    """TPG + kırılım kartları lazy load — sayfayı aşağı kaydır."""
    try:
        page.evaluate(
            """async () => {
              const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
              const h = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
              for (let y = 0; y < h; y += 700) {
                window.scrollTo(0, y);
                await sleep(350);
              }
              window.scrollTo(0, h);
              await sleep(800);
              window.scrollTo(0, 0);
              await sleep(400);
            }"""
        )
    except Exception:
        pass


def _extract_stats_page(page, *, known: tuple[str, ...] | list[str], page_key: str = "page") -> dict[str, Any]:
    """Herhangi bir Play Console istatistik sayfasından kart / kırılım çıkar."""
    known_list = list(known)
    return page.evaluate(
        """(args) => {
      const KNOWN = (args.known || []).slice().sort((a, b) => b.length - a.length);
      const pageKey = args.page_key || 'page';
      const ICON = /^(arrow_|calendar_|schedule|data_usage|devices|star|thumb_|expand_|feature_|visibility_|more_vert|dashboard|vital_|bar_chart|overview|shield|rocket_|finance_|sell|flag|link|youtube_|event_|brightness_)/i;
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const isJunk = (s) => {
        const t = clean(s);
        if (!t || t.length < 2) return true;
        if (ICON.test(t)) return true;
        if (/^(menu|ayar|yardım|ara|TPG'leri ekle|artışın iyi|grafik alan)/i.test(t)) return true;
        return false;
      };
      const isDateAxis = (s) => /^\\d{1,2}\\s*(Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara)\\b/i.test(s);
      const isDelta = (s) => {
        const t = clean(s);
        return /^[+\\-−%]/.test(t) || /yüzde puan/i.test(t) || /^[+]\\d/.test(t);
      };
      const isValue = (s) => {
        const t = clean(s);
        if (!t || !/\\d/.test(t) || isJunk(t) || isDateAxis(t)) return false;
        if (/^[+\\-−]/.test(t)) return false;
        if (t.length > 48) return false;
        return /[\\d₺$€%]/.test(t) || /\\b[BbMmKk]\\b/.test(t) || /yıldız/i.test(t);
      };
      const hintRe = /yükleme|kilitlenme|anr|puan|cihaz|aeks|gelir|alıcı|etkin|kitle|mağaza|öykbog|edinme|kaybı|abonelik|satın|revenue|buyer|arppu|arpu/i;

      const cards = [];
      const breakdowns = [];
      const seen = new Set();
      const seenBr = new Set();

      function pushCard(title, value, delta, period) {
        title = clean(title); value = clean(value); delta = clean(delta || ''); period = clean(period || '');
        if (!title || !value || !isValue(value)) return;
        const key = title + '|' + value;
        if (seen.has(key)) return;
        seen.add(key);
        cards.push({ title, value, delta, period, kind: 'metric', page: pageKey });
      }
      function pushBr(title, value, delta, segment) {
        title = clean(title); value = clean(value); delta = clean(delta || ''); segment = clean(segment || '');
        if (!title || !value || !/\\d/.test(value)) return;
        const key = title + '|' + value;
        if (seenBr.has(key)) return;
        seenBr.add(key);
        breakdowns.push({
          metric: title.split('(')[0].trim(),
          segment: segment || (title.match(/\\((.+)\\)/) || [])[1] || '',
          title, value, delta, kind: 'breakdown', page: pageKey
        });
      }

      const rawBody = (document.body && document.body.innerText) || '';
      const lines = rawBody.split(/\\n+/).map(clean).filter((l) => l && !isJunk(l));
      const bodyHas = {};
      for (const k of KNOWN) bodyHas[k] = rawBody.indexOf(k) >= 0;

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        let bm = line.match(/^(Yükleme tabanı)\\s*\\((.+)\\)$/i)
          || line.match(/^(Yeni cihaz edinme)\\s*\\((.+)\\)$/i)
          || line.match(/^(Gelir|Alıcı|ÖYKBOG|Abonelik)\\s*\\((.+)\\)$/i);
        if (bm) {
          let value = '', delta = '';
          for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
            const l = lines[j];
            if (KNOWN.some((k) => l === k) && l !== line) break;
            if (!value && isValue(l)) { value = l; continue; }
            if (value && !delta && isDelta(l)) { delta = l; break; }
          }
          if (value) pushBr(line, value, delta, bm[2] || bm[1]);
          continue;
        }

        let matched = null;
        for (const k of KNOWN) {
          if (line === k || line.toLowerCase() === k.toLowerCase()) { matched = k; break; }
        }
        if (!matched) {
          for (const k of KNOWN) {
            if (line.startsWith(k + ' ') && isValue(line.slice(k.length).trim())) {
              pushCard(k, line.slice(k.length).trim(), '', '');
              matched = k;
              break;
            }
          }
        }
        if (!matched) continue;
        if (/\\(/.test(line) && /yükleme tabanı|yeni cihaz|gelir|alıcı/i.test(line)) continue;

        let value = '', delta = '', period = '';
        for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
          const l = lines[j];
          if (KNOWN.some((k) => l === k || l.startsWith(k + ' ('))) break;
          if (/son \\d+ gün|önceki|geçen yıl|kıyasla|kümülatif|ortalama|last \\d+ days/i.test(l) && !isValue(l)) {
            period = period || l;
            continue;
          }
          if (!value && isValue(l)) { value = l; continue; }
          if (value && !delta && isDelta(l)) { delta = l; break; }
          if (value && l.length > 28 && !isDelta(l) && !isValue(l)) break;
        }
        if (value) pushCard(matched, value, delta, period);
      }

      const nodes = Array.from(document.querySelectorAll('div, article, section, li'));
      for (const el of nodes) {
        if (!el || el.children.length > 14) continue;
        const text = clean(el.innerText || '');
        if (!text || text.length < 8 || text.length > 320) continue;
        const cardLines = text.split(/\\n+/).map(clean).filter((l) => l && !isJunk(l));
        if (cardLines.length < 2 || cardLines.length > 10) continue;
        const title = cardLines[0];
        if (title.length < 3 || title.length > 90) continue;
        const br = title.match(/^(.+?)\\s*\\((.+)\\)$/);
        let value = cardLines.find((l, idx) => idx > 0 && isValue(l)) || '';
        let delta = cardLines.find((l) => l !== value && isDelta(l)) || '';
        if (!value) continue;
        if (br && /yükleme tabanı|yeni cihaz|gelir|alıcı|ülke|ürün/i.test(br[1])) {
          pushBr(title, value, delta, br[2]);
          continue;
        }
        const knownHit = KNOWN.find((k) => title === k || title.startsWith(k));
        if (knownHit || hintRe.test(title)) {
          pushCard(knownHit || title, value, delta, '');
        }
      }

      return {
        page: pageKey,
        cards,
        tpg: cards,
        breakdowns,
        card_count: cards.length,
        tpg_count: cards.length,
        breakdown_count: breakdowns.length,
        debug: {
          body_len: rawBody.length,
          line_count: lines.length,
          known_found_count: Object.values(bodyHas).filter(Boolean).length,
          known_found: bodyHas,
          url: location.href,
        },
      };
    }""",
        {"known": known_list, "page_key": page_key},
    )


def _extract_dashboard_structured(page) -> dict[str, Any]:
    """Dashboard: KPI + TPG + kırılım."""
    return _extract_stats_page(page, known=_KNOWN_DASHBOARD, page_key="dashboard")


def _extract_monetize_structured(page) -> dict[str, Any]:
    """Monetize overview kartları."""
    known = tuple(dict.fromkeys(list(_KNOWN_MONETIZE) + list(_KNOWN_DASHBOARD)))
    return _extract_stats_page(page, known=known, page_key="monetize")


def _series_from_network(network: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Yakalanan JSON yanıtlardan zaman serisi / metrik adaylarını çıkar."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(obj: Any, path: str = "") -> None:
        if len(out) >= 80:
            return
        if isinstance(obj, dict):
            # tipik: {name/metric, values/points/data}
            keys = {str(k).lower() for k in obj.keys()}
            name = obj.get("name") or obj.get("metric") or obj.get("title") or obj.get("displayName")
            vals = (
                obj.get("values")
                or obj.get("points")
                or obj.get("data")
                or obj.get("timeSeries")
                or obj.get("series")
            )
            if name and isinstance(vals, list) and vals and len(vals) >= 3:
                key = f"{name}|{len(vals)}"
                if key not in seen:
                    seen.add(key)
                    sample = vals[:40]
                    out.append(
                        {
                            "name": str(name)[:120],
                            "points": sample,
                            "point_count": len(vals),
                            "path": path[:120],
                        }
                    )
            for k, v in obj.items():
                walk(v, (path + "." + str(k))[:160])
        elif isinstance(obj, list):
            for i, v in enumerate(obj[:50]):
                walk(v, f"{path}[{i}]")

    for item in network or []:
        body = item.get("body") if isinstance(item, dict) else None
        if body is None:
            continue
        walk(body, str(item.get("url") or "")[:80])
    return out


def _metrics_from_structured(structured: dict[str, Any]) -> list[dict[str, Any]]:
    """Structured tpg + breakdown → düz metrics listesi (geriye uyum)."""
    rows: list[dict[str, Any]] = []
    for t in structured.get("tpg") or []:
        if not isinstance(t, dict):
            continue
        rows.append(
            {
                "title": t.get("title"),
                "value": t.get("value"),
                "delta": t.get("delta") or "",
                "period": t.get("period") or "",
                "kind": "tpg",
                "lines": [t.get("title"), t.get("value"), t.get("delta"), t.get("period")],
            }
        )
    for b in structured.get("breakdowns") or []:
        if not isinstance(b, dict):
            continue
        rows.append(
            {
                "title": b.get("title"),
                "value": b.get("value"),
                "delta": b.get("delta") or "",
                "segment": b.get("segment") or "",
                "metric": b.get("metric") or "",
                "kind": "breakdown",
                "lines": [b.get("title"), b.get("value"), b.get("delta")],
            }
        )
    return rows


def _extract_metrics_dom(page) -> list[dict[str, Any]]:
    """Geriye uyum: structured tpg + breakdown düz liste."""
    return _metrics_from_structured(_extract_dashboard_structured(page) or {})


def _extract_rating_summary_dom(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const text = document.body ? (document.body.innerText || '') : '';
      const pick = (re) => {
        const m = text.match(re);
        return m ? m[1] : null;
      };
      return {
        default_rating: pick(/Varsayılan Google Play puanı[^\\d]*([\\d,\\.]+)/i)
          || pick(/Google Play puanı[^\\d]*([\\d,\\.]+)/i),
        users: pick(/Kullanıcılar[^\\d]*([\\d\\.\\s]+)/i),
        ratings_with_reviews: pick(/Yorum içeren puanlar[^\\d]*([\\d\\.\\s]+)/i),
      };
    }"""
    )


def _extract_reviews_dom(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """() => {
      const out = [];
      const blocks = Array.from(document.querySelectorAll('article, li, div'));
      for (const el of blocks) {
        const t = (el.innerText || '').trim();
        if (!t || t.length < 40 || t.length > 4000) continue;
        if (!/yıldız|star|★|⭐|puan/i.test(t) && !/\\n.*\\n/.test(t)) continue;
        // Heuristic: author line + date + device meta + body
        const lines = t.split('\\n').map(s => s.trim()).filter(Boolean);
        if (lines.length < 3) continue;
        const hasDevice = /Android|iPhone|Samsung|Xiaomi|POCO|Galaxy|version/i.test(t);
        const hasDate = /\\d{1,2}\\s*(Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(t)
          || /\\d{1,2}[\\.\\/]\\d{1,2}[\\.\\/]\\d{2,4}/.test(t);
        if (!(hasDevice || hasDate)) continue;
        const author = lines[0].slice(0, 120);
        const body = lines.slice(1).join(' ').slice(0, 1500);
        const starM = t.match(/([1-5])\\s*(yıldız|star)/i) || t.match(/★{1,5}/);
        out.push({
          author,
          body,
          raw: t.slice(0, 2000),
          stars: starM ? starM[0] : null,
        });
        if (out.length >= 50) break;
      }
      // Dedup by author+body prefix
      const seen = new Set();
      const uniq = [];
      for (const r of out) {
        const k = (r.author || '') + '|' + (r.body || '').slice(0, 80);
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(r);
      }
      return uniq;
    }"""
    )


def _page_needs_login(page) -> tuple[bool, str, str]:
    url = ""
    title = ""
    body_sample = ""
    try:
        url = page.url or ""
    except Exception:
        pass
    try:
        title = page.title() or ""
    except Exception:
        pass
    try:
        body_sample = page.inner_text("body")[:800]
    except Exception:
        pass
    return _need_login(url, title, body_sample), url, title


def _wait_until_console(page, *, timeout_sec: int = 600) -> bool:
    """Login ekranındaysa kullanıcı girene kadar bekle; console URL gelince True."""
    deadline = time.time() + max(60, timeout_sec)
    printed = False
    while time.time() < deadline:
        need, url, _title = _page_needs_login(page)
        if not need and "play.google.com/console" in (url or ""):
            time.sleep(2)
            return True
        if not printed:
            print(
                "Play oturumu yok — açılan Chromium’da cemevecen@nokta.com ile giriş yap. "
                f"Dashboard gelince devam ({timeout_sec}s).",
                flush=True,
            )
            printed = True
        time.sleep(2)
    return False


def _settle(page, *, seconds: float = 4.0) -> None:
    """Play Console sürekli XHR atar — networkidle kullanma (sonsuz/yenileme hissi)."""
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass
    time.sleep(max(1.0, seconds))


def _wait_page_text(page, needles: tuple[str, ...], *, timeout_sec: float = 45.0) -> bool:
    """Sayfa metni hydrate olana kadar bekle."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            text = page.evaluate("() => (document.body && document.body.innerText) || ''") or ""
        except Exception:
            text = ""
        if any(n in text for n in needles):
            return True
        time.sleep(1.2)
    return False


def _wait_dashboard_metrics(page, *, timeout_sec: float = 45.0) -> bool:
    return _wait_page_text(
        page,
        (
            "Kilitlenme oranı",
            "TPG trendlerini izleyin",
            "Etkin cihazlar",
            "Cihaz edinme",
            "Toplam yükleme",
        ),
        timeout_sec=timeout_sec,
    )


def _scrape_one_stats_page(
    page,
    *,
    url: str,
    known: tuple[str, ...],
    page_key: str,
    wait_needles: tuple[str, ...],
    headed: bool,
) -> dict[str, Any]:
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    _settle(page, seconds=4.0)
    need, _, _ = _page_needs_login(page)
    if need and headed:
        _wait_until_console(page, timeout_sec=300)
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=4.0)
    _wait_page_text(page, wait_needles, timeout_sec=40.0)
    _scroll_full_page(page)
    _settle(page, seconds=2.5)
    _wait_page_text(page, wait_needles, timeout_sec=15.0)
    return _extract_stats_page(page, known=known, page_key=page_key) or {}


def scrape_play_console(*, headed: bool | None = None) -> dict[str, Any]:
    if headed is None:
        # Google Play, headless Chromium’da cookie’yi sık sık reddeder.
        env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
        headed = env_hl not in ("1", "true", "yes")
    # Login gerekirse mutlaka headed
    pw, context = _launch_context(headed=True if headed else False)
    network: list[dict[str, Any]] = []
    try:
        page = context.pages[0] if context.pages else context.new_page()
        _attach_network_capture(page, network)

        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=4.0)

        need, url, title = _page_needs_login(page)
        if need:
            if not headed:
                return {
                    "ok": False,
                    "needs_login": True,
                    "message": "Play oturumu yok — headed sync veya --login gerekli",
                    "url": url,
                    "metrics": [],
                    "reviews": [],
                    "rating_summary": {},
                    "raw_network": [],
                }
            # Tarayıcıyı kapatma — kullanıcı giriş yapsın
            if not _wait_until_console(page, timeout_sec=600):
                return {
                    "ok": False,
                    "needs_login": True,
                    "message": "Login zaman aşımı — tekrar dene",
                    "url": page.url if page else url,
                    "metrics": [],
                    "reviews": [],
                    "rating_summary": {},
                    "raw_network": [],
                }
            # Dashboard’a tekrar bas
            page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=4.0)

        need2, url2, _ = _page_needs_login(page)
        if need2:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Giriş sonrası hâlâ accounts.google.com — 2FA/hesap seçimini tamamla",
                "url": url2,
                "metrics": [],
                "reviews": [],
                "rating_summary": {},
                "raw_network": [],
            }

        # Dashboard
        _wait_dashboard_metrics(page, timeout_sec=45.0)
        _scroll_full_page(page)
        _settle(page, seconds=3.0)
        _wait_dashboard_metrics(page, timeout_sec=20.0)
        structured = _extract_dashboard_structured(page) or {}
        series = _series_from_network(network)
        metrics = _metrics_from_structured(structured)
        debug = structured.get("debug") if isinstance(structured.get("debug"), dict) else {}

        # Monetize (Play Console /monetize)
        monetize = {}
        try:
            monetize = _scrape_one_stats_page(
                page,
                url=MONETIZE_URL,
                known=tuple(dict.fromkeys(list(_KNOWN_MONETIZE) + ["Gelir", "ÖYKBOG", "Alıcı Sayısı"])),
                page_key="monetize",
                wait_needles=("Gelir", "ÖYKBOG", "Alıcı", "Toplam gelir", "Monetize", "Para kazan"),
                headed=bool(headed),
            )
        except Exception as mon_exc:  # noqa: BLE001
            monetize = {"page": "monetize", "cards": [], "breakdowns": [], "error": str(mon_exc)[:200]}

        mon_cards = monetize.get("cards") or monetize.get("tpg") or []
        mon_br = monetize.get("breakdowns") or []
        # Monetize kartlarını metrics’e de ekle (kind=monetize)
        for c in mon_cards:
            if not isinstance(c, dict):
                continue
            metrics.append(
                {
                    "title": c.get("title"),
                    "value": c.get("value"),
                    "delta": c.get("delta") or "",
                    "period": c.get("period") or "",
                    "kind": "monetize",
                    "page": "monetize",
                    "lines": [c.get("title"), c.get("value"), c.get("delta")],
                }
            )
        for b in mon_br:
            if not isinstance(b, dict):
                continue
            metrics.append(
                {
                    "title": b.get("title"),
                    "value": b.get("value"),
                    "delta": b.get("delta") or "",
                    "segment": b.get("segment") or "",
                    "kind": "breakdown",
                    "page": "monetize",
                    "lines": [b.get("title"), b.get("value"), b.get("delta")],
                }
            )

        dash_cards = structured.get("tpg") or structured.get("cards") or []
        dash_br = list(structured.get("breakdowns") or [])
        all_br = dash_br + list(mon_br)
        panels = {
            "version": 2,
            "tpg": dash_cards,
            "breakdowns": all_br,
            "monetize": mon_cards,
            "pages": {
                "dashboard": {
                    "url": DASHBOARD_URL,
                    "cards": dash_cards,
                    "breakdowns": dash_br,
                    "debug": debug,
                },
                "monetize": {
                    "url": MONETIZE_URL,
                    "cards": mon_cards,
                    "breakdowns": mon_br,
                    "debug": monetize.get("debug") if isinstance(monetize.get("debug"), dict) else {},
                    "error": monetize.get("error"),
                },
            },
            "sections": structured.get("sections") or [],
            "series": series,
            "tpg_count": len(dash_cards),
            "breakdown_count": len(all_br),
            "monetize_count": len(mon_cards),
            "series_count": len(series),
            "debug": debug,
        }

        # Reviews sayfası
        page.goto(REVIEWS_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=5.0)
        need_r, _, _ = _page_needs_login(page)
        if need_r and headed:
            _wait_until_console(page, timeout_sec=300)
            page.goto(REVIEWS_URL, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=5.0)
        rating_summary = _extract_rating_summary_dom(page) or {}
        reviews = _extract_reviews_dom(page) or []

        ok = bool(
            metrics
            or reviews
            or rating_summary.get("default_rating")
            or panels.get("tpg")
            or panels.get("monetize")
        )
        dbg = ""
        if not (panels.get("tpg") or panels.get("monetize")) and debug:
            dbg = f" · dash_known={debug.get('known_found_count')} body={debug.get('body_len')}"
        msg = (
            f"Play scrape · {len(metrics)} metric · "
            f"{panels.get('tpg_count', 0)} TPG · {panels.get('monetize_count', 0)} monetize · "
            f"{panels.get('breakdown_count', 0)} kırılım · {len(reviews)} review{dbg}"
        )
        return {
            "ok": ok,
            "needs_login": False,
            "message": msg,
            "url": DASHBOARD_URL,
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": metrics,
            "panels": panels,
            "reviews": reviews,
            "rating_summary": rating_summary,
            "raw_network": network[-40:],
            "source": "play_console_bridge",
            "source_url": DASHBOARD_URL,
            "sync_ok": ok,
            "sync_message": None,
            "sync_mode": "dashboard_monetize_reviews",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "message": str(exc),
            "metrics": [],
            "reviews": [],
            "rating_summary": {},
            "raw_network": network[-10:],
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    import requests

    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    payload = {
        "metrics": result.get("metrics") or [],
        "panels": result.get("panels") or {},
        "reviews": result.get("reviews") or [],
        "rating_summary": result.get("rating_summary") or {},
        "raw_network": result.get("raw_network") or [],
        "source": result.get("source") or "play_console_bridge",
        "source_url": result.get("source_url") or DASHBOARD_URL,
        "package_name": result.get("package_name") or PACKAGE,
        "app_id": result.get("app_id") or APP_ID,
        "sync_ok": bool(result.get("ok")),
        "sync_message": result.get("message"),
        "sync_mode": result.get("sync_mode") or "dashboard_reviews",
    }
    resp = requests.post(
        INGEST_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(payload),
        timeout=120,
    )
    try:
        body = resp.json() if resp.content else {}
    except Exception:
        body = {"raw": (resp.text or "")[:400]}
    if not isinstance(body, dict):
        body = {"message": str(body)}
    body["http_status"] = resp.status_code
    body["ok"] = resp.status_code < 400 and body.get("ok") is not False
    return body


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if "--login" in args:
        r = run_login_interactive()
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r.get("ok") else 1
    headed = "--headed" in args or "--headless" not in args
    if "--headless" in args:
        headed = False
        os.environ["PLAY_CONSOLE_HEADLESS"] = "1"
    do_ingest = "--ingest" in args or "--sync" in args
    # --sync implies scrape (+ ingest if token)
    print(f"Play scrape · headed={headed}", flush=True)
    result = scrape_play_console(headed=headed)
    print(json.dumps({k: v for k, v in result.items() if k != "raw_network"}, ensure_ascii=False, indent=2))
    if result.get("needs_login"):
        return 2
    if do_ingest and result.get("ok"):
        ing = ingest_scrape_result(result)
        print("INGEST:", json.dumps(ing, ensure_ascii=False, indent=2))
        return 0 if ing.get("ok") else 1
    if do_ingest and not result.get("ok"):
        # başarısız scrape'i de kaydet (UI'da mesaj görünsün)
        ing = ingest_scrape_result(result)
        print("INGEST (fail state):", json.dumps(ing, ensure_ascii=False, indent=2))
        return 1
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
