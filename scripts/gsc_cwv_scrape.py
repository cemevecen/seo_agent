#!/usr/bin/env python3
"""Google Search Console Core Web Vitals + AMP scrape (Mac bridge).

Playwright (play-console-profile) ile GSC CWV / AMP raporlarını çeker → Railway ingest.
Satır limiti yok — tablo sonuna kadar kaydırılır.

  .venv/bin/python scripts/gsc_cwv_scrape.py --login
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest --site doviz

Not: --login aynı play-console-profile’ı kullanan eski Chrome süreçlerini kapatır
(profil uyarısı / şifre ekranında ani kapanmayı önlemek için).

Env:
  GSC_CWV_PROFILE_DIR / GSC_LINKS_PROFILE_DIR / PLAY_CONSOLE_PROFILE_DIR
  GSC_CWV_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

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

PROFILE_DIR = Path(
    os.environ.get("GSC_CWV_PROFILE_DIR")
    or os.environ.get("GSC_LINKS_PROFILE_DIR")
    or os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()

INGEST_URL = (
    os.environ.get("GSC_CWV_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
).strip()

# device=2 → Mobil, device=1 → Masaüstü (GSC UI)
DEVICE_MOBILE = 2
DEVICE_DESKTOP = 1

# Bilinen kırılım anahtarları (tıklama başarısız olursa fallback)
KNOWN_ITEM_KEYS = {
    DEVICE_MOBILE: [
        "CAMQAhgC",  # LCP NI
        "CAUQAhgC",  # INP NI
        "CAUQAhgD",  # INP Poor
        "CAQQAhgC",  # CLS NI
    ],
    DEVICE_DESKTOP: [
        "CAQQARgC",  # CLS NI
        "CAMQARgD",  # LCP Poor
        "CAMQARgC",  # LCP NI
    ],
}

PROPERTIES: list[dict[str, str]] = [
    {
        "site_key": "doviz",
        "site_domain": "www.doviz.com",
        "resource_id": "sc-domain:doviz.com",
        "label": "doviz.com",
    },
    {
        "site_key": "sinemalar",
        "site_domain": "www.sinemalar.com",
        "resource_id": "https://www.sinemalar.com/",
        "label": "www.sinemalar.com",
    },
]

_PUA_RE = re.compile(r"[\ue000-\uf8ff\u0000-\u001f]")
_NUM_RE = re.compile(r"([\d\.\,]+)\s*(?:B|K|M)?", re.I)

# GSC overview çizgi grafiği renkleri (aplos chart)
_GSC_CHART_COLORS = {
    "#c53929": "poor",
    "#db4437": "poor",
    "#f09300": "needs_improvement",
    "#f4b400": "needs_improvement",
    "#0b8043": "good",
    "#0f9d58": "good",
}


def _ingest_token() -> str:
    return (
        os.environ.get("GSC_CWV_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("GSC_LINKS_INGEST_TOKEN")
        or ""
    ).strip()


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", _PUA_RE.sub("", s or "")).strip()


def _parse_count(raw: str) -> int:
    s = _clean(raw).replace("\u00a0", " ")
    if not s or s in {"-", "—", "Yok", "N/A"}:
        return 0
    m = re.search(r"([\d\.\,]+)\s*([BbKkMm])?", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else 0
    raw_num = m.group(1)
    suf = (m.group(2) or "").upper()
    try:
        if re.fullmatch(r"\d{1,3}([.,]\d{3})+", raw_num):
            val = float(raw_num.replace(".", "").replace(",", ""))
        elif "," in raw_num and "." in raw_num:
            if raw_num.rfind(",") > raw_num.rfind("."):
                val = float(raw_num.replace(".", "").replace(",", "."))
            else:
                val = float(raw_num.replace(",", ""))
        else:
            val = float(raw_num.replace(",", "."))
    except ValueError:
        return 0
    if suf in ("B", "K"):  # GSC TR "B" = bin
        val *= 1000
    elif suf == "M":
        val *= 1_000_000
    return int(round(val))


def _cwv_url(resource_id: str, path: str = "", **params: Any) -> str:
    rid = quote(resource_id, safe="")
    base = f"https://search.google.com/u/0/search-console/core-web-vitals{path}"
    q = f"resource_id={rid}&hl=en"
    for k, v in params.items():
        if v is None or v == "":
            continue
        q += f"&{k}={quote(str(v), safe='')}"
    return f"{base}?{q}"


def _amp_url(resource_id: str, path: str = "", **params: Any) -> str:
    rid = quote(resource_id, safe="")
    base = f"https://search.google.com/u/0/search-console/amp{path}"
    q = f"resource_id={rid}&hl=en"
    for k, v in params.items():
        if v is None or v == "":
            continue
        q += f"&{k}={quote(str(v), safe='')}"
    return f"{base}?{q}"


def _looks_signed_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        if "accounts.google.com" in url or "signin" in url or "challenge" in url:
            return False
        body = ""
        try:
            body = (page.inner_text("body") or "")[:1200].lower()
        except Exception:
            pass
        # Google şifre / 2FA / hesap seçimi — asla “giriş OK” sayma
        login_markers = (
            "email or phone",
            "e-posta veya telefon",
            "enter your password",
            "şifrenizi girin",
            "sifrenizi girin",
            "verify it’s you",
            "verify it's you",
            "2-step verification",
            "iki adımlı doğrulama",
            "forgot password",
            "şifrenizi unuttunuz",
        )
        if any(m in body for m in login_markers):
            return False
        if "search.google.com/search-console" in url or "search.google.com/u/" in url:
            return True
        # Property seçici / welcome
        if "search.google.com" in url and "accounts.google.com" not in url:
            if "search console" in body or "core web vitals" in body or "experience" in body:
                return True
        return False
    except Exception:
        return False


def _kill_stale_profile_browsers(profile_dir: Path) -> int:
    """Aynı user-data-dir’i tutan Chrome/Chromium süreçlerini kapat (profil çakışması)."""
    import signal
    import subprocess

    marker = str(profile_dir.resolve())
    killed = 0
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        out = ""
    for line in out.splitlines():
        if marker not in line:
            continue
        low = line.lower()
        if "chrome" not in low and "chromium" not in low:
            continue
        try:
            pid = int(line.split(None, 1)[0])
        except Exception:
            continue
        if pid <= 1 or pid == os.getpid():
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            killed += 1
        except ProcessLookupError:
            pass
        except Exception:
            pass
    if killed:
        time.sleep(1.0)
        try:
            out2 = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
        except Exception:
            out2 = ""
        for line in out2.splitlines():
            if marker not in line:
                continue
            low = line.lower()
            if "chrome" not in low and "chromium" not in low:
                continue
            try:
                pid = int(line.split(None, 1)[0])
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        time.sleep(0.4)
    return killed


def _clear_profile_locks(profile_dir: Path) -> None:
    for name in (
        "SingletonLock",
        "SingletonCookie",
        "SingletonSocket",
        "DevToolsActivePort",
        "RunningChromeVersion",
        "lockfile",
    ):
        p = profile_dir / name
        try:
            if p.is_symlink() or p.is_file():
                p.unlink(missing_ok=True)
        except Exception:
            pass


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    killed = _kill_stale_profile_browsers(PROFILE_DIR)
    if killed:
        print(f"GSC CWV profil: {killed} eski Chrome süreci kapatıldı ({PROFILE_DIR})", flush=True)
    _clear_profile_locks(PROFILE_DIR)
    pw = sync_playwright().start()
    channel = (
        os.environ.get("GSC_CWV_BROWSER_CHANNEL")
        or os.environ.get("GSC_LINKS_BROWSER_CHANNEL")
        or os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL")
        or "chrome"
    ).strip()
    kwargs: dict[str, Any] = {
        "user_data_dir": str(PROFILE_DIR),
        "headless": not headed,
        "viewport": {"width": 1440, "height": 1100},
        "locale": "en-US",
        "accept_downloads": True,
        "ignore_default_args": ["--enable-automation"],
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--no-default-browser-check",
            "--password-store=basic",
            "--use-mock-keychain",
        ],
    }
    if channel and channel.lower() not in ("0", "none", "chromium"):
        kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**kwargs)
    except Exception as exc:
        print(f"channel={channel!r} launch fail → bundled chromium: {exc}", flush=True)
        kwargs.pop("channel", None)
        _clear_profile_locks(PROFILE_DIR)
        context = pw.chromium.launch_persistent_context(**kwargs)
    return pw, context


def run_login_interactive(timeout_sec: int = 900) -> dict[str, Any]:
    """Headed login — şifre/2FA sırasında tarayıcıyı kapatma; profil kilidini önce temizle."""
    url = _cwv_url("sc-domain:doviz.com")
    print(f"Profil: {PROFILE_DIR}", flush=True)
    print(
        "Not: Aynı profilde başka Chrome açıksa kapatılır (profil uyarısı / ani kapanma önlemi).",
        flush=True,
    )
    pw, context = _launch_context(headed=True)
    ok_streak = 0
    cwv_nav_tried = False
    last_status = 0.0
    try:
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        except Exception as exc:
            print(f"İlk goto uyarısı (devam): {exc}", flush=True)
        print(
            f"Tarayıcıda Google ile GSC girişi yapın (şifre/2FA).\n"
            f"Search Console açılınca oturum otomatik kaydedilir (en fazla {timeout_sec}s).\n"
            f"Takılırsa Ctrl+C ile çıkıp: .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest --charts-only",
            flush=True,
        )
        deadline = time.time() + max(120, timeout_sec)
        while time.time() < deadline:
            try:
                if not context.pages:
                    return {
                        "ok": False,
                        "message": (
                            "Tarayıcı kapandı (profil çakışması veya Chrome çökmesi). "
                            "Tüm seo-agent Chrome pencerelerini kapatıp tekrar: "
                            "scripts/gsc_cwv_scrape.py --login"
                        ),
                        "profile": str(PROFILE_DIR),
                    }
                page = context.pages[0]
                cur = (page.url or "").lower()
                now = time.time()
                if now - last_status >= 15:
                    print(f"  · bekleniyor · url={ (page.url or '')[:120] }", flush=True)
                    last_status = now
                if "accounts.google.com" in cur or "signin" in cur or "challenge" in cur:
                    ok_streak = 0
                    cwv_nav_tried = False
                    time.sleep(2)
                    continue
                if not _looks_signed_in(page):
                    ok_streak = 0
                    time.sleep(2)
                    continue
                # GSC’ye girdik — CWV URL’sine yönlendir (zorunlu değil ama doğrular)
                if "core-web-vitals" not in cur and not cwv_nav_tried:
                    cwv_nav_tried = True
                    print("  · oturum görüldü → CWV sayfasına gidiliyor…", flush=True)
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                        time.sleep(2)
                    except Exception as exc:
                        print(f"  · CWV goto uyarısı: {exc}", flush=True)
                    continue
                ok_streak += 1
                if ok_streak >= 2:
                    time.sleep(4)
                    print(f"Login OK · {page.url}", flush=True)
                    return {"ok": True, "url": page.url, "profile": str(PROFILE_DIR)}
            except Exception as exc:
                msg = str(exc).lower()
                if "has been closed" in msg or "target closed" in msg or "crashed" in msg:
                    return {
                        "ok": False,
                        "message": (
                            "Tarayıcı oturumu kapandı. Profil kilidi için tekrar --login; "
                            "hâlâ olursa Chrome’daki tüm seo-agent pencerelerini kapatın."
                        ),
                        "profile": str(PROFILE_DIR),
                        "error": str(exc)[:200],
                    }
                ok_streak = 0
            time.sleep(2)
        return {
            "ok": False,
            "message": "Login zaman aşımı — şifre/2FA bitmeden süre doldu; tekrar --login",
            "url": (context.pages[0].url if context.pages else ""),
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
        _clear_profile_locks(PROFILE_DIR)


def _scroll_table_fully(page, *, max_rounds: int = 800) -> int:
    """Tabloyu sonuna kadar kaydır — uygulama tarafında satır limiti yok."""
    try:
        return int(
            page.evaluate(
                """async (maxRounds) => {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const rowCount = () => document.querySelectorAll('table tbody tr').length;
  const scrollTargets = () => {
    const out = [];
    const table = document.querySelector('table');
    let el = table;
    while (el && el !== document.body) {
      const st = window.getComputedStyle(el);
      const oy = st.overflowY || st.overflow || '';
      if ((oy.includes('auto') || oy.includes('scroll')) && el.scrollHeight > el.clientHeight + 40) {
        out.push(el);
      }
      el = el.parentElement;
    }
    if (document.scrollingElement) out.push(document.scrollingElement);
    out.push(document.documentElement, document.body);
    return out;
  };
  const clickMore = () => {
    const nodes = [...document.querySelectorAll('button, a, [role=button], span, div')];
    for (const n of nodes) {
      const t = ((n.innerText || n.textContent || '') + '').trim().toLowerCase();
      if (!t || t.length > 48) continue;
      if (t.includes('daha fazla') || t.includes('show more') || t.includes('load more')) {
        try { n.click(); return true; } catch (_) {}
      }
    }
    return false;
  };
  let last = rowCount();
  let stable = 0;
  for (let i = 0; i < maxRounds; i++) {
    clickMore();
    for (const el of scrollTargets()) {
      try { el.scrollTop = el.scrollHeight; } catch (_) {}
    }
    try { window.scrollTo(0, document.body.scrollHeight); } catch (_) {}
    await sleep(280);
    const now = rowCount();
    if (now <= last) {
      stable += 1;
      if (stable >= 10) break;
    } else {
      stable = 0;
      last = now;
    }
  }
  return rowCount();
}""",
                max_rounds,
            )
        )
    except Exception:
        return 0


def _extract_table(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const clean = (s) => (s || '').replace(/[\\ue000-\\uf8ff]/g, '').replace(/\\s+/g, ' ').trim();
      const table = document.querySelector('table');
      if (!table) return { headers: [], rows: [], row_count: 0 };
      const headers = [...table.querySelectorAll('thead th, thead td')].map((el) => clean(el.innerText));
      const rows = [...table.querySelectorAll('tbody tr')].map((tr) =>
        [...tr.querySelectorAll('td')].map((td) => clean(td.innerText))
      );
      return { headers, rows, row_count: rows.length };
    }"""
    )


def _extract_page_meta(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const clean = (s) => (s || '').replace(/[\\ue000-\\uf8ff]/g, '').replace(/\\s+/g, ' ').trim();
      const body = clean((document.body && document.body.innerText) || '');
      const title = clean((document.querySelector('h1, [role=heading]') || {}).innerText || '');
      return { title, body_head: body.slice(0, 2500), url: location.href };
    }"""
    )


def _status_from_text(text: str) -> str:
    t = (text or "").lower()
    if "yetersiz" in t or "poor" in t or "kötü" in t or t.startswith("error"):
        return "poor"
    if "iyileştir" in t or "needs" in t or "improvement" in t or "warning" in t:
        return "needs_improvement"
    if "iyi" in t or "good" in t or "check_circle" in t:
        return "good"
    return "unknown"


def _metric_from_issue(title: str) -> str:
    t = (title or "").upper()
    for m in ("LCP", "INP", "CLS", "FID", "FCP", "TTFB"):
        if m in t:
            return m
    low = (title or "").lower()
    if "görüntü" in low or "image" in low:
        return "AMP_IMAGE"
    if "javascript" in low or "custom js" in low:
        return "AMP_JS"
    return "OTHER"


def explain_causes(metric: str, status: str, title: str = "") -> list[str]:
    """Olası sebepler — panoda ve mailde gösterilir."""
    m = (metric or "").upper()
    causes: list[str] = []
    if m == "LCP":
        causes = [
            "Büyük hero / LCP görselleri (özellikle /amp sayfalarında boyut önerisinin altında kalan img).",
            "Yavaş sunucu yanıtı (TTFB) veya render-blocking CSS/JS.",
            "Client-side hydration / geç yüklenen kritik içerik.",
            "Üçüncü taraf reklam veya widget’ların LCP öğesini geciktirmesi.",
        ]
    elif m == "INP":
        causes = [
            "Uzun ana-thread görevleri (ağır JS, senkron iş).",
            "Tıklama/scroll sırasında pahalı event handler’lar.",
            "Çok sayıda üçüncü taraf script (ads, analytics, chat).",
            "Büyük DOM ve sık layout thrashing.",
        ]
    elif m == "CLS":
        causes = [
            "Boyutsuz görseller / iframe / reklam slotları.",
            "Web font swap (FOUT) ile metin kayması.",
            "Dinamik enjekte edilen banner / sticky elemanlar.",
            "Lazy-load içeriklerin yer tutucu olmadan gelmesi.",
        ]
    elif m == "AMP_IMAGE":
        causes = [
            "AMP görsellerinin önerilen boyuttan küçük olması (geçerli ama best-practice dışı).",
            "srcset / width-height eksikliği.",
            "CDN dönüşümlerinde düşük çözünürlük üretimi.",
        ]
    elif m == "AMP_JS" or (m == "OTHER" and "javascript" in (title or "").lower()):
        causes = [
            "AMP sayfasında özel (custom) JavaScript kullanılmış — AMP spec buna izin vermez.",
            "İlgili script’i kaldırın veya AMP-uyumlu bileşenle değiştirin (amp-script sınırlıdır).",
            "Canonical HTML sürümünde JS kalabilir; AMP kopyası sade kalmalıdır.",
        ]
    else:
        causes = [
            "CrUX alan verisinde eşik aşımı — sayfa şablonunu ve üçüncü tarafları gözden geçirin.",
        ]
        short = (title or "").strip()
        if short and len(short) <= 120 and "breadcrumb" not in short.lower():
            causes.append(f"GSC sorunu: {short}")
    if status == "poor":
        causes.insert(0, "Durum: Poor — kullanıcı deneyimi eşiğinin altında; öncelikli düzeltme.")
    elif status == "needs_improvement":
        causes.insert(0, "Durum: Needs improvement — Good bandına çekmek için iyileştirme gerekir.")
    return causes


_OV_TRIPLET_RES = (
    # TR: 0 yetersiz URL · 14.674 URL iyileştirme gerektiriyor · 3.036 iyi URL
    re.compile(
        r"(\d[\d\.,]*)\s*(?:yetersiz|kötü)\s*URL.{0,80}?"
        r"([\d\.,]+)\s*URL.{0,40}?iyileştir.{0,80}?"
        r"([\d\.,]+)\s*iyi\s*URL",
        re.I | re.S,
    ),
    # EN: 0 poor URLs · 14,674 URLs need improvement · 3,036 good URLs
    re.compile(
        r"([\d,]+)\s*poor\s*URL.{0,80}?"
        r"([\d,]+)\s*URL?s?\s*(?:need improvement|need improv).{0,80}?"
        r"([\d,]+)\s*good\s*URL",
        re.I | re.S,
    ),
    # EN legend: Poor 0 · Need improvement 14,674 · Good 3,036
    re.compile(
        r"Poor\s*([\d\.,]+K?).{0,80}?Need improvement\s*([\d\.,]+K?).{0,80}?Good\s*([\d\.,]+K?)",
        re.I | re.S,
    ),
)


def _parse_overview_triplet(block: str) -> dict[str, int] | None:
    for cre in _OV_TRIPLET_RES:
        m = cre.search(block or "")
        if not m:
            continue
        return {
            "poor": _parse_count(m.group(1)),
            "needs_improvement": _parse_count(m.group(2)),
            "good": _parse_count(m.group(3)),
        }
    return None


def _parse_overview_counts(body: str) -> dict[str, dict[str, int]]:
    out = {
        "mobile": {"poor": 0, "needs_improvement": 0, "good": 0},
        "desktop": {"poor": 0, "needs_improvement": 0, "good": 0},
    }
    text = body or ""
    parts = re.split(r"(?=Mobil\b|Mobile\b|Masaüstü\b|Desktop\b)", text, flags=re.I)
    for part in parts:
        head = part[:24].lower()
        parsed = _parse_overview_triplet(part[:1200])
        if not parsed:
            continue
        if head.startswith("mobil") or head.startswith("mobile"):
            out["mobile"] = parsed
        elif head.startswith("masaüstü") or head.startswith("masaustu") or head.startswith("desktop"):
            out["desktop"] = parsed
    if out["mobile"]["good"] or out["mobile"]["needs_improvement"]:
        return out
    # fallback: whole body
    parsed = _parse_overview_triplet(text)
    if parsed:
        out["mobile"] = parsed
    return out


_TR_MONTHS = {
    "oca": 1, "şub": 2, "sub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6,
    "tem": 7, "ağu": 8, "agu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12,
}
_EN_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _gsc_label_to_iso(label: str, *, default_year: int | None = None) -> str:
    """GSC etiket: M/D/YY, D.M.YYYY, '9 Ağu', 'Aug 9, 2026' → YYYY-MM-DD."""
    s = re.sub(r"\s+", " ", (label or "").strip()).strip(" .,")
    if not s:
        return ""
    year_now = default_year or datetime.now(timezone.utc).year
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + y if y < 100 else y
        month, day = (b, a) if a > 12 else (a, b)  # hl=en → M/D
        try:
            datetime(year, month, day)
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return ""
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$", s)
    if m:
        day, month, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + y if y < 100 else y
        try:
            datetime(year, month, day)
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return ""
    m = re.search(
        r"(\d{1,2})\s+([A-Za-zçğıöşüÇĞİÖŞÜ]{3,})(?:\s+(\d{2,4}))?",
        s,
        re.I,
    )
    if not m:
        m = re.search(
            r"([A-Za-z]{3,})\s+(\d{1,2})(?:,?\s+(\d{2,4}))?",
            s,
            re.I,
        )
        if m:
            mon_s, day_s, year_s = m.group(1), m.group(2), m.group(3)
            key = mon_s.lower()[:3]
            month = _EN_MONTHS.get(key)
            day = int(day_s)
            year = int(year_s) if year_s else year_now
            if year < 100:
                year += 2000
            if month:
                try:
                    datetime(year, month, day)
                    return f"{year:04d}-{month:02d}-{day:02d}"
                except ValueError:
                    return ""
        return ""
    day = int(m.group(1))
    key = m.group(2).lower().replace("ı", "i").replace("ş", "s").replace("ğ", "g")[:3]
    month = _TR_MONTHS.get(m.group(2).lower()[:3]) or _TR_MONTHS.get(key) or _EN_MONTHS.get(key)
    year_s = m.group(3)
    year = int(year_s) if year_s else year_now
    if year < 100:
        year += 2000
    if not month:
        return ""
    try:
        datetime(year, month, day)
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return ""


def _mdy_to_iso(label: str) -> str:
    return _gsc_label_to_iso(label)


def _axis_max_from_labels(labels: list[str]) -> float:
    best = 0.0
    for lab in labels or []:
        s = str(lab or "").strip().upper().replace(",", "")
        m = re.match(r"^([\d\.]+)\s*([KMB])?$", s)
        if not m:
            continue
        n = float(m.group(1))
        suf = m.group(2) or ""
        if suf in ("K", "B"):  # GSC TR "B" = bin
            n *= 1_000
        elif suf == "M":
            n *= 1_000_000
        if n > best:
            best = n
    return best


def _status_from_hex(color: str) -> str | None:
    c = (color or "").strip().lower()
    if not c.startswith("#") or len(c) < 7:
        return None
    return _GSC_CHART_COLORS.get(c[:7])


def _daily_iso_range(t0: datetime, t1: datetime) -> list[str]:
    if t1 < t0:
        t0, t1 = t1, t0
    n = (t1.date() - t0.date()).days
    n = max(0, min(n, 400))
    return [(t0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n + 1)]


def _interp_y(by_x: dict[float, float], x: float) -> float:
    if not by_x:
        return 0.0
    if x in by_x:
        return by_x[x]
    xs = sorted(by_x)
    if x <= xs[0]:
        return by_x[xs[0]]
    if x >= xs[-1]:
        return by_x[xs[-1]]
    for i in range(1, len(xs)):
        if xs[i] >= x:
            x0, x1 = xs[i - 1], xs[i]
            if x1 == x0:
                return by_x[x0]
            t = (x - x0) / (x1 - x0)
            return by_x[x0] + t * (by_x[x1] - by_x[x0])
    return by_x[xs[-1]]


def _parse_gsc_chart_tooltip(text: str) -> dict[str, Any] | None:
    t = _clean(text)
    if not t or len(t) < 8:
        return None
    date = ""
    for line in t.splitlines():
        iso = _gsc_label_to_iso(line)
        if iso:
            date = iso
            break
    if not date:
        m = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})", t)
        if m:
            date = _gsc_label_to_iso(m.group(1))
    if not date:
        m = re.search(
            r"(\d{1,2}\s+[A-Za-zçğıöşüÇĞİÖŞÜ]{3,}(?:\s+\d{2,4})?|[A-Za-z]{3,}\s+\d{1,2}(?:,?\s+\d{2,4})?)",
            t,
        )
        if m:
            date = _gsc_label_to_iso(m.group(0))
    if not date:
        return None

    def grab(*pats: str) -> int:
        for pat in pats:
            m = re.search(pat, t, re.I)
            if m:
                return _parse_count(m.group(1))
        return 0

    poor = grab(
        r"(?:yetersiz|kötü|poor)[^\d]{0,28}([\d\.,]+)",
        r"([\d\.,]+)[^\d]{0,12}(?:yetersiz|kötü|poor)",
    )
    ni = grab(
        r"iyileştir[^\d]{0,40}([\d\.,]+)",
        r"(?:need improvement|needs improvement)[^\d]{0,20}([\d\.,]+)",
        r"([\d\.,]+)[^\d]{0,24}iyileştir",
        r"([\d\.,]+)[^\d]{0,20}(?:need improvement|needs improvement)",
    )
    good = grab(
        r"(?:iyi\s+URL|good(?:\s+URL)?)[^\d]{0,28}([\d\.,]+)",
        r"([\d\.,]+)[^\d]{0,12}(?:iyi\s+URL|good(?:\s+URL)?)",
    )
    return {"date": date, "poor": poor, "needs_improvement": ni, "good": good}


_TIP_TEXT_JS = r"""() => {
  const hit = (el) => {
    const t = (el.innerText || el.textContent || '').trim();
    if (t.length < 12 || t.length > 500) return false;
    return /poor|good|yetersiz|kötü|kotu|iyileştir|improvement|iyi url/i.test(t);
  };
  const nodes = [...document.querySelectorAll('div, span, li, p')].filter((el) => {
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none' || Number(cs.opacity) === 0) return false;
    const r = el.getBoundingClientRect();
    if (r.width < 70 || r.height < 28 || r.width > 520) return false;
    return hit(el);
  });
  nodes.sort((a, b) => (a.innerText || '').length - (b.innerText || '').length);
  return nodes.length ? (nodes[0].innerText || '') : '';
}"""


def _harvest_overview_tooltips(page) -> list[dict[str, Any]]:
    """Grafik üzerinde gezerek GSC tooltip'inden günlük tam sayıları oku."""
    charts: list[dict[str, Any]] = []
    try:
        boxes = page.evaluate(
            """() => [...document.querySelectorAll('svg')]
              .map(svg => svg.getBoundingClientRect())
              .filter(bb => bb.width >= 320 && bb.height >= 120)
              .slice(0, 2)
              .map(bb => ({x: bb.x, y: bb.y, width: bb.width, height: bb.height}))"""
        )
    except Exception:
        return charts
    for box in boxes or []:
        if not isinstance(box, dict):
            continue
        w = float(box.get("width") or 0)
        h = float(box.get("height") or 0)
        if w < 320 or h < 120:
            continue
        samples: dict[str, dict[str, Any]] = {}
        n = 72
        for i in range(n):
            x = float(box["x"]) + 20 + (w - 32) * i / max(n - 1, 1)
            y = float(box["y"]) + h * 0.42
            try:
                page.mouse.move(x, y)
            except Exception:
                continue
            time.sleep(0.025)
            try:
                text = page.evaluate(_TIP_TEXT_JS)
            except Exception:
                text = ""
            parsed = _parse_gsc_chart_tooltip(str(text or ""))
            if parsed and parsed.get("date"):
                samples[str(parsed["date"])] = parsed
        if len(samples) < 12:
            continue
        dates = sorted(samples)
        charts.append(
            {
                "dates": dates,
                "poor": [int(samples[d]["poor"]) for d in dates],
                "needs_improvement": [int(samples[d]["needs_improvement"]) for d in dates],
                "good": [int(samples[d]["good"]) for d in dates],
                "point_count": len(dates),
                "source": "gsc_tooltip",
            }
        )
    return charts


def _snap_series_to_kpis(chart_series: dict[str, Any], overview: dict[str, Any]) -> None:
    """Son günün noktasını GSC başlık KPI’sına kilitle (tooltip/SVG sapmasını kapatır)."""
    if not isinstance(chart_series, dict) or not isinstance(overview, dict):
        return
    for key in ("mobile", "desktop"):
        ser = chart_series.get(key)
        kpis = overview.get(key) if isinstance(overview.get(key), dict) else {}
        if not isinstance(ser, dict) or not kpis:
            continue
        dates = ser.get("dates") or []
        if not dates:
            continue
        for metric in ("poor", "needs_improvement", "good"):
            arr = list(ser.get(metric) or [])
            if not arr:
                continue
            kpi_v = kpis.get(metric)
            if kpi_v is None:
                continue
            arr[-1] = int(kpi_v)
            ser[metric] = arr


def _extract_overview_chart_series(page, *, last_updated: str = "") -> dict[str, Any]:
    """GSC overview Mobil/Masaüstü çizgi grafikleri — tooltip (tam sayı) + SVG yedek."""
    raw = page.evaluate(
        r"""() => {
          const COLOR = {
            '#c53929': 'poor', '#db4437': 'poor',
            '#f09300': 'needs_improvement', '#f4b400': 'needs_improvement',
            '#0b8043': 'good', '#0f9d58': 'good'
          };
          function statusFrom(stroke) {
            if (!stroke) return null;
            const s = String(stroke).trim().toLowerCase();
            if (COLOR[s]) return COLOR[s];
            if (s.startsWith('#') && s.length >= 7) return COLOR[s.slice(0,7)] || null;
            return null;
          }
          function parsePathPoints(d) {
            const tokens = String(d || '').match(/[MmLlHhVvCcSsQqTtAaZz]|[-+]?(?:\d*\.\d+|\d+)(?:e[-+]?\d+)?/g) || [];
            const pts = [];
            let i = 0, cmd = null, x = 0, y = 0;
            const num = () => parseFloat(tokens[i++]);
            while (i < tokens.length) {
              const t = tokens[i];
              if (/^[A-Za-z]$/.test(t)) { cmd = t; i++; continue; }
              if (!cmd) { i++; continue; }
              try {
                if (cmd === 'M' || cmd === 'L') { x = num(); y = num(); pts.push([x,y]); if (cmd === 'M') cmd = 'L'; }
                else if (cmd === 'm' || cmd === 'l') { x += num(); y += num(); pts.push([x,y]); if (cmd === 'm') cmd = 'l'; }
                else if (cmd === 'H') { x = num(); pts.push([x,y]); }
                else if (cmd === 'h') { x += num(); pts.push([x,y]); }
                else if (cmd === 'V') { y = num(); pts.push([x,y]); }
                else if (cmd === 'v') { y += num(); pts.push([x,y]); }
                else if (cmd === 'C') { num();num();num();num(); x=num(); y=num(); pts.push([x,y]); }
                else if (cmd === 'c') { num();num();num();num(); x+=num(); y+=num(); pts.push([x,y]); }
                else if (cmd === 'S' || cmd === 'Q') { num();num(); x=num(); y=num(); pts.push([x,y]); }
                else if (cmd === 's' || cmd === 'q') { num();num(); x+=num(); y+=num(); pts.push([x,y]); }
                else if (cmd === 'T') { x=num(); y=num(); pts.push([x,y]); }
                else if (cmd === 't') { x+=num(); y+=num(); pts.push([x,y]); }
                else if (cmd === 'A') { num();num();num();num();num(); x=num(); y=num(); pts.push([x,y]); }
                else if (cmd === 'a') { num();num();num();num();num(); x+=num(); y+=num(); pts.push([x,y]); }
                else if (cmd === 'Z' || cmd === 'z') { /* close */ }
                else { i++; }
              } catch (e) { break; }
            }
            return pts;
          }
          const svgs = [...document.querySelectorAll('svg')].filter(svg => {
            const bb = svg.getBoundingClientRect();
            return bb.width >= 320 && bb.height >= 120;
          });
          const charts = [];
          for (const svg of svgs) {
            const bb = svg.getBoundingClientRect();
            const texts = [...svg.querySelectorAll('text')].map(t => (t.textContent || '').trim()).filter(Boolean);
            const dateLabels = texts.filter(t =>
              /^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(t) || /^\d{1,2}\.\d{1,2}\.\d{2,4}$/.test(t)
            );
            const axisNums = texts.filter(t =>
              /^[\d\.,]+\s*[KMB]?$/i.test(t) &&
              !/^\d{1,2}[./]\d{1,2}[./]\d{2,4}$/.test(t)
            );
            const series = {};
            for (const path of svg.querySelectorAll('path')) {
              const stroke = path.getAttribute('stroke') || '';
              if (!stroke || stroke === 'none' || stroke === 'transparent') continue;
              const st = statusFrom(stroke);
              if (!st) continue;
              const d = path.getAttribute('d') || '';
              if (d.length < 40) continue;
              const pts = parsePathPoints(d);
              if (pts.length < 8) continue;
              // Tercihen daha uzun seri
              if (!series[st] || pts.length > series[st].length) series[st] = pts;
            }
            if (Object.keys(series).length) {
              charts.push({
                width: bb.width, height: bb.height,
                dateLabels, axisNums, series
              });
            }
          }
          return charts;
        }"""
    )
    end_iso = _gsc_label_to_iso(last_updated)
    charts: list[dict[str, Any]] = []
    for ch in raw or []:
        if not isinstance(ch, dict):
            continue
        date_labels = [str(x) for x in (ch.get("dateLabels") or [])]
        iso_dates = [_gsc_label_to_iso(x) for x in date_labels]
        iso_dates = [d for d in iso_dates if d]
        y_max = _axis_max_from_labels([str(x) for x in (ch.get("axisNums") or [])])
        if y_max <= 0:
            y_max = 1.0
        series_pts = ch.get("series") or {}
        all_xy: list[tuple[float, float]] = []
        for pts in series_pts.values():
            for p in pts or []:
                if isinstance(p, (list, tuple)) and len(p) >= 2:
                    all_xy.append((float(p[0]), float(p[1])))
        if not all_xy:
            continue
        xs = [p[0] for p in all_xy]
        ys = [p[1] for p in all_xy]
        x_min, x_max = min(xs), max(xs)
        y_bottom, y_top = max(ys), min(ys)
        if x_max <= x_min:
            continue
        if iso_dates:
            t0 = datetime.strptime(iso_dates[0], "%Y-%m-%d")
            t1 = datetime.strptime(iso_dates[-1], "%Y-%m-%d")
        else:
            t0 = t1 = datetime.now(timezone.utc).replace(tzinfo=None)
        if end_iso:
            try:
                t_end = datetime.strptime(end_iso, "%Y-%m-%d")
                if t_end >= t0:
                    t1 = t_end
            except ValueError:
                pass
        dates = _daily_iso_range(t0, t1)
        if not dates:
            continue
        span = max(len(dates) - 1, 1)

        def y_to_val(y: float) -> float:
            if y_bottom <= y_top:
                return 0.0
            ratio = (y_bottom - y) / (y_bottom - y_top)
            ratio = max(0.0, min(1.2, ratio))
            return max(0.0, ratio * y_max)

        out_series: dict[str, list[int]] = {
            "poor": [],
            "needs_improvement": [],
            "good": [],
        }
        for status in out_series:
            pts = series_pts.get(status) or []
            by_x = {
                float(p[0]): y_to_val(float(p[1]))
                for p in pts
                if isinstance(p, (list, tuple)) and len(p) >= 2
            }
            vals: list[int] = []
            for i in range(len(dates)):
                x = x_min + (x_max - x_min) * (i / span)
                vals.append(int(round(_interp_y(by_x, x))))
            out_series[status] = vals

        charts.append(
            {
                "dates": dates,
                "poor": out_series["poor"],
                "needs_improvement": out_series["needs_improvement"],
                "good": out_series["good"],
                "y_max": y_max,
                "date_labels": date_labels,
                "point_count": len(dates),
                "source": "gsc_overview_svg",
            }
        )

    tip_charts: list[dict[str, Any]] = []
    try:
        tip_charts = _harvest_overview_tooltips(page)
    except Exception as exc:  # noqa: BLE001
        print(f"    tooltip harvest skip: {exc}", flush=True)
    if len(tip_charts) >= 1:
        print(
            f"    tooltip charts={len(tip_charts)} "
            f"pts={[c.get('point_count') for c in tip_charts]}",
            flush=True,
        )
        charts = tip_charts

    result: dict[str, Any] = {"mobile": None, "desktop": None, "source": "gsc_overview_svg"}
    if charts and charts[0].get("source") == "gsc_tooltip":
        result["source"] = "gsc_tooltip"
    if len(charts) >= 1:
        result["mobile"] = charts[0]
    if len(charts) >= 2:
        result["desktop"] = charts[1]
    return result


def _wait_table(page, timeout_ms: int = 20000) -> None:
    try:
        page.wait_for_selector("table tbody tr", timeout=timeout_ms)
    except Exception:
        pass
    time.sleep(1.2)


def _scrape_url_table(page) -> list[dict[str, Any]]:
    _scroll_table_fully(page)
    raw = _extract_table(page)
    headers = [h.lower() for h in (raw.get("headers") or [])]
    rows_out: list[dict[str, Any]] = []
    for row in raw.get("rows") or []:
        if not row or not any(row):
            continue
        item: dict[str, Any] = {"cells": row}
        # URL
        url = ""
        for cell in row:
            if cell.startswith("http://") or cell.startswith("https://"):
                url = cell
                break
        if url:
            item["url"] = url
        # group count
        for i, h in enumerate(headers):
            if i >= len(row):
                break
            if "grup" in h or "url sayısı" in h or "urls" in h:
                item["group_url_count"] = _parse_count(row[i])
            if h.startswith("lcp") or "lcp" in h:
                item["metric_value"] = row[i]
                item["metric"] = "LCP"
            if h.startswith("inp") or "inp" in h:
                item["metric_value"] = row[i]
                item["metric"] = "INP"
            if h.startswith("cls") or "cls" in h:
                item["metric_value"] = row[i]
                item["metric"] = "CLS"
            if "tarama" in h or "crawl" in h:
                item["last_crawl"] = row[i]
        if not item.get("url") and row[0].startswith("http"):
            item["url"] = row[0]
        if item.get("url") or item.get("cells"):
            rows_out.append(item)
    return rows_out


def _scrape_device(page, *, resource_id: str, device: int, label: str) -> dict[str, Any]:
    print(f"  · {label} summary…", flush=True)
    page.goto(_cwv_url(resource_id, "/summary", device=device), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    _wait_table(page)
    meta = _extract_page_meta(page)
    body = meta.get("body_head") or ""
    # KPIs from summary header
    kpis = {"poor": 0, "needs_improvement": 0, "good": 0}
    m_poor = re.search(r"(?:Yetersiz|Kötü|Poor)\s+([\d\.\,]+(?:\s*[BK])?)", body, re.I)
    m_ni = re.search(r"(?:İyileştirme gerektiriyor|Need improvement)\s+([\d\.\,]+(?:\s*[BK])?)", body, re.I)
    m_good = re.search(r"(?:İyi|Good)\s+([\d\.\,]+(?:\s*[BK])?)", body, re.I)
    if m_poor:
        kpis["poor"] = _parse_count(m_poor.group(1))
    if m_ni:
        kpis["needs_improvement"] = _parse_count(m_ni.group(1))
    if m_good:
        kpis["good"] = _parse_count(m_good.group(1))

    issues_raw = _extract_table(page)
    issues: list[dict[str, Any]] = []
    for row in issues_raw.get("rows") or []:
        if len(row) < 2:
            continue
        status = _status_from_text(row[0])
        title = row[1]
        urls_n = _parse_count(row[-1]) if row else 0
        issues.append(
            {
                "status": status,
                "title": title,
                "verification": row[2] if len(row) > 2 else "",
                "url_count": urls_n,
                "metric": _metric_from_issue(title),
                "causes": explain_causes(_metric_from_issue(title), status, title),
            }
        )

    # Click each issue row → drilldown (discover item_key)
    drilldowns: list[dict[str, Any]] = []
    issue_count = len(issues)
    for idx in range(issue_count):
        page.goto(_cwv_url(resource_id, "/summary", device=device), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page)
        rows = page.locator("table tbody tr")
        n = rows.count()
        if idx >= n:
            break
        print(f"    issue {idx + 1}/{n}…", flush=True)
        try:
            rows.nth(idx).click(timeout=20_000)
            time.sleep(3.5)
        except Exception as exc:  # noqa: BLE001
            print(f"    issue click skip: {exc}", flush=True)
            # Bilinen item_key yoksa satırı atla
            continue
        cur = page.url or ""
        if "drilldown" not in cur and "item_key" not in cur:
            print(f"    issue drilldown açılmadı: {cur[:120]}", flush=True)
            continue
        qs = parse_qs(urlparse(cur).query)
        item_key = (qs.get("item_key") or [""])[0]
        dmeta = _extract_page_meta(page)
        title = issues[idx]["title"] if idx < len(issues) else (dmeta.get("title") or "")
        status = issues[idx]["status"] if idx < len(issues) else _status_from_text(dmeta.get("body_head") or "")
        metric = _metric_from_issue(title)
        try:
            urls = _scrape_url_table(page)
        except Exception as exc:  # noqa: BLE001
            print(f"    url table skip: {exc}", flush=True)
            urls = []
        for u in urls:
            u.setdefault("metric", metric)
        drilldowns.append(
            {
                "status": status,
                "title": title,
                "metric": metric,
                "item_key": item_key,
                "source_url": cur,
                "url_rows": urls,
                "url_row_count": len(urls),
                "causes": explain_causes(metric, status, title),
            }
        )
        if idx < len(issues):
            issues[idx]["item_key"] = item_key
            issues[idx]["drilldown_url"] = cur

    # Tıklanamayan / eksik kırılımlar için bilinen item_key fallback
    have_keys = {str(d.get("item_key") or "") for d in drilldowns}
    for key in KNOWN_ITEM_KEYS.get(device) or []:
        if key in have_keys:
            continue
        print(f"    fallback item_key={key}…", flush=True)
        try:
            page.goto(
                _cwv_url(resource_id, "/drilldown", item_key=key),
                wait_until="domcontentloaded",
                timeout=120_000,
            )
            time.sleep(3.5)
            dmeta = _extract_page_meta(page)
            body = dmeta.get("body_head") or ""
            title = ""
            for marker in ("LCP sorunu", "INP sorunu", "CLS sorunu", "LCP issue", "INP issue", "CLS issue"):
                if marker.lower() in body.lower():
                    m = re.search(re.escape(marker) + r"[^\n]{0,80}", body, re.I)
                    title = (m.group(0) if m else marker).strip()
                    break
            status = _status_from_text(body)
            metric = _metric_from_issue(title or body[:80])
            urls = _scrape_url_table(page)
            # Boş / anlamsız fallback satırlarını ekleme (çift kırılım gürültüsü)
            if not urls and status in ("", "unknown", "good"):
                print(f"    fallback skip empty {key}", flush=True)
                continue
            drilldowns.append(
                {
                    "status": status,
                    "title": title or f"{metric} ({key})",
                    "metric": metric,
                    "item_key": key,
                    "source_url": page.url,
                    "url_rows": urls,
                    "url_row_count": len(urls),
                    "causes": explain_causes(metric, status, title),
                    "via": "item_key_fallback",
                }
            )
        except Exception as exc:  # noqa: BLE001
            print(f"    fallback skip {key}: {exc}", flush=True)

    # Good URLs drilldown
    print(f"  · {label} good URLs…", flush=True)
    good_urls: list[dict[str, Any]] = []
    good_meta: dict[str, Any] = {}
    try:
        page.goto(
            _cwv_url(resource_id, "/drilldown", device=device),
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(4)
        _wait_table(page)
        good_urls = _scrape_url_table(page)
        good_meta = _extract_page_meta(page)
    except Exception as exc:  # noqa: BLE001
        print(f"  · good URLs skip: {exc}", flush=True)

    return {
        "device": device,
        "label": label,
        "kpis": kpis,
        "last_updated": "",
        "issues": issues,
        "issue_drilldowns": drilldowns,
        "good_urls": good_urls,
        "good_url_count": len(good_urls),
        "good_page_url": good_meta.get("url") or "",
        "summary_url": _cwv_url(resource_id, "/summary", device=device),
    }


def _extract_amp_issue_title(body: str, h1: str = "") -> str:
    """Drilldown body/h1 içinden gerçek AMP sorun başlığını çıkar (GSC chrome değil)."""
    for cand in ((h1 or "").strip(),):
        if cand and len(cand) <= 160 and "breadcrumb" not in cand.lower() and "search console" not in cand.lower():
            return _clean(cand)
    body_c = _clean(body or "")
    # Bilinen AMP validation başlıkları
    known = [
        (r"Custom JavaScript is not allowed", "Custom JavaScript is not allowed"),
        (r"Görüntü boyutu önerilen boyuttan daha küçük", "Görüntü boyutu önerilen boyuttan daha küçük"),
        (r"Image is smaller than recommended[^.!\n]*", None),
        (r"Disallowed HTML tag[^.!\n]*", None),
        (r"Disallowed attribute[^.!\n]*", None),
    ]
    for pat, fixed in known:
        m = re.search(pat, body_c, re.I)
        if m:
            return fixed or _clean(m.group(0))
    title_m = re.search(
        r"AMP\s+([A-Za-zÇĞİÖŞÜçğıöşü][^|]{6,90}?)(?:\s+(?:İHRACAT|EXPORT|PAYLAŞ|DIŞA|URL|Örnek)|$)",
        body_c,
        re.I,
    )
    if title_m:
        t = _clean(title_m.group(1))
        if t and "breadcrumb" not in t.lower() and "settings" not in t.lower():
            return t
    return "AMP sorunu"


def _scrape_amp(page, *, resource_id: str) -> dict[str, Any]:
    print("  · AMP overview…", flush=True)
    page.goto(_amp_url(resource_id), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    _extract_page_meta(page)
    issues_table = _extract_table(page)
    amp_issues: list[dict[str, Any]] = []
    # Prefer known drilldown + click rows
    overview_issues = []
    for row in issues_table.get("rows") or []:
        if len(row) < 1:
            continue
        title = row[0] if not row[0].startswith("http") else (row[1] if len(row) > 1 else row[0])
        overview_issues.append({"title": _clean(title), "cells": row})

    # Seed with user-provided image-size issue
    seed_keys = ["GgoIIBACIgAiACIA"]
    discovered_keys: list[str] = list(seed_keys)

    # Click issue rows on AMP summary if present
    n = page.locator("table tbody tr").count()
    for idx in range(n):
        page.goto(_amp_url(resource_id), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page, 10000)
        rows = page.locator("table tbody tr")
        if idx >= rows.count():
            break
        try:
            rows.nth(idx).click(timeout=15_000)
            time.sleep(3)
        except Exception as exc:  # noqa: BLE001
            print(f"  · AMP issue click skip: {exc}", flush=True)
            continue
        qs = parse_qs(urlparse(page.url).query)
        key = (qs.get("item_key") or [""])[0]
        if key and key not in discovered_keys:
            discovered_keys.append(key)

    for key in discovered_keys:
        print(f"  · AMP drilldown {key}…", flush=True)
        page.goto(_amp_url(resource_id, "/drilldown", item_key=key), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(4)
        _wait_table(page)
        dmeta = _extract_page_meta(page)
        body = dmeta.get("body_head") or ""
        title = _extract_amp_issue_title(body, str(dmeta.get("title") or ""))
        status = "needs_improvement"
        if "error" in body.lower() or "kritik" in body.lower() or "not allowed" in body.lower():
            status = "poor"
        urls = _scrape_url_table(page)
        metric = _metric_from_issue(title)
        amp_issues.append(
            {
                "status": status,
                "title": title,
                "metric": metric,
                "item_key": key,
                "source_url": page.url,
                "url_rows": urls,
                "url_row_count": len(urls),
                "causes": explain_causes(metric, status, title),
            }
        )

    total_amp = sum(int(i.get("url_row_count") or 0) for i in amp_issues)
    return {
        "overview_url": _amp_url(resource_id),
        "issues": amp_issues,
        "url_row_count": total_amp,
    }


def scrape_property(page, prop: dict[str, str], *, charts_only: bool = False) -> dict[str, Any]:
    rid = prop["resource_id"]
    print(f"CWV scrape · {prop.get('label') or rid}", flush=True)
    page.goto(_cwv_url(rid), wait_until="domcontentloaded", timeout=120_000)
    # İlk yüklemede GSC shell geç gelebilir — flaky “oturum yok” önlemi
    signed = False
    for _ in range(8):
        time.sleep(2)
        if _looks_signed_in(page):
            signed = True
            break
    if not signed:
        raise RuntimeError("GSC oturumu yok — scripts/gsc_cwv_scrape.py --login")
    meta = _extract_page_meta(page)
    body = page.inner_text("body")
    overview = _parse_overview_counts(body)
    last_upd = ""
    m = re.search(
        r"(?:Son güncelleme(?: tarihi)?|Last update(?:d)?(?: date)?)\s*:?\s*([0-9\./]+)",
        body,
        re.I,
    )
    if m:
        last_upd = m.group(1)

    print("  · overview chart series…", flush=True)
    chart_series: dict[str, Any] = {"mobile": None, "desktop": None}
    try:
        try:
            page.evaluate("window.scrollTo(0, 360)")
        except Exception:
            pass
        time.sleep(1.2)
        chart_series = _extract_overview_chart_series(page, last_updated=last_upd)
        _snap_series_to_kpis(chart_series, overview)
        mob_n = int(((chart_series.get("mobile") or {}).get("point_count")) or 0)
        desk_n = int(((chart_series.get("desktop") or {}).get("point_count")) or 0)
        last_d = ""
        ser_m = chart_series.get("mobile") if isinstance(chart_series.get("mobile"), dict) else {}
        if ser_m and ser_m.get("dates"):
            last_d = str(ser_m["dates"][-1])
        print(
            f"    charts mobile={mob_n} desktop={desk_n} pts last={last_d or '—'} "
            f"src={chart_series.get('source')}",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  · chart series skip: {exc}", flush=True)
        chart_series = {"mobile": None, "desktop": None, "error": str(exc)[:200]}

    if charts_only:
        # KPI’ları overview + chart son noktasından doldur
        mobile_k = dict(overview.get("mobile") or {})
        desktop_k = dict(overview.get("desktop") or {})
        for key, bucket in (("mobile", mobile_k), ("desktop", desktop_k)):
            ser = chart_series.get(key) or {}
            if not isinstance(ser, dict):
                continue
            for metric in ("poor", "needs_improvement", "good"):
                arr = ser.get(metric) or []
                if arr and not bucket.get(metric):
                    bucket[metric] = int(round(float(arr[-1] or 0)))
        poor = int(mobile_k.get("poor") or 0) + int(desktop_k.get("poor") or 0)
        ni = int(mobile_k.get("needs_improvement") or 0) + int(desktop_k.get("needs_improvement") or 0)
        good = int(mobile_k.get("good") or 0) + int(desktop_k.get("good") or 0)
        return {
            "site_key": prop.get("site_key") or "",
            "site_domain": prop.get("site_domain") or "",
            "resource_id": rid,
            "label": prop.get("label") or rid,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "last_updated": last_upd,
            "overview": overview,
            "chart_series": chart_series,
            "mobile": {"kpis": mobile_k, "last_updated": last_upd, "issues": [], "issue_drilldowns": [], "good_urls": []},
            "desktop": {"kpis": desktop_k, "last_updated": last_upd, "issues": [], "issue_drilldowns": [], "good_urls": []},
            "amp": {"issues": [], "url_row_count": 0, "skipped": True},
            "totals": {"poor": poor, "needs_improvement": ni, "good": good},
            "source": "gsc_cwv_scrape",
            "charts_only": True,
        }

    mobile = _scrape_device(page, resource_id=rid, device=DEVICE_MOBILE, label="Mobil")
    desktop = _scrape_device(page, resource_id=rid, device=DEVICE_DESKTOP, label="Masaüstü")
    # Prefer overview KPIs when summary parse weak
    if overview["mobile"]["good"] or overview["mobile"]["needs_improvement"]:
        mobile["kpis"] = overview["mobile"]
    if overview["desktop"]["good"] or overview["desktop"]["poor"] or overview["desktop"]["needs_improvement"]:
        desktop["kpis"] = overview["desktop"]
    mobile["last_updated"] = last_upd
    desktop["last_updated"] = last_upd

    try:
        amp = _scrape_amp(page, resource_id=rid)
    except Exception as exc:  # noqa: BLE001
        print(f"  · AMP skip: {exc}", flush=True)
        amp = {"issues": [], "url_row_count": 0, "error": str(exc)[:200]}

    poor = int(mobile["kpis"].get("poor") or 0) + int(desktop["kpis"].get("poor") or 0)
    ni = int(mobile["kpis"].get("needs_improvement") or 0) + int(desktop["kpis"].get("needs_improvement") or 0)
    good = int(mobile["kpis"].get("good") or 0) + int(desktop["kpis"].get("good") or 0)

    return {
        "site_key": prop.get("site_key") or "",
        "site_domain": prop.get("site_domain") or "",
        "resource_id": rid,
        "label": prop.get("label") or rid,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": last_upd,
        "overview": overview,
        "chart_series": chart_series,
        "mobile": mobile,
        "desktop": desktop,
        "amp": amp,
        "totals": {"poor": poor, "needs_improvement": ni, "good": good},
        "source": "gsc_cwv_scrape",
    }


def _post_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        raise RuntimeError("NOTIFICATION_INGEST_TOKEN gerekli")
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Notification-Ingest-Token": token,
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {exc.code}: {err}") from exc


def run_sync(
    *,
    site_filter: str = "",
    ingest: bool = True,
    headed: bool | None = None,
    charts_only: bool = False,
) -> dict[str, Any]:
    if headed is None:
        env_h = (os.environ.get("GSC_CWV_HEADLESS") or os.environ.get("GSC_LINKS_HEADLESS") or "").strip().lower()
        # Google oturumu için varsayılan headed
        headed = env_h not in ("1", "true", "yes")
    props = PROPERTIES
    if site_filter:
        sk = site_filter.strip().lower()
        props = [p for p in PROPERTIES if p["site_key"] == sk or sk in (p["site_domain"] or "")]
    if not props:
        return {"ok": False, "message": f"site bulunamadı: {site_filter}"}

    pw, context = _launch_context(headed=headed)
    snapshots: list[dict[str, Any]] = []
    try:
        page = context.pages[0] if context.pages else context.new_page()
        for prop in props:
            try:
                snap = scrape_property(page, prop, charts_only=charts_only)
                snapshots.append(snap)
                print(
                    f"OK {prop['label']} · poor={snap['totals']['poor']} "
                    f"ni={snap['totals']['needs_improvement']} good={snap['totals']['good']} "
                    f"amp_rows={snap['amp'].get('url_row_count')}"
                    + (" · charts_only" if charts_only else ""),
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"FAIL {prop.get('label')}: {exc}", flush=True)
                snapshots.append(
                    {
                        "site_domain": prop.get("site_domain"),
                        "resource_id": prop.get("resource_id"),
                        "ok": False,
                        "error": str(exc)[:300],
                        "source": "gsc_cwv_scrape",
                    }
                )
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass

    ok_snaps = [s for s in snapshots if isinstance(s, dict) and not s.get("error")]
    payload = {
        "source": "gsc_cwv_scrape",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "snapshots": snapshots,
    }
    result: dict[str, Any] = {
        "ok": bool(ok_snaps),
        "snapshots": len(snapshots),
        "ok_snapshots": len(ok_snaps),
        "message": f"{len(ok_snaps)}/{len(snapshots)} property OK",
    }
    if ingest and ok_snaps:
        payload["snapshots"] = ok_snaps
        ing = _post_ingest(payload)
        result["ingest"] = ing
        result["ok"] = bool(ing.get("ok", True))
        result["message"] = ing.get("message") or result["message"]
        print(f"ingest · {result['message']}", flush=True)
    elif ingest and not ok_snaps:
        result["ok"] = False
        result["message"] = "Hiç başarılı CWV snapshot yok"
    else:
        out = ROOT / "scratch" / "gsc_cwv_last.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        result["saved"] = str(out)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GSC CWV + AMP scrape")
    parser.add_argument("--login", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--site", default="", help="doviz | sinemalar")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--charts-only",
        action="store_true",
        help="Sadece overview KPI + GSC trend grafikleri (hızlı)",
    )
    args = parser.parse_args(argv)
    if args.login:
        out = run_login_interactive()
        print(json.dumps(out, ensure_ascii=False), flush=True)
        return 0 if out.get("ok") else 1
    if not args.sync and not args.ingest:
        parser.print_help()
        return 2
    headed = True if args.headed else (False if args.headless else None)
    out = run_sync(
        site_filter=args.site,
        ingest=bool(args.ingest or args.sync),
        headed=headed,
        charts_only=bool(args.charts_only),
    )
    print(json.dumps({k: v for k, v in out.items() if k != "snapshots"}, ensure_ascii=False), flush=True)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
