#!/usr/bin/env python3
"""Google Search Console Core Web Vitals + AMP scrape (Mac bridge).

Playwright Firefox (fx-google profili) ile GSC CWV / AMP raporlarını çeker → Railway ingest.
Satır limiti yok — tablo sonuna kadar kaydırılır.

  .venv/bin/python scripts/gsc_cwv_scrape.py --login
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest --site doviz

Not: --login aynı fx-google profilini kullanan eski tarayıcı süreçlerini kapatır.

Env:
  GSC_CWV_PROFILE_DIR / GSC_LINKS_PROFILE_DIR / PLAY_CONSOLE_PROFILE_DIR
  GSC_CWV_INGEST_URL
  GSC_CWV_LOGIN_WAIT_SEC  (headed sync'te giriş bekleme; varsayılan 900)
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

from backend.services.scrape_browser import google_profile_dir

PROFILE_DIR = google_profile_dir()

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


def _login_wait_sec() -> int:
    from backend.services.scrape_browser import login_wait_sec

    return login_wait_sec(env_key="GSC_CWV_LOGIN_WAIT_SEC")


def _wait_until_signed_in(page, *, timeout_sec: int | None = None) -> bool:
    """Headed sync: tarayıcıyı kapatma — kullanıcı şifre/2FA bitirene kadar bekle.

    True → aynı page ile kazıma devam eder.
    """
    from backend.services.scrape_browser import LOGIN_WAIT_SEC

    timeout_sec = _login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    print(
        "LOGIN BEKLENIYOR — açık tarayıcıda Google / GSC girişi yapın (şifre/2FA).\n"
        f"Giriş tamamlanınca aynı pencerede tüm siteler taranır (en fazla {timeout_sec // 60} dk).\n"
        "Pencereyi kapatmayın.",
        flush=True,
    )
    deadline = time.time() + timeout_sec
    last_status = 0.0
    ok_streak = 0
    while time.time() < deadline:
        try:
            ctx = page.context
            if not ctx.pages:
                print("LOGIN FAIL — tarayıcı kapandı (pencereyi kapatmayın)", flush=True)
                return False
            page = ctx.pages[0]
            cur = (page.url or "").lower()
            now = time.time()
            if now - last_status >= 12:
                left = max(0, int(deadline - now))
                print(
                    f"  · login bekleniyor · kalan≈{left}s · url={(page.url or '')[:120]}",
                    flush=True,
                )
                last_status = now
            if "accounts.google.com" in cur or "signin" in cur or "challenge" in cur:
                ok_streak = 0
                time.sleep(2)
                continue
            if _looks_signed_in(page):
                ok_streak += 1
                if ok_streak >= 2:
                    time.sleep(2)
                    print(f"LOGIN OK — tarama devam ediyor · {page.url}", flush=True)
                    return True
            else:
                ok_streak = 0
        except Exception as exc:
            msg = str(exc).lower()
            if "has been closed" in msg or "target closed" in msg or "crashed" in msg:
                print(f"LOGIN FAIL — tarayıcı kapandı: {exc}", flush=True)
                return False
            ok_streak = 0
        time.sleep(2)
    print("LOGIN FAIL — zaman aşımı (şifre/2FA bitmeden süre doldu)", flush=True)
    return False


def _ensure_signed_in(page, *, headed: bool) -> None:
    """Kısa kontrol; yoksa headed ise kullanıcıyı bekle, sonra devam."""
    if _looks_signed_in(page):
        return
    for _ in range(5):
        time.sleep(1.5)
        if _looks_signed_in(page):
            return
    if not headed:
        raise RuntimeError("GSC oturumu yok — headed sync veya --login gerekli")
    if not _wait_until_signed_in(page):
        raise RuntimeError("GSC oturumu yok — Mac köprüde oturum açın")


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
    from backend.services.scrape_browser import acquire_persistent_context

    pw, context, _reused = acquire_persistent_context(
        "gsc-cwv",
        profile=PROFILE_DIR,
        headed=headed,
        env_key="GSC_CWV_KEEP_OPEN",
        label="GSC CWV",
        locale="en-US"
    )
    return pw, context


def _release_context(pw, context, *, headed: bool = True) -> None:
    from backend.services.scrape_browser import release_persistent_context

    release_persistent_context(
        "gsc-cwv",
        pw,
        context,
        headed=headed,
        env_key="GSC_CWV_KEEP_OPEN",
        label="GSC CWV",
        profile=PROFILE_DIR,
    )


def run_login_interactive(timeout_sec: int | None = None) -> dict[str, Any]:
    """Headed login — şifre/2FA sırasında tarayıcıyı kapatma; profil kilidini önce temizle."""
    from backend.services.scrape_browser import LOGIN_WAIT_SEC, login_wait_sec

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    url = _cwv_url("sc-domain:doviz.com")
    print(f"Profil: {PROFILE_DIR}", flush=True)
    print(
        "Not: Aynı profilde başka Firefox açıksa kapatılır.",
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
            f"Search Console açılınca oturum otomatik kaydedilir (en fazla {timeout_sec // 60} dk).\n"
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
                            "Mac köprüde GSC oturumunu yenileyin"
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
        _release_context(pw, context, headed=True)
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
      return { title, body_head: body.slice(0, 8000), url: location.href };
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
    # EN legend: "0 poor URLs · 14,378 URLs need improvement · 3,457 good URLs"
    # (ÖNCE bunu dene — aşağıdaki label→sayı kalıbı "poor URLs\\n14,378"ı poor sanıyor)
    re.compile(
        r"([\d,]+)\s*poor\s*urls?.{0,120}?"
        r"([\d,]+)\s*urls?\s*need(?:s)?\s*improv\w*.{0,120}?"
        r"([\d,]+)\s*good\s*urls?",
        re.I | re.S,
    ),
    # folded TR: 0 yetersiz URL · 14.674 URL iyilestirme gerektiriyor · 3.036 iyi URL
    re.compile(
        r"(\d[\d\.,]*)\s*(?:yetersiz|kotu)\s*urls?.{0,120}?"
        r"([\d\.,]+)\s*urls?.{0,40}?iyilestir.{0,120}?"
        r"([\d\.,]+)\s*iyi\s*urls?",
        re.I | re.S,
    ),
    # EN card headers: Poor 0 · Need improvement 14,674 · Good 3,036
    re.compile(
        r"Poor\s*([\d\.,]+K?).{0,80}?Need(?:s)?\s*improvement\s*([\d\.,]+K?).{0,80}?Good\s*([\d\.,]+K?)",
        re.I | re.S,
    ),
    # Summary kart: etiket sonra sayı ("Yetersiz" / "0"). "poor urls 14k" formunu YAKALAMA.
    re.compile(
        r"(?:yetersiz|kotu|(?<![\d.,])poor)(?!\s*urls?\b)\D{0,40}?"
        r"([\d.,]+\s*[BK]?|\d+)\D{0,100}?"
        r"(?:iyilestir|need(?:s)?\s*improvement)(?!\s*urls?\b)\D{0,40}?"
        r"([\d.,]+\s*[BK]?)\D{0,100}?"
        r"(?:iyi(?!lestir)|(?<![\w])good)(?!\s*urls?\b)\D{0,40}?"
        r"([\d.,]+\s*[BK]?)",
        re.I | re.S,
    ),
)


def _kpi_near_label(text: str, labels: tuple[str, ...]) -> int | None:
    """Etiket üstte/altta, sayı '15,6 B' veya 15.557 olabilir. Metin folded TR.

    Overview legend önce sayı yazar («0 poor URLs») — onu tercih et.
    Aksi halde etiket→sayı (summary kart) dene; «poor URLs» sonrası sonraki metriği alma.
    """
    lab = "|".join(labels)
    t = text or ""
    m = re.search(rf"([\d.,]+\s*[BK]?)\s*(?:{lab})", t, re.I)
    if m:
        return _parse_count(m.group(1))
    m = re.search(
        rf"(?:{lab})(?!\s*urls?\b)[^\d]{{0,40}}([\d.,]+\s*[BK]?)",
        t,
        re.I | re.S,
    )
    if m:
        return _parse_count(m.group(1))
    return None


def _parse_gsc_kpi_triplet(block: str) -> dict[str, int] | None:
    """GSC özet kartları — Mobil rapordaki 0 / 15,6 B / 3,12 B."""
    raw = block or ""
    if not raw.strip():
        return None
    t = _fold_tr(raw)
    parsed = _parse_overview_triplet(t)
    if parsed and (parsed.get("needs_improvement") or parsed.get("good")):
        return parsed
    ni = _kpi_near_label(t, (r"iyilestir", r"need improvement", r"needs improvement"))
    poor = _kpi_near_label(t, (r"yetersiz", r"kotu", r"\bpoor\b"))
    good = _kpi_near_label(t, (r"iyi\s*url", r"\bgood\s*url", r"iyi(?!lestir)\s", r"\bgood\b"))
    if ni is None and good is None and poor is None:
        return None
    out = {
        "poor": int(poor or 0),
        "needs_improvement": int(ni or 0),
        "good": int(good or 0),
    }
    if out["needs_improvement"] or out["good"] or out["poor"]:
        return out
    return None


def _parse_drilldown_affected_count(body: str) -> int | None:
    """Drilldown sayfası — «Etkilenen URL sayısı» / affected URLs grafiği üst sayı."""
    raw = body or ""
    if not raw.strip():
        return None
    t = _fold_tr(raw)
    patterns = (
        r"etkilenen\s*url\s*sayisi\D{0,48}(\d[\d\.,]*)",
        r"number\s*of\s*affected\s*urls?\D{0,48}(\d[\d\.,]*)",
        r"(\d[\d\.,]+)\D{0,24}(?:etkilenen|affected)\s*url",
    )
    for pat in patterns:
        m = re.search(pat, t, re.I | re.S)
        if m:
            return _parse_count(m.group(1))
    return None


def _discover_item_keys_from_page(page) -> list[str]:
    """Özet tablodaki tüm drilldown linklerinden item_key topla."""
    try:
        raw = page.evaluate(
            """() => {
          const out = new Set();
          const grab = (href) => {
            if (!href || !String(href).includes('item_key=')) return;
            try {
              const u = new URL(href, location.href);
              const k = u.searchParams.get('item_key');
              if (k && k.length >= 4) out.add(k);
            } catch (_) {}
          };
          for (const a of document.querySelectorAll('a[href*="item_key="]')) {
            grab(a.getAttribute('href'));
          }
          for (const tr of document.querySelectorAll('table tbody tr')) {
            for (const a of tr.querySelectorAll('a[href]')) grab(a.getAttribute('href'));
          }
          return [...out];
        }"""
        )
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for k in raw or []:
        key = str(k or "").strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _parse_drilldown_chart_tooltip(
    text: str,
    *,
    prefer_after: str | None = None,
    default_year: int | None = None,
) -> dict[str, Any] | None:
    """Drilldown grafiği tooltip — tek etkilenen URL sayısı + tarih."""
    # _clean önce PUA aralığında \n'yi siliyor → "2026"+"110" birleşir; satır sonunu boşluğa çevir.
    raw = re.sub(r"[\u0000-\u001f]+", " ", text or "")
    raw = _PUA_RE.sub("", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    if not raw or len(raw) < 4:
        return None
    t = _fold_tr(raw)
    year_hint = default_year
    if prefer_after and re.match(r"^\d{4}-\d{2}-\d{2}$", str(prefer_after)[:10]):
        try:
            year_hint = int(str(prefer_after)[:4])
        except ValueError:
            year_hint = default_year
    date = ""
    for line in raw.splitlines():
        iso = _gsc_label_to_iso(line, default_year=year_hint)
        if iso:
            date = iso
            break
    if not date:
        m = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})", raw)
        if m:
            date = _gsc_label_to_iso(m.group(1), default_year=year_hint)
    if not date:
        m = re.search(
            r"(\d{1,2}\s+[A-Za-zçğıöşüÇĞİÖŞÜ]{3,}(?:\s+\d{2,4})?|[A-Za-z]{3,}\s+\d{1,2}(?:,?\s+\d{2,4})?)",
            raw,
        )
        if m:
            date = _gsc_label_to_iso(m.group(0), default_year=year_hint)
    if prefer_after and date and re.match(r"^\d{4}-\d{2}-\d{2}$", str(prefer_after)[:10]):
        try:
            d = datetime.strptime(date[:10], "%Y-%m-%d")
            prev = datetime.strptime(str(prefer_after)[:10], "%Y-%m-%d")
            if d < prev - timedelta(days=14):
                bumped = d.replace(year=d.year + 1)
                if bumped >= prev - timedelta(days=2):
                    date = bumped.strftime("%Y-%m-%d")
        except ValueError:
            pass

    num = r"([\d\.,]+\s*[BK]?)"
    count = 0
    for pat in (
        rf"(?:etkilenen|affected)[^\d]{{0,48}}{num}",
        rf"{num}[^\d]{{0,28}}(?:etkilenen|affected)",
        rf"(?:url\s*sayisi|urls?)[^\d]{{0,24}}{num}",
        rf"(?:yetersiz|kotu|poor|iyilestir|good|iyi)[^\d]{{0,28}}{num}",
    ):
        m = re.search(pat, t, re.I)
        if m:
            count = _parse_count(m.group(1))
            if count:
                break
    if not count:
        vals = [_parse_count(x) for x in re.findall(r"[\d\.,]+\s*[BK]?", raw)]
        vals = [v for v in vals if v >= 1 and not (2000 <= v <= 2100)]
        if vals:
            count = max(vals)
    if not date and not count:
        return None
    return {"date": date or "", "count": int(count or 0)}


_DRILLDOWN_TIP_TEXT_JS = r"""() => {
  const hit = (el) => {
    const t = (el.innerText || el.textContent || '').trim();
    if (t.length < 5 || t.length > 480) return false;
    if (!/\d/.test(t)) return false;
    return /affected|etkilenen|url|poor|good|yetersiz|kotu|iyilestir|improvement|iyi/i.test(t);
  };
  const nodes = [...document.querySelectorAll('div, span, li, p')].filter((el) => {
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none' || Number(cs.opacity) === 0) return false;
    const r = el.getBoundingClientRect();
    if (r.width < 60 || r.height < 24 || r.width > 520) return false;
    return hit(el);
  });
  nodes.sort((a, b) => (a.innerText || '').length - (b.innerText || '').length);
  return nodes.length ? (nodes[0].innerText || '') : '';
}"""


def _chart_plot_box(page, chart_idx: int = 0) -> dict[str, Any] | None:
    try:
        box = page.evaluate(
            """(i) => {
              const svgs = [...document.querySelectorAll('svg')].filter(svg => {
                const bb = svg.getBoundingClientRect();
                return bb.width >= 280 && bb.height >= 100;
              });
              const svg = svgs[i];
              if (!svg) return null;
              svg.scrollIntoView({block: 'center', inline: 'nearest'});
              const bb = svg.getBoundingClientRect();
              let plot = null;
              const clips = [...svg.querySelectorAll('clipPath rect, rect')];
              for (const r of clips) {
                const rb = r.getBoundingClientRect();
                if (rb.width >= bb.width * 0.55 && rb.height >= bb.height * 0.45) {
                  if (!plot || rb.width * rb.height > plot.w * plot.h) {
                    plot = {x: rb.x, y: rb.y, w: rb.width, h: rb.height};
                  }
                }
              }
              const padL = 48, padR = 12, padT = 16, padB = 28;
              if (!plot) {
                plot = {
                  x: bb.x + padL,
                  y: bb.y + padT,
                  w: Math.max(40, bb.width - padL - padR),
                  h: Math.max(40, bb.height - padT - padB),
                };
              }
              return { x: plot.x, y: plot.y, width: plot.w, height: plot.h };
            }""",
            chart_idx,
        )
    except Exception:
        return None
    return box if isinstance(box, dict) else None


def _harvest_drilldown_affected_series(page, *, end_iso: str = "") -> dict[str, Any] | None:
    """Drilldown sayfası grafiğinde sütun sütun tooltip tara — ana sayfa ile aynı mantık."""
    box = _chart_plot_box(page, 0)
    if not box:
        return None
    w = float(box.get("width") or 0)
    h = float(box.get("height") or 0)
    if w < 160 or h < 60:
        return None
    time.sleep(0.2)
    samples: dict[str, int] = {}
    n = max(240, min(int(round(w)) + 1, 900))
    y = float(box["y"]) + h * 0.62
    x0 = float(box["x"]) + 2.0
    x1 = float(box["x"]) + w - 2.0
    last_date = ""
    same_streak = 0
    target_end = str(end_iso or "")[:10]
    print(f"    drilldown tooltip sweep: {n} steps · plot {w:.0f}×{h:.0f}px", flush=True)
    i = 0
    while i < n:
        t = i / max(n - 1, 1)
        x = x0 + (x1 - x0) * t
        try:
            page.mouse.move(x, y)
        except Exception:
            i += 1
            continue
        time.sleep(0.028)
        try:
            text = page.evaluate(_DRILLDOWN_TIP_TEXT_JS)
        except Exception:
            text = ""
        parsed = _parse_drilldown_chart_tooltip(
            str(text or ""),
            prefer_after=last_date or (sorted(samples)[-1] if samples else None),
        )
        if parsed and (parsed.get("date") or parsed.get("count")):
            d = str(parsed.get("date") or last_date or "")[:10]
            cnt = int(parsed.get("count") or 0)
            if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d):
                samples[d] = cnt
                if d == last_date:
                    same_streak += 1
                    if same_streak >= 4:
                        i += max(2, n // 120)
                        same_streak = 0
                        continue
                else:
                    same_streak = 0
                    last_date = d
            elif cnt and last_date:
                samples[last_date] = cnt
        i += 1

    if len(samples) < 4:
        samples = {}
        y2 = float(box["y"]) + h * 0.48
        print("    drilldown tooltip sweep: retry y-band", flush=True)
        last_date = ""
        for i in range(n):
            t = i / max(n - 1, 1)
            x = x0 + (x1 - x0) * t
            try:
                page.mouse.move(x, y2)
            except Exception:
                continue
            time.sleep(0.028)
            try:
                text = page.evaluate(_DRILLDOWN_TIP_TEXT_JS)
            except Exception:
                text = ""
            parsed = _parse_drilldown_chart_tooltip(
                str(text or ""),
                prefer_after=last_date or (sorted(samples)[-1] if samples else None),
            )
            if parsed:
                d = str(parsed.get("date") or "")[:10]
                cnt = int(parsed.get("count") or 0)
                if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d):
                    samples[d] = cnt
                    last_date = d
                elif cnt and last_date:
                    samples[last_date] = cnt

    if len(samples) < 3:
        return None
    dates = sorted(samples)
    counts = [int(samples[d]) for d in dates]
    tip_last = dates[-1]
    if target_end and tip_last < target_end:
        dense_n = max(36, int(w * 0.12))
        x_dense0 = x0 + (x1 - x0) * 0.88
        prefer = tip_last
        for i in range(dense_n):
            t = i / max(dense_n - 1, 1)
            x = x_dense0 + (x1 - x_dense0) * t
            try:
                page.mouse.move(x, y)
            except Exception:
                continue
            time.sleep(0.032)
            try:
                text = page.evaluate(_DRILLDOWN_TIP_TEXT_JS)
            except Exception:
                text = ""
            parsed = _parse_drilldown_chart_tooltip(str(text or ""), prefer_after=prefer)
            if parsed and parsed.get("date"):
                d = str(parsed["date"])[:10]
                if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
                    samples[d] = int(parsed.get("count") or samples.get(d) or 0)
                    prefer = d
        dates = sorted(samples)
        counts = [int(samples[d]) for d in dates]

    print(
        f"    drilldown tooltip: {len(dates)} pts {dates[0]} → {dates[-1]} "
        f"last={counts[-1] if counts else '—'}",
        flush=True,
    )
    return {
        "dates": dates,
        "counts": counts,
        "affected_url_count": counts[-1] if counts else None,
        "source": "drilldown_tooltip",
        "tooltip_points": len(dates),
    }


def _kpis_from_issue_drilldowns(
    drilldowns: list[dict[str, Any]],
    *,
    good_affected: int | None = None,
) -> dict[str, int]:
    """Özet kart yerine kırılım (drilldown) sayfalarından KPI türet."""
    out = {"poor": 0, "needs_improvement": 0, "good": 0}
    for d in drilldowns:
        st = str(d.get("status") or "").lower()
        if st not in out:
            continue
        cnt = d.get("affected_url_count")
        if cnt is None:
            cnt = d.get("url_count")
        if cnt is None:
            continue
        out[st] += int(cnt or 0)
    if good_affected is not None:
        out["good"] = int(good_affected)
    return out


def _enrich_drilldown_from_page(page, drill: dict[str, Any], *, end_iso: str = "") -> None:
    """Drilldown — önce grafik tooltip sütun taraması, sonra gövde metni yedek."""
    series = _harvest_drilldown_affected_series(page, end_iso=end_iso)
    if series and series.get("dates"):
        drill["affected_chart_series"] = {
            "dates": series["dates"],
            "counts": series["counts"],
            "source": series.get("source"),
            "tooltip_points": series.get("tooltip_points"),
        }
        if series.get("affected_url_count") is not None:
            drill["affected_url_count"] = int(series["affected_url_count"])
            drill["affected_count_source"] = "drilldown_tooltip"

    body = ""
    if drill.get("affected_url_count") is None:
        try:
            body = page.inner_text("body") or ""
        except Exception:
            body = str((drill.get("body_head") or ""))
        affected = _parse_drilldown_affected_count(body)
        if affected is not None:
            drill["affected_url_count"] = affected
            drill["affected_count_source"] = "body_text"
    drill["drilldown_url"] = drill.get("source_url") or drill.get("drilldown_url") or ""


def _issue_title_from_drilldown_body(body: str) -> str:
    for marker in (
        "LCP sorunu",
        "INP sorunu",
        "CLS sorunu",
        "LCP issue",
        "INP issue",
        "CLS issue",
    ):
        if marker.lower() in (body or "").lower():
            m = re.search(re.escape(marker) + r"[^\n]{0,80}", body, re.I)
            return (m.group(0) if m else marker).strip()
    return ""


def _scrape_cwv_issue_drilldown(
    page,
    *,
    resource_id: str,
    device: int,
    item_key: str,
    issue_hint: dict[str, Any] | None = None,
    end_iso: str = "",
) -> dict[str, Any]:
    """Tek CWV drilldown URL — grafik tooltip + URL tablosu."""
    url = _cwv_url(resource_id, "/drilldown", item_key=item_key, device=device)
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    time.sleep(3.5)
    _wait_table(page)
    dmeta = _extract_page_meta(page)
    body = str(dmeta.get("body_head") or "")
    hint = issue_hint if isinstance(issue_hint, dict) else {}
    title = str(hint.get("title") or "").strip() or _issue_title_from_drilldown_body(body)
    if not title:
        title = str(dmeta.get("title") or "").strip()
    status = str(hint.get("status") or "") or _status_from_text(body)
    metric = str(hint.get("metric") or "") or _metric_from_issue(title or body[:120])
    try:
        urls = _scrape_url_table(page)
    except Exception as exc:  # noqa: BLE001
        print(f"    url table skip ({item_key}): {exc}", flush=True)
        urls = []
    for u in urls:
        u.setdefault("metric", metric)
    dd: dict[str, Any] = {
        "status": status,
        "title": title or f"{metric} ({item_key})",
        "metric": metric,
        "item_key": item_key,
        "source_url": page.url or url,
        "url_rows": urls,
        "url_row_count": len(urls),
        "causes": explain_causes(metric, status, title),
    }
    _enrich_drilldown_from_page(page, dd, end_iso=end_iso)
    return dd


def _scrape_amp_issue_drilldown(
    page,
    *,
    resource_id: str,
    item_key: str,
    issue_hint: dict[str, Any] | None = None,
    end_iso: str = "",
) -> dict[str, Any]:
    """Tek AMP drilldown URL — grafik tooltip + URL tablosu."""
    url = _amp_url(resource_id, "/drilldown", item_key=item_key)
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    _wait_table(page)
    dmeta = _extract_page_meta(page)
    body = str(dmeta.get("body_head") or "")
    hint = issue_hint if isinstance(issue_hint, dict) else {}
    title = str(hint.get("title") or "").strip() or _extract_amp_issue_title(
        body, str(dmeta.get("title") or "")
    )
    status = str(hint.get("status") or "") or "needs_improvement"
    if "error" in body.lower() or "kritik" in body.lower() or "not allowed" in body.lower():
        status = "poor"
    metric = str(hint.get("metric") or "") or _metric_from_issue(title)
    urls = _scrape_url_table(page)
    dd: dict[str, Any] = {
        "status": status,
        "title": title,
        "metric": metric,
        "item_key": item_key,
        "source_url": page.url or url,
        "url_rows": urls,
        "url_row_count": len(urls),
        "causes": explain_causes(metric, status, title),
    }
    _enrich_drilldown_from_page(page, dd, end_iso=end_iso)
    return dd


def _parse_overview_triplet(block: str) -> dict[str, int] | None:
    t = _fold_tr(block or "")
    for cre in _OV_TRIPLET_RES:
        m = cre.search(t)
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
        parsed = _parse_gsc_kpi_triplet(part[:2500])
        if not parsed:
            continue
        if head.startswith("mobil") or head.startswith("mobile"):
            out["mobile"] = parsed
        elif head.startswith("masaüstü") or head.startswith("masaustu") or head.startswith("desktop"):
            out["desktop"] = parsed
    if out["mobile"]["good"] or out["mobile"]["needs_improvement"]:
        return out
    parsed = _parse_gsc_kpi_triplet(text)
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


def _coerce_cwv_year(y: int, year_now: int) -> int | None:
    """2 haneli yıl 07 → 2007 GSC CWV için saçma; yalnızca yakın yıllar."""
    if y < 0:
        return None
    if y < 100:
        for cand in (year_now, year_now - 1, year_now + 1):
            if cand % 100 == y:
                return cand
        y = 2000 + y
    if year_now - 3 <= y <= year_now + 1:
        return y
    return None


def _gsc_label_to_iso(label: str, *, default_year: int | None = None) -> str:
    """GSC etiket: M/D/YY, D.M.YYYY, '9 Ağu', 'Aug 9, 2026' → YYYY-MM-DD."""
    s = re.sub(r"\s+", " ", (label or "").strip()).strip(" .,")
    if not s:
        return ""
    year_now = default_year or datetime.now(timezone.utc).year
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = _coerce_cwv_year(y, year_now)
        if year is None:
            return ""
        month, day = (b, a) if a > 12 else (a, b)  # hl=en → M/D
        try:
            datetime(year, month, day)
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError:
            return ""
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$", s)
    if m:
        day, month, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = _coerce_cwv_year(y, year_now)
        if year is None:
            return ""
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
            year = _coerce_cwv_year(year, year_now)
            if year is None:
                return ""
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
    year = _coerce_cwv_year(year, year_now)
    if year is None:
        return ""
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
    if n > 400:
        # İlk 401 günü (2007…) değil, t1'e biten son 401 günü tut.
        t0 = datetime.combine(t1.date() - timedelta(days=400), datetime.min.time())
        n = 400
    n = max(0, n)
    return [(t0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n + 1)]


def _fold_tr(s: str) -> str:
    table = str.maketrans(
        {
            "İ": "i",
            "I": "i",
            "ı": "i",
            "Ş": "s",
            "ş": "s",
            "Ğ": "g",
            "ğ": "g",
            "Ü": "u",
            "ü": "u",
            "Ö": "o",
            "ö": "o",
            "Ç": "c",
            "ç": "c",
        }
    )
    return (s or "").translate(table).lower()


def _series_from_tooltip_samples(
    samples: dict[str, dict[str, Any]],
    *,
    start_iso: str = "",
    end_iso: str = "",
) -> dict[str, Any] | None:
    """GSC tooltip noktaları → günlük seri. Eksik günler komşu gerçek değerler arasında doğrusal."""
    clean: dict[str, dict[str, int]] = {}
    for raw_d, row in (samples or {}).items():
        d = str(raw_d or "")[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", d) or not isinstance(row, dict):
            continue
        clean[d] = {
            "poor": int(row.get("poor") or 0),
            "needs_improvement": int(row.get("needs_improvement") or 0),
            "good": int(row.get("good") or 0),
        }
    if len(clean) < 5:
        return None
    tip_dates = sorted(clean)
    t0 = tip_dates[0]
    t1 = tip_dates[-1]

    def _near(iso: str, anchor: str, *, max_days: int = 21) -> bool:
        if not iso or not re.match(r"^\d{4}-\d{2}-\d{2}$", iso[:10]):
            return False
        try:
            a = datetime.strptime(iso[:10], "%Y-%m-%d")
            b = datetime.strptime(anchor[:10], "%Y-%m-%d")
        except ValueError:
            return False
        return abs((a - b).days) <= max_days

    # SVG ekseni 2007 gibi sapıksa yok say — tooltip tarihleri asıl kaynak.
    if start_iso and _near(start_iso, tip_dates[0]) and start_iso[:10] < t0:
        t0 = start_iso[:10]
    if end_iso and _near(end_iso, tip_dates[-1], max_days=45) and end_iso[:10] > t1:
        t1 = end_iso[:10]
    try:
        dates = _daily_iso_range(
            datetime.strptime(t0, "%Y-%m-%d"),
            datetime.strptime(t1, "%Y-%m-%d"),
        )
    except ValueError:
        dates = tip_dates
    if not dates:
        return None

    def _metric(name: str) -> list[int]:
        by = {d: clean[d][name] for d in tip_dates}
        out: list[int] = []
        for d in dates:
            if d in by:
                out.append(by[d])
                continue
            prev = [x for x in tip_dates if x < d]
            nxt = [x for x in tip_dates if x > d]
            if prev and nxt:
                d0, d1 = prev[-1], nxt[0]
                span = (datetime.strptime(d1, "%Y-%m-%d") - datetime.strptime(d0, "%Y-%m-%d")).days or 1
                t = (datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(d0, "%Y-%m-%d")).days / span
                out.append(int(round(by[d0] + t * (by[d1] - by[d0]))))
            elif prev:
                out.append(by[prev[-1]])
            elif nxt:
                out.append(by[nxt[0]])
            else:
                out.append(0)
        # Son 14 gün: eksik tooltip günlerinde 0 bırakma (GSC kartıyla uyumlu düz kuyruk).
        if len(out) >= 4:
            tail_start = max(0, len(out) - 14)
            anchor_idx = tail_start - 1
            while anchor_idx >= 0 and out[anchor_idx] <= 0:
                anchor_idx -= 1
            if anchor_idx >= 0:
                anchor = out[anchor_idx]
                for i in range(tail_start, len(out)):
                    if out[i] <= 0:
                        out[i] = anchor
        return out

    return {
        "dates": dates,
        "poor": _metric("poor"),
        "needs_improvement": _metric("needs_improvement"),
        "good": _metric("good"),
        "point_count": len(dates),
        "source": "gsc_tooltip",
        "tooltip_points": len(tip_dates),
    }


def _svg_pts_to_daily(
    pts: list[Any],
    dates: list[str],
    x_min: float,
    x_max: float,
    y_to_val,
) -> list[int]:
    """Yığılmış çubuk path: her gün diliminde tepe−taban (Y interp testere üretmesin)."""
    n = len(dates)
    if n < 2 or x_max <= x_min:
        return [0] * max(n, 0)
    width = (x_max - x_min) / n
    buckets: list[list[float]] = [[] for _ in range(n)]
    for p in pts or []:
        if not isinstance(p, (list, tuple)) or len(p) < 2:
            continue
        x, y = float(p[0]), float(p[1])
        i = int((x - x_min) / width)
        i = max(0, min(n - 1, i))
        buckets[i].append(float(y_to_val(y)))
    out: list[int] = []
    for bucket in buckets:
        if not bucket:
            out.append(0)
            continue
        hi, lo = max(bucket), min(bucket)
        if hi - lo >= max(40.0, 0.12 * hi):
            out.append(int(round(hi - lo)))
        else:
            out.append(int(round(hi)))
    return out


def _rects_to_daily(rects: list[Any], dates: list[str], y_to_val) -> list[int] | None:
    """<rect> çubukları — her rengin yüksekliği o günün URL sayısı."""
    n = len(dates)
    clean: list[tuple[float, float, float, float]] = []
    for r in rects or []:
        if not isinstance(r, (list, tuple)) or len(r) < 4:
            continue
        x, y, w, h = float(r[0]), float(r[1]), float(r[2]), float(r[3])
        if w <= 1 or h <= 1:
            continue
        clean.append((x, y, w, h))
    if n < 2 or len(clean) < 3:
        return None
    x_min = min(r[0] for r in clean)
    x_max = max(r[0] + r[2] for r in clean)
    if x_max <= x_min:
        return None
    width = (x_max - x_min) / n
    buckets: list[list[float]] = [[] for _ in range(n)]
    for x, y, w, h in clean:
        cx = x + w / 2.0
        i = int((cx - x_min) / width)
        i = max(0, min(n - 1, i))
        buckets[i].append(abs(float(y_to_val(y)) - float(y_to_val(y + h))))
    return [int(round(max(b))) if b else 0 for b in buckets]


def _unstack_if_cumulative(
    poor: list[int], ni: list[int], good: list[int]
) -> tuple[list[int], list[int], list[int]]:
    """SVG tepe çizgisi kümülatifse (yeşil=toplam) dilimlere ayır; bağımsızsa dokunma."""
    n = min(len(poor), len(ni), len(good))
    if n < 8:
        return poor, ni, good
    neg = 0
    for i in range(n):
        if int(good[i]) - int(ni[i]) < -80 or int(ni[i]) - int(poor[i]) < -80:
            neg += 1
    if neg >= max(3, int(0.2 * n)):
        return poor, ni, good
    out_p, out_n, out_g = [], [], []
    for i in range(n):
        p = max(0, int(poor[i]))
        out_p.append(p)
        out_n.append(max(0, int(ni[i]) - p))
        out_g.append(max(0, int(good[i]) - int(ni[i])))
    return out_p, out_n, out_g


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


def _parse_gsc_chart_tooltip(
    text: str,
    *,
    prefer_after: str | None = None,
    default_year: int | None = None,
) -> dict[str, Any] | None:
    raw = _clean(text)
    if not raw or len(raw) < 8:
        return None
    t = _fold_tr(raw)
    year_hint = default_year
    if prefer_after and re.match(r"^\d{4}-\d{2}-\d{2}$", str(prefer_after)[:10]):
        try:
            year_hint = int(str(prefer_after)[:4])
        except ValueError:
            year_hint = default_year
    date = ""
    for line in raw.splitlines():
        iso = _gsc_label_to_iso(line, default_year=year_hint)
        if iso:
            date = iso
            break
    if not date:
        m = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})", raw)
        if m:
            date = _gsc_label_to_iso(m.group(1), default_year=year_hint)
    if not date:
        m = re.search(
            r"(\d{1,2}\s+[A-Za-zçğıöşüÇĞİÖŞÜ]{3,}(?:\s+\d{2,4})?|[A-Za-z]{3,}\s+\d{1,2}(?:,?\s+\d{2,4})?)",
            raw,
        )
        if m:
            date = _gsc_label_to_iso(m.group(0), default_year=year_hint)
    if not date:
        return None
    # Yıl yokken yanlış (eski) yıla düşerse: önceki noktadan sonra olmalı
    if prefer_after and re.match(r"^\d{4}-\d{2}-\d{2}$", str(prefer_after)[:10]):
        try:
            d = datetime.strptime(date[:10], "%Y-%m-%d")
            prev = datetime.strptime(str(prefer_after)[:10], "%Y-%m-%d")
            # Aynı ay/gün bir yıl geriye kaçtıysa düzelt
            if d < prev - timedelta(days=14):
                bumped = d.replace(year=d.year + 1)
                if bumped >= prev - timedelta(days=2):
                    date = bumped.strftime("%Y-%m-%d")
        except ValueError:
            pass

    def grab(*pats: str) -> int:
        for pat in pats:
            m = re.search(pat, t, re.I)
            if m:
                return _parse_count(m.group(1))
        return 0

    num = r"([\d\.,]+\s*[BK]?)"
    poor = grab(
        rf"(?:yetersiz|kotu|kötü|poor)[^\d]{{0,28}}{num}",
        rf"{num}[^\d]{{0,12}}(?:yetersiz|kotu|kötü|poor)",
    )
    ni = grab(
        rf"iyilestir[^\d]{{0,40}}{num}",
        rf"iyileştir[^\d]{{0,40}}{num}",
        rf"(?:need improvement|needs improvement)[^\d]{{0,20}}{num}",
        rf"{num}[^\d]{{0,24}}iyilestir",
        rf"{num}[^\d]{{0,20}}(?:need improvement|needs improvement)",
    )
    good = grab(
        rf"(?:iyi\s+url'?l?e?r?|good(?:\s+url)?)[^\d]{{0,28}}{num}",
        rf"iyi(?!lestir)\s+{num}",
        rf"{num}[^\d]{{0,12}}(?:iyi\s+url|good(?:\s+url)?)",
        rf"{num}[^\d]{{0,8}}iyi(?!lestir)",
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


def _harvest_overview_tooltips(page, *, end_iso: str = "") -> list[dict[str, Any]]:
    """Grafik üzerinde sol→sağ tek geçiş: her noktayı oku, son güne kadar git.

    Eski davranış: 2 y_frac × 110 adım + 20 örnekte erken çıkış → fare iki kez
    Mayıs 31’e kadar gidip bitiyordu. Artık plot alanını bulur, ~1px adımlarla
    sağ kenara kadar tarar; aynı tarihte takılırsa adımı artırır ama bitirmez.
    """
    charts: list[dict[str, Any]] = []
    try:
        n_charts = int(
            page.evaluate(
                """() => [...document.querySelectorAll('svg')].filter(svg => {
                  const bb = svg.getBoundingClientRect();
                  return bb.width >= 320 && bb.height >= 120;
                }).length"""
            )
            or 0
        )
    except Exception:
        n_charts = 0
    n_charts = max(0, min(n_charts, 2))
    target_end = str(end_iso or "")[:10]

    for idx in range(n_charts):
        try:
            box = page.evaluate(
                """(i) => {
                  const svgs = [...document.querySelectorAll('svg')].filter(svg => {
                    const bb = svg.getBoundingClientRect();
                    return bb.width >= 320 && bb.height >= 120;
                  });
                  const svg = svgs[i];
                  if (!svg) return null;
                  svg.scrollIntoView({block: 'center', inline: 'nearest'});
                  const bb = svg.getBoundingClientRect();
                  // Plot alanı: clipPath / iç rect (eksen etiketlerini çıkar)
                  let plot = null;
                  const clips = [...svg.querySelectorAll('clipPath rect, rect')];
                  for (const r of clips) {
                    const rb = r.getBoundingClientRect();
                    if (rb.width >= bb.width * 0.55 && rb.height >= bb.height * 0.45) {
                      if (!plot || rb.width * rb.height > plot.w * plot.h) {
                        plot = {x: rb.x, y: rb.y, w: rb.width, h: rb.height};
                      }
                    }
                  }
                  const padL = 48, padR = 12, padT = 16, padB = 28;
                  if (!plot) {
                    plot = {
                      x: bb.x + padL,
                      y: bb.y + padT,
                      w: Math.max(40, bb.width - padL - padR),
                      h: Math.max(40, bb.height - padT - padB),
                    };
                  }
                  return {
                    x: plot.x, y: plot.y, width: plot.w, height: plot.h,
                    svgX: bb.x, svgY: bb.y, svgW: bb.width, svgH: bb.height
                  };
                }""",
                idx,
            )
        except Exception:
            continue
        if not isinstance(box, dict):
            continue
        w = float(box.get("width") or 0)
        h = float(box.get("height") or 0)
        if w < 200 or h < 80:
            continue
        time.sleep(0.25)
        samples: dict[str, dict[str, Any]] = {}
        # Tek yatay geçiş — çift tarama yok. Adım ~1px (min 280 örnek, max ~chart width).
        n = max(280, min(int(round(w)) + 1, 900))
        y = float(box["y"]) + h * 0.62
        x0 = float(box["x"]) + 2.0
        x1 = float(box["x"]) + w - 2.0
        last_date = ""
        same_streak = 0
        empty_streak = 0
        i = 0
        print(
            f"    tooltip sweep chart#{idx + 1}: {n} steps · plot {w:.0f}×{h:.0f}px"
            + (f" · target_end={target_end}" if target_end else ""),
            flush=True,
        )
        while i < n:
            t = i / max(n - 1, 1)
            x = x0 + (x1 - x0) * t
            try:
                page.mouse.move(x, y)
            except Exception:
                i += 1
                continue
            time.sleep(0.028)
            try:
                text = page.evaluate(_TIP_TEXT_JS)
            except Exception:
                text = ""
            parsed = _parse_gsc_chart_tooltip(
                str(text or ""),
                prefer_after=last_date or (sorted(samples)[-1] if samples else None),
            )
            if parsed and parsed.get("date"):
                empty_streak = 0
                d = str(parsed["date"])[:10]
                samples[d] = parsed
                if d == last_date:
                    same_streak += 1
                    # Aynı günde takılma (sticky tooltip) — birkaç adım atla, durma
                    if same_streak >= 4:
                        i += max(2, n // 120)
                        same_streak = 0
                        continue
                else:
                    same_streak = 0
                    last_date = d
            else:
                empty_streak += 1
                # Başta boşsa yavaş ilerle; ortada boşsa devam et
                if i < 40 and empty_streak >= 30 and not samples:
                    # İlk y bandı tutmadı — tek yedek y ile baştan (sadece bir kez)
                    break
            i += 1

        # İlk bant başarısızsa tek alternatif y ile bir kez daha (çift tam tarama değil)
        if len(samples) < 12:
            samples = {}
            last_date = ""
            y2 = float(box["y"]) + h * 0.48
            print(f"    tooltip sweep chart#{idx + 1}: retry y-band", flush=True)
            for i in range(n):
                t = i / max(n - 1, 1)
                x = x0 + (x1 - x0) * t
                try:
                    page.mouse.move(x, y2)
                except Exception:
                    continue
                time.sleep(0.028)
                try:
                    text = page.evaluate(_TIP_TEXT_JS)
                except Exception:
                    text = ""
                parsed = _parse_gsc_chart_tooltip(
                    str(text or ""),
                    prefer_after=last_date or (sorted(samples)[-1] if samples else None),
                )
                if parsed and parsed.get("date"):
                    d = str(parsed["date"])[:10]
                    samples[d] = parsed
                    last_date = d

        # Sağ kenarı sıkılaştır: son %12’yi 2× yoğun tara (son güne ulaşmak için)
        if samples:
            dense_n = max(40, int(w * 0.12))
            x_dense0 = x0 + (x1 - x0) * 0.88
            prefer = sorted(samples)[-1]
            for i in range(dense_n):
                t = i / max(dense_n - 1, 1)
                x = x_dense0 + (x1 - x_dense0) * t
                try:
                    page.mouse.move(x, y)
                except Exception:
                    continue
                time.sleep(0.032)
                try:
                    text = page.evaluate(_TIP_TEXT_JS)
                except Exception:
                    text = ""
                parsed = _parse_gsc_chart_tooltip(str(text or ""), prefer_after=prefer)
                if parsed and parsed.get("date"):
                    d = str(parsed["date"])[:10]
                    samples[d] = parsed
                    prefer = d

            # Hâlâ target_end yoksa sağdan sola kısa geri süpürme
            tip_last = sorted(samples)[-1] if samples else ""
            if target_end and tip_last and tip_last < target_end:
                print(
                    f"    tooltip end gap: last={tip_last} < target={target_end} · reverse densify",
                    flush=True,
                )
                for i in range(dense_n):
                    t = 1.0 - (i / max(dense_n - 1, 1))
                    x = x0 + (x1 - x0) * (0.75 + 0.25 * t)
                    try:
                        page.mouse.move(x, y)
                    except Exception:
                        continue
                    time.sleep(0.032)
                    try:
                        text = page.evaluate(_TIP_TEXT_JS)
                    except Exception:
                        text = ""
                    parsed = _parse_gsc_chart_tooltip(str(text or ""), prefer_after=prefer)
                    if parsed and parsed.get("date"):
                        d = str(parsed["date"])[:10]
                        samples[d] = parsed
                        prefer = max(prefer, d) if prefer else d

        if len(samples) < 8:
            print(f"    tooltip chart#{idx + 1}: only {len(samples)} pts — skip", flush=True)
            continue
        dates = sorted(samples)
        print(
            f"    tooltip chart#{idx + 1}: {len(dates)} unique days "
            f"{dates[0]} → {dates[-1]}",
            flush=True,
        )
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
    """Son gün = GSC özet kartı. Seriyi oranla çarpma (testere şişer)."""
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
            kpi_v = int(kpi_v)
            # KPI 0 iken seri başka metrikle aynı düz çizgiyse (kırmızı=turuncu) hepsini 0 yap.
            if kpi_v == 0 and len(arr) >= 8:
                cloned = False
                for alt in ("needs_improvement", "good"):
                    if alt == metric:
                        continue
                    other = [int(x or 0) for x in (ser.get(alt) or [])]
                    if len(other) != len(arr):
                        continue
                    same = sum(1 for a, b in zip(arr, other) if int(a or 0) == b and b > 0)
                    if same >= int(0.7 * len(arr)):
                        ser[metric] = [0] * len(arr)
                        cloned = True
                        break
                if cloned:
                    continue
            arr[-1] = kpi_v
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
            const rects = {};
            for (const path of svg.querySelectorAll('path')) {
              const stroke = path.getAttribute('stroke') || '';
              const fill = (path.getAttribute('fill') || '').trim().toLowerCase();
              const d = path.getAttribute('d') || '';
              const hv = (d.match(/[HhVv]/g) || []).length;
              const isBars = /[Zz]/.test(d) && hv >= 8;
              // Alan dolgusu (çizgi altı) EKG üretir; yığılmış çubuk path'i tut.
              if (fill && fill !== 'none' && fill !== 'transparent' && !isBars) continue;
              const st = statusFrom(stroke) || statusFrom(fill);
              if (!st) continue;
              if (d.length < 40) continue;
              const pts = parsePathPoints(d);
              if (pts.length < 8) continue;
              if (!series[st] || pts.length > series[st].length) series[st] = pts;
            }
            for (const rect of svg.querySelectorAll('rect')) {
              const fill = (rect.getAttribute('fill') || '').trim().toLowerCase();
              const st = statusFrom(fill) || statusFrom(rect.getAttribute('stroke') || '');
              if (!st) continue;
              const x = parseFloat(rect.getAttribute('x'));
              const y = parseFloat(rect.getAttribute('y'));
              const rw = parseFloat(rect.getAttribute('width'));
              const rh = parseFloat(rect.getAttribute('height'));
              if (!(rw > 1 && rh > 1) || Number.isNaN(x) || Number.isNaN(y)) continue;
              (rects[st] = rects[st] || []).push([x, y, rw, rh]);
            }
            if (Object.keys(series).length || Object.keys(rects).length) {
              charts.push({
                width: bb.width, height: bb.height,
                dateLabels, axisNums, series, rects
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
        series_rects = ch.get("rects") if isinstance(ch.get("rects"), dict) else {}
        all_xy: list[tuple[float, float]] = []
        for pts in series_pts.values():
            for p in pts or []:
                if isinstance(p, (list, tuple)) and len(p) >= 2:
                    all_xy.append((float(p[0]), float(p[1])))
        for recs in (series_rects or {}).values():
            for r in recs or []:
                if isinstance(r, (list, tuple)) and len(r) >= 4:
                    all_xy.append((float(r[0]), float(r[1])))
                    all_xy.append((float(r[0]) + float(r[2]), float(r[1]) + float(r[3])))
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
        # SVG eksen etiketleri dışına uzatma — sağ kenar boş bucket → Ağustos'ta 0 uçurumu.
        # Son günlere tooltip serisi bakar (end_iso orada işlenir).
        dates = _daily_iso_range(t0, t1)
        if not dates:
            continue

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
            from_rects = _rects_to_daily(list(series_rects.get(status) or []), dates, y_to_val)
            if from_rects and sum(from_rects) > 0:
                out_series[status] = from_rects
            else:
                out_series[status] = _svg_pts_to_daily(
                    list(series_pts.get(status) or []), dates, x_min, x_max, y_to_val
                )
        out_series["poor"], out_series["needs_improvement"], out_series["good"] = _unstack_if_cumulative(
            out_series["poor"],
            out_series["needs_improvement"],
            out_series["good"],
        )

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
        tip_charts = _harvest_overview_tooltips(page, end_iso=end_iso or "")
    except Exception as exc:  # noqa: BLE001
        print(f"    tooltip harvest skip: {exc}", flush=True)
    if tip_charts:
        print(
            f"    tooltip charts={len(tip_charts)} "
            f"pts={[c.get('point_count') for c in tip_charts]}",
            flush=True,
        )

    def _merge(svg_ch: dict[str, Any] | None, tip_ch: dict[str, Any] | None) -> dict[str, Any] | None:
        # Tooltip asıl kaynak: SVG Y (alan dolgusu) halı/EKG üretir, sayıları sapıtır.
        samples: dict[str, dict[str, Any]] = {}
        if tip_ch:
            t_dates = list(tip_ch.get("dates") or [])
            poor = list(tip_ch.get("poor") or [])
            ni = list(tip_ch.get("needs_improvement") or [])
            good = list(tip_ch.get("good") or [])
            for i, d in enumerate(t_dates):
                samples[str(d)[:10]] = {
                    "poor": int(poor[i]) if i < len(poor) else 0,
                    "needs_improvement": int(ni[i]) if i < len(ni) else 0,
                    "good": int(good[i]) if i < len(good) else 0,
                }
        start_iso = ""
        end_iso_use = end_iso or ""
        if svg_ch and (svg_ch.get("dates") or []):
            start_iso = str(svg_ch["dates"][0])[:10]
            if not end_iso_use:
                end_iso_use = str(svg_ch["dates"][-1])[:10]
        elif tip_ch and (tip_ch.get("dates") or []):
            tds = [str(d)[:10] for d in (tip_ch.get("dates") or [])]
            if tds:
                if not start_iso:
                    start_iso = tds[0]
                if not end_iso_use:
                    end_iso_use = tds[-1]
        from_tip = _series_from_tooltip_samples(
            samples, start_iso=start_iso, end_iso=end_iso_use
        )
        if from_tip:
            return from_tip
        # Yeterli tooltip yoksa SVG — ama SVG son günlerinde 0 uçurumu olabilir; tercih etme.
        if len(samples) >= 8:
            return None
        return svg_ch or tip_ch

    merged_m = _merge(
        charts[0] if len(charts) >= 1 else None,
        tip_charts[0] if len(tip_charts) >= 1 else None,
    )
    merged_d = _merge(
        charts[1] if len(charts) >= 2 else None,
        tip_charts[1] if len(tip_charts) >= 2 else None,
    )
    src = "gsc_overview_svg"
    if merged_m and merged_m.get("source"):
        src = str(merged_m.get("source"))
    return {"mobile": merged_m, "desktop": merged_d, "source": src}


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
    body = ""
    try:
        body = page.inner_text("body") or ""
    except Exception:
        body = str(meta.get("body_head") or "")
    parsed = _parse_gsc_kpi_triplet(body) or _parse_gsc_kpi_triplet(str(meta.get("body_head") or ""))
    kpis = parsed or {"poor": 0, "needs_improvement": 0, "good": 0}

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

    # Tüm drilldown item_key'leri topla (link + satır tıklama + bilinen yedek)
    discovered_keys: list[str] = list(_discover_item_keys_from_page(page))
    key_to_issue: dict[str, dict[str, Any]] = {}
    issue_count = len(issues)
    rows_n = page.locator("table tbody tr").count()
    for idx in range(max(issue_count, rows_n)):
        page.goto(_cwv_url(resource_id, "/summary", device=device), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page)
        rows = page.locator("table tbody tr")
        n = rows.count()
        if idx >= n:
            break
        print(f"    issue row {idx + 1}/{n} (discover item_key)…", flush=True)
        try:
            rows.nth(idx).click(timeout=20_000)
            time.sleep(3.5)
        except Exception as exc:  # noqa: BLE001
            print(f"    issue click skip: {exc}", flush=True)
            continue
        cur = page.url or ""
        if "drilldown" not in cur and "item_key" not in cur:
            continue
        qs = parse_qs(urlparse(cur).query)
        item_key = (qs.get("item_key") or [""])[0]
        if not item_key:
            continue
        if item_key not in discovered_keys:
            discovered_keys.append(item_key)
        if idx < len(issues):
            key_to_issue[item_key] = issues[idx]

    all_keys: list[str] = []
    seen_keys: set[str] = set()
    for k in discovered_keys + list(KNOWN_ITEM_KEYS.get(device) or []):
        key = str(k or "").strip()
        if key and key not in seen_keys:
            seen_keys.add(key)
            all_keys.append(key)

    drilldowns: list[dict[str, Any]] = []
    end_iso = ""
    m_end = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})", str(meta.get("body_head") or ""))
    if m_end:
        end_iso = _gsc_label_to_iso(m_end.group(1)) or ""

    print(f"    {len(all_keys)} drilldown ziyaret edilecek…", flush=True)
    for i, item_key in enumerate(all_keys):
        print(f"    drilldown {i + 1}/{len(all_keys)} item_key={item_key}…", flush=True)
        try:
            dd = _scrape_cwv_issue_drilldown(
                page,
                resource_id=resource_id,
                device=device,
                item_key=item_key,
                issue_hint=key_to_issue.get(item_key),
                end_iso=end_iso,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"    drilldown skip {item_key}: {exc}", flush=True)
            continue
        if not dd.get("url_rows") and dd.get("affected_url_count") is None:
            if dd.get("status") in ("", "unknown", "good"):
                print(f"    drilldown skip empty {item_key}", flush=True)
                continue
        drilldowns.append(dd)
        hint = key_to_issue.get(item_key)
        if hint is not None:
            for issue in issues:
                if issue is hint or (
                    issue.get("title") == hint.get("title") and issue.get("status") == hint.get("status")
                ):
                    issue["item_key"] = item_key
                    issue["drilldown_url"] = dd.get("source_url") or ""
                    if dd.get("affected_url_count") is not None:
                        issue["url_count"] = dd["affected_url_count"]
                    break
        else:
            for issue in issues:
                if issue.get("item_key") == item_key:
                    if dd.get("affected_url_count") is not None:
                        issue["url_count"] = dd["affected_url_count"]
                    issue["drilldown_url"] = dd.get("source_url") or ""
                    break

    # Good URLs drilldown (device-only — özet değil kırılım sayfası)
    print(f"  · {label} good URLs drilldown…", flush=True)
    good_urls: list[dict[str, Any]] = []
    good_meta: dict[str, Any] = {}
    good_affected: int | None = None
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
        good_stub: dict[str, Any] = {"source_url": page.url}
        _enrich_drilldown_from_page(page, good_stub, end_iso=end_iso)
        good_affected = good_stub.get("affected_url_count")
        if good_affected is None:
            try:
                good_affected = _parse_drilldown_affected_count(page.inner_text("body") or "")
            except Exception:
                good_affected = None
    except Exception as exc:  # noqa: BLE001
        print(f"  · good URLs skip: {exc}", flush=True)

    summary_kpis = dict(kpis)
    drill_kpis = _kpis_from_issue_drilldowns(drilldowns, good_affected=good_affected)
    kpis_source = "summary"
    if drilldowns or good_affected is not None:
        kpis = drill_kpis
        kpis_source = "drilldown"
        print(
            f"  · {label} KPI drilldown: poor={kpis.get('poor')} ni={kpis.get('needs_improvement')} "
            f"good={kpis.get('good')} (summary was poor={summary_kpis.get('poor')})",
            flush=True,
        )

    return {
        "device": device,
        "label": label,
        "kpis": kpis,
        "summary_kpis": summary_kpis,
        "kpis_source": kpis_source,
        "last_updated": "",
        "issues": issues,
        "issue_drilldowns": drilldowns,
        "good_urls": good_urls,
        "good_url_count": good_affected if good_affected is not None else len(good_urls),
        "good_affected_url_count": good_affected,
        "good_page_url": good_meta.get("url") or _cwv_url(resource_id, "/drilldown", device=device),
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
    meta = _extract_page_meta(page)
    issues_table = _extract_table(page)
    amp_issue_hints: list[dict[str, Any]] = []
    for row in issues_table.get("rows") or []:
        if len(row) < 1:
            continue
        title = row[0] if not row[0].startswith("http") else (row[1] if len(row) > 1 else row[0])
        amp_issue_hints.append({"title": _clean(title), "cells": row})

    discovered_keys: list[str] = list(_discover_item_keys_from_page(page))
    seed_keys = ["GgoIIBACIgAiACIA", "GgcIIhADKJ1O"]
    key_to_hint: dict[str, dict[str, Any]] = {}

    n = page.locator("table tbody tr").count()
    for idx in range(n):
        page.goto(_amp_url(resource_id), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page, 10000)
        rows = page.locator("table tbody tr")
        if idx >= rows.count():
            break
        print(f"  · AMP issue row {idx + 1}/{n} (discover item_key)…", flush=True)
        try:
            rows.nth(idx).click(timeout=15_000)
            time.sleep(3)
        except Exception as exc:  # noqa: BLE001
            print(f"  · AMP issue click skip: {exc}", flush=True)
            continue
        qs = parse_qs(urlparse(page.url).query)
        key = (qs.get("item_key") or [""])[0]
        if not key:
            continue
        if key not in discovered_keys:
            discovered_keys.append(key)
        if idx < len(amp_issue_hints):
            key_to_hint[key] = amp_issue_hints[idx]

    all_keys: list[str] = []
    seen_keys: set[str] = set()
    for k in discovered_keys + seed_keys:
        key = str(k or "").strip()
        if key and key not in seen_keys:
            seen_keys.add(key)
            all_keys.append(key)

    end_iso = ""
    m_end = re.search(r"(\d{1,2}[./]\d{1,2}[./]\d{2,4})", str(meta.get("body_head") or ""))
    if m_end:
        end_iso = _gsc_label_to_iso(m_end.group(1)) or ""

    amp_issues: list[dict[str, Any]] = []
    print(f"  · {len(all_keys)} AMP drilldown ziyaret edilecek…", flush=True)
    for i, key in enumerate(all_keys):
        print(f"  · AMP drilldown {i + 1}/{len(all_keys)} item_key={key}…", flush=True)
        try:
            dd = _scrape_amp_issue_drilldown(
                page,
                resource_id=resource_id,
                item_key=key,
                issue_hint=key_to_hint.get(key),
                end_iso=end_iso,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  · AMP drilldown skip {key}: {exc}", flush=True)
            continue
        amp_issues.append(dd)

    total_amp = sum(int(i.get("url_row_count") or 0) for i in amp_issues)
    return {
        "overview_url": _amp_url(resource_id),
        "issues": amp_issues,
        "url_row_count": total_amp,
    }


def scrape_property(page, prop: dict[str, str], *, charts_only: bool = False, headed: bool = True) -> dict[str, Any]:
    rid = prop["resource_id"]
    print(f"CWV scrape · {prop.get('label') or rid}", flush=True)
    page.goto(_cwv_url(rid), wait_until="domcontentloaded", timeout=120_000)
    # İlk yüklemede GSC shell geç gelebilir; oturum yoksa headed'de kullanıcıyı bekle
    _ensure_signed_in(page, headed=headed)
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
        # Cihaz KPI = o grafiğin son GSC noktası (başlık metni birleşik toplam olabiliyor)
        mobile_k = dict(overview.get("mobile") or {})
        desktop_k = dict(overview.get("desktop") or {})
        for key, bucket in (("mobile", mobile_k), ("desktop", desktop_k)):
            ser = chart_series.get(key) or {}
            if not isinstance(ser, dict):
                continue
            for metric in ("poor", "needs_improvement", "good"):
                arr = ser.get(metric) or []
                if arr:
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
    # KPI kaynağı: drilldown kırılım sayfaları (özet kart sayıları sapabiliyor)
    _snap_series_to_kpis(
        chart_series,
        {
            "mobile": mobile.get("kpis") if isinstance(mobile.get("kpis"), dict) else {},
            "desktop": desktop.get("kpis") if isinstance(desktop.get("kpis"), dict) else {},
        },
    )
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
        # Tek seferlik oturum kapısı — giriş bitince tüm property'ler taranır
        gate = props[0]
        print(f"CWV login gate · {gate.get('label') or gate.get('resource_id')}", flush=True)
        try:
            page.goto(_cwv_url(gate["resource_id"]), wait_until="domcontentloaded", timeout=120_000)
        except Exception as exc:
            print(f"Login gate goto uyarısı (devam): {exc}", flush=True)
        try:
            _ensure_signed_in(page, headed=headed)
        except RuntimeError as exc:
            print(f"FAIL login gate: {exc}", flush=True)
            return {
                "ok": False,
                "needs_login": True,
                "snapshots": 0,
                "ok_snapshots": 0,
                "message": str(exc),
            }
        for prop in props:
            try:
                snap = scrape_property(page, prop, charts_only=charts_only, headed=headed)
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
        _release_context(pw, context, headed=headed)

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
