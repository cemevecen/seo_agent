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
import re
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
RATINGS_URL = (
    os.environ.get("PLAY_CONSOLE_RATINGS_URL") or f"{BASE_APP}/user-feedback/ratings"
).strip()
MONETIZE_URL = (
    os.environ.get("PLAY_CONSOLE_MONETIZE_URL") or f"{BASE_APP}/monetize"
).strip()
GROW_URL = (
    os.environ.get("PLAY_CONSOLE_GROW_URL") or f"{BASE_APP}/grow-overview"
).strip()
MONITOR_URL = (
    os.environ.get("PLAY_CONSOLE_MONITOR_URL") or f"{BASE_APP}/monitor"
).strip()
RELEASE_URL = (
    os.environ.get("PLAY_CONSOLE_RELEASE_URL") or f"{BASE_APP}/test-and-release"
).strip()


def _stats_history_start() -> "date":
    from datetime import date

    raw = (os.environ.get("PLAY_CONSOLE_STATS_START") or "2025-01-01").strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return date(2025, 1, 1)


def _console_date_range(days: int | None = None) -> str:
    """Play Console QS: 2025_1_1-2026_8_7 — varsayılan 2025-01-01 → dün."""
    from datetime import date, timedelta

    end = date.today() - timedelta(days=1)
    if days is None:
        start = _stats_history_start()
    else:
        start = end - timedelta(days=max(1, days) - 1)
    if start > end:
        start = end

    def _fmt(d: date) -> str:
        return f"{d.year}_{d.month}_{d.day}"

    return f"{_fmt(start)}-{_fmt(end)}"


def _stats_url(
    *,
    metrics: str,
    dimension: str,
    dimension_values: str,
    days: int | None = None,
) -> str:
    dr = _console_date_range(days)
    qs = (
        f"metrics={metrics}"
        f"&dimension={dimension}"
        f"&dimensionValues={dimension_values}"
        f"&dateRange={dr}"
        f"&tab=APP_STATISTICS"
        f"&ctpMetric=DAU_MAU-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-CALCULATION_UNSPECIFIED-DAY"
        f"&ctpDateRange={dr}"
        f"&ctpDimension=COUNTRY&ctpDimensionValue=OVERALL"
        f"&ctpPeersetKey=3%3A6a1f18dbb44333cd"
    )
    return f"{BASE_APP}/statistics?{qs}"


_ANR_METRICS = "ANRS-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-PER_INTERVAL-DAY"
_CRASH_METRICS = "CRASHES-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-PER_INTERVAL-DAY"

# Kullanıcının verdiği + ANR sürüm/cihaz kırılımları (tarih: 2025-01-01+)
STATISTICS_VIEWS: list[dict[str, Any]] = [
    {
        "id": "device_acquisition",
        "label": "Cihaz edinme",
        "metric_key": "device_acquisition",
        "metrics": "DEVICE_ACQUISITION-NEW-EVENTS-CUMULATIVE-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CFR%2CNL",
        "needles": ("Cihaz edinme", "Device acquisition", "Edinme", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "user_lost",
        "label": "Kullanıcı kaybı",
        "metric_key": "user_lost",
        "metrics": "USER_LOST-ALL-EVENTS-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CCY%2CAT%2CDE",
        "needles": ("Kullanıcı kaybı", "User lost", "Kayıp", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "active_devices",
        "label": "Etkin cihazlar",
        "metric_key": "active_devices",
        "metrics": "ACTIVE_DEVICES-ALL-UNIQUE-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CFR%2CNL",
        "needles": ("Etkin cihaz", "Active device", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "dau",
        "label": "Günlük etkin kullanıcı",
        "metric_key": "dau",
        "metrics": "ENGAGEMENT_DAILY_ACTIVE_USERS-ACQUISITION_UNSPECIFIED-UNIQUE-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CFR%2CNL",
        "needles": ("Günlük etkin", "Daily active", "DAU", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "ar2_acquisitions",
        "label": "Mağaza edinme (AR2)",
        "metric_key": "ar2_acquisitions",
        "metrics": "AR2_ACQUISITIONS-ALL-UNIQUE-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CIQ%2CAT",
        "needles": ("Edinme", "Acquisition", "Mağaza", "İstatistik", "Veri tablosu"),
    },
    {
        # Günlük ortalama / Play puanı — yalnızca protobuf/tablo tarihli fact (kart OVERALL=5 yok)
        "id": "rating",
        "label": "Google Play puanı",
        "metric_key": "rating",
        "metrics": "GOOGLE_PLAY_RATING-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL",
        "dim_hint": "overview",
        "needles": ("Puan", "Rating", "Google Play", "İstatistik", "Veri tablosu", "Ortalama"),
    },
    {
        "id": "active_users",
        "label": "Etkin kullanıcılar",
        "metric_key": "active_users",
        "metrics": "ACTIVE_USERS-ALL-UNIQUE-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL",
        "needles": ("Etkin kullanıcı", "Active user", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "anrs_date",
        "label": "ANR · tarih",
        "metric_key": "anrs",
        "metrics": _ANR_METRICS,
        "dimension": "APP_VERSION",
        "dimension_values": "OVERALL",
        "dim_hint": "overview",
        "needles": ("ANR", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "anrs_os",
        "label": "ANR · Android OS",
        "metric_key": "anrs",
        "metrics": _ANR_METRICS,
        "dimension": "OS_VERSION",
        "dimension_values": "OVERALL%2C28%2C29%2C30%2C31%2C32%2C33%2C34%2C35%2C36",
        "dim_hint": "os_version",
        "needles": ("ANR", "OS", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "anrs_version",
        "label": "ANR · sürüm",
        "metric_key": "anrs",
        "metrics": _ANR_METRICS,
        "dimension": "APP_VERSION",
        "dimension_values": "OVERALL",
        "dim_hint": "app_version",
        "needles": ("ANR", "Sürüm", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "crashes_date",
        "label": "Çökme · tarih",
        "metric_key": "crashes",
        "metrics": _CRASH_METRICS,
        "dimension": "APP_VERSION",
        "dimension_values": "OVERALL",
        "dim_hint": "overview",
        "needles": ("Kilitlenme", "Crash", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "crashes_os",
        "label": "Çökme · Android OS",
        "metric_key": "crashes",
        "metrics": _CRASH_METRICS,
        "dimension": "OS_VERSION",
        "dimension_values": "OVERALL%2C28%2C29%2C30%2C31%2C32%2C33%2C34%2C35%2C36",
        "dim_hint": "os_version",
        "needles": ("Kilitlenme", "Crash", "OS", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "crashes_version",
        "label": "Çökme · sürüm",
        "metric_key": "crashes",
        "metrics": _CRASH_METRICS,
        "dimension": "APP_VERSION",
        "dimension_values": "OVERALL",
        "dim_hint": "app_version",
        "needles": ("Kilitlenme", "Crash", "Sürüm", "Veri tablosu", "İstatistik"),
    },
    {
        "id": "revenue",
        "label": "Gelir (günlük)",
        "metric_key": "revenue",
        # Tek metrik: günlük gelir. REVENUE_GST_USD_28D eklenirse aynı güne 2 seri yazılır ve toplam şişer.
        "metrics": "REVENUE-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL",
        "needles": ("Gelir", "Revenue", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "ar2_visitors",
        "label": "Mağaza ziyaretçileri",
        "metric_key": "ar2_visitors",
        "metrics": "AR2_VISITORS-ALL-UNIQUE-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CFR%2CCY",
        "needles": ("Ziyaret", "Visitor", "Mağaza girişi", "İstatistik", "Veri tablosu"),
    },
]

# Geriye uyum — eski tek URL’ler katalogdan türetilir
STATISTICS_URL = (
    os.environ.get("PLAY_CONSOLE_STATISTICS_URL")
    or _stats_url(
        metrics=_ANR_METRICS,
        dimension="APP_VERSION",
        dimension_values="OVERALL",
    )
).strip()
STATISTICS_VISITORS_URL = (
    os.environ.get("PLAY_CONSOLE_STATISTICS_VISITORS_URL")
    or _stats_url(
        metrics="AR2_VISITORS-ALL-UNIQUE-PER_INTERVAL-DAY",
        dimension="COUNTRY",
        dimension_values="OVERALL%2CTR%2CDE%2CFR%2CCY",
    )
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
_KNOWN_GROW = (
    "Cihaz edinme sayısı",
    "Cihaz ilk açılışları",
    "AEKS",
    "Mağaza girişi ziyaretçileri",
    "Mağaza girişi edinme sayısı",
    "Mağaza girişi dönüşüm oranı",
    "Kitle büyüme oranı",
    "Günlük etkin kullanıcı sayısı",
    "Etkin cihazlar",
    "Uygulamayı yükleyen kullanıcı sayısı",
    "Yüklemeler",
    "Toplam yükleme sayısı",
    "Kullanıcı kaybı",
    "Yeni cihaz edinme",
)
_KNOWN_STATISTICS = (
    "Kilitlenme sayısı",
    "Kilitlenme oranı",
    "ANR sayısı",
    "ANR oranı",
    "Çökme",
    "Crashes",
    "ANRs",
    "DAU",
    "MAU",
    "DAU/MAU",
    "Günlük etkin kullanıcı sayısı",
    "Aylık etkin kullanıcı sayısı",
    "Etkin cihazlar",
    "Yükleme tabanı",
    "Kullanıcı kaybı",
    "Cihaz edinme sayısı",
    "Mağaza girişi ziyaretçileri",
    "Store listing visitors",
    "Ziyaretçiler",
    "Unique visitors",
)
_KNOWN_MONITOR = (
    "Kilitlenme oranı",
    "ANR oranı",
    "Kilitlenme sayısı",
    "ANR sayısı",
    "Ortalama puan",
    "Google Play puanı",
    "Etkin cihazlar",
    "Yükleme tabanı",
    "Kullanıcı kaybı",
    "Vital",
    "Çökme",
)
_KNOWN_RELEASE = (
    "Kilitlenme oranı",
    "ANR oranı",
    "Yüklemeler",
    "Yükleme tabanı",
    "Üretim",
    "Production",
    "Açık test",
    "Kapalı test",
    "Dahili test",
    "Rollout",
    "Yayın",
    "Sürüm",
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
            low = url.lower()
            if not any(
                x in low
                for x in (
                    "play.google.com",
                    "androidpublisher",
                    "playdeveloperreporting",
                    "googleapis.com",
                    "clients6.google.com",
                    "batchexecute",
                    "/_$rpc/",
                    "playconsole",
                )
            ):
                return
            if resp.status and int(resp.status) >= 400:
                return
            ctype = ((resp.headers or {}).get("content-type") or "").lower()
            body = None
            try:
                body = resp.json()
            except Exception:
                try:
                    text = resp.text()
                    if not text:
                        return
                    if len(text) > 500_000:
                        text = text[:500_000]
                    tstrip = text.lstrip()
                    if tstrip.startswith("{") or tstrip.startswith("["):
                        try:
                            body = json.loads(text)
                        except Exception:
                            body = {"_text": text[:80_000]}
                    elif "json" in ctype or "protobuf" in ctype or "text/plain" in ctype:
                        body = {"_text": text[:80_000]}
                    else:
                        return
                except Exception:
                    return
            if body is None:
                return
            bag.append(
                {
                    "url": url[:500],
                    "status": resp.status,
                    "ctype": ctype[:80],
                    "body": body,
                }
            )
            if len(bag) > 500:
                del bag[:80]
        except Exception:
            return

    page.on("response", on_response)


def _attach_network_capture_context(context, bag: list[dict[str, Any]]) -> None:
    """Tüm sayfalar için context-level capture (SPA XHR kaçmasın)."""
    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            low = url.lower()
            if "clients6.google.com" not in low and "playconsole" not in low and "play.google.com" not in low:
                return
            if resp.status and int(resp.status) >= 400:
                return
            body = None
            try:
                body = resp.json()
            except Exception:
                return
            if not isinstance(body, dict):
                return
            bag.append({"url": url[:500], "status": resp.status, "body": body})
            if len(bag) > 500:
                del bag[:80]
        except Exception:
            return

    context.on("response", on_response)


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
      const hintRe = /yükleme|kilitlenme|anr|puan|cihaz|aeks|gelir|alıcı|etkin|kitle|mağaza|öykbog|edinme|kaybı|abonelik|satın|revenue|buyer|arppu|arpu|crash|çökme|dau|mau/i;

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
          || line.match(/^(Gelir|Alıcı|ÖYKBOG|Abonelik)\\s*\\((.+)\\)$/i)
          || line.match(/^(Mağaza girişi ziyaretçileri|Ziyaretçiler|Store listing visitors)\\s*\\((.+)\\)$/i)
          || line.match(/^(Kilitlenme oranı|ANR oranı|Kilitlenme sayısı|ANR sayısı)\\s*\\((.+)\\)$/i);
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
        if (br && /yükleme tabanı|yeni cihaz|gelir|alıcı|ülke|ürün|ziyaret|kilitlenme|anr|türkiye|germany|france|cyprus|\\bTR\\b|\\bDE\\b|\\bFR\\b|\\bCY\\b/i.test(br[1] + ' ' + br[2])) {
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

    def _as_points(vals: list[Any]) -> list[Any]:
        pts: list[Any] = []
        for v in vals[:90]:
            if isinstance(v, (int, float)):
                pts.append(v)
            elif isinstance(v, dict):
                num = (
                    v.get("value")
                    or v.get("count")
                    or v.get("y")
                    or v.get("metricValue")
                    or v.get("v")
                )
                ds = (
                    v.get("date")
                    or v.get("startTime")
                    or v.get("day")
                    or v.get("x")
                    or v.get("time")
                )
                if num is not None:
                    try:
                        pts.append(
                            {
                                "date": str(ds)[:32] if ds else None,
                                "value": float(num),
                            }
                        )
                    except (TypeError, ValueError):
                        pts.append(v)
                else:
                    pts.append(v)
            else:
                pts.append(v)
        return pts

    def walk(obj: Any, path: str = "") -> None:
        if len(out) >= 120:
            return
        if isinstance(obj, dict):
            keys = {str(k).lower() for k in obj.keys()}
            name = (
                obj.get("name")
                or obj.get("metric")
                or obj.get("metricId")
                or obj.get("title")
                or obj.get("displayName")
                or obj.get("id")
            )
            vals = (
                obj.get("values")
                or obj.get("points")
                or obj.get("data")
                or obj.get("timeSeries")
                or obj.get("series")
                or obj.get("dataPoints")
                or obj.get("samples")
            )
            if name and isinstance(vals, list) and vals and len(vals) >= 3:
                key = f"{name}|{len(vals)}|{path[-40:]}"
                if key not in seen:
                    seen.add(key)
                    out.append(
                        {
                            "name": str(name)[:160],
                            "points": _as_points(vals),
                            "point_count": len(vals),
                            "path": path[:160],
                        }
                    )
            # Play: rows[{dimensions, metrics}]
            if "rows" in keys and isinstance(obj.get("rows"), list):
                rows = obj.get("rows") or []
                if len(rows) >= 3:
                    key = f"rows|{len(rows)}|{path[-40:]}"
                    if key not in seen:
                        seen.add(key)
                        out.append(
                            {
                                "name": str(obj.get("metric") or obj.get("name") or "rows")[:160],
                                "points": rows[:90],
                                "point_count": len(rows),
                                "path": path[:160],
                            }
                        )
            for k, v in obj.items():
                walk(v, (path + "." + str(k))[:180])
        elif isinstance(obj, list):
            for i, v in enumerate(obj[:80]):
                walk(v, f"{path}[{i}]")

    for item in network or []:
        body = item.get("body") if isinstance(item, dict) else None
        if body is None:
            continue
        walk(body, str(item.get("url") or "")[:100])
    return out


def _parse_numeric_tr(val: Any) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    # %4,60 · 56,5 B · 2,6 Mn · $1.234 · 2.597.608 (TR binlik)
    s0 = s
    s = s.replace("\u00a0", " ").replace("%", "").replace("$", "").strip()
    mult = 1.0
    low = s.lower()
    if re.search(r"\bmn\b|\bmilyon\b", low) or re.search(r"(?<![a-zA-Z])M(?![a-zA-Z])", s0):
        mult = 1_000_000.0
    elif re.search(r"\bb\b|\bbin\b", low) or re.search(r"(?<![a-zA-Z])K(?![a-zA-Z])", s0):
        mult = 1_000.0
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s or s in {"-", ".", ","}:
        return None
    # TR binlik nokta: 2.597.608 veya 96.992
    if re.fullmatch(r"-?\d{1,3}(\.\d{3})+", s):
        try:
            return float(s.replace(".", "")) * mult
        except ValueError:
            return None
    # 1.234,56
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[-1]) <= 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return float(s) * mult
    except ValueError:
        return None



def _fact_value_ok(metric_key: str, value: float, *, source: str, raw: str = "") -> bool:
    """Tek haneli sahte kartları (ör. '5') ele — rating hariç."""
    if metric_key == "rating":
        return 0 < value <= 5.5
    if source == "card" and value < 20 and "B" not in raw and "Mn" not in raw and "%" not in raw:
        # Büyük metriklerde 5 gibi DOM gürültüsü
        return False
    return True


def _normalize_rating_value(val: float) -> float | None:
    """Play bazen 4.65, bazen 4650 (milli) döner — 1–5.5 aralığına çek."""
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    if 0 < v <= 5.5:
        return round(v, 4)
    if 1000 < v <= 5500:
        return round(v / 1000.0, 4)
    if 100 < v <= 550:
        return round(v / 100.0, 4)
    return None


def _dim_key(dimension: str) -> str:
    d = (dimension or "").upper()
    if d == "COUNTRY":
        return "country"
    if d == "OS_VERSION":
        return "os_version"
    if d == "APP_VERSION":
        return "app_version"
    if d in ("DEVICE", "DEVICE_MODEL", "DEVICE_TYPE"):
        return "device"
    return d.lower() or "overview"



_TR_MONTHS = {
    "oca": 1,
    "şub": 2,
    "sub": 2,
    "mar": 3,
    "nis": 4,
    "may": 5,
    "haz": 6,
    "tem": 7,
    "ağu": 8,
    "agu": 8,
    "eyl": 9,
    "eki": 10,
    "kas": 11,
    "ara": 12,
}


def _parse_tr_day_label(line: str) -> str | None:
    """'3 Ağu 2026' → 2026-08-03"""
    m = re.match(
        r"^(\d{1,2})\s+([A-Za-zÇĞİÖŞÜçğıöşü]{3,})\s+(20\d{2})$",
        (line or "").strip(),
    )
    if not m:
        return None
    day = int(m.group(1))
    mon_key = m.group(2).lower()
    # normalize ı/ş
    mon_key = mon_key.replace("ı", "i").replace("ş", "s").replace("ğ", "g").replace("ü", "u").replace("ö", "o").replace("ç", "c")
    mon = _TR_MONTHS.get(mon_key[:3]) or _TR_MONTHS.get(m.group(2).lower()[:3])
    year = int(m.group(3))
    if not mon or day < 1 or day > 31:
        return None
    return f"{year}-{mon:02d}-{day:02d}"


def _segments_from_dimension_values(dv: str) -> list[str]:
    raw = (dv or "").replace("%2C", ",").split(",")
    out = [p.strip() for p in raw if p.strip()]
    return out or ["OVERALL"]


def _parse_stats_data_table(
    text: str,
    *,
    metric_key: str,
    view_id: str,
    segments: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Play Console 'Veri tablosu' metninden günlük × ülke fact'leri."""
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]
    segs = list(segments or ["OVERALL", "TR", "DE", "FR", "NL"])
    label_map = {
        "tum ulkeler / bolgeler": "OVERALL",
        "tum ulkeler": "OVERALL",
        "genel": "OVERALL",
        "overall": "OVERALL",
        "turkiye": "TR",
        "turkey": "TR",
        "almanya": "DE",
        "germany": "DE",
        "fransa": "FR",
        "france": "FR",
        "hollanda": "NL",
        "netherlands": "NL",
        "avusturya": "AT",
        "austria": "AT",
        "kibris": "CY",
        "cyprus": "CY",
        "irak": "IQ",
        "iraq": "IQ",
        "misir": "EG",
        "egypt": "EG",
    }

    def _norm(s: str) -> str:
        t = (s or "").lower()
        for a, b in (("ı", "i"), ("ş", "s"), ("ğ", "g"), ("ü", "u"), ("ö", "o"), ("ç", "c")):
            t = t.replace(a, b)
        return t

    header_segs: list[str] = []
    for ln in lines:
        key = _norm(ln)
        if key in label_map:
            code = label_map[key]
            if code not in header_segs:
                header_segs.append(code)
        if _parse_tr_day_label(ln):
            break
    if len(header_segs) >= 2:
        segs = header_segs

    facts: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        ds = _parse_tr_day_label(lines[i])
        if not ds:
            i += 1
            continue
        i += 1
        if i < len(lines) and "yuzde" in _norm(lines[i]):
            i += 1
        values: list[float] = []
        while i < len(lines) and len(values) < len(segs):
            ln = lines[i]
            if _parse_tr_day_label(ln):
                break
            if ln.startswith("%") or "yuzde" in _norm(ln):
                i += 1
                continue
            num = _parse_numeric_tr(ln)
            if num is not None:
                values.append(num)
            i += 1
        for seg, val in zip(segs, values):
            facts.append(
                {
                    "metric": metric_key,
                    "view_id": view_id,
                    "dim": "country" if str(view_id) and seg != "OVERALL" else ("overview" if seg == "OVERALL" else "country"),
                    "segment": seg,
                    "date": ds,
                    "value": val,
                    "label": f"{view_id}:{seg}",
                    "source": "data_table",
                }
            )
            if seg == "OVERALL":
                facts[-1]["dim"] = "overview"
            else:
                facts[-1]["dim"] = "country"
    return facts



def _parse_stats_protobuf(
    body: Any,
    *,
    metric_key: str,
    view_id: str,
    dim_hint: str = "overview",
) -> list[dict[str, Any]]:
    """playconsolestatsfrontend JSON+protobuf zaman serisi → explorer facts.

    Satır şekli:
      {"1":[{"2":[{"2":{"2":"63"},"4":100}]}], "2":{"1":2026,"2":8,"3":7}}
    """
    if not isinstance(body, dict):
        return []
    rows = body.get("1")
    if not isinstance(rows, list) or len(rows) < 3:
        return []
    facts: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        dobj = row.get("2") if isinstance(row.get("2"), dict) else None
        if not dobj:
            continue
        try:
            y, m, d = int(dobj.get("1")), int(dobj.get("2")), int(dobj.get("3"))
            ds = f"{y:04d}-{m:02d}-{d:02d}"
        except (TypeError, ValueError):
            continue
        series_list = row.get("1")
        if not isinstance(series_list, list):
            continue
        for series in series_list:
            if not isinstance(series, dict):
                continue
            points = series.get("2")
            if not isinstance(points, list):
                continue
            def _dig_num(obj, depth=0):
                if depth > 6:
                    return None
                if isinstance(obj, (int, float)):
                    return float(obj)
                if isinstance(obj, str):
                    try:
                        return float(obj.replace(",", ""))
                    except ValueError:
                        return None
                if isinstance(obj, dict):
                    # tercih: {"2": "63"} veya {"2": 63}
                    if "2" in obj and not isinstance(obj.get("2"), (dict, list)):
                        return _dig_num(obj.get("2"), depth + 1)
                    for vv in obj.values():
                        n = _dig_num(vv, depth + 1)
                        if n is not None:
                            return n
                if isinstance(obj, list):
                    for vv in obj[:8]:
                        n = _dig_num(vv, depth + 1)
                        if n is not None:
                            return n
                return None

            for pi, pt in enumerate(points):
                if not isinstance(pt, dict):
                    continue
                val = _dig_num(pt.get("2"))
                if val is None:
                    val = _dig_num(pt)
                if val is None:
                    continue
                # segment id (varsa)
                seg_obj = pt.get("1") if isinstance(pt.get("1"), dict) else {}
                seg = "OVERALL"
                if isinstance(seg_obj, dict):
                    if "2" in seg_obj and not isinstance(seg_obj.get("2"), (dict, list)):
                        seg = str(seg_obj.get("2") or "OVERALL")[:80]
                    elif seg_obj.get("1") not in (None, 1, "1"):
                        seg = str(seg_obj.get("1"))[:80]
                dim = dim_hint if seg != "OVERALL" else "overview"
                if dim_hint in ("app_version", "device", "country", "os_version") and seg != "OVERALL":
                    dim = dim_hint
                facts.append(
                    {
                        "metric": metric_key,
                        "view_id": view_id,
                        "dim": dim if seg != "OVERALL" else "overview",
                        "segment": seg,
                        "date": ds,
                        "value": val,
                        "label": f"{view_id}:{seg}",
                        "source": "protobuf",
                    }
                )
    return facts


def _best_stats_protobuf(network_slice: list[dict[str, Any]]) -> Any | None:
    best = None
    best_n = 0
    for item in network_slice or []:
        body = item.get("body") if isinstance(item, dict) else None
        if not isinstance(body, dict):
            continue
        rows = body.get("1")
        if isinstance(rows, list) and len(rows) > best_n:
            best_n = len(rows)
            best = body
    return best if best_n >= 3 else None


def _collect_paginated_table_text(page, *, max_pages: int = 8) -> str:
    """Veri tablosu sayfalarını dolaş — protobuf yoksa sınırlı fallback."""
    chunks: list[str] = []
    try:
        page.evaluate(
            """async () => {
              const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
              const labels = [...document.querySelectorAll('div,span,button,mat-select')];
              const hit = labels.find((el) => /Satırları göster|Rows per page/i.test((el.innerText || '').trim()) && (el.innerText || '').length < 48);
              if (hit) { hit.click(); await sleep(350); }
              const opt = [...document.querySelectorAll('mat-option,button,li,span')]
                .find((el) => /^(100|50)$/.test((el.innerText || '').trim()));
              if (opt) { opt.click(); await sleep(900); }
            }"""
        )
    except Exception:
        pass
    seen_ends: set[int] = set()
    for _ in range(max_pages):
        try:
            text = page.evaluate("() => (document.body && document.body.innerText) || ''") or ""
        except Exception:
            text = ""
        if text:
            chunks.append(text)
        try:
            info = page.evaluate(
                """() => {
                  const t = document.body ? document.body.innerText : '';
                  const m = t.match(/(\\d+)\\s*-\\s*(\\d+)\\s*\\/\\s*(\\d+)/);
                  if (!m) return {done: true, end: 0, total: 0};
                  const end = parseInt(m[2], 10), total = parseInt(m[3], 10);
                  return {done: end >= total, end, total};
                }"""
            )
        except Exception:
            info = {"done": True, "end": 0}
        if not isinstance(info, dict) or info.get("done"):
            break
        end_n = int(info.get("end") or 0)
        if end_n in seen_ends:
            break
        seen_ends.add(end_n)
        try:
            clicked = page.evaluate(
                """() => {
                  const icons = [...document.querySelectorAll('button')].filter((el) => /chevron_right/i.test(el.innerText || ''));
                  const target = icons.length ? icons[icons.length - 1] : null;
                  if (!target || target.disabled) return false;
                  target.click();
                  return true;
                }"""
            )
        except Exception:
            clicked = False
        if not clicked:
            break
        _settle(page, seconds=1.0)
    return "\n".join(chunks)


def _explorer_facts_from_view(
    view: dict[str, Any],
    scraped: dict[str, Any],
    series: list[dict[str, Any]],
    page_text: str | None = None,
    protobuf_body: Any | None = None,
) -> list[dict[str, Any]]:
    """Kart + kırılım + protobuf + veri tablosu → keşif fact’leri."""
    facts: list[dict[str, Any]] = []
    metric_key = str(view.get("metric_key") or view.get("id") or "metric")
    dim = _dim_key(str(view.get("dimension") or "COUNTRY"))
    view_id = str(view.get("id") or metric_key)

    dim_hint = str(view.get("dim_hint") or dim)
    # 1) Protobuf (tam tarih aralığı, örn. 584 gün)
    if protobuf_body is not None:
        facts.extend(
            _parse_stats_protobuf(
                protobuf_body,
                metric_key=metric_key,
                view_id=view_id,
                dim_hint=dim_hint,
            )
        )
    # 2) DOM veri tablosu (sayfalanmış metin)
    table_facts = _parse_stats_data_table(
        page_text or "",
        metric_key=metric_key,
        view_id=view_id,
        segments=_segments_from_dimension_values(str(view.get("dimension_values") or "")),
    )
    # protobuf varsa DOM ile çakışan (date,segment) atla
    have = {(f.get("date"), f.get("segment")) for f in facts if f.get("date")}
    for tf in table_facts:
        key = (tf.get("date"), tf.get("segment"))
        if key in have and tf.get("date"):
            continue
        facts.append(tf)

    # Puan: tarihsiz kart/kırılım (tek OVERALL≈5) günlük seri sanılmasın
    if metric_key != "rating":
        for b in scraped.get("breakdowns") or []:
            if not isinstance(b, dict):
                continue
            raw = str(b.get("value") or "")
            num = _parse_numeric_tr(b.get("value"))
            if num is None or not _fact_value_ok(metric_key, num, source="breakdown", raw=raw):
                continue
            seg = str(b.get("segment") or "OVERALL").strip() or "OVERALL"
            facts.append(
                {
                    "metric": metric_key,
                    "view_id": view_id,
                    "dim": dim,
                    "segment": seg,
                    "date": None,
                    "value": num,
                    "label": str(b.get("title") or "")[:120],
                    "delta": str(b.get("delta") or ""),
                    "source": "breakdown",
                }
            )

        # Kartlar → overview segment
        for c in scraped.get("cards") or scraped.get("tpg") or []:
            if not isinstance(c, dict):
                continue
            raw = str(c.get("value") or "")
            num = _parse_numeric_tr(c.get("value"))
            if num is None or not _fact_value_ok(metric_key, num, source="card", raw=raw):
                continue
            facts.append(
                {
                    "metric": metric_key,
                    "view_id": view_id,
                    "dim": "overview",
                    "segment": "OVERALL",
                    "date": None,
                    "value": num,
                    "label": str(c.get("title") or "")[:120],
                    "delta": str(c.get("delta") or ""),
                    "period": str(c.get("period") or ""),
                    "source": "card",
                }
            )

    for s in series or []:
        if not isinstance(s, dict):
            continue
        pts = s.get("points") or []
        name = str(s.get("name") or "")[:160]
        for i, p in enumerate(pts):
            if isinstance(p, dict) and p.get("value") is not None:
                ds = p.get("date")
                # ISO-ish normalize
                ds_s = None
                if ds:
                    m = re.search(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})", str(ds))
                    if m:
                        ds_s = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                try:
                    val = float(p["value"])
                except (TypeError, ValueError, KeyError):
                    continue
                facts.append(
                    {
                        "metric": metric_key,
                        "view_id": view_id,
                        "dim": dim if ds_s else "overview",
                        "segment": "OVERALL",
                        "date": ds_s or f"i{i:03d}",
                        "value": val,
                        "label": name,
                        "source": "series",
                    }
                )
            elif isinstance(p, (int, float)):
                facts.append(
                    {
                        "metric": metric_key,
                        "view_id": view_id,
                        "dim": "overview",
                        "segment": "OVERALL",
                        "date": f"i{i:03d}",
                        "value": float(p),
                        "label": name,
                        "source": "series",
                    }
                )

    if metric_key == "rating":
        cleaned: list[dict[str, Any]] = []
        for f in facts:
            ds = str(f.get("date") or "")
            if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", ds[:10]):
                continue
            nv = _normalize_rating_value(f.get("value"))
            if nv is None:
                continue
            f = dict(f)
            f["date"] = ds[:10]
            f["value"] = nv
            f["dim"] = "overview"
            f["segment"] = "OVERALL"
            cleaned.append(f)
        return cleaned
    return facts


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
        lifetime_average: pick(/Yaşam boyu ortalama puan[^\\d]*([\\d,\\.]+)/i)
          || pick(/Lifetime average rating[^\\d]*([\\d,\\.]+)/i),
      };
    }"""
    )


def _extract_ratings_series_dom(page) -> list[dict[str, Any]]:
    """/user-feedback/ratings sayfasından günlük ortalama puan serisi.

    Önce veri tablosu satırları (tarih + puan), yoksa SVG/ARIA etiketleri.
    """
    return page.evaluate(
        """() => {
      const out = [];
      const seen = new Set();
      const push = (date, value) => {
        if (!date || value == null || !(value > 0) || value > 5.5) return;
        const k = date + '|' + value;
        if (seen.has(k)) return;
        seen.add(k);
        out.push({ date, value: Math.round(value * 1000) / 1000 });
      };
      const parseTrDate = (s) => {
        const t = (s || '').trim();
        let m = t.match(/^(20\\d{2})[-/.](\\d{1,2})[-/.](\\d{1,2})$/);
        if (m) return m[1] + '-' + String(m[2]).padStart(2,'0') + '-' + String(m[3]).padStart(2,'0');
        m = t.match(/^(\\d{1,2})[./](\\d{1,2})[./](20\\d{2})$/);
        if (m) return m[3] + '-' + String(m[2]).padStart(2,'0') + '-' + String(m[1]).padStart(2,'0');
        const months = {oca:1,sub:2,şub:2,mar:3,nis:4,may:5,haz:6,tem:7,agu:8,ağu:8,eyl:9,eki:10,kas:11,ara:12,
          jan:1,feb:2,apr:4,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
        m = t.match(/^(\\d{1,2})\\s+([A-Za-zÇĞİÖŞÜçğıöşü]{3,})\\s+(20\\d{2})$/i);
        if (m) {
          let mon = m[2].toLowerCase().replace('ı','i').replace('ş','s').replace('ğ','g').replace('ü','u').replace('ö','o').replace('ç','c');
          const mi = months[mon.slice(0,3)] || months[m[2].toLowerCase().slice(0,3)];
          if (mi) return m[3] + '-' + String(mi).padStart(2,'0') + '-' + String(m[1]).padStart(2,'0');
        }
        return null;
      };
      const parseNum = (s) => {
        const t = String(s || '').trim().replace(',', '.');
        const m = t.match(/([1-5](?:\\.\\d{1,3})?)/);
        if (!m) return null;
        const v = parseFloat(m[1]);
        return Number.isFinite(v) ? v : null;
      };
      // Tablo satırları
      for (const tr of Array.from(document.querySelectorAll('tr'))) {
        const cells = Array.from(tr.querySelectorAll('th,td')).map(c => (c.innerText || '').trim()).filter(Boolean);
        if (cells.length < 2) continue;
        let ds = null, val = null;
        for (const c of cells) {
          const d = parseTrDate(c);
          if (d) ds = d;
          const n = parseNum(c);
          if (n != null && n >= 1 && n <= 5.5) val = n;
        }
        if (ds && val != null) push(ds, val);
      }
      // Metin blokları: satır satır tarih + puan
      const lines = (document.body && document.body.innerText || '').split('\\n').map(s => s.trim()).filter(Boolean);
      for (let i = 0; i < lines.length - 1; i++) {
        const ds = parseTrDate(lines[i]);
        if (!ds) continue;
        const val = parseNum(lines[i + 1]);
        if (val != null) push(ds, val);
      }
      out.sort((a, b) => a.date.localeCompare(b.date));
      return out.slice(-400);
    }"""
    )


def _ratings_facts_from_series(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for pt in series or []:
        if not isinstance(pt, dict):
            continue
        ds = str(pt.get("date") or "")[:10]
        try:
            val = float(pt.get("value"))
        except (TypeError, ValueError):
            continue
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", ds):
            continue
        if not (0 < val <= 5.5):
            continue
        facts.append(
            {
                "metric": "rating",
                "view_id": "ratings_page",
                "dim": "overview",
                "segment": "OVERALL",
                "date": ds,
                "value": round(val, 4),
                "label": "Ortalama puan",
                "source": "ratings_page",
            }
        )
    return facts


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
    network_bag: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    captured_proto: list[Any] = []

    def _maybe_store(resp) -> None:
        try:
            u = (resp.url or "").lower()
            if "statsfrontend" not in u and "statspage" not in u:
                return
            if resp.status and int(resp.status) >= 400:
                return
            body = resp.json()
            if isinstance(body, dict) and isinstance(body.get("1"), list) and len(body["1"]) >= 10:
                captured_proto.append(body)
                if network_bag is not None:
                    network_bag.append({"url": (resp.url or "")[:500], "status": resp.status, "body": body})
        except Exception:
            return

    # goto sırasında statsfrontend’i yakala
    try:
        with page.expect_response(
            lambda r: ("statsfrontend" in (r.url or "").lower() or "statspage" in (r.url or "").lower())
            and (not r.status or int(r.status) < 400),
            timeout=45_000,
        ) as ri:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        try:
            _maybe_store(ri.value)
        except Exception:
            pass
    except Exception:
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)

    _settle(page, seconds=5.0)
    need, _, _ = _page_needs_login(page)
    if need and headed:
        _wait_until_console(page, timeout_sec=300)
        try:
            with page.expect_response(
                lambda r: "statsfrontend" in (r.url or "").lower(),
                timeout=45_000,
            ) as ri:
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            _maybe_store(ri.value)
        except Exception:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=5.0)

    _wait_page_text(page, wait_needles, timeout_sec=45.0)
    _scroll_full_page(page)
    _settle(page, seconds=3.0)

    # Hâlâ yoksa kısa bekle + yeniden dene (XHR gecikmeli)
    if not captured_proto:
        for _ in range(8):
            _settle(page, seconds=0.8)
            # network_bag’den de bak
            if network_bag:
                best = _best_stats_protobuf(network_bag[-30:])
                if best is not None:
                    captured_proto.append(best)
                    break

    extracted = _extract_stats_page(page, known=known, page_key=page_key) or {}
    if captured_proto:
        # en uzun seriyi seç
        captured_proto.sort(key=lambda b: len(b.get("1") or []), reverse=True)
        extracted["_protobuf"] = captured_proto[0]
        extracted["_protobuf_rows"] = len(captured_proto[0].get("1") or [])
    return extracted


def _page_payload(url: str, scraped: dict[str, Any]) -> dict[str, Any]:
    cards = scraped.get("cards") or scraped.get("tpg") or []
    br = scraped.get("breakdowns") or []
    return {
        "url": url,
        "cards": cards if isinstance(cards, list) else [],
        "breakdowns": br if isinstance(br, list) else [],
        "debug": scraped.get("debug") if isinstance(scraped.get("debug"), dict) else {},
        "error": scraped.get("error"),
    }


def _append_page_metrics(
    metrics: list[dict[str, Any]],
    scraped: dict[str, Any],
    *,
    kind: str,
    page_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cards = scraped.get("cards") or scraped.get("tpg") or []
    br = scraped.get("breakdowns") or []
    out_cards: list[dict[str, Any]] = []
    out_br: list[dict[str, Any]] = []
    for c in cards:
        if not isinstance(c, dict):
            continue
        row = {
            "title": c.get("title"),
            "value": c.get("value"),
            "delta": c.get("delta") or "",
            "period": c.get("period") or "",
            "kind": kind,
            "page": page_key,
            "lines": [c.get("title"), c.get("value"), c.get("delta")],
        }
        metrics.append(row)
        out_cards.append(row)
    for b in br:
        if not isinstance(b, dict):
            continue
        row = {
            "title": b.get("title"),
            "value": b.get("value"),
            "delta": b.get("delta") or "",
            "segment": b.get("segment") or "",
            "kind": "breakdown",
            "page": page_key,
            "lines": [b.get("title"), b.get("value"), b.get("delta")],
        }
        metrics.append(row)
        out_br.append(row)
    return out_cards, out_br


def _safe_scrape_page(
    page,
    *,
    url: str,
    known: tuple[str, ...],
    page_key: str,
    wait_needles: tuple[str, ...],
    headed: bool,
    network_bag: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        return _scrape_one_stats_page(
            page,
            url=url,
            known=known,
            page_key=page_key,
            wait_needles=wait_needles,
            headed=headed,
            network_bag=network_bag,
        )
    except Exception as exc:  # noqa: BLE001
        return {"page": page_key, "cards": [], "breakdowns": [], "error": str(exc)[:200]}


def scrape_play_console(*, headed: bool | None = None) -> dict[str, Any]:
    if headed is None:
        # Google Play, headless Chromium’da cookie’yi sık sık reddeder.
        env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
        headed = env_hl not in ("1", "true", "yes")
    # Login gerekirse mutlaka headed
    pw, context = _launch_context(headed=True if headed else False)
    network: list[dict[str, Any]] = []
    try:
        _attach_network_capture_context(context, network)
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
        metrics = _metrics_from_structured(structured)
        debug = structured.get("debug") if isinstance(structured.get("debug"), dict) else {}

        # Monetize + Grow + Statistics
        monetize = _safe_scrape_page(
            page,
            url=MONETIZE_URL,
            known=tuple(dict.fromkeys(list(_KNOWN_MONETIZE) + ["Gelir", "ÖYKBOG", "Alıcı Sayısı"])),
            page_key="monetize",
            wait_needles=("Gelir", "ÖYKBOG", "Alıcı", "Toplam gelir", "Monetize", "Para kazan"),
            headed=bool(headed),
        )
        grow = _safe_scrape_page(
            page,
            url=GROW_URL,
            known=tuple(dict.fromkeys(list(_KNOWN_GROW) + list(_KNOWN_DASHBOARD))),
            page_key="grow",
            wait_needles=(
                "Cihaz edinme",
                "Mağaza girişi",
                "AEKS",
                "Grow",
                "Büyüme",
                "Kullanıcı sayısını",
            ),
            headed=bool(headed),
        )
        monitor = _safe_scrape_page(
            page,
            url=MONITOR_URL,
            known=tuple(dict.fromkeys(list(_KNOWN_MONITOR) + list(_KNOWN_DASHBOARD))),
            page_key="monitor",
            wait_needles=(
                "Kilitlenme",
                "ANR",
                "Monitor",
                "İzle",
                "Vital",
                "Ortalama puan",
            ),
            headed=bool(headed),
        )
        release = _safe_scrape_page(
            page,
            url=RELEASE_URL,
            known=tuple(dict.fromkeys(list(_KNOWN_RELEASE) + list(_KNOWN_DASHBOARD))),
            page_key="release",
            wait_needles=(
                "Üretim",
                "Production",
                "Test",
                "Yayın",
                "Sürüm",
                "Rollout",
                "Kilitlenme",
                "Yükleme",
            ),
            headed=bool(headed),
        )
        # Tüm istatistik görünümleri (kullanıcı URL kataloğu)
        stats_cards: list[dict[str, Any]] = []
        stats_br: list[dict[str, Any]] = []
        stats_pages: dict[str, Any] = {}
        explorer_facts: list[dict[str, Any]] = []
        view_summaries: list[dict[str, Any]] = []
        known_stats = tuple(dict.fromkeys(list(_KNOWN_STATISTICS) + list(_KNOWN_DASHBOARD)))

        for view in STATISTICS_VIEWS:
            view_id = str(view["id"])
            url = _stats_url(
                metrics=str(view["metrics"]),
                dimension=str(view["dimension"]),
                dimension_values=str(view["dimension_values"]),
            )
            print(f"  · stats view {view_id} …", flush=True)
            net_before = len(network)
            scraped = _safe_scrape_page(
                page,
                url=url,
                known=known_stats,
                page_key=f"stats_{view_id}",
                wait_needles=tuple(view.get("needles") or ("İstatistik", "Statistics")),
                headed=bool(headed),
                network_bag=network,
            )
            _settle(page, seconds=1.0)
            net_slice = network[net_before:]
            view_series = _series_from_network(net_slice)
            cards_i, br_i = _append_page_metrics(
                metrics, scraped, kind="statistics", page_key=f"stats_{view_id}"
            )
            stats_cards.extend(cards_i)
            stats_br.extend(br_i)
            proto = scraped.get("_protobuf") or _best_stats_protobuf(net_slice)
            page_text = ""
            try:
                if proto is None or len((proto.get("1") or [])) < 30:
                    print(
                        f"    protobuf zayıf ({0 if not proto else len(proto.get('1') or [])}) — tablo fallback",
                        flush=True,
                    )
                    page_text = _collect_paginated_table_text(page, max_pages=6)
                else:
                    page_text = ""
            except Exception:
                page_text = ""
            scraped["_page_text_len"] = len(page_text)
            scraped["_protobuf_rows"] = len((proto or {}).get("1") or []) if isinstance(proto, dict) else 0
            facts_i = _explorer_facts_from_view(
                view, scraped, view_series, page_text=page_text, protobuf_body=proto
            )
            print(
                f"    → facts={len(facts_i)} proto_rows={scraped.get('_protobuf_rows')} cards={len(cards_i)}",
                flush=True,
            )
            explorer_facts.extend(facts_i)
            page_payload = _page_payload(url, scraped)
            page_payload["series"] = view_series[:12]
            page_payload["view"] = {
                "id": view_id,
                "label": view.get("label"),
                "metric_key": view.get("metric_key"),
                "dimension": view.get("dimension"),
            }
            page_payload["fact_count"] = len(facts_i)
            stats_pages[f"stats_{view_id}"] = page_payload
            view_summaries.append(
                {
                    "id": view_id,
                    "label": view.get("label"),
                    "metric_key": view.get("metric_key"),
                    "dimension": view.get("dimension"),
                    "url": url,
                    "cards": len(cards_i),
                    "breakdowns": len(br_i),
                    "series": len(view_series),
                    "facts": len(facts_i),
                    "error": scraped.get("error"),
                }
            )

        mon_cards, mon_br = _append_page_metrics(metrics, monetize, kind="monetize", page_key="monetize")
        grow_cards, grow_br = _append_page_metrics(metrics, grow, kind="grow", page_key="grow")
        monitor_cards, monitor_br = _append_page_metrics(
            metrics, monitor, kind="monitor", page_key="monitor"
        )
        release_cards, release_br = _append_page_metrics(
            metrics, release, kind="release", page_key="release"
        )

        dash_cards = structured.get("tpg") or structured.get("cards") or []
        dash_br = list(structured.get("breakdowns") or [])
        all_br = (
            dash_br
            + list(mon_br)
            + list(grow_br)
            + list(monitor_br)
            + list(release_br)
            + list(stats_br)
        )
        series = _series_from_network(network)
        panels = {
            "version": 3,
            "tpg": dash_cards,
            "breakdowns": all_br,
            "monetize": mon_cards,
            "grow": grow_cards,
            "monitor": monitor_cards,
            "release": release_cards,
            "statistics": stats_cards,
            "pages": {
                "dashboard": {
                    "url": DASHBOARD_URL,
                    "cards": dash_cards,
                    "breakdowns": dash_br,
                    "debug": debug,
                },
                "monetize": _page_payload(MONETIZE_URL, monetize),
                "grow": _page_payload(GROW_URL, grow),
                "monitor": _page_payload(MONITOR_URL, monitor),
                "release": _page_payload(RELEASE_URL, release),
                **stats_pages,
            },
            "sections": structured.get("sections") or [],
            "series": series,
            "explorer_facts": explorer_facts[:50000],
            "stats_views": view_summaries,
            "tpg_count": len(dash_cards),
            "breakdown_count": len(all_br),
            "monetize_count": len(mon_cards),
            "grow_count": len(grow_cards),
            "monitor_count": len(monitor_cards),
            "release_count": len(release_cards),
            "statistics_count": len(stats_cards),
            "series_count": len(series),
            "explorer_fact_count": len(explorer_facts),
            "stats_view_count": len(view_summaries),
            "debug": debug,
        }

        # Ratings sayfası — network protobuf/seri + DOM tablo (statistics rating’e yedek)
        print("  · ratings page …", flush=True)
        net_before_rt = len(network)
        page.goto(RATINGS_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=6.0)
        need_rt, _, _ = _page_needs_login(page)
        if need_rt and headed:
            _wait_until_console(page, timeout_sec=300)
            page.goto(RATINGS_URL, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=6.0)
        rating_summary = _extract_rating_summary_dom(page) or {}
        ratings_series = _extract_ratings_series_dom(page) or []
        net_slice_rt = network[net_before_rt:]
        proto_rt = _best_stats_protobuf(net_slice_rt)
        if proto_rt is not None:
            for f in _parse_stats_protobuf(
                proto_rt, metric_key="rating", view_id="ratings_page", dim_hint="overview"
            ):
                nv = _normalize_rating_value(f.get("value"))
                ds = str(f.get("date") or "")[:10]
                if nv is None or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", ds):
                    continue
                ratings_series.append({"date": ds, "value": nv})
        for s in _series_from_network(net_slice_rt):
            for p in s.get("points") or []:
                if not isinstance(p, dict):
                    continue
                ds = str(p.get("date") or "")
                m = re.search(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})", ds)
                if not m:
                    continue
                ds_s = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                try:
                    nv = _normalize_rating_value(float(p["value"]))
                except (TypeError, ValueError, KeyError):
                    continue
                if nv is None:
                    continue
                ratings_series.append({"date": ds_s, "value": nv})
        # Tablo sayfalama varsa ekstra metin
        if len(ratings_series) < 14:
            try:
                extra_txt = _collect_paginated_table_text(page, max_pages=4)
                extra = _parse_stats_data_table(
                    extra_txt,
                    metric_key="rating",
                    view_id="ratings_page",
                    segments=["OVERALL"],
                )
                have_d = {str(p.get("date")) for p in ratings_series}
                for f in extra:
                    ds = str(f.get("date") or "")[:10]
                    nv = _normalize_rating_value(f.get("value"))
                    if ds and ds not in have_d and nv is not None:
                        ratings_series.append({"date": ds, "value": nv})
                        have_d.add(ds)
            except Exception:
                pass
        # dedupe by date (son değer)
        by_day: dict[str, float] = {}
        for pt in ratings_series:
            if not isinstance(pt, dict):
                continue
            ds = str(pt.get("date") or "")[:10]
            nv = _normalize_rating_value(pt.get("value"))
            if nv is None or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", ds):
                continue
            by_day[ds] = nv
        ratings_series = [{"date": k, "value": by_day[k]} for k in sorted(by_day)]
        rating_facts = _ratings_facts_from_series(ratings_series)
        if rating_facts:
            # statistics’ten gelen tarihli rating fact’leriyle birleştir (tarih bazında)
            have_d = {str(f.get("date"))[:10] for f in rating_facts}
            keep = [
                f
                for f in explorer_facts
                if not (
                    str(f.get("metric")) == "rating"
                    and (
                        not f.get("date")
                        or str(f.get("date"))[:10] in have_d
                    )
                )
            ]
            explorer_facts = keep + rating_facts
            print(f"    → ratings_page facts={len(rating_facts)}", flush=True)
        else:
            print("    → ratings_page facts=0 (statistics rating fact’leri korunur)", flush=True)
        view_summaries.append(
            {
                "id": "ratings_page",
                "label": "Google Play puanı (ratings)",
                "metric_key": "rating",
                "fact_count": len(rating_facts),
                "url": RATINGS_URL,
            }
        )
        panels["explorer_facts"] = explorer_facts[:50000]
        panels["explorer_fact_count"] = len(explorer_facts)
        panels["stats_views"] = view_summaries
        panels["stats_view_count"] = len(view_summaries)
        panels["pages"] = {
            **(panels.get("pages") or {}),
            "ratings": {"url": RATINGS_URL, "fact_count": len(rating_facts)},
        }

        # Reviews sayfası
        page.goto(REVIEWS_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=5.0)
        need_r, _, _ = _page_needs_login(page)
        if need_r and headed:
            _wait_until_console(page, timeout_sec=300)
            page.goto(REVIEWS_URL, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=5.0)
        # Ratings özeti reviews’ta da olabilir — boşsa doldur
        if not rating_summary.get("default_rating"):
            rating_summary = {**rating_summary, **(_extract_rating_summary_dom(page) or {})}
        reviews = _extract_reviews_dom(page) or []

        ok = bool(
            metrics
            or reviews
            or rating_summary.get("default_rating")
            or rating_facts
            or panels.get("tpg")
            or panels.get("monetize")
            or panels.get("grow")
            or panels.get("monitor")
            or panels.get("release")
            or panels.get("statistics")
        )
        dbg = ""
        if not (
            panels.get("tpg")
            or panels.get("monetize")
            or panels.get("grow")
            or panels.get("monitor")
            or panels.get("release")
            or panels.get("statistics")
        ) and debug:
            dbg = f" · dash_known={debug.get('known_found_count')} body={debug.get('body_len')}"
        msg = (
            f"Play scrape · {len(metrics)} metric · "
            f"{panels.get('tpg_count', 0)} dash · {panels.get('monetize_count', 0)} mon · "
            f"{panels.get('grow_count', 0)} grow · {panels.get('monitor_count', 0)} monitor · "
            f"{panels.get('release_count', 0)} release · {panels.get('statistics_count', 0)} stats · "
            f"{panels.get('stats_view_count', 0)} stats_views · "
            f"{len(explorer_facts)} explorer_facts · {len(rating_facts)} rating_days · "
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
            "sync_mode": "dashboard_monetize_grow_monitor_release_stats_catalog_ratings_reviews",
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
