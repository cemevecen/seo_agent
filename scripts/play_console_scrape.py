#!/usr/bin/env python3
"""Google Play Console scrape (Mac bridge).

İlk giriş (headed — bir kez):
  .venv/bin/python scripts/play_console_scrape.py --login

Sync (varsayılan headed; Google headless’ta session düşürür):
  .venv/bin/python scripts/play_console_scrape.py --sync --ingest

Sadece vitals / yorumlar:
  .venv/bin/python scripts/play_console_scrape.py --vitals-only --sync --ingest
  .venv/bin/python scripts/play_console_scrape.py --reviews-only --sync --ingest

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
VITALS_CRASHES_BASE = f"{BASE_APP}/vitals/crashes"
VITALS_METRICS_OVERVIEW_URL = (
    os.environ.get("PLAY_CONSOLE_VITALS_METRICS_URL")
    or f"{BASE_APP}/vitals/metrics/overview?peersetKey=3%3A50984700721a5227"
).strip()

# Play Console "Sorun kategorisi" kırılımları (TR + EN etiketleri)
VITALS_ISSUE_CATEGORIES: list[dict[str, Any]] = [
    {
        "id": "general",
        "label": "Genel",
        "labels": ("Genel", "Overall", "General"),
        "description": "Belirtilen filtrelerin uygulandığı tüm sorunların görünümü",
    },
    {
        "id": "production",
        "label": "Üretimde",
        "labels": ("Üretimde", "In production", "In Production"),
        "description": "Üretim sürümündeki kullanıcı tarafından algılanan en önemli sorunlar",
    },
    {
        "id": "potential_fixes",
        "label": "Olası düzeltmeler içeren",
        "labels": (
            "Olası düzeltmeler içeren",
            "Including potential fixes",
            "Potential fixes",
        ),
        "description": "Olası düzeltmeler içeren, kullanıcı tarafından algılanan sorunlar",
    },
    {
        "id": "analysis",
        "label": "Analiz içeren",
        "labels": ("Analiz içeren", "Including analysis", "Analysis"),
        "description": "Analiz içeren, kullanıcı tarafından algılanan sorunlar",
    },
]


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
    # Stale Singleton* locks from crashed Chromium block relaunch.
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (PROFILE_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    pw = sync_playwright().start()
    channel = (os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL") or "chrome").strip()
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(PROFILE_DIR),
        "headless": not headed,
        "viewport": {"width": 1440, "height": 1100},
        "locale": "tr-TR",
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    # Bundled Chromium can crash on newer macOS; system Chrome is more stable.
    if channel and channel.lower() not in ("0", "none", "chromium"):
        launch_kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    except Exception:
        launch_kwargs.pop("channel", None)
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
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
                if isinstance(obj, (int, float)) and not isinstance(obj, bool):
                    return float(obj)
                if isinstance(obj, str):
                    try:
                        return float(obj.replace(",", "").replace(" ", ""))
                    except ValueError:
                        return None
                if isinstance(obj, dict):
                    # Play value slot çoğunlukla "2"; "1" sık tip/indeks bayrağı (rating’de hep 1.0 yapıyordu)
                    for key in ("2", "3", "4"):
                        if key in obj:
                            n = _dig_num(obj.get(key), depth + 1)
                            if n is not None:
                                return n
                    for key, vv in obj.items():
                        if str(key) == "1":
                            continue
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


def _reviews_days() -> int:
    raw = (os.environ.get("PLAY_CONSOLE_REVIEWS_DAYS") or "365").strip()
    try:
        return max(28, min(400, int(raw)))
    except ValueError:
        return 365


def _reviews_max() -> int:
    raw = (os.environ.get("PLAY_CONSOLE_REVIEWS_MAX") or "2500").strip()
    try:
        return max(50, min(8000, int(raw)))
    except ValueError:
        return 2500


def _extract_reviews_dom(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """() => {
      const out = [];
      const blocks = Array.from(document.querySelectorAll(
        'article, li, [role="listitem"], [data-review-id], div'
      ));
      for (const el of blocks) {
        const t = (el.innerText || '').trim();
        if (!t || t.length < 40 || t.length > 4000) continue;
        if (!/yıldız|star|★|⭐|puan/i.test(t) && !/\\n.*\\n/.test(t)) continue;
        const lines = t.split('\\n').map(s => s.trim()).filter(Boolean);
        if (lines.length < 3) continue;
        const hasDevice = /Android|iPhone|Samsung|Xiaomi|POCO|Galaxy|version|Cihaz:/i.test(t);
        const dateRe = /(\\d{1,2}\\s*(?:Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-zçğıöşü]*\\s*\\d{4}(?:[\\s,]*\\d{1,2}:\\d{2})?)/i;
        const dateRe2 = /(\\d{1,2}[\\.\\/]\\d{1,2}[\\.\\/]\\d{2,4})/;
        const hasDate = dateRe.test(t) || dateRe2.test(t);
        if (!(hasDevice || hasDate)) continue;
        const author = lines[0].slice(0, 120);
        const body = lines.slice(1).join(' ').slice(0, 1500);
        const starM = t.match(/([1-5])\\s*(yıldız|star)/i) || t.match(/★{1,5}/);
        let date = '';
        const dm = t.match(dateRe) || t.match(dateRe2);
        if (dm) date = dm[1];
        out.push({
          author,
          body,
          raw: t.slice(0, 2000),
          stars: starM ? starM[0] : null,
          date: date || null,
        });
        if (out.length >= 800) break;
      }
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


def _parse_review_date(text: str):
    """TR/EN tarih → date | None."""
    from datetime import date as date_cls
    from datetime import datetime

    s = (text or "").strip()
    if not s:
        return None
    months = {
        "oca": 1,
        "sub": 2,
        "mar": 3,
        "nis": 4,
        "may": 5,
        "haz": 6,
        "tem": 7,
        "agu": 8,
        "eyl": 9,
        "eki": 10,
        "kas": 11,
        "ara": 12,
        "jan": 1,
        "feb": 2,
        "apr": 4,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    m = re.search(
        r"(\d{1,2})\s*([A-Za-zÇĞİÖŞÜçğıöşü]{3,})[a-zçğıöşü]*\s*(\d{4})",
        s,
        re.I,
    )
    if m:
        key = (
            m.group(2)[:3]
            .lower()
            .replace("ş", "s")
            .replace("ğ", "g")
            .replace("ü", "u")
            .replace("ö", "o")
            .replace("ç", "c")
            .replace("ı", "i")
            .replace("â", "a")
        )
        mon = months.get(key)
        if mon:
            try:
                return date_cls(int(m.group(3)), int(mon), int(m.group(1)))
            except ValueError:
                pass
    m2 = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", s)
    if m2:
        d, mo, y = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        if y < 100:
            y += 2000
        try:
            return date_cls(y, mo, d)
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(s[:10]).date()
    except ValueError:
        return None


def _filter_reviews_by_days(
    reviews: list[dict[str, Any]], *, days: int
) -> list[dict[str, Any]]:
    from datetime import date, timedelta

    if days <= 0:
        return reviews
    cutoff = date.today() - timedelta(days=days)
    out: list[dict[str, Any]] = []
    for r in reviews:
        if not isinstance(r, dict):
            continue
        blob = " ".join(
            str(x or "")
            for x in (r.get("date"), r.get("author"), r.get("raw"), r.get("body"))
        )
        dt = _parse_review_date(blob)
        if dt is None or dt >= cutoff:
            out.append(r)
    return out


def _apply_reviews_date_filter(page, *, days: int = 365) -> bool:
    """Play Console yorum tarih filtresini son N güne çekmeye çalış."""
    labels: list[str] = []
    if days >= 360:
        labels = [
            "Son 1 yıl",
            "Last year",
            "Last 12 months",
            "Son 12 ay",
            "12 months",
            "365",
        ]
    elif days >= 180:
        labels = ["Son 6 ay", "Last 6 months", "180"]
    elif days >= 90:
        labels = ["Son 3 ay", "Last 90 days", "Last 3 months", "90"]
    else:
        labels = ["Son 28 gün", "Last 28 days", "28"]

    # Dönem / tarih combobox
    try:
        for name in (
            "Dönem",
            "Period",
            "Tarih",
            "Date",
            "Zaman aralığı",
            "Time period",
        ):
            loc = page.get_by_label(name, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=2_500)
                time.sleep(0.5)
                break
        else:
            clicked = page.evaluate(
                """() => {
                  const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
                  const nodes = Array.from(document.querySelectorAll(
                    'button, [role="combobox"], [aria-haspopup="listbox"]'
                  ));
                  for (const el of nodes) {
                    const t = clean(el.innerText || el.getAttribute('aria-label') || '');
                    if (/son\\s*\\d|last\\s*\\d|dönem|period|28 gün|7 gün|90|yıl|year|month/i.test(t)
                        && t.length < 60) {
                      el.click();
                      return true;
                    }
                  }
                  return false;
                }"""
            )
            if clicked:
                time.sleep(0.5)
    except Exception:
        pass

    for label in labels:
        try:
            opt = page.get_by_role("option", name=re.compile(re.escape(label), re.I))
            if opt.count() > 0:
                opt.first.click(timeout=2_500)
                _settle(page, seconds=2.5)
                return True
        except Exception:
            pass
        try:
            loc = page.get_by_text(label, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=2_500)
                _settle(page, seconds=2.5)
                return True
        except Exception:
            pass
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    return False


def _click_reviews_load_more(page) -> bool:
    """Sadece 'daha fazla yorum' tarzı butonlar — Next/pagination tıklama (sayfa kapanmasın)."""
    labels = (
        "Daha fazla göster",
        "Daha fazla yükle",
        "Show more",
        "Load more",
        "Daha fazla yorum",
    )
    for label in labels:
        try:
            loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}", re.I))
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=2_500)
                return True
        except Exception:
            pass
    return bool(
        page.evaluate(
            """() => {
              const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
              const nodes = Array.from(document.querySelectorAll('button, [role="button"]'));
              for (const el of nodes) {
                const t = clean(el.innerText || el.getAttribute('aria-label') || '');
                if (/^(daha fazla( göster| yükle| yorum)?|show more|load more)$/i.test(t)) {
                  el.click();
                  return true;
                }
              }
              return false;
            }"""
        )
    )


def _scrape_reviews_list(
    page, *, days: int | None = None, max_reviews: int | None = None
) -> list[dict[str, Any]]:
    """Yorumlar sayfasından tarih filtresi + scroll ile son N günü çek."""
    days = int(days if days is not None else _reviews_days())
    max_reviews = int(max_reviews if max_reviews is not None else _reviews_max())
    url = f"{REVIEWS_URL}?days={days}"
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    _settle(page, seconds=4.0)
    _wait_page_text(
        page,
        ("Yorum", "Review", "yıldız", "star", "Kullanıcı", "User"),
        timeout_sec=35.0,
    )
    applied = _apply_reviews_date_filter(page, days=days)
    print(
        f"    → reviews filter days={days} applied={applied}",
        flush=True,
    )

    merged: dict[str, dict[str, Any]] = {}
    stagnant = 0
    for round_i in range(100):
        batch = _extract_reviews_dom(page) or []
        before = len(merged)
        for r in batch:
            if not isinstance(r, dict):
                continue
            key = (
                str(r.get("author") or "").strip().lower()
                + "|"
                + str(r.get("body") or r.get("raw") or "")[:90].strip().lower()
            )
            if key.strip("|") and key not in merged:
                merged[key] = r
        gained = len(merged) - before
        print(
            f"    → reviews scroll {round_i + 1}: +{gained} total={len(merged)}",
            flush=True,
        )
        if len(merged) >= max_reviews:
            break
        if gained == 0:
            stagnant += 1
            # Liste konteynerini kaydır (window scroll çoğu zaman yetmez)
            try:
                page.evaluate(
                    """() => {
                      const cands = Array.from(document.querySelectorAll(
                        '[role="list"], [class*="scroll"], main, [class*="content"]'
                      ));
                      let best = null, bestH = 0;
                      for (const el of cands) {
                        const sh = el.scrollHeight || 0;
                        const ch = el.clientHeight || 0;
                        if (sh > ch + 80 && sh > bestH) { best = el; bestH = sh; }
                      }
                      if (best) {
                        best.scrollTop = Math.min(best.scrollHeight, best.scrollTop + best.clientHeight);
                        return true;
                      }
                      window.scrollBy(0, Math.max(1400, window.innerHeight));
                      return false;
                    }"""
                )
            except Exception:
                pass
            _settle(page, seconds=1.6)
            if stagnant in (2, 4):
                if _click_reviews_load_more(page):
                    _settle(page, seconds=2.2)
            if stagnant >= 6:
                break
        else:
            stagnant = 0
            try:
                page.evaluate(
                    """() => {
                      const cands = Array.from(document.querySelectorAll(
                        '[role="list"], [class*="scroll"], main, [class*="content"]'
                      ));
                      let best = null, bestH = 0;
                      for (const el of cands) {
                        const sh = el.scrollHeight || 0;
                        const ch = el.clientHeight || 0;
                        if (sh > ch + 80 && sh > bestH) { best = el; bestH = sh; }
                      }
                      if (best) {
                        best.scrollTop = Math.min(best.scrollHeight, best.scrollTop + Math.floor(best.clientHeight * 0.9));
                        return;
                      }
                      window.scrollBy(0, Math.max(1400, window.innerHeight * 1.2));
                    }"""
                )
            except Exception:
                pass
            _settle(page, seconds=1.2)

    rows = _filter_reviews_by_days(list(merged.values()), days=days)
    print(f"    → reviews kept={len(rows)} (raw={len(merged)}, days={days})", flush=True)
    return rows[:max_reviews]


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


def _extract_version_name_map(page) -> dict[str, str]:
    """Play UI: '290 (9.5.10)' / 'Uygulama sürümü: 290 (9.5.10)' → {290: 9.5.10}."""
    try:
        found = page.evaluate(
            """() => {
          const body = (document.body && document.body.innerText) || '';
          const out = {};
          const re = /(?:Uygulama sürümü|App version|Sürüm adı)\\s*:?\\s*(\\d{1,10})\\s*\\(([^)\\n]{1,40})\\)/gi;
          let m;
          while ((m = re.exec(body)) !== null) {
            const code = String(m[1] || '').trim();
            const name = String(m[2] || '').trim();
            if (code && name) out[code] = name;
          }
          const re2 = /\\b(\\d{2,6})\\s*\\((\\d+\\.\\d+(?:\\.\\d+)*)\\)/g;
          while ((m = re2.exec(body)) !== null) {
            const code = String(m[1] || '').trim();
            const name = String(m[2] || '').trim();
            if (code && name && !out[code]) out[code] = name;
          }
          return out;
        }"""
        )
        if isinstance(found, dict):
            return {
                str(k).strip(): str(v).strip()
                for k, v in found.items()
                if str(k).strip() and str(v).strip()
            }
    except Exception:
        pass
    return {}


def _merge_version_name_maps(*maps: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in maps:
        for k, v in (m or {}).items():
            if str(k).strip() and str(v).strip():
                out[str(k).strip()] = str(v).strip()
    return out


def _vitals_version_code() -> str:
    return (os.environ.get("PLAY_CONSOLE_VITALS_VERSION_CODE") or "").strip()


def _vitals_detail_limit() -> int:
    raw = (os.environ.get("PLAY_CONSOLE_VITALS_DETAIL_LIMIT") or "15").strip()
    try:
        return max(0, min(40, int(raw)))
    except ValueError:
        return 15


def _vitals_crashes_url(
    error_type: str, *, days: int = 28, version_code: str | None = None
) -> str:
    et = (error_type or "CRASH").strip().upper()
    qs = f"errorType={et}&isUserPerceived=true&days={int(days)}"
    vc = (version_code if version_code is not None else _vitals_version_code()).strip()
    if vc:
        qs += f"&versionCode={vc}"
    return f"{VITALS_CRASHES_BASE}?{qs}"


def _vitals_issue_detail_url(
    issue_id: str, *, days: int = 28, version_code: str | None = None
) -> str:
    qs = f"days={int(days)}&isUserPerceived=true"
    vc = (version_code if version_code is not None else _vitals_version_code()).strip()
    if vc:
        qs += f"&versionCode={vc}"
    return f"{VITALS_CRASHES_BASE}/{issue_id}/details?{qs}"


def _extract_vitals_issue_snapshot(page) -> dict[str, Any]:
    """Aktif sorun kategorisi + üst özet + sorun tablosu (tüm sütunlar)."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);

      let issueCount = null;
      for (const l of lines) {
        let m = l.match(/^(\\d[\\d.,]*)\\s*(sorun|issue|issues)\\b/i)
          || l.match(/\\b(\\d[\\d.,]*)\\s*(sorun|issue|issues)\\b/i);
        if (m) { issueCount = m[1]; break; }
      }

      const cards = [];
      const cardHints = /kilitlenme|anr|oran|olay|kullanıcı|affected|events|crash|rate/i;
      for (let i = 0; i < lines.length; i++) {
        const t = lines[i];
        if (!cardHints.test(t) || t.length > 80) continue;
        const v = lines[i + 1] || '';
        if (!/\\d/.test(v) || v.length > 40) continue;
        if (/^(Tem|Oca|Şub|Mar|Nis|May|Haz|Ağu|Eyl|Eki|Kas|Ara|Jan|Feb)/i.test(v)) continue;
        cards.push({ title: t, value: v, delta: lines[i + 2] && /^[+\\-−%]/.test(lines[i + 2]) ? lines[i + 2] : '' });
        if (cards.length >= 8) break;
      }

      const typeRe = /^(Kilitlenme|ANR|Crash|Application Not Responding)$/i;
      const trackRe = /^(Üretimde|In production|Internal testing|Closed testing|Open testing|Önceki sürüm|Previous)/i;
      const agoRe = /(\\d+[\\d.,]*)\\s*(saat|gün|dakika|hafta|ay|yıl|hour|hours|day|days|minute|minutes|week|weeks|month|months|year|years)\\s*(önce|ago)/i;
      const verRe = /^(\\d{2,8})\\s*\\(([^)]{1,40})\\)$/;
      const pctRe = /^%?\\d{1,3}([.,]\\d+)?\\s*%?$/;
      const numRe = /^\\d{1,7}([.,]\\d+)?$/;
      const skipLine = /^(sorun|tür|etkilenen|etkinlik|son gerçekleşme|issue|type|affected|events|last|kilitlenmeler ve anr)/i;
      const tagHints = /sdk|kilit anlaşmazlığı|bağlayıcı|binder|lock contention|potential fix|olası düzeltme|analiz|may be related/i;
      const excHints = /Exception|Error|Timeout|SIGSEGV|SIGABRT|ANR|NullPointer|IllegalState|OutOfMemory|DeadObject|RemoteException|Input dispatching|Native method|java\\.|kotlin\\./i;

      const parseRowLines = (rowLines, issueId, detailUrl) => {
        const ls = (rowLines || []).map(clean).filter(Boolean);
        if (!ls.length) return null;
        let issueType = '';
        let affectedVersions = '';
        let versionTrack = '';
        let users = '';
        let events = '';
        let eventsShare = '';
        let lastOccurrence = '';
        const tags = [];
        const titleBits = [];
        const nums = [];

        for (const l of ls) {
          if (!l || skipLine.test(l)) continue;
          if (typeRe.test(l)) { issueType = l; continue; }
          if (trackRe.test(l)) { versionTrack = l; continue; }
          const ago = l.match(agoRe);
          if (ago && l.length < 48) { lastOccurrence = l; continue; }
          const vm = l.match(verRe);
          if (vm) { affectedVersions = vm[1] + ' (' + vm[2] + ')'; continue; }
          if (tagHints.test(l) && l.length < 80) { tags.push(l); continue; }
          if (pctRe.test(l) && /%/.test(l)) { eventsShare = l.startsWith('%') ? l : ('%' + l.replace(/%/g,'')); continue; }
          if (numRe.test(l)) { nums.push(l.replace(/\\./g, '').replace(',', '.')); continue; }
          if (l.length >= 4 && l.length <= 220) titleBits.push(l);
        }
        if (nums.length >= 1) users = nums[0];
        if (nums.length >= 2) events = nums[1];
        if (nums.length >= 3 && !eventsShare) eventsShare = nums[2];

        const stripNav = (s) => clean(String(s || '')
          .replace(/\\s*ayrıntısını göster\\s*$/i, '')
          .replace(/\\s*show details?\\s*$/i, '')
          .replace(/\\s*view details?\\s*$/i, ''));
        let title = stripNav(titleBits[0] || '');
        let subtitle = '';
        for (let i = 0; i < titleBits.length; i++) {
          const bit = stripNav(titleBits[i]);
          if (excHints.test(bit) && i > 0) {
            title = stripNav(titleBits[0]);
            subtitle = bit;
            break;
          }
        }
        if (!subtitle && titleBits.length >= 2 && excHints.test(titleBits[1])) {
          subtitle = stripNav(titleBits[1]);
        }
        if (!title && subtitle) title = subtitle;
        if (!title && !issueId) return null;
        if (!title) title = 'Issue ' + String(issueId || '').slice(0, 12);
        title = stripNav(title);

        return {
          issue_id: issueId || '',
          detail_url: detailUrl || '',
          title: title.slice(0, 240),
          subtitle: (subtitle || '').slice(0, 240),
          tags: tags.slice(0, 6),
          issue_type: issueType,
          affected_versions: affectedVersions,
          version_track: versionTrack,
          users,
          events,
          events_share: eventsShare,
          last_occurrence: lastOccurrence,
          extra: '',
        };
      };

      const issues = [];
      const seen = new Set();

      // 1) Detay linklerinden satır oku (en güvenilir)
      const anchors = Array.from(document.querySelectorAll('a[href]'));
      for (const a of anchors) {
        const href = String(a.href || a.getAttribute('href') || '');
        const m = href.match(/\\/vitals\\/crashes\\/([a-f0-9]{16,})\\/(?:details|detail)/i);
        if (!m) continue;
        const issueId = m[1];
        if (seen.has(issueId)) continue;
        const row = a.closest('tr, [role="row"], mat-row, [class*="row"]') || a.parentElement;
        if (!row) continue;
        const cellEls = Array.from(row.querySelectorAll('[role="cell"], [role="gridcell"], td'));
        let rowLines = [];
        if (cellEls.length >= 2) {
          for (const c of cellEls) {
            const parts = String(c.innerText || '').split(/\\n+/).map(clean).filter(Boolean);
            rowLines.push(...parts);
          }
        } else {
          rowLines = String(row.innerText || '').split(/\\n+/).map(clean).filter(Boolean);
        }
        // Chip / etiket metinleri
        const chipTexts = Array.from(row.querySelectorAll(
          '[class*="chip"], [class*="tag"], [class*="badge"], mat-chip, md-chip'
        )).map((el) => clean(el.innerText || '')).filter((t) => t && t.length < 80);
        for (const t of chipTexts) {
          if (!rowLines.includes(t)) rowLines.push(t);
        }
        // Link aria-label bazen sorun adını taşır
        const aria = clean(a.getAttribute('aria-label') || a.getAttribute('title') || '');
        if (aria && aria.length > 8 && aria.length < 200 && !rowLines.includes(aria)) {
          rowLines = [aria, ...rowLines];
        }
        const abs = href.startsWith('http') ? href.split('#')[0] : (location.origin + href.split('#')[0]);
        const parsed = parseRowLines(rowLines, issueId, abs);
        if (!parsed) continue;
        if (chipTexts.length && !(parsed.tags || []).length) {
          parsed.tags = chipTexts.filter((t) => tagHints.test(t)).slice(0, 6);
        }
        seen.add(issueId);
        issues.push(parsed);
        if (issues.length >= 60) break;
      }

      // 2) role=row yedek (link yoksa)
      if (issues.length < 3) {
        const rows = Array.from(document.querySelectorAll('[role="row"], tr'));
        for (const row of rows) {
          const cells = Array.from(row.querySelectorAll('[role="cell"], [role="gridcell"], td'));
          if (cells.length < 3) continue;
          const rowLines = [];
          for (const c of cells) {
            rowLines.push(...String(c.innerText || '').split(/\\n+/).map(clean).filter(Boolean));
          }
          const joined = rowLines.join(' | ');
          if (!/Kilitlenme|\\bANR\\b|Exception|Error|SourceFile|Native method|SIGSEGV|Input dispatching/i.test(joined)) continue;
          if (/sorun kategorisi|issue category/i.test(joined)) continue;
          let issueId = '';
          let detailUrl = '';
          const link = row.querySelector('a[href*="/vitals/crashes/"]');
          if (link) {
            const href = String(link.href || link.getAttribute('href') || '');
            const m = href.match(/\\/vitals\\/crashes\\/([a-f0-9]{16,})/i);
            if (m) {
              issueId = m[1];
              detailUrl = href.startsWith('http') ? href.split('#')[0] : (location.origin + href.split('#')[0]);
            }
          }
          const key = issueId || rowLines.slice(0, 3).join('|');
          if (seen.has(key)) continue;
          const parsed = parseRowLines(rowLines, issueId, detailUrl);
          if (!parsed || (!parsed.users && !parsed.events && !parsed.issue_type)) continue;
          seen.add(key);
          issues.push(parsed);
          if (issues.length >= 60) break;
        }
      }

      // 3) Düz metin yedek: başlık + exception + sütunlar
      if (issues.length < 2) {
        for (let i = 0; i < lines.length; i++) {
          const title = lines[i];
          if (!title || title.length < 6 || title.length > 200) continue;
          if (skipLine.test(title) || typeRe.test(title) || trackRe.test(title)) continue;
          const window = lines.slice(i, Math.min(i + 12, lines.length));
          if (!window.some((l) => typeRe.test(l))) continue;
          if (!window.some((l) => numRe.test(l))) continue;
          const parsed = parseRowLines(window, '', '');
          if (!parsed || !parsed.issue_type) continue;
          const key = (parsed.title + '|' + parsed.subtitle + '|' + parsed.users + '|' + parsed.events);
          if (seen.has(key)) continue;
          seen.add(key);
          issues.push(parsed);
          i += 4;
          if (issues.length >= 40) break;
        }
      }

      let selected = '';
      const combos = Array.from(document.querySelectorAll(
        '[role="combobox"], [aria-haspopup="listbox"], button[aria-expanded]'
      ));
      for (const el of combos) {
        const t = clean(el.innerText || el.textContent || '');
        if (/genel|üretimde|olası|analiz|overall|production|potential|analysis/i.test(t) && t.length < 80) {
          selected = t.split('\\n')[0];
          break;
        }
      }

      return {
        selected_category: selected,
        issue_count: issueCount,
        cards,
        issues,
        body_len: body.length,
        page_url: location.href,
      };
    }"""
    )


def _extract_vitals_issue_detail(page) -> dict[str, Any]:
    """Tek sorun detay sayfası: özet, stack, içgörüler."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);
      const href = location.href || '';
      const idm = href.match(/\\/vitals\\/crashes\\/([a-f0-9]{16,})/i);
      const issueId = idm ? idm[1] : '';

      let title = '';
      let subtitle = '';
      const h = document.querySelector('h1, h2, [role="heading"]');
      if (h) title = clean(h.innerText || '').split('\\n')[0];
      for (const l of lines.slice(0, 40)) {
        if (!title && l.length > 8 && l.length < 200 && !/vitals|kilitlenme|android|filtre|filter/i.test(l)) {
          title = l;
        }
        if (/Exception|Error|SIGSEGV|SIGABRT|Timeout|ANR|Input dispatching/i.test(l) && l.length < 200) {
          subtitle = l;
          break;
        }
      }

      const summary = [];
      const summaryHints = /kullanıcı|olay|etkinlik|sürüm|version|cihaz|device|android|oran|rate|son|last/i;
      for (let i = 0; i < Math.min(lines.length, 120); i++) {
        const t = lines[i];
        const v = lines[i + 1] || '';
        if (!summaryHints.test(t) || t.length > 80) continue;
        if (!/\\d/.test(v) || v.length > 60) continue;
        summary.push({ title: t.slice(0, 120), value: v.slice(0, 80) });
        if (summary.length >= 12) break;
      }

      const insights = [];
      for (const l of lines) {
        if (/sdk ile ilgili|may be related to sdk|kilit anlaşmazlığı|lock contention|bağlayıcı|binder call|olası düzeltme|potential fix|analiz/i.test(l)
            && l.length < 160) {
          if (!insights.includes(l)) insights.push(l);
        }
        if (insights.length >= 10) break;
      }

      // Stack / traceback bloğu
      let stack = '';
      const stackIdx = lines.findIndex((l) =>
        /stack\\s*trace|yığın|backtrace|at\\s+[\\w.$]+\\(/i.test(l)
        || /^\\s*at\\s+/i.test(l)
        || /#(0|00)\\s+pc\\s+/i.test(l)
      );
      if (stackIdx >= 0) {
        stack = lines.slice(stackIdx, stackIdx + 40).join('\\n');
      } else {
        const codeish = lines.filter((l) =>
          /^at\\s+/i.test(l) || /\\(\\w+\\.\\w+:\\d+\\)/.test(l) || /SourceFile|#\\d+\\s+pc/.test(l)
        );
        if (codeish.length) stack = codeish.slice(0, 35).join('\\n');
      }

      const sections = [];
      const secRe = /^(Özet|Summary|Yığın|Stack|Cihazlar|Devices|Android sürüm|Android versions|Uygulama sürüm|App versions|Ülke|Country|Breakdown)/i;
      for (let i = 0; i < lines.length; i++) {
        if (!secRe.test(lines[i]) || lines[i].length > 60) continue;
        const vals = [];
        for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
          if (secRe.test(lines[j])) break;
          if (lines[j].length < 80) vals.push(lines[j]);
        }
        sections.push({ title: lines[i], lines: vals.slice(0, 6) });
        if (sections.length >= 10) break;
      }

      return {
        issue_id: issueId,
        url: href.split('#')[0],
        title: (title || '').slice(0, 240),
        subtitle: (subtitle || '').slice(0, 240),
        summary_cards: summary,
        insights: insights.slice(0, 10),
        stack_trace: stack.slice(0, 6000),
        sections,
        body_len: body.length,
      };
    }"""
    )


def _scrape_vitals_issue_details(
    page,
    issues: list[dict[str, Any]],
    *,
    days: int = 28,
    version_code: str | None = None,
    limit: int | None = None,
    headed: bool = True,
) -> dict[str, dict[str, Any]]:
    """Genel listedeki üst sorunların detay sayfalarını çek."""
    if limit is None:
        limit = _vitals_detail_limit()
    if limit <= 0:
        return {}

    ranked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for iss in issues:
        if not isinstance(iss, dict):
            continue
        iid = str(iss.get("issue_id") or "").strip()
        if not iid or iid in seen:
            continue
        seen.add(iid)
        try:
            users_n = int(re.sub(r"[^\d]", "", str(iss.get("users") or "0")) or "0")
        except ValueError:
            users_n = 0
        try:
            events_n = int(re.sub(r"[^\d]", "", str(iss.get("events") or "0")) or "0")
        except ValueError:
            events_n = 0
        ranked.append({**iss, "_users_n": users_n, "_events_n": events_n})
    ranked.sort(key=lambda x: (x.get("_users_n", 0), x.get("_events_n", 0)), reverse=True)
    ranked = ranked[:limit]

    out: dict[str, dict[str, Any]] = {}
    for idx, iss in enumerate(ranked, 1):
        iid = str(iss.get("issue_id") or "").strip()
        url = str(iss.get("detail_url") or "").strip() or _vitals_issue_detail_url(
            iid, days=days, version_code=version_code
        )
        print(f"    · detail {idx}/{len(ranked)} {iid[:12]}…", flush=True)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=3.5)
            need, _, _ = _page_needs_login(page)
            if need and headed:
                _wait_until_console(page, timeout_sec=300)
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                _settle(page, seconds=3.5)
            _wait_page_text(
                page,
                (
                    "Stack",
                    "Yığın",
                    "Exception",
                    "ANR",
                    "Kilitlenme",
                    "Crash",
                    "vitals",
                    iid[:8],
                ),
                timeout_sec=25.0,
            )
            _scroll_full_page(page)
            detail = _extract_vitals_issue_detail(page) or {}
            if not detail.get("issue_id"):
                detail["issue_id"] = iid
            detail["url"] = url
            detail["list_title"] = str(iss.get("title") or "")[:240]
            detail["list_subtitle"] = str(iss.get("subtitle") or "")[:240]
            out[iid] = detail
        except Exception as exc:  # noqa: BLE001
            out[iid] = {
                "issue_id": iid,
                "url": url,
                "error": str(exc)[:200],
            }
    return out


def _open_issue_category_menu(page) -> bool:
    """'Sorun kategorisi' combobox'ını aç."""
    try:
        for name in ("Sorun kategorisi", "Issue category"):
            loc = page.get_by_label(name, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=3_000)
                return True
        for name in ("Sorun kategorisi", "Issue category"):
            loc = page.get_by_text(name, exact=True)
            if loc.count() > 0:
                # Etiketin yakınındaki combobox / button
                handle = loc.first.element_handle()
                if handle:
                    clicked = page.evaluate(
                        """(el) => {
                          const root = el.closest('div') || el.parentElement;
                          const btn = root && root.querySelector(
                            'button, [role="combobox"], [aria-haspopup="listbox"]'
                          );
                          if (btn) { btn.click(); return true; }
                          el.click();
                          return true;
                        }""",
                        handle,
                    )
                    if clicked:
                        return True
        combos = page.locator('[role="combobox"], button[aria-haspopup="listbox"]')
        n = min(combos.count(), 8)
        for i in range(n):
            t = (combos.nth(i).inner_text(timeout=1_000) or "").strip()
            if re.search(
                r"Genel|Üretimde|Olası|Analiz|Overall|production|potential|analysis",
                t,
                re.I,
            ):
                combos.nth(i).click(timeout=3_000)
                return True
    except Exception:
        pass
    try:
        return bool(
            page.evaluate(
                """() => {
              const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
              const nodes = Array.from(document.querySelectorAll(
                'button, [role="combobox"], [aria-haspopup="listbox"], [role="button"]'
              ));
              for (const el of nodes) {
                const t = clean(el.innerText || el.textContent || '');
                const aria = clean(el.getAttribute('aria-label') || '');
                if (/sorun kategorisi|issue category/i.test(t + ' ' + aria)) {
                  el.click(); return true;
                }
                if (/^(Genel|Üretimde|Olası düzeltmeler|Analiz içeren|Overall|In production|Including)/i.test(t)
                    && t.length < 80) {
                  el.click(); return true;
                }
              }
              return false;
            }"""
            )
        )
    except Exception:
        return False


def _click_issue_category_option(page, labels: tuple[str, ...]) -> bool:
    """Açık menüden kategori seç."""
    for label in labels:
        try:
            opt = page.get_by_role("option", name=re.compile(rf"^{re.escape(label)}\\b", re.I))
            if opt.count() > 0:
                opt.first.click(timeout=3_000)
                return True
        except Exception:
            pass
        try:
            # Menü öğesi: başlık + açıklama — exact başlık satırı
            loc = page.locator('[role="option"], [role="menuitem"]').filter(
                has_text=re.compile(rf"^{re.escape(label)}\\b", re.I)
            )
            if loc.count() > 0:
                loc.first.click(timeout=3_000)
                return True
        except Exception:
            pass
        try:
            tloc = page.get_by_text(label, exact=True)
            if tloc.count() > 0:
                tloc.first.click(timeout=3_000)
                return True
        except Exception:
            pass
    try:
        return bool(
            page.evaluate(
                """(labels) => {
              const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
              const opts = Array.from(document.querySelectorAll(
                '[role="option"], [role="menuitem"], li[role="presentation"], div[role="option"]'
              ));
              for (const label of labels) {
                const want = clean(label).toLowerCase();
                for (const el of opts) {
                  const t = clean(el.innerText || el.textContent || '');
                  if (!t) continue;
                  const head = t.split('\\n')[0].toLowerCase();
                  if (head === want || head.startsWith(want)) {
                    el.click();
                    return true;
                  }
                }
              }
              return false;
            }""",
                list(labels),
            )
        )
    except Exception:
        return False


def _scrape_vitals_crashes_error_type(
    page,
    *,
    error_type: str,
    days: int = 28,
    headed: bool = True,
    version_code: str | None = None,
    scrape_details: bool = True,
) -> dict[str, Any]:
    vc = version_code if version_code is not None else _vitals_version_code()
    url = _vitals_crashes_url(error_type, days=days, version_code=vc)
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    _settle(page, seconds=5.0)
    need, _, _ = _page_needs_login(page)
    if need and headed:
        _wait_until_console(page, timeout_sec=300)
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=5.0)
    _wait_page_text(
        page,
        (
            "Sorun kategorisi",
            "Issue category",
            "Kilitlenme",
            "ANR",
            "Crash",
            "Genel",
            "Overall",
            "Etkilenen",
            "Affected",
        ),
        timeout_sec=40.0,
    )
    _scroll_full_page(page)

    categories_out: list[dict[str, Any]] = []
    general_issues: list[dict[str, Any]] = []
    for cat in VITALS_ISSUE_CATEGORIES:
        cat_id = str(cat["id"])
        labels = tuple(cat["labels"])
        selected_ok = False
        # Genel çoğu zaman varsayılan — yine de menüden seçmeyi dene
        if _open_issue_category_menu(page):
            time.sleep(0.6)
            selected_ok = _click_issue_category_option(page, labels)
            if selected_ok:
                _settle(page, seconds=3.5)
                _wait_page_text(page, labels[:2], timeout_sec=12.0)
            else:
                # Menüyü Escape ile kapat
                try:
                    page.keyboard.press("Escape")
                except Exception:
                    pass
                time.sleep(0.3)

        snap = _extract_vitals_issue_snapshot(page) or {}
        issues = snap.get("issues") if isinstance(snap.get("issues"), list) else []
        cards = snap.get("cards") if isinstance(snap.get("cards"), list) else []
        count_raw = snap.get("issue_count")
        if count_raw is None:
            count_raw = str(len(issues)) if issues else None
        # Normalize issue dicts
        clean_issues: list[dict[str, Any]] = []
        for iss in issues[:50]:
            if not isinstance(iss, dict):
                continue
            title = str(iss.get("title") or "").strip()
            title = re.sub(
                r"\s*(ayrıntısını göster|show details?|view details?)\s*$",
                "",
                title,
                flags=re.I,
            ).strip()
            if not title and not iss.get("issue_id"):
                continue
            detail_url = str(iss.get("detail_url") or "").strip()
            iid = str(iss.get("issue_id") or "").strip()
            if iid and not detail_url:
                detail_url = _vitals_issue_detail_url(
                    iid, days=days, version_code=vc
                )
            clean_issues.append(
                {
                    "issue_id": iid,
                    "detail_url": detail_url,
                    "title": title[:240],
                    "subtitle": str(iss.get("subtitle") or "")[:240],
                    "tags": [
                        str(t)[:80]
                        for t in (iss.get("tags") or [])
                        if str(t).strip()
                    ][:6],
                    "issue_type": str(iss.get("issue_type") or "")[:64],
                    "affected_versions": str(iss.get("affected_versions") or "")[:80],
                    "version_track": str(iss.get("version_track") or "")[:64],
                    "users": str(iss.get("users") or "")[:32],
                    "events": str(iss.get("events") or "")[:32],
                    "events_share": str(iss.get("events_share") or "")[:32],
                    "last_occurrence": str(iss.get("last_occurrence") or "")[:64],
                    "extra": str(iss.get("extra") or "")[:120],
                }
            )
        if cat_id == "general":
            general_issues = list(clean_issues)
        categories_out.append(
            {
                "id": cat_id,
                "label": cat["label"],
                "description": cat.get("description") or "",
                "selected_ok": selected_ok or cat_id == "general",
                "selected_label": snap.get("selected_category") or "",
                "issue_count": count_raw,
                "cards": cards[:8],
                "issues": clean_issues,
                "issue_row_count": len(clean_issues),
            }
        )
        print(
            f"    · {error_type}/{cat_id}: issues={len(clean_issues)} "
            f"count={snap.get('issue_count')} selected={selected_ok}",
            flush=True,
        )

    issue_details: dict[str, dict[str, Any]] = {}
    if scrape_details and general_issues:
        print(f"  · vitals issue details ({error_type}) …", flush=True)
        issue_details = _scrape_vitals_issue_details(
            page,
            general_issues,
            days=days,
            version_code=vc,
            headed=headed,
        )
        # Listeye geri dön — sonraki error type / overview için temiz bağlam
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=2.5)
        except Exception:
            pass

    return {
        "error_type": error_type.upper(),
        "url": url,
        "days": days,
        "version_code": vc or None,
        "is_user_perceived": True,
        "categories": categories_out,
        "category_count": len(categories_out),
        "issue_details": issue_details,
        "issue_detail_count": len(issue_details),
    }


def _extract_vitals_metrics_overview(page) -> dict[str, Any]:
    """Vitals metrics overview: crash / ANR / LMK oran tablosu."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);

      const metricRe = /kullanıcı tarafından algılanan\\s+(kilitlenme|anr|lmk)\\s+oranı|user[- ]perceived\\s+(crash|anr|lmk)\\s+rate/i;
      const rows = [];
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (!metricRe.test(line)) continue;
        const vals = [];
        for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
          const l = lines[j];
          if (metricRe.test(l)) break;
          // %0,03 veya +%0,01 veya -%0,08
          if (/^[%+\\-−]?\\s*%?\\s*\\d/.test(l) || /^\\d/.test(l) && /%/.test(l) || /^[+\\-−]%?\\d/.test(l)) {
            vals.push(l);
          } else if (/^\\d[,.]\\d+%?$/.test(l) || /^%\\d/.test(l)) {
            vals.push(l);
          }
          if (vals.length >= 3) break;
        }
        let key = 'other';
        if (/kilitlenme|crash/i.test(line)) key = 'crash';
        else if (/\\banr\\b/i.test(line)) key = 'anr';
        else if (/\\blmk\\b/i.test(line)) key = 'lmk';
        rows.push({
          key,
          metric: line,
          value_28d: vals[0] || '',
          vs_previous_28d: vals[1] || '',
          vs_peers_median: vals[2] || '',
        });
      }

      // Tablo hücrelerinden yedek parse
      if (!rows.length) {
        const trs = Array.from(document.querySelectorAll('tr'));
        for (const tr of trs) {
          const cells = Array.from(tr.querySelectorAll('th,td')).map((c) => clean(c.innerText));
          if (cells.length < 2) continue;
          const title = cells[0];
          if (!metricRe.test(title)) continue;
          let key = 'other';
          if (/kilitlenme|crash/i.test(title)) key = 'crash';
          else if (/\\banr\\b/i.test(title)) key = 'anr';
          else if (/\\blmk\\b/i.test(title)) key = 'lmk';
          rows.push({
            key,
            metric: title,
            value_28d: cells[1] || '',
            vs_previous_28d: cells[2] || '',
            vs_peers_median: cells[3] || '',
          });
        }
      }

      return { rows, body_len: body.length };
    }"""
    )


def _scrape_vitals_metrics_overview(page, *, headed: bool = True) -> dict[str, Any]:
    url = VITALS_METRICS_OVERVIEW_URL
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    _settle(page, seconds=5.0)
    need, _, _ = _page_needs_login(page)
    if need and headed:
        _wait_until_console(page, timeout_sec=300)
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=5.0)
    _wait_page_text(
        page,
        (
            "Kullanıcı tarafından algılanan",
            "User-perceived",
            "ANR",
            "kilitlenme",
            "LMK",
            "Vital",
        ),
        timeout_sec=40.0,
    )
    _scroll_full_page(page)
    extracted = _extract_vitals_metrics_overview(page) or {}
    rows = extracted.get("rows") if isinstance(extracted.get("rows"), list) else []

    # ANR satırına tıkla → kırılımlı crashes ANR görünümü (URL/context)
    anr_drill: dict[str, Any] = {}
    try:
        clicked = page.evaluate(
            """() => {
          const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
          const nodes = Array.from(document.querySelectorAll('a, button, tr, [role="row"], [role="link"]'));
          for (const el of nodes) {
            const t = clean(el.innerText || el.textContent || '');
            if (/kullanıcı tarafından algılanan anr|user[- ]perceived anr/i.test(t) && t.length < 200) {
              el.click();
              return true;
            }
          }
          return false;
        }"""
        )
        if clicked:
            _settle(page, seconds=4.0)
            anr_drill = {
                "url": page.url,
                "snapshot": _extract_vitals_issue_snapshot(page),
            }
            # Overview'a geri dön
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=3.0)
    except Exception as exc:  # noqa: BLE001
        anr_drill = {"error": str(exc)[:160]}

    # Aynı metrik birden fazla geçebilir — peer karşılaştırması dolu olanı tercih et
    by_key: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = str(r.get("key") or "other")
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = r
            continue
        prev_peer = str(prev.get("vs_peers_median") or "").strip()
        new_peer = str(r.get("vs_peers_median") or "").strip()
        if (not prev_peer and new_peer) or (len(new_peer) > len(prev_peer)):
            by_key[key] = r
    rows = [by_key[k] for k in ("crash", "anr", "lmk") if k in by_key]
    rows.extend(by_key[k] for k in by_key if k not in ("crash", "anr", "lmk"))

    print(f"    · metrics overview rows={len(rows)}", flush=True)
    return {
        "url": url,
        "rows": rows,
        "row_count": len(rows),
        "anr_drilldown": anr_drill,
    }


def _scrape_vitals_bundle(page, *, headed: bool = True, days: int = 28) -> dict[str, Any]:
    """Crashes 4 kategori (CRASH+ANR) + metrics overview tablosu."""
    version_name_map: dict[str, str] = {}
    vc = _vitals_version_code()
    if vc:
        print(f"  · vitals versionCode={vc}", flush=True)
    print("  · vitals crashes (CRASH) …", flush=True)
    crash = _scrape_vitals_crashes_error_type(
        page, error_type="CRASH", days=days, headed=headed, version_code=vc
    )
    version_name_map = _merge_version_name_maps(
        version_name_map, _extract_version_name_map(page)
    )
    # Detaylardan da sürüm etiketleri topla
    for det in (crash.get("issue_details") or {}).values():
        if isinstance(det, dict):
            version_name_map = _merge_version_name_maps(
                version_name_map,
                {
                    str(m.group(1)): str(m.group(2))
                    for m in re.finditer(
                        r"\b(\d{2,8})\s*\((\d+\.\d+(?:\.\d+)*)\)",
                        " ".join(
                            [
                                str(det.get("title") or ""),
                                str(det.get("list_title") or ""),
                                " ".join(
                                    str(c.get("value") or "")
                                    for c in (det.get("summary_cards") or [])
                                    if isinstance(c, dict)
                                ),
                            ]
                        ),
                    )
                },
            )
    print("  · vitals crashes (ANR) …", flush=True)
    anr = _scrape_vitals_crashes_error_type(
        page, error_type="ANR", days=days, headed=headed, version_code=vc
    )
    version_name_map = _merge_version_name_maps(
        version_name_map, _extract_version_name_map(page)
    )
    print("  · vitals metrics overview …", flush=True)
    overview = _scrape_vitals_metrics_overview(page, headed=headed)
    version_name_map = _merge_version_name_maps(
        version_name_map, _extract_version_name_map(page)
    )
    # Liste satırlarındaki 290 (9.5.10)
    for block in (crash, anr):
        for cat in block.get("categories") or []:
            if not isinstance(cat, dict):
                continue
            for iss in cat.get("issues") or []:
                if not isinstance(iss, dict):
                    continue
                av = str(iss.get("affected_versions") or "")
                m = re.match(r"^(\d{2,8})\s*\(([^)]+)\)$", av.strip())
                if m:
                    version_name_map[m.group(1)] = m.group(2).strip()
    if version_name_map:
        print(f"    · version_name_map={len(version_name_map)}", flush=True)
    detail_n = int(crash.get("issue_detail_count") or 0) + int(
        anr.get("issue_detail_count") or 0
    )
    print(f"    · issue_details total={detail_n}", flush=True)
    return {
        "version": 2,
        "days": days,
        "version_code": vc or None,
        "is_user_perceived": True,
        "crashes": {"CRASH": crash, "ANR": anr},
        "metrics_overview": overview,
        "version_name_map": version_name_map,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


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

        # Android Vitals: crashes 4 kategori + metrics overview (crash/ANR/LMK)
        vitals_bundle: dict[str, Any] = {}
        try:
            vitals_bundle = _scrape_vitals_bundle(page, headed=bool(headed), days=28)
        except Exception as exc:  # noqa: BLE001
            vitals_bundle = {"version": 1, "error": str(exc)[:240]}
            print(f"  · vitals scrape hata: {exc}", flush=True)

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
        version_name_map = _merge_version_name_maps(
            _extract_version_name_map(page),
            (vitals_bundle.get("version_name_map")
             if isinstance(vitals_bundle.get("version_name_map"), dict)
             else {}),
        )
        panels = {
            "version": 3,
            "tpg": dash_cards,
            "breakdowns": all_br,
            "monetize": mon_cards,
            "grow": grow_cards,
            "monitor": monitor_cards,
            "release": release_cards,
            "statistics": stats_cards,
            "vitals": vitals_bundle,
            "version_name_map": version_name_map,
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
                "vitals_crashes": {
                    "url": _vitals_crashes_url("CRASH", days=28),
                    "anr_url": _vitals_crashes_url("ANR", days=28),
                },
                "vitals_metrics_overview": {
                    "url": VITALS_METRICS_OVERVIEW_URL,
                },
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
            "vitals_category_count": sum(
                len((vitals_bundle.get("crashes") or {}).get(k, {}).get("categories") or [])
                for k in ("CRASH", "ANR")
            ),
            "vitals_overview_row_count": len(
                (vitals_bundle.get("metrics_overview") or {}).get("rows") or []
            ),
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

        # Reviews sayfası — son 1 yıl (scroll + tarih filtresi)
        print("  · reviews (last year) …", flush=True)
        need_r = False
        try:
            page.goto(
                f"{REVIEWS_URL}?days={_reviews_days()}",
                wait_until="domcontentloaded",
                timeout=120_000,
            )
            _settle(page, seconds=3.0)
            need_r, _, _ = _page_needs_login(page)
        except Exception:
            need_r = True
        if need_r and headed:
            _wait_until_console(page, timeout_sec=300)
        reviews = _scrape_reviews_list(page, days=_reviews_days())
        # Ratings özeti reviews’ta da olabilir — boşsa doldur
        if not rating_summary.get("default_rating"):
            rating_summary = {**rating_summary, **(_extract_rating_summary_dom(page) or {})}

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
            or (vitals_bundle.get("metrics_overview") or {}).get("rows")
            or (vitals_bundle.get("crashes") or {}).get("CRASH")
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
            f"{panels.get('breakdown_count', 0)} kırılım · "
            f"vitals_cat={panels.get('vitals_category_count', 0)} "
            f"vitals_ov={panels.get('vitals_overview_row_count', 0)} · "
            f"{len(reviews)} review{dbg}"
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
            "sync_mode": (
                "dashboard_monetize_grow_monitor_release_vitals_"
                "stats_catalog_ratings_reviews"
            ),
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


def scrape_vitals_only(*, headed: bool | None = None) -> dict[str, Any]:
    """Sadece Android Vitals (crashes 4 kategori + metrics overview) — hızlı sync."""
    if headed is None:
        env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
        headed = env_hl not in ("1", "true", "yes")
    pw, context = _launch_context(headed=True if headed else False)
    try:
        page = context.new_page()
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=3.0)
        need, _, _ = _page_needs_login(page)
        if need and headed:
            _wait_until_console(page, timeout_sec=300)
            page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=3.0)
        elif need:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Play Console login gerekli (--login veya headed sync)",
                "panels": {},
            }
        vitals_bundle = _scrape_vitals_bundle(page, headed=bool(headed), days=28)
        ov_n = len((vitals_bundle.get("metrics_overview") or {}).get("rows") or [])
        cat_n = sum(
            len((vitals_bundle.get("crashes") or {}).get(k, {}).get("categories") or [])
            for k in ("CRASH", "ANR")
        )
        panels = {
            "version": 3,
            "vitals": vitals_bundle,
            "version_name_map": (
                vitals_bundle.get("version_name_map")
                if isinstance(vitals_bundle.get("version_name_map"), dict)
                else {}
            ),
            "pages": {
                "vitals_crashes": {
                    "url": _vitals_crashes_url("CRASH", days=28),
                    "anr_url": _vitals_crashes_url("ANR", days=28),
                },
                "vitals_metrics_overview": {"url": VITALS_METRICS_OVERVIEW_URL},
            },
            "vitals_category_count": cat_n,
            "vitals_overview_row_count": ov_n,
        }
        ok = bool(ov_n or cat_n)
        return {
            "ok": ok,
            "needs_login": False,
            "message": f"Vitals scrape · categories={cat_n} overview_rows={ov_n}",
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": [],
            "panels": panels,
            "reviews": None,  # ingest'te mevcut reviews korunur
            "rating_summary": None,
            "merge_vitals": True,
            "source": "play_console_bridge",
            "source_url": VITALS_METRICS_OVERVIEW_URL,
            "sync_ok": ok,
            "sync_mode": "vitals_only",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Vitals scrape hata: {exc}",
            "panels": {},
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


def scrape_reviews_only(*, headed: bool | None = None) -> dict[str, Any]:
    """Sadece yorumlar (son 1 yıl) — mevcut panels korunur."""
    if headed is None:
        env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
        headed = env_hl not in ("1", "true", "yes")
    pw, context = _launch_context(headed=True if headed else False)
    try:
        page = context.new_page()
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=3.0)
        need, _, _ = _page_needs_login(page)
        if need and headed:
            _wait_until_console(page, timeout_sec=300)
        elif need:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Play Console login gerekli (--login veya headed sync)",
                "reviews": [],
            }
        print("  · reviews-only (last year) …", flush=True)
        reviews = _scrape_reviews_list(page, days=_reviews_days())
        rating_summary = _extract_rating_summary_dom(page) or {}
        ok = bool(reviews)
        return {
            "ok": ok,
            "needs_login": False,
            "message": f"Reviews scrape · {len(reviews)} review · days={_reviews_days()}",
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": [],
            "panels": {},
            "reviews": reviews,
            "rating_summary": rating_summary,
            "merge_reviews": True,
            "source": "play_console_bridge",
            "source_url": REVIEWS_URL,
            "sync_ok": ok,
            "sync_mode": "reviews_only",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Reviews scrape hata: {exc}",
            "reviews": None,  # fail ingest'te mevcut yorumları ezme
            "merge_reviews": False,
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


def _snapshot_cache_path() -> Path:
    return PROFILE_DIR.parent / "play-console-last-full.json"


def _save_snapshot_cache(result: dict[str, Any]) -> None:
    """Full/vitals sync sonrası yerel yedek — reviews-only eski sunucuda ezmesin."""
    try:
        panels = result.get("panels")
        metrics = result.get("metrics")
        if not isinstance(panels, dict) or not panels:
            return
        # vitals-only: mevcut cache panelleriyle birleştir
        path = _snapshot_cache_path()
        base: dict[str, Any] = {}
        if path.is_file():
            try:
                base = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                base = {}
        out = {
            "metrics": metrics if metrics is not None else base.get("metrics") or [],
            "panels": panels
            if not result.get("merge_vitals")
            else {
                **(base.get("panels") or {}),
                **panels,
                "vitals": panels.get("vitals") or (base.get("panels") or {}).get("vitals"),
            },
            "reviews": result.get("reviews")
            if result.get("reviews") is not None
            else base.get("reviews") or [],
            "rating_summary": result.get("rating_summary")
            if result.get("rating_summary") is not None
            else base.get("rating_summary") or {},
            "package_name": result.get("package_name") or PACKAGE,
            "app_id": result.get("app_id") or APP_ID,
            "source_url": result.get("source_url") or DASHBOARD_URL,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    import requests

    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}

    panels = result.get("panels") or {}
    metrics = result.get("metrics")
    reviews = result.get("reviews")
    rating_summary = result.get("rating_summary")
    merge_vitals = bool(result.get("merge_vitals"))
    merge_reviews = bool(result.get("merge_reviews"))

    # vitals-only: birleştirme sunucuda yapılır (snapshot admin auth ister)
    if merge_vitals:
        if not isinstance(panels, dict) or not panels.get("vitals"):
            return {"ok": False, "message": "merge_vitals için panels.vitals gerekli"}
        payload = {
            "metrics": [],
            "panels": panels,
            "reviews": [],
            "rating_summary": {},
            "raw_network": result.get("raw_network") or [],
            "source": result.get("source") or "play_console_bridge",
            "source_url": result.get("source_url") or DASHBOARD_URL,
            "package_name": result.get("package_name") or PACKAGE,
            "app_id": result.get("app_id") or APP_ID,
            "sync_ok": bool(result.get("ok")),
            "sync_message": result.get("message"),
            "sync_mode": result.get("sync_mode") or "vitals_only",
            "merge_vitals": True,
            "merge_reviews": False,
        }
    elif merge_reviews:
        if not isinstance(reviews, list) or not reviews:
            return {"ok": False, "message": "merge_reviews için reviews gerekli"}
        # Eski sunucu merge_reviews bilmiyorsa boş panels ile ezer — yerel cache ile full restore gönder
        cache: dict[str, Any] = {}
        try:
            cpath = _snapshot_cache_path()
            if cpath.is_file():
                cache = json.loads(cpath.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
        cached_panels = cache.get("panels") if isinstance(cache.get("panels"), dict) else {}
        cached_metrics = cache.get("metrics") if isinstance(cache.get("metrics"), list) else []
        if cached_panels:
            payload = {
                "metrics": cached_metrics,
                "panels": cached_panels,
                "reviews": reviews,
                "rating_summary": (
                    rating_summary
                    if isinstance(rating_summary, dict) and rating_summary
                    else (cache.get("rating_summary") or {})
                ),
                "raw_network": result.get("raw_network") or [],
                "source": result.get("source") or "play_console_bridge",
                "source_url": result.get("source_url") or REVIEWS_URL,
                "package_name": result.get("package_name") or PACKAGE,
                "app_id": result.get("app_id") or APP_ID,
                "sync_ok": bool(result.get("ok")),
                "sync_message": result.get("message"),
                "sync_mode": "reviews_only_full_restore",
                "merge_vitals": False,
                "merge_reviews": False,
            }
        else:
            payload = {
                "metrics": [],
                "panels": {},
                "reviews": reviews,
                "rating_summary": rating_summary if isinstance(rating_summary, dict) else {},
                "raw_network": result.get("raw_network") or [],
                "source": result.get("source") or "play_console_bridge",
                "source_url": result.get("source_url") or REVIEWS_URL,
                "package_name": result.get("package_name") or PACKAGE,
                "app_id": result.get("app_id") or APP_ID,
                "sync_ok": bool(result.get("ok")),
                "sync_message": result.get("message"),
                "sync_mode": result.get("sync_mode") or "reviews_only",
                "merge_vitals": False,
                "merge_reviews": True,
            }
    else:
        payload = {
            "metrics": metrics if metrics is not None else [],
            "panels": panels if isinstance(panels, dict) else {},
            "reviews": reviews if reviews is not None else [],
            "rating_summary": rating_summary if rating_summary is not None else {},
            "raw_network": result.get("raw_network") or [],
            "source": result.get("source") or "play_console_bridge",
            "source_url": result.get("source_url") or DASHBOARD_URL,
            "package_name": result.get("package_name") or PACKAGE,
            "app_id": result.get("app_id") or APP_ID,
            "sync_ok": bool(result.get("ok")),
            "sync_message": result.get("message"),
            "sync_mode": result.get("sync_mode") or "dashboard_reviews",
            "merge_vitals": False,
            "merge_reviews": False,
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
    vitals_only = "--vitals-only" in args
    reviews_only = "--reviews-only" in args
    # --sync implies scrape (+ ingest if token)
    mode = "vitals-only" if vitals_only else ("reviews-only" if reviews_only else "")
    print(
        f"Play scrape · headed={headed}"
        + (f" · {mode}" if mode else ""),
        flush=True,
    )
    if vitals_only:
        result = scrape_vitals_only(headed=headed)
    elif reviews_only:
        result = scrape_reviews_only(headed=headed)
    else:
        result = scrape_play_console(headed=headed)
    print(json.dumps({k: v for k, v in result.items() if k != "raw_network"}, ensure_ascii=False, indent=2))
    if result.get("needs_login"):
        return 2
    if result.get("ok") and (
        (isinstance(result.get("panels"), dict) and result.get("panels"))
        or result.get("merge_vitals")
    ):
        _save_snapshot_cache(result)
    # reviews-only: cache'e yeni yorumları yaz
    if result.get("ok") and result.get("merge_reviews") and isinstance(result.get("reviews"), list):
        try:
            cpath = _snapshot_cache_path()
            if cpath.is_file():
                cached = json.loads(cpath.read_text(encoding="utf-8"))
                cached["reviews"] = result.get("reviews")
                if isinstance(result.get("rating_summary"), dict) and result.get("rating_summary"):
                    cached["rating_summary"] = result.get("rating_summary")
                cpath.write_text(json.dumps(cached, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
    if do_ingest and result.get("ok"):
        ing = ingest_scrape_result(result)
        print("INGEST:", json.dumps(ing, ensure_ascii=False, indent=2))
        return 0 if ing.get("ok") else 1
    if do_ingest and not result.get("ok"):
        # Kısmi merge / boş payload ile mevcut snapshot'ı ezme
        if result.get("merge_vitals") or result.get("merge_reviews") or reviews_only or vitals_only:
            print(
                "INGEST skipped (fail + merge/partial mode) — mevcut Railway snapshot korunur",
                flush=True,
            )
            return 1
        # başarısız full scrape'i de kaydet (UI'da mesaj görünsün)
        ing = ingest_scrape_result(result)
        print("INGEST (fail state):", json.dumps(ing, ensure_ascii=False, indent=2))
        return 1
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
