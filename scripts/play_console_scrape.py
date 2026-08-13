#!/usr/bin/env python3
"""Google Play Console scrape (Mac bridge).

İlk giriş (headed — bir kez):
  .venv/bin/python scripts/play_console_scrape.py --login

Sync (varsayılan headed; Google headless’ta session düşürür):
  .venv/bin/python scripts/play_console_scrape.py --sync --ingest

Sadece vitals / yorumlar / puan dağılımı:
  .venv/bin/python scripts/play_console_scrape.py --vitals-only --sync --ingest
  .venv/bin/python scripts/play_console_scrape.py --reviews-only --sync --ingest
  .venv/bin/python scripts/play_console_scrape.py --ratings-dist-only --sync --ingest

Env:
  PLAY_CONSOLE_DEVELOPER_ID  (default 7587799419591090593)
  PLAY_CONSOLE_APP_ID        (default 4974102243818231576)
  PLAY_CONSOLE_PACKAGE       (default com.Doviz)
  PLAY_CONSOLE_PROFILE_DIR   (default ~/.seo-agent/fx-google)
  PLAY_CONSOLE_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
  PLAY_CONSOLE_VITALS_ROW_NAV=1   # sorun tablosu satır satır gez (varsayılan açık)
  PLAY_CONSOLE_VITALS_DETAIL_LIMIT=40
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
from backend.services.scrape_browser import (
    google_blocks_automation_text,
    google_profile_dir,
    normalize_nav_url,
)

PROFILE_DIR = google_profile_dir()
BASE_APP = f"https://play.google.com/console/u/0/developers/{DEV_ID}/app/{APP_ID}"
_DEFAULT_DASHBOARD = f"{BASE_APP}/app-dashboard"
DASHBOARD_URL = normalize_nav_url(
    os.environ.get("PLAY_CONSOLE_DASHBOARD_URL") or _DEFAULT_DASHBOARD,
    fallback=_DEFAULT_DASHBOARD,
)
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
STORE_LISTINGS_URL = (
    os.environ.get("PLAY_CONSOLE_STORE_LISTINGS_URL")
    or f"{BASE_APP}/store-listings?metric=METRIC_ACQUISITION"
).strip()
MONITOR_URL = (
    os.environ.get("PLAY_CONSOLE_MONITOR_URL") or f"{BASE_APP}/monitor"
)
# Reach and devices · Overview (kullanıcı peerset + Android sürüm kırılımı)
_DEVICES_PEERSET = os.environ.get("PLAY_CONSOLE_DEVICES_PEERSET") or "3%3A6a1f18dbb44333cd"
DEVICES_URL = (
    os.environ.get("PLAY_CONSOLE_DEVICES_URL")
    or (
        f"{BASE_APP}/devices/dashboard"
        f"?days=28&peerset_key={_DEVICES_PEERSET}"
        f"&expanded_breakdowns=ANDROID_VERSION"
    )
)
# Alt kırılımlar — Play Console expanded_breakdowns
DEVICES_BREAKDOWNS: tuple[str, ...] = (
    "ANDROID_VERSION",
    "RAM",
    "SYSTEM_ON_CHIP",
    "GPU",
    "OPENGL_ES_VERSION",
    "VULKAN_VERSION",
    "SCREEN_METRICS",
    "ABI",
    "DEVICE_TYPE",
    "FORM_FACTOR",
)
RELEASE_URL = (
    os.environ.get("PLAY_CONSOLE_RELEASE_URL") or f"{BASE_APP}/test-and-release"
).strip()
VITALS_CRASHES_BASE = f"{BASE_APP}/vitals/crashes"
VITALS_METRICS_OVERVIEW_URL = (
    os.environ.get("PLAY_CONSOLE_VITALS_METRICS_URL")
    or f"{BASE_APP}/vitals/metrics/overview?peersetKey=1%3A7f5887b4"
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
        "dimension_values": "OVERALL%2CTR%2CDE%2CBG%2CBE",
        "needles": ("Edinme", "Acquisition", "Mağaza", "İstatistik", "Veri tablosu"),
    },
    {
        "id": "user_acquisition",
        "label": "Kullanıcı edinme",
        "metric_key": "user_acquisition",
        "metrics": "USER_ACQUISITION-ALL-EVENTS-PER_INTERVAL-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CBG%2CBE",
        "needles": (
            "Kullanıcı edinme",
            "User acquisition",
            "Edinme",
            "İstatistik",
            "Veri tablosu",
        ),
    },
    {
        "id": "store_listing_conversion",
        "label": "Mağaza dönüşüm oranı",
        "metric_key": "store_listing_conversion",
        "metrics": "STORE_LISTING_CONVERSION_RATE-ALL-COUNT_UNSPECIFIED-PER_INTERVAL-DAY",
        # Ülke kırılımı bu metrikte sık boş protobuf döndürüyor — OVERALL + tablo fallback
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL",
        "force_table": True,
        "needles": (
            "Dönüşüm oranı",
            "Conversion rate",
            "Store listing conversion",
            "Mağaza girişi",
            "Dönüşüm",
            "Conversion",
            "İstatistik",
            "Veri tablosu",
        ),
    },
    {
        "id": "dau_mau",
        "label": "DAU/MAU",
        "metric_key": "dau_mau",
        "metrics": "DAU_MAU-ACQUISITION_UNSPECIFIED-COUNT_UNSPECIFIED-CALCULATION_UNSPECIFIED-DAY",
        "dimension": "COUNTRY",
        "dimension_values": "OVERALL%2CTR%2CDE%2CBG%2CBE",
        "needles": ("DAU/MAU", "DAU", "MAU", "İstatistik", "Veri tablosu"),
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
        "id": "anrs_device",
        "label": "ANR · cihaz",
        "metric_key": "anrs",
        "metrics": _ANR_METRICS,
        "dimension": "DEVICE_MODEL",
        "dimension_values": "OVERALL",
        "dim_hint": "device",
        "needles": ("ANR", "Cihaz", "Device", "Veri tablosu", "İstatistik"),
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
        "id": "crashes_device",
        "label": "Çökme · cihaz",
        "metric_key": "crashes",
        "metrics": _CRASH_METRICS,
        "dimension": "DEVICE_MODEL",
        "dimension_values": "OVERALL",
        "dim_hint": "device",
        "needles": ("Kilitlenme", "Crash", "Cihaz", "Device", "Veri tablosu", "İstatistik"),
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
    "Kullanıcı edinme",
    "User acquisition",
    "Kitle büyüme oranı",
    "Günlük etkin kullanıcı sayısı",
    "Etkin cihazlar",
    "Uygulamayı yükleyen kullanıcı sayısı",
    "Yüklemeler",
    "Toplam yükleme sayısı",
    "Kullanıcı kaybı",
    "Yeni cihaz edinme",
)
_KNOWN_STORE_LISTINGS = (
    "Mağaza girişi ziyaretçileri",
    "Mağaza girişi edinme sayısı",
    "Mağaza girişi dönüşüm oranı",
    "Store listing visitors",
    "Store listing acquisitions",
    "Store listing conversion",
    "Edinme",
    "Acquisition",
    "Ziyaretçi",
    "Visitor",
    "Dönüşüm",
    "Conversion",
    "Varsayılan mağaza girişi",
    "Default store listing",
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
    "Kullanıcı edinme",
    "User acquisition",
    "Mağaza girişi ziyaretçileri",
    "Mağaza girişi edinme sayısı",
    "Mağaza girişi dönüşüm oranı",
    "Store listing visitors",
    "Store listing acquisitions",
    "Store listing conversion",
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
_KNOWN_DEVICES = (
    "Yükleme tabanı",
    "Install base",
    "Etkin cihazlar",
    "Active devices",
    "Kilitlenme oranı",
    "Crash rate",
    "ANR oranı",
    "ANR rate",
    "Kullanıcı tarafından algılanan kilitlenme oranı",
    "User-perceived crash rate",
    "Kullanıcı tarafından algılanan ANR oranı",
    "User-perceived ANR rate",
    "Günlük etkin kullanıcı sayısı",
    "Daily active users",
    "DAU",
    "AEKS",
    "Cihaz edinme sayısı",
    "Device acquisition",
    "Yeni cihaz edinme",
    "Kullanıcı kaybı",
    "User lost",
    "Google Play puanı",
    "Ortalama puan",
    "Slow rendering",
    "Excessive wakeups",
    "Stuck partial wake locks",
    "LMK",
    "Uygulama boyutu",
    "App size",
    "Erişim",
    "Reach",
    "Cihazlar",
    "Devices",
    "Android sürümü",
    "Android version",
    "RAM",
    "SoC",
    "GPU",
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


def _kill_stale_profile_browsers(profile_dir: Path) -> int:
    """Kill Chromium/Chrome still holding this persistent profile (SingletonLock)."""
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
        if "Chromium" not in line and "Google Chrome" not in line and "chrome" not in line.lower():
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
        time.sleep(0.8)
        # Force leftover parents
        try:
            out2 = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
        except Exception:
            out2 = ""
        for line in out2.splitlines():
            if marker not in line:
                continue
            try:
                pid = int(line.split(None, 1)[0])
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        time.sleep(0.3)
    return killed


def _clear_profile_singleton_locks(profile_dir: Path) -> None:
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (profile_dir / name).unlink(missing_ok=True)
        except Exception:
            pass


_CDP_ATTACHED: set[int] = set()


def _launch_context(*, headed: bool):
    from backend.services.selenium_playwright_shim import (
        launch_selenium_context,
        play_console_use_selenium,
    )

    if play_console_use_selenium():
        return launch_selenium_context(PROFILE_DIR, headed=headed)

    from backend.services.store_session_cdp import attach_or_launch

    pw, context, attached = attach_or_launch("play", headed=headed)
    if attached:
        _CDP_ATTACHED.add(id(context))
        print("Play: kalıcı Firefox profili", flush=True)
    return pw, context


def _release_context(pw, context) -> None:
    if getattr(context, "_selenium_mode", False):
        from backend.services.selenium_playwright_shim import release_selenium_context

        release_selenium_context(pw, context)
        return

    from backend.services.store_session_cdp import release_browser

    attached = id(context) in _CDP_ATTACHED
    _CDP_ATTACHED.discard(id(context))
    release_browser(pw, context, attached=attached)


def _page_is_alive(page) -> bool:
    try:
        if page is None or page.is_closed():
            return False
        page.evaluate("() => true")
        return True
    except Exception:
        return False


def run_login_interactive(timeout_sec: int | None = None) -> dict[str, Any]:
    """Gerçek Firefox.app — Google Playwright otomasyonunu 'güvenli değil' diye reddeder."""
    from backend.services.scrape_browser import LOGIN_WAIT_SEC, launch_system_firefox_login, login_wait_sec

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    print(
        "Play Console girişi gerçek Firefox.app ile açılıyor "
        "(Playwright penceresinde Google girişi çalışmaz).\n"
        "E-postayı adres çubuğuna değil, Google formundaki alana yazın.",
        flush=True,
    )
    return launch_system_firefox_login(
        PROFILE_DIR,
        DASHBOARD_URL,
        timeout_sec=timeout_sec,
        success_hint=(
            f"cemevecen@nokta.com ile giriş yap → Play Console dashboard görünsün "
            f"(en fazla {timeout_sec // 60} dk) → Firefox penceresini KAPAT. "
            "(E-postayı adres çubuğuna yazma.)"
        ),
    )


def _system_firefox_relogin(*, timeout_sec: int | None = None) -> dict[str, Any]:
    from backend.services.scrape_browser import LOGIN_WAIT_SEC, launch_system_firefox_login, login_wait_sec

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))

    print(
        "Google 'güvenli değil' dedi — Playwright penceresi kapatılıyor, "
        "gerçek Firefox.app ile giriş açılıyor.",
        flush=True,
    )
    return launch_system_firefox_login(
        PROFILE_DIR,
        DASHBOARD_URL,
        timeout_sec=timeout_sec,
        success_hint=(
            f"Google 'güvenli değil' dedi — gerçek Firefox'ta giriş yap "
            f"(en fazla {timeout_sec // 60} dk), Play Console dashboard görünce kapat."
        ),
    )


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
      const hintRe = /yükleme|kilitlenme|anr|puan|cihaz|aeks|gelir|alıcı|etkin|kitle|mağaza|öykbog|edinme|kaybı|abonelik|satın|revenue|buyer|arppu|arpu|crash|çökme|dau|mau|reach|erişim|ram|soc|vulkan|opengl|install base|slow rendering|wake/i;

      const cards = [];
      const breakdowns = [];
      const seen = new Set();
      const seenBr = new Set();

      function cardHref(fromEl) {
        if (!fromEl || !fromEl.querySelector) return '';
        const a = fromEl.querySelector(
          'a[href*="/statistics"], a[href*="/vitals"], a[href*="/user-feedback"], a[href*="/monetize"], a[href*="/grow"], a[href*="/monitor"], a[href*="/devices"], a[href*="/test-and-release"], a[href*="/app-dashboard"], a[href*="play.google.com/console"]'
        );
        if (!a) return '';
        const href = String(a.href || a.getAttribute('href') || '').trim();
        if (!href || href === '#' || href.startsWith('javascript:')) return '';
        return href.slice(0, 512);
      }
      function pushCard(title, value, delta, period, url) {
        title = clean(title); value = clean(value); delta = clean(delta || ''); period = clean(period || '');
        url = clean(url || '');
        if (!title || !value || !isValue(value)) return;
        const key = title + '|' + value;
        if (seen.has(key)) return;
        seen.add(key);
        const row = { title, value, delta, period, kind: 'metric', page: pageKey };
        if (url) row.url = url;
        cards.push(row);
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
          pushCard(knownHit || title, value, delta, '', cardHref(el));
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
    """Tek haneli sahte kartları (ör. '5') ele — rating / dönüşüm oranı hariç."""
    if metric_key == "rating":
        return 0 < value <= 5.5
    if metric_key == "store_listing_conversion":
        # Oran: 0–1 (kesir) veya 0–100 (%)
        return 0 <= value <= 100
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
        return m ? String(m[1]).trim() : null;
      };
      // Önce etiket → sayı; Play bazen sayıyı etiketten önce koyar
      const pickAround = (labelRe, numRe) => {
        const m = text.match(labelRe);
        if (!m) return null;
        const idx = m.index || 0;
        const window = text.slice(Math.max(0, idx - 40), idx + m[0].length + 60);
        const n = window.match(numRe);
        return n ? String(n[1]).trim() : null;
      };
      const ratingNum = /([1-5](?:[.,]\\d{1,3})?)/;
      const countNum = /([\\d.\\s]{1,20})/;
      let default_rating =
        pick(/Varsayılan Google Play puanı[^\\d]{0,40}([1-5](?:[.,]\\d{1,3})?)/i)
        || pickAround(/Varsayılan Google Play puanı/i, ratingNum)
        || pick(/Default Google Play rating[^\\d]{0,40}([1-5](?:[.,]\\d{1,3})?)/i);
      // Zayıf fallback: "Google Play puanı" (TPG/lifetime karışabilir) — sadece Varsayılan yoksa
      if (!default_rating) {
        default_rating = pick(/Google Play puanı[^\\d]{0,40}([1-5](?:[.,]\\d{1,3})?)/i);
      }
      let users =
        pick(/Kullanıcılar[^\\d]{0,40}([\\d.\\s]{2,20})/i)
        || pickAround(/Kullanıcılar/i, countNum);
      // Kullanıcı satırına puan (4,647) yapışmasın — puan aralığını reddet
      if (users) {
        const uf = parseFloat(String(users).replace(',', '.').replace(/\\s/g, ''));
        if (Number.isFinite(uf) && uf >= 1 && uf <= 5.5) users = null;
      }
      return {
        default_rating,
        users,
        ratings_with_reviews:
          pick(/Yorum içeren puanlar[^\\d]{0,40}([\\d.\\s]+)/i)
          || pickAround(/Yorum içeren puanlar/i, countNum),
        lifetime_average:
          pick(/Yaşam boyu ortalama puan[^\\d]{0,40}([1-5](?:[.,]\\d{1,3})?)/i)
          || pick(/Lifetime average rating[^\\d]{0,40}([1-5](?:[.,]\\d{1,3})?)/i),
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


def _parse_ratings_distribution_csv(text: str) -> list[dict[str, Any]]:
    """Play 'Puan dağılımı' CSV → günlük toplam oy (1–5 yıldız toplamı)."""
    import csv
    import io

    rows_out: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames:
        return rows_out
    star_cols = [
        c
        for c in reader.fieldnames
        if c and re.search(r"yıldız|star", c, re.I)
    ]
    date_col = next(
        (c for c in reader.fieldnames if c and re.search(r"tarih|date", c, re.I)),
        reader.fieldnames[0],
    )
    for row in reader:
        if not isinstance(row, dict):
            continue
        raw_d = str(row.get(date_col) or "").strip()
        ds = _parse_tr_day_label(raw_d)
        if not ds:
            m = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", raw_d)
            if m:
                ds = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        if not ds:
            continue
        total = 0
        stars: dict[str, int] = {}
        for col in star_cols:
            try:
                n = int(float(str(row.get(col) or "0").replace(",", ".").strip() or 0))
            except (TypeError, ValueError):
                n = 0
            sm = re.search(r"([1-5])", col)
            if sm:
                stars[sm.group(1)] = n
            total += max(0, n)
        rows_out.append({"date": ds, "total": total, "stars": stars})
    return rows_out


def _ratings_count_facts_from_distribution(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for pt in rows or []:
        if not isinstance(pt, dict):
            continue
        ds = str(pt.get("date") or "")[:10]
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", ds):
            continue
        try:
            total = int(pt.get("total") or 0)
        except (TypeError, ValueError):
            continue
        facts.append(
            {
                "metric": "ratings_count",
                "view_id": "ratings_distribution",
                "dim": "overview",
                "segment": "OVERALL",
                "date": ds,
                "value": float(total),
                "label": "Günlük puan sayısı",
                "source": "ratings_distribution_csv",
                "stars": pt.get("stars") if isinstance(pt.get("stars"), dict) else {},
            }
        )
    return facts


def _expand_ratings_page_date_range(page) -> str:
    """Mümkünse puanlar sayfasında tarih aralığını genişlet. Seçilen etiketi döner."""
    chosen = ""
    # Önce Ömür boyu / 1 yıl — kısa 28 gün penceresi dağılımı kesiyor
    labels = (
        "Ömür boyu",
        "Lifetime",
        "Tüm zamanlar",
        "All time",
        "Son 1 yıl",
        "Last 1 year",
        "Last year",
        "Son 6 ay",
        "Last 6 months",
        "Son 90 gün",
        "Last 90 days",
    )

    def _pick_label() -> str:
        for label in labels:
            try:
                opt = page.get_by_role(
                    "option", name=re.compile(f"^{re.escape(label)}$", re.I)
                )
                if opt.count() == 0:
                    opt = page.locator("[role='option'], li, button, a").filter(
                        has_text=re.compile(f"^{re.escape(label)}$", re.I)
                    )
                if opt.count() == 0:
                    opt = page.get_by_text(re.compile(f"^{re.escape(label)}$", re.I))
                if opt.count():
                    opt.first.click(timeout=4000)
                    _settle(page, seconds=4.5)
                    return label
            except Exception:
                continue
        return ""

    try:
        # Tüm tarih chip/butonlarını dene (ortalama + dağılım)
        candidates = page.locator(
            "button, [role='button'], [aria-haspopup='listbox'], [aria-haspopup='menu']"
        ).filter(
            has_text=re.compile(
                r"Son\s+\d+\s+gün|Last\s+\d+\s+days|Ömür boyu|Lifetime|"
                r"Son\s+1\s+yıl|Last\s+1\s+year|Son\s+6\s+ay|Tüm zaman|"
                r"\d{1,2}\s*[A-Za-zÇĞİÖŞÜçğıöşü]{3}.{0,40}\d{4}",
                re.I,
            )
        )
        n = min(candidates.count(), 6)
        if n == 0:
            return ""
        for i in range(n):
            try:
                candidates.nth(i).click(timeout=4000)
                page.wait_for_timeout(700)
                chosen = _pick_label()
                if chosen:
                    return chosen
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
            except Exception:
                try:
                    page.keyboard.press("Escape")
                except Exception:
                    pass
        # JS yedek: menüde Ömür boyu görünürse tıkla
        clicked = page.evaluate(
            """() => {
              const want = /^(ömür boyu|lifetime|tüm zamanlar|all time|son 1 yıl|last 1 year)$/i;
              const nodes = Array.from(document.querySelectorAll('[role="option"], li, button, a, span'));
              for (const el of nodes) {
                const t = (el.innerText || el.textContent || '').trim();
                if (want.test(t) && el.offsetParent !== null) {
                  el.click();
                  return t;
                }
              }
              return '';
            }"""
        )
        if clicked:
            _settle(page, seconds=4.5)
            chosen = str(clicked)
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
    return chosen


def _download_ratings_distribution_csv(page) -> list[dict[str, Any]]:
    """Puan dağılımı → CSV indir → günlük oy toplamları (mümkünse ömür boyu)."""
    try:
        # Uzun tarih aralığı (2025-01-01 → dün) — kısa 28g chip’ini URL ile ez
        try:
            base = (RATINGS_URL or "").split("#")[0].split("?")[0]
            page.goto(
                f"{base}?dateRange={_console_date_range()}",
                wait_until="domcontentloaded",
                timeout=90_000,
            )
            _settle(page, seconds=3.5)
        except Exception:
            pass
        selected = _expand_ratings_page_date_range(page)
        if selected:
            print(f"    → ratings date range UI: {selected}", flush=True)
        # Dağılım grafiği yakınındaki aralık seçiciyi de dene
        try:
            dist_hdr = page.get_by_text(
                re.compile(r"Puan\s*dağılımı|Ratings?\s*distribution", re.I)
            )
            if dist_hdr.count():
                dist_hdr.first.scroll_into_view_if_needed(timeout=3000)
                page.wait_for_timeout(400)
                nearby = page.locator("button, [role='button']").filter(
                    has_text=re.compile(
                        r"Ömür boyu|Lifetime|Son\s+1\s+yıl|Last\s+1\s+year|Son\s+\d+\s+gün|Last\s+\d+\s+days",
                        re.I,
                    )
                )
                if nearby.count():
                    nearby.first.click(timeout=4000)
                    page.wait_for_timeout(600)
                    for label in ("Ömür boyu", "Lifetime", "Son 1 yıl", "Last 1 year"):
                        opt = page.get_by_role(
                            "option", name=re.compile(f"^{re.escape(label)}$", re.I)
                        )
                        if opt.count() == 0:
                            opt = page.get_by_text(
                                re.compile(f"^{re.escape(label)}$", re.I)
                            )
                        if opt.count():
                            opt.first.click(timeout=4000)
                            _settle(page, seconds=4.0)
                            print(f"    → ratings dist range UI: {label}", flush=True)
                            break
                    else:
                        page.keyboard.press("Escape")
        except Exception:
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
        # İkinci CSV genelde dağılım; yoksa tüm CSV butonlarını dene
        buttons = page.locator("button").filter(
            has_text=re.compile(r"CSV dosyasını indir|Download CSV|CSV", re.I)
        )
        n = buttons.count()
        if n < 1:
            return []
        # Tercihen "Puan dağılımı" bölümündeki buton
        idx_order = list(range(n))
        if n >= 2:
            idx_order = [1, 0] + list(range(2, n))
        best: list[dict[str, Any]] = []
        for idx in idx_order:
            try:
                with page.expect_download(timeout=25_000) as di:
                    buttons.nth(idx).click(timeout=5000)
                download = di.value
                # Playwright save to temp
                import tempfile

                with tempfile.NamedTemporaryFile(
                    suffix=".csv", delete=False
                ) as tmp:
                    path = tmp.name
                download.save_as(path)
                text = Path(path).read_text(encoding="utf-8", errors="replace")
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass
                if not re.search(r"yıldız|star", text, re.I):
                    continue
                rows = _parse_ratings_distribution_csv(text)
                if not rows:
                    continue
                # En uzun tarih aralığını tut
                if len(rows) > len(best):
                    best = rows
                dates = sorted(str(r.get("date") or "") for r in rows if r.get("date"))
                span = 0
                if len(dates) >= 2:
                    try:
                        from datetime import date as date_cls

                        span = (
                            date_cls.fromisoformat(dates[-1]) - date_cls.fromisoformat(dates[0])
                        ).days
                    except ValueError:
                        span = len(dates)
                print(
                    f"    → ratings CSV rows={len(rows)} span_days≈{span} "
                    f"{dates[0] if dates else '?'}→{dates[-1] if dates else '?'}",
                    flush=True,
                )
                # 60+ gün yeterli; kısa kaldıysa diğer buton / tekrar dene
                if span >= 60 or len(rows) >= 60:
                    return rows
            except Exception:
                continue
        if best:
            return best
    except Exception:
        return []
    return []


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
      const isCalJunk = (s) => {
        const t = String(s || '');
        if (/başlangıç\\s*tarihi|bitiş\\s*tarihi|arrow_drop_down|chevron_left|chevron_right|\\bPSÇPCCP\\b|\\bMTWTFSS\\b/i.test(t)) return true;
        if (/start\\s*date|end\\s*date|date\\s*picker/i.test(t)) return true;
        // BÜYÜK HARF ay başlığı (takvim); "7 Ağu 2026" kalır
        if (/\\b(?:OCA|ŞUB|MAR|N[İI]S|MAY|HAZ|TEM|A[ĞG]U|EYL|EK[İI]|KAS|ARA)\\s+20\\d{2}\\b/.test(t)) return true;
        const months = t.match(/\\b(?:Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara|OCA|ŞUB|MAR|NİS|MAY|HAZ|TEM|AĞU|EYL|EKİ|KAS|ARA)\\b/g);
        if (months && months.length >= 4) return true;
        return false;
      };
      const out = [];
      // Önce gerçek yorum kartları; geniş div tarama takvim popup'ını yutuyor
      let blocks = Array.from(document.querySelectorAll(
        'article, [data-review-id], [data-review], li[class*="review"], div[class*="review"]'
      ));
      if (blocks.length < 3) {
        blocks = Array.from(document.querySelectorAll('article, li, [role="listitem"]'));
      }
      for (const el of blocks) {
        const t = (el.innerText || '').trim();
        if (!t || t.length < 40 || t.length > 4000) continue;
        if (isCalJunk(t)) continue;
        if (!/yıldız|star|★|⭐/i.test(t)) continue;
        const lines = t.split('\\n').map(s => s.trim()).filter(Boolean);
        if (lines.length < 3) continue;
        if (isCalJunk(lines[0])) continue;
        const hasDevice = /Android|iPhone|Samsung|Xiaomi|POCO|Galaxy|Cihaz:/i.test(t);
        const dateRe = /(\\d{1,2}\\s*(?:Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-zçğıöşü]*\\s*\\d{4}(?:[\\s,]*\\d{1,2}:\\d{2})?)/i;
        const dateRe2 = /(\\d{1,2}[\\.\\/]\\d{1,2}[\\.\\/]\\d{2,4})/;
        const hasDate = dateRe.test(t) || dateRe2.test(t);
        if (!(hasDevice || hasDate)) continue;
        // Yazar: ilk satır takvim/ay olmamalı
        let author = lines[0].slice(0, 120);
        if (/^(Başlangıç|Bitiş|Cihaz:|\\d{1,2}\\s)/i.test(author) || isCalJunk(author)) continue;
        const bodyLines = lines.slice(1).filter((l) =>
          !isCalJunk(l)
          && !/^(Cihaz:|Yanıtla|thumb_|feature_search|Cihazın dili|Uygulama sürüm|Android sürümü|\\d+\\s*\\/\\s*350)/i.test(l)
          && !/^[0-9\\s]+$/.test(l)
        );
        // En uzun doğal cümle = yorum; meta satırlarını ele
        let body = '';
        const prose = bodyLines.filter((l) => l.length > 24 && /[a-zçğıöşü]/i.test(l));
        if (prose.length) body = prose.sort((a,b) => b.length - a.length)[0];
        else body = bodyLines.join(' ').slice(0, 1500);
        body = String(body || '').slice(0, 1500);
        if (body.length < 12 || isCalJunk(body)) continue;
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

    # Tarih aralığı seçici (ör. "1 Eki 2008 – 8 Ağu 2026")
    try:
        opened = page.evaluate(
            """() => {
              const clean = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
              const nodes = Array.from(document.querySelectorAll(
                'button, [role="combobox"], [aria-haspopup], div[aria-label]'
              ));
              for (const el of nodes) {
                const aria = clean(el.getAttribute('aria-label') || '');
                const t = clean(el.innerText || '');
                if (/tarih aralığı|date range|date filter/i.test(aria)
                    || (/\\d{4}.+\\d{4}/.test(t) && /arrow_drop_down|–|-/.test(t) && t.length < 80)) {
                  el.click();
                  return true;
                }
              }
              return false;
            }"""
        )
        if opened:
            time.sleep(0.6)
        else:
            for name in (
                "Dönem",
                "Period",
                "Tarih",
                "Date",
                "Zaman aralığı",
                "Time period",
                "Tarih aralığı",
            ):
                loc = page.get_by_label(name, exact=False)
                if loc.count() > 0:
                    loc.first.click(timeout=2_500)
                    time.sleep(0.5)
                    break
    except Exception:
        pass

    for label in labels:
        try:
            opt = page.get_by_role("option", name=re.compile(re.escape(label), re.I))
            if opt.count() > 0:
                opt.first.click(timeout=2_500)
                _settle(page, seconds=2.5)
                # Takvim / combobox popup'ını kapat — DOM'a sızmasın
                for _ in range(3):
                    try:
                        page.keyboard.press("Escape")
                    except Exception:
                        pass
                    time.sleep(0.25)
                _settle(page, seconds=1.0)
                return True
        except Exception:
            pass
        try:
            loc = page.get_by_text(label, exact=True)
            if loc.count() > 0:
                loc.first.click(timeout=2_500)
                _settle(page, seconds=2.5)
                for _ in range(3):
                    try:
                        page.keyboard.press("Escape")
                    except Exception:
                        pass
                    time.sleep(0.25)
                _settle(page, seconds=1.0)
                return True
        except Exception:
            pass
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _click_reviews_load_more(page) -> bool:
    """Sadece 'daha fazla yorum' tarzı butonlar — 'Daha fazla bilgi' vb. yanıltmasın."""
    labels = (
        "Daha fazla göster",
        "Daha fazla yükle",
        "Show more",
        "Load more",
        "Daha fazla yorum",
    )
    for label in labels:
        try:
            loc = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.I))
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
                // 'Daha fazla bilgi' / help linklerini ele
                if (/bilgi|help|info|öğren|learn/i.test(t)) continue;
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
    # Her durumda açık tarih seçici / combobox'ı kapat
    for _ in range(4):
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        time.sleep(0.2)
    _settle(page, seconds=1.0)

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
    # Takvim popup kalıntılarını son kez temizle (BÜYÜK HARF ay başlığı; "7 Ağu 2026" kalır)
    cal_ui = re.compile(
        r"başlangıç\s*tarihi|bitiş\s*tarihi|arrow_drop_down|chevron_|PSÇPCCP",
        re.I,
    )
    cal_hdr = re.compile(
        r"\b(?:OCA|ŞUB|MAR|N[İI]S|MAY|HAZ|TEM|A[ĞG]U|EYL|EK[İI]|KAS|ARA)\s+20\d{2}\b"
    )
    rows = [
        r
        for r in rows
        if isinstance(r, dict)
        and not cal_ui.search(
            " ".join(str(r.get(k) or "") for k in ("author", "body", "raw", "date"))
        )
        and not cal_hdr.search(
            " ".join(str(r.get(k) or "") for k in ("author", "body", "raw", "date"))
        )
        and re.search(r"yıldız|star|★|⭐", str(r.get("raw") or r.get("stars") or ""), re.I)
    ]
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


def _wait_until_console(page, *, timeout_sec: int | None = None) -> bool:
    """Login ekranındaysa kullanıcı girene kadar bekle; console URL gelince True.

    True → aynı pencerede tarama devam eder (kapatılmaz).
    """
    from backend.services.scrape_browser import LOGIN_WAIT_SEC, login_wait_sec

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    deadline = time.time() + timeout_sec
    printed = False
    while time.time() < deadline:
        need, url, _title = _page_needs_login(page)
        if not need and "play.google.com/console" in (url or ""):
            time.sleep(2)
            print("Play giriş OK — aynı pencerede tarama devam ediyor.", flush=True)
            return True
        try:
            body = page.inner_text("body")[:2000]
        except Exception:
            body = ""
        if google_blocks_automation_text(body):
            return False
        if not printed:
            from backend.services.selenium_playwright_shim import play_console_use_selenium

            if play_console_use_selenium():
                print(
                    "Play oturumu yok — açık Firefox penceresinde girişi tamamla "
                    f"(Play Console dashboard görünsün, en fazla {timeout_sec // 60} dk). "
                    "Girişten sonra pencere kapanmaz.",
                    flush=True,
                )
            else:
                print(
                    "Play oturumu yok — bu pencerede Google girişi çalışmayabilir. "
                    "Önce: .venv/bin/python scripts/play_console_scrape.py --login "
                    f"(gerçek Firefox, {timeout_sec // 60} dk).",
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
            "ANR oranı",
            "İzleyin ve geliştirin",
            "Monitor and improve",
            "TPG trendlerini izleyin",
            "Etkin cihazlar",
            "Cihaz edinme",
            "Toplam yükleme",
            "Ortalama puan",
        ),
        timeout_sec=timeout_sec,
    )


def _extract_dashboard_monitor_improve(page) -> dict[str, Any]:
    """app-dashboard · İzleyin ve geliştirin şeridi (kilitlenme / ANR / puan)."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);
      const sectionRe = /^(İzleyin ve geliştirin|Monitor and improve)$/i;
      const cardTitles = [
        { re: /^Kilitlenme oranı$/i, key: 'crash_rate' },
        { re: /^ANR oranı$/i, key: 'anr_rate' },
        { re: /^(Ortalama puan|Google Play puanı|Average rating)$/i, key: 'rating' },
      ];
      const isDelta = (s) => /^[+\\-−]/.test(s) || /yüzde puan|percentage point|puan$/i.test(s);
      const isVal = (s) => /\\d/.test(s) && !isDelta(s) && s.length < 24;
      let start = -1;
      for (let i = 0; i < lines.length; i++) {
        if (sectionRe.test(lines[i])) { start = i; break; }
      }
      let period = '';
      const cards = [];
      if (start < 0) {
        for (const ct of cardTitles) {
          for (let i = 0; i < lines.length; i++) {
            if (!ct.re.test(lines[i])) continue;
            let value = '', delta = '';
            for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
              const l = lines[j];
              if (cardTitles.some((x) => x.re.test(l))) break;
              if (/son \\d+ gün|last \\d+ days|önceki|previous/i.test(l) && !period) period = l;
              if (!value && isVal(l)) { value = l; continue; }
              if (value && !delta && isDelta(l)) { delta = l; break; }
            }
            if (value) cards.push({ key: ct.key, title: lines[i], value, delta, period: period || '' });
            break;
          }
        }
      } else {
        for (let i = start + 1; i < Math.min(start + 40, lines.length); i++) {
          const l = lines[i];
          if (/^TPG trendlerini|^Track key|^Play Console politikaları|^Review changes/i.test(l)) break;
          if (/son \\d+ gün|last \\d+ days|önceki|previous/i.test(l) && !period) { period = l; continue; }
          for (const ct of cardTitles) {
            if (!ct.re.test(l)) continue;
            let value = '', delta = '';
            for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
              const ll = lines[j];
              if (cardTitles.some((x) => x.re.test(ll))) break;
              if (!value && isVal(ll)) { value = ll; continue; }
              if (value && !delta && isDelta(ll)) { delta = ll; break; }
            }
            if (value) cards.push({ key: ct.key, title: l, value, delta, period: period || '' });
          }
        }
      }
      const seen = new Set();
      const uniq = cards.filter((c) => {
        const k = c.key + '|' + c.value;
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      });
      return {
        section_title: start >= 0 ? lines[start] : 'İzleyin ve geliştirin',
        period: period,
        cards: uniq,
        card_count: uniq.length,
      };
    }"""
    )


def _scroll_vitals_overview_deep(page) -> None:
    """Vitals overview: lazy section'ları yükle."""
    try:
        page.evaluate(
            """async () => {
              const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
              const h = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
              for (let y = 0; y <= h; y += Math.max(280, Math.floor(h / 8))) {
                window.scrollTo(0, y);
                await sleep(450);
              }
              window.scrollTo(0, 0);
              await sleep(350);
            }"""
        )
    except Exception:
        _scroll_full_page(page)


def _extract_vitals_metrics_overview(page) -> dict[str, Any]:
    """Vitals metrics overview — tam sayfa: öneriler, kararlılık, bellek, açılış, oluşturma, pil."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);

      const sectionFromTitle = (t) => {
        const s = clean(t).toLowerCase();
        if (/kararlılık|^stability/.test(s)) return 'stability';
        if (/^bellek|^memory/.test(s)) return 'memory';
        if (/açılma|yükleme|startup|loading time/.test(s)) return 'startup';
        if (/^oluşturma|^rendering/.test(s)) return 'rendering';
        if (/^pil$|^battery/.test(s)) return 'battery';
        return '';
      };

      const isDelta = (s) => {
        const t = clean(s);
        return /^[+\\-−]/.test(t) || /yüzde puan|percentage point/i.test(t) || /^[+\\-−]?\\s*\\d/.test(t) && /puan/i.test(t);
      };
      const isMetricVal = (s) => {
        const t = clean(s);
        if (!t || t.length > 48) return false;
        if (!/\\d/.test(t)) return false;
        if (/^(tem|ağu|eyl|eki|kas|ara|oca|şub|mar|nis|may|haz)\\b/i.test(t)) return false;
        return true;
      };

      let pageTitle = '';
      for (const l of lines.slice(0, 8)) {
        if (/vitals|vital|android vitals|genel bakış|overview/i.test(l) && l.length < 120) {
          pageTitle = l;
          break;
        }
      }

      let peerGroup = '';
      for (const l of lines) {
        if (/benzerler grubu|peer group|custom peer|özel benzer/i.test(l)) {
          peerGroup = l;
          break;
        }
      }

      const recommendations = [];
      const recCountM = body.match(/(\\d+)\\s*(işlem\\s*öneriliyor|actions?\\s*recommended)/i);
      const recLimit = recCountM ? Math.min(parseInt(recCountM[1], 10) || 8, 12) : 8;
      let inRec = false;
      for (let i = 0; i < lines.length && recommendations.length < recLimit; i++) {
        const l = lines[i];
        if (/işlem\\s*öneriliyor|actions?\\s*recommended/i.test(l)) { inRec = true; continue; }
        if (inRec) {
          if (sectionFromTitle(l) || /^kararlılık$|^bellek$|^stability$|^memory$/i.test(l)) break;
          if (l.length < 12 || l.length > 240) continue;
          if (/^sürüm adı:|^version name:/i.test(l)) continue;
          const verM = l.match(/(?:Sürüm adı|Version name)\\s*:\\s*(.+)$/i);
          const title = verM ? lines[i - 1] || l : l;
          if (!title || title.length < 8) continue;
          recommendations.push({
            title: verM ? clean(lines[i - 1] || l) : l,
            version: verM ? clean(verM[1]) : (l.match(/\\((\\d{2,4}[^)]*)\\)/) || [])[1] || '',
          });
        }
      }

      const summaryCards = [];
      const summaryRes = [
        { re: /kullanıcı tarafından algılanan kilitlenme|user[- ]perceived crash/i, key: 'crash' },
        { re: /kullanıcı tarafından algılanan anr|user[- ]perceived anr/i, key: 'anr' },
      ];
      for (const sr of summaryRes) {
        for (let i = 0; i < lines.length; i++) {
          if (!sr.re.test(lines[i])) continue;
          const vals = [];
          for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
            const l = lines[j];
            if (summaryRes.some((x) => x.re.test(l))) break;
            if (isMetricVal(l) || isDelta(l)) vals.push(l);
            if (vals.length >= 2) break;
          }
          if (vals.length) {
            summaryCards.push({
              key: sr.key,
              metric: lines[i],
              value: vals[0] || '',
              delta: vals[1] || '',
            });
          }
          break;
        }
      }

      const sections = [];
      const sectionOrder = ['stability', 'memory', 'startup', 'rendering', 'battery'];
      const sectionTitles = {
        stability: 'Kararlılık',
        memory: 'Bellek',
        startup: 'Açılma ve yükleme süreleri',
        rendering: 'Oluşturma',
        battery: 'Pil',
      };

      function pushRow(sectionId, row) {
        let sec = sections.find((s) => s.id === sectionId);
        if (!sec) {
          sec = { id: sectionId, title: sectionTitles[sectionId] || sectionId, rows: [] };
          sections.push(sec);
        }
        sec.rows.push(row);
      }

      const metricKeyFromText = (t) => {
        const s = clean(t).toLowerCase();
        if (/kilitlenme|crash/.test(s)) return 'crash';
        if (/\\banr\\b/.test(s)) return 'anr';
        if (/\\blmk\\b/.test(s)) return 'lmk';
        return 'other';
      };

      const userMetricRe = /kullanıcı tarafından algılanan|user[- ]perceived/i;

      for (const table of Array.from(document.querySelectorAll('table'))) {
        const headers = Array.from(table.querySelectorAll('thead th, tr:first-child th, tr:first-child td'))
          .map((c) => clean(c.innerText)).filter(Boolean);
        if (headers.length < 2) continue;
        let sectionId = '';
        let el = table;
        for (let depth = 0; depth < 8 && el; depth++) {
          el = el.parentElement;
          if (!el) break;
          const heading = el.querySelector('h1,h2,h3,h4,[role="heading"]');
          if (heading) {
            sectionId = sectionFromTitle(clean(heading.innerText));
            if (sectionId) break;
          }
          const prev = el.previousElementSibling;
          if (prev) {
            const pt = clean(prev.innerText || '').split(/\\n+/)[0];
            sectionId = sectionFromTitle(pt);
            if (sectionId) break;
          }
        }
        if (!sectionId) {
          const blob = clean(table.innerText).slice(0, 200).toLowerCase();
          if (/bellek|memory|anonim rss|bitmap/.test(blob)) sectionId = 'memory';
          else if (/yavaş|slow|başlatma|startup|cold|warm|hot/.test(blob)) sectionId = 'startup';
          else if (/kare|frame|render|donmuş|frozen/.test(blob)) sectionId = 'rendering';
          else if (/pil|battery|wakeup|wake/.test(blob)) sectionId = 'battery';
          else sectionId = 'stability';
        }
        const hdr = headers.join(' ').toLowerCase();
        const isMemory = sectionId === 'memory' || /p50|p90|percentile|yüzdelik/.test(hdr);
        const bodyRows = Array.from(table.querySelectorAll('tbody tr'));
        const trs = bodyRows.length ? bodyRows : Array.from(table.querySelectorAll('tr')).slice(1);
        for (const tr of trs) {
          const cells = Array.from(tr.querySelectorAll('th,td')).map((c) => clean(c.innerText));
          if (cells.length < 2) continue;
          const metric = cells[0];
          if (!metric || metric.length < 3 || /^(metrik|metric)$/i.test(metric)) continue;
          if (isMemory && cells.length >= 3) {
            pushRow(sectionId, {
              key: metricKeyFromText(metric),
              metric,
              p50: cells[1] || '',
              vs_previous_p50: cells[2] || '',
              p90: cells[3] || '',
              vs_previous_p90: cells[4] || '',
            });
          } else {
            pushRow(sectionId, {
              key: metricKeyFromText(metric),
              metric,
              value_28d: cells[1] || '',
              vs_previous_28d: cells[2] || '',
              vs_peers_median: cells[3] || '',
            });
          }
        }
      }

      if (!sections.some((s) => s.id === 'stability')) {
        const stabilityRows = [];
        const metricRe = /kullanıcı tarafından algılanan\\s+(kilitlenme|anr|lmk)\\s+oranı|user[- ]perceived\\s+(crash|anr|lmk)\\s+rate/i;
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];
          if (!metricRe.test(line)) continue;
          const vals = [];
          for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
            const l = lines[j];
            if (metricRe.test(l)) break;
            if (isMetricVal(l) || isDelta(l)) vals.push(l);
            if (vals.length >= 3) break;
          }
          stabilityRows.push({
            key: metricKeyFromText(line),
            metric: line,
            value_28d: vals[0] || '',
            vs_previous_28d: vals[1] || '',
            vs_peers_median: vals[2] || '',
          });
        }
        if (stabilityRows.length) {
          sections.unshift({ id: 'stability', title: 'Kararlılık', rows: stabilityRows });
        }
      }

      sections.sort((a, b) => {
        const ai = sectionOrder.indexOf(a.id);
        const bi = sectionOrder.indexOf(b.id);
        return (ai < 0 ? 99 : ai) - (bi < 0 ? 99 : bi);
      });

      const rows = (sections.find((s) => s.id === 'stability') || { rows: [] }).rows.filter((r) =>
        userMetricRe.test(r.metric || '')
      );

      return {
        page_title: pageTitle,
        peer_group: peerGroup,
        recommendations,
        recommendation_count: recommendations.length,
        summary_cards: summaryCards,
        sections,
        section_count: sections.length,
        rows,
        body_len: body.length,
        line_count: lines.length,
      };
    }"""
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
        _wait_until_console(page)
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


def _devices_dashboard_url(*, breakdown: str | None = None, days: int = 28) -> str:
    qs = f"days={days}&peerset_key={_DEVICES_PEERSET}"
    if breakdown:
        qs += f"&expanded_breakdowns={breakdown}"
    return f"{BASE_APP}/devices/dashboard?{qs}"


def _extract_devices_attribute_tables(page, *, dimension: str) -> list[dict[str, Any]]:
    """Devices dashboard tablolarından satır kırılımları (Android sürüm, RAM, …)."""
    try:
        rows = page.evaluate(
            """(dim) => {
          const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
          const out = [];
          const seen = new Set();
          const tables = Array.from(document.querySelectorAll('table'));
          for (const table of tables) {
            const headers = Array.from(table.querySelectorAll('th')).map((th) => clean(th.innerText || ''));
            const bodyRows = Array.from(table.querySelectorAll('tbody tr'));
            for (const tr of bodyRows) {
              const cells = Array.from(tr.querySelectorAll('td')).map((td) => clean(td.innerText || ''));
              if (cells.length < 2) continue;
              const segment = cells[0];
              if (!segment || segment.length > 80) continue;
              if (/^(toplam|total|genel|overall|metric|metrik)$/i.test(segment)) continue;
              // Sayısal hücreler
              const nums = cells.slice(1).filter((c) => /\\d/.test(c) && c.length < 40);
              if (!nums.length) continue;
              const value = nums[0];
              const delta = nums.find((c) => /^[+\\-−%]/.test(c) || /yüzde/i.test(c)) || '';
              const metricHint = headers[1] || headers[0] || dim || 'Cihaz kırılımı';
              const title = metricHint + ' (' + segment + ')';
              const key = title + '|' + value;
              if (seen.has(key)) continue;
              seen.add(key);
              out.push({
                title,
                value,
                delta,
                segment,
                metric: metricHint,
                dimension: dim || '',
                kind: 'breakdown',
                page: 'devices',
              });
              if (out.length >= 80) return out;
            }
          }
          return out;
        }""",
            dimension or "",
        )
        return [r for r in (rows or []) if isinstance(r, dict)]
    except Exception:
        return []


def _scrape_devices_dashboard(page, *, headed: bool = True, days: int = 28) -> dict[str, Any]:
    """Reach and devices · dashboard + alt kırılımlar (Android sürüm, RAM, SoC, …)."""
    known = tuple(dict.fromkeys(list(_KNOWN_DEVICES) + list(_KNOWN_DASHBOARD) + list(_KNOWN_MONITOR)))
    wait_needles = (
        "Yükleme tabanı",
        "Install base",
        "Kilitlenme",
        "ANR",
        "Cihaz",
        "Device",
        "Android",
        "Reach",
        "Erişim",
        "RAM",
    )
    # Önce kullanıcının verdiği URL (ANDROID_VERSION açık), sonra diğer kırılımlar
    order: list[str | None] = [None]
    for bd in DEVICES_BREAKDOWNS:
        if bd not in order:
            order.append(bd)

    all_cards: list[dict[str, Any]] = []
    all_br: list[dict[str, Any]] = []
    seen_c: set[str] = set()
    seen_b: set[str] = set()
    pages_detail: dict[str, Any] = {}
    primary_url = DEVICES_URL
    errors: list[str] = []

    for bd in order:
        url = DEVICES_URL if bd is None else _devices_dashboard_url(breakdown=bd, days=days)
        if bd is None:
            # ENV override veya varsayılan kullanıcı linki
            url = DEVICES_URL
        label = bd or "overview"
        print(f"  · devices/{label} …", flush=True)
        scraped = _safe_scrape_page(
            page,
            url=url,
            known=known,
            page_key="devices",
            wait_needles=wait_needles,
            headed=bool(headed),
        )
        if scraped.get("error"):
            errors.append(f"{label}:{scraped.get('error')}")
        cards = scraped.get("cards") or scraped.get("tpg") or []
        br = list(scraped.get("breakdowns") or [])
        # Tablo satırları (kırılım)
        dim_key = bd or "ANDROID_VERSION"
        table_br = _extract_devices_attribute_tables(page, dimension=dim_key)
        for row in table_br:
            row["dimension"] = dim_key
            br.append(row)
        for c in cards:
            if not isinstance(c, dict):
                continue
            key = f"{c.get('title')}|{c.get('value')}"
            if key in seen_c:
                continue
            seen_c.add(key)
            c = dict(c)
            c["page"] = "devices"
            c["kind"] = c.get("kind") or "metric"
            all_cards.append(c)
        for b in br:
            if not isinstance(b, dict):
                continue
            b = dict(b)
            b["page"] = "devices"
            b["kind"] = "breakdown"
            if bd and not b.get("dimension"):
                b["dimension"] = bd
            if bd and b.get("segment") and "(" not in str(b.get("title") or ""):
                # Boyut bilgisini başlığa ekle
                b["title"] = f"{b.get('title')} · {bd}"
            key = f"{b.get('title')}|{b.get('value')}|{b.get('segment')}|{b.get('dimension')}"
            if key in seen_b:
                continue
            seen_b.add(key)
            all_br.append(b)
        pages_detail[label] = {
            "url": url,
            "card_count": len(cards) if isinstance(cards, list) else 0,
            "breakdown_count": len(br),
            "error": scraped.get("error"),
        }
        # Overview kartları bir kez yeter; kırılımlarda tabloya odaklan
        if bd is None and all_cards:
            primary_url = url

    return {
        "url": primary_url,
        "cards": all_cards,
        "breakdowns": all_br,
        "breakdown_pages": pages_detail,
        "error": "; ".join(errors)[:400] if errors else None,
        "debug": {"breakdown_keys": [x or "overview" for x in order]},
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
            "metric": b.get("metric") or "",
            "dimension": b.get("dimension") or "",
            "kind": "breakdown",
            "page": page_key,
            "lines": [b.get("title"), b.get("value"), b.get("delta")],
        }
        if not row["dimension"]:
            row.pop("dimension", None)
        if not row["metric"]:
            row.pop("metric", None)
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


def _vitals_version_codes() -> list[str]:
    """Son N sürüm (max 3). VERSION_CODES=290,289,288 veya tek CODE → CODE..CODE-2."""
    multi = (os.environ.get("PLAY_CONSOLE_VITALS_VERSION_CODES") or "").strip()
    out: list[str] = []
    if multi:
        for part in multi.split(","):
            p = part.strip()
            if p and p not in out:
                out.append(p)
            if len(out) >= 3:
                break
        return out
    single = _vitals_version_code()
    if not single:
        return []
    try:
        n = int(single)
        return [str(n - i) for i in range(3)]
    except ValueError:
        return [single]


def _vitals_detail_limit() -> int:
    raw = (os.environ.get("PLAY_CONSOLE_VITALS_DETAIL_LIMIT") or "40").strip()
    try:
        return max(0, min(80, int(raw)))
    except ValueError:
        return 40


def _vitals_crashes_url(
    error_type: str, *, days: int = 28, version_code: str | None = None
) -> str:
    et = (error_type or "CRASH").strip().upper()
    qs = f"errorType={et}&isUserPerceived=true&days={int(days)}"
    vc = (version_code if version_code is not None else _vitals_version_code()).strip()
    if vc:
        qs += f"&versionCode={vc}"
    return f"{VITALS_CRASHES_BASE}?{qs}"


def _vitals_issue_id_from_url(url: str) -> str:
    m = re.search(
        r"/vitals/crashes/(?:issues/)?([a-f0-9]{12,32})(?:/(?:details|detail))?(?:[/?#]|$)",
        str(url or ""),
        re.I,
    )
    return m.group(1) if m else ""


def _vitals_issue_detail_url(
    issue_id: str, *, days: int = 28, version_code: str | None = None
) -> str:
    qs = f"days={int(days)}&isUserPerceived=true"
    vc = (version_code if version_code is not None else _vitals_version_code()).strip()
    if vc:
        qs += f"&versionCode={vc}"
    iid = str(issue_id or "").strip()
    return f"{VITALS_CRASHES_BASE}/issues/{iid}/details?{qs}"


def _vitals_issue_detail_url_legacy(
    issue_id: str, *, days: int = 28, version_code: str | None = None
) -> str:
    qs = f"days={int(days)}&isUserPerceived=true"
    vc = (version_code if version_code is not None else _vitals_version_code()).strip()
    if vc:
        qs += f"&versionCode={vc}"
    return f"{VITALS_CRASHES_BASE}/{issue_id}/details?{qs}"


def _vitals_row_nav_enabled() -> bool:
    raw = (os.environ.get("PLAY_CONSOLE_VITALS_ROW_NAV") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _scroll_vitals_issues_table(page) -> None:
    """Sorun tablosu lazy-load — aşağı kaydır."""
    try:
        page.evaluate(
            """async () => {
              const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
              const nodes = Array.from(document.querySelectorAll(
                'mat-table, [role="grid"], table, [class*="issue"], [class*="table"], c-wiz'
              ));
              for (let r = 0; r < 8; r++) {
                window.scrollTo(0, document.body.scrollHeight);
                for (const el of nodes) {
                  try { el.scrollTop = el.scrollHeight; } catch (_) {}
                }
                await sleep(420);
              }
              window.scrollTo(0, 0);
              await sleep(280);
            }"""
        )
    except Exception:
        _scroll_full_page(page)


def _parse_vitals_row_text(text: str, *, issue_id: str, detail_url: str) -> dict[str, Any]:
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in (text or "").splitlines() if ln.strip()]
    issue_type = ""
    affected_versions = ""
    users = ""
    events = ""
    events_share = ""
    last_occurrence = ""
    title = ""
    subtitle = ""
    tags: list[str] = []
    ver_re = re.compile(r"^(\d{2,8})\s*\(([^)]+)\)$")
    ago_re = re.compile(
        r"(\d+)\s*(saat|gün|dakika|hour|day|minute|week|month|year)s?\s*(önce|ago)",
        re.I,
    )
    type_re = re.compile(r"^(Kilitlenme|ANR|Crash|Application Not Responding)$", re.I)
    exc_re = re.compile(
        r"Exception|Error|SIGSEGV|SIGABRT|ANR|SourceFile|Native method|Input dispatching",
        re.I,
    )
    tag_re = re.compile(r"sdk|kilit anlaşmazlığı|binder|olası düzeltme|potential fix|analiz", re.I)
    nums: list[str] = []
    for ln in lines:
        if type_re.match(ln):
            issue_type = ln
            continue
        if ver_re.match(ln):
            affected_versions = ln
            continue
        if ago_re.search(ln) and len(ln) < 48:
            last_occurrence = ln
            continue
        if tag_re.search(ln) and len(ln) < 80:
            tags.append(ln)
            continue
        if re.match(r"^%?\d", ln) and "%" in ln:
            events_share = ln
            continue
        if re.match(r"^\d{1,7}([.,]\d+)?$", ln.replace(".", "").replace(",", "")):
            nums.append(ln)
            continue
        if exc_re.search(ln) and len(ln) < 220:
            if not title:
                title = ln
            elif not subtitle:
                subtitle = ln
        elif len(ln) > 4 and len(ln) < 220 and not title:
            title = ln
    if len(nums) >= 1:
        users = nums[0]
    if len(nums) >= 2:
        events = nums[1]
    title = re.sub(
        r"\s*(ayrıntısını göster|show details?|view details?)\s*$",
        "",
        title,
        flags=re.I,
    ).strip()
    if not title and subtitle:
        title = subtitle
    if not title:
        title = f"Issue {issue_id[:12]}"
    return {
        "issue_id": issue_id,
        "detail_url": detail_url,
        "title": title[:240],
        "subtitle": (subtitle or "")[:240],
        "tags": tags[:6],
        "issue_type": issue_type[:64],
        "affected_versions": affected_versions[:80],
        "version_track": "",
        "users": users[:32],
        "events": events[:32],
        "events_share": events_share[:32],
        "last_occurrence": last_occurrence[:64],
        "extra": "",
    }


def _scrape_vitals_issues_via_table_rows(
    page,
    *,
    list_url: str,
    days: int,
    version_code: str | None,
    scrape_details: bool,
    headed: bool,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Kilitlenme/ANR tablosu — satır satır tıkla, detay sayfasını gez."""
    if limit is None:
        limit = _vitals_detail_limit()
    issues_out: list[dict[str, Any]] = []
    details_out: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()

    _scroll_vitals_issues_table(page)
    row_filter = re.compile(
        r"Exception|Error|ANR|SourceFile|SIGSEGV|SIGABRT|Native method|Input dispatching|Kilitlenme",
        re.I,
    )
    try:
        row_loc = page.locator("mat-row, table tbody tr, [role='row']").filter(has_text=row_filter)
        n = min(row_loc.count(), max(limit, 1))
    except Exception:
        return issues_out, details_out

    if n <= 0:
        return issues_out, details_out

    print(f"    · tablo satır gezintisi: {n} satır (max {limit}) …", flush=True)

    for i in range(n):
        if len(seen) >= limit:
            break
        try:
            row = row_loc.nth(i)
            row_text = (row.inner_text(timeout=4_000) or "").strip()
            if not row_text:
                continue
            if re.search(r"sorun kategorisi|issue category|etkilenen kullanıcılar|affected users", row_text, re.I):
                continue
            if not row_filter.search(row_text):
                continue

            click_target = row.locator(
                'a[href*="/vitals/crashes"], button[aria-label*="Ayrınt"], '
                'button[aria-label*="detail"], button[aria-label*="Show"], [data-test-id*="issue"]'
            ).first
            if click_target.count() > 0:
                click_target.click(timeout=10_000)
            else:
                row.click(timeout=10_000)
            page.wait_for_load_state("domcontentloaded", timeout=90_000)
            _settle(page, seconds=2.5)

            iid = _vitals_issue_id_from_url(page.url)
            if not iid:
                page.goto(list_url, wait_until="domcontentloaded", timeout=120_000)
                _settle(page, seconds=1.5)
                continue
            if iid in seen:
                page.goto(list_url, wait_until="domcontentloaded", timeout=120_000)
                _settle(page, seconds=1.5)
                continue
            seen.add(iid)

            detail_url = _vitals_issue_detail_url(iid, days=days, version_code=version_code)
            row_issue = _parse_vitals_row_text(row_text, issue_id=iid, detail_url=detail_url)
            issues_out.append(row_issue)
            print(f"    · satır {len(issues_out)}/{limit} {iid[:12]}… {row_issue.get('title', '')[:48]}", flush=True)

            if scrape_details:
                if "/details" not in page.url.lower():
                    page.goto(detail_url, wait_until="domcontentloaded", timeout=120_000)
                    _settle(page, seconds=2.5)
                need, _, _ = _page_needs_login(page)
                if need and headed:
                    _wait_until_console(page)
                    page.goto(detail_url, wait_until="domcontentloaded", timeout=120_000)
                    _settle(page, seconds=2.5)
                _wait_page_text(
                    page,
                    ("Stack", "Yığın", "Exception", "ANR", "Kilitlenme", iid[:8]),
                    timeout_sec=20.0,
                )
                _scroll_full_page(page)
                detail = _extract_vitals_issue_detail(page) or {}
                series = _scrape_vitals_issue_events_series(page)
                if series:
                    detail["events_series"] = series
                detail["issue_id"] = iid
                detail["url"] = detail_url
                detail["list_title"] = str(row_issue.get("title") or "")[:240]
                detail["list_subtitle"] = str(row_issue.get("subtitle") or "")[:240]
                details_out[iid] = detail

            page.goto(list_url, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=2.0)
            _scroll_vitals_issues_table(page)
            row_loc = page.locator("mat-row, table tbody tr, [role='row']").filter(has_text=row_filter)
        except Exception as exc:  # noqa: BLE001
            print(f"    · satır {i + 1} gezinti hata: {exc}", flush=True)
            try:
                page.goto(list_url, wait_until="domcontentloaded", timeout=120_000)
                _settle(page, seconds=1.5)
            except Exception:
                pass

    return issues_out, details_out


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

      let summaryRate = null;
      const rateTitleRe = /kullanıcı tarafından algılanan\\s+(kilitlenme|anr|lmk)\\s+oranı|user[- ]perceived\\s+(crash|anr|lmk)\\s+rate/i;
      for (let i = 0; i < lines.length; i++) {
        if (!rateTitleRe.test(lines[i])) continue;
        for (let j = i + 1; j < Math.min(i + 8, lines.length); j++) {
          const l = lines[j];
          if (rateTitleRe.test(l)) break;
          if (/%/.test(l) && /\\d/.test(l) && l.length < 24 && !/^[+\\-−]/.test(l)) {
            summaryRate = l;
            break;
          }
        }
        if (summaryRate) break;
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
        const m = href.match(/\\/vitals\\/crashes\\/issues\\/([a-f0-9]{12,32})(?:\\/(?:details|detail))?(?:[/?#]|$)/i)
          || href.match(/\\/vitals\\/crashes\\/([a-f0-9]{12,32})(?:\\/(?:details|detail))?(?:[/?#]|$)/i)
          || href.match(/[?&]issue[Ii]d=([a-f0-9]{12,32})/i);
        if (!m) continue;
        const issueId = m[1];
        if (!issueId || /^(details|detail|overview)$/i.test(issueId)) continue;
        if (seen.has(issueId)) continue;
        const row = a.closest('tr, [role="row"], mat-row, [class*="row"], [class*="issue"]') || a.parentElement;
        if (!row) continue;
        const cellEls = Array.from(row.querySelectorAll('[role="cell"], [role="gridcell"], td, [class*="cell"]'));
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
            const m = href.match(/\\/vitals\\/crashes\\/issues\\/([a-f0-9]{12,32})/i)
              || href.match(/\\/vitals\\/crashes\\/([a-f0-9]{12,32})/i);
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
        summary_rate: summaryRate,
        issues,
        body_len: body.length,
        page_url: location.href,
      };
    }"""
    )


_VITALS_CHART_TIP_JS = """() => {
  const pick = (el) => String(el && (el.innerText || el.textContent) || '').trim();
  const tips = [...document.querySelectorAll(
    '[role="tooltip"], .mdc-tooltip, .mat-tooltip, .tooltip, [class*="tooltip"], google-chart tooltip'
  )].map(pick).filter(Boolean);
  if (tips.length) return tips.join('\\n');
  const aria = document.querySelector('[aria-live="polite"], [aria-live="assertive"]');
  return pick(aria);
}"""


def _parse_vitals_chart_tooltip(text: str) -> dict[str, Any] | None:
    from datetime import date

    raw = re.sub(r"\s+", " ", (text or "").strip())
    if not raw or len(raw) < 3:
        return None
    tr_months = {
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
    en_months = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    iso_m = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", raw)
    if iso_m:
        d = f"{iso_m.group(1)}-{int(iso_m.group(2)):02d}-{int(iso_m.group(3)):02d}"
    else:
        d = ""
        m = re.search(
            r"\b(\d{1,2})\s+(Oca|Şub|Sub|Mar|Nis|May|Haz|Tem|Ağu|Agu|Eyl|Eki|Kas|Ara|"
            r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-zğüşıöç.]*"
            r"(?:\s+(20\d{2}))?\b",
            raw,
            re.I,
        )
        if m:
            mon_key = m.group(2).lower()[:3]
            mon = tr_months.get(mon_key) or en_months.get(mon_key)
            if mon:
                yr = int(m.group(3)) if m.group(3) else date.today().year
                d = f"{yr}-{mon:02d}-{int(m.group(1)):02d}"
        if not d:
            m2 = re.search(
                r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})"
                r"(?:,?\s+(20\d{2}))?\b",
                raw,
                re.I,
            )
            if m2:
                mon_key = m2.group(1).lower()[:3]
                mon = en_months.get(mon_key)
                if mon:
                    yr = int(m2.group(3)) if m2.group(3) else date.today().year
                    d = f"{yr}-{mon:02d}-{int(m2.group(2)):02d}"
    count = 0
    for m in re.finditer(r"\b(\d[\d.,]*)\b", raw):
        try:
            n = int(float(m.group(1).replace(".", "").replace(",", ".")))
        except ValueError:
            continue
        if n > count:
            count = n
    if not d and not count:
        return None
    return {"date": d or None, "events": count}


def _scrape_vitals_issue_events_series(page, *, steps: int = 56) -> list[dict[str, Any]]:
    """Detay sayfası olay grafiği — tooltip sweep ile günlük noktalar."""
    try:
        box = page.evaluate(
            """() => {
      const isPlot = (el) => {
        if (!el || !el.getBoundingClientRect) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 120 || r.height < 40) return false;
        const tag = (el.tagName || '').toLowerCase();
        if (tag === 'svg' && el.querySelector('path, rect, line')) return true;
        if (el.querySelector && el.querySelector('svg path, svg rect, canvas')) return true;
        return false;
      };
      const cands = [...document.querySelectorAll('svg, canvas, [class*="chart"], google-chart, [role="img"]')]
        .filter(isPlot)
        .map(el => {
          const r = el.getBoundingClientRect();
          return { x: r.left, y: r.top, w: r.width, h: r.height, area: r.width * r.height };
        })
        .sort((a, b) => b.area - a.area);
      return cands[0] || null;
    }"""
        )
    except Exception:
        box = None
    if not box or not box.get("w"):
        return []
    x0 = float(box["x"]) + float(box["w"]) * 0.06
    x1 = float(box["x"]) + float(box["w"]) * 0.96
    y = float(box["y"]) + float(box["h"]) * 0.42
    samples: dict[str, int] = {}
    n = max(24, min(steps, 96))
    for i in range(n):
        t = i / max(n - 1, 1)
        x = x0 + (x1 - x0) * t
        try:
            page.mouse.move(x, y)
        except Exception:
            continue
        time.sleep(0.03)
        try:
            tip = page.evaluate(_VITALS_CHART_TIP_JS)
        except Exception:
            tip = ""
        parsed = _parse_vitals_chart_tooltip(str(tip or ""))
        if not parsed:
            continue
        d = str(parsed.get("date") or "").strip()
        cnt = int(parsed.get("events") or 0)
        if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            samples[d] = max(samples.get(d, 0), cnt)
        elif cnt and samples:
            last = sorted(samples)[-1]
            samples[last] = max(samples[last], cnt)
    if len(samples) < 2:
        y2 = float(box["y"]) + float(box["h"]) * 0.55
        for i in range(n):
            t = i / max(n - 1, 1)
            x = x0 + (x1 - x0) * t
            try:
                page.mouse.move(x, y2)
            except Exception:
                continue
            time.sleep(0.03)
            try:
                tip = page.evaluate(_VITALS_CHART_TIP_JS)
            except Exception:
                tip = ""
            parsed = _parse_vitals_chart_tooltip(str(tip or ""))
            if not parsed:
                continue
            d = str(parsed.get("date") or "").strip()
            cnt = int(parsed.get("events") or 0)
            if d and re.match(r"^\d{4}-\d{2}-\d{2}$", d):
                samples[d] = max(samples.get(d, 0), cnt)
    if len(samples) < 2:
        return []
    out = [{"date": d, "events": int(samples[d])} for d in sorted(samples)]
    print(f"    · issue chart series: {len(out)} pts", flush=True)
    return out[:40]


def _extract_vitals_issue_detail(page) -> dict[str, Any]:
    """Tek sorun detay sayfası: özet, stack, içgörüler."""
    return page.evaluate(
        """() => {
      const clean = (s) => String(s || '').replace(/[\\u00a0\\u200b\\ufeff]/g, ' ').replace(/\\s+/g, ' ').trim();
      const body = (document.body && document.body.innerText) || '';
      const lines = body.split(/\\n+/).map(clean).filter(Boolean);
      const href = location.href || '';
      const idm = href.match(/\\/vitals\\/crashes\\/issues\\/([a-f0-9]{12,32})/i)
        || href.match(/\\/vitals\\/crashes\\/([a-f0-9]{12,32})/i);
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

      // Stack / traceback bloğu (Console chrome + partner-share UI'sını ele)
      const isStackJunk = (l) => {
        const t = clean(l);
        if (!t) return true;
        if (/^(help|gelişmiş|advanced|close|menu|more|önceki|sonraki|previous|next|yardım|yığın\\s*izi|stack\\s*trace|gizlilik|privacy|terms|hizmet\\s*şartları|evet|hayır|yes|no)$/i.test(t)) return true;
        if (/yardımcı\\s*oldu\\s*mu|ürün\\s*güncellemeleri|product\\s*updates|durum\\s*kontrol\\s*paneli|status\\s*dashboard/i.test(t)) return true;
        if (/bu\\s*anr.?yi\\s*paylaş|share\\s*this\\s*anr|daha\\s*fazla\\s*bilgi|learn\\s*more|more\\s*info/i.test(t)) return true;
        if (/play-services-ads|çözülmesine\\s*yardımcı|paylaşın\\.?\\s*böylece|share\\s+with\\s+|uygulamanızın\\s*adını|full\\s*stack\\s*trace/i.test(t)) return true;
        if (/©\\s*\\d{4}|copyright\\s+\\d{4}|\\bgoogle\\s*llc\\b|was\\s*this\\s*helpful|feedback/i.test(t)) return true;
        if (/^(keyboard_arrow_|chevron_|feature_|arrow_|expand_|visibility_)/i.test(t)) return true;
        if (/^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$/.test(t) && t.length < 40 && !/\\d/.test(t)) return true;
        if (/^\\d{1,3}$/.test(t)) return true;
        return false;
      };
      const isFrameish = (l) =>
        /(?:^|\\s)at\\s+[\\w.$]+|#\\d+\\s+pc\\s+|SourceFile|\\.(java|kt|cpp|cc|c|so):\\d+|Exception|Error|SIG[A-Z]+|Native\\s+method|Input\\s+dispatching|ANR\\s+in\\s+|TimeoutException|java\\.|android\\.|kotlin\\.|dalvik\\.|lib[a-z0-9_]+\\.so/i.test(l);
      let stack = '';
      const stackIdx = lines.findIndex((l) =>
        /stack\\s*trace|yığın\\s*izi|backtrace|at\\s+[\\w.$]+\\(/i.test(l)
        || /^\\s*at\\s+/i.test(l)
        || /#(0|00)\\s+pc\\s+/i.test(l)
      );
      if (stackIdx >= 0) {
        const raw = lines.slice(stackIdx, stackIdx + 50).filter((l) => !isStackJunk(l)).slice(0, 40);
        const frames = raw.filter(isFrameish);
        stack = (frames.length ? frames : raw.filter((l) =>
          l.length < 220 && (l.includes('.') || l.includes('(')) && !/\\b(için|ile|your|please|click)\\b/i.test(l)
        )).slice(0, 35).join('\\n');
      } else {
        const codeish = lines.filter((l) =>
          !isStackJunk(l) && (
            /^at\\s+/i.test(l) || /\\(\\w+\\.\\w+:\\d+\\)/.test(l) || /SourceFile|#\\d+\\s+pc/.test(l) || isFrameish(l)
          )
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
        urls = [url]
        legacy = _vitals_issue_detail_url_legacy(iid, days=days, version_code=version_code)
        if legacy not in urls:
            urls.append(legacy)
        detail: dict[str, Any] = {}
        last_exc: Exception | None = None
        for try_url in urls:
            try:
                page.goto(try_url, wait_until="domcontentloaded", timeout=120_000)
                _settle(page, seconds=3.5)
                need, _, _ = _page_needs_login(page)
                if need and headed:
                    _wait_until_console(page)
                    page.goto(try_url, wait_until="domcontentloaded", timeout=120_000)
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
                series = _scrape_vitals_issue_events_series(page)
                if series:
                    detail["events_series"] = series
                if detail.get("issue_id") or detail.get("title") or detail.get("stack_trace"):
                    url = try_url
                    break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                continue
        if detail:
            if not detail.get("issue_id"):
                detail["issue_id"] = iid
            detail["url"] = url
            detail["list_title"] = str(iss.get("title") or "")[:240]
            detail["list_subtitle"] = str(iss.get("subtitle") or "")[:240]
            out[iid] = detail
        else:
            out[iid] = {
                "issue_id": iid,
                "url": url,
                "error": str(last_exc)[:200] if last_exc else "detail empty",
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
        _wait_until_console(page)
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
    all_issues_for_details: list[dict[str, Any]] = []
    seen_detail_ids: set[str] = set()
    prefetched_details: dict[str, dict[str, Any]] = {}
    summary_rate: str | None = None
    list_url = page.url
    row_nav_done = False
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
                try:
                    page.wait_for_selector(
                        'a[href*="/vitals/crashes/"]',
                        timeout=12_000,
                    )
                except Exception:
                    pass
                _scroll_vitals_issues_table(page)
            else:
                # Menüyü Escape ile kapat
                try:
                    page.keyboard.press("Escape")
                except Exception:
                    pass
                time.sleep(0.3)

        list_url = page.url
        _scroll_vitals_issues_table(page)
        snap = _extract_vitals_issue_snapshot(page) or {}
        issues = snap.get("issues") if isinstance(snap.get("issues"), list) else []
        cards = snap.get("cards") if isinstance(snap.get("cards"), list) else []
        if not summary_rate and snap.get("summary_rate"):
            summary_rate = str(snap.get("summary_rate") or "").strip() or None
        if not summary_rate:
            for c in cards:
                if not isinstance(c, dict):
                    continue
                title = str(c.get("title") or "")
                value = str(c.get("value") or "").strip()
                if "%" in value and re.search(r"oran|rate", title, re.I):
                    summary_rate = value
                    break
        count_raw = snap.get("issue_count")
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
            row = {
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
            clean_issues.append(row)
            # Tüm kategorilerden benzersiz sorun → detay sayfası çek
            if iid and iid not in seen_detail_ids:
                seen_detail_ids.add(iid)
                all_issues_for_details.append(row)
        if len(clean_issues) == 0 and _vitals_row_nav_enabled() and not row_nav_done:
            row_issues, row_details = _scrape_vitals_issues_via_table_rows(
                page,
                list_url=list_url,
                days=days,
                version_code=vc,
                scrape_details=scrape_details,
                headed=headed,
            )
            row_nav_done = True
            if row_issues:
                clean_issues = row_issues
                for ri in row_issues:
                    iid = str(ri.get("issue_id") or "").strip()
                    if iid and iid not in seen_detail_ids:
                        seen_detail_ids.add(iid)
                        all_issues_for_details.append(ri)
            if row_details:
                prefetched_details.update(row_details)
        if clean_issues:
            count_raw = str(len(clean_issues))
        elif count_raw is None:
            count_raw = None
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

    general_cat = next((c for c in categories_out if c.get("id") == "general"), None)
    if general_cat and not (general_cat.get("issues") or []):
        merged: list[dict[str, Any]] = []
        seen_merge: set[str] = set()
        best_cards: list[dict[str, Any]] = []
        for cat in categories_out:
            if cat.get("id") == "general":
                continue
            for iss in cat.get("issues") or []:
                if not isinstance(iss, dict):
                    continue
                iid = str(iss.get("issue_id") or "").strip()
                if iid and iid in seen_merge:
                    continue
                if iid:
                    seen_merge.add(iid)
                merged.append(iss)
            if not best_cards and cat.get("cards"):
                best_cards = list(cat.get("cards") or [])
        if merged:
            general_cat["issues"] = merged[:50]
            general_cat["issue_row_count"] = len(merged[:50])
            general_cat["issue_count"] = str(len(merged[:50]))
            if not general_cat.get("cards") and best_cards:
                general_cat["cards"] = best_cards[:8]

    # Sürüm filtresi 0 sorun döndürdüyse — filtresiz geçiş (issue satırlarını kaçırma)
    total_issues = sum(int(c.get("issue_row_count") or 0) for c in categories_out)
    if total_issues == 0 and str(vc or "").strip():
        print(
            f"    · {error_type}: versionCode={vc} boş — filtresiz yeniden tarama …",
            flush=True,
        )
        return _scrape_vitals_crashes_error_type(
            page,
            error_type=error_type,
            days=days,
            headed=headed,
            version_code="",
            scrape_details=scrape_details,
        )

    issue_details: dict[str, dict[str, Any]] = dict(prefetched_details)
    if scrape_details and all_issues_for_details:
        remaining = [
            iss
            for iss in all_issues_for_details
            if str(iss.get("issue_id") or "").strip() not in issue_details
        ]
        if remaining:
            print(
                f"  · vitals issue details ({error_type}) "
                f"candidates={len(remaining)} …",
                flush=True,
            )
            issue_details.update(
                _scrape_vitals_issue_details(
                    page,
                    remaining,
                    days=days,
                    version_code=vc,
                    headed=headed,
                )
            )
        # Listeye geri dön — sonraki error type / overview için temiz bağlam
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            _settle(page, seconds=2.5)
        except Exception:
            pass
    elif prefetched_details:
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
        "summary_rate": summary_rate,
        "categories": categories_out,
        "category_count": len(categories_out),
        "issue_details": issue_details,
        "issue_detail_count": len(issue_details),
    }


def _scrape_vitals_metrics_overview(
    page, *, headed: bool = True, version_code: str | None = None
) -> dict[str, Any]:
    url = VITALS_METRICS_OVERVIEW_URL
    vc = str(version_code or "").strip()
    if vc:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}versionCode={vc}"
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    _settle(page, seconds=5.0)
    need, _, _ = _page_needs_login(page)
    if need and headed:
        _wait_until_console(page)
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
            "Kararlılık",
            "Bellek",
            "Stability",
            "Memory",
        ),
        timeout_sec=50.0,
    )
    _scroll_vitals_overview_deep(page)
    _settle(page, seconds=2.0)
    extracted = _extract_vitals_metrics_overview(page) or {}
    rows = extracted.get("rows") if isinstance(extracted.get("rows"), list) else []
    sections = extracted.get("sections") if isinstance(extracted.get("sections"), list) else []

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

    print(
        f"    · metrics overview rows={len(rows)} sections={len(sections)} "
        f"recs={extracted.get('recommendation_count', 0)}",
        flush=True,
    )
    return {
        "url": url,
        "page_title": str(extracted.get("page_title") or "")[:160],
        "peer_group": str(extracted.get("peer_group") or "")[:160],
        "recommendations": extracted.get("recommendations") or [],
        "recommendation_count": int(extracted.get("recommendation_count") or 0),
        "summary_cards": extracted.get("summary_cards") or [],
        "sections": sections,
        "section_count": len(sections),
        "rows": rows,
        "row_count": len(rows),
        "anr_drilldown": anr_drill,
        "body_len": int(extracted.get("body_len") or 0),
    }


def _harvest_version_names_from_vitals_block(
    block: dict[str, Any], version_name_map: dict[str, str]
) -> dict[str, str]:
    """Issue / detay metinlerinden 290 (9.5.10) eşlemelerini topla."""
    out = dict(version_name_map)
    for det in (block.get("issue_details") or {}).values():
        if not isinstance(det, dict):
            continue
        blob = " ".join(
            [
                str(det.get("title") or ""),
                str(det.get("list_title") or ""),
                " ".join(
                    str(c.get("value") or "")
                    for c in (det.get("summary_cards") or [])
                    if isinstance(c, dict)
                ),
            ]
        )
        for m in re.finditer(r"\b(\d{2,8})\s*\((\d+\.\d+(?:\.\d+)*)\)", blob):
            out[m.group(1)] = m.group(2).strip()
    for cat in block.get("categories") or []:
        if not isinstance(cat, dict):
            continue
        for iss in cat.get("issues") or []:
            if not isinstance(iss, dict):
                continue
            av = str(iss.get("affected_versions") or "").strip()
            m = re.match(r"^(\d{2,8})\s*\(([^)]+)\)$", av)
            if m:
                out[m.group(1)] = m.group(2).strip()
    return out


def _scrape_vitals_bundle(page, *, headed: bool = True, days: int = 28) -> dict[str, Any]:
    """Crashes 4 kategori (CRASH+ANR) × son 3 sürüm + metrics overview."""
    version_name_map: dict[str, str] = {}
    codes = _vitals_version_codes()
    # codes boş → tek geçiş (tüm sürümler / filtresiz)
    pass_codes: list[str | None] = list(codes) if codes else [None]
    by_version: dict[str, Any] = {}
    primary_crash: dict[str, Any] = {}
    primary_anr: dict[str, Any] = {}
    primary_vc: str | None = codes[0] if codes else None
    vitals_errors: list[str] = []

    for idx, vc in enumerate(pass_codes):
        label = vc or "all"
        if not _page_is_alive(page):
            vitals_errors.append(f"{label}:browser_closed")
            print("  · vitals durdu: tarayıcı/pencere kapalı", flush=True)
            break
        # Detay sayfaları yalnızca en yeni sürümde (süre); diğerlerinde liste yeterli
        want_details = idx == 0
        if vc:
            print(f"  · vitals versionCode={vc} (details={want_details}) …", flush=True)
        else:
            print("  · vitals (all versions) …", flush=True)
        try:
            print(f"  · vitals crashes (CRASH) [{label}] …", flush=True)
            crash = _scrape_vitals_crashes_error_type(
                page,
                error_type="CRASH",
                days=days,
                headed=headed,
                version_code=vc if vc is not None else "",
                scrape_details=want_details,
            )
            version_name_map = _merge_version_name_maps(
                version_name_map, _extract_version_name_map(page)
            )
            version_name_map = _harvest_version_names_from_vitals_block(crash, version_name_map)
            print(f"  · vitals crashes (ANR) [{label}] …", flush=True)
            anr = _scrape_vitals_crashes_error_type(
                page,
                error_type="ANR",
                days=days,
                headed=headed,
                version_code=vc if vc is not None else "",
                scrape_details=want_details,
            )
            version_name_map = _merge_version_name_maps(
                version_name_map, _extract_version_name_map(page)
            )
            version_name_map = _harvest_version_names_from_vitals_block(anr, version_name_map)
            key = str(vc) if vc else "all"
            by_version[key] = {"crashes": {"CRASH": crash, "ANR": anr}}
            if idx == 0:
                primary_crash, primary_anr = crash, anr
                if vc:
                    primary_vc = str(vc)
        except Exception as exc:  # noqa: BLE001
            err = str(exc)[:240]
            vitals_errors.append(f"{label}:{err}")
            print(f"  · vitals [{label}] hata: {exc}", flush=True)
            if not _page_is_alive(page):
                print("  · vitals durdu: tarayıcı/pencere kapalı", flush=True)
                break

    overview: dict[str, Any] = {}
    overview_by_version: dict[str, Any] = {}
    anr_latest_7d: dict[str, Any] | None = None
    if _page_is_alive(page):
        try:
            print("  · vitals metrics overview …", flush=True)
            overview = _scrape_vitals_metrics_overview(page, headed=headed)
            version_name_map = _merge_version_name_maps(
                version_name_map, _extract_version_name_map(page)
            )
            if primary_vc:
                print(f"  · vitals metrics overview versionCode={primary_vc} …", flush=True)
                ov_v = _scrape_vitals_metrics_overview(
                    page, headed=headed, version_code=str(primary_vc)
                )
                overview_by_version[str(primary_vc)] = ov_v
                version_name_map = _merge_version_name_maps(
                    version_name_map, _extract_version_name_map(page)
                )
        except Exception as exc:  # noqa: BLE001
            vitals_errors.append(f"overview:{str(exc)[:240]}")
            print(f"  · vitals metrics overview hata: {exc}", flush=True)
        if primary_vc and _page_is_alive(page):
            try:
                print(f"  · vitals ANR 7d versionCode={primary_vc} …", flush=True)
                anr_7d = _scrape_vitals_crashes_error_type(
                    page,
                    error_type="ANR",
                    days=7,
                    headed=headed,
                    version_code=str(primary_vc),
                    scrape_details=False,
                )
                anr_latest_7d = {
                    "days": 7,
                    "version_code": str(primary_vc),
                    "block": anr_7d,
                }
            except Exception as exc:  # noqa: BLE001
                vitals_errors.append(f"anr_7d:{str(exc)[:240]}")
                print(f"  · vitals ANR 7d hata: {exc}", flush=True)
    else:
        vitals_errors.append("browser_closed_before_overview")
        print("  · vitals overview atlandı: tarayıcı/pencere kapalı", flush=True)

    versions: list[dict[str, str]] = []
    for c in pass_codes:
        if c is None:
            continue
        versions.append(
            {
                "code": str(c),
                "name": version_name_map.get(str(c), ""),
            }
        )
    # Map’ten eksik isimleri tamamla
    if not versions and version_name_map:
        for code in sorted(version_name_map.keys(), key=lambda x: int(x) if x.isdigit() else 0, reverse=True)[:3]:
            versions.append({"code": code, "name": version_name_map[code]})

    if version_name_map:
        print(f"    · version_name_map={len(version_name_map)} versions={versions}", flush=True)
    # En yeni sürümden çekilen detayları aynı issue_id ile diğer sürümlere kopyala
    shared_details: dict[str, dict[str, Any]] = {}
    for et in ("CRASH", "ANR"):
        block = (by_version.get(str(primary_vc) if primary_vc else "all") or {}).get(
            "crashes", {}
        )
        if not isinstance(block, dict):
            continue
        et_block = block.get(et) if isinstance(block.get(et), dict) else {}
        for iid, det in (et_block.get("issue_details") or {}).items():
            if iid and isinstance(det, dict):
                shared_details[f"{et}:{iid}"] = det
    for key, payload in by_version.items():
        if primary_vc and key == str(primary_vc):
            continue
        crashes = payload.get("crashes") if isinstance(payload, dict) else None
        if not isinstance(crashes, dict):
            continue
        for et in ("CRASH", "ANR"):
            et_block = crashes.get(et) if isinstance(crashes.get(et), dict) else None
            if not isinstance(et_block, dict):
                continue
            details = et_block.get("issue_details")
            if not isinstance(details, dict):
                details = {}
                et_block["issue_details"] = details
            issue_ids = {
                str(iss.get("issue_id") or "").strip()
                for cat in (et_block.get("categories") or [])
                if isinstance(cat, dict)
                for iss in (cat.get("issues") or [])
                if isinstance(iss, dict) and iss.get("issue_id")
            }
            for iid in issue_ids:
                sk = f"{et}:{iid}"
                if iid not in details and sk in shared_details:
                    details[iid] = dict(shared_details[sk])
            et_block["issue_detail_count"] = len(details)

    detail_n = int(primary_crash.get("issue_detail_count") or 0) + int(
        primary_anr.get("issue_detail_count") or 0
    )
    print(f"    · issue_details total={detail_n} by_version={list(by_version.keys())}", flush=True)

    out: dict[str, Any] = {
        "version": 3,
        "days": days,
        "version_code": primary_vc,
        "versions": versions,
        "by_version": by_version,
        "is_user_perceived": True,
        "crashes": {"CRASH": primary_crash, "ANR": primary_anr},
        "metrics_overview": overview,
        "metrics_overview_by_version": overview_by_version,
        "version_name_map": version_name_map,
        "anr_latest_7d": anr_latest_7d,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if vitals_errors:
        out["error"] = "; ".join(vitals_errors[:4])[:240]
    return out


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
            from backend.services.selenium_playwright_shim import play_console_use_selenium

            try:
                body = page.inner_text("body")[:2000]
            except Exception:
                body = ""
            blocked = (not play_console_use_selenium()) and google_blocks_automation_text(body)
            waited = False if blocked else _wait_until_console(page)
            if blocked or not waited:
                _release_context(pw, context)
                pw = context = None
                if not _system_firefox_relogin().get("ok"):
                    return {
                        "ok": False,
                        "needs_login": True,
                        "message": (
                            "Play login gerekli — "
                            ".venv/bin/python scripts/play_console_scrape.py --login "
                            "(gerçek Firefox.app; Playwright penceresinde Google çalışmaz)"
                        ),
                        "url": url,
                        "metrics": [],
                        "reviews": [],
                        "rating_summary": {},
                        "raw_network": [],
                    }
                pw, context = _launch_context(headed=True)
                _attach_network_capture_context(context, network)
                page = context.pages[0] if context.pages else context.new_page()
                _attach_network_capture(page, network)
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
        monitor_improve = _extract_dashboard_monitor_improve(page) or {}
        metrics = _metrics_from_structured(structured)
        debug = structured.get("debug") if isinstance(structured.get("debug"), dict) else {}
        if isinstance(monitor_improve, dict):
            debug = {**debug, "monitor_improve": monitor_improve}

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
        store_listings = _safe_scrape_page(
            page,
            url=STORE_LISTINGS_URL,
            known=tuple(
                dict.fromkeys(
                    list(_KNOWN_STORE_LISTINGS) + list(_KNOWN_GROW) + list(_KNOWN_DASHBOARD)
                )
            ),
            page_key="store_listings",
            wait_needles=(
                "Mağaza",
                "Store listing",
                "Edinme",
                "Acquisition",
                "Ziyaret",
                "Dönüşüm",
                "Conversion",
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

        # Reach and devices · dashboard + Android sürüm / RAM / SoC … kırılımları
        devices_bundle: dict[str, Any] = {}
        try:
            devices_bundle = _scrape_devices_dashboard(page, headed=bool(headed), days=28)
        except Exception as exc:  # noqa: BLE001
            devices_bundle = {"url": DEVICES_URL, "cards": [], "breakdowns": [], "error": str(exc)[:240]}
            print(f"  · devices scrape hata: {exc}", flush=True)

        # Android Vitals: crashes 4 kategori + metrics overview (crash/ANR/LMK)
        vitals_bundle: dict[str, Any] = {}
        try:
            vitals_bundle = _scrape_vitals_bundle(page, headed=bool(headed), days=28)
            if vitals_bundle.get("error"):
                print(f"  · vitals uyarı: {vitals_bundle.get('error')}", flush=True)
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

        if not _page_is_alive(page):
            print(
                "  · stats atlandı: tarayıcı/pencere kapalı — vitals/overview için "
                "`--vitals-only --sync --ingest` yeterli",
                flush=True,
            )
        for view in STATISTICS_VIEWS:
            if not _page_is_alive(page):
                break
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
            force_table = bool(view.get("force_table")) or str(view.get("metric_key") or "") == "store_listing_conversion"
            try:
                proto_rows = len((proto or {}).get("1") or []) if isinstance(proto, dict) else 0
                if force_table or proto is None or proto_rows < 30:
                    print(
                        f"    protobuf zayıf ({proto_rows}) — tablo fallback"
                        + (" [force]" if force_table else ""),
                        flush=True,
                    )
                    page_text = _collect_paginated_table_text(page, max_pages=8)
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
        store_listings_cards, store_listings_br = _append_page_metrics(
            metrics, store_listings, kind="store_listings", page_key="store_listings"
        )
        monitor_cards, monitor_br = _append_page_metrics(
            metrics, monitor, kind="monitor", page_key="monitor"
        )
        release_cards, release_br = _append_page_metrics(
            metrics, release, kind="release", page_key="release"
        )
        devices_cards, devices_br = _append_page_metrics(
            metrics,
            {
                "cards": devices_bundle.get("cards") or [],
                "breakdowns": devices_bundle.get("breakdowns") or [],
                "error": devices_bundle.get("error"),
            },
            kind="devices",
            page_key="devices",
        )

        dash_cards = structured.get("tpg") or structured.get("cards") or []
        dash_br = list(structured.get("breakdowns") or [])
        all_br = (
            dash_br
            + list(mon_br)
            + list(grow_br)
            + list(store_listings_br)
            + list(monitor_br)
            + list(release_br)
            + list(devices_br)
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
            "store_listings": store_listings_cards,
            "monitor": monitor_cards,
            "release": release_cards,
            "devices": devices_cards,
            "statistics": stats_cards,
            "vitals": vitals_bundle,
            "version_name_map": version_name_map,
            "dashboard_monitor_improve": monitor_improve if isinstance(monitor_improve, dict) else {},
            "pages": {
                "dashboard": {
                    "url": DASHBOARD_URL,
                    "cards": dash_cards,
                    "breakdowns": dash_br,
                    "debug": debug,
                },
                "monetize": _page_payload(MONETIZE_URL, monetize),
                "grow": _page_payload(GROW_URL, grow),
                "store_listings": _page_payload(STORE_LISTINGS_URL, store_listings),
                "monitor": _page_payload(MONITOR_URL, monitor),
                "release": _page_payload(RELEASE_URL, release),
                "devices": {
                    "url": str(devices_bundle.get("url") or DEVICES_URL),
                    "cards": devices_cards,
                    "breakdowns": devices_br,
                    "breakdown_pages": devices_bundle.get("breakdown_pages") or {},
                    "error": devices_bundle.get("error"),
                    "debug": devices_bundle.get("debug") or {},
                },
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
            "store_listings_count": len(store_listings_cards),
            "monitor_count": len(monitor_cards),
            "release_count": len(release_cards),
            "devices_count": len(devices_cards),
            "devices_breakdown_count": len(devices_br),
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
            _wait_until_console(page)
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
        # Günlük puan adedi (yorumlu/yorumsuz kırılımı için)
        dist_rows = _download_ratings_distribution_csv(page) or []
        count_facts = _ratings_count_facts_from_distribution(dist_rows)
        if count_facts:
            # Tarih bazında birleştir — kısa CSV eski günleri silmesin
            by_date: dict[str, dict[str, Any]] = {}
            kept: list[dict[str, Any]] = []
            for f in explorer_facts:
                if not isinstance(f, dict):
                    continue
                if str(f.get("metric")) != "ratings_count":
                    kept.append(f)
                    continue
                ds = str(f.get("date") or "")[:10]
                if ds:
                    by_date[ds] = f
            for f in count_facts:
                ds = str(f.get("date") or "")[:10]
                if not ds:
                    continue
                prev = by_date.get(ds)
                new_stars = f.get("stars") if isinstance(f.get("stars"), dict) else {}
                prev_stars = (
                    prev.get("stars")
                    if isinstance(prev, dict) and isinstance(prev.get("stars"), dict)
                    else {}
                )
                if prev and prev_stars and any(prev_stars.values()) and not any(
                    (new_stars or {}).values()
                ):
                    continue
                by_date[ds] = f
            explorer_facts = kept + list(by_date.values())
            print(
                f"    → ratings_count facts={len(count_facts)} "
                f"(merged_total={len(by_date)})",
                flush=True,
            )
        else:
            print("    → ratings_count facts=0", flush=True)
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
            _wait_until_console(page)
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
            or panels.get("store_listings")
            or panels.get("monitor")
            or panels.get("devices")
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
            or panels.get("store_listings")
            or panels.get("monitor")
            or panels.get("devices")
            or panels.get("release")
            or panels.get("statistics")
        ) and debug:
            dbg = f" · dash_known={debug.get('known_found_count')} body={debug.get('body_len')}"
        msg = (
            f"Play tarama · {len(metrics)} metric · "
            f"{panels.get('tpg_count', 0)} dash · {panels.get('monetize_count', 0)} mon · "
            f"{panels.get('grow_count', 0)} grow · "
            f"{panels.get('store_listings_count', 0)} store · "
            f"{panels.get('monitor_count', 0)} monitor · "
            f"{panels.get('devices_count', 0)} devices · "
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
                "dashboard_monetize_grow_monitor_release_devices_vitals_"
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
        _release_context(pw, context)


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
            _wait_until_console(page)
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
            "message": f"Vitals tarama · categories={cat_n} overview_rows={ov_n}",
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
            "message": f"Vitals tarama hatası: {exc}",
            "panels": {},
        }
    finally:
        _release_context(pw, context)


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
            _wait_until_console(page)
        elif need:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Play Console login gerekli (--login veya headed sync)",
                "reviews": [],
            }
        print("  · reviews-only (last year) …", flush=True)
        days = _reviews_days()
        # 1) Play Store genel API — Console DOM’da görünmeyen/yüklenmeyen tüm yıl yorumları
        store_reviews: list[dict[str, Any]] = []
        try:
            from backend.services.play_store_reviews import fetch_play_store_reviews

            store_reviews = fetch_play_store_reviews(PACKAGE, days=days)
            print(f"    → play store public: {len(store_reviews)}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"    → play store public skip: {exc}", flush=True)
        # 2) Console DOM (yanıt/cihaz meta için tamamlayıcı)
        console_reviews = _scrape_reviews_list(page, days=days)
        print(f"    → play console dom: {len(console_reviews)}", flush=True)
        rating_summary = _extract_rating_summary_dom(page) or {}
        merged: dict[str, dict[str, Any]] = {}
        for r in list(store_reviews) + list(console_reviews or []):
            if not isinstance(r, dict):
                continue
            key = (
                str(r.get("review_id") or "").strip()
                or (
                    str(r.get("author") or "").strip().lower()
                    + "|"
                    + str(r.get("body") or "")[:80].strip().lower()
                    + "|"
                    + str(r.get("date_iso") or r.get("date") or "")[:16]
                )
            )
            if not key or key in merged:
                # Daha uzun body varsa güncelle
                if key in merged and len(str(r.get("body") or "")) > len(
                    str(merged[key].get("body") or "")
                ):
                    merged[key] = r
                continue
            merged[key] = r
        reviews = list(merged.values())
        ok = bool(reviews)
        return {
            "ok": ok,
            "needs_login": False,
            "message": (
                f"Reviews · store={len(store_reviews)} console={len(console_reviews or [])} "
                f"merged={len(reviews)} · days={days}"
            ),
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": [],
            "panels": {},
            "reviews": reviews,
            "rating_summary": rating_summary,
            "merge_reviews": True,
            "source": "play_store_public+console",
            "source_url": REVIEWS_URL,
            "sync_ok": ok,
            "sync_mode": "reviews_only",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Yorum tarama hatası: {exc}",
            "reviews": None,  # fail ingest'te mevcut yorumları ezme
            "merge_reviews": False,
        }
    finally:
        _release_context(pw, context)


def scrape_ratings_dist_only(*, headed: bool | None = None) -> dict[str, Any]:
    """Sadece Puan dağılımı CSV → ratings_count + star_1..5 (ömür boyu mümkünse)."""
    if headed is None:
        env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
        headed = env_hl not in ("1", "true", "yes")
    pw, context = _launch_context(headed=True if headed else False)
    try:
        page = context.new_page()
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        _settle(page, seconds=2.5)
        need, _, _ = _page_needs_login(page)
        if need and headed:
            _wait_until_console(page)
        elif need:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Play Console login gerekli (--login veya headed sync)",
                "panels": {},
            }
        dist_rows = _download_ratings_distribution_csv(page) or []
        count_facts = _ratings_count_facts_from_distribution(dist_rows)
        ok = bool(count_facts)
        dates = sorted(str(f.get("date") or "") for f in count_facts if f.get("date"))
        return {
            "ok": ok,
            "needs_login": False,
            "message": (
                f"Ratings distribution · facts={len(count_facts)}"
                + (
                    f" · {dates[0]}→{dates[-1]}"
                    if len(dates) >= 2
                    else ""
                )
            ),
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": [],
            "panels": {
                "explorer_facts": count_facts,
                "explorer_fact_count": len(count_facts),
            },
            "reviews": None,
            "rating_summary": None,
            "merge_ratings_counts": True,
            "source": "play_console_bridge",
            "source_url": RATINGS_URL,
            "sync_ok": ok,
            "sync_mode": "ratings_count_only",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Puan dağılımı tarama hatası: {exc}",
            "panels": {},
            "merge_ratings_counts": False,
        }
    finally:
        _release_context(pw, context)


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
    merge_ratings_counts = bool(result.get("merge_ratings_counts"))

    def _vitals_usable(vitals: Any) -> bool:
        if not isinstance(vitals, dict):
            return False
        ov = vitals.get("metrics_overview") if isinstance(vitals.get("metrics_overview"), dict) else {}
        if ov.get("rows") or ov.get("sections"):
            return True
        crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
        for k in ("CRASH", "ANR"):
            block = crashes.get(k) if isinstance(crashes.get(k), dict) else {}
            if block.get("categories") or block.get("summary_rate"):
                return True
        return False

    # vitals-only: birleştirme sunucuda yapılır (snapshot admin auth ister)
    if merge_ratings_counts:
        facts = []
        if isinstance(panels, dict):
            facts = [
                f
                for f in (panels.get("explorer_facts") or [])
                if isinstance(f, dict) and str(f.get("metric")) == "ratings_count"
            ]
        if not facts:
            return {"ok": False, "message": "merge_ratings_counts için ratings_count fact gerekli"}
        payload = {
            "metrics": [],
            "panels": {"explorer_facts": facts},
            "reviews": [],
            "rating_summary": {},
            "raw_network": [],
            "source": result.get("source") or "play_console_bridge",
            "source_url": result.get("source_url") or RATINGS_URL,
            "package_name": result.get("package_name") or PACKAGE,
            "app_id": result.get("app_id") or APP_ID,
            "sync_ok": bool(result.get("ok")),
            "sync_message": result.get("message"),
            "sync_mode": result.get("sync_mode") or "ratings_count_only",
            "merge_vitals": False,
            "merge_reviews": False,
            "merge_ratings_counts": True,
        }
    elif merge_vitals:
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
        facts_n = 0
        if isinstance(panels, dict):
            facts_n = len(
                [f for f in (panels.get("explorer_facts") or []) if isinstance(f, dict)]
            )
        if facts_n <= 0 and not result.get("merge_vitals") and not result.get("merge_reviews"):
            vitals = panels.get("vitals") if isinstance(panels, dict) else {}
            if _vitals_usable(vitals):
                payload = {
                    "metrics": metrics if metrics is not None else [],
                    "panels": panels if isinstance(panels, dict) else {},
                    "reviews": [],
                    "rating_summary": {},
                    "raw_network": result.get("raw_network") or [],
                    "source": result.get("source") or "play_console_bridge",
                    "source_url": result.get("source_url") or DASHBOARD_URL,
                    "package_name": result.get("package_name") or PACKAGE,
                    "app_id": result.get("app_id") or APP_ID,
                    "sync_ok": bool(result.get("ok")),
                    "sync_message": result.get("message"),
                    "sync_mode": "vitals_partial",
                    "merge_vitals": True,
                    "merge_reviews": False,
                }
            else:
                return {
                    "ok": False,
                    "message": (
                        "Ingest atlandı: explorer_facts boş — mevcut Railway snapshot korunur. "
                        "Play tarama istatistik görünümleri başarısız; "
                        "Firefox penceresini kapatmadan `--vitals-only --sync --ingest` deneyin."
                    ),
                    "http_status": 0,
                    "skipped_empty_facts": True,
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
    ratings_dist_only = "--ratings-dist-only" in args
    # --sync implies scrape (+ ingest if token)
    mode = (
        "vitals-only"
        if vitals_only
        else (
            "reviews-only"
            if reviews_only
            else ("ratings-dist-only" if ratings_dist_only else "")
        )
    )
    print(
        f"Play scrape · headed={headed}"
        + (f" · {mode}" if mode else ""),
        flush=True,
    )
    if vitals_only:
        result = scrape_vitals_only(headed=headed)
    elif reviews_only:
        result = scrape_reviews_only(headed=headed)
    elif ratings_dist_only:
        result = scrape_ratings_dist_only(headed=headed)
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
        if (
            result.get("merge_vitals")
            or result.get("merge_reviews")
            or result.get("merge_ratings_counts")
            or reviews_only
            or vitals_only
            or ratings_dist_only
        ):
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
