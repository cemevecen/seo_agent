#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde).

Notification stats + aktif haber listesi + Virgül reklam.

Tek sefer (ikisi):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --news-only
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --notifications-only

Daemon (otomatik + Elle yenile localhost:18765):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --daemon
  Play/ASC Firefox profili (~/.seo-agent/fx-*). needs_login → mail (ilk + 6 saat cooldown + resolved).

  POST /sync       → notification (08–20 her 30 dk live; gece 00:08 dünü mühürle)
  POST /sync-news  → news (08–20 her 30 dk)
  POST /sync-virgul → Virgül (04/07/13 TR · yalnız dün+bugün)
  POST /sync-play   → Play / Android (süre sınırı yok · dün+bugün · arka plan)
  POST /sync-asc    → ASC / iOS (3 saatte bir, :10)
  POST /sync-firebase → Firebase Console Crashlytics (günde bir sabah, varsayılan 06:10 TR)
  POST /sync-gsc-links → Backlinks (01:00 + 13:00 TR)
  POST /sync-revenue-targets → Ad hedef sheet (05:40 + 13:40 TR; gece fail → 5×3s retry)
  POST /sync-policy → Ad Manager Policy (01:05 + 13:05 TR)
  POST /sync-noads  → Sinemalar noAds (01:15 + 13:15 TR)
  POST /sync-sinemalar-moderation → Moderasyon (03:04 dün · 14:17 + 20:05 bugün TR)
  POST /sync-pagespeed → pagespeed.web.dev (01:10 + 13:10 TR)
  POST /sync-seo-audit → SEO meta audit scrape (02:45 + 14:45 TR, GA4 top 500)
  POST /sync-gsc-cwv → GSC CWV screenshot (doviz+sinemalar mobile/desktop; 03:00 + 15:00 TR)
  mode=full → eski derin scrape (AMP + issue drilldown)
  POST /sync-market → doviz.com piyasa tablo taraması (00:05 TR)
  GET  /status      → canlı izleme paneli (kuyruk / progress / sonraki slotlar)
  GET  /health      → aynı veri JSON
  POST /open-noads  → noAds sayfasını aç, textarea'ya URL yaz (policy «Ekle»)
  POST /sync-all   → notification + news
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import smtplib
import subprocess
import sys
import threading
import time
import traceback
from email.message import EmailMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    """`.env` yükle: dosya içinde son değer kazanır; mevcut os.environ ezilmez."""
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

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = int(os.environ.get("NOTIFICATION_BRIDGE_PORT") or "18765")
# Worker protokolü: kimlik + kabiliyet + otomatik iş kirası
BRIDGE_VERSION = "2026.08.17"
# Zamanlı (otomatik) taramalar bu makinede koşsun mu — iki Mac aynı işi iki kez yapmasın.
# 0/false → yalnızca panel kuyruğu işlenir. Varsayılan: açık (kira ile tekilleştirilir).
BRIDGE_AUTO_JOBS = (os.environ.get("BRIDGE_AUTO_JOBS") or "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
# Gözetimsiz daemon: oturum ölüyse 15 dk beklemek yerine hızlı düş, kuyruğu bloklama.
os.environ.setdefault("SCRAPE_LOGIN_WAIT_SEC", os.environ.get("BRIDGE_LOGIN_WAIT_SEC") or "150")
# Auto-loop poll (slot kaçırmamak için kısa); iş aralıkları ayrı.
AUTO_POLL_SEC = int(os.environ.get("BRIDGE_AUTO_POLL_SEC") or "60")
# Interval-based
AUTO_INTERVAL_SEC = int(
    os.environ.get("NOTIFICATION_BRIDGE_INTERVAL_SEC") or str(30 * 60)
)  # notification: 30 dk
NEWS_AUTO_INTERVAL_SEC = int(
    os.environ.get("NEWS_BRIDGE_INTERVAL_SEC") or str(30 * 60)
)  # news: 30 dk (notification ile aynı)
# Notification + News aktif pencere (Europe/Istanbul): 08:00–20:00 (saat 20 dahil)
NT_NEWS_ACTIVE_START_HOUR = int(os.environ.get("NT_NEWS_ACTIVE_START_HOUR") or "8")
NT_NEWS_ACTIVE_END_HOUR = int(os.environ.get("NT_NEWS_ACTIVE_END_HOUR") or "20")
# Gece dönümü: dünün notification gövdesini mühürle / kayda al
NOTIFICATION_NIGHT_SEAL_HOUR = int(os.environ.get("NOTIFICATION_NIGHT_SEAL_HOUR") or "0")
NOTIFICATION_NIGHT_SEAL_MINUTE = int(os.environ.get("NOTIFICATION_NIGHT_SEAL_MINUTE") or "8")
PLAY_AUTO_INTERVAL_SEC = int(
    os.environ.get("PLAY_CONSOLE_BRIDGE_INTERVAL_SEC") or str(6 * 60 * 60)
)  # android: 6 saat (health/docs; slot kullanılır)
ASC_AUTO_INTERVAL_SEC = int(
    os.environ.get("ASC_CONSOLE_BRIDGE_INTERVAL_SEC") or str(3 * 60 * 60)
)  # ios: 3 saat
VIRGUL_AUTO_INTERVAL_SEC = int(
    os.environ.get("VIRGUL_BRIDGE_INTERVAL_SEC") or str(6 * 60 * 60)
)  # health/docs; slot kullanılır
GSC_LINKS_AUTO_INTERVAL_SEC = int(
    os.environ.get("GSC_LINKS_BRIDGE_INTERVAL_SEC") or str(12 * 60 * 60)
)
# Slot pencereleri Europe/Istanbul — dakikalar birbirinden ≥4 dk ayrı (scrape çakışması azaltılır)
VIRGUL_SLOT_HOURS = (4, 7, 13)  # gece 04 · sabah 07 · öğlen 13 (dün+bugün)
VIRGUL_SLOT_MINUTE = int(os.environ.get("VIRGUL_BRIDGE_MINUTE") or "8")
_VIRGUL_HOURS_RAW = (os.environ.get("VIRGUL_BRIDGE_HOURS") or "").strip()
if _VIRGUL_HOURS_RAW:
    VIRGUL_SLOT_HOURS = tuple(
        int(h.strip()) for h in _VIRGUL_HOURS_RAW.split(",") if h.strip().isdigit()
    ) or VIRGUL_SLOT_HOURS
PLAY_SLOT_HOURS = (0, 6, 12, 18)  # 6 saatte bir — login baskısını düşür
PLAY_SLOT_MINUTE = int(os.environ.get("PLAY_CONSOLE_BRIDGE_MINUTE") or "2")
ASC_SLOT_HOURS = (0, 3, 6, 9, 12, 15, 18, 21)  # ASC 3 saat (Play’den ayrı)
ASC_SLOT_MINUTE = int(os.environ.get("ASC_CONSOLE_BRIDGE_MINUTE") or "11")
# Mühürlü gövde: Play/ASC/GSC Links günde 1 (dün dilimi) — full history yok
try:
    from backend.services.history_seal import is_pipeline_sealed, mark_all_expensive_pipelines_sealed

    if is_pipeline_sealed("play") and not (os.environ.get("PLAY_CONSOLE_BRIDGE_HOURS") or "").strip():
        PLAY_SLOT_HOURS = (6,)
    if is_pipeline_sealed("asc") and not (os.environ.get("ASC_CONSOLE_BRIDGE_HOURS") or "").strip():
        ASC_SLOT_HOURS = (6,)
    # Mevcut panel verisini mühürle (idempotent meta)
    mark_all_expensive_pipelines_sealed()
except Exception:
    pass
_PLAY_HOURS_RAW = (os.environ.get("PLAY_CONSOLE_BRIDGE_HOURS") or "").strip()
if _PLAY_HOURS_RAW:
    PLAY_SLOT_HOURS = tuple(
        int(h.strip()) for h in _PLAY_HOURS_RAW.split(",") if h.strip().isdigit()
    ) or PLAY_SLOT_HOURS
_ASC_HOURS_RAW = (os.environ.get("ASC_CONSOLE_BRIDGE_HOURS") or "").strip()
if _ASC_HOURS_RAW:
    ASC_SLOT_HOURS = tuple(
        int(h.strip()) for h in _ASC_HOURS_RAW.split(",") if h.strip().isdigit()
    ) or ASC_SLOT_HOURS
FIREBASE_SLOT_HOURS = (6,)  # günde bir — sabah Firebase Console scrape
_FIREBASE_HOURS_RAW = (os.environ.get("FIREBASE_CONSOLE_BRIDGE_HOURS") or "").strip()
if _FIREBASE_HOURS_RAW:
    FIREBASE_SLOT_HOURS = tuple(
        int(h.strip()) for h in _FIREBASE_HOURS_RAW.split(",") if h.strip().isdigit()
    ) or FIREBASE_SLOT_HOURS
FIREBASE_SLOT_MINUTE = int(os.environ.get("FIREBASE_CONSOLE_BRIDGE_MINUTE") or "46")
TWICE_DAILY_HOURS = (1, 13)  # 01:xx + 13:xx
GSC_LINKS_SLOT_HOURS = TWICE_DAILY_HOURS
try:
    from backend.services.history_seal import is_pipeline_sealed as _seal_gsc

    if _seal_gsc("gsc_links") and not (os.environ.get("GSC_LINKS_BRIDGE_HOURS") or "").strip():
        GSC_LINKS_SLOT_HOURS = (1,)  # günde bir snapshot
except Exception:
    pass
_GSC_HOURS_RAW = (os.environ.get("GSC_LINKS_BRIDGE_HOURS") or "").strip()
if _GSC_HOURS_RAW:
    GSC_LINKS_SLOT_HOURS = tuple(
        int(h.strip()) for h in _GSC_HOURS_RAW.split(",") if h.strip().isdigit()
    ) or GSC_LINKS_SLOT_HOURS
REVENUE_TARGETS_SLOT_HOURS = (5, 13)  # 05:34 + 13:34 TR
GSC_SLOT_MINUTE = int(os.environ.get("GSC_LINKS_BRIDGE_MINUTE") or "14")
REVENUE_TARGETS_SLOT_MINUTE = int(os.environ.get("REVENUE_TARGETS_BRIDGE_MINUTE") or "40")
POLICY_SLOT_MINUTE = int(os.environ.get("ADMANAGER_POLICY_BRIDGE_MINUTE") or "24")
SPEED_SLOT_MINUTE = int(os.environ.get("PAGESPEED_BRIDGE_MINUTE") or "28")
NOADS_SLOT_MINUTE = int(os.environ.get("SINEMALAR_NOADS_BRIDGE_MINUTE") or "32")
# SEO audit: pagespeed/noAds sonrası — 02:38 + 14:38 TR
SEO_AUDIT_SLOT_HOURS = (2, 14)
SEO_AUDIT_SLOT_MINUTE = int(os.environ.get("SEO_AUDIT_BRIDGE_MINUTE") or "38")
  # GSC CWV screenshots — 03:42 + 15:42 TR (mode=full for AMP deep scrape)
GSC_CWV_SLOT_HOURS = (3, 15)
GSC_CWV_SLOT_MINUTE = int(os.environ.get("GSC_CWV_BRIDGE_MINUTE") or "42")
# Piyasa tablo taraması — günde bir, 00:16 TR
MARKET_SLOT_HOURS = (0,)
MARKET_SLOT_MINUTE = int(os.environ.get("MARKET_TARAMA_BRIDGE_MINUTE") or "16")
# Empower Intelligence Yesterday — mühürlü: günde 1; aksi halde 02:12 + 13:18 TR
EMPOWER_INTEL_SLOTS: tuple[tuple[int, int], ...] = ((2, 12), (13, 18))
try:
    from backend.services.history_seal import is_pipeline_sealed as _seal_emp

    if _seal_emp("empower") and not (os.environ.get("EMPOWER_INTEL_BRIDGE_SLOTS") or "").strip():
        EMPOWER_INTEL_SLOTS = ((6, 12),)
except Exception:
    pass
_EMPOWER_SLOTS_RAW = (os.environ.get("EMPOWER_INTEL_BRIDGE_SLOTS") or "").strip()
if _EMPOWER_SLOTS_RAW:
    parsed_slots: list[tuple[int, int]] = []
    for part in _EMPOWER_SLOTS_RAW.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        hs, ms = part.split(":", 1)
        if hs.isdigit() and ms.isdigit():
            parsed_slots.append((int(hs), int(ms)))
    if parsed_slots:
        EMPOWER_INTEL_SLOTS = tuple(parsed_slots)
# Sinemalar Empower: döviz slotlarından +5 dk
EMPOWER_INTEL_SINEMALAR_SLOTS: tuple[tuple[int, int], ...] = tuple(
    (h, (m + 5) % 60) if m + 5 < 60 else ((h + 1) % 24, (m + 5) % 60)
    for h, m in EMPOWER_INTEL_SLOTS
)
_EMPOWER_SIN_SLOTS_RAW = (os.environ.get("EMPOWER_INTEL_SINEMALAR_BRIDGE_SLOTS") or "").strip()
if _EMPOWER_SIN_SLOTS_RAW:
    parsed_sin: list[tuple[int, int]] = []
    for part in _EMPOWER_SIN_SLOTS_RAW.split(","):
        part = part.strip()
        if ":" not in part:
            continue
        hs, ms = part.split(":", 1)
        if hs.isdigit() and ms.isdigit():
            parsed_sin.append((int(hs), int(ms)))
    if parsed_sin:
        EMPOWER_INTEL_SINEMALAR_SLOTS = tuple(parsed_sin)
SLOT_WINDOW_MIN = int(os.environ.get("BRIDGE_SLOT_WINDOW_MIN") or "35")
# Tarayıcı scrape'leri arası minimum boşluk (aynı 2–3 dk içinde ikinci scrape başlamasın)
BRIDGE_SCRAPE_MIN_GAP_SEC = int(os.environ.get("BRIDGE_SCRAPE_MIN_GAP_SEC") or "180")
# Başarısız otomatik tur → en fazla 3 yeniden deneme, 10'ar dk arayla
BRIDGE_RETRY_MAX = int(os.environ.get("BRIDGE_RETRY_MAX") or "3")
BRIDGE_RETRY_GAP_SEC = int(os.environ.get("BRIDGE_RETRY_GAP_SEC") or str(10 * 60))
# Revenue targets gece slotu (05:xx) başarısız → 3 saatte bir, en fazla 5 yeniden deneme
REVENUE_TARGETS_NIGHT_RETRY_MAX = int(os.environ.get("REVENUE_TARGETS_NIGHT_RETRY_MAX") or "5")
REVENUE_TARGETS_NIGHT_RETRY_GAP_SEC = int(
    os.environ.get("REVENUE_TARGETS_NIGHT_RETRY_GAP_SEC") or str(3 * 3600)
)
_NEWS_EVERY_N_RAW = (os.environ.get("NEWS_BRIDGE_EVERY_N") or "").strip()
NEWS_AUTO_EVERY_N = int(_NEWS_EVERY_N_RAW) if _NEWS_EVERY_N_RAW.isdigit() else 0
# Geriye dönük isimler
POLICY_AUTO_HOUR = TWICE_DAILY_HOURS[0]
POLICY_AUTO_MINUTE = POLICY_SLOT_MINUTE
NOADS_AUTO_HOURS = list(TWICE_DAILY_HOURS)
NOADS_AUTO_MINUTE = NOADS_SLOT_MINUTE
MODERATION_SLOTS: tuple[tuple[int, int, str], ...] = (
    (3, 4, "yesterday"),
    (14, 17, "today"),
    (20, 5, "today"),  # akşam Today paneli dolu kalsın
)
try:
    from backend.services.history_seal import is_pipeline_sealed as _seal_mod

    # Mühürlü: yalnız dün (bugün kalıcı kayda yazılmaz)
    if _seal_mod("sinemalar_moderation") and not (
        os.environ.get("SINEMALAR_MODERATION_BRIDGE_SLOTS") or ""
    ).strip():
        MODERATION_SLOTS = ((3, 4, "yesterday"),)
except Exception:
    pass
_MOD_SLOTS_RAW = (os.environ.get("SINEMALAR_MODERATION_BRIDGE_SLOTS") or "").strip()
if _MOD_SLOTS_RAW:
    # hour:minute:which,...  e.g. 3:4:yesterday,14:17:today
    parsed_mod: list[tuple[int, int, str]] = []
    for part in _MOD_SLOTS_RAW.split(","):
        bits = [b.strip() for b in part.split(":")]
        if len(bits) >= 3 and bits[0].isdigit() and bits[1].isdigit():
            which = bits[2] if bits[2] in ("yesterday", "today", "both") else "yesterday"
            parsed_mod.append((int(bits[0]), int(bits[1]), which))
    if parsed_mod:
        MODERATION_SLOTS = tuple(parsed_mod)
BRIDGE_ALERT_TO = (
    os.environ.get("BRIDGE_ALERT_EMAIL")
    or os.environ.get("OPERATIONS_MAIL_TO")
    or os.environ.get("MAIL_TO")
    or "cemevecen@nokta.com"
).strip()
BRIDGE_ALERT_COOLDOWN_SEC = int(
    os.environ.get("BRIDGE_ALERT_COOLDOWN_SEC") or str(60 * 60)
)
# Railway 502 / "Application failed to respond" gibi geçici hatalarda
# peş peşe N kez olmadan e-posta gitmesin; olunca da daha uzun cooldown.
BRIDGE_ALERT_TRANSIENT_STREAK = int(
    os.environ.get("BRIDGE_ALERT_TRANSIENT_STREAK") or "3"
)
BRIDGE_ALERT_TRANSIENT_COOLDOWN_SEC = int(
    os.environ.get("BRIDGE_ALERT_TRANSIENT_COOLDOWN_SEC") or str(6 * 60 * 60)
)
# needs_login: ilk uyarı hemen, sonrası 6 saat sessiz; success → resolved mail
BRIDGE_LOGIN_ALERT_COOLDOWN_SEC = int(
    os.environ.get("BRIDGE_LOGIN_ALERT_COOLDOWN_SEC") or str(6 * 60 * 60)
)
VIRGUL_INGEST_TRIES = int(os.environ.get("VIRGUL_INGEST_TRIES") or "4")
VIRGUL_INGEST_TIMEOUT_SEC = int(os.environ.get("VIRGUL_INGEST_TIMEOUT_SEC") or "180")

_TRANSIENT_FAIL_MARKERS = (
    "application failed to respond",
    "gateway timeout",
    "gateway time-out",
    "bad gateway",
    "service unavailable",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "connection refused",
    "remote end closed",
    "server disconnected",
    "cloudflare",
    "error code: 502",
    "error code: 503",
    "error code: 504",
)

# Notification/news aynı admin oturumunu paylaşır; Virgül ayrı — uzun Excel
# sync'i Elle yenile'yi 409 ile kilitlemesin.
_nt_lock = threading.Lock()
_virgul_lock = threading.Lock()
# Play / ASC / Firebase / GSC / Policy — aynı anda tek headed Firefox
# (login beklerken 3-4 pencere açılmasın; auto job manuel Update'i ezmesin)
_browser_scrape_lock = threading.Lock()
_play_lock = _browser_scrape_lock
_asc_lock = _browser_scrape_lock
_firebase_lock = _browser_scrape_lock
_gsc_links_lock = _browser_scrape_lock
_policy_lock = _browser_scrape_lock
_gsc_cwv_lock = _browser_scrape_lock
_empower_intel_lock = _browser_scrape_lock
_empower_intel_sinemalar_lock = _browser_scrape_lock
_noads_lock = threading.Lock()
_moderation_lock = threading.Lock()
_pagespeed_lock = threading.Lock()
_seo_audit_lock = threading.Lock()
_market_lock = threading.Lock()
_noads_open_lock = threading.Lock()
_revenue_targets_lock = threading.Lock()
_last_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_news_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_virgul_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_play_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_asc_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_firebase_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_firebase_progress: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "step": 0,
    "total_steps": 0,
    "platform": "",
    "sub_label": "",
    "message": "",
}
_last_gsc_links_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_revenue_targets_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_policy_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_noads_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_moderation_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_pagespeed_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_empower_intel_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_empower_intel_sinemalar_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_seo_audit_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_gsc_cwv_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_market_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_nt_auto_at = 0.0
_last_news_auto_at = 0.0
_last_notification_night_seal_slot = ""
_last_virgul_auto_slot = ""
_last_play_auto_slot = ""
_last_asc_auto_slot = ""
_last_firebase_auto_slot = ""
_last_gsc_links_auto_slot = ""
_last_revenue_targets_auto_slot = ""
_last_policy_auto_slot = ""
_last_noads_auto_slot = ""
_last_moderation_auto_slot = ""
_last_pagespeed_auto_slot = ""
_last_seo_audit_auto_slot = ""
_last_gsc_cwv_auto_slot = ""
_last_market_auto_slot = ""
_last_empower_intel_auto_slot = ""
_last_empower_intel_sinemalar_auto_slot = ""
_news_progress: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "page": 0,
    "total_pages": int(os.environ.get("NEWS_PAGES_ESTIMATE") or "264"),
    "rows": 0,
    "message": "",
}
_nt_progress: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "step": 0,
    "total_steps": 0,
    "rows": 0,
    "message": "",
}
_gsc_cwv_progress: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "site": "",
    "message": "",
    "step": 0,
    "total_steps": 8,
    "started_at": 0.0,
    "finished_at": 0.0,
}
# Tüm işler (elle / otomatik / page-tarama) — live.NOW + log
_JOB_PROGRESS: dict[str, dict[str, Any]] = {}
_JOB_EVENT_LOG: list[str] = []
_JOB_EVENT_LOCK = threading.Lock()
_auto_cycle = 0
_last_fail_email_at: dict[str, float] = {}
_fail_streak: dict[str, int] = {}
_login_alert_open: dict[str, bool] = {}
_last_login_email_at: dict[str, float] = {}
# Eksik env/kimlik ayarı: tek uyarı, düzelene kadar sessiz; retry de yapılmaz
_config_alert_open: dict[str, bool] = {}
_config_fail: dict[str, bool] = {}
# kind → {attempt: 1..MAX, next_at: float, name: str}
_job_retries: dict[str, dict[str, Any]] = {}
# Tarayıcı scrape kuyruğu — aynı anda / çok kısa aralıkta ikinci scrape başlamasın
_last_browser_scrape_at = 0.0
_scrape_deferred_jobs: dict[str, dict[str, Any]] = {}
_BROWSER_SCRAPE_KINDS = frozenset(
    {
        "play",
        "asc",
        "firebase",
        "gsc_links",
        "revenue_targets",
        "admanager_policy",
        "pagespeed",
        "sinemalar_noads",
        "sinemalar_moderation",
        "seo_audit",
        "gsc_cwv",
        "market",
        "empower_intel",
        "empower_intel_sinemalar",
    }
)


def _is_browser_scrape_kind(kind: str) -> bool:
    return kind in _BROWSER_SCRAPE_KINDS


def browser_scrape_slot_defs() -> tuple[tuple[str, tuple[int, ...], int], ...]:
    """Test / health: tarayıcı slot tanımları (ad, saatler, dakika)."""
    return (
        ("play", PLAY_SLOT_HOURS, PLAY_SLOT_MINUTE),
        ("asc", ASC_SLOT_HOURS, ASC_SLOT_MINUTE),
        ("virgul", VIRGUL_SLOT_HOURS, VIRGUL_SLOT_MINUTE),
        ("market", MARKET_SLOT_HOURS, MARKET_SLOT_MINUTE),
        ("gsc_links", GSC_LINKS_SLOT_HOURS, GSC_SLOT_MINUTE),
        ("policy", TWICE_DAILY_HOURS, POLICY_SLOT_MINUTE),
        ("pagespeed", TWICE_DAILY_HOURS, SPEED_SLOT_MINUTE),
        ("noads", TWICE_DAILY_HOURS, NOADS_SLOT_MINUTE),
        ("revenue_targets", REVENUE_TARGETS_SLOT_HOURS, REVENUE_TARGETS_SLOT_MINUTE),
        ("seo_audit", SEO_AUDIT_SLOT_HOURS, SEO_AUDIT_SLOT_MINUTE),
        ("gsc_cwv", GSC_CWV_SLOT_HOURS, GSC_CWV_SLOT_MINUTE),
        ("firebase", FIREBASE_SLOT_HOURS, FIREBASE_SLOT_MINUTE),
    )


def _can_start_browser_scrape() -> bool:
    if _last_browser_scrape_at <= 0:
        return True
    return (time.time() - _last_browser_scrape_at) >= max(60, BRIDGE_SCRAPE_MIN_GAP_SEC)


def _defer_browser_scrape(
    kind: str,
    *,
    name: str,
    lock: threading.Lock,
    runner,
    on_done: Any | None = None,
) -> None:
    if kind in _scrape_deferred_jobs:
        return
    _scrape_deferred_jobs[kind] = {
        "name": name,
        "lock": lock,
        "runner": runner,
        "on_done": on_done,
    }
    print(
        f"Auto {name} ertelendi — son scrape'ten sonra "
        f"{max(0, int(BRIDGE_SCRAPE_MIN_GAP_SEC - (time.time() - _last_browser_scrape_at)))}s bekleniyor",
        flush=True,
    )


def _flush_deferred_browser_scrapes() -> None:
    global _last_browser_scrape_at
    if not _scrape_deferred_jobs or not _can_start_browser_scrape():
        return
    kind = next(iter(_scrape_deferred_jobs))
    meta = _scrape_deferred_jobs.pop(kind)
    _last_browser_scrape_at = time.time()
    result = _run_locked_job(
        name=str(meta["name"]),
        lock=meta["lock"],
        runner=meta["runner"],
        kind=kind,
        notify=False,
    )
    if result is None:
        _scrape_deferred_jobs[kind] = meta
        return
    _last_browser_scrape_at = time.time()
    on_done = meta.get("on_done")
    if callable(on_done):
        on_done(result)


def _run_browser_scrape_job(
    *,
    kind: str,
    name: str,
    lock: threading.Lock,
    runner,
    on_done: Any | None = None,
    notify: bool = False,
) -> dict[str, Any] | None:
    global _last_browser_scrape_at
    if _is_browser_scrape_kind(kind) and not _can_start_browser_scrape():
        _defer_browser_scrape(kind, name=name, lock=lock, runner=runner, on_done=on_done)
        return None
    if _is_browser_scrape_kind(kind):
        _last_browser_scrape_at = time.time()
    result = _run_locked_job(
        name=name,
        lock=lock,
        runner=runner,
        kind=kind,
        notify=notify,
    )
    if result is not None and _is_browser_scrape_kind(kind):
        _last_browser_scrape_at = time.time()
        if callable(on_done):
            on_done(result)
    return result


def _moderation_slot_due() -> tuple[bool, str, str]:
    """Mühürlü: yalnız dün slotu; aksi halde 03:04 dün / 14:17+20:05 bugün."""
    now = _now_tr()
    cur = now.hour * 60 + now.minute
    window = max(5, SLOT_WINDOW_MIN)
    for hour, minute, which in MODERATION_SLOTS:
        start = int(hour) * 60 + int(minute)
        if start <= cur < start + window:
            slot = f"{now.strftime('%Y-%m-%d')}-{int(hour):02d}{int(minute):02d}"
            if _last_moderation_auto_slot == slot:
                return False, slot, which
            return True, slot, which
    return False, "", ""


def _failure_message(result: dict[str, Any] | None = None, exc: BaseException | None = None) -> str:
    if exc is not None:
        return str(exc) or exc.__class__.__name__
    if isinstance(result, dict):
        return str(result.get("message") or result.get("detail") or result)
    return "bilinmeyen hata"


def _is_transient_failure(
    msg: str,
    *,
    http_status: int | None = None,
    exc: BaseException | None = None,
) -> bool:
    if http_status in (408, 425, 429, 502, 503, 504):
        return True
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    m = (msg or "").lower()
    return any(marker in m for marker in _TRANSIENT_FAIL_MARKERS)


_CONFIG_FAIL_RE = re.compile(r"\b[A-Z][A-Z0-9_]{4,}\b[^\n]{0,60}?\bgerekli\b")


def _is_config_failure(msg: str) -> bool:
    """Eksik env/kimlik ayarı (ör. 'VIRGUL_EMAIL / VIRGUL_PASSWORD gerekli').

    Kendi kendine düzelmez: yeniden deneme ve tekrar mail anlamsız.
    """
    return bool(_CONFIG_FAIL_RE.search(msg or ""))


def _mark_failure_class(
    kind: str,
    result: dict[str, Any] | None = None,
    exc: BaseException | None = None,
) -> bool:
    cfg = _is_config_failure(_failure_message(result, exc))
    _config_fail[kind] = cfg
    return cfg


def _bridge_kind_label(kind: str) -> str:
    labels = {
        "notification": "Notification (/notification)",
        "news": "Doviz News (/doviz-news)",
        "virgul": "Virgül Ad (/ad-virgul)",
        "play": "Play Console",
        "asc": "App Store Connect",
        "firebase": "Firebase Console",
        "gsc_links": "GSC Links",
        "gsc_cwv": "GSC CWV",
        "admanager_policy": "Ad Manager Policy",
        "sinemalar_moderation": "Sinemalar Moderasyon",
        "noads": "Sinemalar noAds",
    }
    return labels.get(kind, kind)


def _result_needs_login(
    kind: str,
    result: dict[str, Any] | None,
    msg: str,
) -> bool:
    if isinstance(result, dict) and result.get("needs_login"):
        return True
    low = (msg or "").lower()
    loginish = any(
        tok in low
        for tok in ("needs_login", "login gerekli", "oturum", "giriş", "giris", "sign in")
    )
    if not loginish:
        return False
    # Hem bridge kind'ları hem page-tarama iş id'leri kabul edilir (cwv/gsc_cwv gibi
    # iki isim de dolaşımda; biri eksik kalırsa oturum hatası diğer Mac'e devredilmez).
    return kind in (
        "play",
        "play_vitals",
        "asc",
        "notification",
        "firebase",
        "gsc_links",
        "links",
        "gsc_cwv",
        "cwv",
        "admanager_policy",
        "policy",
        "noads",
        "sinemalar_noads",
        "sinemalar_moderation",
        "moderation",
    )


def _set_news_progress(**kwargs: Any) -> None:
    _news_progress.update(kwargs)
    _news_progress["ts"] = time.time()


def _set_nt_progress(**kwargs: Any) -> None:
    _nt_progress.update(kwargs)
    _nt_progress["ts"] = time.time()


def _send_bridge_alert_email(*, kind: str, subject: str, body_text: str) -> bool:
    """Auto-refresh hatasında cemevecen@nokta.com (veya BRIDGE_ALERT_EMAIL)."""
    to_addr = (
        os.environ.get("BRIDGE_ALERT_EMAIL")
        or os.environ.get("OPERATIONS_MAIL_TO")
        or os.environ.get("MAIL_TO")
        or BRIDGE_ALERT_TO
        or "cemevecen@nokta.com"
    ).strip()
    host = (os.environ.get("SMTP_HOST") or "").strip()
    user = (os.environ.get("SMTP_USER") or "").strip()
    password = (os.environ.get("SMTP_PASSWORD") or "").strip()
    mail_from = (os.environ.get("MAIL_FROM") or user or to_addr).strip()
    if not to_addr or not host or not user or not password:
        print(
            f"Bridge alert e-posta atlandı (SMTP/alıcı eksik) kind={kind}",
            flush=True,
        )
        return False
    try:
        port = int(os.environ.get("SMTP_PORT") or "587")
    except ValueError:
        port = 587
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_addr
    msg.set_content(body_text)
    try:
        with smtplib.SMTP(host, port, timeout=45) as smtp:
            smtp.ehlo()
            try:
                smtp.starttls()
                smtp.ehlo()
            except smtplib.SMTPException:
                pass
            smtp.login(user, password)
            smtp.send_message(msg)
        print(f"Bridge alert e-posta gönderildi → {to_addr} ({kind})", flush=True)
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"Bridge alert e-posta hatası ({kind}): {exc}", flush=True)
        return False


def _notify_login_session_alert(kind: str, msg: str) -> None:
    """needs_login: ilk mail hemen, açık alert varken 6 saat cooldown."""
    now = time.time()
    cooldown = max(300, BRIDGE_LOGIN_ALERT_COOLDOWN_SEC)
    open_alert = bool(_login_alert_open.get(kind))
    last = float(_last_login_email_at.get(kind) or 0)
    if open_alert and last and (now - last) < cooldown:
        left = int(cooldown - (now - last))
        print(f"Bridge login alert cooldown ({kind}) · ~{left}s", flush=True)
        return
    label = _bridge_kind_label(kind)
    subject = f"[SEO Agent Bridge] {label} oturumu düştü"
    body = (
        f"Kaynak: Mac VPN bridge (127.0.0.1:{BRIDGE_PORT})\n"
        f"Tür: {label} ({kind})\n"
        f"Zaman (UTC): {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z\n"
        f"Durum: needs_login\n"
        f"Hata: {msg}\n\n"
        f"Firefox profilinde tekrar giriş gerekir "
        f"(fx-google / fx-asc / ilgili fx-*); scrape schedule aynı.\n"
        f"Kontrol: curl -s http://127.0.0.1:{BRIDGE_PORT}/health | python3 -m json.tool\n"
    )
    if _send_bridge_alert_email(kind=f"login:{kind}", subject=subject, body_text=body):
        _login_alert_open[kind] = True
        _last_login_email_at[kind] = now
    else:
        # SMTP yokken bile open say — resolved path tutarlı olsun; tekrar deneme cooldown'suz spam olmasın
        _login_alert_open[kind] = True
        _last_login_email_at[kind] = now


def _notify_login_session_resolved(kind: str) -> None:
    if not _login_alert_open.get(kind):
        return
    label = _bridge_kind_label(kind)
    subject = f"[SEO Agent Bridge] {label} oturumu düzeldi"
    body = (
        f"Kaynak: Mac VPN bridge (127.0.0.1:{BRIDGE_PORT})\n"
        f"Tür: {label} ({kind})\n"
        f"Zaman (UTC): {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z\n"
        f"Durum: resolved — scrape tekrar başarılı.\n"
    )
    if _send_bridge_alert_email(kind=f"login-ok:{kind}", subject=subject, body_text=body):
        _login_alert_open[kind] = False
        print(f"Bridge login resolved mail ({kind})", flush=True)
    else:
        # SMTP yoksa bile bayrağı kapat; sonraki needs_login yine uyarsın
        _login_alert_open[kind] = False


def _notify_config_alert(kind: str, msg: str) -> None:
    """Eksik ayar: düzeltilene kadar tek mail (retry/cooldown spam'i yok)."""
    if _config_alert_open.get(kind):
        print(f"Bridge config uyarısı zaten açık ({kind}): {msg[:120]}", flush=True)
        return
    label = _bridge_kind_label(kind)
    subject = f"[SEO Agent Bridge] {label} yapılandırma eksik"
    body = (
        f"Kaynak: Mac VPN bridge (127.0.0.1:{BRIDGE_PORT})\n"
        f"Tür: {label} ({kind})\n"
        f"Zaman (UTC): {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z\n"
        f"Hata: {msg}\n\n"
        f"Bu iş, eksik ayar giderilene kadar duraklatıldı — yeniden deneme ve "
        f"tekrar uyarı yapılmayacak.\n"
        f"Değeri bridge'in .env dosyasına ekleyip daemon'ı yeniden başlatın.\n"
        f"Kontrol: curl -s http://127.0.0.1:{BRIDGE_PORT}/health | python3 -m json.tool\n"
    )
    _send_bridge_alert_email(kind=f"config:{kind}", subject=subject, body_text=body)
    _config_alert_open[kind] = True


def _note_auto_success(kind: str) -> None:
    _fail_streak[kind] = 0
    _config_fail.pop(kind, None)
    _config_alert_open.pop(kind, None)
    _notify_login_session_resolved(kind)


def _notify_auto_failure(
    kind: str,
    result: dict[str, Any] | None = None,
    *,
    exc: BaseException | None = None,
) -> None:
    """Başarısız auto sync → e-posta (kind başına cooldown / transient streak)."""
    msg = (_failure_message(result, exc) or "bilinmeyen hata")[:800]
    http_status = None
    if isinstance(result, dict):
        try:
            http_status = int(result.get("http_status") or 0) or None
        except (TypeError, ValueError):
            http_status = None
    if _is_config_failure(msg):
        _config_fail[kind] = True
        _notify_config_alert(kind, msg)
        return
    if _result_needs_login(kind, result, msg):
        _notify_login_session_alert(kind, msg)
        return
    transient = _is_transient_failure(msg, http_status=http_status, exc=exc)
    streak = int(_fail_streak.get(kind) or 0) + 1
    _fail_streak[kind] = streak
    if transient and streak < max(1, BRIDGE_ALERT_TRANSIENT_STREAK):
        print(
            f"Bridge alert bastırıldı ({kind} geçici hata {streak}/"
            f"{BRIDGE_ALERT_TRANSIENT_STREAK}): {msg[:160]}",
            flush=True,
        )
        return

    now = time.time()
    last = float(_last_fail_email_at.get(kind) or 0)
    cooldown = max(300, BRIDGE_ALERT_COOLDOWN_SEC)
    if transient:
        cooldown = max(cooldown, BRIDGE_ALERT_TRANSIENT_COOLDOWN_SEC)
    if last and (now - last) < cooldown:
        left = int(cooldown - (now - last))
        print(f"Bridge alert cooldown ({kind}) · ~{left}s", flush=True)
        return
    label = _bridge_kind_label(kind)
    subject = f"[SEO Agent Bridge] {label} auto-refresh başarısız"
    suffix = ""
    if kind == "news":
        suffix = "-news"
    elif kind == "virgul":
        suffix = "-virgul"
    body = (
        f"Kaynak: Mac VPN bridge (127.0.0.1:{BRIDGE_PORT})\n"
        f"Tür: {label} ({kind})\n"
        f"Zaman (UTC): {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z\n"
        f"Hata: {msg}\n"
        f"Ardışık hata: {streak}"
        + (" · geçici/Railway" if transient else "")
        + "\n\n"
        f"Kontrol: curl -s http://127.0.0.1:{BRIDGE_PORT}/health | python3 -m json.tool\n"
        f"Elle: POST http://127.0.0.1:{BRIDGE_PORT}/sync{suffix}\n"
    )
    if _send_bridge_alert_email(kind=kind, subject=subject, body_text=body):
        _last_fail_email_at[kind] = now


def _news_pages_estimate() -> int:
    last = int(_last_news_result.get("last_page") or 0)
    env = int(os.environ.get("NEWS_PAGES_ESTIMATE") or "264")
    return max(last, env, 1)


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _notification_ingest_url() -> str:
    return (
        os.environ.get("NOTIFICATION_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/notification-analytics/ingest"
    ).strip()


def _news_ingest_url() -> str:
    return (
        os.environ.get("DOVIZ_NEWS_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/doviz-news/ingest"
    ).strip()


def _virgul_ingest_url() -> str:
    return (
        os.environ.get("VIRGUL_AD_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/virgul-analytics/ingest"
    ).strip()


def _play_console_ingest_url() -> str:
    return (
        os.environ.get("PLAY_CONSOLE_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/play-console/ingest"
    ).strip()


def _asc_console_ingest_url() -> str:
    return (
        os.environ.get("ASC_CONSOLE_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/asc-console/ingest"
    ).strip()


def _firebase_console_ingest_url() -> str:
    return (
        os.environ.get("FIREBASE_CONSOLE_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/firebase-console/ingest"
    ).strip()


def _require_creds() -> dict[str, Any] | None:
    if not _ingest_token():
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    if not (os.environ.get("DOVIZ_ADMIN_EMAIL") and os.environ.get("DOVIZ_ADMIN_PASSWORD")):
        return {"ok": False, "message": "DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD gerekli"}
    return None


def _require_virgul_creds() -> dict[str, Any] | None:
    if not _ingest_token():
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    if not (os.environ.get("VIRGUL_EMAIL") and os.environ.get("VIRGUL_PASSWORD")):
        return {"ok": False, "message": "VIRGUL_EMAIL / VIRGUL_PASSWORD gerekli"}
    return None


def _post_virgul_ingest_files(files: list[dict[str, Any]]) -> tuple[int, dict[str, Any]]:
    """Tek/az dosyalı ingest; Railway 502 için retry."""
    url = _virgul_ingest_url()
    token = _ingest_token()
    payload = json.dumps({"files": files, "replace": False, "source": "virgul_bridge"})
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    last_status = 0
    last_body: dict[str, Any] = {}
    tries = max(1, VIRGUL_INGEST_TRIES)
    for attempt in range(1, tries + 1):
        try:
            resp = requests.post(
                url,
                headers=headers,
                data=payload,
                timeout=max(60, VIRGUL_INGEST_TIMEOUT_SEC),
            )
            last_status = int(resp.status_code)
            try:
                body = resp.json() if resp.content else {}
            except Exception:
                body = {"raw": (resp.text or "")[:500], "message": (resp.text or "")[:300]}
            if not isinstance(body, dict):
                body = {"message": str(body)}
            last_body = body
            ok = (
                last_status < 400
                and body.get("synced") is not False
                and body.get("ok") is not False
            )
            if ok:
                return last_status, body
            msg = str(body.get("message") or body.get("detail") or resp.text or "")
            if not _is_transient_failure(msg, http_status=last_status) or attempt >= tries:
                return last_status, body
            print(
                f"Virgul ingest geçici hata HTTP {last_status} "
                f"(deneme {attempt}/{tries}): {msg[:160]}",
                flush=True,
            )
        except requests.RequestException as exc:
            last_status = 0
            last_body = {"message": str(exc), "ok": False, "synced": False}
            if attempt >= tries or not _is_transient_failure(str(exc), exc=exc):
                return last_status, last_body
            print(
                f"Virgul ingest ağ hatası (deneme {attempt}/{tries}): {exc}",
                flush=True,
            )
        time.sleep(min(60, 2**attempt))
    return last_status, last_body


def run_virgul_bridge_once(on_progress=None) -> dict[str, Any]:
    """Virgül 6 sid Excel/CSV → Railway /ad-virgul ingest (dal dal, retry)."""
    global _last_virgul_result
    _load_dotenv()
    err = _require_virgul_creds()
    if err:
        _last_virgul_result = err
        return err

    import base64

    from backend.services.virgul_ad_client import (
        date_range_yesterday_today,
        fetch_all_sites_exports,
    )
    from backend.services.virgul_ad_config import VIRGUL_AD_SOURCES

    def _cb(info: dict[str, Any] | None = None) -> None:
        if not callable(on_progress):
            return
        try:
            on_progress(info if isinstance(info, dict) else {})
        except Exception:
            pass

    n_sites = len(VIRGUL_AD_SOURCES)
    total_steps = max(1, n_sites * 2)  # export + ingest
    v_start, v_end = date_range_yesterday_today()
    print(
        f"Virgül reklam export çekiliyor (6 sid) · {v_start.isoformat()} → {v_end.isoformat()} "
        "(dün+bugün; mühürlü geçmiş yok)…",
        flush=True,
    )
    _cb(
        {
            "phase": "export",
            "sub_label": "Virgül Excel export",
            "step": 0,
            "total_steps": total_steps,
            "message": f"Virgül Excel {v_start}→{v_end}…",
        }
    )

    def _export_progress(info: dict[str, Any]) -> None:
        step = int(info.get("step") or 0)
        # export steps occupy 1..n_sites
        _cb(
            {
                "phase": str(info.get("phase") or "export"),
                "platform": str(info.get("platform") or ""),
                "sub_label": str(info.get("sub_label") or "")[:160],
                "step": min(n_sites, max(0, step)),
                "total_steps": total_steps,
                "message": str(info.get("message") or "Virgül export")[:200],
            }
        )

    fetched = fetch_all_sites_exports(
        start=v_start, end=v_end, on_progress=_export_progress
    )
    files: list[dict[str, Any]] = []
    for item in fetched.get("items") or []:
        if not item.get("ok") or not item.get("data"):
            print(
                f"  skip {item.get('label') or item.get('sid')}: {item.get('message')}",
                flush=True,
            )
            continue
        files.append(
            {
                "stream_key": item.get("stream_key"),
                "filename": item.get("filename"),
                "data_b64": base64.b64encode(item["data"]).decode("ascii"),
            }
        )
    if not files:
        out = {
            "ok": False,
            "message": fetched.get("message")
            or "Virgül: hiç export alınamadı (API/Excel endpoint Network ile netleştirilmeli)",
            "streams": fetched.get("items") or [],
        }
        _last_virgul_result = out
        return out

    # Tek dev JSON Railway edge timeout'una çarpmasın diye her dal ayrı ingest.
    stream_results: list[dict[str, Any]] = []
    ok_n = 0
    total_parsed = 0
    worst_status = 200
    last_msg = ""
    for idx, f in enumerate(files, start=1):
        sk = f.get("stream_key") or "?"
        print(f"Virgul ingest → {sk}…", flush=True)
        _cb(
            {
                "phase": "ingest",
                "platform": str(sk),
                "sub_label": f"ingest {sk}",
                "step": n_sites + idx,
                "total_steps": total_steps,
                "message": f"Virgül ingest {idx}/{len(files)} · {sk}",
            }
        )
        status, body = _post_virgul_ingest_files([f])
        if status and status > worst_status:
            worst_status = status
        msg = ""
        if isinstance(body, dict):
            msg = str(body.get("message") or body.get("detail") or "")
            total_parsed += int(body.get("total_parsed") or 0)
        last_msg = msg or last_msg
        ok = status < 400 and (
            not isinstance(body, dict)
            or (body.get("synced") is not False and body.get("ok") is not False)
        )
        if ok:
            ok_n += 1
        else:
            print(f"  fail {sk} HTTP {status}: {msg[:200]}", flush=True)
        stream_results.append(
            {
                "stream_key": sk,
                "ok": bool(ok),
                "http_status": status,
                "message": msg or ("OK" if ok else "Ingest başarısız"),
            }
        )
        print(
            f"Virgul ingest {sk} HTTP {status} · {msg or ('OK' if ok else 'fail')}",
            flush=True,
        )

    ok = ok_n > 0
    out = {
        "ok": bool(ok),
        "kind": "virgul",
        "http_status": worst_status if ok else (worst_status or 502),
        "files": len(files),
        "ok_count": ok_n,
        "fail_count": len(files) - ok_n,
        "total_parsed": total_parsed,
        "message": (
            f"Virgül ingest · {ok_n}/{len(files)} dal · {total_parsed} satır"
            if ok
            else (last_msg or "Ingest başarısız")
        ),
        "streams": stream_results,
        "body": {"streams": stream_results, "ok_count": ok_n},
    }
    _last_virgul_result = out
    return out


def run_play_bridge_once() -> dict[str, Any]:
    """Play Console dashboard + reviews scrape → Railway ingest.

    Bridge sürecinde (importlib) çalışır — KEEP_OPEN warm session Firebase/GSC ile
    aynı fx-google penceresini paylaşır. Subprocess yetim Firefox üretmez.

    Varsayılan: süre sınırı yok (PLAY_BRIDGE_TIMEOUT_SEC=0). Mühürlü gövde yalnız
    dün+bugün; ANR/Crash önce checkpoint ingest edilir.
    """
    return _run_play_scrape_inprocess(
        kind="play",
        vitals_only=False,
        timeout_env="PLAY_BRIDGE_TIMEOUT_SEC",
        timeout_default=0,
        label="Play Console",
    )


def run_play_vitals_bridge_once() -> dict[str, Any]:
    """Sadece Android Vitals (crashes + metrics overview) → merge_vitals ingest."""
    return _run_play_scrape_inprocess(
        kind="play_vitals",
        vitals_only=True,
        timeout_env="PLAY_VITALS_BRIDGE_TIMEOUT_SEC",
        timeout_default=0,
        label="Play Vitals",
    )


def _run_play_scrape_inprocess(
    *,
    kind: str,
    vitals_only: bool,
    timeout_env: str,
    timeout_default: int,
    label: str,
) -> dict[str, Any]:
    global _last_play_result
    if not _ingest_token():
        err = {"ok": False, "kind": kind, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_play_result = err
        return err

    import importlib.util
    import threading

    path = ROOT / "scripts" / "play_console_scrape.py"
    if not path.is_file():
        err = {"ok": False, "kind": kind, "message": "Play tarama betiği yok"}
        _last_play_result = err
        return err

    env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    try:
        timeout_sec = int(os.environ.get(timeout_env) or str(timeout_default))
    except ValueError:
        timeout_sec = int(timeout_default)
    # 0 / negatif = süre sınırı yok (kill etme — yarım scrape bırakma)
    unlimited = timeout_sec <= 0
    os.environ.setdefault("PLAY_CONSOLE_INGEST_URL", _play_console_ingest_url())

    print(
        f"{label} scrape başlıyor… (in-process, KEEP_OPEN warm"
        + (", süre sınırı yok)" if unlimited else f", timeout={timeout_sec}s)"),
        flush=True,
    )

    box: dict[str, Any] = {}

    def _worker() -> None:
        try:
            spec = importlib.util.spec_from_file_location("play_console_scrape_bridge", path)
            if spec is None or spec.loader is None:
                box["err"] = "Play tarama betiği yüklenemedi"
                return
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            if hasattr(mod, "INGEST_URL"):
                mod.INGEST_URL = _play_console_ingest_url()
            if vitals_only:
                result = mod.scrape_vitals_only(headed=headed)
            else:
                result = mod.scrape_play_console(headed=headed)
            if not result.get("ok") and (
                result.get("needs_login")
                or "login" in str(result.get("message") or "").lower()
            ):
                box["out"] = {
                    "ok": False,
                    "kind": kind,
                    "needs_login": True,
                    "message": result.get("message") or "Play login gerekli (--login)",
                }
                return
            try:
                ing = mod.ingest_scrape_result(result)
            except Exception as exc:  # noqa: BLE001
                box["out"] = {
                    "ok": False,
                    "kind": kind,
                    "message": f"Ingest hata: {exc}",
                }
                return
            msg = (
                result.get("message")
                or ing.get("message")
                or (f"{label} sync OK" if ing.get("ok") else f"{label} ingest fail")
            )
            box["out"] = {
                "ok": bool(ing.get("ok")) and bool(result.get("ok")),
                "kind": kind,
                "message": str(msg)[:400],
                "needs_login": False,
                "ingest": {
                    k: ing.get(k)
                    for k in ("ok", "updated_at", "message")
                    if k in ing or k == "ok"
                },
            }
        except Exception as exc:  # noqa: BLE001
            box["err"] = str(exc)[:400]

    if unlimited:
        _worker()
    else:
        th = threading.Thread(target=_worker, name=f"play-scrape-{kind}", daemon=True)
        th.start()
        th.join(timeout=max(120, timeout_sec))
        if th.is_alive():
            out = {
                "ok": False,
                "kind": kind,
                "message": f"{label} tarama zaman aşımı ({timeout_sec}s)",
            }
            _last_play_result = out
            try:
                from backend.services.scrape_browser import (
                    google_profile_dir,
                    release_profile_browsers,
                    warm_session_forget,
                )

                warm_session_forget("play")
                release_profile_browsers(
                    google_profile_dir(), force=False, reason="play_timeout"
                )
            except Exception:
                pass
            return out

    if box.get("err"):
        out = {"ok": False, "kind": kind, "message": f"{label}: {box['err']}"}
        _last_play_result = out
        return out

    out = box.get("out") or {"ok": False, "kind": kind, "message": f"{label} boş sonuç"}
    _last_play_result = out
    print(f"{label} sync · {out.get('message')}", flush=True)
    return out


def _run_play_scrape_subprocess(
    *,
    args: list[str],
    kind: str,
    timeout_env: str,
    timeout_default: int,
    label: str,
) -> dict[str, Any]:
    """Geriye uyumluluk / elle CLI — bridge artık in-process kullanır."""
    global _last_play_result
    if not _ingest_token():
        err = {"ok": False, "kind": kind, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_play_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "play_console_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": kind, "message": "Play tarama betiği yok"}
        _last_play_result = err
        return err

    print(f"{label} scrape başlıyor…", flush=True)
    env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    cmd = [sys.executable, str(script), *args]
    if not headed:
        cmd.append("--headless")
    env = os.environ.copy()
    env.setdefault("PLAY_CONSOLE_INGEST_URL", _play_console_ingest_url())
    # Subprocess KEEP_OPEN yetim Firefox bırakmasın — bridge in-process tercih edilir
    env.setdefault("PLAY_CONSOLE_KEEP_OPEN", "0")
    try:
        timeout_sec = int(os.environ.get(timeout_env) or str(timeout_default))
    except ValueError:
        timeout_sec = int(timeout_default)
    run_timeout = None if timeout_sec <= 0 else max(120, timeout_sec)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=run_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        out = {
            "ok": False,
            "kind": kind,
            "message": f"{label} tarama zaman aşımı ({timeout_sec}s)",
        }
        _last_play_result = out
        print(str(exc)[:300], flush=True)
        try:
            from backend.services.scrape_browser import (
                google_profile_dir,
                release_profile_browsers,
            )

            release_profile_browsers(
                google_profile_dir(), force=True, reason="play_subprocess_timeout"
            )
        except Exception:
            pass
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": kind, "message": f"{label} subprocess: {exc}"}
        _last_play_result = out
        return out

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if combined:
        for line in combined.splitlines()[-40:]:
            print(line, flush=True)

    if proc.returncode == 2 or "needs_login" in combined.lower():
        out = {
            "ok": False,
            "kind": kind,
            "needs_login": True,
            "message": "Play login gerekli (--login)",
        }
        _last_play_result = out
        return out

    if proc.returncode == 0:
        msg = f"{label} sync OK"
        for line in reversed((proc.stdout or "").splitlines()):
            s = line.strip()
            if (
                s.startswith("Play tarama")
                or s.startswith("Play scrape")
                or s.startswith("Vitals")
                or s.startswith("INGEST")
            ):
                msg = s[:400]
                break
        out = {"ok": True, "kind": kind, "message": msg, "needs_login": False}
        _last_play_result = out
        print(f"{label} sync · {out['message']}", flush=True)
        return out

    err_msg = f"{label} tarama çıkış {proc.returncode}"
    for line in reversed(combined.splitlines()):
        s = line.strip()
        if not s:
            continue
        if "Sync API inside the asyncio loop" in s:
            err_msg = (
                "Playwright Sync API / asyncio conflict "
                "(önceki tarayıcı kapanmamış olabilir)"
            )
            break
        if "SingletonLock" in s or "ProcessSingleton" in s:
            err_msg = "Play profile SingletonLock — başka tarayıcı oturumu açık"
            break
        if "Error:" in s or "error:" in s or s.startswith("playwright."):
            err_msg = s[:400]
            break
        if s:
            err_msg = s[:400]
            break
    out = {"ok": False, "kind": kind, "message": err_msg, "needs_login": False}
    _last_play_result = out
    print(f"{label} sync · {out['message']}", flush=True)
    return out


def run_gsc_links_bridge_once() -> dict[str, Any]:
    """GSC Links scrape (döviz + sinemalar) → Railway ingest."""
    global _last_gsc_links_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_gsc_links_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "gsc_links_scrape.py"
        spec = importlib.util.spec_from_file_location("gsc_links_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "GSC bağlantı tarama betiği yüklenemedi"}
            _last_gsc_links_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_gsc_links = mod.scrape_gsc_links
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"GSC bağlantı tarama import: {exc}"}
        _last_gsc_links_result = err
        return err

    print("GSC Links scrape başlıyor…", flush=True)
    env_hl = (os.environ.get("GSC_LINKS_HEADLESS") or os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_gsc_links(headed=headed)
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "gsc_links",
            "needs_login": True,
            "message": result.get("message") or "GSC login gerekli (--login)",
        }
        _last_gsc_links_result = out
        return out
    try:
        os.environ.setdefault(
            "GSC_LINKS_INGEST_URL",
            (
                os.environ.get("GSC_LINKS_INGEST_URL")
                or "https://projectcontrol.up.railway.app/api/gsc-links/ingest"
            ),
        )
        ing = ingest_scrape_result(result)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "gsc_links", "message": f"Ingest hata: {exc}"}
        _last_gsc_links_result = out
        return out
    out = {
        "ok": bool(ing.get("ok")) and bool(result.get("ok")),
        "kind": "gsc_links",
        "http_status": ing.get("http_status"),
        "snapshot_count": len(result.get("snapshots") or []),
        "message": result.get("message") or ing.get("message") or "GSC Links sync",
        "needs_login": False,
        "ingest": {
            k: ing.get(k)
            for k in ("ok", "message", "imported", "errors")
            if k in ing or k == "ok"
        },
    }
    _last_gsc_links_result = out
    print(f"GSC Links sync · {out['message']}", flush=True)
    return out


def run_revenue_targets_bridge_once() -> dict[str, Any]:
    """Ad-virgul hedef sheet scrape → Railway ingest.

    • Her çalıştırmada: dokümandaki son (içinde bulunulan) ay
    • Ayın 1–2'sinde ayrıca: bir önceki biten ay (final rakam)
    Eski aylar yeniden taranmaz.
    """
    global _last_revenue_targets_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_revenue_targets_result = err
        return err

    try:
        from backend.services.revenue_targets_sheet import (
            current_month_period_key,
            is_closed_month_sync_day,
            previous_month_period_key,
        )
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "kind": "revenue_targets", "message": f"import: {exc}"[:300]}
        _last_revenue_targets_result = err
        return err

    script = ROOT / "scripts" / "revenue_targets_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "revenue_targets", "message": "revenue_targets_scrape.py yok"}
        _last_revenue_targets_result = err
        return err

    env = os.environ.copy()
    env.setdefault(
        "REVENUE_TARGETS_INGEST_URL",
        "https://projectcontrol.up.railway.app/api/virgul-analytics/revenue-targets/ingest",
    )
    headed = (os.environ.get("REVENUE_TARGETS_HEADLESS") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
    )

    def _run(extra_flags: list[str], label: str) -> dict[str, Any]:
        cmd = [sys.executable, str(script), "--sync", "--ingest", *extra_flags]
        if not headed:
            cmd.append("--headless")
        print(f"Revenue targets scrape · {label}…", flush=True)
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=600,
            )
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "label": label, "message": str(exc)[:300]}
        out_lines = (proc.stdout or "").strip().splitlines()
        err_lines = (proc.stderr or "").strip().splitlines()
        # Scrape başarılıyken bile stderr'de DB cache uyarısı olabiliyor; özet
        # satırı stdout'taki "OK · parsed=…" olmalı, yoksa son anlamlı satır.
        summary = ""
        if proc.returncode == 0:
            for line in reversed(out_lines):
                s = line.strip()
                if s.startswith("OK ·"):
                    summary = s
                    break
        if not summary:
            tail = out_lines + err_lines
            summary = next((s for s in (ln.strip() for ln in reversed(tail)) if s), "")
        return {
            "ok": proc.returncode == 0,
            "label": label,
            "message": summary[:400] if summary else ("OK" if proc.returncode == 0 else f"exit={proc.returncode}"),
            "returncode": proc.returncode,
        }

    steps: list[dict[str, Any]] = []
    # Son ay (güncel sekme) — otomatik + manuel her seferinde
    steps.append(_run(["--current-only"], f"current {current_month_period_key()}"))
    if is_closed_month_sync_day():
        steps.append(
            _run(["--closed-month"], f"closed {previous_month_period_key()}")
        )

    ok = all(bool(s.get("ok")) for s in steps)
    msg = " | ".join(f"{s.get('label')}: {s.get('message')}" for s in steps)
    out = {
        "ok": ok,
        "kind": "revenue_targets",
        "steps": steps,
        "message": msg[:500],
    }
    _last_revenue_targets_result = out
    print(f"Revenue targets sync · {out['message']}", flush=True)
    return out


def run_admanager_policy_bridge_once() -> dict[str, Any]:
    """Ad Manager Policy Center scrape → Railway /api/policy/ingest."""
    global _last_policy_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_policy_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "admanager_policy_scrape.py"
        spec = importlib.util.spec_from_file_location("admanager_policy_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "Policy tarama betiği yüklenemedi"}
            _last_policy_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_fn = mod.scrape_admanager_policy
        ingest_fn = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"Policy tarama import: {exc}"}
        _last_policy_result = err
        return err

    print("Ad Manager Policy scrape başlıyor…", flush=True)
    env_hl = (os.environ.get("ADMANAGER_POLICY_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_fn(headed=headed)
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "admanager_policy",
            "needs_login": True,
            "message": result.get("message") or "Ad Manager login gerekli (--login)",
        }
        _last_policy_result = out
        return out
    try:
        os.environ.setdefault(
            "ADMANAGER_POLICY_INGEST_URL",
            "https://projectcontrol.up.railway.app/api/policy/ingest",
        )
        if hasattr(mod, "INGEST_URL"):
            mod.INGEST_URL = os.environ["ADMANAGER_POLICY_INGEST_URL"]
        ing = ingest_fn(result)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "admanager_policy", "message": f"Ingest hata: {exc}"}
        _last_policy_result = out
        return out
    out = {
        "ok": bool(ing.get("ok")) and bool(result.get("ok")),
        "kind": "admanager_policy",
        "http_status": ing.get("http_status"),
        "row_count": len(result.get("rows") or []),
        "message": result.get("message") or ing.get("message") or "Policy sync",
        "needs_login": False,
        "ingest": {
            k: ing.get(k)
            for k in ("ok", "message", "imported", "new_count", "updated_count")
            if k in ing or k == "ok"
        },
    }
    _last_policy_result = out
    print(f"Ad Manager Policy sync · {out['message']}", flush=True)
    return out


def _load_sinemalar_noads_mod():
    import importlib.util

    path = ROOT / "scripts" / "sinemalar_noads_scrape.py"
    spec = importlib.util.spec_from_file_location("sinemalar_noads_scrape", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("sinemalar_noads_scrape.py yüklenemedi")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_open_noads_prefill(url: str) -> dict[str, Any]:
    """Policy «Ekle»: headed noAds + textarea prefill (arka planda tutulur)."""
    target = (url or "").strip()
    if not target:
        return {"ok": False, "kind": "noads_open", "message": "url gerekli"}
    try:
        mod = _load_sinemalar_noads_mod()
        open_fn = mod.open_noads_prefill
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "kind": "noads_open", "message": f"import: {exc}"}

    def _job() -> None:
        try:
            out = open_fn(target)
            print(f"noAds open · {out.get('message')}", flush=True)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            print(f"noAds open hata: {exc}", flush=True)
        finally:
            try:
                _noads_open_lock.release()
            except RuntimeError:
                pass

    if not _noads_open_lock.acquire(blocking=False):
        return {
            "ok": False,
            "kind": "noads_open",
            "message": "noAds penceresi zaten açık — önce onu kapatın veya bekleyin",
        }
    threading.Thread(target=_job, name="noads-prefill", daemon=True).start()
    return {
        "ok": True,
        "kind": "noads_open",
        "url": target,
        "message": "Tarayıcı açılıyor — textarea doldurulacak; yeşil Ekle'ye basın",
    }


# Oturum açma hedefleri — her Mac'in kendi Firefox profili (~/.seo-agent/fx-*)
LOGIN_TARGETS: dict[str, dict[str, Any]] = {
    "google": {
        "profile": "google",
        "url": "https://accounts.google.com/ServiceLogin?continue="
        "https%3A%2F%2Fsearch.google.com%2Fsearch-console",
        "verify": True,
        "covers": "Play Console · Firebase · Search Console (CWV/Backlinks) · Ad Manager Policy",
    },
    "asc": {
        "profile": "asc",
        "url": "https://appstoreconnect.apple.com/login",
        "verify": False,
        "covers": "App Store Connect (iOS)",
    },
    "sinemalar": {
        "profile": "sinemalar",
        "url": "https://www.sinemalar.com/management/noAds",
        "verify": False,
        "covers": "Sinemalar Moderation · noAds",
    },
    "empower": {
        "profile": "empower",
        "url": (os.environ.get("EMPOWER_INTEL_LOGIN_URL") or "").strip(),
        "verify": False,
        "covers": "Empower Intelligence",
    },
}


def run_open_login(target: str) -> tuple[int, dict[str, Any]]:
    """Bu Mac'te ilgili profille Firefox aç, kullanıcı girişi yapsın (en az 15 dk bekler)."""
    spec = LOGIN_TARGETS.get(target)
    if not spec:
        return 400, {
            "ok": False,
            "message": f"target gerekli: {', '.join(sorted(LOGIN_TARGETS))}",
            "targets": {k: v["covers"] for k, v in LOGIN_TARGETS.items()},
        }
    url = str(spec.get("url") or "")
    if not url:
        return 400, {
            "ok": False,
            "message": f"{target} için giriş adresi tanımlı değil (EMPOWER_INTEL_LOGIN_URL)",
        }
    if not _browser_scrape_lock.acquire(blocking=False):
        return 409, {
            "ok": False,
            "message": "Tarayıcı şu an bir taramada meşgul — bitince tekrar deneyin",
        }

    def _job() -> None:
        try:
            from backend.services.scrape_browser import (
                asc_profile_dir,
                empower_profile_dir,
                google_profile_dir,
                launch_system_firefox_login,
                sinemalar_profile_dir,
            )

            dirs = {
                "google": google_profile_dir,
                "asc": asc_profile_dir,
                "sinemalar": sinemalar_profile_dir,
                "empower": empower_profile_dir,
            }
            profile = dirs[str(spec["profile"])]()
            print(f"Oturum açma penceresi: {target} · profil={profile}", flush=True)
            out = launch_system_firefox_login(
                profile,
                url,
                success_hint=(
                    f"{target} girişini yap → hedef sayfa açılsın → Firefox penceresini KAPAT"
                ),
                verify_session=bool(spec.get("verify")),
            )
            print(f"Oturum açma sonucu ({target}): {out}", flush=True)
            _job_event(f"Oturum açma · {target} · {'ok' if out.get('ok') else 'başarısız'}")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            print(f"Oturum açma hatası ({target}): {exc}", flush=True)
        finally:
            try:
                _browser_scrape_lock.release()
            except Exception:
                pass
            # Oturum durumu değişti — kabiliyet raporu tazelensin
            global _readiness_cache
            _readiness_cache = (0.0, {})

    threading.Thread(target=_job, name=f"login-{target}", daemon=True).start()
    return 200, {
        "ok": True,
        "target": target,
        "covers": spec["covers"],
        "message": "Firefox açılıyor — girişi yapıp pencereyi kapatın (en fazla 15 dk)",
    }


def run_sinemalar_noads_bridge_once() -> dict[str, Any]:
    """Sinemalar management/noAds → Railway /api/policy/noads/ingest."""
    global _last_noads_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_noads_result = err
        return err
    try:
        mod = _load_sinemalar_noads_mod()
        scrape_fn = mod.scrape_sinemalar_noads
        ingest_fn = mod.ingest_noads_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"noAds tarama import: {exc}"}
        _last_noads_result = err
        return err

    print("Sinemalar noAds tarama başlıyor…", flush=True)
    env_hl = (os.environ.get("SINEMALAR_NOADS_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_fn(headed=headed)
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "sinemalar_noads",
            "needs_login": True,
            "message": result.get("message") or "Sinemalar admin login gerekli (--login)",
        }
        _last_noads_result = out
        return out
    if not result.get("ok"):
        out = {
            "ok": False,
            "kind": "sinemalar_noads",
            "message": result.get("message") or "noAds tarama başarısız",
            "needs_login": False,
        }
        _last_noads_result = out
        return out
    try:
        ing = ingest_fn(result)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "sinemalar_noads", "message": f"Ingest hata: {exc}"}
        _last_noads_result = out
        return out
    out = {
        "ok": bool(ing.get("ok")),
        "kind": "sinemalar_noads",
        "entry_count": len(result.get("entries") or []),
        "matched": ing.get("matched"),
        "missing": ing.get("missing"),
        "email_sent": ing.get("email_sent"),
        "message": ing.get("message") or result.get("message") or "noAds sync",
        "needs_login": False,
        "ingest": ing,
    }
    _last_noads_result = out
    print(f"Sinemalar noAds sync · {out['message']}", flush=True)
    return out


def _load_sinemalar_moderation_mod():
    import importlib.util

    path = ROOT / "scripts" / "sinemalar_moderation_scrape.py"
    spec = importlib.util.spec_from_file_location("sinemalar_moderation_scrape", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("sinemalar_moderation_scrape.py yüklenemedi")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_sinemalar_moderation_bridge_once(*, incremental_which: str = "yesterday") -> dict[str, Any]:
    """Sinemalar getModerationSummary → Railway /api/sinemalar-moderation/ingest."""
    global _last_moderation_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_moderation_result = err
        return err
    try:
        mod = _load_sinemalar_moderation_mod()
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"moderation import: {exc}"}
        _last_moderation_result = err
        return err

    env_hl = (os.environ.get("SINEMALAR_MODERATION_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    meta = mod.fetch_remote_meta()
    sealed = False
    try:
        from backend.services.history_seal import force_full_history, is_pipeline_sealed

        sealed = is_pipeline_sealed("sinemalar_moderation") and not force_full_history(
            "sinemalar_moderation"
        )
    except Exception:
        sealed = False

    # Mühürlü gövde tamam: chunk backfill atlanır; yalnız dün dilimi
    if sealed or meta.get("backfill_complete"):
        which = incremental_which if incremental_which in ("yesterday", "today", "both") else "yesterday"
        if sealed:
            which = "yesterday"
        print(f"Sinemalar moderasyon detail incremental ({which})…", flush=True)
        result = mod.run_incremental_detail(which, headed=headed, ingest=True)
        mode = "detail_incremental"
    else:
        print("Sinemalar moderasyon backfill chunk…", flush=True)
        result = mod.run_backfill_chunk(headed=headed, ingest=True)
        mode = "backfill"

    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "sinemalar_moderation",
            "needs_login": True,
            "message": result.get("message") or "Sinemalar admin login gerekli",
        }
        _last_moderation_result = out
        return out
    if not result.get("ok"):
        out = {
            "ok": False,
            "kind": "sinemalar_moderation",
            "message": result.get("message") or "moderation scrape başarısız",
            "mode": mode,
        }
        _last_moderation_result = out
        return out

    ing = (result.get("ingest") or {}).get("ingest") or result.get("ingest") or {}
    out = {
        "ok": True,
        "kind": "sinemalar_moderation",
        "mode": mode,
        "skipped": bool(result.get("skipped")),
        "days": len(result.get("days") or []),
        "upserted": ing.get("upserted"),
        "backfill_complete": bool(result.get("backfill_complete") or meta.get("backfill_complete")),
        "message": result.get("message") or ing.get("message") or "moderation sync",
        "ingest": ing,
    }
    _last_moderation_result = out
    print(f"Sinemalar moderasyon sync · {out['message']}", flush=True)
    return out


def run_asc_bridge_once() -> dict[str, Any]:
    """App Store Connect analytics scrape → Railway ingest."""
    global _last_asc_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_asc_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "asc_console_scrape.py"
        spec = importlib.util.spec_from_file_location("asc_console_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "ASC tarama betiği yüklenemedi"}
            _last_asc_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_asc_console = mod.scrape_asc_console
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"ASC tarama import: {exc}"}
        _last_asc_result = err
        return err

    print("ASC Console scrape başlıyor…", flush=True)
    env_hl = (os.environ.get("ASC_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_asc_console(headed=headed)
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "asc",
            "needs_login": True,
            "message": result.get("message") or "ASC login gerekli (--login)",
        }
        _last_asc_result = out
        return out
    try:
        os.environ.setdefault("ASC_CONSOLE_INGEST_URL", _asc_console_ingest_url())
        # scrape modülü INGEST_URL’i import anında okur — override
        if hasattr(mod, "INGEST_URL"):
            mod.INGEST_URL = _asc_console_ingest_url()
        ing = ingest_scrape_result(result)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "asc", "message": f"Ingest hata: {exc}"}
        _last_asc_result = out
        return out
    fact_n = len((result.get("panels") or {}).get("explorer_facts") or [])
    out = {
        "ok": bool(ing.get("ok")) and bool(result.get("ok")),
        "kind": "asc",
        "http_status": ing.get("http_status"),
        "fact_count": fact_n,
        "message": result.get("message") or ing.get("message") or "ASC sync",
        "needs_login": False,
        "ingest": {
            k: ing.get(k)
            for k in ("ok", "updated_at", "fact_count", "message")
            if k in ing or k == "ok"
        },
    }
    _last_asc_result = out
    print(f"ASC sync · {out['message']}", flush=True)
    return out


def _set_firebase_progress(**kwargs: Any) -> None:
    _firebase_progress.update(kwargs)
    _firebase_progress["ts"] = time.time()


def _firebase_platforms_for_page(page: str) -> tuple[str, ...] | None:
    """Update page kaynağına göre Firebase platform filtresi.

    ios → yalnız iOS projesi; android → yalnız Android; diğer/auto → ikisi (None).
    """
    p = (page or "").strip().lower()
    if p == "ios":
        return ("ios",)
    if p == "android":
        return ("android",)
    return None


def run_firebase_bridge_once(on_progress=None, platforms=None) -> dict[str, Any]:
    """Firebase Console Crashlytics scrape → Railway ingest."""
    global _last_firebase_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_firebase_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "firebase_console_scrape.py"
        spec = importlib.util.spec_from_file_location("firebase_console_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "Firebase tarama betiği yüklenemedi"}
            _last_firebase_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_firebase_console = mod.scrape_firebase_console
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"Firebase tarama import: {exc}"}
        _last_firebase_result = err
        return err

    plat_arg = None
    if platforms:
        plat_arg = [str(x).strip().lower() for x in platforms if str(x).strip()]
        plat_arg = [x for x in plat_arg if x in ("android", "ios")] or None
    total_hint = 12 * len(plat_arg) if plat_arg else 24

    def _cb(info: dict[str, Any]) -> None:
        info = info if isinstance(info, dict) else {}
        _set_firebase_progress(
            running=True,
            phase=str(info.get("phase") or "scrape"),
            step=int(info.get("step") or 0),
            total_steps=int(info.get("total_steps") or total_hint),
            platform=str(info.get("platform") or ""),
            sub_label=str(info.get("sub_label") or ""),
            message=str(info.get("message") or "")[:200],
        )
        if callable(on_progress):
            try:
                on_progress(info)
            except Exception:
                pass

    scope = ",".join(plat_arg) if plat_arg else "android+ios"
    print(f"Firebase Console scrape başlıyor… ({scope})", flush=True)
    _set_firebase_progress(
        running=True,
        phase="starting",
        step=0,
        total_steps=total_hint,
        platform=(plat_arg[0] if plat_arg else ""),
        sub_label="",
        message=f"Firebase Console scrape starting ({scope})",
    )
    env_hl = (os.environ.get("FIREBASE_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    try:
        result = scrape_firebase_console(headed=headed, on_progress=_cb, platforms=plat_arg)
        if not result.get("sync_ok") and "login" in str(result.get("sync_message") or "").lower():
            out = {
                "ok": False,
                "kind": "firebase",
                "needs_login": True,
                "message": result.get("sync_message") or "Firebase login gerekli (--login)",
            }
            _last_firebase_result = out
            _set_firebase_progress(running=False, phase="error", message=out["message"])
            return out
        try:
            _cb(
                {
                    "phase": "ingest",
                    "sub_label": "Railway ingest",
                    "step": max(1, total_hint - 1),
                    "total_steps": total_hint,
                    "message": "Ingesting Firebase scrape",
                }
            )
            os.environ.setdefault("FIREBASE_CONSOLE_INGEST_URL", _firebase_console_ingest_url())
            if hasattr(mod, "INGEST_URL"):
                mod.INGEST_URL = _firebase_console_ingest_url()
            ing = ingest_scrape_result(result)
        except Exception as exc:  # noqa: BLE001
            out = {"ok": False, "kind": "firebase", "message": f"Ingest hata: {exc}"}
            _last_firebase_result = out
            _set_firebase_progress(running=False, phase="error", message=out["message"])
            return out
        plats = ((result.get("panels") or {}).get("platforms") or {})
        out = {
            "ok": bool(ing.get("ok")) and bool(result.get("sync_ok")),
            "kind": "firebase",
            "platforms": list(plats.keys()) if isinstance(plats, dict) else [],
            "metric_count": len(result.get("metrics") or []),
            "message": result.get("sync_message") or ing.get("message") or "Firebase sync",
            "needs_login": False,
            "ingest": ing,
        }
        _last_firebase_result = out
        _set_firebase_progress(
            running=False,
            phase="done" if out["ok"] else "error",
            step=total_hint,
            total_steps=total_hint,
            message=out["message"],
        )
        print(f"Firebase sync · {out['message']}", flush=True)
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "firebase", "message": f"Firebase scrape: {exc}"}
        _last_firebase_result = out
        _set_firebase_progress(running=False, phase="error", message=out["message"])
        print(f"Firebase sync · {out['message']}", flush=True)
        return out
    finally:
        if _firebase_progress.get("running"):
            _set_firebase_progress(
                running=False,
                phase="error",
                message=_firebase_progress.get("message") or "Firebase ended without clear finish",
            )


def run_empower_intel_bridge_once(*, mode: str = "yesterday") -> dict[str, Any]:
    """Empower Intelligence Yesterday (veya backfill) → Railway ingest."""
    global _last_empower_intel_result
    if not _ingest_token():
        err = {"ok": False, "kind": "empower_intel", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_empower_intel_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "empower_intelligence_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "empower_intel", "message": "Empower Intel tarama betiği yok"}
        _last_empower_intel_result = err
        return err

    mode_l = (mode or "yesterday").strip().lower()
    print(f"Empower Intel scrape başlıyor… ({mode_l})", flush=True)
    cmd = [sys.executable, str(script), "--ingest"]
    if mode_l == "backfill":
        try:
            from backend.services.history_seal import history_seal, history_start, scheduled_fetch_window

            win = scheduled_fetch_window("empower", force_full=True)
            start_s = win["start"].isoformat()
            end_s = win["end"].isoformat()
        except Exception:
            from backend.services.history_seal import history_seal, history_start

            start_s = history_start().isoformat()
            end_s = history_seal().isoformat()
        cmd.extend(["--backfill", "--start", start_s, "--end", end_s])
    else:
        cmd.append("--yesterday")
    env = os.environ.copy()
    env.setdefault(
        "EMPOWER_INTEL_INGEST_URL",
        (
            os.environ.get("EMPOWER_INTEL_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/empower-intel/ingest"
        ).strip(),
    )
    timeout_sec = int(os.environ.get("EMPOWER_INTEL_BRIDGE_TIMEOUT_SEC") or "1800")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        out = {
            "ok": False,
            "kind": "empower_intel",
            "message": f"Empower Intel timeout ({timeout_sec}s)",
        }
        _last_empower_intel_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "empower_intel", "message": f"Empower Intel subprocess: {exc}"}
        _last_empower_intel_result = out
        return out

    tail = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()[-1200:]
    if proc.returncode == 0:
        out = {"ok": True, "kind": "empower_intel", "mode": mode_l, "message": "Empower Intel sync OK"}
    else:
        out = {
            "ok": False,
            "kind": "empower_intel",
            "mode": mode_l,
            "message": tail or f"exit {proc.returncode}",
        }
    _last_empower_intel_result = out
    print(f"Empower Intel · {out['message']}", flush=True)
    return out


def run_empower_intel_sinemalar_bridge_once(*, mode: str = "yesterday") -> dict[str, Any]:
    """Sinemalar Empower (web+mweb) Yesterday/backfill → Railway ingest (project=sinemalar)."""
    global _last_empower_intel_sinemalar_result
    if not _ingest_token():
        err = {
            "ok": False,
            "kind": "empower_intel_sinemalar",
            "message": "NOTIFICATION_INGEST_TOKEN gerekli",
        }
        _last_empower_intel_sinemalar_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "empower_intelligence_scrape.py"
    if not script.is_file():
        err = {
            "ok": False,
            "kind": "empower_intel_sinemalar",
            "message": "Empower Intel tarama betiği yok",
        }
        _last_empower_intel_sinemalar_result = err
        return err

    mode_l = (mode or "yesterday").strip().lower()
    print(f"Empower Intel Sinemalar scrape başlıyor… ({mode_l})", flush=True)
    cmd = [
        sys.executable,
        str(script),
        "--project",
        "sinemalar",
        "--platform",
        "web",
        "--platform",
        "mweb",
        "--ingest",
    ]
    if mode_l == "backfill":
        try:
            from backend.services.history_seal import scheduled_fetch_window

            win = scheduled_fetch_window("empower_sinemalar", force_full=True)
            start_s = win["start"].isoformat()
            end_s = win["end"].isoformat()
        except Exception:
            from backend.services.history_seal import history_seal, history_start

            start_s = history_start().isoformat()
            end_s = history_seal().isoformat()
        cmd.extend(["--backfill", "--start", start_s, "--end", end_s])
    else:
        cmd.append("--yesterday")
    env = os.environ.copy()
    env["EMPOWER_INTEL_PROJECT"] = "sinemalar"
    env.setdefault(
        "EMPOWER_INTEL_INGEST_URL",
        (
            os.environ.get("EMPOWER_INTEL_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/empower-intel/ingest"
        ).strip(),
    )
    timeout_sec = int(os.environ.get("EMPOWER_INTEL_BRIDGE_TIMEOUT_SEC") or "1800")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        out = {
            "ok": False,
            "kind": "empower_intel_sinemalar",
            "message": f"Empower Intel Sinemalar timeout ({timeout_sec}s)",
        }
        _last_empower_intel_sinemalar_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {
            "ok": False,
            "kind": "empower_intel_sinemalar",
            "message": f"Empower Intel Sinemalar subprocess: {exc}",
        }
        _last_empower_intel_sinemalar_result = out
        return out

    tail = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()[-1200:]
    if proc.returncode == 0:
        out = {
            "ok": True,
            "kind": "empower_intel_sinemalar",
            "mode": mode_l,
            "message": "Empower Intel Sinemalar sync OK",
        }
    else:
        out = {
            "ok": False,
            "kind": "empower_intel_sinemalar",
            "mode": mode_l,
            "message": tail or f"exit {proc.returncode}",
        }
    _last_empower_intel_sinemalar_result = out
    print(f"Empower Intel Sinemalar · {out['message']}", flush=True)
    return out


def run_pagespeed_bridge_once() -> dict[str, Any]:
    """pagespeed.web.dev scrape (doviz + sinemalar) → Railway ingest."""
    global _last_pagespeed_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_pagespeed_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "pagespeed_web_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "pagespeed", "message": "PageSpeed tarama betiği yok"}
        _last_pagespeed_result = err
        return err

    print("PageSpeed web scrape başlıyor…", flush=True)
    cmd = [sys.executable, str(script), "--sync", "--ingest"]
    env = os.environ.copy()
    env.setdefault(
        "PAGESPEED_WEB_INGEST_URL",
        (
            os.environ.get("PAGESPEED_WEB_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/pagespeed-web/ingest"
        ).strip(),
    )
    timeout_sec = int(os.environ.get("PAGESPEED_BRIDGE_TIMEOUT_SEC") or "900")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(120, timeout_sec),
        )
    except subprocess.TimeoutExpired:
        out = {
            "ok": False,
            "kind": "pagespeed",
            "message": f"PageSpeed tarama zaman aşımı ({timeout_sec}s)",
        }
        _last_pagespeed_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "pagespeed", "message": f"PageSpeed subprocess: {exc}"}
        _last_pagespeed_result = out
        return out

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if combined:
        for line in combined.splitlines()[-30:]:
            print(line, flush=True)

    if proc.returncode == 0:
        out = {"ok": True, "kind": "pagespeed", "message": "PageSpeed sync OK"}
    else:
        tail = (combined[-300:] if combined else f"exit {proc.returncode}")[:300]
        out = {"ok": False, "kind": "pagespeed", "message": tail}
    _last_pagespeed_result = out
    print(f"PageSpeed sync · {out['message']}", flush=True)
    return out


def run_market_tarama_bridge_once() -> dict[str, Any]:
    """doviz.com piyasa tablo taraması (01.01.2025+) → Railway ingest."""
    global _last_market_result
    if not _ingest_token():
        err = {"ok": False, "kind": "market", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_market_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "doviz_market_tarama.py"
    if not script.is_file():
        err = {"ok": False, "kind": "market", "message": "Piyasa tarama betiği yok"}
        _last_market_result = err
        return err

    print("Piyasa tarama başlıyor…", flush=True)
    cmd = [sys.executable, str(script), "--ingest"]
    env = os.environ.copy()
    env.setdefault(
        "MARKET_TARAMA_INGEST_URL",
        (
            os.environ.get("MARKET_TARAMA_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/market-quotes/ingest"
        ).strip(),
    )
    timeout_sec = int(os.environ.get("MARKET_TARAMA_BRIDGE_TIMEOUT_SEC") or "900")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(120, timeout_sec),
        )
    except subprocess.TimeoutExpired:
        out = {
            "ok": False,
            "kind": "market",
            "message": f"Piyasa tarama zaman aşımı ({timeout_sec}s)",
        }
        _last_market_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "market", "message": f"Piyasa tarama subprocess: {exc}"}
        _last_market_result = out
        return out

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if combined:
        for line in combined.splitlines()[-40:]:
            print(line, flush=True)

    if proc.returncode == 0:
        out = {"ok": True, "kind": "market", "message": "Piyasa tarama OK"}
    else:
        tail = (combined[-300:] if combined else f"exit {proc.returncode}")[:300]
        out = {"ok": False, "kind": "market", "message": tail}
    _last_market_result = out
    print(f"Piyasa tarama · {out['message']}", flush=True)
    return out


def run_seo_audit_bridge_once(site_id: int | None = None) -> dict[str, Any]:
    """GA4 top URL SEO meta scrape → Railway ingest (Playwright)."""
    global _last_seo_audit_result
    if not _ingest_token():
        err = {"ok": False, "kind": "seo_audit", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_seo_audit_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "seo_audit_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "seo_audit", "message": "SEO denetim tarama betiği yok"}
        _last_seo_audit_result = err
        return err

    print(
        f"SEO audit scrape başlıyor… site_id={site_id or 'all'}",
        flush=True,
    )
    cmd = [sys.executable, str(script), "--sync", "--ingest"]
    if site_id:
        cmd += ["--site-id", str(int(site_id))]
    env = os.environ.copy()
    env.setdefault(
        "SEO_AUDIT_API_BASE",
        (
            os.environ.get("SEO_AUDIT_API_BASE")
            or "https://projectcontrol.up.railway.app"
        ).strip(),
    )
    # 500 URL × 2 site — uzun sürebilir
    timeout_sec = int(os.environ.get("SEO_AUDIT_BRIDGE_TIMEOUT_SEC") or "5400")
    try:
        # stdout/stderr bridge loguna aksın (canlı progress)
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            timeout=max(300, timeout_sec),
        )
    except subprocess.TimeoutExpired:
        out = {
            "ok": False,
            "kind": "seo_audit",
            "message": f"SEO denetim tarama zaman aşımı ({timeout_sec}s)",
        }
        _last_seo_audit_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "seo_audit", "message": f"SEO audit subprocess: {exc}"}
        _last_seo_audit_result = out
        return out

    if proc.returncode == 0:
        out = {
            "ok": True,
            "kind": "seo_audit",
            "message": "SEO denetim tarama tamam",
            "site_id": site_id,
        }
    else:
        out = {
            "ok": False,
            "kind": "seo_audit",
            "message": f"SEO denetim tarama çıkış {proc.returncode}",
            "site_id": site_id,
        }
    _last_seo_audit_result = out
    print(f"SEO audit sync · {out['message']}", flush=True)
    return out


def _set_gsc_cwv_progress(**kwargs: Any) -> None:
    _gsc_cwv_progress.update(kwargs)


def _gsc_cwv_mode(explicit: str | None = None) -> str:
    """shots (varsayılan, manuel+otomatik) | full | charts.

    charts_only bayrağı artık shots'u ezmez. Eski derin scrape yalnız
    mode=full / GSC_CWV_MODE=full (veya bilinçli mode=charts) ile.
    """
    raw = (explicit or os.environ.get("GSC_CWV_MODE") or "shots").strip().lower()
    if raw in ("full", "scrape", "deep", "amp"):
        return "full"
    if raw in ("charts", "charts_only", "chart"):
        return "charts"
    return "shots"


def run_gsc_cwv_bridge_once(
    site_key: str | None = None,
    *,
    charts_only: bool = False,
    mode: str | None = None,
) -> dict[str, Any]:
    """GSC Core Web Vitals → Railway.

    Varsayılan (manuel Scan, Update page, zamanlanmış slot): screenshot yöntemi.
    Tam scrape (AMP + issue + nokta): GSC_CWV_MODE=full veya mode=full.
    Bridge sürecinde çalışır — fx-google KEEP_OPEN warm (Play/Firebase ile paylaşılır).
    """
    resolved = _gsc_cwv_mode(mode)
    if resolved == "shots":
        return run_gsc_cwv_shots_bridge_once(site_key=site_key)
    if resolved == "charts":
        charts_only = True
    else:
        charts_only = False

    global _last_gsc_cwv_result
    if not _ingest_token():
        err = {"ok": False, "kind": "gsc_cwv", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    import importlib.util
    import threading

    script = ROOT / "scripts" / "gsc_cwv_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "gsc_cwv", "message": "GSC CWV tarama betiği yok"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    print(
        f"GSC CWV scrape başlıyor… site={site_key or 'all'} (in-process, KEEP_OPEN warm)",
        flush=True,
    )
    _set_gsc_cwv_progress(
        running=True,
        phase="scrape",
        site=site_key or "all",
        step=0,
        total_steps=8,
        message=f"GSC CWV tarama · {site_key or 'all'}",
        started_at=time.time(),
        finished_at=0.0,
    )
    os.environ.setdefault(
        "GSC_CWV_INGEST_URL",
        (
            os.environ.get("GSC_CWV_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
        ).strip(),
    )
    timeout_sec = int(os.environ.get("GSC_CWV_BRIDGE_TIMEOUT_SEC") or "7200")
    box: dict[str, Any] = {}

    def _worker() -> None:
        try:
            spec = importlib.util.spec_from_file_location("gsc_cwv_scrape_bridge", script)
            if spec is None or spec.loader is None:
                box["err"] = "GSC CWV betiği yüklenemedi"
                return
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            box["result"] = mod.run_sync(
                site_filter=str(site_key or ""),
                ingest=True,
                headed=True,
                charts_only=charts_only,
            )
        except Exception as exc:  # noqa: BLE001
            box["err"] = str(exc)[:400]

    th = threading.Thread(target=_worker, name="gsc-cwv-scrape", daemon=True)
    th.start()
    th.join(timeout=max(300, timeout_sec))
    if th.is_alive():
        out = {
            "ok": False,
            "kind": "gsc_cwv",
            "message": f"GSC CWV tarama zaman aşımı ({timeout_sec}s)",
        }
        _last_gsc_cwv_result = out
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
        try:
            from backend.services.scrape_browser import (
                google_profile_dir,
                release_profile_browsers,
                warm_session_forget,
            )

            warm_session_forget("gsc-cwv")
            release_profile_browsers(
                google_profile_dir(), force=False, reason="gsc_cwv_timeout"
            )
        except Exception:
            pass
        return out

    if box.get("err"):
        out = {"ok": False, "kind": "gsc_cwv", "message": f"GSC CWV: {box['err']}", "site": site_key}
        _last_gsc_cwv_result = out
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
        return out

    result = box.get("result") or {}
    ok = bool(result.get("ok"))
    msg = str(result.get("message") or ("GSC CWV tarama tamam" if ok else "GSC CWV fail"))
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "gsc_cwv",
            "needs_login": True,
            "message": msg,
            "site": site_key,
        }
    else:
        out = {"ok": ok, "kind": "gsc_cwv", "message": msg, "site": site_key}
    _last_gsc_cwv_result = out
    _set_gsc_cwv_progress(
        running=False,
        phase="done" if out["ok"] else "error",
        step=8,
        total_steps=8,
        message=out["message"],
        finished_at=time.time(),
    )
    print(f"GSC CWV sync · {out['message']}", flush=True)
    return out


def run_gsc_cwv_shots_bridge_once(site_key: str | None = None) -> dict[str, Any]:
    """GSC CWV screenshot → KPI ingest + shots-ingest (in-process KEEP_OPEN warm)."""
    global _last_gsc_cwv_result
    if not _ingest_token():
        err = {"ok": False, "kind": "gsc_cwv", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    import importlib.util
    import threading

    script = ROOT / "scripts" / "gsc_cwv_shots.py"
    if not script.is_file():
        err = {"ok": False, "kind": "gsc_cwv", "message": "gsc_cwv_shots.py yok"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    print(
        f"GSC CWV shots başlıyor… site={site_key or 'all'} (in-process, KEEP_OPEN warm)",
        flush=True,
    )
    _set_gsc_cwv_progress(
        running=True,
        phase="shots",
        site=site_key or "all",
        step=0,
        total_steps=4,
        message=f"CWV screenshot · {site_key or 'all'}",
        started_at=time.time(),
        finished_at=0.0,
    )
    os.environ.setdefault(
        "GSC_CWV_SHOTS_INGEST_URL",
        (
            os.environ.get("GSC_CWV_SHOTS_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/gsc-cwv/shots-ingest"
        ).strip(),
    )
    os.environ.setdefault(
        "GSC_CWV_INGEST_URL",
        (
            os.environ.get("GSC_CWV_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
        ).strip(),
    )
    # 30 dk global tarayıcı slotunu kilitliyordu — oturum ölüyse çok daha erken düşsün.
    timeout_sec = int(os.environ.get("GSC_CWV_SHOTS_TIMEOUT_SEC") or "900")
    box: dict[str, Any] = {}

    def _worker() -> None:
        try:
            spec = importlib.util.spec_from_file_location("gsc_cwv_shots_bridge", script)
            if spec is None or spec.loader is None:
                box["err"] = "gsc_cwv_shots yüklenemedi"
                return
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            box["result"] = mod.run_shots(
                site_filter=str(site_key or ""),
                ingest=True,
                headed=True,
            )
        except Exception as exc:  # noqa: BLE001
            box["err"] = str(exc)[:400]

    th = threading.Thread(target=_worker, name="gsc-cwv-shots", daemon=True)
    th.start()
    th.join(timeout=max(300, timeout_sec))
    if th.is_alive():
        out = {
            "ok": False,
            "kind": "gsc_cwv",
            "mode": "shots",
            "message": f"CWV screenshot zaman aşımı ({timeout_sec}s)",
            "site": site_key,
        }
        _last_gsc_cwv_result = out
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
        try:
            from backend.services.scrape_browser import (
                google_profile_dir,
                release_profile_browsers,
                warm_session_forget,
            )

            warm_session_forget("gsc-cwv")
            release_profile_browsers(
                google_profile_dir(), force=False, reason="gsc_cwv_shots_timeout"
            )
        except Exception:
            pass
        return out

    if box.get("err"):
        out = {
            "ok": False,
            "kind": "gsc_cwv",
            "mode": "shots",
            "message": f"CWV shots: {box['err']}",
            "site": site_key,
        }
        _last_gsc_cwv_result = out
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
        return out

    result = box.get("result") or {}
    ok = bool(result.get("ok"))
    msg = str(result.get("message") or ("shots done" if ok else "shots fail"))
    out = {
        "ok": ok,
        "kind": "gsc_cwv",
        "mode": "shots",
        "message": msg,
        "site": site_key,
        "detail": result,
    }
    _set_gsc_cwv_progress(
        running=False,
        phase="done" if ok else "error",
        step=4,
        total_steps=4,
        message=out["message"],
        finished_at=time.time(),
    )
    _last_gsc_cwv_result = out
    print(f"GSC CWV shots · {out['message']}", flush=True)
    return out


def run_notification_bridge_once(*, mode: str = "live") -> dict[str, Any]:
    """Admin notification stats → Railway ingest.

    mode=live (08–20 / manuel): dün+bugün merge (panel taze; geçmiş silinmez).
    mode=seal_yesterday (gece dönümü): yalnız dünü kayda alır / mühürler.
    mode=full: HISTORY_START→dün replace (nadir / FORCE_FULL).
    """
    global _last_result
    _load_dotenv()
    err = _require_creds()
    if err:
        _last_result = err
        _set_nt_progress(running=False, phase="error", message=err.get("message") or "")
        return err

    from backend.services.doviz_notification_admin import fetch_notification_rows_from_admin

    mode_l = (mode or "live").strip().lower()
    if mode_l not in ("live", "seal_yesterday", "full"):
        mode_l = "live"

    _set_nt_progress(
        running=True,
        phase="login",
        step=0,
        total_steps=13,
        rows=0,
        message="Admin login…",
    )

    def _on_progress(info: dict[str, Any]) -> None:
        step = int(info.get("step") or 0)
        total = int(info.get("total_steps") or 0)
        rows = int(info.get("rows") or 0)
        phase = str(info.get("phase") or "fetch")
        msg = str(info.get("message") or "")
        _set_nt_progress(
            running=True,
            phase=phase,
            step=step,
            total_steps=total,
            rows=rows,
            message=msg or f"{step}/{total}",
        )

    print(f"Admin stats çekiliyor… ({mode_l})", flush=True)
    try:
        from backend.services.history_seal import (
            calendar_today,
            calendar_yesterday,
            force_full_history,
            history_start,
            is_pipeline_sealed,
            mark_pipeline_sealed,
        )

        fetch_start = fetch_end = None
        replace_ingest = False
        allow_today = True
        if mode_l == "full" or (
            mode_l == "live" and force_full_history("notification")
        ):
            fetch_start = history_start()
            fetch_end = calendar_yesterday()
            replace_ingest = True
            allow_today = False
            mode_l = "full"
        elif mode_l == "seal_yesterday":
            yday = calendar_yesterday()
            fetch_start = fetch_end = yday
            replace_ingest = False
            allow_today = False
            print(f"Notification gece mühür · {yday}", flush=True)
        elif is_pipeline_sealed("notification"):
            # Gündüz canlı: dün+bugün üst üste merge (tam geçmişi her 30 dk çekme)
            fetch_start = calendar_yesterday()
            fetch_end = calendar_today()
            replace_ingest = False
            allow_today = True
            print(
                f"Notification live · {fetch_start} → {fetch_end} (merge, allow_today)",
                flush=True,
            )
        else:
            replace_ingest = True
            allow_today = True

        fetched = fetch_notification_rows_from_admin(
            on_progress=_on_progress,
            start=fetch_start,
            end=fetch_end,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc) or "Notification tarama hatası"
        _set_nt_progress(running=False, phase="error", message=msg)
        out = {"ok": False, "message": msg, "parsed": 0, "mode": mode_l}
        _last_result = out
        return out

    rows = fetched.get("rows") or []
    print(f"Notification çekildi: {len(rows)} satır · {fetched.get('elapsed_sec')}s", flush=True)
    if not rows:
        out = {
            "ok": False,
            "message": "Notification: satır yok — gönderilmedi",
            "parsed": 0,
            "mode": mode_l,
        }
        _last_result = out
        _set_nt_progress(running=False, phase="error", message=out["message"], rows=0)
        return out

    _set_nt_progress(
        running=True,
        phase="ingest",
        step=1,
        total_steps=1,
        rows=len(rows),
        message=f"Railway'e yazılıyor · {len(rows)}/{len(rows)} kayıt",
    )

    url = _notification_ingest_url()
    token = _ingest_token()
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(
            {
                "rows": rows,
                "source": "doviz_admin_bridge",
                "replace": replace_ingest,
                "allow_today": allow_today,
                "mode": mode_l,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        timeout=180,
    )
    print(f"Notification ingest HTTP {resp.status_code}", flush=True)
    try:
        body = resp.json() if resp.content else {}
    except Exception:
        body = {"raw": (resp.text or "")[:500]}
    msg = body.get("message") if isinstance(body, dict) else str(body)
    print(msg or body, flush=True)
    ok = resp.status_code < 400 and (
        not isinstance(body, dict) or body.get("synced") is not False
    )
    if ok and mode_l == "seal_yesterday":
        try:
            from backend.services.history_seal import calendar_yesterday, mark_pipeline_sealed

            mark_pipeline_sealed(
                "notification",
                seal=calendar_yesterday(),
                note="night seal — previous calendar day archived",
            )
        except Exception:
            pass
    out = {
        "ok": bool(ok),
        "kind": "notification",
        "mode": mode_l,
        "http_status": resp.status_code,
        "parsed": len(rows),
        "elapsed_sec": fetched.get("elapsed_sec"),
        "message": msg or ("OK" if ok else "Ingest başarısız"),
        "source": "doviz_admin_bridge",
        "updated_at": body.get("updated_at") if isinstance(body, dict) else None,
        "row_count": body.get("row_count") if isinstance(body, dict) else None,
    }
    _last_result = out
    _set_nt_progress(
        running=False,
        phase="done" if ok else "error",
        step=1,
        total_steps=1,
        rows=len(rows),
        message=out["message"],
    )
    return out



def run_news_bridge_once(
    *,
    days: int | None = 7,
    full: bool = False,
) -> dict[str, Any]:
    """Admin aktif haberler → Railway ingest.

    Varsayılan: son `days` gün (Elle yenile + 30dk arka plan).
    full=True: id≥719818 tam geçmiş (seyrek / boş DB).
    """
    global _last_news_result
    _load_dotenv()
    err = _require_creds()
    if err:
        _last_news_result = err
        _set_news_progress(running=False, phase="error", message=err.get("message") or "")
        return err

    from datetime import date, timedelta

    from backend.services.doviz_news_admin import fetch_active_news_rows_from_admin

    use_full = bool(full) or (days is not None and int(days) <= 0)
    min_day = None
    sync_mode = "full"
    max_pages = 320
    if not use_full:
        try:
            from backend.services.history_seal import (
                force_full_history,
                is_pipeline_sealed,
            )

            # Gündüz live: son 7 gün merge (yesterday_only değil — 30 dk refresh)
            if is_pipeline_sealed("doviz_news") and not force_full_history("doviz_news"):
                d = max(1, int(days or 7))
                min_day = (date.today() - timedelta(days=d - 1)).isoformat()
                sync_mode = f"recent_{d}d"
                max_pages = 60
                estimate = 40
            else:
                d = max(1, int(days or 7))
                min_day = (date.today() - timedelta(days=d - 1)).isoformat()
                sync_mode = f"recent_{d}d"
                max_pages = 60
                estimate = 40
        except Exception:
            d = max(1, int(days or 7))
            min_day = (date.today() - timedelta(days=d - 1)).isoformat()
            sync_mode = f"recent_{d}d"
            max_pages = 60
            estimate = 40
    else:
        estimate = _news_pages_estimate()

    _set_news_progress(
        running=True,
        phase="scrape",
        page=0,
        total_pages=estimate,
        rows=0,
        message=("Tam tarama…" if use_full else f"{sync_mode} · {min_day}…"),
    )

    def _on_progress(info: dict[str, Any]) -> None:
        page = int(info.get("page") or 0)
        total = int(info.get("total_pages") or estimate)
        rows = int(info.get("rows") or 0)
        _set_news_progress(
            running=True,
            phase=str(info.get("phase") or "scrape"),
            page=page,
            total_pages=total,
            rows=rows,
            skipped_old=info.get("skipped_old"),
            hit_floor=bool(info.get("hit_floor")),
            message=f"{page}/{total} sayfa · {rows} kayıt",
        )
        if page and page % 25 == 0:
            print(f"News progress {page}/{total} · {rows} kayıt", flush=True)

    print(
        f"Admin haberler çekiliyor ({'full' if use_full else f'{sync_mode} / {min_day}…'})…",
        flush=True,
    )
    try:
        fetched = fetch_active_news_rows_from_admin(
            estimate_pages=estimate,
            max_pages=max_pages,
            min_day=min_day,
            on_progress=_on_progress,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc) or "Haber tarama hatası"
        print(f"News scrape failed: {msg}", flush=True)
        _report_news_sync_failure(msg, sync_mode=sync_mode)
        out = {"ok": False, "message": msg, "parsed": 0, "sync_mode": sync_mode}
        _last_news_result = out
        _set_news_progress(running=False, phase="error", message=msg)
        return out

    rows = fetched.get("rows") or []
    total_pages = int(fetched.get("last_page") or fetched.get("pages") or estimate)
    print(
        f"News çekildi: {len(rows)} satır · {fetched.get('pages')} sayfa · "
        f"{fetched.get('elapsed_sec')}s · mode={sync_mode}",
        flush=True,
    )
    if not rows:
        out = {"ok": False, "message": "News: satır yok — gönderilmedi", "parsed": 0}
        _last_news_result = out
        _set_news_progress(running=False, phase="error", message=out["message"])
        _report_news_sync_failure(out["message"], sync_mode=sync_mode)
        return out

    _set_news_progress(
        running=True,
        phase="ingest",
        page=total_pages,
        total_pages=total_pages,
        rows=len(rows),
        message=f"{total_pages}/{total_pages} sayfa · ingest…",
    )

    url = _news_ingest_url()
    token = _ingest_token()
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(
            {
                "rows": rows,
                "source": "doviz_admin_news_bridge",
                "source_url": fetched.get("source_url"),
                "merge": not use_full,
                "sync_mode": sync_mode,
            },
            ensure_ascii=False,
        ).encode("utf-8"),
        timeout=600,
    )
    print(f"News ingest HTTP {resp.status_code}", flush=True)
    try:
        body = resp.json() if resp.content else {}
    except Exception:
        body = {"raw": (resp.text or "")[:500]}
    msg = body.get("message") if isinstance(body, dict) else str(body)
    print(msg or body, flush=True)
    ok = resp.status_code < 400 and (
        not isinstance(body, dict) or body.get("synced") is not False
    )
    if not ok:
        _report_news_sync_failure(
            msg or f"Ingest HTTP {resp.status_code}",
            sync_mode=sync_mode,
        )
    out = {
        "ok": bool(ok),
        "kind": "news",
        "http_status": resp.status_code,
        "parsed": len(rows),
        "pages": fetched.get("pages"),
        "last_page": fetched.get("last_page"),
        "total_pages": total_pages,
        "elapsed_sec": fetched.get("elapsed_sec"),
        "message": msg or ("OK" if ok else "Ingest başarısız"),
        "source": "doviz_admin_news_bridge",
        "sync_mode": sync_mode,
        "min_day": min_day,
        "fetched_at": body.get("fetched_at") if isinstance(body, dict) else None,
        "background_synced_at": body.get("background_synced_at")
        if isinstance(body, dict)
        else None,
        "row_count": body.get("row_count") if isinstance(body, dict) else len(rows),
    }
    _last_news_result = out
    _set_news_progress(
        running=False,
        phase="done" if ok else "error",
        page=total_pages,
        total_pages=total_pages,
        rows=len(rows),
        message=out["message"],
    )
    return out


def _report_news_sync_failure(message: str, *, sync_mode: str) -> None:
    """Railway’e satır göndermeden sync hatasını yaz."""
    try:
        url = _news_ingest_url()
        token = _ingest_token()
        if not url or not token:
            return
        requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data=json.dumps(
                {
                    "rows": [],
                    "source": "doviz_admin_news_bridge",
                    "sync_ok": False,
                    "sync_mode": sync_mode,
                    "sync_message": (message or "")[:480],
                },
                ensure_ascii=False,
            ).encode("utf-8"),
            timeout=60,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"News failure report skipped: {exc}", flush=True)


def run_bridge_once() -> dict[str, Any]:
    """Geriye uyumluluk: notification sync."""
    return run_notification_bridge_once()


def run_all_once() -> dict[str, Any]:
    nt = run_notification_bridge_once()
    news = run_news_bridge_once()
    ok = bool(nt.get("ok")) and bool(news.get("ok"))
    return {
        "ok": ok,
        "kind": "all",
        "notification": nt,
        "news": news,
        "message": f"notification={nt.get('message')} · news={news.get('message')}",
    }


def _trigger_label(trigger: str) -> str:
    t = (trigger or "").strip().lower()
    if t in ("manual", "elle", "http", "sync"):
        return "Elle"
    if t in ("page-tarama", "panel", "update"):
        return "Panel"
    if t in ("schedule", "auto", "slot"):
        return "Otomatik"
    return (trigger or "İş").strip() or "İş"


def _job_event(line: str) -> None:
    """stdout + canlı log halkası (Settings log_hits) — satır başında TR saati."""
    msg = (line or "").strip()
    if not msg:
        return
    try:
        stamp = _now_tr().strftime("%H:%M:%S")
    except Exception:
        stamp = "—"
    # Log dosyasına da saat yazılsın (LaunchAgent stdout)
    print(f"{stamp} {msg}", flush=True)
    with _JOB_EVENT_LOCK:
        _JOB_EVENT_LOG.append(f"{stamp} {msg[:200]}")
        if len(_JOB_EVENT_LOG) > 100:
            del _JOB_EVENT_LOG[:-80]


def _set_job_progress(kind: str, **kwargs: Any) -> None:
    key = (kind or "job").strip() or "job"
    cur = dict(_JOB_PROGRESS.get(key) or {})
    cur.update(kwargs)
    cur["kind"] = key
    cur["ts"] = time.time()
    if "running" not in cur:
        cur["running"] = True
    _JOB_PROGRESS[key] = cur


def _clear_dedicated_running(kind: str, *, phase: str = "idle", message: str = "") -> None:
    """Dedicated progress bag'lerini ŞU AN listesinden düşür (JOB registry ile senkron)."""
    key = (kind or "").strip()
    msg = (message or "")[:200]
    if key == "firebase" and _firebase_progress.get("running"):
        _set_firebase_progress(running=False, phase=phase, message=msg)
    elif key == "news" and _news_progress.get("running"):
        _set_news_progress(running=False, phase=phase, message=msg)
    elif key in ("notification", "nt") and _nt_progress.get("running"):
        _set_nt_progress(running=False, phase=phase, message=msg)
    elif key in ("gsc_cwv", "cwv") and _gsc_cwv_progress.get("running"):
        _set_gsc_cwv_progress(
            running=False, phase=phase, message=msg, finished_at=time.time()
        )


def _finish_job_progress(
    kind: str,
    result: dict[str, Any] | None,
    *,
    trigger: str = "",
    name: str = "",
) -> None:
    key = (kind or "job").strip() or "job"
    ok = bool((result or {}).get("ok"))
    msg = str((result or {}).get("message") or ("OK" if ok else "Hata"))[:200]
    label = _trigger_label(trigger or str((_JOB_PROGRESS.get(key) or {}).get("trigger") or ""))
    title = name or key
    phase = "done" if ok else "error"
    _set_job_progress(
        key,
        running=False,
        phase=phase,
        message=msg,
        trigger=trigger or (_JOB_PROGRESS.get(key) or {}).get("trigger") or "",
    )
    _clear_dedicated_running(key, phase=phase, message=msg)
    _job_event(f"{label} bitti · {title} · {msg}")


def _reconcile_stale_running_progress() -> None:
    """Kilit yok / thread ölü iken running=True kalan zombi progress'i temizle."""
    now = time.time()
    browser_busy = bool(_browser_scrape_lock.locked())
    nt_busy = bool(_nt_lock.locked())

    def _stale(bag: dict[str, Any], *, lock_busy: bool, grace_sec: float = 25.0) -> bool:
        if not isinstance(bag, dict) or not bag.get("running"):
            return False
        try:
            ts = float(bag.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        age = (now - ts) if ts > 0 else 9_999.0
        if not lock_busy and age >= grace_sec:
            return True
        # Aşırı uzun (ör. 4s+) — kilit tutulsa bile paneli kilitlemesin
        if age >= 4 * 3600:
            return True
        return False

    if _stale(_firebase_progress, lock_busy=browser_busy):
        _set_firebase_progress(
            running=False,
            phase="stale",
            message="Cleared stale Firebase progress (no active browser lock)",
        )
        _set_job_progress(
            "firebase",
            running=False,
            phase="stale",
            message="Cleared stale Firebase progress",
        )
        _job_event("ŞU AN · firebase stale progress temizlendi")

    if _stale(_gsc_cwv_progress, lock_busy=browser_busy):
        _set_gsc_cwv_progress(
            running=False,
            phase="stale",
            message="Cleared stale GSC CWV progress",
            finished_at=now,
        )
        _set_job_progress("gsc_cwv", running=False, phase="stale", message="Cleared stale CWV progress")
        _set_job_progress("cwv", running=False, phase="stale", message="Cleared stale CWV progress")

    if _stale(_news_progress, lock_busy=nt_busy):
        _set_news_progress(
            running=False, phase="stale", message="Cleared stale News progress"
        )
        _set_job_progress("news", running=False, phase="stale", message="Cleared stale News progress")

    if _stale(_nt_progress, lock_busy=nt_busy):
        _set_nt_progress(
            running=False, phase="stale", message="Cleared stale Notification progress"
        )
        _set_job_progress(
            "notification", running=False, phase="stale", message="Cleared stale Notification progress"
        )

    # JOB registry: browser türleri kilit yokken running kalmasın
    browser_kinds = {
        "play",
        "asc",
        "firebase",
        "gsc_links",
        "gsc_cwv",
        "cwv",
        "admanager_policy",
        "policy",
        "pagespeed",
        "empower_intel",
        "empower_intel_sinemalar",
        "revenue_targets",
        "seo_audit",
        "market",
    }
    for key, bag in list(_JOB_PROGRESS.items()):
        if not isinstance(bag, dict) or not bag.get("running"):
            continue
        k = str(key)
        if k in browser_kinds and not browser_busy:
            try:
                ts = float(bag.get("ts") or 0)
            except (TypeError, ValueError):
                ts = 0.0
            age = (now - ts) if ts > 0 else 9_999.0
            if age >= 25.0:
                _set_job_progress(
                    k,
                    running=False,
                    phase="stale",
                    message="Cleared stale job progress (browser lock free)",
                )
                _clear_dedicated_running(
                    k, phase="stale", message="Cleared stale job progress"
                )


def _browser_gap_remaining_sec() -> int:
    if _last_browser_scrape_at <= 0:
        return 0
    gap = max(60, BRIDGE_SCRAPE_MIN_GAP_SEC) - (time.time() - _last_browser_scrape_at)
    return max(0, int(gap))


def _progress_running_jobs() -> list[dict[str, Any]]:
    _reconcile_stale_running_progress()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(kind: str, bag: dict[str, Any]) -> None:
        if not isinstance(bag, dict) or not bag.get("running"):
            return
        k = (kind or bag.get("kind") or "job").strip() or "job"
        if k in seen:
            return
        seen.add(k)
        out.append(
            {
                "kind": k,
                "phase": bag.get("phase") or "",
                "message": bag.get("message") or "",
                "step": bag.get("step"),
                "total_steps": bag.get("total_steps") or bag.get("total_pages"),
                "rows": bag.get("rows"),
                "trigger": bag.get("trigger") or "",
            }
        )

    for key, bag in (
        ("notification", _nt_progress),
        ("news", _news_progress),
        ("firebase", _firebase_progress),
        ("gsc_cwv", _gsc_cwv_progress),
    ):
        merged = dict(bag) if isinstance(bag, dict) else {}
        jp = _JOB_PROGRESS.get(key) or {}
        if isinstance(jp, dict) and jp.get("trigger") and not merged.get("trigger"):
            merged["trigger"] = jp.get("trigger")
        # JOB bitti ama dedicated bag unutulduysa gösterme
        if isinstance(jp, dict) and jp.get("running") is False and merged.get("running"):
            _clear_dedicated_running(
                key,
                phase=str(jp.get("phase") or "done"),
                message=str(jp.get("message") or ""),
            )
            continue
        if isinstance(jp, dict) and jp.get("running") and not merged.get("running"):
            # job registry running ama dedicated bag henüz idle — job'u kullan
            _add(key, jp)
            continue
        _add(key, merged)
    for key, bag in list(_JOB_PROGRESS.items()):
        _add(str(key), bag if isinstance(bag, dict) else {})
    return out


def _upcoming_slots(limit: int = 14) -> list[dict[str, Any]]:
    """Şu andan sonraki slotlar (TR)."""
    now = _now_tr()
    cur_min = now.hour * 60 + now.minute
    entries: list[tuple[int, str, str]] = []

    def add(kind: str, hours: tuple[int, ...] | list[int], minute: int) -> None:
        for h in hours:
            hm = f"{int(h):02d}:{int(minute):02d}"
            m = int(h) * 60 + int(minute)
            delta = m - cur_min
            if delta < 0:
                delta += 24 * 60
            entries.append((delta, kind, hm))

    def add_pairs(kind: str, slots: tuple[tuple[int, int], ...] | list[tuple[int, int]]) -> None:
        for h, m in slots:
            add(kind, (int(h),), int(m))

    add("play", PLAY_SLOT_HOURS, PLAY_SLOT_MINUTE)
    add("asc", ASC_SLOT_HOURS, ASC_SLOT_MINUTE)
    add("virgul", VIRGUL_SLOT_HOURS, VIRGUL_SLOT_MINUTE)
    add("firebase", FIREBASE_SLOT_HOURS, FIREBASE_SLOT_MINUTE)
    add("gsc_links", GSC_LINKS_SLOT_HOURS, GSC_SLOT_MINUTE)
    add("policy", TWICE_DAILY_HOURS, POLICY_SLOT_MINUTE)
    add("pagespeed", TWICE_DAILY_HOURS, SPEED_SLOT_MINUTE)
    add("noads", TWICE_DAILY_HOURS, NOADS_SLOT_MINUTE)
    add("revenue_targets", REVENUE_TARGETS_SLOT_HOURS, REVENUE_TARGETS_SLOT_MINUTE)
    add("seo_audit", SEO_AUDIT_SLOT_HOURS, SEO_AUDIT_SLOT_MINUTE)
    add("gsc_cwv", GSC_CWV_SLOT_HOURS, GSC_CWV_SLOT_MINUTE)
    add("market", MARKET_SLOT_HOURS, MARKET_SLOT_MINUTE)
    add_pairs("empower_intel", EMPOWER_INTEL_SLOTS)
    add_pairs("empower_intel_sinemalar", EMPOWER_INTEL_SINEMALAR_SLOTS)
    for h, m, _mode in MODERATION_SLOTS:
        add("sinemalar_moderation", (h,), m)

    for kind, last, interval in (
        ("notification", _last_result, AUTO_INTERVAL_SEC),
        ("news", _last_news_result, NEWS_AUTO_INTERVAL_SEC),
    ):
        try:
            ts = float((last or {}).get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        if ts <= 0:
            left = 0
        else:
            left = max(0, int(interval - (time.time() - ts)))
        try:
            from datetime import timedelta

            at = now + timedelta(seconds=left)
            hm = at.strftime("%H:%M")
        except Exception:
            hm = "—"
        entries.append((max(0, left // 60), kind, hm))

    entries.sort(key=lambda x: x[0])
    out: list[dict[str, Any]] = []
    for delta_m, kind, hm in entries[: max(1, limit)]:
        out.append(
            {
                "kind": kind,
                "slot_tr": hm,
                "in_min": int(delta_m),
                "today": int(delta_m) < (24 * 60 - cur_min),
            }
        )
    return out


_LOG_HIT_TIME_RE = re.compile(r"^(\d{2}:\d{2}:\d{2})\s+(.*)$")


def _log_hit_body(line: str) -> str:
    s = (line or "").strip()
    m = _LOG_HIT_TIME_RE.match(s)
    return (m.group(2) if m else s).strip()


def _recent_bridge_log_hits(limit: int = 24) -> list[str]:
    """LaunchAgent log + süreç içi job event halkası (satır başı TR saati tercih)."""
    paths = (
        Path.home() / "Library/Logs/doviz-admin-notification-bridge.log",
        Path.home() / ".seo-agent/cache/bridge-daemon.log",
    )
    needles = (
        "SIGTERM",
        "SIGKILL",
        "oturumu düştü",
        "oturumu düzeldi",
        "login alert",
        "needs_login",
        "Elle tetik",
        "Elle bitti",
        "Otomatik tetik",
        "Otomatik bitti",
        "Panel tetik",
        "Panel bitti",
        "in-process",
        "KEEP_OPEN",
        "yeniden kullanılıyor",
        "page-tarama",
        "sync ·",
        "başlıyor",
    )
    with _JOB_EVENT_LOCK:
        ring = [x.strip()[:220] for x in _JOB_EVENT_LOG[-max(limit, 40):] if (x or "").strip()]
    ring_bodies = {_log_hit_body(x) for x in ring}

    file_lines: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for line in raw[-800:]:
            s = line.strip()
            if not s or not any(n in s for n in needles):
                continue
            # Aynı adım ring'de (saatli) varsa dosya satırını atla
            if _log_hit_body(s) in ring_bodies:
                continue
            file_lines.append(s[:220])

    # Dosya (eski) + ring (canlı, saatli) — son N; gövde tekrarında saatli kazanır
    merged: list[str] = []
    seen_body: dict[str, int] = {}
    for line in file_lines + ring:
        body = _log_hit_body(line)
        has_time = bool(_LOG_HIT_TIME_RE.match(line))
        if body in seen_body:
            idx = seen_body[body]
            old = merged[idx]
            old_has = bool(_LOG_HIT_TIME_RE.match(old))
            if has_time and not old_has:
                merged[idx] = line
            continue
        seen_body[body] = len(merged)
        merged.append(line)
    return merged[-limit:]


def _health_payload() -> dict[str, Any]:
    now = _now_tr()
    return {
        "ok": True,
        "service": "doviz-admin-bridge",
        "now_tr": now.strftime("%Y-%m-%d %H:%M:%S"),
        "auto_interval_sec": AUTO_INTERVAL_SEC,
        "news_interval_sec": NEWS_AUTO_INTERVAL_SEC,
        "virgul_interval_sec": VIRGUL_AUTO_INTERVAL_SEC,
        "news_every_n": NEWS_AUTO_EVERY_N or None,
        "last": _last_result,
        "last_news": _last_news_result,
        "last_virgul": _last_virgul_result,
        "last_play": _last_play_result,
        "last_asc": _last_asc_result,
        "last_firebase": _last_firebase_result,
        "last_pagespeed": _last_pagespeed_result,
        "last_empower_intel": _last_empower_intel_result,
        "last_empower_intel_sinemalar": _last_empower_intel_sinemalar_result,
        "last_seo_audit": _last_seo_audit_result,
        "last_gsc_cwv": _last_gsc_cwv_result,
        "last_market": _last_market_result,
        "gsc_cwv_progress": dict(_gsc_cwv_progress),
        "last_gsc_links": _last_gsc_links_result,
        "last_policy": _last_policy_result,
        "last_noads": _last_noads_result,
        "last_moderation": _last_moderation_result,
        "play_interval_sec": PLAY_AUTO_INTERVAL_SEC,
        "asc_interval_sec": ASC_AUTO_INTERVAL_SEC,
        "schedule": {
            "notification_sec": AUTO_INTERVAL_SEC,
            "news_sec": NEWS_AUTO_INTERVAL_SEC,
            "virgul_slots_tr": [f"{h:02d}:{VIRGUL_SLOT_MINUTE:02d}" for h in VIRGUL_SLOT_HOURS],
            "play_slots_tr": [f"{h:02d}:{PLAY_SLOT_MINUTE:02d}" for h in PLAY_SLOT_HOURS],
            "asc_slots_tr": [f"{h:02d}:{ASC_SLOT_MINUTE:02d}" for h in ASC_SLOT_HOURS],
            "firebase_slots_tr": [f"{h:02d}:{FIREBASE_SLOT_MINUTE:02d}" for h in FIREBASE_SLOT_HOURS],
            "gsc_slots_tr": [f"{h:02d}:{GSC_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
            "policy_slots_tr": [f"{h:02d}:{POLICY_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
            "pagespeed_slots_tr": [f"{h:02d}:{SPEED_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
            "noads_slots_tr": [f"{h:02d}:{NOADS_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
            "moderation_slots_tr": [f"{h:02d}:{m:02d}" for h, m, _ in MODERATION_SLOTS],
            "seo_audit_slots_tr": [
                f"{h:02d}:{SEO_AUDIT_SLOT_MINUTE:02d}" for h in SEO_AUDIT_SLOT_HOURS
            ],
            "gsc_cwv_slots_tr": [
                f"{h:02d}:{GSC_CWV_SLOT_MINUTE:02d}" for h in GSC_CWV_SLOT_HOURS
            ],
            "market_slots_tr": [
                f"{h:02d}:{MARKET_SLOT_MINUTE:02d}" for h in MARKET_SLOT_HOURS
            ],
            "empower_intel_slots_tr": [
                f"{h:02d}:{m:02d}" for h, m in EMPOWER_INTEL_SLOTS
            ],
            "empower_intel_sinemalar_slots_tr": [
                f"{h:02d}:{m:02d}" for h, m in EMPOWER_INTEL_SINEMALAR_SLOTS
            ],
            "scrape_min_gap_sec": BRIDGE_SCRAPE_MIN_GAP_SEC,
            "scrape_deferred": sorted(_scrape_deferred_jobs.keys()),
            "retry_max": BRIDGE_RETRY_MAX,
            "retry_gap_sec": BRIDGE_RETRY_GAP_SEC,
        },
        "pending_retries": {
            k: {
                "attempt": v.get("attempt"),
                "name": v.get("name"),
                "next_in_sec": max(0, int(float(v.get("next_at") or 0) - time.time())),
            }
            for k, v in _job_retries.items()
        },
        "news_progress": dict(_news_progress),
        "nt_progress": dict(_nt_progress),
        "live": {
            "browser_lock_held": bool(_browser_scrape_lock.locked()),
            "browser_gap_remaining_sec": _browser_gap_remaining_sec(),
            "deferred": sorted(_scrape_deferred_jobs.keys()),
            "running": _progress_running_jobs(),
            "login_alerts_open": sorted(k for k, v in _login_alert_open.items() if v),
            "upcoming": _upcoming_slots(16),
            "log_hits": _recent_bridge_log_hits(20),
        },
    }


def _bridge_status_html() -> str:
    return """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>SEO Bridge · canlı</title>
<style>
  :root { --bg:#0f1419; --card:#1a222c; --line:#2a3542; --text:#e7eef7; --muted:#8b9aab; --ok:#3ecf8e; --bad:#f07178; --run:#59c2ff; --warn:#e6b450; }
  * { box-sizing: border-box; }
  body { margin:0; font: 13px/1.45 ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; background: var(--bg); color: var(--text); }
  header { display:flex; flex-wrap:wrap; gap:12px; align-items:baseline; justify-content:space-between; padding:16px 20px; border-bottom:1px solid var(--line); }
  h1 { margin:0; font-size:16px; font-weight:700; letter-spacing:.02em; }
  .meta { color: var(--muted); font-variant-numeric: tabular-nums; }
  main { display:grid; gap:14px; padding:16px 20px 40px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
  section { background: var(--card); border:1px solid var(--line); border-radius:10px; padding:12px 14px; }
  section h2 { margin:0 0 10px; font-size:11px; text-transform:uppercase; letter-spacing:.08em; color: var(--muted); }
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; }
  .pill.ok { background:#163528; color: var(--ok); }
  .pill.bad { background:#3a1c1f; color: var(--bad); }
  .pill.run { background:#123447; color: var(--run); }
  .pill.warn { background:#3a2e12; color: var(--warn); }
  table { width:100%; border-collapse: collapse; }
  th, td { text-align:left; padding:5px 4px; border-bottom:1px solid var(--line); vertical-align:top; font-variant-numeric: tabular-nums; }
  th { color: var(--muted); font-weight:600; font-size:11px; }
  .msg { color: var(--muted); max-width: 42ch; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  pre { margin:0; white-space:pre-wrap; word-break:break-word; color: var(--muted); font: 11px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace; max-height:220px; overflow:auto; }
  .empty { color: var(--muted); }
</style>
</head>
<body>
<header>
  <div>
    <h1>SEO Agent Bridge</h1>
    <div class="meta" id="clock">—</div>
  </div>
  <div id="badges"></div>
</header>
<main>
  <section>
    <h2>Şu an</h2>
    <div id="now" class="empty">yükleniyor…</div>
  </section>
  <section>
    <h2>Kuyruk / retry</h2>
    <div id="queue" class="empty">—</div>
  </section>
  <section style="grid-column:1/-1">
    <h2>Sonraki slotlar (TR)</h2>
    <div id="upcoming" class="empty">—</div>
  </section>
  <section style="grid-column:1/-1">
    <h2>Son sonuçlar</h2>
    <div id="lasts"></div>
  </section>
  <section style="grid-column:1/-1">
    <h2>Log (elle / oto / panel · SIGTERM / login)</h2>
    <div id="logs" class="empty">—</div>
  </section>
</main>
<script>
const LAST_KEYS = [
  ['last','notification'],['last_news','news'],['last_virgul','virgul'],
  ['last_play','play'],['last_asc','asc'],['last_firebase','firebase'],
  ['last_gsc_links','gsc_links'],['last_policy','policy'],['last_gsc_cwv','gsc_cwv'],
  ['last_pagespeed','pagespeed'],['last_noads','noads'],['last_moderation','moderation'],
  ['last_seo_audit','seo_audit'],['last_market','market'],
  ['last_empower_intel','empower'],['last_empower_intel_sinemalar','empower_sin']
];
function esc(s){ return String(s??'').replace(/[&<>"']/g, c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }
function okPill(ok){ return ok ? '<span class="pill ok">ok</span>' : '<span class="pill bad">fail</span>'; }
async function tick(){
  try {
    const r = await fetch('/health', { cache:'no-store' });
    const d = await r.json();
    const live = d.live || {};
    document.getElementById('clock').textContent = (d.now_tr || '') + ' TR · yenileme ~4s';
    const badges = [];
    badges.push(d.ok ? '<span class="pill ok">bridge up</span>' : '<span class="pill bad">down</span>');
    if (live.browser_lock_held) badges.push('<span class="pill run">browser busy</span>');
    if ((live.browser_gap_remaining_sec||0) > 0) badges.push('<span class="pill warn">gap '+live.browser_gap_remaining_sec+'s</span>');
    if ((live.login_alerts_open||[]).length) badges.push('<span class="pill bad">login open: '+esc(live.login_alerts_open.join(', '))+'</span>');
    document.getElementById('badges').innerHTML = badges.join(' ');

    const run = live.running || [];
    let nowHtml = '';
    if (run.length) {
      nowHtml = '<table><tr><th>iş</th><th>kaynak</th><th>faz</th><th>mesaj</th></tr>' + run.map(j => {
        const trig = String(j.trigger||'').toLowerCase();
        const src = (trig==='manual'||trig==='elle'||trig==='http') ? 'Elle'
          : (trig==='page-tarama'||trig==='panel') ? 'Panel'
          : (trig==='schedule'||trig==='auto') ? 'Oto'
          : (trig ? esc(j.trigger) : '—');
        return '<tr><td>'+esc(j.kind)+'</td><td>'+src+'</td><td>'+esc(j.phase)+' '+(j.step!=null?j.step+'/'+(j.total_steps||'?') : '')+'</td><td class="msg" title="'+esc(j.message)+'">'+esc(j.message)+'</td></tr>';
      }).join('') + '</table>';
    } else {
      nowHtml = '<p class="empty">Çalışan progress yok'+(live.browser_lock_held?' · browser kilidi tutuluyor':'')+'.</p>';
    }
    document.getElementById('now').innerHTML = nowHtml;

    const def = live.deferred || (d.schedule&&d.schedule.scrape_deferred) || [];
    const retries = d.pending_retries || {};
    let q = '';
    if (def.length) q += '<p><b>Deferred:</b> '+esc(def.join(', '))+'</p>';
    const rk = Object.keys(retries);
    if (rk.length) {
      q += '<table><tr><th>retry</th><th>deneme</th><th>kalan</th></tr>' + rk.map(k => {
        const v = retries[k]||{};
        return '<tr><td>'+esc(v.name||k)+'</td><td>'+esc(v.attempt)+'</td><td>'+esc(v.next_in_sec)+'s</td></tr>';
      }).join('') + '</table>';
    }
    document.getElementById('queue').innerHTML = q || '<p class="empty">Kuyruk boş</p>';

    const up = live.upcoming || [];
    document.getElementById('upcoming').innerHTML = up.length
      ? '<table><tr><th>iş</th><th>saat</th><th>kalan</th></tr>'+up.map(u =>
          '<tr><td>'+esc(u.kind)+'</td><td>'+esc(u.slot_tr)+'</td><td>'+esc(u.in_min)+' dk</td></tr>'
        ).join('')+'</table>'
      : '<p class="empty">—</p>';

    document.getElementById('lasts').innerHTML = '<table><tr><th>iş</th><th></th><th>mesaj</th></tr>' + LAST_KEYS.map(([key,label]) => {
      const v = d[key] || {};
      const ok = !!v.ok;
      const msg = v.message || (v.needs_login ? 'needs_login' : '');
      return '<tr><td>'+esc(label)+'</td><td>'+okPill(ok)+'</td><td class="msg" title="'+esc(msg)+'">'+esc(msg||'—')+'</td></tr>';
    }).join('') + '</table>';

    const logs = live.log_hits || [];
    const logsEl = document.getElementById('logs');
    if (!logs.length) {
      logsEl.className = 'empty';
      logsEl.textContent = 'Henüz SIGTERM/SIGKILL/login satırı yok (veya log dosyası boş).';
    } else {
      logsEl.className = '';
      logsEl.innerHTML = '<table><tr><th>saat</th><th>adım</th></tr>' + logs.map(line => {
        const s = String(line || '');
        const m = s.match(/^(\\d{2}:\\d{2}:\\d{2})\\s+(.*)$/);
        const t = m ? m[1] : '—';
        const msg = m ? m[2] : s;
        return '<tr><td class="t">' + esc(t) + '</td><td>' + esc(msg) + '</td></tr>';
      }).join('') + '</table>';
    }
  } catch (e) {
    document.getElementById('clock').textContent = 'health okunamadı: ' + e;
  }
}
tick();
setInterval(tick, 4000);
</script>
</body>
</html>
"""


def _cors_headers(handler: BaseHTTPRequestHandler) -> dict[str, str]:
    origin = handler.headers.get("Origin") or "*"
    allowed = {
        "http://127.0.0.1:8012",
        "http://localhost:8012",
        "https://projectcontrol.up.railway.app",
    }
    allow = origin if origin in allowed or origin == "null" else (
        origin
        if origin.startswith("http://127.0.0.1:") or origin.startswith("http://localhost:")
        else "https://projectcontrol.up.railway.app"
    )
    return {
        "Access-Control-Allow-Origin": allow,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Accept",
        "Access-Control-Allow-Private-Network": "true",
        "Access-Control-Max-Age": "86400",
        "Cache-Control": "no-store",
    }


class _BridgeHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, code: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        for k, v in _cors_headers(self).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, code: int, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(code)
        for k, v in _cors_headers(self).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        for k, v in _cors_headers(self).items():
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/status", "/dashboard", "/panel"):
            self._send_html(200, _bridge_status_html())
            return
        if path in ("/", "/health"):
            self._send(200, _health_payload())
            return
        if path in ("/whoami", "/worker"):
            # Panel bu ucu 127.0.0.1 üzerinden yoklar: Update page'e basılan Mac'e öncelik verilir
            self._send(
                200,
                {
                    "ok": True,
                    "worker": _worker_name(),
                    "version": BRIDGE_VERSION,
                    "auto_jobs": BRIDGE_AUTO_JOBS,
                    "not_ready": {k: v for k, v in _worker_readiness().items() if v != "ready"},
                },
            )
            return
        if path in ("/news-progress", "/progress-news"):
            self._send(200, {"ok": True, **dict(_news_progress)})
            return
        if path in ("/nt-progress", "/notification-progress", "/progress-nt"):
            self._send(200, {"ok": True, **dict(_nt_progress)})
            return
        if path in ("/firebase-progress", "/progress-firebase"):
            self._send(200, {"ok": True, **dict(_firebase_progress)})
            return
        if path in ("/gsc-cwv-progress", "/progress-gsc-cwv"):
            self._send(200, {"ok": True, **dict(_gsc_cwv_progress)})
            return
        self._send(404, {"ok": False, "message": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query or "")

        def _qs_flag(name: str) -> bool:
            raw = (qs.get(name) or [""])[0].strip().lower()
            return raw in ("1", "true", "yes", "full")

        def _qs_int(name: str, default: int) -> int:
            raw = (qs.get(name) or [""])[0].strip()
            if raw.isdigit():
                return int(raw)
            return default

        if path in ("/sync", "/run", "/"):
            lock, busy, runner = (
                _nt_lock,
                "Notification/news sync zaten çalışıyor, bekleyin.",
                run_notification_bridge_once,
            )
        elif path in ("/sync-news", "/news"):
            lock = _nt_lock
            busy = "Notification/news sync zaten çalışıyor, bekleyin."
            full = _qs_flag("full")
            days = _qs_int("days", 7)

            def runner() -> dict[str, Any]:
                return run_news_bridge_once(days=None if full else days, full=full)

        elif path in ("/sync-virgul", "/virgul"):
            lock, busy, runner = (
                _virgul_lock,
                "Virgül sync zaten çalışıyor, bekleyin.",
                run_virgul_bridge_once,
            )
        elif path in ("/sync-play", "/play", "/sync-android"):
            # Uzun Play scrape — HTTP hemen döner; süre sınırı yok (arka plan)
            if not _play_lock.acquire(blocking=False):
                self._send(
                    409,
                    {"ok": False, "message": "Play Console sync zaten çalışıyor, bekleyin."},
                )
                return
            _set_job_progress(
                "play",
                running=True,
                phase="starting",
                trigger="manual",
                message="Elle · Play Console başladı",
            )
            _job_event("Elle tetik · play · Play Console başladı")

            def _bg_play() -> None:
                try:
                    out = run_play_bridge_once()
                    _finish_job_progress(
                        "play",
                        out if isinstance(out, dict) else {"ok": False, "message": "hata"},
                        trigger="manual",
                        name="Play Console",
                    )
                except Exception as exc:
                    traceback.print_exc()
                    _finish_job_progress(
                        "play",
                        {"ok": False, "message": str(exc)},
                        trigger="manual",
                        name="Play Console",
                    )
                finally:
                    _play_lock.release()

            threading.Thread(target=_bg_play, name="play-bridge-manual", daemon=True).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "play",
                    "message": "Play Console tarama arka planda başladı (süre sınırı yok · dün+bugün)",
                },
            )
            return
        elif path in ("/sync-play-vitals", "/play-vitals", "/sync-android-vitals"):
            if not _play_lock.acquire(blocking=False):
                self._send(
                    409,
                    {"ok": False, "message": "Play Console sync zaten çalışıyor, bekleyin."},
                )
                return
            _set_job_progress(
                "play_vitals",
                running=True,
                phase="starting",
                trigger="manual",
                message="Elle · Play Vitals başladı",
            )
            _job_event("Elle tetik · play_vitals · Play Vitals başladı")

            def _bg_play_vitals() -> None:
                try:
                    out = run_play_vitals_bridge_once()
                    _finish_job_progress(
                        "play_vitals",
                        out if isinstance(out, dict) else {"ok": False, "message": "hata"},
                        trigger="manual",
                        name="Play Vitals",
                    )
                except Exception as exc:
                    traceback.print_exc()
                    _finish_job_progress(
                        "play_vitals",
                        {"ok": False, "message": str(exc)},
                        trigger="manual",
                        name="Play Vitals",
                    )
                finally:
                    _play_lock.release()

            threading.Thread(
                target=_bg_play_vitals, name="play-vitals-bridge-manual", daemon=True
            ).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "play_vitals",
                    "message": "Play Vitals tarama arka planda başladı (süre sınırı yok)",
                },
            )
            return
        elif path in ("/sync-gsc-links", "/gsc-links", "/sync-backlinks"):
            lock, busy, runner = (
                _gsc_links_lock,
                "GSC Links sync zaten çalışıyor, bekleyin.",
                run_gsc_links_bridge_once,
            )
        elif path in (
            "/sync-revenue-targets",
            "/revenue-targets",
            "/sync-ad-targets",
        ):
            lock, busy, runner = (
                _revenue_targets_lock,
                "Revenue targets sync zaten çalışıyor, bekleyin.",
                run_revenue_targets_bridge_once,
            )
        elif path in ("/sync-policy", "/policy", "/sync-admanager-policy"):
            lock, busy, runner = (
                _policy_lock,
                "Ad Manager Policy sync zaten çalışıyor, bekleyin.",
                run_admanager_policy_bridge_once,
            )
        elif path in ("/sync-noads", "/noads", "/sync-sinemalar-noads"):
            lock, busy, runner = (
                _noads_lock,
                "Sinemalar noAds sync zaten çalışıyor, bekleyin.",
                run_sinemalar_noads_bridge_once,
            )
        elif path in (
            "/sync-sinemalar-moderation",
            "/sync-moderation",
            "/moderation",
        ):
            which = (qs.get("which") or ["both"])[0].strip().lower()
            if which not in ("yesterday", "today", "both"):
                which = "both"

            def _mod_runner() -> dict[str, Any]:
                return run_sinemalar_moderation_bridge_once(incremental_which=which)

            lock, busy, runner = (
                _moderation_lock,
                "Sinemalar moderasyon sync zaten çalışıyor, bekleyin.",
                _mod_runner,
            )
        elif path in ("/sync-asc", "/asc", "/sync-ios"):
            lock, busy, runner = (
                _asc_lock,
                "ASC Console sync zaten çalışıyor, bekleyin.",
                run_asc_bridge_once,
            )
        elif path in ("/sync-firebase", "/firebase", "/sync-s-firebase"):
            lock, busy, runner = (
                _firebase_lock,
                "Firebase Console sync zaten çalışıyor, bekleyin.",
                run_firebase_bridge_once,
            )
        elif path in ("/sync-pagespeed", "/pagespeed", "/sync-speed"):
            lock, busy, runner = (
                _pagespeed_lock,
                "PageSpeed sync zaten çalışıyor, bekleyin.",
                run_pagespeed_bridge_once,
            )
        elif path in ("/sync-empower-intel", "/empower-intel", "/sync-empower"):
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            mode = "yesterday"
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and payload.get("mode"):
                        mode = str(payload.get("mode") or "yesterday")
                except Exception:
                    pass
            qs_mode = ((qs.get("mode") or [""])[0] or "").strip()
            if qs_mode:
                mode = qs_mode

            def _empower_runner(*, _mode: str = mode) -> dict[str, Any]:
                return run_empower_intel_bridge_once(mode=_mode)

            lock, busy, runner = (
                _empower_intel_lock,
                "Empower Intel sync zaten çalışıyor, bekleyin.",
                _empower_runner,
            )
        elif path in (
            "/sync-empower-intel-sinemalar",
            "/empower-intel-sinemalar",
            "/sync-empower-sinemalar",
        ):
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            mode = "yesterday"
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and payload.get("mode"):
                        mode = str(payload.get("mode") or "yesterday")
                except Exception:
                    pass
            qs_mode = ((qs.get("mode") or [""])[0] or "").strip()
            if qs_mode:
                mode = qs_mode

            def _empower_sin_runner(*, _mode: str = mode) -> dict[str, Any]:
                return run_empower_intel_sinemalar_bridge_once(mode=_mode)

            lock, busy, runner = (
                _empower_intel_sinemalar_lock,
                "Empower Intel Sinemalar sync zaten çalışıyor, bekleyin.",
                _empower_sin_runner,
            )
        elif path in ("/sync-market", "/market", "/sync-piyasa", "/piyasa"):
            lock, busy, runner = (
                _market_lock,
                "Piyasa tarama zaten çalışıyor, bekleyin.",
                run_market_tarama_bridge_once,
            )
        elif path in ("/sync-seo-audit", "/seo-audit", "/sync-seo"):
            # Uzun süren scrape — HTTP hemen döner; arka planda çalışır (Tara timeout olmasın)
            site_id = _qs_int("site_id", 0) or None
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and payload.get("site_id"):
                        try:
                            site_id = int(payload.get("site_id"))
                        except (TypeError, ValueError):
                            pass
                except Exception:
                    pass
            if not _seo_audit_lock.acquire(blocking=False):
                self._send(
                    409,
                    {"ok": False, "message": "SEO denetim tarama zaten çalışıyor, bekleyin."},
                )
                return
            _set_job_progress(
                "seo_audit",
                running=True,
                phase="starting",
                trigger="manual",
                message="Elle · SEO denetim başladı",
            )
            _job_event("Elle tetik · seo_audit · SEO denetim başladı")

            def _bg() -> None:
                try:
                    out = run_seo_audit_bridge_once(site_id=site_id)
                    _finish_job_progress(
                        "seo_audit",
                        out if isinstance(out, dict) else {"ok": False, "message": "hata"},
                        trigger="manual",
                        name="SEO denetim",
                    )
                except Exception as exc:
                    traceback.print_exc()
                    _finish_job_progress(
                        "seo_audit",
                        {"ok": False, "message": str(exc)},
                        trigger="manual",
                        name="SEO denetim",
                    )
                finally:
                    _seo_audit_lock.release()

            threading.Thread(target=_bg, name="seo-audit-bridge", daemon=True).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "seo_audit",
                    "site_id": site_id,
                    "message": "SEO denetim tarama arka planda başladı (GA4 top URL)",
                },
            )
            return
        elif path in ("/sync-gsc-cwv", "/gsc-cwv", "/sync-web-vitals", "/web-vitals"):
            site_key = (qs.get("site") or [""])[0].strip().lower() or None
            # Manuel + otomatik varsayılan: screenshot. full yalnız açık mode=full.
            mode = (qs.get("mode") or [""])[0].strip().lower() or "shots"
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict):
                        if payload.get("site"):
                            site_key = str(payload.get("site") or "").strip().lower() or site_key
                        if payload.get("mode"):
                            mode = str(payload.get("mode") or "").strip().lower() or mode
                        # domain öncelikli (site_id sabit varsayımı kırılgan)
                        dom = str(payload.get("domain") or payload.get("site_domain") or "").lower()
                        if "doviz" in dom:
                            site_key = "doviz"
                        elif "sinemalar" in dom:
                            site_key = "sinemalar"
                        elif not site_key and payload.get("site_id") in (1, "1"):
                            site_key = "doviz"
                        elif not site_key and payload.get("site_id") in (2, "2"):
                            site_key = "sinemalar"
                except Exception:
                    pass
            if mode not in ("full", "scrape", "deep", "amp", "charts", "charts_only", "chart"):
                mode = "shots"
            if not _gsc_cwv_lock.acquire(blocking=False):
                self._send(
                    409,
                    {
                        "ok": False,
                        "running": True,
                        "progress": dict(_gsc_cwv_progress),
                        "message": "GSC CWV tarama zaten çalışıyor, bekleyin.",
                    },
                )
                return

            _set_gsc_cwv_progress(
                running=True,
                phase="starting",
                site=site_key or "all",
                message="GSC CWV screenshot kuyruğa alındı",
                started_at=time.time(),
                finished_at=0.0,
                trigger="manual",
            )
            _set_job_progress(
                "gsc_cwv",
                running=True,
                phase="starting",
                trigger="manual",
                message=f"Elle · GSC CWV ({mode}) başladı",
                site=site_key or "all",
            )
            _job_event(f"Elle tetik · gsc_cwv · mode={mode} site={site_key or 'all'}")

            def _bg_cwv() -> None:
                try:
                    out = run_gsc_cwv_bridge_once(site_key=site_key, mode=mode)
                    _finish_job_progress(
                        "gsc_cwv",
                        out if isinstance(out, dict) else {"ok": False},
                        trigger="manual",
                        name="GSC CWV",
                    )
                except Exception as exc:
                    traceback.print_exc()
                    _set_gsc_cwv_progress(
                        running=False,
                        phase="error",
                        message="GSC CWV thread hatası",
                        finished_at=time.time(),
                    )
                    _finish_job_progress(
                        "gsc_cwv",
                        {"ok": False, "message": str(exc)},
                        trigger="manual",
                        name="GSC CWV",
                    )
                finally:
                    _gsc_cwv_lock.release()

            threading.Thread(target=_bg_cwv, name="gsc-cwv-bridge", daemon=True).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "gsc_cwv",
                    "mode": mode,
                    "site": site_key,
                    "progress": dict(_gsc_cwv_progress),
                    "message": "GSC CWV screenshot tarama arka planda başladı",
                },
            )
            return
        elif path in ("/sync-gsc-cwv-shots", "/gsc-cwv-shots", "/sync-cwv-shots"):
            site_key = (qs.get("site") or [""])[0].strip().lower() or "doviz"
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and payload.get("site"):
                        site_key = str(payload.get("site") or "doviz").strip().lower()
                except Exception:
                    pass
            if not _gsc_cwv_lock.acquire(blocking=False):
                self._send(
                    409,
                    {
                        "ok": False,
                        "running": True,
                        "message": "GSC CWV / shots zaten çalışıyor, bekleyin.",
                    },
                )
                return

            def _bg_shots() -> None:
                try:
                    out = run_gsc_cwv_shots_bridge_once(site_key=site_key)
                    _finish_job_progress(
                        "gsc_cwv",
                        out if isinstance(out, dict) else {"ok": False},
                        trigger="manual",
                        name="GSC CWV shots",
                    )
                except Exception as exc:
                    traceback.print_exc()
                    _finish_job_progress(
                        "gsc_cwv",
                        {"ok": False, "message": str(exc)},
                        trigger="manual",
                        name="GSC CWV shots",
                    )
                finally:
                    _gsc_cwv_lock.release()

            _set_job_progress(
                "gsc_cwv",
                running=True,
                phase="starting",
                trigger="manual",
                message=f"Elle · CWV shots ({site_key}) başladı",
            )
            _job_event(f"Elle tetik · gsc_cwv_shots · site={site_key}")
            threading.Thread(target=_bg_shots, name="gsc-cwv-shots", daemon=True).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "gsc_cwv_shots",
                    "site": site_key,
                    "message": "CWV screenshot yakalama arka planda başladı",
                },
            )
            return
        elif path in ("/open-noads", "/noads-open", "/noads-prefill"):
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            url = (qs.get("url") or [""])[0].strip()
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and (payload.get("url") or "").strip():
                        url = str(payload.get("url") or "").strip()
                except Exception:
                    pass
            result = run_open_noads_prefill(url)
            self._send(200 if result.get("ok") else 502, result)
            return
        elif path in ("/login", "/oturum", "/open-login"):
            target = (qs.get("target") or qs.get("t") or [""])[0].strip().lower()
            self._send(*run_open_login(target))
            return
        elif path in ("/sync-all", "/all"):
            lock, busy, runner = (_nt_lock, "Sync zaten çalışıyor, bekleyin.", run_all_once)
        else:
            self._send(404, {"ok": False, "message": "not found"})
            return

        # Elle HTTP sync — live.NOW + log
        job_kind = (
            getattr(runner, "__name__", "job")
            .removeprefix("run_")
            .removesuffix("_bridge_once")
            .removesuffix("_once")
        )
        if job_kind in ("_mod_runner", "_empower_sin_runner", "_empower_runner"):
            if "moderation" in path:
                job_kind = "sinemalar_moderation"
            elif "sinemalar" in path:
                job_kind = "empower_intel_sinemalar"
            elif "empower" in path:
                job_kind = "empower_intel"
        job_name = (
            busy.replace(" zaten çalışıyor, bekleyin.", "")
            .replace(" sync", "")
            .strip()
            or job_kind
        )
        result = _run_locked_job(
            name=job_name,
            lock=lock,
            runner=runner,
            kind=job_kind,
            notify=False,
            trigger="manual",
        )
        if result is None:
            self._send(409, {"ok": False, "message": busy})
            return
        self._send(200 if result.get("ok") else 502, result)


def _now_tr():
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime

        return datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        from datetime import datetime, timezone, timedelta

        return datetime.now(timezone.utc) + timedelta(hours=3)


def _slot_due(last_slot: str, hours: tuple[int, ...] | list[int], minute: int) -> tuple[bool, str]:
    """TR saatinde [hour:minute, hour:minute+window) içindeyse ve slot işlenmediyse True."""
    now = _now_tr()
    cur = now.hour * 60 + now.minute
    window = max(5, SLOT_WINDOW_MIN)
    for hour in hours:
        start = int(hour) * 60 + int(minute)
        if start <= cur < start + window:
            slot = f"{now.strftime('%Y-%m-%d')}-{int(hour):02d}{int(minute):02d}"
            if last_slot == slot:
                return False, slot
            return True, slot
    return False, ""


def _multi_slot_due(
    last_slot: str, slots: tuple[tuple[int, int], ...] | list[tuple[int, int]]
) -> tuple[bool, str]:
    """Farklı dakika/saat çiftleri için slot (ör. 02:12 + 13:18)."""
    now = _now_tr()
    cur = now.hour * 60 + now.minute
    window = max(5, SLOT_WINDOW_MIN)
    for hour, minute in slots:
        start = int(hour) * 60 + int(minute)
        if start <= cur < start + window:
            slot = f"{now.strftime('%Y-%m-%d')}-{int(hour):02d}{int(minute):02d}"
            if last_slot == slot:
                return False, slot
            return True, slot
    return False, ""


def _interval_due(last_at: float, interval_sec: int, *, min_sec: int = 60) -> bool:
    if last_at <= 0:
        return True
    return (time.time() - last_at) >= max(min_sec, interval_sec)


def _nt_news_in_active_hours() -> bool:
    """08:00–20:00 Europe/Istanbul (saat 8…20 dahil → 20:59’a kadar)."""
    now = _now_tr()
    return NT_NEWS_ACTIVE_START_HOUR <= int(now.hour) <= NT_NEWS_ACTIVE_END_HOUR


def _should_run_notification_auto() -> bool:
    """08–20 arası 30 dk’da bir live refresh."""
    if not _nt_news_in_active_hours():
        return False
    return _interval_due(_last_nt_auto_at, AUTO_INTERVAL_SEC, min_sec=60)


def _should_run_news_auto() -> bool:
    """08–20 arası 30 dk’da bir (notification ile aynı pencere)."""
    global _auto_cycle
    if not _nt_news_in_active_hours():
        return False
    if NEWS_AUTO_EVERY_N > 0:
        return NEWS_AUTO_EVERY_N <= 1 or (_auto_cycle % NEWS_AUTO_EVERY_N) == 1
    return _interval_due(_last_news_auto_at, NEWS_AUTO_INTERVAL_SEC, min_sec=60)


def _notification_night_seal_due() -> tuple[bool, str]:
    """Gece dönümünde dünü kayda al (günde 1)."""
    global _last_notification_night_seal_slot
    now = _now_tr()
    if int(now.hour) != NOTIFICATION_NIGHT_SEAL_HOUR:
        return False, ""
    minute = int(now.minute)
    start = NOTIFICATION_NIGHT_SEAL_MINUTE
    window = max(5, SLOT_WINDOW_MIN)
    if not (start <= minute < start + window):
        return False, ""
    slot = f"{now.strftime('%Y-%m-%d')}-seal"
    if _last_notification_night_seal_slot == slot:
        return False, slot
    return True, slot


def _clear_job_retry(kind: str) -> None:
    _job_retries.pop(kind, None)


def _retry_policy(kind: str, *, failed_slot: str = "") -> tuple[int, int]:
    """kind/slot için (max_deneme, aralık_saniye)."""
    if kind == "revenue_targets" and failed_slot:
        tail = failed_slot.rsplit("-", 1)[-1]
        if len(tail) >= 2 and tail[:2].isdigit():
            hour = int(tail[:2])
            if hour == int(REVENUE_TARGETS_SLOT_HOURS[0]):
                return REVENUE_TARGETS_NIGHT_RETRY_MAX, max(
                    60, REVENUE_TARGETS_NIGHT_RETRY_GAP_SEC
                )
    return BRIDGE_RETRY_MAX, max(60, BRIDGE_RETRY_GAP_SEC)


def _arm_job_retry(kind: str, *, name: str, failed_slot: str = "") -> None:
    """Başarısız tur sonrası bir sonraki yeniden denemeyi planla."""
    if _config_fail.get(kind):
        print(
            f"Auto {name}: eksik ayar — yeniden deneme yok, sonraki planlı slota bırakıldı",
            flush=True,
        )
        _clear_job_retry(kind)
        return
    retry_max, gap = _retry_policy(kind, failed_slot=failed_slot)
    st = _job_retries.get(kind) or {"attempt": 0, "name": name}
    attempt = int(st.get("attempt") or 0)
    if attempt >= retry_max:
        print(
            f"Auto {name}: {retry_max} yeniden deneme tükendi — "
            "sonraki planlı slota kadar bekleniyor",
            flush=True,
        )
        _clear_job_retry(kind)
        return
    attempt += 1
    _job_retries[kind] = {
        "attempt": attempt,
        "next_at": time.time() + gap,
        "name": name,
        "max": retry_max,
        "gap": gap,
        "failed_slot": failed_slot or st.get("failed_slot") or "",
    }
    gap_label = f"{gap // 3600} sa" if gap >= 3600 else f"{gap // 60} dk"
    print(
        f"Auto {name}: yeniden deneme planlandı {attempt}/{retry_max} · ~{gap_label} sonra",
        flush=True,
    )


def _run_locked_job(
    *,
    name: str,
    lock: threading.Lock,
    runner,
    kind: str,
    notify: bool = True,
    trigger: str = "schedule",
) -> dict[str, Any] | None:
    if not lock.acquire(blocking=False):
        print(f"Auto {name} atlandı (manuel sync sürüyor)", flush=True)
        return None
    label = _trigger_label(trigger)
    _set_job_progress(
        kind,
        running=True,
        phase="starting",
        trigger=trigger,
        message=f"{label} · {name} başladı",
        step=0,
        total_steps=1,
    )
    _job_event(f"{label} tetik · {kind} · {name} başladı")
    try:
        try:
            result = runner()
            if not isinstance(result, dict):
                result = {"ok": False, "message": "boş sonuç"}
            if result.get("ok"):
                _note_auto_success(kind)
            else:
                _mark_failure_class(kind, result)
                if notify:
                    _notify_auto_failure(kind, result)
            _finish_job_progress(kind, result, trigger=trigger, name=name)
            return result
        except Exception as exc:
            traceback.print_exc()
            _mark_failure_class(kind, exc=exc)
            if notify:
                _notify_auto_failure(kind, exc=exc)
            err = {"ok": False, "message": str(exc)}
            _finish_job_progress(kind, err, trigger=trigger, name=name)
            return err
    finally:
        lock.release()


def _auto_job_registry() -> dict[str, dict[str, Any]]:
    return {
        "notification": {
            "name": "Notification",
            "lock": _nt_lock,
            "runner": run_notification_bridge_once,
        },
        "news": {"name": "News", "lock": _nt_lock, "runner": run_news_bridge_once},
        "virgul": {"name": "Virgul", "lock": _virgul_lock, "runner": run_virgul_bridge_once},
        "play": {"name": "Play", "lock": _play_lock, "runner": run_play_bridge_once},
        "asc": {"name": "ASC", "lock": _asc_lock, "runner": run_asc_bridge_once},
        "firebase": {"name": "Firebase", "lock": _firebase_lock, "runner": run_firebase_bridge_once},
        "gsc_links": {
            "name": "GSC Links",
            "lock": _gsc_links_lock,
            "runner": run_gsc_links_bridge_once,
        },
        "revenue_targets": {
            "name": "RevenueTargets",
            "lock": _revenue_targets_lock,
            "runner": run_revenue_targets_bridge_once,
        },
        "admanager_policy": {
            "name": "Policy",
            "lock": _policy_lock,
            "runner": run_admanager_policy_bridge_once,
        },
        "pagespeed": {
            "name": "PageSpeed",
            "lock": _pagespeed_lock,
            "runner": run_pagespeed_bridge_once,
        },
        "empower_intel": {
            "name": "EmpowerIntel",
            "lock": _empower_intel_lock,
            "runner": run_empower_intel_bridge_once,
        },
        "empower_intel_sinemalar": {
            "name": "EmpowerIntelSinemalar",
            "lock": _empower_intel_sinemalar_lock,
            "runner": run_empower_intel_sinemalar_bridge_once,
        },
        "seo_audit": {
            "name": "SEO Audit",
            "lock": _seo_audit_lock,
            "runner": run_seo_audit_bridge_once,
        },
        "gsc_cwv": {
            "name": "GSC CWV",
            "lock": _gsc_cwv_lock,
            "runner": run_gsc_cwv_bridge_once,
        },
        "market": {
            "name": "Piyasa",
            "lock": _market_lock,
            "runner": run_market_tarama_bridge_once,
        },
        "sinemalar_noads": {
            "name": "noAds",
            "lock": _noads_lock,
            "runner": run_sinemalar_noads_bridge_once,
        },
        "sinemalar_moderation": {
            "name": "Moderation",
            "lock": _moderation_lock,
            "runner": run_sinemalar_moderation_bridge_once,
        },
    }


def _process_due_retries() -> None:
    """Zamanı gelen yeniden denemeleri çalıştır; başarıda kuyruğu temizle."""
    global _last_nt_auto_at, _last_news_auto_at
    now = time.time()
    registry = _auto_job_registry()
    for kind, st in list(_job_retries.items()):
        next_at = float(st.get("next_at") or 0)
        if next_at > now:
            continue
        meta = registry.get(kind)
        if not meta:
            _clear_job_retry(kind)
            continue
        if _config_fail.get(kind):
            _clear_job_retry(kind)
            continue
        name = str(st.get("name") or meta["name"])
        attempt = int(st.get("attempt") or 1)
        retry_max = int(st.get("max") or BRIDGE_RETRY_MAX)
        retry_gap = int(st.get("gap") or BRIDGE_RETRY_GAP_SEC)
        # Çalışırken çift tetiklemeyi engelle
        st["next_at"] = now + 86400
        _job_retries[kind] = st
        print(
            f"Auto {name}: yeniden deneme çalışıyor {attempt}/{retry_max}",
            flush=True,
        )
        # Ara denemelerde mail spam olmasın; son denemede bildir
        is_last = attempt >= retry_max
        result = _run_locked_job(
            name=name,
            lock=meta["lock"],
            runner=meta["runner"],
            kind=kind,
            notify=is_last,
        )
        if result is None:
            # Kilit meşgul — 1 dk sonra tekrar dene (attempt sayacı artmaz)
            st["next_at"] = now + 60
            _job_retries[kind] = st
            continue
        if result.get("ok"):
            print(f"Auto {name}: retry #{attempt} başarılı — kalan denemeler iptal", flush=True)
            _clear_job_retry(kind)
            # Interval işlerde sonraki planlı tur bu andan sayılsın
            if kind == "notification":
                _last_nt_auto_at = time.time()
            elif kind == "news":
                _last_news_auto_at = time.time()
            continue
        if is_last:
            print(
                f"Auto {name}: {retry_max} yeniden deneme de başarısız — "
                "sonraki planlı slota bırakıldı",
                flush=True,
            )
            _clear_job_retry(kind)
            continue
        # Bir sonraki retry
        nxt = attempt + 1
        gap = max(60, retry_gap)
        _job_retries[kind] = {
            "attempt": nxt,
            "next_at": now + gap,
            "name": name,
            "max": retry_max,
            "gap": gap,
            "failed_slot": st.get("failed_slot") or "",
        }
        gap_label = f"{gap // 3600} sa" if gap >= 3600 else f"{gap // 60} dk"
        print(
            f"Auto {name}: retry #{attempt} başarısız → #{nxt}/{retry_max} · ~{gap_label} sonra",
            flush=True,
        )


def _auto_loop() -> None:
    """Slot + interval zamanlayıcı; poll ~60s. Hata → 3×10 dk retry, sonra sonraki slot."""
    global _auto_cycle
    global _last_nt_auto_at, _last_news_auto_at
    global _last_notification_night_seal_slot
    global _last_virgul_auto_slot, _last_play_auto_slot, _last_asc_auto_slot
    global _last_gsc_links_auto_slot, _last_policy_auto_slot
    global _last_noads_auto_slot, _last_moderation_auto_slot, _last_pagespeed_auto_slot, _last_seo_audit_auto_slot
    global _last_gsc_cwv_auto_slot, _last_market_auto_slot

    while True:
        _auto_cycle += 1
        _process_due_retries()
        _flush_deferred_browser_scrapes()

        # Notification + News: 08:00–20:00 her 30 dk (aynı admin kilidi)
        nt_due = _should_run_notification_auto() and "notification" not in _job_retries
        news_due = _should_run_news_auto() and "news" not in _job_retries
        # Gece: dünü kayda al (mühür)
        seal_due, seal_slot = _notification_night_seal_due()
        seal_due = seal_due and "notification_seal" not in _job_retries
        # İki Mac de açıkken aynı pencereyi iki kez çekmesin.
        # held → bu turu atla ve tekrarlama; unavailable → işaretleme, sonraki poll dener.
        if nt_due:
            state = _auto_lease_state("notification", _interval_lease_slot("nt", AUTO_INTERVAL_SEC))
            if state != LEASE_GRANTED:
                nt_due = False
                if state == LEASE_HELD:
                    _last_nt_auto_at = time.time()
        if news_due:
            state = _auto_lease_state("news", _interval_lease_slot("news", NEWS_AUTO_INTERVAL_SEC))
            if state != LEASE_GRANTED:
                news_due = False
                if state == LEASE_HELD:
                    _last_news_auto_at = time.time()
        if seal_due:
            state = _auto_lease_state("notification_seal", seal_slot)
            if state != LEASE_GRANTED:
                seal_due = False
                if state == LEASE_HELD:
                    _last_notification_night_seal_slot = seal_slot
        if nt_due or news_due or seal_due:
            if _nt_lock.acquire(blocking=False):
                try:
                    if seal_due:
                        try:
                            nt = run_notification_bridge_once(mode="seal_yesterday")
                            _last_notification_night_seal_slot = seal_slot
                            if nt.get("ok"):
                                _note_auto_success("notification_seal")
                                _clear_job_retry("notification_seal")
                            else:
                                _notify_auto_failure("notification_seal", nt)
                                _arm_job_retry("notification_seal", name="Notification night seal")
                        except Exception as exc:
                            traceback.print_exc()
                            _last_notification_night_seal_slot = seal_slot
                            _notify_auto_failure("notification_seal", exc=exc)
                            _arm_job_retry("notification_seal", name="Notification night seal")
                    if nt_due:
                        try:
                            nt = run_notification_bridge_once(mode="live")
                            _last_nt_auto_at = time.time()
                            if nt.get("ok"):
                                _note_auto_success("notification")
                                _clear_job_retry("notification")
                            else:
                                _notify_auto_failure("notification", nt)
                                _arm_job_retry("notification", name="Notification")
                        except Exception as exc:
                            traceback.print_exc()
                            _last_nt_auto_at = time.time()
                            _notify_auto_failure("notification", exc=exc)
                            _arm_job_retry("notification", name="Notification")
                    if news_due:
                        try:
                            news = run_news_bridge_once()
                            _last_news_auto_at = time.time()
                            if news.get("ok"):
                                _note_auto_success("news")
                                _clear_job_retry("news")
                            else:
                                _notify_auto_failure("news", news)
                                _arm_job_retry("news", name="News")
                        except Exception as exc:
                            traceback.print_exc()
                            _last_news_auto_at = time.time()
                            _notify_auto_failure("news", exc=exc)
                            _arm_job_retry("news", name="News")
                except Exception:
                    traceback.print_exc()
                finally:
                    _nt_lock.release()
            else:
                print("Auto notification/news atlandı (manuel sync sürüyor)", flush=True)

        def _slot_job(
            kind: str,
            name: str,
            lock: threading.Lock,
            runner,
            last_attr: str,
            hours: tuple[int, ...] | list[int],
            minute: int,
            *,
            browser: bool = True,
        ) -> None:
            nonlocal_last = globals()[last_attr]
            if kind in _job_retries:
                return
            due, slot = _slot_due(nonlocal_last, hours, minute)
            if not due:
                return
            lease = _auto_lease_state(kind, slot)
            if lease == LEASE_HELD:
                globals()[last_attr] = slot  # slot başka makinede koşuyor — burada tekrarlama
                return
            if lease == LEASE_UNAVAILABLE:
                return  # slotu işaretleme; bir sonraki poll'da yeniden sorulur
            _clear_job_retry(kind)

            def _mark_slot(result: dict[str, Any], *, _slot: str = slot) -> None:
                globals()[last_attr] = _slot
                if result.get("ok"):
                    _clear_job_retry(kind)
                else:
                    _notify_auto_failure(kind, result)
                    _arm_job_retry(kind, name=name, failed_slot=_slot)

            if browser and _is_browser_scrape_kind(kind):
                result = _run_browser_scrape_job(
                    kind=kind,
                    name=name,
                    lock=lock,
                    runner=runner,
                    on_done=_mark_slot,
                    notify=False,
                )
                if result is None:
                    return
            else:
                result = _run_locked_job(
                    name=name, lock=lock, runner=runner, kind=kind, notify=False
                )
                if result is None:
                    return
                globals()[last_attr] = slot
                if result.get("ok"):
                    _clear_job_retry(kind)
                else:
                    _notify_auto_failure(kind, result)
                    _arm_job_retry(kind, name=name)

        _slot_job(
            "virgul", "Virgul", _virgul_lock, run_virgul_bridge_once,
            "_last_virgul_auto_slot", VIRGUL_SLOT_HOURS, VIRGUL_SLOT_MINUTE,
            browser=False,
        )
        _slot_job(
            "play", "Play", _play_lock, run_play_bridge_once,
            "_last_play_auto_slot", PLAY_SLOT_HOURS, PLAY_SLOT_MINUTE,
        )
        _slot_job(
            "asc", "ASC", _asc_lock, run_asc_bridge_once,
            "_last_asc_auto_slot", ASC_SLOT_HOURS, ASC_SLOT_MINUTE,
        )
        _slot_job(
            "firebase", "Firebase", _firebase_lock, run_firebase_bridge_once,
            "_last_firebase_auto_slot", FIREBASE_SLOT_HOURS, FIREBASE_SLOT_MINUTE,
        )
        _slot_job(
            "gsc_links", "GSC Links", _gsc_links_lock, run_gsc_links_bridge_once,
            "_last_gsc_links_auto_slot", GSC_LINKS_SLOT_HOURS, GSC_SLOT_MINUTE,
        )
        _slot_job(
            "revenue_targets",
            "RevenueTargets",
            _revenue_targets_lock,
            run_revenue_targets_bridge_once,
            "_last_revenue_targets_auto_slot",
            REVENUE_TARGETS_SLOT_HOURS,
            REVENUE_TARGETS_SLOT_MINUTE,
        )
        _slot_job(
            "admanager_policy", "Policy", _policy_lock, run_admanager_policy_bridge_once,
            "_last_policy_auto_slot", TWICE_DAILY_HOURS, POLICY_SLOT_MINUTE,
        )
        _slot_job(
            "pagespeed", "PageSpeed", _pagespeed_lock, run_pagespeed_bridge_once,
            "_last_pagespeed_auto_slot", TWICE_DAILY_HOURS, SPEED_SLOT_MINUTE,
        )
        _slot_job(
            "sinemalar_noads", "noAds", _noads_lock, run_sinemalar_noads_bridge_once,
            "_last_noads_auto_slot", TWICE_DAILY_HOURS, NOADS_SLOT_MINUTE,
        )
        if "sinemalar_moderation" not in _job_retries:
            mod_due, mod_slot, mod_which = _moderation_slot_due()
            if mod_due:
                state = _auto_lease_state("sinemalar_moderation", mod_slot)
                if state != LEASE_GRANTED:
                    mod_due = False
                    if state == LEASE_HELD:
                        _last_moderation_auto_slot = mod_slot
            if mod_due:

                def _mark_mod_slot(result: dict[str, Any], *, _slot: str = mod_slot) -> None:
                    global _last_moderation_auto_slot
                    _last_moderation_auto_slot = _slot
                    if result.get("ok"):
                        _clear_job_retry("sinemalar_moderation")
                    else:
                        _notify_auto_failure("sinemalar_moderation", result)
                        _arm_job_retry("sinemalar_moderation", name="Moderation")

                def _mod_auto_runner() -> dict[str, Any]:
                    return run_sinemalar_moderation_bridge_once(incremental_which=mod_which)

                _run_browser_scrape_job(
                    kind="sinemalar_moderation",
                    name="Moderation",
                    lock=_moderation_lock,
                    runner=_mod_auto_runner,
                    on_done=_mark_mod_slot,
                    notify=False,
                )
        _slot_job(
            "seo_audit", "SEO Audit", _seo_audit_lock, run_seo_audit_bridge_once,
            "_last_seo_audit_auto_slot", SEO_AUDIT_SLOT_HOURS, SEO_AUDIT_SLOT_MINUTE,
        )
        _slot_job(
            "gsc_cwv", "GSC CWV", _gsc_cwv_lock, run_gsc_cwv_bridge_once,
            "_last_gsc_cwv_auto_slot", GSC_CWV_SLOT_HOURS, GSC_CWV_SLOT_MINUTE,
        )
        _slot_job(
            "market", "Piyasa", _market_lock, run_market_tarama_bridge_once,
            "_last_market_auto_slot", MARKET_SLOT_HOURS, MARKET_SLOT_MINUTE,
        )
        if "empower_intel" not in _job_retries:
            emp_due, emp_slot = _multi_slot_due(_last_empower_intel_auto_slot, EMPOWER_INTEL_SLOTS)
            if emp_due:

                def _mark_emp_slot(result: dict[str, Any], *, _slot: str = emp_slot) -> None:
                    global _last_empower_intel_auto_slot
                    _last_empower_intel_auto_slot = _slot
                    if result.get("ok"):
                        _clear_job_retry("empower_intel")
                    else:
                        _notify_auto_failure("empower_intel", result)
                        _arm_job_retry("empower_intel", name="EmpowerIntel")

                _run_browser_scrape_job(
                    kind="empower_intel",
                    name="EmpowerIntel",
                    lock=_empower_intel_lock,
                    runner=run_empower_intel_bridge_once,
                    on_done=_mark_emp_slot,
                    notify=False,
                )

        if "empower_intel_sinemalar" not in _job_retries:
            emp_sin_due, emp_sin_slot = _multi_slot_due(
                _last_empower_intel_sinemalar_auto_slot, EMPOWER_INTEL_SINEMALAR_SLOTS
            )
            if emp_sin_due:

                def _mark_emp_sin_slot(result: dict[str, Any], *, _slot: str = emp_sin_slot) -> None:
                    global _last_empower_intel_sinemalar_auto_slot
                    _last_empower_intel_sinemalar_auto_slot = _slot
                    if result.get("ok"):
                        _clear_job_retry("empower_intel_sinemalar")
                    else:
                        _notify_auto_failure("empower_intel_sinemalar", result)
                        _arm_job_retry("empower_intel_sinemalar", name="EmpowerIntelSinemalar")

                _run_browser_scrape_job(
                    kind="empower_intel_sinemalar",
                    name="EmpowerIntelSinemalar",
                    lock=_empower_intel_sinemalar_lock,
                    runner=run_empower_intel_sinemalar_bridge_once,
                    on_done=_mark_emp_sin_slot,
                    notify=False,
                )

        time.sleep(max(30, AUTO_POLL_SEC))


def _page_tarama_api_base() -> str:
    return (
        os.environ.get("PAGE_TARAMA_API_BASE")
        or os.environ.get("SEO_AUDIT_API_BASE")
        or "https://projectcontrol.up.railway.app"
    ).rstrip("/")


def _page_tarama_auth_headers() -> dict[str, str]:
    token = _ingest_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


# ── Worker kimliği + kabiliyet ────────────────────────────────────────────────
# Ofis ve ev Mac'i aynı kuyruğa bağlanır. Railway hangi makinenin hangi işi
# gerçekten yapabileceğini bilmezse iş yapamayacak makineye düşer ve hata döner.

_worker_name_cache: str = ""
_readiness_cache: tuple[float, dict[str, str]] = (0.0, {})
_playwright_cache: tuple[float, bool] = (0.0, False)
_active_job_ids: set[str] = set()
_active_job_lock = threading.Lock()

# Playwright Firefox şart olan işler (Firebase sistem Firefox'a düşebildiği için hariç)
PLAYWRIGHT_JOB_IDS = frozenset(
    {"asc", "cwv", "links", "play", "play_vitals", "policy", "moderation", "noads", "pagespeed"}
)

# page-tarama job id → bridge kind (needs_login sınıflandırması için)
JOB_ID_KIND_ALIASES = {
    "cwv": "gsc_cwv",
    "links": "gsc_links",
    "policy": "admanager_policy",
    "moderation": "sinemalar_moderation",
    "noads": "sinemalar_noads",
}


def _machine_fingerprint() -> str:
    """Donanım UUID'sinden kısa, makineye özgü sonek — iki Mac'in adı çakışmasın."""
    raw = ""
    try:
        out = subprocess.run(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        hit = re.search(r'"IOPlatformUUID"\s*=\s*"([^"]+)"', out.stdout or "")
        raw = hit.group(1) if hit else ""
    except Exception:  # noqa: BLE001
        raw = ""
    if not raw:
        try:
            import socket

            raw = socket.gethostname()
        except Exception:  # noqa: BLE001
            raw = "mac"
    return hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()[:4]


def _worker_name() -> str:
    """Bu makinenin kuyrukta görünen adı — panelde 'Running on: …' olarak çıkar.

    BRIDGE_WORKER_NAME verilirse (önerilen: cem-office-mac / cem-home-mac) o kullanılır.
    Verilmezse bilgisayar adı + donanım soneki: iki Mac'in adı aynı olsa bile ayrışır.
    """
    global _worker_name_cache
    if _worker_name_cache:
        return _worker_name_cache
    raw = (os.environ.get("BRIDGE_WORKER_NAME") or "").strip()
    explicit = bool(raw)
    if not raw:
        try:
            out = subprocess.run(
                ["scutil", "--get", "ComputerName"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            raw = (out.stdout or "").strip()
        except Exception:  # noqa: BLE001
            raw = ""
    if not raw:
        try:
            import socket

            raw = socket.gethostname().split(".")[0]
        except Exception:  # noqa: BLE001
            raw = "mac"
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")[:44] or "mac"
    _worker_name_cache = slug if explicit else f"{slug}-{_machine_fingerprint()}"
    return _worker_name_cache


def _playwright_firefox_ready() -> bool:
    """Kurulu playwright'ın beklediği Firefox revizyonu gerçekten var mı."""
    global _playwright_cache
    ts, val = _playwright_cache
    now = time.time()
    if now - ts < 600:
        return val
    ok = False
    try:
        import playwright  # noqa: PLC0415

        meta = Path(playwright.__file__).resolve().parent / "driver" / "package" / "browsers.json"
        rev = ""
        data = json.loads(meta.read_text(encoding="utf-8"))
        for entry in data.get("browsers") or []:
            if entry.get("name") == "firefox":
                rev = str(entry.get("revision") or "")
                break
        if rev:
            base = Path.home() / "Library" / "Caches" / "ms-playwright" / f"firefox-{rev}"
            ok = (base / "firefox" / "Nightly.app").exists() or (base / "firefox").exists()
    except Exception:  # noqa: BLE001
        ok = False
    _playwright_cache = (now, ok)
    return ok


# Oturum çerezi tarama profilinde duruyor mu — iş dağıtılmadan önce bakılır ki
# oturumu olmayan Mac boşuna deneyip hata döndürmesin.
SESSION_JOB_PROFILES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    # job id → (profil, çerez host filtresi, oturum çerezi adları)
    "moderation": ("sinemalar", "sinemalar.com", ("PHPSESSID",)),
    "noads": ("sinemalar", "sinemalar.com", ("PHPSESSID",)),
    "asc": ("asc", "apple.com", ("myacinfo",)),
}
# Google oturumu (fx-google): Play/CWV/Backlinks/Policy/Firebase aynı profili kullanır
GOOGLE_SESSION_JOB_IDS = frozenset({"play", "play_vitals", "cwv", "links", "policy", "firebase"})


def _job_session_ok(job_id: str) -> bool | None:
    """True/False kesin, None = bilinmiyor (engelleme)."""
    try:
        from backend.services.scrape_browser import (
            asc_profile_dir,
            google_profile_dir,
            sinemalar_profile_dir,
        )
        from backend.services.system_firefox_driver import (
            google_profile_has_session,
            profile_has_session_cookie,
        )

        if job_id in GOOGLE_SESSION_JOB_IDS:
            return bool(google_profile_has_session(google_profile_dir()))
        spec = SESSION_JOB_PROFILES.get(job_id)
        if not spec:
            return None
        profile_key, host_like, names = spec
        profile = {"sinemalar": sinemalar_profile_dir, "asc": asc_profile_dir}[profile_key]()
        return profile_has_session_cookie(profile, host_like, names)
    except Exception:  # noqa: BLE001
        return None


def _worker_readiness() -> dict[str, str]:
    """İş bazlı hazırlık: ready / no_creds / no_browser / login_required.

    Oturum kontrolü tarama profilindeki çerezlere bakar; okunamazsa iş engellenmez
    (bilinmiyor → ready). Süresi dolmuş ama duran oturumları needs_login devri yakalar.
    """
    global _readiness_cache
    ts, cached = _readiness_cache
    now = time.time()
    if now - ts < 60 and cached:
        return cached
    doviz = bool(os.environ.get("DOVIZ_ADMIN_EMAIL") and os.environ.get("DOVIZ_ADMIN_PASSWORD"))
    virgul = bool(os.environ.get("VIRGUL_EMAIL") and os.environ.get("VIRGUL_PASSWORD"))
    pw_ok = _playwright_firefox_ready()
    out: dict[str, str] = {}
    for jid in _remote_claim_job_registry():
        state = "ready"
        if jid in ("notification", "news") and not doviz:
            state = "no_creds"
        elif jid in ("virgul", "revenue_targets") and not virgul:
            state = "no_creds"
        elif jid in PLAYWRIGHT_JOB_IDS and not pw_ok:
            state = "no_browser"
        elif _job_session_ok(jid) is False:
            state = "login_required"
        out[jid] = state
    _readiness_cache = (now, out)
    return out


def _ready_param() -> str:
    return ",".join(f"{jid}:{state}" for jid, state in sorted(_worker_readiness().items()))


def _active_jobs() -> list[str]:
    with _active_job_lock:
        return sorted(_active_job_ids)


def _worker_ping_payload() -> dict[str, Any]:
    return {
        "worker": _worker_name(),
        "ready": _worker_readiness(),
        "current": _active_jobs(),
        "version": BRIDGE_VERSION,
    }


LEASE_GRANTED = "granted"  # bu makine koşsun
LEASE_HELD = "held"  # diğer Mac aldı → slot burada tekrarlanmasın
LEASE_UNAVAILABLE = "unavailable"  # sorulamadı → slotu işaretleme, sonraki poll'da yeniden dene


def _auto_lease_state(kind: str, slot: str) -> str:
    """Zamanlı taramayı bu makine mi koşsun (granted / held / unavailable).

    İki Mac de açıkken aynı slot iki kez koşmasın diye Railway'den kira alınır.
    Uç yoksa veya yetki hatası varsa (deploy penceresi / eski sürüm) eski davranışa
    dönülür — kira yüzünden zamanlı taramaların tamamen durması daha kötüdür.
    """
    if not _ingest_token():
        return LEASE_GRANTED
    url = _page_tarama_api_base() + "/api/page-tarama/auto-lease"
    try:
        resp = requests.post(
            url,
            headers=_page_tarama_auth_headers(),
            json={"job": kind, "slot": str(slot), "worker": _worker_name()},
            timeout=20,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Auto kira sorulamadı ({kind}/{slot}): {exc}", flush=True)
        return LEASE_UNAVAILABLE
    if resp.status_code in (401, 403, 404):
        print(
            f"Auto kira ucu yok/yetkisiz HTTP {resp.status_code} ({kind}) — kirasız çalışılıyor",
            flush=True,
        )
        return LEASE_GRANTED
    if resp.status_code >= 400:
        print(f"Auto kira HTTP {resp.status_code} ({kind}/{slot})", flush=True)
        return LEASE_UNAVAILABLE
    try:
        data = resp.json() or {}
    except Exception:  # noqa: BLE001
        return LEASE_UNAVAILABLE
    if data.get("granted"):
        return LEASE_GRANTED
    print(
        f"Auto {kind} atlandı — {slot} slotunu {data.get('holder') or 'başka makine'} aldı",
        flush=True,
    )
    return LEASE_HELD


def _interval_lease_slot(prefix: str, seconds: int) -> str:
    return f"{prefix}-{int(time.time() // max(60, int(seconds)))}"


def _remote_claim_job_registry() -> dict[str, dict[str, Any]]:
    return {
        "play": {"name": "Play", "lock": _play_lock, "runner": run_play_bridge_once},
        "asc": {"name": "ASC", "lock": _asc_lock, "runner": run_asc_bridge_once},
        "firebase": {"name": "Firebase", "lock": _firebase_lock, "runner": run_firebase_bridge_once},
        "cwv": {"name": "GSC CWV", "lock": _gsc_cwv_lock, "runner": run_gsc_cwv_bridge_once},
        "notification": {"name": "Notification", "lock": _nt_lock, "runner": run_notification_bridge_once},
        "news": {"name": "News", "lock": _nt_lock, "runner": lambda: run_news_bridge_once(days=7)},
        "virgul": {"name": "Virgul", "lock": _virgul_lock, "runner": run_virgul_bridge_once},
        "revenue_targets": {
            "name": "Virgul Targets",
            "lock": _revenue_targets_lock,
            "runner": run_revenue_targets_bridge_once,
        },
        "market": {"name": "Piyasa", "lock": _market_lock, "runner": run_market_tarama_bridge_once},
        "empower_intel": {
            "name": "EmpowerIntel",
            "lock": _empower_intel_lock,
            "runner": run_empower_intel_bridge_once,
        },
        "empower_intel_sinemalar": {
            "name": "EmpowerIntelSinemalar",
            "lock": _empower_intel_sinemalar_lock,
            "runner": run_empower_intel_sinemalar_bridge_once,
        },
        "links": {"name": "GSC Links", "lock": _gsc_links_lock, "runner": run_gsc_links_bridge_once},
        "policy": {"name": "Policy", "lock": _policy_lock, "runner": run_admanager_policy_bridge_once},
        "noads": {"name": "noAds", "lock": _noads_lock, "runner": run_sinemalar_noads_bridge_once},
        "moderation": {
            "name": "Moderation",
            "lock": _moderation_lock,
            "runner": lambda: run_sinemalar_moderation_bridge_once(incremental_which="both"),
        },
        "pagespeed": {
            "name": "PageSpeed",
            "lock": _pagespeed_lock,
            "runner": run_pagespeed_bridge_once,
        },
        "seo": {"name": "SEO Audit", "lock": _seo_audit_lock, "runner": run_seo_audit_bridge_once},
    }


def _post_page_tarama_result(
    payload: dict[str, Any],
    *,
    timeout: float = 45,
    retries: int = 3,
) -> None:
    url = _page_tarama_api_base() + "/api/page-tarama/result"
    last_exc: Exception | None = None
    attempts = max(1, int(retries))
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                url,
                headers=_page_tarama_auth_headers(),
                json=payload,
                timeout=timeout,
            )
            if resp.status_code < 400:
                return
            print(
                f"page-tarama result HTTP {resp.status_code} (try {attempt}/{attempts})",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"page-tarama result hata (try {attempt}/{attempts}): {exc}", flush=True)
        time.sleep(1.5 * attempt)
    if last_exc:
        print(f"page-tarama result vazgeçildi: {last_exc}", flush=True)


_pt_progress_lock = threading.Lock()
_pt_progress_latest: dict[str, Any] | None = None
_pt_progress_wake = threading.Event()
_pt_progress_worker_started = False


def _pt_progress_worker() -> None:
    """Progress POST'ları scrape thread'ini bloklamasın; en son durumu coalesce et."""
    global _pt_progress_latest
    while True:
        _pt_progress_wake.wait(timeout=3.0)
        with _pt_progress_lock:
            payload = _pt_progress_latest
            _pt_progress_latest = None
            _pt_progress_wake.clear()
        if not payload:
            continue
        # Progress: kısa retry; final result senkron kalır
        _post_page_tarama_result(payload, timeout=45, retries=2)


def _ensure_pt_progress_worker() -> None:
    global _pt_progress_worker_started
    with _pt_progress_lock:
        if _pt_progress_worker_started:
            return
        _pt_progress_worker_started = True
        threading.Thread(
            target=_pt_progress_worker, name="page-tarama-progress", daemon=True
        ).start()


def _post_page_tarama_progress_async(payload: dict[str, Any]) -> None:
    """running=True kalp atışları — kuyruğa at, scrape devam etsin."""
    _ensure_pt_progress_worker()
    with _pt_progress_lock:
        global _pt_progress_latest
        _pt_progress_latest = payload
        _pt_progress_wake.set()


def _keepalive_claim_touch(claim_url: str, headers: dict[str, str]) -> None:
    """Ping yoksa claim ile canlılık ver — ama kapılan işi düşürme, kuyruğa iade et."""
    try:
        resp = requests.get(
            claim_url,
            headers=headers,
            params={"worker": _worker_name(), "ready": _ready_param()},
            timeout=45,
        )
        job = (resp.json() or {}).get("job") if resp.status_code == 200 else None
    except Exception as exc:  # noqa: BLE001
        print(f"page-tarama keepalive claim: {exc}", flush=True)
        return
    if not job:
        return
    try:
        requests.post(
            _page_tarama_api_base() + "/api/page-tarama/requeue",
            headers=headers,
            json={
                "run_id": job.get("run_id"),
                "job_id": job.get("job_id"),
                "message": "Keepalive touch · back in queue",
            },
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"page-tarama keepalive requeue: {exc}", flush=True)


def _page_tarama_keepalive_loop() -> None:
    """Uzun tarama sırasında claim thread bloklansa bile Railway'e canlılık sinyali.

    Önce bridge-ping; 404/401 olursa claim GET (zaten public) ile touch eder.
    """
    ping_url = _page_tarama_api_base() + "/api/page-tarama/bridge-ping"
    claim_url = _page_tarama_api_base() + "/api/page-tarama/claim"
    print(
        f"Uzaktan tarama keepalive: {ping_url} · worker={_worker_name()}",
        flush=True,
    )
    use_ping = True
    while True:
        try:
            if _ingest_token():
                headers = _page_tarama_auth_headers()
                if use_ping:
                    resp = requests.post(
                        ping_url, headers=headers, json=_worker_ping_payload(), timeout=45
                    )
                    if resp.status_code in (401, 403, 404):
                        use_ping = False
                        print(
                            f"page-tarama keepalive: ping HTTP {resp.status_code} → claim fallback",
                            flush=True,
                        )
                        _keepalive_claim_touch(claim_url, headers)
                else:
                    _keepalive_claim_touch(claim_url, headers)
        except Exception as exc:  # noqa: BLE001
            print(f"page-tarama keepalive: {exc}", flush=True)
        time.sleep(20)


def _page_tarama_claim_loop() -> None:
    """Mobil/Railway «Sayfayı güncelle» kuyruğunu Mac’te çalıştır.

    SEO + Virgül gibi farklı işler paralel; aynı job_id kilitleri ayrıca korur.
    """
    url = _page_tarama_api_base() + "/api/page-tarama/claim"
    registry = _remote_claim_job_registry()
    # Railway MAX_INFLIGHT_JOBS ile uyumlu
    worker_slots = threading.Semaphore(3)
    print(f"Uzaktan tarama kuyruğu: {url}", flush=True)

    def _run_claimed_job(job: dict[str, Any]) -> None:
        run_id = ""
        job_id = ""
        final_posted = False
        # finally bloğu erken hatada da geri alabilsin diye burada tanımlı
        prev_login_wait = os.environ.get("SCRAPE_LOGIN_WAIT_SEC")
        try:
            job_id = str(job.get("job_id") or "")
            meta = registry.get(job_id)
            if not meta:
                _post_page_tarama_result(
                    {
                        "run_id": job.get("run_id"),
                        "job_id": job_id,
                        "ok": False,
                        "worker": _worker_name(),
                        "message": "Unknown job",
                    }
                )
                return
            with _active_job_lock:
                _active_job_ids.add(job_id)
            print(
                f"Uzaktan tarama başladı: {meta['name']}"
                + (f" · page={job.get('page')}" if job.get("page") else ""),
                flush=True,
            )
            run_id = str(job.get("run_id") or "")
            started_mono = time.time()
            page_key = str(job.get("page") or "")
            # Kullanıcı bu Mac'in başında ve oturum yok: gözetimsiz 150 sn yerine
            # 15 dk beklenir ki açılan pencerede girişi yapabilsin.
            login_ok = bool(job.get("login_ok"))
            if login_ok:
                os.environ["SCRAPE_LOGIN_WAIT_SEC"] = str(
                    os.environ.get("PANEL_LOGIN_WAIT_SEC") or "900"
                )
                print(
                    f"Uzaktan {meta['name']}: oturum yok — bu Mac'te giriş penceresi açılacak "
                    f"(en fazla {int(os.environ['SCRAPE_LOGIN_WAIT_SEC']) // 60} dk)",
                    flush=True,
                )

            def _progress_post(info: dict[str, Any] | None = None) -> None:
                info = info if isinstance(info, dict) else {}
                elapsed = int(time.time() - started_mono)
                msg = str(
                    info.get("message")
                    or info.get("sub_label")
                    or f"Mac scan running · {elapsed}s"
                )[:200]
                payload = {
                    "run_id": run_id,
                    "job_id": job_id,
                    "running": True,
                    "worker": _worker_name(),
                    "message": msg,
                    "phase": str(info.get("phase") or "running")[:80],
                    "platform": str(info.get("platform") or "")[:40],
                    "sub_label": str(info.get("sub_label") or "")[:160],
                }
                if info.get("step") is not None:
                    try:
                        payload["step"] = int(info["step"])
                    except (TypeError, ValueError):
                        pass
                if info.get("total_steps") is not None:
                    try:
                        payload["total_steps"] = int(info["total_steps"])
                    except (TypeError, ValueError):
                        pass
                _post_page_tarama_progress_async(payload)

            _progress_post(
                {
                    "message": (
                        f"Bu Mac'te giriş bekleniyor — açılan pencereden {meta['name']} girişini yap"
                        if login_ok
                        else "Mac scan claimed · starting"
                    ),
                    "phase": "login" if login_ok else "claimed",
                    "step": 0,
                }
            )
            _set_job_progress(
                job_id,
                running=True,
                phase="starting",
                trigger="page-tarama",
                message=f"Panel · {meta['name']} başladı",
                step=0,
            )
            _job_event(f"Panel tetik · {job_id} · {meta['name']} · page={page_key or '—'}")

            stop_hb = threading.Event()

            def _heartbeat() -> None:
                while not stop_hb.wait(5.0):
                    if job_id == "firebase" and _firebase_progress.get("running"):
                        _progress_post(dict(_firebase_progress))
                    elif job_id == "news" and _news_progress.get("running"):
                        _progress_post(
                            {
                                "phase": _news_progress.get("phase") or "scrape",
                                "step": _news_progress.get("page") or _news_progress.get("step") or 0,
                                "total_steps": _news_progress.get("total_pages")
                                or _news_progress.get("total_steps")
                                or 0,
                                "message": _news_progress.get("message") or "News scrape",
                            }
                        )
                    elif job_id == "notification" and _nt_progress.get("running"):
                        _progress_post(
                            {
                                "phase": _nt_progress.get("phase") or "fetch",
                                "step": _nt_progress.get("step") or 0,
                                "total_steps": _nt_progress.get("total_steps") or 0,
                                "message": _nt_progress.get("message") or "Notification scrape",
                            }
                        )
                    elif job_id == "cwv" and _gsc_cwv_progress.get("running"):
                        _progress_post(
                            {
                                "phase": _gsc_cwv_progress.get("phase") or "scrape",
                                "step": _gsc_cwv_progress.get("step") or 0,
                                "total_steps": _gsc_cwv_progress.get("total_steps") or 8,
                                "message": _gsc_cwv_progress.get("message") or "GSC CWV scrape",
                            }
                        )
                    else:
                        _progress_post(
                            {
                                "phase": "running",
                                "message": f"{meta['name']} · Mac scan · {int(time.time() - started_mono)}s elapsed",
                            }
                        )

            hb = threading.Thread(target=_heartbeat, name=f"pt-hb-{job_id}", daemon=True)
            hb.start()

            def _runner():
                if job_id == "firebase":
                    plats = _firebase_platforms_for_page(page_key)
                    return run_firebase_bridge_once(
                        on_progress=_progress_post,
                        platforms=plats,
                    )
                if job_id == "virgul":
                    return run_virgul_bridge_once(on_progress=_progress_post)
                return meta["runner"]()

            result: dict[str, Any] | None = None
            try:
                # Tarayıcı kilidi meşgulse waiting_lock spam yerine kuyruğa geri koy
                if meta["lock"] is _browser_scrape_lock and not meta["lock"].acquire(
                    blocking=False
                ):
                    print(
                        f"Uzaktan {meta['name']}: tarayıcı kilidi meşgul → kuyruğa iade",
                        flush=True,
                    )
                    try:
                        requests.post(
                            _page_tarama_api_base() + "/api/page-tarama/requeue",
                            headers=_page_tarama_auth_headers(),
                            json={
                                "run_id": run_id,
                                "job_id": job_id,
                                "message": "Waiting in queue · previous browser scan still running",
                            },
                            timeout=30,
                        )
                    except Exception as exc:  # noqa: BLE001
                        print(f"page-tarama requeue: {exc}", flush=True)
                    _finish_job_progress(
                        job_id,
                        {
                            "ok": False,
                            "message": "Requeued · browser lock busy",
                        },
                        trigger="page-tarama",
                        name=str(meta.get("name") or job_id),
                    )
                    final_posted = True
                    return

                held_browser = meta["lock"] is _browser_scrape_lock
                try:
                    while result is None:
                        if held_browser:
                            # Kilidi zaten aldık — runner'ı doğrudan çalıştır
                            try:
                                result = _runner()
                                if isinstance(result, dict) and result.get("ok"):
                                    _note_auto_success(job_id)
                            except Exception as exc:  # noqa: BLE001
                                traceback.print_exc()
                                result = {"ok": False, "message": str(exc)}
                            break
                        result = _run_locked_job(
                            name=meta["name"],
                            lock=meta["lock"],
                            runner=_runner,
                            kind=job_id,
                            notify=False,
                        )
                        if result is None:
                            try:
                                requests.post(
                                    _page_tarama_api_base() + "/api/page-tarama/requeue",
                                    headers=_page_tarama_auth_headers(),
                                    json={
                                        "run_id": run_id,
                                        "job_id": job_id,
                                        "message": "Waiting in queue · previous scan still running",
                                    },
                                    timeout=30,
                                )
                            except Exception as exc:  # noqa: BLE001
                                print(f"page-tarama requeue: {exc}", flush=True)
                            final_posted = True
                            return
                finally:
                    if held_browser:
                        try:
                            meta["lock"].release()
                        except Exception:
                            pass
            finally:
                stop_hb.set()

            if not isinstance(result, dict):
                result = {"ok": False, "message": "Mac scan returned empty result"}
            _finish_job_progress(
                job_id,
                result,
                trigger="page-tarama",
                name=str(meta.get("name") or job_id),
            )
            final_msg = str(
                result.get("message") or ("Done" if result.get("ok") else "Error")
            )[:180]
            needs_login = False
            if not result.get("ok"):
                needs_login = _result_needs_login(
                    JOB_ID_KIND_ALIASES.get(job_id, job_id), result, final_msg
                )
            _post_page_tarama_result(
                {
                    "run_id": run_id,
                    "job_id": job_id,
                    "ok": bool(result.get("ok")),
                    "worker": _worker_name(),
                    "needs_login": bool(needs_login),
                    "message": final_msg,
                }
            )
            final_posted = True
        except Exception as exc:
            traceback.print_exc()
            if run_id and job_id and not final_posted:
                try:
                    _post_page_tarama_result(
                        {
                            "run_id": run_id,
                            "job_id": job_id,
                            "ok": False,
                            "worker": _worker_name(),
                            "message": f"Mac error: {exc}"[:180],
                        }
                    )
                except Exception:
                    pass
        finally:
            if job_id:
                with _active_job_lock:
                    _active_job_ids.discard(job_id)
            try:
                if prev_login_wait is None:
                    os.environ.pop("SCRAPE_LOGIN_WAIT_SEC", None)
                else:
                    os.environ["SCRAPE_LOGIN_WAIT_SEC"] = prev_login_wait
            except Exception:  # noqa: BLE001
                pass
            # Giriş yapılmış olabilir — kabiliyet raporu tazelensin
            global _readiness_cache
            _readiness_cache = (0.0, {})
            worker_slots.release()

    while True:
        try:
            if not _ingest_token():
                time.sleep(12)
                continue
            if not worker_slots.acquire(blocking=False):
                time.sleep(2)
                continue
            resp = requests.get(
                url,
                headers=_page_tarama_auth_headers(),
                params={"worker": _worker_name(), "ready": _ready_param()},
                timeout=20,
            )
            if resp.status_code != 200:
                worker_slots.release()
                time.sleep(5)
                continue
            job = (resp.json() or {}).get("job")
            if not job:
                worker_slots.release()
                time.sleep(3)
                continue
            threading.Thread(
                target=_run_claimed_job,
                args=(job,),
                name=f"pt-job-{job.get('job_id') or 'x'}",
                daemon=True,
            ).start()
        except Exception:
            traceback.print_exc()
            try:
                worker_slots.release()
            except Exception:
                pass
            time.sleep(5)


def _fail_page_tarama_orphans_on_boot() -> None:
    """Restart sonrası Railway'de kalan claimed/running işleri kapat — UI %94 zombie olmasın."""
    url = _page_tarama_api_base() + "/api/page-tarama/fail-inflight"
    try:
        if not _ingest_token():
            return
        resp = requests.post(url, headers=_page_tarama_auth_headers(), timeout=30)
        if resp.status_code < 400:
            body = resp.json() if resp.content else {}
            n = int((body or {}).get("failed") or 0)
            print(f"page-tarama orphan temizliği: {n} iş kapatıldı", flush=True)
        else:
            print(f"page-tarama orphan temizliği HTTP {resp.status_code}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"page-tarama orphan temizliği atlandı: {exc}", flush=True)


def run_daemon() -> int:
    _load_dotenv()
    try:
        from backend.services.store_session_cdp import start_keeper_threads

        start_keeper_threads()
        print("Tarama tarayıcısı: Firefox (Chrome/Chromium açılmaz)", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"Oturum bekçi başlatılamadı: {exc}", flush=True)
    _fail_page_tarama_orphans_on_boot()
    print(
        f"Worker: {_worker_name()} · auto_jobs={'on' if BRIDGE_AUTO_JOBS else 'off'} "
        f"· login_wait={os.environ.get('SCRAPE_LOGIN_WAIT_SEC')}s "
        f"· playwright_firefox={'ok' if _playwright_firefox_ready() else 'YOK'}",
        flush=True,
    )
    not_ready = {k: v for k, v in _worker_readiness().items() if v != "ready"}
    if not_ready:
        print(f"Bu makinede hazır olmayan işler: {not_ready}", flush=True)
    if BRIDGE_AUTO_JOBS:
        threading.Thread(target=_auto_loop, name="nt-bridge-auto", daemon=True).start()
    else:
        print("Otomatik zamanlı taramalar kapalı (BRIDGE_AUTO_JOBS=0)", flush=True)
    threading.Thread(target=_page_tarama_claim_loop, name="page-tarama-claim", daemon=True).start()
    threading.Thread(target=_page_tarama_keepalive_loop, name="page-tarama-keepalive", daemon=True).start()
    server = ThreadingHTTPServer((BRIDGE_HOST, BRIDGE_PORT), _BridgeHandler)
    print(
        f"Bridge daemon dinliyor http://{BRIDGE_HOST}:{BRIDGE_PORT} "
        f"notify+news={AUTO_INTERVAL_SEC}s@{NT_NEWS_ACTIVE_START_HOUR:02d}-{NT_NEWS_ACTIVE_END_HOUR:02d} "
        f"nt_night_seal={NOTIFICATION_NIGHT_SEAL_HOUR:02d}:{NOTIFICATION_NIGHT_SEAL_MINUTE:02d} "
        f"virgul={list(VIRGUL_SLOT_HOURS)}:{VIRGUL_SLOT_MINUTE:02d} play={list(PLAY_SLOT_HOURS)}:{PLAY_SLOT_MINUTE:02d} "
        f"asc={list(ASC_SLOT_HOURS)}:{ASC_SLOT_MINUTE:02d} firebase=:{FIREBASE_SLOT_MINUTE:02d} twice@01/13 gsc=:{GSC_SLOT_MINUTE:02d} "
        f"policy=:{POLICY_SLOT_MINUTE:02d} speed=:{SPEED_SLOT_MINUTE:02d} noads=:{NOADS_SLOT_MINUTE:02d} "
        f"moderation=03:04,14:17 "
        f"seo={list(SEO_AUDIT_SLOT_HOURS)}:{SEO_AUDIT_SLOT_MINUTE:02d} "
        f"cwv={list(GSC_CWV_SLOT_HOURS)}:{GSC_CWV_SLOT_MINUTE:02d} "
        f"market={list(MARKET_SLOT_HOURS)}:{MARKET_SLOT_MINUTE:02d} "
        f"empower={[(f'{h:02d}:{m:02d}') for h, m in EMPOWER_INTEL_SLOTS]} "
        f"empower_sin={[(f'{h:02d}:{m:02d}') for h, m in EMPOWER_INTEL_SINEMALAR_SLOTS]} "
        f"retry={BRIDGE_RETRY_MAX}x/{BRIDGE_RETRY_GAP_SEC}s",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Durduruldu", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if "--daemon" in args or "-d" in args:
        return run_daemon()
    virgul_only = "--virgul-only" in args
    play_only = "--play-only" in args
    if virgul_only:
        lock = _virgul_lock
    elif play_only:
        lock = _play_lock
    else:
        lock = _nt_lock
    if not lock.acquire(blocking=False):
        print("Sync zaten çalışıyor", file=sys.stderr)
        return 1
    try:
        if "--news-only" in args:
            result = run_news_bridge_once()
        elif virgul_only:
            result = run_virgul_bridge_once()
        elif play_only:
            result = run_play_bridge_once()
        elif "--notifications-only" in args:
            result = run_notification_bridge_once()
        else:
            result = run_all_once()
        return 0 if result.get("ok") else 1
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
