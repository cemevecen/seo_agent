#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde).

Notification stats + aktif haber listesi + Virgül reklam.

Tek sefer (ikisi):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --news-only
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --notifications-only

Daemon (otomatik + Elle yenile localhost:18765):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --daemon
  Play/ASC Chrome oturumu açık kalır (CDP); giriş uyarısı maili yok.

  POST /sync       → notification (30 dk)
  POST /sync-news  → news (1 saat)
  POST /sync-virgul → Virgül (00/06/12/18 TR)
  POST /sync-play   → Play / Android (3 saatte bir, :00)
  POST /sync-asc    → ASC / iOS (3 saatte bir, :05)
  POST /sync-firebase → Firebase Console Crashlytics (3 saatte bir, :10)
  POST /sync-gsc-links → Backlinks (01:00 + 13:00 TR)
  POST /sync-policy → Ad Manager Policy (01:05 + 13:05 TR)
  POST /sync-noads  → Sinemalar noAds (01:15 + 13:15 TR)
  POST /sync-pagespeed → pagespeed.web.dev (01:10 + 13:10 TR)
  POST /sync-seo-audit → SEO meta audit scrape (02:45 + 14:45 TR, GA4 top 500)
  POST /sync-gsc-cwv → GSC Core Web Vitals + AMP (03:00 + 15:00 TR)
  POST /sync-market → doviz.com piyasa tablo taraması (00:05 TR)
  POST /open-noads  → noAds sayfasını aç, textarea'ya URL yaz (policy «Ekle»)
  POST /sync-pm-lab → PM lab (3 saatte bir; ?jobs=serp|competitors|store_charts|google_news)
  POST /sync-all   → notification + news
"""

from __future__ import annotations

import json
import os
import smtplib
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
# Auto-loop poll (slot kaçırmamak için kısa); iş aralıkları ayrı.
AUTO_POLL_SEC = int(os.environ.get("BRIDGE_AUTO_POLL_SEC") or "60")
# Interval-based
AUTO_INTERVAL_SEC = int(
    os.environ.get("NOTIFICATION_BRIDGE_INTERVAL_SEC") or str(30 * 60)
)  # notification: 30 dk
NEWS_AUTO_INTERVAL_SEC = int(
    os.environ.get("NEWS_BRIDGE_INTERVAL_SEC") or str(60 * 60)
)  # news: 1 saat
PLAY_AUTO_INTERVAL_SEC = int(
    os.environ.get("PLAY_CONSOLE_BRIDGE_INTERVAL_SEC") or str(3 * 60 * 60)
)  # android: 3 saat
ASC_AUTO_INTERVAL_SEC = int(
    os.environ.get("ASC_CONSOLE_BRIDGE_INTERVAL_SEC") or str(3 * 60 * 60)
)  # ios: 3 saat
VIRGUL_AUTO_INTERVAL_SEC = int(
    os.environ.get("VIRGUL_BRIDGE_INTERVAL_SEC") or str(6 * 60 * 60)
)  # health/docs; slot kullanılır
GSC_LINKS_AUTO_INTERVAL_SEC = int(
    os.environ.get("GSC_LINKS_BRIDGE_INTERVAL_SEC") or str(12 * 60 * 60)
)
# Slot pencereleri Europe/Istanbul
VIRGUL_SLOT_HOURS = (0, 6, 12, 18)  # gece 00’dan 6 saatte bir → 4 tur
VIRGUL_SLOT_MINUTE = int(os.environ.get("VIRGUL_BRIDGE_MINUTE") or "0")
PLAY_SLOT_HOURS = (0, 3, 6, 9, 12, 15, 18, 21)
PLAY_SLOT_MINUTE = int(os.environ.get("PLAY_CONSOLE_BRIDGE_MINUTE") or "0")
ASC_SLOT_MINUTE = int(os.environ.get("ASC_CONSOLE_BRIDGE_MINUTE") or "5")
FIREBASE_SLOT_MINUTE = int(os.environ.get("FIREBASE_CONSOLE_BRIDGE_MINUTE") or "10")
TWICE_DAILY_HOURS = (1, 13)  # 01:00 + 13:00
GSC_SLOT_MINUTE = int(os.environ.get("GSC_LINKS_BRIDGE_MINUTE") or "0")
POLICY_SLOT_MINUTE = int(os.environ.get("ADMANAGER_POLICY_BRIDGE_MINUTE") or "5")
SPEED_SLOT_MINUTE = int(os.environ.get("PAGESPEED_BRIDGE_MINUTE") or "10")
NOADS_SLOT_MINUTE = int(os.environ.get("SINEMALAR_NOADS_BRIDGE_MINUTE") or "15")
# SEO audit: pagespeed/noAds sonrası — 02:45 + 14:45 TR
SEO_AUDIT_SLOT_HOURS = (2, 14)
SEO_AUDIT_SLOT_MINUTE = int(os.environ.get("SEO_AUDIT_BRIDGE_MINUTE") or "45")
# GSC CWV + AMP — SEO scrape sonrası — 03:00 + 15:00 TR
GSC_CWV_SLOT_HOURS = (3, 15)
GSC_CWV_SLOT_MINUTE = int(os.environ.get("GSC_CWV_BRIDGE_MINUTE") or "0")
# Piyasa tablo taraması — günde bir, 00:05 TR
MARKET_SLOT_HOURS = (0,)
MARKET_SLOT_MINUTE = int(os.environ.get("MARKET_TARAMA_BRIDGE_MINUTE") or "5")
SLOT_WINDOW_MIN = int(os.environ.get("BRIDGE_SLOT_WINDOW_MIN") or "20")
# Başarısız otomatik tur → en fazla 3 yeniden deneme, 10'ar dk arayla
BRIDGE_RETRY_MAX = int(os.environ.get("BRIDGE_RETRY_MAX") or "3")
BRIDGE_RETRY_GAP_SEC = int(os.environ.get("BRIDGE_RETRY_GAP_SEC") or str(10 * 60))
_NEWS_EVERY_N_RAW = (os.environ.get("NEWS_BRIDGE_EVERY_N") or "").strip()
NEWS_AUTO_EVERY_N = int(_NEWS_EVERY_N_RAW) if _NEWS_EVERY_N_RAW.isdigit() else 0
# Geriye dönük isimler
POLICY_AUTO_HOUR = TWICE_DAILY_HOURS[0]
POLICY_AUTO_MINUTE = POLICY_SLOT_MINUTE
NOADS_AUTO_HOURS = list(TWICE_DAILY_HOURS)
NOADS_AUTO_MINUTE = NOADS_SLOT_MINUTE
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
_play_lock = threading.Lock()
_asc_lock = threading.Lock()
_firebase_lock = threading.Lock()
_gsc_links_lock = threading.Lock()
_policy_lock = threading.Lock()
_noads_lock = threading.Lock()
_pagespeed_lock = threading.Lock()
_seo_audit_lock = threading.Lock()
_gsc_cwv_lock = threading.Lock()
_market_lock = threading.Lock()
_pm_lab_lock = threading.Lock()
_noads_open_lock = threading.Lock()
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
_last_policy_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_noads_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_pagespeed_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_seo_audit_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_gsc_cwv_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_market_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_pm_lab_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_nt_auto_at = 0.0
_last_news_auto_at = 0.0
_last_virgul_auto_slot = ""
_last_play_auto_slot = ""
_last_asc_auto_slot = ""
_last_firebase_auto_slot = ""
_last_gsc_links_auto_slot = ""
_last_policy_auto_slot = ""
_last_noads_auto_slot = ""
_last_pagespeed_auto_slot = ""
_last_seo_audit_auto_slot = ""
_last_gsc_cwv_auto_slot = ""
_last_market_auto_slot = ""
# Restart sonrası tam interval bekle; ilk dolum manuel --ingest / /sync-pm-lab.
_last_pm_lab_auto_at = time.time()
_last_pm_lab_competitors_auto_at = time.time()
PM_LAB_AUTO_INTERVAL_SEC = int(os.environ.get("PM_LAB_AUTO_INTERVAL_SEC") or str(3 * 3600))
PM_LAB_COMPETITORS_INTERVAL_SEC = int(os.environ.get("PM_LAB_COMPETITORS_INTERVAL_SEC") or "600")
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
_auto_cycle = 0
_last_fail_email_at: dict[str, float] = {}
_fail_streak: dict[str, int] = {}
# kind → {attempt: 1..MAX, next_at: float, name: str}
_job_retries: dict[str, dict[str, Any]] = {}


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


def _note_auto_success(kind: str) -> None:
    _fail_streak[kind] = 0


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
    if isinstance(result, dict) and result.get("needs_login"):
        print(
            f"Bridge alert atlandı ({kind} oturum) — Firefox profilinde giriş gerekir; "
            f"pencerede bir kez giriş yeterli. {msg[:160]}",
            flush=True,
        )
        return
    loginish = any(
        tok in msg.lower()
        for tok in ("needs_login", "login gerekli", "oturum", "giriş", "giris", "sign in")
    )
    if kind in ("play", "asc", "notification", "firebase") and loginish:
        print(
            f"Bridge alert atlandı ({kind} giriş) — oturum bekçi / çerez. {msg[:160]}",
            flush=True,
        )
        return
    labels = {
        "notification": "Notification (/notification)",
        "news": "Doviz News (/doviz-news)",
        "virgul": "Virgül Ad (/ad-virgul)",
    }
    label = labels.get(kind, kind)
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


def run_virgul_bridge_once() -> dict[str, Any]:
    """Virgül 6 sid Excel/CSV → Railway /ad-virgul ingest (dal dal, retry)."""
    global _last_virgul_result
    _load_dotenv()
    err = _require_virgul_creds()
    if err:
        _last_virgul_result = err
        return err

    import base64

    from backend.services.virgul_ad_client import fetch_all_sites_exports

    print("Virgül reklam export çekiliyor (6 sid)…", flush=True)
    fetched = fetch_all_sites_exports()
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
    for f in files:
        sk = f.get("stream_key") or "?"
        print(f"Virgul ingest → {sk}…", flush=True)
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

    Subprocess ile çalışır: sync Playwright'ın asyncio/greenlet state'i
    bridge auto thread'ini kirletmesin (Sync API inside asyncio loop).
    """
    global _last_play_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_play_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "play_console_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "play", "message": "Play tarama betiği yok"}
        _last_play_result = err
        return err

    print("Play Console scrape başlıyor…", flush=True)
    env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    cmd = [sys.executable, str(script), "--sync", "--ingest"]
    if not headed:
        cmd.append("--headless")
    env = os.environ.copy()
    env.setdefault("PLAY_CONSOLE_INGEST_URL", _play_console_ingest_url())
    timeout_sec = int(os.environ.get("PLAY_BRIDGE_TIMEOUT_SEC") or "1200")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=max(120, timeout_sec),
        )
    except subprocess.TimeoutExpired as exc:
        out = {
            "ok": False,
            "kind": "play",
            "message": f"Play tarama zaman aşımı ({timeout_sec}s) — takılı Chromium olabilir",
        }
        _last_play_result = out
        # Best-effort: leave cleanup to next launch's stale-browser kill
        print(str(exc)[:300], flush=True)
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "play", "message": f"Play subprocess: {exc}"}
        _last_play_result = out
        return out

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    # Propagate child logs (last chunk) for debugging
    if combined:
        for line in combined.splitlines()[-40:]:
            print(line, flush=True)

    if proc.returncode == 2 or "needs_login" in combined.lower():
        out = {
            "ok": False,
            "kind": "play",
            "needs_login": True,
            "message": "Play login gerekli (--login)",
        }
        _last_play_result = out
        return out

    if proc.returncode == 0:
        # Prefer a concise success line from child stdout
        msg = "Play sync OK"
        for line in reversed((proc.stdout or "").splitlines()):
            s = line.strip()
            if s.startswith("Play tarama") or s.startswith("Play scrape") or s.startswith("INGEST"):
                msg = s[:400]
                break
        out = {"ok": True, "kind": "play", "message": msg, "needs_login": False}
        _last_play_result = out
        print(f"Play sync · {out['message']}", flush=True)
        return out

    err_msg = ""
    for line in reversed(combined.splitlines()):
        s = line.strip()
        if not s:
            continue
        if "Sync API inside the asyncio loop" in s:
            err_msg = (
                "Playwright Sync API / asyncio conflict "
                "(önceki Chromium kapanmamış olabilir)"
            )
            break
        if "SingletonLock" in s or "ProcessSingleton" in s:
            err_msg = "Play profile SingletonLock — başka Chromium oturumu açık"
            break
        if "Error:" in s or "error:" in s or s.startswith("playwright."):
            err_msg = s[:400]
            break
    if not err_msg:
        err_msg = (combined[-400:] if combined else f"exit {proc.returncode}")[:400]
    out = {"ok": False, "kind": "play", "message": err_msg, "needs_login": False}
    _last_play_result = out
    print(f"Play sync · {out['message']}", flush=True)
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


def run_firebase_bridge_once(on_progress=None) -> dict[str, Any]:
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

    def _cb(info: dict[str, Any]) -> None:
        info = info if isinstance(info, dict) else {}
        _set_firebase_progress(
            running=True,
            phase=str(info.get("phase") or "scrape"),
            step=int(info.get("step") or 0),
            total_steps=int(info.get("total_steps") or 0),
            platform=str(info.get("platform") or ""),
            sub_label=str(info.get("sub_label") or ""),
            message=str(info.get("message") or "")[:200],
        )
        if callable(on_progress):
            try:
                on_progress(info)
            except Exception:
                pass

    print("Firebase Console scrape başlıyor…", flush=True)
    _set_firebase_progress(
        running=True,
        phase="starting",
        step=0,
        total_steps=24,
        platform="",
        sub_label="",
        message="Firebase Console scrape starting",
    )
    env_hl = (os.environ.get("FIREBASE_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_firebase_console(headed=headed, on_progress=_cb)
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
                "step": 23,
                "total_steps": 24,
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
        step=24,
        total_steps=24,
        message=out["message"],
    )
    print(f"Firebase sync · {out['message']}", flush=True)
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


def _run_pm_lab_script(*, jobs: str = "", label: str = "PM lab") -> dict[str, Any]:
    """Owner PM lab taramaları → Railway ingest."""
    global _last_pm_lab_result
    kind = "pm_lab_competitors" if jobs == "competitors" else "pm_lab"
    if not _ingest_token():
        err = {"ok": False, "kind": kind, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_pm_lab_result = err
        return err

    import subprocess

    script = ROOT / "scripts" / "pm_lab_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": kind, "message": "PM lab tarama betiği yok"}
        _last_pm_lab_result = err
        return err

    print(f"{label} tarama başlıyor…", flush=True)
    cmd = [sys.executable, str(script)]
    if jobs:
        cmd.extend(["--jobs", jobs, "--ingest"])
        if jobs == "competitors":
            timeout_sec = int(os.environ.get("PM_LAB_COMPETITORS_TIMEOUT_SEC") or "540")
        else:
            timeout_sec = int(os.environ.get("PM_LAB_JOB_TIMEOUT_SEC") or "1200")
    else:
        cmd.extend(["--sync", "--ingest"])
        timeout_sec = int(os.environ.get("PM_LAB_BRIDGE_TIMEOUT_SEC") or "1800")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=max(120, timeout_sec),
        )
    except subprocess.TimeoutExpired:
        out = {"ok": False, "kind": kind, "message": f"{label} zaman aşımı ({timeout_sec}s)"}
        _last_pm_lab_result = out
        return out
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": kind, "message": f"{label} subprocess: {exc}"}
        _last_pm_lab_result = out
        return out

    combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if combined:
        for line in combined.splitlines()[-40:]:
            print(line, flush=True)
    if proc.returncode == 0:
        out = {"ok": True, "kind": kind, "message": f"{label} sync OK"}
    else:
        tail = (combined[-300:] if combined else f"exit {proc.returncode}")[:300]
        out = {"ok": False, "kind": kind, "message": tail}
    _last_pm_lab_result = out
    print(f"{label} sync · {out['message']}", flush=True)
    return out


def run_pm_lab_bridge_once() -> dict[str, Any]:
    """SERP / rakip / store / news — 3 saatte bir."""
    return _run_pm_lab_script(jobs="", label="PM lab")


def run_pm_lab_competitors_once() -> dict[str, Any]:
    """Rakip fiyat linkleri — 10 dakikada bir."""
    return _run_pm_lab_script(jobs="competitors", label="PM lab fiyat")


PM_LAB_JOB_IDS = ("serp", "competitors", "store_charts", "google_news")


def run_pm_lab_jobs_once(jobs: str = "") -> dict[str, Any]:
    """Tek veya virgüllü PM lab işi (manuel Yenile)."""
    raw = (jobs or "").strip()
    if not raw:
        return run_pm_lab_bridge_once()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if parts == ["competitors"]:
        return run_pm_lab_competitors_once()
    label = "PM lab " + " · ".join(parts)
    return _run_pm_lab_script(jobs=",".join(parts), label=label)


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


def run_gsc_cwv_bridge_once(site_key: str | None = None, *, charts_only: bool = False) -> dict[str, Any]:
    """GSC Core Web Vitals + AMP scrape → Railway ingest."""
    global _last_gsc_cwv_result
    if not _ingest_token():
        err = {"ok": False, "kind": "gsc_cwv", "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    import subprocess

    script = ROOT / "scripts" / "gsc_cwv_scrape.py"
    if not script.is_file():
        err = {"ok": False, "kind": "gsc_cwv", "message": "GSC CWV tarama betiği yok"}
        _last_gsc_cwv_result = err
        _set_gsc_cwv_progress(
            running=False, phase="error", message=err["message"], finished_at=time.time()
        )
        return err

    print(f"GSC CWV scrape başlıyor… site={site_key or 'all'}", flush=True)
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
    cmd = [sys.executable, "-u", str(script), "--sync", "--ingest", "--headed"]
    if charts_only:
        cmd.append("--charts-only")
    if site_key:
        cmd += ["--site", str(site_key)]
    env = os.environ.copy()
    env.setdefault(
        "GSC_CWV_INGEST_URL",
        (
            os.environ.get("GSC_CWV_INGEST_URL")
            or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
        ).strip(),
    )
    env["PYTHONUNBUFFERED"] = "1"
    timeout_sec = int(os.environ.get("GSC_CWV_BRIDGE_TIMEOUT_SEC") or "7200")
    lines: list[str] = []
    step_guess = 0

    def _bump(phase: str, message: str, step: int | None = None) -> None:
        nonlocal step_guess
        if step is not None:
            step_guess = max(step_guess, step)
        _set_gsc_cwv_progress(
            running=True,
            phase=phase,
            site=site_key or "all",
            step=step_guess,
            total_steps=8,
            message=message[:200],
        )

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        deadline = time.time() + max(300, timeout_sec)
        while True:
            if time.time() > deadline:
                proc.kill()
                try:
                    proc.wait(timeout=15)
                except Exception:
                    pass
                out = {
                    "ok": False,
                    "kind": "gsc_cwv",
                    "message": f"GSC CWV tarama zaman aşımı ({timeout_sec}s)",
                }
                _last_gsc_cwv_result = out
                _set_gsc_cwv_progress(
                    running=False, phase="error", message=out["message"], finished_at=time.time()
                )
                return out
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
                continue
            s = line.rstrip()
            if s:
                lines.append(s)
                print(s, flush=True)
                low = s.lower()
                if "login" in low or "oturum" in low:
                    _bump("login", s, 1)
                elif "overview chart" in low or "chart series" in low:
                    _bump("charts", s, 3)
                elif "tooltip" in low:
                    _bump("tooltips", s, 4)
                elif "amp" in low:
                    _bump("amp", s, 5)
                elif "ingest" in low:
                    _bump("ingest", s, 7)
                elif "cwv scrape" in low or "summary" in low or "issue " in low:
                    _bump("scrape", s, 2)
                else:
                    _bump(_gsc_cwv_progress.get("phase") or "scrape", s)
        returncode = proc.wait(timeout=30)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "gsc_cwv", "message": f"GSC CWV subprocess: {exc}"}
        _last_gsc_cwv_result = out
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
        return out

    combined = "\n".join(lines).strip()
    detail = ""
    for line in reversed(lines):
        s = line.strip()
        if not s:
            continue
        if s.startswith("{") and s.endswith("}"):
            try:
                j = json.loads(s)
                if isinstance(j, dict) and j.get("message"):
                    detail = str(j.get("message"))[:240]
                    break
            except Exception:
                pass
        low = s.lower()
        if "oturumu yok" in low or s.startswith("FAIL ") or "login" in low:
            detail = s[:240]
            break
    if not detail and lines:
        detail = lines[-1].strip()[:240]

    if returncode == 0:
        out = {"ok": True, "kind": "gsc_cwv", "message": "GSC CWV tarama tamam", "site": site_key}
        _set_gsc_cwv_progress(
            running=False,
            phase="done",
            step=8,
            total_steps=8,
            message=out["message"],
            finished_at=time.time(),
        )
    else:
        msg = f"GSC CWV tarama çıkış {returncode}"
        if detail:
            msg = f"{msg}: {detail}"
        out = {
            "ok": False,
            "kind": "gsc_cwv",
            "message": msg,
            "site": site_key,
            "detail": detail or None,
        }
        _set_gsc_cwv_progress(
            running=False, phase="error", message=out["message"], finished_at=time.time()
        )
    _last_gsc_cwv_result = out
    print(f"GSC CWV sync · {out['message']}", flush=True)
    return out


def run_notification_bridge_once() -> dict[str, Any]:
    """Admin notification stats → Railway ingest."""
    global _last_result
    _load_dotenv()
    err = _require_creds()
    if err:
        _last_result = err
        _set_nt_progress(running=False, phase="error", message=err.get("message") or "")
        return err

    from backend.services.doviz_notification_admin import fetch_notification_rows_from_admin

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

    print("Admin stats çekiliyor…", flush=True)
    try:
        fetched = fetch_notification_rows_from_admin(on_progress=_on_progress)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc) or "Notification tarama hatası"
        _set_nt_progress(running=False, phase="error", message=msg)
        out = {"ok": False, "message": msg, "parsed": 0}
        _last_result = out
        return out

    rows = fetched.get("rows") or []
    print(f"Notification çekildi: {len(rows)} satır · {fetched.get('elapsed_sec')}s", flush=True)
    if not rows:
        out = {"ok": False, "message": "Notification: satır yok — gönderilmedi", "parsed": 0}
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
            {"rows": rows, "source": "doviz_admin_bridge"},
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
    out = {
        "ok": bool(ok),
        "kind": "notification",
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
        message=("Tam tarama…" if use_full else f"Son {days or 7} gün…"),
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
        f"Admin haberler çekiliyor ({'full' if use_full else f'days={days or 7} / {min_day}…'})…",
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

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        for k, v in _cors_headers(self).items():
            self.send_header(k, v)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/", "/health"):
            self._send(
                200,
                {
                    "ok": True,
                    "service": "doviz-admin-bridge",
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
                    "last_seo_audit": _last_seo_audit_result,
                    "last_gsc_cwv": _last_gsc_cwv_result,
                    "last_market": _last_market_result,
                    "last_pm_lab": _last_pm_lab_result,
                    "gsc_cwv_progress": dict(_gsc_cwv_progress),
                    "last_gsc_links": _last_gsc_links_result,
                    "last_policy": _last_policy_result,
                    "last_noads": _last_noads_result,
                    "pm_lab_interval_sec": PM_LAB_AUTO_INTERVAL_SEC,
                    "pm_lab_competitors_interval_sec": PM_LAB_COMPETITORS_INTERVAL_SEC,
                    "play_interval_sec": PLAY_AUTO_INTERVAL_SEC,
                    "asc_interval_sec": ASC_AUTO_INTERVAL_SEC,
                    "schedule": {
                        "notification_sec": AUTO_INTERVAL_SEC,
                        "news_sec": NEWS_AUTO_INTERVAL_SEC,
                        "virgul_slots_tr": [f"{h:02d}:{VIRGUL_SLOT_MINUTE:02d}" for h in VIRGUL_SLOT_HOURS],
                        "play_slots_tr": [f"{h:02d}:{PLAY_SLOT_MINUTE:02d}" for h in PLAY_SLOT_HOURS],
                        "asc_slots_tr": [f"{h:02d}:{ASC_SLOT_MINUTE:02d}" for h in PLAY_SLOT_HOURS],
                        "firebase_slots_tr": [f"{h:02d}:{FIREBASE_SLOT_MINUTE:02d}" for h in PLAY_SLOT_HOURS],
                        "gsc_slots_tr": [f"{h:02d}:{GSC_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
                        "policy_slots_tr": [f"{h:02d}:{POLICY_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
                        "pagespeed_slots_tr": [f"{h:02d}:{SPEED_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
                        "noads_slots_tr": [f"{h:02d}:{NOADS_SLOT_MINUTE:02d}" for h in TWICE_DAILY_HOURS],
                        "seo_audit_slots_tr": [
                            f"{h:02d}:{SEO_AUDIT_SLOT_MINUTE:02d}" for h in SEO_AUDIT_SLOT_HOURS
                        ],
                        "gsc_cwv_slots_tr": [
                            f"{h:02d}:{GSC_CWV_SLOT_MINUTE:02d}" for h in GSC_CWV_SLOT_HOURS
                        ],
                        "market_slots_tr": [
                            f"{h:02d}:{MARKET_SLOT_MINUTE:02d}" for h in MARKET_SLOT_HOURS
                        ],
                        "pm_lab_sec": PM_LAB_AUTO_INTERVAL_SEC,
                        "pm_lab_competitors_sec": PM_LAB_COMPETITORS_INTERVAL_SEC,
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
            lock, busy, runner = (
                _play_lock,
                "Play Console sync zaten çalışıyor, bekleyin.",
                run_play_bridge_once,
            )
        elif path in ("/sync-gsc-links", "/gsc-links", "/sync-backlinks"):
            lock, busy, runner = (
                _gsc_links_lock,
                "GSC Links sync zaten çalışıyor, bekleyin.",
                run_gsc_links_bridge_once,
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
        elif path in ("/sync-pm-lab", "/pm-lab", "/sync-pmlab"):
            raw_jobs = (qs.get("jobs") or [""])[0].strip()
            job_parts = [p.strip() for p in raw_jobs.split(",") if p.strip()]
            if job_parts and any(p not in PM_LAB_JOB_IDS for p in job_parts):
                self._send(400, {"ok": False, "message": "bilinmeyen PM lab işi"})
                return
            jobs_arg = ",".join(job_parts)
            if not _pm_lab_lock.acquire(blocking=False):
                self._send(
                    409,
                    {
                        "ok": False,
                        "running": True,
                        "message": "PM lab tarama zaten çalışıyor, bekleyin.",
                    },
                )
                return

            def _pm_lab_bg() -> None:
                try:
                    run_pm_lab_jobs_once(jobs_arg)
                except Exception:
                    traceback.print_exc()
                finally:
                    _pm_lab_lock.release()

            threading.Thread(target=_pm_lab_bg, name="pm-lab-bridge", daemon=True).start()
            self._send(
                200,
                {
                    "ok": True,
                    "started": True,
                    "kind": "pm_lab",
                    "jobs": jobs_arg or "all",
                    "message": "PM lab tarama arka planda başladı",
                },
            )
            return
        elif path in ("/sync-pagespeed", "/pagespeed", "/sync-speed"):
            lock, busy, runner = (
                _pagespeed_lock,
                "PageSpeed sync zaten çalışıyor, bekleyin.",
                run_pagespeed_bridge_once,
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

            def _bg() -> None:
                try:
                    run_seo_audit_bridge_once(site_id=site_id)
                except Exception:
                    traceback.print_exc()
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
            charts_only = str((qs.get("charts_only") or [""])[0]).strip().lower() in (
                "1",
                "true",
                "yes",
            )
            length = int(self.headers.get("Content-Length") or 0)
            raw_body = self.rfile.read(length) if length > 0 else b""
            if raw_body:
                try:
                    payload = json.loads(raw_body.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict):
                        if payload.get("site"):
                            site_key = str(payload.get("site") or "").strip().lower() or site_key
                        if payload.get("charts_only") in (True, 1, "1", "true", "yes"):
                            charts_only = True
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
                message="GSC CWV tarama kuyruğa alındı",
                started_at=time.time(),
                finished_at=0.0,
            )

            def _bg_cwv() -> None:
                try:
                    run_gsc_cwv_bridge_once(site_key=site_key, charts_only=charts_only)
                except Exception:
                    traceback.print_exc()
                    _set_gsc_cwv_progress(
                        running=False,
                        phase="error",
                        message="GSC CWV thread hatası",
                        finished_at=time.time(),
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
                    "site": site_key,
                    "progress": dict(_gsc_cwv_progress),
                    "message": "GSC CWV + AMP tarama arka planda başladı",
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
        elif path in ("/sync-all", "/all"):
            lock, busy, runner = (_nt_lock, "Sync zaten çalışıyor, bekleyin.", run_all_once)
        else:
            self._send(404, {"ok": False, "message": "not found"})
            return
        if not lock.acquire(blocking=False):
            self._send(409, {"ok": False, "message": busy})
            return
        try:
            result = runner()
            self._send(200 if result.get("ok") else 502, result)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self._send(500, {"ok": False, "message": str(exc)})
        finally:
            lock.release()


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


def _interval_due(last_at: float, interval_sec: int, *, min_sec: int = 60) -> bool:
    if last_at <= 0:
        return True
    return (time.time() - last_at) >= max(min_sec, interval_sec)


def _should_run_notification_auto() -> bool:
    return _interval_due(_last_nt_auto_at, AUTO_INTERVAL_SEC, min_sec=60)


def _should_run_news_auto() -> bool:
    global _auto_cycle
    if NEWS_AUTO_EVERY_N > 0:
        return NEWS_AUTO_EVERY_N <= 1 or (_auto_cycle % NEWS_AUTO_EVERY_N) == 1
    return _interval_due(_last_news_auto_at, NEWS_AUTO_INTERVAL_SEC, min_sec=60)


def _clear_job_retry(kind: str) -> None:
    _job_retries.pop(kind, None)


def _arm_job_retry(kind: str, *, name: str) -> None:
    """Başarısız tur sonrası bir sonraki yeniden denemeyi planla (max 3, 10 dk arayla)."""
    st = _job_retries.get(kind) or {"attempt": 0, "name": name}
    attempt = int(st.get("attempt") or 0)
    if attempt >= BRIDGE_RETRY_MAX:
        print(
            f"Auto {name}: {BRIDGE_RETRY_MAX} yeniden deneme tükendi — "
            "sonraki planlı slota kadar bekleniyor",
            flush=True,
        )
        _clear_job_retry(kind)
        return
    attempt += 1
    gap = max(60, BRIDGE_RETRY_GAP_SEC)
    _job_retries[kind] = {
        "attempt": attempt,
        "next_at": time.time() + gap,
        "name": name,
    }
    print(
        f"Auto {name}: yeniden deneme planlandı {attempt}/{BRIDGE_RETRY_MAX} · ~{gap // 60} dk sonra",
        flush=True,
    )


def _run_locked_job(
    *,
    name: str,
    lock: threading.Lock,
    runner,
    kind: str,
    notify: bool = True,
) -> dict[str, Any] | None:
    if not lock.acquire(blocking=False):
        print(f"Auto {name} atlandı (manuel sync sürüyor)", flush=True)
        return None
    try:
        try:
            result = runner()
            if result.get("ok"):
                _note_auto_success(kind)
            elif notify:
                _notify_auto_failure(kind, result)
            return result
        except Exception as exc:
            traceback.print_exc()
            if notify:
                _notify_auto_failure(kind, exc=exc)
            return {"ok": False, "message": str(exc)}
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
        "pm_lab": {
            "name": "PM lab",
            "lock": _pm_lab_lock,
            "runner": run_pm_lab_bridge_once,
        },
        "pm_lab_competitors": {
            "name": "PM lab fiyat",
            "lock": _pm_lab_lock,
            "runner": run_pm_lab_competitors_once,
        },
        "sinemalar_noads": {
            "name": "noAds",
            "lock": _noads_lock,
            "runner": run_sinemalar_noads_bridge_once,
        },
    }


def _process_due_retries() -> None:
    """Zamanı gelen yeniden denemeleri çalıştır; başarıda kuyruğu temizle."""
    global _last_nt_auto_at, _last_news_auto_at, _last_pm_lab_auto_at, _last_pm_lab_competitors_auto_at
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
        name = str(st.get("name") or meta["name"])
        attempt = int(st.get("attempt") or 1)
        # Çalışırken çift tetiklemeyi engelle
        st["next_at"] = now + 86400
        _job_retries[kind] = st
        print(
            f"Auto {name}: yeniden deneme çalışıyor {attempt}/{BRIDGE_RETRY_MAX}",
            flush=True,
        )
        # Ara denemelerde mail spam olmasın; son denemede bildir
        is_last = attempt >= BRIDGE_RETRY_MAX
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
            elif kind == "pm_lab":
                _last_pm_lab_auto_at = time.time()
                _last_pm_lab_competitors_auto_at = time.time()
            elif kind == "pm_lab_competitors":
                _last_pm_lab_competitors_auto_at = time.time()
            continue
        if is_last:
            print(
                f"Auto {name}: {BRIDGE_RETRY_MAX} yeniden deneme de başarısız — "
                "sonraki planlı slota bırakıldı",
                flush=True,
            )
            _clear_job_retry(kind)
            continue
        # Bir sonraki retry
        nxt = attempt + 1
        gap = max(60, BRIDGE_RETRY_GAP_SEC)
        _job_retries[kind] = {"attempt": nxt, "next_at": now + gap, "name": name}
        print(
            f"Auto {name}: retry #{attempt} başarısız → #{nxt}/{BRIDGE_RETRY_MAX} · ~{gap // 60} dk sonra",
            flush=True,
        )


def _auto_loop() -> None:
    """Slot + interval zamanlayıcı; poll ~60s. Hata → 3×10 dk retry, sonra sonraki slot."""
    global _auto_cycle
    global _last_nt_auto_at, _last_news_auto_at, _last_pm_lab_auto_at, _last_pm_lab_competitors_auto_at
    global _last_virgul_auto_slot, _last_play_auto_slot, _last_asc_auto_slot
    global _last_gsc_links_auto_slot, _last_policy_auto_slot
    global _last_noads_auto_slot, _last_pagespeed_auto_slot, _last_seo_audit_auto_slot
    global _last_gsc_cwv_auto_slot, _last_market_auto_slot

    while True:
        _auto_cycle += 1
        _process_due_retries()

        # Notification 30 dk + News 1 saat (aynı admin kilidi)
        # Pending retry varken planlı tur atlanır (retry bitene / başarılı olana kadar)
        nt_due = _should_run_notification_auto() and "notification" not in _job_retries
        news_due = _should_run_news_auto() and "news" not in _job_retries
        if nt_due or news_due:
            if _nt_lock.acquire(blocking=False):
                try:
                    if nt_due:
                        try:
                            nt = run_notification_bridge_once()
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

        pm_due = (
            _interval_due(_last_pm_lab_auto_at, PM_LAB_AUTO_INTERVAL_SEC, min_sec=120)
            and "pm_lab" not in _job_retries
        )
        pm_comp_due = (
            _interval_due(_last_pm_lab_competitors_auto_at, PM_LAB_COMPETITORS_INTERVAL_SEC, min_sec=60)
            and "pm_lab_competitors" not in _job_retries
        )
        if pm_due:
            result = _run_locked_job(
                name="PM lab",
                lock=_pm_lab_lock,
                runner=run_pm_lab_bridge_once,
                kind="pm_lab",
                notify=False,
            )
            if result is None:
                print("Auto PM lab atlandı (manuel sync sürüyor)", flush=True)
            else:
                _last_pm_lab_auto_at = time.time()
                _last_pm_lab_competitors_auto_at = time.time()
                if result.get("ok"):
                    _note_auto_success("pm_lab")
                    _clear_job_retry("pm_lab")
                    _clear_job_retry("pm_lab_competitors")
                else:
                    _notify_auto_failure("pm_lab", result)
                    _arm_job_retry("pm_lab", name="PM lab")
        elif pm_comp_due:
            result = _run_locked_job(
                name="PM lab fiyat",
                lock=_pm_lab_lock,
                runner=run_pm_lab_competitors_once,
                kind="pm_lab_competitors",
                notify=False,
            )
            if result is None:
                print("Auto PM lab fiyat atlandı (manuel sync sürüyor)", flush=True)
            else:
                _last_pm_lab_competitors_auto_at = time.time()
                if result.get("ok"):
                    _note_auto_success("pm_lab_competitors")
                    _clear_job_retry("pm_lab_competitors")
                else:
                    _notify_auto_failure("pm_lab_competitors", result)
                    _arm_job_retry("pm_lab_competitors", name="PM lab fiyat")

        def _slot_job(
            kind: str,
            name: str,
            lock: threading.Lock,
            runner,
            last_attr: str,
            hours: tuple[int, ...] | list[int],
            minute: int,
        ) -> None:
            nonlocal_last = globals()[last_attr]
            # Retry beklerken aynı slot penceresinde tekrar tetikleme
            if kind in _job_retries:
                return
            due, slot = _slot_due(nonlocal_last, hours, minute)
            if not due:
                return
            _clear_job_retry(kind)
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
        )
        _slot_job(
            "play", "Play", _play_lock, run_play_bridge_once,
            "_last_play_auto_slot", PLAY_SLOT_HOURS, PLAY_SLOT_MINUTE,
        )
        _slot_job(
            "asc", "ASC", _asc_lock, run_asc_bridge_once,
            "_last_asc_auto_slot", PLAY_SLOT_HOURS, ASC_SLOT_MINUTE,
        )
        _slot_job(
            "firebase", "Firebase", _firebase_lock, run_firebase_bridge_once,
            "_last_firebase_auto_slot", PLAY_SLOT_HOURS, FIREBASE_SLOT_MINUTE,
        )
        _slot_job(
            "gsc_links", "GSC Links", _gsc_links_lock, run_gsc_links_bridge_once,
            "_last_gsc_links_auto_slot", TWICE_DAILY_HOURS, GSC_SLOT_MINUTE,
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


def _remote_claim_job_registry() -> dict[str, dict[str, Any]]:
    return {
        "play": {"name": "Play", "lock": _play_lock, "runner": run_play_bridge_once},
        "asc": {"name": "ASC", "lock": _asc_lock, "runner": run_asc_bridge_once},
        "firebase": {"name": "Firebase", "lock": _firebase_lock, "runner": run_firebase_bridge_once},
        "cwv": {"name": "GSC CWV", "lock": _gsc_cwv_lock, "runner": run_gsc_cwv_bridge_once},
        "notification": {"name": "Notification", "lock": _nt_lock, "runner": run_notification_bridge_once},
        "news": {"name": "News", "lock": _nt_lock, "runner": lambda: run_news_bridge_once(days=7)},
        "virgul": {"name": "Virgul", "lock": _virgul_lock, "runner": run_virgul_bridge_once},
        "market": {"name": "Piyasa", "lock": _market_lock, "runner": run_market_tarama_bridge_once},
        "links": {"name": "GSC Links", "lock": _gsc_links_lock, "runner": run_gsc_links_bridge_once},
        "policy": {"name": "Policy", "lock": _policy_lock, "runner": run_admanager_policy_bridge_once},
        "noads": {"name": "noAds", "lock": _noads_lock, "runner": run_sinemalar_noads_bridge_once},
        "seo": {"name": "SEO Audit", "lock": _seo_audit_lock, "runner": run_seo_audit_bridge_once},
    }


def _post_page_tarama_result(payload: dict[str, Any]) -> None:
    url = _page_tarama_api_base() + "/api/page-tarama/result"
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(
                url, headers=_page_tarama_auth_headers(), json=payload, timeout=20
            )
            if resp.status_code < 400:
                return
            print(
                f"page-tarama result HTTP {resp.status_code} (try {attempt}/3)",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"page-tarama result hata (try {attempt}/3): {exc}", flush=True)
        time.sleep(1.5 * attempt)
    if last_exc:
        print(f"page-tarama result vazgeçildi: {last_exc}", flush=True)


def _pm_lab_claim_loop() -> None:
    """PM Lab Refresh kuyruğunu Mac’te çalıştır."""
    url = _page_tarama_api_base() + "/api/pm-lab/claim-refresh"
    print(f"PM lab yenile kuyruğu: {url}", flush=True)
    while True:
        try:
            if not _ingest_token():
                time.sleep(12)
                continue
            resp = requests.get(url, headers=_page_tarama_auth_headers(), timeout=20)
            if resp.status_code != 200:
                time.sleep(5)
                continue
            job = str((resp.json() or {}).get("job") or "").strip()
            if not job:
                time.sleep(3)
                continue
            print(f"PM lab yenile başladı: {job}", flush=True)
            result = None
            while result is None:
                result = _run_locked_job(
                    name=f"PM lab {job}",
                    lock=_pm_lab_lock,
                    runner=lambda j=job: run_pm_lab_jobs_once(j),
                    kind="pm_lab",
                    notify=False,
                )
                if result is None:
                    print(f"PM lab {job}: kilit meşgul, 8 sn…", flush=True)
                    time.sleep(8)
            print(
                f"PM lab yenile bitti: {job} · ok={bool(result.get('ok'))} · {result.get('message') or ''}",
                flush=True,
            )
        except Exception:
            traceback.print_exc()
            time.sleep(5)


def _page_tarama_claim_loop() -> None:
    """Mobil/Railway «Sayfayı güncelle» kuyruğunu Mac’te çalıştır."""
    url = _page_tarama_api_base() + "/api/page-tarama/claim"
    registry = _remote_claim_job_registry()
    print(f"Uzaktan tarama kuyruğu: {url}", flush=True)
    while True:
        run_id = ""
        job_id = ""
        final_posted = False
        try:
            if not _ingest_token():
                time.sleep(12)
                continue
            resp = requests.get(url, headers=_page_tarama_auth_headers(), timeout=20)
            if resp.status_code != 200:
                time.sleep(5)
                continue
            job = (resp.json() or {}).get("job")
            if not job:
                time.sleep(3)
                continue
            job_id = str(job.get("job_id") or "")
            meta = registry.get(job_id)
            if not meta:
                _post_page_tarama_result(
                    {
                        "run_id": job.get("run_id"),
                        "job_id": job_id,
                        "ok": False,
                        "message": "Unknown job",
                    }
                )
                continue
            print(f"Uzaktan tarama başladı: {meta['name']}", flush=True)
            run_id = str(job.get("run_id") or "")
            started_mono = time.time()

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
                _post_page_tarama_result(payload)

            _progress_post({"message": "Mac scan claimed · starting", "phase": "claimed", "step": 0})

            stop_hb = threading.Event()

            def _heartbeat() -> None:
                while not stop_hb.wait(5.0):
                    # Firebase already posts fine steps; heartbeat fills gaps for other jobs
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
                    return run_firebase_bridge_once(on_progress=_progress_post)
                return meta["runner"]()

            result: dict[str, Any] | None = None
            try:
                while result is None:
                    result = _run_locked_job(
                        name=meta["name"],
                        lock=meta["lock"],
                        runner=_runner,
                        kind=job_id,
                        notify=False,
                    )
                    if result is None:
                        _progress_post(
                            {
                                "phase": "waiting_lock",
                                "message": f"{meta['name']} · another scan holds the lock, retrying…",
                            }
                        )
                        print(f"Uzaktan {meta['name']}: kilit meşgul, 8 sn…", flush=True)
                        time.sleep(8)
            finally:
                stop_hb.set()

            if not isinstance(result, dict):
                result = {"ok": False, "message": "Mac scan returned empty result"}
            _post_page_tarama_result(
                {
                    "run_id": run_id,
                    "job_id": job_id,
                    "ok": bool(result.get("ok")),
                    "message": str(
                        result.get("message") or ("Done" if result.get("ok") else "Error")
                    )[:180],
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
                            "message": f"Mac error: {exc}"[:180],
                        }
                    )
                except Exception:
                    pass
            time.sleep(5)


def run_daemon() -> int:
    _load_dotenv()
    try:
        from backend.services.store_session_cdp import start_keeper_threads

        start_keeper_threads()
        print("Tarama tarayıcısı: Firefox (Chrome/Chromium açılmaz)", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"Oturum bekçi başlatılamadı: {exc}", flush=True)
    threading.Thread(target=_auto_loop, name="nt-bridge-auto", daemon=True).start()
    threading.Thread(target=_page_tarama_claim_loop, name="page-tarama-claim", daemon=True).start()
    threading.Thread(target=_pm_lab_claim_loop, name="pm-lab-claim", daemon=True).start()
    server = ThreadingHTTPServer((BRIDGE_HOST, BRIDGE_PORT), _BridgeHandler)
    print(
        f"Bridge daemon dinliyor http://{BRIDGE_HOST}:{BRIDGE_PORT} "
        f"notify={AUTO_INTERVAL_SEC}s news={NEWS_AUTO_INTERVAL_SEC}s "
        f"virgul={list(VIRGUL_SLOT_HOURS)}:00 play={list(PLAY_SLOT_HOURS)}:{PLAY_SLOT_MINUTE:02d} "
        f"asc=:{ASC_SLOT_MINUTE:02d} firebase=:{FIREBASE_SLOT_MINUTE:02d} twice@01/13 gsc=:{GSC_SLOT_MINUTE:02d} "
        f"policy=:{POLICY_SLOT_MINUTE:02d} speed=:{SPEED_SLOT_MINUTE:02d} noads=:{NOADS_SLOT_MINUTE:02d} "
        f"seo={list(SEO_AUDIT_SLOT_HOURS)}:{SEO_AUDIT_SLOT_MINUTE:02d} "
        f"cwv={list(GSC_CWV_SLOT_HOURS)}:{GSC_CWV_SLOT_MINUTE:02d} "
        f"market={list(MARKET_SLOT_HOURS)}:{MARKET_SLOT_MINUTE:02d} "
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
