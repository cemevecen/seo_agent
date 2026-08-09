#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde).

Notification stats + aktif haber listesi + Virgül reklam.

Tek sefer (ikisi):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --news-only
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --notifications-only

Daemon (otomatik + Elle yenile localhost:18765):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --daemon

  POST /sync       → notification (~30 dk auto)
  POST /sync-news?days=7  → son 1 hafta (Elle yenile + ~30 dk auto)
  POST /sync-news?full=1  → tam geçmiş (seyrek)
  POST /sync-virgul → Virgül Excel (~30 dk auto)
  POST /sync-play   → Play Console scrape (~30 dk auto)
  POST /sync-asc    → App Store Connect scrape (iOS Metrikler)
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
# Üçü de varsayılan 30 dk (notification / news / virgul)
_DEFAULT_INTERVAL = 30 * 60
AUTO_INTERVAL_SEC = int(
    os.environ.get("NOTIFICATION_BRIDGE_INTERVAL_SEC") or str(_DEFAULT_INTERVAL)
)
NEWS_AUTO_INTERVAL_SEC = int(
    os.environ.get("NEWS_BRIDGE_INTERVAL_SEC") or str(_DEFAULT_INTERVAL)
)
VIRGUL_AUTO_INTERVAL_SEC = int(
    os.environ.get("VIRGUL_BRIDGE_INTERVAL_SEC") or str(_DEFAULT_INTERVAL)
)
PLAY_AUTO_INTERVAL_SEC = int(
    os.environ.get("PLAY_CONSOLE_BRIDGE_INTERVAL_SEC") or str(_DEFAULT_INTERVAL)
)
# GSC Links scrape — günde 2 kez (12 saat)
GSC_LINKS_AUTO_INTERVAL_SEC = int(
    os.environ.get("GSC_LINKS_BRIDGE_INTERVAL_SEC") or str(12 * 60 * 60)
)
# Ad Manager Policy — günde 1 kez 02:00 Europe/Istanbul
POLICY_AUTO_HOUR = int(os.environ.get("ADMANAGER_POLICY_BRIDGE_HOUR") or "2")
POLICY_AUTO_MINUTE = int(os.environ.get("ADMANAGER_POLICY_BRIDGE_MINUTE") or "0")
# Sinemalar noAds — günde 2 kez (03:00 + 15:00 Europe/Istanbul)
NOADS_AUTO_HOURS = [
    int(x.strip())
    for x in (os.environ.get("SINEMALAR_NOADS_BRIDGE_HOURS") or "3,15").split(",")
    if x.strip().isdigit()
] or [3, 15]
NOADS_AUTO_MINUTE = int(os.environ.get("SINEMALAR_NOADS_BRIDGE_MINUTE") or "10")
# Eski ayar: her N. bildirim turunda haber (NEWS_BRIDGE_INTERVAL_SEC yoksa)
_NEWS_EVERY_N_RAW = (os.environ.get("NEWS_BRIDGE_EVERY_N") or "").strip()
NEWS_AUTO_EVERY_N = int(_NEWS_EVERY_N_RAW) if _NEWS_EVERY_N_RAW.isdigit() else 0
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
_gsc_links_lock = threading.Lock()
_policy_lock = threading.Lock()
_noads_lock = threading.Lock()
_last_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_news_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_virgul_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_play_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_asc_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_gsc_links_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_gsc_links_auto_at = 0.0
_last_policy_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_policy_auto_date = ""
_last_noads_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_noads_auto_slot = ""
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
_auto_cycle = 0
_last_news_auto_at = 0.0
_last_virgul_auto_at = 0.0
_last_play_auto_at = 0.0
_last_fail_email_at: dict[str, float] = {}
_fail_streak: dict[str, int] = {}


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
    """Play Console dashboard + reviews scrape → Railway ingest."""
    global _last_play_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_play_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "play_console_scrape.py"
        spec = importlib.util.spec_from_file_location("play_console_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "play_console_scrape.py yüklenemedi"}
            _last_play_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_play_console = mod.scrape_play_console
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"play_console_scrape import: {exc}"}
        _last_play_result = err
        return err

    print("Play Console scrape başlıyor…", flush=True)
    # Google headless’ta oturumu düşürüyor; bridge da headed (DISPLAY/Mac GUI).
    env_hl = (os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    headed = env_hl not in ("1", "true", "yes")
    result = scrape_play_console(headed=headed)
    if result.get("needs_login"):
        out = {
            "ok": False,
            "kind": "play",
            "needs_login": True,
            "message": result.get("message") or "Play login gerekli (--login)",
        }
        _last_play_result = out
        return out
    try:
        # URL override for ingest if bridge has custom env
        os.environ.setdefault(
            "PLAY_CONSOLE_INGEST_URL",
            _play_console_ingest_url(),
        )
        ing = ingest_scrape_result(result)
    except Exception as exc:  # noqa: BLE001
        out = {"ok": False, "kind": "play", "message": f"Ingest hata: {exc}"}
        _last_play_result = out
        return out
    out = {
        "ok": bool(ing.get("ok")) and bool(result.get("ok")),
        "kind": "play",
        "http_status": ing.get("http_status"),
        "metric_count": len(result.get("metrics") or []),
        "review_count": len(result.get("reviews") or []),
        "message": result.get("message") or ing.get("message") or "Play sync",
        "needs_login": False,
        "ingest": {
            k: ing.get(k)
            for k in (
                "ok",
                "updated_at",
                "metric_count",
                "tpg_count",
                "breakdown_count",
                "review_count",
                "message",
            )
            if k in ing or k == "ok"
        },
    }
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
            err = {"ok": False, "message": "gsc_links_scrape.py yüklenemedi"}
            _last_gsc_links_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_gsc_links = mod.scrape_gsc_links
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"gsc_links_scrape import: {exc}"}
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
            err = {"ok": False, "message": "admanager_policy_scrape.py yüklenemedi"}
            _last_policy_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_fn = mod.scrape_admanager_policy
        ingest_fn = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"admanager_policy_scrape import: {exc}"}
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


def run_sinemalar_noads_bridge_once() -> dict[str, Any]:
    """Sinemalar management/noAds → Railway /api/policy/noads/ingest."""
    global _last_noads_result
    if not _ingest_token():
        err = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_noads_result = err
        return err
    try:
        import importlib.util

        path = ROOT / "scripts" / "sinemalar_noads_scrape.py"
        spec = importlib.util.spec_from_file_location("sinemalar_noads_scrape", path)
        if spec is None or spec.loader is None:
            err = {"ok": False, "message": "sinemalar_noads_scrape.py yüklenemedi"}
            _last_noads_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_fn = mod.scrape_sinemalar_noads
        ingest_fn = mod.ingest_noads_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"sinemalar_noads_scrape import: {exc}"}
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
            err = {"ok": False, "message": "asc_console_scrape.py yüklenemedi"}
            _last_asc_result = err
            return err
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        scrape_asc_console = mod.scrape_asc_console
        ingest_scrape_result = mod.ingest_scrape_result
    except Exception as exc:  # noqa: BLE001
        err = {"ok": False, "message": f"asc_console_scrape import: {exc}"}
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
        msg = str(exc) or "Notification scrape hatası"
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
        message=("Tam scrape…" if use_full else f"Son {days or 7} gün…"),
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
        msg = str(exc) or "News scrape hatası"
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
                    "play_interval_sec": PLAY_AUTO_INTERVAL_SEC,
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


def _should_run_news_auto() -> bool:
    """30 dk (veya NEWS_BRIDGE_INTERVAL_SEC) / isteğe bağlı EVERY_N."""
    global _last_news_auto_at, _auto_cycle
    if NEWS_AUTO_EVERY_N > 0:
        return NEWS_AUTO_EVERY_N <= 1 or (_auto_cycle % NEWS_AUTO_EVERY_N) == 1
    if _last_news_auto_at <= 0:
        return True
    return (time.time() - _last_news_auto_at) >= max(60, NEWS_AUTO_INTERVAL_SEC)


def _should_run_virgul_auto() -> bool:
    global _last_virgul_auto_at
    if _last_virgul_auto_at <= 0:
        return True
    return (time.time() - _last_virgul_auto_at) >= max(300, VIRGUL_AUTO_INTERVAL_SEC)


def _should_run_play_auto() -> bool:
    global _last_play_auto_at
    if _last_play_auto_at <= 0:
        return True
    return (time.time() - _last_play_auto_at) >= max(300, PLAY_AUTO_INTERVAL_SEC)


def _should_run_gsc_links_auto() -> bool:
    global _last_gsc_links_auto_at
    if _last_gsc_links_auto_at <= 0:
        return True
    return (time.time() - _last_gsc_links_auto_at) >= max(600, GSC_LINKS_AUTO_INTERVAL_SEC)


def _should_run_policy_auto() -> bool:
    """Europe/Istanbul 02:00 civarı, günde bir."""
    global _last_policy_auto_date
    try:
        from zoneinfo import ZoneInfo

        now = __import__("datetime").datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc) + timedelta(hours=3)
    today = now.strftime("%Y-%m-%d")
    if _last_policy_auto_date == today:
        return False
    # 02:00–02:45 penceresi
    minutes = now.hour * 60 + now.minute
    start = POLICY_AUTO_HOUR * 60 + POLICY_AUTO_MINUTE
    return start <= minutes <= start + 45


def _should_run_noads_auto() -> bool:
    """Europe/Istanbul — günde 2 slot (varsayılan 03:10 ve 15:10)."""
    global _last_noads_auto_slot
    try:
        from zoneinfo import ZoneInfo

        now = __import__("datetime").datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc) + timedelta(hours=3)
    minutes = now.hour * 60 + now.minute
    for hour in NOADS_AUTO_HOURS:
        start = hour * 60 + NOADS_AUTO_MINUTE
        if start <= minutes <= start + 40:
            slot = f"{now.strftime('%Y-%m-%d')}-{hour:02d}"
            if _last_noads_auto_slot == slot:
                return False
            return True
    return False


def _mark_noads_auto_slot() -> None:
    global _last_noads_auto_slot
    try:
        from zoneinfo import ZoneInfo

        now = __import__("datetime").datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc) + timedelta(hours=3)
    minutes = now.hour * 60 + now.minute
    for hour in NOADS_AUTO_HOURS:
        start = hour * 60 + NOADS_AUTO_MINUTE
        if start <= minutes <= start + 40:
            _last_noads_auto_slot = f"{now.strftime('%Y-%m-%d')}-{hour:02d}"
            return
    _last_noads_auto_slot = now.strftime("%Y-%m-%d-%H")


def _auto_loop() -> None:
    """Notification/news ve Virgül ayrı kilit — hepsi ~30 dk; hata → e-posta."""
    global _auto_cycle, _last_news_auto_at, _last_virgul_auto_at, _last_play_auto_at, _last_gsc_links_auto_at, _last_policy_auto_date, _last_noads_auto_slot
    while True:
        if _nt_lock.acquire(blocking=False):
            try:
                _auto_cycle += 1
                try:
                    nt = run_notification_bridge_once()
                    if nt.get("ok"):
                        _note_auto_success("notification")
                    else:
                        _notify_auto_failure("notification", nt)
                except Exception as exc:
                    traceback.print_exc()
                    _notify_auto_failure("notification", exc=exc)

                if _should_run_news_auto():
                    try:
                        news = run_news_bridge_once()
                        _last_news_auto_at = time.time()
                        if news.get("ok"):
                            _note_auto_success("news")
                        else:
                            _notify_auto_failure("news", news)
                    except Exception as exc:
                        traceback.print_exc()
                        _last_news_auto_at = time.time()
                        _notify_auto_failure("news", exc=exc)
                else:
                    left = max(
                        0,
                        int(NEWS_AUTO_INTERVAL_SEC - (time.time() - _last_news_auto_at)),
                    )
                    print(
                        f"News auto atlandı (cycle={_auto_cycle}, "
                        f"sonraki ~{left}s / interval={NEWS_AUTO_INTERVAL_SEC}s)",
                        flush=True,
                    )
            except Exception:
                traceback.print_exc()
            finally:
                _nt_lock.release()
        else:
            print("Auto notification/news atlandı (manuel sync sürüyor)", flush=True)

        if _should_run_virgul_auto():
            if _virgul_lock.acquire(blocking=False):
                try:
                    try:
                        vg = run_virgul_bridge_once()
                        _last_virgul_auto_at = time.time()
                        if vg.get("ok"):
                            _note_auto_success("virgul")
                        else:
                            _notify_auto_failure("virgul", vg)
                    except Exception as exc:
                        traceback.print_exc()
                        _last_virgul_auto_at = time.time()
                        _notify_auto_failure("virgul", exc=exc)
                finally:
                    _virgul_lock.release()
            else:
                print("Auto Virgul atlandı (manuel virgul sync sürüyor)", flush=True)
        else:
            left_v = max(
                0,
                int(VIRGUL_AUTO_INTERVAL_SEC - (time.time() - _last_virgul_auto_at)),
            )
            print(f"Virgul auto atlandı (sonraki ~{left_v}s)", flush=True)

        if _should_run_play_auto():
            if _play_lock.acquire(blocking=False):
                try:
                    try:
                        pl = run_play_bridge_once()
                        _last_play_auto_at = time.time()
                        if pl.get("ok"):
                            _note_auto_success("play")
                        else:
                            _notify_auto_failure("play", pl)
                    except Exception as exc:
                        traceback.print_exc()
                        _last_play_auto_at = time.time()
                        _notify_auto_failure("play", exc=exc)
                finally:
                    _play_lock.release()
            else:
                print("Auto Play atlandı (manuel play sync sürüyor)", flush=True)
        else:
            left_p = max(
                0,
                int(PLAY_AUTO_INTERVAL_SEC - (time.time() - _last_play_auto_at)),
            )
            print(f"Play auto atlandı (sonraki ~{left_p}s)", flush=True)

        if _should_run_gsc_links_auto():
            if _gsc_links_lock.acquire(blocking=False):
                try:
                    try:
                        gl = run_gsc_links_bridge_once()
                        _last_gsc_links_auto_at = time.time()
                        if gl.get("ok"):
                            _note_auto_success("gsc_links")
                        else:
                            _notify_auto_failure("gsc_links", gl)
                    except Exception as exc:
                        traceback.print_exc()
                        _last_gsc_links_auto_at = time.time()
                        _notify_auto_failure("gsc_links", exc=exc)
                finally:
                    _gsc_links_lock.release()
            else:
                print("Auto GSC Links atlandı (manuel sync sürüyor)", flush=True)
        else:
            left_g = max(
                0,
                int(GSC_LINKS_AUTO_INTERVAL_SEC - (time.time() - _last_gsc_links_auto_at)),
            )
            print(f"GSC Links auto atlandı (sonraki ~{left_g}s)", flush=True)

        if _should_run_policy_auto():
            if _policy_lock.acquire(blocking=False):
                try:
                    try:
                        pol = run_admanager_policy_bridge_once()
                        try:
                            from zoneinfo import ZoneInfo

                            _last_policy_auto_date = __import__("datetime").datetime.now(
                                ZoneInfo("Europe/Istanbul")
                            ).strftime("%Y-%m-%d")
                        except Exception:
                            from datetime import datetime, timezone, timedelta

                            _last_policy_auto_date = (
                                datetime.now(timezone.utc) + timedelta(hours=3)
                            ).strftime("%Y-%m-%d")
                        if pol.get("ok"):
                            _note_auto_success("admanager_policy")
                        else:
                            _notify_auto_failure("admanager_policy", pol)
                    except Exception as exc:
                        traceback.print_exc()
                        _notify_auto_failure("admanager_policy", exc=exc)
                finally:
                    _policy_lock.release()
            else:
                print("Auto Policy atlandı (manuel sync sürüyor)", flush=True)

        if _should_run_noads_auto():
            if _noads_lock.acquire(blocking=False):
                try:
                    try:
                        nad = run_sinemalar_noads_bridge_once()
                        _mark_noads_auto_slot()
                        if nad.get("ok"):
                            _note_auto_success("sinemalar_noads")
                        else:
                            _notify_auto_failure("sinemalar_noads", nad)
                    except Exception as exc:
                        traceback.print_exc()
                        _notify_auto_failure("sinemalar_noads", exc=exc)
                finally:
                    _noads_lock.release()
            else:
                print("Auto noAds atlandı (manuel sync sürüyor)", flush=True)

        time.sleep(max(60, AUTO_INTERVAL_SEC))


def run_daemon() -> int:
    _load_dotenv()
    threading.Thread(target=_auto_loop, name="nt-bridge-auto", daemon=True).start()
    server = ThreadingHTTPServer((BRIDGE_HOST, BRIDGE_PORT), _BridgeHandler)
    news_mode = (
        f"every_n={NEWS_AUTO_EVERY_N}"
        if NEWS_AUTO_EVERY_N > 0
        else f"news_interval={NEWS_AUTO_INTERVAL_SEC}s"
    )
    print(
        f"Bridge daemon dinliyor http://{BRIDGE_HOST}:{BRIDGE_PORT} "
        f"(POST /sync | /sync-news | /sync-virgul | /sync-play | /sync-gsc-links | /sync-policy | /sync-noads | /sync-all, notify={AUTO_INTERVAL_SEC}s, "
        f"{news_mode}, virgul={VIRGUL_AUTO_INTERVAL_SEC}s, play={PLAY_AUTO_INTERVAL_SEC}s, "
        f"gsc_links={GSC_LINKS_AUTO_INTERVAL_SEC}s, policy=02:00 TR, noads={NOADS_AUTO_HOURS} TR)",
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
