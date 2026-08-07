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

# Notification/news aynı admin oturumunu paylaşır; Virgül ayrı — uzun Excel
# sync'i Elle yenile'yi 409 ile kilitlemesin.
_nt_lock = threading.Lock()
_virgul_lock = threading.Lock()
_last_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_news_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
_last_virgul_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}
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
_last_fail_email_at: dict[str, float] = {}


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
    """Başarısız auto sync → e-posta (kind başına cooldown)."""
    now = time.time()
    last = float(_last_fail_email_at.get(kind) or 0)
    cooldown = max(300, BRIDGE_ALERT_COOLDOWN_SEC)
    if last and (now - last) < cooldown:
        left = int(cooldown - (now - last))
        print(f"Bridge alert cooldown ({kind}) · ~{left}s", flush=True)
        return
    msg = ""
    if exc is not None:
        msg = str(exc) or exc.__class__.__name__
    elif isinstance(result, dict):
        msg = str(result.get("message") or result.get("detail") or result)
    msg = (msg or "bilinmeyen hata")[:800]
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
        f"Hata: {msg}\n\n"
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


def run_virgul_bridge_once() -> dict[str, Any]:
    """Virgül 6 sid Excel/CSV → Railway /ad-virgul ingest."""
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

    url = _virgul_ingest_url()
    token = _ingest_token()
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps({"files": files, "replace": False, "source": "virgul_bridge"}),
        timeout=300,
    )
    print(f"Virgul ingest HTTP {resp.status_code}", flush=True)
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
        "kind": "virgul",
        "http_status": resp.status_code,
        "files": len(files),
        "message": msg or ("OK" if ok else "Ingest başarısız"),
        "body": body if isinstance(body, dict) else {},
    }
    _last_virgul_result = out
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


def _auto_loop() -> None:
    """Notification/news ve Virgül ayrı kilit — hepsi ~30 dk; hata → e-posta."""
    global _auto_cycle, _last_news_auto_at, _last_virgul_auto_at
    while True:
        if _nt_lock.acquire(blocking=False):
            try:
                _auto_cycle += 1
                try:
                    nt = run_notification_bridge_once()
                    if not nt.get("ok"):
                        _notify_auto_failure("notification", nt)
                except Exception as exc:
                    traceback.print_exc()
                    _notify_auto_failure("notification", exc=exc)

                if _should_run_news_auto():
                    try:
                        news = run_news_bridge_once()
                        _last_news_auto_at = time.time()
                        if not news.get("ok"):
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
                        if not vg.get("ok"):
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
        f"(POST /sync | /sync-news | /sync-virgul | /sync-all, notify={AUTO_INTERVAL_SEC}s, "
        f"{news_mode}, virgul={VIRGUL_AUTO_INTERVAL_SEC}s)",
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
    lock = _virgul_lock if virgul_only else _nt_lock
    if not lock.acquire(blocking=False):
        print("Sync zaten çalışıyor", file=sys.stderr)
        return 1
    try:
        if "--news-only" in args:
            result = run_news_bridge_once()
        elif virgul_only:
            result = run_virgul_bridge_once()
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
