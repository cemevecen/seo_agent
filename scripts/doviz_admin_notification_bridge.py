#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde).

Notification stats + aktif haber listesi.

Tek sefer (ikisi):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --news-only
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --notifications-only

Daemon (otomatik + Elle yenile localhost:18765):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --daemon

  POST /sync       → notification (~15 dk auto)
  POST /sync-news  → aktif haberler (pagination, ~30 dk auto)
  POST /sync-all   → ikisi
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = int(os.environ.get("NOTIFICATION_BRIDGE_PORT") or "18765")
# Bildirim stats — sık (varsayılan 15 dk)
AUTO_INTERVAL_SEC = int(os.environ.get("NOTIFICATION_BRIDGE_INTERVAL_SEC") or str(15 * 60))
# Aktif haberler (uzun pagination) — varsayılan 30 dk’da bir
NEWS_AUTO_INTERVAL_SEC = int(
    os.environ.get("NEWS_BRIDGE_INTERVAL_SEC") or str(30 * 60)
)
# Virgül reklam — varsayılan 6 saatte bir (Excel ağır)
VIRGUL_AUTO_INTERVAL_SEC = int(
    os.environ.get("VIRGUL_BRIDGE_INTERVAL_SEC") or str(6 * 60 * 60)
)
# Eski ayar: her N. bildirim turunda haber (NEWS_BRIDGE_INTERVAL_SEC yoksa)
_NEWS_EVERY_N_RAW = (os.environ.get("NEWS_BRIDGE_EVERY_N") or "").strip()
NEWS_AUTO_EVERY_N = int(_NEWS_EVERY_N_RAW) if _NEWS_EVERY_N_RAW.isdigit() else 0

_sync_lock = threading.Lock()
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
_auto_cycle = 0
_last_news_auto_at = 0.0
_last_virgul_auto_at = 0.0


def _set_news_progress(**kwargs: Any) -> None:
    _news_progress.update(kwargs)
    _news_progress["ts"] = time.time()


def _news_pages_estimate() -> int:
    last = int(_last_news_result.get("last_page") or 0)
    env = int(os.environ.get("NEWS_PAGES_ESTIMATE") or "264")
    return max(last, env, 1)


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip("'").strip('"')
        if k and k not in os.environ:
            os.environ[k] = v


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
        data=json.dumps({"files": files, "replace": True, "source": "virgul_bridge"}),
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
        return err

    from backend.services.doviz_notification_admin import fetch_notification_rows_from_admin

    print("Admin stats çekiliyor…", flush=True)
    fetched = fetch_notification_rows_from_admin()
    rows = fetched.get("rows") or []
    print(f"Notification çekildi: {len(rows)} satır · {fetched.get('elapsed_sec')}s", flush=True)
    if not rows:
        out = {"ok": False, "message": "Notification: satır yok — gönderilmedi", "parsed": 0}
        _last_result = out
        return out

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
    return out


def run_news_bridge_once() -> dict[str, Any]:
    """Admin aktif haberler (pagination) → Railway ingest."""
    global _last_news_result
    _load_dotenv()
    err = _require_creds()
    if err:
        _last_news_result = err
        _set_news_progress(running=False, phase="error", message=err.get("message") or "")
        return err

    from backend.services.doviz_news_admin import fetch_active_news_rows_from_admin

    estimate = _news_pages_estimate()
    _set_news_progress(
        running=True,
        phase="scrape",
        page=0,
        total_pages=estimate,
        rows=0,
        message="Admin login…",
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

    print("Admin aktif haberler çekiliyor (pagination)…", flush=True)
    fetched = fetch_active_news_rows_from_admin(
        estimate_pages=estimate,
        on_progress=_on_progress,
    )
    rows = fetched.get("rows") or []
    total_pages = int(fetched.get("last_page") or fetched.get("pages") or estimate)
    print(
        f"News çekildi: {len(rows)} satır · {fetched.get('pages')} sayfa · "
        f"{fetched.get('elapsed_sec')}s",
        flush=True,
    )
    if not rows:
        out = {"ok": False, "message": "News: satır yok — gönderilmedi", "parsed": 0}
        _last_news_result = out
        _set_news_progress(running=False, phase="error", message=out["message"])
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
        "fetched_at": body.get("fetched_at") if isinstance(body, dict) else None,
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
                },
            )
            return
        if path in ("/news-progress", "/progress-news"):
            self._send(200, {"ok": True, **dict(_news_progress)})
            return
        self._send(404, {"ok": False, "message": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/sync", "/run"):
            runner = run_notification_bridge_once
        elif path in ("/sync-news", "/news"):
            runner = run_news_bridge_once
        elif path in ("/sync-virgul", "/virgul"):
            runner = run_virgul_bridge_once
        elif path in ("/sync-all", "/all"):
            runner = run_all_once
        elif path == "/":
            runner = run_notification_bridge_once
        else:
            self._send(404, {"ok": False, "message": "not found"})
            return
        if not _sync_lock.acquire(blocking=False):
            self._send(409, {"ok": False, "message": "Sync zaten çalışıyor, bekleyin."})
            return
        try:
            result = runner()
            self._send(200 if result.get("ok") else 502, result)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self._send(500, {"ok": False, "message": str(exc)})
        finally:
            _sync_lock.release()


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
    global _auto_cycle, _last_news_auto_at, _last_virgul_auto_at
    while True:
        if _sync_lock.acquire(blocking=False):
            try:
                _auto_cycle += 1
                run_notification_bridge_once()
                if _should_run_news_auto():
                    run_news_bridge_once()
                    _last_news_auto_at = time.time()
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
                if _should_run_virgul_auto():
                    run_virgul_bridge_once()
                    _last_virgul_auto_at = time.time()
                else:
                    left_v = max(
                        0,
                        int(VIRGUL_AUTO_INTERVAL_SEC - (time.time() - _last_virgul_auto_at)),
                    )
                    print(f"Virgul auto atlandı (sonraki ~{left_v}s)", flush=True)
            except Exception:
                traceback.print_exc()
            finally:
                _sync_lock.release()
        else:
            print("Auto-sync atlandı (manuel sync sürüyor)", flush=True)
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
    if not _sync_lock.acquire(blocking=False):
        print("Sync zaten çalışıyor", file=sys.stderr)
        return 1
    try:
        if "--news-only" in args:
            result = run_news_bridge_once()
        elif "--virgul-only" in args:
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
        _sync_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
