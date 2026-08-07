#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde).

Tek sefer:
  .venv/bin/python scripts/doviz_admin_notification_bridge.py

Daemon (otomatik 15 dk + Elle yenile için localhost:18765):
  .venv/bin/python scripts/doviz_admin_notification_bridge.py --daemon
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
AUTO_INTERVAL_SEC = int(os.environ.get("NOTIFICATION_BRIDGE_INTERVAL_SEC") or str(15 * 60))

_sync_lock = threading.Lock()
_last_result: dict[str, Any] = {"ok": False, "message": "henüz çalışmadı"}


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


def run_bridge_once() -> dict[str, Any]:
    """Admin stats çek → Railway ingest. Dönüş: UI/daemon için JSON özet."""
    global _last_result
    _load_dotenv()
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    url = (
        os.environ.get("NOTIFICATION_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/notification-analytics/ingest"
    ).strip()
    if not token:
        out = {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
        _last_result = out
        return out
    if not (os.environ.get("DOVIZ_ADMIN_EMAIL") and os.environ.get("DOVIZ_ADMIN_PASSWORD")):
        out = {"ok": False, "message": "DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD gerekli"}
        _last_result = out
        return out

    from backend.services.doviz_notification_admin import fetch_notification_rows_from_admin

    print("Admin stats çekiliyor…", flush=True)
    fetched = fetch_notification_rows_from_admin()
    rows = fetched.get("rows") or []
    print(f"Çekildi: {len(rows)} satır · {fetched.get('elapsed_sec')}s", flush=True)
    if not rows:
        out = {"ok": False, "message": "Satır yok — gönderilmedi", "parsed": 0}
        _last_result = out
        return out

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
    print(f"Ingest HTTP {resp.status_code}", flush=True)
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


def _cors_headers(handler: BaseHTTPRequestHandler) -> dict[str, str]:
    origin = handler.headers.get("Origin") or "*"
    # Yerel UI + production panelinden Elle yenile
    allowed = {
        "http://127.0.0.1:8012",
        "http://localhost:8012",
        "https://projectcontrol.up.railway.app",
    }
    allow = origin if origin in allowed or origin == "null" else (
        origin if origin.startswith("http://127.0.0.1:") or origin.startswith("http://localhost:") else "https://projectcontrol.up.railway.app"
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
                    "service": "doviz-admin-notification-bridge",
                    "auto_interval_sec": AUTO_INTERVAL_SEC,
                    "last": _last_result,
                },
            )
            return
        self._send(404, {"ok": False, "message": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in ("/sync", "/run", "/"):
            self._send(404, {"ok": False, "message": "not found"})
            return
        if not _sync_lock.acquire(blocking=False):
            self._send(409, {"ok": False, "message": "Sync zaten çalışıyor, bekleyin."})
            return
        try:
            result = run_bridge_once()
            self._send(200 if result.get("ok") else 502, result)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self._send(500, {"ok": False, "message": str(exc)})
        finally:
            _sync_lock.release()


def _auto_loop() -> None:
    # İlk sync hemen; sonra interval
    while True:
        if _sync_lock.acquire(blocking=False):
            try:
                run_bridge_once()
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
    print(
        f"Bridge daemon dinliyor http://{BRIDGE_HOST}:{BRIDGE_PORT} "
        f"(POST /sync = Elle yenile, auto={AUTO_INTERVAL_SEC}s)",
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
        result = run_bridge_once()
        return 0 if result.get("ok") else 1
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        traceback.print_exc()
        return 1
    finally:
        _sync_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
