#!/usr/bin/env python3
"""Doviz admin → Railway bridge (VPN makinesinde çalışır).

Railway sunucusu /admin/notifications/stats’e giremez (VPN/IP).
Bu script VPN’li bir makinede periyodik çalışır, admin’den çeker,
Railway’e POST eder. UI’da bilgi girmeniz gerekmez.

Kurulum (bir kez):
  1) Railway Variables:
       NOTIFICATION_INGEST_TOKEN=<uzun-rastgele-token>
       (opsiyonel) DOVIZ_ADMIN_* Railway’de de olabilir; bridge kendi .env’ini kullanır
  2) VPN’li makinede .env veya ortam:
       DOVIZ_ADMIN_EMAIL=...
       DOVIZ_ADMIN_PASSWORD=...
       NOTIFICATION_INGEST_TOKEN=<aynı-token>
       NOTIFICATION_INGEST_URL=https://projectcontrol.up.railway.app/api/notification-analytics/ingest
  3) Cron / launchd her 15 dk:
       cd /path/to/seo_agent && .venv/bin/python scripts/doviz_admin_notification_bridge.py

Tek seferlik test:
  .venv/bin/python scripts/doviz_admin_notification_bridge.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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


def main() -> int:
    _load_dotenv()
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    url = (
        os.environ.get("NOTIFICATION_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/notification-analytics/ingest"
    ).strip()
    if not token:
        print("NOTIFICATION_INGEST_TOKEN gerekli", file=sys.stderr)
        return 2
    if not (os.environ.get("DOVIZ_ADMIN_EMAIL") and os.environ.get("DOVIZ_ADMIN_PASSWORD")):
        print("DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD gerekli (VPN makinesi .env)", file=sys.stderr)
        return 2

    from backend.services.doviz_notification_admin import fetch_notification_rows_from_admin

    print("Admin stats çekiliyor…")
    fetched = fetch_notification_rows_from_admin()
    rows = fetched.get("rows") or []
    print(f"Çekildi: {len(rows)} satır · {fetched.get('elapsed_sec')}s")
    if not rows:
        print("Satır yok — gönderilmedi", file=sys.stderr)
        return 1

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
    print(f"Ingest HTTP {resp.status_code}")
    try:
        body = resp.json()
    except Exception:
        print(resp.text[:500])
        return 1 if resp.status_code >= 400 else 0
    print(body.get("message") or body)
    return 0 if resp.status_code < 400 and body.get("synced") is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
