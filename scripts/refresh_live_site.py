#!/usr/bin/env python3
"""Commit/push sonrasinda canli siteyi otomatik yenilemek icin ping helper."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from urllib.request import Request, urlopen


def load_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def env_value(key: str, defaults: dict[str, str], fallback: str = "") -> str:
    return os.getenv(key, defaults.get(key, fallback)).strip()


def parse_bool(value: str, default: bool = True) -> bool:
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def add_cache_buster(url: str, trigger: str) -> str:
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["refresh_ts"] = str(int(time.time()))
    params["refresh_from"] = trigger
    query = urlencode(params)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))


def ping_url(url: str, method: str, timeout: int) -> tuple[bool, str]:
    req = Request(url=url, method=method.upper())
    try:
        with urlopen(req, timeout=timeout) as resp:
            return True, f"{resp.status} {url}"
    except HTTPError as exc:
        return False, f"HTTP {exc.code} {url}"
    except URLError as exc:
        return False, f"URL error {exc.reason} {url}"
    except Exception as exc:  # noqa: BLE001
        return False, f"error {exc} {url}"


def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "manual"
    repo_root = Path(__file__).resolve().parent.parent
    env_defaults = load_env_file(repo_root / ".env")

    enabled = parse_bool(env_value("LIVE_REFRESH_ENABLED", env_defaults, "true"), default=True)
    if not enabled:
        print("[refresh] disabled")
        return 0

    urls_raw = env_value("LIVE_REFRESH_URLS", env_defaults, "")
    if not urls_raw:
        app_host = env_value("APP_HOST", env_defaults, "127.0.0.1")
        urls = [f"http://{app_host}:8012/health", f"http://{app_host}:8012/"]
    else:
        urls = [item.strip() for item in urls_raw.split(",") if item.strip()]

    method = env_value("LIVE_REFRESH_METHOD", env_defaults, "GET").upper()
    timeout_str = env_value("LIVE_REFRESH_TIMEOUT", env_defaults, "8")
    try:
        timeout = max(2, int(timeout_str))
    except ValueError:
        timeout = 8

    ok_any = False
    for raw_url in urls:
        target = add_cache_buster(raw_url, trigger)
        ok, message = ping_url(target, method, timeout)
        ok_any = ok_any or ok
        state = "ok" if ok else "fail"
        print(f"[refresh] {state} {message}")

    if not ok_any:
        print("[refresh] warning: no refresh endpoint succeeded")

    # Commit/push surecini bloklamamak icin her durumda 0 dondur.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
