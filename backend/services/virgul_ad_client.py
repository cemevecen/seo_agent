"""Virgül panel oturumu + rapor/Excel çekimi (ofis Mac bridge).

Filtreler URL’de görünmez; oturum açıp sid bağlamında report/Excel API denenir.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, timedelta
from typing import Any
from urllib.parse import urljoin

import requests

from backend.services.virgul_ad_config import (
    VIRGUL_AD_SOURCES,
    VIRGUL_LOGIN_URL,
    VIRGUL_REPORT_URL,
    VirgulAdSource,
)

LOGGER = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def virgul_credentials() -> tuple[str, str]:
    email = (
        os.environ.get("VIRGUL_EMAIL")
        or os.environ.get("VIRGUL_USER")
        or ""
    ).strip()
    password = (os.environ.get("VIRGUL_PASSWORD") or "").strip()
    return email, password


def credentials_configured() -> bool:
    e, p = virgul_credentials()
    return bool(e and p)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": _UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )
    return s


def login_virgul(sess: requests.Session | None = None) -> requests.Session:
    email, password = virgul_credentials()
    if not email or not password:
        raise ValueError("VIRGUL_EMAIL / VIRGUL_PASSWORD gerekli (Mac .env)")
    sess = sess or _session()
    # Login sayfası cookie
    sess.get("https://rapor.virgul.com/login", timeout=45)
    resp = sess.post(
        VIRGUL_LOGIN_URL,
        data={
            "user.email": email,
            "user.password": password,
            "redirectUrl": "",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://rapor.virgul.com/login",
            "Origin": "https://rapor.virgul.com",
        },
        timeout=60,
        allow_redirects=True,
    )
    if resp.status_code >= 400:
        raise ValueError(f"Virgül login HTTP {resp.status_code}")
    low = (resp.text or "").lower()
    if "user login" in low and "user.password" in low:
        raise ValueError("Virgül login başarısız — e-posta/şifre kontrol edin")
    return sess


def select_site(sess: requests.Session, src: VirgulAdSource) -> None:
    """sid bağlamını oturuma alır (SPA site seçimi)."""
    r = sess.get(src.panel_url, timeout=60, allow_redirects=True)
    if r.status_code >= 400:
        raise ValueError(f"Virgül site açılış HTTP {r.status_code} sid={src.sid}")
    # Rapor SPA
    sess.get(VIRGUL_REPORT_URL, timeout=60, allow_redirects=True)


def _date_range_this_year(*, today: date | None = None) -> tuple[date, date]:
    today = today or date.today()
    return date(today.year, 1, 1), today


def _looks_like_xlsx(data: bytes) -> bool:
    return bool(data) and data[:2] == b"PK"


def _looks_like_csv(data: bytes) -> bool:
    if not data or data[:2] == b"PK":
        return False
    head = data[:400].decode("utf-8", errors="ignore").lower()
    return "ad unit" in head or "incometype" in head or "gelir" in head or "impression" in head


def fetch_report_export(
    sess: requests.Session,
    src: VirgulAdSource,
    *,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, Any]:
    """Excel/CSV baytlarını çekmeyi dener (birkaç bilinen endpoint).

    Başarılı olursa {"ok": True, "filename", "data", "content_type"}.
    """
    if start is None or end is None:
        start, end = _date_range_this_year()
    select_site(sess, src)

    start_s = start.isoformat()
    end_s = end.isoformat()
    # Virgül UI: Date / Month / Ad Unit / Income Type breakdown — Excel yeşil buton
    payloads: list[dict[str, Any]] = [
        {
            "startDate": start_s,
            "endDate": end_s,
            "dateStart": start_s,
            "dateEnd": end_s,
            "sid": src.sid,
            "siteId": src.sid,
            "breakdown": ["date", "month", "adUnit", "incomeType"],
            "incomeTypes": ["Open Auction", "Programmatic Direct", "Mediation", "Project"],
        },
        {
            "from": start_s,
            "to": end_s,
            "sid": src.sid,
            "export": "excel",
        },
    ]
    endpoints = [
        "/npm/api/report/excel",
        "/npm/api/excel",
        "/npm/api/report/export",
        "/npm/api/report",
        "/npm/excel",
        "/api/report/excel",
    ]
    base = "https://rapor.virgul.com"
    last_err = ""
    for path in endpoints:
        url = urljoin(base + "/", path.lstrip("/"))
        for method in ("POST", "GET"):
            for body in payloads if method == "POST" else [None]:
                try:
                    if method == "POST":
                        resp = sess.post(
                            url,
                            json=body,
                            headers={
                                "Referer": VIRGUL_REPORT_URL,
                                "Origin": "https://rapor.virgul.com",
                                "X-Requested-With": "XMLHttpRequest",
                            },
                            timeout=180,
                        )
                    else:
                        resp = sess.get(
                            url,
                            params={
                                "sid": src.sid,
                                "startDate": start_s,
                                "endDate": end_s,
                            },
                            headers={"Referer": VIRGUL_REPORT_URL},
                            timeout=180,
                        )
                except requests.RequestException as exc:
                    last_err = str(exc)
                    continue
                data = resp.content or b""
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if resp.status_code < 400 and (_looks_like_xlsx(data) or _looks_like_csv(data)):
                    ext = "xlsx" if _looks_like_xlsx(data) else "csv"
                    return {
                        "ok": True,
                        "filename": f"virgul_{src.sid}.{ext}",
                        "data": data,
                        "content_type": ctype,
                        "endpoint": url,
                        "method": method,
                        "bytes": len(data),
                        "start": start_s,
                        "end": end_s,
                    }
                last_err = f"{method} {url} → HTTP {resp.status_code} ({ctype[:40]})"
                # JSON hata gövdesi
                if "json" in ctype:
                    try:
                        last_err += f" {resp.json()}"
                    except Exception:
                        last_err += f" {(resp.text or '')[:120]}"

    return {
        "ok": False,
        "message": (
            "Virgül Excel/API export bulunamadı. Ofiste DevTools→Network’te "
            "yeşil Excel’e basınca çıkan isteği kaydedin. "
            f"Son hata: {last_err[:240]}"
        ),
        "sid": src.sid,
        "start": start_s,
        "end": end_s,
    }


def fetch_all_sites_exports(
    *,
    start: date | None = None,
    end: date | None = None,
    stream_key: str | None = None,
) -> dict[str, Any]:
    sess = login_virgul()
    sources = VIRGUL_AD_SOURCES
    if stream_key:
        sources = tuple(s for s in sources if s.stream_key == stream_key)
    items: list[dict[str, Any]] = []
    ok_n = 0
    for src in sources:
        try:
            item = fetch_report_export(sess, src, start=start, end=end)
            item["stream_key"] = src.stream_key
            item["label"] = src.label
            item["sid"] = src.sid
            items.append(item)
            if item.get("ok"):
                ok_n += 1
            else:
                LOGGER.warning("Virgul export failed %s: %s", src.sid, item.get("message"))
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Virgul export error %s", src.sid)
            items.append(
                {
                    "ok": False,
                    "sid": src.sid,
                    "stream_key": src.stream_key,
                    "label": src.label,
                    "message": str(exc),
                }
            )
    return {
        "ok": ok_n > 0,
        "ok_count": ok_n,
        "fail_count": len(items) - ok_n,
        "items": items,
        "start": (start or _date_range_this_year()[0]).isoformat(),
        "end": (end or _date_range_this_year()[1]).isoformat(),
    }
