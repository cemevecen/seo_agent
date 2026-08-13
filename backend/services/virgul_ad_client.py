"""Virgül panel oturumu + rapor/Excel çekimi (ofis Mac bridge).

Filtreler URL’de görünmez; oturum açıp sid bağlamında report/Excel API denenir.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, timedelta
from typing import Any

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


def _fmt_tr_date(d: date) -> str:
    """Virgül form tarihleri: dd.mm.yyyy"""
    return d.strftime("%d.%m.%Y")


def fetch_report_export(
    sess: requests.Session,
    src: VirgulAdSource,
    *,
    start: date | None = None,
    end: date | None = None,
    report_type: str = "ty",
) -> dict[str, Any]:
    """Yeşil Excel = form POST /npm/report (operation=excel).

    Firefox Network: POST https://rapor.virgul.com/npm/report → xlsx attachment.
    """
    if start is None or end is None:
        start, end = _date_range_this_year()
    select_site(sess, src)

    start_tr = _fmt_tr_date(start)
    end_tr = _fmt_tr_date(end)
    # reportType=ty → This Year; boş + start/end → özel aralık
    form: list[tuple[str, str]] = [
        ("order", "-date"),
        ("operation", "excel"),
        ("limit", "1000000000"),
        ("offset", "0"),
        ("reportType", (report_type or "").strip()),
        ("startDate", start_tr),
        ("endDate", end_tr),
        # Income Type: Open Auction, Programmatic Direct, Mediation, Project
        ("categories[]", "1"),
        ("categories[]", "2"),
        ("categories[]", "10"),
        ("categories[]", "11"),
        # Breakdown: Date, Month, Ad Unit, Income Type
        ("day", "true"),
        ("month", "true"),
        ("category", "true"),
        ("embedAd", "true"),
    ]
    url = VIRGUL_REPORT_URL
    try:
        resp = sess.post(
            url,
            data=form,
            headers={
                "Referer": VIRGUL_REPORT_URL,
                "Origin": "https://rapor.virgul.com",
            },
            # connect hızlı kessin; Excel üretimi yavaş olabiliyor (yıllık rapor)
            timeout=(30, 300),
        )
    except requests.RequestException as exc:
        return {
            "ok": False,
            "message": f"Virgül Excel POST hatası: {exc}",
            "sid": src.sid,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    data = resp.content or b""
    ctype = (resp.headers.get("Content-Type") or "").lower()
    cd = resp.headers.get("Content-Disposition") or ""
    if resp.status_code < 400 and _looks_like_xlsx(data):
        fname = f"virgul_{src.sid}.xlsx"
        m = re.search(r'filename=([^;\s]+)', cd, flags=re.I)
        if m:
            raw_name = m.group(1).strip().strip('"').strip("'")
            if raw_name.lower().endswith(".xlsx"):
                fname = f"virgul_{src.sid}_{raw_name}"
        return {
            "ok": True,
            "filename": fname,
            "data": data,
            "content_type": ctype,
            "endpoint": url,
            "method": "POST",
            "bytes": len(data),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "report_type": report_type,
        }

    return {
        "ok": False,
        "message": (
            f"Virgül Excel beklenen xlsx gelmedi: HTTP {resp.status_code} "
            f"({ctype[:60]}) {(resp.text or '')[:160]}"
        ),
        "sid": src.sid,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def fetch_all_sites_exports(
    *,
    start: date | None = None,
    end: date | None = None,
    stream_key: str | None = None,
    on_progress=None,
) -> dict[str, Any]:
    def _prog(info: dict[str, Any]) -> None:
        if not callable(on_progress):
            return
        try:
            on_progress(info)
        except Exception:
            pass

    _prog({"phase": "login", "sub_label": "Virgül login", "step": 0, "total_steps": 0, "message": "Virgül login…"})
    sess = login_virgul()
    sources = VIRGUL_AD_SOURCES
    if stream_key:
        sources = tuple(s for s in sources if s.stream_key == stream_key)
    total = len(sources)
    items: list[dict[str, Any]] = []
    ok_n = 0
    for i, src in enumerate(sources, start=1):
        label = src.label or src.stream_key
        print(f"  · Virgül export {i}/{total} · {label} ({src.stream_key})…", flush=True)
        _prog(
            {
                "phase": "export",
                "sub_label": f"Excel {label}",
                "step": i,
                "total_steps": total,
                "message": f"Virgül Excel {i}/{total} · {label}",
                "platform": src.stream_key,
            }
        )
        try:
            item = fetch_report_export(sess, src, start=start, end=end)
            item["stream_key"] = src.stream_key
            item["label"] = src.label
            item["sid"] = src.sid
            items.append(item)
            if item.get("ok"):
                ok_n += 1
                print(
                    f"    → ok · {item.get('bytes') or 0} byte · {src.stream_key}",
                    flush=True,
                )
            else:
                LOGGER.warning("Virgul export failed %s: %s", src.sid, item.get("message"))
                print(f"    → fail · {item.get('message')}", flush=True)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Virgul export error %s", src.sid)
            print(f"    → error · {exc}", flush=True)
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
