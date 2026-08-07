"""Doviz.com admin — bildirim istatistikleri (login + HTML tablo).

Developer API gerekmez: DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD ile giriş yapıp
/admin/notifications/stats tablosunu çeker.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from html import unescape
from typing import Any
from urllib.parse import urljoin

import requests

from backend.config import settings

LOGGER = logging.getLogger(__name__)

DOVIZ_ADMIN_BASE = "https://www.doviz.com"
LOGIN_PATH = "/admin/login"
STATS_PATH = "/admin/notifications/stats"
DEFAULT_STATS_START = date(2016, 11, 17)

_TAG_RE = re.compile(r"<[^>]+>", re.I)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_TH_RE = re.compile(r"<th[^>]*>(.*?)</th>", re.I | re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_WS_RE = re.compile(r"\s+")


def admin_credentials_configured() -> bool:
    email = (settings.doviz_admin_email or "").strip()
    password = settings.doviz_admin_password or ""
    return bool(email and password)


def _cell_text(raw: str) -> str:
    s = unescape(_TAG_RE.sub(" ", raw or ""))
    return _WS_RE.sub(" ", s).strip()


def _fmt_tr_date(d: date) -> str:
    return f"{d.day:02d}.{d.month:02d}.{d.year}"


def _parse_login_ok(html: str, final_url: str) -> bool:
    low = (html or "").lower()
    if "/admin/login" in (final_url or "").lower() and 'name="password"' in low:
        return False
    if "hatal" in low and "şifre" in low:
        return False
    if "invalid" in low and "password" in low:
        return False
    return True


def login_admin_session(
    *,
    email: str | None = None,
    password: str | None = None,
    timeout: int = 45,
) -> requests.Session:
    """Doviz admin oturumu açar; başarısızsa ValueError."""
    user = (email if email is not None else settings.doviz_admin_email or "").strip()
    pw = password if password is not None else (settings.doviz_admin_password or "")
    if not user or not pw:
        raise ValueError(
            "DOVIZ_ADMIN_EMAIL ve DOVIZ_ADMIN_PASSWORD tanımlı olmalı "
            "(Railway Variables veya .env — sohbete yapıştırmayın)."
        )

    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; SEOAgent-NotificationSync/1.0; "
                "+https://projectcontrol.up.railway.app)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )
    login_url = urljoin(DOVIZ_ADMIN_BASE, LOGIN_PATH)
    # Cookie / olası CSRF için önce GET
    sess.get(login_url, timeout=timeout, allow_redirects=True)
    resp = sess.post(
        login_url,
        data={"email": user, "password": pw},
        timeout=timeout,
        allow_redirects=True,
    )
    if resp.status_code >= 400:
        raise ValueError(f"Admin giriş HTTP {resp.status_code}")
    if not _parse_login_ok(resp.text or "", str(resp.url)):
        raise ValueError("Admin giriş başarısız — e-posta/şifre veya oturum reddedildi.")
    # Stats’a kısa doğrulama
    probe = sess.get(urljoin(DOVIZ_ADMIN_BASE, STATS_PATH), timeout=timeout, allow_redirects=True)
    if "/admin/login" in str(probe.url).lower():
        raise ValueError("Admin giriş sonrası stats sayfasına erişilemedi (oturum yok).")
    return sess


def _stats_query_candidates(start: date, end: date) -> list[dict[str, str]]:
    """Listele formu alan adları bilinmiyor; birkaç yaygın kombinasyon dene."""
    s = _fmt_tr_date(start)
    e = _fmt_tr_date(end)
    iso_s, iso_e = start.isoformat(), end.isoformat()
    return [
        {"start": s, "end": e},
        {"start_date": s, "end_date": e},
        {"from": s, "to": e},
        {"baslangic": s, "bitis": e},
        {"start": iso_s, "end": iso_e},
        {"start_date": iso_s, "end_date": iso_e},
    ]


def fetch_stats_html(
    sess: requests.Session,
    *,
    start: date | None = None,
    end: date | None = None,
    timeout: int = 120,
) -> str:
    start = start or DEFAULT_STATS_START
    end = end or date.today()
    stats_url = urljoin(DOVIZ_ADMIN_BASE, STATS_PATH)

    best_html = ""
    best_rows = -1
    # Önce parametresiz (sayfa varsayılan aralığı)
    bare = sess.get(stats_url, timeout=timeout, allow_redirects=True)
    bare.raise_for_status()
    if "/admin/login" in str(bare.url).lower():
        raise ValueError("Stats için oturum geçersiz.")
    bare_html = bare.text or ""
    bare_n = len(_TR_RE.findall(bare_html))
    best_html, best_rows = bare_html, bare_n

    for params in _stats_query_candidates(start, end):
        try:
            resp = sess.get(stats_url, params=params, timeout=timeout, allow_redirects=True)
            if resp.status_code >= 400 or "/admin/login" in str(resp.url).lower():
                continue
            html = resp.text or ""
            n = len(_TR_RE.findall(html))
            if n > best_rows:
                best_html, best_rows = html, n
        except requests.RequestException:
            continue

    # POST Listele (bazı admin panelleri form POST kullanır)
    for data in _stats_query_candidates(start, end):
        try:
            resp = sess.post(stats_url, data=data, timeout=timeout, allow_redirects=True)
            if resp.status_code >= 400 or "/admin/login" in str(resp.url).lower():
                continue
            html = resp.text or ""
            n = len(_TR_RE.findall(html))
            if n > best_rows:
                best_html, best_rows = html, n
        except requests.RequestException:
            continue

    if best_rows < 2:
        raise ValueError("Stats HTML içinde tablo satırı bulunamadı.")
    return best_html


def parse_stats_html_to_csv(html: str) -> str:
    """Admin tablo HTML → CSV (mevcut notification parser ile uyumlu)."""
    rows_raw = _TR_RE.findall(html or "")
    if not rows_raw:
        return ""

    matrix: list[list[str]] = []
    header: list[str] | None = None
    for block in rows_raw:
        ths = [_cell_text(x) for x in _TH_RE.findall(block)]
        tds = [_cell_text(x) for x in _TD_RE.findall(block)]
        if ths and len(ths) >= 3:
            header = ths
            continue
        if not tds:
            continue
        if header is None and len(tds) >= 3 and tds[0].lower() in ("id", "text", "tarih"):
            header = tds
            continue
        matrix.append(tds)

    if not header:
        # İlk satır başlık gibi görünüyorsa kullan
        if matrix and any("id" in c.lower() or "text" in c.lower() for c in matrix[0]):
            header = matrix.pop(0)
        else:
            header = [
                "id",
                "text",
                "android app impression",
                "android app click",
                "android app ctr",
                "ios app click",
                "desktop impression",
                "desktop click",
                "desktop ctr",
                "mobileweb impression",
                "mobileweb click",
                "mobileweb ctr",
                "date",
            ]

    # CTR sütunları bazen "android app impression" diye yanlış isimlenmiş olabilir;
    # normalize: yüzde içeren 3. metrik sütununu ctr yap.
    norm_header = list(header)
    for i, h in enumerate(norm_header):
        hl = re.sub(r"[^a-z0-9]+", "", (h or "").lower())
        if hl.startswith("%") or "ctr" in hl:
            continue
        # örnek: "%2.699" hücreleri — header'ı değil

    import csv
    import io

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(norm_header)
    width = len(norm_header)
    for cols in matrix:
        if len(cols) < 2:
            continue
        # tarih yoksa atla
        row = (cols + [""] * width)[:width]
        w.writerow(row)
    return buf.getvalue()


def fetch_notification_rows_from_admin(
    *,
    force_login: bool = True,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, Any]:
    """Admin’den satırları çeker; parse_csv_text ile aynı sözlük listesini üretir."""
    from backend.services.notification_analytics_store import parse_csv_text

    t0 = datetime.utcnow()
    sess = login_admin_session()
    html = fetch_stats_html(sess, start=start, end=end)
    csv_text = parse_stats_html_to_csv(html)
    rows = parse_csv_text(csv_text)
    return {
        "ok": True,
        "parsed": len(rows),
        "rows": rows,
        "csv_chars": len(csv_text),
        "html_chars": len(html),
        "elapsed_sec": round((datetime.utcnow() - t0).total_seconds(), 2),
        "source": "doviz_admin",
        "source_url": urljoin(DOVIZ_ADMIN_BASE, STATS_PATH),
    }
