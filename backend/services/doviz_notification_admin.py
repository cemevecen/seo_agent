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
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

_TAG_RE = re.compile(r"<[^>]+>", re.I)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_TH_RE = re.compile(r"<th[^>]*>(.*?)</th>", re.I | re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_WS_RE = re.compile(r"\s+")


def admin_base_url() -> str:
    base = (getattr(settings, "doviz_admin_base_url", None) or DOVIZ_ADMIN_BASE).strip()
    return base.rstrip("/") or DOVIZ_ADMIN_BASE


def admin_credentials_configured() -> bool:
    email = (settings.doviz_admin_email or "").strip()
    password = settings.doviz_admin_password or ""
    return bool(email and password)


def _cell_text(raw: str) -> str:
    s = unescape(_TAG_RE.sub(" ", raw or ""))
    return _WS_RE.sub(" ", s).strip()


def _fmt_tr_date(d: date) -> str:
    return f"{d.day:02d}.{d.month:02d}.{d.year}"


def _looks_like_login_page(html: str, url: str) -> bool:
    low = (html or "").lower()
    if "/admin/login" in (url or "").lower() and 'name="password"' in low:
        return True
    return 'name="password"' in low and 'name="email"' in low and "login" in low


def _login_rejected(html: str) -> bool:
    raw = html or ""
    low = raw.lower()
    if "hatalı e-mail" in low or "hatali e-mail" in low:
        return True
    if "hatalı" in low and "şifre" in low:
        return True
    if "invalid" in low and ("password" in low or "email" in low):
        return True
    return False


def _session_seems_authenticated(sess: requests.Session) -> bool:
    """Stats’a bak: login’e düşmüyorsa oturum var say."""
    base = admin_base_url()
    stats_url = urljoin(base + "/", STATS_PATH.lstrip("/"))
    try:
        probe = sess.get(stats_url, timeout=45, allow_redirects=True)
    except requests.RequestException as exc:
        LOGGER.warning("Admin stats probe failed: %s", exc)
        return False
    final = str(probe.url or "").lower()
    if "/admin/login" in final:
        return False
    if _looks_like_login_page(probe.text or "", final):
        return False
    # 200 + tablo, veya en azından login değil
    if probe.status_code == 200 and ("<table" in (probe.text or "").lower() or "notification" in final):
        return True
    if probe.status_code == 200 and not _looks_like_login_page(probe.text or "", final):
        return True
    return False


def login_admin_session(
    *,
    email: str | None = None,
    password: str | None = None,
    timeout: int = 45,
) -> requests.Session:
    """Doviz admin oturumu açar; başarısızsa ValueError.

    Not: Bazı başarılı girişlerde landing page 404 dönebiliyor; oturum çerezi
    varsa stats doğrulaması esas alınır (HTTP 404 yüzünden düşülmez).
    """
    user = (email if email is not None else settings.doviz_admin_email or "").strip()
    pw = password if password is not None else (settings.doviz_admin_password or "")
    if not user or not pw:
        raise ValueError(
            "DOVIZ_ADMIN_EMAIL ve DOVIZ_ADMIN_PASSWORD tanımlı olmalı "
            "(Railway Variables veya .env — sohbete yapıştırmayın)."
        )

    base = admin_base_url()
    login_url = urljoin(base + "/", LOGIN_PATH.lstrip("/"))
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )

    # Cookie / form için GET (trailing slash varyantı da dene)
    warm = None
    last_get_err: Exception | None = None
    for candidate in (login_url, login_url.rstrip("/") + "/", login_url.rstrip("/")):
        try:
            warm = sess.get(candidate, timeout=timeout, allow_redirects=True)
            if warm.status_code < 400 and not _looks_like_login_page(warm.text or "", str(warm.url)):
                # login formu yoksa diğer adaya bak
                if 'name="password"' not in (warm.text or "").lower():
                    continue
            if warm.status_code < 400:
                login_url = str(warm.url) or candidate
                break
        except requests.RequestException as exc:
            last_get_err = exc
            warm = None
    if warm is None:
        raise ValueError(f"Admin login sayfası açılamadı: {last_get_err or 'bilinmeyen hata'}")
    if warm.status_code >= 400:
        raise ValueError(f"Admin login sayfası HTTP {warm.status_code}")

    # Hidden alanları (CSRF vb.) koru; email/password üzerine yaz
    form_data: dict[str, str] = {"email": user, "password": pw}
    for m in re.finditer(
        r'<input[^>]+type=["\']hidden["\'][^>]*>',
        warm.text or "",
        re.I,
    ):
        tag = m.group(0)
        nm = re.search(r'name=["\']([^"\']+)', tag, re.I)
        val = re.search(r'value=["\']([^"\']*)', tag, re.I)
        if nm and nm.group(1) not in form_data:
            form_data[nm.group(1)] = val.group(1) if val else ""

    post_headers = {
        "Origin": base,
        "Referer": login_url,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Redirect zincirindeki 404 landing’e takılmamak için önce no-follow
    try:
        resp = sess.post(
            login_url,
            data=form_data,
            timeout=timeout,
            allow_redirects=False,
            headers=post_headers,
        )
    except requests.RequestException as exc:
        raise ValueError(f"Admin giriş isteği başarısız: {exc}") from exc

    status = int(resp.status_code or 0)
    body = resp.text or ""
    loc = (resp.headers.get("Location") or "").strip()

    if status == 200:
        if _login_rejected(body):
            raise ValueError("Hatalı e-mail veya şifre")
        if _looks_like_login_page(body, str(resp.url)):
            raise ValueError("Admin giriş başarısız — login sayfasında kaldı.")
    elif status in (301, 302, 303, 307, 308):
        if loc:
            try:
                # Landing 404 olsa bile çerezler session’da kalır — HTTP 404 ile düşme
                landed = sess.get(urljoin(login_url, loc), timeout=timeout, allow_redirects=True)
                LOGGER.info(
                    "Admin login redirect -> %s HTTP %s",
                    landed.url,
                    landed.status_code,
                )
            except requests.RequestException as exc:
                LOGGER.warning("Admin login redirect follow failed: %s", exc)
    elif status >= 400:
        # Eski hata: başarılı giriş sonrası 404 landing → "Admin giriş HTTP 404"
        # Artık hemen düşmüyoruz; stats ile doğrula.
        LOGGER.warning(
            "Admin login POST HTTP %s (cookies=%s loc=%s); verifying via stats",
            status,
            bool(sess.cookies),
            loc or "-",
        )
        if loc:
            try:
                sess.get(urljoin(login_url, loc), timeout=timeout, allow_redirects=True)
            except requests.RequestException:
                pass
    else:
        LOGGER.info("Admin login POST unexpected status %s", status)

    if not _session_seems_authenticated(sess):
        # Bazı kurulumlar tek POST sonrası 200+boş döner; bir kez daha follow’lu dene
        try:
            resp2 = sess.post(
                login_url,
                data=form_data,
                timeout=timeout,
                allow_redirects=True,
                headers=post_headers,
            )
            if _login_rejected(resp2.text or ""):
                raise ValueError("Hatalı e-mail veya şifre")
            # Follow sonrası 404 landing olsa bile stats’a bakacağız
            if resp2.status_code >= 400:
                LOGGER.warning(
                    "Admin login follow POST HTTP %s at %s",
                    resp2.status_code,
                    resp2.url,
                )
        except ValueError:
            raise
        except requests.RequestException as exc:
            raise ValueError(f"Admin giriş doğrulanamadı: {exc}") from exc
        if not _session_seems_authenticated(sess):
            raise ValueError(
                "Admin oturumu doğrulanamadı (stats hâlâ login’e düşüyor). "
                f"POST={status}"
                + (f", Location={loc}" if loc else "")
                + ". Railway’de DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD "
                "ve gerekirse DOVIZ_ADMIN_BASE_URL değerlerini kontrol edin."
            )
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
    stats_url = urljoin(admin_base_url() + "/", STATS_PATH.lstrip("/"))

    best_html = ""
    best_rows = -1
    # Önce parametresiz (sayfa varsayılan aralığı)
    bare = sess.get(stats_url, timeout=timeout, allow_redirects=True)
    bare.raise_for_status()
    if "/admin/login" in str(bare.url).lower() or _looks_like_login_page(bare.text or "", str(bare.url)):
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
        "source_url": urljoin(admin_base_url() + "/", STATS_PATH.lstrip("/")),
    }
