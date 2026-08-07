"""Doviz.com admin — bildirim istatistikleri (login + HTML tablo).

Developer API gerekmez: DOVIZ_ADMIN_EMAIL / DOVIZ_ADMIN_PASSWORD ile giriş yapıp
/admin/notifications/stats tablosunu çeker.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

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
# Kullanıcı bazen login/stats tam URL’sini BASE olarak yapıştırıyor; path’i at.
_BASE_PATH_STRIP = (
    "/admin/notifications/stats",
    "/admin/login",
    "/admin/",
    "/admin",
)

_TAG_RE = re.compile(r"<[^>]+>", re.I)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_TH_RE = re.compile(r"<th[^>]*>(.*?)</th>", re.I | re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.I | re.S)
_WS_RE = re.compile(r"\s+")


def _origin_from_url(raw: str) -> str | None:
    text = (raw or "").strip()
    if not text:
        return None
    if "://" not in text:
        text = "https://" + text
    parsed = urlparse(text)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")


def admin_base_url() -> str:
    """Doviz admin origin (scheme+host). Tam login/stats URL’si verilse bile origin’e iner.

    Railway/localhost gibi bu uygulamanın host’u DOVIZ_ADMIN_BASE_URL olarak
    yapıştırılırsa /admin/login/admin/login → HTTP 404 oluşur; bunları yok say.
    """
    raw = (getattr(settings, "doviz_admin_base_url", None) or DOVIZ_ADMIN_BASE).strip()
    lowered = raw.lower()
    for suffix in _BASE_PATH_STRIP:
        if lowered.endswith(suffix):
            raw = raw[: -len(suffix)]
            lowered = raw.lower()
            break

    origin = _origin_from_url(raw) or DOVIZ_ADMIN_BASE
    host = (urlparse(origin).netloc or "").lower()
    if (
        not host
        or "doviz.com" not in host
        or host.endswith("railway.app")
        or host in ("localhost", "127.0.0.1")
        or host.startswith("127.0.0.1:")
        or host.startswith("localhost:")
    ):
        if raw and raw.rstrip("/") != DOVIZ_ADMIN_BASE:
            LOGGER.warning(
                "DOVIZ_ADMIN_BASE_URL geçersiz/host uyumsuz (%r) — %s kullanılıyor",
                raw,
                DOVIZ_ADMIN_BASE,
            )
        return DOVIZ_ADMIN_BASE
    return origin


def login_url_candidates() -> list[str]:
    """Denenecek login URL’leri (çift /admin/login üretmeden)."""
    primary = admin_base_url()
    origins = [primary]
    for extra in (DOVIZ_ADMIN_BASE, "https://admin.doviz.com", "https://doviz.com"):
        o = _origin_from_url(extra)
        if o and o not in origins:
            origins.append(o)
    out: list[str] = []
    for origin in origins:
        for path in (LOGIN_PATH, LOGIN_PATH + "/"):
            u = urljoin(origin + "/", path.lstrip("/"))
            if u not in out:
                out.append(u)
    return out


def stats_url() -> str:
    return urljoin(admin_base_url() + "/", STATS_PATH.lstrip("/"))


def admin_http_proxy() -> str:
    """VPN çıkışı için opsiyonel proxy (Railway doğrudan admin’e giremez)."""
    for key in (
        getattr(settings, "doviz_admin_http_proxy", None) or "",
        os.environ.get("DOVIZ_ADMIN_HTTP_PROXY") or "",
        os.environ.get("HTTPS_PROXY") or "",
        os.environ.get("https_proxy") or "",
        os.environ.get("HTTP_PROXY") or "",
        os.environ.get("http_proxy") or "",
    ):
        val = (key or "").strip()
        if val:
            return val
    return ""


def _apply_admin_proxy(sess: requests.Session) -> None:
    proxy = admin_http_proxy()
    if not proxy:
        return
    sess.proxies.update({"http": proxy, "https": proxy})
    LOGGER.info("Doviz admin requests via proxy host=%s", urlparse(proxy).hostname or "?")


def is_admin_vpn_unreachable_error(exc: BaseException | str) -> bool:
    """Admin paneli VPN arkasında; Railway 404/timeout bu sınıfa girer."""
    msg = str(exc or "").lower()
    markers = (
        "login sayfası",
        "http 404",
        "http 403",
        "http 451",
        "timed out",
        "timeout",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network is unreachable",
        "name or service not known",
        "nodename nor servname",
        "temporary failure in name resolution",
        "max retries exceeded",
        "vpn",
    )
    return any(m in msg for m in markers)


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
    try:
        probe = sess.get(stats_url(), timeout=45, allow_redirects=True)
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


def _has_login_form(html: str) -> bool:
    low = (html or "").lower()
    return 'name="password"' in low and ('name="email"' in low or 'id="email"' in low)


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
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )
    _apply_admin_proxy(sess)

    # Ana sayfa cookie/warmup (bazı edge’ler doğrudan /admin’i sert düşürüyor)
    try:
        sess.get(base + "/", timeout=min(timeout, 20), allow_redirects=True)
    except requests.RequestException as exc:
        LOGGER.info("Doviz homepage warm skipped: %s", exc)

    warm = None
    login_url = urljoin(base + "/", LOGIN_PATH.lstrip("/"))
    attempts: list[str] = []
    last_status = 0
    for candidate in login_url_candidates():
        attempts.append(candidate)
        try:
            resp = sess.get(candidate, timeout=timeout, allow_redirects=True)
        except requests.RequestException as exc:
            LOGGER.warning("Admin login GET failed %s: %s", candidate, exc)
            continue
        last_status = int(resp.status_code or 0)
        body = resp.text or ""
        # Soft-404: status kötü olsa bile form varsa kullan
        if _has_login_form(body):
            warm = resp
            login_url = str(resp.url) or candidate
            break
        if last_status < 400 and _looks_like_login_page(body, str(resp.url)):
            warm = resp
            login_url = str(resp.url) or candidate
            break

    if warm is None:
        tried = ", ".join(attempts[:6])
        proxy_hint = (
            " DOVIZ_ADMIN_HTTP_PROXY ile VPN çıkışı tanımlayın"
            if not admin_http_proxy()
            else ""
        )
        raise ValueError(
            f"Admin login sayfası açılamadı (son HTTP {last_status or '?'}). "
            f"Doviz admin VPN arkasında; Railway doğrudan erişemez.{proxy_hint} "
            f"Denenen: {tried}."
        )

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
        if _looks_like_login_page(body, str(resp.url)) and not _session_seems_authenticated(sess):
            # 200 + hâlâ login formu: reddedilmemişse bile oturum yok olabilir
            pass
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
                "ve DOVIZ_ADMIN_BASE_URL=https://www.doviz.com değerlerini kontrol edin."
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
    stats = stats_url()

    best_html = ""
    best_rows = -1
    # Önce parametresiz (sayfa varsayılan aralığı)
    bare = sess.get(stats, timeout=timeout, allow_redirects=True)
    bare.raise_for_status()
    if "/admin/login" in str(bare.url).lower() or _looks_like_login_page(bare.text or "", str(bare.url)):
        raise ValueError("Stats için oturum geçersiz.")
    bare_html = bare.text or ""
    bare_n = len(_TR_RE.findall(bare_html))
    best_html, best_rows = bare_html, bare_n

    for params in _stats_query_candidates(start, end):
        try:
            resp = sess.get(stats, params=params, timeout=timeout, allow_redirects=True)
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
            resp = sess.post(stats, data=data, timeout=timeout, allow_redirects=True)
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
        "source_url": stats_url(),
    }
