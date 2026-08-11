"""Hata izleme — CSV/HTTP tarama sonuçlarını SiteErrorLog'a yazar; sıkı e-posta eşikleri."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.doviz_asset_monitor import format_ts_tr, html_esc
from backend.services.error_monitor import TARAMA_SOURCE, save_error_logs

logger = logging.getLogger(__name__)

# Mail: her şey değil — yalnızca önemli sapmalar.
MIN_NEW_5XX = 1
MIN_NEW_404_REGRESSION = 5
MIN_NEW_HTTP_BURST = 15
MIN_FAILURE_RATE_JUMP_PP = 20.0
MIN_NEW_PRICES_EMPTY = 10
MIN_TIMEOUTS = 30
FIRST_SCAN_FAIL_RATE = 0.35
EMAIL_COOLDOWN_HOURS = 12.0


def resolve_site_id(db: Session, url: str, cache: dict[str, int | None] | None = None) -> int | None:
    host = (urlparse(url).hostname or "").lower().strip()
    if not host:
        return None
    if cache is not None and host in cache:
        return cache[host]
    sites = list(db.query(Site).all())
    hit: int | None = None
    for s in sites:
        d = (s.domain or "").lower().strip()
        if not d:
            continue
        if d == host or d == f"www.{host}" or host == f"www.{d}":
            hit = s.id
            break
    if hit is None:
        for key in ("doviz.com", "sinemalar.com"):
            if not host.endswith(key):
                continue
            for s in sites:
                d = (s.domain or "").lower()
                if key in d:
                    hit = s.id
                    break
            if hit is not None:
                break
    if cache is not None:
        cache[host] = hit
    return hit


def is_http_error_probe(probe: dict[str, Any]) -> bool:
    """Sayfa gerçekten hata mı (HTTP 404/5xx/erişilemez) — fiyat tablosu boşluğu değil."""
    status = int(probe.get("http_status") or 0)
    if status == 0:
        return True
    if status == 404 or status >= 500:
        return True
    return False


def probe_error_type(probe: dict[str, Any]) -> str:
    status = int(probe.get("http_status") or 0)
    if status == 0:
        return "unreachable"
    if status == 404:
        return "not_found"
    if status >= 500:
        return "server_error"
    kind = str(probe.get("kind") or "")
    if kind == "prices_empty":
        return "prices_empty"
    return "http_error"


def probes_to_error_dicts(probes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in probes or []:
        if not is_http_error_probe(p):
            continue
        url = (p.get("url") or "").strip()
        if not url:
            continue
        status = int(p.get("http_status") or 0)
        if status == 0:
            status = 503
        out.append(
            {
                "url": url[:2048],
                "status_code": status,
                "users": 1,
                "error_type": probe_error_type(p),
                "page_title": (p.get("message") or p.get("kind") or "")[:200],
            }
        )
    return out


def persist_tarama_errors(
    db: Session,
    probes: list[dict[str, Any]],
    *,
    scan_iso: str,
    url_count: int,
    ok_count: int,
    failure_count: int,
) -> dict[str, Any]:
    """CSV/HTTP tarama sonuçlarını site bazında SiteErrorLog (source=tarama) olarak yazar."""
    cache: dict[str, Any] = {}
    by_site: dict[int, list[dict[str, Any]]] = defaultdict(list)
    skipped = 0
    for row in probes_to_error_dicts(probes):
        sid = resolve_site_id(db, row["url"], cache)
        if not sid:
            skipped += 1
            continue
        by_site[sid].append(row)

    fetch_meta = {
        "url_count": int(url_count),
        "ok_count": int(ok_count),
        "failure_count": int(failure_count),
        "scan_at": scan_iso,
        "scan_at_tr": format_ts_tr(scan_iso),
        "source": TARAMA_SOURCE,
    }
    # Döviz CSV taraması — hata olmasa da snapshot yaz (eski 404'ler kalksın)
    if not by_site:
        for s in db.query(Site).all():
            d = (s.domain or "").lower()
            if "doviz.com" in d:
                by_site[s.id] = []
                break

    saved = 0
    for site_id, errors in by_site.items():
        saved += save_error_logs(
            db,
            site_id,
            errors,
            source=TARAMA_SOURCE,
            fetch_meta=fetch_meta,
        )
    logger.info(
        "Hata tarama persist: sites=%d saved=%d skipped_host=%d http_errors=%d",
        len(by_site),
        saved,
        skipped,
        sum(len(v) for v in by_site.values()),
    )
    return {"saved": saved, "sites": len(by_site), "skipped_host": skipped}


def _probe_key(p: dict[str, Any]) -> str:
    return str(p.get("url") or "").strip()


def _status(p: dict[str, Any]) -> int:
    return int(p.get("http_status") or 0)


def _kind(p: dict[str, Any]) -> str:
    return str(p.get("kind") or "")


def classify_csv_anomalies(
    failures: list[dict[str, Any]],
    prev_failures: list[dict[str, Any]],
    *,
    url_count: int,
    prev_url_count: int,
    prev_failure_count: int,
) -> dict[str, Any]:
    """Önceki taramaya göre olağan dışı HTTP/CSV sapmalarını sınıflandır."""
    prev_by_url = {_probe_key(p): p for p in prev_failures if _probe_key(p)}

    new_5xx: list[dict[str, Any]] = []
    new_404: list[dict[str, Any]] = []
    timeouts: list[dict[str, Any]] = []
    new_http: list[dict[str, Any]] = []
    new_prices: list[dict[str, Any]] = []
    stable = 0

    for p in failures:
        key = _probe_key(p)
        prev = prev_by_url.get(key)
        status = _status(p)
        kind = _kind(p)
        is_new = prev is None

        if status == 0:
            timeouts.append(p)
            if is_new:
                new_http.append(p)
            continue
        if status >= 500:
            if is_new:
                new_5xx.append(p)
                new_http.append(p)
            continue
        if status == 404 or kind == "http_error":
            if is_new:
                new_404.append(p)
                new_http.append(p)
            else:
                stable += 1
            continue
        if kind == "prices_empty":
            if is_new:
                new_prices.append(p)
            else:
                stable += 1
            continue
        if is_new:
            new_http.append(p)
        else:
            stable += 1

    fail_n = len(failures)
    prev_fail_n = int(prev_failure_count if prev_failure_count is not None else len(prev_failures))
    rate = (fail_n / url_count * 100.0) if url_count else 0.0
    prev_rate = (prev_fail_n / prev_url_count * 100.0) if prev_url_count else 0.0
    rate_jump = rate - prev_rate
    first_scan = not prev_failures and prev_url_count <= 0

    reasons: list[str] = []
    mail = False

    if len(new_5xx) >= MIN_NEW_5XX:
        mail = True
        reasons.append(f"{len(new_5xx)} yeni 5xx")
    if len(new_404) >= MIN_NEW_404_REGRESSION:
        mail = True
        reasons.append(f"{len(new_404)} yeni 404 (önceki taramada yok)")
    if len(new_http) >= MIN_NEW_HTTP_BURST:
        mail = True
        reasons.append(f"{len(new_http)} yeni HTTP hatası (patlama)")
    if (not first_scan) and rate_jump >= MIN_FAILURE_RATE_JUMP_PP:
        mail = True
        reasons.append(f"hata oranı +{rate_jump:.0f} puan")
    if len(new_prices) >= MIN_NEW_PRICES_EMPTY:
        mail = True
        reasons.append(f"{len(new_prices)} sayfada fiyat/tablo kaybı")
    if len(timeouts) >= MIN_TIMEOUTS:
        mail = True
        reasons.append(f"{len(timeouts)} zaman aşımı / erişilemez")
    if first_scan and url_count:
        if rate >= FIRST_SCAN_FAIL_RATE * 100 or new_5xx:
            mail = True
            if not reasons:
                reasons.append(f"ilk tarama · %{rate:.0f} hatalı")
        else:
            mail = False
            reasons = []

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for bucket in (new_5xx, timeouts[:40], new_404, new_prices, new_http):
        for p in bucket:
            k = _probe_key(p)
            if not k or k in seen:
                continue
            seen.add(k)
            items.append(p)
            if len(items) >= 80:
                break
        if len(items) >= 80:
            break

    return {
        "should_mail": mail,
        "reasons": reasons,
        "items": items if mail else [],
        "new_5xx": len(new_5xx),
        "new_404": len(new_404),
        "new_http": len(new_http),
        "new_prices_empty": len(new_prices),
        "timeouts": len(timeouts),
        "stable": stable,
        "failure_rate": round(rate, 1),
        "prev_failure_rate": round(prev_rate, 1),
        "rate_jump": round(rate_jump, 1),
        "first_scan": first_scan,
    }


def email_cooldown_ok(prev_payload: dict[str, Any], *, hours: float = EMAIL_COOLDOWN_HOURS) -> bool:
    last = prev_payload.get("last_anomaly_email_at") or prev_payload.get("last_email_at")
    if not last:
        return True
    from backend.services.doviz_asset_monitor import _hours_since

    return _hours_since(str(last)) >= hours


def send_csv_anomaly_email(
    items: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    scan_iso: str,
    reasons: list[str],
) -> bool:
    if not settings.doviz_asset_monitor_email_enabled or not settings.outbound_email_enabled:
        return False
    if not items:
        return False
    from backend.services.mailer import send_email

    scan_tr = format_ts_tr(scan_iso)
    reason_txt = " · ".join(reasons[:4]) or "kritik sapma"

    def _row(p: dict[str, Any]) -> str:
        return (
            f"<tr><td>{html_esc(str(p.get('kind') or ''))}</td>"
            f"<td>{html_esc(str(p.get('http_status', '')))}</td>"
            f"<td><a href=\"{html_esc(p.get('url', ''))}\">{html_esc(p.get('url', ''))}</a></td>"
            f"<td>{html_esc(p.get('first_seen_tr') or '—')}</td>"
            f"<td>{html_esc(p.get('message', ''))}</td></tr>"
        )

    rows = "".join(_row(p) for p in items[:80])
    th = "<tr><th>Tür</th><th>HTTP</th><th>URL</th><th>İlk tespit</th><th>Not</th></tr>"
    body = f"""
    <h2>Döviz CSV — kritik tarama uyarısı</h2>
    <p><b>Tarama:</b> {html_esc(scan_tr)}<br/>
    Liste: {payload.get('url_count', 0)} URL · sorunlu: {payload.get('failure_count', 0)}<br/>
    <b>Neden:</b> {html_esc(reason_txt)}</p>
    <p style="font-size:12px;color:#64748b">Yalnızca yeni 5xx, yeni 404 patlaması, toplu fiyat kaybı veya oran sıçraması mail gider.
    Bilinen (sabit) 404'ler tekrarlanmaz.</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:12px">{th}{rows}</table>
    <p><a href="https://projectcontrol.up.railway.app/errors">Panel: Hata izleme</a></p>
    """
    subject = f"[Döviz CSV] {scan_tr} — {reason_txt}"
    try:
        send_email(subject, body)
        return True
    except Exception as exc:
        logger.warning("Döviz CSV anomali maili gönderilemedi: %s", exc)
        return False
