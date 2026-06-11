"""CSV manifest URL listesi — DB'den saatlik tarama ve sınırlı mail (issue başına max N)."""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import DovizAssetMonitorRun, DovizAssetMonitorUrl
from backend.services.doviz_asset_monitor import (
    _fetch_text,
    _iso_utc,
    format_ts_tr,
    html_esc,
    html_has_gold_price_rows,
)

logger = logging.getLogger(__name__)

RUN_KIND = "csv_manifest"
_URL_LINE_RE = re.compile(r"https?://[^\s,\"]+", re.I)
_MAX_URL_LEN = 2048


def parse_urls_from_csv_text(text: str) -> list[str]:
    """Satır veya virgül ayrımlı CSV metninden benzersiz https URL'leri."""
    seen: set[str] = set()
    out: list[str] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for m in _URL_LINE_RE.finditer(line):
            u = m.group(0).strip().rstrip(",;")
            norm = normalize_manifest_url(u)
            if norm and norm not in seen:
                seen.add(norm)
                out.append(norm)
    return out


def normalize_manifest_url(raw: str) -> str | None:
    u = (raw or "").strip()
    if not u or len(u) > _MAX_URL_LEN:
        return None
    if not u.lower().startswith("http"):
        return None
    try:
        p = urlparse(u)
    except Exception:
        return None
    if p.scheme not in ("http", "https"):
        return None
    host = (p.hostname or "").lower()
    if not host.endswith("doviz.com"):
        return None
    path = p.path or "/"
    if p.query:
        return None
    return f"https://{host}{path}".rstrip("/") if path != "/" else f"https://{host}/"


def manifest_url_count(db: Session) -> int:
    return int(db.query(DovizAssetMonitorUrl).count())


def manifest_upload_info(db: Session) -> dict[str, Any]:
    row = db.query(DovizAssetMonitorUrl).order_by(DovizAssetMonitorUrl.uploaded_at.desc()).first()
    if not row:
        return {"url_count": 0, "uploaded_at": None, "uploaded_at_tr": None}
    cnt = manifest_url_count(db)
    iso = row.uploaded_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "url_count": cnt,
        "uploaded_at": iso,
        "uploaded_at_tr": format_ts_tr(iso),
    }


def replace_manifest_urls(db: Session, urls: list[str], *, source_label: str = "") -> dict[str, Any]:
    """Mevcut listeyi silip yeni CSV URL setini yazar."""
    if not urls:
        raise ValueError("CSV içinde geçerli doviz.com URL bulunamadı.")
    now = datetime.utcnow()
    db.query(DovizAssetMonitorUrl).delete(synchronize_session=False)
    for u in urls:
        db.add(DovizAssetMonitorUrl(url=u, uploaded_at=now))
    db.commit()
    return {
        "url_count": len(urls),
        "uploaded_at": now.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_label": (source_label or "")[:255],
    }


def _issue_key(url: str) -> str:
    return f"url:{url}"


def probe_manifest_url(url: str, *, timeout: int | None = None) -> dict[str, Any]:
    t = timeout or int(settings.doviz_asset_csv_manifest_fetch_timeout or 15)
    status, html = _fetch_text(url, timeout=t)
    err = ""
    if status == 0:
        err = (html or "")[:200]
    has_rows = status == 200 and html_has_gold_price_rows(html)
    ok = status == 200 and has_rows
    if status != 200:
        kind = "http_error"
        message = f"HTTP {status}: {url}"
    elif not has_rows:
        kind = "prices_empty"
        message = f"Sayfa var, fiyat/tablo yok: {url}"
    else:
        kind = "ok"
        message = ""
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        host = ""
    return {
        "url": url,
        "host": host,
        "http_status": status,
        "has_price_rows": has_rows,
        "ok": ok,
        "kind": kind,
        "error": err,
        "message": message,
    }


def _probe_all_urls(urls: list[str]) -> list[dict[str, Any]]:
    workers = max(1, int(settings.doviz_asset_csv_manifest_workers or 10))
    timeout = int(settings.doviz_asset_csv_manifest_fetch_timeout or 15)
    results: list[dict[str, Any]] = []
    if not urls:
        return results
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(probe_manifest_url, u, timeout=timeout): u for u in urls}
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                u = futs[fut]
                results.append(
                    {
                        "url": u,
                        "host": urlparse(u).hostname or "",
                        "http_status": 0,
                        "has_price_rows": False,
                        "ok": False,
                        "kind": "http_error",
                        "error": str(exc)[:200],
                        "message": f"Probe hatası: {u}",
                    }
                )
    results.sort(key=lambda x: str(x.get("url") or ""))
    return results


def _build_csv_issue_state(
    *,
    scan_iso: str,
    failures: list[dict[str, Any]],
    prev_issue_state: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for p in failures:
        key = _issue_key(p["url"])
        prev = prev_issue_state.get(key) or {}
        first = prev.get("first_seen_at") or scan_iso
        email_count = int(prev.get("email_notify_count") or 0)
        out[key] = {
            "key": key,
            "kind": p.get("kind") or "prices_empty",
            "url": p["url"],
            "host": p.get("host") or "",
            "http_status": p.get("http_status"),
            "first_seen_at": first,
            "last_seen_at": scan_iso,
            "first_seen_tr": format_ts_tr(first),
            "last_seen_tr": format_ts_tr(scan_iso),
            "email_notify_count": email_count,
            "open": True,
        }
    return out


def _failures_for_email(
    failures: list[dict[str, Any]],
    issue_state: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Açık sorunlar; issue başına en fazla max_emails mail."""
    max_n = int(settings.doviz_asset_csv_manifest_max_emails_per_issue or 2)
    out: list[dict[str, Any]] = []
    for p in failures:
        key = _issue_key(p["url"])
        row = issue_state.get(key) or {}
        if int(row.get("email_notify_count") or 0) >= max_n:
            continue
        item = dict(p)
        item["first_seen_tr"] = row.get("first_seen_tr") or format_ts_tr(row.get("first_seen_at"))
        item["last_seen_tr"] = row.get("last_seen_tr") or format_ts_tr(row.get("last_seen_at"))
        item["email_notify_count"] = int(row.get("email_notify_count") or 0)
        out.append(item)
    return out


def _increment_email_counts(issue_state: dict[str, dict[str, Any]], mailed: list[dict[str, Any]]) -> None:
    for p in mailed:
        key = _issue_key(p["url"])
        if key not in issue_state:
            continue
        issue_state[key]["email_notify_count"] = int(issue_state[key].get("email_notify_count") or 0) + 1


def _get_prev_csv_payload(db: Session) -> dict[str, Any]:
    prev = (
        db.query(DovizAssetMonitorRun)
        .filter(DovizAssetMonitorRun.run_kind == RUN_KIND)
        .order_by(DovizAssetMonitorRun.collected_at.desc())
        .first()
    )
    if not prev or not prev.payload_json:
        return {}
    try:
        return json.loads(prev.payload_json)
    except Exception:
        return {}


def run_doviz_asset_csv_manifest(db: Session) -> dict[str, Any]:
    """DB'deki URL listesini tarar; boş/hatalı sayfalar için mail (issue başına max 2)."""
    urls = [r.url for r in db.query(DovizAssetMonitorUrl).order_by(DovizAssetMonitorUrl.id).all()]
    if not urls:
        return {
            "run_kind": RUN_KIND,
            "skipped": True,
            "reason": "no_manifest_urls",
            "url_count": 0,
        }

    probes = _probe_all_urls(urls)
    failures = [p for p in probes if not p.get("ok")]
    ok_count = len(probes) - len(failures)

    prev_payload = _get_prev_csv_payload(db)
    prev_issue_state = prev_payload.get("issue_state") or {}

    scan_iso = _iso_utc()
    issue_state = _build_csv_issue_state(
        scan_iso=scan_iso,
        failures=failures,
        prev_issue_state=prev_issue_state,
    )

    for p in failures:
        key = _issue_key(p["url"])
        hit = issue_state.get(key)
        if hit:
            p["first_seen_at"] = hit["first_seen_at"]
            p["last_seen_at"] = hit["last_seen_at"]
            p["first_seen_tr"] = hit["first_seen_tr"]
            p["last_seen_tr"] = hit["last_seen_tr"]

    mail_items = _failures_for_email(failures, issue_state)

    payload: dict[str, Any] = {
        "run_kind": RUN_KIND,
        "scan_at": scan_iso,
        "scan_at_tr": format_ts_tr(scan_iso),
        "url_count": len(urls),
        "ok_count": ok_count,
        "failure_count": len(failures),
        "probes": probes,
        "failures": failures,
        "issue_state": issue_state,
        "mail_items": mail_items,
    }

    run = DovizAssetMonitorRun(
        collected_at=datetime.utcnow(),
        run_kind=RUN_KIND,
        catalog_count=len(urls),
        alert_count=len(failures),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    if mail_items and settings.doviz_asset_monitor_email_enabled and settings.outbound_email_enabled:
        _send_csv_manifest_email(mail_items, payload, scan_iso=scan_iso)
        _increment_email_counts(issue_state, mail_items)
        payload["issue_state"] = issue_state
        run.payload_json = json.dumps(payload, ensure_ascii=False)
        db.commit()

    return {
        "run_id": run.id,
        "run_kind": RUN_KIND,
        "scan_at": scan_iso,
        "scan_at_tr": format_ts_tr(scan_iso),
        "url_count": len(urls),
        "ok_count": ok_count,
        "failure_count": len(failures),
        "emailed_count": len(mail_items),
        "failures": failures[:50],
    }


def _send_csv_manifest_email(
    items: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    scan_iso: str,
) -> None:
    from backend.services.mailer import send_email

    scan_tr = format_ts_tr(scan_iso)
    max_n = int(settings.doviz_asset_csv_manifest_max_emails_per_issue or 2)

    def _row(p: dict[str, Any]) -> str:
        return (
            f"<tr><td>{html_esc(p.get('kind', ''))}</td>"
            f"<td>{html_esc(str(p.get('http_status', '')))}</td>"
            f"<td><a href=\"{html_esc(p.get('url', ''))}\">{html_esc(p.get('url', ''))}</a></td>"
            f"<td>{html_esc(p.get('first_seen_tr') or '—')}</td>"
            f"<td>{html_esc(p.get('last_seen_tr') or '—')}</td>"
            f"<td>{html_esc(p.get('message', ''))}</td></tr>"
        )

    rows = "".join(_row(p) for p in items[:80])
    th = (
        "<tr><th>Tür</th><th>HTTP</th><th>URL</th>"
        "<th>İlk tespit</th><th>Son kontrol</th><th>Not</th></tr>"
    )
    body = f"""
    <h2>Döviz CSV manifest tarama</h2>
    <p><b>Tarama:</b> {html_esc(scan_tr)}<br/>
    Liste: {payload.get('url_count', 0)} URL · sorunlu: {payload.get('failure_count', 0)} · bu mailde: {len(items)} satır<br/>
    <small>Aynı URL için en fazla {max_n} e-posta gönderilir; sonraki saatlerde tekrarlanmaz.</small></p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:12px">{th}{rows}</table>
    <p><a href="https://projectcontrol.up.railway.app/doviz-varliklar">Panel: Döviz varlıklar</a></p>
    """
    subject = f"[Döviz CSV] {scan_tr} — {len(items)} sorunlu sayfa"
    try:
        send_email(subject, body)
    except Exception as exc:
        logger.warning("Döviz CSV manifest maili gönderilemedi: %s", exc)


def get_latest_csv_run(db: Session) -> dict[str, Any] | None:
    row = (
        db.query(DovizAssetMonitorRun)
        .filter(DovizAssetMonitorRun.run_kind == RUN_KIND)
        .order_by(DovizAssetMonitorRun.collected_at.desc())
        .first()
    )
    if not row:
        return None
    return {
        "id": row.id,
        "collected_at": row.collected_at.isoformat() + "Z",
        "collected_at_tr": format_ts_tr(row.collected_at.isoformat() + "Z"),
        "url_count": row.catalog_count,
        "failure_count": row.alert_count,
        "payload": json.loads(row.payload_json or "{}"),
    }


def cleanup_old_csv_runs(db: Session, *, keep_days: int = 14) -> int:
    cutoff = datetime.utcnow() - timedelta(days=max(1, keep_days))
    deleted = (
        db.query(DovizAssetMonitorRun)
        .filter(DovizAssetMonitorRun.run_kind == RUN_KIND)
        .filter(DovizAssetMonitorRun.collected_at < cutoff)
        .delete(synchronize_session=False)
    )
    db.commit()
    return int(deleted or 0)
