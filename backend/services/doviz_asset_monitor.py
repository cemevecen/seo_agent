"""Döviz banka altını / varlık kataloğu ve fiyat satırı izleme (web → app sinyali)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import DovizAssetMonitorRun

logger = logging.getLogger(__name__)

_FETCH_UA = "Mozilla/5.0 (compatible; SeoAgent-DovizAssetMonitor/1.0)"
_BANK_SLUG_RE = re.compile(
    r'href="(?:https://(?:altin|m)\.doviz\.com)?/([a-z0-9][a-z0-9\-]*bank[a-z0-9\-]*|kuveyt-turk)"',
    re.I,
)
_TR_ROW_RE = re.compile(r"<tr[^>]*>.*?</tr>", re.I | re.S)
_NUMERIC_CELL_RE = re.compile(r"\d[\d.,]{2,}")

# altin.doviz.com menüsünde banka altını değil (TCMB / haber köprüsü).
_CATALOG_SLUG_EXCLUDE = frozenset({"merkez-bankasi"})


def _excluded_slugs() -> set[str]:
    raw = (getattr(settings, "doviz_asset_monitor_exclude_slugs", None) or "merkez-bankasi").strip()
    out = set(_CATALOG_SLUG_EXCLUDE)
    for part in raw.split(","):
        s = part.strip().lower()
        if s:
            out.add(s)
    return out


@dataclass(frozen=True)
class ProbeResult:
    slug: str
    host: str
    url: str
    http_status: int
    has_price_rows: bool
    error: str = ""


def _fetch_text(url: str, *, timeout: int = 25) -> tuple[int, str]:
    req = Request(url, headers={"User-Agent": _FETCH_UA, "Accept": "text/html,*/*"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            charset = getattr(resp.headers, "get_content_charset", lambda: None)() or "utf-8"
            return int(resp.status), body.decode(charset, errors="replace")
    except HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return int(exc.code), raw
    except URLError as exc:
        return 0, str(exc.reason or exc)


def html_has_gold_price_rows(html: str) -> bool:
    """Banka altın tablosunda sayısal alış/satış satırı var mı (başlık satırları hariç)."""
    if not html:
        return False
    for tr in _TR_ROW_RE.findall(html):
        low = tr.lower()
        if "alış" in low or "satış" in low or "satis" in low:
            if low.count("<td") <= 1:
                continue
        if _NUMERIC_CELL_RE.search(tr):
            return True
    return False


def discover_bank_slugs_from_catalog(catalog_url: str | None = None) -> list[str]:
    url = (catalog_url or settings.doviz_asset_monitor_catalog_url or "https://altin.doviz.com/").strip()
    status, html = _fetch_text(url)
    if status != 200 or not html:
        logger.warning("Döviz katalog çekilemedi: %s status=%s", url, status)
        return []
    slugs = sorted({m.group(1).lower() for m in _BANK_SLUG_RE.finditer(html)})
    ex = _excluded_slugs()
    return [s for s in slugs if s not in ex]


def probe_bank_on_host(slug: str, host: str) -> ProbeResult:
    host = host.strip().lower()
    if host == "m.doviz.com":
        path = f"/altin/{slug}"
    elif host == "altin.doviz.com":
        path = f"/{slug}"
    else:
        path = f"/altin/{slug}"
    url = f"https://{host}{path}"
    status, html = _fetch_text(url)
    err = ""
    if status == 0:
        err = html[:200]
    has_rows = status == 200 and html_has_gold_price_rows(html)
    return ProbeResult(
        slug=slug,
        host=host,
        url=url,
        http_status=status,
        has_price_rows=has_rows,
        error=err,
    )


def _tz() -> ZoneInfo:
    name = getattr(settings, "report_calendar_timezone", None) or "Europe/Istanbul"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Europe/Istanbul")


def _iso_utc(dt: datetime | None = None) -> str:
    d = dt or datetime.utcnow()
    return d.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def format_ts_tr(iso_z: str | None) -> str:
    """UTC ISO → TR okunur damga (panel / mail)."""
    if not iso_z:
        return "—"
    raw = str(iso_z).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(_tz())
        return local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(iso_z)[:19]


def _probe_key(slug: str, host: str) -> str:
    return f"{slug}|{host}"


def _build_issue_state(
    *,
    scan_iso: str,
    prices_missing: list[dict[str, Any]],
    catalog_removed: list[str],
    prev_issue_state: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Açık sorunlar: first_seen korunur, last_seen güncellenir."""
    out: dict[str, dict[str, Any]] = {}
    for p in prices_missing:
        key = _probe_key(p["slug"], p["host"])
        prev = prev_issue_state.get(key) or {}
        first = prev.get("first_seen_at") or scan_iso
        out[key] = {
            "key": key,
            "kind": "prices_empty",
            "slug": p["slug"],
            "host": p["host"],
            "url": p.get("url") or "",
            "http_status": p.get("http_status"),
            "first_seen_at": first,
            "last_seen_at": scan_iso,
            "first_seen_tr": format_ts_tr(first),
            "last_seen_tr": format_ts_tr(scan_iso),
            "open": True,
        }
    for slug in catalog_removed:
        key = f"catalog:{slug}"
        prev = prev_issue_state.get(key) or {}
        first = prev.get("first_seen_at") or scan_iso
        out[key] = {
            "key": key,
            "kind": "catalog_removed",
            "slug": slug,
            "host": "",
            "url": "",
            "first_seen_at": first,
            "last_seen_at": scan_iso,
            "first_seen_tr": format_ts_tr(first),
            "last_seen_tr": format_ts_tr(scan_iso),
            "open": True,
        }
    return out


def _attach_issue_timestamps(items: list[dict[str, Any]], issue_state: dict[str, dict[str, Any]], scan_iso: str) -> None:
    for p in items:
        key = _probe_key(p.get("slug", ""), p.get("host", ""))
        hit = issue_state.get(key)
        if hit:
            p["first_seen_at"] = hit["first_seen_at"]
            p["last_seen_at"] = hit["last_seen_at"]
            p["first_seen_tr"] = hit["first_seen_tr"]
            p["last_seen_tr"] = hit["last_seen_tr"]
        else:
            p["first_seen_at"] = scan_iso
            p["last_seen_at"] = scan_iso
            p["first_seen_tr"] = format_ts_tr(scan_iso)
            p["last_seen_tr"] = format_ts_tr(scan_iso)


def _attach_alert_timestamps(alerts: list[dict[str, Any]], issue_state: dict[str, dict[str, Any]], scan_iso: str) -> None:
    for a in alerts:
        key = _probe_key(a.get("slug", ""), a.get("host", ""))
        if a.get("kind") == "catalog_removed":
            key = f"catalog:{a.get('slug', '')}"
        hit = issue_state.get(key)
        if hit:
            a["first_seen_at"] = hit["first_seen_at"]
            a["last_seen_at"] = hit["last_seen_at"]
            a["first_seen_tr"] = hit["first_seen_tr"]
            a["last_seen_tr"] = hit["last_seen_tr"]
        else:
            a["detected_at"] = scan_iso
            a["detected_tr"] = format_ts_tr(scan_iso)
            a["first_seen_at"] = scan_iso
            a["last_seen_at"] = scan_iso
            a["first_seen_tr"] = format_ts_tr(scan_iso)
            a["last_seen_tr"] = format_ts_tr(scan_iso)
        a["scan_tr"] = format_ts_tr(scan_iso)


def _probe_hosts() -> list[str]:
    raw = (settings.doviz_asset_monitor_probe_hosts or "m.doviz.com,altin.doviz.com").strip()
    return [h.strip() for h in raw.split(",") if h.strip()]


def run_doviz_asset_monitor(db: Session) -> dict[str, Any]:
    """Katalog + fiyat sondası; önceki çalışmayla diff; uyarı üret."""
    hosts = _probe_hosts()
    catalog = discover_bank_slugs_from_catalog()
    extra = [
        s.strip().lower()
        for s in (settings.doviz_asset_monitor_extra_slugs or "").split(",")
        if s.strip()
    ]
    ex = _excluded_slugs()
    slug_set = sorted((set(catalog) | set(extra)) - ex)

    probes: list[dict[str, Any]] = []
    for slug in slug_set:
        for host in hosts:
            pr = probe_bank_on_host(slug, host)
            probes.append(
                {
                    "slug": pr.slug,
                    "host": pr.host,
                    "url": pr.url,
                    "http_status": pr.http_status,
                    "has_price_rows": pr.has_price_rows,
                    "error": pr.error,
                }
            )

    prev = (
        db.query(DovizAssetMonitorRun)
        .order_by(DovizAssetMonitorRun.collected_at.desc())
        .offset(1)
        .first()
    )
    prev_payload = json.loads(prev.payload_json) if prev and prev.payload_json else {}
    prev_catalog = set(prev_payload.get("catalog_slugs") or [])
    prev_prices: dict[str, bool] = {}
    for p in prev_payload.get("probes") or []:
        key = f"{p.get('slug')}|{p.get('host')}"
        prev_prices[key] = bool(p.get("has_price_rows"))

    curr_catalog = set(catalog)
    catalog_removed = sorted(prev_catalog - curr_catalog) if prev_catalog else []
    catalog_added = sorted(curr_catalog - prev_catalog) if prev_catalog else []

    prices_lost: list[dict[str, Any]] = []
    prices_missing: list[dict[str, Any]] = []
    for p in probes:
        key = f"{p['slug']}|{p['host']}"
        if p["http_status"] == 200 and not p["has_price_rows"]:
            prices_missing.append(p)
        if prev_prices.get(key) and not p["has_price_rows"]:
            prices_lost.append(p)

    alerts: list[dict[str, Any]] = []
    is_baseline = prev is None
    if not is_baseline:
        for slug in catalog_removed:
            alerts.append(
                {
                    "kind": "catalog_removed",
                    "severity": "critical",
                    "slug": slug,
                    "message": f"Katalogdan kalktı: {slug} (altin.doviz.com indeks)",
                }
            )
    if not is_baseline:
        for p in prices_lost:
            alerts.append(
                {
                    "kind": "prices_lost",
                    "severity": "critical",
                    "slug": p["slug"],
                    "host": p["host"],
                    "url": p["url"],
                    "message": f"Fiyat satırları kayboldu: {p['slug']} @ {p['host']}",
                }
            )
    watch = set(extra)
    for p in prices_missing:
        if p["slug"] in catalog_removed:
            continue
        if is_baseline:
            if p["slug"] not in watch:
                continue
        alerts.append(
            {
                "kind": "prices_empty",
                "severity": "warning",
                "slug": p["slug"],
                "host": p["host"],
                "url": p["url"],
                "message": f"Sayfa var, fiyat yok: {p['slug']} @ {p['host']}",
            }
        )

    scan_iso = _iso_utc()
    prev_issue_state = prev_payload.get("issue_state") or {}
    issue_state = _build_issue_state(
        scan_iso=scan_iso,
        prices_missing=prices_missing,
        catalog_removed=catalog_removed if not is_baseline else [],
        prev_issue_state=prev_issue_state,
    )
    _attach_issue_timestamps(prices_missing, issue_state, scan_iso)
    _attach_issue_timestamps(prices_lost, issue_state, scan_iso)
    _attach_alert_timestamps(alerts, issue_state, scan_iso)

    payload = {
        "scan_at": scan_iso,
        "scan_at_tr": format_ts_tr(scan_iso),
        "catalog_url": settings.doviz_asset_monitor_catalog_url,
        "catalog_slugs": catalog,
        "slug_set": slug_set,
        "probes": probes,
        "catalog_added": catalog_added,
        "catalog_removed": catalog_removed,
        "prices_lost": prices_lost,
        "prices_missing": prices_missing,
        "alerts": alerts,
        "issue_state": issue_state,
        "last_email_at": prev_payload.get("last_email_at"),
    }

    run = DovizAssetMonitorRun(
        collected_at=datetime.utcnow(),
        catalog_count=len(catalog),
        alert_count=len(alerts),
        payload_json=json.dumps(payload, ensure_ascii=False),
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    open_issues = sorted(issue_state.values(), key=lambda x: str(x.get("first_seen_at") or ""))
    if _should_send_asset_email(alerts, open_issues, prev_payload, scan_iso):
        payload["last_email_at"] = scan_iso
        run.payload_json = json.dumps(payload, ensure_ascii=False)
        db.commit()
        _notify_alerts(alerts, payload, scan_iso=scan_iso, open_issues=open_issues)

    return {
        "run_id": run.id,
        "scan_at": scan_iso,
        "scan_at_tr": format_ts_tr(scan_iso),
        "catalog_count": len(catalog),
        "alert_count": len(alerts),
        "open_issue_count": len(open_issues),
        "alerts": alerts,
    }


def _hours_since(iso_z: str) -> float:
    try:
        raw = str(iso_z).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
        return delta.total_seconds() / 3600.0
    except Exception:
        return 999.0


def _should_send_asset_email(
    alerts: list[dict[str, Any]],
    open_issues: list[dict[str, Any]],
    prev_payload: dict[str, Any],
    scan_iso: str,
) -> bool:
    if alerts:
        return True
    if not open_issues:
        return False
    if not settings.doviz_asset_monitor_open_issues_email:
        return False
    if not settings.doviz_asset_monitor_email_enabled or not settings.outbound_email_enabled:
        return False
    last = prev_payload.get("last_email_at")
    cooldown = float(settings.doviz_asset_monitor_email_cooldown_hours or 6)
    if not last:
        return True
    return _hours_since(last) >= cooldown


def _notify_alerts(
    alerts: list[dict[str, Any]],
    payload: dict[str, Any],
    *,
    scan_iso: str,
    open_issues: list[dict[str, Any]],
) -> None:
    from backend.services.agent_tools import create_alert

    scan_tr = format_ts_tr(scan_iso)
    critical = [a for a in alerts if a.get("severity") == "critical"]
    title_slugs = ", ".join({a.get("slug", "?") for a in critical[:5]})
    if not title_slugs:
        title_slugs = ", ".join({a.get("slug", "?") for a in (alerts or open_issues)[:5]})

    summary_lines = [a.get("message", "") for a in alerts[:25]]
    if not summary_lines and open_issues:
        summary_lines = [
            f"{i.get('slug')} @ {i.get('host') or 'katalog'} (ilk: {i.get('first_seen_tr')})"
            for i in open_issues[:6]
        ]
    create_alert(
        alert_type="doviz_asset_monitor",
        severity="critical" if critical else "warning",
        title=f"Döviz varlık/banka: {title_slugs}",
        summary=f"Tarama {scan_tr} · " + " · ".join(summary_lines[:6]),
        detail={
            "alerts": alerts,
            "open_issues": open_issues,
            "scan_at": scan_iso,
            "catalog_removed": payload.get("catalog_removed"),
            "prices_lost": payload.get("prices_lost"),
        },
    )

    if not settings.doviz_asset_monitor_email_enabled:
        return
    if not settings.outbound_email_enabled:
        return

    from backend.services.mailer import send_email

    def _row_alert(a: dict[str, Any]) -> str:
        return (
            f"<tr><td>{html_esc(a.get('kind', ''))}</td>"
            f"<td><b>{html_esc(a.get('slug', ''))}</b></td>"
            f"<td>{html_esc(a.get('host', ''))}</td>"
            f"<td>{html_esc(a.get('first_seen_tr') or format_ts_tr(a.get('first_seen_at')))}</td>"
            f"<td>{html_esc(a.get('last_seen_tr') or format_ts_tr(a.get('last_seen_at')))}</td>"
            f"<td>{html_esc(a.get('message', ''))}</td></tr>"
        )

    def _row_open(i: dict[str, Any]) -> str:
        return (
            f"<tr><td>{html_esc(i.get('kind', ''))}</td>"
            f"<td><b>{html_esc(i.get('slug', ''))}</b></td>"
            f"<td>{html_esc(i.get('host', ''))}</td>"
            f"<td>{html_esc(i.get('first_seen_tr', ''))}</td>"
            f"<td>{html_esc(i.get('last_seen_tr', ''))}</td>"
            f"<td>Devam ediyor</td></tr>"
        )

    alert_rows = "".join(_row_alert(a) for a in alerts[:40])
    open_rows = "".join(_row_open(i) for i in open_issues[:40])
    th = (
        "<tr><th>Tür</th><th>Slug</th><th>Host</th>"
        "<th>İlk tespit (TR)</th><th>Son kontrol (TR)</th><th>Not</th></tr>"
    )
    sections = []
    if alerts:
        sections.append(f"<h3>Bu taramada yeni uyarı ({len(alerts)})</h3><table border=\"1\" cellpadding=\"6\" cellspacing=\"0\" style=\"border-collapse:collapse;font-size:13px\">{th}{alert_rows}</table>")
    if open_issues:
        sections.append(
            f"<h3>Açık sorunlar ({len(open_issues)})</h3>"
            f"<table border=\"1\" cellpadding=\"6\" cellspacing=\"0\" style=\"border-collapse:collapse;font-size:13px\">{th}{open_rows}</table>"
        )
    body = f"""
    <h2>Döviz varlık izleme</h2>
    <p><b>Tarama:</b> {html_esc(scan_tr)} (UTC {html_esc(scan_iso[:19])}Z)<br/>
    Katalog: {len(payload.get('catalog_slugs') or [])} banka</p>
    {"".join(sections)}
    <p><a href="https://projectcontrol.up.railway.app/doviz-varliklar">Panel: Döviz varlıklar</a></p>
    """
    subject = f"[Döviz varlık] {scan_tr} — {len(alerts)} yeni, {len(open_issues)} açık — {title_slugs[:60]}"
    try:
        send_email(subject, body)
    except Exception as exc:
        logger.warning("Döviz varlık maili gönderilemedi: %s", exc)


def html_esc(s: str) -> str:
    return (
        str(s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def get_latest_run(db: Session) -> dict[str, Any] | None:
    row = db.query(DovizAssetMonitorRun).order_by(DovizAssetMonitorRun.collected_at.desc()).first()
    if not row:
        return None
    return {
        "id": row.id,
        "collected_at": row.collected_at.isoformat() + "Z",
        "collected_at_tr": format_ts_tr(row.collected_at.isoformat() + "Z"),
        "catalog_count": row.catalog_count,
        "alert_count": row.alert_count,
        "payload": json.loads(row.payload_json or "{}"),
    }


def cleanup_old_runs(db: Session, *, keep_days: int = 30) -> int:
    cutoff = datetime.utcnow() - timedelta(days=max(1, keep_days))
    deleted = (
        db.query(DovizAssetMonitorRun)
        .filter(DovizAssetMonitorRun.collected_at < cutoff)
        .delete(synchronize_session=False)
    )
    db.commit()
    return int(deleted or 0)
