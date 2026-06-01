"""Search Console Links CSV import (Latest / More sample / Top linking sites)."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from sqlalchemy.orm import Session

from backend.models import BacklinkDomainAction, BacklinkImport, BacklinkRow, Site
from backend.services.backlink_risk import (
    ACTION_DISAVOW,
    ACTION_IGNORE,
    ACTION_MONITOR,
    ACTION_REVIEW,
    assess_linking_url,
    finalize_domain_risk_summary,
    is_trusted_media_domain,
    normalize_domain,
)

LOGGER = logging.getLogger(__name__)

REPORT_TYPES = ("latest_links", "more_sample", "top_linking_sites")

REPORT_TYPE_LABELS: dict[str, str] = {
    "latest_links": "Latest links",
    "more_sample": "More sample links",
    "top_linking_sites": "Top linking sites",
}

_HEADER_ALIASES: dict[str, list[str]] = {
    "source_url": [
        "bağlantı verilen sayfa",
        "baglanti verilen sayfa",
        "bağlantı sayfası",
        "baglanti sayfasi",
        "kaynak sayfa",
        "linking page",
        "source page",
        "referring page",
        "referrer page",
        "page url",
        "url",
    ],
    "target_url": [
        "hedef sayfa",
        "target page",
        "target url",
        "linked page",
    ],
    "anchor_text": [
        "bağlantı metni",
        "baglanti metni",
        "link text",
        "anchor text",
        "anchor",
    ],
    "last_crawled": [
        "son tarama",
        "last crawled",
        "last crawl date",
    ],
    "linking_site": [
        "bağlantı veren site",
        "linking site",
        "site",
        "domain",
    ],
}


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("\ufeff", "")


def _build_header_map(headers: list[str]) -> dict[str, int]:
    norm_headers = [_norm(h) for h in headers]
    out: dict[str, int] = {}
    for std_key, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            alias_norm = _norm(alias)
            for i, h in enumerate(norm_headers):
                if h == alias_norm:
                    out[std_key] = i
                    break
            if std_key in out:
                break
    return out


def _looks_like_url(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return False
    if re.match(r"^https?://", v, re.I):
        return True
    if "." in v and " " not in v and len(v) < 500:
        return bool(re.search(r"[a-z0-9-]+\.[a-z]{2,}", v, re.I))
    return False


def _cell(row: list[str], idx: int | None) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def normalize_csv_text(text: str) -> str:
    """UTF-16 / BOM ve GSC export satır sonları."""
    raw = text or ""
    if not raw.strip():
        return ""
    if "\x00" in raw:
        try:
            as_bytes = raw.encode("utf-8", errors="surrogateescape")
        except UnicodeEncodeError:
            as_bytes = raw.encode("latin-1", errors="replace")
        if len(as_bytes) >= 2 and as_bytes[:2] in (b"\xff\xfe", b"\xfe\xff"):
            encoding = "utf-16-le" if as_bytes[:2] == b"\xff\xfe" else "utf-16-be"
            raw = as_bytes.decode(encoding, errors="replace")
        else:
            raw = as_bytes.decode("utf-16-le", errors="replace")
    return raw.replace("\ufeff", "").replace("\r\n", "\n").strip()


def parse_csv_text(text: str, *, report_type: str) -> list[dict[str, Any]]:
    """CSV metnini satır dict listesine çevirir."""
    raw = normalize_csv_text(text)
    if not raw:
        return []
    sample = raw[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(raw), dialect=dialect)
    rows = list(reader)
    if not rows:
        return []

    header_idx = 0
    header_map: dict[str, int] = {}
    data_rows: list[list[str]] = rows
    for i, row in enumerate(rows[:12]):
        hm = _build_header_map(row)
        if hm.get("source_url") or hm.get("linking_site"):
            header_idx = i
            header_map = hm
            data_rows = rows[i + 1 :]
            break
    if not header_map:
        for i, row in enumerate(rows[:8]):
            if any(_looks_like_url(c) for c in row):
                header_idx = i
                data_rows = rows[header_idx:]
                break

    out: list[dict[str, Any]] = []
    for row in data_rows:
        if not row or not any((c or "").strip() for c in row):
            continue
        if not header_map and report_type == "top_linking_sites":
            domain_cell = _cell(row, 0)
            if domain_cell:
                src = domain_cell if _looks_like_url(domain_cell) else f"http://{domain_cell}/"
                out.append(
                    {
                        "source_url": src,
                        "target_url": "",
                        "anchor_text": "",
                        "last_crawled": "",
                    }
                )
            continue

        src = _cell(row, header_map.get("source_url"))
        if not src and header_map.get("linking_site") is not None:
            dom = _cell(row, header_map.get("linking_site"))
            if dom:
                src = dom if _looks_like_url(dom) else f"http://{dom}/"
        if not src:
            for c in row:
                if _looks_like_url(c):
                    src = c.strip()
                    break
        if not src:
            continue
        out.append(
            {
                "source_url": src,
                "target_url": _cell(row, header_map.get("target_url")),
                "anchor_text": _cell(row, header_map.get("anchor_text")),
                "last_crawled": _cell(row, header_map.get("last_crawled")),
            }
        )
    return out


def _parse_spreadsheet_url(url: str) -> tuple[str, str | None]:
    u = (url or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", u)
    if not m:
        m = re.search(r"/spreadsheets/d/e/([a-zA-Z0-9-_]+)", u)
    if not m:
        raise ValueError("Geçerli Google Sheets URL değil.")
    sheet_id = m.group(1)
    gid: str | None = None
    parsed = urlparse(u)
    q = parse_qs(parsed.query)
    if "gid" in q and q["gid"]:
        gid = str(q["gid"][0])
    frag = parsed.fragment or ""
    frag_m = re.search(r"gid=(\d+)", frag)
    if frag_m:
        gid = frag_m.group(1)
    return sheet_id, gid


def _sheet_values_to_csv(values: list[list[Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in values:
        writer.writerow(row)
    return buf.getvalue()


def _fetch_sheet_via_service_account(spreadsheet_id: str, gid: str | None) -> str | None:
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        from backend.services.ga4_auth import _load_service_account_payload
    except ImportError:
        return None
    payload = _load_service_account_payload()
    if not payload:
        return None
    try:
        creds = service_account.Credentials.from_service_account_info(
            payload,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = meta.get("sheets") or []
        title = None
        if gid is not None:
            for sh in sheets:
                props = sh.get("properties") or {}
                if str(props.get("sheetId")) == str(gid):
                    title = props.get("title")
                    break
        if not title and sheets:
            title = (sheets[0].get("properties") or {}).get("title")
        if not title:
            return None
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=title)
            .execute()
        )
        values = result.get("values") or []
        if not values:
            return None
        return _sheet_values_to_csv(values)
    except Exception as exc:  # noqa: BLE001
        LOGGER.info("Sheets service account fetch failed: %s", exc)
        return None


def fetch_public_sheet_csv(url: str, *, timeout: int = 25) -> str:
    """Google Sheets'ten CSV metni (herkese açık, gviz/export veya service account)."""
    u = (url or "").strip()
    if not u:
        raise ValueError("Sheets URL boş.")

    sheet_id: str | None = None
    gid: str | None = None
    if "docs.google.com/spreadsheets" in u:
        sheet_id, gid = _parse_spreadsheet_url(u)
    gid_q = gid if gid is not None else "0"

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SEOAgent/1.0; +https://github.com/)",
    }
    candidates: list[str] = []
    if u not in candidates:
        candidates.append(u)
    if sheet_id:
        candidates.extend(
            [
                f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid_q}",
                f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_q}",
            ]
        )

    last_status: int | None = None
    for fetch_url in candidates:
        try:
            resp = requests.get(fetch_url, timeout=timeout, headers=headers, allow_redirects=True)
            last_status = resp.status_code
            if resp.status_code in (401, 403):
                continue
            resp.raise_for_status()
            text = resp.text or ""
            low = text[:300].lower()
            if "<!doctype html" in low or "<html" in low:
                continue
            if not text.strip():
                continue
            return text
        except requests.RequestException:
            continue

    if sheet_id:
        sa_text = _fetch_sheet_via_service_account(sheet_id, gid)
        if sa_text:
            return sa_text

    hint = (
        "Sayfa erişilemedi. Google Sheets’te: Dosya → Paylaş → «Bağlantısı olan herkes» en az "
        "«Görüntüleyici»; veya Dosya → Web’de yayınla. Özel sayfalar için tabloyu GA4 service "
        "account e-postasıyla paylaşın (GA4_SERVICE_ACCOUNT_JSON)."
    )
    if last_status in (401, 403):
        raise ValueError(f"{hint} (HTTP {last_status})")
    raise ValueError(hint)


def import_backlink_csv(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    csv_text: str,
    source_filename: str = "",
    source_kind: str = "csv_upload",
) -> dict[str, Any]:
    rt = (report_type or "latest_links").strip().lower()
    if rt not in REPORT_TYPES:
        raise ValueError(f"Geçersiz report_type: {rt}")

    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise ValueError("Site bulunamadı.")

    parsed = parse_csv_text(csv_text, report_type=rt)
    if not parsed:
        raise ValueError("CSV'de geçerli bağlantı satırı bulunamadı.")

    imp = BacklinkImport(
        site_id=site.id,
        report_type=rt,
        source_filename=(source_filename or "")[:255],
        source_kind=(source_kind or "csv_upload")[:32],
        row_count=0,
        created_at=datetime.utcnow(),
    )
    db.add(imp)
    db.flush()

    existing_fps = _existing_link_fingerprints(db, site_id=site.id, report_type=rt)
    batch_seen: set[str] = set()
    row_models: list[BacklinkRow] = []
    skipped_duplicate = 0
    for item in parsed:
        src = item["source_url"]
        tgt = item.get("target_url") or ""
        risk = assess_linking_url(
            src,
            anchor_text=item.get("anchor_text") or "",
            target_url=tgt,
        )
        dom = ((risk.get("domain") or normalize_domain(src)) or "").lower()[:255]
        fp = _link_fingerprint(dom, src, tgt)
        if fp in existing_fps or fp in batch_seen:
            skipped_duplicate += 1
            continue
        batch_seen.add(fp)
        row_models.append(
            BacklinkRow(
                import_id=imp.id,
                site_id=site.id,
                source_url=src[:2048],
                target_url=tgt[:2048],
                domain=dom,
                anchor_text=(item.get("anchor_text") or "")[:512],
                last_crawled=(item.get("last_crawled") or "")[:64],
                risk_score=int(risk.get("risk_score") or 0),
                risk_flags_json=json.dumps(risk.get("risk_flags") or [], ensure_ascii=False),
                recommended_action=str(risk.get("recommended_action") or ACTION_MONITOR),
            )
        )
    if row_models:
        db.bulk_save_objects(row_models)
    imp.row_count = len(row_models)
    db.commit()
    db.refresh(imp)

    summary = build_dashboard(db, site_id=site.id, report_type=rt)
    summary["import"] = {
        "id": imp.id,
        "row_count": imp.row_count,
        "rows_in_file": len(parsed),
        "rows_skipped_duplicate": skipped_duplicate,
        "created_at": imp.created_at.isoformat() if imp.created_at else None,
        "source_filename": imp.source_filename,
    }
    return summary


def _domain_actions_map(db: Session, site_id: int) -> dict[str, str]:
    rows = db.query(BacklinkDomainAction).filter(BacklinkDomainAction.site_id == site_id).all()
    return {(r.domain or "").lower(): (r.action or "") for r in rows if r.domain}


def _effective_action(recommended: str, override: str | None) -> str:
    o = (override or "").strip().lower()
    if o in (ACTION_IGNORE, ACTION_MONITOR, ACTION_REVIEW, ACTION_DISAVOW):
        return o
    return (recommended or ACTION_MONITOR).strip().lower()


def delete_backlink_import(db: Session, *, site_id: int, import_id: int) -> dict[str, Any]:
    imp = (
        db.query(BacklinkImport)
        .filter(BacklinkImport.id == import_id, BacklinkImport.site_id == site_id)
        .first()
    )
    if imp is None:
        raise ValueError("Import bulunamadı.")
    rt = imp.report_type
    db.delete(imp)
    db.commit()
    out = build_dashboard(db, site_id=site_id, report_type=rt or "latest_links")
    out["deleted_import_id"] = import_id
    return out


def _link_pair_key(source_url: str, target_url: str) -> tuple[str, str]:
    return ((source_url or "").strip().lower(), (target_url or "").strip().lower())


def _link_fingerprint(domain: str, source_url: str, target_url: str) -> str:
    sk, tk = _link_pair_key(source_url, target_url)
    return f"{(domain or '').lower()}\t{sk}\t{tk}"


def _existing_link_fingerprints(db: Session, *, site_id: int, report_type: str) -> set[str]:
    """Bu site + rapor türü için önceki tüm importlardaki benzersiz link anahtarları."""
    rt = (report_type or "latest_links").strip().lower()
    rows = (
        db.query(BacklinkRow.domain, BacklinkRow.source_url, BacklinkRow.target_url)
        .join(BacklinkImport, BacklinkRow.import_id == BacklinkImport.id)
        .filter(BacklinkImport.site_id == site_id, BacklinkImport.report_type == rt)
        .all()
    )
    out: set[str] = set()
    for dom, src, tgt in rows:
        out.add(_link_fingerprint((dom or "").lower(), src or "", tgt or ""))
    return out


def _normalize_last_crawled(value: str | None) -> str:
    return (value or "").strip()[:64]


def _last_crawled_sort_key(value: str | None) -> tuple[int, str]:
    """Deduplicate sırasında en güncel Son tarama değerini seçmek için."""
    s = _normalize_last_crawled(value)
    if not s:
        return (0, "")
    for fmt, take in (
        ("%Y-%m-%d", 10),
        ("%d.%m.%Y", 10),
        ("%d/%m/%Y", 10),
        ("%m/%d/%Y", 10),
    ):
        try:
            dt = datetime.strptime(s[:take], fmt)
            return (2, dt.strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return (1, s.lower())


def _merge_link_entry(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    if _last_crawled_sort_key(incoming.get("last_crawled")) > _last_crawled_sort_key(
        existing.get("last_crawled")
    ):
        return {**existing, **incoming}
    return existing


def _link_entries_from_rows(rows: list[BacklinkRow]) -> list[dict[str, Any]]:
    by_fp: dict[str, dict[str, Any]] = {}
    for r in rows:
        dom = (r.domain or "").lower()
        if not dom:
            continue
        fp = _link_fingerprint(dom, r.source_url or "", r.target_url or "")
        entry = {
            "domain": dom,
            "source_url": r.source_url or "",
            "target_url": r.target_url or "",
            "anchor_text": (r.anchor_text or "")[:200],
            "last_crawled": _normalize_last_crawled(r.last_crawled),
        }
        if fp in by_fp:
            by_fp[fp] = _merge_link_entry(by_fp[fp], entry)
        else:
            by_fp[fp] = entry
    return list(by_fp.values())


def _merge_row_into_domain_bucket(bucket: dict[str, Any], r: BacklinkRow, *, url_keys: set[str]) -> None:
    src = (r.source_url or "").strip()
    dom = (r.domain or "").lower()
    pair = _link_fingerprint(dom, src, r.target_url or "")
    bucket["raw_row_count"] = int(bucket.get("raw_row_count") or 0) + 1
    if pair and pair not in url_keys:
        url_keys.add(pair)
        bucket["link_count"] += 1
    score = int(r.risk_score or 0)
    bucket["max_risk_score"] = max(bucket["max_risk_score"], score)
    bucket["min_risk_score"] = min(int(bucket.get("min_risk_score") or 999), score)
    try:
        flags = json.loads(r.risk_flags_json or "[]")
    except json.JSONDecodeError:
        flags = []
    for f in flags:
        bucket["risk_flags"].add(str(f))
    rec = r.recommended_action or ACTION_MONITOR
    ac = bucket.setdefault(
        "action_counts",
        {ACTION_IGNORE: 0, ACTION_MONITOR: 0, ACTION_REVIEW: 0, ACTION_DISAVOW: 0},
    )
    ac[rec] = ac.get(rec, 0) + 1
    if score < 25:
        bucket["low_risk_links"] = int(bucket.get("low_risk_links") or 0) + 1
    if _action_rank(rec) > _action_rank(bucket["recommended_action"]):
        bucket["recommended_action"] = rec
    if src and src not in bucket["sample_urls"] and len(bucket["sample_urls"]) < 3:
        bucket["sample_urls"].append(src)
    if len(bucket["sample_links"]) < 8:
        sample = {
            "source_url": r.source_url or "",
            "target_url": r.target_url or "",
            "anchor_text": (r.anchor_text or "")[:200],
            "last_crawled": _normalize_last_crawled(r.last_crawled),
            "risk_score": int(r.risk_score or 0),
        }
        existing_src = {x.get("source_url") for x in bucket["sample_links"]}
        if (r.source_url or "") not in existing_src:
            bucket["sample_links"].append(sample)
        elif int(r.risk_score or 0) > max(
            (x.get("risk_score") or 0 for x in bucket["sample_links"] if x.get("source_url") == r.source_url),
            default=0,
        ):
            bucket["sample_links"] = [
                x for x in bucket["sample_links"] if x.get("source_url") != r.source_url
            ] + [sample]


def build_dashboard(db: Session, *, site_id: int, report_type: str = "latest_links") -> dict[str, Any]:
    rt = (report_type or "latest_links").strip().lower()
    imports = (
        db.query(BacklinkImport)
        .filter(BacklinkImport.site_id == site_id, BacklinkImport.report_type == rt)
        .order_by(BacklinkImport.created_at.desc())
        .all()
    )
    actions = _domain_actions_map(db, site_id)
    latest = imports[0] if imports else None
    previous = imports[1] if len(imports) > 1 else None

    domain_stats: dict[str, dict[str, Any]] = {}
    url_keys_by_domain: dict[str, set[str]] = {}
    diff: dict[str, Any] = {
        "new_domains": [],
        "lost_domains": [],
        "new_links": [],
        "lost_links": [],
        "has_previous": bool(previous),
        "latest_import_label": "",
        "previous_import_label": "",
    }

    import_ids = [i.id for i in imports]
    if import_ids:
        rows = db.query(BacklinkRow).filter(BacklinkRow.import_id.in_(import_ids)).all()
        for r in rows:
            dom = (r.domain or "").lower()
            if not dom:
                continue
            bucket = domain_stats.setdefault(
                dom,
                {
                    "domain": dom,
                    "link_count": 0,
                    "raw_row_count": 0,
                    "max_risk_score": 0,
                    "risk_flags": set(),
                    "recommended_action": ACTION_MONITOR,
                    "sample_urls": [],
                    "sample_links": [],
                },
            )
            keys = url_keys_by_domain.setdefault(dom, set())
            _merge_row_into_domain_bucket(bucket, r, url_keys=keys)

        if latest and previous:
            latest_rows = db.query(BacklinkRow).filter(BacklinkRow.import_id == latest.id).all()
            prev_rows = db.query(BacklinkRow).filter(BacklinkRow.import_id == previous.id).all()
            latest_entries = _link_entries_from_rows(latest_rows)
            prev_entries = _link_entries_from_rows(prev_rows)
            latest_fps = {
                _link_fingerprint(e["domain"], e["source_url"], e["target_url"]): e for e in latest_entries
            }
            prev_fps = {
                _link_fingerprint(e["domain"], e["source_url"], e["target_url"]): e for e in prev_entries
            }
            latest_domains = {e["domain"] for e in latest_entries}
            prev_domains = {e["domain"] for e in prev_entries}
            diff["new_domains"] = sorted(latest_domains - prev_domains)[:200]
            diff["lost_domains"] = sorted(prev_domains - latest_domains)[:200]
            diff["new_links"] = [
                latest_fps[k] for k in sorted(latest_fps.keys() - prev_fps.keys())
            ][:300]
            diff["lost_links"] = [
                prev_fps[k] for k in sorted(prev_fps.keys() - latest_fps.keys())
            ][:300]
            diff["latest_import_label"] = (latest.source_filename or f"#{latest.id}")[:120]
            diff["previous_import_label"] = (previous.source_filename or f"#{previous.id}")[:120]

    domains_out: list[dict[str, Any]] = []
    for dom, b in domain_stats.items():
        finalize_domain_risk_summary(b)
        samples = sorted(
            b.get("sample_links") or [],
            key=lambda ln: (int(ln.get("risk_score") or 0), ln.get("source_url") or ""),
        )[:8]
        override = actions.get(dom)
        eff = _effective_action(b["recommended_action"], override)
        ac = b.get("action_counts") or {}
        domains_out.append(
            {
                "domain": dom,
                "link_count": b["link_count"],
                "raw_row_count": int(b.get("raw_row_count") or 0),
                "max_risk_score": b["max_risk_score"],
                "min_risk_score": int(b.get("min_risk_score") or 0),
                "low_risk_pct": float(b.get("low_risk_pct") or 0),
                "domain_category": b.get("domain_category") or "mixed",
                "action_breakdown": {
                    ACTION_IGNORE: ac.get(ACTION_IGNORE, 0),
                    ACTION_MONITOR: ac.get(ACTION_MONITOR, 0),
                    ACTION_REVIEW: ac.get(ACTION_REVIEW, 0),
                    ACTION_DISAVOW: ac.get(ACTION_DISAVOW, 0),
                },
                "risk_flags": sorted(b["risk_flags"]),
                "recommended_action": b["recommended_action"],
                "effective_action": eff,
                "operator_action": override,
                "is_trusted_media": is_trusted_media_domain(dom),
                "sample_urls": b["sample_urls"],
                "sample_links": [
                    {k: v for k, v in ln.items() if k != "risk_score"} for ln in samples
                ],
            }
        )
    domains_out.sort(key=lambda x: (-x["link_count"], -x["max_risk_score"], x["domain"]))

    action_counts = {ACTION_IGNORE: 0, ACTION_MONITOR: 0, ACTION_REVIEW: 0, ACTION_DISAVOW: 0}
    category_counts = {"media": 0, "mostly_clean": 0, "mixed": 0, "spammy": 0, "unknown": 0}
    for d in domains_out:
        action_counts[d["effective_action"]] = action_counts.get(d["effective_action"], 0) + 1
        cat = d.get("domain_category") or "unknown"
        category_counts[cat] = category_counts.get(cat, 0) + 1

    rows_total = sum(int(i.row_count or 0) for i in imports)
    return {
        "site_id": site_id,
        "report_type": rt,
        "report_type_label": REPORT_TYPE_LABELS.get(rt, rt),
        "aggregate": {
            "import_count": len(imports),
            "rows_total": rows_total,
            "includes_all_imports": True,
        },
        "imports": [
            {
                "id": i.id,
                "row_count": i.row_count,
                "source_filename": i.source_filename or "",
                "source_kind": i.source_kind,
                "created_at": i.created_at.isoformat() if i.created_at else None,
                "report_type": i.report_type,
                "report_type_label": REPORT_TYPE_LABELS.get(i.report_type or "", i.report_type or ""),
            }
            for i in imports
        ],
        "latest_import_id": latest.id if latest else None,
        "previous_import_id": previous.id if previous else None,
        "diff": diff,
        "action_counts": action_counts,
        "category_counts": category_counts,
        "domains": domains_out,
        "domain_total": len(domains_out),
    }


def list_domain_links(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    domain: str,
    limit: int = 10000,
) -> dict[str, Any]:
    """Tek domain için tüm benzersiz kaynak+hedef linkleri (tüm importlar birleşik)."""
    rt = (report_type or "latest_links").strip().lower()
    dom = normalize_domain(domain) or (domain or "").strip().lower()
    if not dom:
        raise ValueError("Domain boş.")
    import_ids = [
        i.id
        for i in db.query(BacklinkImport)
        .filter(BacklinkImport.site_id == site_id, BacklinkImport.report_type == rt)
        .all()
    ]
    if not import_ids:
        return {
            "domain": dom,
            "report_type": rt,
            "link_count": 0,
            "links": [],
            "truncated": False,
        }
    rows = (
        db.query(BacklinkRow)
        .filter(
            BacklinkRow.site_id == site_id,
            BacklinkRow.import_id.in_(import_ids),
            BacklinkRow.domain == dom,
        )
        .all()
    )
    links = _link_entries_from_rows(rows)
    links.sort(key=lambda x: ((x.get("source_url") or "").lower(), (x.get("target_url") or "").lower()))
    cap = max(1, min(int(limit), 50000))
    truncated = len(links) > cap
    return {
        "domain": dom,
        "report_type": rt,
        "link_count": len(links),
        "links": links[:cap],
        "truncated": truncated,
    }


def _action_rank(action: str) -> int:
    order = {ACTION_IGNORE: 0, ACTION_MONITOR: 1, ACTION_REVIEW: 2, ACTION_DISAVOW: 3}
    return order.get((action or "").lower(), 1)


def set_domain_action(db: Session, *, site_id: int, domain: str, action: str) -> dict[str, Any]:
    dom = normalize_domain(domain) or (domain or "").strip().lower()
    if not dom:
        raise ValueError("Domain boş.")
    act = (action or "").strip().lower()
    if act not in (ACTION_IGNORE, ACTION_MONITOR, ACTION_REVIEW, ACTION_DISAVOW):
        raise ValueError("Geçersiz aksiyon.")
    row = (
        db.query(BacklinkDomainAction)
        .filter(BacklinkDomainAction.site_id == site_id, BacklinkDomainAction.domain == dom)
        .first()
    )
    if row is None:
        row = BacklinkDomainAction(site_id=site_id, domain=dom, action=act, updated_at=datetime.utcnow())
        db.add(row)
    else:
        row.action = act
        row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "site_id": site_id, "domain": dom, "action": act}


def build_disavow_text(db: Session, *, site_id: int, report_type: str = "latest_links") -> str:
    dash = build_dashboard(db, site_id=site_id, report_type=report_type)
    lines = ["# ProjectControl — GSC backlink disavow taslağı", f"# report_type={report_type}", ""]
    for d in dash.get("domains") or []:
        if d.get("effective_action") == ACTION_DISAVOW:
            lines.append(f"domain:{d['domain']}")
    if len(lines) <= 3:
        lines.append("# (disavow adayı domain yok)")
    return "\n".join(lines) + "\n"
