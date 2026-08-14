"""GSC Links scrape → BacklinkImport snapshot store."""

from __future__ import annotations

import json
import logging
import unicodedata
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urlparse

from sqlalchemy.orm import Session

from backend.models import BacklinkImport, BacklinkRow, Site
from backend.services import backlink_csv
from backend.services.backlink_risk import (
    ACTION_IGNORE,
    ACTION_MONITOR,
    assess_linking_url,
    normalize_domain,
)
from backend.services.ga4_page_urls import ga4_site_host

LOGGER = logging.getLogger(__name__)

# Türkçe diyakritik → ASCII (ı/İ→i …). Snapshot’lar arası NEW+LOST aynası engeli.
_TR_FOLD = str.maketrans(
    {
        "ı": "i",
        "İ": "i",
        "ş": "s",
        "Ş": "s",
        "ğ": "g",
        "Ğ": "g",
        "ü": "u",
        "Ü": "u",
        "ö": "o",
        "Ö": "o",
        "ç": "c",
        "Ç": "c",
    }
)
LINK_TYPE_TO_REPORT: dict[str, str] = {
    "EXTERNAL": "external",
    "DOMAIN": "domain",
    "ANCHOR_TEXT": "anchor_text",
    "INTERNAL": "internal",
}

REPORT_TO_LINK_TYPE: dict[str, str] = {v: k for k, v in LINK_TYPE_TO_REPORT.items()}

# Panel site.domain → GSC resource listesi
SITE_GSC_PROPERTIES: dict[str, list[dict[str, str]]] = {
    "www.doviz.com": [
        {"resource_id": "sc-domain:doviz.com", "label": "doviz.com (domain)"},
        {"resource_id": "sc-domain:m.doviz.com", "label": "m.doviz.com"},
    ],
    "doviz.com": [
        {"resource_id": "sc-domain:doviz.com", "label": "doviz.com (domain)"},
        {"resource_id": "sc-domain:m.doviz.com", "label": "m.doviz.com"},
    ],
    "www.sinemalar.com": [
        {"resource_id": "https://www.sinemalar.com/", "label": "www.sinemalar.com"},
        {"resource_id": "https://m.sinemalar.com/", "label": "m.sinemalar.com"},
    ],
    "sinemalar.com": [
        {"resource_id": "https://www.sinemalar.com/", "label": "www.sinemalar.com"},
        {"resource_id": "https://m.sinemalar.com/", "label": "m.sinemalar.com"},
    ],
}


def resolve_site_domain(db: Session, site_domain: str) -> Site | None:
    want = (site_domain or "").strip().lower()
    if not want:
        return None
    naked = want[4:] if want.startswith("www.") else want
    candidates = [want, naked, f"www.{naked}"]
    sites = db.query(Site).filter(Site.is_active.is_(True)).all()
    for s in sites:
        d = (s.domain or "").strip().lower()
        if d in candidates:
            return s
        host = ga4_site_host(s.domain) or ""
        if host in candidates or host.endswith("." + naked) or naked.endswith("." + host):
            if naked in {"doviz.com", "sinemalar.com"} and naked in (host, d, d.replace("www.", "")):
                return s
    # fallback: domain contains key
    for key in ("doviz.com", "sinemalar.com"):
        if key in want:
            for s in sites:
                d = (s.domain or "").lower()
                if key in d and "canli" not in d:
                    return s
    return None


def properties_for_site(site: Site) -> list[dict[str, str]]:
    d = (site.domain or "").strip().lower()
    if d in SITE_GSC_PROPERTIES:
        return list(SITE_GSC_PROPERTIES[d])
    naked = d[4:] if d.startswith("www.") else d
    if naked in SITE_GSC_PROPERTIES:
        return list(SITE_GSC_PROPERTIES[naked])
    if "doviz.com" in d:
        return list(SITE_GSC_PROPERTIES["www.doviz.com"])
    if "sinemalar.com" in d:
        return list(SITE_GSC_PROPERTIES["www.sinemalar.com"])
    return []


def _rows_from_external(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        tgt = (r.get("target_url") or "").strip()
        if not tgt:
            continue
        inc = int(r.get("incoming_links") or 0)
        sites = int(r.get("linking_sites") or 0)
        out.append(
            {
                "source_url": tgt,
                "target_url": tgt,
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{inc}:{sites}",
                "last_crawled": "",
                "incoming_links": inc,
                "linking_sites": sites,
                "is_top_target_aggregate": True,
            }
        )
    return out


def _rows_from_internal(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        tgt = (r.get("target_url") or "").strip()
        if not tgt:
            continue
        inc = int(r.get("incoming_links") or 0)
        sites = int(r.get("linking_sites") or 0)
        out.append(
            {
                "source_url": tgt,
                "target_url": tgt,
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{inc}:{sites}",
                "last_crawled": "",
                "incoming_links": inc,
                "linking_sites": sites,
                "is_top_target_aggregate": True,
            }
        )
    return out


def _rows_from_domain(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        dom = (r.get("linking_site") or "").strip().lower()
        if not dom:
            continue
        pages = int(r.get("linking_pages") or 0)
        targets = int(r.get("target_pages") or 0)
        src = dom if dom.startswith("http") else f"http://{dom}/"
        out.append(
            {
                "source_url": src,
                "target_url": "",
                "anchor_text": f"{backlink_csv.GSC_TARGET_AGG_ANCHOR_PREFIX}{pages}:{targets}",
                "last_crawled": "",
                "incoming_links": pages,
                "linking_sites": targets,
                "is_linking_site_aggregate": True,
            }
        )
    return out


def _rows_from_anchor(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        text = (r.get("anchor_text") or "").strip()
        if not text:
            continue
        rank = int(r.get("rank") or 0)
        out.append(
            {
                "source_url": f"gsc-anchor://{quote(text, safe='')}",
                "target_url": "",
                "anchor_text": text[:512],
                "last_crawled": str(rank),
                "is_anchor_rank": True,
            }
        )
    return out


def ingest_snapshot(
    db: Session,
    *,
    site: Site,
    resource_id: str,
    link_type: str,
    rows: list[dict[str, Any]],
    kpis: dict[str, Any] | None = None,
    property_label: str = "",
    scraped_at: str = "",
) -> dict[str, Any]:
    lt = (link_type or "").strip().upper()
    report_type = LINK_TYPE_TO_REPORT.get(lt)
    if not report_type:
        return {"ok": False, "message": f"Geçersiz link_type: {link_type}"}

    if lt == "EXTERNAL":
        parsed = _rows_from_external(rows)
    elif lt == "INTERNAL":
        parsed = _rows_from_internal(rows)
    elif lt == "DOMAIN":
        parsed = _rows_from_domain(rows)
    else:
        parsed = _rows_from_anchor(rows)

    if not parsed:
        return {"ok": False, "message": "Snapshot satırı yok", "report_type": report_type}

    meta = {
        "kpis": kpis or {},
        "property_label": property_label,
        "scraped_at": scraped_at,
        "link_type": lt,
        "resource_id": resource_id,
        "row_count_raw": len(rows),
    }

    is_target_agg = lt in {"EXTERNAL", "INTERNAL"}
    is_domain_agg = lt == "DOMAIN"
    is_anchor = lt == "ANCHOR_TEXT"

    imp = BacklinkImport(
        site_id=site.id,
        report_type=report_type,
        source_filename=f"gsc_links|{resource_id}|{lt}"[:255],
        source_kind="gsc_scrape",
        row_count=0,
        created_at=datetime.utcnow(),
        gsc_resource_id=(resource_id or "")[:255],
        meta_json=json.dumps(meta, ensure_ascii=False),
    )
    db.add(imp)
    db.flush()

    row_models: list[BacklinkRow] = []
    batch_seen: set[str] = set()

    for item in parsed:
        if is_target_agg:
            tgt = (item.get("target_url") or item.get("source_url") or "").strip()
            if not tgt:
                continue
            # Scrape property scope — site host filter daha gevşek (m./kur. dahil)
            if not backlink_csv.target_url_belongs_to_site(tgt, site.domain or ""):
                # m.doviz property can still list www targets; keep if same registrable
                host = ""
                try:
                    from urllib.parse import urlparse

                    host = (urlparse(tgt).hostname or "").lower()
                except Exception:
                    host = ""
                site_host = (ga4_site_host(site.domain) or "").lower()
                naked = site_host[4:] if site_host.startswith("www.") else site_host
                if not host or not naked or naked not in host:
                    continue
            tkey = backlink_csv._canonical_target_key(tgt, site.domain or "")  # noqa: SLF001
            if tkey in batch_seen:
                continue
            batch_seen.add(tkey)
            from urllib.parse import urlparse

            host_dom = urlparse(tgt if tgt.startswith("http") else f"https://{tgt}").hostname or ""
            dom = (normalize_domain(host_dom or tgt) or ga4_site_host(site.domain) or "target")[:255]
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=tgt[:2048],
                    target_url=tgt[:2048],
                    domain=dom.lower(),
                    anchor_text=(item.get("anchor_text") or "")[:512],
                    last_crawled="",
                    risk_score=0,
                    risk_flags_json="[]",
                    recommended_action=ACTION_IGNORE,
                )
            )
            continue

        if is_domain_agg:
            src = item["source_url"]
            risk = assess_linking_url(src, anchor_text="", target_url="")
            dom = ((risk.get("domain") or normalize_domain(src)) or "").lower()[:255]
            if not dom or dom in batch_seen:
                continue
            batch_seen.add(dom)
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=src[:2048],
                    target_url="",
                    domain=dom,
                    anchor_text=(item.get("anchor_text") or "")[:512],
                    last_crawled="",
                    risk_score=int(risk.get("risk_score") or 0),
                    risk_flags_json=json.dumps(risk.get("risk_flags") or [], ensure_ascii=False),
                    recommended_action=str(risk.get("recommended_action") or ACTION_MONITOR),
                )
            )
            continue

        if is_anchor:
            text = (item.get("anchor_text") or "").strip()
            if not text:
                continue
            key = text.lower()
            if key in batch_seen:
                continue
            batch_seen.add(key)
            row_models.append(
                BacklinkRow(
                    import_id=imp.id,
                    site_id=site.id,
                    source_url=(item.get("source_url") or f"gsc-anchor://{quote(text)}")[:2048],
                    target_url="",
                    domain="gsc-anchor",
                    anchor_text=text[:512],
                    last_crawled=(item.get("last_crawled") or "")[:64],
                    risk_score=0,
                    risk_flags_json="[]",
                    recommended_action=ACTION_IGNORE,
                )
            )

    if row_models:
        db.bulk_save_objects(row_models)
    imp.row_count = len(row_models)
    db.commit()
    db.refresh(imp)
    return {
        "ok": True,
        "import_id": imp.id,
        "site_id": site.id,
        "report_type": report_type,
        "gsc_resource_id": resource_id,
        "row_count": imp.row_count,
        "link_type": lt,
    }


def ingest_gsc_links_payload(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    snapshots = payload.get("snapshots") or []
    if not isinstance(snapshots, list) or not snapshots:
        return {"ok": False, "message": "snapshots boş"}

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for snap in snapshots:
        if not isinstance(snap, dict):
            continue
        site_domain = (snap.get("site_domain") or "").strip()
        site = resolve_site_domain(db, site_domain)
        if site is None:
            errors.append(f"Site bulunamadı: {site_domain}")
            continue
        if not snap.get("ok") and not (snap.get("rows") or []):
            errors.append(f"{snap.get('property_label') or site_domain} · {snap.get('link_type')}: boş")
            continue
        try:
            one = ingest_snapshot(
                db,
                site=site,
                resource_id=str(snap.get("resource_id") or ""),
                link_type=str(snap.get("link_type") or ""),
                rows=list(snap.get("rows") or []),
                kpis=dict(snap.get("kpis") or {}),
                property_label=str(snap.get("property_label") or ""),
                scraped_at=str(snap.get("scraped_at") or payload.get("scraped_at") or ""),
            )
            results.append(one)
            if not one.get("ok"):
                errors.append(one.get("message") or "ingest fail")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("gsc links ingest snapshot failed")
            errors.append(str(exc))
            db.rollback()

    ok_n = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_n > 0,
        "message": f"GSC Links ingest · {ok_n}/{len(snapshots)} snapshot"
        + (f" · {len(errors)} hata" if errors else ""),
        "imported": results,
        "errors": errors[:20],
        "source": payload.get("source") or "gsc_links_bridge",
    }


def _agg_incoming(anchor: str) -> int:
    a, _ = backlink_csv._parse_gsc_agg_anchor(anchor)  # noqa: SLF001
    return int(a or 0)


def _agg_sites(anchor: str) -> int:
    _, b = backlink_csv._parse_gsc_agg_anchor(anchor)  # noqa: SLF001
    return int(b or 0)


_CHANGE_LIST_CAP = 100


def _fold_change_key(raw: str) -> str:
    """Karşılaştırma anahtarı: NFKC + TR fold + casefold + www strip.

    Aynı site bir taramada «mutfakcılar.com.tr», diğerinde «mutfakcilar.com.tr»
    gelirse NEW+LOST aynası oluşmasın.
    """
    s = unicodedata.normalize("NFKC", (raw or "").strip())
    if not s:
        return ""
    s = s.translate(_TR_FOLD).casefold()
    if s.startswith("www."):
        s = s[4:]
    return s


def _phonetic_change_key(raw: str) -> str:
    """TR/EN yazım varyantları: technopat↔teknopat, popneoism↔popneoizm."""
    s = _fold_change_key(raw)
    if not s:
        return ""
    for a, b in (
        ("ch", "k"),
        ("sh", "s"),
        ("ph", "f"),
        ("th", "t"),
        ("ism", "izm"),
        ("tion", "syon"),
        ("x", "ks"),
        ("w", "v"),
        ("q", "k"),
        ("cce", "kse"),
        ("cc", "k"),
    ):
        s = s.replace(a, b)
    # c → k (ca/co/cu/c + sessiz); ck zaten k
    out: list[str] = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "c" and i + 1 < len(s) and s[i + 1] not in "eiy":
            out.append("k")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _levenshtein(a: str, b: str, *, limit: int = 8) -> int:
    if a == b:
        return 0
    if not a or not b:
        return max(len(a), len(b))
    if abs(len(a) - len(b)) > limit:
        return limit + 1
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        row_min = i
        for j, cb in enumerate(b, start=1):
            ins = cur[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            v = min(ins, delete, sub)
            cur.append(v)
            if v < row_min:
                row_min = v
        if row_min > limit:
            return limit + 1
        prev = cur
    return prev[-1]


def _change_keys_similar(a: str, b: str) -> bool:
    """Encoding / yazım aynası mı? (çeviri domainleri hariç — count eşlemesi ayrıca)."""
    if not a or not b:
        return False
    fa, fb = _fold_change_key(a), _fold_change_key(b)
    if fa and fa == fb:
        return True
    pa, pb = _phonetic_change_key(a), _phonetic_change_key(b)
    if pa and pa == pb:
        return True
    # Etiket gövdesi (TLD'siz) kısa edit mesafesi
    def _hostish(x: str) -> str:
        x = _fold_change_key(x)
        if "/" in x or "://" in x:
            try:
                host = (urlparse(x if "://" in x else f"https://{x}").hostname or x) or x
                return _fold_change_key(host)
            except Exception:
                return x
        return x

    ha, hb = _hostish(a), _hostish(b)
    if ha and ha == hb:
        return True
    # aynı TLD + kısa mesafe
    def _split_tld(h: str) -> tuple[str, str]:
        parts = h.rsplit(".", 2)
        if len(parts) >= 3 and len(parts[-1]) <= 3 and len(parts[-2]) <= 3:
            return ".".join(parts[:-2]), ".".join(parts[-2:])
        if len(parts) >= 2:
            return parts[0], parts[-1] if len(parts) == 2 else ".".join(parts[1:])
        return h, ""

    ba, ta = _split_tld(ha)
    bb, tb = _split_tld(hb)
    if ta and ta == tb and ba and bb:
        lim = max(2, min(4, len(ba) // 8 + 1))
        if _levenshtein(ba, bb, limit=lim) <= lim:
            return True
    lim = max(2, min(4, max(len(fa), len(fb)) // 10 + 1))
    return bool(fa and fb and _levenshtein(fa, fb, limit=lim) <= lim)


def _cancel_false_churn(
    new_keys: list[str],
    lost_keys: list[str],
    latest_map: dict[str, Any],
    base_map: dict[str, Any],
    *,
    rt: str,
) -> tuple[list[str], list[str]]:
    """NEW↔LOST encoding aynasını düş (aynı count + benzer anahtar / tekil count çifti)."""
    if not new_keys or not lost_keys:
        return new_keys, lost_keys

    def _metric(k: str, m: dict[str, Any]) -> int:
        row = m.get(k) or {}
        if rt == "anchor_text":
            return int(row.get("rank") or 0)
        return int(row.get("count") or 0)

    def _label(k: str, m: dict[str, Any]) -> str:
        row = m.get(k) or {}
        return str(row.get("label") or k)

    new_left = list(new_keys)
    lost_left = list(lost_keys)
    used_lost: set[str] = set()

    # 1) Benzer yazım + aynı metrik
    for nk in list(new_left):
        n_metric = _metric(nk, latest_map)
        n_lab = _label(nk, latest_map)
        best: str | None = None
        for lk in lost_left:
            if lk in used_lost:
                continue
            if _metric(lk, base_map) != n_metric:
                continue
            if _change_keys_similar(n_lab, _label(lk, base_map)) or _change_keys_similar(nk, lk):
                best = lk
                break
        if best is not None:
            new_left.remove(nk)
            lost_left.remove(best)
            used_lost.add(best)

    # 2) Kalanlarda aynı metrik tekil eşleşme (spam farm çeviri çiftleri: campaign↔kampanya)
    n_counts = Counter(_metric(k, latest_map) for k in new_left)
    l_counts = Counter(_metric(k, base_map) for k in lost_left)
    for metric, nc in list(n_counts.items()):
        if metric <= 0 or nc != 1 or l_counts.get(metric) != 1:
            continue
        nk = next(k for k in new_left if _metric(k, latest_map) == metric)
        lk = next(k for k in lost_left if _metric(k, base_map) == metric)
        new_left.remove(nk)
        lost_left.remove(lk)

    return new_left, lost_left


def _upsert_change_row(out: dict[str, Any], key: str, row: dict[str, Any]) -> None:
    """Aynı folded key birden fazla satırda gelirse count/sites birleştir."""
    if not key:
        return
    prev = out.get(key)
    if not prev:
        out[key] = row
        return
    prev["count"] = max(int(prev.get("count") or 0), int(row.get("count") or 0))
    prev["sites"] = max(int(prev.get("sites") or 0), int(row.get("sites") or 0))
    # Görünen label: diyakritikli / daha uzun olanı tercih et
    lab = str(row.get("label") or "")
    prev_lab = str(prev.get("label") or "")
    if lab and (not prev_lab or len(lab) >= len(prev_lab)):
        prev["label"] = lab


def _naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _fingerprint_key_map(m: dict[str, Any]) -> tuple:
    """Stable fingerprint for equality (keys + count/sites/rank)."""
    parts = []
    for k in sorted(m.keys()):
        row = m[k] or {}
        parts.append(
            (
                k,
                int(row.get("count") or 0),
                int(row.get("sites") or 0),
                int(row.get("rank") or 0),
            )
        )
    return tuple(parts)


def _build_key_map(
    db: Session,
    imp: BacklinkImport,
    *,
    rt: str,
    site_domain: str = "",
) -> dict[str, Any]:
    rows = db.query(BacklinkRow).filter(BacklinkRow.import_id == imp.id).all()
    out: dict[str, Any] = {}
    if rt in {"external", "internal"}:
        for r in rows:
            raw = (r.source_url or r.target_url or "").strip()
            if not raw:
                continue
            key = backlink_csv._canonical_target_key(raw, site_domain) or raw  # noqa: SLF001
            key = _fold_change_key(key) or key
            _upsert_change_row(
                out,
                key,
                {
                    "key": key,
                    "label": raw,
                    "count": _agg_incoming(r.anchor_text or ""),
                    "sites": _agg_sites(r.anchor_text or ""),
                },
            )
        return out
    if rt == "domain":
        for r in rows:
            dom_raw = (r.domain or "").strip()
            if not dom_raw:
                dom_raw = normalize_domain(r.source_url or "") or ""
            if not dom_raw:
                continue
            key = _fold_change_key(dom_raw) or dom_raw.lower()
            _upsert_change_row(
                out,
                key,
                {
                    "key": key,
                    "label": dom_raw,
                    "count": _agg_incoming(r.anchor_text or ""),
                    "sites": _agg_sites(r.anchor_text or ""),
                },
            )
        return out
    # anchor_text
    for r in rows:
        text = (r.anchor_text or "").strip()
        if not text:
            continue
        key = _fold_change_key(text) or text.lower()
        rank_raw = r.last_crawled or ""
        rank = int(rank_raw) if str(rank_raw).isdigit() else 0
        prev = out.get(key)
        if prev and int(prev.get("rank") or 10**9) <= rank:
            continue
        out[key] = {
            "key": key,
            "label": text,
            "rank": rank,
            "count": 0,
            "sites": 0,
        }
    return out


def _attach_rank_index(m: dict[str, Any], *, rt: str) -> None:
    """Add 1-based rank_index by count (or stored rank for anchors)."""
    if rt == "anchor_text":
        ordered = sorted(
            m.items(),
            key=lambda kv: (int((kv[1] or {}).get("rank") or 10**9), kv[0]),
        )
    else:
        ordered = sorted(
            m.items(),
            key=lambda kv: (-int((kv[1] or {}).get("count") or 0), kv[0]),
        )
    for i, (_k, row) in enumerate(ordered, start=1):
        row["rank_index"] = i


def _diff_key_maps(
    latest_map: dict[str, Any],
    base_map: dict[str, Any],
    *,
    rt: str,
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    new_keys = sorted(
        set(latest_map) - set(base_map),
        key=lambda k: -int((latest_map.get(k) or {}).get("count") or 0),
    )
    lost_keys = sorted(
        set(base_map) - set(latest_map),
        key=lambda k: -int((base_map.get(k) or {}).get("count") or 0),
    )
    new_keys, lost_keys = _cancel_false_churn(
        new_keys, lost_keys, latest_map, base_map, rt=rt
    )
    changed: list[dict[str, Any]] = []
    for k in set(latest_map) & set(base_map):
        a = latest_map[k]
        b = base_map[k]
        if rt == "anchor_text":
            ra = int(a.get("rank") or 0)
            rb = int(b.get("rank") or 0)
            if ra != rb:
                changed.append(
                    {
                        "key": k,
                        "label": a.get("label"),
                        "rank_from": rb,
                        "rank_to": ra,
                        "delta_rank": rb - ra,
                    }
                )
            continue
        dcount = int(a.get("count") or 0) - int(b.get("count") or 0)
        dsites = int(a.get("sites") or 0) - int(b.get("sites") or 0)
        dri = int(a.get("rank_index") or 0) - int(b.get("rank_index") or 0)
        if not dcount and not dsites and not dri:
            continue
        item: dict[str, Any] = {
            "key": k,
            "label": a.get("label"),
            "count_from": b.get("count"),
            "count_to": a.get("count"),
            "delta": dcount,
        }
        if dsites:
            item["sites_from"] = b.get("sites")
            item["sites_to"] = a.get("sites")
            item["delta_sites"] = dsites
        if dri:
            # positive dri = fell in ranking (worse index)
            item["rank_from"] = b.get("rank_index")
            item["rank_to"] = a.get("rank_index")
            item["delta_rank"] = -dri  # keep ▲ = improved convention (baseline_idx - latest_idx)
        changed.append(item)
    changed.sort(
        key=lambda x: (
            abs(int(x.get("delta") or 0)),
            abs(int(x.get("delta_sites") or 0)),
            abs(int(x.get("delta_rank") or 0)),
        ),
        reverse=True,
    )
    return new_keys, lost_keys, changed


def build_change_window(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    gsc_resource_id: str = "",
    window: str = "daily",
) -> dict[str, Any]:
    """Günlük (~24s) veya haftalık (~7g) snapshot farkı + KPI serisi.

    GSC Links tablosu günlerce aynı kalabilir; bu yüzden hedef pencereye en yakın
    baseline ile başlayıp içerik birebir aynıysa geriye doğru ilk farklı
    snapshot'a kadar yürürüz (en fazla 14 gün).
    """
    rt = (report_type or "external").strip().lower()
    if rt == "top_target_pages":
        rt = "external"
    if rt == "top_target_pages_internal":
        rt = "internal"

    hours = 24 if (window or "daily").lower() in {"daily", "day", "1d"} else 24 * 7
    now = datetime.utcnow()
    site = db.query(Site).filter(Site.id == site_id).first()
    site_domain = (site.domain if site else "") or ""

    q = db.query(BacklinkImport).filter(
        BacklinkImport.site_id == site_id,
        BacklinkImport.report_type == rt,
    )
    rid = (gsc_resource_id or "").strip()
    if rid:
        q = q.filter(BacklinkImport.gsc_resource_id == rid)
    imports = q.order_by(BacklinkImport.created_at.desc()).limit(60).all()
    if not imports:
        return {
            "ok": True,
            "window": window,
            "has_baseline": False,
            "message": "Henüz tarama kaydı yok",
            "series": [],
            "diff": {},
        }

    latest = imports[0]
    latest_ts = _naive_utc(latest.created_at) or now
    target_ts = latest_ts - timedelta(hours=hours)

    candidates: list[BacklinkImport] = []
    for imp in imports[1:]:
        ts = _naive_utc(imp.created_at)
        if not ts:
            continue
        # window hedefinden biraz daha yeni olanları da aday tut (6s tolerans)
        if ts <= target_ts + timedelta(hours=6):
            candidates.append(imp)

    baseline: BacklinkImport | None = None
    if candidates:
        baseline = min(
            candidates,
            key=lambda i: abs(((_naive_utc(i.created_at) or target_ts) - target_ts).total_seconds()),
        )
    elif len(imports) > 1:
        baseline = imports[-1] if hours >= 24 * 6 else imports[1]

    latest_map = _build_key_map(db, latest, rt=rt, site_domain=site_domain)
    _attach_rank_index(latest_map, rt=rt)

    base_map: dict[str, Any] = {}
    baseline_relaxed = False
    if baseline is not None:
        base_map = _build_key_map(db, baseline, rt=rt, site_domain=site_domain)
        _attach_rank_index(base_map, rt=rt)
        latest_fp = _fingerprint_key_map(latest_map)
        # İçerik aynıysa: 14 gün içindeki ilk farklı snapshot'a yürü
        if latest_fp and latest_fp == _fingerprint_key_map(base_map):
            walk_until = latest_ts - timedelta(days=14)
            for cand in imports[1:]:
                cts = _naive_utc(cand.created_at)
                if not cts or cts < walk_until:
                    break
                if cand.id == getattr(baseline, "id", None):
                    continue
                cmap = _build_key_map(db, cand, rt=rt, site_domain=site_domain)
                _attach_rank_index(cmap, rt=rt)
                if _fingerprint_key_map(cmap) != latest_fp:
                    baseline = cand
                    base_map = cmap
                    baseline_relaxed = True
                    break

    new_keys, lost_keys, changed = _diff_key_maps(latest_map, base_map, rt=rt)

    series = []
    for imp in reversed(imports[:14]):
        meta: dict[str, Any] = {}
        try:
            meta = json.loads(imp.meta_json or "{}")
        except json.JSONDecodeError:
            meta = {}
        kpis = meta.get("kpis") or {}
        series.append(
            {
                "import_id": imp.id,
                "created_at": imp.created_at.isoformat() if imp.created_at else None,
                "row_count": imp.row_count,
                "mapped_rows": None,
                "total_links": kpis.get("total_links"),
                "gsc_resource_id": imp.gsc_resource_id or "",
            }
        )

    latest_meta: dict[str, Any] = {}
    try:
        latest_meta = json.loads(latest.meta_json or "{}")
    except json.JSONDecodeError:
        latest_meta = {}

    baseline_meta: dict[str, Any] = {}
    if baseline is not None:
        try:
            baseline_meta = json.loads(baseline.meta_json or "{}")
        except json.JSONDecodeError:
            baseline_meta = {}

    latest_links = int((latest_meta.get("kpis") or {}).get("total_links") or 0)
    base_links = int((baseline_meta.get("kpis") or {}).get("total_links") or 0)
    kpi_delta_links = latest_links - base_links if baseline is not None else None

    empty_reason = ""
    if baseline is None:
        empty_reason = "baseline_missing"
    elif not latest_map and int(latest.row_count or 0) > 0:
        empty_reason = "latest_rows_unmapped"
    elif baseline is not None and not base_map and int(baseline.row_count or 0) > 0:
        empty_reason = "baseline_rows_missing"
    elif not new_keys and not lost_keys and not changed:
        empty_reason = "snapshots_identical"

    return {
        "ok": True,
        "window": "daily" if hours <= 24 else "weekly",
        "window_hours": hours,
        "has_baseline": bool(baseline),
        "baseline_relaxed": baseline_relaxed,
        "report_type": rt,
        "gsc_resource_id": rid or (latest.gsc_resource_id or ""),
        "latest": {
            "import_id": latest.id,
            "created_at": latest.created_at.isoformat() if latest.created_at else None,
            "row_count": latest.row_count,
            "mapped_keys": len(latest_map),
            "kpis": (latest_meta.get("kpis") or {}),
            "label": latest.source_filename,
        },
        "baseline": (
            {
                "import_id": baseline.id,
                "created_at": baseline.created_at.isoformat() if baseline.created_at else None,
                "row_count": baseline.row_count,
                "mapped_keys": len(base_map),
                "label": baseline.source_filename,
                "kpis": (baseline_meta.get("kpis") or {}),
            }
            if baseline
            else None
        ),
        "kpi_delta": {
            "total_links": kpi_delta_links,
            "latest_total_links": latest_links or None,
            "baseline_total_links": base_links or None,
        },
        "empty_reason": empty_reason,
        "diff": {
            "new_count": len(new_keys),
            "lost_count": len(lost_keys),
            "changed_count": len(changed),
            "new": [latest_map[k] for k in new_keys[:_CHANGE_LIST_CAP]],
            "lost": [base_map[k] for k in lost_keys[:_CHANGE_LIST_CAP]],
            "changed": changed[:_CHANGE_LIST_CAP],
            "truncated": (
                len(new_keys) > _CHANGE_LIST_CAP
                or len(lost_keys) > _CHANGE_LIST_CAP
                or len(changed) > _CHANGE_LIST_CAP
            ),
        },
        "series": series,
    }
