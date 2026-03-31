#!/usr/bin/env python3
"""
GA4: doviz ailesi için son 30 günde en çok oturum alan ilk 500 landing (host+path),
üretilen https URL'leri HTTP ile doğrular. Breadcrumb: sayfada BreadcrumbList şeması var mı notu.

Çalıştırma (repo kökü seo-agent):
  cd seo-agent && python scripts/audit_doviz_ga4_links.py
  python scripts/audit_doviz_ga4_links.py --output /tmp/doviz_audit.json

Gereksinim: .env içinde GA4 service account + veritabanında doviz site + GA4 property (web).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import httpx  # noqa: E402

from backend.collectors.ga4 import fetch_ga4_top_landing_audit  # noqa: E402
from backend.database import SessionLocal, init_db  # noqa: E402
from backend.models import Site  # noqa: E402
from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties  # noqa: E402

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _breadcrumb_note(html: str) -> str:
    if not html:
        return "empty body"
    if "BreadcrumbList" in html:
        return "has BreadcrumbList"
    if 'rel="canonical"' in html or "rel='canonical'" in html:
        return "has canonical (no breadcrumb schema)"
    return "no BreadcrumbList in HTML"


def main() -> int:
    parser = argparse.ArgumentParser(description="Doviz GA4 top 500 landing URL HTTP audit")
    parser.add_argument("--days", type=int, default=30, help="Son N gün (varsayılan 30)")
    parser.add_argument("--limit", type=int, default=500, help="En fazla kaç satır (max 500)")
    parser.add_argument("--output", type=str, default="", help="Başarısız satırları JSON dosyasına yaz")
    parser.add_argument("--sleep", type=float, default=0.08, help="İstekler arası saniye")
    args = parser.parse_args()

    init_db()
    with SessionLocal() as db:
        site = (
            db.query(Site)
            .filter(Site.domain.ilike("%doviz%"))
            .filter(Site.is_active.is_(True))
            .order_by(Site.id.asc())
            .first()
        )
        if not site:
            print("Aktif ve domain’inde 'doviz' geçen site yok.", file=sys.stderr)
            return 1
        rec = get_ga4_credentials_record(db, site.id)
        props = load_ga4_properties(rec)
        property_id = (props.get("web") or "").strip()
        if not property_id:
            print("Bu site için GA4 web property_id tanımlı değil.", file=sys.stderr)
            return 1

    print(f"Site: {site.domain}  GA4 property: {property_id}")
    rows = fetch_ga4_top_landing_audit(
        property_id=property_id,
        days=args.days,
        limit=min(args.limit, 500),
        exclude_news=True,
    )
    print(f"GA4 satır sayısı: {len(rows)}")

    ok = 0
    failures: list[dict] = []
    with httpx.Client(
        timeout=25.0,
        follow_redirects=True,
        max_redirects=20,
        headers={"User-Agent": UA},
    ) as client:
        for i, r in enumerate(rows):
            url = (r.get("page_url") or "").strip()
            host = r.get("page_host") or ""
            path = r.get("page") or ""
            if not url:
                failures.append(
                    {
                        "rank": i + 1,
                        "reason": "empty_page_url",
                        "ga4_host": host,
                        "path": path,
                        "sessions": r.get("sessions"),
                    }
                )
                continue
            try:
                resp = client.get(url)
                status = resp.status_code
                final = str(resp.url)
                body = resp.text or ""
                note = _breadcrumb_note(body[:800_000])
                row_out = {
                    "rank": i + 1,
                    "requested": url,
                    "status": status,
                    "final_url": final,
                    "ga4_host": host,
                    "path": path,
                    "sessions": r.get("sessions"),
                    "breadcrumb_note": note,
                }
                if 200 <= status < 400:
                    ok += 1
                else:
                    failures.append(row_out)
                if (i + 1) % 100 == 0:
                    print(f"  … {i + 1}/{len(rows)} işlendi (şu ana kadar HTTP ok: {ok})")
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "rank": i + 1,
                        "requested": url,
                        "error": str(exc),
                        "ga4_host": host,
                        "path": path,
                        "sessions": r.get("sessions"),
                    }
                )
            time.sleep(args.sleep)

    print(f"\nÖzet: HTTP 2xx/3xx (başarılı): {ok}  Sorunlu: {len(failures)}  Toplam: {len(rows)}")
    if failures:
        preview = failures[:25]
        print("\nİlk sorunlu örnekler:")
        for f in preview:
            print(json.dumps(f, ensure_ascii=False)[:500])
        if args.output:
            Path(args.output).write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"\nTüm sorunlular: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
