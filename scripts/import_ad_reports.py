#!/usr/bin/env python3
"""Reklam Excel raporlarını DB'ye aktarır. Örnek:
  python scripts/import_ad_reports.py /path/to/report1.xlsx /path/to/report2.xlsx
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal, init_db  # noqa: E402
from backend.services import ad_analytics_store as store  # noqa: E402


def main() -> int:
    paths = [Path(p) for p in sys.argv[1:]]
    if not paths:
        print("Kullanım: import_ad_reports.py dosya1.xlsx [dosya2.xlsx ...]")
        return 1
    init_db()
    with SessionLocal() as db:
        for path in paths:
            if not path.is_file():
                print("Atlandı (yok):", path)
                continue
            raw = path.read_bytes()
            result = store.import_upload_file(db, raw, filename=path.name)
            print(path.name, "→", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
