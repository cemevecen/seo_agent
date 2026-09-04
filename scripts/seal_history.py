#!/usr/bin/env python3
"""Mevcut panel verisini HISTORY_SEAL (varsayılan 2026-08-13) ile mühürle.

  .venv/bin/python scripts/seal_history.py
  .venv/bin/python scripts/seal_history.py --through 2026-08-13
  HISTORY_SEALED=0  # full backfill’i yeniden açmak için (Mac/Railway env)
  PLAY_FORCE_FULL=1          # tek seferlik HISTORY_START→seal ({PIPELINE}_FORCE_FULL)

Policy (Ad Manager) mühürlenmez — sistem baştan çekmeye devam eder.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    through: date | None = None
    if "--through" in args:
        i = args.index("--through")
        through = date.fromisoformat(args[i + 1][:10])
    from backend.services.history_seal import (
        history_seal,
        history_start,
        mark_all_expensive_pipelines_sealed,
    )

    seal = through or history_seal()
    meta = mark_all_expensive_pipelines_sealed(seal=seal)
    print(
        json.dumps(
            {
                "ok": True,
                "history_start": history_start().isoformat(),
                "seal_through": seal.isoformat(),
                "meta": meta,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
