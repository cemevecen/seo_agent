#!/usr/bin/env python3
"""stdin'den .env okur; # satırlarına dokunmaz, tekrarlayan KEY= satırlarından birini bırakır."""
from __future__ import annotations

import sys


def main() -> None:
    lines = sys.stdin.read().splitlines()
    # key -> [(line_index, value_stripped_or_none)]
    by_key: dict[str, list[tuple[int, str]]] = {}
    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, _, rest = s.partition("=")
        k = k.strip()
        by_key.setdefault(k, []).append((i, rest))

    winners: dict[str, int] = {}
    for k, occ in by_key.items():
        if len(occ) == 1:
            winners[k] = occ[0][0]
            continue
        # Son boş olmayan kazanır; hepsi boşsa son satır
        w = occ[-1][0]
        for idx, val in reversed(occ):
            if val.strip():
                w = idx
                break
        winners[k] = w

    out: list[str] = []
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            out.append(line)
            continue
        if s.startswith("#"):
            out.append(line)
            continue
        if "=" not in s:
            out.append(line)
            continue
        k = s.partition("=")[0].strip()
        if k not in winners:
            out.append(line)
            continue
        if i == winners[k]:
            out.append(line)
        # else: duplicate, skip

    sys.stdout.write("\n".join(out))
    if lines and not lines[-1].endswith("\n"):
        pass
    else:
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
