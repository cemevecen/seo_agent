#!/usr/bin/env python3
"""Bir kerelik veya idempotent: templates/*.html içinde yaygın sınıflara dark: ekler.

`bg-white` önekini `bg-white/85` gibi opaklık sınıflarında eşleştirmemek için regex kullanılır.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "templates"

# Uzun eşleşmeler önce.
ORDERED: list[tuple[str, str]] = [
    ("hover:bg-white", "hover:bg-white dark:hover:bg-slate-800"),
    ("bg-white/85", "bg-white/85 dark:bg-slate-800/90"),
    ("bg-white/95", "bg-white/95 dark:bg-slate-800/95"),
    ("bg-white/80", "bg-white/80 dark:bg-slate-800/80"),
    ("bg-white/90", "bg-white/90 dark:bg-slate-800/90"),
    ("bg-white/70", "bg-white/70 dark:bg-slate-800/70"),
    ("bg-slate-50/80", "bg-slate-50/80 dark:bg-slate-900/55"),
    ("bg-slate-50/70", "bg-slate-50/70 dark:bg-slate-900/45"),
    ("group-hover:bg-slate-50 ", "group-hover:bg-slate-50 dark:group-hover:bg-slate-800 "),
    ('group-hover:bg-slate-50"', 'group-hover:bg-slate-50 dark:group-hover:bg-slate-800"'),
    ("hover:bg-slate-50", "hover:bg-slate-50 dark:hover:bg-slate-800/80"),
    ("hover:bg-slate-100", "hover:bg-slate-100 dark:hover:bg-slate-800"),
    ("border-slate-200", "border-slate-200 dark:border-slate-700"),
    ("border-slate-100", "border-slate-100 dark:border-slate-800"),
    ("border-slate-300", "border-slate-300 dark:border-slate-600"),
    ("ring-slate-200", "ring-slate-200 dark:ring-slate-700"),
    ("ring-slate-100", "ring-slate-100 dark:ring-slate-800"),
    ("ring-sky-100", "ring-sky-100 dark:ring-sky-900/40"),
    ("ring-violet-100", "ring-violet-100 dark:ring-violet-900/40"),
    ("text-slate-900", "text-slate-900 dark:text-slate-100"),
    ("text-slate-800", "text-slate-800 dark:text-slate-100"),
    ("text-slate-700", "text-slate-700 dark:text-slate-200"),
    ("text-slate-600", "text-slate-600 dark:text-slate-300"),
    ("text-slate-500", "text-slate-500 dark:text-slate-400"),
    ("placeholder:text-slate-400", "placeholder:text-slate-400 dark:placeholder:text-slate-500"),
]

# bg-white ve bg-slate-50 yalnızca opaklık eki yoksa (ör. bg-white/80 değil).
RE_BG_WHITE = re.compile(r"(?<![\w-])bg-white(?![\w/])")
RE_BG_SLATE_50 = re.compile(r"(?<![\w-])bg-slate-50(?![\w/])")
RE_BG_SLATE_100 = re.compile(r"(?<![\w-])bg-slate-100(?![\w/])")

REPL_BG_WHITE = "bg-white dark:bg-slate-900"
REPL_BG_SLATE_50 = "bg-slate-50 dark:bg-slate-900/50"
REPL_BG_SLATE_100 = "bg-slate-100 dark:bg-slate-800/70"


def apply_regex(text: str) -> str:
    text = RE_BG_WHITE.sub(REPL_BG_WHITE, text)
    text = RE_BG_SLATE_50.sub(REPL_BG_SLATE_50, text)
    text = RE_BG_SLATE_100.sub(REPL_BG_SLATE_100, text)
    return text


def main() -> None:
    for path in sorted(ROOT.rglob("*.html")):
        if path.name == "base.html":
            continue
        text = path.read_text(encoding="utf-8")
        orig = text
        for old, new in ORDERED:
            text = text.replace(old, new)
        text = apply_regex(text)
        if text != orig:
            path.write_text(text, encoding="utf-8")
            print("updated:", path.relative_to(ROOT.parent))


if __name__ == "__main__":
    main()
