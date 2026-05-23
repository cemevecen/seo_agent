"""noreply@doviz.com ziyaret raporu e-postalarını tablo + renkli değişim olarak render eder."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser

_PCT_CELL_RE = re.compile(r"^%?\s*-?\d[\d.,]*$")
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
_ZIYARET_TEXT_ROW_RE = re.compile(
    r"(https?://\S+)\s+([\d.]+)\s+([\d.]+)\s+(%?-?[\d.,]+)\s+([\d.]+)\s+(%?-?[\d.,]+)"
)
_ZIYARET_HEADER = (
    "URL",
    "Bugün",
    "Dün",
    "Dün'e Göre Değişim",
    "Geçen Hafta",
    "Geçen Hafta'ya Göre Değişim",
)


def _pct_class(value: str) -> str:
    raw = (value or "").strip().replace("%", "").replace(" ", "")
    if not raw:
        return "inbox-ziyaret-neutral"
    if raw.count(",") == 1 and raw.count(".") >= 1:
        raw = raw.replace(".", "").replace(",", ".")
    else:
        raw = raw.replace(",", ".")
    try:
        num = float(raw)
    except ValueError:
        return "inbox-ziyaret-neutral"
    if num > 0:
        return "inbox-ziyaret-up"
    if num < 0:
        return "inbox-ziyaret-down"
    return "inbox-ziyaret-neutral"


def _cell_html(value: str, *, is_pct: bool = False) -> str:
    text = html.escape((value or "").strip())
    if not text:
        return "<td></td>"
    if is_pct or _PCT_CELL_RE.match(text.replace(" ", "")):
        cls = _pct_class(text)
        return f'<td class="{cls}">{text}</td>'
    if text.startswith("http://") or text.startswith("https://"):
        return (
            f'<td class="inbox-ziyaret-url"><a href="{html.escape(text)}" '
            f'target="_blank" rel="noopener">{text}</a></td>'
        )
    return f"<td>{text}</td>"


def _rows_from_plain_text(body_text: str) -> list[list[str]]:
    text = re.sub(r"\s+", " ", (body_text or "").strip())
    if not text:
        return []
    rows: list[list[str]] = []
    for m in _ZIYARET_TEXT_ROW_RE.finditer(text):
        rows.append([m.group(i) for i in range(1, 7)])
    return rows


class _TableCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._cur_table: list[list[str]] = []
        self._cur_row: list[str] = []
        self._cell_buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t == "table":
            self._in_table = True
            self._cur_table = []
        elif self._in_table and t == "tr":
            self._in_row = True
            self._cur_row = []
        elif self._in_row and t in ("td", "th"):
            self._in_cell = True
            self._cell_buf = []

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("td", "th") and self._in_cell:
            self._cur_row.append("".join(self._cell_buf).strip())
            self._in_cell = False
            self._cell_buf = []
        elif t == "tr" and self._in_row:
            if any(c.strip() for c in self._cur_row):
                self._cur_table.append(self._cur_row)
            self._in_row = False
            self._cur_row = []
        elif t == "table" and self._in_table:
            if self._cur_table:
                self.tables.append(self._cur_table)
            self._in_table = False
            self._cur_table = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_buf.append(data)


def _pick_table_rows(body_html: str, body_text: str) -> list[list[str]]:
    if body_html and "<table" in body_html.lower():
        parser = _TableCollector()
        try:
            parser.feed(body_html)
        except Exception:  # noqa: BLE001
            pass
        for table in parser.tables:
            if len(table) >= 2 and any(_URL_RE.search(" ".join(r)) for r in table[1:]):
                return table
    rows = _rows_from_plain_text(body_text)
    if rows:
        return [_ZIYARET_HEADER, *rows]
    return []


def _render_table_html(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    header = rows[0]
    body_rows = rows[1:]
    pct_cols = set()
    for i, h in enumerate(header):
        hl = h.lower()
        if "değişim" in hl or "degisim" in hl or "change" in hl:
            pct_cols.add(i)
    thead = "".join(f"<th>{html.escape(c)}</th>" for c in header)
    tbody_parts: list[str] = []
    for row in body_rows:
        cells: list[str] = []
        for i, val in enumerate(row):
            cells.append(_cell_html(val, is_pct=(i in pct_cols)))
        tbody_parts.append(f"<tr>{''.join(cells)}</tr>")
    return (
        '<div class="inbox-ziyaret-report">'
        '<table class="inbox-ziyaret-table"><thead><tr>'
        f"{thead}</tr></thead><tbody>{''.join(tbody_parts)}</tbody></table></div>"
    )


def render_ziyaret_message_html(*, body_html: str = "", body_text: str = "") -> str:
    """Ziyaret raporu gövdesini Gmail benzeri tablo + renkli yüzde ile döndürür."""
    rows = _pick_table_rows(body_html, body_text)
    if not rows:
        fallback = html.escape(body_text or "").replace("\n", "<br>")
        return f'<div class="inbox-ziyaret-fallback">{fallback}</div>'
    table = _render_table_html(rows)
    intro = ""
    plain = (body_text or "").strip()
    if plain:
        before_url = plain.split("https://", 1)[0].strip()
        if before_url and len(before_url) < 400 and "Bugün" not in before_url[:80]:
            intro = f'<p class="inbox-ziyaret-intro">{html.escape(before_url)}</p>'
    return f'<div class="inbox-ziyaret-wrap">{intro}{table}</div>'
