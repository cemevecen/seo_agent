"""noreply@doviz.com ziyaret raporu e-postalarını tablo + renkli değişim olarak render eder."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser

_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)
_ZIYARET_TEXT_ROW_RE = re.compile(
    r"(https?://\S+)\s+([\d.]+)\s+([\d.]+)\s+(%?-?[\d.,]+)\s+([\d.]+)\s+(%?-?[\d.,]+)"
)
_ZIYARET_HEADER = (
    "URL",
    "Bugün",
    "Dün",
    "günlük fark",
    "Geçen Hafta",
    "haftalık fark",
)


_ZIYARET_URL_DISPLAY_LEN = 45
# nstat sekmesi — yalnızca bu konu kalıbı (noreply@doviz.com ziyaret raporu)
ZIYARET_REPORT_SUBJECT_MARKERS = (
    "en çok ziyaret edilen sayfalar",
    "en cok ziyaret edilen sayfalar",
)


def _normalize_subject_key(subject: str) -> str:
    s = (subject or "").strip().lower()
    for src, dst in (
        ("ı", "i"),
        ("ş", "s"),
        ("ğ", "g"),
        ("ü", "u"),
        ("ö", "o"),
        ("ç", "c"),
    ):
        s = s.replace(src, dst)
    return re.sub(r"\s+", " ", s)


def is_ziyaret_report_subject(subject: str) -> bool:
    """noreply@doviz.com «En çok ziyaret edilen sayfalar» raporu mu?"""
    key = _normalize_subject_key(subject)
    return any(marker in key for marker in ZIYARET_REPORT_SUBJECT_MARKERS)


def _display_url(url: str) -> tuple[str, str]:
    """Tam href + ekranda https:// olmadan, en fazla 45 karakter."""
    href = (url or "").strip()
    if href and not re.match(r"^https?://", href, re.I):
        href = "https://" + href.lstrip("/")
    display = re.sub(r"^https?://", "", href, flags=re.I)
    # nstat: haber.doviz.com/path → /path (domain gösterilmez)
    if re.match(r"^haber\.doviz\.com(?:/|$)", display, re.I):
        rest = display[len("haber.doviz.com") :]
        display = rest if rest.startswith("/") else ("/" + rest if rest else "/")
    if len(display) > _ZIYARET_URL_DISPLAY_LEN:
        display = display[: _ZIYARET_URL_DISPLAY_LEN - 1] + "…"
    return href, display


def _normalize_header_label(label: str) -> str:
    h = (label or "").strip()
    hl = h.lower()
    if ("dün" in hl or "dun" in hl) and ("değişim" in hl or "degisim" in hl or "göre" in hl or "gore" in hl):
        return "günlük fark"
    if "geçen hafta" in hl and ("değişim" in hl or "degisim" in hl or "göre" in hl or "gore" in hl):
        return "haftalık fark"
    if hl in ("günlük fark", "gunluk fark"):
        return "günlük fark"
    if hl in ("haftalık fark", "haftalik fark"):
        return "haftalık fark"
    return h


def _mobile_header_label(label: str) -> str:
    hl = (label or "").strip().lower()
    mapping = {
        "bugün": "Bugün",
        "bugun": "Bugün",
        "dün": "Dün",
        "dun": "Dün",
        "günlük fark": "Günlük",
        "gunluk fark": "Günlük",
        "geçen hafta": "Hafta",
        "gecen hafta": "Hafta",
        "haftalık fark": "Haftalık",
        "haftalik fark": "Haftalık",
    }
    return mapping.get(hl, (label or "").strip())


def _is_pct_column(header: str) -> bool:
    hl = (header or "").strip().lower()
    if hl in ("günlük fark", "gunluk fark", "haftalık fark", "haftalik fark"):
        return True
    return "değişim" in hl or "degisim" in hl or "change" in hl or hl.endswith(" fark")


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


def _cell_html(value: str, *, is_pct: bool = False, is_url_col: bool = False) -> str:
    text = html.escape((value or "").strip())
    if not text:
        return "<td></td>"
    if is_url_col or text.startswith("http://") or text.startswith("https://"):
        href, display = _display_url(text)
        return (
            f'<td class="inbox-ziyaret-url" title="{html.escape(href)}">'
            f'<a href="{html.escape(href)}" target="_blank" rel="noopener">'
            f"{html.escape(display)}</a></td>"
        )
    if is_pct:
        cls = _pct_class(text)
        return f'<td class="{cls}">{text}</td>'
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


_SECTION_TITLE_RE = re.compile(
    r"(desktop\s*(?:web|trafik)?|mobil(?:e)?\s*(?:web|trafik)?|web\s*desktop|web\s*mobil|masaüstü|masaustu)",
    re.I,
)
_DEFAULT_TABLE_TITLES = ("Desktop Web", "Mobil Web")


def _is_visit_table(rows: list[list[str]]) -> bool:
    if len(rows) < 2:
        return False
    return any(_URL_RE.search(" ".join(r)) for r in rows[1:])


def _title_from_html_chunk(chunk: str) -> str:
    chunk = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", chunk or "")
    for pat in (
        r"(?is)<h[1-6][^>]*>([^<]+)</h[1-6]>",
        r"(?is)<(?:b|strong)[^>]*>([^<]+)</(?:b|strong)>",
        r"(?is)<p[^>]*>([^<]{3,120})</p>",
        r"(?is)<td[^>]*>([^<]{3,120})</td>",
    ):
        for raw in reversed(re.findall(pat, chunk)):
            clean = html.unescape(re.sub(r"\s+", " ", raw).strip())
            if not clean or len(clean) > 120:
                continue
            if _SECTION_TITLE_RE.search(clean) or clean.lower() in ("desktop", "mobil", "mobile", "web"):
                return clean
    plain = html.unescape(re.sub(r"<[^>]+>", "\n", chunk))
    lines = [ln.strip() for ln in plain.splitlines() if ln.strip()]
    for ln in reversed(lines[-8:]):
        if _SECTION_TITLE_RE.search(ln):
            return ln
        if len(ln) <= 48 and not _URL_RE.search(ln) and "bugün" not in ln.lower():
            return ln
    return ""


def _extract_titles_before_tables(body_html: str, table_count: int) -> list[str]:
    parts = re.split(r"(?is)<table\b", body_html or "")
    titles: list[str] = []
    for i in range(1, min(len(parts), table_count + 1)):
        titles.append(_title_from_html_chunk(parts[i - 1][-1200:]))
    while len(titles) < table_count:
        titles.append("")
    return titles[:table_count]


def _guess_table_title(title_hint: str, index: int, total: int) -> str:
    hint = (title_hint or "").strip()
    if hint:
        return hint
    if total == 2 and index < 2:
        return _DEFAULT_TABLE_TITLES[index]
    if total > 1:
        return f"Tablo {index + 1}"
    return ""


def _pick_visit_tables(body_html: str, body_text: str) -> list[tuple[str, list[list[str]]]]:
    """E-postadaki tüm ziyaret tablolarını (Desktop + Mobil vb.) başlıkla döndürür."""
    if body_html and "<table" in body_html.lower():
        parser = _TableCollector()
        try:
            parser.feed(body_html)
        except Exception:  # noqa: BLE001
            pass
        visit_tables = [t for t in parser.tables if _is_visit_table(t)]
        if visit_tables:
            titles = _extract_titles_before_tables(body_html, len(visit_tables))
            return [
                (_guess_table_title(titles[i], i, len(visit_tables)), visit_tables[i])
                for i in range(len(visit_tables))
            ]
    rows = _rows_from_plain_text(body_text)
    if rows:
        return [("", [_ZIYARET_HEADER, *rows])]
    return []


def _pick_table_rows(body_html: str, body_text: str) -> list[list[str]]:
    tables = _pick_visit_tables(body_html, body_text)
    if not tables:
        return []
    return tables[0][1]


def _render_table_html(rows: list[list[str]], *, title: str = "") -> str:
    if not rows:
        return ""
    header = [_normalize_header_label(c) for c in rows[0]]
    body_rows = rows[1:]
    pct_cols = {i for i, h in enumerate(header) if _is_pct_column(h)}
    thead_parts: list[str] = []
    for i, c in enumerate(header):
        cls = ' class="inbox-ziyaret-url"' if i == 0 else ""
        if i == 0:
            label_html = html.escape(c)
        else:
            short = _mobile_header_label(c)
            label_html = (
                f'<span class="inbox-ziyaret-head-full">{html.escape(c)}</span>'
                f'<span class="inbox-ziyaret-head-short">{html.escape(short)}</span>'
            )
        thead_parts.append(f'<th{cls} title="{html.escape(c)}">{label_html}</th>')
    thead = "".join(thead_parts)
    tbody_parts: list[str] = []
    for row in body_rows:
        cells: list[str] = []
        for i, val in enumerate(row):
            cells.append(_cell_html(val, is_pct=(i in pct_cols), is_url_col=(i == 0)))
        tbody_parts.append(f"<tr>{''.join(cells)}</tr>")
    title_html = (
        f'<p class="inbox-ziyaret-section-title">{html.escape(title)}</p>'
        if (title or "").strip()
        else ""
    )
    colgroup = (
        '<colgroup><col class="inbox-ziyaret-col-url">'
        + '<col class="inbox-ziyaret-col-metric">' * max(0, len(header) - 1)
        + "</colgroup>"
    )
    return (
        f'<section class="inbox-ziyaret-section">{title_html}'
        '<div class="inbox-ziyaret-report">'
        f'<table class="inbox-ziyaret-table">{colgroup}<thead><tr>'
        f"{thead}</tr></thead><tbody>{''.join(tbody_parts)}</tbody></table></div></section>"
    )


def render_ziyaret_message_html(*, body_html: str = "", body_text: str = "") -> str:
    """Ziyaret raporu gövdesini Gmail benzeri tablo + renkli yüzde ile döndürür."""
    tables = _pick_visit_tables(body_html, body_text)
    if not tables:
        fallback = html.escape(body_text or "").replace("\n", "<br>")
        return f'<div class="inbox-ziyaret-fallback">{fallback}</div>'
    rendered = "".join(_render_table_html(rows, title=title) for title, rows in tables)
    intro = ""
    plain = (body_text or "").strip()
    if plain:
        before_url = plain.split("https://", 1)[0].strip()
        if before_url and len(before_url) < 400 and "Bugün" not in before_url[:80]:
            intro = f'<p class="inbox-ziyaret-intro">{html.escape(before_url)}</p>'
    return f'<div class="inbox-ziyaret-wrap">{intro}{rendered}</div>'


def ziyaret_thread_preview(body_text: str, *, max_rows: int = 2) -> str:
    """Liste satırı için kısa önizleme."""
    rows = _rows_from_plain_text(body_text)
    if not rows:
        plain = re.sub(r"\s+", " ", (body_text or "").strip())
        return plain[:240] + ("…" if len(plain) > 240 else "")
    bits: list[str] = []
    for row in rows[:max_rows]:
        url = re.sub(r"^https?://", "", row[0], flags=re.I)
        if len(url) > _ZIYARET_URL_DISPLAY_LEN:
            url = url[: _ZIYARET_URL_DISPLAY_LEN - 1] + "…"
        bits.append(f"{url} · bugün {row[1]} · {row[3]}")
    suffix = f" (+{len(rows) - max_rows} satır)" if len(rows) > max_rows else ""
    return " | ".join(bits) + suffix
