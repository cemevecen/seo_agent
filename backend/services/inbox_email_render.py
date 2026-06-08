"""Gelen kutusu ileti gösterimi — HTML sanitize, charset, placeholder düz metin filtre."""

from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import urlparse

from backend.services.inbox_visit_report import render_ziyaret_message_html

# Gönderdiğimiz / yaygın multipart placeholder metinleri — HTML varken gösterilmez
# UTF-8 baytları Latin-1/CP1252 gibi okununca oluşan tipik bozulma (KÃ¼ltÃ¼r → Kültür).
_MOJIBAKE_MARKER_RE = re.compile(
    r"Ã[\x80-\xBF]|Ä[\x80-\xBF]|Å[\x80-\xBF]|Â[\x80-\xBF]|â€[™œžŸ]|ï»¿"
)


def _inbox_text_quality(text: str) -> int:
    score = 0
    for ch in text:
        if ch in "şğıüöçŞĞİÜÖÇ":
            score += 4
    score -= text.count("Ã") * 3
    score -= text.count("Ä") * 3
    score -= text.count("Å") * 3
    score -= text.count("Â") * 2
    score -= text.count("\ufffd") * 8
    return score


def repair_utf8_mojibake(text: str) -> str:
    """UTF-8 içeriğin ISO-8859-1/CP1252 sanılarak okunmasından kaynaklanan metni onar."""
    if not text:
        return text
    out = text
    for _ in range(3):
        if not _MOJIBAKE_MARKER_RE.search(out) and "Ã" not in out and "Ä" not in out and "Å" not in out:
            break
        fixed: str | None = None
        for enc in ("latin-1", "cp1252"):
            try:
                fixed = out.encode(enc).decode("utf-8")
                break
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
        if not fixed or fixed == out:
            break
        if _inbox_text_quality(fixed) >= _inbox_text_quality(out):
            out = fixed
        else:
            break
    return out


def normalize_inbox_text(text: str | None) -> str:
    """Inbox’ta gösterilen / saklanan metin — bozuk charset onarımı."""
    if not text:
        return ""
    return repair_utf8_mojibake(text)


_PLACEHOLDER_PLAIN_RE = re.compile(
    r"^(?:this is a plain-text fallback for the html email\.?|"
    r"plain-?text fallback.*|"
    r"ga4 realtime alarm — düz metin özet\.?|"
    r"html e-?posta için düz metin.*)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_STRIP_TAGS_RE = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style|iframe|object|embed|form|link|meta)\b[^>]*>.*?</\1>")
_EVENT_ATTR_RE = re.compile(r"""\s+on\w+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)""", re.IGNORECASE)
_JS_HREF_RE = re.compile(r"""\s+href\s*=\s*["']\s*javascript:[^"']*["']""", re.IGNORECASE)
_CID_SRC_RE = re.compile(r"""\ssrc\s*=\s*["']cid:[^"']*["']""", re.IGNORECASE)

_ALLOWED_TAGS = frozenset(
    {
        "a",
        "b",
        "blockquote",
        "br",
        "div",
        "em",
        "h1",
        "h2",
        "h3",
        "h4",
        "hr",
        "i",
        "img",
        "li",
        "ol",
        "p",
        "pre",
        "span",
        "strong",
        "table",
        "tbody",
        "td",
        "th",
        "thead",
        "tr",
        "u",
        "ul",
    }
)


def is_placeholder_plain_text(text: str | None) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if _PLACEHOLDER_PLAIN_RE.match(t):
        return True
    if len(t) < 120 and "plain-text fallback" in t.lower():
        return True
    return False


def html_to_plain_text(html_body: str) -> str:
    """HTML'den okunabilir düz metin (önizleme / LLM için)."""
    text = html_body or ""
    text = _SCRIPT_STYLE_RE.sub(" ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?i)</t[dh]\s*>", "\t", text)
    text = _STRIP_TAGS_RE.sub(" ", text)
    text = html.unescape(text)
    text = normalize_inbox_text(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def effective_plain_text(body_text: str | None, body_html: str | None) -> str:
    """Gösterim için anlamlı düz metin — placeholder'ları atlar."""
    plain = (body_text or "").strip()
    html_part = (body_html or "").strip()
    if html_part and is_placeholder_plain_text(plain):
        return html_to_plain_text(html_part)
    if not plain and html_part:
        return html_to_plain_text(html_part)
    if is_placeholder_plain_text(plain):
        return html_to_plain_text(html_part) if html_part else ""
    return normalize_inbox_text(plain)


def _safe_url(url: str, *, allow_mailto: bool = True) -> str | None:
    u = (url or "").strip()
    if not u or u.startswith("//"):
        return None
    low = u.lower()
    if low.startswith(("http://", "https://")):
        return html.escape(u, quote=True)
    if allow_mailto and low.startswith("mailto:"):
        return html.escape(u, quote=True)
    if low.startswith("data:image/"):
        return html.escape(u, quote=True)
    return None


def _sanitize_attrs(tag: str, attr_blob: str) -> str:
    """İzinli attribute'ları koru; event handler ve javascript href kaldır."""
    attrs: list[str] = []
    for m in re.finditer(
        r'([a-zA-Z_:][\w:.-]*)\s*=\s*("([^"]*)"|\'([^\']*)\'|([^\s>]+))',
        attr_blob,
    ):
        name = (m.group(1) or "").lower()
        val = m.group(3) or m.group(4) or m.group(5) or ""
        if name.startswith("on"):
            continue
        if name == "href":
            safe = _safe_url(val, allow_mailto=True)
            if safe:
                attrs.append(f'href="{safe}" target="_blank" rel="noopener noreferrer"')
            continue
        if name == "src":
            safe = _safe_url(val, allow_mailto=False)
            if safe:
                attrs.append(f'src="{safe}"')
            continue
        if name in ("alt", "title", "colspan", "rowspan", "width", "height", "align", "valign", "class", "style"):
            if name == "style":
                style = re.sub(r"(?i)expression\s*\(", "", val)
                style = re.sub(r"(?i)url\s*\(\s*['\"]?\s*javascript:", "", style)
                attrs.append(f'style="{html.escape(style, quote=True)}"')
            else:
                attrs.append(f'{name}="{html.escape(val, quote=True)}"')
    return " ".join(attrs)


def sanitize_email_html(html_body: str) -> str:
    """XSS'siz, kırık link/img azaltılmış HTML — inbox iframe'siz gösterim."""
    raw = normalize_inbox_text(html.unescape((html_body or "").strip()))
    if not raw:
        return ""
    raw = _SCRIPT_STYLE_RE.sub("", raw)
    raw = _EVENT_ATTR_RE.sub("", raw)
    raw = _JS_HREF_RE.sub("", raw)
    raw = _CID_SRC_RE.sub(' src=""', raw)

    def repl_tag(m: re.Match[str]) -> str:
        closing = m.group(1)
        tag = (m.group(2) or "").lower()
        rest = m.group(3) or ""
        if closing:
            return f"</{tag}>" if tag in _ALLOWED_TAGS else ""
        if tag not in _ALLOWED_TAGS:
            return ""
        attrs = _sanitize_attrs(tag, rest) if rest else ""
        return f"<{tag}{(' ' + attrs) if attrs else ''}>"

    out = re.sub(r"(?is)<(/?)\s*([a-zA-Z0-9]+)([^>]*)>", repl_tag, raw)
    if "<" not in out and ">" not in out:
        return f'<p style="margin:0;line-height:1.55;">{html.escape(out)}</p>'
    return out.strip()


def render_inbox_message_html(
    *,
    body_html: str = "",
    body_text: str = "",
    route_tag: str | None = None,
    subject: str | None = None,
    use_visit_table: bool = False,
) -> str | None:
    """Inbox UI için güvenli HTML gövde; yoksa None (düz metin kullanılır)."""
    from backend.services.inbox_sync import INBOX_ROUTE_NSTAT, normalize_inbox_route_tag
    from backend.services.inbox_visit_report import is_ziyaret_report_subject

    tag = normalize_inbox_route_tag(route_tag)
    plain = effective_plain_text(body_text, body_html)
    html_part = (body_html or "").strip()

    if use_visit_table or tag == INBOX_ROUTE_NSTAT or is_ziyaret_report_subject(subject or ""):
        visit_html = render_ziyaret_message_html(body_html=html_part, body_text=plain)
        if visit_html:
            return visit_html

    if html_part and ("<" in html_part and ">" in html_part):
        cleaned = sanitize_email_html(html_part)
        if cleaned:
            return cleaned

    if plain and re.search(r"<[a-zA-Z][^>]*>", plain):
        cleaned = sanitize_email_html(plain)
        if cleaned:
            return cleaned

    return None


def plain_text_for_mailer(html_body: str, subject: str = "") -> str:
    """Outbound e-postalar için anlamlı text/plain alternatif."""
    plain = html_to_plain_text(html_body)
    if plain:
        return plain[:50_000]
    subj = (subject or "").strip()
    return f"{subj}\n\n(E-postayı HTML olarak görüntüleyin.)" if subj else "(HTML e-posta)"
