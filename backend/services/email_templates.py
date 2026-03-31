"""Reusable HTML email templates with table-based layouts."""

from __future__ import annotations

import re
from html import escape

from backend.locale import tr as tr_locale
from backend.services.ga4_page_urls import ga4_email_page_url, ga4_site_host


# GA4 özet e-posta: yalnızca Değişim sütunu — işaretli yüzde eşikleri
GA4_DIGEST_DELTA_DARK_ABS = 120.0
GA4_DIGEST_DELTA_RED_ABS = 70.0
# Site sütunu yok; 5 sütunlu tablolarda Değişim bu indekste
GA4_DIGEST_DELTA_COL = 2

# Tablo vurgusu: satır zeminini boyama; yalnızca metin rengi + kalınlık (koyu/açık yeşil ve kırmızı)
_STY = {
    "g_d": "color:#14532d;font-weight:800;",
    "g_m": "color:#15803d;font-weight:700;",
    "g_l": "color:#22c55e;font-weight:700;",
    "r_d": "color:#7f1d1d;font-weight:800;",
    "r_m": "color:#b91c1c;font-weight:700;",
    "r_l": "color:#dc2626;font-weight:600;",
}


def ga4_digest_style_for_delta_pct(signed_pct: float | None) -> str:
    """Değişim sütunu: artış → yeşil tonları; düşüş → kırmızı tonları. Sıfır dışı her değişim en az açık ton alır."""
    if signed_pct is None:
        return ""
    try:
        signed = float(signed_pct)
        ap = abs(signed)
    except (TypeError, ValueError):
        return ""
    if signed == 0:
        return ""
    if signed > 0:
        if ap >= GA4_DIGEST_DELTA_DARK_ABS:
            return _STY["g_d"]
        if ap >= GA4_DIGEST_DELTA_RED_ABS:
            return _STY["g_m"]
        return _STY["g_l"]
    if signed < 0:
        if ap >= GA4_DIGEST_DELTA_DARK_ABS:
            return _STY["r_d"]
        if ap >= GA4_DIGEST_DELTA_RED_ABS:
            return _STY["r_m"]
        return _STY["r_l"]
    return ""


PALETTES = {
    "blue": {
        "accent": "#2563eb",
        "accent_soft": "#dbeafe",
        "accent_text": "#1d4ed8",
        "border": "#bfdbfe",
        "surface": "#f8fbff",
    },
    "emerald": {
        "accent": "#059669",
        "accent_soft": "#d1fae5",
        "accent_text": "#047857",
        "border": "#a7f3d0",
        "surface": "#f7fffb",
    },
    "amber": {
        "accent": "#d97706",
        "accent_soft": "#fef3c7",
        "accent_text": "#b45309",
        "border": "#fde68a",
        "surface": "#fffdf7",
    },
    "rose": {
        "accent": "#e11d48",
        "accent_soft": "#ffe4e6",
        "accent_text": "#be123c",
        "border": "#fecdd3",
        "surface": "#fff8f8",
    },
    "slate": {
        "accent": "#334155",
        "accent_soft": "#e2e8f0",
        "accent_text": "#334155",
        "border": "#cbd5e1",
        "surface": "#f8fafc",
    },
}


def _palette(name: str) -> dict[str, str]:
    return PALETTES.get(name, PALETTES["blue"])


def _cell(text: str, *, align: str = "left", muted: bool = False, weight: str = "500") -> str:
    color = "#64748b" if muted else "#0f172a"
    rendered = text if ("<" in text and ">" in text) else escape(text)
    return (
        f'<td style="padding:12px 14px;border-bottom:1px solid #e2e8f0;'
        f'text-align:{align};font-size:14px;line-height:1.5;color:{color};font-weight:{weight};">'
        f"{rendered}</td>"
    )


def status_chip(text: str, *, tone: str = "blue") -> str:
    colors = _palette(tone)
    return (
        f'<span style="display:inline-block;padding:7px 12px;border-radius:999px;'
        f'background:{colors["accent_soft"]};color:{colors["accent_text"]};'
        f'font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;">'
        f"{escape(text)}</span>"
    )


def summary_table(rows: list[tuple[str, str]]) -> str:
    body = "".join(
        "<tr>"
        + _cell(label, muted=True, weight="700")
        + _cell(value)
        + "</tr>"
        for label, value in rows
    )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;background:#ffffff;">'
        f"{body}</table>"
    )


def stat_cards(items: list[dict[str, str]]) -> str:
    cards = []
    for item in items:
        tone = item.get("tone", "blue")
        colors = _palette(tone)
        label = escape(item.get("label", ""))
        value = escape(item.get("value", ""))
        caption = escape(item.get("caption", ""))
        cards.append(
            '<td valign="top" style="padding:0 10px 10px 0;">'
            f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
            f'style="border-collapse:collapse;border:1px solid {colors["border"]};border-radius:18px;overflow:hidden;background:{colors["surface"]};min-width:160px;">'
            f'<tr><td style="padding:16px 16px 14px 16px;">'
            f'<p style="margin:0;font-size:12px;line-height:1.4;color:{colors["accent_text"]};font-weight:800;letter-spacing:0.05em;text-transform:uppercase;">{label}</p>'
            f'<p style="margin:10px 0 0 0;font-size:28px;line-height:1.1;color:#0f172a;font-weight:800;">{value}</p>'
            f'<p style="margin:8px 0 0 0;font-size:13px;line-height:1.55;color:#64748b;">{caption}</p>'
            f'</td></tr></table></td>'
        )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:0;">'
        "<tr>"
        + "".join(cards)
        + "</tr></table>"
    )


def data_table(headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(
        f'<th style="padding:12px 14px;background:#eff6ff;border-bottom:1px solid #dbeafe;'
        f'text-align:left;font-size:12px;line-height:1.4;color:#1e3a8a;font-weight:800;'
        f'letter-spacing:0.05em;text-transform:uppercase;">{escape(header)}</th>'
        for header in headers
    )
    row_html = ""
    for row in rows:
        row_html += "<tr>" + "".join(_cell(cell) for cell in row) + "</tr>"
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #dbeafe;border-radius:18px;overflow:hidden;background:#ffffff;">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{row_html}</tbody></table>"
    )


def html_unordered_list(items: list[str]) -> str:
    """E-posta içi madde işaretli liste (sıralı önem notları için)."""
    if not items:
        return '<p style="margin:0;font-size:14px;line-height:1.6;color:#64748b;">Bu bölümde madde yok.</p>'
    lis = []
    for item in items:
        rendered = item if ("<" in item and ">" in item) else escape(item)
        lis.append(
            f'<li style="margin:0 0 10px 0;font-size:14px;line-height:1.6;color:#334155;">{rendered}</li>'
        )
    return (
        '<ul style="margin:8px 0 0 0;padding-left:20px;">'
        + "".join(lis)
        + "</ul>"
    )


def note_box(title: str, body: str, *, tone: str = "slate") -> str:
    colors = _palette(tone)
    body_html = escape(body).replace("\n", "<br>")
    return (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        f'style="border-collapse:collapse;border:1px solid {colors["border"]};border-radius:18px;overflow:hidden;background:{colors["surface"]};">'
        f'<tr><td style="padding:16px 18px;">'
        f'<p style="margin:0 0 8px 0;font-size:13px;line-height:1.4;color:{colors["accent_text"]};font-weight:800;letter-spacing:0.05em;text-transform:uppercase;">{escape(title)}</p>'
        f'<p style="margin:0;font-size:14px;line-height:1.65;color:#334155;">{body_html}</p>'
        f"</td></tr></table>"
    )


def section(title: str, content: str, *, subtitle: str = "") -> str:
    subtitle_html = (
        f'<p style="margin:6px 0 0 0;font-size:14px;line-height:1.6;color:#64748b;">{escape(subtitle)}</p>'
        if subtitle
        else ""
    )
    return (
        '<tr><td style="padding:0 32px 24px 32px;">'
        f'<p style="margin:0;font-size:13px;line-height:1.4;color:#475569;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;">{escape(title)}</p>'
        f"{subtitle_html}"
        f'<div style="margin-top:14px;">{content}</div>'
        "</td></tr>"
    )


def ga4_digest_meta_table(rows: list[tuple[str, str]]) -> str:
    """İki sütunlu özet tablosu (etiket | değer)."""
    body = ""
    for label, value in rows:
        body += (
            "<tr>"
            f'<td style="padding:11px 16px;width:34%;vertical-align:top;border-bottom:1px solid #f1f5f9;'
            f'font-size:13px;font-weight:700;color:#64748b;">{escape(label)}</td>'
            f'<td style="padding:11px 16px;vertical-align:top;border-bottom:1px solid #f1f5f9;'
            f'font-size:14px;line-height:1.5;color:#0f172a;">{escape(value)}</td>'
            "</tr>"
        )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;background:#ffffff;">'
        '<tr><td colspan="2" style="padding:12px 16px;background:linear-gradient(90deg,#f8fafc 0%,#ffffff 100%);'
        'border-bottom:1px solid #e2e8f0;font-size:11px;font-weight:800;letter-spacing:0.1em;text-transform:uppercase;'
        f'color:#64748b;">{escape(tr_locale.GA4_DIGEST_META_TABLE_CAPTION)}</td></tr>'
        f"{body}"
        "</table>"
    )


def ga4_digest_styled_table(
    headers: list[str],
    rows: list[list[str]],
    *,
    caption: str = "",
    delta_col_index: int | None = None,
    delta_signed_pct_per_row: list[float | None] | None = None,
) -> str:
    """Excel benzeri grid; yalnızca Değişim sütununda metin rengi (eşik vurgusu)."""
    if not rows:
        return ""
    cap = (
        f'<p style="margin:0 0 8px 0;font-size:12px;font-weight:700;color:#64748b;">{escape(caption)}</p>'
        if caption
        else ""
    )
    th = "".join(
        f'<th style="padding:10px 8px;text-align:left;font-size:11px;font-weight:800;letter-spacing:0.06em;'
        f'text-transform:uppercase;color:#475569;border-bottom:2px solid #cbd5e1;background:#f1f5f9;">'
        f"{escape(h)}</th>"
        for h in headers
    )
    body = ""
    for i, row in enumerate(rows):
        zebra = "#fafafa" if i % 2 == 0 else "#ffffff"
        tds = ""
        for j, c in enumerate(row):
            cell = c if ("<" in c and ">" in c) else escape(c)
            extra = ""
            if (
                delta_col_index is not None
                and j == delta_col_index
                and delta_signed_pct_per_row is not None
                and i < len(delta_signed_pct_per_row)
            ):
                extra = ga4_digest_style_for_delta_pct(delta_signed_pct_per_row[i])
            base = (
                "padding:10px 8px;font-size:13px;line-height:1.45;"
                "border-bottom:1px solid #e2e8f0;vertical-align:top;"
            )
            if extra:
                tds += f'<td style="{base}background:{zebra};{extra}">{cell}</td>'
            else:
                tds += (
                    f'<td style="{base}color:#334155;background:{zebra};">{cell}</td>'
                )
        body += f"<tr>{tds}</tr>"
    return (
        f'{cap}'
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #cbd5e1;border-radius:10px;overflow:hidden;background:#ffffff;">'
        f"<thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"
    )


def _parse_digest_delta_pct_display(s: str) -> float | None:
    """E-posta hücresi biçimi: '%-26,7', '%1.234,5' (TR) → float."""
    t = (s or "").strip()
    if not t or t == "—":
        return None
    if t.startswith("%"):
        t = t[1:].strip()
    if not t:
        return None
    if t.count(",") == 1 and "." in t:
        left, right = t.rsplit(",", 1)
        left = left.replace(".", "")
        t = f"{left}.{right}"
    else:
        t = t.replace(".", "").replace(",", ".") if "," in t else t
    try:
        return float(t)
    except ValueError:
        return None


def _ga4_digest_signed_pct_one(d: dict) -> float | None:
    """Renklendirme ile aynı işaretli %; kanal yok."""
    kind = str(d.get("kind") or "")
    if kind == "channel":
        return None
    raw = d.get("pct_value")
    if raw is not None and str(raw).strip() != "":
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    return _parse_digest_delta_pct_display(str(d.get("delta_pct") or ""))


def _ga4_digest_delta_signed_from_dicts(dicts: list[dict]) -> list[float | None]:
    """Değişim sütunu için satır başına işaretli % (kanal → None)."""
    return [_ga4_digest_signed_pct_one(d) for d in dicts]


def _digest_site_root_url(domain: str | None) -> str | None:
    h = ga4_site_host(domain)
    return f"https://{h}/" if h else None


def _digest_source_medium_url(source_medium: str) -> str | None:
    s = (source_medium or "").strip()
    if not s:
        return None
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        first = s.split()[0]
        return first if first.startswith("http") else None
    if " / " in s:
        left = s.split(" / ")[0].strip()
        if re.fullmatch(r"[\w.-]+\.[a-zA-Z]{2,}", left):
            return f"https://{left}"
    return None


def _digest_anchor(text: str, href: str | None) -> str:
    if not href:
        return escape(text)
    return (
        f'<a href="{escape(href, quote=True)}" target="_blank" rel="noopener noreferrer" '
        f'style="color:#1d4ed8;text-decoration:underline;">{escape(text)}</a>'
    )


def _ga4_digest_profile_cell(d: dict) -> str:
    return _digest_anchor(str(d.get("profile") or ""), _digest_site_root_url(str(d.get("domain") or "")))


def _ga4_digest_detail_cell(d: dict) -> str:
    kind = str(d.get("kind") or "")
    dom = str(d.get("domain") or "")
    if kind == "kpi":
        return _digest_anchor(str(d.get("metric_label") or ""), _digest_site_root_url(dom))
    if kind == "page":
        path = str(d.get("path") or "")
        disp = str(d.get("page_label") or path)[:220]
        href = ga4_email_page_url(
            site_domain=dom,
            path=path,
            page_host=str(d.get("page_host") or ""),
            stored_page_url=str(d.get("page_url") or ""),
        )
        return _digest_anchor(disp, href or None)
    if kind == "source":
        sm = str(d.get("source_medium") or "")
        disp = sm[:220]
        return _digest_anchor(disp, _digest_source_medium_url(sm))
    if kind == "channel":
        return _digest_anchor(str(d.get("channel") or ""), _digest_site_root_url(dom))
    return escape(str(d.get("metric_label") or ""))


def ga4_digest_critical_table(rows: list[dict]) -> str:
    """Kritik satırlar — tek tablo. Sütun: Profil, Hedef, Değişim, Son 7g, Önceki 7g."""
    if not rows:
        return ""
    data_rows: list[list[str]] = []
    for d in rows:
        kind = str(d.get("kind") or "")
        if kind == "kpi":
            data_rows.append(
                [
                    _ga4_digest_profile_cell(d),
                    _ga4_digest_detail_cell(d),
                    str(d.get("delta_pct") or ""),
                    str(d.get("last") or ""),
                    str(d.get("prev") or ""),
                ]
            )
        elif kind == "page":
            data_rows.append(
                [
                    _ga4_digest_profile_cell(d),
                    _ga4_digest_detail_cell(d),
                    str(d.get("delta_pct") or ""),
                    str(d.get("last") or ""),
                    str(d.get("prev") or ""),
                ]
            )
        elif kind == "source":
            data_rows.append(
                [
                    _ga4_digest_profile_cell(d),
                    _ga4_digest_detail_cell(d),
                    str(d.get("delta_pct") or ""),
                    str(d.get("last") or ""),
                    str(d.get("prev") or ""),
                ]
            )
        elif kind == "channel":
            data_rows.append(
                [
                    _ga4_digest_profile_cell(d),
                    _ga4_digest_detail_cell(d),
                    "—",
                    str(d.get("sessions") or ""),
                    "—",
                ]
            )
    delta_signed = _ga4_digest_delta_signed_from_dicts(rows)
    inner = ga4_digest_styled_table(
        ["Profil", "Hedef / ayrıntı", "Değişim", "Son 7 gün", "Önceki 7 gün"],
        data_rows,
        delta_col_index=GA4_DIGEST_DELTA_COL,
        delta_signed_pct_per_row=delta_signed,
    )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #e2e8f0;border-radius:14px;background:#ffffff;">'
        '<tr><td style="padding:14px 12px 10px 14px;">'
        '<p style="margin:0 0 10px 0;font-size:11px;font-weight:800;letter-spacing:0.12em;color:#475569;">'
        f"{escape(tr_locale.GA4_DIGEST_CRITICAL_SECTION_LABEL)}</p>"
        f"{inner}"
        "</td></tr></table>"
    )


def ga4_digest_area_block(area_title: str, items: list[dict]) -> str:
    """Alan: KPI / sayfa / kaynak / kanal alt tabloları."""
    if not items:
        return ""
    kpis = [x for x in items if x.get("kind") == "kpi"]
    pages = [x for x in items if x.get("kind") == "page"]
    sources = [x for x in items if x.get("kind") == "source"]
    channels = [x for x in items if x.get("kind") == "channel"]
    parts: list[str] = []

    if kpis:
        kr = [
            [
                _ga4_digest_profile_cell(x),
                _ga4_digest_detail_cell(x),
                str(x.get("delta_pct") or ""),
                str(x.get("last") or ""),
                str(x.get("prev") or ""),
            ]
            for x in kpis
        ]
        da = _ga4_digest_delta_signed_from_dicts(kpis)
        parts.append(
            ga4_digest_styled_table(
                ["Profil", "Metrik", "Değişim", "Son 7 gün", "Önceki 7 gün"],
                kr,
                caption="Temel metrikler (haftalık karşılaştırma)",
                delta_col_index=GA4_DIGEST_DELTA_COL,
                delta_signed_pct_per_row=da,
            )
        )
    if pages:
        pr = [
            [
                _ga4_digest_profile_cell(x),
                _ga4_digest_detail_cell(x),
                str(x.get("delta_pct") or ""),
                str(x.get("last") or ""),
                str(x.get("prev") or ""),
            ]
            for x in pages
        ]
        da = _ga4_digest_delta_signed_from_dicts(pages)
        parts.append(
            ga4_digest_styled_table(
                ["Profil", "Landing yolu", "Değişim", "Son 7 gün", "Önceki 7 gün"],
                pr,
                caption="Landing sayfalar (haber kategorileri hariç)",
                delta_col_index=GA4_DIGEST_DELTA_COL,
                delta_signed_pct_per_row=da,
            )
        )
    if sources:
        sr = [
            [
                _ga4_digest_profile_cell(x),
                _ga4_digest_detail_cell(x),
                str(x.get("delta_pct") or ""),
                str(x.get("last") or ""),
                str(x.get("prev") or ""),
            ]
            for x in sources
        ]
        da = _ga4_digest_delta_signed_from_dicts(sources)
        parts.append(
            ga4_digest_styled_table(
                ["Profil", "Kaynak / ortam", "Değişim", "Son 7 gün", "Önceki 7 gün"],
                sr,
                caption="Trafik kaynakları",
                delta_col_index=GA4_DIGEST_DELTA_COL,
                delta_signed_pct_per_row=da,
            )
        )
    if channels:
        cr = [
            [
                _ga4_digest_profile_cell(x),
                _ga4_digest_detail_cell(x),
                str(x.get("sessions") or ""),
            ]
            for x in channels
        ]
        parts.append(
            ga4_digest_styled_table(
                ["Profil", "Kanal", "Oturum (son 7 gün)"],
                cr,
                caption="Öne çıkan kanallar",
            )
        )

    inner = "".join(f'<div style="margin:0 0 16px 0;">{p}</div>' for p in parts)
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="margin:0 0 22px 0;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;background:#ffffff;">'
        '<tr><td style="padding:14px 14px 10px 14px;background:linear-gradient(180deg,#f8fafc 0%,#ffffff 60%);'
        'border-bottom:1px solid #e2e8f0;font-size:12px;font-weight:800;letter-spacing:0.04em;color:#0f172a;">'
        f"{escape(area_title)}"
        "</td></tr>"
        f'<tr><td style="padding:14px;">{inner}</td></tr></table>'
    )


def render_ga4_digest_email(
    *,
    eyebrow: str,
    title: str,
    tone: str,
    status_label: str,
    meta_rows: list[tuple[str, str]],
    critical_rows: list[dict],
    area_blocks: list[tuple[str, list[dict]]],
) -> str:
    """GA4 haftalık özet için tablo mimarili HTML."""
    colors = _palette(tone)
    meta_html = ga4_digest_meta_table(meta_rows)
    crit_block = ""
    if critical_rows:
        crit_block = (
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">'
            '<tr><td style="padding:18px 0 10px 0;">'
            '<p style="margin:0;font-size:11px;font-weight:800;letter-spacing:0.12em;color:#94a3b8;text-transform:uppercase;">'
            f"{escape(tr_locale.GA4_DIGEST_CRITICAL_HIGHLIGHTS_TITLE)}</p>"
            "</td></tr>"
            f'<tr><td style="padding:0 0 8px 0;">{ga4_digest_critical_table(critical_rows)}</td></tr>'
            "</table>"
        )
    areas_html = "".join(
        ga4_digest_area_block(area_title, items) for area_title, items in area_blocks if items
    )
    body_rows: list[str] = [f'<tr><td style="padding:0 28px 12px 28px;">{meta_html}</td></tr>']
    if crit_block:
        body_rows.append(f'<tr><td style="padding:0 28px 4px 28px;">{crit_block}</td></tr>')
    body_rows.append(f'<tr><td style="padding:0 28px 28px 28px;">{areas_html}</td></tr>')
    body_inner = "".join(body_rows)
    return f"""
<!doctype html>
<html lang="{escape(tr_locale.HTML_LANG)}">
  <body style="margin:0;padding:0;background:#e8eef7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#e8eef7;">
      <tr>
        <td align="center" style="padding:24px 12px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:720px;border-collapse:collapse;">
            <tr>
              <td style="padding:0 4px 14px 4px;font-size:11px;line-height:1.4;color:#64748b;font-weight:800;letter-spacing:0.14em;text-transform:uppercase;">
                {escape(eyebrow)}
              </td>
            </tr>
            <tr>
              <td style="background:#ffffff;border:1px solid #dbe4f0;border-radius:24px;overflow:hidden;box-shadow:0 20px 50px rgba(15,23,42,0.07);">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                  <tr>
                    <td style="padding:26px 28px 22px 28px;background:linear-gradient(145deg, {colors["surface"]} 0%, #ffffff 42%, {colors["accent_soft"]} 100%);border-bottom:1px solid #e2e8f0;">
                      <p style="margin:0 0 10px 0;">{status_chip(status_label, tone=tone)}</p>
                      <h1 style="margin:0;font-size:28px;line-height:1.2;color:#0f172a;font-weight:800;letter-spacing:-0.02em;">{escape(title)}</h1>
                    </td>
                  </tr>
                  {body_inner}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()


def render_email_shell(
    *,
    eyebrow: str,
    title: str,
    intro: str,
    tone: str,
    status_label: str,
    sections: list[str],
) -> str:
    colors = _palette(tone)
    section_html = "".join(sections)
    return f"""
<!doctype html>
<html lang="{escape(tr_locale.HTML_LANG)}">
  <body style="margin:0;padding:0;background:#eef3fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#eef3fb;">
      <tr>
        <td align="center" style="padding:28px 16px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:760px;border-collapse:collapse;">
            <tr>
              <td style="padding:0 0 16px 4px;font-size:12px;line-height:1.4;color:#64748b;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;">
                {escape(eyebrow)}
              </td>
            </tr>
            <tr>
              <td style="background:#ffffff;border:1px solid #dbe4f0;border-radius:28px;overflow:hidden;box-shadow:0 18px 40px rgba(15,23,42,0.08);">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                  <tr>
                    <td style="padding:30px 32px 26px 32px;background:linear-gradient(135deg, {colors["surface"]} 0%, #ffffff 48%, {colors["accent_soft"]} 100%);border-bottom:1px solid #e2e8f0;">
                      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
                        <tr>
                          <td valign="top">
                            <p style="margin:0 0 12px 0;">{status_chip(status_label, tone=tone)}</p>
                            <h1 style="margin:0;font-size:34px;line-height:1.15;color:#0f172a;font-weight:800;">{escape(title)}</h1>
                            {"" if not intro else f'<p style="margin:14px 0 0 0;font-size:15px;line-height:1.7;color:#475569;max-width:560px;">{escape(intro)}</p>'}
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                  {section_html}
                  <tr>
                    <td style="padding:0 32px 30px 32px;"></td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
""".strip()
