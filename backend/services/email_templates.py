"""Reusable HTML email templates with table-based layouts."""

from __future__ import annotations

from html import escape


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
        'color:#64748b;">Özet bilgi</td></tr>'
        f"{body}"
        "</table>"
    )


def ga4_digest_critical_row(body_text: str) -> str:
    """Tek kritik satırı: sol etiket sütunu + sağ içerik (pembe kart)."""
    safe = escape(body_text).replace("\n", "<br>")
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="margin:0 0 10px 0;border-collapse:collapse;">'
        "<tr>"
        '<td style="padding:0;">'
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="border-collapse:collapse;border:1px solid #fecdd3;border-radius:14px;background:#fffafb;">'
        "<tr>"
        '<td width="84" valign="top" style="padding:14px 10px 14px 16px;font-size:11px;font-weight:800;'
        'letter-spacing:0.08em;color:#be123c;">ÖNEMLİ</td>'
        f'<td valign="top" style="padding:14px 16px 14px 0;font-size:14px;line-height:1.55;color:#334155;">{safe}</td>'
        "</tr></table>"
        "</td>"
        "</tr></table>"
    )


def ga4_digest_area_table(area_title: str, items: list[str]) -> str:
    """Alan başlığı + numaralı satırlar."""
    if not items:
        return ""
    rows_html = ""
    for idx, raw in enumerate(items, start=1):
        cell = raw if ("<" in raw and ">" in raw) else escape(raw)
        rows_html += (
            "<tr>"
            f'<td width="40" valign="top" style="padding:12px 6px 12px 14px;border-bottom:1px solid #f1f5f9;'
            f'font-size:12px;font-weight:800;color:#94a3b8;">{idx}</td>'
            f'<td valign="top" style="padding:12px 14px 12px 0;border-bottom:1px solid #f1f5f9;'
            f'font-size:14px;line-height:1.55;color:#334155;">{cell}</td>'
            "</tr>"
        )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'style="margin:0 0 20px 0;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;background:#ffffff;">'
        '<tr><td colspan="2" style="padding:13px 16px;background:linear-gradient(180deg,#f1f5f9 0%,#ffffff 55%);'
        'border-bottom:1px solid #e2e8f0;font-size:12px;font-weight:800;letter-spacing:0.04em;color:#0f172a;">'
        f"{escape(area_title)}"
        "</td></tr>"
        f"{rows_html}"
        "</table>"
    )


def render_ga4_digest_email(
    *,
    eyebrow: str,
    title: str,
    tone: str,
    status_label: str,
    meta_rows: list[tuple[str, str]],
    critical_lines: list[str],
    area_blocks: list[tuple[str, list[str]]],
) -> str:
    """
    GA4 haftalık özet için tablo mimarili HTML (giriş paragrafı yok).
    """
    colors = _palette(tone)
    meta_html = ga4_digest_meta_table(meta_rows)
    crit_block = ""
    if critical_lines:
        crit_rows = "".join(ga4_digest_critical_row(line) for line in critical_lines[:24])
        crit_block = (
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">'
            '<tr><td style="padding:22px 0 12px 0;">'
            '<p style="margin:0;font-size:11px;font-weight:800;letter-spacing:0.12em;color:#94a3b8;text-transform:uppercase;">'
            "Kritik vurgular</p>"
            "</td></tr></table>"
            f"{crit_rows}"
        )
    areas_html = "".join(
        ga4_digest_area_table(area_title, items) for area_title, items in area_blocks if items
    )
    body_rows: list[str] = [f'<tr><td style="padding:0 28px 12px 28px;">{meta_html}</td></tr>']
    if crit_block:
        body_rows.append(f'<tr><td style="padding:0 28px 4px 28px;">{crit_block}</td></tr>')
    body_rows.append(f'<tr><td style="padding:0 28px 28px 28px;">{areas_html}</td></tr>')
    body_inner = "".join(body_rows)
    return f"""
<!doctype html>
<html>
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
<html>
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
