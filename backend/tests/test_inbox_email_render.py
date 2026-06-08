"""inbox_email_render — placeholder düz metin, sanitize, charset yardımcıları."""

from backend.services.inbox_email_render import (
    effective_plain_text,
    html_to_plain_text,
    is_placeholder_plain_text,
    normalize_inbox_text,
    plain_text_for_mailer,
    render_inbox_message_html,
    repair_utf8_mojibake,
    sanitize_email_html,
)
from backend.services.inbox_sync import _decode_bytes


def test_placeholder_plain_detected():
    assert is_placeholder_plain_text("This is a plain-text fallback for the HTML email.")
    assert not is_placeholder_plain_text("Merhaba dünya")


def test_effective_plain_prefers_html_over_placeholder():
    html = "<p>Gelen Kutusu <b>Özeti</b> — 53 okunmamış</p>"
    plain = effective_plain_text("This is a plain-text fallback for the HTML email.", html)
    assert "Gelen Kutusu" in plain
    assert "Özeti" in plain
    assert "fallback" not in plain.lower()


def test_sanitize_strips_script_and_fixes_links():
    raw = (
        '<p>Merhaba</p><script>alert(1)</script>'
        '<a href="javascript:evil()">x</a>'
        '<a href="https://example.com">ok</a>'
    )
    out = sanitize_email_html(raw)
    assert "script" not in out.lower()
    assert "javascript:" not in out
    assert 'href="https://example.com"' in out
    assert 'target="_blank"' in out


def test_render_inbox_summary_html():
    html = (
        "<h1 style='color:#7c3aed;'>Gelen Kutusu Özeti</h1>"
        "<table><tr><td>all</td><td>5</td></tr></table>"
    )
    rendered = render_inbox_message_html(
        body_html=html,
        body_text="This is a plain-text fallback for the HTML email.",
        route_tag="all",
        subject="Inbox özeti — 53 okunmamış",
    )
    assert rendered
    assert "Gelen Kutusu" in rendered
    assert "<table>" in rendered.lower() or "<tr>" in rendered.lower()


def test_plain_text_for_mailer_from_html():
    text = plain_text_for_mailer("<h1>Başlık</h1><p>İçerik özet</p>", subject="Konu")
    assert "Başlık" in text
    assert "İçerik" in text


def test_decode_bytes_turkish_cp1254():
    raw = "Gelen kutusu özeti".encode("cp1254")
    assert "özeti" in _decode_bytes(raw, "cp1254")


def test_repair_utf8_mojibake_turkish():
    broken = "KÃ¼ltÃ¼r AvcÄ±larÄ±\nBu projeyi yeniliklerle yeniden baÅŸlatÄ±yoruz."
    fixed = repair_utf8_mojibake(broken)
    assert "Kültür Avcıları" in fixed
    assert "başlatıyoruz" in fixed
    assert "Ã" not in fixed


def test_sanitize_email_html_repairs_mojibake():
    raw = "<h1>KÃ¼ltÃ¼r AvcÄ±larÄ±</h1><p>baÅŸlatÄ±yoruz</p>"
    out = sanitize_email_html(raw)
    assert "Kültür" in out
    assert "başlat" in out


def test_effective_plain_repairs_mojibake():
    assert "Kültür" in effective_plain_text("KÃ¼ltÃ¼r AvcÄ±larÄ±", "")


def test_decode_bytes_utf8_wrong_charset_header():
    raw = "Kültür Avcıları".encode("utf-8")
    text = _decode_bytes(raw, "iso-8859-1")
    assert "Kültür" in text
    assert "Ã" not in text


def test_normalize_inbox_text_idempotent():
    good = "Kültür Avcıları"
    assert normalize_inbox_text(good) == good
