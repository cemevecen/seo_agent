"""inbox_visit_report — çoklu tablo (Desktop + Mobil) render."""

from backend.services.inbox_email_render import render_inbox_message_html
from backend.services.inbox_visit_report import _pick_visit_tables, render_ziyaret_message_html


def _sample_table(url: str, today: str = "1000") -> str:
    return (
        "<table><tr>"
        "<th>URL</th><th>Bugün</th><th>Dün</th><th>günlük fark</th><th>Geçen Hafta</th><th>haftalık fark</th>"
        "</tr><tr>"
        f"<td>{url}</td><td>{today}</td><td>900</td><td>11.1%</td><td>800</td><td>25.0%</td>"
        "</tr></table>"
    )


def test_pick_visit_tables_returns_both_desktop_and_mobile():
    html = (
        "<p>Rapor</p>"
        "<b>Desktop Web</b>"
        + _sample_table("https://www.doviz.com/", "5000")
        + "<br><b>Mobil Web</b>"
        + _sample_table("https://www.doviz.com/", "1200")
    )
    tables = _pick_visit_tables(html, "")
    assert len(tables) == 2
    assert "Desktop" in tables[0][0]
    assert "Mobil" in tables[1][0]
    assert tables[0][1][1][0].startswith("https://")
    assert tables[1][1][1][0].startswith("https://")


def test_render_ziyaret_message_html_includes_both_tables():
    html = (
        "<b>Desktop Web</b>"
        + _sample_table("https://www.doviz.com/", "5000")
        + "<b>Mobil Web</b>"
        + _sample_table("https://m.doviz.com/", "800")
    )
    out = render_ziyaret_message_html(body_html=html, body_text="")
    assert out.count("inbox-ziyaret-table") == 2
    assert "Desktop Web" in out
    assert "Mobil Web" in out
    assert "m.doviz.com" in out


def test_render_inbox_message_nstat_dual_table():
    html = (
        "<b>Desktop Web</b>"
        + _sample_table("https://www.doviz.com/")
        + "<b>Mobil Web</b>"
        + _sample_table("https://www.doviz.com/mobil")
    )
    rendered = render_inbox_message_html(
        body_html=html,
        body_text="",
        route_tag="nstat",
        subject="En çok ziyaret edilen sayfalar - 23.05.2026 20:00",
    )
    assert rendered
    assert rendered.count("<table") == 2
