from backend.services.doviz_asset_monitor import html_has_gold_price_rows


def test_html_has_gold_price_rows_positive():
    html = """
    <table>
    <tr><th>Alış</th><th>Satış</th></tr>
    <tr><td><a href="#">Gram</a></td><td>2.450,12</td><td>2.480,00</td></tr>
  </table>
    """
    assert html_has_gold_price_rows(html) is True


def test_html_has_gold_price_rows_empty_table():
    html = """
    <h1>Kuveyt Türk</h1>
    <table><tr><th>Alış</th><th>Satış</th></tr></table>
    """
    assert html_has_gold_price_rows(html) is False


def test_html_has_gold_price_rows_header_only_row():
    html = "<tr><td>Alış</td><td>Satış</td></tr>"
    assert html_has_gold_price_rows(html) is False
