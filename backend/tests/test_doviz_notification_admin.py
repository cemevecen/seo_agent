"""Doviz admin notifications/stats HTML → CSV parse."""

from backend.services.doviz_notification_admin import parse_stats_html_to_csv
from backend.services.notification_analytics_store import parse_csv_text


SAMPLE_HTML = """
<html><body>
<table class="tablesorter">
<thead>
<tr>
  <th>ID</th><th>Text</th>
  <th>Android App Impression</th><th>Android App Click</th><th>Android App CTR</th>
  <th>iOS App Click</th>
  <th>Desktop Impression</th><th>Desktop Click</th><th>Desktop CTR</th>
  <th>Mobileweb Impression</th><th>Mobileweb Click</th><th>Mobileweb CTR</th>
  <th>Date</th>
</tr>
</thead>
<tbody>
<tr>
  <td>2988029</td>
  <td>Piyasaların gözü istihdam verisinde</td>
  <td>85.815</td><td>2.316</td><td>%2.699</td>
  <td>3.186</td>
  <td>7.688</td><td>184</td><td>%2.393</td>
  <td>5.738</td><td>138</td><td>%2.405</td>
  <td>07.08.2026 10:15</td>
</tr>
<tr>
  <td>150389</td>
  <td>Dolar, FED Başkanı&#39;nın Açıklaması!</td>
  <td>2</td><td>0</td><td>%0</td>
  <td>0</td>
  <td>0</td><td>0</td><td>-</td>
  <td>0</td><td>0</td><td>-</td>
  <td>17.11.2016 17:21</td>
</tr>
</tbody>
</table>
</body></html>
"""


def test_parse_stats_html_to_rows():
    csv_text = parse_stats_html_to_csv(SAMPLE_HTML)
    rows = parse_csv_text(csv_text)
    assert len(rows) == 2
    assert rows[0]["id"] == "2988029"
    assert "istihdam" in rows[0]["text"]
    assert rows[0]["platforms"]["android"]["impression"] == 85815.0
    assert rows[0]["platforms"]["android"]["click"] == 2316.0
    assert rows[0]["platforms"]["ios"]["click"] == 3186.0
    assert rows[0]["date"].startswith("2026-08-07")
    assert rows[1]["id"] == "150389"
