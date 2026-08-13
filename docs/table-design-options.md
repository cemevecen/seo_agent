# Android / iOS tablo — 10 interaktif tasarım

Konuşmalarda kaybolmasın diye sabit referans.

- Canvas: `table-design-options` — her seçenekte **mini mockup çizimi** (sadece isim değil)
- Tek çizim PNG: [`docs/assets/table-10-interactive-designs.png`](assets/table-10-interactive-designs.png)
- Cursor kuralı: `.cursor/rules/table-design-options.mdc` (alwaysApply)

Metrik KPI kartları ayrı konu; bu liste **veri tablosu** (`#pa-table-shell` / `#ia-table-shell`) görünümü içindir.

| # | Ad | Özet |
|---|-----|------|
| 1 | **Zebra** | Sticky renkli header + satır şeritleri |
| 2 | **Yoğunluk** | Kompakt / Rahat fiziksel toggle |
| 3 | **Pin** | KIRILIM sabit, diğer kolonlar yatay kayar |
| 4 | **Hover** | Satır lift + sol accent (tıklanabilir his) |
| 5 | **Spark** | Hücre içinde mini sparkline |
| 6 | **Heat** | Değere göre hücre boyası + legend |
| 7 | **Expand** | Güne özel bağımsız açılır satır |
| 8 | **Sürükle** | Kolon header’dan yeniden sıralama |
| 9 | **Karşılaştır** | Dönem / önceki + Δ pill |
| 10 | **Fit** | Kart içeriğe yapışır, altta resize handle |

## Durum

- Hangisi seçildi: **#6 Heat** + **#3 Pin** (varsayılan açık) + **eksen değiştir** (Metrics on top ↔ Dates on top) + **#8 Sürükle**
- Uygulandı: `static/js/metric_table_ux.js` · Android/iOS overview tabloları
- Not: Izgara heat (köşesiz hücre), zebra satır, legend’da Pin / Metrics on top / Remove colors.
