# SEO Agent Data Warehouse Plan

Bu repo artik yalnizca anlik dashboard gosteren bir uygulama degil; buyuk hacimli SEO verisini biriktirebilecek bir temel katmana sahip.

## Katmanlar

### 1. Hot layer
- `metrics`
- dashboard ve site detail ekranlarinda kullanilan hizli ozet degerler
- mobil/desktop PageSpeed skorlar
- Search Console 28 gun ozetleri
- teknik crawler ozetleri

### 2. Warm layer
- `pagespeed_audit_snapshots`
- `pagespeed_payload_snapshots`
- `lighthouse_audit_records`
- `search_console_query_snapshots`
- `collector_runs`

Bu katman buyuk veri, detay analiz ve daha sonra kurulacak raporlama ekranlari icin tutulur.

## Saklanan veri

### PageSpeed / Lighthouse
- ham API payload
- strategy bazli (`mobile`, `desktop`) normalized metricler
- her audit icin tek satir normalized kayıt
- category / section / state / priority / display value
- cozum adimlari ve iki dilli aciklamalar

### Search Console
- property bazli canli fetch kaydi
- `query + device` kiriliminda genis satir verisi
- mevcut 28 gun verisi
- onceki gun verisi
- row pagination ile daha yuksek hacim

### CrUX History
- mobile ve desktop form factor bazli zaman serisi
- LCP, INP, CLS, FCP, TTFB history pointleri
- sonraki asamada trend karsilastirma ve benchmark icin temel veri

### URL Inspection
- index verdict
- coverage / indexing / fetch / robots durumlari
- Google canonical ve user canonical
- last crawl time

## Tasarim ilkesi

- UI tarafindaki mevcut canli ozet akisi korunur.
- Buyuk veri saklama katmani bu UI'yi bozmaz.
- Canli fetch basarisiz olsa bile son basarili detay kayitlari arastirma ve raporlama icin elde tutulur.

## Sonraki genisleme

1. Domain bazli warehouse API endpoint'leri
2. Search Console query explorer
3. Lighthouse audit explorer
4. CrUX History API entegrasyonu
5. URL Inspection API entegrasyonu
6. BigQuery export / bulk export entegrasyonu
