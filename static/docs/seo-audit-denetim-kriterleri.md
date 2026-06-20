# SEO Audit — Denetim Kriterleri ve Skorlama

Bu doküman, SEO Agent `/seo-audit` sayfasında (ör. `filter=poor`) kullanılan denetim mantığının kaynak koduyla birebir özetidir.

**Kaynak dosyalar:** `backend/collectors/site_audit.py`, `backend/services/meta_audit.py`, `backend/services/seo_audit_runner.py`

**Son güncelleme:** 2026-06-20

---

## 1. Hangi sayfalar taranır?

| Kaynak | Açıklama |
|--------|----------|
| GA4 web | Son 7 gün, oturum (sessions) bazında **top 250** sayfa (`hostName` + `pagePath`) |
| GA4 mweb | Aynı mantık, **top 250** mobil web sayfa |
| Fallback (doviz.com) | GA4 listesinde yoksa sabit akaryakıt URL’leri eklenir |
| Manuel «Tara» | Yukarıdaki URL listesi anlık crawl edilir |
| Otomatik job | Her gece **03:00 TSİ** (ayar: `SEO_AUDIT_SCHEDULED_*`) aynı mantık |

**Hariç tutulan URL’ler:**

- GA4 çöp boyutları: `(other)`, `(not set)`, `(blank)`, `(not provided)`, `(data not available)`
- `www.doviz.com/yorum` kök listeleme (alt sayfalar `/yorum/123` kalır)
- Geçersiz `m.doviz.com` düz slug URL’leri (path onarım kurallarına uymayanlar)
- `is_seo_audit_excluded_url()` / `is_seo_audit_crawl_url()` ile elenen adresler

---

## 2. Sayfa başına kontrol edilen alanlar

Her URL için HTML indirilir (`GET`, redirect takibi, timeout ~8 sn) ve şu alanlar parse edilir:

| Alan | Nasıl okunur |
|------|----------------|
| HTTP durum | 200 beklenir |
| `<title>` | Varlık + karakter uzunluğu |
| `<meta name="description">` | Varlık + karakter uzunluğu |
| `<h1>` | Varlık + adet (tek H1 beklenir) |
| `<h2>` | Adet (en az 1 beklenir — listeleme filtresi) |
| `<link rel="canonical">` | Varlık + final URL ile eşleşme |
| `<meta name="robots">` | `noindex` tespiti |
| Open Graph | `og:title` ve `og:description` |
| Schema | `application/ld+json` script varlığı |

---

## 3. Uzunluk eşikleri

| Alan | Minimum | Maksimum | Kod sabiti |
|------|---------|----------|------------|
| Title | 20 kr | 65 kr | `TITLE_MIN`, `TITLE_MAX` |
| Meta description | 70 kr | 170 kr | `DESC_MIN`, `DESC_MAX` |

---

## 4. Skor sınıfları (good / needs_improvement / poor)

### Kritik — `poor`

Aşağıdakilerden **biri** bile varsa sayfa **Kritik** sayılır (`filter=poor`):

- HTTP ≠ 200 veya HTML alınamadı
- Title yok
- Meta description yok
- H1 yok

### İyileştir — `needs_improvement`

Kritik değilse, aşağıdakilerden **biri** varsa **İyileştir**:

- Title kısa (<20) veya uzun (>65)
- Description kısa (<70) veya uzun (>170)
- Canonical yok
- Canonical, final URL ile eşleşmiyor
- Schema (JSON-LD) yok
- OG title veya OG description eksik
- Birden fazla H1
- Noindex veya indekslenemez durum

### İyi — `good`

Yukarıdaki hiçbiri yoksa **İyi**.

---

## 5. Sorun etiketleri (UI / filtreler)

| Kod | UI etiketi | Filtre |
|-----|------------|--------|
| `title_missing` | Başlık yok | `missing_title` |
| `title_short` | Başlık kısa | `short_title` |
| `title_long` | Başlık uzun | `long_title` |
| `desc_missing` | Desc yok | `missing_desc` |
| `desc_short` | Desc kısa | `short_desc` |
| `desc_long` | Desc uzun | `long_desc` |
| `canonical_missing` | Canonical yok | `missing_canonical` |
| `canonical_mismatch` | Canonical mismatch | `broken_canonical` |
| `noindex` | Noindex | `noindex` |
| `og_missing` | OG eksik | `missing_og` |
| `h1_missing` | H1 yok | `missing_h1` |
| `h1_multiple` | Çoklu H1 | `multiple_h1` |
| `schema_missing` | Schema yok | `missing_schema` |
| `h2_missing` | H2 yok | `missing_h2` |

Skor filtreleri: `poor` (Kritik), `needs_improvement` (İyileştir), `all` (Tümü).

---

## 6. Duplicate tespiti

Aynı **subdomain** içinde:

- Aynı `<title>` metnini paylaşan birden fazla URL → duplicate title grubu
- Aynı meta description metnini paylaşan birden fazla URL → duplicate description grubu

Farklı subdomain’ler (ör. `m.doviz.com` vs `www.doviz.com`) aynı title taşısa bile ayrı gruplar sayılır.

---

## 7. Değişiklikler sekmesi

Son 7 günde tarama snapshot’ları karşılaştırılır; title, description, canonical, H1, schema, noindex alanlarında regresyon / iyileşme listelenir.

---

## 8. Veri modeli

Sonuçlar `url_audit_records` tablosunda saklanır. Her taramada aynı site için **eski kayıtlar silinir**, yalnızca son crawl kalır.

---

## 9. İndirme / API

| İşlem | Adres |
|-------|--------|
| Bu doküman (Markdown) | `/static/docs/seo-audit-denetim-kriterleri.md` |
| Tüm URL’ler (Excel) | `GET /api/seo-audit/{site_id}/export.xlsx?filter=poor` |
| Tüm URL’ler (CSV) | `GET /api/seo-audit/{site_id}/export.csv?filter=poor` |
| Kritik sayfa listesi (JSON) | `GET /api/seo-audit/{site_id}/issues?filter=poor` |
| Özet istatistik | Sayfa yüklemesinde `get_audit_summary()` |
| Manuel tarama | `POST /api/seo-audit/{site_id}/run` |

---

## 10. Örnek karar ağacı (özet)

```
Sayfa crawl
  ├─ HTTP/HTML/title/desc/h1 eksik? → POOR (Kritik)
  └─ Değilse
       ├─ Uzunluk / canonical / schema / OG / çoklu H1 / noindex? → NEEDS_IMPROVEMENT
       └─ Değilse → GOOD
```

---

*SEO Agent — projectcontrol.up.railway.app*
