# SEO Agent — yerel kurulum

Bu depo **gizli anahtar içermez**. Üretim veya yerel çalışma için tüm sırlar yalnızca sizin makinenizde, repoya hiç eklenmeyen dosyalarda tutulur.

## Önkoşullar

- **Python 3.10+** (3.11 veya 3.12 önerilir)
- İhtiyaca göre: **PostgreSQL**, **Redis** (`.env` içindeki adreslere göre)
- Git

## 1. Depoyu alma ve dal

```bash
git clone https://github.com/cemevecen/seo_agent.git
cd seo_agent
git checkout codex/reporting-and-alerts-refresh
git pull
```

Dal adı değişmişse `git branch -a` ile güncel dalı seçin.

## 2. Sanal ortam ve bağımlılıklar

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Ortam değişkenleri (güvenli kullanım)

1. **Asla** gerçek anahtarları repoya commit etmeyin. `.env` zaten `.gitignore` içindedir.
2. Şablonu kopyalayın ve yalnızca **kendi makinenizde** düzenleyin:

   ```bash
   cp .env.example .env
   ```

3. `.env` içindeki alanları kendi değerlerinizle doldurun. Örnek dosyada boş bırakılan alanlar için üretici dokümantasyonuna bakın (Google OAuth, GA4 service account, veritabanı URL’si vb.).
4. **Service account / JSON anahtar dosyaları:** Repoda örnek olarak `keys/` klasörü `.gitignore` ile dışlanmıştır. Dosyaları bu klasöre koyup yolu `.env` içinde (ör. `GA4_SERVICE_ACCOUNT_FILE`) verin; dosyaları Git’e eklemeyin.
5. Hassas dosyalar için izin sıkılaştırması (Unix):

   ```bash
   chmod 600 .env
   chmod 600 keys/*.json   # kullandığınız anahtar dosyalarına göre
   ```

6. **Yedekleme:** `.env` ve `keys/` yalnızca güvenli kanallarla (şifreli yedek, kasa, yönetilen gizli deposu) saklanmalıdır; düz metin sohbet veya e-posta ile paylaşılmamalıdır.

## 4. Sunucuyu çalıştırma

```bash
source .venv/bin/activate
python run_server.py
```

Varsayılan adres: `http://127.0.0.1:8012` (`run_server.py` içinde tanımlı).

## 5. Güvenlik kontrol listesi

| Konu | Uygulama |
|------|-----------|
| Sırlar | `.env`, `keys/`, `secrets/` repoya girmez; commit öncesi `git status` ile kontrol edin. |
| Yapay zeka / asistan | Tam `.env` içeriğini veya ham anahtarları sohbete yapıştırmayın; sızdıysa ilgili anahtarı üreticide **iptal / rotate** edin. |
| OAuth | Redirect URI’ler (ör. Google) uygulamanın gerçekten dinlediği adresle birebir eşleşmeli. |
| Üretim | `SECRET_KEY`, veritabanı ve e-posta şifreleri güçlü ve benzersiz olsun; üretimde ortam değişkenleri mümkünse platform gizli yönetimi ile verilsin. |
| Bağımlılıklar | Düzenli `pip install -r requirements.txt` ve güvenlik güncellemeleri için takip. |

## Sorun giderme

- **Bağlantı hataları:** PostgreSQL/Redis çalışıyor mu ve `.env` içindeki host/port doğru mu?
- **Google / GA4:** OAuth veya service account izinleri ilgili projede tanımlı mı?
- Eksik alanlar için `.env.example` içindeki yorum satırlarına bakın; değer örneği olarak gerçek anahtar kullanmayın.

---

*Bu dosya işletim sırları içermez; yalnızca güvenli kurulum akışını tarif eder.*
