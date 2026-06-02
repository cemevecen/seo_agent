"""Uygulama konfigürasyonlarını .env üzerinden yöneten güvenli ayar katmanı."""

import os
from pathlib import Path

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"


class Settings(BaseSettings):
    # Güvenlik açısından zorunlu gizli alanlar yalnızca .env'den okunur.
    google_api_key: str
    google_client_id: str = ""
    google_client_secret: str = ""
    google_oauth_redirect_uri: str = "http://127.0.0.1:8012/api/search-console/oauth/callback"
    # Gmail gelen kutusu (/inbox): Google Cloud OAuth istemcisine bu redirect URI de eklenmeli.
    gmail_inbox_oauth_redirect_uri: str = Field(
        default="http://127.0.0.1:8012/api/inbox/oauth/callback",
        validation_alias=AliasChoices("GMAIL_INBOX_OAUTH_REDIRECT_URI", "gmail_inbox_oauth_redirect_uri"),
    )
    # Gmail OAuth başlatırken login_hint (örn. cemevecen@nokta.com) — yanlış hesap seçimini azaltır.
    inbox_oauth_login_hint: str = Field(
        default="",
        validation_alias=AliasChoices("INBOX_OAUTH_LOGIN_HINT", "inbox_oauth_login_hint"),
    )
    # Gmail arama sözdizimi: https://support.google.com/mail/answer/7190
    inbox_gmail_query: str = Field(
        default=(
            "(to:info@doviz.com OR to:feedback@doviz.com OR to:info@sinemalar.com OR to:feedback@sinemalar.com "
            "OR to:info@blogcu.com OR deliveredto:info@blogcu.com "
            "OR to:info@izlesene.com OR deliveredto:info@izlesene.com "
            "OR to:medya@nokta.com OR deliveredto:medya@nokta.com "
            "OR to:reklam@nokta.com OR deliveredto:reklam@nokta.com "
            "OR from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com "
            "OR from:noreply@doviz.com OR to:me)"
        ),
        validation_alias=AliasChoices("INBOX_GMAIL_QUERY", "inbox_gmail_query"),
    )
    inbox_openai_model: str = Field(
        default="gpt-4.1-mini",
        validation_alias=AliasChoices("INBOX_OPENAI_MODEL", "inbox_openai_model"),
    )
    database_url: str
    secret_key: str
    smtp_password: str
    # İsteğe bağlı: yalnızca günlük AI özetinde (Gemini sağlayıcı seçilirse) kullanılır — uygulama başı zorunlu değil.
    gemini_api_key: str = ""
    # İsteğe bağlı: günlük AI özetinde Groq sağlayıcı seçilirse kullanılır. Başka modüller bu anahtarı kullanmaz.
    groq_api_key: str = ""
    # İsteğe bağlı: günlük AI özetinde OpenAI sağlayıcı seçilirse kullanılır.
    openai_api_key: str = ""
    encryption_key: str

    # GitHub entegrasyonu (ajan araçları için)
    github_token: str = Field(
        default="",
        validation_alias=AliasChoices("GITHUB_TOKEN", "github_token"),
    )
    github_repo: str = Field(
        default="cemevecen/seo_agent",
        validation_alias=AliasChoices("GITHUB_REPO", "github_repo"),
    )
    # Railway API (deployment & log araçları için)
    railway_api_token: str = Field(
        default="",
        validation_alias=AliasChoices("RAILWAY_API_TOKEN", "railway_api_token"),
    )
    railway_project_id: str = Field(
        default="",
        validation_alias=AliasChoices("RAILWAY_PROJECT_ID", "railway_project_id"),
    )

    # OMDB (Open Movie Database) — IMDb / RT / Metacritic zenginleştirme
    omdb_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("OMDB_API_KEY", "omdb_api_key"),
    )

    # TMDB (The Movie Database) — vizyon takvimi ve film içerik planlama
    tmdb_read_access_token: str = Field(
        default="",
        validation_alias=AliasChoices("TMDB_READ_ACCESS_TOKEN", "tmdb_read_access_token"),
    )

    # GA4 (Google Analytics Data API) - Service Account JSON (string veya dosya yolu)
    ga4_service_account_json: str = ""
    ga4_service_account_file: str = ""
    # Firebase Crashlytics → BigQuery: platform başına ayrı service account JSON.
    # Railway’de CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON ve CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON olarak tanımlanır.
    crashlytics_ios_service_account_json: str = ""
    crashlytics_android_service_account_json: str = ""
    # Eski tek-proje ayarı (geriye uyumluluk için bırakıldı).
    firebase_crashlytics_bigquery_project: str = ""
    firebase_crashlytics_bigquery_dataset: str = "firebase_crashlytics"

    # GA4, Search Console, mağaza analitiği: rapor takvim günü (sunucu UTC olsa bile dün/son N gün TSİ).
    report_calendar_timezone: str = "Europe/Istanbul"
    # GA4 landing sayfa filtresi: virgülle ayrılmış path alt dizeleri (haber vb. hariç)
    ga4_exclude_path_substrings: str = "/haber/,/news/,/gundem/"

    # Uygulama sadece localhost üzerinde dinlenecek şekilde varsayılanlanır.
    app_host: str = "127.0.0.1"
    allowed_client_ips: str = "176.40.240.237,78.187.20.15"
    trust_proxy_headers: bool = True
    # DB'de admin hash yokken tek seferlik bootstrap (ilk deploy). UI'dan şifre kaydı ile aynı tablo.
    # Env: ADMIN_PASSWORD — en az 6 karakter; doluysa ve veritabanında kayıt yoksa açılışta yazılır.
    admin_bootstrap_password: str = Field(
        default="",
        validation_alias=AliasChoices("ADMIN_PASSWORD", "admin_bootstrap_password"),
    )
    # Settings sayfasına özel ikinci şifre katmanı.
    settings_password: str = Field(
        default="",
        validation_alias=AliasChoices("SETTINGS_PASSWORD", "settings_password"),
    )
    inbox_action_password: str = Field(
        default="",
        validation_alias=AliasChoices("INBOX_ACTION_PASSWORD", "inbox_action_password"),
    )
    # IP allowlist + /admin girişi. Yerelde false yapılabilir. Railway tespit edilirse uygulama yok sayar, her zaman açık kalır.
    admin_auth_enforced: bool = Field(
        default=True,
        validation_alias=AliasChoices("ADMIN_AUTH_ENFORCED", "admin_auth_enforced"),
    )
    admin_login_alert_email: str = Field(
        default="cemevecen@nokta.com",
        validation_alias=AliasChoices("ADMIN_LOGIN_ALERT_EMAIL", "admin_login_alert_email"),
    )
    admin_login_alert_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices("ADMIN_LOGIN_ALERT_ENABLED", "admin_login_alert_enabled"),
    )
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    mail_from: str = ""
    mail_to: str = ""
    operations_mail_to: str = ""
    error_report_mail_to: str = Field(
        default="",
        validation_alias=AliasChoices("ERROR_REPORT_MAIL_TO", "error_report_mail_to"),
    )
    # SMTP günlük kota: Gmail tüketici ~500 mesaj/gün; Workspace ücretli kullanıcı ~2000/gün (Google duyurusu, değişebilir).
    # Workspace’te POP/IMAP/SMTP ile mesaj başına en fazla 100 alıcı (RCPT) sınırı yaygındır.
    # Referans: https://support.google.com/mail/answer/22839 — https://support.google.com/a/answer/166852
    # 0 = günlük sayaç kapalı (yalnızca smtp_daily_quota_enabled=false ile de kapatılır).
    smtp_daily_quota_enabled: bool = False
    smtp_daily_send_limit: int = Field(default=5000, ge=0, le=100000)
    smtp_quota_calendar_timezone: str = "Europe/Istanbul"
    smtp_max_recipients_per_message: int = Field(default=100, ge=1, le=500)

    quota_guard_enabled: bool = True
    quota_warning_ratio: float = 0.8
    pagespeed_daily_limit: int = 80
    pagespeed_monthly_limit: int = 1500
    pagespeed_request_timeout: int = 75
    pagespeed_max_retries: int = 2
    pagespeed_retry_backoff_seconds: float = 2.0
    pagespeed_refresh_cooldown_seconds: int = 1800
    # POST /api/site/.../data-explorer/refresh (ve tam site manuel yenileme): günlük/aylık kota sayacını atla.
    # False iken kota dolunca PageSpeed çağrısı yapılmaz; arayüzde açık hata gösterilir.
    pagespeed_manual_refresh_bypass_quota: bool = True
    lighthouse_live_score_cache_seconds: int = 1800
    pagespeed_live_sync_on_page_load: bool = False
    pagespeed_auto_collect_on_page_load: bool = False
    search_console_daily_limit: int = 80
    search_console_monthly_limit: int = 1500
    # Storage/DB güvenliği: çok büyük SC snapshot'ları diski hızla şişiriyor.
    # Gerekirse .env ile yükseltilebilir.
    # batch_size >= max_rows → tek API çağrısıyla tüm sayfayı çeker (pagination yok).
    search_console_row_batch_size: int = 2500
    search_console_max_rows: int = 2500
    # Tam yenilemede "page" boyutlu raporlar çok fazla API çağrısı / süre üretir (Railway timeout).
    search_console_include_page_dimension: bool = True
    search_console_page_report_row_cap: int = 1000
    # Günlük trend: son N tam gün (date boyutu; karşılaştırma yok).
    search_console_trend_12m_days: int = 365
    ga4_trend_12m_days: int = 365
    ga4_trend_12m_period_days: int = 365
    search_console_refresh_cooldown_seconds: int = 21600
    search_console_live_fetch_on_read: bool = False
    search_console_scheduled_refresh_enabled: bool = True
    search_console_scheduled_refresh_hour: int = 4
    search_console_scheduled_refresh_minute: int = 0
    search_console_scheduled_refresh_site_spacing_seconds: int = 20
    # Tam SC yenilemesinde (collect_search_console_metrics): çekimden önce bu siteye ait snapshot satırlarını sil.
    # Alert/hafif job buna dokunmaz (yoksa 28g/trend tek başına yazılamaz).
    # RAILWAY_ENVIRONMENT / RAILWAY_PROJECT_ID yoksa yalnızca search_console_purge_before_collect=true ile çalışır.
    search_console_purge_on_railway: bool = True
    search_console_purge_before_collect: bool = False
    alerts_scheduled_refresh_enabled: bool = False
    alerts_scheduled_refresh_hour: int = 4
    alerts_scheduled_refresh_minute: int = 0

    crawler_refresh_cooldown_seconds: int = 1800
    crawler_request_timeout_seconds: int = 10
    crawler_source_page_limit: int = 40
    crawler_target_url_limit: int = 250
    crawler_links_per_page_limit: int = 30
    crawler_sitemap_url_limit: int = 500
    crawler_issue_sample_limit: int = 8
    site_audit_sitemap_url_limit: int = 2000
    site_audit_request_timeout_seconds: int = 12
    site_audit_recent_days: int = 90
    site_audit_index_mode_default: str = "quick"
    site_audit_exact_inspection_limit: int = 200
    site_audit_sc_page_limit: int = 5000
    outbound_min_interval_seconds: float = 2.0
    outbound_cache_ttl_seconds: int = 1800
    outbound_user_agent: str = "SEO-Agent/1.0 (+https://example.com; polite-monitoring)"

    url_inspection_refresh_cooldown_seconds: int = 21600
    crux_refresh_cooldown_seconds: int = 21600
    scheduled_refresh_enabled: bool = True
    scheduled_refresh_hour: int = 7
    scheduled_refresh_minute: int = 0
    scheduled_refresh_timezone: str = "Europe/Istanbul"
    scheduled_refresh_site_spacing_seconds: int = 10
    scheduled_refresh_monitor_enabled: bool = True
    scheduled_refresh_monitor_interval_minutes: int = 15
    scheduled_refresh_monitor_grace_minutes: int = 45
    # Genel uygulama e-postaları: SEO Alert, operasyon özeti, günlük AI özet, GA4 haftalık özet vb.
    # False iken bu kanallar SMTP kullanmaz. GA4 Realtime alarm postası ayrı bayrakla (ga4_realtime_email_enabled) gönderilir.
    outbound_email_enabled: bool = False
    # Tetik/operasyon özet mailleri (PageSpeed/CrUX günlük vb.); outbound açıkken anlam taşır.
    operations_trigger_email_enabled: bool = False
    # True iken operasyon/GA4 özet e-postaları yalnızca trigger_source=manual ile gider (zamanlayıcı ve monitör dahil).
    email_manual_triggers_only: bool = True

    # GA4 günlük toplama (Ankara saati; Search Console 04:00, PageSpeed+Crawler+CrUX tam yenileme 07:00 ile sıralı)
    ga4_scheduled_refresh_enabled: bool = True
    ga4_scheduled_refresh_hour: int = 4
    ga4_scheduled_refresh_minute: int = 30
    ga4_scheduled_refresh_site_spacing_seconds: int = 15

    # App Store + Google Play analitik günlük özet (TSİ)
    app_intel_scheduled_refresh_enabled: bool = True
    app_intel_scheduled_refresh_hour: int = 7
    app_intel_scheduled_refresh_minute: int = 0

    # Günlük AI özet (GA4, PageSpeed, Search Console, uyarılar). False iken zamanlanmış LLM job çalışmaz;
    # POST /ai/generate (force) yine üretir.
    ai_daily_brief_enabled: bool = False
    # APScheduler ile sabit saatte otomatik üretim. Kapalıyken yalnızca manuel tetik (ör. /ai/generate).
    ai_daily_brief_scheduler_enabled: bool = False
    # Özet kaydedildikten sonra operasyon alıcılarına e-posta gönder.
    ai_daily_brief_send_email: bool = False
    ai_daily_brief_hour: int = 6
    ai_daily_brief_minute: int = 15
    ai_daily_brief_timezone: str = "Europe/Istanbul"
    # Kalite / maliyet dengesi: Flash ailesi, JSON çıktı ile uyumlu (gerekirse .env ile değiştirin).
    ai_daily_brief_gemini_model: str = "gemini-2.5-flash"
    # gemini: zamanlanmış günlük özet önce Gemini (ücretsiz katman); failover açıksa hata/kota/bütçede Groq.
    # auto: her iki anahtar varsa önce Gemini, yoksa yalnızca mevcut olan.
    ai_daily_brief_provider: str = "gemini"
    # Groq üretim modeli: GPT-OSS 120B — özet kalitesi güçlü; Groq fiyat tablosuna göre .env fiyatlarını güncelleyin.
    ai_daily_brief_groq_model: str = "openai/gpt-oss-120b"
    # OpenAI üretim modeli: maliyet/kalite dengesi için varsayılan mini model.
    ai_daily_brief_openai_model: str = "gpt-4.1-mini"
    # 429 / kota / erişim hatasında diğer anahtar varsa otomatik Groq ↔ Gemini geçişi (tek üretim turu içinde).
    ai_daily_brief_provider_failover: bool = True
    # true: üretim + Türkçe tek istekte (günde en fazla yarı yarıya daha az LLM çağrısı).
    ai_daily_brief_single_llm_call: bool = True
    # Takvim tutamağı: Europe/Istanbul günü başına toplam LLM HTTP çağrısı üst sınırı.
    # Tek-istek modunda 1 başarılı özet ≈ 1 çağrı; failover ile aynı günde en fazla +1 deneme; çift çağrı modunda ×2.
    ai_daily_brief_max_llm_calls_per_calendar_day: int = 6

    # LLM tahmini harcama (TRY), Europe/Istanbul takvim ayına göre. 0 = TRY tavanı yok (yalnızca günlük çağrı kotası).
    llm_spend_budget_try: float = 100.0
    # API faturaları USD ise tahmini çevrim; güncel kur ile .env ayarlayın.
    llm_spend_usd_to_try: float = 35.0
    # Groq üretim fiyatı (varsayılan GPT-OSS 120B: input/output $/1M token — console.groq.com)
    llm_groq_prompt_usd_per_mtok: float = 0.15
    llm_groq_completion_usd_per_mtok: float = 0.60
    # Gemini Flash ailesi için kabaca $/1M — model/plana göre .env ile düzeltin.
    llm_gemini_prompt_usd_per_mtok: float = 0.075
    llm_gemini_completion_usd_per_mtok: float = 0.30
    llm_openai_prompt_usd_per_mtok: float = 0.15
    llm_openai_completion_usd_per_mtok: float = 0.60

    # GA4 Realtime monitoring (anlık karşılaştırma & alarm)
    ga4_realtime_enabled: bool = True
    # Backend alarm job: 30dk. Tüm realtime alarmları (site/sayfa/haber/404/app) tek bir job
    # döngüsünde toplanıp TEK mail olarak gönderildiğinden, bu aralık aynı zamanda
    # "en erken yarım saatte bir mail" garantisini verir + her kontrolde daha çok veri birikir.
    ga4_realtime_interval_minutes: int = 30
    # KPI toplam penceresi: GA4 Realtime UI ile aynı (max 30 dk).
    ga4_realtime_window_minutes: int = 30
    # Realtime sayfası açıkken GA4 KPI çekimi (tarayıcı). Job aralığından bağımsız.
    ga4_realtime_ui_poll_seconds: int = Field(default=60, ge=15, le=600)   # 1 dakika
    # Sunucu-tarafı GA4 Realtime cache TTL'leri (saniye). Çok-istemcili/sık polling'in
    # GA4 saatlik token kotasını tüketmesini engeller; aynı pencerede tek upstream çağrı.
    ga4_realtime_kpi_cache_seconds: int = Field(default=30, ge=0, le=300)
    ga4_realtime_list_cache_seconds: int = Field(default=60, ge=0, le=300)
    # 429/hata anında son başarılı CANLI sonucun gösterileceği azami yaş (saniye).
    ga4_realtime_last_good_seconds: int = Field(default=1800, ge=0, le=21600)
    ga4_realtime_page_alerts_enabled: bool = True
    # Haberler (unifiedScreenName): snapshot karşılaştırması + e-posta; kontrol aralığı ayrı (dakika).
    ga4_realtime_news_alerts_enabled: bool = True
    ga4_realtime_news_alert_interval_minutes: int = Field(default=5, ge=5, le=120)   # 5dk
    ga4_realtime_news_alert_window_minutes: int = Field(default=15, ge=5, le=30)
    # Realtime alarm e-postaları (site + sayfa): OUTBOUND_EMAIL_ENABLED veya günlük GA4/AI özetlerinden bağımsız.
    ga4_realtime_email_enabled: bool = True
    ga4_realtime_page_alert_email: bool = True
    ga4_realtime_news_alert_email: bool = True
    # Aynı site/kural için e-posta tekrar baskılama süresi (dakika). 0 = baskılama yok.
    ga4_realtime_alarm_email_cooldown_minutes: int = Field(default=30, ge=0, le=480)  # 30dk cooldown
    # Realtime «Haberler» sekmesi: unifiedScreenName bu öneklerle başlıyorsa elenir (virgülle). Boş = yerleşik liste.
    ga4_realtime_news_screen_exclude_prefixes: str = ""

    # Realtime 404 spike: anlık 404 sayfasındaki kullanıcı eşikleri
    ga4_realtime_404_enabled: bool = True
    ga4_realtime_404_warning_threshold: int = Field(default=10, ge=1, le=500)   # uyarı eşiği
    ga4_realtime_404_critical_threshold: int = Field(default=25, ge=1, le=500)  # kritik eşik
    ga4_realtime_404_window_minutes: int = Field(default=15, ge=5, le=30)       # GA4 penceresi

    # False: otomatik sayfa yükü ölçümleri kapalı kalır; manuel PSI/Data Explorer ve dashboard ölçümü (force) çalışır.
    live_refresh_enabled: bool = True
    live_refresh_method: str = "GET"
    live_refresh_timeout: int = 8
    live_refresh_urls: str = ""

    # Zaman serisi temizliği (gecelik job). Railway disk sıkışırsa süreleri kısaltın.
    db_retention_collector_run_days: int = Field(default=30, ge=1, le=3650)
    db_retention_alert_log_days: int = Field(default=60, ge=1, le=3650)
    db_retention_metric_days: int = Field(default=90, ge=1, le=3650)
    db_retention_notification_delivery_days: int = Field(default=30, ge=1, le=3650)
    # Realtime ve Uygulama verileri (Hızlı büyüyen tablolar)
    db_retention_realtime_snapshot_days: int = Field(default=8, ge=1, le=365) # Kullanıcı talebi: 8 gün
    db_retention_realtime_alarm_log_days: int = Field(default=30, ge=1, le=365)
    db_retention_app_intel_cache_days: int = Field(default=7, ge=1, le=365)
    db_retention_ai_report_days: int = Field(default=30, ge=1, le=365)

    # Gecelik cleanup sonrası tam DB VACUUM ANALYZE (Postgres). Varsayılan kapalı.
    db_retention_run_vacuum: bool = False

    model_config = SettingsConfigDict(env_file=str(ENV_PATH), env_file_encoding="utf-8", case_sensitive=False, extra="ignore")

    @model_validator(mode="after")
    def validate_required_values(self):
        # Boş bırakılan güvenlik anahtarlarında uygulama başlangıcını engeller.
        required_fields = {
            "GOOGLE_API_KEY": self.google_api_key,
            "DATABASE_URL": self.database_url,
            "SECRET_KEY": self.secret_key,
            "SMTP_PASSWORD": self.smtp_password,
            "ENCRYPTION_KEY": self.encryption_key,
        }
        missing = [key for key, value in required_fields.items() if not value or not value.strip()]
        if missing:
            raise ValueError(f"Eksik zorunlu .env anahtarları: {', '.join(missing)}")
        return self


settings = Settings()


def email_allows_trigger_source(trigger_source: str) -> bool:
    """Operasyon/GA4 özet gibi tetik e-postalarında: yalnızca manuel mi?"""
    if not settings.email_manual_triggers_only:
        return True
    return (trigger_source or "").strip().lower() == "manual"


def is_railway_runtime() -> bool:
    """Railway deploy: ortam değişkenleri ile tespit (resmi image'da RAILWAY_ENVIRONMENT set)."""
    return bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))


def search_console_should_purge_before_collect() -> bool:
    """SC canlı çekiminden önce bu site için eski snapshot satırlarını silmek gerekiyor mu."""
    if settings.search_console_purge_before_collect:
        return True
    return bool(is_railway_runtime() and settings.search_console_purge_on_railway)
