"""Uygulama konfigürasyonlarını .env üzerinden yöneten güvenli ayar katmanı."""

from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"


class Settings(BaseSettings):
    # Güvenlik açısından zorunlu gizli alanlar yalnızca .env'den okunur.
    google_api_key: str
    google_client_id: str = ""
    google_client_secret: str = ""
    google_oauth_redirect_uri: str = "http://127.0.0.1:8012/api/search-console/oauth/callback"
    database_url: str
    secret_key: str
    smtp_password: str
    gemini_api_key: str
    encryption_key: str

    # GA4 (Google Analytics Data API) - Service Account JSON (string veya dosya yolu)
    ga4_service_account_json: str = ""
    ga4_service_account_file: str = ""
    # GA4 landing sayfa filtresi: virgülle ayrılmış path alt dizeleri (haber vb. hariç)
    ga4_exclude_path_substrings: str = "/haber/,/news/,/gundem/"

    # Uygulama sadece localhost üzerinde dinlenecek şekilde varsayılanlanır.
    app_host: str = "127.0.0.1"
    allowed_client_ips: str = ""
    trust_proxy_headers: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    mail_from: str = ""
    mail_to: str = ""
    operations_mail_to: str = "cemevecen@nokta.com"

    quota_guard_enabled: bool = True
    quota_warning_ratio: float = 0.8
    pagespeed_daily_limit: int = 80
    pagespeed_monthly_limit: int = 1500
    pagespeed_request_timeout: int = 75
    pagespeed_max_retries: int = 2
    pagespeed_retry_backoff_seconds: float = 2.0
    pagespeed_refresh_cooldown_seconds: int = 1800
    lighthouse_live_score_cache_seconds: int = 1800
    pagespeed_live_sync_on_page_load: bool = False
    pagespeed_auto_collect_on_page_load: bool = False
    search_console_daily_limit: int = 80
    search_console_monthly_limit: int = 1500
    search_console_row_batch_size: int = 2500
    search_console_max_rows: int = 10000
    search_console_refresh_cooldown_seconds: int = 21600
    search_console_live_fetch_on_read: bool = False
    search_console_scheduled_refresh_enabled: bool = True
    search_console_scheduled_refresh_hour: int = 4
    search_console_scheduled_refresh_minute: int = 0
    search_console_scheduled_refresh_site_spacing_seconds: int = 20
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
    outbound_min_interval_seconds: float = 2.0
    outbound_cache_ttl_seconds: int = 1800
    outbound_user_agent: str = "SEO-Agent/1.0 (+https://example.com; polite-monitoring)"

    url_inspection_refresh_cooldown_seconds: int = 21600
    crux_refresh_cooldown_seconds: int = 21600
    scheduled_refresh_enabled: bool = True
    scheduled_refresh_hour: int = 5
    scheduled_refresh_minute: int = 0
    scheduled_refresh_timezone: str = "Europe/Istanbul"
    scheduled_refresh_site_spacing_seconds: int = 10
    scheduled_refresh_monitor_enabled: bool = True
    scheduled_refresh_monitor_interval_minutes: int = 15
    scheduled_refresh_monitor_grace_minutes: int = 45
    # Tetik/operasyon mailleri (manual refresh, zamanında güncellenmedi vb.)
    # Local ortamda False, prod'a taşıyınca True yap
    operations_trigger_email_enabled: bool = True

    # GA4 günlük toplama (Ankara saati; Search Console 04:00, tam yenileme 05:00 ile sıralı)
    ga4_scheduled_refresh_enabled: bool = True
    ga4_scheduled_refresh_hour: int = 4
    ga4_scheduled_refresh_minute: int = 30
    ga4_scheduled_refresh_site_spacing_seconds: int = 15

    live_refresh_enabled: bool = True
    live_refresh_method: str = "GET"
    live_refresh_timeout: int = 8
    live_refresh_urls: str = ""

    model_config = SettingsConfigDict(env_file=str(ENV_PATH), env_file_encoding="utf-8", case_sensitive=False)

    @model_validator(mode="after")
    def validate_required_values(self):
        # Boş bırakılan güvenlik anahtarlarında uygulama başlangıcını engeller.
        required_fields = {
            "GOOGLE_API_KEY": self.google_api_key,
            "DATABASE_URL": self.database_url,
            "SECRET_KEY": self.secret_key,
            "SMTP_PASSWORD": self.smtp_password,
            "GEMINI_API_KEY": self.gemini_api_key,
            "ENCRYPTION_KEY": self.encryption_key,
        }
        missing = [key for key, value in required_fields.items() if not value or not value.strip()]
        if missing:
            raise ValueError(f"Eksik zorunlu .env anahtarları: {', '.join(missing)}")
        return self


settings = Settings()
