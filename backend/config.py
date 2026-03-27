"""Uygulama konfigürasyonlarını .env üzerinden yöneten güvenli ayar katmanı."""

from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"


class Settings(BaseSettings):
    # Güvenlik açısından zorunlu gizli alanlar yalnızca .env'den okunur.
    google_api_key: str
    database_url: str
    secret_key: str
    smtp_password: str
    gemini_api_key: str
    encryption_key: str

    # Uygulama sadece localhost üzerinde dinlenecek şekilde varsayılanlanır.
    app_host: str = "127.0.0.1"
    allowed_client_ips: str = ""
    trust_proxy_headers: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    mail_from: str = ""
    mail_to: str = ""

    quota_guard_enabled: bool = True
    quota_warning_ratio: float = 0.8
    pagespeed_daily_limit: int = 80
    pagespeed_monthly_limit: int = 1500
    pagespeed_request_timeout: int = 75
    pagespeed_max_retries: int = 2
    pagespeed_retry_backoff_seconds: float = 2.0
    search_console_daily_limit: int = 80
    search_console_monthly_limit: int = 1500

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
