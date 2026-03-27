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
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    mail_from: str = ""
    mail_to: str = ""

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
