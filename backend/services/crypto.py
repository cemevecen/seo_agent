"""Credential verilerini Fernet ile şifreleme/çözme yardımcıları."""

from cryptography.fernet import Fernet

from backend.config import settings


def _get_fernet() -> Fernet:
    # Şifreleme anahtarı sadece .env üzerinden okunur.
    return Fernet(settings.encryption_key.encode("utf-8"))


def encrypt_text(plain_text: str) -> str:
    """Düz metni veritabanına yazmadan önce şifreler."""
    token = _get_fernet().encrypt(plain_text.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_text(encrypted_text: str) -> str:
    """Veritabanındaki şifreli metni uygulama içinde çözer."""
    data = _get_fernet().decrypt(encrypted_text.encode("utf-8"))
    return data.decode("utf-8")
