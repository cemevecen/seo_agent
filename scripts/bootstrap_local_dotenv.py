#!/usr/bin/env python3
"""Yerel .env oluşturur: .env.example + üretilen SECRET_KEY ve ENCRYPTION_KEY."""
from __future__ import annotations

from pathlib import Path

from cryptography.fernet import Fernet
import secrets


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    example = root / ".env.example"
    out = root / ".env"
    if out.exists():
        print(f"Zaten var: {out} (üzerine yazılmadı)")
        return
    text = example.read_text(encoding="utf-8")
    text = text.replace(
        "SECRET_KEY=cok_uzun_rastgele_bir_string_gir",
        "SECRET_KEY=" + secrets.token_hex(32),
        1,
    )
    text = text.replace(
        "ENCRYPTION_KEY=fernet_ile_uretilmis_key_gir",
        "ENCRYPTION_KEY=" + Fernet.generate_key().decode(),
        1,
    )
    # Boş bırakılırsa uygulama başlamaz; yerel geliştirme için geçerli dolgu (ofisten gerçek anahtarları yapıştır)
    text = text.replace(
        "GOOGLE_API_KEY=\n",
        "GOOGLE_API_KEY=yerel-dolgu-ofisten-gercek-anahtari-yapistir\n",
        1,
    )
    text = text.replace(
        "SMTP_PASSWORD=\n",
        "SMTP_PASSWORD=yerel-dolgu\n",
        1,
    )
    out.write_text(text, encoding="utf-8")
    print(f"Yazıldı: {out}")


if __name__ == "__main__":
    main()
