"""Scrape login kimlik bilgileri — macOS Keychain.

Parola neden `.env` değil de Keychain?

* `.env` uygulama sürecine yükleniyor; kaza eseri log'a, hata izine veya bir
  teşhis çıktısına düşme riski var. Keychain'den okunan değer yalnızca login
  anında bellekte kalır.
* `.env` dosyası kopyalanıp taşınabiliyor; Keychain kaydı kullanıcıya ve
  makineye bağlı.
* Repoya sızma ihtimali sıfır — dosya yok.

Kayıt (parolayı yalnızca kullanıcı girer, burada asla görünmez):

    security add-generic-password -U \\
      -s seo-agent-asc -a "apple-id@ornek.com" -w

E-posta gizli değil; `-a` alanında saklanır ve buradan okunur. İstenirse
ortam değişkeniyle de verilebilir (ASC_LOGIN_EMAIL / GOOGLE_LOGIN_EMAIL).

Bu modül **hiçbir koşulda parolayı log'lamaz**; hata mesajlarında da yalnızca
"bulundu / bulunamadı" bilgisi geçer.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)

# target → (keychain service adı, e-posta için ortam değişkeni)
KEYCHAIN_TARGETS: dict[str, tuple[str, str]] = {
    "asc": ("seo-agent-asc", "ASC_LOGIN_EMAIL"),
    "google": ("seo-agent-google", "GOOGLE_LOGIN_EMAIL"),
}

_ACCT_RE = re.compile(r'"acct"<blob>="([^"]*)"')
_TIMEOUT_SEC = 10


@dataclass(frozen=True)
class Credentials:
    """Login bilgisi. `password` asla log'lanmaz, __repr__ maskelenir."""

    target: str
    email: str
    password: str

    @property
    def complete(self) -> bool:
        return bool(self.email and self.password)

    def __repr__(self) -> str:  # noqa: D105 - parola sızmasın
        state = "dolu" if self.password else "boş"
        return f"Credentials(target={self.target!r}, email={self.email!r}, password=<{state}>)"


def keychain_available() -> bool:
    return bool(shutil.which("security"))


def _run_security(args: list[str]) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            ["security", *args],
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_SEC,
            check=False,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Keychain okunamadı (%s): %s", args[:2], exc)
        return 1, "", str(exc)


def _keychain_password(service: str) -> str:
    code, out, _ = _run_security(["find-generic-password", "-s", service, "-w"])
    return out.strip() if code == 0 else ""


def _keychain_account(service: str) -> str:
    """`-a` alanı (e-posta). Parola istenmediği için `-w` kullanılmaz."""
    code, out, err = _run_security(["find-generic-password", "-s", service])
    if code != 0:
        return ""
    match = _ACCT_RE.search(out or err or "")
    return match.group(1).strip() if match else ""


def load_credentials(target: str) -> Credentials:
    """Hedef için e-posta + parola. Eksikse boş alanlarla döner, hata atmaz."""
    key = (target or "").strip().lower()
    service, email_env = KEYCHAIN_TARGETS.get(key, ("", ""))
    if not service:
        return Credentials(target=key, email="", password="")

    email = (os.environ.get(email_env) or "").strip()
    password = ""
    if keychain_available():
        password = _keychain_password(service)
        if not email:
            email = _keychain_account(service)
    return Credentials(target=key, email=email, password=password)


def credentials_status(target: str) -> dict[str, object]:
    """Teşhis için — parola içermez, yalnızca var/yok bilgisi."""
    creds = load_credentials(target)
    service, email_env = KEYCHAIN_TARGETS.get((target or "").lower(), ("", ""))
    return {
        "target": target,
        "keychain_service": service,
        "email_env": email_env,
        "keychain_available": keychain_available(),
        "has_email": bool(creds.email),
        "has_password": bool(creds.password),
        "ready": creds.complete,
    }
