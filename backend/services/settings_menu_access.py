"""Ayarlar (/settings) menüsü — yalnızca tanımlı e-postalar."""

from __future__ import annotations

import logging
import time
from html import escape
from typing import TYPE_CHECKING

from backend.config import settings
from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS, _normalize_email

if TYPE_CHECKING:
    from starlette.requests import Request

LOGGER = logging.getLogger(__name__)

SETTINGS_MENU_ALLOWED_EMAILS = ADMIN_MEMBER_EMAILS
SETTINGS_ACCESS_REQUEST_TO = "cemevecen@nokta.com"
SETTINGS_ACCESS_REQUEST_SUBJECT = "Ayarlar menüsüne erişim isteği"
SETTINGS_ACCESS_REQUEST_COOLDOWN_SEC = 3600
_SETTINGS_REQUEST_COOLDOWN_SEC = SETTINGS_ACCESS_REQUEST_COOLDOWN_SEC


def is_settings_menu_allowed_email(email: str) -> bool:
    em = _normalize_email(email)
    return bool(em) and em in SETTINGS_MENU_ALLOWED_EMAILS


def resolve_settings_menu_visible(
    *,
    member_email: str | None,
    admin_authenticated: bool,
) -> bool:
    """Üst menüde settings linki: Google üyesi allowlist veya (üye yok) admin şifre oturumu."""
    if member_email:
        return is_settings_menu_allowed_email(member_email)
    return bool(admin_authenticated)


def member_denied_settings_access(member_email: str) -> bool:
    """Panele girmiş üye var ama ayarlar allowlist'te değil."""
    em = _normalize_email(member_email)
    return bool(em) and not is_settings_menu_allowed_email(em)


def render_settings_denied_html(
    *,
    member_email: str,
    member_name: str = "",
    requested: bool = False,
    request_error: str = "",
) -> str:
    em = escape(member_email or "—")
    name = escape((member_name or "").strip())
    who = f"{name} ({em})" if name else em
    ok_banner = ""
    if requested:
        ok_banner = (
            '<p class="mt-4 rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm '
            'text-emerald-200">İsteğiniz iletildi. Onay sonrası erişim açılabilir.</p>'
        )
    err_banner = ""
    if request_error:
        err_banner = (
            f'<p class="mt-4 rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm '
            f'text-rose-200">{escape(request_error)}</p>'
        )
    return f"""<!DOCTYPE html>
<html lang="tr" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Yetkiniz yok — Ayarlar</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>body {{ background: radial-gradient(circle at top, #09090b, #111113 55%, #18181b); color: #d4d4d8; }}</style>
</head>
<body class="min-h-screen flex items-center justify-center p-4">
  <div class="w-full max-w-lg p-8 rounded-3xl border border-zinc-800 bg-zinc-900/50 shadow-2xl backdrop-blur-xl">
    <h1 class="text-2xl font-bold text-white text-center">Yetkiniz yok</h1>
    <p class="text-zinc-400 text-sm mt-4 text-center leading-relaxed">
      Ayarlar menüsüne yalnızca yetkili hesaplar erişebilir.<br>
      Oturum: <span class="text-zinc-200 font-medium">{who}</span>
    </p>
    {ok_banner}
    {err_banner}
    <form action="/admin/settings-access-request" method="POST" class="mt-8">
      <button type="submit"
        class="w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-xl shadow-lg shadow-indigo-500/20 transition-all active:scale-95">
        Yetki isteyin
      </button>
    </form>
    <p class="text-xs text-zinc-500 text-center mt-3">
      Bu işlem <span class="text-zinc-400">{escape(SETTINGS_ACCESS_REQUEST_TO)}</span> adresine e-posta gönderir.
    </p>
    <div class="mt-8 text-center">
      <a href="/" class="text-xs text-zinc-600 hover:text-zinc-400">Ana sayfaya dön</a>
    </div>
  </div>
</body>
</html>"""


def _access_request_cooldown_ok(request: Request) -> bool:
    raw = str(request.cookies.get("seo_settings_req_at") or "").strip()
    if not raw.isdigit():
        return True
    try:
        last = int(raw)
    except ValueError:
        return True
    return (time.time() - last) >= _SETTINGS_REQUEST_COOLDOWN_SEC


def send_settings_access_request_email(
    *,
    requester_email: str,
    requester_name: str,
    client_ip: str,
    user_agent: str,
) -> bool:
    from backend.services.mailer import send_email

    em = escape(requester_email or "—")
    nm = escape((requester_name or "").strip() or "—")
    ip = escape(client_ip or "—")
    ua = escape((user_agent or "")[:400])
    panel = escape(
        (getattr(settings, "app_public_host", "") or "projectcontrol.up.railway.app").strip()
    )
    html = f"""
    <p><strong>Ayarlar menüsüne erişim isteği</strong></p>
    <ul>
      <li>E-posta: {em}</li>
      <li>Ad: {nm}</li>
      <li>IP: {ip}</li>
      <li>Panel: {panel}</li>
    </ul>
    <p style="font-size:12px;color:#666">User-Agent: {ua}</p>
    """
    try:
        return bool(
            send_email(
                SETTINGS_ACCESS_REQUEST_SUBJECT,
                html,
                recipients=[SETTINGS_ACCESS_REQUEST_TO],
            )
        )
    except Exception:
        LOGGER.exception("Ayarlar erişim isteği e-postası gönderilemedi")
        return False
