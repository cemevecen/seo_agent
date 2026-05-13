"""Gelen kutusu: düz metin özet ve yanıt taslağı (OpenAI)."""

from __future__ import annotations

import logging

import httpx

from backend.config import settings

LOGGER = logging.getLogger(__name__)

_MAX_CHARS = 14_000


def _truncate(s: str, n: int = _MAX_CHARS) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 20] + "\n… [kesildi]"


def openai_plain_text(system: str, user: str, *, model: str | None = None) -> str:
    key = (settings.openai_api_key or "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY tanımlı değil.")
    m = (model or settings.inbox_openai_model or "gpt-4.1-mini").strip()
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    body = {
        "model": m,
        "temperature": 0.35,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    with httpx.Client(timeout=120.0) as client:
        r = client.post(url, headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    return str(content).strip()


def summarize_thread_tr_tr(messages_plain: str) -> str:
    system = (
        "Sen bir müşteri e-postası özetleyicisisin. Çıktıyı Türkçe yaz; madde işaretli kısa özet; "
        "talep, ton ve varsa teknik detayları belirt. Markdown başlık kullanma, düz metin."
    )
    return openai_plain_text(system, _truncate(messages_plain))


def draft_reply_tr_tr(messages_plain: str, *, brand: str = "döviz.com") -> str:
    system = (
        f"Sen {brand} müşteri iletişim temsilcisisin. Aşağıdaki e-posta zincirine profesyonel, "
        "kibar ve çözüm odaklı bir Türkçe yanıt taslağı yaz. Selamlama ve kapanış ekle. "
        "Yalnızca e-posta gövdesini yaz; konu satırı yazma."
    )
    return openai_plain_text(system, _truncate(messages_plain))
