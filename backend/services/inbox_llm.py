"""Gelen kutusu: düz metin özet, yanıt taslağı ve çoklu LLM ile yanıt şablonları."""

from __future__ import annotations

import logging
from typing import Any

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


def inbox_llm_any_configured() -> bool:
    """Şablon üretimi için kullanılabilecek en az bir LLM anahtarı var mı?"""
    return bool(
        (settings.openai_api_key or "").strip()
        or (settings.gemini_api_key or "").strip()
        or (settings.groq_api_key or "").strip()
    )


def _inbox_llm_chain() -> list[tuple[str, str]]:
    """Öncelik: OpenAI (gelen kutusu modeli) → Gemini → Groq."""
    out: list[tuple[str, str]] = []
    if (settings.openai_api_key or "").strip():
        out.append(("openai", (settings.inbox_openai_model or "gpt-4.1-mini").strip()))
    if (settings.gemini_api_key or "").strip():
        out.append(("gemini", (settings.ai_daily_brief_gemini_model or "gemini-2.5-flash").strip()))
    if (settings.groq_api_key or "").strip():
        out.append(("groq", (settings.ai_daily_brief_groq_model or "llama-3.3-70b-versatile").strip()))
    return out


def _reply_templates_user_prompt(thread_blob: str) -> str:
    return (
        "Görev: Aşağıdaki e-posta zincirinde, === YANITLANACAK İLETİ === bölümündeki müşteri mesajına "
        "cevap verecek tam 3 farklı Türkçe yanıt taslağı üret.\n\n"
        "Kurallar:\n"
        "- Yalnızca tek bir geçerli JSON nesnesi döndür; kod bloğu veya açıklama yazma.\n"
        '- Şekil: {"templates":[{"label":"kısa etiket","body":"..."},{"label":"...","body":"..."},{"label":"...","body":"..."}]}\n'
        "- Tam 3 öğe; her body yalnızca e-posta gövdesi (konu satırı yok), selamlama ve imza/kapanış dahil.\n"
        "- Stiller belirgin şekilde farklı olsun: (1) resmî ve ayrıntılı (2) kısa ve net (3) empatik ve çözüm odaklı.\n"
        "- döviz.com müşteri desteği tonu; gereksiz vaat verme.\n\n"
        "E-posta bağlamı:\n"
        + _truncate(thread_blob)
    )


def _coerce_three_templates(data: dict[str, Any]) -> list[dict[str, str]]:
    raw = data.get("templates") if isinstance(data, dict) else None
    if not isinstance(raw, list):
        raw = []
    out: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or item.get("etiket") or "").strip() or "Şablon"
        body = str(item.get("body") or item.get("govde") or item.get("metin") or "").strip()
        if body:
            out.append({"label": label[:160], "body": body})
        if len(out) >= 3:
            break
    if not out:
        raise ValueError("LLM yanıtında geçerli şablon yok.")
    while len(out) < 3:
        out.append(
            {
                "label": f"Yedek şablon {len(out) + 1}",
                "body": "[Bu varyant modelden gelmedi; yukarıdaki şablonlardan birini kullanın veya metni kendiniz yazın.]",
            }
        )
    return out[:3]


def reply_templates_three_tr_tr(thread_blob: str) -> tuple[list[dict[str, str]], str]:
    """Üç yanıt şablonu döndürür; (şablonlar, kullanılan_sağlayıcı)."""
    chain = _inbox_llm_chain()
    if not chain:
        raise RuntimeError(
            "Yanıt şablonları için OPENAI_API_KEY, GEMINI_API_KEY veya GROQ_API_KEY tanımlanmalı."
        )
    prompt = _reply_templates_user_prompt(thread_blob)
    last_err: Exception | None = None
    from backend.services.ai_daily_brief import _llm_json

    for provider, model_name in chain:
        try:
            data, _ = _llm_json(prompt, provider=provider, model_name=model_name)
            templates = _coerce_three_templates(data)
            return templates, provider
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("inbox reply templates provider=%s failed: %s", provider, exc)
            last_err = exc
            continue
    raise RuntimeError(str(last_err) if last_err else "LLM şablon üretimi başarısız.")
