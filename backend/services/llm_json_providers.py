"""Groq / Gemini / OpenAI ile tek tur JSON üretimi (gelen kutusu şablonları için hafif modül).

``ai_daily_brief`` üst düzeyde GA4 / warehouse vb. içe aktarır; gelen kutusu yalnızca bu modülü
yükleyerek LLM şablonlarını Railway gibi ortamlarda kırmadan çalıştırabilir.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from backend.config import settings

LOGGER = logging.getLogger(__name__)


def _parse_json_object(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            return json.loads(m.group(0))
        raise


_GEMINI_MAX_PROMPT_CHARS = 300_000
_GROQ_MAX_PROMPT_CHARS = 300_000
_GROQ_FALLBACK_PROMPT_CHARS = 180_000


def _truncate_prompt(prompt: str, max_chars: int) -> str:
    if len(prompt) <= max_chars:
        return prompt
    LOGGER.warning("Prompt %d karakter; %d'e kırpılıyor.", len(prompt), max_chars)
    return prompt[:max_chars]


def _gemini_json(prompt: str, *, model_name: str) -> tuple[dict, tuple[int, int]]:
    prompt = _truncate_prompt(prompt, _GEMINI_MAX_PROMPT_CHARS)
    key = (settings.gemini_api_key or "").strip()
    if not key:
        raise RuntimeError("GEMINI_API_KEY tanımlı değil.")
    try:
        import google.genai as genai  # yeni SDK: google-genai
        client = genai.Client(api_key=key, http_options={"timeout": 300})
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config={"response_mime_type": "application/json", "temperature": 0.35},
        )
        text = response.text or ""
        um = getattr(response, "usage_metadata", None)
        pt = int(getattr(um, "prompt_token_count", None) or 0) if um else 0
        ct = int(getattr(um, "candidates_token_count", None) or 0) if um else 0
    except ImportError:
        # Eski SDK: google-generativeai
        import google.generativeai as genai_old  # type: ignore
        genai_old.configure(api_key=key)
        model_obj = genai_old.GenerativeModel(
            model_name,
            generation_config={"temperature": 0.35, "response_mime_type": "application/json"},
        )
        resp = model_obj.generate_content(prompt, request_options={"timeout": 300})
        text = resp.text or ""
        um = getattr(resp, "usage_metadata", None)
        pt = int(getattr(um, "prompt_token_count", None) or 0) if um else 0
        ct = int(getattr(um, "candidates_token_count", None) or 0) if um else 0
    return _parse_json_object(text), (pt, ct)


def _groq_chat_json(prompt: str, *, model: str) -> tuple[dict, tuple[int, int]]:
    prompt = _truncate_prompt(prompt, _GROQ_MAX_PROMPT_CHARS)
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {(settings.groq_api_key or '').strip()}",
        "Content-Type": "application/json",
    }
    model_candidates: list[str] = []
    for cand in (
        str(model or "").strip(),
        "llama-3.3-70b-versatile",
        "llama3-70b-8192",
        "llama3-8b-8192",
    ):
        if cand and cand not in model_candidates:
            model_candidates.append(cand)
    last_err: Exception | None = None
    with httpx.Client(timeout=180.0) as client:
        for model_name in model_candidates:
            for json_mode in (True, False):
                req_body: dict[str, Any] = {
                    "model": model_name,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.35,
                }
                if json_mode:
                    req_body["response_format"] = {"type": "json_object"}
                try:
                    r = client.post(url, headers=headers, json=req_body)
                    if r.status_code == 413:
                        LOGGER.warning("Groq 413 aldı; prompt %d'e kırpılıyor.", _GROQ_FALLBACK_PROMPT_CHARS)
                        req_body["messages"][0]["content"] = _truncate_prompt(
                            prompt, _GROQ_FALLBACK_PROMPT_CHARS
                        )
                        r = client.post(url, headers=headers, json=req_body)
                    r.raise_for_status()
                    data = r.json()
                    usage = data.get("usage") or {}
                    pt = int(usage.get("prompt_tokens") or 0)
                    ct = int(usage.get("completion_tokens") or 0)
                    if ct <= 0:
                        tt = int(usage.get("total_tokens") or 0)
                        if tt > pt:
                            ct = max(0, tt - pt)
                    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
                    return _parse_json_object(content), (pt, ct)
                except httpx.HTTPStatusError as exc:
                    last_err = exc
                    continue
                except (httpx.TimeoutException, httpx.NetworkError, json.JSONDecodeError) as exc:
                    last_err = exc
                    continue
    raise last_err if last_err else RuntimeError("Groq çağrısı başarısız")


def _openai_chat_json(prompt: str, *, model: str) -> tuple[dict, tuple[int, int]]:
    prompt = _truncate_prompt(prompt, _GROQ_MAX_PROMPT_CHARS)
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {(settings.openai_api_key or '').strip()}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=420.0) as client:
        for json_mode in (True, False):
            req_body: dict[str, Any] = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.35,
            }
            if json_mode:
                req_body["response_format"] = {"type": "json_object"}
            r = client.post(url, headers=headers, json=req_body)
            if r.status_code == 413:
                req_body["messages"][0]["content"] = _truncate_prompt(prompt, _GROQ_FALLBACK_PROMPT_CHARS)
                r = client.post(url, headers=headers, json=req_body)
            try:
                r.raise_for_status()
                data = r.json()
                usage = data.get("usage") or {}
                pt = int(usage.get("prompt_tokens") or 0)
                ct = int(usage.get("completion_tokens") or 0)
                if ct <= 0:
                    tt = int(usage.get("total_tokens") or 0)
                    if tt > pt:
                        ct = max(0, tt - pt)
                content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
                return _parse_json_object(content), (pt, ct)
            except (httpx.HTTPStatusError, json.JSONDecodeError):
                continue
    raise RuntimeError("OpenAI çağrısı başarısız")


def llm_json(prompt: str, *, provider: str, model_name: str) -> tuple[dict, float]:
    """LLM JSON çıktısı ve tahmini TRY (record_llm_call_try ile aynı mantık)."""
    if provider == "groq":
        data, (pt, ct) = _groq_chat_json(prompt, model=model_name)
    elif provider == "gemini":
        data, (pt, ct) = _gemini_json(prompt, model_name=model_name)
    elif provider == "openai":
        data, (pt, ct) = _openai_chat_json(prompt, model=model_name)
    else:
        raise ValueError(f"Bilinmeyen LLM sağlayıcı: {provider}")
    delta_try = 0.0
    if pt or ct:
        try:
            from backend.services.llm_spend import record_llm_call_try

            delta_try = float(
                record_llm_call_try(
                    provider=provider,
                    model=model_name,
                    prompt_tokens=pt,
                    completion_tokens=ct,
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("LLM harcama kaydı yazılamadı: %s", exc)
    return data, delta_try


# ai_daily_brief ile uyumluluk
_llm_json = llm_json
