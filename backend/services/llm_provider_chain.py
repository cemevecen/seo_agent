"""Ortak LLM sağlayıcı zinciri — Gemini sorununda Groq (ve OpenAI) failover."""

from __future__ import annotations

import re

from backend.config import settings

ProviderPair = tuple[str, str]

_FAILOVER_TAIL: tuple[str, ...] = ("groq", "gemini", "openai")


def llm_provider_keys() -> dict[str, bool]:
    return {
        "gemini": bool((settings.gemini_api_key or "").strip()),
        "groq": bool((settings.groq_api_key or "").strip()),
        "openai": bool((settings.openai_api_key or "").strip()),
    }


def llm_provider_models() -> dict[str, str]:
    return {
        "gemini": (settings.ai_daily_brief_gemini_model or "gemini-2.5-flash").strip(),
        "groq": (settings.ai_daily_brief_groq_model or "llama-3.3-70b-versatile").strip(),
        "openai": (settings.ai_daily_brief_openai_model or "gpt-4.1-mini").strip(),
    }


def any_llm_configured() -> bool:
    return any(llm_provider_keys().values())


def build_llm_try_chain(
    *,
    primary: str,
    failover: bool = True,
) -> list[ProviderPair]:
    """primary önce; failover açıksa diğer yapılandırılmış sağlayıcılar (groq → gemini → openai)."""
    p = (primary or "").strip().lower()
    keys = llm_provider_keys()
    models = llm_provider_models()
    chain: list[ProviderPair] = []
    if p in keys and keys[p]:
        chain.append((p, models[p]))
    if not failover:
        return chain
    for name in _FAILOVER_TAIL:
        if name == p:
            continue
        if keys.get(name):
            pair = (name, models[name])
            if pair not in chain:
                chain.append(pair)
    return chain


def brief_primary_provider(provider_override: str | None) -> str:
    ovr = (provider_override or "").strip().lower()
    if ovr in ("groq", "gemini", "openai"):
        return ovr
    pref = (settings.ai_daily_brief_provider or "gemini").strip().lower()
    if pref in ("groq", "gemini", "openai"):
        return pref
    keys = llm_provider_keys()
    for name in ("gemini", "groq", "openai"):
        if keys.get(name):
            return name
    return "gemini"


def brief_provider_try_chain(*, provider_override: str | None) -> list[ProviderPair]:
    primary = brief_primary_provider(provider_override)
    failover = bool(getattr(settings, "ai_daily_brief_provider_failover", True))
    return build_llm_try_chain(primary=primary, failover=failover)


def inbox_provider_try_chain() -> list[ProviderPair]:
    """Gelen kutusu: Groq önce (Gemini kesintisinde minimum etki)."""
    keys = llm_provider_keys()
    models = llm_provider_models()
    oai_model = (settings.inbox_openai_model or models["openai"] or "gpt-4.1-mini").strip()
    order: list[ProviderPair] = []
    for name in ("groq", "gemini", "openai"):
        if keys.get(name):
            m = oai_model if name == "openai" else models[name]
            order.append((name, m))
    return order


def agent_provider_try_chain() -> list[ProviderPair]:
    """Panel ajanı: Gemini varsa önce; hata olursa Groq, sonra OpenAI."""
    keys = llm_provider_keys()
    if keys.get("gemini"):
        primary = "gemini"
    elif keys.get("groq"):
        primary = "groq"
    elif keys.get("openai"):
        primary = "openai"
    else:
        return []
    return build_llm_try_chain(primary=primary, failover=True)


_GEMINI_FAIL_RE = re.compile(
    r"gemini|generativelanguage|api key|403|401|429|quota|permission|restricted|invalid",
    re.I,
)


def looks_like_provider_outage(exc: BaseException) -> bool:
    """Failover tetiklemek için geçici / anahtar / kota hataları."""
    if exc is None:
        return False
    msg = str(exc).strip()
    if not msg:
        return True
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    try:
        import httpx

        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in (401, 403, 429, 500, 502, 503, 504)
    except ImportError:
        pass
    return bool(_GEMINI_FAIL_RE.search(msg))
