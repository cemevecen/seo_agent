from unittest.mock import patch

from backend.services.llm_provider_chain import (
    agent_provider_try_chain,
    brief_provider_try_chain,
    build_llm_try_chain,
    inbox_provider_try_chain,
)


def test_brief_chain_gemini_primary_includes_groq_failover():
    with patch("backend.services.llm_provider_chain.settings") as s:
        s.gemini_api_key = "g"
        s.groq_api_key = "q"
        s.openai_api_key = "o"
        s.ai_daily_brief_provider = "gemini"
        s.ai_daily_brief_provider_failover = True
        s.ai_daily_brief_gemini_model = "gemini-2.5-flash"
        s.ai_daily_brief_groq_model = "llama-3.3-70b-versatile"
        s.ai_daily_brief_openai_model = "gpt-4.1-mini"
        chain = brief_provider_try_chain(provider_override=None)
        assert chain == [
            ("gemini", "gemini-2.5-flash"),
            ("groq", "llama-3.3-70b-versatile"),
            ("openai", "gpt-4.1-mini"),
        ]


def test_brief_chain_groq_only():
    with patch("backend.services.llm_provider_chain.settings") as s:
        s.gemini_api_key = ""
        s.groq_api_key = "q"
        s.openai_api_key = ""
        s.ai_daily_brief_provider = "gemini"
        s.ai_daily_brief_provider_failover = True
        s.ai_daily_brief_gemini_model = "gemini-2.5-flash"
        s.ai_daily_brief_groq_model = "llama-3.3-70b-versatile"
        s.ai_daily_brief_openai_model = "gpt-4.1-mini"
        chain = brief_provider_try_chain(provider_override=None)
        assert chain == [("groq", "llama-3.3-70b-versatile")]


def test_inbox_chain_groq_first():
    with patch("backend.services.llm_provider_chain.settings") as s:
        s.gemini_api_key = "g"
        s.groq_api_key = "q"
        s.openai_api_key = ""
        s.ai_daily_brief_gemini_model = "gemini-2.5-flash"
        s.ai_daily_brief_groq_model = "llama-3.3-70b-versatile"
        s.inbox_openai_model = "gpt-4.1-mini"
        chain = inbox_provider_try_chain()
        assert chain[0][0] == "groq"


def test_agent_chain_failover_order():
    with patch("backend.services.llm_provider_chain.settings") as s:
        s.gemini_api_key = "g"
        s.groq_api_key = "q"
        s.openai_api_key = "o"
        s.ai_daily_brief_gemini_model = "gemini-2.5-flash"
        s.ai_daily_brief_groq_model = "llama-3.3-70b-versatile"
        s.ai_daily_brief_openai_model = "gpt-4.1-mini"
        chain = agent_provider_try_chain()
        assert [p for p, _ in chain] == ["gemini", "groq", "openai"]


def test_build_try_chain_no_duplicate():
    with patch("backend.services.llm_provider_chain.settings") as s:
        s.gemini_api_key = "g"
        s.groq_api_key = "q"
        s.openai_api_key = ""
        s.ai_daily_brief_gemini_model = "gemini-2.5-flash"
        s.ai_daily_brief_groq_model = "llama-3.3-70b-versatile"
        s.ai_daily_brief_openai_model = "gpt-4.1-mini"
        chain = build_llm_try_chain(primary="groq", failover=True)
        assert chain == [
            ("groq", "llama-3.3-70b-versatile"),
            ("gemini", "gemini-2.5-flash"),
        ]
