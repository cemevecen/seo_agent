"""LLM çağrıları için tahmini harcama (TRY) izleme — aylık tavan ve kota dostu ön kontrol."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.models import LlmSpendMonth

LOGGER = logging.getLogger(__name__)


def _month_key_istanbul() -> str:
    tz = ZoneInfo(settings.ai_daily_brief_timezone or "Europe/Istanbul")
    return datetime.now(tz).strftime("%Y-%m")


def _budget_cap_try() -> float:
    return float(getattr(settings, "llm_spend_budget_try", 0.0) or 0.0)


def _usd_to_try() -> float:
    v = float(getattr(settings, "llm_spend_usd_to_try", 35.0) or 35.0)
    return max(0.01, v)


def _tokens_to_usd(provider: str, prompt_tokens: int, completion_tokens: int) -> float:
    p = (provider or "").strip().lower()
    pt = max(0, int(prompt_tokens))
    ct = max(0, int(completion_tokens))
    if p == "groq":
        inp = float(getattr(settings, "llm_groq_prompt_usd_per_mtok", 0.15) or 0.0)
        out = float(getattr(settings, "llm_groq_completion_usd_per_mtok", 0.60) or 0.0)
    elif p == "gemini":
        inp = float(getattr(settings, "llm_gemini_prompt_usd_per_mtok", 0.075) or 0.0)
        out = float(getattr(settings, "llm_gemini_completion_usd_per_mtok", 0.30) or 0.0)
    else:
        return 0.0
    return (pt / 1_000_000.0) * inp + (ct / 1_000_000.0) * out


def tokens_to_try(provider: str, prompt_tokens: int, completion_tokens: int) -> float:
    return _tokens_to_usd(provider, prompt_tokens, completion_tokens) * _usd_to_try()


def estimate_try_upper_bound(*, provider: str, prompt_text: str, completion_token_cap: int) -> float:
    """Tek çağrı için üst tahmin (bütçe ön kontrolü; gerçek usage genelde daha düşük)."""
    ch = len(prompt_text or "")
    est_in = max(800, int(ch // 3))
    est_out = max(1024, int(completion_token_cap))
    return tokens_to_try(provider, est_in, est_out)


def current_month_spent_try(db: Session) -> float:
    mk = _month_key_istanbul()
    row = db.query(LlmSpendMonth).filter(LlmSpendMonth.month_key == mk).first()
    return float(row.total_try) if row is not None else 0.0


def preflight_month_budget_allows(
    db: Session, *, marginal_try_upper: float, context_label: str = "brief"
) -> bool:
    """Yeni çağrı(lar) öncesi: aylık TRY tavanı aşılmasın (tahmini üst sınır ile)."""
    cap = _budget_cap_try()
    if cap <= 0:
        return True
    spent = current_month_spent_try(db)
    marginal = max(0.0, float(marginal_try_upper))
    if spent + marginal > cap:
        LOGGER.warning(
            "LLM aylık bütçe ön kontrolü reddedildi (%s): harcanan≈%.4f TRY + tahmini_üst≈%.4f TRY > tavan=%.2f TRY (ay=%s)",
            context_label,
            spent,
            marginal,
            cap,
            _month_key_istanbul(),
        )
        return False
    return True


def record_llm_call_try(
    *,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> float:
    """Gerçek (veya API'den gelen) token sayılarıyla harcamayı kaydeder; eklenen TRY'yi döner."""
    delta = tokens_to_try(provider, prompt_tokens, completion_tokens)
    if delta <= 0 and prompt_tokens == 0 and completion_tokens == 0:
        return 0.0
    mk = _month_key_istanbul()
    with SessionLocal() as db:
        row = db.query(LlmSpendMonth).filter(LlmSpendMonth.month_key == mk).first()
        if row is None:
            row = LlmSpendMonth(month_key=mk, total_try=0.0)
            db.add(row)
            db.flush()
        row.total_try = float(row.total_try) + float(delta)
        row.updated_at = datetime.utcnow()
        db.add(row)
        db.commit()
    cap = _budget_cap_try()
    with SessionLocal() as db2:
        total = current_month_spent_try(db2)
    if cap > 0 and total > cap:
        LOGGER.warning(
            "LLM aylık tavan aşıldı (gerçek usage): ay=%s toplam≈%.4f TRY > tavan=%.2f TRY (%s/%s)",
            mk,
            total,
            cap,
            provider,
            model,
        )
    elif cap > 0 and total >= cap * 0.95:
        LOGGER.info(
            "LLM aylık bütçe uyarısı: ay=%s toplam≈%.4f TRY / tavan=%.2f TRY",
            mk,
            total,
            cap,
        )

    LOGGER.debug(
        "LLM harcama kaydı: +%.6f TRY (%s %s, prompt=%s completion=%s)",
        delta,
        provider,
        model,
        prompt_tokens,
        completion_tokens,
    )
    return float(delta)


def estimate_failover_upper_bound_try(
    *,
    try_chain: list[tuple[str, str]],
    context: dict,
    planned_calls_per_attempt: int,
) -> float:
    """Tüm failover zinciri başarısızlıkla tükense bile harcama üst sınırı (güvenli ön kontrol)."""
    raw = json.dumps(context or {}, ensure_ascii=False)
    completion_cap = 16000 if planned_calls_per_attempt <= 1 else 12000
    worst = 0.0
    for prov, _mod in try_chain:
        attempt = 0.0
        for _ in range(max(1, planned_calls_per_attempt)):
            attempt += estimate_try_upper_bound(
                provider=prov, prompt_text=raw, completion_token_cap=completion_cap
            )
        worst += attempt
    return worst


def estimate_single_attempt_upper_bound_try(
    *, provider: str, context: dict, planned_calls_per_attempt: int
) -> float:
    """Tek sağlayıcı denemesi (1 veya 2 LLM çağrısı) için üst tahmin."""
    raw = json.dumps(context or {}, ensure_ascii=False)
    completion_cap = 16000 if planned_calls_per_attempt <= 1 else 12000
    total = 0.0
    for _ in range(max(1, planned_calls_per_attempt)):
        total += estimate_try_upper_bound(
            provider=provider, prompt_text=raw, completion_token_cap=completion_cap
        )
    return total
