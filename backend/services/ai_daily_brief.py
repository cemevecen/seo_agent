"""Günlük AI özet: GA4, PageSpeed, Search Console ve uyarılar — Türkçe, sabah cron, e-posta.

LLM çağrıları *yalnızca* bu modülde yapılır: zamanlanmış günlük job veya POST /ai/generate.
Kota: İstanbul günü başına üst sınır (ayar) ve eşzamanlı çalışma kilidi.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from datetime import datetime, timedelta
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.models import AiBriefRunLog, AiDailyBriefReport, ExternalSite, Site
from backend.services.alert_engine import get_recent_alerts
from backend.services.email_templates import render_email_shell, section
from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.mailer import send_email
from backend.services.metric_store import get_latest_metrics, get_metric_history
from backend.services.operations_notifier import operations_recipients
from backend.services.warehouse import get_latest_search_console_rows_batch

LOGGER = logging.getLogger(__name__)

_BRIEF_SECTION_KEYS = ("ga4", "pagespeed", "search_console", "alerts")

_BRIEF_RUN_LOGS_SEED_LOCK = threading.Lock()

# LLM: tarama dostu iskelet + stratejik yorum; uygulama profillerinde SEO-organik saçmalığı yasak
_BRIEF_DATA_FIRST_RULES_TR = """
ÜSLUP (ZORUNLU — tüm "metin" alanları):
- Üst düzey değerlendirme: önce iş/trağik anlamı, sonra teknik detay. Metin değerli ve bu projeye özel olsun; boş şablon veya alakasız cümle yasak.
- Editör gibi yaz: net fiil, okunaklı cümleler. Tek satırda onlarca rakam dökmek yasak (RAKAMLAR maddelerine böl).
- Her proje + her başlık (ga4, pagespeed, search_console, alerts) için "metin" AYNI sabit iskeleti kullan; blok başlıkları tam şu şekilde (Türkçe, iki nokta üst üste):

DURUM:
(2–3 cümle: bu başlık altında gidişatın özü + stratejik çerçeve; en az bir somut ölçü JSON’dan.)

RAKAMLAR:
(4–10 madde; her satır "• " ile başlasın.)
GA4: 1g / 7g / 30g için Web ve Mobil web satırlarında oturum etiketi, değişim %, varsa organik pay %.
  • Profil adında Android veya iOS geçen satırlar mobil UYGULAMA oturumudur: yalnızca oturum hacmi ve değişim %; organik pay, organik trafik, arama motoru veya SEO organik dili KULLANMA (anlamsız ve yasak).
  • "Mobil web" ile "Android/iOS uygulama" profillerini birbirine karıştırma.
PageSpeed: güncel mobil/masaüstü, 7g kabaca önceki skor, son ölçümlerde ani düşüş/0 gibi anomali.
Search Console: 1g/7g/30g tıklama, gösterim, CTR, pozisyon; mümkünse önceki dönem kıyısı.
Uyarılar: her madde bir uyarı (tip, sorgu/metrik, yön).

NE ANLAMA GELİYOR:
(En az 6 cümle, tercihen 7–10 cümle: bulguları birlikte oku — güçlü/zayıf yönler, risk, fırsat, birbiriyle tutarlılık; spekülasyonu veriyle sınırla. Genel geçer kurum dili yazma.)

ÖNCELİK:
(2 numaralı ana eylem korunur: "1) …" ve "2) …". Her madde altında birkaç cümle veya net alt adımlar olabilir; toplamda bu blokta en az 6–9 cümle düzeyinde içerik ver — yani önceki tek satırlık maddeleri genişlet.)

OKUNAKLI PARAGRAF YAPISI (ZORUNLU — JSON "metin" içinde kaçışlı satır sonu kullan):
- DURUM: / NE ANLAMA GELİYOR: / ÖNCELİK: başlık satırından sonra gelen gövdeyi uzun tek paragraf halinde yazma.
- Tercih 1 — cümle cümle: Her tamamlanmış cümleden sonra çift satır sonu bırak (\\n\\n); her cümle ayrı görsel paragraf olsun.
- Tercih 2 — konu birimi: Aynı alt konuda 2–3 cümle kalacaksa bunları tek paragraf yap; konu değişince mutlaka \\n\\n ile yeni paragraf başlat. Bir paragrafta dörtten fazla cümle yazma.
- "1) …" ve "2) …" maddeleri: Numaralı satır kendi başına kalsın; açıklayıcı cümleler varsa her cümleden sonra \\n\\n ver veya madde içi ilgili 2–3 cümleyi bir paragrafta tutup sonraki alt adımı yeni paragrafla ayır.
- RAKAMLAR: Başlık altında madde listesi kalır; maddeler tek \\n ile alt alta; listeyi gereksiz \\n\\n ile bölme.

UZUNLUK: Proje başına bu başlık (ga4 / pagespeed / search_console / alerts) için toplam yaklaşık 220–360 kelime; tekrar ve kopya yasak.
SOMUT VERİ: Her blokta JSON’dan gelen alan/sayı; uydurma yok; eksikte "veri setinde görünmüyor" de.
- İlk satır (DURUM:) "izlenmektedir / kaydedilmiştir" ile başlama.

KESİNLİKLE KULLANMA:
- Android veya iOS GA4 profili için "organik pay", "organik trafik", "SEO organik" veya Search Console ile doğrudan bağ kurmak
- "… verileri izlenmektedir / takip edilmektedir" boş kalıpları
- İki siteyi aynı metinde birleştirmek veya diğer projeden örnek uydurmak
"""

_BRIEF_JOB_LOCK = threading.Lock()
_LLM_QUOTA_LOCK = threading.Lock()
_LLM_QUOTA_DATE: str | None = None
_LLM_QUOTA_USED: int = 0


def _istanbul_today_str() -> str:
    tz = ZoneInfo(settings.ai_daily_brief_timezone or "Europe/Istanbul")
    return datetime.now(tz).date().isoformat()


def _external_site_ids(db: Session) -> set[int]:
    return {int(r.site_id) for r in db.query(ExternalSite.site_id).all()}


def _resolve_brief_llm() -> tuple[str, str] | None:
    """(provider, model_id) veya yapılandırma yetersizse None. Başka kod bu anahtarları kullanmaz."""
    groq_k = (settings.groq_api_key or "").strip()
    gem_k = (settings.gemini_api_key or "").strip()
    pref = (settings.ai_daily_brief_provider or "auto").strip().lower()
    gm = (settings.ai_daily_brief_gemini_model or "gemini-2.5-flash").strip()
    gq = (settings.ai_daily_brief_groq_model or "openai/gpt-oss-120b").strip()

    if pref == "gemini":
        return ("gemini", gm) if gem_k else None
    if pref == "groq":
        return ("groq", gq) if groq_k else None
    if gem_k:
        return "gemini", gm
    if groq_k:
        return "groq", gq
    return None


def _resolve_brief_llm_with_override(provider_override: str | None) -> tuple[str, str] | None:
    """Arayüzden gelen groq|gemini seçimini uygular; geçersiz veya boşsa ayarlardaki auto mantığı."""
    o = (provider_override or "").strip().lower()
    if o not in ("groq", "gemini"):
        return _resolve_brief_llm()
    groq_k = (settings.groq_api_key or "").strip()
    gem_k = (settings.gemini_api_key or "").strip()
    gm = (settings.ai_daily_brief_gemini_model or "gemini-2.5-flash").strip()
    gq = (settings.ai_daily_brief_groq_model or "openai/gpt-oss-120b").strip()
    if o == "groq":
        return ("groq", gq) if groq_k else None
    return ("gemini", gm) if gem_k else None


def brief_provider_try_chain(*, provider_override: str | None) -> list[tuple[str, str]]:
    """Özet üretimi için sıra: önce tercih, sonra (failover açıksa) diğer sağlayıcı.

    Zamanlanmış iş `provider_override=None` ile çağrılır; `AI_DAILY_BRIEF_PROVIDER` ve failover buna göre sıralar.
    """
    groq_k = bool((settings.groq_api_key or "").strip())
    gem_k = bool((settings.gemini_api_key or "").strip())
    if not groq_k and not gem_k:
        return []
    gq = (settings.ai_daily_brief_groq_model or "openai/gpt-oss-120b").strip()
    gm = (settings.ai_daily_brief_gemini_model or "gemini-2.5-flash").strip()
    failover = bool(getattr(settings, "ai_daily_brief_provider_failover", True))

    order_slug: list[str] = []
    ovr = (provider_override or "").strip().lower()
    if ovr == "groq":
        order_slug = ["groq"] + (["gemini"] if failover else [])
    elif ovr == "gemini":
        order_slug = ["gemini"] + (["groq"] if failover else [])
    else:
        pref = (settings.ai_daily_brief_provider or "auto").strip().lower()
        if pref == "groq":
            order_slug = ["groq"] + (["gemini"] if failover else [])
        elif pref == "gemini":
            order_slug = ["gemini"] + (["groq"] if failover else [])
        else:
            # auto: ücretsiz Gemini öncelikli; olmazsa veya failover ile Groq.
            if failover:
                order_slug = ["gemini", "groq"]
            else:
                order_slug = ["gemini"] if gem_k else ["groq"]

    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for p in order_slug:
        if p in seen:
            continue
        if p == "groq" and groq_k:
            out.append(("groq", gq))
            seen.add("groq")
        elif p == "gemini" and gem_k:
            out.append(("gemini", gm))
            seen.add("gemini")
    return out


def _try_reserve_llm_calls(n: int) -> bool:
    global _LLM_QUOTA_DATE, _LLM_QUOTA_USED
    day = _istanbul_today_str()
    cap = max(1, int(settings.ai_daily_brief_max_llm_calls_per_calendar_day))
    n = max(0, int(n))
    if n == 0:
        return True
    with _LLM_QUOTA_LOCK:
        if _LLM_QUOTA_DATE != day:
            _LLM_QUOTA_DATE = day
            _LLM_QUOTA_USED = 0
        if _LLM_QUOTA_USED + n > cap:
            LOGGER.warning(
                "AI günlük LLM kota sınırı: kullanılan=%s istenen=%s üst_sınır=%s (tarih=%s)",
                _LLM_QUOTA_USED,
                n,
                cap,
                day,
            )
            return False
        _LLM_QUOTA_USED += n
        return True


def _refund_llm_calls(n: int) -> None:
    global _LLM_QUOTA_USED
    n = max(0, int(n))
    if n == 0:
        return
    with _LLM_QUOTA_LOCK:
        _LLM_QUOTA_USED = max(0, _LLM_QUOTA_USED - n)


def _parse_hist_ts(iso_s: str) -> datetime:
    s = (iso_s or "").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.utcnow()


def _metric_near_days_ago(series: list[dict], days_ago: int) -> float | None:
    if not series:
        return None
    target = datetime.utcnow() - timedelta(days=days_ago)
    best: float | None = None
    best_ts: datetime | None = None
    for pt in series:
        ts = _parse_hist_ts(str(pt.get("collected_at") or ""))
        if ts <= target and (best_ts is None or ts >= best_ts):
            best_ts = ts
            try:
                best = float(pt.get("value"))
            except (TypeError, ValueError):
                best = None
    return best


def _series_tail(series: list[dict], *, max_points: int = 10) -> list[dict]:
    if not series:
        return []
    tail = series[-max_points:]
    out: list[dict] = []
    for p in tail:
        try:
            out.append({"deger": float(p.get("value")), "tarih": str(p.get("collected_at") or "")[:10]})
        except (TypeError, ValueError):
            continue
    return out


def _ga4_hucre_ozet(layout: dict) -> list[dict]:
    cells: list[dict] = []
    for col in ("col_left", "col_right"):
        for block in layout.get(col) or []:
            cells.append(
                {
                    "profil": block.get("label"),
                    "oturum_etiket": block.get("sessions_display"),
                    "oturum_degisim_pct": block.get("sessions_change"),
                    "organik_pay": block.get("organic_display"),
                    "veri_var": bool(block.get("has_data")),
                }
            )
    return cells


def _ga4_cells_scrub_app_organic(cells: list[dict]) -> list[dict]:
    """Android/iOS GA4 stream'lerinde organik pay SEO için anlamsız; LLM girdisinden düşür."""
    out: list[dict] = []
    for c in cells:
        row = dict(c)
        prof = str(row.get("profil") or "").strip().lower()
        if prof in ("android", "ios") or "android" in prof or "ios" in prof:
            row["organik_pay"] = None
            row["profil_aciklama"] = (
                "Mobil uygulama (app) akışı — organik arama payı veya GSC ile karşılaştırma yazılmaz."
            )
        out.append(row)
    return out


def gather_ai_brief_context(db: Session) -> dict:
    """Dashboard + 1/7/30 gün kırılımları; LLM girdisi."""
    from backend.main import (
        _build_search_console_top_queries,
        _dashboard_ga4_layout,
        _normalize_dashboard_platform,
        _preferred_site_order_key,
        _search_console_status,
        _summarize_search_console_rows,
    )

    ext = _external_site_ids(db)
    sites = (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.desc())
        .all()
    )
    sites = [s for s in sites if s.id not in ext]
    sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
    recent_alerts = get_recent_alerts(db, limit=50, include_external=False)
    sc_scopes = ["current_day", "current_7d", "previous_7d", "current_30d", "previous_30d"]
    site_payloads: list[dict] = []
    for site in sites:
        latest_list = get_latest_metrics(db, site.id)
        latest = {m.metric_type: m for m in latest_list}
        latest_floats = {k: float(v.value) for k, v in latest.items()}
        mob = latest_floats.get("pagespeed_mobile_score")
        desk = latest_floats.get("pagespeed_desktop_score")
        sc_status = _search_console_status(db, latest, site.id)
        sc_batch = get_latest_search_console_rows_batch(db, site_id=site.id, scopes=sc_scopes)
        cur7 = sc_batch.get("current_7d") or []
        prev7 = sc_batch.get("previous_7d") or []
        cur30 = sc_batch.get("current_30d") or []
        prev30 = sc_batch.get("previous_30d") or []
        cur1 = sc_batch.get("current_day") or []
        top_q = _build_search_console_top_queries(cur7, prev7, limit=12)
        ga4_conn = get_ga4_connection_status(db, site.id)
        platform = _normalize_dashboard_platform("web")
        ga4_by_gun: dict[str, list[dict]] = {}
        for gun, pd in (("1", 1), ("7", 7), ("30", 30)):
            layout = _dashboard_ga4_layout(db, site, platform, latest_floats, ga4_conn, period_days=pd)
            ga4_by_gun[gun] = _ga4_cells_scrub_app_organic(_ga4_hucre_ozet(layout))
        hist = get_metric_history(db, site.id, days=35)
        mob_hist = hist.get("pagespeed_mobile_score") or []
        desk_hist = hist.get("pagespeed_desktop_score") or []
        site_alerts = [a for a in recent_alerts if str(a.get("domain") or "").lower() == (site.domain or "").lower()]
        sum1 = _summarize_search_console_rows(cur1)
        sum7 = _summarize_search_console_rows(cur7)
        sum30 = _summarize_search_console_rows(cur30)
        sum_prev7 = _summarize_search_console_rows(prev7)
        sum_prev30 = _summarize_search_console_rows(prev30)
        site_payloads.append(
            {
                "alan_adi": site.display_name,
                "domain": site.domain,
                "pagespeed": {
                    "mobil_guncel": int(mob) if mob is not None else None,
                    "masaustu_guncel": int(desk) if desk is not None else None,
                    "mobil_yaklasik_7g_once": (
                        int(x) if (x := _metric_near_days_ago(mob_hist, 7)) is not None else None
                    ),
                    "mobil_yaklasik_30g_once": (
                        int(x) if (x := _metric_near_days_ago(mob_hist, 30)) is not None else None
                    ),
                    "masaustu_yaklasik_7g_once": (
                        int(x) if (x := _metric_near_days_ago(desk_hist, 7)) is not None else None
                    ),
                    "masaustu_yaklasik_30g_once": (
                        int(x) if (x := _metric_near_days_ago(desk_hist, 30)) is not None else None
                    ),
                    "mobil_son_olcumler": _series_tail(mob_hist, max_points=8),
                    "masaustu_son_olcumler": _series_tail(desk_hist, max_points=8),
                },
                "search_console_durum": str(sc_status.get("label") or sc_status.get("state") or ""),
                "search_console_ozet": {
                    "gun_1": {k: round(v, 2) if isinstance(v, float) else v for k, v in sum1.items()},
                    "gun_7": {k: round(v, 2) if isinstance(v, float) else v for k, v in sum7.items()},
                    "gun_30": {k: round(v, 2) if isinstance(v, float) else v for k, v in sum30.items()},
                    "onceki_7g": {k: round(v, 2) if isinstance(v, float) else v for k, v in sum_prev7.items()},
                    "onceki_30g": {k: round(v, 2) if isinstance(v, float) else v for k, v in sum_prev30.items()},
                },
                "one_cikan_sorgular_7g": [
                    {
                        "sorgu": r.get("query"),
                        "tiklamalar_guncel": round(float(r.get("clicks_current") or 0), 1),
                        "tiklamalar_fark": round(float(r.get("clicks_diff") or 0), 1),
                    }
                    for r in top_q[:10]
                ],
                "ga4_hucreler_1_7_30_gun": ga4_by_gun,
                "bu_projeye_ozel_uyarilar": [
                    {
                        "baslik": a.get("display_title"),
                        "sorgu": a.get("display_query"),
                        "metrik": a.get("display_metric"),
                        "tip": a.get("alert_type"),
                        "zaman": a.get("triggered_at"),
                    }
                    for a in site_alerts[:20]
                ],
            }
        )
    return {
        "tarih": _istanbul_today_str(),
        "saat_dilimi": settings.ai_daily_brief_timezone,
        "not": (
            "GA4: Android/iOS profilleri uygulama oturumudur — organik arama/yüzde organik anlatma. "
            "Organik pay yalnızca Web ve Mobil web satırlarında. "
            "Üst düzey (stratejik) yorum + somut rakam; alakasız doldurma yok."
        ),
        "siteler": site_payloads,
        "tum_son_uyarilar": [
            {
                "domain": a.get("domain"),
                "baslik": a.get("display_title"),
                "sorgu": a.get("display_query"),
                "metrik": a.get("display_metric"),
                "tip": a.get("alert_type"),
                "zaman": a.get("triggered_at"),
            }
            for a in recent_alerts[:30]
        ],
    }


def _parse_json_object(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            return json.loads(m.group(0))
        raise


def _gemini_json(prompt: str, *, model_name: str) -> tuple[dict, tuple[int, int]]:
    import google.generativeai as genai

    genai.configure(api_key=(settings.gemini_api_key or "").strip())
    model = genai.GenerativeModel(
        model_name,
        generation_config={
            "temperature": 0.35,
            "response_mime_type": "application/json",
        },
    )
    resp = model.generate_content(prompt)
    um = getattr(resp, "usage_metadata", None)
    pt = int(getattr(um, "prompt_token_count", None) or 0) if um is not None else 0
    ct = int(getattr(um, "candidates_token_count", None) or 0) if um is not None else 0
    if um is not None and ct <= 0:
        tot = getattr(um, "total_token_count", None)
        if tot is not None:
            ct = max(0, int(tot) - pt)
    return _parse_json_object(resp.text or ""), (pt, ct)


def _groq_chat_json(prompt: str, *, model: str) -> tuple[dict, tuple[int, int]]:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {(settings.groq_api_key or '').strip()}",
        "Content-Type": "application/json",
    }
    last_err: Exception | None = None
    with httpx.Client(timeout=420.0) as client:
        for json_mode in (True, False):
            body: dict = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.35,
            }
            if json_mode:
                body["response_format"] = {"type": "json_object"}
            try:
                r = client.post(url, headers=headers, json=body)
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
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                continue
    raise last_err if last_err else RuntimeError("Groq çağrısı başarısız")


def _llm_json(prompt: str, *, provider: str, model_name: str) -> tuple[dict, float]:
    """LLM yanıtı ve bu çağrı için tahmini TRY (record_llm_call_try ile aynı mantık)."""
    if provider == "groq":
        data, (pt, ct) = _groq_chat_json(prompt, model=model_name)
    elif provider == "gemini":
        data, (pt, ct) = _gemini_json(prompt, model_name=model_name)
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


def _site_order_from_context(context: dict) -> list[tuple[str, str]]:
    """(domain_lower, baslik) sırası — bağlamdaki siteler ile aynı düzen."""
    out: list[tuple[str, str]] = []
    for s in context.get("siteler") or []:
        if not isinstance(s, dict):
            continue
        dom = str(s.get("domain") or "").strip().lower()
        if not dom:
            continue
        title = str(s.get("alan_adi") or s.get("domain") or dom).strip()
        out.append((dom, title))
    return out


def _coerce_site_blocks(val: object, context: dict) -> list[dict[str, str]]:
    """LLM çıktısını proje başına blok listesine çevir; eksik projeleri doldur."""
    order = _site_order_from_context(context)
    placeholder = "Bu proje için özet üretilemedi; veriyi kontrol edin."
    by_domain: dict[str, dict[str, str]] = {}
    if isinstance(val, list):
        for item in val:
            if not isinstance(item, dict):
                continue
            dom = str(item.get("domain") or "").strip().lower()
            if not dom:
                continue
            title = str(item.get("baslik") or "").strip()
            metin = str(item.get("metin") or "").strip()
            if not title:
                title = next((t for d, t in order if d == dom), dom)
            by_domain[dom] = {"domain": dom, "baslik": title, "metin": metin or placeholder}
    result: list[dict[str, str]] = []
    seen_order = {d for d, _ in order}
    for dom, title in order:
        if dom in by_domain:
            result.append(by_domain[dom])
        else:
            result.append({"domain": dom, "baslik": title, "metin": placeholder})
    for dom, blk in by_domain.items():
        if dom not in seen_order:
            result.append(blk)
    return result


def _finalize_brief_payload(data: dict, context: dict) -> dict[str, str]:
    """Her sütunu proje dizisi JSON metni olarak sakla."""
    out: dict[str, str] = {}
    for k in _BRIEF_SECTION_KEYS:
        blocks = _coerce_site_blocks(data.get(k), context)
        out[k] = json.dumps(blocks, ensure_ascii=False)
    return out


def parse_stored_brief_section_for_ui(text: str | None) -> dict:
    """Jinja / şablon: site bazlı veya eski düz metin."""
    t = (text or "").strip()
    if not t:
        return {"mode": "empty", "items": [], "text": ""}
    if t.startswith("["):
        try:
            data = json.loads(t)
            if isinstance(data, list):
                items: list[dict[str, str]] = []
                for x in data:
                    if isinstance(x, dict):
                        items.append(
                            {
                                "domain": str(x.get("domain") or ""),
                                "baslik": str(x.get("baslik") or x.get("domain") or "Proje"),
                                "metin": str(x.get("metin") or ""),
                            }
                        )
                return {"mode": "sites", "items": items, "text": ""}
        except json.JSONDecodeError:
            pass
    return {"mode": "legacy", "items": [], "text": t}


def generate_brief_sections(
    context: dict, *, provider: str, model_name: str
) -> tuple[dict[str, str], float]:
    ctx_json = json.dumps(context, ensure_ascii=False, indent=2)
    prompt = f"""Sen kıdemli bir Türkçe SEO ve analitik danışmanısın. Aşağıdaki JSON verisi gerçek izleme özetidir.

{ctx_json}
{_BRIEF_DATA_FIRST_RULES_TR}
YAPILANDIRMA: Girdi JSON içindeki "siteler" dizisindeki her proje için ayrı değerlendirme yaz. Siteleri asla tek metinde birleştirme. "domain" birebir (küçük harf). "baslik" = "alan_adi".

GÖREV: Dört ana başlık (ga4, pagespeed, search_console, alerts) için her projede "metin" alanı yukarıdaki ZORUNLU iskeleti aynen kullanmalı: DURUM / RAKAMLAR / NE ANLAMA GELİYOR / ÖNCELİK (büyük harf ve iki nokta üst üste ile).
- Bloklar arasında boş satır bırak (\\\\n\\\\n).
- alerts başlığında RAKAMLAR maddeleri uyarı kayıtları; uyarı yoksa DURUM’da açıkça belirt.

Çıktı YALNIZCA şu yapıda TEK JSON nesnesi (başka metin yok):
{{
  "ga4": [
    {{"domain": "ornek.com", "baslik": "Görünen ad", "metin": "Düz metin, paragraflar \\\\n\\\\n ile"}}
  ],
  "pagespeed": [ {{"domain": "...", "baslik": "...", "metin": "..."}} ],
  "search_console": [ {{"domain": "...", "baslik": "...", "metin": "..."}} ],
  "alerts": [ {{"domain": "...", "baslik": "...", "metin": "..."}} ]
}}

Kurallar:
- Markdown yok; UTF-8 Türkçe (ı, ş, ğ, ü, ö, ç, İ).
- Veride olmayan metrik uydurma; "veri setinde görünmüyor" de.
- Her dizi, girdideki sitelerle aynı projeleri içersin (sıra aynı olsun).
- ASCII kısaltmaları koru (GA4, CTR, URL).
"""
    data, delta_try = _llm_json(prompt, provider=provider, model_name=model_name)
    return _finalize_brief_payload(data, context), delta_try


def generate_brief_single_pass(
    context: dict, *, provider: str, model_name: str
) -> tuple[dict[str, str], bool, str, float]:
    """Tek LLM çağrısı: dört bölüm (proje bazlı) + Türkçe öz değerlendirme (tamam)."""
    ctx_json = json.dumps(context, ensure_ascii=False, indent=2)
    prompt = f"""Sen kıdemli bir Türkçe SEO ve analitik danışmanısın. Aşağıdaki JSON verisi gerçek izleme özetidir.

{ctx_json}
{_BRIEF_DATA_FIRST_RULES_TR}
YAPILANDIRMA: "siteler" içindeki her proje için ayrı metin üret; projeleri tek paragrafta birleştirme. "domain" birebir eşleşsin. "baslik" = "alan_adi".

Her projede ve her başlıkta (ga4, pagespeed, search_console, alerts) "metin" şu dört bloğu bu sırayla ve bu etiketlerle içermeli: DURUM: / RAKAMLAR: / NE ANLAMA GELİYOR: / ÖNCELİK:
- Bloklar arasında boş satır (\\n\\n). RAKAMLAR’da madde işareti satırları kullan.
- alerts: bu_projeye_ozel_uyarilar somut; boşsa DURUM’da yaz.

Çıktı YALNIZCA şu alanlara sahip TEK bir JSON nesnesi olsun:
- "ga4", "pagespeed", "search_console", "alerts": her biri [{{"domain","baslik","metin"}}] dizisi (girdideki tüm siteler, aynı sıra).
- "tamam": boolean — Türkçe, iskelet doğru ve metinler şablon dolgu değilse true.

Kurallar:
- Markdown yok; metin içinde \\n\\n ile paragraflar.
- Uydurma veri yok; eksikte "veri setinde görünmüyor" de.
- ASCII kısaltmaları koru (GA4, CTR, URL).
"""
    data, delta_try = _llm_json(prompt, provider=provider, model_name=model_name)
    ok = bool(data.get("tamam", True))
    detail = "single_pass_self_qc_ok" if ok else "single_pass_self_qc_flagged"
    return _finalize_brief_payload(data, context), ok, detail, delta_try


def verify_turkish_batch(
    sections: dict[str, str], *, provider: str, model_name: str, context: dict
) -> tuple[dict[str, str], bool, str, float]:
    """İkinci geçiş: proje bloklarında yalnızca metin alanlarını Türkçe açısından düzelt."""
    structured: dict[str, object] = {}
    for k in _BRIEF_SECTION_KEYS:
        raw = sections.get(k) or ""
        try:
            if raw.strip().startswith("["):
                structured[k] = json.loads(raw)
            else:
                structured[k] = raw
        except json.JSONDecodeError:
            structured[k] = raw
    payload = json.dumps(structured, ensure_ascii=False, indent=2)
    prompt = f"""Aşağıdaki JSON, SEO günlük özetidir. Yapı şöyle: her anahtar (ga4, pagespeed, search_console, alerts) bir dizi; her öğe {{"domain","baslik","metin"}}.

GÖREV: Yalnızca "metin" alanlarını Türkçe yazım ve noktalama açısından düzelt (TDK yaklaşımı). domain ve baslik alanlarını aynen koru. DURUM:/RAKAMLAR:/NE ANLAMA GELİYOR:/ÖNCELİK: iskeletini ve tüm rakamları koru; içeriği kısaltma veya jenerik cümlelerle değiştirme.
Paragraf yapısını koru: metindeki \\n\\n ile ayrılmış bölümleri tek paragrafa sıkıştırma; gerekirse anlam birimlerini yine \\n\\n ile ayırılmış tut.
Boş ifadeleri ("izlenmektedir" vb.) anlamlı özgül cümlelerle değiştirmek yalnızca yazım amaçlı değilse yapma.

Çıktı: Aynı yapıda JSON — "ga4","pagespeed","search_console","alerts" dizileri + "tamam" (boolean).

GİRDİ:
{payload}
"""
    try:
        data, delta_try = _llm_json(prompt, provider=provider, model_name=model_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("AI Turkish QC JSON parse failed: %s", exc)
        return sections, False, str(exc), 0.0
    ok = bool(data.get("tamam", True))
    out = _finalize_brief_payload(data, context)
    for k in _BRIEF_SECTION_KEYS:
        if not (data.get(k)) and sections.get(k):
            out[k] = sections[k]
    detail = "model_qc_ok" if ok else "model_qc_flagged"
    return out, ok, detail, delta_try


def _paragraphs_to_html(text: str, *, color: str = "#1e293b") -> str:
    parts = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    if not parts:
        return f'<p style="margin:0 0 12px 0;font-size:14px;line-height:1.75;color:{color};">{escape("(Boş)")}</p>'
    html = ""
    for p in parts:
        html += (
            f'<p style="margin:0 0 12px 0;font-size:14px;line-height:1.75;color:{color};">{escape(p)}</p>'
        )
    return html


def _email_html_for_stored_section(raw: str) -> str:
    parsed = parse_stored_brief_section_for_ui(raw)
    if parsed["mode"] == "sites" and parsed.get("items"):
        parts: list[str] = []
        for it in parsed["items"]:
            title = escape(str(it.get("baslik") or it.get("domain") or "Proje"))
            dom = escape(str(it.get("domain") or ""))
            body = _paragraphs_to_html(str(it.get("metin") or ""), color="#334155")
            parts.append(
                '<div style="margin:0 0 20px 0;padding:12px 14px;border:1px solid #e2e8f0;border-radius:10px;background:#f8fafc;">'
                f'<p style="margin:0 0 4px 0;font-size:13px;font-weight:700;color:#0f172a;">{title}</p>'
                f'<p style="margin:0 0 10px 0;font-size:11px;color:#64748b;">{dom}</p>'
                f"{body}</div>"
            )
        return "".join(parts)
    return _paragraphs_to_html(parsed.get("text") or raw or "", color="#334155")


def build_brief_email_html(
    brief_date: str,
    sections: dict[str, str],
    *,
    qc_ok: bool,
    qc_detail: str,
) -> str:
    titles = {
        "ga4": "GA4 değerlendirmesi",
        "pagespeed": "PageSpeed skorları",
        "search_console": "Search Console",
        "alerts": "Uyarılar ve öncelik",
    }
    blocks = [
        section(titles[k], _email_html_for_stored_section(sections.get(k, "")), subtitle="")
        for k in _BRIEF_SECTION_KEYS
    ]
    intro = (
        f"Tarih (Türkiye): {brief_date}. "
        f"Otomatik özet; Türkçe kontrol: {'tamam' if qc_ok else 'manuel göz önerilir'} ({qc_detail})."
    )
    return render_email_shell(
        eyebrow="SEO Agent · AI günlük özet",
        title="Günlük AI strateji özeti",
        intro=intro,
        tone="slate",
        status_label="AI ÖZET",
        sections=blocks,
    )


def run_ai_daily_brief_job(*, force: bool = False, provider_override: str | None = None) -> None:
    if not settings.ai_daily_brief_enabled:
        LOGGER.info("AI daily brief disabled via settings.")
        return

    if not _BRIEF_JOB_LOCK.acquire(blocking=False):
        LOGGER.info("AI günlük özet zaten çalışıyor; çağrı atlandı (eşzamanlılık koruması).")
        return
    try:
        _run_ai_daily_brief_job_impl(force=force, provider_override=provider_override)
    finally:
        _BRIEF_JOB_LOCK.release()


def _run_ai_daily_brief_job_impl(*, force: bool = False, provider_override: str | None = None) -> None:
    try_chain = brief_provider_try_chain(provider_override=provider_override)
    if not try_chain:
        LOGGER.warning("AI günlük özet atlandı: GROQ_API_KEY veya GEMINI_API_KEY yapılandırılmadı.")
        return

    single = bool(settings.ai_daily_brief_single_llm_call)
    planned_calls = 1 if single else 2

    brief_date = _istanbul_today_str()
    label_stored = ""

    with SessionLocal() as db:
        existing = db.query(AiDailyBriefReport).filter(AiDailyBriefReport.brief_date == brief_date).first()
        if (
            existing
            and not force
            and existing.email_sent_at is not None
            and len((existing.ga4_text or "").strip()) > 50
        ):
            LOGGER.info("AI brief for %s already emailed, skip.", brief_date)
            return
        if (
            existing
            and not force
            and len((existing.ga4_text or "").strip()) > 50
            and existing.email_sent_at is None
        ):
            recap = {
                "ga4": existing.ga4_text,
                "pagespeed": existing.pagespeed_text,
                "search_console": existing.search_console_text,
                "alerts": existing.alerts_text,
            }
            recipients = operations_recipients()
            subject = f"SEO Agent · AI günlük özet · {brief_date}"
            html = build_brief_email_html(
                brief_date,
                recap,
                qc_ok=bool(existing.turkish_qc_ok),
                qc_detail=existing.qc_detail or "",
            )
            if send_email(subject, html, recipients=recipients):
                existing.email_sent_at = datetime.utcnow()
                db.add(existing)
                db.commit()
            return
        try:
            context = gather_ai_brief_context(db)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("AI brief context failed: %s", exc)
            return

        from backend.services.llm_spend import (
            estimate_failover_upper_bound_try,
            estimate_single_attempt_upper_bound_try,
            preflight_month_budget_allows,
        )

        worst_try = estimate_failover_upper_bound_try(
            try_chain=try_chain,
            context=context,
            planned_calls_per_attempt=planned_calls,
        )
        if not preflight_month_budget_allows(
            db, marginal_try_upper=worst_try, context_label="ai-brief-failover-worst"
        ):
            return

        final: dict[str, str] | None = None
        qc_ok = False
        qc_detail = ""
        run_try_delta = 0.0

        for provider, model_name in try_chain:
            marginal_one = estimate_single_attempt_upper_bound_try(
                provider=provider,
                context=context,
                planned_calls_per_attempt=planned_calls,
            )
            if not preflight_month_budget_allows(
                db, marginal_try_upper=marginal_one, context_label=f"ai-brief-{provider}-attempt"
            ):
                continue
            if not _try_reserve_llm_calls(planned_calls):
                LOGGER.warning(
                    "AI günlük özet kota sınırı: %s denemesi atlandı (günlük LLM üst sınırı).",
                    provider,
                )
                break
            try:
                if single:
                    final, qc_ok, qc_detail, run_try_delta = generate_brief_single_pass(
                        context, provider=provider, model_name=model_name
                    )
                else:
                    draft, d_try = generate_brief_sections(
                        context, provider=provider, model_name=model_name
                    )
                    final, qc_ok, qc_detail, qc_try = verify_turkish_batch(
                        draft, provider=provider, model_name=model_name, context=context
                    )
                    run_try_delta = float(d_try) + float(qc_try)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "AI özet üretimi %s (%s) ile başarısız: %s",
                    provider,
                    model_name,
                    exc,
                )
                _refund_llm_calls(planned_calls)
                final = None
                continue

            label_stored = f"{provider}:{model_name}"[:80]
            LOGGER.info("AI günlük özet üretildi: sağlayıcı=%s model=%s", provider, model_name)
            break

        if final is None:
            if try_chain:
                LOGGER.error(
                    "AI günlük özet tüm sağlayıcılarla başarısız (denenen: %s).",
                    [p for p, _ in try_chain],
                )
            return

        row = existing or AiDailyBriefReport(brief_date=brief_date)
        row.ga4_text = final.get("ga4", "")
        row.pagespeed_text = final.get("pagespeed", "")
        row.search_console_text = final.get("search_console", "")
        row.alerts_text = final.get("alerts", "")
        row.turkish_qc_ok = qc_ok
        row.qc_detail = qc_detail[:2000]
        row.model_name = label_stored
        db.add(row)
        db.commit()
        try:
            db.add(
                AiBriefRunLog(
                    day_key=brief_date,
                    model_name=label_stored[:80],
                    source="manual" if force else "scheduled",
                    brief_date=brief_date,
                    approx_try=float(run_try_delta),
                )
            )
            db.commit()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("ai_brief_run_logs kaydı yazılamadı: %s", exc)
            db.rollback()
        recipients = operations_recipients()
        subject = f"SEO Agent · AI günlük özet · {brief_date}"
        html = build_brief_email_html(brief_date, final, qc_ok=qc_ok, qc_detail=qc_detail)
        if send_email(subject, html, recipients=recipients):
            row2 = db.query(AiDailyBriefReport).filter(AiDailyBriefReport.brief_date == brief_date).first()
            if row2:
                row2.email_sent_at = datetime.utcnow()
                db.add(row2)
                db.commit()
        else:
            LOGGER.warning("AI brief email not sent (SMTP or recipients).")


def get_latest_brief_for_ui(db: Session) -> AiDailyBriefReport | None:
    return db.query(AiDailyBriefReport).order_by(AiDailyBriefReport.brief_date.desc()).first()


def _seed_brief_run_logs_from_reports_if_empty(db: Session) -> None:
    """Log tablosu boşsa geçmiş ai_daily_brief_reports satırlarından tek seferlik içe aktarır."""
    n = int(db.query(func.count(AiBriefRunLog.id)).scalar() or 0)
    if n > 0:
        return
    with _BRIEF_RUN_LOGS_SEED_LOCK:
        n2 = int(db.query(func.count(AiBriefRunLog.id)).scalar() or 0)
        if n2 > 0:
            return
        reps = (
            db.query(AiDailyBriefReport)
            .filter(func.length(AiDailyBriefReport.ga4_text) > 50)
            .order_by(AiDailyBriefReport.brief_date.asc())
            .all()
        )
        for r in reps:
            mn = (r.model_name or "").strip()[:80] or "kayıtta yok"
            db.add(
                AiBriefRunLog(
                    created_at=r.created_at,
                    day_key=r.brief_date,
                    model_name=mn,
                    source="scheduled",
                    brief_date=r.brief_date,
                )
            )
        try:
            db.commit()
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("ai_brief_run_logs tohumlaması başarısız: %s", exc)


def get_ai_brief_run_stats(db: Session, *, window_days: int = 120) -> dict[str, Any]:
    """AI sayfası: toplam çalıştırma ve İstanbul gününe göre model kırılımı."""
    _seed_brief_run_logs_from_reports_if_empty(db)
    total_all = int(db.query(func.count(AiBriefRunLog.id)).scalar() or 0)
    tz = ZoneInfo(settings.ai_daily_brief_timezone or "Europe/Istanbul")
    cutoff = (datetime.now(tz).date() - timedelta(days=max(7, int(window_days)))).isoformat()
    logs = (
        db.query(AiBriefRunLog)
        .filter(AiBriefRunLog.day_key >= cutoff)
        .order_by(AiBriefRunLog.day_key.desc(), AiBriefRunLog.id.desc())
        .all()
    )
    by_day: dict[str, dict[str, Any]] = {}
    for log in logs:
        dk = log.day_key
        row_try = float(getattr(log, "approx_try", 0) or 0)
        if dk not in by_day:
            by_day[dk] = {"day_key": dk, "total": 0, "models": {}, "try_sum": 0.0}
        b = by_day[dk]
        b["total"] += 1
        b["try_sum"] = float(b.get("try_sum", 0.0)) + row_try
        mn = (log.model_name or "").strip() or "—"
        if mn not in b["models"]:
            b["models"][mn] = {"model": mn, "count": 0, "scheduled": 0, "manual": 0, "approx_try": 0.0}
        e = b["models"][mn]
        e["count"] += 1
        e["approx_try"] = float(e.get("approx_try", 0.0)) + row_try
        if (log.source or "").strip().lower() == "manual":
            e["manual"] += 1
        else:
            e["scheduled"] += 1
    days_out: list[dict[str, Any]] = []
    for dk in sorted(by_day.keys(), reverse=True):
        d = by_day[dk]
        models_list = sorted(d["models"].values(), key=lambda x: (-x["count"], x["model"]))
        days_out.append(
            {
                "day_key": dk,
                "total": d["total"],
                "try_sum": round(float(d.get("try_sum", 0.0)), 4),
                "models": models_list,
            }
        )

    manual_case = case((AiBriefRunLog.source == "manual", 1), else_=0)
    model_totals_rows = (
        db.query(
            AiBriefRunLog.model_name,
            func.count(AiBriefRunLog.id).label("cnt"),
            func.sum(manual_case).label("manual_n"),
            func.coalesce(func.sum(AiBriefRunLog.approx_try), 0.0).label("try_sum"),
        )
        .group_by(AiBriefRunLog.model_name)
        .all()
    )
    models_all_time: list[dict[str, Any]] = []
    for mn_row, cnt, manual_n, try_sum in model_totals_rows:
        mn = (mn_row or "").strip() or "—"
        c = int(cnt or 0)
        m = int(manual_n or 0)
        s = max(0, c - m)
        ts = float(try_sum or 0)
        models_all_time.append(
            {
                "model": mn,
                "count": c,
                "scheduled": s,
                "manual": m,
                "approx_try": round(ts, 4),
            }
        )
    models_all_time.sort(key=lambda x: (-x["count"], x["model"]))

    spent_try = 0.0
    budget_try = float(getattr(settings, "llm_spend_budget_try", 0.0) or 0.0)
    try:
        from backend.services.llm_spend import current_month_spent_try

        spent_try = float(current_month_spent_try(db))
    except Exception:  # noqa: BLE001
        pass

    return {
        "total_all": total_all,
        "days": days_out,
        "window_days": int(window_days),
        "models_all_time": models_all_time,
        "llm_spent_try_approx_month": round(spent_try, 4),
        "llm_budget_try": budget_try,
    }
