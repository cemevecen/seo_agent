"""Günlük AI özet: GA4, PageSpeed, Search Console ve uyarılar — Türkçe, sabah cron, e-posta.

LLM çağrıları *yalnızca* bu modülde yapılır: zamanlanmış günlük job veya POST /ai/generate.
Kota: İstanbul günü başına üst sınır (ayar) ve eşzamanlı çalışma kilidi.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.models import AiDailyBriefReport, ExternalSite, Site
from backend.services.alert_engine import get_recent_alerts
from backend.services.email_templates import render_email_shell, section
from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.mailer import send_email
from backend.services.metric_store import get_latest_metrics, get_metric_history
from backend.services.operations_notifier import operations_recipients
from backend.services.warehouse import get_latest_search_console_rows_batch

LOGGER = logging.getLogger(__name__)

_BRIEF_SECTION_KEYS = ("ga4", "pagespeed", "search_console", "alerts")

# LLM: jenerik özet cümleleri yerine rakam ve çıkarım zorunluluğu
_BRIEF_DATA_FIRST_RULES_TR = """
SOMUT VERİ VE ÇIKARIM (ZORUNLU — tüm "metin" alanları için geçerli):
- Her başlık (ga4 / pagespeed / search_console / alerts) ve her proje için metin tamamen veri yorumu ve eyleme dönük çıkarım olmalı; genel tanım veya rapor dili kullanma.
- İlk cümle mutlaka o projeye ait girdiden alınmış en az bir sayı veya ölçü ile başlasın (ör. oturum sayısı, yüzde değişim, PageSpeed skoru, tıklama/gösterim, ortalama pozisyon, uyarı kaydı sayısı). "İzlenmektedir / değerlendirilmektedir / bakılmaktadır" ile başlama.
- Her ana paragrafta (çift satır sonu ile ayrılan blok) en az bir somut rakam veya JSON’daki alan adıyla eşleşen ölçü geçsin; yalnızca süreç anlatan cümle yazma.
- GA4: "ga4_hucreler_1_7_30_gun" içindeki profil, oturum_etiket, oturum_degisim_pct, organik_pay alanlarını metne yansıt; boşsa "veri setinde görünmüyor" de.
- PageSpeed: "pagespeed" nesnesindeki mobil_guncel, masaustu_guncel ve varsa yaklasik_7g_once / 30g_once veya son_olcumler dizisindeki değerleri adıyla kullan.
- Search Console: "search_console_ozet" altındaki gun_1, gun_7, gun_30 (ve varsa onceki_7g / onceki_30g) için tıklama, gösterim, ctr, position değerlerini karşılaştır; "Search Console verilerine göre izlenmektedir" gibi ifadeleri KULLANMA.
- Uyarılar: "bu_projeye_ozel_uyarilar" listesindeki tip, baslik, sorgu, metrik, zaman alanlarından en az üç kaydı metne göm; yalnızca "CTR düşüşü görülmektedir" demek yetmez — hangi uyarı, ne zaman veya hangi metin alanı. Liste boşsa bunu açıkça söyle ve tum_son_uyarilar içinden yalnızca bu domain’e ait olanları kullan.

KESİNLİKLE KULLANMA (yasak kalıplar ve eş anlamlıları):
- "… verilerine göre … izlenmektedir / takip edilmektedir / değerlendirilmektedir"
- "… değerlerine bakıldığında" ile başlayıp ardından somut rakam vermeden genel yorum
- "çeşitli uyarılar kaydedilmiştir / görülmektedir" gibi sayısız listeleme; yerine uyarı tipi ve bağlam (JSON’dan)
- "performansları izlenmektedir", "sistematik olarak değerlendirilmelidir" gibi boş normatif cümleler (somut önlem ve rakam olmadan)
- İki siteyi aynı cümlede şablonla özetleme; her proje metni o projenin JSON alt ağacına dayanmalı.
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
    gm = (settings.ai_daily_brief_gemini_model or "gemini-2.0-flash").strip()
    gq = (settings.ai_daily_brief_groq_model or "llama-3.3-70b-versatile").strip()

    if pref == "gemini":
        return ("gemini", gm) if gem_k else None
    if pref == "groq":
        return ("groq", gq) if groq_k else None
    if groq_k:
        return "groq", gq
    if gem_k:
        return "gemini", gm
    return None


def _resolve_brief_llm_with_override(provider_override: str | None) -> tuple[str, str] | None:
    """Arayüzden gelen groq|gemini seçimini uygular; geçersiz veya boşsa ayarlardaki auto mantığı."""
    o = (provider_override or "").strip().lower()
    if o not in ("groq", "gemini"):
        return _resolve_brief_llm()
    groq_k = (settings.groq_api_key or "").strip()
    gem_k = (settings.gemini_api_key or "").strip()
    gm = (settings.ai_daily_brief_gemini_model or "gemini-2.0-flash").strip()
    gq = (settings.ai_daily_brief_groq_model or "llama-3.3-70b-versatile").strip()
    if o == "groq":
        return ("groq", gq) if groq_k else None
    return ("gemini", gm) if gem_k else None


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
            ga4_by_gun[gun] = _ga4_hucre_ozet(layout)
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
        "not": "Metinlerde 1, 7 ve 30 günlük verileri tek akışta karşılaştır; ayrı başlıklar şart değil.",
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


def collect_ordered_domains_for_charts(db: Session, brief: AiDailyBriefReport | None) -> list[str]:
    """Özet veya canlı site sırası (grafikler için)."""
    if brief:
        parsed = parse_stored_brief_section_for_ui(brief.ga4_text)
        if parsed["mode"] == "sites" and parsed["items"]:
            return [str(x.get("domain") or "").strip().lower() for x in parsed["items"] if x.get("domain")]
    from backend.main import _preferred_site_order_key

    sites = (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.desc())
        .all()
    )
    sites = [s for s in sites if s.id not in _external_site_ids(db)]
    sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
    return [s.domain.lower() for s in sites]


def build_ai_brief_charts_payload(db: Session, site: Site) -> dict:
    """Plotly.newPlot için ham seriler (sayfa yükünde güncel metrik)."""
    from backend.main import _summarize_search_console_rows

    latest_list = get_latest_metrics(db, site.id)
    latest_floats = {m.metric_type: float(m.value) for m in latest_list}
    mob = latest_floats.get("pagespeed_mobile_score")
    desk = latest_floats.get("pagespeed_desktop_score")
    sc_batch = get_latest_search_console_rows_batch(
        db, site_id=site.id, scopes=["current_day", "current_7d", "current_30d"]
    )
    s1 = _summarize_search_console_rows(sc_batch.get("current_day") or [])
    s7 = _summarize_search_console_rows(sc_batch.get("current_7d") or [])
    s30 = _summarize_search_console_rows(sc_batch.get("current_30d") or [])
    rows7 = sc_batch.get("current_7d") or []
    dev: dict[str, float] = defaultdict(float)
    for r in rows7:
        dev[str(r.get("device") or "—")] += float(r.get("clicks") or 0)
    g7 = latest_floats.get("ga4_web_sessions_last7d_total")
    g30 = latest_floats.get("ga4_web_sessions_last30d_total")
    g1 = latest_floats.get("ga4_web_sessions_last1d_total")
    ga4_labels: list[str] = []
    ga4_vals: list[float] = []
    if g1 is not None:
        ga4_labels.append("1g")
        ga4_vals.append(float(g1))
    if g7 is not None:
        ga4_labels.append("7g")
        ga4_vals.append(float(g7))
    if g30 is not None:
        ga4_labels.append("30g")
        ga4_vals.append(float(g30))
    site_alerts = get_recent_alerts(db, limit=200, include_external=False, site_id_filter=site.id)
    ac = Counter(str(a.get("alert_type") or "diger") for a in site_alerts)
    alert_labels = list(ac.keys())[:12]
    alert_vals = [float(ac[k]) for k in alert_labels]
    return {
        "domain": site.domain,
        "pagespeed_bar": {
            "labels": ["Mobil", "Masaüstü"],
            "values": [mob, desk],
        },
        "sc_clicks_bar": {
            "labels": ["1 gün", "7 gün", "30 gün"],
            "values": [float(s1.get("clicks") or 0), float(s7.get("clicks") or 0), float(s30.get("clicks") or 0)],
        },
        "sc_device_pie": {
            "labels": list(dev.keys()),
            "values": [float(v) for v in dev.values()],
        },
        "ga4_sessions_bar": {"labels": ga4_labels, "values": ga4_vals} if ga4_vals else None,
        "alerts_by_type_bar": {"labels": alert_labels, "values": alert_vals} if alert_labels else None,
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


def _gemini_json(prompt: str, *, model_name: str) -> dict:
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
    return _parse_json_object(resp.text or "")


def _groq_chat_json(prompt: str, *, model: str) -> dict:
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
                content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
                return _parse_json_object(content)
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                continue
    raise last_err if last_err else RuntimeError("Groq çağrısı başarısız")


def _llm_json(prompt: str, *, provider: str, model_name: str) -> dict:
    if provider == "groq":
        return _groq_chat_json(prompt, model=model_name)
    if provider == "gemini":
        return _gemini_json(prompt, model_name=model_name)
    raise ValueError(f"Bilinmeyen LLM sağlayıcı: {provider}")


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


def generate_brief_sections(context: dict, *, provider: str, model_name: str) -> dict[str, str]:
    ctx_json = json.dumps(context, ensure_ascii=False, indent=2)
    prompt = f"""Sen kıdemli bir Türkçe SEO ve analitik danışmanısın. Aşağıdaki JSON verisi gerçek izleme özetidir.

{ctx_json}
{_BRIEF_DATA_FIRST_RULES_TR}
YAPILANDIRMA: Girdi JSON içindeki "siteler" dizisindeki her proje için ayrı değerlendirme yaz. Siteleri TEK paragrafta birbirine karıştırma. "domain" alanını girdideki "domain" ile birebir (küçük harf) eşleştir. "baslik" olarak "alan_adi" değerini kullan.

GÖREV: Dört ana başlık altında (ga4, pagespeed, search_console, alerts) her projede:
- En az 500 kelime (proje başına, başlık başına). JSON içinde metin uzun olabilir.
- Girdi JSON’daki 1, 7 ve 30 günlük özetleri (search_console_ozet, ga4_hucreler_1_7_30_gun, pagespeed tarihçesi, bu_projeye_ozel_uyarilar) tek metin içinde karşılaştırmalı yorumla; ayrı alt başlık zorunlu değil.
- "Öne çıkanlar:", "Risk:", "Öneri:", "Öncelik:" gibi kısa iç başlıklar kullan (markdown # yok) — her blokta somut rakam veya JSON alanına dayalı çıkarım olsun.
- Pozitif / negatif gidişatı ayır; ne yapılmalı, erken müdahale, öncelik — mümkünse ölçülebilir hedef veya eşik ile.
- alerts: yalnızca bu projeye ilişkin uyarıları; JSON’daki kayıtları tek tek veya sayımla bağla, şablon cümle kullanma.

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
    data = _llm_json(prompt, provider=provider, model_name=model_name)
    return _finalize_brief_payload(data, context)


def generate_brief_single_pass(
    context: dict, *, provider: str, model_name: str
) -> tuple[dict[str, str], bool, str]:
    """Tek LLM çağrısı: dört bölüm (proje bazlı) + Türkçe öz değerlendirme (tamam)."""
    ctx_json = json.dumps(context, ensure_ascii=False, indent=2)
    prompt = f"""Sen kıdemli bir Türkçe SEO ve analitik danışmanısın. Aşağıdaki JSON verisi gerçek izleme özetidir.

{ctx_json}
{_BRIEF_DATA_FIRST_RULES_TR}
YAPILANDIRMA: "siteler" içindeki her proje için ayrı metin üret; projeleri tek paragrafta birleştirme. "domain" birebir eşleşsin. "baslik" = "alan_adi".

ÜSLUP: Türkçe, TDK’ya yakın yazım, okunaklı paragraflar.
Her projede (her başlık altında) en az 500 kelime. 1, 7 ve 30 günlük verileri aynı metinde birlikte değerlendir (ayrı bölüm şart değil).
"Öne çıkanlar:", "Dikkat:", "Öncelikli adımlar:" gibi kısa iç başlıklar kullan (markdown # yok) — altında mutlaka JSON’dan gelen sayı veya alan değeri geçsin.

BAŞLIKLAR: ga4, pagespeed, search_console, alerts — her biri proje nesnelerinden oluşan bir dizi.
alerts: bu_projeye_ozel_uyarilar kayıtlarını somut kullan; boşsa açıkça belirt.

Çıktı YALNIZCA şu alanlara sahip TEK bir JSON nesnesi olsun:
- "ga4", "pagespeed", "search_console", "alerts": her biri [{{"domain","baslik","metin"}}] dizisi (girdideki tüm siteler, aynı sıra).
- "tamam": boolean — Türkçe ve yapı tutarlıysa true; ayrıca metinlerin jenerik şablon içermediğini kendin doğrula.

Kurallar:
- Markdown yok; metin içinde \\n\\n ile paragraflar.
- Uydurma veri yok; eksikte "veri setinde görünmüyor" de.
- ASCII kısaltmaları koru (GA4, CTR, URL).
"""
    data = _llm_json(prompt, provider=provider, model_name=model_name)
    ok = bool(data.get("tamam", True))
    detail = "single_pass_self_qc_ok" if ok else "single_pass_self_qc_flagged"
    return _finalize_brief_payload(data, context), ok, detail


def verify_turkish_batch(
    sections: dict[str, str], *, provider: str, model_name: str, context: dict
) -> tuple[dict[str, str], bool, str]:
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

GÖREV: Yalnızca "metin" alanlarını Türkçe yazım ve noktalama açısından düzelt (TDK yaklaşımı). domain ve baslik alanlarını aynen koru. Teknik anlamı, TÜM rakamları, uyarı tiplerini, sorgu/metrik alıntılarını ve metin uzunluğunu (≥500 kelime) koru; içeriği kısaltma, özetleme veya jenerik cümlelerle değiştirme.
Eğer bir metin zaten somut veri içeriyorsa, yazım düzeltmesi dışında cümle yapısını bozma; "izlenmektedir / değerlendirilmektedir" gibi boş ifadelerle değiştirme.

Çıktı: Aynı yapıda JSON — "ga4","pagespeed","search_console","alerts" dizileri + "tamam" (boolean).

GİRDİ:
{payload}
"""
    try:
        data = _llm_json(prompt, provider=provider, model_name=model_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("AI Turkish QC JSON parse failed: %s", exc)
        return sections, False, str(exc)
    ok = bool(data.get("tamam", True))
    out = _finalize_brief_payload(data, context)
    for k in _BRIEF_SECTION_KEYS:
        if not (data.get(k)) and sections.get(k):
            out[k] = sections[k]
    detail = "model_qc_ok" if ok else "model_qc_flagged"
    return out, ok, detail


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
    resolved = _resolve_brief_llm_with_override(provider_override)
    if not resolved:
        LOGGER.warning("AI günlük özet atlandı: GROQ_API_KEY veya GEMINI_API_KEY (veya AI_DAILY_BRIEF_PROVIDER) yapılandırılmadı.")
        return

    provider, model_name = resolved
    single = bool(settings.ai_daily_brief_single_llm_call)
    planned_calls = 1 if single else 2

    brief_date = _istanbul_today_str()
    label_stored = f"{provider}:{model_name}"[:80]

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

        if not _try_reserve_llm_calls(planned_calls):
            return

        try:
            if single:
                final, qc_ok, qc_detail = generate_brief_single_pass(
                    context, provider=provider, model_name=model_name
                )
            else:
                draft = generate_brief_sections(context, provider=provider, model_name=model_name)
                final, qc_ok, qc_detail = verify_turkish_batch(
                    draft, provider=provider, model_name=model_name, context=context
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("AI brief generation failed: %s", exc)
            _refund_llm_calls(planned_calls)
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
