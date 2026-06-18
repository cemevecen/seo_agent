"""Gelen kutusu: düz metin özet, yanıt taslağı ve çoklu LLM ile yanıt şablonları."""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import httpx

from backend.config import settings
from backend.services import inbox_sync

LOGGER = logging.getLogger(__name__)

_MAX_CHARS = 14_000
_OPENAI_MAX_ATTEMPTS = 7


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = (response.headers.get("retry-after") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _truncate(s: str, n: int = _MAX_CHARS) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 20] + "\n… [kesildi]"


def _openai_rate_limit_exceeded(r: httpx.Response) -> RuntimeError:
    return RuntimeError(
        "OpenAI hız sınırına takıldı (429). Gelen kutusunda öncelik Groq ve Gemini’dir; "
        "Railway/.env’de GROQ_API_KEY ve GEMINI_API_KEY tanımlıysa çoğu istek OpenAI’ya hiç gitmez. "
        "Yine de OpenAI kullanıldıysa birkaç dakika sonra tekrar deneyin."
    )


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
    data: dict[str, Any] | None = None
    with httpx.Client(timeout=120.0) as client:
        for attempt in range(_OPENAI_MAX_ATTEMPTS):
            r = client.post(url, headers=headers, json=body)
            if r.status_code == 429:
                header_wait = _retry_after_seconds(r)
                if header_wait is not None:
                    wait_s = min(120.0, max(1.0, header_wait))
                else:
                    wait_s = min(90.0, (2**attempt) + random.uniform(0.0, 2.5))
                LOGGER.warning(
                    "OpenAI 429 (gelen kutusu LLM); %.1fs bekleniyor, deneme %s/%s",
                    wait_s,
                    attempt + 1,
                    _OPENAI_MAX_ATTEMPTS,
                )
                if attempt >= _OPENAI_MAX_ATTEMPTS - 1:
                    raise _openai_rate_limit_exceeded(r) from None
                time.sleep(wait_s)
                continue
            if r.status_code in (500, 502, 503):
                wait_s = min(45.0, (2**attempt) + random.uniform(0.0, 1.5))
                LOGGER.warning("OpenAI HTTP %s; %.1fs sonra tekrar", r.status_code, wait_s)
                if attempt >= _OPENAI_MAX_ATTEMPTS - 1:
                    r.raise_for_status()
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            data = r.json()
            break
    if data is None:
        raise RuntimeError("OpenAI: yanıt alınamadı.")
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    return str(content).strip()


def _gemini_plain_text(system: str, user: str, *, model_name: str) -> str:
    import google.generativeai as genai

    key = (settings.gemini_api_key or "").strip()
    if not key:
        raise RuntimeError("GEMINI_API_KEY tanımlı değil.")
    user_t = _truncate(user, 180_000)
    genai.configure(api_key=key)
    model = genai.GenerativeModel(model_name, generation_config={"temperature": 0.35})
    prompt = f"{system.strip()}\n\n{user_t}"
    resp = model.generate_content(prompt)
    return str(resp.text or "").strip()


def _groq_plain_text(system: str, user: str, *, model: str) -> str:
    key = (settings.groq_api_key or "").strip()
    if not key:
        raise RuntimeError("GROQ_API_KEY tanımlı değil.")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    user_t = _truncate(user, 180_000)
    body = {
        "model": (model or "llama-3.3-70b-versatile").strip(),
        "temperature": 0.35,
        "messages": [
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": user_t},
        ],
    }
    with httpx.Client(timeout=120.0) as client:
        r = client.post(url, headers=headers, json=body)
        if r.status_code == 429:
            raise RuntimeError("Groq hız sınırına takıldı (429). Birkaç dakika sonra tekrar deneyin.")
        r.raise_for_status()
        data = r.json()
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    out = str(content).strip()
    if not out:
        raise RuntimeError("Groq boş yanıt verdi.")
    return out


def analyze_alert_thread_tr_tr(messages_plain: str, *, route_tag: str) -> str:
    """Firebase / Ziyaret uyarı e-postaları için en az 15 cümlelik durum analizi."""
    structure = (
        "Yanıtı Markdown biçiminde yaz; her bölüm ## başlık ile ayrılsın. "
        "Bölümler arasında boş satır bırak. Yüzde değişimlerini aynen koru (ör. -81,42%). "
        "Madde listeleri için - veya numaralı satır kullan."
    )
    tag = (route_tag or "").strip().lower()
    tag = inbox_sync.normalize_inbox_route_tag(tag)
    if tag == "firebase":
        system = (
            "Sen kıdemli bir mobil uygulama güvenilirliği mühendisisin (Android/iOS crash, ANR, "
            "non-fatal, performans regresyonu). Aşağıdaki Firebase Crashlytics uyarı e-postasını Türkçe analiz et. "
            "En az 15 tam cümle yaz; gerekirse daha uzun ol.\n\n"
            "ÖNCELİK — e-postadaki teknik ibareleri yorumla:\n"
            "- E-postada geçen crash / fatal / non-fatal / ANR / bug / exception / error / stack trace "
            "satırlarını bul ve her birinin ne anlama geldiğini sade Türkçe ile açıkla.\n"
            "- Exception sınıf adı (ör. NullPointerException, EXC_BAD_ACCESS), hata mesajı, "
            "sınıf/metod adları ve stack trace satırları varsa bunların pratikte neyi ifade ettiğini yaz.\n"
            "- Uyarı crash mi, ANR mi, non-fatal mi, performans/ regresyon mu — net sınıflandır; "
            "ANR, crash ve non-fatal arasındaki farkı okuyucuya anlat.\n"
            "- Bu ibareler son kullanıcıda ne yaşatır (uygulama kapanması, donma, ekranın yanıt vermemesi vb.)?\n\n"
            "YAPMA:\n"
            "- E-postanın Firebase'den geldiğini, gönderen adresi, bildirim kanalı veya mail meta bilgisini "
            "uzun uzun anlatma; bunlara en fazla 1 cümle ayır.\n"
            "- Genel Crashlytics tanıtımı veya konu dışı giriş yapma.\n"
            "- Müşteriye cevap e-postası yazma.\n\n"
            "Şu bölümleri kullan:\n"
            "## Sorunun türü (crash / ANR / non-fatal / performans)\n"
            "## E-postadaki hata ibarelerinin anlamı\n"
            "## Kullanıcıya ve işe etkisi\n"
            "## Olası kök nedenler\n"
            "## Aciliyet\n"
            "## Önerilen teknik aksiyonlar\n\n"
            "«E-postadaki hata ibarelerinin anlamı» bölümünde e-postadan alıntıladığın terimleri "
            "backtick ile vurgula (ör. `NullPointerException`, `ANR`, `SIGSEGV`). "
            + structure
        )
    elif tag in ("nstat", "ziyaret"):
        system = (
            "Sen web analitiği ve trafik istihbaratı uzmanısın. "
            "Aşağıdaki noreply@doviz.com (nstat / ziyaret raporu) bildirim e-postasını Türkçe analiz et. "
            "En az 15 tam cümle yaz; daha uzun olabilir. "
            "Şu bölümleri kullan: ## Genel özet, ## Desktop trafik, ## Mobil trafik, "
            "## Trafik kaynakları, ## Anomaliler ve dikkat noktaları, ## Önerilen aksiyonlar. "
            "Trafik hacmi/trend, dönem karşılaştırması, sayfa ve kanal vurguları, risk ve fırsatları açıkla. "
            "Bu e-postaya cevap yazılmayacağını varsay; yalnızca durum değerlendirmesi yap. "
            + structure
        )
    else:
        raise ValueError(f"Desteklenmeyen uyarı rotası: {route_tag}")
    text, _ = inbox_plain_text_with_failover(system, _truncate(messages_plain))
    return text


def summarize_thread_tr_tr(messages_plain: str) -> str:
    system = (
        "Sen bir müşteri e-postası özetleyicisisin. Çıktıyı Türkçe yaz; madde işaretli kısa özet; "
        "talep, ton ve varsa teknik detayları belirt. Markdown başlık kullanma, düz metin."
    )
    text, _ = inbox_plain_text_with_failover(system, _truncate(messages_plain))
    return text


def inbox_brand_key(route_tag: str | None) -> str:
    tag = inbox_sync.normalize_inbox_route_tag(route_tag or "")
    if tag == inbox_sync.INBOX_ROUTE_SINEMALAR:
        return "sinemalar"
    return "doviz"


def _reply_style_rules() -> str:
    return (
        "Biçim: Selamlama 'Merhaba,'; 'Sayın ...' kullanma. "
        "Kapanış 'İyi günler dileriz,' veya 'Teşekkür ederiz.' / 'Bilginize sunarız.'; 'Saygılarımla' kullanma. "
        "İmza, isim, ünvan, telefon veya ek iletişim satırı ekleme. "
        "Yalnızca müşteriye gidecek e-posta gövdesini yaz; açıklama veya meta yorum ekleme.\n"
        "Operatör talimatı (ör. «konuyu net anlat») stil/yönlendirmedir; talimatı kelimesi kelimesine "
        "paragrafa çevirme («…anlatılmalıdır» gibi üçüncü şahıs gereklilik cümleleri yazma)."
    )


def _sinemalar_playbook() -> str:
    return (
        "Marka: Sinemalar.com — sinema seans ve içerik bilgisi paylaşan platform; "
        "hangi yapımın hangi şehir/salonda gösterileceğine karar vermez.\n"
        "ÖNCE gelen iletinin gerçek konusunu belirle; talebi yanlış kategoriye sokma "
        "(ör. telif/fotoğraf kaldırma talebini «gösterim/seans talebi» sanma).\n"
        "- Telif / içerik veya fotoğraf kaldırma talebi: talep alındığını belirt; içerik kaldırıldıysa "
        "kaldırıldığını net biçimde teyit et; gerekiyorsa hangi sayfa/URL olduğunu sor; "
        "seans/gösterim, sinema salonu yönlendirmesi YAPMA.\n"
        "- Gösterim talebi (film/konser/şehir/salon): ilgi için teşekkür; karar yapımcı/dağıtımcı ve "
        "sinema işletmelerindedir; Sinemalar.com yalnızca mevcut seansları yayımlar; "
        "talebi doğrudan ilgili sinema salonlarına iletmelerini söyle.\n"
        "- Teknik sorun: «Yaşadığınız sorun için üzgünüz»; hangi içeriklerde olduğuna örnek; "
        "ekran görüntüsü; işletim sistemi, tarayıcı sürümü, web mi uygulama mı — hepsini iste.\n"
        "- Resmi site tarihi ile uyuşmazlık: kurumsal dil; sorun için üzgünüz; örnek içerik + SS iste.\n"
        "Ton: kibar, net sınır çizen, abartısız vaat yok."
    )


def _doviz_playbook() -> str:
    return (
        "Marka: doviz.com — döviz/altın/finans veri uygulaması.\n"
        "ÖNCE gelen iletinin gerçek konusunu belirle; talebi yanlış kategoriye sokma.\n"
        "- Telif / içerik kaldırma talebi: talep alındığını belirt; içerik kaldırıldıysa kaldırıldığını "
        "net biçimde teyit et; gerekiyorsa ilgili sayfa/URL'yi sor; konu dışı yönlendirme yapma.\n"
        "- Ürün/veri talebi: dinle; bazı veriler (ör. dünya katılım altın) üyelik veya API kapsamında "
        "olabilir; konuyu ilgili iş ortağı/API ekibine ileteceğinizi belirt (İpek Hanım / dünya katılım "
        "kontağı — doğrudan kişi adı yazma, «ilgili birim» de). Kredi/mevduat karşılaştırma verileri "
        "enuygunfinans.com üzerinden; repo/mevduat API’de olmayabilir, paylaşılıp paylaşılamayacağı sorulur.\n"
        "- Teknik sorun: tam olarak ne çalışmıyor (uygulama açılmıyor mu, tek ekran mı, veri mi gelmiyor); "
        "SS veya ekran kaydı; OS, uygulama sürümü (formda varsa referans ver). "
        "Ana sayfada ilgili alan varsa belirt; «biz değişiklik yapmadık» deme, nötr kal.\n"
        "- Canlı yayın/kaçak link şikayeti: kaçak yayınlardan sızıntı olabildiği; yönlendirilen yayıncı "
        "ve reklamı engellemenin sınırlı yöntem olduğu; reklama tıklamasa bile SS iletebileceği.\n"
        "- Performans/donma: arka plan altyapı detayı (MySQL vb.) yazma; iyileştirme çalışması sürüyor de; "
        "kullanıcıya «hâlâ yaşıyor musunuz» diye sorma.\n"
        "- Hesap/e-posta uyuşmazlığı: güvenlik için doğrulama gerekebileceğini nazikçe belirt.\n"
        "- KESİNLİKLE support@doviz.com veya +90 telefon ekleme.\n"
        "İki sorun çözüldüyse bildirirken App Store / Google Play puanı rica edilebilir (kısa, baskıcı değil)."
    )


def _reply_system_prompt(brand: str) -> str:
    name = "Sinemalar.com" if brand == "sinemalar" else "doviz.com"
    playbook = _sinemalar_playbook() if brand == "sinemalar" else _doviz_playbook()
    return (
        f"Sen {name} müşteri destek temsilcisisin.\n"
        + _reply_style_rules()
        + "\n"
        + playbook
    )


def generate_email_from_instruction(
    *,
    instruction: str,
    thread_blob: str,
    route_tag: str,
    subject: str = "",
) -> tuple[str, str]:
    """Operatör talimatı + gelen ileti bağlamıyla müşteriye gidecek yanıt gövdesi üretir."""
    brand = inbox_brand_key(route_tag)
    system = (
        _reply_system_prompt(brand)
        + "\nGörev: Aşağıdaki e-posta bağlamındaki GELEN müşteri iletisine yanıt yaz.\n"
        "ÇOK ÖNEMLİ — operatör notu (talimat) en yüksek önceliklidir:\n"
        "1) Önce GELEN iletinin gerçek konusunu/talebini doğru anla; uydurma konu ekleme. "
        "Talebi alakasız bir kategoriye SOKMA (ör. 'fotoğraf kaldırma / telif' talebini "
        "'seans/gösterim talebi' sanıp sinema salonuna yönlendirme).\n"
        "2) Operatör notunu OLGUSAL GERÇEK ve alınan KARAR say. Notta bir aksiyon bildirildiyse "
        "(ör. 'kaldırdık fotoları' → fotoğraflar kaldırıldı), yanıtı bu aksiyonun YAPILMIŞ olduğunu "
        "müşteriye teyit edecek biçimde kur. Notla çelişen genel kalıpları (playbook) uygulama.\n"
        "3) Playbook yalnızca yardımcı kılavuzdur; operatör notu veya iletinin gerçek konusuyla "
        "çelişiyorsa o kısmı yok say.\n"
        "Operatör notunu olduğu gibi gövdeye kopyalama; doğal, kibar bir müşteri yanıtına çevir."
    )
    user = (
        f"Konu: {(subject or '').strip() or '(belirtilmedi)'}\n\n"
        f"Operatör notu (yanıtın dayanacağı gerçek/karar):\n{instruction.strip()}\n\n"
        f"E-posta bağlamı (GELEN iletinin konusunu buradan anla):\n{_truncate(thread_blob)}"
    )
    return inbox_plain_text_with_failover(system, user)


def generate_email_from_prompt(prompt: str) -> tuple[str, str]:
    """Geriye dönük: bağlam yoksa doviz varsayılanı ile üretir."""
    return generate_email_from_instruction(
        instruction=prompt,
        thread_blob="(Seçili konuşma yok — talimatı genel destek yanıtı olarak uygula.)",
        route_tag=inbox_sync.INBOX_ROUTE_DOVIZ,
        subject="",
    )


def draft_reply_tr_tr(messages_plain: str, *, route_tag: str | None = None, brand: str | None = None) -> str:
    b = brand or inbox_brand_key(route_tag)
    system = (
        _reply_system_prompt(b)
        + "\nGörev: Aşağıdaki e-posta zincirine doğrudan yanıt taslağı yaz."
    )
    text, _ = inbox_plain_text_with_failover(system, _truncate(messages_plain))
    return text


def inbox_llm_any_configured() -> bool:
    """Şablon üretimi için kullanılabilecek en az bir LLM anahtarı var mı?"""
    return bool(
        (settings.openai_api_key or "").strip()
        or (settings.gemini_api_key or "").strip()
        or (settings.groq_api_key or "").strip()
    )


def _inbox_llm_chain() -> list[tuple[str, str]]:
    """Öncelik: Groq → Gemini → OpenAI."""
    from backend.services.llm_provider_chain import inbox_provider_try_chain

    return inbox_provider_try_chain()


def inbox_plain_text_with_failover(system: str, user: str) -> tuple[str, str]:
    """Groq → Gemini → OpenAI sırasıyla düz metin üretir; (metin, sağlayıcı)."""
    chain = _inbox_llm_chain()
    if not chain:
        raise RuntimeError(
            "GROQ_API_KEY, GEMINI_API_KEY veya OPENAI_API_KEY tanımlanmalı."
        )
    last_err: Exception | None = None
    for provider, model_name in chain:
        try:
            if provider == "openai":
                text = openai_plain_text(system, user, model=model_name)
            elif provider == "gemini":
                text = _gemini_plain_text(system, user, model_name=model_name)
            elif provider == "groq":
                text = _groq_plain_text(system, user, model=model_name)
            else:
                continue
            if text.strip():
                return text, provider
            LOGGER.warning("inbox plain LLM provider=%s returned empty", provider)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("inbox plain LLM provider=%s failed: %s", provider, exc)
            last_err = exc
            continue
    raise RuntimeError(
        str(last_err)
        if last_err
        else "LLM çağrısı başarısız (yapılandırma veya boş yanıt)."
    )


def _reply_templates_user_prompt(thread_blob: str, *, route_tag: str | None = None) -> str:
    brand = inbox_brand_key(route_tag)
    name = "Sinemalar.com" if brand == "sinemalar" else "doviz.com"
    return (
        f"Görev: Aşağıdaki e-posta zincirinde, === YANITLANACAK İLETİ === bölümündeki müşteri mesajına "
        f"cevap verecek tam 3 farklı Türkçe yanıt taslağı üret ({name}).\n\n"
        "Kurallar:\n"
        "- Yalnızca tek bir geçerli JSON nesnesi döndür; kod bloğu veya açıklama yazma.\n"
        '- Şekil: {"templates":[{"label":"kısa etiket","body":"..."},{"label":"...","body":"..."},{"label":"...","body":"..."}]}\n'
        "- Etiketler: örn. «Dengeli», «Kısa», «Resmi».\n"
        + _reply_style_rules()
        + "\n"
        + (_sinemalar_playbook() if brand == "sinemalar" else _doviz_playbook())
        + "\n\nE-posta bağlamı:\n"
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


def reply_templates_three_tr_tr(
    thread_blob: str,
    *,
    preferred_provider: str | None = None,
    route_tag: str | None = None,
) -> tuple[list[dict[str, str]], str]:
    """Üç yanıt şablonu döndürür; (şablonlar, kullanılan_sağlayıcı)."""
    chain = _inbox_llm_chain()
    if preferred_provider:
        p = preferred_provider.strip().lower()
        if p in ("groq", "gemini", "openai"):
            chain = [c for c in chain if c[0] == p]
    if not chain:
        raise RuntimeError(
            "Yanıt şablonları için GROQ_API_KEY, GEMINI_API_KEY veya OPENAI_API_KEY tanımlanmalı "
            "(veya seçilen sağlayıcı yapılandırılmamış)."
        )
    prompt = _reply_templates_user_prompt(thread_blob, route_tag=route_tag)
    last_err: Exception | None = None
    from backend.services.llm_json_providers import _llm_json

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
