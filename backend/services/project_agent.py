"""ProjectControl AI Ajan — Gemini REST API ile streaming tool-use ajanı."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncGenerator

import httpx

from backend.config import settings
from backend.services.agent_tools import TOOL_DEFINITIONS, execute_tool

LOGGER = logging.getLogger(__name__)

_GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
_MODEL = "gemini-2.5-flash"

_SYSTEM_PROMPT = """sen ProjectControl'ün kıdemli teknik danışmanı ve gömülü AI ajanısın. kullanıcının hocası gibi davran — her teknik soruyu cevapla, araç olmasa bile kendi bilginle yardım et.

## platform hakkında
- **proje**: seo_agent — FastAPI + PostgreSQL + Railway deploy
- **github**: cemevecen/seo_agent (default branch: main)
- **dil**: Python 3.11 (backend), Jinja2 + Tailwind CSS (frontend), vanilla JS
- **servisler**: Google Analytics 4, Search Console, App Store Connect (ASC), Google Play Reports, Firebase Crashlytics → BigQuery, Google Cloud Storage
- **deploy**: Railway (production), main'e push edince otomatik deploy

## araçlar ve hangi soruda hangisini kullanırsın

### github istatistik soruları
| soru tipi | kullanılacak araç |
|---|---|
| "toplam kaç commit var" / "kaç commit atıldı" | `github_commit_stats` |
| "kim en çok commit attı" / "en aktif geliştirici" | `github_contributor_stats` |
| "kaç branch var" / "hangi branch'ler mevcut" | `github_list_branches` |
| "X dosyası ne zaman değişti" / "kim yazdı" | `github_file_history(path)` |
| "hangi diller kullanılıyor" / "dil dağılımı" | `github_repo_languages` |
| "X fonksiyonu nerede" / "Y nerede tanımlı" | `github_search_code(query)` |
| "son commitler" / "ne değişti" | `github_recent_commits` |
| "open issue'lar" / "bug'lar" | `github_list_issues` |
| "PR'lar" / "review bekleyenler" | `github_list_prs` |
| "CI/CD durumu" / "test geçti mi" | `github_list_workflows` |
| "release'ler" / "versiyon geçmişi" | `github_get_releases` |
| "repo genel bilgisi" | `github_get_repo_info` |
| "iki branch farkı" | `github_get_branch_diff(base, head)` |

### railway soruları
| soru tipi | kullanılacak araç |
|---|---|
| "son deploy" / "başarılı mı" / "ne zaman deploy edildi" | `railway_get_deployments` |
| "servisler ayakta mı" / "production sağlıklı mı" / "çalışıyor mu" | `railway_get_service_status` |
| "kaç servis var" / "ortamlar neler" / "proje bilgisi" | `railway_get_project_info` |
| "log" / "hata nerede" | `railway_get_logs` (yapı verir, dashboard yönlendirir) |
| "genel durum" / "her şey yolunda mı" | `railway_get_service_status` + `railway_get_deployments` |

### veritabanı soruları
| soru tipi | kullanılacak araç |
|---|---|
| "kaç kayıt var" / "tablo boyutu" | `db_table_stats` |
| "X sitesinin Y verisi" / herhangi veri sorusu | önce `db_get_schema`, sonra `db_custom_query` |

### sistem soruları
| soru tipi | kullanılacak araç |
|---|---|
| "sağlık durumu" / "token'lar tanımlı mı" | `system_health_check` |
| "proje yapısı" / "hangi dosyalar var" | `project_structure` |
| "X kodu nerede" / "dosya içeriği" | `github_get_file(path)` veya `github_search_code` |

## karmaşık sorularda düşünme yaklaşımı

**soru belirsizse veya birden fazla araç gerekiyorsa:**
1. soruyu alt parçalara böl
2. hangi araçların birleşimi cevabı verir? — sırayla çağır
3. sonuçları birleştir, sayısal veriye yorum ekle
4. "bu beklenen mi?" diye değerlendir

**örnek: "projeyi bir bak genel durum nasıl"**
→ `github_get_repo_info` + `railway_get_deployments` + `github_recent_commits` + `system_health_check`
→ hepsini çek, tek bir özet paragrafta sun

**örnek: "hangi dosyalar en çok değişmiş"**
→ `github_recent_commits(limit=20)` ile son commit'leri çek
→ mesajlara bakarak hangi dosyaların adı geçiyor analiz et
→ `github_search_code` ile kritik dosyaları bul

**örnek: "X özelliğini implemente et"**
→ önce `github_search_code(query)` ile ilgili kodu bul
→ `github_get_file(path)` ile tam dosyayı oku
→ değişikliği uygula, `github_create_or_update_file` ile kaydet

## davranış kuralları — kesinlikle uy

1. **her soruyu cevapla.** araç yoksa kendi bilginle cevap ver. asla "bu konuda aracım yok, yapamam" deme. git, github, railway, python, fastapi, sql, devops — her konuda bilgin var, kullan.

2. **mentor gibi davran.** sadece sonucu verme, neden böyle olduğunu kısaca açıkla. kullanıcıyı eğit.

3. **proaktif ol.** branch soruyorsa PR durumunu da kontrol et. deploy soruyorsa son commite de bak. bağlantılı konuları kendiliğinden araştır.

4. **türkçe konuş**, samimi ve teknik ol. branch, commit, deploy, PR, merge gibi terimleri çevirme. **zorunlu haller (özel isimler, kod, hata mesajları) dışında küçük harf kullan** — cümle başları dahil. rahat, samimi bir abi/hoca tonu.

5. **araç çağırmadan önce** ne yapacağını tek cümleyle belirt.

6. **veri yorumla.** ham sonuç döndürme — "3 branch var, ikisi 2 aydır dokunulmamış, silinebilir" gibi anlamlı yorum ekle.

7. **hata/sorun bulursan** github issue öner ama kullanıcı onayı olmadan açma.

8. **kod örnekleri** her zaman markdown kod bloğunda göster.

## kod yazma workflow'u
kullanıcı "şu dosyaya şunu ekle" veya "bunu implemente et" derse:
1. `github_search_code(query)` ile ilgili kodu/dosyayı bul (dosya yolu bilmiyorsan)
2. `github_get_file(path)` ile mevcut kodu oku
3. değişikliği uygula, tam dosya içeriğini hazırla
4. `github_create_or_update_file(path, content, message)` ile kaydet (doğrudan main'e)
5. ne değiştirdiğini kısaca açıkla

PR istenirse:
1. `github_create_branch_from_main(branch_name)` ile branch oluştur
2. değişikliği o branch'e yaz
3. `github_create_pr(title, body, branch)` ile PR aç

## doğal dil → veritabanı
kullanıcı bir veri sorusu sorarsa:
1. `db_get_schema()` ile tablo yapısını öğren
2. uygun SELECT sorgusunu oluştur
3. `db_custom_query(sql)` ile çalıştır, sonucu yorumla
sadece SELECT kullan, DML/DDL asla.

## sayfa bağlamı (page context)
kullanıcı admin panelinde bir sayfadayken sohbet eder. her istekte «aktif sayfa bağlamı» JSON'u system prompt'a eklenir.

kurallar:
1. «bu sayfa», «ekranda görünen», «şu filtrelerle», «özetle» denince önce aktif sayfa bağlamındaki `dom_snapshot`, `filters`, `custom` alanlarına bak.
2. bağlam yetersizse sayfa tipine göre `page_fetch_*` araçlarını kullan:
   - /firebase → `page_fetch_crashlytics_summary` (product/platform/days bağlamdan)
   - /inbox → `page_fetch_inbox_threads` veya `page_fetch_inbox_thread`
   - /intelligence → `page_fetch_news_intelligence`
   - /app → `page_fetch_app_intel`
   - /errors → `page_fetch_errors_summary` (site_id bağlamdan veya `page_list_sites`)
   - /realtime, /ga4 → `page_fetch_ga4_realtime` veya `page_list_sites`
3. hangi sayfada olduğunu kullanıcıya kısaca hatırlat (ör. «firebase · doviz ekranındasın»).
4. dom_snapshot metnini olduğu gibi kopyalama; özetle ve yorumla."""


def _api_key() -> str:
    key = (settings.gemini_api_key or "").strip()
    if not key:
        raise RuntimeError("GEMINI_API_KEY tanımlı değil. Railway environment variables'a ekle.")
    return key


def _tool_declarations() -> list[dict]:
    """TOOL_DEFINITIONS'ı Gemini REST formatına çevirir."""
    result = []
    for t in TOOL_DEFINITIONS:
        schema = {k: v for k, v in t.get("input_schema", {}).items() if k != "$schema"}
        result.append({
            "name": t["name"],
            "description": t["description"],
            "parameters": schema,
        })
    return result


def _build_request_body(contents: list[dict], page_context: dict[str, Any] | None = None) -> dict:
    from backend.services.page_context_tools import format_page_context_for_prompt

    prompt = _SYSTEM_PROMPT + format_page_context_for_prompt(page_context)
    return {
        "systemInstruction": {"parts": [{"text": prompt}]},
        "contents": contents,
        "tools": [{"functionDeclarations": _tool_declarations()}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 4096},
    }


def _messages_to_contents(messages: list[dict[str, Any]]) -> list[dict]:
    """messages [{role, content}] → Gemini contents formatı."""
    contents = []
    for m in messages:
        role = "model" if m["role"] == "assistant" else "user"
        contents.append({"role": role, "parts": [{"text": m["content"]}]})
    return contents


async def stream_agent_response(
    messages: list[dict[str, Any]],
    max_iterations: int = 8,
    session_id: str = "",
    page_context: dict[str, Any] | None = None,
) -> AsyncGenerator[str, None]:
    """Gemini REST ile tool-use döngüsü — SSE formatında string generator."""
    import asyncio
    import threading

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    final_messages: list[dict[str, Any]] = []

    def _send(event: dict[str, Any]):
        asyncio.run_coroutine_threadsafe(queue.put(event), loop)

    def _worker():
        try:
            result_msgs = _run_agent_loop(messages, max_iterations, _send, page_context=page_context)
            final_messages.extend(result_msgs)
        except Exception as e:
            LOGGER.exception("Ajan worker hatası")
            _send({"type": "error", "message": str(e)[:400]})
        finally:
            _send({"type": "done"})

    threading.Thread(target=_worker, daemon=True).start()

    while True:
        try:
            event = await asyncio.wait_for(queue.get(), timeout=120)
        except asyncio.TimeoutError:
            yield 'data: {"type":"error","message":"Zaman aşımı."}\n\n'
            break
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        if event.get("type") in ("done", "error"):
            # Geçmişi DB'ye kaydet (stream bitti, asistan yanıtı artık final_messages'da)
            if session_id and final_messages:
                def _save():
                    try:
                        from backend.services.agent_tools import ai_talk_save_messages
                        ai_talk_save_messages(session_id, final_messages)
                    except Exception:
                        pass
                threading.Thread(target=_save, daemon=True).start()
            break


def _gemini_generate(contents: list[dict], page_context: dict[str, Any] | None = None) -> dict:
    """Gemini REST API'ye tek istek atar, JSON yanıt döner."""
    key = _api_key()
    url = f"{_GEMINI_BASE}/{_MODEL}:generateContent?key={key}"
    body = _build_request_body(contents, page_context=page_context)
    with httpx.Client(timeout=90) as client:
        r = client.post(url, json=body)
        if r.status_code != 200:
            raise RuntimeError(f"Gemini API {r.status_code}: {r.text[:300]}")
        return r.json()


def _parse_response(data: dict) -> tuple[str, list[dict]]:
    """
    Yanıttan (text, function_calls) çıkarır.
    function_calls: [{"name": ..., "args": {...}}]
    """
    candidates = data.get("candidates") or []
    if not candidates:
        error = data.get("error", {})
        raise RuntimeError(f"Gemini yanıt boş: {error.get('message', json.dumps(data)[:200])}")

    candidate = candidates[0]
    finish_reason = candidate.get("finishReason", "")

    # content yoksa (SAFETY, RECITATION, OTHER vb.) anlamlı hata ver
    if finish_reason and finish_reason not in ("STOP", "MAX_TOKENS", "TOOL_USE", ""):
        raise RuntimeError(f"Gemini yanıtı kesildi (finishReason: {finish_reason}). Soruyu farklı bir şekilde sormayı dene.")

    parts = (candidate.get("content") or {}).get("parts") or []

    # content hiç yoksa ve STOP değilse uyar
    if not parts and finish_reason not in ("STOP", "TOOL_USE", ""):
        LOGGER.warning("Gemini boş parts döndü. finishReason=%s, data=%s", finish_reason, json.dumps(data)[:300])

    text_parts = []
    func_calls = []
    for part in parts:
        if "text" in part:
            text_parts.append(part["text"])
        if "functionCall" in part:
            fc = part["functionCall"]
            func_calls.append({"name": fc["name"], "args": fc.get("args", {})})
    return "".join(text_parts), func_calls


def _run_agent_loop(
    messages: list[dict[str, Any]],
    max_iterations: int,
    send: Any,
    page_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Senkron Gemini REST ajan döngüsü. Tamamlanan mesaj listesini döner (DB kayıt için)."""
    contents = _messages_to_contents(messages)
    # Başlangıç mesajlarını kopyala (role/content formatında tutuyoruz)
    saved: list[dict[str, Any]] = list(messages)

    for iteration in range(max_iterations):
        send({"type": "thinking", "iteration": iteration + 1})

        try:
            data = _gemini_generate(contents, page_context=page_context)
        except Exception as e:
            send({"type": "error", "message": str(e)})
            return saved

        try:
            text, func_calls = _parse_response(data)
        except Exception as e:
            send({"type": "error", "message": str(e)})
            return saved

        # Asistan yanıtını contents'e ekle (Gemini formatı)
        assistant_parts: list[dict] = []
        if text:
            assistant_parts.append({"text": text})
        for fc in func_calls:
            assistant_parts.append({"functionCall": {"name": fc["name"], "args": fc["args"]}})
        if assistant_parts:
            contents.append({"role": "model", "parts": assistant_parts})

        # Tool call yoksa bitir
        if not func_calls:
            if not text:
                LOGGER.warning("Gemini boş text + sıfır tool call döndü. iteration=%d", iteration)
                send({"type": "error", "message": "model yanıt üretemedi, tekrar dene."})
                return saved
            _stream_text(text, send)
            send({"type": "complete", "text": text})
            # Asistan yanıtını DB formatında kaydet
            if text:
                saved.append({"role": "assistant", "content": text})
            return saved

        # Araçları çalıştır
        tool_result_parts = []
        for fc in func_calls:
            tname = fc["name"]
            tinputs = fc.get("args") or {}
            send({"type": "tool_start", "tool": tname})
            result = execute_tool(tname, tinputs)
            result_str = json.dumps(result, ensure_ascii=False, default=str)
            send({"type": "tool_result", "tool": tname, "result_preview": result_str[:200]})
            tool_result_parts.append({
                "functionResponse": {"name": tname, "response": {"result": result_str}}
            })

        contents.append({"role": "user", "parts": tool_result_parts})

    send({"type": "complete", "text": "Maksimum iterasyon sayısına ulaşıldı."})
    return saved


def _stream_text(text: str, send: Any) -> None:
    """Metni kelime kelime streaming simüle eder."""
    import time
    words = text.split(" ")
    chunk = ""
    for i, word in enumerate(words):
        chunk += ("" if i == 0 else " ") + word
        if len(chunk) >= 15 or i == len(words) - 1:
            send({"type": "text_chunk", "text": chunk})
            chunk = ""
            time.sleep(0.02)
