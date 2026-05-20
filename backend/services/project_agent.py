"""ProjectControl AI Ajan — Gemini 2.0 Flash ile streaming tool-use ajanı."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncGenerator

from backend.config import settings
from backend.services.agent_tools import TOOL_DEFINITIONS, execute_tool

LOGGER = logging.getLogger(__name__)

_SYSTEM_PROMPT = """Sen ProjectControl'ün gömülü AI ajanısın. Bu uygulama bir SEO ve uygulama analitik platformudur.

## Platform hakkında
- **Proje**: seo_agent — FastAPI + PostgreSQL + Railway deploy
- **GitHub**: cemevecen/seo_agent
- **Dil**: Python (backend), Jinja2 + Tailwind (frontend), vanilla JS
- **Servisler**: Google Analytics 4, Search Console, App Store Connect (ASC), Google Play, Firebase Crashlytics, BigQuery

## Yeteneklerin
- GitHub issue ve PR'ları listele, yeni issue aç
- Railway deployment durumunu kontrol et
- Veritabanı istatistiklerini ve sorgularını çalıştır
- Sistem sağlık durumunu kontrol et
- Proje yapısını analiz et
- Bug tespit et ve GitHub'a otomatik issue aç

## Davranış kuralları
- Türkçe konuş, samimi ve teknik ol
- Araç çağırmadan önce ne yapacağını kısaca belirt
- Hata bulursan doğrudan GitHub'a issue açmayı öner
- Kullanıcı izni olmadan issue açma veya destructive işlem yapma
- Veri sorunu fark edersen proaktif olarak haber ver
- Kod parçalarını markdown kod bloğunda göster

Şu an aktif platformdasın. Soruları cevapla, araçları kullan, kullanıcıya yardım et."""


def _gemini_tool_declarations():
    """TOOL_DEFINITIONS'ı Gemini FunctionDeclaration listesine çevirir."""
    try:
        import google.generativeai as genai
        declarations = []
        for t in TOOL_DEFINITIONS:
            schema = dict(t.get("input_schema", {}))
            schema.pop("$schema", None)
            declarations.append(
                genai.protos.FunctionDeclaration(
                    name=t["name"],
                    description=t["description"],
                    parameters=schema,
                )
            )
        return [genai.protos.Tool(function_declarations=declarations)]
    except Exception as e:
        LOGGER.warning("Gemini tool declaration hatası: %s", e)
        return []


def _get_model():
    """Gemini modelini döner."""
    try:
        import google.generativeai as genai
    except ImportError:
        raise RuntimeError("google-generativeai paketi yüklü değil.")

    api_key = (settings.gemini_api_key or "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY tanımlı değil. Railway environment variables'a ekle.")

    genai.configure(api_key=api_key)
    return genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        system_instruction=_SYSTEM_PROMPT,
        tools=_gemini_tool_declarations(),
    )


def _to_gemini_history(messages: list[dict[str, Any]]):
    """messages [{role, content}] → Gemini chat history formatı."""
    import google.generativeai as genai
    history = []
    for m in messages[:-1]:  # Son mesaj hariç — o send_message ile gönderilecek
        role = "model" if m["role"] == "assistant" else "user"
        history.append(genai.protos.Content(
            role=role,
            parts=[genai.protos.Part(text=m["content"])],
        ))
    return history


async def stream_agent_response(
    messages: list[dict[str, Any]],
    max_iterations: int = 8,
) -> AsyncGenerator[str, None]:
    """Gemini ile tool-use döngüsü — SSE formatında string generator."""
    import asyncio
    import threading

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def _send(event: dict[str, Any]):
        asyncio.run_coroutine_threadsafe(queue.put(event), loop)

    def _worker():
        try:
            _run_agent_loop(messages, max_iterations, _send)
        except Exception as e:
            LOGGER.exception("Ajan worker hatası")
            _send({"type": "error", "message": str(e)[:300]})
        finally:
            _send({"type": "done"})

    threading.Thread(target=_worker, daemon=True).start()

    while True:
        try:
            event = await asyncio.wait_for(queue.get(), timeout=120)
        except asyncio.TimeoutError:
            yield 'data: {"type":"error","message":"Zaman aşımı — ajan yanıt vermedi."}\n\n'
            break
        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        if event.get("type") in ("done", "error"):
            break


def _run_agent_loop(
    messages: list[dict[str, Any]],
    max_iterations: int,
    send: Any,
) -> None:
    """Senkron Gemini ajan döngüsü — ayrı thread'de çalışır."""
    import google.generativeai as genai

    model = _get_model()
    history = _to_gemini_history(messages)
    chat = model.start_chat(history=history)
    user_text = messages[-1]["content"]

    for iteration in range(max_iterations):
        send({"type": "thinking", "iteration": iteration + 1})

        # Streaming ile yanıt al
        try:
            response_stream = chat.send_message(user_text, stream=True)
        except Exception as e:
            send({"type": "error", "message": f"Gemini API hatası: {e}"})
            return

        # Stream chunk'larını topla
        full_text = ""
        function_calls: list[Any] = []

        try:
            for chunk in response_stream:
                # Metin chunk'ları
                try:
                    chunk_text = chunk.text
                    if chunk_text:
                        full_text += chunk_text
                        send({"type": "text_chunk", "text": chunk_text})
                except Exception:
                    pass

                # Function call chunk'ları
                try:
                    for part in chunk.parts:
                        if hasattr(part, "function_call") and part.function_call.name:
                            function_calls.append(part.function_call)
                except Exception:
                    pass
        except Exception as e:
            send({"type": "error", "message": f"Stream okuma hatası: {e}"})
            return

        # Tool call yoksa bitti
        if not function_calls:
            send({"type": "complete", "text": full_text})
            return

        # Araçları çalıştır
        tool_response_parts = []
        for fc in function_calls:
            tname = fc.name
            try:
                tinputs = dict(fc.args)
            except Exception:
                tinputs = {}

            send({"type": "tool_start", "tool": tname})
            result = execute_tool(tname, tinputs)
            send({"type": "tool_result", "tool": tname, "result_preview": json.dumps(result, ensure_ascii=False, default=str)[:200]})

            tool_response_parts.append(
                genai.protos.Part(
                    function_response=genai.protos.FunctionResponse(
                        name=tname,
                        response={"result": json.dumps(result, ensure_ascii=False, default=str)},
                    )
                )
            )

        # Sonraki tur için kullanıcı mesajı = tool sonuçları
        user_text = genai.protos.Content(role="user", parts=tool_response_parts)

    send({"type": "complete", "text": "Maksimum iterasyon sayısına ulaşıldı."})
