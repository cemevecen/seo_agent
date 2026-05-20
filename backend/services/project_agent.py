"""ProjectControl AI Ajan — Claude claude-opus-4-7 ile streaming tool-use ajanı."""
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


def _get_client():
    """Anthropic istemcisini döner."""
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic paketi yüklü değil. requirements.txt'e ekle ve yeniden deploy et.")

    api_key = settings.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY tanımlı değil. Railway environment variables'a ekle.")
    return anthropic.Anthropic(api_key=api_key)


async def stream_agent_response(
    messages: list[dict[str, Any]],
    max_iterations: int = 8,
) -> AsyncGenerator[str, None]:
    """
    Claude ile tool-use döngüsü — SSE formatında string generator.
    Her yield bir `data: {...}\n\n` satırıdır.
    """
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
    """Senkron ajan döngüsü — ayrı thread'de çalışır."""
    client = _get_client()
    conversation = list(messages)

    for iteration in range(max_iterations):
        send({"type": "thinking", "iteration": iteration + 1})

        with client.messages.stream(
            model="claude-opus-4-7",
            max_tokens=4096,
            thinking={"type": "adaptive"},
            system=_SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=conversation,
        ) as stream:
            # Metin token'larını chunk chunk gönder
            full_text = ""
            tool_uses: list[dict[str, Any]] = []
            current_tool: dict[str, Any] | None = None
            current_input_json = ""
            stop_reason = None

            for event in stream:
                etype = type(event).__name__

                if etype == "RawContentBlockStartEvent":
                    block = event.content_block
                    btype = getattr(block, "type", "")
                    if btype == "text":
                        current_tool = None
                    elif btype == "tool_use":
                        current_tool = {
                            "id": block.id,
                            "name": block.name,
                            "input": {},
                        }
                        current_input_json = ""
                        send({"type": "tool_start", "tool": block.name})

                elif etype == "RawContentBlockDeltaEvent":
                    delta = event.delta
                    dtype = getattr(delta, "type", "")
                    if dtype == "text_delta":
                        chunk = delta.text
                        full_text += chunk
                        send({"type": "text_chunk", "text": chunk})
                    elif dtype == "input_json_delta":
                        current_input_json += delta.partial_json

                elif etype == "RawContentBlockStopEvent":
                    if current_tool is not None:
                        try:
                            current_tool["input"] = json.loads(current_input_json) if current_input_json else {}
                        except json.JSONDecodeError:
                            current_tool["input"] = {}
                        tool_uses.append(dict(current_tool))
                        current_tool = None
                        current_input_json = ""

                elif etype == "RawMessageStopEvent":
                    stop_reason = getattr(event.message, "stop_reason", None) if hasattr(event, "message") else None

            final_message = stream.get_final_message()
            stop_reason = final_message.stop_reason

        # Asistan yanıtını konuşma geçmişine ekle
        conversation.append({"role": "assistant", "content": final_message.content})

        # Tool kullanımı yoksa bitti
        if stop_reason != "tool_use" or not tool_uses:
            send({"type": "complete", "text": full_text})
            return

        # Araçları çalıştır ve sonuçları gönder
        tool_results = []
        for tool in tool_uses:
            tname = tool["name"]
            tinputs = tool.get("input", {})
            send({"type": "tool_running", "tool": tname, "inputs": _safe_inputs(tname, tinputs)})

            result = execute_tool(tname, tinputs)
            result_str = json.dumps(result, ensure_ascii=False, default=str)
            send({"type": "tool_result", "tool": tname, "result_preview": result_str[:200]})

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool["id"],
                "content": result_str,
            })

        conversation.append({"role": "user", "content": tool_results})

    # Max iterasyona ulaşıldı
    send({"type": "complete", "text": "Maksimum iterasyon sayısına ulaşıldı."})


def _safe_inputs(tool_name: str, inputs: dict[str, Any]) -> dict[str, Any]:
    """SQL içeriğini kısalt, gizli bilgileri maskele."""
    result = dict(inputs)
    if "sql" in result:
        result["sql"] = str(result["sql"])[:100] + "..."
    return result
