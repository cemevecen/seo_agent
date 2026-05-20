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
_MODEL = "gemini-2.0-flash"

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

## Davranış kuralları
- Türkçe konuş, samimi ve teknik ol
- Araç çağırmadan önce ne yapacağını kısaca belirt
- Hata bulursan doğrudan GitHub'a issue açmayı öner
- Kullanıcı izni olmadan issue açma veya destructive işlem yapma
- Veri sorunu fark edersen proaktif olarak haber ver
- Kod parçalarını markdown kod bloğunda göster"""


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


def _build_request_body(contents: list[dict]) -> dict:
    return {
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
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
) -> AsyncGenerator[str, None]:
    """Gemini REST ile tool-use döngüsü — SSE formatında string generator."""
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
            break


def _gemini_generate(contents: list[dict]) -> dict:
    """Gemini REST API'ye tek istek atar, JSON yanıt döner."""
    key = _api_key()
    url = f"{_GEMINI_BASE}/{_MODEL}:generateContent?key={key}"
    body = _build_request_body(contents)
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

    parts = candidates[0].get("content", {}).get("parts") or []
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
) -> None:
    """Senkron Gemini REST ajan döngüsü — ayrı thread'de çalışır."""
    contents = _messages_to_contents(messages)

    for iteration in range(max_iterations):
        send({"type": "thinking", "iteration": iteration + 1})

        try:
            data = _gemini_generate(contents)
        except Exception as e:
            send({"type": "error", "message": str(e)})
            return

        try:
            text, func_calls = _parse_response(data)
        except Exception as e:
            send({"type": "error", "message": str(e)})
            return

        # Asistan yanıtını history'e ekle
        assistant_parts: list[dict] = []
        if text:
            assistant_parts.append({"text": text})
        for fc in func_calls:
            assistant_parts.append({"functionCall": {"name": fc["name"], "args": fc["args"]}})
        if assistant_parts:
            contents.append({"role": "model", "parts": assistant_parts})

        # Tool call yoksa bitir — metni streaming simüle ederek gönder
        if not func_calls:
            # Metni kelime kelime gönder (streaming hissi)
            _stream_text(text or "(Yanıt yok)", send)
            send({"type": "complete", "text": text or ""})
            return

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
                "functionResponse": {
                    "name": tname,
                    "response": {"result": result_str},
                }
            })

        # Tool sonuçlarını bir sonraki tura ekle
        contents.append({"role": "user", "parts": tool_result_parts})

    send({"type": "complete", "text": "Maksimum iterasyon sayısına ulaşıldı."})


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
