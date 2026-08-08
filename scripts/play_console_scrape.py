#!/usr/bin/env python3
"""Google Play Console scrape (Mac bridge).

İlk giriş (headed — bir kez):
  .venv/bin/python scripts/play_console_scrape.py --login

Sync (headless, kayıtlı session):
  .venv/bin/python scripts/play_console_scrape.py --sync
  .venv/bin/python scripts/play_console_scrape.py --sync --ingest

Env:
  PLAY_CONSOLE_DEVELOPER_ID  (default 7587799419591090593)
  PLAY_CONSOLE_APP_ID        (default 4974102243818231576)
  PLAY_CONSOLE_PACKAGE       (default com.Doviz)
  PLAY_CONSOLE_PROFILE_DIR   (default ~/.seo-agent/play-console-profile)
  PLAY_CONSOLE_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    parsed: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        k, v = k.strip(), v.strip().strip("'").strip('"')
        if k:
            parsed[k] = v
    for k, v in parsed.items():
        if k not in os.environ:
            os.environ[k] = v


_load_dotenv()

DEV_ID = (os.environ.get("PLAY_CONSOLE_DEVELOPER_ID") or "7587799419591090593").strip()
APP_ID = (os.environ.get("PLAY_CONSOLE_APP_ID") or "4974102243818231576").strip()
PACKAGE = (os.environ.get("PLAY_CONSOLE_PACKAGE") or "com.Doviz").strip()
PROFILE_DIR = Path(
    os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()
DASHBOARD_URL = (
    os.environ.get("PLAY_CONSOLE_DASHBOARD_URL")
    or f"https://play.google.com/console/u/0/developers/{DEV_ID}/app/{APP_ID}/app-dashboard"
).strip()
REVIEWS_URL = (
    os.environ.get("PLAY_CONSOLE_REVIEWS_URL")
    or f"https://play.google.com/console/u/0/developers/{DEV_ID}/app/{APP_ID}/user-feedback/reviews"
).strip()
INGEST_URL = (
    os.environ.get("PLAY_CONSOLE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/play-console/ingest"
).strip()


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _need_login(page_url: str, title: str, body_sample: str) -> bool:
    u = (page_url or "").lower()
    t = (title or "").lower()
    b = (body_sample or "").lower()
    if "accounts.google.com" in u or "signin" in u:
        return True
    if "sign in" in t or "oturum aç" in t:
        return True
    if "email or phone" in b or "e-posta veya telefon" in b:
        return True
    return False


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    pw = sync_playwright().start()
    context = pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=not headed,
        viewport={"width": 1440, "height": 1100},
        locale="tr-TR",
        args=["--disable-blink-features=AutomationControlled"],
    )
    return pw, context


def run_login_interactive(timeout_sec: int = 600) -> dict[str, Any]:
    """Headed browser — kullanıcı cemevecen@nokta.com ile giriş yapar."""
    pw, context = _launch_context(headed=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        print(
            f"Tarayıcıda giriş yap (cemevecen@nokta.com). "
            f"Dashboard açılınca beklemeye gerek yok — {timeout_sec}s içinde otomatik kapanır.",
            flush=True,
        )
        deadline = time.time() + max(60, timeout_sec)
        while time.time() < deadline:
            url = page.url or ""
            title = ""
            try:
                title = page.title() or ""
            except Exception:
                pass
            if "play.google.com/console" in url and "accounts.google.com" not in url:
                # App dashboard veya console ana — session OK
                time.sleep(2)
                print(f"Login OK · {url}", flush=True)
                return {"ok": True, "url": url, "title": title, "profile": str(PROFILE_DIR)}
            time.sleep(2)
        return {
            "ok": False,
            "message": "Login zaman aşımı — tekrar --login dene",
            "url": page.url,
            "profile": str(PROFILE_DIR),
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _attach_network_capture(page, bag: list[dict[str, Any]]) -> None:
    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            if not any(
                x in url
                for x in (
                    "play.google.com",
                    "androidpublisher",
                    "playdeveloperreporting",
                    "googleapis.com",
                )
            ):
                return
            ctype = (resp.headers or {}).get("content-type", "")
            if "json" not in ctype and "javascript" not in ctype:
                return
            # Boyut sınırı
            body = None
            try:
                body = resp.json()
            except Exception:
                try:
                    text = resp.text()
                    if len(text) > 200_000:
                        text = text[:200_000]
                    body = {"_text": text}
                except Exception:
                    return
            bag.append(
                {
                    "url": url[:500],
                    "status": resp.status,
                    "body": body,
                }
            )
        except Exception:
            return

    page.on("response", on_response)


def _extract_metrics_dom(page) -> list[dict[str, Any]]:
    """Dashboard kartlarından title/value/delta çıkar (esnek DOM)."""
    return page.evaluate(
        """() => {
      const out = [];
      const seen = new Set();
      const nodes = Array.from(document.querySelectorAll('div, article, section, li'));
      for (const el of nodes) {
        if (!el || el.children.length > 12) continue;
        const text = (el.innerText || '').trim();
        if (!text || text.length < 8 || text.length > 280) continue;
        const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
        if (lines.length < 2 || lines.length > 8) continue;
        const title = lines[0];
        // Tipik kart: başlık + ana değer + değişim
        if (title.length < 4 || title.length > 80) continue;
        const value = lines[1] || '';
        const delta = lines.find((l, i) => i > 1 && (/^[+\\-−%]/.test(l) || l.includes('%') || l.includes('yüzde'))) || '';
        const key = title + '|' + value;
        if (seen.has(key)) continue;
        // Çöp filtre
        if (/^(menu|ayar|yardım|search|ara)$/i.test(title)) continue;
        if (!/\\d/.test(value) && !/\\d/.test(delta)) continue;
        seen.add(key);
        out.push({ title, value, delta, lines });
        if (out.length >= 40) break;
      }
      return out;
    }"""
    )


def _extract_rating_summary_dom(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const text = document.body ? (document.body.innerText || '') : '';
      const pick = (re) => {
        const m = text.match(re);
        return m ? m[1] : null;
      };
      return {
        default_rating: pick(/Varsayılan Google Play puanı[^\\d]*([\\d,\\.]+)/i)
          || pick(/Google Play puanı[^\\d]*([\\d,\\.]+)/i),
        users: pick(/Kullanıcılar[^\\d]*([\\d\\.\\s]+)/i),
        ratings_with_reviews: pick(/Yorum içeren puanlar[^\\d]*([\\d\\.\\s]+)/i),
      };
    }"""
    )


def _extract_reviews_dom(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """() => {
      const out = [];
      const blocks = Array.from(document.querySelectorAll('article, li, div'));
      for (const el of blocks) {
        const t = (el.innerText || '').trim();
        if (!t || t.length < 40 || t.length > 4000) continue;
        if (!/yıldız|star|★|⭐|puan/i.test(t) && !/\\n.*\\n/.test(t)) continue;
        // Heuristic: author line + date + device meta + body
        const lines = t.split('\\n').map(s => s.trim()).filter(Boolean);
        if (lines.length < 3) continue;
        const hasDevice = /Android|iPhone|Samsung|Xiaomi|POCO|Galaxy|version/i.test(t);
        const hasDate = /\\d{1,2}\\s*(Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(t)
          || /\\d{1,2}[\\.\\/]\\d{1,2}[\\.\\/]\\d{2,4}/.test(t);
        if (!(hasDevice || hasDate)) continue;
        const author = lines[0].slice(0, 120);
        const body = lines.slice(1).join(' ').slice(0, 1500);
        const starM = t.match(/([1-5])\\s*(yıldız|star)/i) || t.match(/★{1,5}/);
        out.push({
          author,
          body,
          raw: t.slice(0, 2000),
          stars: starM ? starM[0] : null,
        });
        if (out.length >= 50) break;
      }
      // Dedup by author+body prefix
      const seen = new Set();
      const uniq = [];
      for (const r of out) {
        const k = (r.author || '') + '|' + (r.body || '').slice(0, 80);
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(r);
      }
      return uniq;
    }"""
    )


def scrape_play_console(*, headed: bool = False) -> dict[str, Any]:
    pw, context = _launch_context(headed=headed)
    network: list[dict[str, Any]] = []
    try:
        page = context.pages[0] if context.pages else context.new_page()
        _attach_network_capture(page, network)

        page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=120_000)
        try:
            page.wait_for_load_state("networkidle", timeout=45_000)
        except Exception:
            pass
        time.sleep(3)

        url = page.url or ""
        title = ""
        try:
            title = page.title() or ""
        except Exception:
            pass
        body_sample = ""
        try:
            body_sample = page.inner_text("body")[:800]
        except Exception:
            pass

        if _need_login(url, title, body_sample):
            return {
                "ok": False,
                "needs_login": True,
                "message": "Play Console oturumu yok — scripts/play_console_scrape.py --login çalıştır",
                "url": url,
                "metrics": [],
                "reviews": [],
                "rating_summary": {},
                "raw_network": [],
            }

        metrics = _extract_metrics_dom(page) or []
        # Reviews sayfası
        page.goto(REVIEWS_URL, wait_until="domcontentloaded", timeout=120_000)
        try:
            page.wait_for_load_state("networkidle", timeout=45_000)
        except Exception:
            pass
        time.sleep(2)
        rating_summary = _extract_rating_summary_dom(page) or {}
        reviews = _extract_reviews_dom(page) or []

        ok = bool(metrics or reviews or rating_summary.get("default_rating"))
        return {
            "ok": ok,
            "needs_login": False,
            "message": (
                f"Play scrape · {len(metrics)} metric · {len(reviews)} review"
                if ok
                else "Sayfa açıldı ama kart/yorum parse edilemedi (DOM değişmiş olabilir)"
            ),
            "url": DASHBOARD_URL,
            "package_name": PACKAGE,
            "app_id": APP_ID,
            "metrics": metrics,
            "reviews": reviews,
            "rating_summary": rating_summary,
            "raw_network": network[-40:],
            "source": "play_console_bridge",
            "source_url": DASHBOARD_URL,
            "sync_ok": ok,
            "sync_message": None,
            "sync_mode": "dashboard_reviews",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "message": str(exc),
            "metrics": [],
            "reviews": [],
            "rating_summary": {},
            "raw_network": network[-10:],
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    import requests

    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    payload = {
        "metrics": result.get("metrics") or [],
        "reviews": result.get("reviews") or [],
        "rating_summary": result.get("rating_summary") or {},
        "raw_network": result.get("raw_network") or [],
        "source": result.get("source") or "play_console_bridge",
        "source_url": result.get("source_url") or DASHBOARD_URL,
        "package_name": result.get("package_name") or PACKAGE,
        "app_id": result.get("app_id") or APP_ID,
        "sync_ok": bool(result.get("ok")),
        "sync_message": result.get("message"),
        "sync_mode": result.get("sync_mode") or "dashboard_reviews",
    }
    resp = requests.post(
        INGEST_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(payload),
        timeout=120,
    )
    try:
        body = resp.json() if resp.content else {}
    except Exception:
        body = {"raw": (resp.text or "")[:400]}
    if not isinstance(body, dict):
        body = {"message": str(body)}
    body["http_status"] = resp.status_code
    body["ok"] = resp.status_code < 400 and body.get("ok") is not False
    return body


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if "--login" in args:
        r = run_login_interactive()
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0 if r.get("ok") else 1
    headed = "--headed" in args
    do_ingest = "--ingest" in args or "--sync" in args
    # --sync implies scrape (+ ingest if token)
    result = scrape_play_console(headed=headed)
    print(json.dumps({k: v for k, v in result.items() if k != "raw_network"}, ensure_ascii=False, indent=2))
    if result.get("needs_login"):
        return 2
    if do_ingest and result.get("ok"):
        ing = ingest_scrape_result(result)
        print("INGEST:", json.dumps(ing, ensure_ascii=False, indent=2))
        return 0 if ing.get("ok") else 1
    if do_ingest and not result.get("ok"):
        # başarısız scrape'i de kaydet (UI'da mesaj görünsün)
        ing = ingest_scrape_result(result)
        print("INGEST (fail state):", json.dumps(ing, ensure_ascii=False, indent=2))
        return 1
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
