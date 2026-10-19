#!/usr/bin/env python3
"""Empower Creative Review — keyword tara / onaylı engelle.

  # Oturum (fx-empower)
  .venv/bin/python scripts/empower_creative_review.py --login

  # Tara (panel ingest)
  .venv/bin/python scripts/empower_creative_review.py --scan --keyword vw --ingest

  # Engelle (id listesi)
  .venv/bin/python scripts/empower_creative_review.py --block --ids 6abd8859… --ingest

Env:
  EMPOWER_CREATIVE_URL          default https://analytics.empower.net/creative-review
  EMPOWER_INTEL_PROFILE_DIR     default ~/.seo-agent/fx-empower
  EMPOWER_CREATIVE_INGEST_URL   default …/api/empower-creative/ingest-scan
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()

from backend.services.scrape_browser import (  # noqa: E402
    acquire_profile_login_lock,
    empower_profile_dir,
    login_wait_sec,
    release_profile_login_lock,
)
from backend.services.system_firefox_driver import (  # noqa: E402
    launch_system_firefox_driver,
)

PROFILE_DIR = empower_profile_dir()
CREATIVE_URL = (
    os.environ.get("EMPOWER_CREATIVE_URL")
    or "https://analytics.empower.net/creative-review"
).strip()
SCAN_INGEST_URL = (
    os.environ.get("EMPOWER_CREATIVE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/empower-creative/ingest-scan"
).strip()
BLOCK_INGEST_URL = (
    os.environ.get("EMPOWER_CREATIVE_BLOCK_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/empower-creative/ingest-block"
).strip()
PENDING_URL = (
    os.environ.get("EMPOWER_CREATIVE_PENDING_URL")
    or "https://projectcontrol.up.railway.app/api/empower-creative/pending"
).strip()

# Keyword → ekstra alias (kart metni / alt text)
KEYWORD_ALIASES: dict[str, tuple[str, ...]] = {
    "vw": ("vw", "volkswagen", "volks wagen"),
    "volkswagen": ("volkswagen", "vw"),
}


def _token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    token = _token()
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            **({"X-Notification-Ingest-Token": token} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {"ok": True}
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")[:400]
        return {"ok": False, "message": f"HTTP {exc.code}: {err}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:300]}


def _get_json(url: str) -> dict[str, Any]:
    token = _token()
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            **({"X-Notification-Ingest-Token": token} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:300]}


def match_terms(keyword: str) -> list[str]:
    raw = (keyword or "").strip().lower()
    if not raw:
        return []
    terms = {raw}
    for t in KEYWORD_ALIASES.get(raw, ()):
        terms.add(t.lower())
    # boşluksuz / tireli varyantlar
    terms.add(re.sub(r"[\s_\-]+", "", raw))
    return sorted(t for t in terms if t)


def card_matches(card: dict[str, Any], terms: list[str]) -> bool:
    hay = " ".join(
        [
            str(card.get("ocr_text") or ""),
            str(card.get("alt") or ""),
            str(card.get("img_src") or ""),
            str(card.get("text") or ""),
            str(card.get("ad_type") or ""),
        ]
    ).lower()
    # ID'yi haystack'e koyma — rastgele hex keyword false-positive üretebilir
    return any(t in hay for t in terms)


EXTRACT_CARDS_JS = r"""
return (function () {
  function hexIds(text) {
    var out = [];
    var re = /[a-f0-9]{20,40}/gi;
    var m;
    while ((m = re.exec(String(text || "")))) {
      var id = m[0].toLowerCase();
      if (out.indexOf(id) < 0) out.push(id);
    }
    return out;
  }
  function isBannerBox(el) {
    if (!el || el.tagName !== 'DIV') return false;
    var st = window.getComputedStyle(el);
    var h = parseFloat(st.height) || el.offsetHeight || 0;
    // Creative Review önizleme kutusu ~170px (ör. 264×170)
    if (h < 140 || h > 210) return false;
    var img = el.querySelector('img');
    if (!img) return false;
    var mh = parseFloat(img.style.maxHeight) || 0;
    if (mh && (mh < 140 || mh > 210) && h < 140) return false;
    return true;
  }
  var boxes = Array.from(document.querySelectorAll('div')).filter(isBannerBox);
  var seen = {};
  var cards = [];
  for (var i = 0; i < boxes.length; i++) {
    var box = boxes[i];
    var card = box.parentElement;
    if (!card) continue;
    // checkbox + body aynı parent'ta
    var txt = (card.innerText || card.textContent || '').replace(/\s+/g, ' ').trim();
    if (!txt || txt.length < 12) continue;
    if (!/BANNER|NATIVE|VIDEO|INTERSTITIAL|OPEN|Approve|Block|impress/i.test(txt)) continue;
    var ids = hexIds(txt);
    if (!ids.length) continue;
    var id = ids[0];
    if (seen[id]) continue;
    var img = box.querySelector('img');
    var src = (img && (img.currentSrc || img.src)) || '';
    var alt = (img && img.alt) || '';
    var typeM = txt.match(/\b(BANNER|NATIVE|VIDEO|INTERSTITIAL|REWARDED)\b/i);
    var imprM = txt.match(/([\d.,]+)\s*impress/i);
    var appsM = txt.match(/(\d+)\s*apps?/i);
    seen[id] = true;
    cards.push({
      id: id,
      ad_type: typeM ? typeM[1].toUpperCase() : '',
      impressions: imprM ? imprM[1] : '',
      apps: appsM ? appsM[1] : '',
      text: txt.slice(0, 800),
      alt: String(alt).slice(0, 400),
      img_src: String(src).slice(0, 800),
      box_h: box.offsetHeight || 0,
      box_w: box.offsetWidth || 0
    });
  }
  return cards;
})();
"""


def _ocr_image_bytes(data: bytes) -> str:
    if not data or len(data) < 80:
        return ""
    try:
        from io import BytesIO

        from PIL import Image, ImageOps
        import pytesseract
    except Exception:
        return ""
    try:
        img = Image.open(BytesIO(data))
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        # Banner metinleri için büyüt + gri
        w, h = img.size
        if w < 400:
            scale = max(2, int(400 / max(w, 1)))
            img = img.resize((w * scale, h * scale))
        gray = ImageOps.grayscale(img)
        text = pytesseract.image_to_string(gray, lang="eng+tur") or ""
        return re.sub(r"\s+", " ", text).strip()[:1200]
    except Exception as exc:
        print(f"  · OCR hata: {exc}", flush=True)
        return ""


def _fetch_bytes(url: str) -> bytes:
    if not url or not url.startswith("http"):
        return b""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "seo-agent-creative-review/1.0", "Accept": "image/*"},
        )
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.read()
    except Exception:
        return b""


def _ocr_card_banner(driver: Any, card: dict[str, Any]) -> str:
    """~264×170 önizleme kutusundaki banner görselini OCR et."""
    src = str(card.get("img_src") or "").strip()
    data = _fetch_bytes(src)
    if not data:
        cid = str(card.get("id") or "")
        short = cid[:18]
        try:
            # Kart metninde id geçen ~170px kutunun img'ini screenshot al
            els = driver.find_elements(
                "xpath",
                "//div[contains(@style,'height: 170') or contains(@style,'height:170')]"
                f"[.//img][ancestor::*[contains(translate(.,"
                f"'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{short}')]]"
                "//img",
            )
            if not els:
                els = driver.find_elements(
                    "xpath",
                    f"//*[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                    f"'abcdefghijklmnopqrstuvwxyz'),'{short}')]"
                    "/descendant::img[1] | "
                    f"//*[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                    f"'abcdefghijklmnopqrstuvwxyz'),'{short}')]/preceding::img[1]",
                )
            for el in els[:3]:
                try:
                    data = el.screenshot_as_png
                    if data and len(data) > 80:
                        break
                except Exception:
                    continue
        except Exception:
            data = b""
    return _ocr_image_bytes(data)


def _needs_login(driver: Any) -> bool:
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        return True
    if "creative-review" in url and "login" not in url:
        return False
    if "analytics.empower.net" in url and "login" not in url and "cognito" not in url:
        # Publisher panelindeyiz — giriş formu değil
        try:
            body = (driver.page_source or "")[:12000].lower()
        except Exception:
            return False
        if "creative review" in body or "approve" in body or "block" in body:
            return False
    if "login" in url or "cognito" in url or "amazonaws.com" in url:
        return True
    try:
        body = (driver.page_source or "")[:8000].lower()
    except Exception:
        return True
    if "sign in" in body and "creative review" not in body and "approve" not in body:
        return True
    return False


def _wait_logged_in(driver: Any, timeout_sec: int | None = None) -> bool:
    # Tarama için kısa süreli bekleme — login kilidi tutmadan
    timeout_sec = 180 if timeout_sec is None else int(timeout_sec)
    deadline = time.time() + max(30, timeout_sec)
    while time.time() < deadline:
        try:
            url = (driver.current_url or "").lower()
        except Exception:
            url = ""
        if not _needs_login(driver):
            if "creative-review" in url:
                return True
            if "analytics.empower.net" in url:
                try:
                    driver.get(CREATIVE_URL)
                    time.sleep(2.5)
                except Exception:
                    pass
                if "creative-review" in (driver.current_url or "").lower():
                    return True
        time.sleep(2)
    return not _needs_login(driver)


def _goto_creative_review(driver: Any) -> str:
    """Her zaman Creative Review URL'sine git; mevcut URL'yi döndür."""
    print(f"  · açılıyor · {CREATIVE_URL}", flush=True)
    driver.get(CREATIVE_URL)
    time.sleep(3.0)
    try:
        url = driver.current_url or ""
    except Exception:
        url = ""
    print(f"  · sayfa · {url}", flush=True)
    return url


def _click_text(driver: Any, labels: list[str]) -> bool:
    for label in labels:
        try:
            els = driver.find_elements("xpath", f"//*[self::button or self::a or self::span][contains(normalize-space(.), '{label}')]")
            for el in els[:3]:
                try:
                    if el.is_displayed():
                        el.click()
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


APPLY_FILTERS_JS = r"""
return (function () {
  function norm(s) { return String(s || '').replace(/\s+/g, ' ').trim(); }
  function clickLabel(labelRe) {
    var nodes = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], select, div, span'));
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      var t = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || '');
      if (!t || t.length > 48) continue;
      if (labelRe.test(t)) {
        try { el.click(); return t; } catch (e) {}
      }
    }
    return '';
  }
  function pickOption(optionRe) {
    var opts = Array.from(document.querySelectorAll(
      '[role="option"], li[role="menuitem"], li, button, div[class*="option"], div[class*="menu"] button, select option'
    ));
    for (var i = 0; i < opts.length; i++) {
      var el = opts[i];
      var t = norm(el.innerText || el.textContent || '');
      if (!t || t.length > 40) continue;
      if (optionRe.test(t)) {
        try {
          if (el.tagName === 'OPTION' && el.parentElement) {
            el.parentElement.value = el.value;
            el.parentElement.dispatchEvent(new Event('change', { bubbles: true }));
            return t;
          }
          el.click();
          return t;
        } catch (e) {}
      }
    }
    return '';
  }
  var out = { open: '', pageSize: '', properties: '', adTypes: '' };

  // Open only — default; yoksa aç
  var openShown = /Open only/i.test(document.body.innerText || '');
  if (!openShown) {
    clickLabel(/^(Status|All statuses|Approved|Blocked)$/i);
    out.open = pickOption(/^Open only$/i) || '';
  } else {
    out.open = 'Open only (already)';
  }

  // All properties / All ad types — default bırak; yanlışlıkla değiştiyse geri al
  var body = document.body.innerText || '';
  if (!/All properties/i.test(body)) {
    clickLabel(/properties/i);
    out.properties = pickOption(/^All properties$/i) || '';
  } else {
    out.properties = 'All properties (already)';
  }
  if (!/All ad types/i.test(body)) {
    clickLabel(/ad types?/i);
    out.adTypes = pickOption(/^All ad types$/i) || '';
  } else {
    out.adTypes = 'All ad types (already)';
  }

  // Sayfa boyutu ayrıca Python tarafında 200'e çekilir
  out.pageSizeHint = norm(
    (document.body.innerText.match(/\b(\d+)\s*per page\b/i) || [])[0] || ''
  );
  return out;
})();
"""


SET_PAGE_SIZE_200_JS = r"""
return (function () {
  function norm(s) { return String(s || '').replace(/\s+/g, ' ').trim(); }
  // Zaten 200 ise dokunma
  var shown = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span, select'));
  for (var i = 0; i < shown.length; i++) {
    var t = norm(shown[i].innerText || shown[i].textContent || '');
    if (/^200\s*per page$/i.test(t)) return { ok: true, already: true, label: t };
  }
  // "100 per page" / "50 per page" tetikleyicisini aç
  var trigger = null;
  for (var j = 0; j < shown.length; j++) {
    var tt = norm(shown[j].innerText || shown[j].textContent || '');
    if (/^\d+\s*per page$/i.test(tt)) { trigger = shown[j]; break; }
  }
  if (trigger) {
    try { trigger.click(); } catch (e) { return { ok: false, error: String(e) }; }
  }
  var opts = Array.from(document.querySelectorAll(
    '[role="option"], li[role="menuitem"], li, button, div[class*="option"], div[class*="menu"] *'
  ));
  for (var k = 0; k < opts.length; k++) {
    var ot = norm(opts[k].innerText || opts[k].textContent || '');
    if (/^200(\s*per page)?$/i.test(ot) || ot === '200') {
      try { opts[k].click(); return { ok: true, picked: ot }; } catch (e) {}
    }
  }
  var sels = Array.from(document.querySelectorAll('select'));
  for (var s = 0; s < sels.length; s++) {
    var sel = sels[s];
    for (var o = 0; o < (sel.options || []).length; o++) {
      if (/200/.test(norm(sel.options[o].text))) {
        sel.selectedIndex = o;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        return { ok: true, picked: 'select:200' };
      }
    }
  }
  return { ok: false, error: '200 option yok' };
})();
"""


def _apply_default_filters(driver: Any) -> dict[str, Any]:
    """All properties + Open only + All ad types (default) · her zaman 200 per page."""
    try:
        result = driver.execute_script(APPLY_FILTERS_JS)
    except Exception as exc:
        print(f"  · filtre JS hata: {exc}", flush=True)
        result = {}
    if not isinstance(result, dict):
        result = {}
    time.sleep(0.6)
    # Dropdown aç → bekle → 200 seç (tek JS bazen menü render olmadan bakar)
    try:
        driver.execute_script(
            """
            var nodes = Array.from(document.querySelectorAll('button, [role="button"], [role="combobox"], div, span'));
            for (var i = 0; i < nodes.length; i++) {
              var t = String(nodes[i].innerText || '').replace(/\\s+/g, ' ').trim();
              if (/^\\d+\\s*per page$/i.test(t) && !/^200\\s*per page$/i.test(t)) {
                nodes[i].click(); return t;
              }
            }
            return '';
            """
        )
        time.sleep(0.7)
        page_out = driver.execute_script(SET_PAGE_SIZE_200_JS)
    except Exception as exc:
        page_out = {"ok": False, "error": str(exc)[:120]}
    if isinstance(page_out, dict):
        result["pageSize"] = (
            page_out.get("picked")
            or ("200 (already)" if page_out.get("already") else "")
            or page_out.get("error")
            or "?"
        )
    print(
        "  · filtreler · "
        f"open={result.get('open') or '?'} · page={result.get('pageSize') or '?'} · "
        f"props={result.get('properties') or '?'} · types={result.get('adTypes') or '?'}",
        flush=True,
    )
    time.sleep(1.5)
    try:
        driver.execute_script("document.body.click();")
    except Exception:
        pass
    time.sleep(0.5)
    return result


def _goto_next_page(driver: Any) -> bool:
    try:
        moved = driver.execute_script(
            """
            var cands = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            for (var i = 0; i < cands.length; i++) {
              var el = cands[i];
              var t = (el.innerText || el.textContent || el.getAttribute('aria-label') || '').trim().toLowerCase();
              var dis = el.disabled || el.getAttribute('aria-disabled') === 'true' || /disabled|opacity-50/.test(el.className||'');
              if (dis) continue;
              if (t === 'next' || t === '>' || t === '›' || t.indexOf('next page') >= 0) {
                el.click();
                return true;
              }
            }
            return false;
            """
        )
        return bool(moved)
    except Exception:
        return False


def _extract_cards(driver: Any) -> list[dict[str, Any]]:
    try:
        rows = driver.execute_script(EXTRACT_CARDS_JS)
    except Exception as exc:
        print(f"  · kart extract hata: {exc}", flush=True)
        return []
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        cid = str(r.get("id") or "").strip().lower()
        if len(cid) < 16:
            continue
        out.append(
            {
                "id": cid,
                "ad_type": str(r.get("ad_type") or "")[:32],
                "impressions": str(r.get("impressions") or "")[:32],
                "apps": str(r.get("apps") or "")[:16],
                "text": str(r.get("text") or "")[:800],
                "alt": str(r.get("alt") or "")[:400],
                "img_src": str(r.get("img_src") or "")[:800],
                "box_w": int(r.get("box_w") or 0),
                "box_h": int(r.get("box_h") or 0),
                "ocr_text": "",
            }
        )
    return out


def _enrich_cards_with_ocr(driver: Any, cards: list[dict[str, Any]]) -> None:
    for i, card in enumerate(cards):
        if card.get("ocr_text"):
            continue
        ocr = _ocr_card_banner(driver, card)
        card["ocr_text"] = ocr
        if (i + 1) % 5 == 0 or i == 0:
            print(
                f"  · OCR {i + 1}/{len(cards)} · id={str(card.get('id') or '')[:12]}… · "
                f"{(ocr[:60] + '…') if len(ocr) > 60 else (ocr or '(boş)')}",
                flush=True,
            )


def _block_ids_on_page(driver: Any, ids: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    remaining = set(ids)
    for cid in list(remaining):
        try:
            ok = driver.execute_script(
                """
                var want = String(arguments[0] || '').toLowerCase();
                var nodes = Array.from(document.querySelectorAll('article, [class*="card"], [class*="Card"], li, div'));
                for (var i = 0; i < nodes.length; i++) {
                  var el = nodes[i];
                  var txt = (el.innerText || '').toLowerCase();
                  if (txt.indexOf(want) < 0) continue;
                  var btns = Array.from(el.querySelectorAll('button, a, [role="button"]'));
                  for (var j = 0; j < btns.length; j++) {
                    var t = (btns[j].innerText || btns[j].textContent || '').trim().toLowerCase();
                    if (t === 'block' || t.indexOf('block') === 0) {
                      btns[j].click();
                      return true;
                    }
                  }
                }
                return false;
                """,
                cid,
            )
            time.sleep(0.6)
            # olası confirm
            _click_text(driver, ["Block", "Confirm", "Yes", "OK", "Engelle"])
            time.sleep(0.4)
            if ok:
                blocked.append({"id": cid, "ok": True})
                remaining.discard(cid)
            else:
                failed.append({"id": cid, "error": "kart/buton bulunamadı"})
        except Exception as exc:  # noqa: BLE001
            failed.append({"id": cid, "error": str(exc)[:160]})
    return blocked, failed


def open_session(*, headed: bool = True, login_only: bool = False) -> Any:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    # Bizim bıraktığımız takılı login kilidini temizle (pid ölüyse)
    try:
        from backend.services.scrape_browser import profile_login_lock_path

        lock = profile_login_lock_path(PROFILE_DIR)
        if lock.is_file():
            raw = lock.read_text(encoding="utf-8", errors="replace")
            if "empower-creative" in raw:
                release_profile_login_lock(PROFILE_DIR)
                print("  · eski empower-creative login kilidi temizlendi", flush=True)
    except Exception:
        pass

    driver = launch_system_firefox_driver(PROFILE_DIR, headed=headed, page_load_timeout=120)
    url = _goto_creative_review(driver)

    if _needs_login(driver) or login_only:
        # Login kilidi ALMA — diğer Empower scrapelerini kilitlemesin.
        # Yalnızca --login veya gerçek giriş formu için headed bekle.
        print("  · Empower girişi gerekli — Firefox'ta Sign In + 2FA", flush=True)
        if login_only:
            acquire_profile_login_lock(PROFILE_DIR, reason="empower-creative-login")
        try:
            _click_text(driver, ["Sign In", "Sign in", "Giriş"])
            time.sleep(1)
            if not _wait_logged_in(driver, timeout_sec=login_wait_sec() if login_only else 180):
                raise RuntimeError(
                    "Empower Creative Review login zaman aşımı — "
                    "Firefox'ta analytics.empower.net oturumu açın, sonra tekrar Tara"
                )
            url = _goto_creative_review(driver)
        finally:
            if login_only:
                release_profile_login_lock(PROFILE_DIR)

    if "creative-review" not in (url or "").lower():
        url = _goto_creative_review(driver)
    if "creative-review" not in (url or "").lower() and _needs_login(driver):
        raise RuntimeError(
            f"Creative Review açılamadı (url={url!r}). "
            "Önce Empower oturumu gerekli."
        )

    if login_only:
        return driver
    _apply_default_filters(driver)
    time.sleep(1.2)
    # Filtre sonrası sayfa yenilenebilir — URL'yi doğrula
    try:
        if "creative-review" not in (driver.current_url or "").lower():
            _goto_creative_review(driver)
            _apply_default_filters(driver)
    except Exception:
        pass
    print(f"  · hazır · {driver.current_url}", flush=True)
    return driver


def run_scan(
    *,
    keyword: str,
    ingest: bool = True,
    headed: bool = True,
    max_pages: int = 25,
) -> dict[str, Any]:
    terms = match_terms(keyword)
    if not terms:
        return {"ok": False, "message": "keyword boş", "matches": [], "scanned": 0}
    print(f"Creative Review tarama · keyword={keyword!r} terms={terms}", flush=True)
    driver = None
    try:
        driver = open_session(headed=headed)
        all_cards: dict[str, dict[str, Any]] = {}
        pages = 0
        for page_i in range(max(1, max_pages)):
            pages = page_i + 1
            time.sleep(1.2)
            batch = _extract_cards(driver)
            print(f"  · sayfa {pages}: {len(batch)} kart (200/page)", flush=True)
            new_cards = []
            for c in batch:
                if c["id"] not in all_cards:
                    all_cards[c["id"]] = c
                    new_cards.append(c)
            # Önce yalnızca yeni kartlarda OCR — 170px banner görselleri
            _enrich_cards_with_ocr(driver, new_cards)
            if page_i + 1 >= max_pages:
                break
            if not _goto_next_page(driver):
                break
            time.sleep(1.5)
        matches = [c for c in all_cards.values() if card_matches(c, terms)]
        matches.sort(key=lambda x: x.get("id") or "")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out = {
            "ok": True,
            "keyword": keyword,
            "scanned": len(all_cards),
            "pages": pages,
            "matches": matches,
            "scraped_at": now,
            "source": "empower_creative_bridge",
            "message": (
                f"Tarama · {len(matches)} eşleşme / {len(all_cards)} kart · "
                f"{pages} sayfa · «{keyword}» · OCR=banner"
            ),
        }
        print(out["message"], flush=True)
        if ingest:
            ing = _post_json(SCAN_INGEST_URL, out)
            out["ingest"] = ing
            if not ing.get("ok"):
                out["ok"] = False
                out["message"] = f"{out['message']} · ingest hata: {ing.get('message')}"
        return out
    except Exception as exc:  # noqa: BLE001
        err = {
            "ok": False,
            "keyword": keyword,
            "scanned": 0,
            "pages": 0,
            "matches": [],
            "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "empower_creative_bridge",
            "message": str(exc)[:400],
        }
        if ingest:
            _post_json(SCAN_INGEST_URL, err)
        return err
    finally:
        # KEEP_OPEN — pencereyi kapatma (scrape_browser varsayılanı)
        pass


def run_block(
    *,
    ids: list[str],
    keyword: str = "",
    ingest: bool = True,
    headed: bool = True,
    max_pages: int = 40,
) -> dict[str, Any]:
    want = {str(x).strip().lower() for x in ids if str(x).strip()}
    if not want:
        return {"ok": False, "message": "id yok", "blocked": [], "failed": []}
    print(f"Creative Review engelle · {len(want)} id", flush=True)
    try:
        driver = open_session(headed=headed)
        blocked_all: list[dict[str, Any]] = []
        failed_all: list[dict[str, Any]] = []
        remaining = set(want)
        for page_i in range(max(1, max_pages)):
            if not remaining:
                break
            time.sleep(1.0)
            # önce bu sayfadaki kartlardan eşleşenleri engelle
            page_ids = {c["id"] for c in _extract_cards(driver)} & remaining
            if page_ids:
                b, f = _block_ids_on_page(driver, page_ids)
                blocked_all.extend(b)
                for row in f:
                    # sonraki sayfada tekrar dene
                    if row.get("error") == "kart/buton bulunamadı":
                        continue
                    failed_all.append(row)
                    remaining.discard(str(row.get("id") or ""))
                for row in b:
                    remaining.discard(str(row.get("id") or ""))
            if not remaining:
                break
            if not _goto_next_page(driver):
                break
            time.sleep(1.4)
        for cid in sorted(remaining):
            failed_all.append({"id": cid, "error": "sayfalarda bulunamadı"})
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out = {
            "ok": len(failed_all) == 0,
            "keyword": keyword,
            "blocked": blocked_all,
            "failed": failed_all,
            "scraped_at": now,
            "source": "empower_creative_bridge",
            "message": f"Engelleme · {len(blocked_all)} ok / {len(failed_all)} hata",
        }
        print(out["message"], flush=True)
        if ingest:
            ing = _post_json(BLOCK_INGEST_URL, out)
            out["ingest"] = ing
        return out
    except Exception as exc:  # noqa: BLE001
        err = {
            "ok": False,
            "keyword": keyword,
            "blocked": [],
            "failed": [{"id": x, "error": str(exc)[:160]} for x in sorted(want)],
            "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "empower_creative_bridge",
            "message": str(exc)[:400],
        }
        if ingest:
            _post_json(BLOCK_INGEST_URL, err)
        return err


def run_from_pending(*, ingest: bool = True, headed: bool = True) -> dict[str, Any]:
    q = f"{PENDING_URL}?pop=1"
    data = _get_json(q)
    pending = data.get("pending") if isinstance(data, dict) else None
    if not isinstance(pending, dict) or not pending.get("action"):
        return {"ok": False, "message": "bekleyen iş yok", "skipped": True}
    action = str(pending.get("action") or "").lower()
    if action == "scan":
        return run_scan(
            keyword=str(pending.get("keyword") or ""),
            ingest=ingest,
            headed=headed,
        )
    if action == "block":
        ids = pending.get("ids") if isinstance(pending.get("ids"), list) else []
        return run_block(
            ids=[str(x) for x in ids],
            keyword=str(pending.get("keyword") or ""),
            ingest=ingest,
            headed=headed,
        )
    return {"ok": False, "message": f"bilinmeyen action: {action}"}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Empower Creative Review scan/block")
    p.add_argument("--login", action="store_true")
    p.add_argument("--scan", action="store_true")
    p.add_argument("--block", action="store_true")
    p.add_argument("--pending", action="store_true", help="Railway pending işi çek")
    p.add_argument("--keyword", default="")
    p.add_argument("--ids", default="", help="virgülle id listesi")
    p.add_argument("--ingest", action="store_true")
    p.add_argument("--no-ingest", action="store_true")
    p.add_argument("--max-pages", type=int, default=40)
    p.add_argument("--headless", action="store_true")
    args = p.parse_args(argv)
    ingest = bool(args.ingest) and not bool(args.no_ingest)
    headed = not bool(args.headless)
    if args.login:
        driver = open_session(headed=True, login_only=True)
        print(f"login ok · {getattr(driver, 'current_url', '')}", flush=True)
        return 0
    if args.pending:
        out = run_from_pending(ingest=True, headed=headed)
        print(json.dumps(out, ensure_ascii=False)[:1200])
        return 0 if out.get("ok") or out.get("skipped") else 1
    if args.scan:
        out = run_scan(
            keyword=args.keyword,
            ingest=ingest or True,
            headed=headed,
            max_pages=args.max_pages,
        )
        print(json.dumps({k: out.get(k) for k in ("ok", "message", "scanned", "match_count") if k in out or k == "match_count"}, ensure_ascii=False))
        print(f"matches={len(out.get('matches') or [])}", flush=True)
        return 0 if out.get("ok") else 1
    if args.block:
        ids = [x.strip() for x in str(args.ids or "").split(",") if x.strip()]
        out = run_block(
            ids=ids,
            keyword=args.keyword,
            ingest=ingest or True,
            headed=headed,
            max_pages=args.max_pages,
        )
        print(json.dumps({"ok": out.get("ok"), "message": out.get("message")}, ensure_ascii=False))
        return 0 if out.get("ok") else 1
    p.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
