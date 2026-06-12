"""Mobil mağaza (Google Play + App Store web) yorum analitiği."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import calendar
import os
import re
import threading
import time
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, as_completed, wait
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import delete

from backend.database import SessionLocal
from backend.models import AppIntelRawCache, AppStoreRankSnapshot
from backend.services.timezone_utils import (
    inclusive_local_period_start_utc,
    report_calendar_today,
    report_calendar_tz,
)

logger = logging.getLogger(__name__)

_APP_INTEL_ROOT = Path(__file__).resolve().parent.parent.parent
_DISK_RAW_DIR = _APP_INTEL_ROOT / "data" / "app_intel_raw"

_CACHE_LOCK = threading.Lock()
_RAW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
# Normal API isteklerinde mağaza kaynaklarını tekrar tekrar çağırmamak için uzun TTL.
# Zorunlu güncelleme sadece manuel tetikleme ve zamanlanmış job'da force_refresh ile yapılır.
_CACHE_TTL_SEC = 26 * 60 * 60
_FORCED_REFRESH_META_FILE = Path(__file__).resolve().parent / "app_intel_last_refresh.json"
_RANK_HISTORY_FILE = Path(__file__).resolve().parent / "app_intel_rank_history.json"
_FORCED_REFRESH_AT: dict[str, str] = {}
_RANK_HISTORY: dict[str, Any] = {}

# Google Play: continuation token ile sayfalama (çok büyük değerler ilk yüklemeyi ve google-play-scraper takılmalarını uzatır).
GOOGLE_PLAY_MAX_REVIEWS = 600

# Paralel vitrin+Play yorum fazı: tek süre üst sınırı; aksi halde HTTP isteği 10+ dk sürebiliyor.
_APP_INTEL_FETCH_PHASE_TIMEOUT_SEC = 150.0


def _railway_fast_mode() -> bool:
    try:
        from backend.config import is_railway_runtime

        return bool(is_railway_runtime())
    except Exception:
        return False


def _fetch_phase_timeout_sec() -> float:
    # Railway / ters vekil çoğu zaman ~60s üzerinde yanıt keser; kısmi veri + disk cache şart.
    # Sonrasında sıra çekimi için pay bırak (aşağıdaki _bounded_rank_call).
    return 38.0 if _railway_fast_mode() else _APP_INTEL_FETCH_PHASE_TIMEOUT_SEC


def _store_rank_call_budget_sec() -> float:
    """Play HTTP sıra fallback'i için tavan."""
    return 10.0 if _railway_fast_mode() else 55.0


def _ios_store_rank_call_budget_sec() -> float:
    """App Store charts + RSS sıra turu; Railway'de proxy payı için kısa tavan."""
    return 12.0 if _railway_fast_mode() else 55.0


def _bounded_rank_call(fn, timeout_sec: float) -> Any:
    """İsteğe bağlı sıra çağrılarını sınırla; asıl mağaza+yorum verisini geciktirmesin."""
    if timeout_sec <= 0:
        return None
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(fn)
        try:
            return fut.result(timeout=timeout_sec)
        except TimeoutError:
            logger.warning(
                "app_intel: kategori sırası çağrısı %.0fs içinde tamamlanamadı",
                timeout_sec,
            )
            return None
        except Exception as exc:
            logger.warning("app_intel: kategori sırası çağrısı hata: %s", exc)
            return None


def _play_review_cap() -> int:
    return 300 if _railway_fast_mode() else GOOGLE_PLAY_MAX_REVIEWS


def _skip_android_playwright_rank() -> bool:
    """Railway'de varsayılan kapalı (süre); canlı boşsa DB'deki son gerçek snapshot kullanılır.

    Tam kategori listesi taraması için: APP_INTEL_ALLOW_PLAYWRIGHT_ON_RAILWAY=1
    """
    if (os.environ.get("APP_INTEL_ALLOW_PLAYWRIGHT_ON_RAILWAY") or "").strip().lower() in ("1", "true", "yes"):
        return False
    return _railway_fast_mode()


def _ios_review_storefronts() -> tuple[str, ...]:
    if _railway_fast_mode():
        return ("tr", "us", "gb", "de", "fr", "nl")
    return _IOS_STOREFRONTS


def _ios_lookup_countries() -> tuple[str, ...]:
    if _railway_fast_mode():
        return ("tr", "us", "gb", "de", "fr", "nl", "se", "jp")
    return _IOS_STOREFRONTS


def _disk_raw_path(product_id: str) -> Path:
    return _DISK_RAW_DIR / f"{product_id}.json"


def _hydrate_raw_payload(data: dict[str, Any]) -> dict[str, Any]:
    def fix_rows(rows: list[Any]) -> None:
        for r in rows:
            if not isinstance(r, dict):
                continue
            at = r.get("at")
            if isinstance(at, str):
                try:
                    iso = at.replace("Z", "+00:00") if at.endswith("Z") else at
                    r["at"] = datetime.fromisoformat(iso)
                except ValueError:
                    pass

    for key in ("android", "ios"):
        block = data.get(key)
        if not isinstance(block, dict):
            continue
        revs = block.get("reviews")
        if isinstance(revs, list):
            fix_rows(revs)
    return data


def _load_disk_raw(product_id: str) -> dict[str, Any] | None:
    p = _disk_raw_path(product_id)
    if not p.is_file():
        return None
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("product_id") != product_id:
            return None
        return _hydrate_raw_payload(raw)
    except Exception as exc:
        logger.warning("app_intel disk okunamadı (%s): %s", product_id, exc)
        return None


def _serialize_raw_payload(payload: dict[str, Any]) -> str:
    def _safe(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, dict):
            return {k: _safe(v) for k, v in o.items()}
        if isinstance(o, list):
            return [_safe(v) for v in o]
        if isinstance(o, float):
            return float(o) if o == o else None
        return o

    return json.dumps(_safe(payload), ensure_ascii=False)


def _write_db_raw(product_id: str, json_text: str) -> None:
    """Postgres'te ham payload; Railway çoklu dyno / ephemeral disk için."""
    try:
        db = SessionLocal()
        try:
            row = db.get(AppIntelRawCache, product_id)
            now = datetime.utcnow()
            if row:
                row.payload_json = json_text
                row.updated_at = now
            else:
                db.add(AppIntelRawCache(product_id=product_id, payload_json=json_text, updated_at=now))
            db.commit()
        finally:
            db.close()
    except Exception as exc:
        logger.warning("app_intel db önbellek yazılamadı (%s): %s", product_id, exc)


def _load_db_raw_with_age(product_id: str, now: float) -> tuple[dict[str, Any] | None, float | None]:
    """Disk yoksa veya okunamıyorsa Postgres'ten dön; (payload, age_seconds) veya (None, None)."""
    try:
        db = SessionLocal()
        try:
            row = db.get(AppIntelRawCache, product_id)
            if not row or not row.payload_json:
                return (None, None)
            raw = json.loads(row.payload_json)
            if not isinstance(raw, dict) or raw.get("product_id") != product_id:
                return (None, None)
            ut = row.updated_at
            if ut is None:
                return (None, None)
            if ut.tzinfo is None:
                written = float(calendar.timegm(ut.timetuple()))
            else:
                written = ut.timestamp()
            age = max(0.0, now - written)
            return (_hydrate_raw_payload(raw), age)
        finally:
            db.close()
    except Exception as exc:
        logger.debug("app_intel db önbellek okunamadı (%s): %s", product_id, exc)
        return (None, None)


def _write_disk_raw(product_id: str, payload: dict[str, Any]) -> None:
    text = _serialize_raw_payload(payload)
    try:
        _DISK_RAW_DIR.mkdir(parents=True, exist_ok=True)
        p = _disk_raw_path(product_id)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(p)
    except Exception as exc:
        logger.warning("app_intel disk yazılamadı (%s): %s", product_id, exc)
        return
    _write_db_raw(product_id, text)


def prewarm_app_intel_cache_background() -> None:
    """Deploy / cold start sonrası disk+RAM doldurur; kullanıcı isteği proxy süresinde takılmasın.
    cache_only=True: sadece mevcut disk verisini RAM'e yükler, mağaza scraping yapmaz."""

    def _runner() -> None:
        time.sleep(2.5)
        for pid in list(APP_PRODUCTS.keys()):
            try:
                get_raw_product_data(pid, force_refresh=False, cache_only=True)
                logger.info("app_intel prewarm bitti: %s", pid)
            except Exception as exc:
                logger.warning("app_intel prewarm başarısız (%s): %s", pid, exc)

    threading.Thread(target=_runner, name="app-intel-prewarm", daemon=True).start()

# App Store web: tek vitrin ~50 yorum; birçok ülke vitrini birleştirerek geçmiş artar.
_IOS_STOREFRONTS: tuple[str, ...] = (
    "tr",
    "us",
    "gb",
    "de",
    "fr",
    "nl",
    "se",
    "no",
    "dk",
    "fi",
    "it",
    "pt",
    "pl",
    "ru",
    "cz",
    "ro",
    "ie",
    "at",
    "ch",
    "be",
    "jp",
    "kr",
    "au",
    "nz",
    "ca",
    "br",
    "mx",
    "in",
    "ae",
    "sa",
    "sg",
    "hk",
    "tw",
    "th",
    "vn",
    "id",
    "my",
    "ph",
)

APP_PRODUCTS: dict[str, dict[str, str]] = {
    "doviz": {
        "label": "Döviz",
        "crashlytics_bigquery": True,
        "android_package": "com.Doviz",
        # BigQuery’de tablo adı: bundle id noktaları → alt çizgi (örn. com.nokta.Finans.Takip → com_nokta_Finans_Takip).
        # Firebase'deki gerçek iOS bundle id "com.nokta.Finans.Takip" (App Store: "Döviz - Kur, Altın, Borsa, Koin").
        "ios_bundle_id": "com.nokta.Finans.Takip",
        "android_url": "https://play.google.com/store/apps/details?id=com.Doviz&hl=tr",
        "ios_app_id": "465599322",
        "ios_slug": "d%C3%B6viz-kur-alt%C4%B1n-borsa-koin",
        "ios_url": "https://apps.apple.com/tr/app/d%C3%B6viz-kur-alt%C4%B1n-borsa-koin/id465599322",
    },
    "sinemalar": {
        "label": "Sinemalar",
        "android_package": "com.nokta.sinemalar",
        "ios_bundle_id": "com.nokta.sinemalar",
        "android_url": "https://play.google.com/store/apps/details?id=com.nokta.sinemalar&hl=tr",
        "ios_app_id": "711475888",
        "ios_slug": "sinemalar-com-vizyon-filmleri",
        "ios_url": "https://apps.apple.com/tr/app/sinemalar-com-vizyon-filmleri/id711475888",
    },
}

# (category_id, Türkçe etiket, anahtar kelimeler küçük harf)
_REVIEW_CATEGORIES: list[tuple[str, str, tuple[str, ...]]] = [
    ("reklam", "Reklam / IAP", ("reklam", "reklamlı", "ads", "advertisement", "içi satın", "in-app purchase")),
    ("performans", "Performans / hata", ("hata", "çök", "crash", "donma", "yavaş", "açılmıyor", "bug", "update", "güncelle")),
    ("bildirim", "Bildirim / alarm", ("bildirim", "notification", "alarm", "push")),
    ("arama", "Arama / keşif", ("arama", "search", "bulamıyorum", "bulamadım", "listem")),
    ("arayuz", "Arayüz / UX", ("tasarım", "arayüz", "arayuz", "ui", "karanlık mod", "dark mode", "çözünürlük")),
    ("dogruluk", "Veri / doğruluk", ("yanlış", "doğru değil", "güncel değil", "eksik veri", "sinema yoktu")),
    ("ozellik", "Özellik isteği", ("özellik", "ekleyin", "ekler misiniz", "olsun", "feature", "lütfen")),
    ("olumlu", "Olumlu (genel)", ("teşekkür", "süper", "mükemmel", "harika", "başarılı", "çok iyi", "great", "love", "perfect")),
    ("olumsuz", "Olumsuz (genel)", ("kötü", "berbat", "rezalet", "kullanılmaz", "sil", "waste", "terrible", "annoying")),
]

_UTC = timezone.utc


def _play_updated_iso(value: Any) -> str | None:
    """google-play-scraper `updated` alanını JSON için ISO UTC string yap."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=_UTC)
        return dt.astimezone(_UTC).isoformat()
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=_UTC).isoformat()
        except Exception:
            return None
    s = str(value).strip()
    return s if s else None


def _load_forced_refresh_meta() -> None:
    global _FORCED_REFRESH_AT
    try:
        if not _FORCED_REFRESH_META_FILE.exists():
            _FORCED_REFRESH_AT = {}
            return
        data = json.loads(_FORCED_REFRESH_META_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _FORCED_REFRESH_AT = {str(k): str(v) for k, v in data.items() if v}
        else:
            _FORCED_REFRESH_AT = {}
    except Exception:
        _FORCED_REFRESH_AT = {}


def _save_forced_refresh_meta() -> None:
    try:
        _FORCED_REFRESH_META_FILE.write_text(
            json.dumps(_FORCED_REFRESH_AT, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.debug("App intel forced refresh metadata kaydedilemedi.")


def _load_rank_history() -> None:
    # Backward-compat: legacy dosya geçmişi artık kullanılmıyor (DB kullanılıyor).
    global _RANK_HISTORY
    _RANK_HISTORY = {}


def _save_rank_history() -> None:
    # Backward-compat no-op: yeni geçmiş yazımı veritabanına yapılıyor.
    return


def _parse_utc_iso(at_iso: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(at_iso or "").replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(tz=_UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def _rank_row_to_dict(row: AppStoreRankSnapshot) -> dict[str, Any]:
    dt = row.collected_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)
    return {
        "at": dt.astimezone(_UTC).isoformat(),
        "rank": int(row.rank),
        "total": row.total,
        "category": row.category_name,
        "chart": row.chart,
    }


def _latest_stored_category_rank(product_id: str, platform: str) -> dict[str, Any] | None:
    """Veritabanındaki son gerçek sıra kaydı (cron / önceki Playwright turu). Canlı çekim boşsa doldurur."""
    if platform not in ("android", "ios"):
        return None
    try:
        with SessionLocal() as db:
            rows = (
                db.query(AppStoreRankSnapshot)
                .filter(
                    AppStoreRankSnapshot.product_id == product_id,
                    AppStoreRankSnapshot.platform == platform,
                )
                .order_by(AppStoreRankSnapshot.collected_at.desc(), AppStoreRankSnapshot.id.desc())
                .limit(48)
                .all()
            )
    except Exception as exc:
        logger.debug("Son sıra snapshot okunamadı (%s/%s): %s", product_id, platform, exc)
        return None
    row = None
    for r in rows or []:
        ch = str(r.chart or "").strip().lower()
        # Eski sürümler paket-id ile Play aramasından #1 üretip DB'ye yazıyordu; kategori sırası değil.
        if platform == "android" and ch in (
            "category_dom_only",
            "store_search_package",
        ):
            continue
        row = r
        break
    if row is None:
        return None
    chart = (str(row.chart or "").strip() or None) or ("category_top" if platform == "android" else None)
    out: dict[str, Any] = {
        "rank": int(row.rank),
        "total": row.total,
        "chart": chart,
        "chart_label": "Kategori",
        "category_name": row.category_name,
        "rank_source": "db_snapshot",
    }
    if platform == "ios" and row.chart:
        out["chart_label"] = _CHART_LABELS.get(str(row.chart), str(row.chart))
    return out


def _load_rank_rows(product_id: str, platform: str) -> list[dict[str, Any]]:
    try:
        with SessionLocal() as db:
            rows = (
                db.query(AppStoreRankSnapshot)
                .filter(
                    AppStoreRankSnapshot.product_id == product_id,
                    AppStoreRankSnapshot.platform == platform,
                )
                .order_by(AppStoreRankSnapshot.collected_at.asc(), AppStoreRankSnapshot.id.asc())
                .all()
            )
        return [_rank_row_to_dict(r) for r in rows]
    except Exception as e:
        logger.debug("Rank history DB okunamadi (%s/%s): %s", product_id, platform, e)
        return (((_RANK_HISTORY.get(product_id) or {}).get(platform)) or [])


def _append_rank_snapshot(
    product_id: str,
    platform: str,
    rank_info: dict[str, Any] | None,
    *,
    at_iso: str,
) -> None:
    if platform not in ("android", "ios"):
        return
    if not rank_info:
        return
    rank_val = rank_info.get("rank")
    if rank_val is None:
        return
    if str(rank_info.get("rank_basis") or "") == "env_override":
        return
    try:
        rank_int = int(rank_val)
    except Exception:
        return
    rec = {
        "at": at_iso,
        "rank": rank_int,
        "total": rank_info.get("total"),
        "category": rank_info.get("category_name"),
        "chart": rank_info.get("chart"),
    }
    arr = _load_rank_rows(product_id, platform)
    last = arr[-1] if arr else None
    should_push = False
    if not last:
        should_push = True
    else:
        last_rank = int(last.get("rank") or -1)
        last_at = str(last.get("at") or "")
        # Sıra değiştiyse her zaman kaydet
        if last_rank != rank_int:
            should_push = True
        # Kategori değiştiyse kaydet
        elif str(last.get("category") or "") != str(rec.get("category") or ""):
            should_push = True
        else:
            # Aynı sıra — son kayıttan en az 3 saat geçtiyse yeniden kaydet
            try:
                last_dt = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=_UTC)
                now_dt = _parse_utc_iso(at_iso)
                if now_dt.tzinfo is None:
                    now_dt = now_dt.replace(tzinfo=_UTC)
                if (now_dt - last_dt).total_seconds() >= 3 * 3600:
                    should_push = True
            except Exception:
                # Tarih parse edilemezse gün bazında kontrol
                if last_at[:10] != at_iso[:10]:
                    should_push = True
    if not should_push:
        return
    try:
        with SessionLocal() as db:
            db.add(
                AppStoreRankSnapshot(
                    product_id=product_id,
                    platform=platform,
                    rank=rank_int,
                    total=int(rec.get("total")) if rec.get("total") is not None else None,
                    category_name=(str(rec.get("category") or "").strip() or None),
                    chart=(str(rec.get("chart") or "").strip() or None),
                    collected_at=_parse_utc_iso(at_iso).replace(tzinfo=None),
                )
            )
            db.commit()
    except Exception as e:
        logger.debug("Rank snapshot DB kaydedilemedi (%s/%s): %s", product_id, platform, e)


def _at_report_tz_date(at: datetime) -> date:
    dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
    return dt.astimezone(report_calendar_tz()).date()


def _rank_history_series(product_id: str, platform: str, *, days: int = 7) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    start = inclusive_local_period_start_utc(n_calendar_days=days)
    if start is None:
        return []
    out: list[dict[str, Any]] = []
    for x in arr:
        at_s = str(x.get("at") or "")
        try:
            dt = datetime.fromisoformat(at_s.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_UTC)
        if dt < start:
            continue
        out.append(
            {
                "at": at_s,
                "rank": x.get("rank"),
                "total": x.get("total"),
                "category": x.get("category"),
            }
        )
    return out[-200:]


def _rank_history_changes(product_id: str, platform: str, *, limit: int = 50) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    out: list[dict[str, Any]] = []
    prev_rank: int | None = None
    for x in arr:
        try:
            cur = int(x.get("rank"))
        except Exception:
            continue
        if prev_rank is None or cur != prev_rank:
            out.append(
                {
                    "at": x.get("at"),
                    "rank": cur,
                    "total": x.get("total"),
                    "category": x.get("category"),
                }
            )
        prev_rank = cur
    return out[-limit:]


def _rank_history_daily(product_id: str, platform: str, *, days: int = 30) -> list[dict[str, Any]]:
    arr = _load_rank_rows(product_id, platform)
    if not arr:
        return []
    start_d = report_calendar_today() - timedelta(days=days - 1)
    by_day: dict[str, dict[str, Any]] = {}
    for x in arr:
        at_s = str(x.get("at") or "")
        try:
            dt = datetime.fromisoformat(at_s.replace("Z", "+00:00"))
        except Exception:
            continue
        if _at_report_tz_date(dt) < start_d:
            continue
        day_key = _at_report_tz_date(dt).isoformat()
        prev = by_day.get(day_key)
        if prev is None or str(prev.get("at") or "") < at_s:
            by_day[day_key] = {
                "at": at_s,
                "rank": x.get("rank"),
                "total": x.get("total"),
                "category": x.get("category"),
            }
    out = [by_day[k] for k in sorted(by_day.keys())]
    return out[-days:]


_load_forced_refresh_meta()
_load_rank_history()


def list_products() -> list[dict[str, str]]:
    return [{"id": k, "label": v["label"]} for k, v in APP_PRODUCTS.items()]


def _normalize_review_text(text: str) -> str:
    src = (text or "").strip()
    if not src:
        return ""
    markers = ("Ã", "Ä", "Å", "â", "Ð", "Þ", "�")

    def score(s: str) -> tuple[int, int]:
        bad = sum(s.count(m) for m in markers)
        return bad, len(s)

    candidates = [src]
    for enc in ("latin1", "cp1252"):
        try:
            repaired = src.encode(enc).decode("utf-8")
            candidates.append(repaired)
        except Exception:
            continue

    best = min(candidates, key=score)
    return " ".join(best.split())


def _parse_ios_review_page(html: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """App Store 'see-all=reviews' HTML içinden yorum + mağaza rozet özeti."""
    reviews_out: list[dict[str, Any]] = []
    pat = re.compile(
        r'"date":"([0-9]{4}-[0-9]{2}-[0-9]{2}T[^"]+)".{0,800}?"contents":"([^"]{0,12000})","rating":([1-5])',
        re.DOTALL,
    )
    for m in pat.finditer(html):
        raw_date, raw_body, r_s = m.group(1), m.group(2), m.group(3)
        try:
            body = raw_body.encode("utf-8").decode("unicode_escape")
        except Exception:
            body = raw_body.replace('\\"', '"').replace("\\n", "\n")
        try:
            if raw_date.endswith("Z"):
                dt = datetime.strptime(raw_date[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=_UTC)
            else:
                dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            continue
        # App Store review JSON parçalarında sürüm sinyali geçebiliyor (userReviewVersion / softwareVersion).
        snippet_start = max(0, m.start() - 400)
        snippet_end = min(len(html), m.end() + 400)
        snippet = html[snippet_start:snippet_end]
        ver: str | None = None
        mv = re.search(r'"(?:userReviewVersion|softwareVersion|reviewVersion)"\s*:\s*"([^"]+)"', snippet)
        if mv:
            cand = str(mv.group(1)).strip()
            if cand:
                ver = cand
        reviews_out.append({"at": dt, "score": int(r_s), "text": _normalize_review_text(body), "version": ver})

    snap: dict[str, Any] = {}
    m_badge = re.search(
        r'"type":"rating"[^}]*"content":\{"rating":([0-9.]+),"ratingFormatted":"([0-9.]+)"\}[^}]*"heading":"([^"]+)"',
        html,
    )
    if m_badge:
        snap["score"] = float(m_badge.group(1))
        snap["score_formatted"] = m_badge.group(2)
        snap["ratings_caption"] = m_badge.group(3)
    # Total ratings count from dedicated count fields
    for cnt_pat in (
        r'"ratingCount"\s*:\s*([0-9]+)',
        r'"userRatingCount"\s*:\s*([0-9]+)',
    ):
        m_cnt = re.search(cnt_pat, html)
        if m_cnt:
            try:
                snap["ratings_count"] = int(m_cnt.group(1))
                break
            except Exception:
                pass
    m_icon = re.search(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', html)
    if m_icon:
        snap["icon"] = m_icon.group(1)
    return reviews_out, snap


def _fetch_ios_ssr_ratings(
    app_id: str,
    ios_slug: str,
    *,
    country: str = "tr",
) -> dict[str, Any] | None:
    """App Store uygulama sayfasının SSR JSON'undan ülkeye özgü puan ve dağılımı çeker.

    Sıralama için kullandığımız SSR scrape yöntemiyle aynı prensip:
    HTML içindeki ``<script type="application/json">`` bloğunu parse eder.
    Dönen yapı:
      {
        "score": 4.8,
        "ratings_count": 1844,
        "star_histogram": {"1": 31, "2": 10, "3": 46, "4": 137, "5": 1620},
      }
    Tüm değerler belirtilen ülkeye (country) özgüdür; global toplama yapılmaz.
    """
    url = f"https://apps.apple.com/{country}/app/{ios_slug}/id{app_id}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        )
    }
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
        html = r.text
    except Exception as exc:
        logger.debug("iOS SSR ratings fetch hatası (%s, %s): %s", app_id, country, exc)
        return None

    # SSR JSON bloğunu parse et
    m_json = re.search(
        r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not m_json:
        logger.debug("iOS SSR ratings: JSON bloğu bulunamadı (%s)", app_id)
        return _fetch_ios_main_page_histogram_single(app_id, ios_slug, country)

    try:
        page_data = json.loads(m_json.group(1))
        ratings_item = (
            page_data["data"][0]["data"]["shelfMapping"]["productRatings"]["items"][0]
        )
        score = float(ratings_item["ratingAverage"])
        ratings_count = int(ratings_item["totalNumberOfRatings"])
        counts_raw: list[int] = ratings_item["ratingCounts"]  # [5★, 4★, 3★, 2★, 1★]
        if len(counts_raw) != 5 or sum(counts_raw) == 0:
            raise ValueError("ratingCounts geçersiz")
        star_histogram = {
            "5": counts_raw[0],
            "4": counts_raw[1],
            "3": counts_raw[2],
            "2": counts_raw[3],
            "1": counts_raw[4],
        }
        logger.info(
            "iOS SSR ratings (%s, %s): %.2f★ %d puan – 5★=%d 4★=%d 3★=%d 2★=%d 1★=%d",
            app_id, country.upper(), score, ratings_count,
            star_histogram["5"], star_histogram["4"], star_histogram["3"],
            star_histogram["2"], star_histogram["1"],
        )
        return {"score": score, "ratings_count": ratings_count, "star_histogram": star_histogram}
    except Exception as exc:
        logger.debug("iOS SSR ratings JSON parse hatası (%s): %s", app_id, exc)
        # Fallback: regex ile tek vitrin
        return _fetch_ios_main_page_histogram_single(app_id, ios_slug, country)


def _fetch_ios_main_page_histogram_single(
    app_id: str,
    ios_slug: str,
    country: str = "tr",
) -> dict[str, Any] | None:
    """SSR JSON başarısız olursa regex fallback — sadece tek ülke."""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    _pat = re.compile(
        r'"ratingCounts"\s*:\s*\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*\]'
    )
    url = f"https://apps.apple.com/{country}/app/{ios_slug}/id{app_id}"
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
        m = _pat.search(r.text)
        if not m:
            return None
        counts = [int(m.group(i)) for i in range(1, 6)]
        total = sum(counts)
        if total == 0:
            return None
        star_histogram = {"5": counts[0], "4": counts[1], "3": counts[2], "2": counts[3], "1": counts[4]}
        logger.info("iOS regex histogram (%s, %s): toplam=%d", app_id, country, total)
        return {"star_histogram": star_histogram}
    except Exception as exc:
        logger.debug("iOS regex histogram hatası (%s, %s): %s", app_id, country, exc)
        return None


def _fetch_ios_main_page_histogram(app_id: str, ios_slug: str) -> dict[str, int] | None:
    """Geriye dönük uyumluluk için bırakıldı. Artık `_fetch_ios_ssr_ratings` kullanılıyor."""
    result = _fetch_ios_ssr_ratings(app_id, ios_slug, country="tr")
    return result.get("star_histogram") if result else None


def _ios_review_key(at: datetime, text: str, score: int) -> str:
    payload = f"{at.isoformat()}\0{text}\0{score}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def _fetch_ios_rss_reviews(
    app_id: str,
    *,
    country: str = "tr",
    max_pages: int = 5,
) -> list[dict[str, Any]]:
    """iTunes RSS feed'inden en son reviewları tarih sıralı çeker.

    ``sortby=mostrecent`` ile sayfa sayfa giderek max_pages * 50 yorum döner.
    Dönen her yorum: {"at": datetime, "score": int, "text": str}.
    Zaman filtreli histogram hesaplamak için kullanılır.
    """
    headers = {"User-Agent": "iTunes/12.0 (Macintosh; OS X 10.15.7)"}
    reviews: list[dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        url = (
            f"https://itunes.apple.com/rss/customerreviews"
            f"/page={page}/id={app_id}/sortby=mostrecent/json"
        )
        try:
            with httpx.Client(timeout=12.0, follow_redirects=True, headers=headers) as client:
                r = client.get(url, params={"l": country, "cc": country})
                r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
        except Exception as exc:
            logger.debug("iTunes RSS reviews hata (page=%d, %s): %s", page, app_id, exc)
            break

        if not entries:
            break

        for e in entries:
            try:
                score = int(e.get("im:rating", {}).get("label", 0) or 0)
                if not (1 <= score <= 5):
                    continue
                date_str = (e.get("updated", {}).get("label") or "")[:10]
                at = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=_UTC)
                text = e.get("content", {}).get("label") or e.get("title", {}).get("label") or ""
                reviews.append({"at": at, "score": score, "text": text.strip(), "source": "rss"})
            except Exception:
                continue

    logger.info("iTunes RSS reviews (%s, %s): %d yorum çekildi", app_id, country.upper(), len(reviews))
    return reviews


def _fetch_ios_one_storefront(
    app_id: str, ios_slug: str, loc: str,
) -> tuple[str, list[dict[str, Any]], dict[str, Any], bool, str | None]:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    url = f"https://apps.apple.com/{loc}/app/{ios_slug}/id{app_id}"
    params = {"see-all": "reviews"}
    try:
        with httpx.Client(timeout=16.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
        revs, page_snap = _parse_ios_review_page(r.text)
        return loc, revs, page_snap, True, None
    except Exception as e:
        return loc, [], {}, False, str(e)


def _fetch_ios_reviews_multistore(
    app_id: str, ios_slug: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None, int, int]:
    """Birden fazla ülke mağaza sayfasından yorum birleştir (kimlik: tarih+metin+puan)."""
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    snap: dict[str, Any] = {}
    storefronts_ok = 0
    last_err: str | None = None
    storefronts = _ios_review_storefronts()
    n_sf = len(storefronts)
    # Apple rate limit'i aşmamak için eşzamanlı istek sayısı sınırlı tutulur.
    max_workers = min(4, n_sf)
    by_loc: dict[str, tuple[list[dict[str, Any]], dict[str, Any], bool, str | None]] = {}

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_loc = {
                pool.submit(_fetch_ios_one_storefront, app_id, ios_slug, loc): loc
                for loc in storefronts
            }
            for fut in as_completed(future_to_loc):
                loc = future_to_loc[fut]
                _loc, revs, page_snap, ok, one_err = fut.result()
                by_loc[loc] = (revs, page_snap, ok, one_err)
    except Exception as e:
        last_err = str(e)
        logger.warning("App Store çoklu vitrin hatası (%s): %s", app_id, e)

    for loc in storefronts:
        if loc not in by_loc:
            continue
        revs, page_snap, ok, one_err = by_loc[loc]
        if ok:
            storefronts_ok += 1
            if page_snap and not snap:
                snap = page_snap
            for rv in revs:
                k = _ios_review_key(rv["at"], rv["text"], rv["score"])
                if k in seen:
                    continue
                seen.add(k)
                merged.append(rv)
        elif one_err:
            last_err = one_err
            logger.debug("App Store vitrin atlandı (%s %s): %s", loc, app_id, one_err)

    err: str | None = None if merged else last_err
    return merged, snap, err, storefronts_ok, n_sf


def normalize_ios_app_id(raw: str | None) -> str:
    """App Store uygulama id'si: URL/kullanıcı girişi 'id465599322', 'ID 465 ...' → '465599322'.

    iTunes lookup `id=` parametresi yalnızca rakam kabul eder; önek verilirse sonuç 0 gelir.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"^id\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+", "", s)
    if re.fullmatch(r"\d+", s):
        return s
    m = re.search(r"\d{6,}", s)
    return m.group(0) if m else ""


def _fetch_ios_lookup_meta(app_id: str) -> dict[str, Any]:
    """iTunes lookup API'den çoklu ülke toplanmış all-time rating count al."""
    app_id = normalize_ios_app_id(app_id)
    if not app_id:
        return {}
    url = "https://itunes.apple.com/lookup"

    primary_genre_id: int | None = None
    primary_genre_name: str | None = None
    icon_candidate: str | None = None
    version_candidate: str | None = None
    cvrd_candidate: str | None = None

    def one(country: str) -> tuple[int, float | None, int | None, str | None, str | None, str | None, str | None]:
        try:
            with httpx.Client(timeout=10.0, follow_redirects=True) as client:
                r = client.get(url, params={"id": app_id, "country": country})
                r.raise_for_status()
                data = r.json()
            first = ((data or {}).get("results") or [])[0] or {}
        except Exception:
            return 0, None, None, None, None, None, None
        cnt_raw = first.get("userRatingCount")
        if cnt_raw is None:
            cnt_raw = first.get("userRatingCountForCurrentVersion")
        score_raw = first.get("averageUserRating")
        if score_raw is None:
            score_raw = first.get("averageUserRatingForCurrentVersion")
        try:
            cnt = int(cnt_raw or 0)
        except Exception:
            cnt = 0
        try:
            score = float(score_raw) if score_raw is not None else None
        except Exception:
            score = None
        gid = first.get("primaryGenreId")
        gname = first.get("primaryGenreName")
        try:
            gid_i = int(gid) if gid is not None else None
        except Exception:
            gid_i = None
        art = first.get("artworkUrl512") or first.get("artworkUrl100") or first.get("artworkUrl60")
        art_s = str(art).strip() if art else None
        ver_s = str(first.get("version")).strip() if first.get("version") else None
        cvrd_s = str(first.get("currentVersionReleaseDate")).strip() if first.get("currentVersionReleaseDate") else None
        return cnt, score, gid_i, (str(gname).strip() if gname else None), art_s, ver_s, cvrd_s

    total_count = 0
    weighted_sum = 0.0
    _countries = _ios_lookup_countries()
    with ThreadPoolExecutor(max_workers=min(12, len(_countries))) as pool:
        futs = [pool.submit(one, c) for c in _countries]
        for fut in futs:
            cnt, score, gid_i, gname, art_s, ver_s, cvrd_s = fut.result()
            if cnt > 0:
                total_count += cnt
                if score is not None:
                    weighted_sum += cnt * score
            if primary_genre_id is None and gid_i is not None:
                primary_genre_id = gid_i
            if primary_genre_name is None and gname:
                primary_genre_name = gname
            if icon_candidate is None and art_s:
                icon_candidate = art_s
            if version_candidate is None and ver_s:
                version_candidate = ver_s
            if cvrd_candidate is None and cvrd_s:
                cvrd_candidate = cvrd_s

    out: dict[str, Any] = {}
    if total_count > 0:
        out["ratings_count"] = total_count
        if weighted_sum > 0:
            out["score"] = round(weighted_sum / total_count, 5)
    if primary_genre_id is not None:
        out["primary_genre_id"] = primary_genre_id
    if primary_genre_name:
        out["primary_genre_name"] = primary_genre_name
    if icon_candidate:
        out["icon"] = icon_candidate
    if version_candidate:
        out["version"] = version_candidate
    if cvrd_candidate:
        out["currentVersionReleaseDate"] = cvrd_candidate
    return out


_CHART_LABELS: dict[str, str] = {
    "top-free": "Ücretsiz",
    "top-paid": "Ücretli",
    "top-grossing": "En Çok Kazanan",
    # Legacy RSS chart names
    "topfreeapplications": "Ücretsiz",
    "topgrossingapplications": "En Çok Kazanan",
    "toppaidapplications": "Ücretli",
}

_IOS_CHARTS_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


def _fetch_ios_category_rank(
    app_id: str,
    *,
    country: str = "tr",
    genre_id: int | None = None,
) -> dict[str, Any] | None:
    """App Store charts sayfasının SSR JSON'undan gerçek sıralamayı çek.

    Apple App Store charts sayfası (https://apps.apple.com/{country}/iphone/charts/{genre_id})
    SSR HTML'inde iki veri kaynağı barındırır:
      - shelves[].items  : sayfada ilk görünen (sparseLimit=25) uygulamalar
      - nextPage.remainingContent : geri kalan uygulamalar (toplam limit=200)
    İkisi birleşince tüm sıralama elde edilir; RSS'in 100 uygulama sınırı yoktur.

    genre_id yoksa veya sayfadan sıra bulunamazsa RSS fallback çalışır.
    """
    if not app_id:
        return None

    app_id_str = str(app_id).strip()

    # ------- 1) App Store charts sayfası scrape (birincil yöntem) -------
    if genre_id:
        try:
            url = f"https://apps.apple.com/{country}/iphone/charts/{genre_id}"
            with httpx.Client(timeout=20.0, follow_redirects=True) as client:
                r = client.get(url, headers={"User-Agent": _IOS_CHARTS_UA})
                r.raise_for_status()
            html = r.text

            # SSR JSON bloğunu bul
            json_match = re.search(
                r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
                html, re.DOTALL
            )
            if json_match:
                page_data = json.loads(json_match.group(1))
                segments = (
                    (page_data.get("data") or [{}])[0]
                    .get("data", {})
                    .get("segments", [])
                )
                all_charts: dict[str, dict[str, Any]] = {}

                for segment in segments:
                    chart_key = segment.get("chart", "")  # "top-free", "top-paid", ...
                    # İlk görünen uygulamalar (shelves)
                    shelf_items: list[str] = []
                    for shelf in segment.get("shelves", []):
                        for item in shelf.get("items", []):
                            if isinstance(item, dict):
                                shelf_items.append(str(item.get("id", "")))
                    # Geri kalan uygulamalar
                    remaining_items: list[str] = [
                        str(item.get("id", ""))
                        for item in segment.get("nextPage", {}).get("remainingContent", [])
                        if isinstance(item, dict)
                    ]
                    all_app_ids = shelf_items + remaining_items

                    if app_id_str in all_app_ids:
                        rank = all_app_ids.index(app_id_str) + 1
                        total = len(all_app_ids)
                        all_charts[chart_key] = {
                            "rank": rank,
                            "total": total,
                            "chart": chart_key,
                            "chart_label": _CHART_LABELS.get(chart_key, chart_key),
                            "scope": "category",
                        }
                        logger.info(
                            "iOS charts scrape sırası (%s, %s, genre=%s): #%d/%d",
                            app_id, chart_key, genre_id, rank, total,
                        )

                if all_charts:
                    # Birincil: top-free; yoksa ilk bulunan
                    primary_key = "top-free" if "top-free" in all_charts else next(iter(all_charts))
                    primary = dict(all_charts[primary_key])
                    if len(all_charts) > 1:
                        primary["all_charts"] = all_charts
                    return primary

                logger.debug("iOS charts scrape: uygulama bulunamadı (%s, genre=%s)", app_id, genre_id)
        except Exception as exc:
            logger.warning("iOS charts scrape hatası (%s, genre=%s): %s", app_id, genre_id, exc)

    # ------- 2) RSS fallback (top 100, genre filtreli) -------
    rss_chart_types = ("topfreeapplications", "topgrossingapplications", "toppaidapplications")

    def _search_rss(chart: str, genre: int | None) -> tuple[int | None, int]:
        try:
            if genre:
                rss_url = f"https://itunes.apple.com/{country}/rss/{chart}/genre={genre}/limit=200/json"
            else:
                rss_url = f"https://itunes.apple.com/{country}/rss/{chart}/limit=200/json"
            with httpx.Client(timeout=14.0, follow_redirects=True) as client:
                rss_r = client.get(rss_url)
                rss_r.raise_for_status()
            entries = ((rss_r.json() or {}).get("feed") or {}).get("entry") or []
            for idx, e in enumerate(entries, start=1):
                eid = str((((e.get("id") or {}).get("attributes") or {}).get("im:id") or "")).strip()
                if eid == app_id_str:
                    return idx, len(entries)
            return None, len(entries)
        except Exception:
            return None, 0

    rss_results: dict[str, dict[str, Any]] = {}
    if genre_id:
        for chart in rss_chart_types:
            rank, total = _search_rss(chart, genre_id)
            if rank is not None:
                logger.info("iOS RSS sırası (%s, %s, genre=%s): #%d/%d", app_id, chart, genre_id, rank, total)
                rss_results[chart] = {
                    "rank": rank, "total": total, "chart": chart,
                    "chart_label": _CHART_LABELS.get(chart, chart), "scope": "category",
                }

    if rss_results:
        primary_chart = "topfreeapplications" if "topfreeapplications" in rss_results else next(iter(rss_results))
        primary = dict(rss_results[primary_chart])
        if len(rss_results) > 1:
            primary["all_charts"] = rss_results
        return primary

    # ------- 3) Genel RSS (genre filtresi yok) -------
    for chart in rss_chart_types:
        rank, total = _search_rss(chart, None)
        if rank is not None:
            logger.info("iOS genel RSS sırası (%s, %s): #%d/%d", app_id, chart, rank, total)
            return {
                "rank": rank, "total": total, "chart": chart,
                "chart_label": _CHART_LABELS.get(chart, chart), "scope": "overall",
            }

    logger.debug("iOS sıra bulunamadı (%s, genre=%s)", app_id, genre_id)
    return None


def _android_play_category_slug(category_id: str | None) -> str:
    """Play mağaza URL segmenti (örn. FINANCE). Geçersizse FINANCE."""
    raw = (category_id or "FINANCE").strip().upper()
    if raw and re.fullmatch(r"[A-Z][A-Z0-9_]*", raw):
        return raw
    return "FINANCE"


def _merge_android_pkg_orders(*sequences: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for seq in sequences:
        for p in seq:
            k = (p or "").lower()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(p)
    return out


def _android_pkg_index(pkgs: list[str], target: str) -> int | None:
    t = (target or "").strip().lower()
    if not t:
        return None
    for i, p in enumerate(pkgs):
        if (p or "").strip().lower() == t:
            return i
    return None


def _fetch_android_category_rank(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
    category_id: str | None = None,
    skip_playwright: bool | None = None,
    genre_name_hint: str | None = None,
) -> dict[str, Any] | None:
    """Play kategori sırası (Playwright); yoksa detay sayfası HTML (grafik metni).

    Paket kimliğiyle `store/search` kullanılmaz: Play bu sorguda uygulamayı
    listenin başına aldığı için neredeyse her zaman #1 üretir; kategori sırası
    ile karıştırılmamalıdır.
    """

    if not package:
        return None

    slug = _android_play_category_slug(category_id)
    sp = _skip_android_playwright_rank() if skip_playwright is None else bool(skip_playwright)
    if not sp:
        result = _fetch_android_rank_playwright(
            package, country=country, lang=lang, category_slug=slug
        )
        if result is not None:
            return result

    return _fetch_android_rank_http_fallback(
        package,
        country=country,
        lang=lang,
        category_id=category_id,
        genre_name_hint=genre_name_hint,
    )


def _extract_android_packages(text: str) -> list[str]:
    """batchexecute yanıt metninden Android paket adlarını sıralı döner (tekrarsız)."""
    # Format: [\"com.package.name\" veya ["com.package.name"
    pkgs_raw = re.findall(r'\[\\"([a-zA-Z][a-zA-Z0-9._]{4,})\\"', text)
    if not pkgs_raw:
        pkgs_raw = re.findall(r'\["([a-zA-Z][a-zA-Z0-9._]{4,})"', text)
    _pkg_re = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$')
    seen: set[str] = set()
    result: list[str] = []
    for p in pkgs_raw:
        if p not in seen and _pkg_re.match(p):
            seen.add(p)
            result.append(p)
    return result


def _fetch_android_rank_playwright(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
    category_slug: str = "FINANCE",
) -> dict[str, Any] | None:
    """Kategori sayfasını (ör. FINANCE) scroll ederek DOM + batchexecute ile sıra bulur.

    Eski `/collection/top_free` URL'i 404; mağaza `category/{SLUG}` kullanıyor.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.debug("playwright kurulu değil; android rank fallback kullanılıyor")
        return None

    slug = _android_play_category_slug(category_slug)
    chart_url = f"https://play.google.com/store/apps/category/{slug}?hl={lang}&gl={country}"
    captured_texts: list[str] = []
    batch_ordered: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            # Mağaza mobil web daha uzun liste yükler; masaüstü headless'ta sık sık ~75 uygulamada kalınıyor.
            ctx = browser.new_context(
                locale=f"{lang}-{country.upper()}",
                viewport={"width": 412, "height": 915},
                is_mobile=True,
                has_touch=True,
                user_agent=(
                    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
                ),
            )
            page = ctx.new_page()

            def _on_response(response) -> None:  # noqa: ANN001
                if "batchexecute" in response.url:
                    try:
                        captured_texts.append(response.text())
                    except Exception:
                        pass

            page.on("response", _on_response)

            try:
                page.goto(chart_url, wait_until="domcontentloaded", timeout=55_000)
                page.wait_for_timeout(4_500)
            except PWTimeout:
                pass

            seen_batch: set[str] = set()
            pkg_l = (package or "").strip().lower()
            _scroll_js = """() => {
  const se = document.scrollingElement;
  if (se) { se.scrollTop = se.scrollHeight; }
  window.scrollTo(0, document.body.scrollHeight);
  document.querySelectorAll('c-wiz,div,section').forEach((el) => {
    try {
      const sh = el.scrollHeight - el.clientHeight;
      if (sh > 400) { el.scrollTop = el.scrollHeight; }
    } catch (e) {}
  });
}"""

            def _drain_batches() -> None:
                for txt in list(captured_texts):
                    for p in _extract_android_packages(txt):
                        k = p.lower()
                        if k not in seen_batch:
                            seen_batch.add(k)
                            batch_ordered.append(p)
                captured_texts.clear()

            vw = page.viewport_size or {"width": 900, "height": 900}
            cx = max(120, int(vw.get("width", 900)) // 2)
            cy = max(200, int(vw.get("height", 900)) // 2)
            try:
                page.mouse.move(cx, cy)
            except Exception:
                pass

            # İlk yükleme + batchexecute
            for _scroll_i in range(8):
                _drain_batches()
                if pkg_l and _android_pkg_index(batch_ordered, package) is not None:
                    break
                if len(batch_ordered) >= 5000:
                    break
                try:
                    page.evaluate(_scroll_js)
                    page.wait_for_timeout(900)
                except Exception:
                    break

            # Kategori listesi çoğunlukla wheel ile ek sayfa yükler (headless'ta scrollHeight tek başına yetmez).
            for step_i in range(220):
                _drain_batches()
                if pkg_l and _android_pkg_index(batch_ordered, package) is not None:
                    break
                if len(batch_ordered) >= 5000:
                    break
                try:
                    page.mouse.move(cx, cy)
                    page.mouse.wheel(0, 700)
                    if step_i % 18 == 17:
                        page.evaluate(_scroll_js)
                    page.wait_for_timeout(280)
                except Exception:
                    break

            _drain_batches()

            dom_ids: list[str] = []
            try:
                hrefs = page.eval_on_selector_all(
                    'a[href*="details?id="]',
                    'els => els.map(e => e.getAttribute("href"))',
                )
                seen_dom: set[str] = set()
                for href in hrefs or []:
                    m = re.search(r"details\?id=([A-Za-z0-9._]+)", href or "")
                    if not m:
                        continue
                    pid = m.group(1)
                    k = pid.lower()
                    if k not in seen_dom:
                        seen_dom.add(k)
                        dom_ids.append(pid)
            except Exception:
                pass

            browser.close()
    except Exception as exc:
        logger.warning("Playwright android rank hatası: %s", exc)
        return None

    # ÖNEMLİ: DOM'daki tüm details?id= linkleri (üst menü, benzer uygulamalar vb.) grafik sırası değil.
    # dom_ids'i batch'ten önce birleştirmek #39 gibi yanlış düşük sıralar üretir; sıra yalnızca
    # batchexecute ile gelen kategori listesi sırasına dayanmalı.
    idx_b = _android_pkg_index(batch_ordered, package)
    if idx_b is not None:
        rank = idx_b + 1
        total = len(batch_ordered)
        logger.info(
            "android rank playwright: slug=%s batch=%d dom=%d (sıra batchexecute)",
            slug,
            len(batch_ordered),
            len(dom_ids),
        )
        logger.info("android rank: %s #%d / %d (%s)", package, rank, total, slug)
        return {
            "rank": rank,
            "total": total,
            "chart": "category_top",
            "chart_label": "Kategori",
            "category_name": slug.replace("_", " ").title(),
            "estimated": False,
            "rank_basis": "batchexecute",
        }

    if batch_ordered:
        logger.info(
            "android rank: paket batch listesinde yok (batch=%d, dom=%d, slug=%s) — DOM birleştirilmiyor",
            len(batch_ordered),
            len(dom_ids),
            slug,
        )
        return None

    idx_d = _android_pkg_index(dom_ids, package)
    if idx_d is None:
        logger.debug("android rank: kategori DOM/batchexecute boş (%s)", slug)
        return None

    rank = idx_d + 1
    total = len(dom_ids)
    logger.warning(
        "android rank: %s #%d / %d (%s) — yalnızca DOM (batchexecute boş), sıra güvenilir olmayabilir",
        package,
        rank,
        total,
        slug,
    )
    return {
        "rank": rank,
        "total": total,
        "chart": "category_dom_only",
        "chart_label": "Kategori",
        "category_name": slug.replace("_", " ").title(),
        "estimated": True,
        "rank_basis": "dom_only",
    }


def _fetch_android_rank_http_fallback(
    package: str,
    *,
    country: str = "tr",
    lang: str = "tr",
    category_id: str | None = None,
    genre_name_hint: str | None = None,
) -> dict[str, Any] | None:
    """HTTP ile Play Store detay sayfası ve kategori chart tarama (fallback)."""
    url = "https://play.google.com/store/apps/details"
    params = {"id": package, "hl": lang, "gl": country}
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"}
    try:
        with httpx.Client(timeout=12.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
        html = r.text
    except Exception:
        return None

    category_name = None
    m_cat = re.search(r'"applicationCategory":"([^"]+)"', html)
    if m_cat:
        category_name = m_cat.group(1).strip()

    gname = (genre_name_hint or "").strip()
    gid = (category_id or "").strip().upper()
    slug_words = gid.replace("_", " ").title() if gid else ""

    patterns: list[str] = []
    if gname and len(gname) <= 60:
        patterns.append(r"#\s*([0-9]{1,5})\s+içinde\s+" + re.escape(gname))
        patterns.append(r"#\s*([0-9]{1,5})\s+i̇çinde\s+" + re.escape(gname))
    if slug_words and slug_words != gname and len(slug_words) <= 60:
        patterns.append(r"#\s*([0-9]{1,5})\s+(?:in|içinde|i̇çinde)?\s*" + re.escape(slug_words))
    patterns.extend(
        [
            r"#\s*([0-9]{1,5})\s*(?:in|içinde|i̇çinde)?\s*"
            r"(Finance|Finans|Business|İş|Haberleşme|Communication|Tools|Araçlar|Eğlence|Entertainment)",
            r"Top charts[^#]{0,120}#\s*([0-9]{1,5})",
            r"ranking[^#]{0,120}#\s*([0-9]{1,5})",
        ]
    )
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            rank = int(m.group(1))
            if rank <= 0 or rank > 500_000:
                continue
            cat_g2 = m.group(2) if m.lastindex and m.lastindex >= 2 else None
            if cat_g2:
                category_name = category_name or cat_g2
            return {
                "rank": rank,
                "total": None,
                "chart": "details_page",
                "chart_label": "Ücretsiz",
                "category_name": category_name,
            }

    return None


def _fetch_google_bundle(
    package: str, max_reviews: int = GOOGLE_PLAY_MAX_REVIEWS,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    """google-play-scraper ile mağaza özeti + en yeni yorumlar."""
    try:
        from google_play_scraper import Sort, app as gp_app
        from google_play_scraper import reviews as gp_reviews
    except ImportError:
        return {}, [], "google-play-scraper kurulu değil; sunucuda: pip install -r requirements.txt"

    err: str | None = None
    try:
        meta = gp_app(package, lang="tr", country="tr")
    except Exception as e:
        logger.warning("Play meta alınamadı (%s): %s", package, e)
        meta = {}
        err = str(e)

    collected: list[dict[str, Any]] = []
    token = None
    try:
        while len(collected) < max_reviews:
            batch_n = min(200, max_reviews - len(collected))
            chunk, token = gp_reviews(
                package,
                lang="tr",
                country="tr",
                sort=Sort.NEWEST,
                count=batch_n,
                continuation_token=token,
            )
            collected.extend(chunk)
            if not token:
                break
    except Exception as e:
        logger.warning("Play yorumları alınamadı (%s): %s", package, e)
        if err is None:
            err = str(e)

    norm: list[dict[str, Any]] = []
    for rv in collected:
        at = rv.get("at")
        if isinstance(at, datetime):
            dt = at if at.tzinfo else at.replace(tzinfo=_UTC)
        else:
            continue
        norm.append(
            {
                "at": dt,
                "score": int(rv.get("score") or 0),
                "text": _normalize_review_text(rv.get("content") or ""),
                "version": (str(rv.get("reviewCreatedVersion") or "").strip() or None),
            }
        )

    return meta, norm, err


def _categorize(text: str) -> str:
    low = (text or "").lower()
    for cat_id, _label, keys in _REVIEW_CATEGORIES:
        if any(k in low for k in keys):
            return cat_id
    return "diger"


def _cutoff(days: int) -> datetime:
    start = inclusive_local_period_start_utc(n_calendar_days=days)
    if start is None:
        return datetime.now(tz=_UTC)
    return start


def _filter_by_period(rows: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    start = _cutoff(days)
    return [r for r in rows if r["at"] >= start]


def _filter_by_period_or_anchor(
    rows: list[dict[str, Any]], days: int
) -> tuple[list[dict[str, Any]], datetime | None, str | None]:
    """Önce bugünden geriye `days` gün; yorum yoksa çekilen kümedeki en yeni tarihe göre son `days` gün."""
    cal = _filter_by_period(rows, days)
    if cal:
        return cal, None, None
    if not rows:
        return [], None, None
    anchor = max(r["at"] for r in rows)
    start = anchor - timedelta(days=days)
    anchored = [r for r in rows if r["at"] >= start]
    note = (
        f"Bu aralıkta (TSİ takviminde son {days} gün) örnek yorum yok; "
        f"grafikler en güncel örnek tarihine göre ({_at_report_tz_date(anchor).isoformat()} yerel gün) kaydırıldı."
    )
    return anchored, anchor, note


def _daily_rating_series(
    rows: list[dict[str, Any]],
    days: int,
    anchor_end: datetime | None = None,
) -> list[dict[str, Any]]:
    """Rapor takvimi (TSİ) günü bazında, dönemdeki yorumların günlük ortalama yıldızı (mağaza genel ortalama geçmişi değil)."""
    by_day: dict[str, list[int]] = {}
    if anchor_end is None:
        end_d = report_calendar_today()
    else:
        end_d = _at_report_tz_date(anchor_end)
    start_d = end_d - timedelta(days=days - 1)
    for r in rows:
        d = _at_report_tz_date(r["at"])
        if d < start_d or d > end_d:
            continue
        k = d.isoformat()
        by_day.setdefault(k, []).append(r["score"])
    out: list[dict[str, Any]] = []
    cur = start_d
    while cur <= end_d:
        k = cur.isoformat()
        scores = by_day.get(k, [])
        out.append(
            {
                "day": k,
                "avg_rating": round(sum(scores) / len(scores), 3) if scores else None,
                "review_count": len(scores),
            }
        )
        cur += timedelta(days=1)
    return out


def _histogram_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    c = Counter()
    for r in rows:
        s = r["score"]
        if 1 <= s <= 5:
            c[str(s)] += 1
    return {str(i): c.get(str(i), 0) for i in range(1, 6)}


def _satisfaction_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    sat = uns = neu = 0
    for r in rows:
        s = r["score"]
        if s >= 4:
            sat += 1
        elif s <= 2:
            uns += 1
        else:
            neu += 1
    n = len(rows) or 1
    return {
        "memnun": sat,
        "notr": neu,
        "memnun_degil": uns,
        "memnun_oran": round(100.0 * sat / n, 1),
        "memnun_degil_oran": round(100.0 * uns / n, 1),
        "notr_oran": round(100.0 * neu / n, 1),
    }


def _category_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cc: Counter[str] = Counter()
    labels = {cid: lab for cid, lab, _ in _REVIEW_CATEGORIES}
    labels["diger"] = "Diğer"
    for r in rows:
        cc[_categorize(r.get("text") or "")] += 1
    return [{"id": k, "label": labels.get(k, k), "count": v} for k, v in cc.most_common()]


def _category_review_map(rows: list[dict[str, Any]], *, per_category_limit: int = 40) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    for r in ordered:
        cid = _categorize(r.get("text") or "")
        bucket = out.setdefault(cid, [])
        if len(bucket) >= per_category_limit:
            continue
        bucket.append(
            {
                "at": r["at"],
                "score": int(r.get("score") or 0),
                "text": _normalize_review_text(r.get("text") or ""),
            }
        )
    return out


def _star_review_map(rows: list[dict[str, Any]], *, per_star_limit: int = 60) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {str(i): [] for i in range(1, 6)}
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    for r in ordered:
        s = int(r.get("score") or 0)
        if s < 1 or s > 5:
            continue
        key = str(s)
        bucket = out[key]
        if len(bucket) >= per_star_limit:
            continue
        bucket.append(
            {
                "at": r["at"],
                "score": s,
                "text": _normalize_review_text(r.get("text") or ""),
            }
        )
    return out


def _dedupe_reviews(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda r: r["at"], reverse=True)
    seen_text: set[str] = set()
    seen_fallback: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in ordered:
        txt = _normalize_review_text(r.get("text") or "")
        canonical = re.sub(r"\s+", " ", txt).strip().lower()
        if canonical:
            if canonical in seen_text:
                continue
            seen_text.add(canonical)
        else:
            fb = f'{r["at"].isoformat()}\0{int(r.get("score") or 0)}'
            if fb in seen_fallback:
                continue
            seen_fallback.add(fb)
        out.append(
            {
                "at": r["at"],
                "score": int(r.get("score") or 0),
                "text": txt,
            }
        )
    return out


def _latest_reviews(rows: list[dict[str, Any]], limit: int = 100) -> list[dict[str, Any]]:
    return _dedupe_reviews(rows)[:limit]


def _android_histogram_overall(meta: dict[str, Any]) -> dict[str, int] | None:
    h = meta.get("histogram")
    if not h or len(h) != 5:
        return None
    return {str(i + 1): int(h[i]) for i in range(5)}




def invalidate_raw_cache(product_id: str | None = None) -> None:
    with _CACHE_LOCK:
        if product_id:
            _RAW_CACHE.pop(product_id, None)
        else:
            _RAW_CACHE.clear()
    try:
        if product_id:
            _disk_raw_path(product_id).unlink(missing_ok=True)
        elif _DISK_RAW_DIR.is_dir():
            for child in _DISK_RAW_DIR.glob("*.json"):
                child.unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("app_intel disk cache silinemedi: %s", exc)
    try:
        db = SessionLocal()
        try:
            if product_id:
                row = db.get(AppIntelRawCache, product_id)
                if row:
                    db.delete(row)
            else:
                db.execute(delete(AppIntelRawCache))
            db.commit()
        finally:
            db.close()
    except Exception as exc:
        logger.warning("app_intel db önbellek silinemedi: %s", exc)


def get_cached_raw_product_data(product_id: str) -> dict[str, Any] | None:
    """Ağ çağrısı yapmadan yalnızca sıcak cache'deki raw payload'ı döndür."""
    if product_id not in APP_PRODUCTS:
        return None
    now = time.time()
    with _CACHE_LOCK:
        hit = _RAW_CACHE.get(product_id)
        if not hit:
            return None
        ts, payload = hit
        if now - ts >= _CACHE_TTL_SEC:
            return None
        return payload


def get_last_forced_refresh_at(product_id: str) -> str | None:
    v = _FORCED_REFRESH_AT.get(product_id)
    return str(v) if v else None


def _android_category_rank_chart_rejected(chart: Any) -> bool:
    ch = str(chart or "").strip().lower()
    return ch in ("store_search_package", "category_dom_only")


def _android_category_rank_is_displayable(cr: Any) -> bool:
    if not isinstance(cr, dict) or cr.get("rank") is None:
        return False
    return not _android_category_rank_chart_rejected(cr.get("chart"))


def _android_cached_category_rank_is_obsolete(cr: Any) -> bool:
    """Eski yanlış kaynaklar (arama #1, DOM-only) yeniden hesaplanır; Play detay sayfası (#N Finans) geçerlidir."""
    if not isinstance(cr, dict) or cr.get("rank") is None:
        return False
    if _android_category_rank_chart_rejected(cr.get("chart")):
        return True
    rb = str(cr.get("rank_basis") or "")
    if rb == "env_override":
        return False
    ch = str(cr.get("chart") or "").strip().lower()
    if ch in ("category_top", "details_page", "operator_override"):
        return False
    if rb == "batchexecute":
        return False
    return True


def _apply_android_rank_env_to_payload(product_id: str, payload: dict[str, Any]) -> None:
    """Disk/bellekten dönen ham payload'ta da APP_INTEL_ANDROID_RANK_OVERRIDES uygulanır."""
    ov = _android_rank_env_override(product_id)
    if not ov:
        return
    android = payload.get("android")
    if not isinstance(android, dict):
        return
    meta = android.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    payload["android"] = {**android, "meta": {**meta, "category_rank": ov}}


def _android_rank_env_override(product_id: str) -> dict[str, Any] | None:
    """Play headless sık sık ~50–75 uygulamada kalıyor; doğrulanmış sıra için operatör env.

    APP_INTEL_ANDROID_RANK_OVERRIDES='{"doviz":161,"sinemalar":null}' — yalnızca sayı olan anahtarlar kullanılır.
    """
    raw = (os.environ.get("APP_INTEL_ANDROID_RANK_OVERRIDES") or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("APP_INTEL_ANDROID_RANK_OVERRIDES JSON okunamadı")
        return None
    if not isinstance(data, dict):
        return None
    val = data.get(product_id)
    if val is None:
        val = data.get(str(product_id).lower())
    if val is None:
        return None
    try:
        rank_int = int(val)
    except (TypeError, ValueError):
        return None
    if rank_int < 1 or rank_int > 999_999:
        return None
    return {
        "rank": rank_int,
        "total": None,
        "chart": "operator_override",
        "chart_label": "Kategori (doğrulanmış)",
        "category_name": None,
        "rank_basis": "env_override",
        "estimated": False,
    }


def _recompute_android_category_rank_only(
    product_id: str,
    spec: dict[str, str],
    meta: dict[str, Any],
    *,
    allow_db_fallback: bool,
) -> dict[str, Any]:
    """Play Android kategori sırasını yeniden hesaplar (meta içinde genre/genreId kullanılır)."""
    meta = dict(meta or {})
    _prev_cr = meta.get("category_rank")
    if isinstance(_prev_cr, dict) and str(_prev_cr.get("chart") or "").strip().lower() == "store_search_package":
        meta = {k: v for k, v in meta.items() if k != "category_rank"}

    def _android_rank_job() -> dict[str, Any] | None:
        return _fetch_android_category_rank(
            spec["android_package"],
            country="tr",
            lang="tr",
            category_id=str(meta.get("genreId") or "") or None,
            skip_playwright=None,
            genre_name_hint=(str(meta.get("genre") or "").strip() or None),
        )

    a_rank = _bounded_rank_call(_android_rank_job, _store_rank_call_budget_sec())
    if _android_category_rank_is_displayable(a_rank):
        meta = {**meta, "category_rank": a_rank}

    cr_a = meta.get("category_rank")
    if allow_db_fallback and not (isinstance(cr_a, dict) and cr_a.get("rank") is not None):
        fb_a = _latest_stored_category_rank(product_id, "android")
        if fb_a:
            meta = {**meta, "category_rank": fb_a}

    ov = _android_rank_env_override(product_id)
    if ov:
        # Operatör .env ile verdiği sıra, headless'taki kısmi liste / yanlış pozitiflerden önceliklidir.
        meta = {**meta, "category_rank": ov}
    return meta


def ensure_android_category_rank_on_raw(
    product_id: str,
    raw: dict[str, Any],
    *,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    """Cache-only payload'ta Android SIRA boşsa Play detay sayfası metnini çeker (100+ sıra dahil)."""
    spec = APP_PRODUCTS.get(product_id)
    if not spec or not isinstance(raw, dict):
        return raw
    android = raw.get("android")
    if not isinstance(android, dict):
        return raw
    meta = android.get("meta")
    if not isinstance(meta, dict):
        meta = {}

    ov = _android_rank_env_override(product_id)
    if ov:
        meta = {**meta, "category_rank": ov}
        return {**raw, "android": {**android, "meta": meta}}

    cr = meta.get("category_rank")
    if _android_category_rank_is_displayable(cr):
        return raw

    if allow_live_fetch:

        def _http_rank_job() -> dict[str, Any] | None:
            return _fetch_android_rank_http_fallback(
                spec["android_package"],
                country="tr",
                lang="tr",
                category_id=str(meta.get("genreId") or "") or None,
                genre_name_hint=(str(meta.get("genre") or "").strip() or None),
            )

        fetched = _bounded_rank_call(_http_rank_job, _store_rank_call_budget_sec())
        if _android_category_rank_is_displayable(fetched):
            meta = {**meta, "category_rank": fetched}
            at_iso = str(raw.get("fetched_at") or datetime.now(tz=_UTC).isoformat())
            _append_rank_snapshot(
                product_id,
                "android",
                {**fetched, "category_name": meta.get("genre")},
                at_iso=at_iso,
            )

    if not _android_category_rank_is_displayable(meta.get("category_rank")):
        fb = _latest_stored_category_rank(product_id, "android")
        if fb:
            meta = {**meta, "category_rank": fb}

    return {**raw, "android": {**android, "meta": meta}}


def _refresh_obsolete_android_rank_in_payload(
    product_id: str, spec: dict[str, str], payload: dict[str, Any]
) -> bool:
    """Disk/bellekten gelen payload'ta eski Android sırası varsa yeniden hesaplar. DB fallback kullanmaz (#39 tekrarını önler)."""
    android = payload.get("android")
    if not isinstance(android, dict):
        return False
    meta = android.get("meta")
    if not isinstance(meta, dict):
        return False
    if not _android_cached_category_rank_is_obsolete(meta.get("category_rank")):
        return False
    logger.info("app_intel: eski Android kategori sırası tespit edildi; yeniden hesaplanıyor (%s)", product_id)
    new_meta = _recompute_android_category_rank_only(
        product_id, spec, meta, allow_db_fallback=False
    )
    payload["android"] = {**android, "meta": new_meta}
    at_iso = str(payload.get("fetched_at") or datetime.now(tz=_UTC).isoformat())
    cr = (new_meta or {}).get("category_rank")
    if isinstance(cr, dict) and cr.get("rank") is not None:
        _append_rank_snapshot(
            product_id,
            "android",
            {**cr, "category_name": new_meta.get("genre")},
            at_iso=at_iso,
        )
        _save_rank_history()
    try:
        _write_disk_raw(product_id, payload)
    except OSError as exc:
        logger.warning("app_intel: sıra yenilemesi disk yazılamadı (%s): %s", product_id, exc)
    return True


def get_raw_product_data(product_id: str, *, force_refresh: bool = False, cache_only: bool = False) -> dict[str, Any]:
    if product_id not in APP_PRODUCTS:
        return {"error": "unknown_product"}
    spec = APP_PRODUCTS[product_id]
    now = time.time()
    cache_key = product_id
    mem_hit: dict[str, Any] | None = None
    with _CACHE_LOCK:
        hit = _RAW_CACHE.get(cache_key)
        if (not force_refresh) and hit and now - hit[0] < _CACHE_TTL_SEC:
            mem_hit = copy.deepcopy(hit[1])
    if mem_hit is not None:
        if _refresh_obsolete_android_rank_in_payload(product_id, spec, mem_hit):
            with _CACHE_LOCK:
                _RAW_CACHE[cache_key] = (now, mem_hit)
        _apply_android_rank_env_to_payload(product_id, mem_hit)
        return mem_hit

    if not force_refresh:
        disk_payload = _load_disk_raw(product_id)
        age: float = float("inf")
        if disk_payload is not None:
            try:
                age = now - _disk_raw_path(product_id).stat().st_mtime
            except OSError:
                age = float("inf")
        else:
            db_pl, db_age = _load_db_raw_with_age(product_id, now)
            if db_pl is not None and db_age is not None:
                disk_payload = db_pl
                age = db_age

        if disk_payload is not None and (age < _CACHE_TTL_SEC or cache_only):
            pl = copy.deepcopy(disk_payload)
            _refresh_obsolete_android_rank_in_payload(product_id, spec, pl)
            _apply_android_rank_env_to_payload(product_id, pl)
            with _CACHE_LOCK:
                _RAW_CACHE[cache_key] = (now, pl)
            src = "disk" if _disk_raw_path(product_id).is_file() else "db"
            logger.info("app_intel %s cache hit: %s (%.1f h)", src, product_id, age / 3600.0)
            return pl

    if cache_only:
        return {"error": "no_cached_data", "message": "Henüz mağaza verisi yok. Yorum çekmek için 'Verileri yenile' butonunu kullanın."}

    phase_timeout = _fetch_phase_timeout_sec()
    play_cap = _play_review_cap()

    # Ağ çağrılarını paralelleştir; tüm faz için tek üst süre (takılı istek yok).
    pool = ThreadPoolExecutor(max_workers=5)
    try:
        f_play = pool.submit(_fetch_google_bundle, spec["android_package"], play_cap)
        f_ios_reviews = pool.submit(_fetch_ios_reviews_multistore, spec["ios_app_id"], spec["ios_slug"])
        f_ios_lookup = pool.submit(_fetch_ios_lookup_meta, spec["ios_app_id"])
        f_ios_ratings = pool.submit(
            _fetch_ios_ssr_ratings, spec["ios_app_id"], spec["ios_slug"], country="tr"
        )
        f_ios_rss = pool.submit(_fetch_ios_rss_reviews, spec["ios_app_id"], country="tr", max_pages=5)
        futs = (f_play, f_ios_reviews, f_ios_lookup, f_ios_ratings, f_ios_rss)
        _, pending_futs = wait(futs, timeout=phase_timeout, return_when=ALL_COMPLETED)

        def _future_get(fut, empty, label: str):  # noqa: ANN001
            if fut in pending_futs:
                logger.warning(
                    "app_intel: %s cevabı %.0fs içinde tamamlanmadı; kısmi veri.",
                    label,
                    phase_timeout,
                )
                return empty
            try:
                return fut.result()
            except Exception as exc:
                logger.warning("app_intel: %s future hata: %s", label, exc)
                return empty

        meta, g_rows, g_err = _future_get(
            f_play,
            ({}, [], f"Play: mağaza verisi {phase_timeout:.0f} sn içinde tamamlanamadı."),
            "google_play",
        )
        i_rows, i_snap, i_err, i_sf_ok, i_sf_n = _future_get(
            f_ios_reviews,
            ([], {}, f"App Store vitrinleri {phase_timeout:.0f} sn içinde tamamlanamadı.", 0, len(_ios_review_storefronts())),
            "ios_multistore",
        )
        i_lookup = _future_get(f_ios_lookup, {}, "ios_lookup")
        i_ssr_ratings = _future_get(f_ios_ratings, {}, "ios_ssr_ratings")
        i_rss_rows = _future_get(f_ios_rss, [], "ios_rss")
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    # RSS reviewlarını mevcut listeyie ekle (dedup: aynı tarih+puan+metin önlenir)
    if i_rss_rows:
        existing_keys = {_ios_review_key(rv["at"], rv["text"], rv["score"]) for rv in i_rows}
        for rv in i_rss_rows:
            k = _ios_review_key(rv["at"], rv["text"], rv["score"])
            if k not in existing_keys:
                existing_keys.add(k)
                i_rows.append(rv)
    if i_lookup:
        i_snap = {**(i_snap or {}), **{k: v for k, v in i_lookup.items() if v is not None}}
    # SSR JSON ile çekilen ülkeye özgü gerçek dağılım — öncelikli kaynak
    if i_ssr_ratings:
        i_snap = {**(i_snap or {}), "star_histogram": i_ssr_ratings.get("star_histogram")}
        # SSR'dan gelen score/count, iTunes lookup'tan gelmiyorsa kullan
        if i_ssr_ratings.get("score") is not None and not (i_snap or {}).get("score"):
            i_snap = {**(i_snap or {}), "score": i_ssr_ratings["score"]}
        if i_ssr_ratings.get("ratings_count") is not None and not (i_snap or {}).get("ratings_count"):
            i_snap = {**(i_snap or {}), "ratings_count": i_ssr_ratings["ratings_count"]}
    # iOS kategori sırası birden fazla HTTP turu yapabilir; üst süre ile sınırla (Railway ~60s proxy).
    def _ios_rank_job() -> dict[str, Any] | None:
        return _fetch_ios_category_rank(
            spec["ios_app_id"],
            country="tr",
            genre_id=(i_snap or {}).get("primary_genre_id"),
        )

    i_rank = _bounded_rank_call(_ios_rank_job, _ios_store_rank_call_budget_sec())
    if i_rank:
        i_snap = {**(i_snap or {}), **{"category_rank": i_rank}}

    meta = _recompute_android_category_rank_only(
        product_id, spec, meta, allow_db_fallback=True
    )

    cr_i = (i_snap or {}).get("category_rank")
    if not (isinstance(cr_i, dict) and cr_i.get("rank") is not None):
        fb_i = _latest_stored_category_rank(product_id, "ios")
        if fb_i:
            i_snap = {**(i_snap or {}), "category_rank": fb_i}

    payload = {
        "product_id": product_id,
        "label": spec["label"],
        "urls": {
            "android": spec["android_url"],
            "ios": spec["ios_url"],
        },
        "fetched_at": datetime.now(tz=_UTC).isoformat(),
        "android": {
            "meta": {
                "score": meta.get("score"),
                "ratings": meta.get("ratings"),
                "histogram": _android_histogram_overall(meta),
                "reviews": meta.get("reviews"),
                "icon": meta.get("icon"),
                "genre": meta.get("genre"),
                "category_rank": meta.get("category_rank"),
                "play_version": meta.get("version"),
                "play_last_updated_at": _play_updated_iso(meta.get("updated")),
            },
            "reviews": g_rows,
            "error": g_err,
        },
        "ios": {
            "meta": i_snap,
            "reviews": i_rows,
            "error": i_err,
            "storefronts_ok": i_sf_ok,
            "storefronts_total": i_sf_n,
            "note_tr": None,
        },
    }
    _append_rank_snapshot(
        product_id,
        "android",
        {
            **(((payload.get("android", {}).get("meta") or {}).get("category_rank") or {})),
            "category_name": (payload.get("android", {}).get("meta") or {}).get("genre"),
        },
        at_iso=payload["fetched_at"],
    )
    _append_rank_snapshot(
        product_id,
        "ios",
        {
            **(((payload.get("ios", {}).get("meta") or {}).get("category_rank") or {})),
            "category_name": (payload.get("ios", {}).get("meta") or {}).get("primary_genre_name"),
        },
        at_iso=payload["fetched_at"],
    )
    _save_rank_history()

    if force_refresh:
        _FORCED_REFRESH_AT[product_id] = payload["fetched_at"]
        _save_forced_refresh_meta()

    _write_disk_raw(product_id, payload)

    with _CACHE_LOCK:
        _RAW_CACHE[cache_key] = (now, payload)
    return payload


def build_intel_payload(product_id: str, period_days: int, *, force_refresh: bool = False, cache_only: bool = False) -> dict[str, Any]:
    valid_periods = (0, 7, 30, 90, 180, 365, 730)
    if period_days not in valid_periods:
        period_days = 7
    raw = get_raw_product_data(product_id, force_refresh=force_refresh, cache_only=cache_only)
    if raw.get("error"):
        return raw

    intel: dict[str, Any] = {
        "product_id": product_id,
        "label": raw["label"],
        "product_key": product_id,
        "app_icon": raw["android"]["meta"].get("icon") or (raw["ios"]["meta"] or {}).get("icon"),
        "urls": raw["urls"],
        "fetched_at": raw["fetched_at"],
        "display_fetched_at": get_last_forced_refresh_at(product_id) or raw["fetched_at"],
        "errors": {"android": raw["android"].get("error"), "ios": raw["ios"].get("error")},
        "meta": {
            "android": raw["android"]["meta"],
            "ios": raw["ios"]["meta"],
        },
        "scrape": {
            "android_review_samples": len(raw["android"]["reviews"]),
            "ios_review_samples": len(raw["ios"]["reviews"]),
            "ios_storefronts_ok": raw["ios"].get("storefronts_ok"),
            "ios_storefronts_total": raw["ios"].get("storefronts_total"),
        },
        "windows": {},
    }

    for p in valid_periods:
        if p == 0:
            fa = list(raw["android"]["reviews"])
            fi = list(raw["ios"]["reviews"])
            fa_anchor = None
            fi_anchor = None
            fa_note = "Tüm zaman görünümünde detaylar çekilen örnek yorum havuzundan hesaplanır."
            fi_note = "Tüm zaman görünümünde detaylar çekilen örnek yorum havuzundan hesaplanır."
        else:
            fa, fa_anchor, fa_note = _filter_by_period_or_anchor(raw["android"]["reviews"], p)
            fi, fi_anchor, fi_note = _filter_by_period_or_anchor(raw["ios"]["reviews"], p)
        fa = _dedupe_reviews(fa)
        fi = _dedupe_reviews(fi)
        intel["windows"][str(p)] = {
            "period_days": p,
            "android": {
                "review_count_period": len(fa),
                "period_note_tr": fa_note,
                "rating_series": _daily_rating_series(fa, p, fa_anchor),
                "star_distribution_period": _histogram_counts(fa),
                "star_distribution_overall": raw["android"]["meta"].get("histogram"),
                "store_score": raw["android"]["meta"].get("score"),
                "store_ratings": raw["android"]["meta"].get("ratings"),
                "store_category_rank": raw["android"]["meta"].get("category_rank"),
                "store_category_name": raw["android"]["meta"].get("genre")
                or ((raw["android"]["meta"].get("category_rank") or {}).get("category_name")),
                "store_rank_history_7d": _rank_history_series(product_id, "android", days=7),
                "store_rank_history_30d": _rank_history_series(product_id, "android", days=30),
                "store_rank_daily_30d": _rank_history_daily(product_id, "android", days=30),
                "store_rank_changes_50": _rank_history_changes(product_id, "android", limit=50),
                "satisfaction": _satisfaction_split(fa),
                "categories": _category_counts(fa),
                "category_reviews": _category_review_map(fa),
                "star_reviews": _star_review_map(fa),
                "latest_reviews": _latest_reviews(raw["android"]["reviews"], 100),
            },
            "ios": {
                "review_count_period": len(fi),
                "period_note_tr": fi_note,
                "rating_series": _daily_rating_series(fi, p, fi_anchor),
                "star_distribution_period": _histogram_counts(fi),
                "star_distribution_overall": (raw["ios"]["meta"] or {}).get("star_histogram"),
                "store_score": (raw["ios"]["meta"] or {}).get("score"),
                "store_ratings_count": (raw["ios"]["meta"] or {}).get("ratings_count"),
                "store_ratings_caption": (raw["ios"]["meta"] or {}).get("ratings_caption"),
                "store_category_rank": (raw["ios"]["meta"] or {}).get("category_rank"),
                "store_category_name": (raw["ios"]["meta"] or {}).get("primary_genre_name"),
                "store_rank_history_7d": _rank_history_series(product_id, "ios", days=7),
                "store_rank_history_30d": _rank_history_series(product_id, "ios", days=30),
                "store_rank_daily_30d": _rank_history_daily(product_id, "ios", days=30),
                "store_rank_changes_50": _rank_history_changes(product_id, "ios", limit=50),
                "satisfaction": _satisfaction_split(fi),
                "categories": _category_counts(fi),
                "category_reviews": _category_review_map(fi),
                "star_reviews": _star_review_map(fi),
                "note_tr": raw["ios"].get("note_tr"),
                "latest_reviews": _latest_reviews(raw["ios"]["reviews"], 100),
            },
        }

    intel["active_window"] = intel["windows"][str(period_days)]
    return intel


def refresh_category_ranks() -> dict[str, Any]:
    """Tüm ürünler için sadece kategori sırasını çekip DB'ye kaydeder.

    Tam yorum yenileme yapmaz — sadece iOS ve Android chart sıralarını
    günceller. 3 saatlik cron job tarafından çağrılır.
    """
    now_iso = datetime.now(tz=_UTC).isoformat()
    results: dict[str, Any] = {}

    for product_id, spec in APP_PRODUCTS.items():
        try:
            # iOS: genre_id'yi lookup'tan al (cache varsa oradan, yoksa API'den)
            ios_genre_id: int | None = None
            with _CACHE_LOCK:
                for cache_val in _RAW_CACHE.values():
                    if isinstance(cache_val, tuple) and len(cache_val) == 2:
                        cached_payload = cache_val[1]
                        if (cached_payload or {}).get("product_id") == product_id:
                            ios_genre_id = (
                                (cached_payload.get("ios") or {})
                                .get("meta") or {}
                            ).get("primary_genre_id")
                            break

            if ios_genre_id is None:
                # Cache boşsa lookup'tan çek
                lookup = _fetch_ios_lookup_meta(spec["ios_app_id"])
                ios_genre_id = lookup.get("primary_genre_id")

            i_rank = _fetch_ios_category_rank(
                spec["ios_app_id"], country="tr", genre_id=ios_genre_id
            )
            if i_rank:
                _append_rank_snapshot(
                    product_id, "ios",
                    {**i_rank, "category_name": i_rank.get("chart_label") or "Finans"},
                    at_iso=now_iso,
                )
            results[product_id] = {"ios": i_rank}
            logger.info("Rank refresh (%s) iOS: %s", product_id, i_rank)

            # Android: cron sırasında Playwright açık (Railway ana istekten ayrı); genre ile HTTP yedeği de iyileşir.
            meta_play, _, _ = _fetch_google_bundle(spec["android_package"], 1)
            a_rank = _fetch_android_category_rank(
                spec["android_package"],
                country="tr",
                lang="tr",
                category_id=str((meta_play or {}).get("genreId") or "") or None,
                skip_playwright=False,
                genre_name_hint=(str((meta_play or {}).get("genre") or "").strip() or None),
            )
            if a_rank and a_rank.get("rank") is not None:
                _append_rank_snapshot(product_id, "android", a_rank, at_iso=now_iso)
            results[product_id]["android"] = a_rank
            logger.info("Rank refresh (%s) Android: %s", product_id, a_rank)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Rank refresh hatası (%s): %s", product_id, exc)
            results[product_id] = {"error": str(exc)}

    _save_rank_history()
    for pid, block in results.items():
        if isinstance(block, dict) and block.get("android"):
            invalidate_raw_cache(pid)
    return results


def intel_json_safe(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: intel_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [intel_json_safe(v) for v in obj]
    if isinstance(obj, float):
        return float(obj) if obj == obj else None
    return obj
