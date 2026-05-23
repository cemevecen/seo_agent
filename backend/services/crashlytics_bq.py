"""Firebase Crashlytics → BigQuery servis katmanı.

iOS : doviz-ios projesi   → CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON
Android: doviz-android projesi → CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON

Tasarım kararları:
- Her BigQuery sorgusuna 25 saniye timeout (job_config.timeout)
- Sorgu başında dry-run ile byte tahmini; 200 MB üzerindeyse sorgu iptal
- Eş zamanlı max 2 sorgu (threading.Semaphore)
- Sonuçlar 4 saat in-memory cache (product+days+platform anahtar)
- Progress durumu global dict üzerinden; UI 1 s'de bir poll eder
- Günlük 06:15 scheduler job; startup'ta ÇALIŞMAZ
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from backend.config import settings
from backend.services.app_intel import APP_PRODUCTS

logger = logging.getLogger(__name__)

# ── Sabitler ──────────────────────────────────────────────────────────────────
BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/bigquery",)
QUERY_TIMEOUT_S = 25.0          # BigQuery job timeout (saniye)
BYTES_BUDGET = 200_000_000      # Sorgu başına 200 MB limit
MAX_CONCURRENT = 6              # Eş zamanlı max sorgu sayısı (paralel platform sorguları için)
CACHE_TTL_S = 4 * 3600         # 4 saat fresh cache
CACHE_STALE_TTL_S = 24 * 3600  # 24 saat stale — anında sun, arka planda yenile

_PLATFORM_PROJECTS = {"ios": "doviz-ios", "android": "doviz-android"}
_DATASET = "firebase_crashlytics"
_SESSIONS_DATASET = "firebase_sessions"

# ── Thread araçları ───────────────────────────────────────────────────────────
_BQ_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT)
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, Any]] = {}           # key → (ts, data)
_JOB_LOCK = threading.Lock()
_JOBS: dict[str, dict[str, Any]] = {}               # job_id → progress state

# Issue detail: ayrı semaphore (6 eşzamanlı — sorgular küçük/filtrelenmiş)
# ve ayrı cache (15 dakika TTL, issue_id bazlı)
_DETAIL_SEMAPHORE = threading.Semaphore(6)
_DETAIL_CACHE_TTL_S = 15 * 60
_DETAIL_CACHE_LOCK = threading.Lock()
_DETAIL_CACHE: dict[str, tuple[float, Any]] = {}    # key → (ts, data)
_DETAIL_BUILD_LOCKS_LOCK = threading.Lock()
_DETAIL_BUILD_LOCKS: dict[str, threading.Lock] = {}

# Circuit breaker: bir platform için "dataset boş" tespit edilirse 1 saat boyunca
# yeni sorgu deneme — BQ free-tier'ı tüketmesin ve audit log'u kirlemesin.
_EMPTY_DATASET_TTL_S = 60 * 60   # 1 saat
_EMPTY_DATASET_LOCK = threading.Lock()
_EMPTY_DATASET_UNTIL: dict[str, float] = {}        # platform → expiry timestamp

# Aynı cache key için eş zamanlı build_full_payload çağrılarını engelle.
# İlk çağrı BQ sorgularını başlatır; sonrakiler cache dolana dek bekler.
_BUILD_LOCKS_LOCK = threading.Lock()
_BUILD_LOCKS: dict[str, threading.Lock] = {}

# Stale-while-revalidate: hangi cache key'lerin arka plan yenilemesi aktif?
_BGREFRESH_LOCK = threading.Lock()
_BGREFRESH_ACTIVE: set[str] = set()

# UNION ALL şema uyumsuzluğu olan platformlar — _table() direkt batch tablosunu döner
_UNION_INCOMPAT_LOCK = threading.Lock()
_UNION_INCOMPAT: set[str] = set()  # platform keys


def _mark_union_incompat(platform: str) -> None:
    with _UNION_INCOMPAT_LOCK:
        _UNION_INCOMPAT.add(platform)


def _union_incompat(platform: str) -> bool:
    with _UNION_INCOMPAT_LOCK:
        return platform in _UNION_INCOMPAT


def _circuit_open(platform: str) -> bool:
    """True ise: bu platform için kısa süre içinde 'dataset boş' tespit edilmiş; sorgu atlanmalı."""
    with _EMPTY_DATASET_LOCK:
        until = _EMPTY_DATASET_UNTIL.get(platform, 0.0)
        return time.time() < until


def _circuit_trip(platform: str) -> None:
    with _EMPTY_DATASET_LOCK:
        _EMPTY_DATASET_UNTIL[platform] = time.time() + _EMPTY_DATASET_TTL_S


def _circuit_reset(platform: str) -> None:
    with _EMPTY_DATASET_LOCK:
        _EMPTY_DATASET_UNTIL.pop(platform, None)


# Dataset location cache — Firebase EU'da kuruluyor genelde, sorgular default US'e gidiyor.
# Lokasyonu bir kez tespit edip cache'liyoruz, sonraki sorgular doğru bölgeye gidiyor.
_DATASET_LOCATION_LOCK = threading.Lock()
_DATASET_LOCATION_CACHE: dict[str, str] = {}    # platform → "EU" / "europe-west1" / "US" vb.


def _get_dataset_location(platform: str) -> str | None:
    """Dataset'in BigQuery lokasyonunu öğren (cache'li). Bulunamazsa EU/US probe."""
    with _DATASET_LOCATION_LOCK:
        if platform in _DATASET_LOCATION_CACHE:
            return _DATASET_LOCATION_CACHE[platform] or None
    # 1) Önce dataset metadata API'si ile dene (en ucuz yol)
    try:
        from google.cloud import bigquery as _bq
        client = _get_client(platform)
        proj = _effective_project(platform)
        ref = _bq.DatasetReference(proj, _DATASET)
        ds = client.get_dataset(ref)
        loc = (ds.location or "").strip()
        with _DATASET_LOCATION_LOCK:
            _DATASET_LOCATION_CACHE[platform] = loc
        logger.info("Dataset lokasyonu tespit edildi: %s → %s", platform, loc or "(bilinmiyor)")
        return loc or None
    except Exception as exc:
        logger.warning("Dataset lokasyonu get_dataset ile alınamadı (%s): %s — lokasyon probe başlıyor", platform, exc)
    # 2) Metadata API başarısız olduysa yaygın Firebase konumlarını probe et.
    #    Firebase Crashlytics EU/Avrupa projeleri için varsayılan EU olur.
    _PROBE_LOCS = ("EU", "US", "europe-west1", "us-central1")
    try:
        client = _get_client(platform)
        proj = _effective_project(platform)
        sql = f"SELECT 1 FROM `{proj}.{_DATASET}.__TABLES__` LIMIT 1"
        for loc_try in _PROBE_LOCS:
            try:
                job = client.query(sql, location=loc_try)
                job.result(timeout=12)
                logger.info("Dataset lokasyonu probe ile tespit edildi: %s → %s", platform, loc_try)
                with _DATASET_LOCATION_LOCK:
                    _DATASET_LOCATION_CACHE[platform] = loc_try
                return loc_try
            except Exception:
                continue
    except Exception as exc2:
        logger.warning("Dataset lokasyonu probe da başarısız (%s): %s", platform, exc2)
    with _DATASET_LOCATION_LOCK:
        _DATASET_LOCATION_CACHE[platform] = ""
    return None


# ── Credential yükleme ────────────────────────────────────────────────────────

def _load_creds(platform: str) -> dict | None:
    raw = (
        settings.crashlytics_ios_service_account_json
        if platform == "ios"
        else settings.crashlytics_android_service_account_json
    ) or ""
    if not raw.strip():
        return None
    try:
        return json.loads(raw.strip())
    except Exception as exc:
        logger.warning("Crashlytics %s JSON ayrıştırma hatası: %s", platform, exc)
        return None


def _sa_email(platform: str) -> str | None:
    info = _load_creds(platform)
    if not info:
        return None
    return info.get("client_email")


def platform_ready(platform: str) -> bool:
    return bool(_load_creds(platform))


def any_platform_ready() -> bool:
    return platform_ready("ios") or platform_ready("android")


def _effective_project(platform: str) -> str:
    """BigQuery sorguları için kullanılacak GCP project_id'sini döndür.
    Service account JSON'undaki project_id öncelikli (gerçek erişim sahibi orası);
    yoksa _PLATFORM_PROJECTS fallback."""
    info = _load_creds(platform) or {}
    return str(info.get("project_id") or _PLATFORM_PROJECTS.get(platform, "")).strip()


def _get_client(platform: str):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = _load_creds(platform)
    if not info:
        raise ValueError(f"CRASHLYTICS_{platform.upper()}_SERVICE_ACCOUNT_JSON tanımlı değil.")
    creds = service_account.Credentials.from_service_account_info(info, scopes=BIGQUERY_SCOPES)
    project = _effective_project(platform)
    return bigquery.Client(credentials=creds, project=project)


# ── Tablo keşfi (BigQuery'de gerçek tablo adını otomatik bul) ──────────────────
# Firebase Crashlytics BigQuery export şablonu: {bundle_underscored}_{PLATFORM}
#   ör. com.Doviz + android  →  com_Doviz_ANDROID
# Realtime export ise:           {bundle_underscored}_REALTIME_{PLATFORM}
# Bazı eski/elle kurulan datasetlerde sadece bundle_underscored da olabilir.
# Tahmin etmek yerine __TABLES__ üzerinden gerçek adı keşfedip cache'liyoruz.
_TABLE_DISCOVERY_CACHE: dict[str, str | None] = {}
_TABLE_DISCOVERY_LOCK = threading.Lock()


def _list_dataset_tables(platform: str) -> list[str]:
    try:
        client = _get_client(platform)
        proj = _effective_project(platform)
        # _get_dataset_location zaten EU/US probe yapıyor; None dönerse EU'dan başla
        loc = _get_dataset_location(platform) or "EU"
        sql = f"SELECT table_id FROM `{proj}.{_DATASET}.__TABLES__`"
        query_job = client.query(sql, location=loc)
        return [str(r.table_id) for r in query_job.result(timeout=12)]
    except Exception as exc:
        logger.warning("BQ dataset listesi alınamadı (%s): %s", platform, exc)
        return []


def diagnose_platform(platform: str) -> dict:
    """Detaylı teşhis: tüm datasetleri listele, firebase_crashlytics var mı kontrol et,
    içindeki tabloları getir. Her adımda hata varsa onu da göster.
    """
    info = _load_creds(platform) or {}
    out: dict = {
        "platform": platform,
        "configured": bool(info),
        "service_account_email": info.get("client_email"),
        "sa_json_project_id": info.get("project_id"),
        "effective_project_id": _effective_project(platform),
        "hardcoded_project_id": _PLATFORM_PROJECTS.get(platform),
        "dataset_target": _DATASET,
        "dataset_location": _get_dataset_location(platform),
    }
    if not info:
        return out

    proj = _effective_project(platform)
    try:
        client = _get_client(platform)
        from google.cloud import bigquery as _bq
        # 1) Projedeki tüm dataset'leri listele
        try:
            datasets = [ds.dataset_id for ds in client.list_datasets(max_results=50)]
            out["all_datasets_in_project"] = datasets
        except Exception as exc:
            out["list_datasets_error"] = str(exc)[:300]
            datasets = []
        # 2) HER dataset için tablo sayısı (smoking gun: hangi dataset'e veri akıyor?)
        all_dataset_status: dict[str, dict] = {}
        for ds_id in datasets:
            ds_info: dict = {}
            try:
                ref = _bq.DatasetReference(proj, ds_id)
                ds = client.get_dataset(ref)
                ds_info["location"] = ds.location
                ds_info["created"] = ds.created.isoformat() if ds.created else None
                ds_info["modified"] = ds.modified.isoformat() if ds.modified else None
            except Exception as exc:
                ds_info["meta_error"] = str(exc)[:200]
            try:
                tables = list(client.list_tables(f"{proj}.{ds_id}", max_results=20))
                ds_info["table_count"] = len(tables)
                ds_info["tables"] = [t.table_id for t in tables[:10]]
            except Exception as exc:
                ds_info["tables_error"] = str(exc)[:200]
            all_dataset_status[ds_id] = ds_info
        out["all_datasets_status"] = all_dataset_status
        # 3) firebase_crashlytics dataset'i var mı? (geriye uyumluluk için)
        try:
            client.get_dataset(_bq.DatasetReference(proj, _DATASET))
            out["dataset_exists"] = True
        except Exception as exc:
            out["dataset_exists"] = False
            out["dataset_check_error"] = str(exc)[:300]
        # 4) Crashlytics dataset'inde tablo getir (eski alan, korunuyor)
        if out.get("dataset_exists"):
            try:
                tables = [t.table_id for t in client.list_tables(f"{proj}.{_DATASET}")]
                out["dataset_tables"] = tables
                out["dataset_table_count"] = len(tables)
            except Exception as exc:
                out["list_tables_error"] = str(exc)[:300]
    except Exception as exc:
        out["client_error"] = str(exc)[:300]
    return out


def _discover_table_id(platform: str, bundle: str) -> str | None:
    """Verilen bundle için datasette mevcut gerçek table_id'yi bul."""
    key = f"{platform}:{bundle}"
    with _TABLE_DISCOVERY_LOCK:
        if key in _TABLE_DISCOVERY_CACHE:
            return _TABLE_DISCOVERY_CACHE[key]
    base = bundle.replace(".", "_")
    plat_up = platform.upper()
    # Olası adlandırmalar (öncelik sırasıyla). Firebase _REALTIME suffix'ini
    # hem önde hem arkada koyabiliyor (streaming export'ta sona, batch'te yok).
    candidates = [
        f"{base}_{plat_up}",                    # com_Doviz_ANDROID  (standart)
        f"{base}_{plat_up}_REALTIME",           # com_nokta_Finans_Takip_IOS_REALTIME (streaming)
        f"{base}_REALTIME_{plat_up}",           # com_Doviz_REALTIME_ANDROID
        base,                                    # com_Doviz (eski)
        base.lower() + "_" + plat_up,
        base.lower(),
        base.lower() + "_realtime_" + plat_up.lower(),
        base.lower() + "_" + plat_up.lower() + "_realtime",
    ]
    available = _list_dataset_tables(platform)
    if not available:
        with _TABLE_DISCOVERY_LOCK:
            _TABLE_DISCOVERY_CACHE[key] = None
        return None
    available_lower = {t.lower(): t for t in available}
    # 1) Exact (case-insensitive) match
    for c in candidates:
        match = available_lower.get(c.lower())
        if match:
            with _TABLE_DISCOVERY_LOCK:
                _TABLE_DISCOVERY_CACHE[key] = match
            logger.info("Crashlytics tablo keşfedildi: %s → %s", key, match)
            return match
    # 2) Substring match: bundle base + platform suffix tablo adında geçiyor mu?
    base_lower = base.lower()
    for t_lower, t_orig in available_lower.items():
        if base_lower in t_lower and plat_up.lower() in t_lower:
            with _TABLE_DISCOVERY_LOCK:
                _TABLE_DISCOVERY_CACHE[key] = t_orig
            logger.info("Crashlytics tablo (fuzzy) keşfedildi: %s → %s", key, t_orig)
            return t_orig
    # 3) Sadece base substring
    for t_lower, t_orig in available_lower.items():
        if base_lower in t_lower:
            with _TABLE_DISCOVERY_LOCK:
                _TABLE_DISCOVERY_CACHE[key] = t_orig
            logger.info("Crashlytics tablo (base-only) keşfedildi: %s → %s", key, t_orig)
            return t_orig
    with _TABLE_DISCOVERY_LOCK:
        _TABLE_DISCOVERY_CACHE[key] = None
    logger.warning(
        "Crashlytics tablosu bulunamadı: %s; datasette mevcut: %s",
        key, ", ".join(available[:15]) or "(boş)",
    )
    return None


def _table(platform: str, bundle: str) -> str:
    """Bundle için BigQuery FROM kaynağı.

    Hem standart (batch) hem de _REALTIME (streaming) tablosu varsa ikisini
    UNION ALL ile birleştiren bir subquery döner — eski + yeni veri tek sorguda.
    Sadece biri varsa o tablonun backtick'li path'ini döner.
    """
    proj = _effective_project(platform)
    base = bundle.replace(".", "_")
    plat_up = platform.upper()

    available = _list_dataset_tables(platform)
    available_lower = {t.lower(): t for t in available} if available else {}

    # Standart (batch) ve streaming (REALTIME) tablolarını bağımsız keşfet
    batch_candidates = [
        f"{base}_{plat_up}",
        base,
        base.lower() + "_" + plat_up.lower(),
    ]
    realtime_candidates = [
        f"{base}_{plat_up}_REALTIME",
        f"{base}_REALTIME_{plat_up}",
        base.lower() + "_" + plat_up.lower() + "_realtime",
        base.lower() + "_realtime_" + plat_up.lower(),
    ]

    def _find(candidates: list[str]) -> str | None:
        for c in candidates:
            m = available_lower.get(c.lower())
            if m:
                return m
        return None

    batch_tid   = _find(batch_candidates)
    realtime_tid = _find(realtime_candidates)

    # Her iki tablo varsa explicit sütun seçimiyle UNION ALL — SELECT * yapmak BOOL/STRING
    # uyumsuzluğuna yol açıyor (is_fatal/is_anr). Kullandığımız sütunları seçerek bunu atlıyoruz.
    # Realtime tablosu sadece BUGÜNÜN verisini içersin: gece batch export'u dünü kapattı, realtime
    # bugünü tamamlıyor. TIMESTAMP_TRUNC filtresi sayesinde aynı event iki tabloda çift sayılmaz.
    if batch_tid and realtime_tid and not _union_incompat(platform):
        logger.info("Crashlytics batch+realtime UNION ALL: %s + %s", batch_tid, realtime_tid)
        batch_ref = f"`{proj}.{_DATASET}.{batch_tid}`"
        rt_ref    = f"`{proj}.{_DATASET}.{realtime_tid}`"
        # iOS tablolarında device.manufacturer alanı yoktur (sadece Android'de var).
        # NULL dökmek yerine platform'a göre seçiyoruz; dış sorgular manufacturer'a NULL olarak erişir.
        if platform == "android":
            device_struct = "STRUCT(device.model AS model, device.manufacturer AS manufacturer) AS device"
        else:
            device_struct = "STRUCT(device.model AS model, CAST(NULL AS STRING) AS manufacturer) AS device"
        cols = (
            "event_timestamp, error_type, installation_uuid, issue_id, issue_title, "
            "firebase_session_id, "
            "STRUCT(application.display_version AS display_version) AS application, "
            f"{device_struct}, "
            "STRUCT(operating_system.display_version AS display_version) AS operating_system, "
            "STRUCT(blame_frame.file AS file, blame_frame.symbol AS symbol, "
            "       blame_frame.line AS line) AS blame_frame"
        )
        # Realtime tablosunu sadece bugünkü eventlerle sınırla (batch ile overlap önlenir)
        rt_today = "event_timestamp >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)"
        return (
            f"(SELECT {cols} FROM {batch_ref} "
            f"UNION ALL "
            f"SELECT {cols} FROM {rt_ref} WHERE {rt_today})"
        )
    if batch_tid:
        logger.info("Crashlytics tablo (batch): %s", batch_tid)
        return f"`{proj}.{_DATASET}.{batch_tid}`"
    if realtime_tid:
        logger.info("Crashlytics tablo (realtime): %s", realtime_tid)
        return f"`{proj}.{_DATASET}.{realtime_tid}`"

    # Discovery başarısız → fallback pattern
    fallback = f"{base}_{plat_up}"
    logger.warning("Crashlytics tablo keşfedilemedi, fallback: %s", fallback)
    return f"`{proj}.{_DATASET}.{fallback}`"


# ── Cache yardımcıları ────────────────────────────────────────────────────────

def _cache_get(key: str) -> Any | None:
    """Fresh cache (< CACHE_TTL_S). None if expired or missing."""
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and time.time() - entry[0] < CACHE_TTL_S:
            return entry[1]
    return None


def _cache_get_stale(key: str) -> Any | None:
    """Stale cache (< CACHE_STALE_TTL_S). Returns data even if past fresh TTL."""
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and time.time() - entry[0] < CACHE_STALE_TTL_S:
            return entry[1]
    return None


def _cache_set(key: str, value: Any) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), value)


def _trigger_bg_refresh(pid: str, days: int, platform_filter: str, cache_key: str) -> None:
    """Stale veri sunulduğunda arka planda cache'i yenile (tekrar giriş önlenir)."""
    with _BGREFRESH_LOCK:
        if cache_key in _BGREFRESH_ACTIVE:
            return
        _BGREFRESH_ACTIVE.add(cache_key)

    def _worker():
        try:
            # Stale girdiyi sil ki build_full_payload yeni sorgu yapsın
            with _CACHE_LOCK:
                _CACHE.pop(cache_key, None)
            build_full_payload(pid, days=days, platform_filter=platform_filter)
        except Exception as exc:
            logger.warning("Arka plan cache yenileme başarısız (%s): %s", cache_key, exc)
        finally:
            with _BGREFRESH_LOCK:
                _BGREFRESH_ACTIVE.discard(cache_key)

    threading.Thread(target=_worker, daemon=True, name=f"bg-refresh-{cache_key}").start()


# ── Progress tracking ─────────────────────────────────────────────────────────

def _job_new(product: str) -> str:
    jid = str(uuid.uuid4())[:8]
    with _JOB_LOCK:
        _JOBS[jid] = {"product": product, "pct": 0, "step": "Başlatılıyor…", "done": False, "error": None, "ts": time.time()}
        # 10'dan fazla iş varsa eskisini temizle
        if len(_JOBS) > 10:
            oldest = sorted(_JOBS.items(), key=lambda x: x[1]["ts"])
            for k, _ in oldest[:5]:
                del _JOBS[k]
    return jid


def _job_update(jid: str, pct: int, step: str) -> None:
    with _JOB_LOCK:
        if jid in _JOBS:
            _JOBS[jid].update({"pct": pct, "step": step})


def _job_done(jid: str, error: str | None = None) -> None:
    with _JOB_LOCK:
        if jid in _JOBS:
            _JOBS[jid].update({"pct": 100, "done": True, "error": error, "step": "Tamamlandı" if not error else "Hata"})


def get_job_state(product: str) -> dict | None:
    """Ürün için en son iş durumunu döndür."""
    with _JOB_LOCK:
        running = [v for v in _JOBS.values() if v["product"] == product and not v["done"]]
        if running:
            return sorted(running, key=lambda x: -x["ts"])[0]
        completed = [v for v in _JOBS.values() if v["product"] == product and v["done"]]
        if completed:
            return sorted(completed, key=lambda x: -x["ts"])[0]
    return None


# ── Byte tahmini (dry-run) ────────────────────────────────────────────────────

def _dry_run_bytes(client, sql: str, location: str | None = None) -> int:
    """Sorgunun kaç byte işleyeceğini tahmin et (ağ trafiği yok, ücretsiz)."""
    try:
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=cfg, location=location) if location else client.query(sql, job_config=cfg)
        return int(job.total_bytes_processed or 0)
    except Exception:
        return 0


# ── Storage kullanımı (metadata, ücretsiz) ────────────────────────────────────

def get_storage_bytes(platform: str) -> int:
    """Dataset'in toplam byte boyutunu döndür (metadata sorgusu, byte taramaz)."""
    try:
        client = _get_client(platform)
        proj = _effective_project(platform)
        loc = _get_dataset_location(platform)
        sql = f"SELECT SUM(size_bytes) AS total FROM `{proj}.{_DATASET}.__TABLES__`"
        query_job = client.query(sql, location=loc) if loc else client.query(sql)
        for row in query_job.result(timeout=10):
            return int(row.total or 0)
    except Exception as exc:
        logger.debug("Storage byte sorgusu başarısız (%s): %s", platform, exc)
    return 0


def get_all_storage_mb() -> dict[str, float]:
    """Her platform için MB cinsinden storage döndür."""
    out: dict[str, float] = {}
    for plat in ("ios", "android"):
        if platform_ready(plat):
            b = get_storage_bytes(plat)
            out[plat] = round(b / 1_000_000, 1)
    return out


# ── Temel sorgu çalıştırıcı ───────────────────────────────────────────────────

def _run_query(platform: str, sql: str, *, skip_budget: bool = False) -> tuple[list[dict], str | None]:
    """Güvenli sorgu çalıştırıcı: semaphore, budget, timeout, hata yönetimi."""
    from google.api_core import exceptions as gexc

    if not platform_ready(platform):
        return [], f"CRASHLYTICS_{platform.upper()}_SERVICE_ACCOUNT_JSON tanımlı değil."

    # Circuit breaker: bu platform için yakın zamanda 'boş dataset' tespit ettiysek
    # tekrar denemeye gerek yok — kullanıcıya net mesaj göster, BQ kotasını tüketme.
    if _circuit_open(platform):
        return [], (
            "Firebase Crashlytics tablosu yok ve son 1 saat içinde bu tespit edildi "
            "(query yağmurunu engellemek için bekleniyor). Firebase Console → BigQuery "
            "integration'da Crashlytics toggle'ını açıp Save yaptıktan sonra ~6-24 saat bekleyin."
        )

    acquired = _BQ_SEMAPHORE.acquire(timeout=5)
    if not acquired:
        return [], "Çok fazla eş zamanlı sorgu. Lütfen bekleyin."

    try:
        client = _get_client(platform)

        # Dataset lokasyonu (EU/US vb.) — sorguları doğru bölgeye yolla
        loc = _get_dataset_location(platform)

        # Budget kontrolü
        if not skip_budget:
            est = _dry_run_bytes(client, sql, location=loc)
            if est > BYTES_BUDGET:
                mb = round(est / 1_000_000, 1)
                return [], f"Sorgu tahmini çok büyük ({mb} MB > {BYTES_BUDGET//1_000_000} MB). Dönem filtresi daraltın."

        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(use_query_cache=True)
        job = client.query(sql, job_config=cfg, location=loc) if loc else client.query(sql, job_config=cfg)
        rows = []
        for r in job.result(timeout=QUERY_TIMEOUT_S):
            rows.append(dict(r))
        return rows, None

    except gexc.NotFound as exc:
        logger.warning("BQ tablo bulunamadı (%s): %s", platform, exc)
        # Datasette ne var, gerçekten yok mu yoksa adlandırma mı kaymış — kullanıcıya göster
        proj = _effective_project(platform) or platform
        available = _list_dataset_tables(platform)
        if not available:
            # Dataset boş — circuit breaker'ı 1 saat tetikle; gereksiz query atma
            _circuit_trip(platform)
            return [], (
                f"`{proj}.{_DATASET}` datasetinde tablo yok. "
                "Firebase Console → Project Settings → Integrations → BigQuery'den "
                "Crashlytics export'unun aktif olduğunu doğrulayın; ilk tablo oluşması "
                "için Firebase export başladıktan ~24 saat geçmesi gerekir. "
                "(Tekrar denemek 1 saat boyunca atlandı — BQ kotasını korumak için.)"
            )
        # Bundle ile eşleşen tablo yok — circuit breaker'ı tetikle ki aynı build'in
        # geri kalan 5 sorgusu (ve sonraki manuel yenilemeler) BQ kotasını tüketmesin.
        _circuit_trip(platform)
        shown = ", ".join(available[:8]) + ("…" if len(available) > 8 else "")
        return [], (
            f"Bundle ile eşleşen tablo bulunamadı (`{proj}.{_DATASET}`). "
            f"Datasette mevcut tablolar: {shown}. "
            "APP_PRODUCTS'taki bundle/package adı ile BigQuery tablo adı eşleşmiyor "
            "olabilir — beklenen şablon: `<bundle>_ANDROID` / `<bundle>_IOS` / `<bundle>_IOS_REALTIME`."
        )
    except gexc.Forbidden as exc:
        logger.warning("BQ erişim reddedildi (%s): %s", platform, exc)
        proj = _effective_project(platform) or platform
        email = _sa_email(platform) or "(service account)"
        raw = str(exc).strip()[:300]
        return [], (
            f"Erişim reddedildi · proje `{proj}` · {email}. "
            f"GCP Console → IAM → bu service account'a şu iki rolü verin: "
            f"`BigQuery Data Viewer` + `BigQuery Job User`. "
            f"[Detay: {raw}]"
        )
    except Exception as exc:
        msg = str(exc).strip()
        if "Timeout" in msg or "deadline" in msg.lower():
            return [], f"Sorgu zaman aşımı ({QUERY_TIMEOUT_S}s). Dönem filtresini daraltın veya tekrar deneyin."
        # Batch ve realtime tablo şemaları farklı olduğunda UNION ALL patlar (BOOL vs STRING vb.)
        # Fallback: subquery'den sadece batch tablosunu kullan, realtime'ı atla.
        if "UNION ALL" in msg and "incompatible types" in msg:
            import re as _re2
            fixed_sql = _re2.sub(
                r"\(SELECT \* FROM (`[^`]+`) UNION ALL SELECT \* FROM `[^`]+`\)",
                r"\1",
                sql,
            )
            if fixed_sql != sql:
                _mark_union_incompat(platform)  # sonraki sorgular UNION ALL oluşturmaz
                logger.warning("UNION ALL şema uyumsuzluğu (%s), batch tablosuna fallback: %s", platform, msg[:120])
                try:
                    from google.cloud import bigquery as _bq2
                    cfg2 = _bq2.QueryJobConfig(use_query_cache=True)
                    loc2 = _get_dataset_location(platform)
                    job2 = client.query(fixed_sql, job_config=cfg2, location=loc2) if loc2 else client.query(fixed_sql, job_config=cfg2)
                    rows2 = [dict(r) for r in job2.result(timeout=QUERY_TIMEOUT_S)]
                    return rows2, None
                except Exception as exc2:
                    return [], f"BigQuery hatası: {str(exc2).strip()[:200]}"
        logger.exception("BQ sorgu hatası (%s)", platform)
        return [], f"BigQuery hatası: {msg[:200]}"
    finally:
        _BQ_SEMAPHORE.release()


# ── SQL şablonları ────────────────────────────────────────────────────────────

def _ts_filter(days: int) -> str:
    return f"event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)"


def _type_filter(error_type: str | None) -> str:
    if error_type and error_type.upper() in ("FATAL", "NON_FATAL", "ANR"):
        return f"AND error_type = '{error_type.upper()}'"
    return ""


def _version_filter(version: str | None) -> str:
    if version and version.strip():
        safe = version.replace("'", "''")
        return f"AND application.display_version = '{safe}'"
    return ""


def _versions_filter_sql(
    versions: list[str] | None = None,
    version: str | None = None,
) -> str:
    """Tek veya çoklu app sürümü — SQL IN / =."""
    vers: list[str] = []
    for v in versions or []:
        s = (v or "").strip()
        if s:
            vers.append(s.replace("'", "''"))
    if version and version.strip():
        safe = version.strip().replace("'", "''")
        if safe not in vers:
            vers.append(safe)
    if not vers:
        return ""
    if len(vers) == 1:
        return f"AND application.display_version = '{vers[0]}'"
    quoted = ", ".join(f"'{v}'" for v in vers)
    return f"AND application.display_version IN ({quoted})"


def _event_filters_sql(
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> str:
    """error_type + app sürümü — WHERE içine eklenir."""
    return _type_filter(error_type) + _versions_filter_sql(versions, version)


# ── Sorgu fonksiyonları ───────────────────────────────────────────────────────

def query_summary(
    platform: str,
    table: str,
    days: int,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    # ROLLUP: son satırda error_type=NULL → tüm event_type'lar arası gerçek unique kullanıcı sayısı.
    # ROLLUP olmadan FATAL+NON_FATAL toplandığında her ikisinde de olan kullanıcılar çift sayılır.
    sql = f"""
SELECT
  error_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM {table}
WHERE {_ts_filter(days)}
  {_type_filter(error_type)}
  {_versions_filter_sql(versions, version)}
GROUP BY ROLLUP(error_type)
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    result: dict[str, Any] = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0, "error": err}
    for r in rows:
        et = (r.get("error_type") or "").upper()
        n = int(r.get("event_count") or 0)
        u = int(r.get("affected_users") or 0)
        if et == "FATAL":
            result["fatal"] = n
        elif et == "ANR":
            result["anr"] = n
        elif et == "NON_FATAL":
            result["non_fatal"] = n
        else:
            # ROLLUP grand total satırı (error_type IS NULL) — gerçek unique kullanıcı sayısı
            result["affected_users"] = u
    return result


def _list_sessions_tables(platform: str) -> list[str]:
    try:
        client = _get_client(platform)
        proj = _effective_project(platform)
        loc = _get_dataset_location(platform) or "EU"
        sql = f"SELECT table_id FROM `{proj}.{_SESSIONS_DATASET}.__TABLES__`"
        query_job = client.query(sql, location=loc)
        return [str(r.table_id) for r in query_job.result(timeout=12)]
    except Exception as exc:
        logger.debug("Sessions dataset listesi alınamadı (%s): %s", platform, exc)
        return []


def _discover_sessions_table_id(platform: str, bundle: str) -> str | None:
    """firebase_sessions dataset'inde app tablosunu bul."""
    base = bundle.replace(".", "_")
    plat_up = platform.upper()
    candidates = [
        f"{base}_{plat_up}",
        f"{base}_{plat_up}_REALTIME",
        f"{base}_REALTIME_{plat_up}",
        base,
        base.lower() + "_" + plat_up.lower(),
    ]
    available = _list_sessions_tables(platform)
    if not available:
        return None
    available_lower = {t.lower(): t for t in available}
    for c in candidates:
        m = available_lower.get(c.lower())
        if m:
            return m
    base_lower = base.lower()
    for t_lower, t_orig in available_lower.items():
        if base_lower in t_lower and plat_up.lower() in t_lower:
            return t_orig
    return None


def _sessions_table_ref(platform: str, bundle: str) -> str | None:
    tid = _discover_sessions_table_id(platform, bundle)
    if not tid:
        return None
    proj = _effective_project(platform)
    return f"`{proj}.{_SESSIONS_DATASET}.{tid}`"


def query_table_health_stats(platform: str, table: str, days: int) -> dict[str, Any]:
    """Platform tablosu sağlık metrikleri — iOS/Android karşılaştırma için."""
    sql = f"""
SELECT
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users,
  COUNTIF(error_type = 'FATAL') AS fatal_count,
  COUNTIF(error_type = 'ANR') AS anr_count,
  COUNTIF(error_type = 'NON_FATAL') AS non_fatal_count,
  COUNTIF(firebase_session_id IS NOT NULL AND firebase_session_id != '') AS events_with_session_id,
  MIN(event_timestamp) AS first_event,
  MAX(event_timestamp) AS last_event,
  COUNT(DISTINCT DATE(event_timestamp, 'Europe/Istanbul')) AS active_days
FROM {table}
WHERE {_ts_filter(days)}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    if err or not rows:
        return {"error": err}
    r = rows[0]
    ev = int(r.get("event_count") or 0)
    with_sid = int(r.get("events_with_session_id") or 0)
    fe, le = r.get("first_event"), r.get("last_event")
    return {
        "event_count": ev,
        "affected_users": int(r.get("affected_users") or 0),
        "fatal_count": int(r.get("fatal_count") or 0),
        "anr_count": int(r.get("anr_count") or 0),
        "non_fatal_count": int(r.get("non_fatal_count") or 0),
        "events_with_session_id": with_sid,
        "session_id_coverage_pct": round(with_sid / ev * 100, 1) if ev > 0 else 0.0,
        "first_event": fe.isoformat() if hasattr(fe, "isoformat") else (str(fe) if fe else None),
        "last_event": le.isoformat() if hasattr(le, "isoformat") else (str(le) if le else None),
        "active_days": int(r.get("active_days") or 0),
    }


def _query_crash_free_sessions(
    platform: str, crash_table: str, sessions_ref: str, days: int
) -> dict[str, Any] | None:
    """Firebase Console uyumlu crash-free — firebase_sessions + crash join."""
    sql = f"""
SELECT
  COUNT(DISTINCT s.session_id) AS total_sessions,
  COUNT(DISTINCT IF(c.error_type = 'FATAL', s.session_id, NULL)) AS crashed_sessions,
  COUNT(DISTINCT s.instance_id) AS total_instances,
  COUNT(DISTINCT IF(c.error_type = 'FATAL', c.installation_uuid, NULL)) AS fatal_users
FROM {sessions_ref} AS s
LEFT JOIN {crash_table} AS c
  ON c.firebase_session_id = s.session_id
  AND c.event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
WHERE s.event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    if err or not rows:
        return None
    r = rows[0]
    total_sess = int(r.get("total_sessions") or 0)
    crashed_sess = int(r.get("crashed_sessions") or 0)
    total_inst = int(r.get("total_instances") or 0)
    fatal_users = int(r.get("fatal_users") or 0)
    if total_sess < 10 and total_inst < 10:
        return None
    # Oturum bazlı (Console'daki crash-free sessions'a yakın)
    cfs = round((1 - crashed_sess / total_sess) * 100, 4) if total_sess > 0 else None
    cfu = round((1 - fatal_users / total_inst) * 100, 4) if total_inst > 0 else None
    pct = cfs if cfs is not None else cfu
    if pct is None:
        return None
    return {
        "total_users": total_inst or total_sess,
        "crashed_users": fatal_users or crashed_sess,
        "total_sessions": total_sess,
        "crashed_sessions": crashed_sess,
        "crash_free_pct": pct,
        "crash_free_sessions_pct": cfs,
        "crash_free_users_pct": cfu,
        "method": "firebase_sessions",
    }


def _query_crash_free_crashes_only(platform: str, table: str, days: int) -> dict[str, Any] | None:
    """Eski yöntem — yalnız crash tablosu; payda yanlış olduğu için güvenilmez."""
    sql = f"""
SELECT
  COUNT(DISTINCT installation_uuid) AS total_users,
  COUNT(DISTINCT IF(error_type = 'FATAL', installation_uuid, NULL)) AS crashed_users
FROM {table}
WHERE {_ts_filter(days)}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    if err or not rows:
        return None
    r = rows[0]
    total = int(r.get("total_users") or 0)
    crashed = int(r.get("crashed_users") or 0)
    if total <= 0:
        return None
    pct = round((1 - crashed / total) * 100, 2)
    return {
        "total_users": total,
        "crashed_users": crashed,
        "crash_free_pct": pct,
        "method": "crashes_only_unreliable",
        "note": "Sadece crash tablosu — Console ile uyuşmaz; firebase_sessions export gerekir.",
    }


def query_crash_free(
    platform: str, table: str, days: int, *, bundle: str = ""
) -> dict[str, Any] | None:
    """Crash-free oranı — önce firebase_sessions, yoksa güvenilmez fallback (gizlenir)."""
    bundle = (bundle or "").strip()
    # Session join için batch tablo tercih et (UNION alt sorgusu eksik alan içerebilir)
    crash_table = table
    if bundle:
        batch_ref = _batch_table_ref(platform, bundle)
        if batch_ref:
            crash_table = batch_ref
    if bundle:
        sessions_ref = _sessions_table_ref(platform, bundle)
        if sessions_ref:
            result = _query_crash_free_sessions(platform, crash_table, sessions_ref, days)
            if result and result.get("crash_free_pct") is not None:
                result["sessions_table"] = sessions_ref.strip("`")
                return result
    legacy = _query_crash_free_crashes_only(platform, crash_table, days)
    if not legacy:
        return None
    # Crash-only taban: neredeyse her satır crash → %0–2; Console'a gösterme
    if legacy.get("crash_free_pct", 0) < 2.0:
        return None
    return legacy


def analyze_platform_parity(product_id: str, days: int = 7) -> dict[str, Any]:
    """iOS vs Android veri farkı + crash-free teşhisi."""
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product"}
    meta = APP_PRODUCTS[pid]
    android_pkg = (meta.get("android_package") or "").strip()
    ios_bundle = (meta.get("ios_bundle_id") or "").strip()

    report: dict[str, Any] = {
        "ok": True,
        "product": pid,
        "days": days,
        "platforms": {},
        "comparison": {},
        "findings": [],
    }
    plat_specs = [
        ("android", android_pkg),
        ("ios", ios_bundle),
    ]
    for plat, bundle in plat_specs:
        block: dict[str, Any] = {
            "bundle_id": bundle,
            "configured": platform_ready(plat),
            "circuit_open": _circuit_open(plat),
            "union_incompat": _union_incompat(plat),
        }
        if not bundle:
            block["error"] = "bundle_missing"
            report["platforms"][plat] = block
            continue
        if not platform_ready(plat):
            block["error"] = "credential_missing"
            report["platforms"][plat] = block
            continue
        if _circuit_open(plat):
            block["error"] = "circuit_breaker_open"
            report["platforms"][plat] = block
            continue

        discovered = _discover_table_id(plat, bundle)
        block["discovered_crash_table"] = discovered
        block["sessions_table_id"] = _discover_sessions_table_id(plat, bundle)
        block["sessions_export_enabled"] = bool(block["sessions_table_id"])
        tbl = _table(plat, bundle)
        block["query_table"] = tbl[:120] + ("…" if len(tbl) > 120 else "")
        block["uses_union"] = "UNION ALL" in tbl
        health_table = _batch_table_ref(plat, bundle) or tbl
        block["health_table"] = health_table[:120] + ("…" if len(health_table) > 120 else "")

        summary = query_summary(plat, tbl, days)
        block["summary"] = {k: v for k, v in summary.items() if k != "error"}
        if summary.get("error"):
            block["summary_error"] = summary["error"]

        health = query_table_health_stats(plat, health_table, days)
        block["health"] = health

        cf = query_crash_free(plat, tbl, days, bundle=bundle)
        block["crash_free"] = cf
        legacy = _query_crash_free_crashes_only(plat, health_table, days)
        block["crash_free_legacy"] = legacy

        batch_ref = _batch_table_ref(plat, bundle)
        if batch_ref and batch_ref != tbl:
            batch_health = query_table_health_stats(plat, batch_ref, days)
            block["batch_only_health"] = batch_health

        report["platforms"][plat] = block

    a = report["platforms"].get("android", {})
    i = report["platforms"].get("ios", {})
    a_ev = int((a.get("health") or {}).get("event_count") or 0)
    i_ev = int((i.get("health") or {}).get("event_count") or 0)
    ratio = round(i_ev / a_ev * 100, 1) if a_ev > 0 else None
    report["comparison"] = {
        "android_events": a_ev,
        "ios_events": i_ev,
        "ios_vs_android_pct": ratio,
        "android_users": int((a.get("health") or {}).get("affected_users") or 0),
        "ios_users": int((i.get("health") or {}).get("affected_users") or 0),
    }

    findings: list[str] = []
    if not i.get("sessions_export_enabled"):
        findings.append(
            "iOS'ta firebase_sessions tablosu yok — Crash-free oranı Console ile hesaplanamaz. "
            "Firebase Console → Integrations → BigQuery → «Include sessions» açın."
        )
    if not a.get("sessions_export_enabled"):
        findings.append("Android'de firebase_sessions export yok — crash-free yine de crash tablosundan tahmin edilemez.")

    if i.get("crash_free_legacy") and (i["crash_free_legacy"].get("crash_free_pct") or 0) < 5:
        findings.append(
            "iOS crash-free eski formülle ~%0 çıkıyor (crash tablosu paydası yanlış) — bu yüzden UI'da hiç görünmüyordu."
        )

    if ratio is not None and ratio < 50:
        findings.append(
            f"iOS olay sayısı Android'in %{ratio}'i — teknik veya trafik farkı olabilir; "
            "tablo keşfi, union_incompat ve active_days alanlarına bakın."
        )
    if i.get("union_incompat"):
        findings.append("iOS batch/realtime UNION şema uyumsuz — yalnız batch tablosu kullanılıyor (bugünkü realtime eksik olabilir).")
    if a.get("union_incompat"):
        findings.append("Android UNION şema uyumsuz — realtime verisi atlanıyor.")

    i_cov = (i.get("health") or {}).get("session_id_coverage_pct")
    a_cov = (a.get("health") or {}).get("session_id_coverage_pct")
    if i_cov is not None and a_cov is not None and i_cov < a_cov * 0.5:
        findings.append(
            f"iOS firebase_session_id doluluk %{i_cov} (Android %{a_cov}) — session join zayıf; crash-free join etkilenebilir."
        )

    i_days = (i.get("health") or {}).get("active_days") or 0
    a_days = (a.get("health") or {}).get("active_days") or 0
    if i_days and a_days and i_days < a_days * 0.5:
        findings.append(
            f"iOS'ta yalnız {i_days} günlük veri var (Android {a_days} gün) — export geç başlamış veya tablo eksik olabilir."
        )

    report["findings"] = findings
    return report


def query_oldest_date(platform: str, table: str, days: int) -> int | None:
    """Tablodaki en eski event_timestamp'i sorgular; gün cinsinden gerçek veri yaşını döner."""
    # Türkiye saati (Firebase Console'un gösterdiği yerel saat) ile aynı gün hesabı için.
    sql = f"""
SELECT DATE(MIN(event_timestamp), 'Europe/Istanbul') AS oldest_date
FROM {table}
WHERE {_ts_filter(days)}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    if err or not rows:
        return None
    oldest = rows[0].get("oldest_date")
    if not oldest:
        return None
    today = datetime.now(timezone.utc).date()
    delta = (today - oldest).days + 1
    return min(delta, days)


def query_top_issues(
    platform: str,
    table: str,
    days: int,
    error_type: str | None = None,
    version: str | None = None,
    *,
    versions: list[str] | None = None,
    limit: int = 30,
) -> tuple[list[dict], str | None]:
    ver_sql = _versions_filter_sql(versions, version)
    sql = f"""
SELECT
  COALESCE(issue_id, '') AS issue_id,
  COALESCE(issue_title, '') AS issue_title,
  COALESCE(error_type, '') AS error_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users,
  APPROX_TOP_COUNT(COALESCE(application.display_version, 'bilinmiyor'), 1)[OFFSET(0)].value AS latest_version,
  MIN(event_timestamp) AS first_seen,
  MAX(event_timestamp) AS last_seen
FROM {table}
WHERE {_ts_filter(days)}
  {_type_filter(error_type)}
  {ver_sql}
GROUP BY issue_id, issue_title, error_type
HAVING COUNT(*) > 0
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "issue_id": r.get("issue_id", ""),
            "issue_title": r.get("issue_title", "") or "(başlıksız)",
            "error_type": r.get("error_type", ""),
            "event_count": int(r.get("event_count") or 0),
            "affected_users": int(r.get("affected_users") or 0),
            "latest_version": r.get("latest_version") or "—",
            "first_seen": r.get("first_seen"),
            "last_seen": r.get("last_seen"),
        }
        for r in rows
    ], err


def _batch_table_ref(platform: str, bundle: str) -> str | None:
    """Batch tablo referansı — threads/breadcrumbs gibi geniş şema alanları için."""
    proj = _effective_project(platform)
    base = bundle.replace(".", "_")
    plat_up = platform.upper()
    available = _list_dataset_tables(platform)
    available_lower = {t.lower(): t for t in available} if available else {}
    batch_candidates = [
        f"{base}_{plat_up}",
        base,
        base.lower() + "_" + plat_up.lower(),
    ]
    for c in batch_candidates:
        m = available_lower.get(c.lower())
        if m and "realtime" not in m.lower():
            return f"`{proj}.{_DATASET}.{m}`"
    tid = _discover_table_id(platform, bundle)
    if tid and "realtime" not in tid.lower():
        return f"`{proj}.{_DATASET}.{tid}`"
    return None


def query_version_time_series(
    platform: str,
    table: str,
    days: int,
    *,
    limit_versions: int = 6,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    """Günlük olay sayısı × app versiyonu (Console'daki versiyon grafiği)."""
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    if extra.strip():
        sql = f"""
SELECT
  DATE(event_timestamp, 'Europe/Istanbul') AS crash_date,
  COALESCE(application.display_version, 'bilinmiyor') AS app_version,
  COUNT(*) AS event_count
FROM {table}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY crash_date, app_version
ORDER BY crash_date ASC, app_version ASC
"""
    else:
        sql = f"""
WITH top_versions AS (
  SELECT application.display_version AS v
  FROM {table}
  WHERE {_ts_filter(days)} AND application.display_version IS NOT NULL
  GROUP BY v ORDER BY COUNT(*) DESC LIMIT {limit_versions}
)
SELECT
  DATE(event_timestamp, 'Europe/Istanbul') AS crash_date,
  COALESCE(application.display_version, 'bilinmiyor') AS app_version,
  COUNT(*) AS event_count
FROM {table}
WHERE {_ts_filter(days)}
  AND application.display_version IN (SELECT v FROM top_versions)
GROUP BY crash_date, app_version
ORDER BY crash_date ASC, app_version ASC
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "date": str(r.get("crash_date") or ""),
            "app_version": r.get("app_version") or "—",
            "event_count": int(r.get("event_count") or 0),
        }
        for r in rows
    ], err


def query_device_breakdown(
    platform: str,
    table: str,
    days: int,
    limit: int = 20,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    sql = f"""
SELECT
  COALESCE(device.manufacturer, '') AS manufacturer,
  COALESCE(device.model, 'bilinmiyor') AS model,
  COUNT(*) AS event_count
FROM {table}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY manufacturer, model
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "manufacturer": r.get("manufacturer") or "",
            "model": r.get("model") or "—",
            "event_count": int(r.get("event_count") or 0),
        }
        for r in rows
    ], err


def query_os_breakdown(
    platform: str,
    table: str,
    days: int,
    limit: int = 20,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    sql = f"""
SELECT
  COALESCE(operating_system.display_version, 'bilinmiyor') AS os_version,
  COUNT(*) AS event_count
FROM {table}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY os_version
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {"os_version": r.get("os_version") or "—", "event_count": int(r.get("event_count") or 0)}
        for r in rows
    ], err


def query_process_state_breakdown(
    platform: str,
    batch_ref: str | None,
    days: int,
    limit: int = 15,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    if not batch_ref:
        return [], None
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    sql = f"""
SELECT
  COALESCE(process_state, 'UNKNOWN') AS state,
  COUNT(*) AS event_count
FROM {batch_ref}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY state
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_detail_query(platform, sql)
    return [
        {"state": r.get("state") or "UNKNOWN", "event_count": int(r.get("event_count") or 0)}
        for r in rows
    ], err


def query_issue_events(
    platform: str, batch_ref: str | None, issue_id: str, days: int, *, limit: int = 40
) -> tuple[list[dict], str | None]:
    if not batch_ref or not issue_id:
        return [], None
    safe_id = issue_id.replace("'", "''")
    sql = f"""
SELECT
  event_timestamp,
  installation_uuid,
  process_state,
  device.model AS device_model,
  device.manufacturer AS manufacturer,
  application.display_version AS app_version,
  operating_system.display_version AS os_version
FROM {batch_ref}
WHERE {_ts_filter(days)} AND issue_id = '{safe_id}'
ORDER BY event_timestamp DESC
LIMIT {limit}
"""
    rows, err = _run_detail_query(platform, sql)
    from backend.services.device_names import get_display_name

    out = []
    for r in rows:
        ts = r.get("event_timestamp")
        out.append({
            "event_timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts or ""),
            "installation_uuid": (r.get("installation_uuid") or "")[:12],
            "process_state": r.get("process_state") or "",
            "device_model": r.get("device_model") or "",
            "manufacturer": r.get("manufacturer") or "",
            "marketing_name": get_display_name(r.get("manufacturer") or "", r.get("device_model") or ""),
            "app_version": r.get("app_version") or "",
            "os_version": r.get("os_version") or "",
        })
    return out, err


def query_issue_event_raw(
    platform: str, batch_ref: str | None, issue_id: str, event_timestamp: str, days: int
) -> dict[str, Any]:
    """Tek olay: stack, breadcrumbs, keys."""
    from backend.services.crashlytics_detail import (
        parse_breadcrumbs,
        parse_custom_keys,
        parse_exceptions,
        parse_threads,
    )

    if not batch_ref or not issue_id or not event_timestamp:
        return {"ok": False, "error": "missing_params"}

    safe_id = issue_id.replace("'", "''")
    safe_ts = event_timestamp.replace("'", "''")
    sql = f"""
SELECT
  event_timestamp,
  process_state,
  breadcrumbs,
  custom_keys,
  threads,
  exceptions,
  blame_frame
FROM {batch_ref}
WHERE {_ts_filter(days)}
  AND issue_id = '{safe_id}'
  AND event_timestamp = TIMESTAMP('{safe_ts}')
LIMIT 1
"""
    rows, err = _run_detail_query(platform, sql)
    if err:
        return {"ok": False, "error": err}
    if not rows:
        return {"ok": False, "error": "event_not_found"}
    r = rows[0]
    bf = r.get("blame_frame") or {}
    if isinstance(bf, dict):
        blame = {
            "file": bf.get("file") or "",
            "symbol": bf.get("symbol") or "",
            "line": bf.get("line"),
        }
    else:
        blame = {}
    return {
        "ok": True,
        "process_state": r.get("process_state") or "",
        "blame_frame": blame,
        "breadcrumbs": parse_breadcrumbs(r.get("breadcrumbs")),
        "custom_keys": parse_custom_keys(r.get("custom_keys")),
        "threads": parse_threads(r.get("threads")),
        "exceptions": parse_exceptions(r.get("exceptions")),
    }


def query_anr_list(
    platform: str,
    table: str,
    days: int,
    version: str | None = None,
    *,
    versions: list[str] | None = None,
    limit: int = 30,
) -> tuple[list[dict], str | None]:
    sql = f"""
SELECT
  COALESCE(issue_id, '') AS issue_id,
  COALESCE(issue_title, '') AS issue_title,
  application.display_version AS app_version,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM {table}
WHERE {_ts_filter(days)}
  AND error_type = 'ANR'
  {_versions_filter_sql(versions, version)}
GROUP BY issue_id, issue_title, application.display_version
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "issue_id": r.get("issue_id", ""),
            "issue_title": r.get("issue_title", "") or "(başlıksız)",
            "app_version": r.get("app_version") or "—",
            "event_count": int(r.get("event_count") or 0),
            "affected_users": int(r.get("affected_users") or 0),
        }
        for r in rows
    ], err


def query_version_breakdown(
    platform: str,
    table: str,
    days: int,
    limit: int = 20,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    sql = f"""
SELECT
  COALESCE(application.display_version, 'bilinmiyor') AS app_version,
  COUNTIF(error_type = 'FATAL') AS fatal_count,
  COUNTIF(error_type = 'ANR') AS anr_count,
  COUNTIF(error_type = 'NON_FATAL') AS non_fatal_count,
  COUNT(*) AS total_events,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM {table}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY application.display_version
ORDER BY total_events DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "app_version": r.get("app_version", "—"),
            "fatal_count": int(r.get("fatal_count") or 0),
            "anr_count": int(r.get("anr_count") or 0),
            "non_fatal_count": int(r.get("non_fatal_count") or 0),
            "total_events": int(r.get("total_events") or 0),
            "affected_users": int(r.get("affected_users") or 0),
        }
        for r in rows
    ], err


def query_daily_trend(
    platform: str,
    table: str,
    days: int,
    *,
    error_type: str | None = None,
    versions: list[str] | None = None,
    version: str | None = None,
) -> tuple[list[dict], str | None]:
    # Gün gruplaması Türkiye saati (Firebase Console'un yerel saati) ile aynı olmalı —
    # aksi halde 21:00 UTC sonrası crash'ler bir gün önceye düşer, Firebase'le uyuşmaz.
    extra = _event_filters_sql(error_type=error_type, versions=versions, version=version)
    sql = f"""
SELECT
  DATE(event_timestamp, 'Europe/Istanbul') AS crash_date,
  COUNTIF(error_type = 'FATAL') AS fatal_count,
  COUNTIF(error_type = 'ANR') AS anr_count,
  COUNTIF(error_type = 'NON_FATAL') AS non_fatal_count
FROM {table}
WHERE {_ts_filter(days)}
  {extra}
GROUP BY crash_date
ORDER BY crash_date ASC
LIMIT {days + 5}
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    return [
        {
            "date": str(r.get("crash_date") or ""),
            "fatal": int(r.get("fatal_count") or 0),
            "anr": int(r.get("anr_count") or 0),
            "non_fatal": int(r.get("non_fatal_count") or 0),
        }
        for r in rows
    ], err


def query_available_versions(platform: str, table: str, days: int, *, limit: int = 200) -> list[str]:
    sql = f"""
SELECT DISTINCT application.display_version AS v
FROM {table}
WHERE {_ts_filter(days)} AND application.display_version IS NOT NULL
  AND application.display_version != ''
ORDER BY v DESC
LIMIT {limit}
"""
    rows, _ = _run_query(platform, sql, skip_budget=True)
    versions = [str(r.get("v", "")).strip() for r in rows if r.get("v")]
    return _semver_sort_versions(versions)


def _pick_higher_version(a: str | None, b: str | None) -> str:
    """İki sürüm dizesinden semver-benzeri olarak daha yüksek olanı."""
    aa = (a or "").strip()
    bb = (b or "").strip()
    if not aa or aa == "—":
        return bb
    if not bb or bb == "—":
        return aa
    ranked = _semver_sort_versions([aa, bb])
    return ranked[0] if ranked else aa


def _semver_sort_versions(versions: list[str]) -> list[str]:
    def key(v: str) -> tuple:
        parts = re.split(r"[.\-_]", v)
        nums: list[int] = []
        for p in parts:
            try:
                nums.append(int(p))
            except ValueError:
                nums.append(0)
        return tuple(nums)

    return sorted(set(versions), key=key, reverse=True)


# ── Issue derinlik sorgusu (drill-down) ───────────────────────────────────────

def _run_detail_query(platform: str, sql: str) -> tuple[list[dict], str | None]:
    """Issue detail için optimize edilmiş sorgu çalıştırıcı.

    Ana _BQ_SEMAPHORE yerine _DETAIL_SEMAPHORE kullanır (6 eşzamanlı izin);
    issue detail sorguları küçük/filtrelenmiş olduğundan bütçe kontrolü atlanır.
    """
    if not platform_ready(platform):
        return [], f"CRASHLYTICS_{platform.upper()}_SERVICE_ACCOUNT_JSON tanımlı değil."
    if _circuit_open(platform):
        return [], "Dataset erişim devre kesici aktif."

    acquired = _DETAIL_SEMAPHORE.acquire(timeout=8)
    if not acquired:
        return [], "Çok fazla eş zamanlı sorgu."
    try:
        client = _get_client(platform)
        loc = _get_dataset_location(platform)
        job = client.query(sql, location=loc) if loc else client.query(sql)
        rows = [dict(r) for r in job.result(timeout=QUERY_TIMEOUT_S)]
        return rows, None
    except Exception as exc:
        msg = str(exc).strip()
        # UNION ALL şema uyumsuzluğu — batch tablosuna fallback
        if "UNION ALL" in msg and "incompatible types" in msg:
            import re as _re3
            fixed_sql = _re3.sub(
                r"\(SELECT \* FROM (`[^`]+`) UNION ALL SELECT \* FROM `[^`]+`\)",
                r"\1",
                sql,
            )
            if fixed_sql != sql:
                _mark_union_incompat(platform)  # sonraki sorgular UNION ALL oluşturmaz
                logger.warning("Detail UNION ALL şema uyumsuzluğu (%s), batch'e fallback", platform)
                try:
                    job2 = client.query(fixed_sql, location=loc) if loc else client.query(fixed_sql)
                    rows2 = [dict(r) for r in job2.result(timeout=QUERY_TIMEOUT_S)]
                    return rows2, None
                except Exception as exc2:
                    return [], str(exc2).strip()[:200]
        logger.warning("Detail sorgu hatası (%s): %s", platform, msg[:200])
        return [], msg[:200]
    finally:
        _DETAIL_SEMAPHORE.release()


def query_issue_detail(
    platform: str,
    table: str,
    issue_id: str,
    days: int,
    *,
    versions: list[str] | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    """Tek bir issue için: trend + version/OS/device kırılımı + ilk/son görülme + stack frame.

    6 sorgu paralel çalışır; sonuç 15 dakika cache'lenir.
    """
    from backend.services.device_names import get_display_name

    if not issue_id:
        return {"ok": False, "error": "missing_issue_id"}

    ver_key = ",".join(sorted(versions or [])) or (version or "")
    cache_key = f"detail:{platform}:{issue_id}:{days}:{ver_key}"
    with _DETAIL_CACHE_LOCK:
        entry = _DETAIL_CACHE.get(cache_key)
        if entry and time.time() - entry[0] < _DETAIL_CACHE_TTL_S:
            return entry[1]

    # Per-issue build lock: aynı issue için eş zamanlı iki sorgu takımı başlamasın
    with _DETAIL_BUILD_LOCKS_LOCK:
        if cache_key not in _DETAIL_BUILD_LOCKS:
            _DETAIL_BUILD_LOCKS[cache_key] = threading.Lock()
        build_lock = _DETAIL_BUILD_LOCKS[cache_key]

    with build_lock:
        # Kilidi aldıktan sonra tekrar cache kontrolü
        with _DETAIL_CACHE_LOCK:
            entry = _DETAIL_CACHE.get(cache_key)
            if entry and time.time() - entry[0] < _DETAIL_CACHE_TTL_S:
                return entry[1]

        safe_id = issue_id.replace("'", "''")
        where = (
            f"{_ts_filter(days)} AND issue_id = '{safe_id}'"
            f" {_versions_filter_sql(versions, version)}"
        )

        sql_summary = f"""
SELECT
  COUNT(*) AS total_events,
  COUNT(DISTINCT installation_uuid) AS affected_users,
  MIN(event_timestamp) AS first_seen,
  MAX(event_timestamp) AS last_seen,
  ANY_VALUE(issue_title) AS issue_title,
  ANY_VALUE(error_type) AS error_type
FROM {table}
WHERE {where}
"""
        sql_trend = f"""
SELECT DATE(event_timestamp, 'Europe/Istanbul') AS d, COUNT(*) AS c
FROM {table}
WHERE {where}
GROUP BY d ORDER BY d ASC
"""
        sql_versions = f"""
SELECT
  COALESCE(application.display_version, 'bilinmiyor') AS app_version,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM {table}
WHERE {where}
GROUP BY app_version ORDER BY event_count DESC LIMIT 10
"""
        sql_os = f"""
SELECT
  COALESCE(operating_system.display_version, 'bilinmiyor') AS os_version,
  COUNT(*) AS event_count
FROM {table}
WHERE {where}
GROUP BY os_version ORDER BY event_count DESC LIMIT 10
"""
        sql_devices = f"""
SELECT
  COALESCE(device.model, 'bilinmiyor') AS model,
  COALESCE(device.manufacturer, '') AS manufacturer,
  COUNT(*) AS event_count
FROM {table}
WHERE {where}
GROUP BY model, manufacturer ORDER BY event_count DESC LIMIT 10
"""
        sql_blame = f"""
SELECT
  blame_frame.file AS file,
  blame_frame.symbol AS symbol,
  blame_frame.line AS line,
  COUNT(*) AS occurrences
FROM {table}
WHERE {where}
GROUP BY file, symbol, line ORDER BY occurrences DESC LIMIT 15
"""

        # 6 sorguyu paralel çalıştır
        queries = [
            ("summary",  sql_summary),
            ("trend",    sql_trend),
            ("versions", sql_versions),
            ("os",       sql_os),
            ("devices",  sql_devices),
            ("blame",    sql_blame),
        ]
        results: dict[str, tuple[list, str | None]] = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = {pool.submit(_run_detail_query, platform, sql): name for name, sql in queries}
            for fut in as_completed(futs):
                results[futs[fut]] = fut.result()

        summary_rows, sum_err = results.get("summary", ([], None))
        trend_rows,   _       = results.get("trend",    ([], None))
        version_rows, _       = results.get("versions", ([], None))
        os_rows,      _       = results.get("os",       ([], None))
        device_rows,  _       = results.get("devices",  ([], None))
        blame_rows,   _       = results.get("blame",    ([], None))

        summary = summary_rows[0] if summary_rows else {}
        first_seen = summary.get("first_seen")
        last_seen  = summary.get("last_seen")

        payload: dict[str, Any] = {
            "ok": True,
            "platform": platform,
            "issue_id": issue_id,
            "error": sum_err,
            "summary": {
                "issue_title": summary.get("issue_title") or "",
                "error_type":  (summary.get("error_type") or "").upper(),
                "total_events": int(summary.get("total_events") or 0),
                "affected_users": int(summary.get("affected_users") or 0),
                "first_seen": first_seen.isoformat() if hasattr(first_seen, "isoformat") else (str(first_seen) if first_seen else None),
                "last_seen":  last_seen.isoformat()  if hasattr(last_seen,  "isoformat") else (str(last_seen)  if last_seen  else None),
            },
            "trend": [
                {"date": str(r.get("d") or ""), "count": int(r.get("c") or 0)}
                for r in trend_rows
            ],
            "versions": [
                {
                    "app_version":   r.get("app_version") or "—",
                    "event_count":   int(r.get("event_count") or 0),
                    "affected_users": int(r.get("affected_users") or 0),
                }
                for r in version_rows
            ],
            "os_versions": [
                {"os_version": r.get("os_version") or "—", "event_count": int(r.get("event_count") or 0)}
                for r in os_rows
            ],
            "devices": [
                {
                    "model":        r.get("model") or "—",
                    "manufacturer": r.get("manufacturer") or "",
                    "marketing_name": get_display_name(r.get("manufacturer") or "", r.get("model") or ""),
                    "event_count":  int(r.get("event_count") or 0),
                }
                for r in device_rows
            ],
            "blame_frames": [
                {
                    "file":        r.get("file") or "",
                    "symbol":      r.get("symbol") or "",
                    "line":        int(r.get("line") or 0) if r.get("line") is not None else None,
                    "occurrences": int(r.get("occurrences") or 0),
                }
                for r in blame_rows
            ],
        }

        with _DETAIL_CACHE_LOCK:
            _DETAIL_CACHE[cache_key] = (time.time(), payload)
            # 200'den fazla entry varsa en eskilerini temizle
            if len(_DETAIL_CACHE) > 200:
                cutoff = time.time() - _DETAIL_CACHE_TTL_S
                stale = [k for k, (ts, _) in _DETAIL_CACHE.items() if ts < cutoff]
                for k in stale:
                    del _DETAIL_CACHE[k]

        return payload


def get_issue_detail_for_product(
    product_id: str,
    platform: str,
    issue_id: str,
    days: int,
    *,
    versions: list[str] | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    """Wrapper: product+platform'dan tablo adresini çözüp query_issue_detail'i çalıştırır."""
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product"}
    if platform not in ("ios", "android"):
        return {"ok": False, "error": "invalid_platform"}
    if not platform_ready(platform):
        return {"ok": False, "error": "credential_missing"}

    meta = APP_PRODUCTS[pid]
    bundle = (meta.get("android_package") if platform == "android" else meta.get("ios_bundle_id")) or ""
    if not bundle:
        return {"ok": False, "error": "bundle_missing"}
    table = _table(platform, bundle)
    batch_ref = _batch_table_ref(platform, bundle)
    detail = query_issue_detail(
        platform, table, issue_id, days, versions=versions, version=version
    )
    if not detail.get("ok"):
        return detail

    events, ev_err = query_issue_events(platform, batch_ref, issue_id, days)
    detail["events"] = events
    if ev_err:
        detail["events_error"] = ev_err

    if events:
        sample = query_issue_event_raw(platform, batch_ref, issue_id, events[0]["event_timestamp"], days)
        detail["sample_event"] = sample

    if batch_ref:
        safe_id = issue_id.replace("'", "''")
        ps_sql = f"""
SELECT COALESCE(process_state, 'UNKNOWN') AS state, COUNT(*) AS event_count
FROM {batch_ref}
WHERE {_ts_filter(days)} AND issue_id = '{safe_id}'
GROUP BY state ORDER BY event_count DESC LIMIT 5
"""
        ps_rows, _ = _run_detail_query(platform, ps_sql)
        total_ps = sum(int(r.get("event_count") or 0) for r in ps_rows)
        detail["process_states"] = [
            {
                "state": r.get("state") or "UNKNOWN",
                "event_count": int(r.get("event_count") or 0),
                "pct": round(int(r.get("event_count") or 0) / total_ps * 100, 1) if total_ps else 0,
            }
            for r in ps_rows
        ]

    te = int(detail.get("summary", {}).get("total_events") or 0)
    au = int(detail.get("summary", {}).get("affected_users") or 0)
    detail["summary"]["events_per_user"] = round(te / au, 2) if au > 0 else float(te)
    return detail


# ── Platform birleştirici ─────────────────────────────────────────────────────

def _platforms_for(pid: str, platform_filter: str) -> list[tuple[str, str]]:
    """(platform_key, table) çiftleri; platform_filter=all → her ikisi."""
    meta = APP_PRODUCTS.get(pid, {})
    android_pkg = (meta.get("android_package") or "").strip()
    ios_bundle = (meta.get("ios_bundle_id") or "").strip()
    out = []
    if platform_filter in ("all", "android") and android_pkg and platform_ready("android"):
        out.append(("android", _table("android", android_pkg)))
    if platform_filter in ("all", "ios") and ios_bundle and platform_ready("ios"):
        out.append(("ios", _table("ios", ios_bundle)))
    return out


def _merge_issues(results: list[tuple[str, list[dict]]], *, days: int = 7) -> list[dict]:
    from backend.services.crashlytics_detail import enrich_issue_row

    merged: dict[str, dict] = {}
    for plat, rows in results:
        for r in rows:
            key = r["issue_id"] or r["issue_title"]
            if key in merged:
                merged[key]["event_count"] += r["event_count"]
                merged[key]["affected_users"] += r["affected_users"]
                if r.get("latest_version") and r["latest_version"] != "—":
                    merged[key]["latest_version"] = _pick_higher_version(
                        merged[key].get("latest_version"), r["latest_version"]
                    )
                fs, ls = r.get("first_seen"), r.get("last_seen")
                if fs and (not merged[key].get("first_seen") or fs < merged[key]["first_seen"]):
                    merged[key]["first_seen"] = fs
                if ls and (not merged[key].get("last_seen") or ls > merged[key]["last_seen"]):
                    merged[key]["last_seen"] = ls
            else:
                merged[key] = {**r, "platform": plat}
    out = sorted(merged.values(), key=lambda x: -x["event_count"])
    return [enrich_issue_row(row, days=days) for row in out]


def _aggregate_crash_free(cfp: dict[str, dict]) -> dict[str, float | None]:
    """Platform birleşik crash-free — users ve sessions ayrı, yüksek hassasiyet."""
    total_sess = sum(int(v.get("total_sessions") or 0) for v in cfp.values())
    crashed_sess = sum(int(v.get("crashed_sessions") or 0) for v in cfp.values())
    total_inst = sum(int(v.get("total_users") or 0) for v in cfp.values())
    crashed_inst = sum(int(v.get("crashed_users") or 0) for v in cfp.values())
    cfs = round((1 - crashed_sess / total_sess) * 100, 4) if total_sess > 0 else None
    cfu = round((1 - crashed_inst / total_inst) * 100, 4) if total_inst > 0 else None
    primary = cfs if cfs is not None else cfu
    return {
        "crash_free_pct": primary,
        "crash_free_sessions_pct": cfs,
        "crash_free_users_pct": cfu,
    }


def slice_payload_for_platform(data: dict[str, Any], platform: str) -> dict[str, Any]:
    """platform=all cache'inden ios/android görünümünü bellek içi dilimle — BQ tekrar çalışmaz."""
    plat = (platform or "all").strip().lower()
    if plat == "all" or not data or not data.get("ok"):
        return data
    if plat not in ("ios", "android"):
        return data

    out = dict(data)
    out["platform_filter"] = plat

    summary = (data.get("summary_by_platform") or {}).get(plat) or {}
    out["totals"] = {
        "fatal": summary.get("fatal", 0),
        "anr": summary.get("anr", 0),
        "non_fatal": summary.get("non_fatal", 0),
        "affected_users": summary.get("affected_users", 0),
    }
    out["summary_by_platform"] = {plat: summary} if summary else {}

    cf = (data.get("crash_free_by_platform") or {}).get(plat)
    out["crash_free_by_platform"] = {plat: cf} if cf else {}
    if cf:
        out["crash_free_pct"] = cf.get("crash_free_pct")
        out["crash_free_sessions_pct"] = cf.get("crash_free_sessions_pct")
        out["crash_free_users_pct"] = cf.get("crash_free_users_pct")
    else:
        out["crash_free_pct"] = None
        out["crash_free_sessions_pct"] = None
        out["crash_free_users_pct"] = None
    out["crash_free_method"] = cf.get("method") if cf else None
    hints = [h for h in (data.get("crash_free_hints") or []) if h.upper().startswith(plat.upper())]
    out["crash_free_hints"] = hints

    out["trend"] = (data.get("trend_by_platform") or {}).get(plat) or []
    out["trend_by_platform"] = {plat: out["trend"]} if out["trend"] else {}

    out["issues"] = (data.get("issues_by_platform") or {}).get(plat) or []
    out["anr"] = (data.get("anr_by_platform") or {}).get(plat) or []

    ver_plat = (data.get("versions_by_platform") or {}).get(plat) or []
    out["versions"] = _merge_versions([(plat, ver_plat)]) if ver_plat else []
    out["versions_by_platform"] = {plat: ver_plat} if ver_plat else {}

    vt = [r for r in (data.get("version_trend") or []) if (r.get("platform") or plat) == plat]
    if not vt:
        vt = [r for r in (data.get("version_trend") or []) if not r.get("platform")]
    out["version_trend"] = vt

    out["device_breakdown"] = (data.get("device_breakdown_by_platform") or {}).get(plat) or []
    out["os_breakdown"] = (data.get("os_breakdown_by_platform") or {}).get(plat) or []
    out["process_state_breakdown"] = (data.get("process_state_breakdown_by_platform") or {}).get(plat) or []
    out["filter_versions_by_platform"] = {
        plat: (data.get("filter_versions_by_platform") or {}).get(plat) or []
    }
    return out


# ── Ana build fonksiyonu ──────────────────────────────────────────────────────

def build_full_payload(
    product_id: str,
    days: int = 7,
    platform_filter: str = "all",
    error_type: str | None = None,
    version: str | None = None,
    *,
    jid: str | None = None,
) -> dict[str, Any]:
    """Tek çağrıyla tüm sekmelerin verisini döndür (cache destekli)."""
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"ok": False, "error": "unknown_product"}
    if not any_platform_ready():
        return {"ok": False, "configured": False, "message": "Service account tanımlı değil."}

    cache_key = f"{pid}:{days}:{platform_filter}"

    # 1) Fresh cache → anlık dön
    cached = _cache_get(cache_key)
    if cached:
        return cached

    # 2) Stale cache → anlık dön + arka planda yenile (stale-while-revalidate)
    stale = _cache_get_stale(cache_key)
    if stale:
        _trigger_bg_refresh(pid, days, platform_filter, cache_key)
        return stale

    # 3) Hiç veri yok → BQ sorgusunu bekle (normal ilk yükleme akışı)
    # Per-key lock: aynı cache key için eş zamanlı iki BQ sorgu seti başlamasın.
    # İlk çağrı build'i tamamlayıp cache'e yazar; bekleyen ikinci çağrı kilidi alınca
    # cache'den okuyup döner — BQ sorguları tekrar çalışmaz.
    with _BUILD_LOCKS_LOCK:
        if cache_key not in _BUILD_LOCKS:
            _BUILD_LOCKS[cache_key] = threading.Lock()
        build_lock = _BUILD_LOCKS[cache_key]

    with build_lock:
        cached = _cache_get(cache_key)
        if cached:
            return cached

        platforms = _platforms_for(pid, platform_filter)
        if not platforms:
            return {"ok": False, "message": "Seçili platform için credential bulunamadı."}

        def _step(pct: int, msg: str) -> None:
            if jid:
                _job_update(jid, pct, msg)

        _step(5, "BigQuery bağlantısı kuruluyor…")

        summary_by_plat: dict[str, dict] = {}
        crash_free_by_plat: dict[str, Any] = {}
        issues_all: list[tuple[str, list[dict]]] = []
        anr_all: list[tuple[str, list[dict]]] = []
        ver_all: list[tuple[str, list[dict]]] = []
        chip_ver_all: list[tuple[str, list[dict]]] = []
        trend_all: list[tuple[str, list[dict]]] = []
        version_trend_all: list[tuple[str, list[dict]]] = []
        device_all: list[tuple[str, list[dict]]] = []
        os_all: list[tuple[str, list[dict]]] = []
        process_all: list[tuple[str, list[dict]]] = []
        filter_ver_all: list[tuple[str, list[str]]] = []
        oldest_days_all: list[int] = []
        errors: list[str] = []
        meta = APP_PRODUCTS.get(pid, {})
        android_pkg = (meta.get("android_package") or "").strip()
        ios_bundle = (meta.get("ios_bundle_id") or "").strip()

        def _fetch_platform(plat: str, tbl: str) -> dict:
            """Platform için BQ sorgularını paralel çalıştır."""
            out: dict[str, Any] = {"platform": plat}
            bundle = android_pkg if plat == "android" else ios_bundle
            batch_ref = _batch_table_ref(plat, bundle) if bundle else None
            chip_days = max(days, 30)
            sub_tasks = {
                "summary":      lambda: ("summary",      query_summary(plat, tbl, days),              None),
                "crash_free":   lambda: ("crash_free",   query_crash_free(plat, tbl, days, bundle=bundle), None),
                "issues":       lambda: ("issues",       *query_top_issues(plat, tbl, days, None, None)),
                "anr":          lambda: ("anr",          *query_anr_list(plat, tbl, days, None)),
                "versions":     lambda: ("versions",     *query_version_breakdown(plat, tbl, days)),
                "trend":        lambda: ("trend",        *query_daily_trend(plat, tbl, days)),
                "version_trend": lambda: ("version_trend", *query_version_time_series(plat, tbl, days)),
                "devices":      lambda: ("devices",      *query_device_breakdown(plat, tbl, days)),
                "os":           lambda: ("os",           *query_os_breakdown(plat, tbl, days)),
                "process_state": lambda: ("process_state", *query_process_state_breakdown(plat, batch_ref, days)),
                "oldest_date":  lambda: ("oldest_date",  query_oldest_date(plat, tbl, days),          None),
                "chip_versions": lambda: ("chip_versions", *query_version_breakdown(plat, tbl, chip_days)),
                "filter_versions": lambda: ("filter_versions", query_available_versions(plat, tbl, days), None),
            }
            with ThreadPoolExecutor(max_workers=8) as sub_pool:
                sub_futs = {sub_pool.submit(fn): name for name, fn in sub_tasks.items()}
                for fut in as_completed(sub_futs):
                    try:
                        key, data, err = fut.result()
                        if key in ("summary", "crash_free", "oldest_date", "filter_versions"):
                            out[key] = data
                        else:
                            out[key] = data
                            out[f"{key}_err"] = err
                    except Exception as exc:
                        name = sub_futs[fut]
                        out[name] = [] if name != "summary" else {}
                        if name not in ("summary", "crash_free", "oldest_date", "filter_versions"):
                            out[f"{name}_err"] = str(exc)[:200]
            return out

        _step(15, "Crash verileri sorgulanıyor…")
        with ThreadPoolExecutor(max_workers=2) as pool:
            futs = {pool.submit(_fetch_platform, p, t): p for p, t in platforms}
            completed = 0
            for fut in as_completed(futs):
                completed += 1
                pct = 15 + int(completed / len(futs) * 65)
                _step(pct, f"Platform verisi işleniyor… ({completed}/{len(futs)})")
                try:
                    res = fut.result()
                    plat = res["platform"]
                    summary_by_plat[plat] = res["summary"]
                    if res["crash_free"]:
                        crash_free_by_plat[plat] = res["crash_free"]
                    if res["issues"]:
                        issues_all.append((plat, res["issues"]))
                    if res["anr"]:
                        anr_all.append((plat, res["anr"]))
                    if res["versions"]:
                        ver_all.append((plat, res["versions"]))
                    chip_rows = res.get("chip_versions") or res.get("versions") or []
                    if chip_rows:
                        chip_ver_all.append((plat, chip_rows))
                    if res["trend"]:
                        trend_all.append((plat, res["trend"]))
                    if res.get("version_trend"):
                        version_trend_all.append((plat, res["version_trend"]))
                    if res.get("devices"):
                        device_all.append((plat, res["devices"]))
                    if res.get("os"):
                        os_all.append((plat, res["os"]))
                    if res.get("process_state"):
                        process_all.append((plat, res["process_state"]))
                    if res.get("filter_versions"):
                        filter_ver_all.append((plat, res["filter_versions"]))
                    if res.get("oldest_date") is not None:
                        oldest_days_all.append(res["oldest_date"])
                    seen_for_plat: set[str] = set()
                    for field in ("issues_err", "anr_err", "ver_err", "trend_err", "version_trend_err", "devices_err", "os_err", "process_state_err"):
                        msg = res.get(field)
                        if not msg or msg in seen_for_plat:
                            continue
                        seen_for_plat.add(msg)
                        errors.append(f"{plat}: {msg}")
                except Exception as exc:
                    errors.append(str(exc)[:200])

        if errors:
            _seen: set[str] = set()
            deduped: list[str] = []
            for e in errors:
                if e not in _seen:
                    _seen.add(e)
                    deduped.append(e)
            errors = deduped

        _step(85, "Sonuçlar birleştiriliyor…")

        totals = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0}
        for s in summary_by_plat.values():
            totals["fatal"] += s.get("fatal", 0)
            totals["anr"] += s.get("anr", 0)
            totals["non_fatal"] += s.get("non_fatal", 0)
            totals["affected_users"] += s.get("affected_users", 0)

        cf_agg = _aggregate_crash_free(crash_free_by_plat) if crash_free_by_plat else {}
        crash_free_pct = cf_agg.get("crash_free_pct")
        crash_free_sessions_pct = cf_agg.get("crash_free_sessions_pct")
        crash_free_users_pct = cf_agg.get("crash_free_users_pct")
        cf_methods = {plat: (v.get("method") or "") for plat, v in crash_free_by_plat.items()}
        crash_free_method = "firebase_sessions" if any(m == "firebase_sessions" for m in cf_methods.values()) else None
        crash_free_hints: list[str] = []
        for plat in summary_by_plat:
            if plat not in crash_free_by_plat or not crash_free_by_plat.get(plat):
                s = summary_by_plat[plat]
                ev = (s.get("fatal") or 0) + (s.get("anr") or 0) + (s.get("non_fatal") or 0)
                if ev > 0:
                    crash_free_hints.append(
                        f"{plat.upper()}: Crash-free hesaplanamadı — Firebase Console → BigQuery → "
                        "«Include sessions» export'unu açın (firebase_sessions dataset)."
                    )

        storage_mb = get_all_storage_mb()

        data_days = max(oldest_days_all) if oldest_days_all else days

        from backend.services.crashlytics_detail import merge_breakdown_rows, enrich_issue_row
        from backend.services.android_device_names import enrich_device_row

        device_breakdown_by_platform: dict[str, list[dict]] = {}
        device_rows_merged: list[dict] = []
        device_raw_by_label: dict[str, str | None] = {}
        for plat, rows in device_all:
            plat_labeled: list[dict] = []
            plat_raw: dict[str, str | None] = {}
            for r in rows:
                er = enrich_device_row(r, platform=plat)
                plat_labeled.append({
                    "label": er["label"],
                    "manufacturer": er.get("manufacturer") or r.get("manufacturer"),
                    "model": er.get("model") or r.get("model"),
                    "event_count": er["event_count"],
                })
                device_rows_merged.append({
                    "label": er["label"],
                    "manufacturer": er.get("manufacturer") or r.get("manufacturer"),
                    "model": er.get("model") or r.get("model"),
                    "event_count": er["event_count"],
                })
                if er.get("label_raw"):
                    plat_raw[er["label"]] = er["label_raw"]
                    device_raw_by_label[er["label"]] = er["label_raw"]
            plat_rows = merge_breakdown_rows([plat_labeled], "label", limit=20)
            for row in plat_rows:
                if plat_raw.get(row["label"]):
                    row["label_raw"] = plat_raw[row["label"]]
            device_breakdown_by_platform[plat] = plat_rows

        os_breakdown_by_platform: dict[str, list[dict]] = {}
        for plat, rows in os_all:
            os_breakdown_by_platform[plat] = merge_breakdown_rows([rows], "os_version", limit=20)

        process_breakdown_by_platform: dict[str, list[dict]] = {}
        for plat, rows in process_all:
            process_breakdown_by_platform[plat] = merge_breakdown_rows([rows], "state", limit=15)

        device_breakdown = merge_breakdown_rows([device_rows_merged], "label", limit=20)
        for row in device_breakdown:
            if device_raw_by_label.get(row["label"]):
                row["label_raw"] = device_raw_by_label[row["label"]]

        os_breakdown = merge_breakdown_rows([r for _p, r in os_all], "os_version", limit=20)
        process_breakdown = merge_breakdown_rows([r for _p, r in process_all], "state", limit=15)

        issues_by_platform: dict[str, list[dict]] = {}
        for plat, rows in issues_all:
            issues_by_platform[plat] = sorted(
                [enrich_issue_row({**r, "platform": plat}, days=days) for r in rows],
                key=lambda x: -x["event_count"],
            )
        anr_by_platform: dict[str, list[dict]] = {}
        for plat, rows in anr_all:
            anr_by_platform[plat] = sorted(
                [enrich_issue_row({**r, "platform": plat}, days=days) for r in rows],
                key=lambda x: -x["event_count"],
            )

        filter_versions_by_platform = {plat: vers for plat, vers in filter_ver_all}

        version_trend_merged: list[dict] = []
        for _plat, rows in version_trend_all:
            version_trend_merged.extend(rows)

        result = {
            "ok": True,
            "configured": True,
            "product": pid,
            "days": days,
            "data_days": data_days,
            "platform_filter": platform_filter,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "totals": totals,
            "crash_free_pct": crash_free_pct,
            "crash_free_sessions_pct": crash_free_sessions_pct,
            "crash_free_users_pct": crash_free_users_pct,
            "crash_free_method": crash_free_method,
            "crash_free_hints": crash_free_hints,
            "crash_free_by_platform": crash_free_by_plat,
            "summary_by_platform": summary_by_plat,
            "issues": _merge_issues(issues_all, days=days),
            "issues_by_platform": issues_by_platform,
            "anr": _merge_issues(anr_all, days=days),
            "anr_by_platform": anr_by_platform,
            "versions": _merge_versions(ver_all),
            "versions_by_platform": {plat: rows for plat, rows in chip_ver_all},
            "filter_versions_by_platform": filter_versions_by_platform,
            "trend": _merge_trend(trend_all),
            "trend_by_platform": {plat: rows for plat, rows in trend_all},
            "version_trend": version_trend_merged,
            "device_breakdown": device_breakdown,
            "device_breakdown_by_platform": device_breakdown_by_platform,
            "os_breakdown": os_breakdown,
            "os_breakdown_by_platform": os_breakdown_by_platform,
            "process_state_breakdown": process_breakdown,
            "process_state_breakdown_by_platform": process_breakdown_by_platform,
            "storage_mb": storage_mb,
            "errors": errors,
        }

        has_access_error = any(
            "reddedildi" in e.lower() or "forbidden" in e.lower() or "access denied" in e.lower()
            for e in errors
        )
        if not has_access_error:
            _cache_set(cache_key, result)
    _step(100, "Tamamlandı")
    return result


def _merge_versions(results: list[tuple[str, list[dict]]]) -> list[dict]:
    merged: dict[str, dict] = {}
    for _plat, rows in results:
        for r in rows:
            v = r["app_version"]
            if v in merged:
                for k in ("fatal_count", "anr_count", "non_fatal_count", "total_events", "affected_users"):
                    merged[v][k] = merged[v].get(k, 0) + r.get(k, 0)
            else:
                merged[v] = {**r}
    return sorted(merged.values(), key=lambda x: -x["total_events"])


def _merge_trend(results: list[tuple[str, list[dict]]]) -> list[dict]:
    merged: dict[str, dict] = {}
    for _plat, rows in results:
        for r in rows:
            d = r["date"]
            if d in merged:
                merged[d]["fatal"] += r["fatal"]
                merged[d]["anr"] += r["anr"]
                merged[d]["non_fatal"] = merged[d].get("non_fatal", 0) + r.get("non_fatal", 0)
            else:
                merged[d] = {"date": d, "fatal": r["fatal"], "anr": r["anr"], "non_fatal": r.get("non_fatal", 0)}
    return sorted(merged.values(), key=lambda x: x["date"])


def assemble_breakdown_payload(
    *,
    trend_all: list[tuple[str, list[dict]]],
    device_all: list[tuple[str, list[dict]]],
    os_all: list[tuple[str, list[dict]]],
    process_all: list[tuple[str, list[dict]]],
    ver_all: list[tuple[str, list[dict]]],
    version_trend_all: list[tuple[str, list[dict]]],
) -> dict[str, Any]:
    """Platform kırılım listelerinden UI payload alanlarını üret."""
    from backend.services.android_device_names import enrich_device_row
    from backend.services.crashlytics_detail import merge_breakdown_rows

    device_breakdown_by_platform: dict[str, list[dict]] = {}
    device_rows_merged: list[dict] = []
    device_raw_by_label: dict[str, str | None] = {}
    for plat, rows in device_all:
        plat_labeled: list[dict] = []
        plat_raw: dict[str, str | None] = {}
        for r in rows:
            er = enrich_device_row(r, platform=plat)
            plat_labeled.append({
                "label": er["label"],
                "manufacturer": er.get("manufacturer") or r.get("manufacturer"),
                "model": er.get("model") or r.get("model"),
                "event_count": er["event_count"],
            })
            device_rows_merged.append({
                "label": er["label"],
                "manufacturer": er.get("manufacturer") or r.get("manufacturer"),
                "model": er.get("model") or r.get("model"),
                "event_count": er["event_count"],
            })
            if er.get("label_raw"):
                plat_raw[er["label"]] = er["label_raw"]
                device_raw_by_label[er["label"]] = er["label_raw"]
        plat_rows = merge_breakdown_rows([plat_labeled], "label", limit=20)
        for row in plat_rows:
            if plat_raw.get(row["label"]):
                row["label_raw"] = plat_raw[row["label"]]
        device_breakdown_by_platform[plat] = plat_rows

    os_breakdown_by_platform: dict[str, list[dict]] = {}
    for plat, rows in os_all:
        os_breakdown_by_platform[plat] = merge_breakdown_rows([rows], "os_version", limit=20)

    process_breakdown_by_platform: dict[str, list[dict]] = {}
    for plat, rows in process_all:
        process_breakdown_by_platform[plat] = merge_breakdown_rows([rows], "state", limit=15)

    device_breakdown = merge_breakdown_rows([device_rows_merged], "label", limit=20)
    for row in device_breakdown:
        if device_raw_by_label.get(row["label"]):
            row["label_raw"] = device_raw_by_label[row["label"]]

    version_trend_merged: list[dict] = []
    for _plat, rows in version_trend_all:
        version_trend_merged.extend(rows)

    return {
        "trend_by_platform": {plat: rows for plat, rows in trend_all},
        "trend": _merge_trend(trend_all),
        "device_breakdown_by_platform": device_breakdown_by_platform,
        "device_breakdown": device_breakdown,
        "os_breakdown_by_platform": os_breakdown_by_platform,
        "os_breakdown": merge_breakdown_rows([r for _p, r in os_all], "os_version", limit=20),
        "process_state_breakdown_by_platform": process_breakdown_by_platform,
        "process_state_breakdown": merge_breakdown_rows([r for _p, r in process_all], "state", limit=15),
        "versions_by_platform": {plat: rows for plat, rows in ver_all},
        "versions": _merge_versions(ver_all),
        "version_trend": version_trend_merged,
    }


def get_available_versions_all(product_id: str, days: int, platform_filter: str) -> list[str]:
    pid = (product_id or "doviz").strip().lower()
    platforms = _platforms_for(pid, platform_filter)
    all_versions: set[str] = set()
    for plat, tbl in platforms:
        versions = query_available_versions(plat, tbl, days)
        all_versions.update(versions)
    return sorted(all_versions, reverse=True)[:30]


# ── Background job (scheduler + manual refresh) ───────────────────────────────

_REFRESH_LOCK = threading.Lock()
_REFRESH_RUNNING = False


def is_cache_warm(product_id: str = "doviz", days: int = 7, platform_filter: str = "all") -> bool:
    """Fresh veya stale cache varsa True — JS progress bar'ı atla."""
    key = f"{product_id}:{days}:{platform_filter}"
    return _cache_get(key) is not None or _cache_get_stale(key) is not None


def prewarm_cache(product_id: str = "doviz") -> None:
    """Startup veya scheduled re-warm için arka planda cache'i ısıt.
    Manuel refresh'ten farklı olarak cache/circuit breaker'ı sıfırlamaz —
    sadece cache soğuksa sorgu başlatır."""
    global _REFRESH_RUNNING
    if is_cache_warm(product_id):
        return
    with _REFRESH_LOCK:
        if _REFRESH_RUNNING:
            return
        _REFRESH_RUNNING = True

    jid = _job_new(product_id)

    def _worker():
        global _REFRESH_RUNNING
        try:
            build_full_payload(product_id, days=7, platform_filter="all", jid=jid)
            _job_done(jid)
        except Exception as exc:
            logger.warning("Crashlytics prewarm başarısız: %s", exc)
            _job_done(jid, error=str(exc)[:200])
        finally:
            with _REFRESH_LOCK:
                _REFRESH_RUNNING = False

    threading.Thread(target=_worker, daemon=True, name="crashlytics-prewarm").start()


def run_daily_refresh(product_id: str = "doviz") -> str:
    """Arkaplanda tam veri çekimi başlat. Job ID döndürür."""
    global _REFRESH_RUNNING
    with _REFRESH_LOCK:
        if _REFRESH_RUNNING:
            return "already_running"
        _REFRESH_RUNNING = True

    # Manuel refresh: tüm cache'leri ve circuit breaker'ı sıfırla.
    # Kullanıcı "Yenile" butonuna bastığında tablolar artık mevcut olabilir;
    # circuit breaker sıfırlanmazsa tablolar oluştuktan sonra bile 1 saat beklenir.
    # _REFRESH_RUNNING kilidi zaten art arda refresh'i engeller.
    with _DATASET_LOCATION_LOCK:
        _DATASET_LOCATION_CACHE.clear()
    with _TABLE_DISCOVERY_LOCK:
        _TABLE_DISCOVERY_CACHE.clear()
    for plat in ("ios", "android"):
        _circuit_reset(plat)

    jid = _job_new(product_id)

    def _worker():
        global _REFRESH_RUNNING
        try:
            # Bu ürüne ait tüm cache girdilerini temizle
            with _CACHE_LOCK:
                stale = [k for k in _CACHE if k.startswith(f"{product_id}:")]
                for k in stale:
                    del _CACHE[k]
            build_full_payload(product_id, days=7, platform_filter="all", jid=jid)
            _job_done(jid)
        except Exception as exc:
            logger.exception("Crashlytics daily refresh başarısız")
            _job_done(jid, error=str(exc)[:200])
        finally:
            with _REFRESH_LOCK:
                _REFRESH_RUNNING = False

    threading.Thread(target=_worker, daemon=True, name="crashlytics-refresh").start()
    return jid
