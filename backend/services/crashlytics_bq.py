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
MAX_CONCURRENT = 2              # Eş zamanlı max sorgu sayısı
CACHE_TTL_S = 4 * 3600         # 4 saat cache

_PLATFORM_PROJECTS = {"ios": "doviz-ios", "android": "doviz-android"}
_DATASET = "firebase_crashlytics"

# ── Thread araçları ───────────────────────────────────────────────────────────
_BQ_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT)
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[float, Any]] = {}           # key → (ts, data)
_JOB_LOCK = threading.Lock()
_JOBS: dict[str, dict[str, Any]] = {}               # job_id → progress state

# Circuit breaker: bir platform için "dataset boş" tespit edilirse 1 saat boyunca
# yeni sorgu deneme — BQ free-tier'ı tüketmesin ve audit log'u kirlemesin.
_EMPTY_DATASET_TTL_S = 60 * 60   # 1 saat
_EMPTY_DATASET_LOCK = threading.Lock()
_EMPTY_DATASET_UNTIL: dict[str, float] = {}        # platform → expiry timestamp


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
    """Dataset'in BigQuery lokasyonunu öğren (cache'li). Bulunamazsa None."""
    with _DATASET_LOCATION_LOCK:
        if platform in _DATASET_LOCATION_CACHE:
            return _DATASET_LOCATION_CACHE[platform] or None
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
        logger.warning("Dataset lokasyonu alınamadı (%s): %s", platform, exc)
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
        loc = _get_dataset_location(platform)
        sql = f"SELECT table_id FROM `{proj}.{_DATASET}.__TABLES__`"
        query_job = client.query(sql, location=loc) if loc else client.query(sql)
        return [str(r.table_id) for r in query_job.result(timeout=10)]
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
    # 1) Projedeki tüm dataset'leri listele
    try:
        client = _get_client(platform)
        try:
            datasets = [ds.dataset_id for ds in client.list_datasets(max_results=50)]
            out["all_datasets_in_project"] = datasets
        except Exception as exc:
            out["list_datasets_error"] = str(exc)[:300]
        # 2) firebase_crashlytics dataset'i var mı?
        from google.cloud import bigquery as _bq
        try:
            client.get_dataset(_bq.DatasetReference(proj, _DATASET))
            out["dataset_exists"] = True
        except Exception as exc:
            out["dataset_exists"] = False
            out["dataset_check_error"] = str(exc)[:300]
        # 3) Varsa içindeki tabloları getir
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
    # Olası adlandırmalar (öncelik sırasıyla)
    candidates = [
        f"{base}_{plat_up}",                    # com_Doviz_ANDROID  (standart)
        f"{base}_REALTIME_{plat_up}",           # com_Doviz_REALTIME_ANDROID
        base,                                    # com_Doviz (eski)
        base.lower() + "_" + plat_up,
        base.lower(),
        base.lower() + "_realtime_" + plat_up.lower(),
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
    """Bundle için BigQuery tam tablo path'i. Discovery → standart pattern fallback."""
    proj = _effective_project(platform)
    tid = _discover_table_id(platform, bundle)
    if tid:
        return f"{proj}.{_DATASET}.{tid}"
    base = bundle.replace(".", "_")
    return f"{proj}.{_DATASET}.{base}_{platform.upper()}"


# ── Cache yardımcıları ────────────────────────────────────────────────────────

def _cache_get(key: str) -> Any | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and time.time() - entry[0] < CACHE_TTL_S:
            return entry[1]
    return None


def _cache_set(key: str, value: Any) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), value)


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
        shown = ", ".join(available[:8]) + ("…" if len(available) > 8 else "")
        return [], (
            f"Bundle ile eşleşen tablo bulunamadı (`{proj}.{_DATASET}`). "
            f"Datasette mevcut tablolar: {shown}. "
            "APP_PRODUCTS'taki bundle/package adı ile BigQuery tablo adı eşleşmiyor "
            "olabilir — beklenen şablon: `<bundle>_ANDROID` / `<bundle>_IOS`."
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
        return f"AND app_info.app_version = '{safe}'"
    return ""


# ── Sorgu fonksiyonları ───────────────────────────────────────────────────────

def query_summary(platform: str, table: str, days: int) -> dict[str, Any]:
    sql = f"""
SELECT
  error_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM `{table}`
WHERE {_ts_filter(days)}
GROUP BY error_type
"""
    rows, err = _run_query(platform, sql, skip_budget=True)
    result: dict[str, Any] = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0, "error": err}
    seen_users: set = set()
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
        seen_users.add(u)
    result["affected_users"] = sum(seen_users)
    return result


def query_crash_free(platform: str, table: str, days: int) -> dict[str, Any] | None:
    sql = f"""
SELECT
  COUNT(DISTINCT installation_uuid) AS total_users,
  COUNT(DISTINCT IF(error_type = 'FATAL', installation_uuid, NULL)) AS crashed_users
FROM `{table}`
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
    return {"total_users": total, "crashed_users": crashed, "crash_free_pct": pct}


def query_top_issues(platform: str, table: str, days: int,
                     error_type: str | None = None, version: str | None = None,
                     limit: int = 30) -> tuple[list[dict], str | None]:
    sql = f"""
SELECT
  COALESCE(issue.issue_id, '') AS issue_id,
  COALESCE(issue.title, '') AS issue_title,
  COALESCE(error_type, '') AS error_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users,
  MAX(app_info.app_version) AS latest_version
FROM `{table}`
WHERE {_ts_filter(days)}
  {_type_filter(error_type)}
  {_version_filter(version)}
GROUP BY issue.issue_id, issue.title, error_type
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql)
    return [
        {
            "issue_id": r.get("issue_id", ""),
            "issue_title": r.get("issue_title", "") or "(başlıksız)",
            "error_type": r.get("error_type", ""),
            "event_count": int(r.get("event_count") or 0),
            "affected_users": int(r.get("affected_users") or 0),
            "latest_version": r.get("latest_version") or "—",
        }
        for r in rows
    ], err


def query_anr_list(platform: str, table: str, days: int,
                   version: str | None = None, limit: int = 30) -> tuple[list[dict], str | None]:
    sql = f"""
SELECT
  COALESCE(issue.issue_id, '') AS issue_id,
  COALESCE(issue.title, '') AS issue_title,
  app_info.app_version AS app_version,
  COUNT(*) AS event_count,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM `{table}`
WHERE {_ts_filter(days)}
  AND error_type = 'ANR'
  {_version_filter(version)}
GROUP BY issue.issue_id, issue.title, app_info.app_version
ORDER BY event_count DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql)
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


def query_version_breakdown(platform: str, table: str, days: int,
                            limit: int = 20) -> tuple[list[dict], str | None]:
    sql = f"""
SELECT
  COALESCE(app_info.app_version, 'bilinmiyor') AS app_version,
  COUNTIF(error_type = 'FATAL') AS fatal_count,
  COUNTIF(error_type = 'ANR') AS anr_count,
  COUNTIF(error_type = 'NON_FATAL') AS non_fatal_count,
  COUNT(*) AS total_events,
  COUNT(DISTINCT installation_uuid) AS affected_users
FROM `{table}`
WHERE {_ts_filter(days)}
GROUP BY app_info.app_version
ORDER BY total_events DESC
LIMIT {limit}
"""
    rows, err = _run_query(platform, sql)
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


def query_daily_trend(platform: str, table: str, days: int) -> tuple[list[dict], str | None]:
    sql = f"""
SELECT
  DATE(event_timestamp) AS crash_date,
  COUNTIF(error_type = 'FATAL') AS fatal_count,
  COUNTIF(error_type = 'ANR') AS anr_count,
  COUNTIF(error_type = 'NON_FATAL') AS non_fatal_count
FROM `{table}`
WHERE {_ts_filter(days)}
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


def query_available_versions(platform: str, table: str, days: int) -> list[str]:
    sql = f"""
SELECT DISTINCT app_info.app_version AS v
FROM `{table}`
WHERE {_ts_filter(days)} AND app_info.app_version IS NOT NULL
ORDER BY v DESC
LIMIT 30
"""
    rows, _ = _run_query(platform, sql, skip_budget=True)
    return [str(r.get("v", "")) for r in rows if r.get("v")]


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


def _merge_issues(results: list[tuple[str, list[dict]]]) -> list[dict]:
    merged: dict[str, dict] = {}
    for plat, rows in results:
        for r in rows:
            key = r["issue_id"] or r["issue_title"]
            if key in merged:
                merged[key]["event_count"] += r["event_count"]
                merged[key]["affected_users"] += r["affected_users"]
                if r.get("latest_version") and r["latest_version"] != "—":
                    merged[key]["latest_version"] = r["latest_version"]
            else:
                merged[key] = {**r, "platform": plat}
    return sorted(merged.values(), key=lambda x: -x["event_count"])


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

    cache_key = f"{pid}:{days}:{platform_filter}:{error_type or ''}:{version or ''}"
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

    # Paralel sorgular platform başına
    summary_by_plat: dict[str, dict] = {}
    crash_free_by_plat: dict[str, Any] = {}
    issues_all: list[tuple[str, list[dict]]] = []
    anr_all: list[tuple[str, list[dict]]] = []
    ver_all: list[tuple[str, list[dict]]] = []
    trend_all: list[tuple[str, list[dict]]] = []
    errors: list[str] = []

    def _fetch_platform(plat: str, tbl: str) -> dict:
        out: dict[str, Any] = {"platform": plat}
        out["summary"] = query_summary(plat, tbl, days)
        out["crash_free"] = query_crash_free(plat, tbl, days)
        out["issues"], out["issues_err"] = query_top_issues(plat, tbl, days, error_type, version)
        out["anr"], out["anr_err"] = query_anr_list(plat, tbl, days, version)
        out["versions"], out["ver_err"] = query_version_breakdown(plat, tbl, days)
        out["trend"], out["trend_err"] = query_daily_trend(plat, tbl, days)
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
                if res["trend"]:
                    trend_all.append((plat, res["trend"]))
                # Her platform için aynı erişim/yapılandırma hatası birden fazla
                # alanda tekrar ediyor; tek temsilci mesajı koruyalım.
                seen_for_plat: set[str] = set()
                for field in ("issues_err", "anr_err", "ver_err", "trend_err"):
                    msg = res.get(field)
                    if not msg or msg in seen_for_plat:
                        continue
                    seen_for_plat.add(msg)
                    errors.append(f"{plat}: {msg}")
            except Exception as exc:
                errors.append(str(exc)[:200])

    # Aynı mesaj birden fazla platformda tekrar etmesin
    if errors:
        _seen: set[str] = set()
        deduped: list[str] = []
        for e in errors:
            if e not in _seen:
                _seen.add(e)
                deduped.append(e)
        errors = deduped

    _step(85, "Sonuçlar birleştiriliyor…")

    # Toplam hesapla
    totals = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0}
    for s in summary_by_plat.values():
        totals["fatal"] += s.get("fatal", 0)
        totals["anr"] += s.get("anr", 0)
        totals["non_fatal"] += s.get("non_fatal", 0)
        totals["affected_users"] += s.get("affected_users", 0)

    # Crash-free birleştir (ağırlıklı ortalama)
    cf_total_users = sum(v.get("total_users", 0) for v in crash_free_by_plat.values())
    cf_crashed = sum(v.get("crashed_users", 0) for v in crash_free_by_plat.values())
    crash_free_pct = round((1 - cf_crashed / cf_total_users) * 100, 2) if cf_total_users > 0 else None

    # Storage
    storage_mb = get_all_storage_mb()

    result = {
        "ok": True,
        "configured": True,
        "product": pid,
        "days": days,
        "platform_filter": platform_filter,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "totals": totals,
        "crash_free_pct": crash_free_pct,
        "crash_free_by_platform": crash_free_by_plat,
        "summary_by_platform": summary_by_plat,
        "issues": _merge_issues(issues_all),
        "anr": _merge_issues(anr_all),
        "versions": _merge_versions(ver_all),
        "trend": _merge_trend(trend_all),
        "trend_by_platform": {plat: rows for plat, rows in trend_all},
        "storage_mb": storage_mb,
        "errors": errors,
    }

    # Erişim hatası içeren sonuçları cache'leme; izinler düzeltilince hemen yansısın.
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


def run_daily_refresh(product_id: str = "doviz") -> str:
    """Arkaplanda tam veri çekimi başlat. Job ID döndürür."""
    global _REFRESH_RUNNING
    with _REFRESH_LOCK:
        if _REFRESH_RUNNING:
            return "already_running"
        _REFRESH_RUNNING = True

    # Manuel refresh: circuit breaker'ı + location cache'i sıfırla (kullanıcı bilinçli
    # olarak yeniden deniyor — belki Firebase tarafında ayar düzeltildi).
    for plat in ("ios", "android"):
        _circuit_reset(plat)
    with _DATASET_LOCATION_LOCK:
        _DATASET_LOCATION_CACHE.clear()

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
