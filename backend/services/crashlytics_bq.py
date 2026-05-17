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
BIGQUERY_SCOPES = ("https://www.googleapis.com/auth/bigquery.readonly",)
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


def platform_ready(platform: str) -> bool:
    return bool(_load_creds(platform))


def any_platform_ready() -> bool:
    return platform_ready("ios") or platform_ready("android")


def _get_client(platform: str):
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = _load_creds(platform)
    if not info:
        raise ValueError(f"CRASHLYTICS_{platform.upper()}_SERVICE_ACCOUNT_JSON tanımlı değil.")
    creds = service_account.Credentials.from_service_account_info(info, scopes=BIGQUERY_SCOPES)
    return bigquery.Client(credentials=creds, project=_PLATFORM_PROJECTS[platform])


def _table(platform: str, bundle: str) -> str:
    tid = bundle.replace(".", "_")
    return f"{_PLATFORM_PROJECTS[platform]}.{_DATASET}.{tid}"


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

def _dry_run_bytes(client, sql: str) -> int:
    """Sorgunun kaç byte işleyeceğini tahmin et (ağ trafiği yok, ücretsiz)."""
    try:
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=cfg)
        return int(job.total_bytes_processed or 0)
    except Exception:
        return 0


# ── Storage kullanımı (metadata, ücretsiz) ────────────────────────────────────

def get_storage_bytes(platform: str) -> int:
    """Dataset'in toplam byte boyutunu döndür (metadata sorgusu, byte taramaz)."""
    try:
        client = _get_client(platform)
        proj = _PLATFORM_PROJECTS[platform]
        sql = f"SELECT SUM(size_bytes) AS total FROM `{proj}.{_DATASET}.__TABLES__`"
        for row in client.query(sql).result(timeout=10):
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

    acquired = _BQ_SEMAPHORE.acquire(timeout=5)
    if not acquired:
        return [], "Çok fazla eş zamanlı sorgu. Lütfen bekleyin."

    try:
        client = _get_client(platform)

        # Budget kontrolü
        if not skip_budget:
            est = _dry_run_bytes(client, sql)
            if est > BYTES_BUDGET:
                mb = round(est / 1_000_000, 1)
                return [], f"Sorgu tahmini çok büyük ({mb} MB > {BYTES_BUDGET//1_000_000} MB). Dönem filtresi daraltın."

        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(use_query_cache=True)
        job = client.query(sql, job_config=cfg)
        rows = []
        for r in job.result(timeout=QUERY_TIMEOUT_S):
            rows.append(dict(r))
        return rows, None

    except gexc.NotFound as exc:
        logger.warning("BQ tablo bulunamadı (%s): %s", platform, exc)
        return [], "Tablo henüz oluşmamış. Firebase export başladıktan 24 saat bekleyin."
    except gexc.Forbidden as exc:
        logger.warning("BQ erişim reddedildi (%s): %s", platform, exc)
        return [], "Erişim reddedildi. Service account'a BigQuery Data Viewer + Job User rolü verin."
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
  COUNTIF(error_type = 'ANR') AS anr_count
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
                for field in ("issues_err", "anr_err", "ver_err", "trend_err"):
                    if res.get(field):
                        errors.append(f"{plat}: {res[field]}")
            except Exception as exc:
                errors.append(str(exc)[:200])

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
        "storage_mb": storage_mb,
        "errors": errors,
    }

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
            else:
                merged[d] = {**r}
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

    jid = _job_new(product_id)

    def _worker():
        global _REFRESH_RUNNING
        try:
            for days in (1, 7, 30, 90):
                for pf in ("android", "ios"):
                    if platform_ready(pf):
                        # Cache'i temizle ki taze veri çekilsin
                        cache_key = f"{product_id}:{days}:{pf}::"
                        with _CACHE_LOCK:
                            _CACHE.pop(cache_key, None)
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
