"""
App Store Connect API istemcisi.

Apple'ın resmi REST API'sine (https://api.appstoreconnect.apple.com) JWT (ES256)
ile bağlanır ve Sales & Trends + Analytics Reports verilerini çeker.

Gerekli ortam değişkenleri (Railway Variables):
    ASC_KEY_ID          — App Store Connect API Key ID (10 karakter)
    ASC_ISSUER_ID       — Issuer ID (UUID)
    ASC_PRIVATE_KEY     — .p8 dosyasının TAM içeriği (-----BEGIN PRIVATE KEY----- ile başlar)
    ASC_VENDOR_NUMBER   — Sales & Trends için vendor ID (App Store Connect → Payments and Financial Reports)

Bunlar yoksa `is_configured()` False döner ve servis demo'ya düşer.
"""
from __future__ import annotations

import csv
import gzip
import io
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ASC_BASE = "https://api.appstoreconnect.apple.com"
_TOKEN_TTL = 60 * 18  # JWT en fazla 20 dk; biz 18 dk tutuyoruz
_token_cache: dict[str, Any] = {"token": None, "exp": 0}

# ─── Döviz çevirimi (USD) ────────────────────────────────────────────────────
# Apple Sales Report'ta gelirler yerel para biriminde gelir; USD'ye çevirmek
# için yaklaşık kurlar kullanılır. Kurlar 24 saatte bir güncellenir.
# Format: 1 USD = X yerel birim (exchangerate-api.com ile aynı format)
_FALLBACK_FX: dict[str, float] = {
    "USD": 1.0,    "EUR": 0.92,  "GBP": 0.79,  "TRY": 36.0,   "CAD": 1.37,
    "AUD": 1.56,   "JPY": 154.0, "CHF": 0.89,  "SEK": 10.5,   "NOK": 10.8,
    "DKK": 6.88,   "PLN": 4.0,   "CZK": 22.7,  "HUF": 370.0,  "RON": 4.55,
    "BGN": 1.80,   "RUB": 91.0,  "BRL": 5.55,  "MXN": 19.2,   "ARS": 900.0,
    "CLP": 910.0,  "COP": 4150.0,"SAR": 3.75,  "AED": 3.67,   "KWD": 0.306,
    "QAR": 3.64,   "ILS": 3.70,  "ZAR": 18.5,  "KRW": 1370.0, "SGD": 1.33,
    "HKD": 7.82,   "TWD": 32.2,  "INR": 83.5,  "THB": 35.7,   "IDR": 16400.0,
    "MYR": 4.55,   "PHP": 58.0,  "NZD": 1.67,  "CNY": 7.25,
}
_fx_cache: dict[str, Any] = {"rates": {}, "updated": 0.0}


def _to_usd(amount: float, currency: str) -> float:
    """Verilen tutarı yaklaşık USD'ye çevirir (24 saatlik cache).

    rates["TRY"] = 36 → 1 USD = 36 TRY → amount_TRY / 36 = USD
    """
    now = time.time()
    if now - _fx_cache["updated"] > 86400:
        try:
            with httpx.Client(timeout=5) as cli:
                r = cli.get("https://api.exchangerate-api.com/v4/latest/USD")
            if r.status_code == 200:
                _fx_cache["rates"] = r.json().get("rates", {})
                _fx_cache["updated"] = now
        except Exception:
            pass
    cur = (currency or "USD").strip().upper()
    rate = (_fx_cache["rates"] or _FALLBACK_FX).get(cur) or _FALLBACK_FX.get(cur, 1.0)
    return amount / rate


# ─── Yapılandırma ────────────────────────────────────────────────────────────

def _env(name: str) -> str | None:
    v = os.getenv(name)
    if v is None:
        return None
    v = v.strip()
    return v or None


def is_configured() -> bool:
    return all(_env(k) for k in ("ASC_KEY_ID", "ASC_ISSUER_ID", "ASC_PRIVATE_KEY"))


def _get_private_key_pem() -> str:
    raw = _env("ASC_PRIVATE_KEY") or ""
    if not raw:
        return raw
    # Railway env vars literal "\n" olarak gelebiliyor — normalize et
    raw = raw.replace("\\n", "\n").strip()
    begin = "-----BEGIN PRIVATE KEY-----"
    end = "-----END PRIVATE KEY-----"
    # Header/footer varsa içeriği oradan al, yoksa tüm string'i body kabul et
    if begin in raw and end in raw:
        body = raw.split(begin, 1)[1].split(end, 1)[0]
    else:
        body = raw
    # Tüm boşluk/satır sonlarını temizle; sadece base64 karakterleri kalsın
    body = "".join(body.split())
    if not body:
        return ""
    wrapped = "\n".join(body[i:i + 64] for i in range(0, len(body), 64))
    return f"{begin}\n{wrapped}\n{end}\n"


# ─── JWT üretimi (ES256) ─────────────────────────────────────────────────────

def _generate_token() -> str | None:
    """JWT üret; başarısız olursa None."""
    now = int(time.time())
    if _token_cache["token"] and _token_cache["exp"] - 60 > now:
        return _token_cache["token"]

    try:
        import jwt  # PyJWT
    except ImportError:
        logger.error("PyJWT yüklü değil; App Store Connect entegrasyonu çalışmaz.")
        return None

    key_id = _env("ASC_KEY_ID")
    issuer = _env("ASC_ISSUER_ID")
    pem = _get_private_key_pem()
    if not (key_id and issuer and pem):
        return None

    payload = {
        "iss": issuer,
        "iat": now,
        "exp": now + _TOKEN_TTL,
        "aud": "appstoreconnect-v1",
    }
    headers = {"alg": "ES256", "kid": key_id, "typ": "JWT"}
    try:
        token = jwt.encode(payload, pem, algorithm="ES256", headers=headers)
    except Exception as exc:
        logger.error("ASC JWT üretilemedi: %s", exc)
        return None
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    _token_cache["token"] = token
    _token_cache["exp"] = now + _TOKEN_TTL
    return token


def _auth_headers() -> dict[str, str] | None:
    tok = _generate_token()
    if not tok:
        return None
    return {"Authorization": f"Bearer {tok}", "Accept": "application/a-gzip, application/json"}


# ─── Sales & Trends API ──────────────────────────────────────────────────────
# /v1/salesReports?filter[...]
# Frequency: DAILY (son 365 gün), WEEKLY, MONTHLY, YEARLY
# reportType: SALES, SUBSCRIPTION, SUBSCRIPTION_EVENT, NEWSSTAND, PRE_ORDER, SUBSCRIBER
# reportSubType: SUMMARY, DETAILED

def _fetch_sales_report(
    *,
    report_type: str,
    report_sub_type: str,
    frequency: str,
    report_date: str,  # YYYY-MM-DD (DAILY), YYYY-MM-DD (haftanın bitiş günü), YYYY-MM (MONTHLY), YYYY (YEARLY)
    vendor_number: str,
    version: str = "1_1",
) -> list[dict[str, str]] | None:
    headers = _auth_headers()
    if headers is None:
        return None
    params = {
        "filter[frequency]": frequency,
        "filter[reportType]": report_type,
        "filter[reportSubType]": report_sub_type,
        "filter[vendorNumber]": vendor_number,
        "filter[reportDate]": report_date,
        "filter[version]": version,
    }
    url = f"{ASC_BASE}/v1/salesReports"
    try:
        with httpx.Client(timeout=30) as cli:
            resp = cli.get(url, headers=headers, params=params)
        if resp.status_code == 404:
            return []  # O tarih için rapor yok (çok yeni / hafta sonu vs.)
        if resp.status_code == 410:
            # Apple DAILY raporları ~365 gün saklar; daha eski tarihler beklenen 410
            logger.debug("ASC sales %s/%s/%s → 410 (süresi dolmuş, atlanıyor)",
                         report_type, frequency, report_date)
            return []
        if resp.status_code != 200:
            logger.warning("ASC sales %s/%s/%s → %d: %s",
                           report_type, frequency, report_date, resp.status_code, resp.text[:200])
            return None
        raw = resp.content
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text = raw.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        return list(reader)
    except Exception as exc:
        logger.error("ASC sales fetch hatası (%s %s %s): %s",
                     report_type, frequency, report_date, exc)
        return None


# ─── Analytics Reports API ───────────────────────────────────────────────────
# Analytics Reports daha gecikmeli (24-48 saat) ama impression / page view /
# conversion gibi metrikleri sağlar. İlk istek bir analyticsReportRequest
# oluşturur, sonra hazır raporlar listelenir.

# Bilinen Adam ID ↔ bundle (liste/filtre başarısız olursa Analytics yine açılsın)
_KNOWN_APP_IDS: dict[str, str] = {
    "com.nokta.finans.takip": "465599322",
    "com.nokta.sinemalar": "711475888",
}


def _list_apps() -> list[dict[str, Any]] | None:
    """Tüm uygulamaları sayfala (limit 200’de takılı kalma)."""
    headers = _auth_headers()
    if headers is None:
        return None
    out: list[dict[str, Any]] = []
    url: str | None = f"{ASC_BASE}/v1/apps"
    params: dict[str, Any] | None = {"limit": 200, "fields[apps]": "bundleId,name"}
    try:
        with httpx.Client(timeout=30) as cli:
            while url:
                resp = cli.get(url, headers=headers, params=params)
                params = None  # sonraki sayfada links.next tam URL
                if resp.status_code != 200:
                    logger.warning("ASC /v1/apps → %d %s", resp.status_code, resp.text[:180])
                    return out or None
                payload = resp.json()
                out.extend(payload.get("data") or [])
                url = (payload.get("links") or {}).get("next")
        return out
    except Exception as exc:
        logger.error("ASC apps listesi hatası: %s", exc)
        return out or None


def _find_app_by_bundle_filter(bundle_id: str) -> str | None:
    headers = _auth_headers()
    if headers is None:
        return None
    bid = (bundle_id or "").strip()
    if not bid:
        return None
    try:
        with httpx.Client(timeout=20) as cli:
            resp = cli.get(
                f"{ASC_BASE}/v1/apps",
                headers=headers,
                params={
                    "filter[bundleId]": bid,
                    "limit": 10,
                    "fields[apps]": "bundleId,name",
                },
            )
        if resp.status_code != 200:
            return None
        rows = resp.json().get("data") or []
        if rows:
            return rows[0].get("id")
    except Exception as exc:
        logger.debug("ASC filter[bundleId] %s: %s", bid, exc)
    return None


def find_app_id_by_bundle(bundle_id: str) -> str | None:
    bid = (bundle_id or "").strip()
    bid_lc = bid.lower()
    if not bid_lc:
        return None

    # 1) Doğrudan filtre
    hit = _find_app_by_bundle_filter(bid)
    if hit:
        return hit
    # Orijinal casing farklı olabilir
    if bid != bid_lc:
        hit = _find_app_by_bundle_filter(bid_lc)
        if hit:
            return hit

    # 2) Tam liste tarama
    apps = _list_apps() or []
    for a in apps:
        attr = a.get("attributes") or {}
        if (attr.get("bundleId") or "").strip().lower() == bid_lc:
            return a.get("id")
    # Kısmi: son segment / contains
    for a in apps:
        attr = a.get("attributes") or {}
        ab = (attr.get("bundleId") or "").strip().lower()
        if ab.endswith(bid_lc.split(".")[-1]) and "finans" in ab and "finans" in bid_lc:
            return a.get("id")
        if ab == bid_lc.replace("finans", "Finans".lower()):
            return a.get("id")

    # 3) Adam ID doğrudan erişilebilir mi?
    known = _KNOWN_APP_IDS.get(bid_lc)
    if known and app_exists(known):
        logger.info("ASC bundle=%s → bilinen Adam ID %s", bid, known)
        return known
    if known:
        # Liste boş/izin dar olsa bile Analytics request Adam ID ile denenebilir
        logger.warning(
            "ASC bundle=%s listede yok; bilinen Adam ID %s ile devam", bid, known
        )
        return known
    return None


def app_exists(app_id: str) -> bool:
    headers = _auth_headers()
    if headers is None or not (app_id or "").strip():
        return False
    try:
        with httpx.Client(timeout=15) as cli:
            resp = cli.get(
                f"{ASC_BASE}/v1/apps/{app_id.strip()}",
                headers=headers,
                params={"fields[apps]": "bundleId,name"},
            )
        return resp.status_code == 200
    except Exception:
        return False


# ─── Üst seviye toplama ──────────────────────────────────────────────────────

def fetch_daily_sales_summary(
    *,
    bundle_id: str,
    days: int,
    country: str = "all",
    device: str = "all",
    progress_cb=None,  # Callable[[done: int, total: int], None] | None
) -> dict[str, Any] | None:
    """Belirtilen gün sayısı için DAILY SUMMARY Sales raporlarını çeker ve özetler.

    Apple bir günü genelde 1-2 gün gecikmeyle yayımlıyor; bu yüzden "bugün"
    için rapor bulamayabiliriz — sessizce atlanır.
    """
    vendor = _env("ASC_VENDOR_NUMBER")
    if not vendor:
        return None

    # 0 = "tümü" — Apple DAILY raporları son 365 günü saklar
    effective_days = 365 if days == 0 else days

    end = date.today()
    start = end - timedelta(days=effective_days - 1)

    bundle_lc = (bundle_id or "").strip().lower()
    country_uc = (country or "all").strip().upper()
    device_filter = (device or "all").strip().lower()

    # device filtresi raporda "Apple Identifier" / "Device" alanları üzerinden uygulanır
    # ASC sales raporu Device kolonu: "iPhone" / "iPad" / "iPod" / "Desktop" / "Apple Watch" ...
    device_map = {"iphone": "iPhone", "ipad": "iPad", "ipod": "iPod"}
    device_match = device_map.get(device_filter)

    daily_rows: dict[str, dict[str, float]] = {}
    total_first_dl = 0
    total_redownloads = 0
    total_updates = 0
    total_proceeds = 0.0   # USD cinsinden (tüm para birimleri çevrilir)
    total_iap_units = 0    # in-app purchase + abonelik yenileme birimi
    total_paying_users = 0
    country_agg: dict[str, dict[str, float]] = {}
    version_agg: dict[str, dict[str, float]] = {}

    # Tüm günler için paralel HTTP isteği
    all_dates = []
    cur = start
    while cur <= end:
        all_dates.append(cur.isoformat())
        cur = cur + timedelta(days=1)

    def _fetch_day(ds: str):
        return ds, _fetch_sales_report(
            report_type="SALES",
            report_sub_type="SUMMARY",
            frequency="DAILY",
            report_date=ds,
            vendor_number=vendor,
        )

    # Apple API rate limit aşmamak için max 20 eşzamanlı istek
    workers = min(20, len(all_dates)) if all_dates else 1
    date_rows: dict[str, list | None] = {}
    total = len(all_dates)
    done_count = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_day, ds): ds for ds in all_dates}
        for fut in as_completed(futures):
            ds, rows = fut.result()
            date_rows[ds] = rows
            done_count += 1
            if progress_cb:
                try:
                    progress_cb(done_count, total)
                except Exception:
                    pass

    for ds in all_dates:
        rows = date_rows.get(ds)
        if not rows:
            continue
        day_dl = 0
        day_proc = 0.0
        for r in rows:
            r_bundle = (r.get("SKU") or r.get("Apple Identifier") or "").strip().lower()
            # Bundle eşleşmesi: SKU yerine "Title" / "Apple Identifier" kullanılır; ama biz tüm
            # raporu çekiyoruz çünkü vendor altında bu uygulamadan başka uygulama olabilir.
            # Apple SALES raporunda bundle bilgisi "SKU" olarak gelir.
            if bundle_lc and r_bundle and r_bundle != bundle_lc:
                # SKU genelde bundle ID ile uyuşmaz; bu yüzden ikincil olarak Title eşleşmesi
                # yapmak yerine vendor genelini topluyoruz. Şu an için filtreyi atla.
                pass
            if country_uc != "ALL":
                if (r.get("Country Code") or "").strip().upper() != country_uc:
                    continue
            if device_match:
                if (r.get("Device") or "").strip() != device_match:
                    continue

            units = int(float(r.get("Units") or 0))
            developer_proceeds = float(r.get("Developer Proceeds") or 0)
            currency = (r.get("Currency of Proceeds") or "USD").strip()
            product_type_id = (r.get("Product Type Identifier") or "").strip()
            # Apple Sales & Trends Report product type IDs (doğrulanmış mapping):
            # 1, 1F, 1T, 1E, 1EP = iPhone/iPod ilk indirme
            # 7, 7F, 7T           = Universal (iPad+iPhone) ilk indirme
            # 2, 2F, 2T           = iPad ilk indirme
            # 3, 3F, 3T           = iPhone/iPod GÜNCELLEME (update) — NOT: redownload değil!
            # 4, 4F, 4T           = iPad GÜNCELLEME
            # 8, 8F, 8T           = Universal GÜNCELLEME
            # 1EU, F1             = Diğer (eğitim güncelleme vs.)
            # IA1, IA9, IAY, IAC  = in-app satın alma / abonelik
            _FIRST_DL = {"1", "1F", "1T", "1E", "1EP", "7", "7F", "7T", "2", "2F", "2T"}
            _UPDATES   = {"3", "3F", "3T", "4", "4F", "4T", "8", "8F", "8T", "1EU"}
            if product_type_id in _FIRST_DL:
                total_first_dl += units
                day_dl += units
            elif product_type_id in _UPDATES:
                total_updates += units
            # Redownloads: Sales Report'ta ayrı bir product type yok;
            # Analytics Reports API'dan gelir — şimdilik 0 kalır.

            # In-App Purchase / abonelik birimleri (IAY=abonelik, IA1/IA9=iap, IAC=iptal)
            _IAP = {"IAY", "IAYF", "IA1", "IA9", "IAC", "IA1F", "IA9F"}
            if product_type_id in _IAP:
                total_iap_units += units

            # Para — tüm para birimlerini USD'ye çevir
            if developer_proceeds and currency:
                proceeds_usd = _to_usd(developer_proceeds * units, currency)
                total_proceeds += proceeds_usd
                day_proc += proceeds_usd

            # Ülke kırılımı (sadece ilk indirmeler)
            if product_type_id in _FIRST_DL:
                cc_code = (r.get("Country Code") or "").strip().upper()
                if cc_code:
                    country_agg.setdefault(cc_code, {"downloads": 0, "proceeds": 0.0})
                    country_agg[cc_code]["downloads"] += units
                ver = (r.get("Version") or "").strip()
                if ver:
                    version_agg.setdefault(ver, {"downloads": 0})
                    version_agg[ver]["downloads"] += units
                ver = (r.get("Version") or "").strip()
                if ver:
                    version_agg.setdefault(ver, {"downloads": 0})
                    version_agg[ver]["downloads"] += units

        daily_rows[ds] = {"downloads": day_dl, "proceeds": day_proc}

    if not daily_rows:
        return None

    # Günlük seri (tarih sırasına göre)
    dates_sorted = sorted(daily_rows.keys())
    dl_series = [daily_rows[d]["downloads"] for d in dates_sorted]
    pr_series = [daily_rows[d]["proceeds"] for d in dates_sorted]
    # total_redownloads Sales Report'tan alınamaz (Analytics API gerekir) — her zaman 0
    total_redownloads = 0
    total_downloads = total_first_dl

    logger.info(
        "ASC sales özet: days=%d, days_with_data=%d, first_dl=%d, "
        "updates=%d, iap_units=%d, total_dl=%d, proceeds_usd=%.2f, countries=%d, versions=%d",
        effective_days, len(daily_rows), total_first_dl,
        total_updates, total_iap_units, total_downloads, total_proceeds,
        len(country_agg), len(version_agg),
    )

    return {
        "first_time_downloads": total_first_dl,
        "redownloads": total_redownloads,
        "updates": total_updates,
        "iap_units": total_iap_units,
        "total_downloads": total_downloads,
        "proceeds_usd": round(total_proceeds, 2),
        "dl_series": dl_series,
        "pr_series": pr_series,
        "dates": dates_sorted,
        "country_breakdown": country_agg,
        "version_breakdown": version_agg,
    }


_FIRST_DL_TYPES = frozenset({"1", "1F", "1T", "1E", "1EP", "7", "7F", "7T", "2", "2F", "2T"})
_IAP_TYPES = frozenset({"IAY", "IAYF", "IA1", "IA9", "IAC", "IA1F", "IA9F"})
_SALES_DIM_METRICS = frozenset({"units", "proceeds", "sales", "total_downloads", "iap"})
_SALES_DIMS = ("country", "device", "app_version")


def sales_dimension_supported(metric: str) -> bool:
    return (metric or "").strip() in _SALES_DIM_METRICS


def fetch_sales_dimension_series(
    *,
    start: date,
    end: date,
    metric: str,
    dim: str,
    segment: str = "all",
    breakdown: str = "segment",
    limit: int = 30,
) -> dict[str, Any] | None:
    """Sales SUMMARY satırlarından ülke / cihaz / sürüm kırılımı.

    breakdown=segment → dönem toplamı (key=segment)
    breakdown=date|week|month → zaman serisi (segment=all: top segmentler ayrı
    satır değil; segment seçiliyse o segmentin zaman serisi)
    """
    vendor = _env("ASC_VENDOR_NUMBER")
    if not vendor:
        return None
    metric_key = (metric or "units").strip()
    dim_key = (dim or "overview").strip().lower()
    if metric_key not in _SALES_DIM_METRICS or dim_key not in _SALES_DIMS:
        return None
    if start > end:
        return None

    col_map = {
        "country": "Country Code",
        "device": "Device",
        "app_version": "Version",
    }
    col = col_map[dim_key]
    seg_filter = (segment or "all").strip()
    want_all = seg_filter.lower() in ("", "all")
    br = (breakdown or "segment").strip().lower()
    if br not in ("segment", "date", "week", "month"):
        br = "segment"

    all_dates: list[str] = []
    cur = start
    while cur <= end:
        all_dates.append(cur.isoformat())
        cur = cur + timedelta(days=1)

    def _fetch_day(ds: str):
        return ds, _fetch_sales_report(
            report_type="SALES",
            report_sub_type="SUMMARY",
            frequency="DAILY",
            report_date=ds,
            vendor_number=vendor,
        )

    workers = min(20, len(all_dates)) if all_dates else 1
    date_rows: dict[str, list | None] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_day, ds): ds for ds in all_dates}
        for fut in as_completed(futures):
            ds, rows = fut.result()
            date_rows[ds] = rows

    # day → seg → value
    daily_seg: dict[str, dict[str, float]] = {}
    seg_totals: dict[str, float] = {}

    def _metric_val(r: dict[str, Any]) -> float:
        units = int(float(r.get("Units") or 0))
        product_type_id = (r.get("Product Type Identifier") or "").strip()
        if metric_key in ("units", "total_downloads"):
            return float(units) if product_type_id in _FIRST_DL_TYPES else 0.0
        if metric_key == "iap":
            return float(units) if product_type_id in _IAP_TYPES else 0.0
        # proceeds / sales → USD
        developer_proceeds = float(r.get("Developer Proceeds") or 0)
        currency = (r.get("Currency of Proceeds") or "USD").strip()
        if not developer_proceeds:
            return 0.0
        return float(_to_usd(developer_proceeds * units, currency))

    for ds in all_dates:
        rows = date_rows.get(ds) or []
        if not rows:
            continue
        bucket = daily_seg.setdefault(ds, {})
        for r in rows:
            seg_val = (r.get(col) or "").strip()
            if dim_key == "country":
                seg_val = seg_val.upper()
            if not seg_val:
                continue
            if not want_all and seg_val != seg_filter:
                continue
            v = _metric_val(r)
            if not v:
                continue
            bucket[seg_val] = bucket.get(seg_val, 0.0) + v
            seg_totals[seg_val] = seg_totals.get(seg_val, 0.0) + v

    if not seg_totals:
        return {
            "ok": True,
            "series": [],
            "segments": [],
            "total": 0.0,
            "dim": dim_key,
            "segment": seg_filter if not want_all else "all",
            "breakdown": br,
        }

    top_segs = sorted(seg_totals.items(), key=lambda kv: kv[1], reverse=True)[: max(1, int(limit))]
    top_keys = [k for k, _ in top_segs]
    facets_segments = [
        {"key": k, "label": k, "total": round(v, 4)} for k, v in top_segs
    ]

    series: list[dict[str, Any]] = []
    if br == "segment":
        # Dönem toplamı — seçili segment yoksa top-N
        keys = [seg_filter] if not want_all else top_keys
        for k in keys:
            if k not in seg_totals:
                continue
            series.append({"key": k, "value": round(seg_totals[k], 4)})
    else:
        # Zaman serisi
        if want_all:
            # Tüm segmentlerin günlük toplamı (overview benzeri)
            for ds in all_dates:
                day_map = daily_seg.get(ds) or {}
                series.append({
                    "key": ds,
                    "value": round(sum(day_map.values()), 4),
                })
        else:
            for ds in all_dates:
                day_map = daily_seg.get(ds) or {}
                series.append({
                    "key": ds,
                    "value": round(float(day_map.get(seg_filter, 0.0)), 4),
                })
        if br in ("week", "month"):
            series = _aggregate_dim_series(series, br)

    total = round(sum(float(r.get("value") or 0) for r in series), 4) if br == "segment" else round(
        sum(seg_totals.get(k, 0.0) for k in ([seg_filter] if not want_all else top_keys)),
        4,
    )
    if br != "segment" and not want_all:
        total = round(seg_totals.get(seg_filter, 0.0), 4)
    elif br != "segment" and want_all:
        total = round(sum(seg_totals.values()), 4)

    return {
        "ok": True,
        "series": series,
        "segments": facets_segments,
        "total": total,
        "dim": dim_key,
        "segment": seg_filter if not want_all else "all",
        "breakdown": br,
    }


def _aggregate_dim_series(series: list[dict[str, Any]], breakdown: str) -> list[dict[str, Any]]:
    if not series or breakdown not in ("week", "month"):
        return series
    buckets: dict[str, float] = {}
    order: list[str] = []
    for row in series:
        key_src = str(row.get("key") or "")[:10]
        if len(key_src) < 10:
            continue
        if breakdown == "month":
            bkey = key_src[:7]
        else:
            try:
                d = date.fromisoformat(key_src)
            except ValueError:
                continue
            iso = d.isocalendar()
            bkey = f"{iso.year}-W{iso.week:02d}"
        if bkey not in buckets:
            order.append(bkey)
            buckets[bkey] = 0.0
        buckets[bkey] += float(row.get("value") or 0)
    return [{"key": k, "value": round(buckets[k], 4)} for k in order]


def fetch_subscription_summary(*, days: int) -> dict[str, Any] | None:
    """En son haftalık SUBSCRIPTION raporu (aktif abonelik metrikleri).

    Apple aboneliği daily veriyor — son 7 gün özetlenir.
    """
    series = fetch_subscription_daily_series(days=max(int(days or 7), 7))
    if not series or not series.get("dates"):
        return None
    dates = series["dates"]
    last = dates[-1]
    idx = dates.index(last)
    return {
        "active_plans": int(series["active_plans_series"][idx]),
        "paid_plans": int(series["active_plans_series"][idx]),
        "free_trials": int(series["free_trials_series"][idx]),
        "dates": dates,
        "active_plans_series": series["active_plans_series"],
        "free_trials_series": series["free_trials_series"],
    }


def fetch_subscription_daily_series(*, days: int = 90) -> dict[str, Any] | None:
    """Günlük aktif abonelik (subscription-state-plans-active) serisi."""
    vendor = _env("ASC_VENDOR_NUMBER")
    if not vendor:
        return None

    effective_days = 365 if days == 0 else max(1, min(int(days), 365))
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=effective_days - 1)
    all_dates: list[str] = []
    cur = start
    while cur <= end:
        all_dates.append(cur.isoformat())
        cur += timedelta(days=1)

    def _fetch_day(ds: str):
        return ds, _fetch_sales_report(
            report_type="SUBSCRIPTION",
            report_sub_type="SUMMARY",
            frequency="DAILY",
            report_date=ds,
            vendor_number=vendor,
            version="1_4",
        )

    workers = min(12, len(all_dates)) if all_dates else 1
    date_rows: dict[str, list | None] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_day, ds): ds for ds in all_dates}
        for fut in as_completed(futures):
            ds, rows = fut.result()
            date_rows[ds] = rows

    dates_out: list[str] = []
    active_series: list[float] = []
    trial_series: list[float] = []
    for ds in all_dates:
        rows = date_rows.get(ds) or []
        if not rows:
            continue
        active = 0.0
        trials = 0.0
        for r in rows:
            active += float(r.get("Active Standard Price Subscriptions") or 0)
            trials += float(r.get("Active Free Trial Introductory Offer Subscriptions") or 0)
        dates_out.append(ds)
        active_series.append(active)
        trial_series.append(trials)

    if not dates_out:
        return None
    return {
        "dates": dates_out,
        "active_plans_series": active_series,
        "free_trials_series": trial_series,
    }
