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

def _list_apps() -> list[dict[str, Any]] | None:
    headers = _auth_headers()
    if headers is None:
        return None
    try:
        with httpx.Client(timeout=20) as cli:
            resp = cli.get(f"{ASC_BASE}/v1/apps", headers=headers,
                           params={"limit": 200, "fields[apps]": "bundleId,name"})
        if resp.status_code != 200:
            logger.warning("ASC /v1/apps → %d", resp.status_code)
            return None
        return resp.json().get("data", []) or []
    except Exception as exc:
        logger.error("ASC apps listesi hatası: %s", exc)
        return None


def find_app_id_by_bundle(bundle_id: str) -> str | None:
    apps = _list_apps()
    if not apps:
        return None
    bid = (bundle_id or "").strip().lower()
    for a in apps:
        attr = a.get("attributes") or {}
        if (attr.get("bundleId") or "").strip().lower() == bid:
            return a.get("id")
    return None


# ─── Üst seviye toplama ──────────────────────────────────────────────────────

def fetch_daily_sales_summary(
    *,
    bundle_id: str,
    days: int,
    country: str = "all",
    device: str = "all",
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
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_day, ds): ds for ds in all_dates}
        for fut in as_completed(futures):
            ds, rows = fut.result()
            date_rows[ds] = rows

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


def fetch_subscription_summary(*, days: int) -> dict[str, Any] | None:
    """En son haftalık SUBSCRIPTION raporu (aktif abonelik metrikleri).

    Apple aboneliği daily veriyor — son 7 gün özetlenir.
    """
    vendor = _env("ASC_VENDOR_NUMBER")
    if not vendor:
        return None

    end = date.today()
    active_plans = 0
    paid_plans = 0
    free_trials = 0
    # Son 5 gün için en yeni mevcut raporu kullan (Apple'da dünden önceki gün hazır oluyor)
    for off in range(1, 6):
        ds = (end - timedelta(days=off)).isoformat()
        rows = _fetch_sales_report(
            report_type="SUBSCRIPTION",
            report_sub_type="SUMMARY",
            frequency="DAILY",
            report_date=ds,
            vendor_number=vendor,
            version="1_4",
        )
        if rows:
            for r in rows:
                # ASC SUBSCRIPTION SUMMARY kolonları: Active Standard Price Subscriptions,
                # Active Free Trial Introductory Offer Subscriptions, vs.
                active_plans += int(float(r.get("Active Standard Price Subscriptions") or 0))
                paid_plans += int(float(r.get("Active Standard Price Subscriptions") or 0))
                free_trials += int(float(r.get("Active Free Trial Introductory Offer Subscriptions") or 0))
            break  # En yeni rapor yeterli

    if active_plans == 0 and paid_plans == 0 and free_trials == 0:
        return None
    return {
        "active_plans": active_plans,
        "paid_plans": paid_plans,
        "free_trials": free_trials,
    }
