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
from datetime import date, datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ASC_BASE = "https://api.appstoreconnect.apple.com"
_TOKEN_TTL = 60 * 18  # JWT en fazla 20 dk; biz 18 dk tutuyoruz
_token_cache: dict[str, Any] = {"token": None, "exp": 0}


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
        raw = os.getenv("ASC_PRIVATE_KEY") or ""
        logger.error(
            "ASC JWT üretilemedi: %s | raw_len=%d, has_newline=%s, has_escaped_n=%s, "
            "starts=%r, ends=%r, normalized_lines=%d",
            exc, len(raw), "\n" in raw, "\\n" in raw,
            raw[:35], raw[-35:], pem.count("\n"),
        )
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
        if resp.status_code != 200:
            logger.warning("ASC sales %s/%s/%s → %d: %s",
                           report_type, frequency, report_date, resp.status_code, resp.text[:600])
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
        logger.warning("ASC fetch_daily_sales_summary: VENDOR_NUMBER yok, atlanıyor")
        return None
    logger.warning("ASC fetch_daily_sales_summary başladı: vendor=%s, days=%d, bundle=%s",
                   vendor, days, bundle_id)

    end = date.today()
    start = end - timedelta(days=days - 1)

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
    total_proceeds = 0.0
    total_paying_users = 0
    country_agg: dict[str, dict[str, float]] = {}
    version_agg: dict[str, dict[str, float]] = {}

    cur = start
    while cur <= end:
        ds = cur.isoformat()
        rows = _fetch_sales_report(
            report_type="SALES",
            report_sub_type="SUMMARY",
            frequency="DAILY",
            report_date=ds,
            vendor_number=vendor,
        )
        cur = cur + timedelta(days=1)
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
            # Product Type IDs:
            # 1, 1T, 1F, 1E, 1EP, 1EU = iOS app first-time download
            # 3, 3T, 3F = redownload
            # 7, 7T, 7F = update
            # IA1, IA9 = in-app purchase
            # IAY, IAC = auto-renewable subscription
            if product_type_id in {"1", "1T", "1F", "1E", "1EP", "1EU"}:
                total_first_dl += units
                day_dl += units
            elif product_type_id in {"3", "3T", "3F"}:
                total_redownloads += units
            elif product_type_id in {"7", "7T", "7F"}:
                total_updates += units
            # Para — TL/EUR vs. USD karışık; sadece USD topla, gerisi atla (basit yaklaşım)
            if currency == "USD":
                total_proceeds += developer_proceeds * units
                day_proc += developer_proceeds * units

            # Ülke kırılımı (sadece downloads)
            if product_type_id in {"1", "1T", "1F", "1E", "1EP", "1EU"}:
                cc = (r.get("Country Code") or "").strip().upper()
                if cc:
                    country_agg.setdefault(cc, {"downloads": 0, "proceeds": 0.0})
                    country_agg[cc]["downloads"] += units
                    if currency == "USD":
                        country_agg[cc]["proceeds"] += developer_proceeds * units
                ver = (r.get("Version") or "").strip()
                if ver:
                    version_agg.setdefault(ver, {"downloads": 0})
                    version_agg[ver]["downloads"] += units

        daily_rows[ds] = {"downloads": day_dl, "proceeds": day_proc}

    if not daily_rows:
        logger.warning("ASC fetch_daily_sales_summary: tüm günler için rapor yok (daily_rows boş)")
        return None

    # Günlük seri (tarih sırasına göre)
    dates_sorted = sorted(daily_rows.keys())
    dl_series = [daily_rows[d]["downloads"] for d in dates_sorted]
    pr_series = [daily_rows[d]["proceeds"] for d in dates_sorted]
    total_downloads = total_first_dl + total_redownloads

    logger.warning(
        "ASC sales özet: days=%d, days_with_data=%d, first_dl=%d, redl=%d, "
        "updates=%d, total_dl=%d, proceeds_usd=%.2f, countries=%d, versions=%d",
        days, len(daily_rows), total_first_dl, total_redownloads,
        total_updates, total_downloads, total_proceeds,
        len(country_agg), len(version_agg),
    )

    return {
        "first_time_downloads": total_first_dl,
        "redownloads": total_redownloads,
        "updates": total_updates,
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
