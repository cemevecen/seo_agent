"""Doviz.com haber yayın istatistikleri — tek kaynak admin haber listesi."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from backend.services.doviz_news_admin import DOVIZ_NEWS_MIN_ID, news_id_in_scope

logger = logging.getLogger(__name__)

# Eski Google Sheet yedeği kullanılmaz — tek kaynak admin haber listesi.
DOVIZ_NEWS_ADMIN_URL = (
    "https://www.doviz.com/admin/news"
    "?page=1&type=N&status=1&is_advertorial=0&source=all&sort=id_desc"
)
# Geriye uyum (eski test/import); fetch asla sheet çekmez.
DOVIZ_NEWS_SHEET_URL = DOVIZ_NEWS_ADMIN_URL

_CACHE: dict[str, Any] | None = None
_CACHE_TTL_SEC = 300.0  # 5 dk — canlı tamamlamayla birlikte daha taze
_TZ_TR = ZoneInfo("Europe/Istanbul") if ZoneInfo else None


def filter_news_rows_in_scope(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """id < 719818 (2024 öncesi) satırları atar."""
    out: list[dict[str, Any]] = []
    for r in rows or []:
        if isinstance(r, dict) and news_id_in_scope(r.get("id")):
            out.append(r)
    return out

PERIOD_TABS = (
    {"key": "today", "label": "Bugün"},
    {"key": "yesterday", "label": "Dün"},
    {"key": "last_7d", "label": "Son 1 hafta"},
    {"key": "prev_week", "label": "Geçen hafta"},
    {"key": "this_month", "label": "Bu ay"},
    {"key": "last_month", "label": "Geçen ay"},
    {"key": "all", "label": "Tümü"},
)
_PERIOD_KEYS = frozenset(p["key"] for p in PERIOD_TABS)

_WD_TR = ("Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar")
_DATE_FMTS = ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y")
_ISO_WEEK_RE = re.compile(r"^(\d{4})-W(\d{2})$")
_WORD_RE = re.compile(r"[a-zA-ZçğıöşüÇĞİÖŞÜ0-9]{3,}", re.UNICODE)
# Trump'tan / İstanbul'da → apostrof + ek düşer (tan/ndan gürültüsü).
_APOSTROPHE_SUFFIX_RE = re.compile(r"[''`´′’][\wçğıöşüÇĞİÖŞÜ]+", re.UNICODE)

_ALLOW_SHORT = frozenset(
    {
        "abd", "fed", "imf", "spk", "tcmb", "nato", "chp", "akp", "mhp", "iyi",
        "btc", "eth", "usd", "eur", "gbp", "ons", "bist", "tufe", "ipo", "etf",
    }
)

_STOPWORDS = frozenset(
    {
        "ve", "ile", "icin", "bir", "bu", "su", "o", "da", "de", "ki", "mi", "mu",
        "ne", "hem", "ama", "fakat", "veya", "ya", "her", "cok", "daha", "en",
        "gibi", "kadar", "gore", "uzere", "karsi", "yonelik", "arasinda",
        "uzerine", "uzerinden", "hakkinda", "iliskin", "dolayi", "nedeniyle",
        "sonra", "once", "beri", "ise", "ancak", "ayrica", "ozellikle",
        "olan", "olarak", "oldu", "olacak", "oluyor", "edildi", "ediyor", "edecek",
        "etti", "yapti", "yapiliyor", "yapilacak", "dedi", "soyledi", "belirtti",
        "acikladi", "aciklandi", "duyurdu", "duyuruldu", "ulasti", "asti", "indi",
        "aldi", "geldi", "basladi", "devam", "eden", "bekleniyor", "gosterdi",
        "artti", "azaldi", "dustu", "yukseldi", "gerceklesti",
        "tan", "ten", "dan", "den", "ndan", "nden", "nda", "nde", "nin", "nun",
        "daki", "deki", "teki", "taki", "yla", "yle",
        "kisi", "kisiye", "kisinin", "ilde", "ilden", "ilce", "gun", "gunu", "gununde",
        "bugun", "dun", "yarin", "ayin", "ayi", "yil", "yilin", "yilda", "donemde",
        "yuzde", "oraninda", "sonrasi", "oncesi", "sirada", "siradan",
        "mesaji", "mesaj", "uyarisi", "uyari", "aciklama", "aciklamasi",
        "baskani", "baskan", "bankasi", "banka", "sirketi", "sirket", "grubu",
        "bakani", "bakan", "muduru", "yetkilisi", "sozcusu", "temsilcisi",
        "haber", "haberleri", "gundem", "son", "ilk", "yeni", "buyuk", "onemli",
        "genel", "resmi", "ozel", "ayri", "tek", "iki", "uc", "dort", "bes",
        "alti", "yedi", "sekiz", "dokuz", "on", "bin", "milyon", "milyar",
        "tl", "www", "http", "https", "com", "net", "org", "html",
        "the", "and", "of", "to", "in", "for", "from", "with", "that", "this",
        "var", "yok", "belli", "iste", "geri", "yeniden", "orta", "acik", "disi",
        "nasil", "hangi", "neden", "guncel", "tarihi", "karari", "toplantisi",
        "raporu", "verisi", "verileri", "iddiasi", "iddia", "yorumu", "tahmini",
        "hedefi", "seviyesi", "hava", "nin", "nun",
    }
)

_FILLER_VERBS = frozenset(
    {
        "geldi", "aldi", "indi", "asti", "etti", "oldu", "yapti", "dedi",
        "dustu", "artti", "ulasti", "basladi", "acikladi", "aciklandi",
        "duyurdu", "belirtti", "gosterdi", "azaldi", "yukseldi", "geliyor",
        "gidiyor", "oluyor", "ediyor", "yapiyor", "bekliyor",
    }
)


def _fold_tr(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    return (
        s.replace("ı", "i")
        .replace("İ", "i")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ş", "s")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def _tokenize_title(title: str) -> list[str]:
    cleaned = _APOSTROPHE_SUFFIX_RE.sub("", title or "")
    cleaned = cleaned.replace("-", " ")
    return _WORD_RE.findall(cleaned)


def _is_meaningful_keyword(key: str) -> bool:
    if not key or key.isdigit():
        return False
    if key in _STOPWORDS or key in _FILLER_VERBS:
        return False
    if len(key) <= 2:
        return False
    if len(key) == 3 and key not in _ALLOW_SHORT:
        return False
    if re.fullmatch(r"n?(d[ae]|t[ae]n|d[ae]n|d[ae]ki)", key):
        return False
    return True


def _top_title_keywords(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    display: dict[str, str] = {}
    for r in rows:
        for raw in _tokenize_title(str(r.get("title") or "")):
            key = _fold_tr(raw)
            if not _is_meaningful_keyword(key):
                continue
            counter[key] += 1
            if key not in display:
                display[key] = key
    total_hits = sum(counter.values()) or 1
    out = []
    for key, n in counter.most_common(limit):
        out.append(
            {
                "word": display.get(key, key),
                "count": n,
                "share_pct": round(100.0 * n / total_hits, 2),
            }
        )
    return out


def _last_n_days(date_max: str | None, n: int = 7) -> list[str]:
    if not date_max:
        return []
    try:
        end = datetime.strptime(date_max, "%Y-%m-%d").date()
    except ValueError:
        return []
    return [(end - timedelta(days=n - 1 - i)).isoformat() for i in range(n)]


def _source_spark_series(
    rows: list[dict[str, Any]],
    source: str,
    day_keys: list[str],
) -> list[int]:
    if not day_keys:
        return []
    day_set = set(day_keys)
    counts: Counter[str] = Counter()
    for r in rows:
        if r.get("is_own"):
            continue
        if str(r.get("source") or "").strip() != source:
            continue
        d = r.get("date_day")
        if d in day_set:
            counts[str(d)] += 1
    return [int(counts.get(d, 0)) for d in day_keys]


def _iso_week_range(week_key: str) -> tuple[str, str, str] | None:
    """ISO hafta (Pzt–Paz) için TR tarih aralığı: start, end, label."""
    m = _ISO_WEEK_RE.match(str(week_key or "").strip())
    if not m:
        return None
    year, week = int(m.group(1)), int(m.group(2))
    try:
        start = datetime.fromisocalendar(year, week, 1).date()
        end = datetime.fromisocalendar(year, week, 7).date()
    except ValueError:
        return None
    label = f"{start.strftime('%d.%m.%Y')} – {end.strftime('%d.%m.%Y')}"
    return start.isoformat(), end.isoformat(), label


def _parse_dt(raw: str | None) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _norm_source(raw: str | None) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s, flags=re.I)
    s = s.rstrip("/")
    return s.lower() if s else ""


def _display_source(raw: str | None) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s, flags=re.I).rstrip("/")
    return s


def parse_doviz_news_csv(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text or ""))
    out: list[dict[str, Any]] = []
    for row in reader:
        news_id = str(row.get("ID") or row.get("Id") or row.get("id") or "").strip()
        title = str(row.get("Title") or row.get("Başlık") or "").strip()
        if not news_id and not title:
            continue
        source_raw = str(row.get("Source") or row.get("Kaynak") or "").strip()
        category = str(row.get("Category") or row.get("Kategori") or "").strip() or "Diğer"
        active_raw = str(row.get("Active") or row.get("Aktif") or "").strip()
        active = active_raw in ("✅", "1", "true", "True", "yes", "YES", "aktif", "Aktif")
        dt = _parse_dt(row.get("Date") or row.get("Tarih"))
        is_own = not bool(source_raw)
        out.append(
            {
                "id": news_id,
                "active": active,
                "title": title,
                "source": _display_source(source_raw),
                "source_key": _norm_source(source_raw),
                "is_own": is_own,
                "category": category,
                "date": dt.isoformat(sep=" ") if dt else None,
                "date_day": dt.strftime("%Y-%m-%d") if dt else None,
                "hour": dt.hour if dt else None,
                "weekday": dt.weekday() if dt else None,
                "iso_week": f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}" if dt else None,
            }
        )
    out.sort(key=lambda r: r.get("date") or "", reverse=True)
    return filter_news_rows_in_scope(out)


def set_doviz_news_rows_cache(
    rows: list[dict[str, Any]],
    *,
    source: str = "doviz_admin_news",
    source_url: str | None = None,
    db: Any | None = None,
    sync_ok: bool = True,
    sync_message: str = "",
    sync_mode: str = "",
    mark_background: bool = True,
) -> None:
    """Admin bridge / ingest sonrası önbellek + DB snapshot."""
    global _CACHE
    rows = filter_news_rows_in_scope(rows)
    src_url = source_url or DOVIZ_NEWS_ADMIN_URL
    now = datetime.utcnow()
    fetched_at = now.isoformat(timespec="seconds") + "Z"
    mode = (sync_mode or "").strip()[:32]
    msg = (sync_message or "").strip()[:512]
    _CACHE = {
        "ts": time.monotonic(),
        "fetched_at": fetched_at,
        "rows": list(rows),
        "source": source,
        "source_url": src_url,
        "min_id": DOVIZ_NEWS_MIN_ID,
        "sync_ok": bool(sync_ok),
        "sync_message": msg,
        "sync_mode": mode,
        "background_synced_at": fetched_at if mark_background else None,
    }
    try:
        from backend.database import SessionLocal
        from backend.models import DovizNewsWorkspace

        own = db is None
        session = db or SessionLocal()
        try:
            row = session.get(DovizNewsWorkspace, 1)
            if row is None:
                row = DovizNewsWorkspace(id=1, rows_json="[]")
                session.add(row)
            row.rows_json = json.dumps(rows, ensure_ascii=False)
            row.source = (source or "")[:64]
            row.source_url = (src_url or "")[:512]
            row.row_count = len(rows)
            row.updated_at = now
            if hasattr(row, "sync_ok"):
                row.sync_ok = bool(sync_ok)
                row.sync_message = msg
                row.sync_mode = mode
                if mark_background:
                    row.background_synced_at = now
            if own:
                session.commit()
            else:
                session.flush()
        finally:
            if own:
                session.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Doviz news DB snapshot write failed: %s", exc)


def record_doviz_news_sync_failure(
    *,
    message: str,
    sync_mode: str = "recent_7d",
    source: str = "doviz_admin_news_bridge",
) -> None:
    """Satırları silmeden arka plan sync hatasını kaydet."""
    global _CACHE
    now = datetime.utcnow()
    msg = (message or "Sync başarısız")[:512]
    mode = (sync_mode or "")[:32]
    if _CACHE is not None:
        _CACHE["sync_ok"] = False
        _CACHE["sync_message"] = msg
        _CACHE["sync_mode"] = mode
        _CACHE["admin_error"] = msg
    try:
        from backend.database import SessionLocal
        from backend.models import DovizNewsWorkspace

        with SessionLocal() as session:
            row = session.get(DovizNewsWorkspace, 1)
            if row is None:
                return
            if hasattr(row, "sync_ok"):
                row.sync_ok = False
                row.sync_message = msg
                row.sync_mode = mode
                row.background_synced_at = now
            if source:
                row.source = (source or row.source or "")[:64]
            session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Doviz news sync failure record failed: %s", exc)


def _load_doviz_news_rows_from_db() -> list[dict[str, Any]] | None:
    try:
        from backend.database import SessionLocal
        from backend.models import DovizNewsWorkspace

        with SessionLocal() as session:
            row = session.get(DovizNewsWorkspace, 1)
            if row is None or not (row.rows_json or "").strip():
                return None
            data = json.loads(row.rows_json)
            if not isinstance(data, list) or not data:
                return None
            scoped = filter_news_rows_in_scope(data)
            # Eski snapshot'ta 2024 öncesi varsa bir kez budayıp kaydet
            if len(scoped) < len(data):
                set_doviz_news_rows_cache(
                    scoped,
                    source=row.source or "db",
                    source_url=row.source_url or DOVIZ_NEWS_ADMIN_URL,
                )
                return list(scoped)
            global _CACHE
            fetched = None
            bg = None
            if row.updated_at:
                fetched = row.updated_at.isoformat(timespec="seconds") + "Z"
            if getattr(row, "background_synced_at", None):
                bg = row.background_synced_at.isoformat(timespec="seconds") + "Z"
            _CACHE = {
                "ts": time.monotonic(),
                "fetched_at": fetched,
                "rows": scoped,
                "source": row.source or "db",
                "source_url": row.source_url or DOVIZ_NEWS_ADMIN_URL,
                "min_id": DOVIZ_NEWS_MIN_ID,
                "sync_ok": bool(getattr(row, "sync_ok", True)),
                "sync_message": str(getattr(row, "sync_message", "") or ""),
                "sync_mode": str(getattr(row, "sync_mode", "") or ""),
                "background_synced_at": bg or fetched,
            }
            return list(scoped)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Doviz news DB load skipped: %s", exc)
        return None


def _db_snapshot_meta() -> tuple[datetime | None, int]:
    try:
        from backend.database import SessionLocal
        from backend.models import DovizNewsWorkspace

        with SessionLocal() as session:
            row = session.get(DovizNewsWorkspace, 1)
            if row is None:
                return None, 0
            return row.updated_at, int(row.row_count or 0)
    except Exception:
        return None, 0


def _is_admin_news_source(source: str | None) -> bool:
    s = (source or "").lower()
    return ("admin" in s) or ("bridge" in s)


def _db_snapshot_source() -> str:
    try:
        from backend.database import SessionLocal
        from backend.models import DovizNewsWorkspace

        with SessionLocal() as session:
            row = session.get(DovizNewsWorkspace, 1)
            if row is None:
                return ""
            return str(row.source or "")
    except Exception:
        return ""


def fetch_doviz_news_rows(
    *,
    force: bool = False,
    prefer_sheet: bool = False,
) -> list[dict[str, Any]]:
    """Aktif haberler: yalnızca admin (köprü/DB) — Google Sheet asla kullanılmaz.

    prefer_sheet yok sayılır (geriye uyum için imzada kalır).
    """
    global _CACHE
    _ = prefer_sheet  # sheet yedeği kaldırıldı

    if not force and _CACHE is not None:
        age = time.monotonic() - float(_CACHE.get("ts") or 0)
        if age < _CACHE_TTL_SEC and isinstance(_CACHE.get("rows"), list):
            db_updated, db_count = _db_snapshot_meta()
            cache_fetched = str(_CACHE.get("fetched_at") or "")
            if db_updated and db_count > 0:
                db_iso = db_updated.isoformat(timespec="seconds") + "Z"
                if db_iso > cache_fetched or (
                    db_count != len(_CACHE.get("rows") or []) and age > 5
                ):
                    db_rows = _load_doviz_news_rows_from_db()
                    if db_rows:
                        return db_rows
            scoped = filter_news_rows_in_scope(list(_CACHE["rows"]))
            if len(scoped) != len(_CACHE["rows"]):
                set_doviz_news_rows_cache(
                    scoped,
                    source=str(_CACHE.get("source") or "cache"),
                    source_url=_CACHE.get("source_url") or DOVIZ_NEWS_ADMIN_URL,
                )
            return scoped

    if not force:
        db_rows = _load_doviz_news_rows_from_db()
        if db_rows:
            return db_rows

    from backend.config import settings
    from backend.services.doviz_notification_admin import (
        admin_credentials_configured,
        admin_http_proxy,
        is_admin_vpn_unreachable_error,
    )

    admin_err = ""
    # Doğrudan scrape: bayrak veya VPN çıkış proxy’si (online otomatik tarama)
    try_admin = bool(
        getattr(settings, "doviz_admin_notification_sync_enabled", True)
        and admin_credentials_configured()
        and (
            getattr(settings, "doviz_admin_news_direct_scrape", False)
            or bool(admin_http_proxy())
        )
    )
    if try_admin:
        try:
            from backend.services.doviz_news_admin import fetch_active_news_rows_from_admin

            fetched = fetch_active_news_rows_from_admin()
            rows = fetched.get("rows") or []
            if rows:
                try:
                    from backend.services.doviz_news_live import (
                        enrich_rows_with_publish_dates,
                    )

                    rows, _pub = enrich_rows_with_publish_dates(
                        rows, limit=80, discover_limit=160
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("doviz news publish enrich after admin: %s", exc)
                set_doviz_news_rows_cache(
                    rows,
                    source="doviz_admin_news",
                    source_url=fetched.get("source_url") or DOVIZ_NEWS_ADMIN_URL,
                )
                return list(rows)
            admin_err = "Admin haber tablosu boş"
        except Exception as exc:  # noqa: BLE001
            admin_err = str(exc) or "admin news failed"
            logger.warning("Doviz news admin fetch failed: %s", admin_err)
            low = admin_err.lower()
            if ("şifre" in low or "password" in low) and not is_admin_vpn_unreachable_error(admin_err):
                raise

    # Google Sheet yasağı: yalnızca mevcut admin/bridge snapshot
    kept = _load_doviz_news_rows_from_db()
    if kept:
        src = _db_snapshot_source() or str((_CACHE or {}).get("source") or "db")
        logger.info(
            "Doviz news: admin snapshot kullanılıyor (%s, %s kayıt); sheet yok",
            src,
            len(kept),
        )
        if force:
            try:
                from backend.services.doviz_news_live import (
                    enrich_rows_with_publish_dates,
                )

                kept, pub = enrich_rows_with_publish_dates(
                    kept, limit=80, discover_limit=160
                )
                set_doviz_news_rows_cache(
                    kept,
                    source=src,
                    source_url=DOVIZ_NEWS_ADMIN_URL,
                    sync_ok=True,
                    sync_message=(
                        f"Snapshot + yayın saati ({int(pub.get('updated') or 0)} düzeltme)"
                    ),
                    sync_mode="publish_enrich",
                    mark_background=True,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("doviz news publish enrich on db snapshot: %s", exc)
        if _CACHE is not None:
            _CACHE["sheet_skipped"] = True
            if admin_err:
                _CACHE["admin_error"] = admin_err[:240]
        return kept

    msg = (
        "Doviz News verisi yok. Tek kaynak admin: "
        + DOVIZ_NEWS_ADMIN_URL
        + " — VPN köprüsü ile «Elle yenile» veya "
        "scripts/doviz_admin_notification_bridge.py --daemon çalıştırın."
    )
    if admin_err:
        msg += f" (admin: {admin_err[:160]})"
    raise ValueError(msg)



def ingest_doviz_news_rows(
    rows: list[dict[str, Any]],
    *,
    source: str = "doviz_admin_news_bridge",
    source_url: str | None = None,
    merge: bool = True,
    sync_mode: str = "recent_7d",
) -> dict[str, Any]:
    """VPN köprüsünden gelen aktif haber satırlarını DB + cache yazar.

    merge=True: mevcut snapshot ile id üzerinden birleştirir (son 7 gün scrape
    eski kayıtları silmez). merge=False: tam replace.
    """
    raw_n = len(rows or [])
    cleaned = [
        r
        for r in (rows or [])
        if isinstance(r, dict) and (r.get("id") or r.get("title")) and news_id_in_scope(r.get("id"))
    ]
    skipped_old = raw_n - len(cleaned)
    if not cleaned:
        return {
            "ok": False,
            "synced": False,
            "parsed": 0,
            "skipped_old": skipped_old,
            "min_id": DOVIZ_NEWS_MIN_ID,
            "message": f"Ingest: satır yok (min_id>={DOVIZ_NEWS_MIN_ID}).",
            "source": source,
        }

    norm: list[dict[str, Any]] = []
    for r in cleaned:
        news_id = str(r.get("id") or "").strip()
        title = str(r.get("title") or "").strip()
        if not news_id and not title:
            continue
        source_raw = str(r.get("source") or "").strip()
        if source_raw in ("Kendi içeriği", "-"):
            source_raw = ""
        category = str(r.get("category") or "Diğer").strip() or "Diğer"
        active = bool(r.get("active", True))
        dt = _parse_dt(str(r.get("date") or ""))
        if dt is None and r.get("date_day"):
            dt = _parse_dt(str(r.get("date_day")))
        norm.append(
            {
                "id": news_id,
                "active": active,
                "title": title,
                "source": _display_source(source_raw),
                "source_key": _norm_source(source_raw),
                "is_own": not bool(source_raw),
                "category": category,
                "date": dt.isoformat(sep=" ") if dt else (str(r.get("date") or "") or None),
                "date_day": dt.strftime("%Y-%m-%d") if dt else (str(r.get("date_day") or "")[:10] or None),
                "hour": dt.hour if dt else r.get("hour"),
                "weekday": dt.weekday() if dt else r.get("weekday"),
                "iso_week": (
                    f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
                    if dt
                    else r.get("iso_week")
                ),
            }
        )

    mode = (sync_mode or ("merge" if merge else "full")).strip() or "recent_7d"
    merged_n = 0
    if merge:
        existing = _load_doviz_news_rows_from_db() or []
        by_id: dict[str, dict[str, Any]] = {}
        for r in existing:
            rid = str(r.get("id") or "").strip()
            if rid:
                by_id[rid] = r
        before = len(by_id)
        for r in norm:
            rid = str(r.get("id") or "").strip()
            if rid:
                by_id[rid] = r
        merged_n = max(0, len(by_id) - before)
        final_rows = list(by_id.values())
    else:
        final_rows = list(norm)

    final_rows.sort(key=lambda r: r.get("date") or "", reverse=True)

    # Admin Date ≈ dateCreated; yayına alma = JSON-LD datePublished
    publish_meta: dict[str, Any] = {}
    try:
        from backend.services.doviz_news_live import enrich_rows_with_publish_dates

        final_rows, publish_meta = enrich_rows_with_publish_dates(
            final_rows, limit=80, discover_limit=160
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("doviz news publish-date enrich failed: %s", exc)
        publish_meta = {"ok": False, "error": str(exc)[:160]}

    pub_n = int(publish_meta.get("updated") or 0)
    set_doviz_news_rows_cache(
        final_rows,
        source=source,
        source_url=source_url
        or "https://www.doviz.com/admin/news?page=1&type=N&status=1&is_advertorial=0&sort=id_desc",
        sync_ok=True,
        sync_message=(
            f"{len(norm)} yeni/güncel · toplam {len(final_rows)}"
            + (f" · +{merged_n} yeni id" if merge and merged_n else "")
            + (f" · {pub_n} yayın saati düzeltildi" if pub_n else "")
        ),
        sync_mode=mode,
        mark_background=True,
    )
    return {
        "ok": True,
        "synced": True,
        "parsed": len(norm),
        "row_count": len(final_rows),
        "incoming": len(norm),
        "merged": bool(merge),
        "new_ids": merged_n if merge else len(norm),
        "skipped_old": skipped_old,
        "min_id": DOVIZ_NEWS_MIN_ID,
        "sync_mode": mode,
        "publish_dates_updated": pub_n,
        "message": (
            f"Doviz news admin ingest · {len(norm)} çekildi → {len(final_rows)} toplam"
            f" ({mode}"
            + (f", {skipped_old} eski atıldı" if skipped_old else "")
            + (f", {pub_n} yayın saati" if pub_n else "")
            + ")."
        ),
        "source": source,
        "fetched_at": (_CACHE or {}).get("fetched_at"),
        "background_synced_at": (_CACHE or {}).get("background_synced_at"),
        "source_url": (_CACHE or {}).get("source_url"),
        "sync_ok": True,
    }


def _short_category_label(name: str) -> str:
    s = str(name or "").strip()
    if not s:
        return s
    s2 = re.sub(r"\s+haberleri\s*$", "", s, flags=re.IGNORECASE)
    s2 = re.sub(r"\s+haberler\s*$", "", s2, flags=re.IGNORECASE)
    s2 = s2.strip(" -–—")
    return s2 or s


def _filter_rows(rows: list[dict[str, Any]], category: str | None) -> list[dict[str, Any]]:
    cat = (category or "").strip()
    if not cat or cat.lower() in ("all", "tümü", "tumu"):
        return rows
    return [r for r in rows if r.get("category") == cat]


def _today_tr() -> date:
    if _TZ_TR is not None:
        return datetime.now(_TZ_TR).date()
    return datetime.utcnow().date()


def _parse_day(value: Any) -> date | None:
    raw = str(value or "").strip()[:10]
    if len(raw) < 10:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _iso_week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _shift_month(d: date, months: int) -> date:
    y = d.year
    m = d.month + months
    while m < 1:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    return date(y, m, 1)


def _month_end(d: date) -> date:
    nxt = _shift_month(d.replace(day=1), 1)
    return nxt - timedelta(days=1)


def resolve_period(
    period: str | None,
    *,
    today: date | None = None,
    custom_start: str | date | None = None,
    custom_end: str | date | None = None,
) -> dict[str, Any]:
    """Seçilen dönem + karşılaştırma penceresi (önceki eşdeğer aralık)."""
    today = today or _today_tr()
    cs = custom_start if isinstance(custom_start, date) else _parse_day(custom_start)
    ce = custom_end if isinstance(custom_end, date) else _parse_day(custom_end)
    if cs and ce:
        if ce < cs:
            cs, ce = ce, cs
        span_days = (ce - cs).days + 1
        cmp_end = cs - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=span_days - 1)
        if cs == ce:
            range_label = cs.isoformat()
            cmp_label = "Önceki gün"
            kpi_label = "Gün vs önceki"
        else:
            range_label = f"{cs.isoformat()} → {ce.isoformat()}"
            cmp_label = f"Önceki {span_days} gün"
            kpi_label = "Aralık vs önceki"
        return {
            "key": "custom",
            "label": "Tarih aralığı",
            "start": cs,
            "end": ce,
            "cmp_start": cmp_start,
            "cmp_end": cmp_end,
            "cmp_label": cmp_label,
            "kpi_label": kpi_label,
            "range_label": range_label,
            "cmp_range_label": f"{cmp_start.isoformat()} → {cmp_end.isoformat()}",
        }

    key = (period or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "tumu": "all",
        "tümü": "all",
        "hepsi": "all",
        "bugun": "today",
        "bugün": "today",
        "dun": "yesterday",
        "dün": "yesterday",
        "son_1_hafta": "last_7d",
        "son1hafta": "last_7d",
        "last_week": "last_7d",
        "gecen_hafta": "prev_week",
        "geçen_hafta": "prev_week",
        "bu_ay": "this_month",
        "gecen_ay": "last_month",
        "geçen_ay": "last_month",
        "tarih": "custom",
        "tarih_araligi": "custom",
        "custom_range": "custom",
    }
    key = aliases.get(key, key)
    if key == "custom" or key not in _PERIOD_KEYS:
        # Tarih verilmeden custom istenirse son 7 güne düş
        key = "last_7d"

    if key == "all":
        return {
            "key": "all",
            "label": "Tümü",
            "start": None,
            "end": None,
            "cmp_start": None,
            "cmp_end": None,
            "cmp_label": "Önceki 7 gün",
            "kpi_label": "Son 7 vs önceki",
            "range_label": "Tüm veri",
            "cmp_range_label": None,
        }

    if key == "today":
        # Bugün vs geçen haftanın aynı günü
        start = end = today
        cmp_start = cmp_end = today - timedelta(days=7)
        kpi_label = "Bugün vs geçen hf"
        cmp_label = "Geçen hafta aynı gün"
    elif key == "yesterday":
        # Önceki haftanın aynı günü (örn. cuma → geçen cuma)
        start = end = today - timedelta(days=1)
        cmp_start = cmp_end = start - timedelta(days=7)
        kpi_label = "Dün vs geçen hf"
        cmp_label = "Geçen hafta aynı gün"
    elif key == "last_7d":
        end = today
        start = today - timedelta(days=6)
        cmp_end = start - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=6)
        kpi_label = "Hafta vs önceki"
        cmp_label = "Önceki 7 gün"
    elif key == "prev_week":
        this_week = _iso_week_start(today)
        end = this_week - timedelta(days=1)
        start = end - timedelta(days=6)
        cmp_end = start - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=6)
        kpi_label = "Geçen hf vs önceki"
        cmp_label = "Önceki hafta"
    elif key == "this_month":
        start = today.replace(day=1)
        end = today
        span = (end - start).days
        prev_month_start = _shift_month(start, -1)
        cmp_start = prev_month_start
        cmp_end = min(prev_month_start + timedelta(days=span), _month_end(prev_month_start))
        kpi_label = "Ay vs önceki"
        cmp_label = "Geçen ay (aynı gün)"
    else:  # last_month
        this_month_start = today.replace(day=1)
        end = this_month_start - timedelta(days=1)
        start = end.replace(day=1)
        cmp_end = start - timedelta(days=1)
        cmp_start = cmp_end.replace(day=1)
        kpi_label = "Geçen ay vs önceki"
        cmp_label = "Önceki ay"

    label = next((p["label"] for p in PERIOD_TABS if p["key"] == key), key)
    return {
        "key": key,
        "label": label,
        "start": start,
        "end": end,
        "cmp_start": cmp_start,
        "cmp_end": cmp_end,
        "cmp_label": cmp_label,
        "kpi_label": kpi_label,
        "range_label": f"{start.isoformat()} → {end.isoformat()}",
        "cmp_range_label": f"{cmp_start.isoformat()} → {cmp_end.isoformat()}",
    }


def _day_has_rows(rows: list[dict[str, Any]], day: date) -> bool:
    key = day.isoformat()
    for r in rows:
        if str(r.get("date_day") or "")[:10] == key:
            return True
    return False


def _shift_last_7d_if_today_empty(
    period_info: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    today: date | None = None,
) -> dict[str, Any]:
    """Son 1 hafta: bugün henüz veri yoksa pencereyi önceki aynı günden düne kaydır.

    Örn. Salı ve bugün boş → geçen Salı … Pazartesi (bu Salı dahil değil).
    """
    if period_info.get("key") != "last_7d":
        return period_info
    today = today or _today_tr()
    if _day_has_rows(rows, today):
        return period_info
    end = today - timedelta(days=1)
    start = end - timedelta(days=6)
    cmp_end = start - timedelta(days=1)
    cmp_start = cmp_end - timedelta(days=6)
    return {
        **period_info,
        "start": start,
        "end": end,
        "cmp_start": cmp_start,
        "cmp_end": cmp_end,
        "range_label": f"{start.isoformat()} → {end.isoformat()}",
        "cmp_range_label": f"{cmp_start.isoformat()} → {cmp_end.isoformat()}",
        "trimmed_empty_today": True,
    }


def _filter_by_date_range(
    rows: list[dict[str, Any]],
    start: date | None,
    end: date | None,
) -> list[dict[str, Any]]:
    if start is None or end is None:
        return rows
    out = []
    for r in rows:
        d = _parse_day(r.get("date_day"))
        if d is None:
            continue
        if start <= d <= end:
            out.append(r)
    return out


def _period_comparison(current_n: int, prev_n: int, period_info: dict[str, Any]) -> dict[str, Any]:
    delta = current_n - prev_n
    return {
        "recent": current_n,
        "prev": prev_n,
        "recent_7d": current_n,
        "prev_7d": prev_n,
        "delta": delta,
        "delta_pct": round(100.0 * delta / prev_n, 1) if prev_n else None,
        "label": period_info.get("label"),
        "cmp_label": period_info.get("cmp_label"),
        "kpi_label": period_info.get("kpi_label") or "Dönem vs önceki",
        "range_label": period_info.get("range_label"),
        "cmp_range_label": period_info.get("cmp_range_label"),
    }


def _delta_pair(cur: Any, prev: Any) -> dict[str, Any]:
    if cur is None:
        return {"current": None, "previous": None, "delta": None, "delta_pct": None}
    try:
        cur_f = float(cur)
    except (TypeError, ValueError):
        return {"current": None, "previous": None, "delta": None, "delta_pct": None}
    try:
        prev_f = float(prev) if prev is not None else None
    except (TypeError, ValueError):
        prev_f = None
    if prev_f is None:
        return {"current": cur_f, "previous": None, "delta": None, "delta_pct": None}
    delta = cur_f - prev_f
    return {
        "current": cur_f,
        "previous": prev_f,
        "delta": round(delta, 2) if abs(delta) < 1000 else delta,
        "delta_pct": round(100.0 * delta / prev_f, 1) if prev_f != 0 else None,
    }


def _kpi_compare(cur_summary: dict[str, Any], prev_summary: dict[str, Any] | None) -> dict[str, Any]:
    """Seçili dönem KPI'ları vs önceki eşdeğer dönem."""
    cur_s = cur_summary or {}
    prev_s = prev_summary or {}
    cur_peak = cur_s.get("peak_hour") or {}
    prev_peak = prev_s.get("peak_hour") or {}
    return {
        "total": _delta_pair(cur_s.get("total"), prev_s.get("total")),
        "own": _delta_pair(cur_s.get("own"), prev_s.get("own")),
        "own_pct": _delta_pair(cur_s.get("own_pct"), prev_s.get("own_pct")),
        "avg_per_day": _delta_pair(cur_s.get("avg_per_day"), prev_s.get("avg_per_day")),
        "median_per_day": _delta_pair(cur_s.get("median_per_day"), prev_s.get("median_per_day")),
        "weekend_count": _delta_pair(cur_s.get("weekend_count"), prev_s.get("weekend_count")),
        "weekend_pct": _delta_pair(cur_s.get("weekend_pct"), prev_s.get("weekend_pct")),
        "weekday_count": _delta_pair(cur_s.get("weekday_count"), prev_s.get("weekday_count")),
        "peak_count": _delta_pair(cur_peak.get("count"), prev_peak.get("count")),
        "peak_hour_cur": cur_peak.get("label"),
        "peak_hour_prev": prev_peak.get("label"),
        "cmp_label": None,
    }


def _build_analytics(rows: list[dict[str, Any]], *, keyword_limit: int = 15) -> dict[str, Any]:
    total = len(rows)
    active = sum(1 for r in rows if r.get("active"))
    own = sum(1 for r in rows if r.get("is_own"))
    sourced = total - own

    by_cat = Counter(str(r.get("category") or "Diğer") for r in rows)
    categories = [
        {
            "key": k,
            "label": _short_category_label(k),
            "count": n,
            "share_pct": round(100.0 * n / total, 2) if total else 0,
        }
        for k, n in by_cat.most_common()
    ]

    src_counter: Counter[str] = Counter()
    for r in rows:
        if r.get("is_own"):
            continue
        key = str(r.get("source") or "").strip() or "(bilinmeyen)"
        src_counter[key] += 1

    date_min_tmp = None
    date_max_tmp = None
    day_counts_pre: Counter[str] = Counter()
    for r in rows:
        if r.get("date_day"):
            day_counts_pre[str(r["date_day"])] += 1
    if day_counts_pre:
        sorted_days = sorted(day_counts_pre.keys())
        date_min_tmp = sorted_days[0]
        date_max_tmp = sorted_days[-1]
    spark_days = _last_n_days(date_max_tmp, 7)

    by_source = []
    for s, n in src_counter.most_common():
        spark = _source_spark_series(rows, s, spark_days)
        by_source.append(
            {
                "source": s,
                "count": n,
                "share_pct": round(100.0 * n / total, 2) if total else 0,
                "spark_7d": spark,
                "spark_days": spark_days,
                "spark_total": sum(spark),
            }
        )

    day_counts: Counter[str] = Counter()
    hour_counts: Counter[int] = Counter()
    wd_counts: Counter[int] = Counter()
    week_counts: Counter[str] = Counter()
    for r in rows:
        if r.get("date_day"):
            day_counts[str(r["date_day"])] += 1
        if r.get("hour") is not None:
            hour_counts[int(r["hour"])] += 1
        if r.get("weekday") is not None:
            wd_counts[int(r["weekday"])] += 1
        if r.get("iso_week"):
            week_counts[str(r["iso_week"])] += 1

    by_day = [{"day": d, "count": day_counts[d]} for d in sorted(day_counts.keys())]
    avg_per_day = round(sum(day_counts.values()) / len(day_counts), 2) if day_counts else 0.0
    median_day = 0
    if day_counts:
        sorted_counts = sorted(day_counts.values())
        mid = len(sorted_counts) // 2
        median_day = (
            sorted_counts[mid]
            if len(sorted_counts) % 2
            else round((sorted_counts[mid - 1] + sorted_counts[mid]) / 2, 1)
        )

    by_hour = [{"hour": h, "label": f"{h:02d}:00", "count": hour_counts.get(h, 0)} for h in range(24)]
    peak_hour = max(by_hour, key=lambda x: x["count"]) if by_hour else None

    by_weekday = []
    for i, name in enumerate(_WD_TR):
        by_weekday.append(
            {
                "weekday": i,
                "label": name,
                "count": wd_counts.get(i, 0),
                "is_weekend": i >= 5,
            }
        )
    weekend_count = wd_counts.get(5, 0) + wd_counts.get(6, 0)
    weekday_count = sum(wd_counts.get(i, 0) for i in range(5))

    week_keys = sorted(week_counts.keys())
    by_week = []
    prev = None
    for wk in week_keys:
        cnt = week_counts[wk]
        delta = None if prev is None else cnt - prev
        delta_pct = None if prev in (None, 0) else round(100.0 * (cnt - prev) / prev, 1)
        wr = _iso_week_range(wk)
        by_week.append(
            {
                "week": wk,
                "start": wr[0] if wr else None,
                "end": wr[1] if wr else None,
                "range": wr[2] if wr else None,
                "label": f"{wk} · {wr[2]}" if wr else wk,
                "count": cnt,
                "delta": delta,
                "delta_pct": delta_pct,
                "trend": (
                    "—" if delta is None else ("up" if delta > 0 else "down" if delta < 0 else "flat")
                ),
            }
        )
        prev = cnt

    own_by_cat_map: dict[str, dict[str, int]] = defaultdict(lambda: {"own": 0, "sourced": 0, "total": 0})
    for r in rows:
        cat = str(r.get("category") or "Diğer")
        own_by_cat_map[cat]["total"] += 1
        if r.get("is_own"):
            own_by_cat_map[cat]["own"] += 1
        else:
            own_by_cat_map[cat]["sourced"] += 1
    own_by_category = []
    for cat, vals in sorted(own_by_cat_map.items(), key=lambda x: -x[1]["total"]):
        t = vals["total"] or 1
        own_by_category.append(
            {
                "category": cat,
                "label": _short_category_label(cat),
                "own": vals["own"],
                "sourced": vals["sourced"],
                "total": vals["total"],
                "own_pct": round(100.0 * vals["own"] / t, 1),
            }
        )

    # Son 7 gün vs önceki 7 gün (haftalık)
    if by_day:
        last_day = datetime.strptime(by_day[-1]["day"], "%Y-%m-%d").date()
        recent_start = last_day - timedelta(days=6)
        prev_start = recent_start - timedelta(days=7)
        prev_end = recent_start - timedelta(days=1)
        recent_n = sum(
            x["count"]
            for x in by_day
            if recent_start <= datetime.strptime(x["day"], "%Y-%m-%d").date() <= last_day
        )
        prev_n = sum(
            x["count"]
            for x in by_day
            if prev_start <= datetime.strptime(x["day"], "%Y-%m-%d").date() <= prev_end
        )
        recent_vs_prev = {
            "recent_7d": recent_n,
            "prev_7d": prev_n,
            "delta": recent_n - prev_n,
            "delta_pct": round(100.0 * (recent_n - prev_n) / prev_n, 1) if prev_n else None,
        }
    else:
        recent_vs_prev = {"recent_7d": 0, "prev_7d": 0, "delta": 0, "delta_pct": None}

    date_min = by_day[0]["day"] if by_day else date_min_tmp
    date_max = by_day[-1]["day"] if by_day else date_max_tmp

    top_days = sorted(by_day, key=lambda x: -x["count"])[:10]
    low_days = sorted([d for d in by_day if d["count"] > 0], key=lambda x: x["count"])[:10]
    top_keywords = _top_title_keywords(rows, limit=keyword_limit)

    return {
        "summary": {
            "total": total,
            "active": active,
            "own": own,
            "sourced": sourced,
            "own_pct": round(100.0 * own / total, 2) if total else 0,
            "sourced_pct": round(100.0 * sourced / total, 2) if total else 0,
            "avg_per_day": avg_per_day,
            "median_per_day": median_day,
            "day_count": len(day_counts),
            "weekend_count": weekend_count,
            "weekday_count": weekday_count,
            "weekend_pct": round(100.0 * weekend_count / total, 2) if total else 0,
            "peak_hour": peak_hour,
            "date_min": date_min,
            "date_max": date_max,
            "category_count": len(categories),
            "source_count": len(src_counter),
            "recent_vs_prev": recent_vs_prev,
        },
        "categories": categories,
        "by_category": categories,
        "by_source": by_source,
        "by_day": by_day,
        "by_week": by_week,
        "by_hour": by_hour,
        "by_weekday": by_weekday,
        "own_by_category": own_by_category,
        "top_days": top_days,
        "low_days": low_days,
        "top_keywords": top_keywords,
        "spark_days": spark_days,
    }


def doviz_news_payload(
    *,
    category: str | None = None,
    period: str | None = None,
    force: bool = False,
    items_limit: int = 250,
    db: Any | None = None,
    include_traffic: bool = True,
    site_id: int = 1,
    custom_start: str | None = None,
    custom_end: str | None = None,
) -> dict[str, Any]:
    all_rows = fetch_doviz_news_rows(force=force)
    period_info = resolve_period(
        period,
        custom_start=custom_start,
        custom_end=custom_end,
    )
    cat_rows = _filter_rows(all_rows, category)
    # Son 1 hafta + bugün boş: önceki aynı günden düne (boş bugünü basma)
    period_info = _shift_last_7d_if_today_empty(period_info, cat_rows)
    rows = _filter_by_date_range(cat_rows, period_info["start"], period_info["end"])
    keyword_limit = 30 if period_info["key"] == "all" else 15
    analytics = _build_analytics(rows, keyword_limit=keyword_limit)

    if period_info["key"] == "all":
        rvp = analytics["summary"].get("recent_vs_prev") or {}
        cmp_rows_n = int(rvp.get("prev_7d") or 0)
        current_for_cmp = int(rvp.get("recent_7d") or 0)
        # Enrich KPI labels for all-time view (still last-7 vs prev-7 inside data)
        rvp = {
            **rvp,
            "recent": current_for_cmp,
            "prev": cmp_rows_n,
            "label": period_info["label"],
            "cmp_label": period_info["cmp_label"],
            "kpi_label": period_info["kpi_label"],
            "range_label": period_info["range_label"],
        }
        analytics["summary"]["recent_vs_prev"] = rvp
        analytics["summary"]["kpi_compare"] = None
        date_min = analytics["summary"].get("date_min")
        date_max = analytics["summary"].get("date_max")
        range_label = (
            f"{date_min} → {date_max}" if date_min and date_max else "Tüm veri"
        )
        period_meta = {
            "key": "all",
            "label": "Tümü",
            "start": date_min,
            "end": date_max,
            "cmp_start": None,
            "cmp_end": None,
            "cmp_label": period_info["cmp_label"],
            "kpi_label": period_info["kpi_label"],
            "range_label": range_label,
            "cmp_range_label": None,
            "current_count": len(rows),
            "previous_count": cmp_rows_n,
            "delta": current_for_cmp - cmp_rows_n,
            "delta_pct": rvp.get("delta_pct"),
            "cmp_note": "Son 7 vs önceki 7",
        }
    else:
        cmp_rows = _filter_by_date_range(
            cat_rows, period_info["cmp_start"], period_info["cmp_end"]
        )
        prev_analytics = _build_analytics(cmp_rows)
        analytics["summary"]["recent_vs_prev"] = _period_comparison(
            len(rows), len(cmp_rows), period_info
        )
        kpi_cmp = _kpi_compare(analytics["summary"], prev_analytics.get("summary"))
        kpi_cmp["cmp_label"] = period_info.get("cmp_label")
        analytics["summary"]["kpi_compare"] = kpi_cmp
        period_meta = {
            "key": period_info["key"],
            "label": period_info["label"],
            "start": period_info["start"].isoformat() if period_info["start"] else None,
            "end": period_info["end"].isoformat() if period_info["end"] else None,
            "cmp_start": period_info["cmp_start"].isoformat() if period_info["cmp_start"] else None,
            "cmp_end": period_info["cmp_end"].isoformat() if period_info["cmp_end"] else None,
            "cmp_label": period_info["cmp_label"],
            "kpi_label": period_info["kpi_label"],
            "range_label": period_info["range_label"],
            "cmp_range_label": period_info["cmp_range_label"],
            "current_count": len(rows),
            "previous_count": len(cmp_rows),
            "delta": len(rows) - len(cmp_rows),
            "delta_pct": (
                round(100.0 * (len(rows) - len(cmp_rows)) / len(cmp_rows), 1) if cmp_rows else None
            ),
        }

    tab_source = _filter_by_date_range(all_rows, period_info["start"], period_info["end"])
    all_cats = Counter(str(r.get("category") or "Diğer") for r in tab_source)
    category_tabs = [{"key": "all", "label": "Tümü", "count": len(tab_source)}] + [
        {"key": k, "label": _short_category_label(k), "count": n} for k, n in all_cats.most_common()
    ]

    fetched_at = None
    live_meta: dict[str, Any] = {}
    if _CACHE:
        fetched_at = _CACHE.get("fetched_at")
        # Eski canlı/sheet gap meta artık doldurulmaz; alan geriye uyum için boş.

    traffic: dict[str, Any] | None = None
    by_article: dict[str, Any] = {}
    from backend.services.notification_content_traffic import normalize_article_id as _norm_aid

    if include_traffic and db is not None:
        try:
            from backend.services.doviz_news_traffic import enrich_doviz_news_traffic

            traffic = enrich_doviz_news_traffic(
                db,
                rows=rows,
                period_meta=period_meta,
                period_key=period_info["key"],
                site_id=site_id,
            )
            by_article = (traffic or {}).get("by_article") or {}
        except Exception as exc:
            logger.exception("doviz news traffic enrich failed")
            traffic = {
                "ok": False,
                "error": str(exc) or "Trafik zenginleştirme başarısız",
            }

    items = []
    # Son içerikler: dönem KPI'sından bağımsız — kategoriye göre en yeni N kayıt
    item_rows = cat_rows
    item_limit = max(1, min(int(items_limit or 250), 500))
    for r in item_rows[:item_limit]:
        aid = _norm_aid(str(r.get("id") or ""))
        tr = by_article.get(aid) or {}
        items.append(
            {
                "id": r.get("id"),
                "title": r.get("title"),
                "source": r.get("source") or None,
                "is_own": r.get("is_own"),
                "category": r.get("category"),
                "date": r.get("date"),
                "active": r.get("active"),
                "views": tr.get("views"),
                "sessions": tr.get("sessions"),
                "gsc_clicks": tr.get("gsc_clicks"),
                "gsc_impressions": tr.get("gsc_impressions"),
                "gsc_ctr": tr.get("gsc_ctr"),
                "gsc_position": tr.get("gsc_position"),
            }
        )

    latest_id = None
    for r in item_rows:
        rid = str(r.get("id") or "").strip()
        if rid:
            latest_id = rid
            break

    cache = _CACHE or {}
    bg_at = cache.get("background_synced_at") or fetched_at or cache.get("fetched_at")
    sync_ok = cache.get("sync_ok")
    if sync_ok is None:
        sync_ok = True
    sync_message = str(cache.get("sync_message") or cache.get("admin_error") or "")
    sync_mode = str(cache.get("sync_mode") or "")
    sync_age_sec = None
    sync_stale = False
    if bg_at:
        try:
            ts = str(bg_at).replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                from datetime import timezone as _tz

                dt = dt.replace(tzinfo=_tz.utc)
            from datetime import timezone as _tz2

            sync_age_sec = int((datetime.now(_tz2.utc) - dt.astimezone(_tz2.utc)).total_seconds())
            # 30 dk hedef + 15 dk tolerans
            sync_stale = sync_age_sec > (45 * 60)
        except Exception:
            sync_age_sec = None
    sync_health = "ok"
    if not sync_ok:
        sync_health = "error"
    elif sync_stale:
        sync_health = "stale"

    return {
        "ok": True,
        "source": cache.get("source") or "doviz_admin_news",
        "source_url": cache.get("source_url") or DOVIZ_NEWS_ADMIN_URL,
        "fetched_at": fetched_at or cache.get("fetched_at"),
        "background_synced_at": bg_at,
        "sync_ok": bool(sync_ok),
        "sync_message": sync_message,
        "sync_mode": sync_mode,
        "sync_age_sec": sync_age_sec,
        "sync_stale": sync_stale,
        "sync_health": sync_health,
        "admin_error": cache.get("admin_error"),
        "min_id": DOVIZ_NEWS_MIN_ID,
        "live": live_meta,
        "category": (category or "all"),
        "period": period_info["key"],
        "period_meta": period_meta,
        "period_tabs": list(PERIOD_TABS),
        "category_tabs": category_tabs,
        "latest_id": latest_id,
        "items_total": len(item_rows),
        "items": items,
        "traffic": traffic,
        **analytics,
    }
