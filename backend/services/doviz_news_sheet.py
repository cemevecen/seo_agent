"""Google Sheets — Doviz.com haber yayın istatistikleri."""

from __future__ import annotations

import csv
import io
import logging
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

from backend.services.backlink_csv import fetch_public_sheet_csv

logger = logging.getLogger(__name__)

DOVIZ_NEWS_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1alTittOPWf8nHpF6Mt_zQ0IPpHRdRxMXcFB6v_rqWk4/"
    "edit?gid=1290391379#gid=1290391379"
)

_CACHE: dict[str, Any] | None = None
_CACHE_TTL_SEC = 900.0

_WD_TR = ("Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar")
_DATE_FMTS = ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y")
_ISO_WEEK_RE = re.compile(r"^(\d{4})-W(\d{2})$")
_WORD_RE = re.compile(r"[a-zA-ZçğıöşüÇĞİÖŞÜ0-9]{3,}", re.UNICODE)
_STOPWORDS = frozenset(
    {
        "ve", "ile", "icin", "için", "bir", "bu", "da", "de", "mi", "mı", "mu", "mü",
        "ne", "olan", "olarak", "daha", "cok", "çok", "var", "yok", "gibi", "kadar",
        "sonra", "once", "önce", "yeni", "gore", "göre", "den", "dan", "nin", "nın",
        "nun", "nün", "the", "and", "of", "to", "in", "on", "for", "from", "with",
        "that", "this", "was", "are", "not", "but", "its", "his", "her", "they",
        "hava", "olan", "oldu", "edildi", "etti", "icin", "uzerine", "üzerine",
        "hakkinda", "hakkında", "iliskin", "ilişkin", "karsi", "karşı", "icin",
        "son", "ilk", "iki", "uc", "üç", "dort", "dört", "bes", "beş", "alti", "altı",
        "yedi", "sekiz", "dokuz", "on", "bin", "milyon", "milyar", "tl", "usd",
        "www", "http", "https", "com", "net", "org", "html",
        "belli", "acikladi", "açıkladı", "basladi", "başladı", "geldi", "etti",
        "dedi", "soyledi", "söyledi", "yapti", "yaptı", "olacak", "ediyor",
        "icin", "guncel", "güncel", "haber", "haberleri", "devam", "eden",
        "uzerinden", "üzerinden", "arasinda", "arasında", "icin", "neden",
        "nasil", "nasıl", "hangi", "karsi", "yonelik", "yönelik",
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


def _top_title_keywords(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    display: dict[str, str] = {}
    for r in rows:
        title = str(r.get("title") or "")
        for raw in _WORD_RE.findall(title):
            key = _fold_tr(raw)
            if len(key) < 3 or key in _STOPWORDS or key.isdigit():
                continue
            counter[key] += 1
            if key not in display:
                display[key] = raw.lower()
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
    return out


def fetch_doviz_news_rows(*, force: bool = False) -> list[dict[str, Any]]:
    global _CACHE
    if not force and _CACHE is not None:
        age = time.monotonic() - float(_CACHE.get("ts") or 0)
        if age < _CACHE_TTL_SEC and isinstance(_CACHE.get("rows"), list):
            return list(_CACHE["rows"])

    csv_text = fetch_public_sheet_csv(DOVIZ_NEWS_SHEET_URL)
    rows = parse_doviz_news_csv(csv_text)
    _CACHE = {
        "ts": time.monotonic(),
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rows": rows,
        "source_url": DOVIZ_NEWS_SHEET_URL,
    }
    return list(rows)


def _filter_rows(rows: list[dict[str, Any]], category: str | None) -> list[dict[str, Any]]:
    cat = (category or "").strip()
    if not cat or cat.lower() in ("all", "tümü", "tumu"):
        return rows
    return [r for r in rows if r.get("category") == cat]


def _build_analytics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    active = sum(1 for r in rows if r.get("active"))
    own = sum(1 for r in rows if r.get("is_own"))
    sourced = total - own

    by_cat = Counter(str(r.get("category") or "Diğer") for r in rows)
    categories = [
        {"key": k, "label": k, "count": n, "share_pct": round(100.0 * n / total, 2) if total else 0}
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
    for s, n in src_counter.most_common(80):
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
                "own": vals["own"],
                "sourced": vals["sourced"],
                "total": vals["total"],
                "own_pct": round(100.0 * vals["own"] / t, 1),
            }
        )

    # Son 14 gün vs önceki 14 gün
    if by_day:
        last_day = datetime.strptime(by_day[-1]["day"], "%Y-%m-%d").date()
        recent_start = last_day - timedelta(days=13)
        prev_start = recent_start - timedelta(days=14)
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
            "recent_14d": recent_n,
            "prev_14d": prev_n,
            "delta": recent_n - prev_n,
            "delta_pct": round(100.0 * (recent_n - prev_n) / prev_n, 1) if prev_n else None,
        }
    else:
        recent_vs_prev = {"recent_14d": 0, "prev_14d": 0, "delta": 0, "delta_pct": None}

    date_min = by_day[0]["day"] if by_day else date_min_tmp
    date_max = by_day[-1]["day"] if by_day else date_max_tmp

    top_days = sorted(by_day, key=lambda x: -x["count"])[:10]
    low_days = sorted([d for d in by_day if d["count"] > 0], key=lambda x: x["count"])[:10]
    top_keywords = _top_title_keywords(rows, limit=15)

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
    force: bool = False,
    items_limit: int = 80,
) -> dict[str, Any]:
    all_rows = fetch_doviz_news_rows(force=force)
    rows = _filter_rows(all_rows, category)
    analytics = _build_analytics(rows)

    all_cats = Counter(str(r.get("category") or "Diğer") for r in all_rows)
    category_tabs = [{"key": "all", "label": "Tümü", "count": len(all_rows)}] + [
        {"key": k, "label": k, "count": n} for k, n in all_cats.most_common()
    ]

    fetched_at = None
    if _CACHE:
        fetched_at = _CACHE.get("fetched_at")

    items = [
        {
            "id": r.get("id"),
            "title": r.get("title"),
            "source": r.get("source") or None,
            "is_own": r.get("is_own"),
            "category": r.get("category"),
            "date": r.get("date"),
            "active": r.get("active"),
        }
        for r in rows[: max(1, min(int(items_limit or 80), 500))]
    ]

    return {
        "ok": True,
        "source_url": DOVIZ_NEWS_SHEET_URL,
        "fetched_at": fetched_at,
        "category": (category or "all"),
        "category_tabs": category_tabs,
        "items_total": len(rows),
        "items": items,
        **analytics,
    }
