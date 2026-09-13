"""Play bulk CSV → Android reverse-list overview facts.

Günlük Playwright taraması mühürde yalnız ANR/Crash çektiği ve oturum düşünce
donduğu için tablo kolonları buradan dolar. Kaynak: Play reports bucket
(installs / crashes / ratings / store_performance). Değiştirmez: mevcut
kırılım (OS/sürüm/cihaz) fact'leri; yalnız overview günü yazar.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import date, timedelta
from typing import Any

LOGGER = logging.getLogger(__name__)

_SOURCE = "play_reports_csv"
_VALUE_KIND = "daily"


def _decode_csv(raw: bytes) -> str:
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        text = raw.decode("utf-16")
    else:
        try:
            text = raw.decode("utf-16")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", errors="replace")
    return text.lstrip("\ufeff")


def _num(raw: str | None) -> float | None:
    s = (raw or "").strip().replace(",", "")
    if not s or s in ("-", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _in_window(day: str, start: date, end: date) -> bool:
    if len(day) != 10:
        return False
    try:
        d = date.fromisoformat(day)
    except ValueError:
        return False
    return start <= d <= end


def _fact(metric: str, day: str, value: float) -> dict[str, Any]:
    return {
        "metric": metric,
        "view_id": f"{metric}_csv",
        "dim": "overview",
        "segment": "OVERALL",
        "date": day,
        "value": value,
        "label": metric,
        "source": _SOURCE,
        "value_kind": _VALUE_KIND,
    }


def facts_from_installs_csv(text: str, *, start: date, end: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in csv.DictReader(io.StringIO(text)):
        day = str(row.get("Date") or "")[:10]
        if not _in_window(day, start, end):
            continue
        mapping = (
            ("device_acquisition", "Daily Device Installs"),
            ("user_acquisition", "Daily User Installs"),
            ("user_lost", "Daily User Uninstalls"),
        )
        for metric, col in mapping:
            val = _num(row.get(col))
            if val is None:
                continue
            out.append(_fact(metric, day, val))
    return out


def facts_from_crashes_csv(text: str, *, start: date, end: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in csv.DictReader(io.StringIO(text)):
        day = str(row.get("Date") or "")[:10]
        if not _in_window(day, start, end):
            continue
        for metric, col in (("crashes", "Daily Crashes"), ("anrs", "Daily ANRs")):
            val = _num(row.get(col))
            if val is None:
                continue
            out.append(_fact(metric, day, val))
    return out


def facts_from_ratings_csv(text: str, *, start: date, end: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in csv.DictReader(io.StringIO(text)):
        day = str(row.get("Date") or "")[:10]
        if not _in_window(day, start, end):
            continue
        val = _num(row.get("Total Average Rating"))
        if val is None:
            continue
        out.append(_fact("rating", day, round(val, 4)))
    return out


def facts_from_store_country_csv(text: str, *, start: date, end: date) -> list[dict[str, Any]]:
    """Ülke satırlarını güne topla — overview ziyaretçi / edinme."""
    visitors: dict[str, float] = {}
    acq: dict[str, float] = {}
    for row in csv.DictReader(io.StringIO(text)):
        day = str(row.get("Date") or "")[:10]
        if not _in_window(day, start, end):
            continue
        v = _num(row.get("Store listing visitors"))
        a = _num(row.get("Store listing acquisitions"))
        if v is not None:
            visitors[day] = visitors.get(day, 0.0) + v
        if a is not None:
            acq[day] = acq.get(day, 0.0) + a
    out: list[dict[str, Any]] = []
    for day, val in visitors.items():
        out.append(_fact("ar2_visitors", day, val))
    for day, val in acq.items():
        out.append(_fact("ar2_acquisitions", day, val))
    return out


def build_overview_facts_from_bucket(
    *,
    package_name: str = "com.Doviz",
    start: date | None = None,
    end: date | None = None,
    months_back: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Play reports bucket'tan overview fact'leri. months_back set ise yalnız son N ay."""
    from backend.services import gp_client
    from backend.services.history_seal import calendar_yesterday, history_start

    start_d = start or history_start()
    end_d = end or calendar_yesterday()
    if months_back is not None and months_back > 0:
        start_d = max(start_d, end_d.replace(day=1) - timedelta(days=32 * int(months_back)))
    meta: dict[str, Any] = {
        "package_name": package_name,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "files": 0,
        "errors": [],
    }
    bucket_name = os.environ.get("GP_REPORTS_BUCKET") or ""
    if not bucket_name:
        meta["errors"].append("GP_REPORTS_BUCKET yok")
        return [], meta
    client = gp_client._get_storage_client()
    if client is None:
        meta["errors"].append("storage client yok")
        return [], meta
    bucket = client.bucket(bucket_name)

    wanted_suffix = {
        f"stats/installs/installs_{package_name}_": "_overview.csv",
        f"stats/crashes/crashes_{package_name}_": "_overview.csv",
        f"stats/ratings/ratings_{package_name}_": "_overview.csv",
        f"stats/store_performance/store_performance_{package_name}_": "_country.csv",
    }
    parsers = {
        "stats/installs/": facts_from_installs_csv,
        "stats/crashes/": facts_from_crashes_csv,
        "stats/ratings/": facts_from_ratings_csv,
        "stats/store_performance/": facts_from_store_country_csv,
    }
    facts: list[dict[str, Any]] = []
    for prefix, suffix in wanted_suffix.items():
        parser = next(fn for key, fn in parsers.items() if prefix.startswith(key))
        try:
            names = [
                blob.name
                for blob in bucket.list_blobs(prefix=prefix)
                if blob.name.endswith(suffix)
            ]
        except Exception as exc:  # noqa: BLE001
            meta["errors"].append(f"{prefix}: {exc}"[:180])
            continue
        for name in names:
            # YYYYMM in filename — skip months far outside the window
            ym = name.rsplit("_", 2)[-2] if "_" in name else ""
            if len(ym) == 6 and ym.isdigit():
                month_start = date(int(ym[:4]), int(ym[4:6]), 1)
                if month_start > end_d or (month_start + timedelta(days=40)) < start_d:
                    continue
            try:
                raw = bucket.blob(name).download_as_bytes()
                text = _decode_csv(raw)
                facts.extend(parser(text, start=start_d, end=end_d))
                meta["files"] = int(meta["files"]) + 1
            except Exception as exc:  # noqa: BLE001
                meta["errors"].append(f"{name}: {exc}"[:160])
                LOGGER.warning("play csv skip %s: %s", name, exc)
    # Same day can appear once per file; last write wins if duplicate keys
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for fact in facts:
        by_key[(str(fact["metric"]), str(fact["date"]))] = fact
    merged = list(by_key.values())
    meta["fact_count"] = len(merged)
    return merged, meta


def ingest_overview_facts(facts: list[dict[str, Any]], *, package_name: str = "com.Doviz") -> dict[str, Any]:
    """Railway play-console ingest. Overview günlerini CSV ile değiştirir; kırılımlar kalır."""
    import requests

    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    url = (
        os.environ.get("PLAY_CONSOLE_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/play-console/ingest"
    ).strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    if not facts:
        return {"ok": False, "message": "fact yok"}
    body = {
        "metrics": [],
        "panels": {"explorer_facts": facts},
        "reviews": [],
        "rating_summary": {},
        "package_name": package_name,
        "source": "play_reports_csv",
        "sync_ok": True,
        "sync_message": f"Play CSV overview · {len(facts)} fact",
        "sync_mode": "csv_overview_fill",
    }
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=180,
    )
    try:
        payload = resp.json()
    except Exception:
        payload = {"message": resp.text[:300]}
    if not isinstance(payload, dict):
        payload = {"message": str(payload)[:300]}
    payload["http_status"] = resp.status_code
    payload["ok"] = resp.status_code < 400 and payload.get("ok") is not False
    return payload
