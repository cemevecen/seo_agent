"""Android Vitals — Firebase Console scrape birincil sorun kaynağı (Play scrape yedek)."""

from __future__ import annotations

import copy
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_CATEGORY_TEMPLATES: list[dict[str, str]] = [
    {
        "id": "general",
        "label": "Genel",
        "description": "Belirtilen filtrelerin uygulandığı tüm sorunların görünümü",
    },
    {
        "id": "production",
        "label": "Üretimde",
        "description": "Üretim sürümündeki kullanıcı tarafından algılanan en önemli sorunlar",
    },
    {
        "id": "potential_fixes",
        "label": "Olası düzeltmeler içeren",
        "description": "Olası düzeltmeler içeren, kullanıcı tarafından algılanan sorunlar",
    },
    {
        "id": "analysis",
        "label": "Analiz içeren",
        "description": "Analiz içeren, kullanıcı tarafından algılanan sorunlar",
    },
]


def _parse_count(val: Any) -> int:
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip().replace(".", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0
    try:
        return int(float(m.group(1)))
    except (TypeError, ValueError):
        return 0


def _count_block_issue_rows(block: dict[str, Any] | None) -> int:
    if not isinstance(block, dict):
        return 0
    total = 0
    for cat in block.get("categories") or []:
        if not isinstance(cat, dict):
            continue
        issues = cat.get("issues")
        if isinstance(issues, list):
            total += len(issues)
    return total


def count_vitals_issues(vitals: dict[str, Any] | None) -> int:
    """Yalnızca gerçek issue satırlarını say (Play UI issue_count metnini yok say)."""
    if not isinstance(vitals, dict):
        return 0
    total = 0
    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    for et in ("CRASH", "ANR"):
        total += _count_block_issue_rows(crashes.get(et) if isinstance(crashes.get(et), dict) else {})
    byv = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    for payload in byv.values():
        if not isinstance(payload, dict):
            continue
        cr = payload.get("crashes") if isinstance(payload.get("crashes"), dict) else {}
        for et in ("CRASH", "ANR"):
            total += _count_block_issue_rows(cr.get(et) if isinstance(cr.get(et), dict) else {})
    return total


def _version_label(code: str | None, name: str | None) -> str:
    code_s = str(code or "").strip()
    name_s = str(name or "").strip()
    if name_s and code_s and name_s != code_s:
        return f"{name_s} ({code_s})"
    return name_s or code_s


def _parse_version_pair(raw: str | None) -> tuple[str | None, str | None]:
    s = str(raw or "").strip()
    if not s:
        return None, None
    m = re.search(r"(\d+\.\d+(?:\.\d+)?)\s*\((\d+)\)", s)
    if m:
        return m.group(2), m.group(1)
    if re.fullmatch(r"\d+", s):
        return s, None
    return None, s


def _firebase_version_pairs(
    android: dict[str, Any],
    vitals: dict[str, Any] | None = None,
) -> list[tuple[str | None, str | None]]:
    pairs: list[tuple[str | None, str | None]] = []
    seen: set[str] = set()

    def _add(code: str | None, name: str | None) -> None:
        key = f"{code or ''}|{name or ''}"
        if key in seen or key == "|":
            return
        seen.add(key)
        pairs.append((code, name))

    for row in android.get("by_version") or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("build") or row.get("code") or "").strip() or None
        name = str(row.get("version") or row.get("label") or "").strip() or None
        if code or name:
            _add(code, name)

    code, name = _parse_version_pair(android.get("latest_version"))
    if code or name:
        _add(code, name)

    vitals = vitals if isinstance(vitals, dict) else {}
    vmap = vitals.get("version_name_map") if isinstance(vitals.get("version_name_map"), dict) else {}
    for v in vitals.get("versions") or []:
        if isinstance(v, dict) and v.get("code"):
            _add(str(v["code"]), str(v.get("name") or vmap.get(str(v["code"])) or "").strip() or None)
    if not pairs and vmap:
        for code in sorted(vmap.keys(), key=lambda x: int(x) if str(x).isdigit() else 0, reverse=True)[:3]:
            _add(str(code), str(vmap.get(code) or "") or None)
    return pairs


def _issue_matches_version(iss: dict[str, Any], code: str | None, name: str | None) -> bool:
    if not code and not name:
        return True
    hay = " ".join(
        str(iss.get(k) or "")
        for k in ("version", "app_version", "latest_version", "title", "affected_versions")
    ).lower()
    if not hay.strip():
        return True
    if code and (code in hay or f"({code})" in hay):
        return True
    if name and name.lower() in hay:
        return True
    return False


def _firebase_detail_url(android: dict[str, Any], issue_id: str) -> str:
    iid = str(issue_id or "").strip()
    if not iid:
        return ""
    pages = android.get("pages") if isinstance(android.get("pages"), dict) else {}
    base = str(pages.get("crashlytics") or "").split("?")[0].rstrip("/")
    if base:
        return f"{base}/{iid}"[:512]
    return ""


def _firebase_to_vitals_row(
    iss: dict[str, Any],
    *,
    etype: str,
    android: dict[str, Any],
    version_code: str | None = None,
    version_name: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(iss, dict):
        return None
    iid = str(iss.get("id") or iss.get("issue_id") or "").strip()
    title = str(iss.get("title") or iss.get("issue_title") or "").strip()
    if not title and not iid:
        return None
    if not title:
        title = f"Issue {iid[:12]}"
    ver = str(
        iss.get("version")
        or iss.get("app_version")
        or iss.get("latest_version")
        or version_name
        or android.get("latest_version")
        or ""
    ).strip()
    parsed_code, parsed_name = _parse_version_pair(ver)
    if not parsed_code and not version_code:
        fb_code, fb_name = _parse_version_pair(android.get("latest_version"))
        parsed_code = parsed_code or fb_code
        parsed_name = parsed_name or fb_name
    code = version_code or parsed_code
    name = version_name or parsed_name or (ver if not parsed_code else parsed_name)
    av = _version_label(code, name or ver)
    events_n = _parse_count(iss.get("event_count") or iss.get("events"))
    users_n = _parse_count(iss.get("affected_users") or iss.get("users"))
    eu = round(events_n / users_n, 1) if users_n > 0 else None
    mapped_et = "ANR" if etype == "ANR" else "FATAL"
    tags = [
        str(t)[:80]
        for t in (iss.get("badges") or iss.get("tags") or [])
        if str(t).strip()
    ][:6]
    subtitle = str(iss.get("exception") or iss.get("detail") or iss.get("subtitle") or "")[:240]
    detail_url = str(iss.get("url") or iss.get("detail_url") or "")[:512]
    if not detail_url:
        detail_url = _firebase_detail_url(android, iid)
    return {
        "issue_id": (iid or title[:64])[:80],
        "detail_url": detail_url,
        "title": title[:240],
        "subtitle": subtitle,
        "tags": tags,
        "issue_type": mapped_et,
        "error_type": mapped_et,
        "affected_versions": av[:80],
        "version_track": "",
        "users": str(users_n)[:32],
        "events": str(events_n)[:32],
        "events_per_user": eu,
        "events_share": "",
        "last_occurrence": str(iss.get("last_seen") or iss.get("last_occurrence") or "")[:64],
        "extra": "",
        "source": "firebase_console_scrape",
    }


def _sanitize_category_counts(cats: list[dict[str, Any]]) -> None:
    for cat in cats:
        if not isinstance(cat, dict):
            continue
        issues = cat.get("issues")
        n = len(issues) if isinstance(issues, list) else 0
        cat["issue_row_count"] = n
        cat["issue_count"] = str(n) if n else None


def _sanitize_vitals_blocks(vitals: dict[str, Any]) -> None:
    crashes = vitals.get("crashes") if isinstance(vitals.get("crashes"), dict) else {}
    for et in ("CRASH", "ANR"):
        block = crashes.get(et)
        if isinstance(block, dict) and isinstance(block.get("categories"), list):
            _sanitize_category_counts(block["categories"])
    byv = vitals.get("by_version") if isinstance(vitals.get("by_version"), dict) else {}
    for payload in byv.values():
        if not isinstance(payload, dict):
            continue
        cr = payload.get("crashes") if isinstance(payload.get("crashes"), dict) else {}
        for et in ("CRASH", "ANR"):
            block = cr.get(et)
            if isinstance(block, dict) and isinstance(block.get("categories"), list):
                _sanitize_category_counts(block["categories"])


def _ensure_categories(block: dict[str, Any]) -> list[dict[str, Any]]:
    cats = block.get("categories")
    if isinstance(cats, list) and cats:
        _sanitize_category_counts(cats)
        return cats
    out = []
    for tpl in _CATEGORY_TEMPLATES:
        out.append(
            {
                **tpl,
                "selected_ok": tpl["id"] == "general",
                "selected_label": "",
                "issue_count": None,
                "cards": [],
                "issues": [],
                "issue_row_count": 0,
            }
        )
    block["categories"] = out
    return out


def _inject_into_block(
    block: dict[str, Any],
    issues: list[dict[str, Any]],
    *,
    source: str,
) -> None:
    if not issues:
        return
    cats = _ensure_categories(block)
    general = next((c for c in cats if c.get("id") == "general"), cats[0])
    general["issues"] = issues[:50]
    general["issue_row_count"] = len(general["issues"])
    general["issue_count"] = str(len(general["issues"]))
    general["issues_source"] = source
    for cat in cats:
        if cat.get("id") == "general":
            continue
        if not isinstance(cat.get("issues"), list) or not cat["issues"]:
            cat["issues"] = []
            cat["issue_row_count"] = 0
            cat["issue_count"] = None


def _collect_firebase_issues(
    android: dict[str, Any],
    *,
    etype: str,
    version_code: str | None = None,
    version_name: str | None = None,
    allow_unfiltered_fallback: bool = True,
) -> list[dict[str, Any]]:
    anr_ids = {
        str(i.get("id") or i.get("issue_id") or "").strip()
        for i in (android.get("anr_issues") or [])
        if isinstance(i, dict)
    }
    source_lists: list[list[dict[str, Any]]] = []
    if etype == "ANR":
        source_lists.append(
            [i for i in (android.get("anr_issues") or []) if isinstance(i, dict)]
        )
    else:
        fatal: list[dict[str, Any]] = []
        for iss in android.get("issues") or []:
            if not isinstance(iss, dict):
                continue
            sid = str(iss.get("id") or iss.get("issue_id") or "").strip()
            if sid and sid in anr_ids:
                continue
            title_l = str(iss.get("title") or "").lower()
            et_hint = str(iss.get("error_type") or iss.get("page") or "").lower()
            if "anr" in et_hint or title_l.startswith("anr"):
                continue
            fatal.append(iss)
        source_lists.append(fatal)

    def _build(version_code_f: str | None, version_name_f: str | None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for lst in source_lists:
            for iss in lst:
                if not _issue_matches_version(iss, version_code_f, version_name_f):
                    continue
                row = _firebase_to_vitals_row(
                    iss,
                    etype=etype,
                    android=android,
                    version_code=version_code_f,
                    version_name=version_name_f,
                )
                if not row:
                    continue
                key = row.get("issue_id") or row.get("title") or ""
                if key in seen:
                    continue
                seen.add(str(key))
                out.append(row)
        out.sort(key=lambda r: -_parse_count(r.get("events")))
        return out[:50]

    filtered = _build(version_code, version_name)
    if filtered or not (version_code or version_name) or not allow_unfiltered_fallback:
        return filtered
    return _build(None, None)


def enrich_vitals_with_firebase(
    vitals: dict[str, Any] | None,
    android: dict[str, Any] | None,
) -> dict[str, Any]:
    """Play Vitals'ta gerçek satır yoksa Firebase Android scrape listesini bas."""
    if not isinstance(vitals, dict):
        return vitals or {}
    if not isinstance(android, dict) or android.get("empty"):
        return vitals

    out = copy.deepcopy(vitals)
    _sanitize_vitals_blocks(out)

    play_rows = count_vitals_issues(out)
    fatal_all = _collect_firebase_issues(android, etype="CRASH")
    anr_all = _collect_firebase_issues(android, etype="ANR")
    if not fatal_all and not anr_all:
        return out

    if play_rows > 0:
        return out

    crashes = out.setdefault("crashes", {})
    if fatal_all:
        _inject_into_block(
            crashes.setdefault("CRASH", {"error_type": "CRASH"}),
            fatal_all,
            source="firebase_console_scrape",
        )
    if anr_all:
        _inject_into_block(
            crashes.setdefault("ANR", {"error_type": "ANR"}),
            anr_all,
            source="firebase_console_scrape",
        )

    byv = out.setdefault("by_version", {})
    version_pairs = _firebase_version_pairs(android, out)
    for code, name in version_pairs:
        if not code:
            continue
        key = str(code)
        payload = byv.get(key)
        if not isinstance(payload, dict):
            payload = {"crashes": {}}
            byv[key] = payload
        cr = payload.setdefault("crashes", {})
        fat_v = _collect_firebase_issues(
            android, etype="CRASH", version_code=code, version_name=name
        )
        anr_v = _collect_firebase_issues(
            android, etype="ANR", version_code=code, version_name=name
        )
        if fat_v:
            _inject_into_block(
                cr.setdefault("CRASH", {"error_type": "CRASH", "version_code": key}),
                fat_v,
                source="firebase_console_scrape",
            )
        if anr_v:
            _inject_into_block(
                cr.setdefault("ANR", {"error_type": "ANR", "version_code": key}),
                anr_v,
                source="firebase_console_scrape",
            )

    pages = android.get("pages") if isinstance(android.get("pages"), dict) else {}
    if pages.get("crashlytics"):
        for et in ("CRASH", "ANR"):
            block = crashes.get(et)
            if isinstance(block, dict) and not block.get("url"):
                block["url"] = str(pages.get("crashlytics"))[:512]

    out["issues_primary_source"] = "firebase_console_scrape"
    out["issues_fallback"] = "firebase_console_scrape"
    out["issues_fallback_note"] = (
        "Play Console sorun tablosu boş veya okunamadı; Firebase Crashlytics listesi gösteriliyor."
    )
    logger.info(
        "vitals enriched from firebase: crash=%s anr=%s",
        len(fatal_all),
        len(anr_all),
    )
    return out


def enrich_panels_vitals_from_firebase(
    panels: dict[str, Any],
    android: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(panels, dict):
        return panels
    vitals = panels.get("vitals")
    if not isinstance(vitals, dict):
        return panels
    enriched = enrich_vitals_with_firebase(vitals, android)
    if enriched is not vitals:
        panels = {**panels, "vitals": enriched}
    return panels
