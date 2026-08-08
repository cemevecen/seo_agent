"""Play Console scrape DOM gürültüsünü temizle (Material icon / sidebar)."""

from __future__ import annotations

import re
from typing import Any

_ICON_NAMES = {
    "arrow_left_alt",
    "arrow_right_alt",
    "arrow_drop_down",
    "calendar_today",
    "schedule",
    "data_usage",
    "devices",
    "star",
    "thumb_up",
    "thumb_down",
    "expand_more",
    "expand_less",
    "feature_search",
    "visibility_off",
    "more_vert",
    "dashboard",
    "vital_signs",
    "bar_chart",
    "overview",
    "shield",
    "rocket_launch",
    "finance_mode",
    "sell",
    "flag",
    "link",
    "youtube_live",
    "event_upcoming",
    "brightness_1",
}

_METRIC_TITLE_HINTS = (
    "toplam yükleme",
    "kilitlenme",
    "anr",
    "ortalama puan",
    "cihaz edinme",
    "cihaz ilk",
    "aeks",
    "gelir",
    "alıcı",
    "yükleme tabanı",
    "kullanıcı kaybı",
    "etkin cihaz",
    "kitle",
    "günlük etkin",
    "mağaza girişi",
    "google play puanı",
    "öykbog",
    "üretim sürümü",
    "yeni cihaz",
    "yüklemeler",
    "abonelik",
    "toplam gelir",
    "alıcı oranı",
    "tek seferlik",
    "kilitlenme sayısı",
    "anr sayısı",
    "dau",
    "mau",
    "üretim",
    "rollout",
    "sürüm",
)

_JUNK_AUTHOR = re.compile(
    r"^(cihaz:|star|thumb_|dashboard|vital_|expand_|feature_|brightness_|arrow_|calendar_|youtube_|event_|"
    r"erişim ve cihazlar|tüm zamanlar|kontol paneli|kontrol paneli)",
    re.I,
)


def _is_iconish(s: str) -> bool:
    raw = (s or "").strip()
    if not raw:
        return True
    # İnsan adı / cümle (boşluklu) icon değildir
    if " " in raw or any(ch in raw for ch in "çğıöşüÇĞİÖŞÜ"):
        return False
    t = raw.lower()
    if t in _ICON_NAMES:
        return True
    # Tek kelime snake_case Material ikon
    if re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+", t) and len(t) < 40:
        if not re.search(r"\d", t):
            return True
    return False


def _clean_lines(lines: list[str] | None) -> list[str]:
    out: list[str] = []
    for ln in lines or []:
        s = str(ln or "").strip()
        if not s or _is_iconish(s):
            continue
        if s.lower() in (
            "artışın iyi olduğu delta",
            "grafik alan değerlerinde gezinmek için sol ve sağ tuşları kullanın.",
        ):
            continue
        out.append(s)
    return out


def _looks_like_metric_title(title: str) -> bool:
    t = (title or "").strip()
    if len(t) < 3 or _is_iconish(t):
        return False
    # Saf tarih / sayı başlık olmasın
    if re.match(
        r"^\d{1,2}\s*(Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara)\b",
        t,
        re.I,
    ):
        return False
    if re.fullmatch(r"[%\d\s.,+\-−₺BmMkK]+", t):
        return False
    tl = t.lower()
    return any(h in tl for h in _METRIC_TITLE_HINTS) or (
        any(c.isalpha() for c in t) and len(t) >= 4 and not t.startswith("%")
    )


def normalize_metrics(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows = raw if isinstance(raw, list) else []
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in rows:
        if not isinstance(m, dict):
            continue
        lines = _clean_lines(m.get("lines") if isinstance(m.get("lines"), list) else None)
        title = str(m.get("title") or "").strip()
        value = str(m.get("value") or "").strip()
        delta = str(m.get("delta") or "").strip()
        if lines:
            # lines[0] title; value = ilk sayısal satır; delta = %/+/- satır
            title = lines[0]
            value = next((x for x in lines[1:] if re.search(r"\d", x) and not _is_iconish(x)), value)
            delta = next(
                (
                    x
                    for x in lines[1:]
                    if x != value and (x.startswith(("+", "-", "−", "%")) or "yüzde" in x.lower() or re.match(r"^[+\-−]?%", x))
                ),
                delta if not _is_iconish(delta) else "",
            )
        if _is_iconish(title) or not _looks_like_metric_title(title):
            continue
        if title.lower() == (value or "").lower():
            # "Kilitlenme oranı" / "Kilitlenme oranı" tekrarı — sonraki satırdan value al
            if lines and len(lines) >= 2:
                value = next((x for x in lines[1:] if x.lower() != title.lower() and re.search(r"\d", x)), value)
        if _is_iconish(value):
            value = next((x for x in lines[1:] if not _is_iconish(x) and re.search(r"\d", x)), "")
        if _is_iconish(delta):
            delta = ""
        if not value or not re.search(r"\d", value):
            continue
        key = f"{title.lower()}|{str(m.get('kind') or '')}|{str(m.get('segment') or '')}|{value}"
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(
            {
                "title": title,
                "value": value,
                "delta": delta,
                "kind": str(m.get("kind") or "").strip() or None,
                "segment": str(m.get("segment") or "").strip() or None,
                "metric": str(m.get("metric") or "").strip() or None,
                "period": str(m.get("period") or "").strip() or None,
                "lines": lines[:6] if lines else [title, value] + ([delta] if delta else []),
            }
        )
    # None alanları temizle
    for row in cleaned:
        for k in ("kind", "segment", "metric", "period"):
            if not row.get(k):
                row.pop(k, None)
    return cleaned


def _norm_kind_list(raw_list: Any, kind: str) -> list[dict[str, Any]]:
    return normalize_metrics(
        [{**x, "kind": kind} for x in (raw_list or []) if isinstance(x, dict)]
    )


def _normalize_vitals(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Vitals crashes (4 sorun kategorisi) + metrics overview tablosu."""
    d = dict(raw) if isinstance(raw, dict) else {}
    crashes_in = d.get("crashes") if isinstance(d.get("crashes"), dict) else {}
    crashes_out: dict[str, Any] = {}
    category_count = 0
    for et in ("CRASH", "ANR"):
        block = crashes_in.get(et) if isinstance(crashes_in.get(et), dict) else {}
        cats_out: list[dict[str, Any]] = []
        for cat in block.get("categories") or []:
            if not isinstance(cat, dict):
                continue
            cid = str(cat.get("id") or "").strip()
            label = str(cat.get("label") or "").strip()
            if not cid and not label:
                continue
            issues = []
            for iss in cat.get("issues") or []:
                if not isinstance(iss, dict):
                    continue
                title = str(iss.get("title") or "").strip()
                if not title:
                    continue
                issues.append(
                    {
                        "title": title[:240],
                        "users": str(iss.get("users") or "")[:64],
                        "events": str(iss.get("events") or "")[:64],
                        "extra": str(iss.get("extra") or "")[:120],
                    }
                )
            cards = []
            for c in cat.get("cards") or []:
                if not isinstance(c, dict):
                    continue
                t = str(c.get("title") or "").strip()
                v = str(c.get("value") or "").strip()
                if t and v:
                    cards.append(
                        {
                            "title": t[:160],
                            "value": v[:64],
                            "delta": str(c.get("delta") or "")[:64],
                        }
                    )
            cats_out.append(
                {
                    "id": cid or label.lower().replace(" ", "_")[:40],
                    "label": label or cid,
                    "description": str(cat.get("description") or "")[:240],
                    "selected_ok": bool(cat.get("selected_ok")),
                    "selected_label": str(cat.get("selected_label") or "")[:80],
                    "issue_count": str(cat.get("issue_count") or "")[:32] or None,
                    "cards": cards[:8],
                    "issues": issues[:20],
                    "issue_row_count": len(issues),
                }
            )
        category_count += len(cats_out)
        crashes_out[et] = {
            "error_type": et,
            "url": str(block.get("url") or "")[:512],
            "days": int(block.get("days") or 28),
            "is_user_perceived": bool(block.get("is_user_perceived", True)),
            "categories": cats_out,
            "category_count": len(cats_out),
        }

    ov_in = d.get("metrics_overview") if isinstance(d.get("metrics_overview"), dict) else {}
    rows_out: list[dict[str, Any]] = []
    for row in ov_in.get("rows") or []:
        if not isinstance(row, dict):
            continue
        metric = str(row.get("metric") or "").strip()
        if not metric:
            continue
        key = str(row.get("key") or "").strip().lower()
        if key not in ("crash", "anr", "lmk", "other"):
            if re.search(r"kilitlenme|crash", metric, re.I):
                key = "crash"
            elif re.search(r"\banr\b", metric, re.I):
                key = "anr"
            elif re.search(r"\blmk\b", metric, re.I):
                key = "lmk"
            else:
                key = "other"
        rows_out.append(
            {
                "key": key,
                "metric": metric[:200],
                "value_28d": str(row.get("value_28d") or "")[:48],
                "vs_previous_28d": str(row.get("vs_previous_28d") or "")[:48],
                "vs_peers_median": str(row.get("vs_peers_median") or "")[:48],
            }
        )

    anr_drill = ov_in.get("anr_drilldown") if isinstance(ov_in.get("anr_drilldown"), dict) else {}
    return {
        "version": int(d.get("version") or 1),
        "days": int(d.get("days") or 28),
        "is_user_perceived": bool(d.get("is_user_perceived", True)),
        "scraped_at": str(d.get("scraped_at") or "")[:40] or None,
        "error": str(d.get("error") or "")[:240] or None,
        "crashes": crashes_out,
        "metrics_overview": {
            "url": str(ov_in.get("url") or "")[:512],
            "rows": rows_out,
            "row_count": len(rows_out),
            "anr_drilldown": {
                "url": str(anr_drill.get("url") or "")[:512],
                "error": str(anr_drill.get("error") or "")[:160] or None,
            }
            if anr_drill
            else {},
        },
        "category_count": category_count,
        "overview_row_count": len(rows_out),
    }


def normalize_panels(raw: dict[str, Any] | None) -> dict[str, Any]:
    d = dict(raw) if isinstance(raw, dict) else {}
    pages_in = d.get("pages") if isinstance(d.get("pages"), dict) else {}

    tpg = _norm_kind_list(d.get("tpg"), "tpg")
    monetize = _norm_kind_list(d.get("monetize"), "monetize")
    grow = _norm_kind_list(d.get("grow"), "grow")
    monitor = _norm_kind_list(d.get("monitor"), "monitor")
    release = _norm_kind_list(d.get("release"), "release")
    statistics = _norm_kind_list(d.get("statistics"), "statistics")

    for key, bucket, kind in (
        ("monetize", "monetize", "monetize"),
        ("grow", "grow", "grow"),
        ("monitor", "monitor", "monitor"),
        ("release", "release", "release"),
        ("statistics", "statistics", "statistics"),
    ):
        cur = {
            "monetize": monetize,
            "grow": grow,
            "monitor": monitor,
            "release": release,
            "statistics": statistics,
        }[bucket]
        if not cur and isinstance(pages_in.get(key), dict):
            filled = _norm_kind_list(pages_in[key].get("cards"), kind)
            if bucket == "monetize":
                monetize = filled
            elif bucket == "grow":
                grow = filled
            elif bucket == "monitor":
                monitor = filled
            elif bucket == "release":
                release = filled
            else:
                statistics = filled
    # visitors URL cards → statistics'e ekle
    if isinstance(pages_in.get("statistics_visitors"), dict):
        extra = _norm_kind_list(pages_in["statistics_visitors"].get("cards"), "statistics")
        if extra:
            seen = {(x.get("title"), x.get("value")) for x in statistics}
            for x in extra:
                if (x.get("title"), x.get("value")) not in seen:
                    statistics.append(x)

    breakdowns = []
    for x in d.get("breakdowns") or []:
        if not isinstance(x, dict):
            continue
        title = str(x.get("title") or "").strip()
        value = str(x.get("value") or "").strip()
        if not title or not re.search(r"\d", value or ""):
            continue
        breakdowns.append(
            {
                "title": title,
                "value": value,
                "delta": str(x.get("delta") or "").strip(),
                "segment": str(x.get("segment") or "").strip(),
                "metric": str(x.get("metric") or "").strip() or title.split("(")[0].strip(),
                "kind": "breakdown",
                "page": str(x.get("page") or "").strip() or None,
            }
        )
    for row in breakdowns:
        if not row.get("page"):
            row.pop("page", None)
    series = []
    for s in d.get("series") or []:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name") or "").strip()
        if not name:
            continue
        series.append(
            {
                "name": name[:120],
                "point_count": int(s.get("point_count") or 0),
                "points": (s.get("points") or [])[:40],
            }
        )
        if len(series) >= 40:
            break
    pages_out: dict[str, Any] = {}
    for pk, pv in pages_in.items():
        if not isinstance(pv, dict):
            continue
        pages_out[str(pk)] = {
            "url": str(pv.get("url") or "")[:512],
            "cards": normalize_metrics(pv.get("cards") if isinstance(pv.get("cards"), list) else []),
            "breakdowns": [
                {
                    "title": str(x.get("title") or ""),
                    "value": str(x.get("value") or ""),
                    "delta": str(x.get("delta") or ""),
                    "segment": str(x.get("segment") or ""),
                    "kind": "breakdown",
                }
                for x in (pv.get("breakdowns") or [])
                if isinstance(x, dict) and re.search(r"\d", str(x.get("value") or ""))
            ],
        }
    vitals_in = d.get("vitals") if isinstance(d.get("vitals"), dict) else {}
    vitals = _normalize_vitals(vitals_in)

    return {
        "version": 2,
        "tpg": tpg,
        "monetize": monetize,
        "grow": grow,
        "monitor": monitor,
        "release": release,
        "statistics": statistics,
        "breakdowns": breakdowns,
        "vitals": vitals,
        "pages": pages_out,
        "sections": d.get("sections") if isinstance(d.get("sections"), list) else [],
        "series": series,
        "explorer_facts": [
            x
            for x in (d.get("explorer_facts") or [])
            if isinstance(x, dict) and x.get("metric") is not None
        ][:50000],
        "stats_views": [
            x for x in (d.get("stats_views") or []) if isinstance(x, dict)
        ][:40],
        "tpg_count": len(tpg),
        "monetize_count": len(monetize),
        "grow_count": len(grow),
        "monitor_count": len(monitor),
        "release_count": len(release),
        "statistics_count": len(statistics),
        "breakdown_count": len(breakdowns),
        "series_count": len(series),
        "explorer_fact_count": len(
            [x for x in (d.get("explorer_facts") or []) if isinstance(x, dict)]
        ),
        "stats_view_count": len(
            [x for x in (d.get("stats_views") or []) if isinstance(x, dict)]
        ),
        "vitals_category_count": int(vitals.get("category_count") or 0),
        "vitals_overview_row_count": int(vitals.get("overview_row_count") or 0),
    }


def _strip_review_noise(text: str) -> str:
    t = text or ""
    for junk in (
        "thumb_up",
        "thumb_down",
        "visibility_off",
        "feature_search",
        "flag",
        "link",
        "star",
        "Yanıtla",
        "Yanıtı düzenle",
        "expand_more",
        "expand_less",
        "brightness_1",
        "Göster",
        "Geçmiş",
    ):
        t = re.sub(rf"\b{re.escape(junk)}\b", " ", t, flags=re.I)
    t = re.sub(r"\b\d+\s*/\s*350\b", " ", t)
    t = re.sub(r"Metin\s+\d+\s+karakter.*", " ", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def _parse_author_date(author_raw: str) -> tuple[str, str]:
    s = (author_raw or "").strip()
    # "Yunus Leblebici7 Ağu 2026, 18:08" → split
    m = re.search(
        r"^(.*?)(\d{1,2}\s*(?:Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara)\s*\d{4}.*)$",
        s,
        re.I,
    )
    if m:
        return m.group(1).strip() or s, m.group(2).strip()
    return s, ""


def normalize_reviews(raw: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows = raw if isinstance(raw, list) else []
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        author_raw = str(r.get("author") or "").strip()
        if not author_raw or _JUNK_AUTHOR.search(author_raw) or _is_iconish(author_raw):
            continue
        if author_raw.lower().startswith("cihaz:"):
            continue
        author, date = _parse_author_date(author_raw)
        if _is_iconish(author) or len(author) < 2:
            continue
        body = _strip_review_noise(str(r.get("body") or r.get("raw") or ""))
        # cihaz satırlarını ayır
        device = ""
        dm = re.search(r"Cihaz:\s*([^\n]+)", str(r.get("raw") or r.get("body") or ""), re.I)
        if dm:
            device = dm.group(1).strip()[:120]
        # asıl yorum metni: "Yanıtla" öncesi / meta sonrası
        raw_full = str(r.get("raw") or "")
        # yıldız satırından sonraki paragraf
        parts = [p.strip() for p in raw_full.split("\n") if p.strip() and not _is_iconish(p.strip())]
        candidate_lines = []
        for p in parts:
            if re.match(r"^Cihaz:", p, re.I):
                continue
            if re.match(r"^(Cihazın dili|Uygulama sürüm|Android sürümü):", p, re.I):
                continue
            if re.match(r"^\d+\s*/\s*350", p):
                continue
            if p in ("Yanıtla", "Güncellendi") or p.startswith("thumb_"):
                continue
            if re.match(r"^\d{1,2}\s*(Oca|Şub|Mar|Nis|May|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara)", p, re.I):
                continue
            if author and p.startswith(author[: min(8, len(author))]):
                continue
            candidate_lines.append(p)
        # en uzun anlamlı satır = body
        text_lines = [x for x in candidate_lines if len(x) > 20 and not x.startswith("Merhaba")]
        if text_lines:
            body = max(text_lines, key=len)[:800]
        else:
            body = body[:800]
        body = _strip_review_noise(body)
        if len(body) < 12:
            continue
        stars = r.get("stars")
        if not stars:
            sm = re.search(r"([1-5])\s*yıldız", str(r.get("raw") or ""), re.I)
            if sm:
                stars = f"{sm.group(1)} yıldız"
            elif (str(r.get("raw") or "").count("star") >= 5) or ("★" * 3 in str(r.get("raw") or "")):
                stars = "5 yıldız"
        key = (author.lower() + "|" + body[:60].lower())
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(
            {
                "author": author[:80],
                "date": date,
                "device": device,
                "body": body,
                "stars": stars,
            }
        )
    return cleaned


def normalize_rating_summary(raw: dict[str, Any] | None) -> dict[str, Any]:
    d = dict(raw) if isinstance(raw, dict) else {}
    def _num(s: Any) -> str:
        t = str(s or "").strip().split("\n")[0].strip()
        t = re.sub(r"[^\d,.\s]", "", t).strip()
        return t
    rating = _num(d.get("default_rating"))
    users = _num(d.get("users"))
    with_rev = _num(d.get("ratings_with_reviews"))
    # "Kullanıcılar: 4" yanlış parse — tek haneliyse ve with_rev büyükse boşalt
    try:
        if users and with_rev and float(users.replace(".", "").replace(",", ".").split()[0]) < 20:
            if float(re.sub(r"[^\d]", "", with_rev) or "0") > 100:
                users = with_rev  # sık hata: yanlış alan; UI'da yorum içereni kullanıcılara yazma
                # aslında users alanı bozuk — boş bırak
                users = ""
    except Exception:
        pass
    return {
        "default_rating": rating or d.get("default_rating") or "—",
        "users": users or "—",
        "ratings_with_reviews": with_rev or "—",
    }


def normalize_play_snapshot(
    *,
    metrics: list[dict[str, Any]] | None = None,
    reviews: list[dict[str, Any]] | None = None,
    rating_summary: dict[str, Any] | None = None,
    panels: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metrics_n = normalize_metrics(metrics)
    panels_n = normalize_panels(panels)
    # panels boşsa metrics'ten türet
    special = ("breakdown", "monetize", "grow", "monitor", "release", "statistics")
    if (
        not panels_n.get("tpg")
        and not panels_n.get("breakdowns")
        and not panels_n.get("monetize")
        and not panels_n.get("grow")
        and not panels_n.get("monitor")
        and not panels_n.get("release")
        and not panels_n.get("statistics")
        and metrics_n
    ):
        panels_n = normalize_panels(
            {
                "tpg": [m for m in metrics_n if m.get("kind") not in special],
                "monetize": [m for m in metrics_n if m.get("kind") == "monetize"],
                "grow": [m for m in metrics_n if m.get("kind") == "grow"],
                "monitor": [m for m in metrics_n if m.get("kind") == "monitor"],
                "release": [m for m in metrics_n if m.get("kind") == "release"],
                "statistics": [m for m in metrics_n if m.get("kind") == "statistics"],
                "breakdowns": [m for m in metrics_n if m.get("kind") == "breakdown"],
            }
        )
    elif metrics_n:
        for kind in ("monetize", "grow", "monitor", "release", "statistics"):
            if not panels_n.get(kind):
                rows = [m for m in metrics_n if m.get("kind") == kind]
                if rows:
                    panels_n[kind] = rows
                    panels_n[f"{kind}_count"] = len(rows)
    return {
        "metrics": metrics_n,
        "panels": panels_n,
        "reviews": normalize_reviews(reviews),
        "rating_summary": normalize_rating_summary(rating_summary),
    }
