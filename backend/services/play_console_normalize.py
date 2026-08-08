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


def normalize_panels(raw: dict[str, Any] | None) -> dict[str, Any]:
    d = dict(raw) if isinstance(raw, dict) else {}
    tpg = normalize_metrics(
        [
            {**x, "kind": "tpg"}
            for x in (d.get("tpg") or [])
            if isinstance(x, dict)
        ]
    )
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
            }
        )
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
    return {
        "version": 2,
        "tpg": tpg,
        "breakdowns": breakdowns,
        "sections": d.get("sections") if isinstance(d.get("sections"), list) else [],
        "series": series,
        "tpg_count": len(tpg),
        "breakdown_count": len(breakdowns),
        "series_count": len(series),
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
    if not panels_n.get("tpg") and not panels_n.get("breakdowns") and metrics_n:
        panels_n = normalize_panels(
            {
                "tpg": [m for m in metrics_n if m.get("kind") != "breakdown"],
                "breakdowns": [m for m in metrics_n if m.get("kind") == "breakdown"],
            }
        )
    return {
        "metrics": metrics_n,
        "panels": panels_n,
        "reviews": normalize_reviews(reviews),
        "rating_summary": normalize_rating_summary(rating_summary),
    }
