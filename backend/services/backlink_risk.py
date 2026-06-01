"""GSC backlink export satırları için spam / toksik link risk skoru."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

ACTION_IGNORE = "ignore"
ACTION_MONITOR = "monitor"
ACTION_REVIEW = "review"
ACTION_DISAVOW = "disavow"

_IP_HOST_RE = re.compile(
    r"^(?:\d{1,3}\.){3}\d{1,3}$|^(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}$",
    re.IGNORECASE,
)

# Bilinen TR / uluslararası haber & editoryal yayıncılar (suffix eşleşmesi)
_TR_MEDIA_SUFFIXES: tuple[str, ...] = (
    "hurriyet.com.tr",
    "haberturk.com",
    "ntv.com.tr",
    "cnn.com.tr",
    "cnnturk.com",
    "sozcu.com.tr",
    "sabah.com.tr",
    "milliyet.com.tr",
    "cumhuriyet.com.tr",
    "bbc.com",
    "bbc.co.uk",
    "reuters.com",
    "aa.com.tr",
    "anadoluajansi.com.tr",
    "dha.com.tr",
    "iha.com.tr",
    "ensonhaber.com",
    "haberler.com",
    "internethaber.com",
    "takvim.com.tr",
    "yenisafak.com",
    "star.com.tr",
    "posta.com.tr",
    "fanatik.com.tr",
    "sporx.com",
    "mynet.com",
    "eksisozluk.com",
    "wikipedia.org",
    "wikimedia.org",
)

_EDITORIAL_PATH_RE = re.compile(
    r"/(haber|haberler|news|gundem|gündem|ekonomi|spor|article|articles|story|"
    r"magazine|blog|yazar|column|dunya|dünya|teknoloji|saglik|sağlık|kultur|kültür)/",
    re.IGNORECASE,
)

_RISK_PATTERNS: list[tuple[str, int, str]] = [
    (r"\b(porn|porno|xxx|sex|nude|naked|hentai|escort|camgirl|onlyfans)\b", 35, "adult"),
    (r"\b(casino|betting|poker|\bslot\b|bahis|kumar)\b", 30, "gambling"),
    (r"\b(canl[ıi]\s*bahis|canli\s*bahis)\b", 30, "gambling"),
    (r"\b(viagra|cialis|pharma|pharmacy|pill|steroid)\b", 28, "pharma"),
    (r"\b(crack|keygen|serial|warez|nulled|torrent|pirate|1080p|brrip|bluray|x264)\b", 32, "warez"),
    (r"\b(hack|cheat|mod\s*apk|free\s*download\s*full)\b", 22, "warez"),
    (r"\b(loan|payday|forex|binary\s*option|crypto\s*signal)\b", 18, "spam_finance"),
    (r"\b(seo\s*service|buy\s*backlink|link\s*building\s*service)\b", 20, "link_spam"),
]

_COMPILED = [(re.compile(pat, re.IGNORECASE), pts, label) for pat, pts, label in _RISK_PATTERNS]


def domain_is_ip_host(domain_or_host: str) -> bool:
    d = (domain_or_host or "").strip().lower()
    return bool(_IP_HOST_RE.match(d))


def normalize_domain(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, re.I):
        raw = "http://" + raw
    try:
        host = (urlparse(raw).hostname or "").strip().lower()
    except Exception:  # noqa: BLE001
        return ""
    if not host:
        return ""
    if _IP_HOST_RE.match(host):
        return host
    if host.startswith("www."):
        host = host[4:]
    return host


def is_trusted_media_domain(domain: str) -> bool:
    d = (domain or "").strip().lower()
    if not d:
        return False
    for suffix in _TR_MEDIA_SUFFIXES:
        if d == suffix or d.endswith("." + suffix):
            return True
    return False


def _score_to_action(score: int) -> str:
    if score >= 70:
        return ACTION_DISAVOW
    if score >= 45:
        return ACTION_REVIEW
    if score >= 18:
        return ACTION_MONITOR
    return ACTION_IGNORE


def assess_linking_url(source_url: str, *, anchor_text: str = "", target_url: str = "") -> dict[str, Any]:
    """Tek bir «bağlantı verilen sayfa» satırı için risk."""
    url = (source_url or "").strip()
    blob = " ".join([url, anchor_text or "", target_url or ""]).lower()
    score = 0
    flags: list[str] = []
    domain = normalize_domain(url)

    if not url or not domain:
        return {
            "domain": domain,
            "risk_score": 0,
            "risk_flags": [],
            "recommended_action": ACTION_MONITOR,
        }

    trusted_media = is_trusted_media_domain(domain)
    editorial_path = bool(_EDITORIAL_PATH_RE.search(url))

    if _IP_HOST_RE.match(domain):
        score += 40
        flags.append("ip_host")

    for rx, pts, label in _COMPILED:
        if rx.search(blob):
            score += pts
            if label not in flags:
                flags.append(label)

    if not trusted_media and not editorial_path:
        if len(url) > 220:
            score += 6
            flags.append("long_url")
        if url.count("/") >= 10:
            score += 4
            flags.append("deep_path")
    elif editorial_path:
        flags.append("editorial_path")

    if trusted_media:
        score = min(score, 14)
        flags.append("trusted_media")
    elif editorial_path and score > 0:
        score = max(0, score - 8)

    score = min(100, score)
    action = _score_to_action(score)

    if trusted_media and score < 45 and "ip_host" not in flags:
        action = ACTION_IGNORE

    return {
        "domain": domain,
        "risk_score": score,
        "risk_flags": flags,
        "recommended_action": action,
    }


def finalize_domain_risk_summary(bucket: dict[str, Any]) -> None:
    """Domain satırı: link çoğunluğuna göre öneri; tek outlier max skoru yüzünden disavow olmasın."""
    total_links = int(bucket.get("link_count") or 0)
    if total_links <= 0:
        bucket["domain_category"] = "unknown"
        bucket["low_risk_pct"] = 0
        bucket["min_risk_score"] = 0
        return

    ac: dict[str, int] = bucket.get("action_counts") or {}
    low = int(bucket.get("low_risk_links") or 0)
    low_pct = round(100.0 * low / total_links, 1)
    bucket["low_risk_pct"] = low_pct
    min_risk = int(bucket.get("min_risk_score") or 0)
    if min_risk == 999:
        min_risk = 0
    bucket["min_risk_score"] = min_risk

    dom = (bucket.get("domain") or "").lower()
    max_score = int(bucket.get("max_risk_score") or 0)

    if is_trusted_media_domain(dom):
        bucket["domain_category"] = "media"
        if max_score < 70:
            bucket["recommended_action"] = ACTION_IGNORE
        return

    if low_pct >= 75 and max_score < 65:
        bucket["domain_category"] = "mostly_clean"
        bucket["recommended_action"] = ACTION_IGNORE
        return

    disavow_n = ac.get(ACTION_DISAVOW, 0)
    review_n = ac.get(ACTION_REVIEW, 0)
    if disavow_n / total_links >= 0.25 or (disavow_n + review_n) / total_links >= 0.5:
        bucket["domain_category"] = "spammy"
        return

    if low_pct >= 50 and max_score < 55:
        bucket["domain_category"] = "mixed"
        bucket["recommended_action"] = ACTION_MONITOR
        return

    bucket["domain_category"] = "mixed"
