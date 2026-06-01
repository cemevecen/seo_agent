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

_RISK_PATTERNS: list[tuple[str, int, str]] = [
    (r"\b(porn|porno|xxx|sex|nude|naked|hentai|escort|camgirl|onlyfans)\b", 35, "adult"),
    (r"\b(casino|betting|poker|slot|bahis|kumar|canl[ıi]\s*bahis)\b", 30, "gambling"),
    (r"\b(viagra|cialis|pharma|pharmacy|pill|steroid)\b", 28, "pharma"),
    (r"\b(crack|keygen|serial|warez|nulled|torrent|pirate|1080p|brrip|bluray|x264)\b", 32, "warez"),
    (r"\b(hack|cheat|mod\s*apk|free\s*download\s*full)\b", 22, "warez"),
    (r"\b(loan|payday|forex|binary\s*option|crypto\s*signal)\b", 18, "spam_finance"),
    (r"\b(seo\s*service|buy\s*backlink|link\s*building\s*service)\b", 20, "link_spam"),
]

_COMPILED = [(re.compile(pat, re.IGNORECASE), pts, label) for pat, pts, label in _RISK_PATTERNS]


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

    if _IP_HOST_RE.match(domain):
        score += 40
        flags.append("ip_host")

    for rx, pts, label in _COMPILED:
        if rx.search(blob):
            score += pts
            if label not in flags:
                flags.append(label)

    if len(url) > 180:
        score += 8
        flags.append("long_url")

    if url.count("/") >= 8:
        score += 6
        flags.append("deep_path")

    score = min(100, score)

    if score >= 70:
        action = ACTION_DISAVOW
    elif score >= 45:
        action = ACTION_REVIEW
    elif score >= 18:
        action = ACTION_MONITOR
    else:
        action = ACTION_IGNORE

    return {
        "domain": domain,
        "risk_score": score,
        "risk_flags": flags,
        "recommended_action": action,
    }
