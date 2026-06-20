"""medya@nokta.com gelen kutusu — işbirliği / reklam sınıflandırması."""

from __future__ import annotations

import re

MEDYA_KIND_ISBIRLIGI = "isbirligi"
MEDYA_KIND_REKLAM = "reklam"
MEDYA_KIND_DIGER = "diger"

MEDYA_KIND_LABELS: dict[str, str] = {
    MEDYA_KIND_ISBIRLIGI: "İşbirliği",
    MEDYA_KIND_REKLAM: "Reklam",
    MEDYA_KIND_DIGER: "Diğer",
}

# Reklam / satış odaklı teklifler (medya@ kutusuna düşen reklam satış mailleri)
_MEDYA_REKLAM_MARKERS: tuple[str, ...] = (
    "reklam",
    "reklamveren",
    "advertis",
    "advertisement",
    "sponsorlu reklam",
    "banner reklam",
    "display reklam",
    "programmatic",
    "media buying",
    "medya satın",
    "tanıtım alanı",
    "reklam alanı",
    "reklam yerleşim",
    "ad slot",
    "ad placement",
    "cpm teklif",
    "cpc teklif",
    "google ads",
    "meta ads",
    "facebook ads",
    "yayın reklam",
    "satın alma teklifi",
    "rate card",
    "mediakit",
    "media kit",
    "reklam fiyat",
    "reklam ücret",
)

# İçerik / marka işbirliği, PR, influencer
_MEDYA_ISBIRLIGI_MARKERS: tuple[str, ...] = (
    "iş birliği",
    "işbirliği",
    "isbirligi",
    "is birligi",
    "collaboration",
    "collab teklif",
    "ortaklık teklifi",
    "ortak proje",
    "partnerlik",
    "influencer",
    "içerik işbirliği",
    "icerik isbirligi",
    "brand ambassador",
    "marka elçisi",
    "barter",
    "takas teklifi",
    "basın bülteni",
    "basin bulteni",
    "press release",
    "pr teklifi",
    "röportaj teklifi",
    "roportaj teklifi",
    "interview request",
    "guest post",
    "konuk yazar",
    "içerik önerisi",
    "icerik onerisi",
    "haber değeri",
    "sponsorlu içerik",
    "sponsorlu icerik",
)


def _medya_kind_haystack(*parts: str) -> str:
    text = " ".join(p for p in parts if (p or "").strip())
    text = text.lower().replace("\u2019", "'").replace("\u2018", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _score_markers(hay: str, markers: tuple[str, ...]) -> int:
    score = 0
    for marker in markers:
        if marker in hay:
            score += 2 if " " in marker else 1
    return score


def classify_medya_thread_kind(
    *,
    subject: str = "",
    snippet: str = "",
    body: str = "",
) -> str:
    """medya@ thread'ini işbirliği / reklam / diğer olarak sınıflandırır."""
    hay = _medya_kind_haystack(subject, snippet, body)
    if not hay:
        return MEDYA_KIND_DIGER

    reklam_score = _score_markers(hay, _MEDYA_REKLAM_MARKERS)
    isbirligi_score = _score_markers(hay, _MEDYA_ISBIRLIGI_MARKERS)

    if reklam_score > 0 and reklam_score >= isbirligi_score:
        return MEDYA_KIND_REKLAM
    if isbirligi_score > 0:
        return MEDYA_KIND_ISBIRLIGI
    return MEDYA_KIND_DIGER


def medya_kind_label(kind: str | None) -> str:
    return MEDYA_KIND_LABELS.get((kind or "").strip().lower(), MEDYA_KIND_LABELS[MEDYA_KIND_DIGER])
