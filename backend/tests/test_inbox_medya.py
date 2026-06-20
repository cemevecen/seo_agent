"""medya@ sınıflandırma birim testleri."""

from backend.services.inbox_medya import (
    MEDYA_KIND_ISBIRLIGI,
    MEDYA_KIND_REKLAM,
    classify_medya_thread_kind,
)


def test_medya_kind_reklam_from_subject():
    kind = classify_medya_thread_kind(
        subject="Banner reklam alanı teklifi — doviz.com",
        snippet="CPM teklifimiz ektedir",
    )
    assert kind == MEDYA_KIND_REKLAM


def test_medya_kind_isbirligi_from_subject():
    kind = classify_medya_thread_kind(
        subject="İçerik işbirliği teklifi",
        snippet="Influencer kampanyası için görüşmek isteriz",
    )
    assert kind == MEDYA_KIND_ISBIRLIGI


def test_medya_kind_prefers_reklam_when_both_markers():
    kind = classify_medya_thread_kind(
        subject="Reklam ve sponsorlu içerik teklifi",
        snippet="Display reklam + işbirliği paketi",
    )
    assert kind == MEDYA_KIND_REKLAM
