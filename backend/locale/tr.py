"""tr-TR: e-posta ve işlem bildirimleri için ortak metin ve HTML ayarları."""

from datetime import date

# HTML e-posta kök öğesi
HTML_LANG = "tr"


def weekday_tr(d: date) -> str:
    """Python weekday(): Pazartesi=0 … Pazar=6."""
    names = ("Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar")
    return names[d.weekday()]

# GA4 haftalık özet e-postası (send_ga4_weekly_digest_emails / render_ga4_digest_email)
GA4_DIGEST_EYEBROW = "SEO Agent Operations"
GA4_DIGEST_STATUS_LABEL = "GA4 Özet"
GA4_DIGEST_META_TABLE_CAPTION = "Özet bilgi"
GA4_DIGEST_CRITICAL_SECTION_LABEL = "ÖNEMLİ"
GA4_DIGEST_CRITICAL_HIGHLIGHTS_TITLE = "Kritik vurgular"
GA4_DIGEST_SAME_WEEKDAY_TITLE = "Haftalık aynı gün (son gün vs önceki haftanın aynı günü)"
GA4_DIGEST_SAME_WEEKDAY_SUBTITLE = (
    "Son tam gün ile bir önceki haftanın aynı hafta günü (ör. çarşamba–çarşamba) GA4 KPI kıyası; Analytics’te tarih aralığını buna göre seçebilirsiniz."
)

# GA4 aynı gün (haftalık) KPI satır etiketleri
GA4_KPI_LABELS = {
    "sessions": "Oturumlar",
    "totalUsers": "Kullanıcılar",
    "newUsers": "Yeni kullanıcılar",
    "engagedSessions": "Etkileşimli oturumlar",
    "engagementRate": "Etkileşim oranı",
    "averageSessionDuration": "Ort. oturum süresi (sn)",
    "screenPageViews": "Sayfa görüntüleme",
}

# Aynı gün e-posta tablosunda gösterilecek KPI sırası
GA4_DIGEST_WOW_KPI_FIELDS = (
    "sessions",
    "totalUsers",
    "newUsers",
    "engagedSessions",
    "engagementRate",
    "screenPageViews",
    "averageSessionDuration",
)
