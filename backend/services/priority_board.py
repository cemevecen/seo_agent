"""Ana sayfa — Döviz / Sinemalar öncelik panosu (Open · Doing · Testing · Closed)."""

from __future__ import annotations

from typing import Any

# status: open | doing | testing | closed
# source: web | ios | android | cross (hangi GitLab sekmesiyle ilişkili)

PRIORITY_BOARD_COLUMNS: list[dict[str, str]] = [
    {"id": "open", "label": "Open", "hint": "Sırada / alınacak"},
    {"id": "doing", "label": "Doing", "hint": "Aktif iş"},
    {"id": "testing", "label": "Testing", "hint": "Doğrulama"},
    {"id": "closed", "label": "Closed", "hint": "Tamam / arşiv"},
]

_PRIORITY_BOARD: dict[str, dict[str, Any]] = {
    "doviz": {
        "id": "doviz",
        "label": "Döviz",
        "subtitle": "Web · iOS · Android",
        "accent": "sky",
        "items": [
            {
                "id": "dvz-open-1",
                "status": "open",
                "title": "Mobil CrUX LCP / INP iyileştirme backlog",
                "note": "Saha verisinde mobil yavaş metrikler; LCP ve INP için sayfa bazlı öncelik listesi çıkar.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-open-2",
                "status": "open",
                "title": "Notification click / CTR düşüş alarmları",
                "note": "7g vs önceki 7g click drop ve CTR medyan altı — eşik ve mail alıcılarını gözden geçir.",
                "source": "cross",
                "source_label": "Web+App",
            },
            {
                "id": "dvz-open-3",
                "status": "open",
                "title": "Crashlytics iOS top issue triage",
                "note": "Ana sayfa Crashlytics kartındaki en kritik iOS crash’leri sahiplen ve ticket’a bağla.",
                "source": "ios",
                "source_label": "iOS",
            },
            {
                "id": "dvz-open-4",
                "status": "open",
                "title": "Android release crash-free izleme",
                "note": "Yeni sürüm sonrası CF% ve top crash’leri 48 saat takip et.",
                "source": "android",
                "source_label": "Android",
            },
            {
                "id": "dvz-doing-1",
                "status": "doing",
                "title": "Notification 7g WoW ana sayfa izleme",
                "note": "Platform click/impression farkları + Top 5 gönderim tablosu canlıda takip ediliyor.",
                "source": "cross",
                "source_label": "Web+App",
            },
            {
                "id": "dvz-doing-2",
                "status": "doing",
                "title": "SC Top 50 pozisyon düşüş / yükseliş",
                "note": "7g kartındaki anlamlı hareketleri içerik / teknik aksiyona çevir.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-doing-3",
                "status": "doing",
                "title": "GA4 realtime alarm kalibrasyonu",
                "note": "Yanlış pozitifleri azalt; kritik trafik düşüşlerinin kaçırılmadığını doğrula.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-test-1",
                "status": "testing",
                "title": "CrUX stale auto-refresh (Data Explorer)",
                "note": "Nisan’da takılan serinin Temmuz ucuna gelmesini production’da doğrula; nightly job’ı kontrol et.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-test-2",
                "status": "testing",
                "title": "Notification Top 5 tablo sütunları",
                "note": "# · ID · İçerik · Toplam · Web · MWeb · Android · iOS · Gönderim — panel ile birebir mi bak.",
                "source": "cross",
                "source_label": "Web+App",
            },
            {
                "id": "dvz-closed-1",
                "status": "closed",
                "title": "Ana sayfa Kritik 404 kartları kaldırıldı",
                "note": "Dün · Kritik 404 URL container’ları home’dan çıkarıldı.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-closed-2",
                "status": "closed",
                "title": "Home Data Explorer CWV özeti kaldırıldı",
                "note": "Core Web Vitals özet kartları ana sayfadan alındı; detay /data-explorer’da.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "dvz-closed-3",
                "status": "closed",
                "title": "Notification home WoW kartı yayında",
                "note": "Döviz full-width 7g vs önceki 7g özet + donut + platform tablosu.",
                "source": "cross",
                "source_label": "Web+App",
            },
        ],
    },
    "sinemalar": {
        "id": "sinemalar",
        "label": "Sinemalar",
        "subtitle": "Web",
        "accent": "violet",
        "items": [
            {
                "id": "snm-open-1",
                "status": "open",
                "title": "SC Top 50 pozisyon düşüş aksiyonları",
                "note": "Film / seans sayfalarında anlamlı düşüşleri içerik ve dahili link ile ele al.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-open-2",
                "status": "open",
                "title": "SEO kritik hatalar (title / canonical / index)",
                "note": "Ana sayfa SEO · Kritik Hatalar kartındaki maddeleri sprint’e al.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-open-3",
                "status": "open",
                "title": "Mobil CWV (LCP / CLS) iyileştirme",
                "note": "CrUX saha verisine göre en kötü şablonları önceliklendir.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-doing-1",
                "status": "doing",
                "title": "Search Console 7g cihaz kırılımı",
                "note": "Web / MWeb click ve pozisyon farklarını haftalık gözden geçir.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-doing-2",
                "status": "doing",
                "title": "Pozisyon düşüş / yükseliş kartı takibi",
                "note": "Top 50 · 7g hareketlerini editöryel takvimle hizala.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-test-1",
                "status": "testing",
                "title": "Data Explorer CrUX güncelliği",
                "note": "Sinemalar CrUX son döneminin TSI’ye yakın olduğunu doğrula.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-test-2",
                "status": "testing",
                "title": "www / apex domain alias",
                "note": "Data Explorer ve site çözümlemesinin www.sinemalar.com ile tutarlı olduğunu kontrol et.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-closed-1",
                "status": "closed",
                "title": "Home CWV özet kartı kaldırıldı",
                "note": "Ana sayfa Data Explorer özeti Sinemalar için de kaldırıldı.",
                "source": "web",
                "source_label": "Web",
            },
            {
                "id": "snm-closed-2",
                "status": "closed",
                "title": "Home Kritik 404 kartı kaldırıldı",
                "note": "404 özet container’ı Sinemalar kolonundan çıkarıldı.",
                "source": "web",
                "source_label": "Web",
            },
        ],
    },
}


def get_priority_board_sections() -> list[dict[str, Any]]:
    """UI için section + column + item listesi."""
    sections: list[dict[str, Any]] = []
    for key in ("doviz", "sinemalar"):
        raw = _PRIORITY_BOARD[key]
        columns = []
        for col in PRIORITY_BOARD_COLUMNS:
            items = [it for it in raw["items"] if it.get("status") == col["id"]]
            columns.append(
                {
                    "id": col["id"],
                    "label": col["label"],
                    "hint": col["hint"],
                    "count": len(items),
                    "items": items,
                }
            )
        sections.append(
            {
                "id": raw["id"],
                "label": raw["label"],
                "subtitle": raw["subtitle"],
                "accent": raw["accent"],
                "columns": columns,
            }
        )
    return sections
