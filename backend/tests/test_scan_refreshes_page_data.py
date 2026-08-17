"""Tarama bitince sayfada taze veri görünsün.

Panel taramadan sonra sayfayı yeniler; ama sunucu okuma önbelleği düşmezse
kullanıcı 5-15 dk eski veriyi görür. Her ingest kendi sayfasının önbelleğini
düşürmek zorunda.
"""

from __future__ import annotations

import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_firebase_ingest_drops_crashlytics_read_caches():
    from backend.services import firebase_from_store_tabs as fst
    from backend.services import stability_free as sf
    from backend.services.firebase_console_store import invalidate_firebase_read_caches

    with fst._CACHE_LOCK:
        fst._CACHE["doviz:7"] = (time.time(), {"stale": True})
    with sf._STABILITY_CACHE_LOCK:
        sf._STABILITY_CACHE["doviz:android"] = (time.time(), {"stale": True})

    invalidate_firebase_read_caches()

    with fst._CACHE_LOCK:
        assert fst._CACHE == {}
    with sf._STABILITY_CACHE_LOCK:
        assert sf._STABILITY_CACHE == {}


def test_asc_ingest_drops_ios_read_caches():
    from backend.services import asc_metrics_warehouse as amw

    amw._bundle_cache["x"] = (time.time(), {"stale": True})
    amw._scrape_cache = (time.time(), [{"stale": True}], {})

    amw.invalidate_asc_metrics_cache()

    assert amw._bundle_cache == {}
    assert amw._scrape_cache is None


def test_ingest_paths_call_their_cache_invalidation():
    """Sözleşme: ingest fonksiyonları önbellek düşürmeyi çağırmalı."""
    fb = (ROOT / "backend/services/firebase_console_store.py").read_text(encoding="utf-8")
    body = fb.split("def ingest_firebase_console_payload", 1)[1]
    assert "invalidate_firebase_read_caches()" in body

    asc = (ROOT / "backend/services/asc_console_store.py").read_text(encoding="utf-8")
    asc_body = asc.split("def ingest_asc_console_payload", 1)[1]
    assert asc_body.count("_invalidate_asc_read_caches()") >= 2  # reviews-only + analytics

    play = (ROOT / "backend/services/play_console_store.py").read_text(encoding="utf-8")
    assert "invalidate_play_scrape_facts_cache()" in play

    targets = (ROOT / "backend/services/revenue_targets_sheet.py").read_text(encoding="utf-8")
    save_body = targets.split("def save_ingested_revenue_targets", 1)[1]
    assert "global _CACHE" in save_body


# Tarama verisini saklayan modüller: okuma önbelleği eklenirse ingest'te düşürülmeli.
# Aksi halde tarama biter, sayfa yenilenir, kullanıcı yine eski veriyi görür.
SCRAPE_STORE_MODULES = (
    "backend/services/firebase_console_store.py",
    "backend/services/asc_console_store.py",
    "backend/services/play_console_store.py",
    "backend/services/gsc_cwv_scrape_store.py",
    "backend/services/gsc_links_scrape_store.py",
    "backend/services/policy_csv.py",
    "backend/services/sinemalar_noads.py",
    "backend/services/sinemalar_moderation.py",
    "backend/services/seo_audit_store.py",
    "backend/services/market_sheets_sync.py",
    "backend/services/doviz_news_sheet.py",
    "backend/services/revenue_targets_sheet.py",
    "backend/services/notification_analytics_store.py",
)


def test_scrape_stores_persist_and_do_not_serve_stale_cache():
    """Her tarama deposu ya veriyi DB'ye yazar ve önbelleksizdir, ya da önbelleğini tazeler."""
    missing = []
    for rel in SCRAPE_STORE_MODULES:
        path = ROOT / rel
        assert path.is_file(), rel
        src = path.read_text(encoding="utf-8")
        assert ".commit()" in src, f"{rel}: veriyi kalıcı yazmıyor"
        # Okuma önbelleği = TTL + veri tutan bir kap. Yalnız TTL varsa bu bir
        # senkron kısıtıdır (ör. Sheet'i 5 dk'da birden sık çekme), staleness yaratmaz.
        has_ttl = "_CACHE_TTL" in src or "_TTL_S" in src or "_TTL_SEC" in src
        has_store = "_CACHE" in src or "_cache" in src
        if not (has_ttl and has_store):
            continue
        refreshes = (
            "invalidate" in src
            or "global _CACHE" in src
            or "set_doviz_news_rows_cache" in src
        )
        if not refreshes:
            missing.append(rel)
    assert missing == [], missing


def test_scan_finishes_with_a_page_reload():
    """Kuyruk bitince panel sayfayı yeniler — yeni veri ekrana gelsin."""
    js = (ROOT / "static/js/page_tarama.js").read_text(encoding="utf-8")
    assert "window.location.reload()" in js


def test_pages_do_not_trigger_mac_scrapes_behind_your_back():
    """Tarama tek noktadan: Update page kuyruğu.

    Sayfa JS'i doğrudan 127.0.0.1:18765/sync-* çağırırsa kuyruk, kota, worker
    yönlendirmesi ve ilerleme paneli devre dışı kalır; Android'de bu çağrı
    rastgele (%35) ve kullanıcıya görünmeden tetikleniyordu.
    """
    for rel in ("templates/android.html", "templates/ios.html"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "18765/sync" not in text, rel
        assert "Math.random() < 0.35" not in text, rel


# Kuyruk öncesinden kalan, artık hiçbir butona bağlı olmayan köprü çağrıları.
# Yeni dosya eklenmemeli; buradakiler temizlendikçe liste küçülmeli.
LEGACY_BRIDGE_TEMPLATES = {
    "templates/ad.html",  # syncMzVirgulViaBridge — mz-refresh-sheets butonu kaldırılmış
    "templates/doviz_news.html",  # syncNewsViaVpnBridge — force parametresi hiçbir yerden gelmiyor
    "templates/notification.html",  # syncViaVpnBridge — aynı şekilde
    "templates/partials/policy_content.html",  # refreshNoAdsMatch — çağıran yok
    "templates/web_vitals.html",  # runWebVitals — wv-run butonu kaldırılmış
    "templates/seo_audit.html",  # sunucu "bridge_required" derse bilinçli fallback (canlı)
}


def test_update_page_queue_is_the_only_new_scan_entry_point():
    """Yeni sayfalar doğrudan Mac köprüsünü çağırmasın; tarama Update page kuyruğundan geçsin."""
    offenders = []
    for path in sorted((ROOT / "templates").rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        if "18765/sync" not in text:
            continue
        rel = str(path.relative_to(ROOT))
        if rel in LEGACY_BRIDGE_TEMPLATES:
            continue
        offenders.append(rel)
    assert offenders == [], offenders


def test_legacy_bridge_calls_are_not_wired_to_any_button():
    """Ölü köprü yolları yeniden bir butona bağlanırsa kuyruk devre dışı kalır — yakala."""
    removed_button_ids = (
        "mz-refresh-sheets",
        "policy-noads-refresh-btn",
        "wv-run",
        "pc-refresh",
        "ia-refresh",
    )
    for rel in sorted(LEGACY_BRIDGE_TEMPLATES):
        text = (ROOT / rel).read_text(encoding="utf-8")
        for bid in removed_button_ids:
            assert f'id="{bid}"' not in text, f"{rel} → #{bid} geri gelmiş"


def test_no_second_refresh_button_competes_with_update_page():
    """Sayfa başına tek tetik: Update page. Eski «Refresh» düğmeleri geri gelmesin."""
    for rel in (
        "templates/partials/app_content.html",
        "templates/partials/firebase_content.html",
        "templates/android.html",
        "templates/ios.html",
    ):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert 'id="crash-refresh-btn"' not in text, rel
        assert "/api/app/crashlytics/refresh" not in text, rel
