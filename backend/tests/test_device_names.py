"""Cihaz model kodu → pazarlama adı.

GA4 `deviceModel` üretici kodu veriyor (SM-S938B / iPhone18,2); panelde bu
kodlar hiçbir şey anlatmıyor. Çeviri tablosu depoya gömülü, çalışma anında ağ
isteği yok.
"""

from __future__ import annotations

from pathlib import Path

from backend.services.device_names import (
    _IOS_MODELS,
    android_table_size,
    pretty_device_model,
)

ROOT = Path(__file__).resolve().parents[2]


def test_android_codes_become_marketing_names():
    assert pretty_device_model("SM-S938B", platform="android") == "Samsung Galaxy S25 Ultra"
    assert pretty_device_model("SM-S928B", platform="android") == "Samsung Galaxy S24 Ultra"
    # Marka adı zaten adın içindeyse tekrarlanmamalı ("Redmi Redmi Note 13 Pro")
    assert pretty_device_model("23117RA68G", platform="android") == "Redmi Note 13 Pro"


def test_ios_codes_become_marketing_names():
    assert pretty_device_model("iPhone14,5", platform="ios") == "iPhone 13"
    assert pretty_device_model("iPhone17,2", platform="ios") == "iPhone 16 Pro Max"


def test_unknown_codes_are_left_alone_never_guessed():
    """Uydurulmuş bir isim koddan daha kötüdür: yanlış cihaza bakılır."""
    assert pretty_device_model("SM-YOKBOYLE", platform="android") == "SM-YOKBOYLE"
    # iPhone 17 ailesi (iPhone18,x) bilerek eşlenmedi — alt numaralandırma
    # doğrulanamadı, kod gösteriliyor
    assert pretty_device_model("iPhone18,2", platform="ios") == "iPhone18,2"
    assert not any(k.startswith("iPhone18,") for k in _IOS_MODELS)


def test_ga4_buckets_are_not_translated():
    """(other) cihaz değil, GA4'ün kardinalite kovası."""
    for bucket in ("(other)", "(not set)", "(none)", ""):
        assert pretty_device_model(bucket, platform="android") == bucket


def test_platform_is_inferred_when_not_given():
    assert pretty_device_model("iPhone14,5") == "iPhone 13"
    assert pretty_device_model("SM-S938B") == "Samsung Galaxy S25 Ultra"


def test_table_is_bundled_and_loads():
    """Tablo depoda olmalı; indirilmesi gereken bir şey kalmamalı."""
    data = ROOT / "backend/data/android_device_names.json.gz"
    assert data.is_file(), "gömülü tablo yok"
    assert data.stat().st_size < 2_000_000, "tablo şişmiş"
    assert android_table_size() > 30_000


def test_lookup_does_no_network_at_runtime():
    src = (ROOT / "backend/services/device_names.py").read_text(encoding="utf-8")
    for forbidden in ("requests", "httpx", "urllib.request", "socket"):
        assert forbidden not in src, forbidden


def test_x_ga4_applies_the_lookup_with_the_right_platform():
    src = (ROOT / "backend/services/x_ga4.py").read_text(encoding="utf-8")
    body = src.split("def _breakdown_task(", 1)[1].split("\ndef ", 1)[0]
    assert 'if dim == "deviceModel":' in body
    assert "pretty_device_model(value, platform=profile)" in body
    # Ham kod payload'da kalmalı — teşhis ve eşleşme kontrolü için
    assert '"raw": r[dim]' in body
