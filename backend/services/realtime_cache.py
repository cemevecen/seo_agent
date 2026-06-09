"""GA4 Realtime için bellek-içi TTL cache + son-iyi (last-good) yedeği.

GA4 Realtime Data API'nin "tokens per project per hour" kotası, çok sayıda
site×profil için sık polling (KPI + sayfa/haber/olay listeleri, birden çok
açık sekme veya prod istemci) yapıldığında tükeniyor. Kota dolunca API 429
döndürüyor ve realtime sayfası boş / eski snapshot'a düşüyor.

Bu modül iki sorunu birden çözer:

1. **Çağrı azaltma:** Aynı anahtar için TTL süresi boyunca tek upstream çağrı
   yapılır; aynı pencerede gelen tekrarlı/çok-istemcili istekler cache'ten
   beslenir. Böylece saatlik token kotası korunur.
2. **Son-iyi yedeği:** Upstream çağrı hata verirse (ör. 429), son BAŞARILI canlı
   sonuç `stale` işaretiyle döndürülür; böylece KPI değişimleri ve link'ler
   ekranda kalmaya devam eder.

Süreç içi (in-process), thread-safe ve harici bağımlılıksızdır.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable

_LOCK = threading.RLock()
# anahtar -> (kaydedilme zamanı, değer, errored)
_FRESH: dict[str, tuple[float, Any, bool]] = {}
# anahtar -> (kaydedilme zamanı, değer) — yalnızca BAŞARILI canlı sonuçlar
_LAST_GOOD: dict[str, tuple[float, Any]] = {}

DEFAULT_LAST_GOOD_TTL = 1800.0  # 30 dk — son-iyi sonucun gösterileceği azami yaş


def _flag(value: Any, *, cached: bool, stale: bool) -> Any:
    """dict sonuçlara cache durum bayraklarını ekler (kopya üzerinde)."""
    if isinstance(value, dict):
        out = dict(value)
        out["cached"] = cached
        if stale:
            out["stale"] = True
        return out
    return value


def get_cached_only(
    key: str,
    ttl: float,
    *,
    last_good_ttl: float = DEFAULT_LAST_GOOD_TTL,
) -> Any | None:
    """TTL veya last-good içindeyse değeri döndürür; aksi halde None (producer çağırmaz)."""
    now = time.time()
    with _LOCK:
        fresh = _FRESH.get(key)
        if fresh and (now - fresh[0]) < ttl and not fresh[2]:
            return _flag(fresh[1], cached=True, stale=False)
        lg = _LAST_GOOD.get(key)
        if lg is not None and (now - lg[0]) < last_good_ttl:
            return _flag(lg[1], cached=True, stale=True)
    return None


def get_or_call(
    key: str,
    ttl: float,
    producer: Callable[[], Any],
    *,
    is_error: Callable[[Any], bool],
    last_good_ttl: float = DEFAULT_LAST_GOOD_TTL,
) -> Any:
    """`key` için TTL'li cache.

    - TTL içinde taze ve hatasız değer varsa onu döndürür (`cached=True`).
    - TTL içinde taze ama HATALI bir sonuç varsa, GA4'e tekrar gidilmez; bunun
      yerine `last_good_ttl` içindeki son başarılı CANLI sonuç (`stale=True`)
      döndürülür. Bu, 429 (kota) sırasında her poll'de yeniden API çağrısı
      yapılmasını engeller.
    - TTL dolduysa `producer()` çağrılır. Sonuç hatasızsa cache + son-iyi olarak
      saklanır; hatalıysa son-iyi yedeği (`stale=True`) döndürülür, o da yoksa
      ham hata sonucu döner.

    producer ağ çağrısı içerdiği için kilit DIŞINDA çalıştırılır.
    """
    now = time.time()
    with _LOCK:
        fresh = _FRESH.get(key)
        if fresh and (now - fresh[0]) < ttl:
            ts, value, errored = fresh
            if not errored:
                return _flag(value, cached=True, stale=False)
            lg = _LAST_GOOD.get(key)
            if lg is not None and (now - lg[0]) < last_good_ttl:
                return _flag(lg[1], cached=True, stale=True)
            return _flag(value, cached=True, stale=True)

    try:
        value = producer()
        errored = value is None or bool(is_error(value))
    except Exception:  # noqa: BLE001 — her tür upstream hatasını son-iyi'ye düş
        value = None
        errored = True

    now = time.time()
    with _LOCK:
        # Hata da olsa TTL boyunca tekrar çağırma (kota koruması).
        _FRESH[key] = (now, value, errored)
        if not errored:
            _LAST_GOOD[key] = (now, value)
            return _flag(value, cached=False, stale=False)
        lg = _LAST_GOOD.get(key)
        if lg is not None and (now - lg[0]) < last_good_ttl:
            return _flag(lg[1], cached=True, stale=True)
    return value


def invalidate(prefix: str = "") -> None:
    """Verilen önekle başlayan (boşsa tüm) cache girdilerini temizler."""
    with _LOCK:
        if not prefix:
            _FRESH.clear()
            _LAST_GOOD.clear()
            return
        for store in (_FRESH, _LAST_GOOD):
            for k in [k for k in store if k.startswith(prefix)]:
                store.pop(k, None)
