"""iOS cihaz kimliği → okunabilir pazarlama adı.

GA4 `deviceModel` iOS'ta Apple donanım kimliğini veriyor (`iPhone14,5`) ve
panelde hiçbir şey anlatmıyor. Bu tablo onu çevirir.

Android burada YOK ve olmamalı: GA4'ün kendi `mobileDeviceMarketingName`
boyutu Android'de doğrudan «Galaxy S25 Ultra» döndürüyor. Önce Google Play
cihaz listesinden 40 bin satırlık bir tablo gömülmüştü; hazır boyut hem
bayatlamıyor hem de bakım istemiyor, o yüzden kaldırıldı. Aynı boyut iOS'ta
boş döndüğü için buradaki tablo duruyor.

Kural: bilinmeyen kod **olduğu gibi** bırakılır. Tahmin edilen bir isim,
kodun kendisinden daha kötüdür — kullanıcı yanlış cihaza bakarak karar verir.
"""

from __future__ import annotations

from typing import Any

# GA4'ün kendi kardinalite kovaları — cihaz değil, çevrilmez
_BUCKETS = frozenset({"(other)", "(not set)", "(none)", ""})

# Apple donanım kimlikleri. iPhone18,x (iPhone 17 ailesi) bilerek YOK:
# alt numaralandırmasını doğrulayamadım, tahmin etmektense kodu göstermek
# doğru. Kaynak bulunduğunda buraya eklenir.
_IOS_MODELS: dict[str, str] = {
    "iPhone8,1": "iPhone 6s",
    "iPhone8,2": "iPhone 6s Plus",
    "iPhone8,4": "iPhone SE (1. nesil)",
    "iPhone9,1": "iPhone 7",
    "iPhone9,2": "iPhone 7 Plus",
    "iPhone9,3": "iPhone 7",
    "iPhone9,4": "iPhone 7 Plus",
    "iPhone10,1": "iPhone 8",
    "iPhone10,2": "iPhone 8 Plus",
    "iPhone10,3": "iPhone X",
    "iPhone10,4": "iPhone 8",
    "iPhone10,5": "iPhone 8 Plus",
    "iPhone10,6": "iPhone X",
    "iPhone11,2": "iPhone XS",
    "iPhone11,4": "iPhone XS Max",
    "iPhone11,6": "iPhone XS Max",
    "iPhone11,8": "iPhone XR",
    "iPhone12,1": "iPhone 11",
    "iPhone12,3": "iPhone 11 Pro",
    "iPhone12,5": "iPhone 11 Pro Max",
    "iPhone12,8": "iPhone SE (2. nesil)",
    "iPhone13,1": "iPhone 12 mini",
    "iPhone13,2": "iPhone 12",
    "iPhone13,3": "iPhone 12 Pro",
    "iPhone13,4": "iPhone 12 Pro Max",
    "iPhone14,2": "iPhone 13 Pro",
    "iPhone14,3": "iPhone 13 Pro Max",
    "iPhone14,4": "iPhone 13 mini",
    "iPhone14,5": "iPhone 13",
    "iPhone14,6": "iPhone SE (3. nesil)",
    "iPhone14,7": "iPhone 14",
    "iPhone14,8": "iPhone 14 Plus",
    "iPhone15,2": "iPhone 14 Pro",
    "iPhone15,3": "iPhone 14 Pro Max",
    "iPhone15,4": "iPhone 15",
    "iPhone15,5": "iPhone 15 Plus",
    "iPhone16,1": "iPhone 15 Pro",
    "iPhone16,2": "iPhone 15 Pro Max",
    "iPhone17,1": "iPhone 16 Pro",
    "iPhone17,2": "iPhone 16 Pro Max",
    "iPhone17,3": "iPhone 16",
    "iPhone17,4": "iPhone 16 Plus",
    "iPhone17,5": "iPhone 16e",
}


def pretty_device_model(model: Any, *, platform: str | None = None) -> str:
    """iOS model kodu → pazarlama adı; bilinmiyorsa kodun kendisi.

    Android çağrıları olduğu gibi döner — orada GA4 zaten adı veriyor.
    """
    raw = str(model or "").strip()
    if not raw or raw in _BUCKETS:
        return raw
    pf = (platform or "").strip().lower()
    looks_apple = raw.startswith(("iPhone", "iPad", "iPod"))
    if pf == "ios" or (not pf and looks_apple):
        return _IOS_MODELS.get(raw, raw)
    return raw
