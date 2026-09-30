"""Piyasa günlük kapanış serileri — doviz.com tarihsel tablo taraması."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSheetSeries:
    key: str
    label: str
    source_url: str
    unit: str = ""
    # Eski Google Sheets alanı; tarama sonrası kullanılmaz.
    sheet_url: str = ""
    category: str = "other"


# 01.01.2025+ Tablo / Verileri Getir — Mac köprüsü günde bir (00:05 TR).
MARKET_SHEET_SERIES: tuple[MarketSheetSeries, ...] = (
    # —— Altın / gümüş ——
    MarketSheetSeries(
        key="gram_altin",
        label="Gram altın",
        source_url="https://altin.doviz.com/gram-altin",
        unit="TL/gr",
        category="gold",
    ),
    MarketSheetSeries(
        key="harem_gram_altin",
        label="Harem gram altın",
        source_url="https://altin.doviz.com/harem/gram-altin",
        unit="TL/gr",
        category="gold",
    ),
    MarketSheetSeries(
        key="altinkaynak_gram_altin",
        label="Altınkaynak gram altın",
        source_url="https://altin.doviz.com/altinkaynak/gram-altin",
        unit="TL/gr",
        category="gold",
    ),
    MarketSheetSeries(
        key="ons_altin",
        label="Ons altın",
        source_url="https://altin.doviz.com/ons",
        unit="USD/oz",
        category="gold",
    ),
    MarketSheetSeries(
        key="ceyrek_altin",
        label="Çeyrek altın",
        source_url="https://altin.doviz.com/ceyrek-altin",
        unit="TL",
        category="gold",
    ),
    MarketSheetSeries(
        key="ata_altin",
        label="Ata altın",
        source_url="https://altin.doviz.com/ata-altin",
        unit="TL",
        category="gold",
    ),
    MarketSheetSeries(
        key="cumhuriyet_altini",
        label="Cumhuriyet altını",
        source_url="https://altin.doviz.com/cumhuriyet-altini",
        unit="TL",
        category="gold",
    ),
    MarketSheetSeries(
        key="tam_altin",
        label="Tam altın",
        source_url="https://altin.doviz.com/tam-altin",
        unit="TL",
        category="gold",
    ),
    MarketSheetSeries(
        key="resat_altin",
        label="Reşat altın",
        source_url="https://altin.doviz.com/resat-altin",
        unit="TL",
        category="gold",
    ),
    MarketSheetSeries(
        key="gram_gumus",
        label="Gram gümüş",
        source_url="https://altin.doviz.com/gumus",
        unit="TL/gr",
        category="silver",
    ),
    MarketSheetSeries(
        key="harem_gram_gumus",
        label="Harem gram gümüş",
        source_url="https://altin.doviz.com/harem/gumus",
        unit="TL/gr",
        category="silver",
    ),
    # —— Emtia ——
    MarketSheetSeries(
        key="brent",
        label="Brent petrol",
        source_url="https://www.doviz.com/emtia/brent-petrol",
        unit="USD/varil",
        category="commodity",
    ),
    MarketSheetSeries(
        key="gumus_ons",
        label="Gümüş ons",
        source_url="https://www.doviz.com/emtia/gumus-ons",
        unit="USD/oz",
        category="commodity",
    ),
    MarketSheetSeries(
        key="altin_gumus",
        label="Altın / gümüş",
        source_url="https://www.doviz.com/emtia/altin-gumus",
        unit="oran",
        category="commodity",
    ),
    MarketSheetSeries(
        key="aluminyum",
        label="Alüminyum",
        source_url="https://www.doviz.com/emtia/aluminyum",
        unit="USD/t",
        category="commodity",
    ),
    # —— Döviz ——
    MarketSheetSeries(
        key="usd_try",
        label="USD/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/amerikan-dolari",
        unit="TL",
        category="fx",
    ),
    MarketSheetSeries(
        key="eur_try",
        label="EUR/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/euro",
        unit="TL",
        category="fx",
    ),
    MarketSheetSeries(
        key="gbp_try",
        label="GBP/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/sterlin",
        unit="TL",
        category="fx",
    ),
    MarketSheetSeries(
        key="chf_try",
        label="CHF/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/isvicre-frangi",
        unit="TL",
        category="fx",
    ),
    MarketSheetSeries(
        key="sar_try",
        label="SAR/TRY",
        source_url="https://kur.doviz.com/serbest-piyasa/suudi-arabistan-riyali",
        unit="TL",
        category="fx",
    ),
    # —— Endeks ——
    MarketSheetSeries(
        key="bist100",
        label="BIST 100",
        source_url="https://borsa.doviz.com/endeksler/xu100-bist-100",
        unit="puan",
        category="index",
    ),
    # —— Kripto ——
    MarketSheetSeries(
        key="bitcoin",
        label="Bitcoin",
        source_url="https://www.doviz.com/kripto-paralar/bitcoin",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="ethereum",
        label="Ethereum",
        source_url="https://www.doviz.com/kripto-paralar/ethereum",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="solana",
        label="Solana",
        source_url="https://www.doviz.com/kripto-paralar/solana",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="luna_classic",
        label="Terra Luna Classic",
        source_url="https://www.doviz.com/kripto-paralar/terra-luna-classic",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="dogecoin",
        label="Dogecoin",
        source_url="https://www.doviz.com/kripto-paralar/dogecoin",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="avalanche",
        label="Avalanche",
        source_url="https://www.doviz.com/kripto-paralar/avalanche",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="xrp",
        label="XRP",
        source_url="https://www.doviz.com/kripto-paralar/xrp",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="pepe",
        label="Pepe",
        source_url="https://www.doviz.com/kripto-paralar/pepe",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="shiba_inu",
        label="Shiba Inu",
        source_url="https://www.doviz.com/kripto-paralar/shiba-inu",
        unit="USD",
        category="crypto",
    ),
    MarketSheetSeries(
        key="arbitrum",
        label="Arbitrum",
        source_url="https://www.doviz.com/kripto-paralar/arbitrum",
        unit="USD",
        category="crypto",
    ),
    # —— BIST hisseleri ——
    MarketSheetSeries(
        key="asels",
        label="ASELS",
        source_url="https://borsa.doviz.com/hisseler/asels-aselsan",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="thyao",
        label="THYAO",
        source_url="https://borsa.doviz.com/hisseler/thyao-turk-hava-yollari",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="sasa",
        label="SASA",
        source_url="https://borsa.doviz.com/hisseler/sasa-sasa-polyester",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="akbnk",
        label="AKBNK",
        source_url="https://borsa.doviz.com/hisseler/akbnk-akbank",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="tuprs",
        label="TUPRS",
        source_url="https://borsa.doviz.com/hisseler/tuprs-tupras",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="tralt",
        label="TRALT",
        source_url="https://borsa.doviz.com/hisseler/tralt-turk-altin-isletmeleri",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="ykbnk",
        label="YKBNK",
        source_url="https://borsa.doviz.com/hisseler/ykbnk-yapi-ve-kredi-bank",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="kchol",
        label="KCHOL",
        source_url="https://borsa.doviz.com/hisseler/kchol-koc-holding",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="isctr",
        label="ISCTR",
        source_url="https://borsa.doviz.com/hisseler/isctr-is-bankasi-c",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="bimas",
        label="BIMAS",
        source_url="https://borsa.doviz.com/hisseler/bimas-bim-magazalar",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="eregl",
        label="EREGL",
        source_url="https://borsa.doviz.com/hisseler/eregl-eregli-demir-celik",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="garan",
        label="GARAN",
        source_url="https://borsa.doviz.com/hisseler/garan-garanti-bankasi",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="sahol",
        label="SAHOL",
        source_url="https://borsa.doviz.com/hisseler/sahol-sabanci-holding",
        unit="TL",
        category="equity",
    ),
    MarketSheetSeries(
        key="krdmd",
        label="KRDMD",
        source_url="https://borsa.doviz.com/hisseler/krdmd-kardemir-d",
        unit="TL",
        category="equity",
    ),
)

SERIES_BY_KEY = {s.key: s for s in MARKET_SHEET_SERIES}

# Assets sayfası varsayılan seçim (Metrics kart ızgarası gibi ~10)
DEFAULT_ASSET_KEYS: tuple[str, ...] = (
    "gram_altin",
    "usd_try",
    "eur_try",
    "bist100",
    "bitcoin",
    "brent",
    "gram_gumus",
    "ons_altin",
    "ethereum",
    "asels",
)

TARAMA_SOURCE_ID = "doviz.com"
# En erken makul başlangıç — API/site ne dönerse o alınır (2025 kesmesi yok).
TARAMA_START_DATE = "2010-01-01"

# doviz.com CDN — bayrak / coin / emtia / hisse ikonları (hotlink, public).
_CDN_IMG = "https://cdn.doviz.com/images"
_ASSET_ICON_PATHS: dict[str, str] = {
    # Altın / gümüş
    "gram_altin": "/other-assets/altin.png",
    "harem_gram_altin": "/bank-logos/harem.png",
    "altinkaynak_gram_altin": "/bank-logos/altinkaynak.png",
    "ons_altin": "/other-assets/ons.png",
    "ceyrek_altin": "/other-assets/altin.png",
    "ata_altin": "/other-assets/altin.png",
    "cumhuriyet_altini": "/other-assets/altin.png",
    "tam_altin": "/other-assets/altin.png",
    "resat_altin": "/other-assets/altin.png",
    "gram_gumus": "/other-assets/gumus.png",
    "harem_gram_gumus": "/bank-logos/harem.png",
    # Emtia
    "brent": "/other-assets/brent.png",
    "gumus_ons": "/other-assets/xag-usd.png",
    "altin_gumus": "/other-assets/altin.png",
    "aluminyum": "/other-assets/aluminum.png",
    # Döviz
    "usd_try": "/flags/usd.png",
    "eur_try": "/flags/eur.png",
    "gbp_try": "/flags/gbp.png",
    "chf_try": "/flags/chf.png",
    "sar_try": "/flags/sar.png",
    # Endeks
    "bist100": "/other-assets/bist.png",
    # Kripto
    "bitcoin": "/coin/bitcoin.png",
    "ethereum": "/coin/ethereum.png",
    "solana": "/coin/solana.png",
    "luna_classic": "/coin/terra-luna.png",
    "dogecoin": "/coin/dogecoin.png",
    "avalanche": "/coin/avalanche-2.png",
    "xrp": "/coin/ripple.png",
    "pepe": "/coin/pepe.png",
    "shiba_inu": "/coin/shiba-inu.png",
    "arbitrum": "/coin/arbitrum.png",
    # BIST hisseleri
    "asels": "/stock/ASELS.png",
    "thyao": "/stock/THYAO.png",
    "sasa": "/stock/SASA.png",
    "akbnk": "/stock/AKBNK.png",
    "tuprs": "/stock/TUPRS.png",
    "tralt": "/other-assets/altin.png",  # CDN'de TRALT yok
    "ykbnk": "/stock/YKBNK.png",
    "kchol": "/stock/KCHOL.png",
    "isctr": "/stock/ISCTR.png",
    "bimas": "/stock/BIMAS.png",
    "eregl": "/stock/EREGL.png",
    "garan": "/stock/GARAN.png",
    "sahol": "/stock/SAHOL.png",
    "krdmd": "/stock/KRDMD.png",
}
_PLACEHOLDER_ICON = f"{_CDN_IMG}/other-assets/placeholder.png"


def icon_url_for(key: str) -> str:
    """doviz.com CDN ikon URL'si — bilinmeyen anahtar için placeholder."""
    path = _ASSET_ICON_PATHS.get(str(key or "").strip())
    if not path:
        return _PLACEHOLDER_ICON
    return f"{_CDN_IMG}{path}"


def series_public_dict(series: MarketSheetSeries) -> dict[str, str]:
    return {
        "key": series.key,
        "label": series.label,
        "unit": series.unit,
        "source_url": series.source_url,
        "category": series.category or "other",
        "icon_url": icon_url_for(series.key),
    }
