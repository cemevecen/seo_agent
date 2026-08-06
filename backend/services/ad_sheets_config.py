"""Reklam gelirleri — Google Sheets kaynakları (stream başına bir doküman)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AdSheetSource:
    stream_key: str
    sheet_url: str
    label: str
    # Web sheet çoğu zaman Mweb satırlarını da içeriyor; import’ta bu dalın
    # (ad_unit, date, income_type) anahtarları desktop’tan düşülür (çift sayım / aynı grafik).
    exclude_sibling_stream_key: str | None = None


# Mweb önce: kardeş anahtar seti hazır; desktop sync aynı satırları yazmaz.
# Kullanıcı sheet altına gün/hafta satır ekler; sync upsert ile son güne kadar tutar.
AD_SHEET_SOURCES: tuple[AdSheetSource, ...] = (
    AdSheetSource(
        stream_key="doviz:mweb",
        sheet_url="https://docs.google.com/spreadsheets/d/1YL7h35HjNSnYph0KDV6u1wxS39YB7cA2kF5H6ZPfXCM/edit?gid=0#gid=0",
        label="Döviz · Mweb",
    ),
    AdSheetSource(
        stream_key="doviz:desktop",
        sheet_url="https://docs.google.com/spreadsheets/d/1Y8kQvH4uEqEPJtrRWM4BewBbRNW0QSlZoo79yzSTFhE/edit?gid=0#gid=0",
        label="Döviz · Web",
        exclude_sibling_stream_key="doviz:mweb",
    ),
    AdSheetSource(
        stream_key="doviz:ios",
        sheet_url="https://docs.google.com/spreadsheets/d/1bb2pO_nP12WsnqiyX6OZ0e36INOmnGXN4X9ehHP67aA/edit?gid=0#gid=0",
        label="Döviz · iOS",
    ),
    AdSheetSource(
        stream_key="doviz:android",
        sheet_url="https://docs.google.com/spreadsheets/d/11q7XUi0YExXotelxh8cAT_wNjcw9ksnttrtpfqqlIrY/edit?gid=0#gid=0",
        label="Döviz · Android",
    ),
    AdSheetSource(
        stream_key="sinemalar:mweb",
        sheet_url="https://docs.google.com/spreadsheets/d/1dPEGdkM1XhwE1ewiUG3QNw8KMrWERy1RcRdHblnQiIo/edit?gid=0#gid=0",
        label="Sinemalar · Mweb",
    ),
    AdSheetSource(
        stream_key="sinemalar:desktop",
        sheet_url="https://docs.google.com/spreadsheets/d/1z7wsPaJeV2Ac1qEp9etx_Df0vrwjwN2F7ghvF08HboY/edit?gid=0#gid=0",
        label="Sinemalar · Web",
        exclude_sibling_stream_key="sinemalar:mweb",
    ),
)


def sheet_catalog_filename(stream_key: str) -> str:
    """Katalogda tek kararlı kaynak adı (her sync aynı dosya gibi görünür)."""
    return f"{(stream_key or 'unknown').replace(':', '_')}_google_sheet.csv"


def is_sheet_catalog_filename(name: str) -> bool:
    low = (name or "").strip().lower()
    return low.endswith("_google_sheet.csv")
