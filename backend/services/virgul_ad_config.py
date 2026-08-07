"""Virgül reklam paneli — site (sid) → SEO Agent reklam dalı eşlemesi.

Kaynak yalnızca https://rapor.virgul.com/npm?sid=… (Google Sheet / manuel yükleme yok).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VirgulAdSource:
    sid: str
    stream_key: str
    label: str

    @property
    def panel_url(self) -> str:
        return f"https://rapor.virgul.com/npm?sid={self.sid}"

    @property
    def catalog_filename(self) -> str:
        return f"virgul_{self.sid}.xlsx"


# Kullanıcının verdiği 6 link.
# sid …ac2 → m.sinemalar.com = sinemalar:mweb (doğrulandı).
# Döviz web/mweb sid’leri ilk atamada ters düşmüştü — düzeltildi.
VIRGUL_AD_SOURCES: tuple[VirgulAdSource, ...] = (
    VirgulAdSource(
        sid="5062c6cb87354585c0e19abe",
        stream_key="doviz:desktop",
        label="Döviz · Web",
    ),
    VirgulAdSource(
        sid="5062c6cc87354585c0e19ac1",
        stream_key="doviz:mweb",
        label="Döviz · Mweb",
    ),
    VirgulAdSource(
        sid="576910bba503b020048b4568",
        stream_key="doviz:ios",
        label="Döviz · iOS",
    ),
    VirgulAdSource(
        sid="55af4685a503b0ad628b4567",
        stream_key="doviz:android",
        label="Döviz · Android",
    ),
    VirgulAdSource(
        sid="5062c6a187354585c0e19aba",
        stream_key="sinemalar:desktop",
        label="Sinemalar · Web",
    ),
    VirgulAdSource(
        sid="5062c6cc87354585c0e19ac2",
        stream_key="sinemalar:mweb",
        label="Sinemalar · Mweb",
    ),
)

VIRGUL_REPORT_URL = "https://rapor.virgul.com/npm/report"
VIRGUL_LOGIN_URL = "https://rapor.virgul.com/login/authenticate"
VIRGUL_SOURCE_PREFIX = "virgul_"


def is_virgul_source_file(name: str | None) -> bool:
    return str(name or "").strip().lower().startswith(VIRGUL_SOURCE_PREFIX)


def virgul_sources_payload() -> list[dict[str, str]]:
    return [
        {
            "sid": s.sid,
            "key": s.stream_key,
            "label": s.label,
            "url": s.panel_url,
            "catalog": s.catalog_filename,
        }
        for s in VIRGUL_AD_SOURCES
    ]


def source_by_sid(sid: str) -> VirgulAdSource | None:
    sid = (sid or "").strip()
    for s in VIRGUL_AD_SOURCES:
        if s.sid == sid:
            return s
    return None


def source_by_stream(stream_key: str) -> VirgulAdSource | None:
    key = (stream_key or "").strip()
    for s in VIRGUL_AD_SOURCES:
        if s.stream_key == key:
            return s
    return None
