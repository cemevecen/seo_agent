"""Gelir hedefleri: tek aylık ingest geçmiş ayları silmemeli.

Önceki davranış: `save_ingested_revenue_targets` birleşimi yalnızca gelen
satırlardan kuruyordu; tek aylık bir scrape (ör. sadece bu ayın MCM sekmesi)
tüm geçmiş ayları uçuruyordu.
"""

from __future__ import annotations

import json

from backend.services import revenue_targets_sheet as rts


def _row(period: str, project: str = "doviz", target: float = 100.0) -> dict:
    return {"period_key": period, "project": project, "target": target}


def _isolate(monkeypatch, tmp_path):
    """Yazımı geçici dosyaya yönlendir, gerçek veritabanına dokunma."""
    out = tmp_path / "revenue_targets_ingest.json"
    monkeypatch.setattr(rts, "_INGEST_PATH", out)

    def _no_db(*a, **k):
        raise RuntimeError("test: veritabanı kullanılmıyor")

    monkeypatch.setattr("backend.database.SessionLocal", _no_db)
    return out


def _written(path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_existing_months_survive_a_single_month_ingest(monkeypatch, tmp_path):
    out = _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(rts, "_existing_target_rows", lambda: [_row("2026-06"), _row("2026-07")])

    rts.save_ingested_revenue_targets(rows=[_row("2026-08", target=999.0)])

    periods = [r["period_key"] for r in _written(out)["rows"]]
    assert periods == ["2026-06", "2026-07", "2026-08"]


def test_same_period_is_overwritten_not_duplicated(monkeypatch, tmp_path):
    out = _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(rts, "_existing_target_rows", lambda: [_row("2026-08", target=1.0)])

    rts.save_ingested_revenue_targets(rows=[_row("2026-08", target=2.0)])

    rows = _written(out)["rows"]
    assert len(rows) == 1
    assert rows[0]["target"] == 2.0        # yeni değer kazanır


def test_projects_are_kept_separate(monkeypatch, tmp_path):
    out = _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(rts, "_existing_target_rows", lambda: [_row("2026-08", "sinemalar", 5.0)])

    rts.save_ingested_revenue_targets(rows=[_row("2026-08", "doviz", 7.0)])

    rows = {(r["period_key"], r["project"]): r["target"] for r in _written(out)["rows"]}
    assert rows == {("2026-08", "sinemalar"): 5.0, ("2026-08", "doviz"): 7.0}


def test_unknown_projects_are_dropped(monkeypatch, tmp_path):
    out = _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(rts, "_existing_target_rows", lambda: [])

    rts.save_ingested_revenue_targets(
        rows=[_row("2026-08", "doviz"), _row("2026-08", "baska_proje")]
    )
    assert [r["project"] for r in _written(out)["rows"]] == ["doviz"]


def test_history_read_failure_does_not_block_ingest(monkeypatch, tmp_path):
    """Geçmiş okunamazsa bu turun verisi yine de kaydedilmeli."""
    out = _isolate(monkeypatch, tmp_path)

    def _boom():
        raise RuntimeError("db yok")

    monkeypatch.setattr(rts, "_existing_rows_from_db", _boom)
    monkeypatch.setattr(rts, "_existing_rows_from_file", _boom)

    rts.save_ingested_revenue_targets(rows=[_row("2026-08")])
    assert [r["period_key"] for r in _written(out)["rows"]] == ["2026-08"]


def test_period_keys_reflect_the_merged_set(monkeypatch, tmp_path):
    out = _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(rts, "_existing_target_rows", lambda: [_row("2026-01")])

    rts.save_ingested_revenue_targets(rows=[_row("2026-02")])
    payload = _written(out)
    assert payload["period_keys"] == ["2026-01", "2026-02"]
    assert payload["row_count"] == 2
