"""Konsol kapsaması — «elimde hangi günler var» ve eksik gün tespiti.

Bu uç `history_seal.gap_fill` modunu besler; boşluk tespiti buradan gelen
`known_dates` olmadan hiç çalışmıyordu.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from backend.services import console_coverage as CC

ROOT = Path(__file__).resolve().parents[2]


# ── Tarih toplama: iki farklı JSON şekli ────────────────────────────────────

def test_collects_dates_from_asc_explorer_facts():
    blob = {
        "version": 2,
        "items": [],
        "panels": {
            "explorer_facts": [
                {"metric": "impressions", "date": "2026-08-15", "dim": "overview"},
                {"metric": "downloads", "date": "2026-08-15", "dim": "overview"},
                {"metric": "impressions", "date": "2026-08-16", "dim": "overview"},
            ]
        },
    }
    counts: dict[date, int] = {}
    CC._collect_dates(blob, counts)
    assert counts == {date(2026, 8, 15): 2, date(2026, 8, 16): 1}


def test_collects_dates_from_firebase_nested_series():
    blob = {
        "android": {"series": [{"day": "2026-08-14", "v": 1}, {"day": "2026-08-15", "v": 2}]},
        "ios": {"sessions_series": [{"report_date": "2026-08-15", "v": 3}]},
    }
    counts: dict[date, int] = {}
    CC._collect_dates(blob, counts)
    assert set(counts) == {date(2026, 8, 14), date(2026, 8, 15)}
    assert counts[date(2026, 8, 15)] == 2  # iki farklı seriden


def test_shape_change_does_not_silently_empty_coverage():
    """Özyinelemeli gezinme sayesinde yapı değişse de tarihler bulunur."""
    counts: dict[date, int] = {}
    CC._collect_dates({"yeni": {"kutu": [{"date": "2026-08-17"}]}}, counts)
    assert counts == {date(2026, 8, 17): 1}


def test_bad_dates_are_ignored():
    counts: dict[date, int] = {}
    CC._collect_dates(
        [{"date": "dun"}, {"date": ""}, {"date": None}, {"date": "2026-13-40"}], counts
    )
    assert counts == {}


def test_recursion_is_bounded():
    node: dict = {"date": "2026-08-10"}
    for _ in range(40):
        node = {"n": node}
    counts: dict[date, int] = {}
    CC._collect_dates(node, counts)  # patlamamalı
    assert isinstance(counts, dict)


def test_broken_json_is_not_fatal():
    assert CC._load_blob("{bozuk") is None
    assert CC._load_blob("") is None


# ── Eksik gün hesabı ────────────────────────────────────────────────────────

def test_missing_between_finds_the_hole():
    known = {date(2026, 8, 10), date(2026, 8, 12)}
    missing = CC.missing_between(known, date(2026, 8, 10), date(2026, 8, 13))
    assert missing == [date(2026, 8, 11), date(2026, 8, 13)]


def test_missing_between_handles_inverted_range():
    assert CC.missing_between(set(), date(2026, 8, 13), date(2026, 8, 10)) == []


def test_payload_reports_gap_and_window():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()
    counts = {yday: 3}
    out = CC._coverage_payload("asc", counts, start=(yday - timedelta(days=2)).isoformat(), end=yday.isoformat())
    assert out["ok"] is True
    assert out["known_count"] == 1
    assert out["has_gap"] is True
    assert out["missing_count"] == 2
    assert yday.isoformat() not in out["missing"]
    assert out["counts"][yday.isoformat()] == 3


def test_payload_has_no_gap_when_complete():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()
    counts = {yday - timedelta(days=i): 1 for i in range(3)}
    out = CC._coverage_payload("asc", counts, start=(yday - timedelta(days=2)).isoformat(), end=yday.isoformat())
    assert out["has_gap"] is False and out["missing"] == []


def test_default_window_matches_gap_fill_range():
    """Panel ile scraper aynı aralığı konuşmalı: mühür+1 → dün."""
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    out = CC._coverage_payload("asc", {}, start=None, end=None)
    assert out["end"] == calendar_yesterday().isoformat()
    assert out["start"] == (pipeline_seal_through("asc") + timedelta(days=1)).isoformat()


# ── Uçlar ───────────────────────────────────────────────────────────────────

def test_both_endpoints_exist_and_are_token_protected():
    asc = (ROOT / "backend/api/asc_console.py").read_text(encoding="utf-8")
    fb = (ROOT / "backend/api/firebase_console.py").read_text(encoding="utf-8")
    for src, path in ((asc, "/asc-console/coverage"), (fb, "/firebase-console/coverage")):
        assert f'@router.get("{path}")' in src
        block = src.split(f'@router.get("{path}")', 1)[1]
        assert "_check_ingest_token" in block


def test_bridge_can_reach_coverage_without_panel_cookie():
    main = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    prefixes = main.split("public_prefixes = (", 1)[1].split("\n    )", 1)[0]
    assert '"/api/asc-console/coverage"' in prefixes
    assert '"/api/firebase-console/coverage"' in prefixes


def test_coverage_registry_covers_both_pipelines():
    assert set(CC.COVERAGE_BY_PIPELINE) == {"asc", "firebase"}


def test_coverage_reads_from_db_without_crashing_when_empty():
    class _Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return None

    class _DB:
        def query(self, *a, **k):
            return _Q()

    out = CC.asc_coverage(_DB())
    assert out["ok"] is True and out["known_count"] == 0
    assert out["has_gap"] in (True, False)


def test_coverage_uses_real_stored_json():
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()

    class _Row:
        metrics_json = json.dumps({
            "version": 2, "items": [],
            "panels": {"explorer_facts": [{"metric": "m", "date": yday.isoformat()}]},
        })

    class _Q:
        def filter(self, *a, **k):
            return self

        def first(self):
            return _Row()

    class _DB:
        def query(self, *a, **k):
            return _Q()

    out = CC.asc_coverage(_DB(), start=yday.isoformat(), end=yday.isoformat())
    assert out["known_count"] == 1 and out["has_gap"] is False


# ── Boşluk doldurma: sınırlı, idempotent, hatada zararsız ───────────────────

def test_gap_fill_is_bounded_so_one_hole_cannot_trigger_a_huge_scrape():
    """Mühür aylar öncesine düşse bile tarama penceresi lookback ile sınırlı."""
    from backend.services.history_seal import calendar_yesterday, scheduled_fetch_window

    yday = calendar_yesterday()
    # Son 14 günün tamamı eksik gibi davran (kayıtlı gün yok)
    known = CC.bounded_known_dates([], pipeline="asc", lookback_days=14)
    win = scheduled_fetch_window("asc", force_full=False, known_dates=known)
    assert win["mode"] == "gap_fill"
    span = (win["end"] - win["start"]).days + 1
    assert span <= 14, f"pencere {span} gün — sınır aşıldı"
    assert win["end"] <= yday
    assert win["store_end"] <= yday          # bugün asla kaydedilmez


def test_only_the_missing_day_is_fetched_not_the_whole_history():
    from backend.services.history_seal import calendar_yesterday, scheduled_fetch_window

    yday = calendar_yesterday()
    hole = yday - timedelta(days=3)
    have = [yday - timedelta(days=i) for i in range(14) if (yday - timedelta(days=i)) != hole]
    known = CC.bounded_known_dates(have, pipeline="asc", lookback_days=14)
    win = scheduled_fetch_window("asc", force_full=False, known_dates=known)
    assert win["mode"] == "gap_fill"
    assert win["start"] == hole and win["end"] == hole   # tek gün, fazlası yok


def test_no_gap_falls_back_to_yesterday_only():
    from backend.services.history_seal import calendar_yesterday, scheduled_fetch_window

    yday = calendar_yesterday()
    have = [yday - timedelta(days=i) for i in range(14)]
    known = CC.bounded_known_dates(have, pipeline="asc", lookback_days=14)
    win = scheduled_fetch_window("asc", force_full=False, known_dates=known)
    assert win["mode"] == "yesterday_only"
    assert win["store_end"] == yday


def test_dates_older_than_lookback_are_treated_as_known():
    """Geçmiş backfill force_full'un işi; boşluk doldurma oraya taşmamalı."""
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    yday = calendar_yesterday()
    known = set(CC.bounded_known_dates([], pipeline="asc", lookback_days=7))
    old_day = pipeline_seal_through("asc") + timedelta(days=1)
    boundary = yday - timedelta(days=6)
    if old_day < boundary:
        assert old_day in known          # eski gün "var" sayıldı
    assert boundary not in known         # pencere içi gerçekten eksik


def test_coverage_failure_leaves_behaviour_unchanged(monkeypatch):
    """Uca ulaşılamazsa None döner → çağıran taraf bugünkü davranışta kalır."""
    monkeypatch.setattr(CC, "fetch_remote_coverage", lambda p, base_url=None: None)
    assert CC.known_dates_for_scrape("asc") is None
    assert CC.oldest_missing_within("firebase") is None


def test_missing_token_does_not_call_the_network(monkeypatch):
    monkeypatch.delenv("NOTIFICATION_INGEST_TOKEN", raising=False)
    called = {"n": 0}

    def _boom(*a, **k):
        called["n"] += 1
        raise AssertionError("ağa gidilmemeliydi")

    monkeypatch.setattr("requests.get", _boom, raising=False)
    assert CC.fetch_remote_coverage("asc") is None
    assert called["n"] == 0


def test_coverage_url_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("ASC_CONSOLE_COVERAGE_URL", "https://x.test/cov")
    assert CC.coverage_url("asc") == "https://x.test/cov"
    monkeypatch.delenv("ASC_CONSOLE_COVERAGE_URL")
    monkeypatch.setenv("PROJECT_CONTROL_BASE_URL", "https://panel.test/")
    assert CC.coverage_url("firebase") == "https://panel.test/api/firebase-console/coverage"


def test_oldest_missing_returns_the_earliest_hole(monkeypatch):
    """Arama alanı mühür+1 → dün; mühürlü geçmişteki delik yeniden çekilmez."""
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    yday = calendar_yesterday()
    gap_start = pipeline_seal_through("firebase") + timedelta(days=1)
    span = max(2, (yday - gap_start).days + 1)
    holes = {yday - timedelta(days=1), yday}
    have = [(yday - timedelta(days=i)).isoformat() for i in range(span + 3)
            if (yday - timedelta(days=i)) not in holes]
    monkeypatch.setattr(CC, "fetch_remote_coverage",
                        lambda p, base_url=None: {"ok": True, "dates": have})
    got = CC.oldest_missing_within("firebase", lookback_days=14)
    assert got == max(yday - timedelta(days=1), gap_start)


def test_holes_before_the_seal_are_not_refetched(monkeypatch):
    """Mühürlü gövde nihai kabul edilir; oradaki eksik yeniden taranmaz."""
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    yday = calendar_yesterday()
    seal = pipeline_seal_through("firebase")
    have = [(yday - timedelta(days=i)).isoformat() for i in range(30)
            if (yday - timedelta(days=i)) > seal]
    monkeypatch.setattr(CC, "fetch_remote_coverage",
                        lambda p, base_url=None: {"ok": True, "dates": have})
    assert CC.oldest_missing_within("firebase", lookback_days=14) is None


# ── Scraper bağlantıları ────────────────────────────────────────────────────

def test_asc_scraper_passes_known_dates():
    src = (ROOT / "scripts/asc_console_scrape.py").read_text(encoding="utf-8")
    assert "known_dates_for_scrape" in src
    assert 'scheduled_fetch_window("asc", known_dates=known)' in src
    # Hata yutulmalı, scrape düşmemeli
    block = src.split("def _scrape_window", 1)[1].split("\ndef ", 1)[0]
    assert "except Exception" in block


def test_firebase_scraper_widens_window_only_for_gaps():
    src = (ROOT / "scripts/firebase_console_scrape.py").read_text(encoding="utf-8")
    assert "_gap_days" in src and "oldest_missing_within" in src
    block = src.split("def _gap_days", 1)[1].split("\ndef _scrape_days", 1)[0]
    assert "min(span, 90)" in block          # Console UI üst sınırı
    assert "return 0" in block               # boşluk yoksa genişletme yok
    use = src.split("def _scrape_days", 1)[1].split("\ndef ", 1)[0]
    assert "if gap > 1:" in use and "return 1" in use


def test_stores_upsert_by_date_so_refetch_cannot_duplicate_rows():
    """Bir günü tekrar çekmek satır çoğaltmamalı — boşluk doldurmanın şartı."""
    asc = (ROOT / "backend/services/asc_console_store.py").read_text(encoding="utf-8")
    fb = (ROOT / "backend/services/firebase_console_store.py").read_text(encoding="utf-8")
    # ASC: (metric, date, dim) anahtarı
    assert 'str(f.get("date") or "")[:10]' in asc and "by_key[key] = f" in asc
    # Firebase: tarih anahtarlı sözlük
    assert "by_d[ds] = it" in fb
    # İkisi de bugünü kalıcı yazmaz
    assert "never_store_today" in asc and "never_store_today" in fb


# ── Yanlış gün üretmeme garantileri ─────────────────────────────────────────

def test_today_never_enters_coverage():
    """Bugün yarım gündür; kapsamada görünürse gerçek boşluğu maskeler."""
    from backend.services.history_seal import calendar_today, calendar_yesterday

    today = calendar_today()
    yday = calendar_yesterday()
    out = CC._coverage_payload("asc", {today: 5, yday: 2}, start=None, end=None)
    assert today.isoformat() not in out["dates"]
    assert today.isoformat() not in out["counts"]
    assert out["known_count"] == 1


def test_today_is_ignored_in_known_dates_too():
    from backend.services.history_seal import calendar_today

    known = CC.bounded_known_dates([calendar_today().isoformat()], pipeline="asc", lookback_days=7)
    assert calendar_today() not in known


def test_pre_history_is_not_treated_as_a_gap():
    """Hattın hiç verisi olmayan geçmişi «eksik» sayılmamalı — boşuna tarama."""
    from backend.services.history_seal import calendar_yesterday, scheduled_fetch_window

    yday = calendar_yesterday()
    # Hat yalnızca 3 gündür veri üretiyor, ortada da bir delik var
    have = [yday, yday - timedelta(days=1), yday - timedelta(days=3)]
    known = CC.bounded_known_dates(have, pipeline="firebase", lookback_days=14)
    win = scheduled_fetch_window("firebase", force_full=False, known_dates=known)
    assert win["mode"] == "gap_fill"
    # Yalnızca gerçek delik (yday-2), ilk kayıttan öncesi değil
    assert win["start"] == yday - timedelta(days=2)
    assert win["end"] == yday - timedelta(days=2)


def test_new_pipeline_with_only_yesterday_has_no_gap():
    from backend.services.history_seal import calendar_yesterday, scheduled_fetch_window

    yday = calendar_yesterday()
    known = CC.bounded_known_dates([yday], pipeline="firebase", lookback_days=14)
    win = scheduled_fetch_window("firebase", force_full=False, known_dates=known)
    assert win["mode"] == "yesterday_only"    # geçmiş yok ≠ boşluk var


def test_earliest_is_computed_from_real_records_not_synthetic_fill():
    """(a) sentetik doldurma (b) ilk-kayıt kuralını bozmamalı."""
    from backend.services.history_seal import calendar_yesterday

    yday = calendar_yesterday()
    from backend.services.history_seal import missing_days, pipeline_seal_through

    have = [yday - timedelta(days=1)]
    known = set(CC.bounded_known_dates(have, pipeline="firebase", lookback_days=14))
    gap_start = pipeline_seal_through("firebase") + timedelta(days=1)

    # İlk kayıttan (dün-1) önceki hiçbir gün eksik sayılmamalı
    earliest = yday - timedelta(days=1)
    before = missing_days(known, start=gap_start, end=earliest - timedelta(days=1))
    assert before == [], f"ilk kayıttan öncesi boşluk sayıldı: {before}"

    # Gerçek delik yalnızca dün
    assert missing_days(known, start=gap_start, end=yday) == [yday]
