"""Crashlytics sürüm filtresi — SQL ve bellek içi filtre tutarlılığı."""

from backend.main import _refetch_filtered_payload, _version_list_from_params
from backend.services.crashlytics_bq import (
    _event_filters_sql,
    _pick_higher_version,
    _versions_filter_sql,
)


def test_versions_filter_sql_single():
    assert "9.5.5" in _versions_filter_sql(version="9.5.5")
    assert "IN" not in _versions_filter_sql(version="9.5.5")


def test_versions_filter_sql_multi():
    sql = _versions_filter_sql(versions=["9.5.5", "9.5.4"])
    assert "IN" in sql
    assert "'9.5.5'" in sql
    assert "'9.5.4'" in sql


def test_version_list_from_params():
    p = {"versions": ["9.5.5"], "version": "9.5.4"}
    got = _version_list_from_params(p)
    assert "9.5.5" in got
    assert "9.5.4" in got


def test_pick_higher_version_semver():
    assert _pick_higher_version("9.5.4", "9.5.5") == "9.5.5"
    assert _pick_higher_version("9.5.10", "9.5.5") == "9.5.10"


def test_event_filters_sql_combines_type_and_version():
    sql = _event_filters_sql(error_type="FATAL", versions=["9.5.5"])
    assert "error_type = 'FATAL'" in sql
    assert "9.5.5" in sql


def test_refetch_filtered_payload_no_filters_passthrough():
    base = {"ok": True, "days": 7, "issues": [{"issue_id": "a", "event_count": 10}]}
    out = _refetch_filtered_payload(base, {"product": "doviz", "platform": "all"})
    assert out is base or out.get("issues") == base.get("issues")
