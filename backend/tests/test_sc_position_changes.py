"""Pozisyon düşüş / yükseliş satır çıkarımı."""
from backend.services.alert_engine import (
    _position_drop_from_row,
    _position_rise_from_row,
    is_adult_position_query,
)


def test_position_drop_from_row_requires_worse_position():
    row = {"query": "dolar", "previous_position": 5.0, "position": 8.2, "clicks": 100}
    out = _position_drop_from_row(row, min_diff=0.1)
    assert out is not None
    assert out["direction"] == "drop"
    assert out["diff_fmt"] == "3.2"


def test_position_rise_from_row_requires_better_position():
    row = {"query": "altın", "previous_position": 12.0, "position": 8.5, "clicks": 50}
    out = _position_rise_from_row(row, min_diff=0.1)
    assert out is not None
    assert out["direction"] == "rise"
    assert out["diff_fmt"] == "3.5"


def test_position_rise_ignored_when_flat():
    row = {"query": "x", "previous_position": 5.0, "position": 5.0, "clicks": 10}
    assert _position_rise_from_row(row, min_diff=0.1) is None
    assert _position_drop_from_row(row, min_diff=0.1) is None


def test_adult_position_query_filter():
    assert is_adult_position_query("am resimleri") is True
    assert is_adult_position_query("yetişkin film") is True
    assert is_adult_position_query("+18 film") is True
    assert is_adult_position_query("18+ film izle") is True
    assert is_adult_position_query("erotik filmler") is True
    assert is_adult_position_query("aksiyon filmleri") is False
    assert is_adult_position_query("kamp alanları") is False
    assert is_adult_position_query("film izle") is False
    assert is_adult_position_query("korku filmleri") is False
