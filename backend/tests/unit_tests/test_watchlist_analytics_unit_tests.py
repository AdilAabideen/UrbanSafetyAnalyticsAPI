"""
Unit tests for watchlist analytics helper functions.

These tests intentionally avoid DB and HTTP so they focus on pure business logic.
"""

from datetime import date

import pytest

from app.errors import ValidationError
from app.services.watchlist_analytics_service import (
    _month_span,
    _normalize_mode,
    _poisson_interval,
    _risk_band,
    _score_from_raw,
    _shift_month,
    _to_month_start,
    _weighted_mean,
)


def test_normalize_mode_maps_aliases_and_rejects_invalid_values():
    """
    Unit: mode normalization maps aliases and rejects unknown values.

    Checks:
    - walking -> walk
    - car -> drive
    - invalid mode raises ValidationError
    """
    assert _normalize_mode("walking") == "walk"
    assert _normalize_mode("car") == "drive"

    with pytest.raises(ValidationError) as exc_info:
        _normalize_mode("tram")

    assert exc_info.value.error == "INVALID_TRAVEL_MODE"


def test_score_from_raw_is_bounded_and_monotonic():
    """
    Unit: score conversion remains bounded and non-decreasing.

    Checks:
    - all outputs are in [0, 100]
    - increasing raw input never decreases score
    """
    raw_values = [-5.0, 0.0, 0.2, 1.0, 5.0, 20.0, 200.0]
    scores = [_score_from_raw(value) for value in raw_values]

    assert all(0 <= score <= 100 for score in scores)
    assert all(scores[index] <= scores[index + 1] for index in range(len(scores) - 1))


def test_risk_band_returns_expected_band_thresholds():
    """
    Unit: risk band helper classifies threshold boundaries correctly.

    Checks:
    - 24 -> low
    - 25 -> medium
    - 50 -> high
    - 75 -> very_high
    """
    assert _risk_band(24) == "low"
    assert _risk_band(25) == "medium"
    assert _risk_band(49) == "medium"
    assert _risk_band(50) == "high"
    assert _risk_band(74) == "high"
    assert _risk_band(75) == "very_high"


def test_to_month_start_normalizes_day_to_first_of_month():
    """
    Unit: month-start helper always returns day 1.
    """
    assert _to_month_start(date(2026, 3, 14)) == date(2026, 3, 1)


def test_shift_month_handles_year_boundaries():
    """
    Unit: month shift helper handles forward/backward year transitions.
    """
    assert _shift_month(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert _shift_month(date(2025, 12, 1), 1) == date(2026, 1, 1)


def test_month_span_is_inclusive():
    """
    Unit: month span includes both endpoints.
    """
    assert _month_span(date(2026, 1, 1), date(2026, 1, 1)) == 1
    assert _month_span(date(2025, 12, 1), date(2026, 2, 1)) == 3


def test_forecast_weighted_mean_and_poisson_interval_behaviour():
    """
    Unit: weighted mean and Poisson interval helpers behave as expected.

    Checks:
    - weighted mean favors recent values
    - interval bounds are non-negative and ordered
    """
    ascending = [10.0, 20.0, 30.0]
    weighted_value = _weighted_mean(ascending)
    unweighted_value = sum(ascending) / len(ascending)

    assert weighted_value > unweighted_value

    interval = _poisson_interval(12.5)
    assert interval["low"] >= 0
    assert interval["high"] >= interval["low"]
