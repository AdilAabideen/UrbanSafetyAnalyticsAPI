"""
Unit tests for watchlist business-rule helpers.

These tests intentionally avoid HTTP and DB to validate pure service logic.
"""

import pytest

from app.errors import ValidationError
from app.services.watchlist_service import _normalize_watchlist_mode, _validate_bbox


def test_validate_bbox_rejects_invalid_bounds():
    """
    Unit: bbox validation rejects invalid coordinate ordering.

    Checks:
    - min_lon >= max_lon raises ValidationError
    - min_lat >= max_lat raises ValidationError
    """
    # Longitude ordering rule must hold.
    with pytest.raises(ValidationError) as lon_exc:
        _validate_bbox(min_lon=-1.50, min_lat=53.70, max_lon=-1.50, max_lat=53.80)
    assert lon_exc.value.error == "INVALID_REQUEST"

    # Latitude ordering rule must also hold.
    with pytest.raises(ValidationError) as lat_exc:
        _validate_bbox(min_lon=-1.60, min_lat=53.80, max_lon=-1.50, max_lat=53.80)
    assert lat_exc.value.error == "INVALID_REQUEST"


def test_normalize_watchlist_mode_maps_aliases_correctly():
    """
    Unit: watchlist mode normalization maps aliases and rejects unsupported values.

    Checks:
    - "walking" -> "walk"
    - "car" -> "drive"
    - unsupported mode raises ValidationError
    """
    # Alias mapping for pedestrian mode.
    assert _normalize_watchlist_mode("walking", error_context="unit-test") == "walk"

    # Alias mapping for driving mode.
    assert _normalize_watchlist_mode("car", error_context="unit-test") == "drive"

    # Unknown mode should fail with a validation error.
    with pytest.raises(ValidationError) as exc_info:
        _normalize_watchlist_mode("scooter", error_context="unit-test")

    assert exc_info.value.error == "INVALID_TRAVEL_MODE"
