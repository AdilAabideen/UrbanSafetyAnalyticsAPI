"""
Unit tests for tiles service helper logic.

Notes:
- These tests target pure helper behavior only.
- They do not use an in-memory DB and do not hit HTTP routes.
"""

import pytest

from app.errors import ValidationError
from app.services import tile_service


def test_unit_resolve_month_filter_rejects_missing_window_when_required():
    """
    Test 8 (Unit):
    Ensure month window validation fails when risk mode requires it.

    Current API removed standalone `month`, so this checks the equivalent
    guardrail for required startMonth/endMonth pairs.
    """
    with pytest.raises(ValidationError) as exc_info:
        tile_service.resolve_month_filter(
            startMonth=None,
            endMonth=None,
            require_window=True,
        )

    err = exc_info.value
    assert err.status_code == 400
    assert err.error == "MISSING_MONTH_FILTER"


def test_unit_validate_tile_coordinates_rejects_out_of_range_xy():
    """
    Test 9 (Unit):
    Validate x/y bounds checking in isolation from the API layer.
    """
    with pytest.raises(ValidationError) as exc_info:
        tile_service.validate_tile_coordinates(z=2, x=5, y=0)

    err = exc_info.value
    assert err.status_code == 400
    assert err.error == "INVALID_TILE_COORDINATES"
