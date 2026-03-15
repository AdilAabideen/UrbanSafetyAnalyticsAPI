"""
Integration + regression tests for /tiles/roads.

Notes:
- These tests intentionally use the real app wiring and DB dependency.
- No in-memory DB fixture is used.
- Scenarios are documented to explain behavior and protect API contracts.
"""

from fastapi.testclient import TestClient

from app.main import app
from app.schemas.tiles_schemas import MVT_MEDIA_TYPE


client = TestClient(app)


def _assert_mvt_response_ok(response):
    """Shared assertion helper for successful MVT tile responses."""
    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith(MVT_MEDIA_TYPE)
    assert isinstance(response.content, (bytes, bytearray))


def test_integration_tiles_roads_without_risk_returns_successfully():
    """
    Test 2 (Integration):
    Validate the standard roads tile path works without risk toggles.
    """
    response = client.get("/tiles/roads/9/252/165.mvt")

    _assert_mvt_response_ok(response)
    # Cache header is part of the external contract for tile responses.
    assert "cache-control" in response.headers


def test_integration_tiles_with_risk_single_month_range_returns_successfully():
    """
    Test 3 (Integration):
    Validate risk tile generation for a single month via start=end.

    Current API removed `month`; single-month mode is represented by
    startMonth and endMonth set to the same value.
    """
    response = client.get(
        "/tiles/roads/9/252/165.mvt",
        params={
            "crime": "true",
            "collisions": "true",
            "userReportedEvents": "true",
            "startMonth": "2026-03",
            "endMonth": "2026-03",
        },
    )

    _assert_mvt_response_ok(response)


def test_integration_tiles_with_risk_date_range_returns_successfully():
    """
    Test 4 (Integration):
    Validate risk tile generation with a valid month range.
    """
    response = client.get(
        "/tiles/roads/9/252/165.mvt",
        params={
            "crime": "true",
            "collisions": "true",
            "userReportedEvents": "true",
            "startMonth": "2026-01",
            "endMonth": "2026-03",
        },
    )

    _assert_mvt_response_ok(response)


def test_integration_tiles_risk_enabled_without_month_window_returns_400():
    """
    Test 5 (Integration):
    Validate month-window requirement when any risk toggle is enabled.
    """
    response = client.get(
        "/tiles/roads/9/252/165.mvt",
        params={"crime": "true"},
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload.get("error") == "MISSING_MONTH_FILTER"
    assert "startMonth and endMonth are required" in payload.get("message", "")


def test_integration_tiles_invalid_coordinates_return_400():
    """
    Test 6 (Integration):
    Validate slippy tile coordinate bounds checking.
    """
    # z=2 permits x in [0,3], so x=5 must be rejected by service validation.
    response = client.get("/tiles/roads/2/5/0.mvt")

    assert response.status_code == 400
    payload = response.json()
    assert payload.get("error") == "INVALID_TILE_COORDINATES"
    assert "out of range" in payload.get("message", "").lower()


def test_integration_tiles_invalid_month_format_returns_400():
    """
    Test 7 (Integration):
    Validate month parsing and request validation for malformed month input.
    """
    response = client.get(
        "/tiles/roads/9/252/165.mvt",
        params={
            "crime": "true",
            "startMonth": "2026-13",  # invalid month
            "endMonth": "2026-03",
        },
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload.get("error") == "INVALID_MONTH_FORMAT"
    assert "startMonth must be in YYYY-MM format" in payload.get("message", "")


def test_regression_tiles_removed_month_parameter_returns_400():
    """
    Test 10 (Regression):
    Lock in the migration behavior that legacy `month` is rejected.

    Why this exists:
    - Prevents silent fallback to deprecated behavior.
    - Ensures clients get a clear migration error.
    """
    response = client.get(
        "/tiles/roads/9/252/165.mvt",
        params={"month": "2026-03"},
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload.get("error") == "MONTH_PARAMETER_REMOVED"
