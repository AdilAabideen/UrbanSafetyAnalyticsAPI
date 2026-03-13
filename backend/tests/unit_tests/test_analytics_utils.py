import pytest

from app.api_utils import analytics_utils


def test_parse_month_accepts_valid_yyyy_mm():
    parsed = analytics_utils._parse_month("2025-03", "from")

    assert parsed.year == 2025
    assert parsed.month == 3
    assert parsed.day == 1


def test_parse_month_rejects_invalid_format():
    with pytest.raises(analytics_utils.AnalyticsAPIError) as exc_info:
        analytics_utils._parse_month("2025/03", "from")

    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "INVALID_MONTH_FORMAT"


def test_validate_month_window_rejects_large_span():
    with pytest.raises(analytics_utils.AnalyticsAPIError) as exc_info:
        analytics_utils._validate_month_window("2023-01", "2025-12")

    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "RANGE_TOO_LARGE"


def test_validate_month_window_returns_normalized_payload():
    payload = analytics_utils._validate_month_window("2025-01", "2025-03")

    assert payload["from"] == "2025-01"
    assert payload["to"] == "2025-03"
    assert payload["span_months"] == 3


def test_optional_bbox_requires_all_or_none():
    with pytest.raises(analytics_utils.AnalyticsAPIError) as exc_info:
        analytics_utils._optional_bbox(-1.6, None, -1.5, 53.9)

    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "INVALID_BBOX"


def test_optional_bbox_validates_when_all_values_supplied():
    bbox = analytics_utils._optional_bbox(-1.6, 53.7, -1.5, 53.9)

    assert bbox == {
        "min_lon": -1.6,
        "min_lat": 53.7,
        "max_lon": -1.5,
        "max_lat": 53.9,
    }


def test_applied_collision_weight_depends_on_mode_and_toggle():
    assert analytics_utils._applied_collision_weight(False, "drive", 0.7) == 0.0
    assert analytics_utils._applied_collision_weight(True, "walk", 0.7) == 0.0
    assert analytics_utils._applied_collision_weight(True, "drive", 0.0) == 1.0
    assert analytics_utils._applied_collision_weight(True, "drive", 0.3) == 0.3


def test_band_from_pct_thresholds():
    assert analytics_utils._band_from_pct(0.79) == "green"
    assert analytics_utils._band_from_pct(0.80) == "amber"
    assert analytics_utils._band_from_pct(0.95) == "red"
