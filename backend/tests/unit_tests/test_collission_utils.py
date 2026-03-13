from datetime import date, time

import pytest
from fastapi import HTTPException

import app.api_utils.collission_db_utils as collission_utils


def test_collision_time_value_formats_time_objects():
    assert collission_utils._collision_time_value(time(9, 5)) == "09:05"
    assert collission_utils._collision_time_value(None) is None


def test_parse_collision_cursor_accepts_valid_cursor():
    parsed = collission_utils._parse_collision_cursor("2025-03|abc-123")

    assert parsed["cursor_collision_index"] == "abc-123"
    assert parsed["cursor_month"] == date(2025, 3, 1)


def test_parse_collision_cursor_rejects_invalid_cursor():
    with pytest.raises(HTTPException) as exc_info:
        collission_utils._parse_collision_cursor("2025-03")

    assert exc_info.value.status_code == 400
    assert "YYYY-MM|collision_index" in exc_info.value.detail


def test_collision_filter_values_splits_and_trims_tokens():
    values = collission_utils._collision_filter_values(
        [" Slight , Serious "],
        ["Roundabout"],
        ["LSOA-1"],
        ["Fine"],
        ["Daylight"],
        ["Dry"],
    )

    assert values[0] == ["Slight", "Serious"]
    assert values[1] == ["Roundabout"]


def test_collision_analytics_response_filters_serializes_bbox():
    range_filter = {"from": "2025-01", "to": "2025-02"}
    bbox = {"min_lon": -1.6, "min_lat": 53.7, "max_lon": -1.5, "max_lat": 53.9}

    payload = collission_utils._collision_analytics_response_filters(
        range_filter,
        bbox,
        ["Slight"],
        ["Roundabout"],
        ["LSOA-1"],
        ["Fine"],
        ["Daylight"],
        ["Dry"],
    )

    assert payload["from"] == "2025-01"
    assert payload["to"] == "2025-02"
    assert payload["bbox"]["maxLat"] == 53.9
