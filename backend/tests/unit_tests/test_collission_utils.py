from datetime import date, time

import pytest
from fastapi import HTTPException

from app.api_utils import collission_utils
from tests.inmemory_db import InMemoryDB


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


def test_collision_snapshot_aggregates_counts_from_paged_results():
    db = InMemoryDB(
        {
            "/* collisions_analytics_snapshot */": [
                {
                    "rows": [
                        {
                            "collision_index": "c-3",
                            "month_date": date(2025, 2, 1),
                            "collision_severity": "Slight",
                            "road_type": "Single carriageway",
                            "weather_condition": "Fine",
                            "light_condition": "Daylight",
                            "lsoa_code": "LSOA-1",
                            "number_of_casualties": 1,
                            "casualty_severity_counts": '{"Slight": 1}',
                        },
                        {
                            "collision_index": "c-2",
                            "month_date": date(2025, 1, 1),
                            "collision_severity": "Serious",
                            "road_type": "Roundabout",
                            "weather_condition": "Rain",
                            "light_condition": "Darkness",
                            "lsoa_code": None,
                            "number_of_casualties": 2,
                            "casualty_severity_counts": '{"Serious": 1, "Fatal": 1}',
                        },
                        {
                            "collision_index": "c-1",
                            "month_date": date(2025, 1, 1),
                            "collision_severity": "Slight",
                            "road_type": "Single carriageway",
                            "weather_condition": "Fine",
                            "light_condition": "Daylight",
                            "lsoa_code": "LSOA-2",
                            "number_of_casualties": 0,
                            "casualty_severity_counts": '{}',
                        },
                    ]
                },
                {
                    "rows": [
                        {
                            "collision_index": "c-1",
                            "month_date": date(2025, 1, 1),
                            "collision_severity": "Slight",
                            "road_type": "Single carriageway",
                            "weather_condition": "Fine",
                            "light_condition": "Daylight",
                            "lsoa_code": "LSOA-2",
                            "number_of_casualties": 0,
                            "casualty_severity_counts": '{}',
                        }
                    ]
                },
            ]
        }
    )
    range_filter = collission_utils._resolve_from_to_filter("2025-01", "2025-02", required=True)

    snapshot = collission_utils._collision_snapshot(
        range_filter,
        bbox=None,
        severities=None,
        road_types=None,
        lsoa_codes=None,
        weather_conditions=None,
        light_conditions=None,
        road_surface_conditions=None,
        db=db,
        page_size=2,
    )

    assert snapshot["total_collisions"] == 3
    assert snapshot["total_casualties"] == 3
    assert snapshot["collisions_with_casualties"] == 2
    assert snapshot["unique_lsoas"] == 2
    assert snapshot["severity_counts"] == {"Slight": 2, "Serious": 1}
    assert snapshot["casualty_severity_counts"] == {"Slight": 1, "Serious": 1, "Fatal": 1}
    assert snapshot["monthly_counts"] == {"2025-01": 2, "2025-02": 1}
    assert len(db.executed_sql) == 2
