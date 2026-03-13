from datetime import date, datetime, time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import app.api_utils.report_events_db_utils as report_event_utils
from app.schemas.report_event_schemas import (
    ReportedCollisionPayload,
    ReportedCrimePayload,
    ReportedEventCreateRequest,
)


def _report_row(event_kind="crime", report_id=1, created_at=None):
    return {
        "id": report_id,
        "event_kind": event_kind,
        "reporter_type": "anonymous",
        "user_id": None,
        "reporter_email": None,
        "event_date": date(2025, 1, 15),
        "event_time": time(9, 30),
        "month": date(2025, 1, 1),
        "longitude": -1.55,
        "latitude": 53.8,
        "segment_id": 42,
        "snap_distance_m": 10.5,
        "description": "test report",
        "admin_approved": False,
        "moderation_status": "pending",
        "moderation_notes": None,
        "moderated_by": None,
        "moderated_at": None,
        "created_at": created_at or datetime(2025, 1, 20, 10, 15, 0),
        "crime_type": "theft" if event_kind == "crime" else None,
        "weather_condition": "Fine" if event_kind == "collision" else None,
        "light_condition": "Daylight" if event_kind == "collision" else None,
        "number_of_vehicles": 2 if event_kind == "collision" else None,
    }


def test_get_optional_current_user_rejects_non_bearer_scheme():
    credentials = SimpleNamespace(scheme="Basic", credentials="token")

    with pytest.raises(HTTPException) as exc_info:
        report_event_utils.get_optional_current_user(credentials=credentials, db=None)

    assert exc_info.value.status_code == 401


def test_require_admin_allows_admin_and_blocks_non_admin():
    assert report_event_utils.require_admin({"is_admin": True}) == {"is_admin": True}

    with pytest.raises(HTTPException) as exc_info:
        report_event_utils.require_admin({"is_admin": False})

    assert exc_info.value.status_code == 403


@pytest.mark.parametrize(
    "func,value,expected",
    [
        (report_event_utils.status_query, "pending", "pending"),
        (report_event_utils.event_kind_query, "crime", "crime"),
        (report_event_utils.reporter_type_query, "anonymous", "anonymous"),
    ],
)
def test_query_value_parsers_accept_valid_values(func, value, expected):
    assert func(value) == expected


@pytest.mark.parametrize(
    "func,value",
    [
        (report_event_utils.status_query, "queued"),
        (report_event_utils.event_kind_query, "incident"),
        (report_event_utils.reporter_type_query, "guest"),
    ],
)
def test_query_value_parsers_reject_invalid_values(func, value):
    with pytest.raises(HTTPException) as exc_info:
        func(value)

    assert exc_info.value.status_code == 400


def test_normalize_text_helpers():
    assert report_event_utils._normalize_required_text("  hello  ", "field") == "hello"
    assert report_event_utils._normalize_optional_text("  value  ") == "value"
    assert report_event_utils._normalize_optional_text("   ") is None

    with pytest.raises(HTTPException) as exc_info:
        report_event_utils._normalize_required_text("   ", "field")

    assert exc_info.value.status_code == 400


def test_validate_coordinates_and_bbox_helpers():
    report_event_utils._validate_coordinates(-1.55, 53.8)
    assert report_event_utils._validate_optional_bbox(None, None, None, None) is None

    bbox = report_event_utils._validate_optional_bbox(-1.6, 53.7, -1.5, 53.9)
    assert bbox["min_lon"] == -1.6

    with pytest.raises(HTTPException):
        report_event_utils._validate_coordinates(-181, 53.8)
    with pytest.raises(HTTPException):
        report_event_utils._validate_optional_bbox(-1.6, 53.7, None, 53.9)


def test_parse_month_and_cursor_helpers():
    assert report_event_utils._parse_month("2025-03", "from") == date(2025, 3, 1)
    assert report_event_utils._parse_cursor("2025-01-01T10:00:00|5")["cursor_id"] == 5

    with pytest.raises(HTTPException):
        report_event_utils._parse_month("03-2025", "from")
    with pytest.raises(HTTPException):
        report_event_utils._parse_cursor("bad-cursor")


def test_next_cursor_and_serialize_helpers():
    rows = [
        {"id": 3, "created_at": datetime(2025, 1, 3, 12, 0, 0)},
        {"id": 2, "created_at": datetime(2025, 1, 2, 12, 0, 0)},
    ]

    next_cursor = report_event_utils._next_cursor(rows, 1)

    assert next_cursor == "2025-01-03T12:00:00|3"
    assert report_event_utils._serialize_date(date(2025, 1, 1)) == "2025-01-01"
    assert report_event_utils._serialize_month(date(2025, 1, 1)) == "2025-01"
    assert report_event_utils._serialize_time(time(9, 5)) == "09:05"
    assert report_event_utils._serialize_timestamp(datetime(2025, 1, 1, 0, 0, 0)) == "2025-01-01T00:00:00"


def test_validate_create_payload_enforces_event_specific_fields():
    crime_payload = ReportedEventCreateRequest(
        event_kind="crime",
        event_date=date(2025, 1, 15),
        longitude=-1.55,
        latitude=53.8,
        crime=ReportedCrimePayload(crime_type="  theft  "),
    )
    report_event_utils._validate_create_payload(crime_payload)
    assert crime_payload.crime.crime_type == "theft"

    collision_payload = ReportedEventCreateRequest(
        event_kind="collision",
        event_date=date(2025, 1, 15),
        longitude=-1.55,
        latitude=53.8,
        collision=ReportedCollisionPayload(
            weather_condition="  Fine ",
            light_condition=" Daylight ",
            number_of_vehicles=2,
        ),
    )
    report_event_utils._validate_create_payload(collision_payload)
    assert collision_payload.collision.weather_condition == "Fine"

    with pytest.raises(HTTPException):
        report_event_utils._validate_create_payload(
            ReportedEventCreateRequest(
                event_kind="crime",
                event_date=date(2025, 1, 15),
                longitude=-1.55,
                latitude=53.8,
                collision=ReportedCollisionPayload(
                    weather_condition="Fine",
                    light_condition="Daylight",
                    number_of_vehicles=2,
                ),
            )
        )


def test_event_month():
    assert report_event_utils._event_month(date(2025, 1, 31)) == date(2025, 1, 1)


def test_report_select_and_serializers():
    sql = report_event_utils._report_select_sql()
    assert "FROM user_reported_events e" in sql

    crime_payload = report_event_utils._report_to_dict(_report_row("crime"), include_admin_fields=True)
    collision_payload = report_event_utils._report_to_dict(_report_row("collision"), include_admin_fields=False)
    feature = report_event_utils._report_to_feature(_report_row("crime"))

    assert crime_payload["details"]["crime_type"] == "theft"
    assert "user_id" in crime_payload
    assert collision_payload["details"]["weather_condition"] == "Fine"
    assert feature["type"] == "Feature"
    assert feature["geometry"]["coordinates"] == [-1.55, 53.8]
