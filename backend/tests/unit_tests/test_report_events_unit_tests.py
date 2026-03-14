"""
Unit tests for report-events pure validation/parsing helpers.

These tests intentionally avoid DB and HTTP routing to isolate small logic units.
"""

from datetime import date

import pytest

from app.errors import ValidationError
from app.schemas.report_event_schemas import (
    ReportedCollisionPayload,
    ReportedEventCreateRequest,
)
from app.services.report_events_service import parse_cursor, validate_create_payload, validate_optional_bbox


def test_create_reported_event_rejects_mismatched_details():
    """
    Unit: cross-field payload rule.

    A crime event must provide crime details and must NOT provide collision details.
    This verifies validation logic directly without API/DB involvement.
    """
    payload = ReportedEventCreateRequest(
        event_kind="crime",
        event_date=date(2026, 1, 15),
        longitude=-1.55,
        latitude=53.80,
        collision=ReportedCollisionPayload(
            weather_condition="Fine",
            light_condition="Daylight",
            number_of_vehicles=2,
        ),
    )

    with pytest.raises(ValidationError) as exc_info:
        validate_create_payload(payload)

    assert exc_info.value.error == "INVALID_REQUEST"


def test_validate_optional_bbox_rejects_partial_bbox():
    """
    Unit: bbox validation.

    BBox filters are all-or-nothing. Partial bounds should fail immediately.
    """
    with pytest.raises(ValidationError) as exc_info:
        validate_optional_bbox(min_lon=-1.6, min_lat=53.7, max_lon=None, max_lat=53.9)

    assert exc_info.value.error == "INVALID_REQUEST"
    assert exc_info.value.details["field"] == "bbox"


def test_parse_cursor_rejects_invalid_format():
    """
    Unit: cursor parsing.

    Cursor must use `created_at|id` format. Any malformed cursor should fail.
    """
    with pytest.raises(ValidationError) as exc_info:
        parse_cursor("bad-cursor-format")

    assert exc_info.value.error == "INVALID_REQUEST"
    assert exc_info.value.details["field"] == "cursor"
