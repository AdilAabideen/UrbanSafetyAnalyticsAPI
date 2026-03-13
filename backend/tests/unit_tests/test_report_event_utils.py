from datetime import date, datetime, time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import InternalError

import app.api_utils.report_events_db_utils as report_event_utils
from app.schemas.report_event_schemas import (
    ReportedCollisionPayload,
    ReportedCrimePayload,
    ReportedEventCreateRequest,
    ReportedEventModerationRequest,
)
from tests.inmemory_db import InMemoryDB


class TransactionalInMemoryDB(InMemoryDB):
    def __init__(self, handlers):
        super().__init__(handlers)
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


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


def test_execute_returns_result_for_successful_query():
    db = TransactionalInMemoryDB({"SELECT 1": {"rows": [{"value": 1}]}})

    result = report_event_utils._execute(db, text("SELECT 1"), {})

    assert result.mappings().first()["value"] == 1


def test_execute_translates_database_errors_to_http_503():
    class BrokenDB:
        def __init__(self):
            self.rollback_calls = 0

        def execute(self, query, params):
            raise InternalError("SELECT 1", params, Exception("boom"))

        def rollback(self):
            self.rollback_calls += 1

    db = BrokenDB()

    with pytest.raises(HTTPException) as exc_info:
        report_event_utils._execute(db, text("SELECT 1"), {})

    assert exc_info.value.status_code == 503
    assert db.rollback_calls == 1


def test_get_optional_current_user_without_credentials_returns_none():
    db = TransactionalInMemoryDB({})

    assert report_event_utils.get_optional_current_user(credentials=None, db=db) is None


def test_get_optional_current_user_rejects_non_bearer_scheme():
    credentials = SimpleNamespace(scheme="Basic", credentials="token")

    with pytest.raises(HTTPException) as exc_info:
        report_event_utils.get_optional_current_user(credentials=credentials, db=TransactionalInMemoryDB({}))

    assert exc_info.value.status_code == 401


def test_get_optional_current_user_returns_user_for_valid_token(monkeypatch):
    credentials = SimpleNamespace(scheme="Bearer", credentials="token")
    db = TransactionalInMemoryDB(
        {
            "FROM users u": {
                "rows": [
                    {
                        "id": 7,
                        "email": "user@example.com",
                        "is_admin": False,
                        "created_at": datetime(2025, 1, 1, 0, 0, 0),
                    }
                ]
            }
        }
    )
    monkeypatch.setattr(report_event_utils, "decode_access_token", lambda _token: {"sub": "7"})

    user = report_event_utils.get_optional_current_user(credentials=credentials, db=db)

    assert user["id"] == 7
    assert user["email"] == "user@example.com"


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


def test_event_month_and_snap_to_segment():
    assert report_event_utils._event_month(date(2025, 1, 31)) == date(2025, 1, 1)

    db = TransactionalInMemoryDB(
        {
            "FROM road_segments_4326": {
                "rows": [
                    {
                        "id": 9,
                        "snap_distance_m": 55.0,
                    }
                ]
            }
        }
    )
    segment_id, distance = report_event_utils._snap_to_segment(db, -1.55, 53.8)

    assert segment_id == 9
    assert distance == 55.0


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


def test_get_report_by_id_returns_report_and_404_when_missing():
    db = TransactionalInMemoryDB({"WHERE e.id = :report_id": {"rows": [_report_row("crime", report_id=5)]}})

    report = report_event_utils.get_report_by_id(db, 5)
    assert report["id"] == 5

    empty_db = TransactionalInMemoryDB({"WHERE e.id = :report_id": {"rows": []}})
    with pytest.raises(HTTPException) as exc_info:
        report_event_utils.get_report_by_id(empty_db, 999)
    assert exc_info.value.status_code == 404


def test_create_report_record_for_crime_and_collision_paths():
    crime_db = TransactionalInMemoryDB(
        {
            "FROM road_segments_4326": {"rows": [{"id": 9, "snap_distance_m": 10.0}]},
            "INSERT INTO user_reported_events": {"rows": [{"id": 11}]},
            "INSERT INTO user_reported_crime_details": {"rows": []},
            "WHERE e.id = :report_id": {"rows": [_report_row("crime", report_id=11)]},
        }
    )
    crime_payload = ReportedEventCreateRequest(
        event_kind="crime",
        event_date=date(2025, 1, 15),
        longitude=-1.55,
        latitude=53.8,
        description="  some note  ",
        crime=ReportedCrimePayload(crime_type="theft"),
    )

    created_crime = report_event_utils.create_report_record(crime_db, crime_payload, current_user=None)

    assert created_crime["id"] == 11
    assert crime_db.commit_calls == 1

    collision_db = TransactionalInMemoryDB(
        {
            "FROM road_segments_4326": {"rows": [{"id": 10, "snap_distance_m": 15.0}]},
            "INSERT INTO user_reported_events": {"rows": [{"id": 12}]},
            "INSERT INTO user_reported_collision_details": {"rows": []},
            "WHERE e.id = :report_id": {"rows": [_report_row("collision", report_id=12)]},
        }
    )
    collision_payload = ReportedEventCreateRequest(
        event_kind="collision",
        event_date=date(2025, 1, 15),
        longitude=-1.55,
        latitude=53.8,
        collision=ReportedCollisionPayload(
            weather_condition="Fine",
            light_condition="Daylight",
            number_of_vehicles=2,
        ),
    )

    created_collision = report_event_utils.create_report_record(
        collision_db,
        collision_payload,
        current_user={"id": 99, "is_admin": True},
    )

    assert created_collision["id"] == 12
    assert collision_db.commit_calls == 1


def test_list_reports_and_list_own_reports():
    db = TransactionalInMemoryDB(
        {
            "ORDER BY e.created_at DESC, e.id DESC": {
                "rows": [
                    _report_row("crime", report_id=3, created_at=datetime(2025, 1, 3, 10, 0, 0)),
                    _report_row("crime", report_id=2, created_at=datetime(2025, 1, 2, 10, 0, 0)),
                ]
            }
        }
    )

    listing = report_event_utils._list_reports(db, ["e.user_id = :user_id"], {"user_id": 7}, limit=1)
    assert len(listing["items"]) == 1
    assert listing["nextCursor"] == "2025-01-03T10:00:00|3"

    own = report_event_utils.list_own_reports(db, 7, "pending", "crime", 1, None)
    assert own["meta"]["limit"] == 1
    assert own["meta"]["filters"]["status"] == "pending"


def test_list_admin_reports_validates_months_and_returns_payload():
    db = TransactionalInMemoryDB(
        {
            "ORDER BY e.created_at DESC, e.id DESC": {
                "rows": [_report_row("crime", report_id=4, created_at=datetime(2025, 1, 4, 10, 0, 0))]
            }
        }
    )

    payload = report_event_utils.list_admin_reports(
        db,
        status_value="pending",
        event_kind="crime",
        reporter_type="anonymous",
        from_month="2025-01",
        to_month="2025-01",
        limit=10,
        cursor=None,
    )
    assert payload["meta"]["returned"] == 1

    with pytest.raises(HTTPException):
        report_event_utils.list_admin_reports(db, None, None, None, "2025-02", "2025-01", 10, None)


def test_moderate_report_updates_and_returns_record():
    db = TransactionalInMemoryDB(
        {
            "UPDATE user_reported_events": {"rows": [{"id": 8}]},
            "WHERE e.id = :report_id": {"rows": [_report_row("crime", report_id=8)]},
        }
    )
    payload = ReportedEventModerationRequest(moderation_status="approved", moderation_notes="  reviewed ")

    report = report_event_utils.moderate_report(db, report_id=8, moderator_id=1, payload=payload)

    assert report["id"] == 8
    assert db.commit_calls == 1


def test_list_user_event_features_returns_geojson_payload():
    db = TransactionalInMemoryDB(
        {
            "/* user_events_geojson */": {
                "rows": [
                    _report_row("crime", report_id=21),
                ]
            }
        }
    )

    payload = report_event_utils.list_user_event_features(
        db,
        status_value="pending",
        event_kind="crime",
        reporter_type="anonymous",
        from_month="2025-01",
        to_month="2025-01",
        admin_approved=False,
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.9,
        limit=10,
    )

    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["returned"] == 1
    assert payload["features"][0]["properties"]["event_kind"] == "crime"

    with pytest.raises(HTTPException):
        report_event_utils.list_user_event_features(
            db,
            status_value=None,
            event_kind=None,
            reporter_type=None,
            from_month="2025-01",
            to_month=None,
            admin_approved=None,
            min_lon=None,
            min_lat=None,
            max_lon=None,
            max_lat=None,
            limit=10,
        )
