from datetime import date, datetime, time

from fastapi.testclient import TestClient

from app.api_utils import report_event_utils
from app.api_utils.auth_utils import get_current_user
from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


class TransactionalInMemoryDB(InMemoryDB):
    def __init__(self, handlers):
        super().__init__(handlers)
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def _report_row(event_kind="crime", report_id=1, created_at=None, moderation_status="pending"):
    return {
        "id": report_id,
        "event_kind": event_kind,
        "reporter_type": "anonymous",
        "user_id": 7,
        "reporter_email": "user@example.com",
        "event_date": date(2025, 1, 15),
        "event_time": time(9, 30),
        "month": date(2025, 1, 1),
        "longitude": -1.55,
        "latitude": 53.8,
        "segment_id": 42,
        "snap_distance_m": 10.5,
        "description": "test report",
        "admin_approved": False,
        "moderation_status": moderation_status,
        "moderation_notes": None,
        "moderated_by": None,
        "moderated_at": None,
        "created_at": created_at or datetime(2025, 1, 20, 10, 15, 0),
        "crime_type": "theft" if event_kind == "crime" else None,
        "weather_condition": "Fine" if event_kind == "collision" else None,
        "light_condition": "Daylight" if event_kind == "collision" else None,
        "number_of_vehicles": 2 if event_kind == "collision" else None,
    }


def _run_with_db(handlers, method, url, *, json=None, overrides=None):
    def _override_get_db():
        yield TransactionalInMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    if overrides:
        for dep, override in overrides.items():
            app.dependency_overrides[dep] = override

    try:
        return client.request(method, url, json=json)
    finally:
        app.dependency_overrides.clear()


def test_create_reported_event_returns_report_payload():
    response = _run_with_db(
        {
            "FROM road_segments_4326": {"rows": [{"id": 9, "snap_distance_m": 12.0}]},
            "INSERT INTO user_reported_events": {"rows": [{"id": 100}]},
            "INSERT INTO user_reported_crime_details": {"rows": []},
            "WHERE e.id = :report_id": {"rows": [_report_row("crime", report_id=100)]},
        },
        "POST",
        "/reported-events",
        json={
            "event_kind": "crime",
            "event_date": "2025-01-15",
            "longitude": -1.55,
            "latitude": 53.8,
            "description": "suspicious activity",
            "crime": {"crime_type": "theft"},
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["report"]["id"] == 100
    assert payload["report"]["event_kind"] == "crime"


def test_read_my_reported_events_returns_paginated_payload():
    response = _run_with_db(
        {
            "ORDER BY e.created_at DESC, e.id DESC": {
                "rows": [
                    _report_row("crime", report_id=3, created_at=datetime(2025, 1, 3, 10, 0, 0)),
                    _report_row("crime", report_id=2, created_at=datetime(2025, 1, 2, 10, 0, 0)),
                ]
            }
        },
        "GET",
        "/reported-events/mine?status=pending&event_kind=crime&limit=1",
        overrides={get_current_user: lambda: {"id": 7, "is_admin": False}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 1
    assert payload["meta"]["nextCursor"] == "2025-01-03T10:00:00|3"


def test_read_admin_reported_events_returns_admin_fields():
    response = _run_with_db(
        {
            "ORDER BY e.created_at DESC, e.id DESC": {
                "rows": [
                    _report_row("crime", report_id=4, created_at=datetime(2025, 1, 4, 10, 0, 0)),
                ]
            }
        },
        "GET",
        "/admin/reported-events?from=2025-01&to=2025-01&limit=1",
        overrides={report_event_utils.require_admin: lambda: {"id": 1, "is_admin": True}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 1
    assert "user_id" in payload["items"][0]


def test_moderate_reported_event_returns_updated_record():
    response = _run_with_db(
        {
            "UPDATE user_reported_events": {"rows": [{"id": 5}]},
            "WHERE e.id = :report_id": {"rows": [_report_row("crime", report_id=5, moderation_status="approved")]},
        },
        "PATCH",
        "/admin/reported-events/5/moderation",
        json={"moderation_status": "approved", "moderation_notes": "checked"},
        overrides={report_event_utils.require_admin: lambda: {"id": 1, "is_admin": True}},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["report"]["id"] == 5
    assert payload["report"]["moderation_status"] == "approved"


def test_user_events_returns_geojson_feature_collection():
    response = _run_with_db(
        {
            "/* user_events_geojson */": {
                "rows": [
                    _report_row("crime", report_id=21),
                ]
            }
        },
        "GET",
        "/user-events?from=2025-01&to=2025-01&limit=10",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["returned"] == 1
    assert payload["features"][0]["properties"]["id"] == 21
