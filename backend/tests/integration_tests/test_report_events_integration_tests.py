"""
Integration tests for reported-events API using InMemoryDB-backed query handlers.

Why InMemoryDB here:
- Exercises API -> service -> repository orchestration and response contracts.
- Avoids mutating real Postgres data during test runs.
"""

from datetime import datetime

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from app.services.auth_service import create_access_token
from tests.inmemory_db import InMemoryDB


# Disable startup DB bootstrap side effects for these mocked integration tests.
app.router.on_startup.clear()
client = TestClient(app)


def _run_with_db(handlers, method, url, json=None, headers=None):
    """
    Execute one API request with query handlers wired through InMemoryDB.

    `handlers` maps SQL needle -> payload, where payload can be:
    - dict with rows/scalar
    - list of payloads for sequential query calls
    - callable(params) returning a payload
    """

    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url, json=json, headers=headers)
    finally:
        app.dependency_overrides.clear()


def _report_row(*, report_id: int, event_kind: str = "crime", user_id: int = 1, email: str = "u@example.com"):
    """Build one repository-style report row with all required selected columns."""
    return {
        "id": report_id,
        "event_kind": event_kind,
        "reporter_type": "authenticated" if user_id else "anonymous",
        "user_id": user_id,
        "reporter_email": email if user_id else None,
        "event_date": datetime(2026, 1, 15).date(),
        "event_time": datetime(2026, 1, 15, 9, 30).time(),
        "month": datetime(2026, 1, 1).date(),
        "longitude": -1.55,
        "latitude": 53.80,
        "segment_id": 77,
        "snap_distance_m": 6.2,
        "description": "integration-test-report",
        "admin_approved": False,
        "moderation_status": "pending",
        "moderation_notes": None,
        "moderated_by": None,
        "moderated_at": None,
        "created_at": datetime(2026, 1, 15, 10, 0, 0),
        "crime_type": "theft" if event_kind == "crime" else None,
        "weather_condition": "Fine" if event_kind == "collision" else None,
        "light_condition": "Daylight" if event_kind == "collision" else None,
        "number_of_vehicles": 2 if event_kind == "collision" else None,
    }


def test_create_reported_event_authenticated_collision_returns_201():
    """
    Integration:
    Authenticated user creates collision report and receives created payload (201).
    """
    user_id = 11
    token = create_access_token(user_id)
    handlers = {
        # Auth path: resolve bearer token subject to user row.
        "WHERE u.id = :user_id": {"rows": [{"id": user_id, "email": "driver@example.com", "is_admin": False, "created_at": datetime(2026, 1, 1, 9, 0, 0)}]},
        # Report create path: snap, insert base, insert collision details, fetch created row.
        "ORDER BY rs.geom_4326 <->": {"rows": [{"id": 77, "snap_distance_m": 6.2}]},
        "INSERT INTO user_reported_events": {"rows": [{"id": 701}]},
        "INSERT INTO user_reported_collision_details": {"rows": []},
        "WHERE e.id = :report_id": {"rows": [_report_row(report_id=701, event_kind="collision", user_id=user_id, email="driver@example.com")]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/reported-events",
        json={
            "event_kind": "collision",
            "event_date": "2026-01-15",
            "event_time": "09:30:00",
            "longitude": -1.55,
            "latitude": 53.80,
            "description": "collision event",
            "collision": {
                "weather_condition": "Fine",
                "light_condition": "Daylight",
                "number_of_vehicles": 2,
            },
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["report"]["id"] == 701
    assert payload["report"]["event_kind"] == "collision"
    assert payload["report"]["reporter_type"] == "authenticated"
    assert payload["report"]["details"]["number_of_vehicles"] == 2


def test_admin_moderation_patch_updates_report_successfully():
    """
    Integration:
    Admin moderation PATCH updates status/approval and returns updated report.
    """
    admin_id = 2
    token = create_access_token(admin_id)
    updated_row = _report_row(report_id=702, event_kind="crime", user_id=7, email="reporter@example.com")
    updated_row.update(
        {
            "admin_approved": True,
            "moderation_status": "approved",
            "moderation_notes": "Reviewed by admin",
            "moderated_by": admin_id,
            "moderated_at": datetime(2026, 1, 16, 11, 15, 0),
        }
    )

    handlers = {
        # Auth path for admin token.
        "WHERE u.id = :user_id": {"rows": [{"id": admin_id, "email": "admin@example.com", "is_admin": True, "created_at": datetime(2026, 1, 1, 9, 0, 0)}]},
        # Moderation update then fetch by id for API response.
        "UPDATE user_reported_events": {"rows": [{"id": 702}]},
        "WHERE e.id = :report_id": {"rows": [updated_row]},
    }

    response = _run_with_db(
        handlers,
        "PATCH",
        "/admin/reported-events/702/moderation",
        json={"moderation_status": "approved", "moderation_notes": "Reviewed by admin"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["report"]["id"] == 702
    assert payload["report"]["moderation_status"] == "approved"
    assert payload["report"]["admin_approved"] is True
    assert payload["report"]["moderated_by"] == admin_id


def test_admin_moderation_patch_forbids_non_admin():
    """
    Integration:
    Non-admin caller must be rejected on admin moderation endpoint.
    """
    non_admin_id = 3
    token = create_access_token(non_admin_id)
    handlers = {
        # Auth resolves valid user, but is_admin=False should block moderation.
        "WHERE u.id = :user_id": {
            "rows": [
                {
                    "id": non_admin_id,
                    "email": "user@example.com",
                    "is_admin": False,
                    "created_at": datetime(2026, 1, 1, 9, 0, 0),
                }
            ]
        }
    }

    response = _run_with_db(
        handlers,
        "PATCH",
        "/admin/reported-events/703/moderation",
        json={"moderation_status": "rejected", "moderation_notes": "Not valid"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403
    payload = response.json()
    assert payload["error"] == "FORBIDDEN"


def test_read_my_reported_events_returns_only_current_users_reports():
    """
    Integration:
    /reported-events/mine applies current-user scoping and returns user-specific list payload.
    """
    user_id = 44
    token = create_access_token(user_id)
    row_1 = _report_row(report_id=801, event_kind="crime", user_id=user_id, email="mine@example.com")
    row_2 = _report_row(report_id=802, event_kind="collision", user_id=user_id, email="mine@example.com")

    def _mine_page(params):
        # Validate that the service passed current-user filter to repository layer.
        assert params["user_id"] == user_id
        assert params["row_limit"] == 3  # limit=2 -> over-fetch by 1
        return {"rows": [row_1, row_2]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [{"id": user_id, "email": "mine@example.com", "is_admin": False, "created_at": datetime(2026, 1, 1, 9, 0, 0)}]},
        "e.user_id = :user_id": _mine_page,
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/reported-events/mine?limit=2",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 2
    assert payload["meta"]["limit"] == 2
    assert [item["id"] for item in payload["items"]] == [801, 802]


def test_read_admin_reported_events_applies_filters():
    """
    Integration:
    Admin list endpoint should apply status/event/reporter/month filters and return list metadata.
    """
    admin_id = 55
    token = create_access_token(admin_id)
    admin_row = {"id": admin_id, "email": "admin@example.com", "is_admin": True, "created_at": datetime(2026, 1, 1, 9, 0, 0)}
    filtered_row = _report_row(report_id=901, event_kind="crime", user_id=21, email="filtered@example.com")
    filtered_row.update({"reporter_type": "authenticated", "moderation_status": "approved", "admin_approved": True})

    def _admin_page(params):
        # Verify filter plumbing reached repository query params.
        assert params["status"] == "approved"
        assert params["event_kind"] == "crime"
        assert params["reporter_type"] == "authenticated"
        assert str(params["from_month_date"]) == "2025-01-01"
        assert str(params["to_month_date"]) == "2025-03-01"
        assert params["row_limit"] == 3
        return {"rows": [filtered_row]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [admin_row]},
        "e.reporter_type = :reporter_type": _admin_page,
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/admin/reported-events?status=approved&event_kind=crime&reporter_type=authenticated&from=2025-01&to=2025-03&limit=2",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["filters"]["status"] == "approved"
    assert payload["meta"]["filters"]["event_kind"] == "crime"
    assert payload["meta"]["filters"]["reporter_type"] == "authenticated"
    assert payload["meta"]["returned"] == 1
    assert payload["items"][0]["id"] == 901


def test_read_user_events_returns_geojson_feature_collection():
    """
    Integration:
    /user-events returns GeoJSON FeatureCollection contract with populated feature entries.
    """
    row = _report_row(report_id=1001, event_kind="crime", user_id=8, email="geo@example.com")
    handlers = {
        # Unique SQL marker in repository fetch_user_event_rows.
        "/* user_events_geojson */": {"rows": [row]}
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/user-events?from=2025-01&to=2025-03&limit=10",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["returned"] == 1
    assert payload["features"][0]["type"] == "Feature"
    assert payload["features"][0]["geometry"]["type"] == "Point"
