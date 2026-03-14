from uuid import uuid4

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from app.schemas.tiles_schemas import MVT_MEDIA_TYPE
from app.services.auth_service import create_access_token
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def test_smoke_auth_register_endpoint():
    """Smoke-test auth registration endpoint wiring with a minimal valid payload."""
    email = f"smoke-auth-{uuid4().hex[:12]}@example.com"
    response = client.post(
        "/auth/register",
        json={
            "email": email,
            "password": "smoke-pass-123",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "user" in payload
    assert payload["user"]["email"] == email


def test_smoke_tiles_route_basic_reachable():
    """
    Smoke Test 1 (tiles): prove the tiles route is alive and returns byte content.

    Why this exists:
    - Fast signal that routing + dependency wiring + DB path are reachable.
    - Mirrors production usage by calling the real FastAPI app (no in-memory DB).
    """
    response = client.get("/tiles/roads/9/252/165.mvt")

    # Basic route health checks for MVT output.
    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith(MVT_MEDIA_TYPE)
    assert isinstance(response.content, (bytes, bytearray))


def test_create_reported_event_anonymous_crime_returns_201():
    """
    Smoke Test (reported-events):
    Proves the anonymous create-report endpoint is alive end-to-end through routing,
    request parsing, write path invocation, and response serialization.

    Important:
    - This smoke test uses InMemoryDB so no real database rows are written.
    """

    # Minimal helper to run one request with fake DB wiring.
    def _override_get_db():
        handlers = {
            # Simulate nearest-segment lookup used during report creation.
            "ORDER BY rs.geom_4326 <->": {"rows": [{"id": 11, "snap_distance_m": 8.5}]},
            # Simulate insert into base events table and return generated id.
            "INSERT INTO user_reported_events": {"rows": [{"id": 501}]},
            # Simulate insert into crime detail table.
            "INSERT INTO user_reported_crime_details": {"rows": []},
            # Simulate fetch-by-id used to shape final response payload.
            "WHERE e.id = :report_id": {
                "rows": [
                    {
                        "id": 501,
                        "event_kind": "crime",
                        "reporter_type": "anonymous",
                        "user_id": None,
                        "reporter_email": None,
                        "event_date": "2026-01-15",
                        "event_time": None,
                        "month": "2026-01-01",
                        "longitude": -1.55,
                        "latitude": 53.80,
                        "segment_id": 11,
                        "snap_distance_m": 8.5,
                        "description": "smoke-anon-crime",
                        "admin_approved": False,
                        "moderation_status": "pending",
                        "moderation_notes": None,
                        "moderated_by": None,
                        "moderated_at": None,
                        "created_at": "2026-01-15T10:00:00",
                        "crime_type": "theft",
                        "weather_condition": None,
                        "light_condition": None,
                        "number_of_vehicles": None,
                    }
                ]
            },
        }
        yield InMemoryDB(handlers)

    # Override FastAPI DB dependency for this one request only.
    app.dependency_overrides[get_db] = _override_get_db
    try:
        response = client.post(
            "/reported-events",
            json={
                "event_kind": "crime",
                "event_date": "2026-01-15",
                "longitude": -1.55,
                "latitude": 53.80,
                "description": "smoke-anon-crime",
                "crime": {"crime_type": "theft"},
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    payload = response.json()
    assert payload["report"]["id"] == 501
    assert payload["report"]["event_kind"] == "crime"


def test_create_watchlist_smoke_returns_201():
    """
    Smoke Test (watchlists):
    Prove POST /watchlists is alive end-to-end with auth + DB write path.

    Why this exists:
    - Fast verification that routing, auth dependency, write orchestration,
      and response serialization are wired correctly.
    - Uses InMemoryDB to avoid touching a real database.
    """

    user_id = 1201
    token = create_access_token(user_id)

    # Build a persisted watchlist row returned by repository fetch-after-create.
    persisted_row = {
        "id": 9101,
        "user_id": user_id,
        "name": "Smoke Watchlist",
        "min_lon": -1.60,
        "min_lat": 53.70,
        "max_lon": -1.50,
        "max_lat": 53.80,
        "start_month": None,
        "end_month": None,
        "crime_types": None,
        "travel_mode": None,
        "include_collisions": False,
        "baseline_months": 6,
        "created_at": "2026-03-14T10:00:00",
    }

    def _override_get_db():
        handlers = {
            # Auth resolution for get_current_user.
            "WHERE u.id = :user_id": {
                "rows": [
                    {
                        "id": user_id,
                        "email": "smoke-watchlist@example.com",
                        "is_admin": False,
                        "created_at": "2026-01-01T00:00:00",
                    }
                ]
            },
            # Insert base watchlist row.
            "INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)": {
                "rows": [{"id": 9101}]
            },
            # Fetch created row for final response shaping.
            "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": [persisted_row]},
        }
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        response = client.post(
            "/watchlists",
            json={
                "name": "Smoke Watchlist",
                "min_lon": -1.60,
                "min_lat": 53.70,
                "max_lon": -1.50,
                "max_lat": 53.80,
            },
            headers={"Authorization": f"Bearer {token}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    payload = response.json()
    assert "watchlist" in payload
    assert payload["watchlist"]["id"] == 9101
    assert payload["watchlist"]["name"] == "Smoke Watchlist"
