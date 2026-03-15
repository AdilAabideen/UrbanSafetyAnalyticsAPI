"""
Integration tests for watchlist endpoints using InMemoryDB.

Why InMemoryDB:
- Exercises API -> service -> repository orchestration through real routes.
- Avoids touching the real Postgres instance.
"""

from datetime import date, datetime

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from app.services.auth_service import create_access_token
from app.services import watchlist_analytics_service
from tests.inmemory_db import InMemoryDB


# Prevent startup bootstrap side effects during mocked integration tests.
app.router.on_startup.clear()
client = TestClient(app)


def _run_with_db(handlers, method, url, json=None, headers=None):
    """Execute one request with query handlers served by InMemoryDB."""

    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url, json=json, headers=headers)
    finally:
        app.dependency_overrides.clear()


def _user_row(user_id: int, is_admin: bool = False):
    """Build a minimal user row used by get_current_user dependency."""
    return {
        "id": user_id,
        "email": f"user{user_id}@example.com",
        "is_admin": is_admin,
        "created_at": datetime(2026, 1, 1, 9, 0, 0),
    }


def _watchlist_row(
    *,
    watchlist_id: int,
    user_id: int,
    name: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    start_month=None,
    end_month=None,
    crime_types=None,
    travel_mode=None,
    include_collisions=False,
    baseline_months=6,
):
    """Build one watchlist row in repository SELECT shape."""
    return {
        "id": watchlist_id,
        "user_id": user_id,
        "name": name,
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
        "start_month": start_month,
        "end_month": end_month,
        "crime_types": crime_types,
        "travel_mode": travel_mode,
        "include_collisions": include_collisions,
        "baseline_months": baseline_months,
        "created_at": datetime(2026, 1, 2, 10, 0, 0),
    }


def test_create_watchlist_successfully_creates_watchlist():
    """
    Integration: standard create flow returns 201 and persisted watchlist fields.

    Checks:
    - status 201
    - name is correct
    - bbox fields are returned as saved
    - user_id matches authenticated user
    """
    user_id = 101
    token = create_access_token(user_id)

    saved_row = _watchlist_row(
        watchlist_id=501,
        user_id=user_id,
        name="City Centre",
        min_lon=-1.60,
        min_lat=53.70,
        max_lon=-1.50,
        max_lat=53.80,
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)": {"rows": [{"id": 501}]},
        "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": [saved_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists",
        json={
            "name": "City Centre",
            "min_lon": -1.60,
            "min_lat": 53.70,
            "max_lon": -1.50,
            "max_lat": 53.80,
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 201
    payload = response.json()["watchlist"]
    assert payload["name"] == "City Centre"
    assert payload["min_lon"] == -1.60
    assert payload["min_lat"] == 53.70
    assert payload["max_lon"] == -1.50
    assert payload["max_lat"] == 53.80
    assert payload["user_id"] == user_id


def test_create_watchlist_with_preference_saves_preference_fields():
    """
    Integration: create watchlist with preference values and verify preference output.

    Checks:
    - preference block exists
    - month/type/mode/collision/baseline fields are correct
    - fixed scoring fields are present with expected values
    """
    user_id = 102
    token = create_access_token(user_id)

    final_row = _watchlist_row(
        watchlist_id=502,
        user_id=user_id,
        name="Commuter Zone",
        min_lon=-1.61,
        min_lat=53.71,
        max_lon=-1.49,
        max_lat=53.81,
        start_month=date(2026, 1, 1),
        end_month=date(2026, 3, 1),
        crime_types=["Burglary", "Robbery"],
        travel_mode="drive",
        include_collisions=True,
        baseline_months=9,
    )

    def _assert_preference_update(params):
        # Service should normalize aliases before repository write.
        assert params["travel_mode"] == "drive"
        assert params["include_collisions"] is True
        return {"rows": [{"id": 502}]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)": {"rows": [{"id": 502}]},
        "start_month = :start_month": _assert_preference_update,
        "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": [final_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists",
        json={
            "name": "Commuter Zone",
            "min_lon": -1.61,
            "min_lat": 53.71,
            "max_lon": -1.49,
            "max_lat": 53.81,
            "preference": {
                "start_month": "2026-01-01",
                "end_month": "2026-03-01",
                "crime_types": ["Burglary", "Robbery"],
                "travel_mode": "drive",
                "include_collisions": True,
                "baseline_months": 9,
            },
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 201
    pref = response.json()["watchlist"]["preference"]
    assert pref is not None
    assert pref["start_month"] == "2026-01-01"
    assert pref["end_month"] == "2026-03-01"
    assert pref["crime_types"] == ["Burglary", "Robbery"]
    assert pref["travel_mode"] == "drive"
    assert pref["include_collisions"] is True
    assert pref["baseline_months"] == 9
    assert pref["hotspot_k"] == 20
    assert pref["include_hotspot_stability"] is True
    assert pref["include_forecast"] is True
    assert pref["weight_crime"] == 1.0
    assert pref["weight_collision"] == 0.8


def test_read_watchlists_returns_only_current_user_watchlists():
    """
    Integration: list route enforces user isolation.

    Checks:
    - only current user rows are returned
    - repository query receives current user's id
    """
    user_id = 103
    token = create_access_token(user_id)

    own_row_1 = _watchlist_row(
        watchlist_id=601,
        user_id=user_id,
        name="Own A",
        min_lon=-1.65,
        min_lat=53.70,
        max_lon=-1.55,
        max_lat=53.80,
    )
    own_row_2 = _watchlist_row(
        watchlist_id=602,
        user_id=user_id,
        name="Own B",
        min_lon=-1.66,
        min_lat=53.71,
        max_lon=-1.56,
        max_lat=53.81,
    )

    def _list_for_current_user(params):
        # If service accidentally passes a different user, fail loudly.
        assert params["user_id"] == user_id
        return {"rows": [own_row_1, own_row_2]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "WHERE w.user_id = :user_id": _list_for_current_user,
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/watchlists",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 2
    assert all(item["user_id"] == user_id for item in items)
    assert [item["id"] for item in items] == [601, 602]


def test_read_single_watchlist_by_id_returns_watchlist():
    """
    Integration: single-watchlist read path by query parameter.

    Checks:
    - status 200
    - requested watchlist is returned
    - preference block is present when row has preference values
    """
    user_id = 104
    token = create_access_token(user_id)

    row = _watchlist_row(
        watchlist_id=701,
        user_id=user_id,
        name="Single Lookup",
        min_lon=-1.60,
        min_lat=53.72,
        max_lon=-1.52,
        max_lat=53.79,
        start_month=date(2026, 2, 1),
        end_month=date(2026, 4, 1),
        crime_types=["Vehicle crime"],
        travel_mode="walk",
        include_collisions=False,
        baseline_months=6,
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": [row]},
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/watchlists?watchlist_id=701",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()["watchlist"]
    assert payload["id"] == 701
    assert payload["name"] == "Single Lookup"
    assert payload["preference"] is not None
    assert payload["preference"]["start_month"] == "2026-02-01"
    assert payload["preference"]["travel_mode"] == "walk"


def test_update_watchlist_updates_name_and_bbox():
    """
    Integration: update base watchlist fields (name + bbox).

    Checks:
    - status 200
    - updated values are returned
    - update parameters persisted through repository update call
    """
    user_id = 105
    token = create_access_token(user_id)

    updated_row = _watchlist_row(
        watchlist_id=801,
        user_id=user_id,
        name="Updated Name",
        min_lon=-1.70,
        min_lat=53.68,
        max_lon=-1.50,
        max_lat=53.85,
    )

    def _assert_update_fields(params):
        assert params["name"] == "Updated Name"
        assert params["min_lon"] == -1.70
        assert params["min_lat"] == 53.68
        assert params["max_lon"] == -1.50
        assert params["max_lat"] == 53.85
        return {"rows": [{"id": 801}]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "SET name = :name, min_lon = :min_lon": _assert_update_fields,
        "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": [updated_row]},
    }

    response = _run_with_db(
        handlers,
        "PATCH",
        "/watchlists/801",
        json={
            "name": "Updated Name",
            "min_lon": -1.70,
            "min_lat": 53.68,
            "max_lon": -1.50,
            "max_lat": 53.85,
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()["watchlist"]
    assert payload["name"] == "Updated Name"
    assert payload["min_lon"] == -1.70
    assert payload["min_lat"] == 53.68
    assert payload["max_lon"] == -1.50
    assert payload["max_lat"] == 53.85


def test_update_watchlist_preference_updates_preference_fields():
    """
    Integration: update only preference fields.

    Checks:
    - preference block is updated
    - travel mode aliases are normalized before persistence
    - include_collisions with drive mode is accepted and returned
    """
    user_id = 106
    token = create_access_token(user_id)

    before_row = _watchlist_row(
        watchlist_id=901,
        user_id=user_id,
        name="Preference Update",
        min_lon=-1.60,
        min_lat=53.70,
        max_lon=-1.50,
        max_lat=53.80,
    )
    after_row = _watchlist_row(
        watchlist_id=901,
        user_id=user_id,
        name="Preference Update",
        min_lon=-1.60,
        min_lat=53.70,
        max_lon=-1.50,
        max_lat=53.80,
        start_month=date(2026, 1, 1),
        end_month=date(2026, 2, 1),
        crime_types=["Burglary"],
        travel_mode="drive",
        include_collisions=True,
        baseline_months=8,
    )

    def _assert_preference_write(params):
        assert params["travel_mode"] == "drive"
        assert params["include_collisions"] is True
        return {"rows": [{"id": 901}]}

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        # Service reads existing row first, then final row after update.
        "WHERE w.id = :watchlist_id AND w.user_id = :user_id": [
            {"rows": [before_row]},
            {"rows": [after_row]},
        ],
        "start_month = :start_month": _assert_preference_write,
    }

    response = _run_with_db(
        handlers,
        "PATCH",
        "/watchlists/901",
        json={
            "preference": {
                "start_month": "2026-01-01",
                "end_month": "2026-02-01",
                "crime_types": ["Burglary"],
                "travel_mode": "drive",
                "include_collisions": True,
                "baseline_months": 8,
            }
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    pref = response.json()["watchlist"]["preference"]
    assert pref["start_month"] == "2026-01-01"
    assert pref["end_month"] == "2026-02-01"
    assert pref["crime_types"] == ["Burglary"]
    assert pref["travel_mode"] == "drive"
    assert pref["include_collisions"] is True
    assert pref["baseline_months"] == 8


def test_delete_watchlist_removes_watchlist():
    """
    Integration: delete watchlist and verify it is gone afterward.

    Checks:
    - delete returns 200 and deleted=true
    - subsequent single fetch returns 404
    """
    user_id = 107
    token = create_access_token(user_id)

    shared_db = InMemoryDB(
        {
            "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
            "DELETE FROM watchlists": {"rows": [{"id": 1001}]},
            # After deletion, single-fetch path should not find row.
            "WHERE w.id = :watchlist_id AND w.user_id = :user_id": {"rows": []},
        }
    )

    def _override_get_db():
        yield shared_db

    app.dependency_overrides[get_db] = _override_get_db
    try:
        delete_response = client.delete(
            "/watchlists/1001",
            headers={"Authorization": f"Bearer {token}"},
        )
        read_response = client.get(
            "/watchlists?watchlist_id=1001",
            headers={"Authorization": f"Bearer {token}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert delete_response.status_code == 200
    delete_payload = delete_response.json()
    assert delete_payload["deleted"] is True
    assert delete_payload["watchlist_id"] == 1001

    assert read_response.status_code == 404
    assert read_response.json()["error"] == "WATCHLIST_NOT_FOUND"


def test_compute_watchlist_risk_score_returns_404_for_missing_watchlist():
    """
    Integration: risk-score endpoint returns 404 when watchlist is missing/unowned.
    """
    user_id = 201
    token = create_access_token(user_id)

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": []},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/999/analytics/risk-score",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "WATCHLIST_NOT_FOUND"


def test_compute_watchlist_risk_score_rejects_missing_month_window():
    """
    Integration: risk-score endpoint rejects watchlists with missing month window.
    """
    user_id = 202
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1201,
        user_id=user_id,
        name="Missing Months",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
        start_month=None,
        end_month=None,
        crime_types=[],
        travel_mode="walk",
    )
    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1201/analytics/risk-score",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "MISSING_MONTH_WINDOW"


def test_compute_watchlist_risk_score_rejects_invalid_month_range():
    """
    Integration: risk-score endpoint rejects start_month > end_month.
    """
    user_id = 203
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1202,
        user_id=user_id,
        name="Invalid Month Range",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
        start_month=date(2026, 4, 1),
        end_month=date(2026, 1, 1),
        crime_types=[],
        travel_mode="walk",
    )
    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1202/analytics/risk-score",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "INVALID_MONTH_RANGE"


def test_compute_watchlist_risk_score_persists_and_returns_historical_comparison(monkeypatch):
    """
    Integration: risk-score service uses historical_same_signature branch when cohort is sufficient.
    """
    user_id = 204
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1203,
        user_id=user_id,
        name="Historical Branch",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
        start_month=date(2026, 1, 1),
        end_month=date(2026, 3, 1),
        crime_types=["Burglary"],
        travel_mode="walk",
    )

    inserted_rows = []

    monkeypatch.setattr(
        watchlist_analytics_service,
        "_compute_risk_result",
        lambda *args, **kwargs: {
            "risk_score": 63,
            "raw_score": 1.5,
            "components": {"crime_component": 4.0, "collision_density": 0.2, "user_support": 0.1},
        },
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "load_historical_rows",
        lambda *args, **kwargs: [{"id": 1, "risk_score": 50}, {"id": 2, "risk_score": 80}],
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "insert_risk_score_run",
        lambda *args, **kwargs: inserted_rows.append(kwargs) or {"id": 7001},
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1203/analytics/risk-score",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["comparison"]["cohort_type"] == "historical_same_signature"
    assert payload["comparison"]["cohort_size"] == 2
    assert payload["comparison"]["rank"] is not None
    assert payload["comparison"]["percentile"] is not None
    assert len(inserted_rows) == 1
    assert inserted_rows[0]["watchlist_id"] == 1203


def test_compute_watchlist_risk_score_falls_back_to_reference_bboxes_when_history_insufficient(monkeypatch):
    """
    Integration: risk-score service falls back to reference_bboxes when historical cohort is too small.
    """
    user_id = 205
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1204,
        user_id=user_id,
        name="Reference Fallback",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
        start_month=date(2026, 1, 1),
        end_month=date(2026, 3, 1),
        crime_types=["Robbery"],
        travel_mode="drive",
    )

    monkeypatch.setattr(
        watchlist_analytics_service,
        "_compute_risk_result",
        lambda *args, **kwargs: {
            "risk_score": 64,
            "raw_score": 1.8,
            "components": {"crime_component": 4.5, "collision_density": 0.4, "user_support": 0.1},
        },
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "load_historical_rows",
        lambda *args, **kwargs: [{"id": 1, "risk_score": 58}],
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "nearest_reference_bboxes",
        lambda *args, **kwargs: [
            {"id": 91, "min_lon": -1.61, "min_lat": 53.69, "max_lon": -1.51, "max_lat": 53.79},
            {"id": 92, "min_lon": -1.62, "min_lat": 53.68, "max_lon": -1.50, "max_lat": 53.80},
        ],
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "latest_reference_score",
        lambda *args, **kwargs: {"id": kwargs["reference_bbox_id"], "risk_score": 70},
    )
    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "insert_risk_score_run",
        lambda *args, **kwargs: {"id": 7010},
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1204/analytics/risk-score",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["comparison"]["cohort_type"] == "reference_bboxes"
    assert payload["comparison"]["cohort_size"] == 2
    assert payload["comparison"]["reference_ids"] == [91, 92]


def test_list_watchlist_risk_runs_returns_runs_for_owned_watchlist():
    """
    Integration: /risk-score/runs returns persisted runs for an owned watchlist.
    """
    user_id = 206
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1205,
        user_id=user_id,
        name="Runs List",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
    )

    run_rows = [
        {
            "id": 2002,
            "watchlist_id": 1205,
            "start_month": date(2026, 1, 1),
            "end_month": date(2026, 3, 1),
            "crime_types": ["Burglary"],
            "travel_mode": "walk",
            "risk_score": 72,
            "raw_score": 2.1,
            "band": "high",
            "crime_component": 4.3,
            "collision_component": 0.2,
            "user_component": 0.1,
            "comparison_basis": "historical_same_signature",
            "comparison_sample_size": 4,
            "comparison_percentile": 75.0,
            "execution_time_ms": 55.0,
            "created_at": datetime(2026, 3, 14, 10, 0, 0),
        },
        {
            "id": 2001,
            "watchlist_id": 1205,
            "start_month": date(2025, 10, 1),
            "end_month": date(2025, 12, 1),
            "crime_types": [],
            "travel_mode": "drive",
            "risk_score": 60,
            "raw_score": 1.2,
            "band": "medium",
            "crime_component": 3.0,
            "collision_component": 0.8,
            "user_component": 0.0,
            "comparison_basis": "reference_bboxes",
            "comparison_sample_size": 2,
            "comparison_percentile": 50.0,
            "execution_time_ms": 42.5,
            "created_at": datetime(2026, 3, 13, 10, 0, 0),
        },
    ]

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
        "FROM risk_score_runs r": {"rows": run_rows},
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/watchlists/1205/analytics/risk-score/runs?limit=50",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["watchlist_id"] == 1205
    assert len(payload["items"]) == 2
    assert payload["items"][0]["run_id"] == 2002
    assert "risk_result" in payload["items"][0]
    assert "components" in payload["items"][0]["risk_result"]


def test_list_watchlist_risk_runs_returns_404_for_unowned_watchlist():
    """
    Integration: /risk-score/runs returns 404 when the watchlist is not owned by current user.
    """
    user_id = 207
    token = create_access_token(user_id)

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": []},
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/watchlists/5555/analytics/risk-score/runs",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "WATCHLIST_NOT_FOUND"


def test_forecast_watchlist_next_month_returns_forecast_successfully(monkeypatch):
    """
    Integration: forecast endpoint returns a correctly shaped response for valid input.
    """
    user_id = 208
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1206,
        user_id=user_id,
        name="Forecast Happy Path",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
    )

    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "fetch_forecast_baseline_rows",
        lambda *args, **kwargs: [
            {"month": "2025-11", "crime_count": 40.0, "collision_count": 2, "collision_points": 4.0},
            {"month": "2025-12", "crime_count": 50.0, "collision_count": 1, "collision_points": 2.0},
            {"month": "2026-01", "crime_count": 60.0, "collision_count": 1, "collision_points": 3.0},
        ],
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1206/analytics/forecast",
        json={
            "start_month": "2025-06",
            "mode": "walk",
            "crime_types": ["Burglary", "Robbery"],
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "generated_at" in payload
    assert "forecast" in payload
    assert "score" in payload["forecast"]
    assert "band" in payload["forecast"]
    assert "intervals" in payload["forecast"]
    assert "components" in payload["forecast"]


def test_forecast_watchlist_next_month_rejects_future_start_month():
    """
    Integration: forecast endpoint rejects start month later than last complete month.
    """
    user_id = 209
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1207,
        user_id=user_id,
        name="Forecast Future Start",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1207/analytics/forecast",
        json={"start_month": "2100-01", "mode": "walk", "crime_types": []},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "INVALID_DATE_RANGE"


def test_forecast_watchlist_next_month_rejects_when_baseline_history_missing(monkeypatch):
    """
    Integration: forecast endpoint rejects when baseline history query returns no rows.
    """
    user_id = 210
    token = create_access_token(user_id)
    watchlist_row = _watchlist_row(
        watchlist_id=1208,
        user_id=user_id,
        name="Forecast Missing Baseline",
        min_lon=-1.6,
        min_lat=53.7,
        max_lon=-1.5,
        max_lat=53.8,
    )

    monkeypatch.setattr(
        watchlist_analytics_service.watchlist_analytics_repository,
        "fetch_forecast_baseline_rows",
        lambda *args, **kwargs: [],
    )

    handlers = {
        "WHERE u.id = :user_id": {"rows": [_user_row(user_id)]},
        "FROM watchlists w": {"rows": [watchlist_row]},
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/watchlists/1208/analytics/forecast",
        json={"start_month": "2025-06", "mode": "walk", "crime_types": []},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "BASELINE_HISTORY_INSUFFICIENT"
