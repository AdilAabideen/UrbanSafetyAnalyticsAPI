from datetime import date

from fastapi.testclient import TestClient

import app.api.watchlist as watchlist_api
from app.api_utils.auth_utils import get_current_user
from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


app.router.on_startup.clear()
client = TestClient(app)


def _run_with_db(handlers, method, url, json=None):
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url, json=json)
    finally:
        app.dependency_overrides.clear()


def _run_with_auth_db(handlers, method, url, json=None, current_user=None):
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_user] = lambda: current_user or {
        "id": 1,
        "email": "test@example.com",
        "is_admin": False,
    }
    try:
        return client.request(method, url, json=json)
    finally:
        app.dependency_overrides.clear()


def test_analytics_risk_score_returns_payload_without_persisting():
    handlers = {
        "/* analytics_risk_score_area */": {
            "rows": [
                {
                    "total_crimes": 120,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "total_collisions": 8,
                    "total_collision_points": 14.0,
                    "area_km2": 4.0,
                }
            ],
        },
        "/* analytics_risk_score_segments */": {
            "rows": [
                {
                    "segments_considered": 12,
                    "avg_density": 3.4,
                    "avg_crimes_per_km": 2.5,
                    "avg_user_reported_crime_signal_per_km": 0.0,
                    "avg_collisions_per_km": 0.4,
                    "avg_collision_points_per_km": 0.9,
                    "red_segment_share": 0.25,
                    "avg_density_pct": 0.86,
                }
            ],
        },
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/analytics/risk/score",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["score"] == 86
    assert payload["risk_score"] == 86
    assert payload["score_basis"] == "crime"
    assert payload["band"] == "amber"
    assert "snapshot_id" not in payload
    assert "stored_at" not in payload
    assert payload["metrics"]["segments_considered"] == 12
    assert "total_collisions" not in payload["metrics"]


def test_analytics_risk_forecast_returns_history_without_persisting():
    handlers = {
        "/* analytics_risk_forecast_coverage */": {"rows": [{"missing_months": 0}]},
        "/* analytics_risk_forecast_history */": {
            "rows": [
                {
                    "month": "2025-01",
                    "official_count": 10,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 10,
                },
                {
                    "month": "2025-02",
                    "official_count": 12,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 12,
                },
                {
                    "month": "2025-03",
                    "official_count": 14,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 14,
                },
                {
                    "month": "2025-04",
                    "official_count": 9,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 9,
                },
                {
                    "month": "2025-05",
                    "official_count": 11,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 11,
                },
                {
                    "month": "2025-06",
                    "official_count": 13,
                    "approved_user_reports": 0,
                    "user_reported_crime_signal": 0.0,
                    "count": 13,
                },
            ],
        },
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/analytics/risk/forecast",
        json={
            "target": "2025-07",
            "baselineMonths": 6,
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "returnRiskProjection": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["history"]) == 6
    assert payload["score_basis"] == "crime"
    assert "snapshot_id" not in payload
    assert "stored_at" not in payload
    assert payload["forecast"]["expected_count"] == 12
    assert "collisions" not in payload["forecast"]["components"]


def test_analytics_hotspot_stability_returns_series_without_persisting():
    handlers = {
        "/* analytics_hotspot_stability_monthly */": {
            "rows": [
                {"month": date(2025, 1, 1), "segment_id": 1, "crimes": 5, "crimes_per_km": 10.0},
                {"month": date(2025, 1, 1), "segment_id": 2, "crimes": 3, "crimes_per_km": 8.0},
                {"month": date(2025, 2, 1), "segment_id": 1, "crimes": 4, "crimes_per_km": 9.0},
                {"month": date(2025, 2, 1), "segment_id": 3, "crimes": 2, "crimes_per_km": 7.5},
                {"month": date(2025, 3, 1), "segment_id": 1, "crimes": 6, "crimes_per_km": 11.0},
                {"month": date(2025, 3, 1), "segment_id": 3, "crimes": 2, "crimes_per_km": 6.0},
            ]
        },
    }

    response = _run_with_db(
        handlers,
        "GET",
        "/analytics/patterns/hotspot-stability?from=2025-01&to=2025-03&k=5&includeLists=true",
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["stability_series"]) == 2
    assert payload["persistent_hotspots"][0]["segment_id"] == 1
    assert len(payload["topk_by_month"]) == 3
    assert "snapshot_id" not in payload
    assert "stored_at" not in payload


def test_watchlist_risk_forecast_run_persists_against_watchlist(monkeypatch):
    monkeypatch.setattr(
        watchlist_api,
        "_latest_complete_month",
        lambda _db, _include_collisions=False: date(2025, 6, 1),
    )

    handlers = {
        "FROM watchlists w": {
            "rows": [
                {
                    "id": 7,
                    "user_id": 1,
                    "name": "City Centre",
                    "min_lon": -1.6,
                    "min_lat": 53.78,
                    "max_lon": -1.52,
                    "max_lat": 53.82,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
        "FROM watchlist_preferences wp": {
            "rows": [
                {
                    "id": 9,
                    "watchlist_id": 7,
                    "window_months": 6,
                    "crime_types": ["burglary"],
                    "travel_mode": "walking",
                    "include_collisions": False,
                    "baseline_months": 6,
                    "hotspot_k": 20,
                    "include_hotspot_stability": True,
                    "include_forecast": True,
                    "weight_crime": 1.0,
                    "weight_collision": 0.0,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
        "/* analytics_risk_forecast_coverage */": {"rows": [{"missing_months": 0}]},
        "/* analytics_risk_forecast_history */": {
            "rows": [
                {"month": "2024-12", "official_count": 8, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 8},
                {"month": "2025-01", "official_count": 10, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 10},
                {"month": "2025-02", "official_count": 12, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 12},
                {"month": "2025-03", "official_count": 9, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 9},
                {"month": "2025-04", "official_count": 11, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 11},
                {"month": "2025-05", "official_count": 13, "approved_user_reports": 0, "user_reported_crime_signal": 0.0, "count": 13},
            ],
        },
        "INSERT INTO watchlist_analytics_runs": {
            "rows": [{"id": 71, "created_at": "2026-03-13T12:03:00Z"}],
        },
    }

    response = _run_with_auth_db(handlers, "POST", "/watchlists/7/risk-forecast/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["watchlist_id"] == 7
    assert payload["report_type"] == "risk_forecast"
    assert payload["watchlist_run_id"] == 71
    assert payload["request"]["target"] == "2025-07"
    assert payload["request"]["mode"] == "walk"
    assert payload["result"]["results_by_crime_type"]["burglary"]["forecast"]["expected_count"] == 10


def test_watchlist_hotspot_run_requires_preference():
    handlers = {
        "FROM watchlists w": {
            "rows": [
                {
                    "id": 7,
                    "user_id": 1,
                    "name": "City Centre",
                    "min_lon": -1.6,
                    "min_lat": 53.78,
                    "max_lon": -1.52,
                    "max_lat": 53.82,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
        "FROM watchlist_preferences wp": {"rows": []},
    }

    response = _run_with_auth_db(handlers, "POST", "/watchlists/7/hotspot-stability/run")

    assert response.status_code == 400
    data = response.json()
    assert data["error"] == "INVALID_REQUEST"
    assert data["message"] == "Watchlist preference is required to run analytics"


def test_watchlist_risk_forecast_results_returns_stored_runs():
    handlers = {
        "FROM watchlists w": {
            "rows": [
                {
                    "id": 7,
                    "user_id": 1,
                    "name": "City Centre",
                    "min_lon": -1.6,
                    "min_lat": 53.78,
                    "max_lon": -1.52,
                    "max_lat": 53.82,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
        "FROM watchlist_analytics_runs war": {
            "rows": [
                {
                    "id": 71,
                    "watchlist_id": 7,
                    "report_type": "risk_forecast",
                    "request_params_json": {
                        "target": "2025-07",
                        "baselineMonths": 6,
                    },
                    "payload_json": {
                        "results_by_crime_type": {
                            "burglary": {
                                "forecast": {"expected_count": 10}
                            }
                        }
                    },
                    "created_at": "2026-03-13T12:03:00Z",
                }
            ],
        },
    }

    response = _run_with_auth_db(
        handlers,
        "GET",
        "/watchlists/7/risk-forecast/results?run_id=71",
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["id"] == 71
    assert payload["items"][0]["request"]["target"] == "2025-07"
    assert payload["items"][0]["result"]["results_by_crime_type"]["burglary"]["forecast"]["expected_count"] == 10


def test_watchlist_risk_forecast_rejects_collisions_without_drive():
    handlers = {
        "FROM watchlists w": {
            "rows": [
                {
                    "id": 7,
                    "user_id": 1,
                    "name": "City Centre",
                    "min_lon": -1.6,
                    "min_lat": 53.78,
                    "max_lon": -1.52,
                    "max_lat": 53.82,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
        "FROM watchlist_preferences wp": {
            "rows": [
                {
                    "id": 9,
                    "watchlist_id": 7,
                    "window_months": 6,
                    "crime_types": [],
                    "travel_mode": "walking",
                    "include_collisions": True,
                    "baseline_months": 6,
                    "hotspot_k": 20,
                    "include_hotspot_stability": True,
                    "include_forecast": True,
                    "weight_crime": 1.0,
                    "weight_collision": 0.5,
                    "created_at": "2026-03-13T12:00:00Z",
                }
            ],
        },
    }

    response = _run_with_auth_db(handlers, "POST", "/watchlists/7/risk-forecast/run")

    assert response.status_code == 400
    data = response.json()
    assert data["error"] == "INVALID_REQUEST"
    assert data["message"] == "include_collisions is only supported when travel_mode is drive"
