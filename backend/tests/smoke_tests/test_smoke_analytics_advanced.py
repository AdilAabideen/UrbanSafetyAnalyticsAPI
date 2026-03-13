from datetime import date

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _run_with_db(handlers, method, url, json=None):
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url, json=json)
    finally:
        app.dependency_overrides.clear()


def test_analytics_risk_score_returns_payload():
    handlers = {
        "/* analytics_risk_score_area */": {
            "rows": [{"total_crimes": 120, "total_collisions": 8, "total_collision_points": 14.0, "area_km2": 4.0}],
        },
        "/* analytics_risk_score_segments */": {
            "rows": [
                {
                    "segments_considered": 12,
                    "avg_density": 3.4,
                    "avg_crimes_per_km": 2.5,
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
    assert payload["metrics"]["segments_considered"] == 12
    assert "total_collisions" not in payload["metrics"]
    assert "avg_collisions_per_km" not in payload["metrics"]
    assert payload["scope"]["from"] == "2025-01"


def test_analytics_risk_score_rejects_walk_collisions():
    response = _run_with_db(
        {},
        "POST",
        "/analytics/risk/score",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "includeCollisions": True,
            "mode": "walk",
        },
    )

    assert response.status_code == 400
    assert response.json()["error"] == "INVALID_MODE_FOR_COLLISIONS"


def test_analytics_risk_forecast_returns_history():
    handlers = {
        "/* analytics_risk_forecast_coverage */": {
            "rows": [{"missing_months": 0}],
        },
        "/* analytics_risk_forecast_history */": {
            "rows": [
                {"month": "2025-01", "count": 10},
                {"month": "2025-02", "count": 12},
                {"month": "2025-03", "count": 14},
                {"month": "2025-04", "count": 9},
                {"month": "2025-05", "count": 11},
                {"month": "2025-06", "count": 13},
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
    assert "collision_count" not in payload["history"][0]
    assert payload["forecast"]["expected_count"] == 12
    assert "collisions" not in payload["forecast"]["components"]
    assert payload["forecast"]["predicted_band"] == "green"


def test_analytics_hotspot_stability_returns_series():
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
        }
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


def test_analytics_route_risk_returns_route_metrics():
    handlers = {
        "/* analytics_route_segments_by_ids */": lambda params: {
            "rows": [
                {"segment_id": 1, "name": "A", "highway": "primary", "length_m": 100.0},
                {"segment_id": 2, "name": "B", "highway": "primary", "length_m": 150.0},
                {"segment_id": 3, "name": "C", "highway": "residential", "length_m": 200.0},
            ]
        },
        "/* analytics_route_connectivity */": {
            "rows": [
                {"break_index": 2, "from_segment_id": 2, "to_segment_id": 3, "distance_m": 31.2},
            ]
        },
        "/* analytics_route_segment_metrics */": lambda params: {
            "rows": [
                {
                    "segment_id": segment_id,
                    "name": f"S{segment_id}",
                    "highway": "primary",
                    "length_m": 100.0 * segment_id,
                    "crimes": float(segment_id),
                    "collisions": 0.0,
                    "casualties": 0.0,
                    "fatal_casualties": 0.0,
                    "serious_casualties": 0.0,
                    "slight_casualties": 0.0,
                }
                for segment_id in params["selected_segment_ids"]
            ]
        },
        "/* analytics_density_percentile */": {
            "rows": [{"density_pct": 0.82}],
        },
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/analytics/routes/risk",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "segment_ids": [1, 2, 3],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["route_stats"]["segment_count"] == 3
    assert payload["connectivity"]["is_connected"] is False
    assert len(payload["worst_segments"]) == 3
    assert payload["route_stats"]["band"] == "amber"


def test_analytics_routes_compare_returns_ranked_routes():
    handlers = {
        "/* analytics_route_segments_by_ids */": lambda params: {
            "rows": [
                {"segment_id": segment_id, "name": f"S{segment_id}", "highway": "primary", "length_m": 100.0}
                for segment_id in params["segment_ids"]
            ]
        },
        "/* analytics_route_connectivity */": {"rows": []},
        "/* analytics_route_segment_metrics */": lambda params: {
            "rows": [
                {
                    "segment_id": segment_id,
                    "name": f"S{segment_id}",
                    "highway": "primary",
                    "length_m": 100.0,
                    "crimes": 1.0 if segment_id in {1, 2} else 4.0,
                    "collisions": 0.0,
                    "casualties": 0.0,
                    "fatal_casualties": 0.0,
                    "serious_casualties": 0.0,
                    "slight_casualties": 0.0,
                }
                for segment_id in params["selected_segment_ids"]
            ]
        },
        "/* analytics_density_percentile */": {
            "rows": [{"density_pct": 0.75}],
        },
    }

    response = _run_with_db(
        handlers,
        "POST",
        "/analytics/routes/compare",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "routes": [
                {"name": "Route A", "segment_ids": [1, 2]},
                {"name": "Route B", "segment_ids": [3, 4]},
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["safest_route"] == "Route A"
    assert payload["summary"]["riskiest_route"] == "Route B"
    assert len(payload["ranking"]) == 2
