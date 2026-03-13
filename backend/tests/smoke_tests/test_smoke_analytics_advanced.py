from datetime import date, datetime, timezone

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


def test_analytics_risk_score_returns_payload_and_snapshot():
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
        "INSERT INTO analytics_risk_score_snapshots": {
            "rows": [{"id": 11, "created_at": "2026-03-13T12:00:00Z"}],
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
    assert payload["snapshot_id"] == 11
    assert payload["stored_at"] == "2026-03-13T12:00:00Z"
    assert payload["metrics"]["segments_considered"] == 12
    assert "total_collisions" not in payload["metrics"]


def test_analytics_risk_forecast_returns_history_and_snapshot():
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
        "INSERT INTO analytics_risk_forecast_snapshots": {
            "rows": [{"id": 21, "created_at": "2026-03-13T12:01:00Z"}],
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
    assert payload["snapshot_id"] == 21
    assert payload["stored_at"] == "2026-03-13T12:01:00Z"
    assert payload["forecast"]["expected_count"] == 12
    assert "collisions" not in payload["forecast"]["components"]


def test_analytics_hotspot_stability_returns_series_and_snapshot():
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
        "INSERT INTO analytics_hotspot_stability_snapshots": {
            "rows": [{"id": 31, "created_at": "2026-03-13T12:02:00Z"}],
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
    assert payload["snapshot_id"] == 31
    assert payload["stored_at"] == "2026-03-13T12:02:00Z"
