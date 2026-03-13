from datetime import date

from fastapi.testclient import TestClient

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


def test_analytics_meta_returns_counts_and_types():
    response = _run_with_db(
        {
            "/* analytics_meta_counts */": {
                "rows": [
                    {
                        "min_month": "2024-01",
                        "max_month": "2026-01",
                        "crime_events_total": 1000,
                        "crime_events_with_geom": 970,
                        "crime_events_snapped": 910,
                        "road_segments_total": 500,
                    }
                ]
            },
            "/* analytics_meta_types */": {
                "rows": [
                    {"crime_type": "burglary"},
                    {"crime_type": "robbery"},
                ]
            },
        },
        "GET",
        "/analytics/meta",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["months"]["min"] == "2024-01"
    assert payload["months"]["max"] == "2026-01"
    assert payload["counts"]["crime_events_total"] == 1000
    assert payload["crime_types"] == ["burglary", "robbery"]


def test_analytics_risk_score_returns_expected_payload():
    response = _run_with_db(
        {
            "/* analytics_risk_score_area */": {
                "rows": [
                    {
                        "total_crimes": 120,
                        "approved_user_reports": 8,
                        "user_reported_crime_signal": 1.6,
                        "total_collisions": 0,
                        "total_collision_points": 0.0,
                        "area_km2": 4.0,
                    }
                ]
            },
            "/* analytics_risk_score_segments */": {
                "rows": [
                    {
                        "segments_considered": 20,
                        "avg_density": 3.2,
                        "avg_crimes_per_km": 2.8,
                        "avg_user_reported_crime_signal_per_km": 0.1,
                        "avg_collisions_per_km": 0.0,
                        "avg_collision_points_per_km": 0.0,
                        "red_segment_share": 0.2,
                        "avg_density_pct": 0.86,
                    }
                ]
            },
        },
        "POST",
        "/analytics/risk/score",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "mode": "walk",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["score"] == 86
    assert payload["band"] == "amber"
    assert payload["metrics"]["total_crimes"] == 120
    assert payload["metrics"]["approved_user_reports"] == 8
    assert payload["score_basis"] == "crime"


def test_analytics_risk_forecast_returns_history():
    response = _run_with_db(
        {
            "/* analytics_risk_forecast_coverage */": {"rows": [{"missing_months": 0}]},
            "/* analytics_risk_forecast_history */": {
                "rows": [
                    {
                        "month": "2025-01",
                        "official_count": 10,
                        "approved_user_reports": 1,
                        "user_reported_crime_signal": 0.2,
                        "count": 10.2,
                    },
                    {
                        "month": "2025-02",
                        "official_count": 12,
                        "approved_user_reports": 0,
                        "user_reported_crime_signal": 0.0,
                        "count": 12.0,
                    },
                    {
                        "month": "2025-03",
                        "official_count": 14,
                        "approved_user_reports": 2,
                        "user_reported_crime_signal": 0.4,
                        "count": 14.4,
                    },
                ]
            },
        },
        "POST",
        "/analytics/risk/forecast",
        json={
            "target": "2025-04",
            "baselineMonths": 3,
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "returnRiskProjection": True,
            "mode": "walk",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["history"]) == 3
    assert payload["forecast"]["expected_count"] == 12
    assert payload["forecast"]["projection_basis"] == "crimes"
    assert payload["score_basis"] == "crime"


def test_analytics_hotspot_stability_returns_series_and_lists():
    response = _run_with_db(
        {
            "/* analytics_hotspot_stability_monthly */": {
                "rows": [
                    {"month": date(2025, 1, 1), "segment_id": 1, "crimes": 4.0, "crimes_per_km": 9.0},
                    {"month": date(2025, 1, 1), "segment_id": 2, "crimes": 3.0, "crimes_per_km": 7.0},
                    {"month": date(2025, 2, 1), "segment_id": 1, "crimes": 5.0, "crimes_per_km": 10.0},
                    {"month": date(2025, 2, 1), "segment_id": 3, "crimes": 2.0, "crimes_per_km": 6.0},
                ]
            }
        },
        "GET",
        "/analytics/patterns/hotspot-stability?from=2025-01&to=2025-02&k=10&includeLists=true",
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["stability_series"]) == 1
    assert payload["persistent_hotspots"][0]["segment_id"] == 1
    assert len(payload["topk_by_month"]) == 2
