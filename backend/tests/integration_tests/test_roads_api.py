from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _run_with_db(handlers, method, url):
    # Route all SQL through the in-memory handler map for endpoint-level assertions.
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url)
    finally:
        app.dependency_overrides.clear()


def test_roads_analytics_meta_endpoint():
    response = _run_with_db(
        {
            "/* roads_meta_months */": {"rows": [{"min_month": "2025-01", "max_month": "2025-06"}]},
            "/* roads_meta_highways */": {"rows": [{"highway": "primary"}, {"highway": "residential"}]},
            "/* roads_meta_crime_types */": {"rows": [{"crime_type": "burglary"}]},
            "/* roads_meta_outcomes */": {"rows": [{"outcome": "unknown"}]},
            "/* roads_meta_counts */": {
                "rows": [
                    {
                        "road_segments_total": 100,
                        "named_roads_total": 80,
                        "total_length_m": 10000.0,
                        "roads_with_incidents": 35,
                        "incidents_total": 250,
                    }
                ]
            },
        },
        "GET",
        "/roads/analytics/meta",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["months"]["min"] == "2025-01"
    assert payload["counts"]["road_segments_total"] == 100
    assert payload["highways"][0] == "primary"


def test_roads_analytics_overview_endpoint():
    response = _run_with_db(
        {
            "/* roads_overview_summary */": {
                "rows": [
                    {
                        "total_segments": 10,
                        "total_length_m": 2500.0,
                        "unique_highway_types": 2,
                        "roads_with_incidents": 4,
                        "total_incidents": 20,
                        "avg_incidents_per_km": 8.0,
                    }
                ]
            },
            "/* roads_overview_top_highway */": {
                "rows": [
                    {
                        "highway": "primary",
                        "segment_count": 3,
                        "length_m": 900.0,
                        "incident_count": 12,
                        "incidents_per_km": 13.3,
                    }
                ]
            },
            "/* roads_overview_top_crime_type */": {"rows": [{"crime_type": "burglary", "count": 8}]},
            "/* roads_overview_top_outcome */": {"rows": [{"outcome": "unknown", "count": 9}]},
            "/* roads_overview_top_road */": {
                "rows": [
                    {
                        "segment_id": 42,
                        "name": "A Street",
                        "highway": "primary",
                        "length_m": 300.0,
                        "incident_count": 7,
                        "incidents_per_km": 23.3,
                        "risk_score": 91.2,
                        "band": "red",
                    }
                ]
            },
            "/* roads_overview_band_breakdown */": {"rows": [{"band": "red", "count": 2}, {"band": "green", "count": 2}]},
            "/* roads_overview_previous */": {"rows": [{"incident_count": 16}]},
        },
        "GET",
        "/roads/analytics/overview?from=2025-03&to=2025-05",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_incidents"] == 20
    assert payload["top_road"]["segment_id"] == 42
    assert payload["band_breakdown"]["red"] == 2


def test_roads_analytics_charts_endpoint():
    response = _run_with_db(
        {
            "/* roads_charts_timeseries_overall */": {
                "rows": [
                    {"group_key": "overall", "month": "2025-03", "count": 3},
                    {"group_key": "overall", "month": "2025-04", "count": 5},
                    {"group_key": "overall", "month": "2025-05", "count": 2},
                ]
            },
            "/* roads_charts_highway */": {
                "rows": [
                    {
                        "highway": "primary",
                        "segment_count": 4,
                        "length_m": 800.0,
                        "count": 6,
                        "incidents_per_km": 7.5,
                    }
                ]
            },
            "/* roads_charts_crime_type */": {"rows": [{"crime_type": "burglary", "count": 6}]},
            "/* roads_charts_outcome */": {"rows": [{"outcome": "unknown", "count": 7}]},
            "/* roads_charts_total */": {"rows": [{"total_incidents": 10}]},
            "/* roads_charts_band_breakdown */": {"rows": [{"band": "orange", "count": 1}]},
            "/* roads_charts_previous */": {"rows": [{"incident_count": 8}]},
        },
        "GET",
        "/roads/analytics/charts?from=2025-03&to=2025-05",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["timeseries"]["groupBy"] == "overall"
    assert payload["timeseries"]["total"] == 10
    assert payload["by_crime_type"][0]["crime_type"] == "burglary"


def test_roads_analytics_risk_endpoint():
    response = _run_with_db(
        {
            "/* roads_risk_main */": {
                "rows": [
                    {
                        "segment_id": 7,
                        "name": "Ring Road",
                        "highway": "primary",
                        "length_m": 600.0,
                        "incident_count": 9,
                        "incidents_per_km": 15.0,
                        "dominant_crime_type": "burglary",
                        "dominant_outcome": "unknown",
                        "previous_incident_count": 6,
                        "total_incidents_in_scope": 30,
                        "risk_score": 88.1,
                        "band": "orange",
                    }
                ]
            }
        },
        "GET",
        "/roads/analytics/risk?from=2025-03&to=2025-05&limit=10",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 1
    assert payload["items"][0]["segment_id"] == 7
    assert payload["items"][0]["share_of_incidents"] == 30.0
