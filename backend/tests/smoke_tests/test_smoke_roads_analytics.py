from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_meta_db():
    handlers = {
        "to_char(MIN(c.month), 'YYYY-MM') AS min_month": {
            "rows": [{"min_month": "2023-02", "max_month": "2024-09"}]
        },
        "SELECT DISTINCT COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway": {
            "rows": [{"highway": "primary"}, {"highway": "residential"}]
        },
        "COUNT(*) FILTER (WHERE rs.name IS NOT NULL": {
            "rows": [
                {
                    "road_segments_total": 212450,
                    "named_roads_total": 180000,
                    "total_length_m": 9876543.2,
                }
            ]
        },
    }
    yield InMemoryDB(handlers)


def _override_summary_db():
    handlers = {
        "COUNT(DISTINCT rs.highway)::bigint AS unique_highway_types": {
            "rows": [
                {
                    "total_segments": 5500,
                    "total_length_m": 123456.7,
                    "unique_highway_types": 6,
                    "segments_with_incidents": 420,
                    "total_incidents": 1200,
                }
            ]
        },
        "GROUP BY highway": {
            "rows": [{"highway": "residential", "segment_count": 3200, "length_m": 65432.1}]
        },
    }
    yield InMemoryDB(handlers)


def _override_timeseries_db():
    handlers = {
        "SELECT generate_series(": {
            "rows": [
                {"month": "2023-02", "count": 410},
                {"month": "2023-03", "count": 430},
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_highways_db():
    handlers = {
        "GROUP BY rs.highway": {
            "rows": [
                {
                    "highway": "residential",
                    "segment_count": 3200,
                    "length_m": 65432.1,
                    "incident_count": 900,
                    "incidents_per_km": 13.75,
                },
                {
                    "highway": "primary",
                    "segment_count": 800,
                    "length_m": 22345.6,
                    "incident_count": 200,
                    "incidents_per_km": 8.95,
                },
                {
                    "highway": "secondary",
                    "segment_count": 500,
                    "length_m": 12345.0,
                    "incident_count": 100,
                    "incidents_per_km": 8.1,
                },
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_risk_db():
    handlers = {
        "percent_rank() OVER (ORDER BY incidents_per_km)": {
            "rows": [
                {
                    "segment_id": 7,
                    "name": "Test Road",
                    "highway": "primary",
                    "length_m": 120.5,
                    "incident_count": 8,
                    "incidents_per_km": 66.39,
                    "band": "red",
                }
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_anomaly_db():
    handlers = {
        "SELECT COALESCE(SUM(c.crime_count), 0)::bigint AS target_count": {
            "rows": [{"target_count": 120}]
        },
        "AVG(COALESCE(counts.count, 0))": {
            "rows": [{"baseline_mean": 80.0}]
        },
    }
    yield InMemoryDB(handlers)


def test_roads_analytics_meta_returns_discoverability_payload():
    app.dependency_overrides[get_db] = _override_meta_db
    try:
        response = client.get("/roads/analytics/meta")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "months": {"min": "2023-02", "max": "2024-09"},
        "highways": ["primary", "residential"],
        "counts": {
            "road_segments_total": 212450,
            "named_roads_total": 180000,
            "total_length_m": 9876543.2,
        },
    }


def test_roads_analytics_summary_returns_kpi_payload():
    app.dependency_overrides[get_db] = _override_summary_db
    try:
        response = client.get("/roads/analytics/summary", params={"from": "2023-02", "to": "2023-06"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "from": "2023-02",
        "to": "2023-06",
        "total_segments": 5500,
        "total_length_m": 123456.7,
        "unique_highway_types": 6,
        "top_highway_type": {
            "highway": "residential",
            "segment_count": 3200,
            "length_m": 65432.1,
        },
        "total_incidents": 1200,
        "segments_with_incidents": 420,
    }


def test_roads_analytics_timeseries_returns_series_and_total():
    app.dependency_overrides[get_db] = _override_timeseries_db
    try:
        response = client.get("/roads/analytics/timeseries", params={"from": "2023-02", "to": "2023-03"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "series": [
            {"month": "2023-02", "count": 410},
            {"month": "2023-03", "count": 430},
        ],
        "total": 840,
    }


def test_roads_analytics_highways_returns_items_and_other():
    app.dependency_overrides[get_db] = _override_highways_db
    try:
        response = client.get("/roads/analytics/highways", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "highway": "residential",
                "segment_count": 3200,
                "length_m": 65432.1,
                "incident_count": 900,
                "incidents_per_km": 13.75,
            },
            {
                "highway": "primary",
                "segment_count": 800,
                "length_m": 22345.6,
                "incident_count": 200,
                "incidents_per_km": 8.95,
            },
        ],
        "other": {
            "segment_count": 500,
            "length_m": 12345.0,
            "incident_count": 100,
        },
    }


def test_roads_analytics_risk_returns_ranked_segments():
    app.dependency_overrides[get_db] = _override_risk_db
    try:
        response = client.get("/roads/analytics/risk", params={"from": "2023-02", "to": "2023-06"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "segment_id": 7,
                "name": "Test Road",
                "highway": "primary",
                "length_m": 120.5,
                "incident_count": 8,
                "incidents_per_km": 66.39,
                "band": "red",
            }
        ],
        "meta": {
            "returned": 1,
            "limit": 50,
            "sort": "incidents_per_km",
        },
    }


def test_roads_analytics_anomaly_returns_target_vs_baseline():
    app.dependency_overrides[get_db] = _override_anomaly_db
    try:
        response = client.get("/roads/analytics/anomaly", params={"target": "2023-06", "baselineMonths": 6})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "target": "2023-06",
        "target_count": 120,
        "baseline_mean": 80.0,
        "ratio": 1.5,
        "flag": True,
    }


def test_roads_analytics_risk_rejects_invalid_sort_without_db_access():
    response = client.get("/roads/analytics/risk", params={"from": "2023-02", "to": "2023-06", "sort": "bad"})

    assert response.status_code == 400
    assert response.json()["detail"] == "sort must be incidents_per_km or incident_count"
