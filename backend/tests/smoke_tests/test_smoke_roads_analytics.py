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
        "COUNT(*) FILTER (WHERE incident_count > 0)::bigint AS roads_with_incidents": {
            "rows": [
                {
                    "total_segments": 5500,
                    "total_length_m": 123456.7,
                    "unique_highway_types": 6,
                    "roads_with_incidents": 420,
                    "total_incidents": 1200,
                    "avg_incidents_per_km": 9.72,
                }
            ]
        },
        "ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC": {
            "rows": [
                {
                    "highway": "residential",
                    "segment_count": 3200,
                    "length_m": 65432.1,
                    "incident_count": 900,
                    "incidents_per_km": 13.75,
                }
            ]
        },
        "GROUP BY crime_type": {"rows": [{"crime_type": "Shoplifting", "count": 350}]},
        "GROUP BY outcome": {"rows": [{"outcome": "Under investigation", "count": 260}]},
        "ORDER BY risk_score DESC, incident_count DESC, incidents_per_km DESC": {
            "rows": [
                {
                    "segment_id": 7,
                    "name": "Test Road",
                    "highway": "primary",
                    "length_m": 120.5,
                    "incident_count": 80,
                    "incidents_per_km": 66.39,
                    "risk_score": 95.0,
                    "band": "red",
                }
            ]
        },
        "GROUP BY band": {
            "rows": [
                {"band": "red", "count": 4},
                {"band": "orange", "count": 10},
                {"band": "green", "count": 406},
            ]
        },
        "SELECT COUNT(*)::bigint AS incident_count\n        FROM crime_events ce": {
            "rows": [{"incident_count": 1000}]
        },
    }
    yield InMemoryDB(handlers)


def _override_trends_db():
    handlers = {
        "'overall' AS group_key": {
            "rows": [
                {"group_key": "overall", "month": "2023-02", "count": 410},
                {"group_key": "overall", "month": "2023-03", "count": 430},
            ]
        },
        "top_groups AS (": {
            "rows": [
                {"group_key": "residential", "total": 500, "month": "2023-02", "count": 210},
                {"group_key": "residential", "total": 500, "month": "2023-03", "count": 290},
                {"group_key": "primary", "total": 200, "month": "2023-02", "count": 90},
                {"group_key": "primary", "total": 200, "month": "2023-03", "count": 110},
            ]
        },
        "SELECT COUNT(*)::bigint AS incident_count\n        FROM crime_events ce": {
            "rows": [{"incident_count": 700}]
        },
    }
    yield InMemoryDB(handlers)


def _override_highways_db():
    handlers = {
        "ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC": {
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


def _override_breakdowns_db():
    handlers = {
        "ORDER BY count DESC, rs.highway ASC": {
            "rows": [
                {
                    "highway": "residential",
                    "segment_count": 3200,
                    "length_m": 65432.1,
                    "count": 900,
                    "incidents_per_km": 13.75,
                },
                {
                    "highway": "primary",
                    "segment_count": 800,
                    "length_m": 22345.6,
                    "count": 200,
                    "incidents_per_km": 8.95,
                },
            ]
        },
        "GROUP BY crime_type\n        ORDER BY count DESC, crime_type ASC": {
            "rows": [
                {"crime_type": "Shoplifting", "count": 350},
                {"crime_type": "Vehicle crime", "count": 120},
            ]
        },
        "GROUP BY outcome\n        ORDER BY count DESC, outcome ASC": {
            "rows": [
                {"outcome": "Under investigation", "count": 260},
                {"outcome": "Unable to prosecute suspect", "count": 180},
            ]
        },
        "SELECT COUNT(*)::bigint AS total_incidents FROM events_scope": {
            "rows": [{"total_incidents": 1200}]
        },
    }
    yield InMemoryDB(handlers)


def _override_risk_db():
    handlers = {
        "dominant_crime_types AS (": {
            "rows": [
                {
                    "segment_id": 7,
                    "name": "Test Road",
                    "highway": "primary",
                    "length_m": 120.5,
                    "incident_count": 80,
                    "incidents_per_km": 66.39,
                    "dominant_crime_type": "Shoplifting",
                    "dominant_outcome": "Under investigation",
                    "previous_incident_count": 50,
                    "total_incidents_in_scope": 1200,
                    "risk_score": 92.5,
                    "band": "red",
                }
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_anomaly_db():
    handlers = {
        "SELECT COUNT(*)::bigint AS target_count": {"rows": [{"target_count": 120}]},
        "AVG(COALESCE(counts.count, 0))": {"rows": [{"baseline_mean": 80.0}]},
    }
    yield InMemoryDB(handlers)


def test_roads_analytics_meta_returns_discoverability_payload():
    app.dependency_overrides[get_db] = _override_meta_db
    try:
        response = client.get("/roads/analytics/meta")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["months"] == {"min": "2023-02", "max": "2024-09"}
    assert data["highways"] == ["primary", "residential"]
    assert data["counts"]["road_segments_total"] == 212450


def test_roads_analytics_summary_returns_richer_kpi_payload():
    app.dependency_overrides[get_db] = _override_summary_db
    try:
        response = client.get("/roads/analytics/summary", params={"from": "2023-02", "to": "2023-06"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["total_incidents"] == 1200
    assert data["roads_with_incidents"] == 420
    assert data["top_road"]["segment_id"] == 7
    assert data["top_crime_type"]["crime_type"] == "Shoplifting"
    assert data["top_outcome"]["outcome"] == "Under investigation"
    assert data["band_breakdown"]["red"] == 4
    assert isinstance(data["insights"], list)
    assert data["current_vs_previous_pct"] == 20.0


def test_roads_analytics_trends_returns_overall_series():
    app.dependency_overrides[get_db] = _override_trends_db
    try:
        response = client.get("/roads/analytics/trends", params={"from": "2023-02", "to": "2023-03"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["groupBy"] == "overall"
    assert data["series"][0]["key"] == "overall"
    assert data["total"] == 840
    assert data["peak"]["month"] == "2023-03"


def test_roads_analytics_trends_supports_grouping():
    app.dependency_overrides[get_db] = _override_trends_db
    try:
        response = client.get(
            "/roads/analytics/trends",
            params={"from": "2023-02", "to": "2023-03", "groupBy": "highway"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["groupBy"] == "highway"
    assert data["series"][0]["key"] == "residential"
    assert data["series"][0]["total"] == 500


def test_roads_analytics_highways_returns_items_and_insights():
    app.dependency_overrides[get_db] = _override_highways_db
    try:
        response = client.get("/roads/analytics/highways", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert "message" in data["items"][0]
    assert "share_of_incidents" in data["items"][0]
    assert len(data["insights"]) >= 1


def test_roads_analytics_breakdowns_returns_three_breakdown_sets():
    app.dependency_overrides[get_db] = _override_breakdowns_db
    try:
        response = client.get("/roads/analytics/breakdowns", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["by_highway"][0]["highway"] == "residential"
    assert data["by_crime_type"][0]["crime_type"] == "Shoplifting"
    assert data["by_outcome"][0]["outcome"] == "Under investigation"
    assert len(data["insights"]) >= 1


def test_roads_analytics_risk_returns_ranked_segments():
    app.dependency_overrides[get_db] = _override_risk_db
    try:
        response = client.get("/roads/analytics/risk", params={"from": "2023-02", "to": "2023-06"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["items"][0]["segment_id"] == 7
    assert data["items"][0]["risk_score"] == 92.5
    assert data["items"][0]["dominant_crime_type"] == "Shoplifting"
    assert data["items"][0]["dominant_outcome"] == "Under investigation"
    assert "message" in data["items"][0]


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
    assert response.json()["detail"] == "sort must be risk_score, incidents_per_km, or incident_count"
