from datetime import date

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _run_with_db(handlers, method, url):
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url)
    finally:
        app.dependency_overrides.clear()


def test_collision_incidents_returns_paginated_payload():
    response = _run_with_db(
        {
            "/* collisions_incidents */": {
                "rows": [
                    {
                        "collision_index": "c-3",
                        "month_label": "2025-02",
                        "collision_date_label": "2025-02-14",
                        "collision_time": "08:45:00",
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "speed_limit": "30",
                        "light_conditions": "Daylight",
                        "weather_conditions": "Fine",
                        "road_surface_conditions": "Dry",
                        "lsoa_code": "LSOA-1",
                        "number_of_vehicles": 2,
                        "number_of_casualties": 1,
                        "longitude": -1.55,
                        "latitude": 53.8,
                    },
                    {
                        "collision_index": "c-2",
                        "month_label": "2025-01",
                        "collision_date_label": "2025-01-20",
                        "collision_time": "17:30:00",
                        "collision_severity": "Serious",
                        "road_type": "Roundabout",
                        "speed_limit": "40",
                        "light_conditions": "Darkness",
                        "weather_conditions": "Rain",
                        "road_surface_conditions": "Wet",
                        "lsoa_code": "LSOA-2",
                        "number_of_vehicles": 3,
                        "number_of_casualties": 2,
                        "longitude": -1.54,
                        "latitude": 53.81,
                    },
                    {
                        "collision_index": "c-1",
                        "month_label": "2025-01",
                        "collision_date_label": "2025-01-05",
                        "collision_time": "12:00:00",
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "speed_limit": "30",
                        "light_conditions": "Daylight",
                        "weather_conditions": "Fine",
                        "road_surface_conditions": "Dry",
                        "lsoa_code": "LSOA-3",
                        "number_of_vehicles": 2,
                        "number_of_casualties": 0,
                        "longitude": -1.53,
                        "latitude": 53.82,
                    },
                ]
            }
        },
        "GET",
        "/collisions/incidents?from=2025-01&to=2025-02&limit=2",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 2
    assert payload["meta"]["truncated"] is True
    assert payload["meta"]["nextCursor"] == "2025-01|c-2"
    assert payload["items"][0]["collision_index"] == "c-3"


def test_collisions_map_points_returns_feature_collection():
    response = _run_with_db(
        {
            "/* collisions_map_points */": {
                "rows": [
                    {
                        "collision_index": "c-10",
                        "month_label": "2025-03",
                        "collision_date_label": "2025-03-10",
                        "collision_time": "09:15:00",
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "speed_limit": "30",
                        "light_conditions": "Daylight",
                        "weather_conditions": "Fine",
                        "road_surface_conditions": "Dry",
                        "lsoa_code": "LSOA-10",
                        "number_of_vehicles": 2,
                        "number_of_casualties": 1,
                        "geometry": '{"type":"Point","coordinates":[-1.55,53.8]}',
                    }
                ]
            }
        },
        "GET",
        "/collisions/map?minLon=-1.6&minLat=53.7&maxLon=-1.5&maxLat=53.9&zoom=15&mode=points&month=2025-03",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["mode"] == "points"
    assert payload["features"][0]["geometry"]["type"] == "Point"
    assert payload["features"][0]["properties"]["collision_index"] == "c-10"


def test_collisions_map_clusters_returns_cluster_features():
    response = _run_with_db(
        {
            "/* collisions_map_clusters */": {
                "rows": [
                    {
                        "cluster_id": "10:2:3",
                        "count": 5,
                        "total_casualties": 3,
                        "geometry": '{"type":"Point","coordinates":[-1.55,53.8]}',
                        "top_collision_severities": '{"Slight": 4, "Serious": 1}',
                    }
                ]
            }
        },
        "GET",
        "/collisions/map?minLon=-1.6&minLat=53.7&maxLon=-1.5&maxLat=53.9&zoom=10&mode=clusters&month=2025-03",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["mode"] == "clusters"
    assert payload["features"][0]["properties"]["cluster"] is True
    assert payload["features"][0]["properties"]["top_collision_severities"]["Slight"] == 4


def test_collision_analytics_summary_returns_expected_totals():
    response = _run_with_db(
        {
            "/* collisions_analytics_snapshot */": {
                "rows": [
                    {
                        "collision_index": "c-2",
                        "month_date": date(2025, 2, 1),
                        "collision_severity": "Serious",
                        "road_type": "Roundabout",
                        "weather_condition": "Rain",
                        "light_condition": "Darkness",
                        "lsoa_code": "LSOA-1",
                        "number_of_casualties": 2,
                        "casualty_severity_counts": '{"Serious": 1, "Slight": 1}',
                    },
                    {
                        "collision_index": "c-1",
                        "month_date": date(2025, 1, 1),
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "weather_condition": "Fine",
                        "light_condition": "Daylight",
                        "lsoa_code": None,
                        "number_of_casualties": 0,
                        "casualty_severity_counts": '{"Fatal": 1}',
                    },
                ]
            }
        },
        "GET",
        "/collisions/analytics/summary?from=2025-01&to=2025-02",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["from"] == "2025-01"
    assert payload["to"] == "2025-02"
    assert payload["total_collisions"] == 2
    assert payload["total_casualties"] == 2
    assert payload["unique_lsoas"] == 1
    assert payload["fatal_casualties"] == 1
    assert payload["serious_casualties"] == 1
    assert payload["slight_casualties"] == 1
    assert payload["top_collision_severity"]["count"] == 1


def test_collision_analytics_timeseries_returns_monthly_series():
    response = _run_with_db(
        {
            "/* collisions_analytics_snapshot */": {
                "rows": [
                    {
                        "collision_index": "c-3",
                        "month_date": date(2025, 2, 1),
                        "collision_severity": "Serious",
                        "road_type": "Roundabout",
                        "weather_condition": "Rain",
                        "light_condition": "Darkness",
                        "lsoa_code": "LSOA-2",
                        "number_of_casualties": 1,
                        "casualty_severity_counts": '{"Serious": 1}',
                    },
                    {
                        "collision_index": "c-2",
                        "month_date": date(2025, 1, 1),
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "weather_condition": "Fine",
                        "light_condition": "Daylight",
                        "lsoa_code": "LSOA-1",
                        "number_of_casualties": 0,
                        "casualty_severity_counts": '{}',
                    },
                    {
                        "collision_index": "c-1",
                        "month_date": date(2025, 1, 1),
                        "collision_severity": "Slight",
                        "road_type": "Single carriageway",
                        "weather_condition": "Fine",
                        "light_condition": "Daylight",
                        "lsoa_code": None,
                        "number_of_casualties": 0,
                        "casualty_severity_counts": '{}',
                    },
                ]
            }
        },
        "GET",
        "/collisions/analytics/timeseries?from=2025-01&to=2025-02",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["series"] == [
        {"month": "2025-01", "count": 2},
        {"month": "2025-02", "count": 1},
    ]
    assert payload["total"] == 3
