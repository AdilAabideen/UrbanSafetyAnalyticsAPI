from datetime import date, time

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _analytics_scan_rows():
    return [
        {
            "collision_index": "c-106",
            "month_date": date(2025, 3, 1),
            "collision_severity": "Serious",
            "road_type": "Single carriageway",
            "weather_condition": "Fine no high winds",
            "light_condition": "Daylight",
            "lsoa_code": "E0101",
            "number_of_casualties": 2,
            "casualty_severity_counts": {"Serious": 1, "Slight": 1},
        },
        {
            "collision_index": "c-105",
            "month_date": date(2025, 3, 1),
            "collision_severity": "Serious",
            "road_type": "Single carriageway",
            "weather_condition": "Rain no high winds",
            "light_condition": "Daylight",
            "lsoa_code": "E0101",
            "number_of_casualties": 1,
            "casualty_severity_counts": {"Slight": 1},
        },
        {
            "collision_index": "c-104",
            "month_date": date(2025, 2, 1),
            "collision_severity": "Fatal",
            "road_type": "Roundabout",
            "weather_condition": "Fine no high winds",
            "light_condition": "Darkness - lights lit",
            "lsoa_code": "E0102",
            "number_of_casualties": 1,
            "casualty_severity_counts": {"Fatal": 1},
        },
    ]


def _override_analytics_scan_db():
    handlers = {
        "ce.casualty_severity_counts": {"rows": _analytics_scan_rows()},
    }
    yield InMemoryDB(handlers)


def _override_incidents_db():
    handlers = {
        "ORDER BY ce.month DESC, ce.collision_index DESC": {
            "rows": [
                {
                    "collision_index": "c-101",
                    "month_label": "2025-03",
                    "collision_date_label": "2025-03-06",
                    "collision_time": time(8, 15),
                    "collision_severity": "Serious",
                    "road_type": "Single carriageway",
                    "speed_limit": "30 mph",
                    "light_conditions": "Daylight",
                    "weather_conditions": "Fine no high winds",
                    "road_surface_conditions": "Dry",
                    "lsoa_code": "E0101",
                    "number_of_vehicles": 2,
                    "number_of_casualties": 1,
                    "longitude": -1.55,
                    "latitude": 53.8,
                },
                {
                    "collision_index": "c-100",
                    "month_label": "2025-03",
                    "collision_date_label": "2025-03-05",
                    "collision_time": time(7, 45),
                    "collision_severity": "Slight",
                    "road_type": "Roundabout",
                    "speed_limit": "20 mph",
                    "light_conditions": "Darkness - lights lit",
                    "weather_conditions": "Rain no high winds",
                    "road_surface_conditions": "Wet or damp",
                    "lsoa_code": "E0102",
                    "number_of_vehicles": 1,
                    "number_of_casualties": 2,
                    "longitude": -1.551,
                    "latitude": 53.801,
                },
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_anomaly_db():
    handlers = {
        "SELECT COUNT(*)::bigint AS target_count": {
            "rows": [{"target_count": 12}]
        },
        "AVG(COALESCE(counts.count, 0))": {
            "rows": [{"baseline_mean": 8.0}]
        },
    }
    yield InMemoryDB(handlers)


def test_collision_analytics_summary_returns_region_summary():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get(
            "/collisions/analytics/summary",
            params={"from": "2025-02", "to": "2025-03", "minLon": -1.6, "minLat": 53.78, "maxLon": -1.52, "maxLat": 53.82},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "from": "2025-02",
        "to": "2025-03",
        "total_collisions": 3,
        "total_casualties": 4,
        "unique_lsoas": 2,
        "collisions_with_casualties": 3,
        "fatal_casualties": 1,
        "serious_casualties": 1,
        "slight_casualties": 2,
        "avg_casualties_per_collision": 1.33,
        "top_collision_severity": {"collision_severity": "Serious", "count": 1},
        "top_road_type": {"road_type": "Single carriageway", "count": 2},
        "top_weather_condition": {"weather_condition": "Fine no high winds", "count": 2},
        "top_light_condition": {"light_condition": "Daylight", "count": 2},
    }


def test_collisions_incidents_returns_paginated_items():
    app.dependency_overrides[get_db] = _override_incidents_db
    try:
        response = client.get(
            "/collisions/incidents",
            params={"from": "2025-02", "to": "2025-03", "limit": 1, "collisionSeverity": "Serious"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "collision_index": "c-101",
                "month": "2025-03",
                "date": "2025-03-06",
                "time": "08:15",
                "collision_severity": "Serious",
                "road_type": "Single carriageway",
                "speed_limit": "30 mph",
                "light_conditions": "Daylight",
                "weather_conditions": "Fine no high winds",
                "road_surface_conditions": "Dry",
                "number_of_vehicles": 2,
                "number_of_casualties": 1,
                "lsoa_code": "E0101",
                "lon": -1.55,
                "lat": 53.8,
            }
        ],
        "meta": {
            "returned": 1,
            "limit": 1,
            "truncated": True,
            "nextCursor": "2025-03|c-101",
            "filters": {
                "from": "2025-02",
                "to": "2025-03",
                "collisionSeverity": ["Serious"],
                "roadType": None,
                "lsoaCode": None,
                "weatherCondition": None,
                "lightCondition": None,
                "roadSurfaceCondition": None,
                "bbox": None,
            },
        },
    }


def test_collision_analytics_timeseries_returns_series_and_total():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get(
            "/collision/analytics/timeseries",
            params={"from": "2025-02", "to": "2025-03", "roadType": "Single carriageway"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "series": [
            {"month": "2025-02", "count": 1},
            {"month": "2025-03", "count": 2},
        ],
        "total": 3,
    }


def test_collision_analytics_anomaly_returns_ratio():
    app.dependency_overrides[get_db] = _override_anomaly_db
    try:
        response = client.get(
            "/collisions/analytics/anomaly",
            params={"target": "2025-03", "baselineMonths": 6},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "target": "2025-03",
        "target_count": 12,
        "baseline_mean": 8.0,
        "ratio": 1.5,
        "flag": True,
    }
