from datetime import date

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _analytics_scan_rows():
    return [
        {
            "id": 106,
            "month_date": date(2023, 3, 1),
            "crime_type": "Violence and sexual offences",
            "raw_outcome": "Investigation complete; no suspect identified",
            "outcome": "Investigation complete; no suspect identified",
            "lsoa_code": "E0101",
            "lsoa_name": "Leeds 010A",
        },
        {
            "id": 105,
            "month_date": date(2023, 3, 1),
            "crime_type": "Violence and sexual offences",
            "raw_outcome": "Investigation complete; no suspect identified",
            "outcome": "Investigation complete; no suspect identified",
            "lsoa_code": "E0101",
            "lsoa_name": "Leeds 010A",
        },
        {
            "id": 104,
            "month_date": date(2023, 3, 1),
            "crime_type": "Shoplifting",
            "raw_outcome": "Under investigation",
            "outcome": "Under investigation",
            "lsoa_code": "E0102",
            "lsoa_name": "Leeds 010B",
        },
        {
            "id": 103,
            "month_date": date(2023, 2, 1),
            "crime_type": "Violence and sexual offences",
            "raw_outcome": "Investigation complete; no suspect identified",
            "outcome": "Investigation complete; no suspect identified",
            "lsoa_code": "E0102",
            "lsoa_name": "Leeds 010B",
        },
        {
            "id": 102,
            "month_date": date(2023, 2, 1),
            "crime_type": "Shoplifting",
            "raw_outcome": "Under investigation",
            "outcome": "Under investigation",
            "lsoa_code": "E0103",
            "lsoa_name": "Leeds 010C",
        },
        {
            "id": 101,
            "month_date": date(2023, 2, 1),
            "crime_type": "Public order",
            "raw_outcome": "Court result unavailable",
            "outcome": "Court result unavailable",
            "lsoa_code": None,
            "lsoa_name": None,
        },
    ]


def _override_analytics_scan_db():
    handlers = {
        "NULLIF(ce.last_outcome_category, '') AS raw_outcome": {
            "rows": _analytics_scan_rows()
        }
    }
    yield InMemoryDB(handlers)


def _override_incidents_db():
    handlers = {
        "ORDER BY ce.month DESC, ce.id DESC": {
            "rows": [
                {
                    "id": 101,
                    "crime_id": "crime-101",
                    "month_label": "2023-06",
                    "crime_type": "Shoplifting",
                    "last_outcome_category": "Under investigation",
                    "location_text": "Market Street",
                    "reported_by": "West Yorkshire Police",
                    "falls_within": "Leeds",
                    "lsoa_code": "E0101",
                    "lsoa_name": "Leeds 010A",
                    "lon": -1.55,
                    "lat": 53.8,
                },
                {
                    "id": 100,
                    "crime_id": "crime-100",
                    "month_label": "2023-06",
                    "crime_type": "Public order",
                    "last_outcome_category": "unknown",
                    "location_text": "Briggate",
                    "reported_by": "West Yorkshire Police",
                    "falls_within": "Leeds",
                    "lsoa_code": "E0102",
                    "lsoa_name": "Leeds 010B",
                    "lon": -1.551,
                    "lat": 53.801,
                },
            ]
        }
    }
    yield InMemoryDB(handlers)


def _override_anomaly_db():
    handlers = {
        "SELECT COUNT(*)::bigint AS target_count": {
            "rows": [{"target_count": 1200}]
        },
        "AVG(COALESCE(counts.count, 0))": {
            "rows": [{"baseline_mean": 800.0}]
        },
    }
    yield InMemoryDB(handlers)


def test_crimes_analytics_summary_returns_region_summary():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get(
            "/crimes/analytics/summary",
            params={"from": "2023-02", "to": "2023-06", "minLon": -1.6, "minLat": 53.78, "maxLon": -1.52, "maxLat": 53.82},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "from": "2023-02",
        "to": "2023-06",
        "total_crimes": 6,
        "unique_lsoas": 3,
        "unique_crime_types": 3,
        "top_crime_type": {
            "crime_type": "Violence and sexual offences",
            "count": 3,
        },
        "crimes_with_outcomes": 6,
        "top_crime_types": [
            {"crime_type": "Violence and sexual offences", "count": 3},
            {"crime_type": "Shoplifting", "count": 2},
            {"crime_type": "Public order", "count": 1},
        ],
        "top_outcomes": [
            {"outcome": "Investigation complete; no suspect identified", "count": 3},
            {"outcome": "Under investigation", "count": 2},
            {"outcome": "Court result unavailable", "count": 1},
        ],
    }


def test_crimes_incidents_returns_paginated_items():
    app.dependency_overrides[get_db] = _override_incidents_db
    try:
        response = client.get(
            "/crimes/incidents",
            params={"from": "2023-02", "to": "2023-06", "limit": 1, "crimeType": "Shoplifting"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "id": 101,
                "crime_id": "crime-101",
                "month": "2023-06",
                "crime_type": "Shoplifting",
                "last_outcome_category": "Under investigation",
                "location_text": "Market Street",
                "reported_by": "West Yorkshire Police",
                "falls_within": "Leeds",
                "lsoa_code": "E0101",
                "lsoa_name": "Leeds 010A",
                "lon": -1.55,
                "lat": 53.8,
            }
        ],
        "meta": {
            "returned": 1,
            "limit": 1,
            "truncated": True,
            "nextCursor": "2023-06|101",
            "filters": {
                "from": "2023-02",
                "to": "2023-06",
                "crimeType": ["Shoplifting"],
                "lastOutcomeCategory": None,
                "lsoaName": None,
                "bbox": None,
            },
        },
    }


def test_crime_analytics_timeseries_returns_series_and_total():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get(
            "/crime/analytics/timeseries",
            params={"from": "2023-02", "to": "2023-03", "crimeType": "Shoplifting"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "series": [
            {"month": "2023-02", "count": 3},
            {"month": "2023-03", "count": 3},
        ],
        "total": 6,
    }


def test_crimes_analytics_type_breakdown_returns_items_and_other_count():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get("/crimes/analytics/types", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"crime_type": "Violence and sexual offences", "count": 3},
            {"crime_type": "Shoplifting", "count": 2},
        ],
        "other_count": 1,
    }


def test_crimes_analytics_outcome_breakdown_returns_items_and_other_count():
    app.dependency_overrides[get_db] = _override_analytics_scan_db
    try:
        response = client.get("/crimes/analytics/outcomes", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"outcome": "Investigation complete; no suspect identified", "count": 3},
            {"outcome": "Under investigation", "count": 2},
        ],
        "other_count": 1,
    }


def test_crime_analytics_anomoly_alias_returns_target_vs_baseline():
    app.dependency_overrides[get_db] = _override_anomaly_db
    try:
        response = client.get(
            "/crime/analytics/anomoly",
            params={"target": "2023-06", "baselineMonths": 6, "minLon": -1.6, "minLat": 53.78, "maxLon": -1.52, "maxLat": 53.82},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "target": "2023-06",
        "target_count": 1200,
        "baseline_mean": 800.0,
        "ratio": 1.5,
        "flag": True,
    }


def test_crimes_analytics_summary_rejects_partial_bbox_without_db_access():
    response = client.get("/crimes/analytics/summary", params={"from": "2023-02", "to": "2023-06", "minLon": -1.6})

    assert response.status_code == 400
    assert response.json()["detail"] == "minLon, minLat, maxLon, and maxLat must all be provided together"
