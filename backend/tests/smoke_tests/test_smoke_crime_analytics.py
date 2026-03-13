from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_summary_db():
    handlers = {
        "COUNT(DISTINCT COALESCE(NULLIF(ce.lsoa_code, ''), NULLIF(ce.lsoa_name, '')))": {
            "rows": [
                {
                    "total_crimes": 12345,
                    "unique_lsoas": 84,
                    "unique_crime_types": 12,
                    "crimes_with_outcomes": 8700,
                }
            ]
        },
        "GROUP BY COALESCE(NULLIF(ce.crime_type, ''), 'unknown')": {
            "rows": [
                {"crime_type": "Violence and sexual offences", "count": 4200},
                {"crime_type": "Shoplifting", "count": 2100},
            ]
        },
        "GROUP BY COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown')": {
            "rows": [
                {"outcome": "Investigation complete; no suspect identified", "count": 3100},
            ]
        },
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


def _override_timeseries_db():
    counts_by_month = {
        "2023-02": 4100,
        "2023-03": 4300,
    }

    handlers = {
        "SELECT COUNT(*)::bigint AS count": lambda params: {
            "rows": [
                {
                    "count": counts_by_month[params["series_month_date"].strftime("%Y-%m")],
                }
            ]
        },
    }
    yield InMemoryDB(handlers)


def _override_types_db():
    rows = [
        {"crime_type": "Violence and sexual offences", "count": 4200},
        {"crime_type": "Shoplifting", "count": 2100},
        {"crime_type": "Public order", "count": 900},
    ]

    handlers = {
        "SELECT COUNT(*)::bigint AS total_count": {"rows": [{"total_count": 7200}]},
        "GROUP BY COALESCE(NULLIF(ce.crime_type, ''), 'unknown')": lambda params: {
            "rows": rows[: params["limit"]]
        },
    }
    yield InMemoryDB(handlers)


def _override_outcomes_db():
    rows = [
        {"outcome": "Investigation complete; no suspect identified", "count": 3100},
        {"outcome": "Under investigation", "count": 1200},
        {"outcome": "Court result unavailable", "count": 900},
    ]

    handlers = {
        "SELECT COUNT(*)::bigint AS total_count": {"rows": [{"total_count": 5200}]},
        "GROUP BY COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown')": lambda params: {
            "rows": rows[: params["limit"]]
        },
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
    app.dependency_overrides[get_db] = _override_summary_db
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
        "total_crimes": 12345,
        "unique_lsoas": 84,
        "unique_crime_types": 12,
        "top_crime_type": {
            "crime_type": "Violence and sexual offences",
            "count": 4200,
        },
        "crimes_with_outcomes": 8700,
        "top_crime_types": [
            {"crime_type": "Violence and sexual offences", "count": 4200},
            {"crime_type": "Shoplifting", "count": 2100},
        ],
        "top_outcomes": [
            {"outcome": "Investigation complete; no suspect identified", "count": 3100},
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
    app.dependency_overrides[get_db] = _override_timeseries_db
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
            {"month": "2023-02", "count": 4100},
            {"month": "2023-03", "count": 4300},
        ],
        "total": 8400,
    }


def test_crimes_analytics_type_breakdown_returns_items_and_other_count():
    app.dependency_overrides[get_db] = _override_types_db
    try:
        response = client.get("/crimes/analytics/types", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"crime_type": "Violence and sexual offences", "count": 4200},
            {"crime_type": "Shoplifting", "count": 2100},
        ],
        "other_count": 900,
    }


def test_crimes_analytics_outcome_breakdown_returns_items_and_other_count():
    app.dependency_overrides[get_db] = _override_outcomes_db
    try:
        response = client.get("/crimes/analytics/outcomes", params={"limit": 2})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {"outcome": "Investigation complete; no suspect identified", "count": 3100},
            {"outcome": "Under investigation", "count": 1200},
        ],
        "other_count": 900,
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
