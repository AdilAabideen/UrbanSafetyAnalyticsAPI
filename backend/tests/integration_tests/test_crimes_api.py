from datetime import date

from fastapi.testclient import TestClient

from app.api_utils import crime_utils
from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _run_with_db(handlers, method, url):
    def _override_get_db():
        yield InMemoryDB(handlers)

    crime_utils._analytics_snapshot_cache.clear()
    crime_utils._analytics_snapshot_inflight.clear()
    # override the real DB dependency with the in-memory stub for each smoke call
    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url)
    finally:
        app.dependency_overrides.clear()


def test_crime_incidents_returns_paginated_payload():
    # ensure the list endpoint returns a correctly page-sized payload
    response = _run_with_db(
        {
            "/* crimes_incidents */": {
                "rows": [
                    {
                        "id": 3,
                        "crime_id": "crime-3",
                        "month_label": "2025-02",
                        "crime_type": "burglary",
                        "last_outcome_category": "Under investigation",
                        "location_text": "High Street",
                        "reported_by": "Police",
                        "falls_within": "Leeds",
                        "lsoa_code": "LSOA-3",
                        "lsoa_name": "Leeds Central",
                        "lon": -1.55,
                        "lat": 53.8,
                    },
                    {
                        "id": 2,
                        "crime_id": "crime-2",
                        "month_label": "2025-01",
                        "crime_type": "robbery",
                        "last_outcome_category": "unknown",
                        "location_text": "Station Road",
                        "reported_by": "Police",
                        "falls_within": "Leeds",
                        "lsoa_code": "LSOA-2",
                        "lsoa_name": "Leeds North",
                        "lon": -1.54,
                        "lat": 53.81,
                    },
                    {
                        "id": 1,
                        "crime_id": "crime-1",
                        "month_label": "2025-01",
                        "crime_type": "vehicle crime",
                        "last_outcome_category": "Resolved",
                        "location_text": "Park Lane",
                        "reported_by": "Police",
                        "falls_within": "Leeds",
                        "lsoa_code": "LSOA-1",
                        "lsoa_name": "Leeds South",
                        "lon": -1.53,
                        "lat": 53.82,
                    },
                ]
            }
        },
        "GET",
        "/crimes/incidents?from=2025-01&to=2025-02&limit=2",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["returned"] == 2
    assert payload["meta"]["truncated"] is True
    assert payload["meta"]["nextCursor"] == "2025-01|2"
    assert payload["items"][0]["crime_type"] == "burglary"


def test_crimes_map_points_returns_feature_collection():
    response = _run_with_db(
        {
            "/* crimes_map_points */": {
                "rows": [
                    {
                        "id": 7,
                        "crime_id": "crime-7",
                        "month_label": "2025-03",
                        "crime_type": "burglary",
                        "last_outcome_category": "Resolved",
                        "location_text": "Briggate",
                        "reported_by": "Police",
                        "falls_within": "Leeds",
                        "lsoa_code": "LSOA-7",
                        "lsoa_name": "Leeds Central",
                        "geometry": '{"type":"Point","coordinates":[-1.55,53.8]}',
                    }
                ]
            }
        },
        "GET",
        "/crimes/map?minLon=-1.6&minLat=53.7&maxLon=-1.5&maxLat=53.9&zoom=15&mode=points&month=2025-03",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["mode"] == "points"
    assert payload["features"][0]["geometry"]["type"] == "Point"
    assert payload["features"][0]["properties"]["crime_id"] == "crime-7"


def test_crimes_map_clusters_returns_cluster_features():
    response = _run_with_db(
        {
            "/* crimes_map_clusters */": {
                "rows": [
                    {
                        "cluster_id": "10:2:3",
                        "count": 4,
                        "geometry": '{"type":"Point","coordinates":[-1.55,53.8]}',
                        "top_crime_types": '{"burglary": 3, "robbery": 1}',
                    }
                ]
            }
        },
        "GET",
        "/crimes/map?minLon=-1.6&minLat=53.7&maxLon=-1.5&maxLat=53.9&zoom=10&mode=clusters&month=2025-03",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["mode"] == "clusters"
    assert payload["features"][0]["properties"]["cluster"] is True
    assert payload["features"][0]["properties"]["top_crime_types"]["burglary"] == 3


def test_crime_analytics_summary_returns_expected_totals():
    response = _run_with_db(
        {
            "/* crimes_analytics_snapshot */": {
                "rows": [
                    {
                        "id": 2,
                        "month_date": date(2025, 2, 1),
                        "crime_type": "burglary",
                        "raw_outcome": "Under investigation",
                        "outcome": "Under investigation",
                        "lsoa_code": "LSOA-1",
                        "lsoa_name": "Leeds Central",
                    },
                    {
                        "id": 1,
                        "month_date": date(2025, 1, 1),
                        "crime_type": "robbery",
                        "raw_outcome": None,
                        "outcome": "unknown",
                        "lsoa_code": None,
                        "lsoa_name": "Leeds North",
                    },
                ]
            }
        },
        "GET",
        "/crimes/analytics/summary?from=2025-01&to=2025-02",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["from"] == "2025-01"
    assert payload["to"] == "2025-02"
    assert payload["total_crimes"] == 2
    assert payload["unique_lsoas"] == 2
    assert payload["top_crime_type"]["count"] == 1
    assert len(payload["top_outcomes"]) == 2


def test_crime_analytics_timeseries_returns_monthly_series():
    response = _run_with_db(
        {
            "/* crimes_analytics_snapshot */": {
                "rows": [
                    {
                        "id": 3,
                        "month_date": date(2025, 2, 1),
                        "crime_type": "burglary",
                        "raw_outcome": "Under investigation",
                        "outcome": "Under investigation",
                        "lsoa_code": "LSOA-1",
                        "lsoa_name": "Leeds Central",
                    },
                    {
                        "id": 2,
                        "month_date": date(2025, 1, 1),
                        "crime_type": "robbery",
                        "raw_outcome": None,
                        "outcome": "unknown",
                        "lsoa_code": None,
                        "lsoa_name": "Leeds North",
                    },
                    {
                        "id": 1,
                        "month_date": date(2025, 1, 1),
                        "crime_type": "burglary",
                        "raw_outcome": "Resolved",
                        "outcome": "Resolved",
                        "lsoa_code": "LSOA-2",
                        "lsoa_name": "Leeds South",
                    },
                ]
            }
        },
        "GET",
        "/crimes/analytics/timeseries?from=2025-01&to=2025-02",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["series"] == [
        {"month": "2025-01", "count": 2},
        {"month": "2025-02", "count": 1},
    ]
    assert payload["total"] == 3


def test_crime_by_id_returns_feature():
    response = _run_with_db(
        {
            "/* crimes_detail */": {
                "scalar": (
                    '{"type":"Feature","geometry":{"type":"Point","coordinates":[-1.55,53.8]},'
                    '"properties":{"id":1,"crime_id":"crime-1","month":"2025-03-01",'
                    '"reported_by":"Police","falls_within":"Leeds","lon":-1.55,"lat":53.8,'
                    '"location_text":"Briggate","lsoa_code":"LSOA-1","lsoa_name":"Leeds Central",'
                    '"crime_type":"burglary","last_outcome_category":"Resolved","context":null,'
                    '"created_at":"2026-03-13T12:00:00Z"}}'
                )
            }
        },
        "GET",
        "/crimes/1",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Feature"
    assert payload["properties"]["crime_type"] == "burglary"
    assert payload["geometry"]["coordinates"] == [-1.55, 53.8]
