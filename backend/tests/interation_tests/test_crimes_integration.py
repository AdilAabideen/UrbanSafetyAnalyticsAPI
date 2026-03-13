from datetime import date

from fastapi.testclient import TestClient

from app.api_utils import crime_utils_db
from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _run_smoke(handlers, method, url):
    def _override_db():
        yield InMemoryDB(handlers)

    crime_utils_db._analytics_snapshot_cache.clear()
    crime_utils_db._analytics_snapshot_inflight.clear()
    app.dependency_overrides[get_db] = _override_db
    try:
        return client.request(method, url)
    finally:
        app.dependency_overrides.clear()


def test_smoke_crime_incidents_endpoint():
    # quick validation that the incidents route responds with one record
    response = _run_smoke(
        {
            "/* crimes_incidents */": {
                "rows": [
                    {
                        "id": 1,
                        "crime_id": "smoke-1",
                        "month_label": "2025-01",
                        "crime_type": "burglary",
                        "last_outcome_category": "Resolved",
                        "location_text": "Smoke Lane",
                        "reported_by": "Police",
                        "falls_within": "Test",
                        "lsoa_code": "LSOA-123",
                        "lsoa_name": "Test Town",
                        "lon": -1.5,
                        "lat": 53.8,
                    }
                ]
            }
        },
        "GET",
        "/crimes/incidents?from=2025-01&to=2025-01&limit=1",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["crime_type"] == "burglary"
    assert payload["meta"]["returned"] == 1


def test_smoke_crimes_map_points():
    # ensure the map points endpoint still produces GeoJSON
    response = _run_smoke(
        {
            "/* crimes_map_points */": {
                "rows": [
                    {
                        "id": 2,
                        "crime_id": "smoke-2",
                        "month_label": "2025-01",
                        "crime_type": "vehicle crime",
                        "last_outcome_category": "unknown",
                        "location_text": "Smoke Road",
                        "reported_by": "Police",
                        "falls_within": "Test",
                        "lsoa_code": "LSOA-124",
                        "lsoa_name": "Test Town",
                        "geometry": '{"type":"Point","coordinates":[-1.5,53.8]}'
                    }
                ]
            }
        },
        "GET",
        "/crimes/map?minLon=-1.6&minLat=53.7&maxLon=-1.4&maxLat=53.9&zoom=15&mode=points&month=2025-01",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"


def test_smoke_crime_analytics_summary():
    # verify the analytics summary composes basic totals
    response = _run_smoke(
        {
            "/* crimes_analytics_snapshot */": {
                "rows": [
                    {
                        "id": 3,
                        "month_date": date(2025, 1, 1),
                        "crime_type": "robbery",
                        "raw_outcome": None,
                        "outcome": "unknown",
                        "lsoa_code": "LSOA-125",
                        "lsoa_name": "Test Town",
                    }
                ]
            }
        },
        "GET",
        "/crimes/analytics/summary?from=2025-01&to=2025-01",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_crimes"] == 1
    assert payload["top_crime_type"]["crime_type"] == "robbery"
