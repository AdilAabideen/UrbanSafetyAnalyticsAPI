from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_get_db():
    handlers = {
        "COUNT(*) FILTER (WHERE ce.geom IS NOT NULL)": {
            "rows": [
                {
                    "min_month": "2023-02",
                    "max_month": "2024-09",
                    "crime_events_total": 937822,
                    "crime_events_with_geom": 927447,
                    "crime_events_snapped": 927447,
                    "road_segments_total": 212450,
                }
            ]
        },
        "SELECT DISTINCT ce.crime_type": {
            "rows": [
                {"crime_type": "Public order"},
                {"crime_type": "Shoplifting"},
                {"crime_type": "Violence and sexual offences"},
            ]
        },
    }
    yield InMemoryDB(handlers)


def test_analytics_meta_returns_discoverability_payload():
    app.dependency_overrides[get_db] = _override_get_db
    try:
        response = client.get("/analytics/meta")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "months": {
            "min": "2023-02",
            "max": "2024-09",
        },
        "crime_types": [
            "Public order",
            "Shoplifting",
            "Violence and sexual offences",
        ],
        "counts": {
            "crime_events_total": 937822,
            "crime_events_with_geom": 927447,
            "crime_events_snapped": 927447,
            "road_segments_total": 212450,
        },
    }
