from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_crimes_db():
    handlers = {
        "ORDER BY ce.month DESC, ce.id DESC": {
            "rows": [
                {
                    "id": 11,
                    "crime_id": "crime-11",
                    "month_label": "2024-01",
                    "crime_type": "burglary",
                    "last_outcome_category": "Under investigation",
                    "location_text": "Test Street",
                    "reported_by": "West Yorkshire Police",
                    "falls_within": "Leeds",
                    "lsoa_code": "E0001",
                    "lsoa_name": "Leeds 001",
                    "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                }
            ]
        }
    }
    yield InMemoryDB(handlers)


def test_crimes_alias_route_returns_same_points_envelope_as_map():
    app.dependency_overrides[get_db] = _override_crimes_db
    try:
        response = client.get(
            "/crimes",
            params={"minLon": -1.56, "minLat": 53.79, "maxLon": -1.54, "maxLat": 53.81, "zoom": 13},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["features"][0]["properties"]["crime_id"] == "crime-11"
