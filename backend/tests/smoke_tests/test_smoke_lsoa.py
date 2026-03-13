from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_lsoa_db():
    handlers = {
        "FROM crime_events ce": {
            "rows": [
                {
                    "lsoa_code": "E01011354",
                    "lsoa_name": "Leeds 111B",
                    "count": 23439,
                    "min_lon": -1.582,
                    "min_lat": 53.781,
                    "max_lon": -1.541,
                    "max_lat": 53.812,
                },
                {
                    "lsoa_code": "E0101065C",
                    "lsoa_name": "Bradford 065C",
                    "count": 9361,
                    "min_lon": -1.812,
                    "min_lat": 53.744,
                    "max_lon": -1.775,
                    "max_lat": 53.769,
                },
                {
                    "lsoa_code": "unknown",
                    "lsoa_name": "unknown",
                    "count": 10375,
                    "min_lon": None,
                    "min_lat": None,
                    "max_lon": None,
                    "max_lat": None,
                },
            ]
        }
    }
    yield InMemoryDB(handlers)


def test_lsoa_categories_returns_ranked_lsoa_names():
    app.dependency_overrides[get_db] = _override_lsoa_db
    try:
        response = client.get("/lsoa/categories")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert data["items"][0]["lsoa_name"] == "Leeds 111B"
    assert data["items"][0]["lsoa_code"] == "E01011354"
    assert data["items"][0]["count"] == 23439
    assert data["items"][0]["bbox"]["minLon"] == -1.582
    assert data["items"][0]["bbox"]["maxLat"] == 53.812
