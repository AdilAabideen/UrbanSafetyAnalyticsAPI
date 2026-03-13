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


def test_lsoa_categories_returns_items():
    response = _run_with_db(
        {
            "GROUP BY COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown')": {
                "rows": [
                    {
                        "lsoa_code": "E01000001",
                        "lsoa_name": "Central Area",
                        "count": 12,
                        "min_lon": -1.6,
                        "min_lat": 53.7,
                        "max_lon": -1.5,
                        "max_lat": 53.8,
                    },
                    {
                        "lsoa_code": "E01000002",
                        "lsoa_name": "North Area",
                        "count": 5,
                        "min_lon": -1.55,
                        "min_lat": 53.75,
                        "max_lon": -1.45,
                        "max_lat": 53.85,
                    },
                ]
            }
        },
        "GET",
        "/lsoa/categories",
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    assert payload["items"][0]["lsoa_code"] == "E01000001"
    assert payload["items"][0]["minLon"] == -1.6
    assert payload["items"][1]["lsoa_name"] == "North Area"


def test_lsoa_categories_returns_empty_items_when_no_rows():
    response = _run_with_db(
        {
            "GROUP BY COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown')": {
                "rows": []
            }
        },
        "GET",
        "/lsoa/categories",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == []
