from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


class _FakeScalarResult:
    def __init__(self, payload):
        self.payload = payload

    def scalar_one(self):
        return self.payload

    def scalar_one_or_none(self):
        return self.payload


class _FakeMappingResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class _FakeCollectionSession:
    def execute(self, query, params):
        return _FakeScalarResult(
            {
                "type": "FeatureCollection",
                "meta": {"returned": 1, "limit": params.get("limit", 5000), "truncated": False},
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                        "properties": {"id": 1, "crime_type": "burglary"},
                    }
                ],
            }
        )


class _FakeDetailSession:
    def execute(self, query, params):
        return _FakeScalarResult(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                "properties": {"id": params["crime_id"], "crime_type": "burglary"},
            }
        )


class _FakeStatsSession:
    def execute(self, query, params):
        return _FakeMappingResult(
            [
                {"crime_type": "burglary", "count": 2},
                {"crime_type": "vehicle-crime", "count": 1},
            ]
        )


client = TestClient(app)


def _override_collection_db():
    yield _FakeCollectionSession()


def _override_detail_db():
    yield _FakeDetailSession()


def _override_stats_db():
    yield _FakeStatsSession()


def test_crimes_collection_route_returns_geojson():
    app.dependency_overrides[get_db] = _override_collection_db
    try:
        response = client.get("/crimes", params={"limit": 10})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["limit"] == 10
    assert data["features"][0]["geometry"]["type"] == "Point"


def test_crime_detail_route_returns_geojson():
    app.dependency_overrides[get_db] = _override_detail_db
    try:
        response = client.get("/crimes/1")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "Feature"
    assert data["properties"]["id"] == 1


def test_crime_stats_route_returns_grouped_counts():
    app.dependency_overrides[get_db] = _override_stats_db
    try:
        response = client.get("/crimes/stats", params={"month": "2024-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["filters"]["month"] == "2024-01"
    assert data["total"] == 3
    assert data["crime_type_counts"]["burglary"] == 2


def test_crimes_invalid_month_returns_400_without_db_access():
    response = client.get("/crimes", params={"month": "2024-01-01"})

    assert response.status_code == 400
    assert response.json()["detail"] == "month must be in YYYY-MM format"


def test_crimes_partial_bbox_returns_400_without_db_access():
    response = client.get("/crimes", params={"minLon": -1.5, "minLat": 53.8})

    assert response.status_code == 400
    assert response.json()["detail"] == "minLon, minLat, maxLon, and maxLat must all be provided together"
