from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_roads_db():
    handlers = {
        "FROM (\n            SELECT json_build_object(": {
            "scalar": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "LineString", "coordinates": [[-1.55, 53.8], [-1.54, 53.81]]},
                        "properties": {"id": 7, "osm_id": 77, "name": "Test Road", "highway": "primary"},
                    }
                ],
            }
        },
        "ORDER BY rs.geom <->": {
            "rows": [
                {
                    "id": 7,
                    "osm_id": 77,
                    "name": "Test Road",
                    "highway": "primary",
                    "length_m": 120.5,
                    "geometry": {"type": "LineString", "coordinates": [[-1.55, 53.8], [-1.54, 53.81]]},
                }
            ]
        },
        "GROUP BY COALESCE(rs.highway, 'unknown')": {
            "rows": [
                {"highway": "primary", "count": 2},
                {"highway": "residential", "count": 1},
            ]
        },
        "WHERE rs.id = :road_id": lambda params: {
            "rows": (
                [
                    {
                        "id": params["road_id"],
                        "osm_id": 77,
                        "name": "Test Road",
                        "highway": "primary",
                        "length_m": 120.5,
                        "geometry": {"type": "LineString", "coordinates": [[-1.55, 53.8], [-1.54, 53.81]]},
                    }
                ]
                if params["road_id"] == 7
                else []
            )
        },
    }
    yield InMemoryDB(handlers)


def test_roads_route_returns_geojson_featurecollection():
    app.dependency_overrides[get_db] = _override_roads_db
    try:
        response = client.get(
            "/roads",
            params={"minLon": -1.6, "minLat": 53.78, "maxLon": -1.5, "maxLat": 53.82, "limit": 5},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["features"][0]["geometry"]["type"] == "LineString"


def test_roads_nearest_route_returns_single_road():
    app.dependency_overrides[get_db] = _override_roads_db
    try:
        response = client.get("/roads/nearest", params={"lon": -1.55, "lat": 53.8})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 7
    assert data["geometry"]["type"] == "LineString"


def test_roads_stats_route_returns_counts():
    app.dependency_overrides[get_db] = _override_roads_db
    try:
        response = client.get("/roads/stats", params={"minLon": -1.6, "minLat": 53.78, "maxLon": -1.5, "maxLat": 53.82})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["highway_counts"]["primary"] == 2


def test_roads_by_id_route_returns_404_for_missing_row():
    app.dependency_overrides[get_db] = _override_roads_db
    try:
        response = client.get("/roads/999")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == "Road segment not found"
