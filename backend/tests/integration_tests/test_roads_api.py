from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

LEEDS_BBOX = {
    "minLon": -1.60,
    "minLat": 53.78,
    "maxLon": -1.52,
    "maxLat": 53.82,
}

LEEDS_POINT = {"lon": -1.5491, "lat": 53.8008}
LEEDS_TILE_MID = {"z": 9, "x": 253, "y": 164}
LEEDS_TILE_HIGH = {"z": 14, "x": 8121, "y": 5275}
EMPTY_TILE = {"z": 9, "x": 56, "y": 318}


def test_roads_bbox_returns_featurecollection():
    r = client.get("/roads", params={**LEEDS_BBOX, "limit": 25})
    assert r.status_code == 200

    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert isinstance(data["features"], list)
    assert len(data["features"]) > 0

    f0 = data["features"][0]
    assert f0["type"] == "Feature"
    assert "geometry" in f0 and "properties" in f0
    assert f0["geometry"]["type"] == "LineString"
    assert "id" in f0["properties"]
    assert "highway" in f0["properties"]


def test_roads_bbox_respects_limit():
    r = client.get("/roads", params={**LEEDS_BBOX, "limit": 5})
    assert r.status_code == 200
    data = r.json()
    assert len(data["features"]) <= 5


def test_roads_nearest_returns_valid_road():
    r = client.get("/roads/nearest", params=LEEDS_POINT)
    assert r.status_code == 200
    data = r.json()

    assert isinstance(data["id"], int)
    assert "highway" in data
    assert "geometry" in data
    assert data["geometry"]["type"] == "LineString"


def test_roads_by_id_matches_nearest():
    nearest = client.get("/roads/nearest", params=LEEDS_POINT)
    assert nearest.status_code == 200
    road_id = nearest.json()["id"]

    r = client.get(f"/roads/{road_id}")
    assert r.status_code == 200
    data = r.json()

    assert data["id"] == road_id
    assert data["geometry"]["type"] == "LineString"


def test_roads_stats_total_matches_sum():
    r = client.get("/roads/stats", params=LEEDS_BBOX)
    assert r.status_code == 200
    data = r.json()

    counts = data["highway_counts"]
    assert isinstance(counts, dict)
    assert data["total"] == sum(counts.values())


def test_roads_invalid_bbox_returns_400():
    bad = {
        "minLon": -1.52,
        "minLat": 53.78,
        "maxLon": -1.60,
        "maxLat": 53.82,
        "limit": 10,
    }
    r = client.get("/roads", params=bad)
    assert r.status_code == 400


def test_roads_vector_tile_mid_zoom_returns_binary_tile():
    r = client.get(f"/tiles/roads/{LEEDS_TILE_MID['z']}/{LEEDS_TILE_MID['x']}/{LEEDS_TILE_MID['y']}.mvt")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    assert r.headers["cache-control"] == "public, max-age=60"
    assert len(r.content) > 0


def test_roads_vector_tile_high_zoom_returns_binary_tile():
    r = client.get(f"/tiles/roads/{LEEDS_TILE_HIGH['z']}/{LEEDS_TILE_HIGH['x']}/{LEEDS_TILE_HIGH['y']}.mvt")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    assert len(r.content) > 0


def test_roads_vector_tile_empty_area_returns_200():
    r = client.get(f"/tiles/roads/{EMPTY_TILE['z']}/{EMPTY_TILE['x']}/{EMPTY_TILE['y']}.mvt")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.mapbox-vector-tile"


def test_roads_vector_tile_invalid_coordinates_return_400():
    r = client.get("/tiles/roads/9/512/0.mvt")
    assert r.status_code == 400
