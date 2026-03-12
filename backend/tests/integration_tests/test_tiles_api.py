import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db import engine
from app.main import app


client = TestClient(app)

# 10/507/331 covers central Leeds. For other tiles, use any XYZ tile calculator
# with a Leeds lat/lon and the target zoom level.
LEEDS_TILE = {"z": 10, "x": 507, "y": 331}


@pytest.fixture(scope="module")
def leeds_tile_has_roads():
    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM road_segments rs
            WHERE rs.geom && ST_TileEnvelope(:z, :x, :y)
        ) AS has_roads
        """
    )

    try:
        with engine.connect() as conn:
            has_roads = conn.execute(query, LEEDS_TILE).scalar()
    except SQLAlchemyError as exc:
        pytest.skip(f"Tile integration tests need a healthy reachable database: {exc}")

    if not has_roads:
        pytest.skip("Selected Leeds tile has no road data in road_segments")

    return True


def test_roads_pbf_tile_returns_binary_payload(leeds_tile_has_roads):
    response = client.get(
        f"/tiles/roads/{LEEDS_TILE['z']}/{LEEDS_TILE['x']}/{LEEDS_TILE['y']}.pbf"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
    assert response.headers["cache-control"] == "public, max-age=60"
    assert len(response.content) > 0


def test_roads_mvt_tile_returns_binary_payload(leeds_tile_has_roads):
    response = client.get(
        f"/tiles/roads/{LEEDS_TILE['z']}/{LEEDS_TILE['x']}/{LEEDS_TILE['y']}.mvt"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    assert response.headers["cache-control"] == "public, max-age=60"
    assert len(response.content) > 0


def test_roads_pbf_tile_with_risk_returns_binary_payload(leeds_tile_has_roads):
    response = client.get(
        f"/tiles/roads/{LEEDS_TILE['z']}/{LEEDS_TILE['x']}/{LEEDS_TILE['y']}.pbf",
        params={"includeRisk": "true", "month": "2023-03"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
    assert len(response.content) > 0


def test_roads_mvt_tile_with_risk_returns_vector_tile_payload(leeds_tile_has_roads):
    response = client.get(
        f"/tiles/roads/{LEEDS_TILE['z']}/{LEEDS_TILE['x']}/{LEEDS_TILE['y']}.mvt",
        params={"includeRisk": "true", "month": "2023-03"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    assert len(response.content) > 0
