from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


class _FakeResult:
    def __init__(self, payload):
        self.payload = payload

    def scalar_one(self):
        return self.payload


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload

    def execute(self, query, params):
        return _FakeResult(self.payload)


def _override_get_db():
    yield _FakeSession(b"\x1a\x2b\x3c")


client = TestClient(app)


def test_vector_tile_route_returns_binary_response():
    app.dependency_overrides[get_db] = _override_get_db
    try:
        response = client.get("/tiles/roads/9/253/164.mvt")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    assert response.headers["cache-control"] == "public, max-age=60"
    assert response.content == b"\x1a\x2b\x3c"


def test_vector_tile_route_rejects_out_of_range_coordinates():
    response = client.get("/tiles/roads/9/512/0.mvt")

    assert response.status_code == 400
    assert response.json()["detail"] == "Tile coordinates out of range for zoom level"
