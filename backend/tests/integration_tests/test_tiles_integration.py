from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url):
    return client.request(method, url)


def test_smoke_tiles_mvt_endpoint():
    response = _run_smoke("GET", "/tiles/roads/10/500/340.mvt")

    assert response.status_code == 200
    assert response.content is not None
    assert response.headers["content-type"].startswith("application/vnd.mapbox-vector-tile")


def test_smoke_tiles_pbf_endpoint():
    response = _run_smoke("GET", "/tiles/roads/10/500/340.pbf?includeRisk=true&month=2025-01")

    assert response.status_code == 200
    assert response.content is not None
    assert response.headers["content-type"].startswith("application/x-protobuf")
