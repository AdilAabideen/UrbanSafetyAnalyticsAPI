from fastapi.testclient import TestClient

from app.api_utils import tiles_utils
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


def test_tiles_mvt_without_risk_returns_tile_payload():
    response = _run_with_db(
        {"/* tiles_roads_only */": {"scalar": b"roads-tile"}},
        "GET",
        "/tiles/roads/10/500/340.mvt",
    )

    assert response.status_code == 200
    assert response.content == b"roads-tile"
    assert response.headers["cache-control"] == tiles_utils.TILE_CACHE_CONTROL
    assert response.headers["content-type"].startswith(tiles_utils.MVT_MEDIA_TYPE)


def test_tiles_pbf_with_risk_returns_tile_payload():
    def _risk_payload(params):
        assert params["month_date"].strftime("%Y-%m") == "2025-01"
        assert params["crime_type"] == "burglary"
        return {"scalar": memoryview(b"risk-tile")}

    response = _run_with_db(
        {"/* tiles_roads_with_risk */": _risk_payload},
        "GET",
        "/tiles/roads/10/500/340.pbf?includeRisk=true&month=2025-01&crimeType=burglary",
    )

    assert response.status_code == 200
    assert response.content == b"risk-tile"
    assert response.headers["content-type"].startswith(tiles_utils.PBF_MEDIA_TYPE)


def test_tiles_endpoint_rejects_invalid_tile_coordinates():
    response = _run_with_db({}, "GET", "/tiles/roads/2/6/0.mvt")

    assert response.status_code == 400
    assert "out of range" in response.json()["detail"]


def test_tiles_endpoint_requires_month_or_range_for_risk():
    response = _run_with_db({}, "GET", "/tiles/roads/10/500/340.mvt?includeRisk=true")

    assert response.status_code == 400
    assert "required when includeRisk=true" in response.json()["detail"]
