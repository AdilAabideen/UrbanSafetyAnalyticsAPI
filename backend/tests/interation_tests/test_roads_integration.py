from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url):
    # Smoke tests intentionally use the real DB dependency.
    return client.request(method, url)


def test_smoke_roads_meta():
    response = _run_smoke("GET", "/roads/analytics/meta")

    assert response.status_code == 200
    payload = response.json()
    assert "months" in payload
    assert "counts" in payload
    assert "highways" in payload


def test_smoke_roads_overview():
    response = _run_smoke("GET", "/roads/analytics/overview?from=2025-01&to=2025-03")

    assert response.status_code == 200
    payload = response.json()
    assert "total_incidents" in payload
    assert "band_breakdown" in payload
    assert "insights" in payload


def test_smoke_roads_risk():
    response = _run_smoke("GET", "/roads/analytics/risk?from=2025-01&to=2025-03")

    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "meta" in payload
