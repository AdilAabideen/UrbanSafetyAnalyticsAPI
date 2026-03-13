from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url):
    return client.request(method, url)


def test_smoke_collision_incidents_endpoint():
    response = _run_smoke("GET", "/collisions/incidents?from=2025-01&to=2025-03&limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
    assert "meta" in payload


def test_smoke_collisions_map_points_endpoint():
    response = _run_smoke(
        "GET",
        "/collisions/map?minLon=-2.25&minLat=53.55&maxLon=-1.1&maxLat=54.05&zoom=15&mode=points&month=2025-01&limit=1",
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["meta"]["mode"] == "points"


def test_smoke_collisions_analytics_summary_endpoint():
    response = _run_smoke("GET", "/collisions/analytics/summary?from=2025-01&to=2025-03")

    assert response.status_code == 200
    payload = response.json()
    assert "total_collisions" in payload
    assert "avg_casualties_per_collision" in payload


def test_smoke_collisions_analytics_timeseries_endpoint():
    response = _run_smoke("GET", "/collisions/analytics/timeseries?from=2025-01&to=2025-03")

    assert response.status_code == 200
    payload = response.json()
    assert "series" in payload
    assert "total" in payload
