from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url, json=None):
    return client.request(method, url, json=json)


def test_smoke_analytics_meta_endpoint():
    response = _run_smoke("GET", "/analytics/meta")

    assert response.status_code == 200
    payload = response.json()
    assert "months" in payload
    assert "counts" in payload


def test_smoke_analytics_risk_score_endpoint():
    response = _run_smoke(
        "POST",
        "/analytics/risk/score",
        json={
            "from": "2025-01",
            "to": "2025-03",
            "minLon": -2.25,
            "minLat": 53.55,
            "maxLon": -1.1,
            "maxLat": 54.05,
            "mode": "walk",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "score" in payload
    assert "metrics" in payload


def test_smoke_analytics_hotspot_stability_endpoint():
    response = _run_smoke(
        "GET",
        "/analytics/patterns/hotspot-stability?from=2025-01&to=2025-03&k=10",
    )

    assert response.status_code == 200
    payload = response.json()
    assert "stability_series" in payload
    assert "persistent_hotspots" in payload
