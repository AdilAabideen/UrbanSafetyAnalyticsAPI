from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url, *, json=None):
    return client.request(method, url, json=json)


def test_smoke_user_events_endpoint_real_db():
    response = _run_smoke("GET", "/user-events?from=2025-01&to=2025-03&limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "FeatureCollection"
    assert "meta" in payload


def test_smoke_create_reported_event_endpoint_real_db():
    unique_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    response = _run_smoke(
        "POST",
        "/reported-events",
        json={
            "event_kind": "crime",
            "event_date": "2025-02-15",
            "longitude": -1.55,
            "latitude": 53.8,
            "description": f"smoke-test-{unique_suffix}",
            "crime": {"crime_type": "other"},
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert "report" in payload
    assert payload["report"]["event_kind"] == "crime"
