from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _run_smoke(method, url):
    return client.request(method, url)


def test_smoke_lsoa_categories():
    response = _run_smoke("GET", "/lsoa/categories")

    assert response.status_code == 200
    payload = response.json()
    assert "items" in payload
