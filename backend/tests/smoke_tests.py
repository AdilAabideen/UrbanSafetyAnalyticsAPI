from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.schemas.tiles_schemas import MVT_MEDIA_TYPE


client = TestClient(app)


def test_smoke_auth_register_endpoint():
    """Smoke-test auth registration endpoint wiring with a minimal valid payload."""
    email = f"smoke-auth-{uuid4().hex[:12]}@example.com"
    response = client.post(
        "/auth/register",
        json={
            "email": email,
            "password": "smoke-pass-123",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "user" in payload
    assert payload["user"]["email"] == email


def test_smoke_tiles_route_basic_reachable():
    """
    Smoke Test 1 (tiles): prove the tiles route is alive and returns byte content.

    Why this exists:
    - Fast signal that routing + dependency wiring + DB path are reachable.
    - Mirrors production usage by calling the real FastAPI app (no in-memory DB).
    """
    response = client.get("/tiles/roads/9/252/165.mvt")

    # Basic route health checks for MVT output.
    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith(MVT_MEDIA_TYPE)
    assert isinstance(response.content, (bytes, bytearray))
