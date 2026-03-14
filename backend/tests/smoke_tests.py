from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_smoke_auth_register_endpoint():
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
