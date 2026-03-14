from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _unique_email(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}@example.com"


def _register(email: str, password: str):
    return client.post(
        "/auth/register",
        json={
            "email": email,
            "password": password,
        },
    )


def _login(email: str, password: str):
    return client.post(
        "/auth/login",
        json={
            "email": email,
            "password": password,
        },
    )


def test_register_user_successfully():
    email = _unique_email("auth-register")
    password = "Register-123"
    response = _register(email, password)

    assert response.status_code == 200
    payload = response.json()
    assert "user" in payload
    assert isinstance(payload["user"]["id"], int)
    assert payload["user"]["email"] == email
    assert payload["user"]["is_admin"] is False


def test_login_user_successfully():
    email = _unique_email("auth-login")
    password = "Login-12345"
    register_response = _register(email, password)
    assert register_response.status_code == 200

    response = _login(email, password)

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_type"] == "bearer"
    assert isinstance(payload["access_token"], str)
    assert payload["access_token"]
    assert payload["user"]["email"] == email


def test_get_current_user_me_successfully():
    email = _unique_email("auth-me")
    password = "MePass-1234"
    assert _register(email, password).status_code == 200
    login_response = _login(email, password)
    token = login_response.json()["access_token"]

    response = client.get("/me", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["email"] == email
    assert isinstance(payload["user"]["id"], int)


def test_authenticated_user_can_update_email_or_password():
    email = _unique_email("auth-update")
    password = "Original-123"
    new_email = _unique_email("auth-updated")
    new_password = "Updated-1234"

    assert _register(email, password).status_code == 200
    login_response = _login(email, password)
    token = login_response.json()["access_token"]

    update_response = client.patch(
        "/me",
        json={
            "email": new_email,
            "password": new_password,
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert update_response.status_code == 200
    updated_payload = update_response.json()
    assert updated_payload["user"]["email"] == new_email

    old_login_response = _login(email, password)
    assert old_login_response.status_code == 401

    new_login_response = _login(new_email, new_password)
    assert new_login_response.status_code == 200
    assert new_login_response.json()["user"]["email"] == new_email


def test_protected_route_rejects_unauthenticated_request():
    response = client.get("/me")

    assert response.status_code == 401
    payload = response.json()
    assert payload["error"] == "NOT_AUTHENTICATED"
