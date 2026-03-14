from datetime import datetime
from uuid import uuid4

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from app.services.auth_service import create_access_token, hash_password
from tests.inmemory_db import InMemoryDB


app.router.on_startup.clear()
client = TestClient(app)


def _run_with_db(handlers, method, url, json=None, headers=None):
    """Run one HTTP request with a per-test InMemoryDB handler map."""
    def _override_get_db():
        yield InMemoryDB(handlers)

    app.dependency_overrides[get_db] = _override_get_db
    try:
        return client.request(method, url, json=json, headers=headers)
    finally:
        app.dependency_overrides.clear()


def _unique_email(prefix: str) -> str:
    """Generate a unique email for test payload isolation/readability."""
    return f"{prefix}-{uuid4().hex[:12]}@example.com"


def test_register_user_successfully():
    """Register route should create and return a user when email is available."""
    email = _unique_email("auth-register")
    response = _run_with_db(
        {
            "WHERE u.email = :email": {"rows": []},
            "INSERT INTO users (email, password_hash, is_admin)": {
                "rows": [
                    {
                        "id": 101,
                        "email": email,
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            },
        },
        "POST",
        "/auth/register",
        json={"email": email, "password": "Register-123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "user" in payload
    assert payload["user"]["id"] == 101
    assert payload["user"]["email"] == email
    assert payload["user"]["is_admin"] is False


def test_login_user_successfully():
    """Login route should return bearer token and user payload for valid credentials."""
    email = _unique_email("auth-login")
    password = "Login-12345"
    response = _run_with_db(
        {
            "u.password_hash": {
                "rows": [
                    {
                        "id": 202,
                        "email": email,
                        "password_hash": hash_password(password),
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            }
        },
        "POST",
        "/auth/login",
        json={"email": email, "password": password},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_type"] == "bearer"
    assert isinstance(payload["access_token"], str)
    assert payload["user"]["email"] == email


def test_get_current_user_me_successfully():
    """Protected /me should resolve and return current user for a valid bearer token."""
    user_id = 303
    email = _unique_email("auth-me")
    token = create_access_token(user_id)
    response = _run_with_db(
        {
            "WHERE u.id = :user_id": {
                "rows": [
                    {
                        "id": user_id,
                        "email": email,
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            }
        },
        "GET",
        "/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["id"] == user_id
    assert payload["user"]["email"] == email


def test_authenticated_user_can_update_email_or_password():
    """PATCH /me should return updated profile and support subsequent login with new credentials."""
    user_id = 404
    old_email = _unique_email("auth-update")
    new_email = _unique_email("auth-updated")
    old_password = "Original-123"
    new_password = "Updated-1234"
    token = create_access_token(user_id)

    update_response = _run_with_db(
        {
            "WHERE u.id = :user_id": {
                "rows": [
                    {
                        "id": user_id,
                        "email": old_email,
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            },
            "UPDATE users": {
                "rows": [
                    {
                        "id": user_id,
                        "email": new_email,
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            },
        },
        "PATCH",
        "/me",
        json={"email": new_email, "password": new_password},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert update_response.status_code == 200
    assert update_response.json()["user"]["email"] == new_email

    old_login_response = _run_with_db(
        {"u.password_hash": {"rows": []}},
        "POST",
        "/auth/login",
        json={"email": old_email, "password": old_password},
    )
    assert old_login_response.status_code == 401

    new_login_response = _run_with_db(
        {
            "u.password_hash": {
                "rows": [
                    {
                        "id": user_id,
                        "email": new_email,
                        "password_hash": hash_password(new_password),
                        "is_admin": False,
                        "created_at": datetime(2026, 1, 1, 12, 0, 0),
                    }
                ]
            }
        },
        "POST",
        "/auth/login",
        json={"email": new_email, "password": new_password},
    )
    assert new_login_response.status_code == 200
    assert new_login_response.json()["user"]["email"] == new_email


def test_protected_route_rejects_unauthenticated_request():
    """Protected /me should reject requests without bearer authentication."""
    response = _run_with_db({}, "GET", "/me")

    assert response.status_code == 401
    payload = response.json()
    assert payload["error"] == "NOT_AUTHENTICATED"
