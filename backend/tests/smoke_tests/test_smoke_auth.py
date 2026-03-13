from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


client = TestClient(app)


class _RowsResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None


class _AuthMemoryDB:
    def __init__(self):
        self.users = {}
        self.next_id = 1

    def execute(self, query, params):
        sql = str(query)

        if "FROM users u" in sql and "WHERE u.email = :email" in sql:
            user = self.users.get(params["email"])
            return _RowsResult([user] if user else [])

        if "INSERT INTO users" in sql:
            user = {
                "id": self.next_id,
                "email": params["email"],
                "password_hash": params["password_hash"],
                "is_admin": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.users[user["email"]] = user
            self.next_id += 1
            return _RowsResult([user])

        if "WHERE u.id = :user_id" in sql:
            for user in self.users.values():
                if user["id"] == params["user_id"]:
                    return _RowsResult(
                        [
                            {
                                "id": user["id"],
                                "email": user["email"],
                                "is_admin": user["is_admin"],
                                "created_at": user["created_at"],
                            }
                        ]
                    )
            return _RowsResult([])

        if "UPDATE users" in sql and "WHERE id = :user_id" in sql:
            target_user = None
            for user in self.users.values():
                if user["id"] == params["user_id"]:
                    target_user = user
                    break

            if target_user is None:
                return _RowsResult([])

            if "email" in params:
                self.users.pop(target_user["email"], None)
                target_user["email"] = params["email"]
                self.users[target_user["email"]] = target_user

            if "password_hash" in params:
                target_user["password_hash"] = params["password_hash"]

            return _RowsResult(
                [
                    {
                        "id": target_user["id"],
                        "email": target_user["email"],
                        "is_admin": target_user["is_admin"],
                        "created_at": target_user["created_at"],
                    }
                ]
            )

        raise AssertionError(f"Unexpected auth query: {sql}")

    def commit(self):
        return None

    def rollback(self):
        return None


def test_register_login_and_me_flow_requires_authentication():
    fake_db = _AuthMemoryDB()

    def _override_auth_db():
        yield fake_db

    app.dependency_overrides[get_db] = _override_auth_db
    try:
        register_response = client.post(
            "/auth/register",
            json={"email": "user@example.com", "password": "supersecure"},
        )
        assert register_response.status_code == 200
        assert register_response.json()["user"]["email"] == "user@example.com"
        assert register_response.json()["user"]["is_admin"] is False

        login_response = client.post(
            "/auth/login",
            json={"email": "user@example.com", "password": "supersecure"},
        )
        assert login_response.status_code == 200
        payload = login_response.json()
        assert payload["token_type"] == "bearer"
        assert payload["user"]["email"] == "user@example.com"

        unauthenticated = client.get("/me")
        assert unauthenticated.status_code == 401

        authenticated = client.get(
            "/me",
            headers={"Authorization": f"Bearer {payload['access_token']}"},
        )
        assert authenticated.status_code == 200
        assert authenticated.json()["user"]["email"] == "user@example.com"

        profile_update = client.patch(
            "/me",
            json={"email": "updated@example.com", "password": "newpassword"},
            headers={"Authorization": f"Bearer {payload['access_token']}"},
        )
        assert profile_update.status_code == 200
        assert profile_update.json()["user"]["email"] == "updated@example.com"

        old_login = client.post(
            "/auth/login",
            json={"email": "user@example.com", "password": "supersecure"},
        )
        assert old_login.status_code == 401

        new_login = client.post(
            "/auth/login",
            json={"email": "updated@example.com", "password": "newpassword"},
        )
        assert new_login.status_code == 200
        assert new_login.json()["user"]["email"] == "updated@example.com"
    finally:
        app.dependency_overrides.clear()
