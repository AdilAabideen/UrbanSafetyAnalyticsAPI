from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.auth_utils import get_current_user
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

    def all(self):
        return list(self.rows)


class _WatchlistMemoryDB:
    def __init__(self):
        self.watchlists = {}
        self.preferences = {}
        self.next_watchlist_id = 1
        self.next_preference_id = 1

    def execute(self, query, params):
        sql = str(query)

        if "INSERT INTO watchlists" in sql:
            watchlist = {
                "id": self.next_watchlist_id,
                "user_id": params["user_id"],
                "name": params["name"],
                "min_lon": params["min_lon"],
                "min_lat": params["min_lat"],
                "max_lon": params["max_lon"],
                "max_lat": params["max_lat"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.watchlists[watchlist["id"]] = watchlist
            self.next_watchlist_id += 1
            return _RowsResult([watchlist])

        if "INSERT INTO watchlist_preferences" in sql:
            watchlist = self.watchlists.get(params["watchlist_id"])
            if not watchlist or watchlist["user_id"] != params["user_id"]:
                return _RowsResult([])

            preference = {
                "id": self.next_preference_id,
                "watchlist_id": watchlist["id"],
                "window_months": params["window_months"],
                "crime_type": params["crime_type"],
                "banding_mode": params["banding_mode"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.preferences[preference["id"]] = preference
            self.next_preference_id += 1
            return _RowsResult([preference])

        if "UPDATE watchlist_preferences wp" in sql:
            preference = self.preferences.get(params["preference_id"])
            watchlist = self.watchlists.get(params["watchlist_id"])
            if (
                not preference
                or not watchlist
                or preference["watchlist_id"] != watchlist["id"]
                or watchlist["user_id"] != params["user_id"]
            ):
                return _RowsResult([])

            for field in ("window_months", "crime_type", "banding_mode"):
                if field in params:
                    preference[field] = params[field]
            return _RowsResult([preference])

        if "FROM watchlists w" in sql and "WHERE w.user_id = :user_id" in sql and "LIMIT 1" not in sql:
            rows = [
                watchlist
                for watchlist in self.watchlists.values()
                if watchlist["user_id"] == params["user_id"]
            ]
            rows.sort(key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return _RowsResult(rows)

        if "FROM watchlists w" in sql and "WHERE w.id = :watchlist_id AND w.user_id = :user_id" in sql:
            watchlist = self.watchlists.get(params["watchlist_id"])
            if not watchlist or watchlist["user_id"] != params["user_id"]:
                return _RowsResult([])
            return _RowsResult([watchlist])

        if "UPDATE watchlists" in sql:
            watchlist = self.watchlists.get(params["watchlist_id"])
            if not watchlist or watchlist["user_id"] != params["user_id"]:
                return _RowsResult([])

            for field in ("name", "min_lon", "min_lat", "max_lon", "max_lat"):
                if field in params:
                    watchlist[field] = params[field]
            return _RowsResult([watchlist])

        if "DELETE FROM watchlists" in sql:
            watchlist = self.watchlists.get(params["watchlist_id"])
            if not watchlist or watchlist["user_id"] != params["user_id"]:
                return _RowsResult([])

            del self.watchlists[watchlist["id"]]
            self.preferences = {
                preference_id: preference
                for preference_id, preference in self.preferences.items()
                if preference["watchlist_id"] != watchlist["id"]
            }
            return _RowsResult([{"id": watchlist["id"]}])

        if "FROM watchlist_preferences wp" in sql and "ORDER BY wp.created_at ASC, wp.id ASC" in sql:
            rows = [
                preference
                for preference in self.preferences.values()
                if preference["watchlist_id"] == params["watchlist_id"]
            ]
            rows.sort(key=lambda row: (row["created_at"], row["id"]))
            return _RowsResult(rows)

        raise AssertionError(f"Unexpected watchlist query: {sql}")

    def commit(self):
        return None

    def rollback(self):
        return None


def test_watchlist_crud_and_preference_crud_flow():
    fake_db = _WatchlistMemoryDB()

    def _override_db():
        yield fake_db

    def _override_user():
        return {
            "id": 7,
            "email": "watcher@example.com",
            "is_admin": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user
    try:
        create_watchlist = client.post(
            "/watchlists",
            json={
                "name": "Leeds Centre",
                "min_lon": -1.7,
                "min_lat": 53.75,
                "max_lon": -1.45,
                "max_lat": 53.88,
                "preferences": [
                    {
                        "window_months": 6,
                        "crime_type": "burglary",
                        "banding_mode": "quantile",
                    }
                ],
            },
        )
        assert create_watchlist.status_code == 201
        watchlist = create_watchlist.json()["watchlist"]
        assert watchlist["id"] == 1
        assert len(watchlist["preferences"]) == 1
        preference = watchlist["preferences"][0]
        assert preference["id"] == 1
        assert preference["watchlist_id"] == watchlist["id"]

        list_watchlists = client.get("/watchlists")
        assert list_watchlists.status_code == 200
        assert len(list_watchlists.json()["items"]) == 1
        assert list_watchlists.json()["items"][0]["id"] == watchlist["id"]

        get_watchlist = client.get("/watchlists", params={"watchlist_id": watchlist["id"]})
        assert get_watchlist.status_code == 200
        assert len(get_watchlist.json()["watchlist"]["preferences"]) == 1
        assert get_watchlist.json()["watchlist"]["preferences"][0]["banding_mode"] == "quantile"

        update_watchlist = client.patch(
            f"/watchlists/{watchlist['id']}",
            json={
                "name": "Leeds Inner Ring",
                "min_lon": -1.68,
                "min_lat": 53.76,
                "max_lon": -1.43,
                "max_lat": 53.9,
                "preferences": [
                    {
                        "id": preference["id"],
                        "window_months": 12,
                        "crime_type": "vehicle crime",
                        "banding_mode": "equal_interval",
                    }
                ],
            },
        )
        assert update_watchlist.status_code == 200
        updated_watchlist = update_watchlist.json()["watchlist"]
        assert updated_watchlist["name"] == "Leeds Inner Ring"
        assert updated_watchlist["min_lon"] == -1.68
        assert len(updated_watchlist["preferences"]) == 1
        updated_preference = updated_watchlist["preferences"][0]
        assert updated_preference["window_months"] == 12
        assert updated_preference["crime_type"] == "vehicle crime"
        assert updated_preference["banding_mode"] == "equal_interval"

        delete_watchlist = client.delete(f"/watchlists/{watchlist['id']}")
        assert delete_watchlist.status_code == 200
        assert delete_watchlist.json() == {"deleted": True, "watchlist_id": watchlist["id"]}
        assert fake_db.watchlists == {}
        assert fake_db.preferences == {}
    finally:
        app.dependency_overrides.clear()
