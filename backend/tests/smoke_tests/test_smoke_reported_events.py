from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.api.report_event_utils import get_optional_current_user, require_admin
from app.auth_utils import get_current_user
from app.db import get_db
from app.main import app


class _RowsResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return list(self.rows)


class _FakeReportedEventsDB:
    def __init__(self):
        self.next_id = 1
        self.created_counter = 0
        self.reports = {}
        self.crime_details = {}
        self.collision_details = {}
        self.users = {
            1: {"id": 1, "email": "user@example.com", "is_admin": False},
            99: {"id": 99, "email": "admin@example.com", "is_admin": True},
        }

    def commit(self):
        return None

    def rollback(self):
        return None

    def _next_created_at(self):
        self.created_counter += 1
        return datetime(2026, 3, 13, 12, 0, self.created_counter, tzinfo=timezone.utc)

    def _row_for(self, report):
        user = self.users.get(report["user_id"]) if report["user_id"] is not None else None
        row = {
            "id": report["id"],
            "event_kind": report["event_kind"],
            "reporter_type": report["reporter_type"],
            "user_id": report["user_id"],
            "reporter_email": None if not user else user["email"],
            "event_date": report["event_date"],
            "event_time": report["event_time"],
            "month": report["month"],
            "longitude": report["longitude"],
            "latitude": report["latitude"],
            "segment_id": report["segment_id"],
            "snap_distance_m": report["snap_distance_m"],
            "description": report["description"],
            "admin_approved": report["admin_approved"],
            "moderation_status": report["moderation_status"],
            "moderation_notes": report["moderation_notes"],
            "moderated_by": report["moderated_by"],
            "moderated_at": report["moderated_at"],
            "created_at": report["created_at"],
            "crime_type": None,
            "weather_condition": None,
            "light_condition": None,
            "number_of_vehicles": None,
        }
        if report["event_kind"] == "crime":
            row["crime_type"] = self.crime_details[report["id"]]["crime_type"]
        else:
            detail = self.collision_details[report["id"]]
            row["weather_condition"] = detail["weather_condition"]
            row["light_condition"] = detail["light_condition"]
            row["number_of_vehicles"] = detail["number_of_vehicles"]
        return row

    def _list_rows(self, params, own_only):
        rows = list(self.reports.values())
        if own_only:
            rows = [row for row in rows if row["user_id"] == params["user_id"]]
        if "status" in params:
            rows = [row for row in rows if row["moderation_status"] == params["status"]]
        if "event_kind" in params:
            rows = [row for row in rows if row["event_kind"] == params["event_kind"]]
        if "reporter_type" in params:
            rows = [row for row in rows if row["reporter_type"] == params["reporter_type"]]
        if "from_month_date" in params and "to_month_date" in params:
            rows = [row for row in rows if params["from_month_date"] <= row["month"] <= params["to_month_date"]]
        if "cursor_created_at" in params and "cursor_id" in params:
            rows = [
                row
                for row in rows
                if row["created_at"] < params["cursor_created_at"]
                or (row["created_at"] == params["cursor_created_at"] and row["id"] < params["cursor_id"])
            ]
        rows.sort(key=lambda row: (row["created_at"], row["id"]), reverse=True)
        limit = params["row_limit"]
        return [_RowsResult([self._row_for(row) for row in rows[:limit]])]

    def execute(self, query, params):
        sql = str(query)

        if "FROM road_segments_4326 rs" in sql:
            return _RowsResult([{"id": 42, "snap_distance_m": 18.5}])

        if "INSERT INTO user_reported_events" in sql:
            report_id = self.next_id
            self.next_id += 1
            self.reports[report_id] = {
                "id": report_id,
                "event_kind": params["event_kind"],
                "reporter_type": params["reporter_type"],
                "user_id": params["user_id"],
                "event_date": params["event_date"],
                "event_time": params["event_time"],
                "month": params["month"],
                "longitude": params["longitude"],
                "latitude": params["latitude"],
                "segment_id": params["segment_id"],
                "snap_distance_m": params["snap_distance_m"],
                "description": params["description"],
                "admin_approved": False,
                "moderation_status": "pending",
                "moderation_notes": None,
                "moderated_by": None,
                "moderated_at": None,
                "created_at": self._next_created_at(),
            }
            return _RowsResult([{"id": report_id}])

        if "INSERT INTO user_reported_crime_details" in sql:
            self.crime_details[params["event_id"]] = {"crime_type": params["crime_type"]}
            return _RowsResult([])

        if "INSERT INTO user_reported_collision_details" in sql:
            self.collision_details[params["event_id"]] = {
                "weather_condition": params["weather_condition"],
                "light_condition": params["light_condition"],
                "number_of_vehicles": params["number_of_vehicles"],
            }
            return _RowsResult([])

        if "UPDATE user_reported_events" in sql:
            report = self.reports.get(params["report_id"])
            if not report:
                return _RowsResult([])
            report["moderation_status"] = params["moderation_status"]
            report["admin_approved"] = params["admin_approved"]
            report["moderation_notes"] = params["moderation_notes"]
            report["moderated_by"] = params["moderator_id"]
            report["moderated_at"] = datetime(2026, 3, 13, 13, 0, 0, tzinfo=timezone.utc)
            return _RowsResult([{"id": report["id"]}])

        if "WHERE e.id = :report_id" in sql:
            report = self.reports.get(params["report_id"])
            return _RowsResult([] if report is None else [self._row_for(report)])

        if "WHERE e.user_id = :user_id" in sql:
            rows = [item for item in self.reports.values() if item["user_id"] == params["user_id"]]
            if "status" in params:
                rows = [item for item in rows if item["moderation_status"] == params["status"]]
            if "event_kind" in params:
                rows = [item for item in rows if item["event_kind"] == params["event_kind"]]
            rows = sorted(rows, key=lambda item: (item["created_at"], item["id"]), reverse=True)
            rows = [self._row_for(row) for row in rows[: params["row_limit"]]]
            return _RowsResult(rows)

        if "WHERE TRUE" in sql:
            rows = list(self.reports.values())
            if "status" in params:
                rows = [row for row in rows if row["moderation_status"] == params["status"]]
            if "event_kind" in params:
                rows = [row for row in rows if row["event_kind"] == params["event_kind"]]
            if "reporter_type" in params:
                rows = [row for row in rows if row["reporter_type"] == params["reporter_type"]]
            rows.sort(key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return _RowsResult([self._row_for(row) for row in rows[: params["row_limit"]]])

        raise AssertionError(f"Unexpected reported-events query: {sql}")


client = TestClient(app)


def test_reported_events_flow():
    fake_db = _FakeReportedEventsDB()

    def _override_db():
        yield fake_db

    def _override_optional_user():
        return {"id": 1, "email": "user@example.com", "is_admin": False}

    def _override_user():
        return {"id": 1, "email": "user@example.com", "is_admin": False}

    def _override_admin():
        return {"id": 99, "email": "admin@example.com", "is_admin": True}

    app.dependency_overrides[get_db] = _override_db
    try:
        anonymous_create = client.post(
            "/reported-events",
            json={
                "event_kind": "crime",
                "event_date": "2026-03-13",
                "event_time": "08:45",
                "longitude": -1.55,
                "latitude": 53.8,
                "description": "Phone snatch reported by passer-by",
                "crime": {"crime_type": "Robbery"},
            },
        )
        assert anonymous_create.status_code == 201
        anonymous_report = anonymous_create.json()["report"]
        assert anonymous_report["reporter_type"] == "anonymous"
        assert anonymous_report["month"] == "2026-03"
        assert anonymous_report["details"]["crime_type"] == "Robbery"

        app.dependency_overrides[get_optional_current_user] = _override_optional_user
        authenticated_create = client.post(
            "/reported-events",
            json={
                "event_kind": "collision",
                "event_date": "2026-03-14",
                "event_time": "17:30",
                "longitude": -1.54,
                "latitude": 53.81,
                "description": "Two-car shunt at lights",
                "collision": {
                    "weather_condition": "Raining no high winds",
                    "light_condition": "Daylight",
                    "number_of_vehicles": 2,
                },
            },
        )
        assert authenticated_create.status_code == 201
        authenticated_report = authenticated_create.json()["report"]
        assert authenticated_report["reporter_type"] == "authenticated"
        assert authenticated_report["details"]["number_of_vehicles"] == 2

        app.dependency_overrides[get_current_user] = _override_user
        my_reports = client.get("/reported-events/mine")
        assert my_reports.status_code == 200
        assert my_reports.json()["meta"]["returned"] == 1
        assert my_reports.json()["items"][0]["event_kind"] == "collision"

        app.dependency_overrides[require_admin] = _override_admin
        admin_queue = client.get("/admin/reported-events")
        assert admin_queue.status_code == 200
        assert admin_queue.json()["meta"]["returned"] == 2
        assert admin_queue.json()["items"][0]["reporter_email"] == "user@example.com"

        moderation = client.patch(
            f"/admin/reported-events/{authenticated_report['id']}/moderation",
            json={"moderation_status": "approved", "moderation_notes": "Looks valid"},
        )
        assert moderation.status_code == 200
        moderated_report = moderation.json()["report"]
        assert moderated_report["admin_approved"] is True
        assert moderated_report["moderation_status"] == "approved"
        assert moderated_report["moderated_by"] == 99
    finally:
        app.dependency_overrides.clear()
