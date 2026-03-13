import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.api import watchlist as watchlist_api
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


class _WatchlistReportsMemoryDB:
    def __init__(self):
        self.watchlists = {
            1: {
                "id": 1,
                "user_id": 7,
                "name": "Leeds Centre",
                "min_lon": -1.7,
                "min_lat": 53.75,
                "max_lon": -1.45,
                "max_lat": 53.88,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        }
        self.preference = {
            "id": 1,
            "watchlist_id": 1,
            "window_months": 6,
            "crime_types": ["burglary"],
            "travel_mode": "walking",
            "include_collisions": False,
            "baseline_months": 6,
            "hotspot_k": 20,
            "include_hotspot_stability": True,
            "include_forecast": True,
            "weight_crime": 1.0,
            "weight_collision": 0.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.reports = {}
        self.next_report_id = 1

    def execute(self, query, params):
        sql = str(query)

        if "FROM watchlists w" in sql and "WHERE w.id = :watchlist_id AND w.user_id = :user_id" in sql:
            watchlist = self.watchlists.get(params["watchlist_id"])
            if not watchlist or watchlist["user_id"] != params["user_id"]:
                return _RowsResult([])
            return _RowsResult([watchlist])

        if "FROM watchlist_preferences wp" in sql and "ORDER BY wp.created_at DESC, wp.id DESC" in sql:
            if params["watchlist_id"] != self.preference["watchlist_id"]:
                return _RowsResult([])
            return _RowsResult([self.preference])

        if "INSERT INTO watchlist_reports" in sql:
            report = {
                "id": self.next_report_id,
                "watchlist_id": params["watchlist_id"],
                "user_id": params["user_id"],
                "from_month": params["from_month"],
                "to_month": params["to_month"],
                "forecast_target_month": params["forecast_target_month"],
                "crime_type": params["crime_type"],
                "mode": params["mode"],
                "include_collisions": params["include_collisions"],
                "payload_json": json.loads(params["payload_json"]),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            self.reports[report["id"]] = report
            self.next_report_id += 1
            return _RowsResult([{"id": report["id"], "created_at": report["created_at"]}])

        if "FROM watchlist_reports wr" in sql and "wr.payload_json" in sql:
            report = self.reports.get(params["report_id"])
            if not report:
                return _RowsResult([])
            if report["watchlist_id"] != params["watchlist_id"] or report["user_id"] != params["user_id"]:
                return _RowsResult([])
            return _RowsResult([report])

        if "FROM watchlist_reports wr" in sql and "ORDER BY wr.created_at DESC, wr.id DESC" in sql:
            rows = [
                report
                for report in self.reports.values()
                if report["watchlist_id"] == params["watchlist_id"] and report["user_id"] == params["user_id"]
            ]
            rows.sort(key=lambda row: (row["created_at"], row["id"]), reverse=True)
            return _RowsResult(rows[: params["limit"]])

        raise AssertionError(f"Unexpected watchlist report query: {sql}")

    def commit(self):
        return None

    def rollback(self):
        return None


def test_watchlist_report_generation_and_reads():
    fake_db = _WatchlistReportsMemoryDB()

    def _override_db():
        yield fake_db

    def _override_user():
        return {
            "id": 7,
            "email": "watcher@example.com",
            "is_admin": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _fake_risk(*args, **kwargs):
        return {
            "scope": {"from": "2025-01", "to": "2025-03"},
            "generated_at": "2026-03-13T12:00:00Z",
            "score_basis": "crime+collision",
            "risk_score": 73,
            "score": 73,
            "pct": 0.731,
            "band": "amber",
            "metrics": {"total_crimes": 15},
            "explain": {"reading": "This bbox sits above the wider network average."},
        }

    def _fake_forecast(*args, **kwargs):
        return {
            "scope": {"target": "2025-04", "baselineMonths": 8},
            "generated_at": "2026-03-13T12:00:01Z",
            "score_basis": "crime+collision",
            "history": [{"month": "2025-01", "crime_count": 5}],
            "forecast": {"expected_count": 6, "predicted_band": "amber"},
            "explanation": {"summary": "Forecast summary"},
        }

    def _fake_hotspots(*args, **kwargs):
        return {
            "scope": {"from": "2025-01", "to": "2025-03", "k": 12},
            "generated_at": "2026-03-13T12:00:02Z",
            "stability_series": [{"month": "2025-02", "jaccard_vs_prev": 0.5, "overlap_count": 6}],
            "persistent_hotspots": [{"segment_id": 123, "appearances": 2, "appearance_ratio": 1.0}],
            "summary": {"months_evaluated": 3, "average_jaccard": 0.5, "persistent_hotspot_count": 1},
        }

    def _fake_crime_summary(*args, **kwargs):
        return {
            "from": "2025-01",
            "to": "2025-03",
            "total_crimes": 25,
            "unique_lsoas": 3,
            "unique_crime_types": 4,
            "top_crime_type": {"crime_type": "Shoplifting", "count": 10},
            "crimes_with_outcomes": 20,
            "top_crime_types": [{"crime_type": "Shoplifting", "count": 10}],
            "top_outcomes": [{"outcome": "Under investigation", "count": 6}],
        }

    def _fake_collision_summary(*args, **kwargs):
        return {
            "from": "2025-01",
            "to": "2025-03",
            "total_collisions": 4,
            "total_casualties": 6,
            "unique_lsoas": 2,
            "collisions_with_casualties": 3,
            "fatal_casualties": 0,
            "serious_casualties": 1,
            "slight_casualties": 5,
            "avg_casualties_per_collision": 1.5,
            "top_collision_severity": {"collision_severity": "Slight", "count": 3},
            "top_road_type": {"road_type": "Single carriageway", "count": 2},
            "top_weather_condition": {"weather_condition": "Fine no high winds", "count": 4},
            "top_light_condition": {"light_condition": "Daylight", "count": 3},
        }

    def _fake_road_overview(*args, **kwargs):
        return {
            "from": "2025-01",
            "to": "2025-03",
            "total_segments": 12,
            "total_length_m": 2400.0,
            "unique_highway_types": 3,
            "roads_with_incidents": 5,
            "total_incidents": 25,
            "avg_incidents_per_km": 10.4,
            "top_road": {"segment_id": 501, "name": "High Street", "risk_score": 89.5},
            "top_highway": {"highway": "primary", "incident_count": 8},
            "top_crime_type": {"crime_type": "Shoplifting", "count": 10},
            "top_outcome": {"outcome": "Under investigation", "count": 6},
            "band_breakdown": {"red": 1, "amber": 2, "green": 9},
            "insights": ["Primary roads account for most incidents in this watchlist."],
        }

    def _fake_road_risk(*args, **kwargs):
        return {
            "filters": {"from": "2025-01", "to": "2025-03"},
            "items": [
                {
                    "segment_id": 501,
                    "name": "High Street",
                    "highway": "primary",
                    "length_m": 320.0,
                    "incident_count": 8,
                    "incidents_per_km": 25.0,
                    "dominant_crime_type": "Shoplifting",
                    "dominant_outcome": "Under investigation",
                    "share_of_incidents": 32.0,
                    "previous_period_change_pct": 18.2,
                    "risk_score": 89.5,
                    "band": "amber",
                    "message": "This road combines elevated incident volume with a high normalized rate.",
                }
            ],
        }

    original_risk = watchlist_api.build_risk_score_payload
    original_forecast = watchlist_api.build_risk_forecast_payload
    original_hotspots = watchlist_api.build_hotspot_stability_payload
    original_crime_summary = watchlist_api.get_crime_analytics_summary
    original_collision_summary = watchlist_api.get_collision_analytics_summary
    original_road_overview = watchlist_api.get_road_analytics_overview
    original_road_risk = watchlist_api.get_road_analytics_risk

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user
    watchlist_api.build_risk_score_payload = _fake_risk
    watchlist_api.build_risk_forecast_payload = _fake_forecast
    watchlist_api.build_hotspot_stability_payload = _fake_hotspots
    watchlist_api.get_crime_analytics_summary = _fake_crime_summary
    watchlist_api.get_collision_analytics_summary = _fake_collision_summary
    watchlist_api.get_road_analytics_overview = _fake_road_overview
    watchlist_api.get_road_analytics_risk = _fake_road_risk
    try:
        create_report = client.post(
            "/watchlists/1/report",
            json={
                "from": "2025-01",
                "to": "2025-03",
                "crimeType": "Shoplifting",
                "mode": "drive",
                "includeCollisions": True,
                "baselineMonths": 8,
                "k": 12,
            },
        )
        assert create_report.status_code == 200
        report = create_report.json()["report"]
        assert report["id"] == 1
        assert report["snapshot_id"] == 1
        assert report["preferences_used"]["crimeType"] == "Shoplifting"
        assert report["preferences_used"]["mode"] == "drive"
        assert report["preferences_used"]["includeCollisions"] is True
        assert report["preferences_used"]["baselineMonths"] == 8
        assert report["score"]["risk_score"] == 73
        assert report["forecast"]["forecast"]["expected_count"] == 6
        assert report["hotspot_stability"]["summary"]["months_evaluated"] == 3
        assert report["general_statistics"]["crime_summary"]["total_crimes"] == 25
        assert report["general_statistics"]["collision_summary"]["total_collisions"] == 4
        assert report["general_statistics"]["roads_overview"]["total_incidents"] == 25
        assert report["general_statistics"]["headline"]["most_dangerous_road"]["segment_id"] == 501
        assert report["general_statistics"]["headline"]["worst_crime_category"]["crime_type"] == "Shoplifting"

        list_reports = client.get("/watchlists/1/reports")
        assert list_reports.status_code == 200
        assert len(list_reports.json()["items"]) == 1
        assert list_reports.json()["items"][0]["id"] == 1
        assert list_reports.json()["items"][0]["mode"] == "drive"

        read_report = client.get("/watchlists/1/reports/1")
        assert read_report.status_code == 200
        stored = read_report.json()["report"]
        assert stored["id"] == 1
        assert stored["snapshot_id"] == 1
        assert stored["score"]["band"] == "amber"
        assert stored["general_statistics"]["top_risky_roads"][0]["name"] == "High Street"
    finally:
        watchlist_api.build_risk_score_payload = original_risk
        watchlist_api.build_risk_forecast_payload = original_forecast
        watchlist_api.build_hotspot_stability_payload = original_hotspots
        watchlist_api.get_crime_analytics_summary = original_crime_summary
        watchlist_api.get_collision_analytics_summary = original_collision_summary
        watchlist_api.get_road_analytics_overview = original_road_overview
        watchlist_api.get_road_analytics_risk = original_road_risk
        app.dependency_overrides.clear()
