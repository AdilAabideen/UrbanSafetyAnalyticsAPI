from datetime import date, time

from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


class _FakeScalarResult:
    def __init__(self, payload):
        self.payload = payload

    def scalar_one_or_none(self):
        return self.payload


class _FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)

    def __iter__(self):
        return iter(self.rows)


class _FakePointsSession:
    def execute(self, query, params):
        rows = [
            {
                "collision_index": "c-11",
                "month_label": "2025-01",
                "collision_date_label": "2025-01-01",
                "collision_time": time(12, 30),
                "collision_severity": "Serious",
                "road_type": "Single carriageway",
                "speed_limit": "30 mph",
                "light_conditions": "Daylight",
                "weather_conditions": "Fine no high winds",
                "road_surface_conditions": "Dry",
                "lsoa_code": "E0101",
                "number_of_vehicles": 2,
                "number_of_casualties": 1,
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
            },
            {
                "collision_index": "c-10",
                "month_label": "2025-01",
                "collision_date_label": "2025-01-01",
                "collision_time": time(12, 10),
                "collision_severity": "Slight",
                "road_type": "Roundabout",
                "speed_limit": "20 mph",
                "light_conditions": "Darkness - lights lit",
                "weather_conditions": "Rain no high winds",
                "road_surface_conditions": "Wet or damp",
                "lsoa_code": "E0102",
                "number_of_vehicles": 1,
                "number_of_casualties": 2,
                "geometry": {"type": "Point", "coordinates": [-1.551, 53.801]},
            },
        ]
        return _FakeRowsResult(rows[: params["row_limit"]])


class _FakeClustersSession:
    def execute(self, query, params):
        rows = [
            {
                "cluster_id": f"{params['zoom']}:1:2",
                "count": 8,
                "total_casualties": 5,
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                "top_collision_severities": {"Serious": 5, "Slight": 3},
            }
        ]
        return _FakeRowsResult(rows[: params["row_limit"]])


class _FakeDetailSession:
    def execute(self, query, params):
        return _FakeScalarResult(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                "properties": {"collision_index": params["collision_index"], "collision_severity": "Serious"},
            }
        )


class _FakeStatsSession:
    def execute(self, query, params):
        return _FakeRowsResult(
            [
                {"collision_severity": "Serious", "count": 2, "casualty_count": 3},
                {"collision_severity": "Slight", "count": 1, "casualty_count": 1},
            ]
        )


client = TestClient(app)


def _override_points_db():
    yield _FakePointsSession()


def _override_clusters_db():
    yield _FakeClustersSession()


def _override_detail_db():
    yield _FakeDetailSession()


def _override_stats_db():
    yield _FakeStatsSession()


def _map_params(**kwargs):
    params = {
        "minLon": -1.56,
        "minLat": 53.79,
        "maxLon": -1.54,
        "maxLat": 53.81,
        "zoom": 13,
    }
    params.update(kwargs)
    return params


def test_collisions_map_points_route_returns_envelope():
    app.dependency_overrides[get_db] = _override_points_db
    try:
        response = client.get(
            "/collisions/map",
            params=_map_params(
                limit=1,
                collisionSeverity="Serious",
                roadType="Single carriageway",
                lsoaCode="E0101",
            ),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["meta"]["truncated"] is True
    assert data["meta"]["nextCursor"] == "2025-01|c-11"
    assert data["meta"]["filters"]["collisionSeverity"] == ["Serious"]
    assert data["meta"]["filters"]["roadType"] == ["Single carriageway"]
    assert data["features"][0]["geometry"]["type"] == "Point"


def test_collisions_map_auto_clusters_returns_cluster_features():
    app.dependency_overrides[get_db] = _override_clusters_db
    try:
        response = client.get("/collisions/map", params=_map_params(zoom=10))
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["mode"] == "clusters"
    assert data["features"][0]["properties"]["cluster"] is True
    assert data["features"][0]["properties"]["total_casualties"] == 5


def test_collision_detail_route_returns_geojson():
    app.dependency_overrides[get_db] = _override_detail_db
    try:
        response = client.get("/collisions/c-1")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "Feature"
    assert data["properties"]["collision_index"] == "c-1"


def test_collision_stats_route_returns_grouped_counts():
    app.dependency_overrides[get_db] = _override_stats_db
    try:
        response = client.get("/collisions/stats", params={"month": "2025-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["filters"]["month"] == "2025-01"
    assert data["total"] == 3
    assert data["total_casualties"] == 4
    assert data["collision_severity_counts"]["Serious"] == 2
