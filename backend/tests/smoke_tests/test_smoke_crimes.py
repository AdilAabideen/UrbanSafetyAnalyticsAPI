from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


class _FakeScalarResult:
    def __init__(self, payload):
        self.payload = payload

    def scalar_one(self):
        return self.payload

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
                "id": 11,
                "crime_id": "crime-11",
                "month_label": "2024-01",
                "crime_type": "burglary",
                "last_outcome_category": "Under investigation",
                "location_text": "Test Street",
                "reported_by": "West Yorkshire Police",
                "falls_within": "Leeds",
                "lsoa_code": "E0001",
                "lsoa_name": "Leeds 001",
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
            },
            {
                "id": 10,
                "crime_id": "crime-10",
                "month_label": "2024-01",
                "crime_type": "vehicle-crime",
                "last_outcome_category": "No suspect identified",
                "location_text": "Test Street",
                "reported_by": "West Yorkshire Police",
                "falls_within": "Leeds",
                "lsoa_code": "E0001",
                "lsoa_name": "Leeds 001",
                "geometry": {"type": "Point", "coordinates": [-1.551, 53.801]},
            },
        ]
        return _FakeRowsResult(rows[: params["row_limit"]])


class _FakeClustersSession:
    def execute(self, query, params):
        rows = [
            {
                "cluster_id": f"{params['zoom']}:1:2",
                "count": 12,
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                "top_crime_types": {"burglary": 7, "vehicle-crime": 5},
            }
        ]
        return _FakeRowsResult(rows[: params["row_limit"]])


class _FakeDetailSession:
    def execute(self, query, params):
        return _FakeScalarResult(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-1.55, 53.8]},
                "properties": {"id": params["crime_id"], "crime_type": "burglary"},
            }
        )

client = TestClient(app)


def _override_points_db():
    yield _FakePointsSession()


def _override_clusters_db():
    yield _FakeClustersSession()


def _override_detail_db():
    yield _FakeDetailSession()

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


def test_crimes_map_points_route_returns_envelope():
    app.dependency_overrides[get_db] = _override_points_db
    try:
        response = client.get(
            "/crimes/map",
            params=_map_params(
                limit=1,
                crimeType="burglary",
                lastOutcomeCategory="Under investigation",
                lsoaName="Leeds 001",
            ),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["meta"]["limit"] == 1
    assert data["meta"]["truncated"] is True
    assert data["meta"]["nextCursor"] == "2024-01|11"
    assert data["meta"]["filters"]["crimeType"] == ["burglary"]
    assert data["meta"]["filters"]["lastOutcomeCategory"] == ["Under investigation"]
    assert data["meta"]["filters"]["lsoaName"] == ["Leeds 001"]
    assert data["features"][0]["geometry"]["type"] == "Point"


def test_crimes_map_auto_clusters_returns_cluster_features():
    app.dependency_overrides[get_db] = _override_clusters_db
    try:
        response = client.get("/crimes/map", params=_map_params(zoom=10))
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["mode"] == "clusters"
    assert data["meta"]["nextCursor"] is None
    assert data["features"][0]["properties"]["cluster"] is True
    assert data["features"][0]["properties"]["count"] == 12


def test_crime_detail_route_returns_geojson():
    app.dependency_overrides[get_db] = _override_detail_db
    try:
        response = client.get("/crimes/1")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "Feature"
    assert data["properties"]["id"] == 1

def test_crimes_map_invalid_cursor_returns_400_without_db_access():
    response = client.get("/crimes/map", params=_map_params(cursor="bad"))

    assert response.status_code == 400
    assert response.json()["detail"] == "cursor must be in YYYY-MM|id format"


def test_crimes_map_cursor_not_allowed_for_clusters_without_db_access():
    response = client.get("/crimes/map", params=_map_params(zoom=10, cursor="2024-01|1"))

    assert response.status_code == 400
    assert response.json()["detail"] == "cursor is only supported for points mode"


def test_crimes_map_invalid_mode_returns_400_without_db_access():
    response = client.get("/crimes/map", params=_map_params(mode="bad"))

    assert response.status_code == 400
    assert response.json()["detail"] == "mode must be one of auto, points, or clusters"


def test_crimes_map_invalid_month_returns_400_without_db_access():
    response = client.get("/crimes/map", params=_map_params(month="2024-01-01"))

    assert response.status_code == 400
    assert response.json()["detail"] == "month must be in YYYY-MM format"
