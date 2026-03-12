from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app


class _FakeRowsResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


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
            }
        ]
        return _FakeRowsResult(rows[: params["row_limit"]])


client = TestClient(app)


def _override_points_db():
    yield _FakePointsSession()


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


def test_crimes_map_accepts_month_range_and_returns_filter_meta():
    app.dependency_overrides[get_db] = _override_points_db
    try:
        response = client.get(
            "/crimes/map",
            params=_map_params(startMonth="2023-03", endMonth="2023-05"),
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["filters"]["month"] is None
    assert data["meta"]["filters"]["startMonth"] == "2023-03"
    assert data["meta"]["filters"]["endMonth"] == "2023-05"


def test_crimes_map_rejects_partial_month_range_without_db_access():
    response = client.get("/crimes/map", params=_map_params(startMonth="2023-03"))

    assert response.status_code == 400
    assert response.json()["detail"] == "startMonth and endMonth must be provided together"
