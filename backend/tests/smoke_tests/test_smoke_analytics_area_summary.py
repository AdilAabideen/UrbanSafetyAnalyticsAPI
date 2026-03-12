from fastapi.testclient import TestClient

from app.db import get_db
from app.main import app
from tests.inmemory_db import InMemoryDB


client = TestClient(app)


def _override_get_db():
    handlers = {
        "COUNT(*)::bigint AS total_crimes": {
            "rows": [
                {
                    "total_crimes": 12345,
                }
            ]
        },
        "GROUP BY COALESCE(NULLIF(ce.crime_type, ''), 'unknown')": {
            "rows": [
                {
                    "crime_type": "Violence and sexual offences",
                    "count": 4200,
                },
                {
                    "crime_type": "Shoplifting",
                    "count": 2100,
                },
            ]
        },
        "SELECT generate_series(": {
            "rows": [
                {"month": "2023-02", "count": 4100},
                {"month": "2023-03", "count": 4300},
                {"month": "2023-04", "count": 3945},
            ]
        },
    }
    yield InMemoryDB(handlers)


def test_analytics_area_summary_returns_bbox_breakdown_and_trend():
    app.dependency_overrides[get_db] = _override_get_db
    try:
        response = client.get(
            "/analytics/area/summary",
            params={
                "minLon": -1.6,
                "minLat": 53.78,
                "maxLon": -1.52,
                "maxLat": 53.82,
                "from": "2023-02",
                "to": "2023-04",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "bbox": {
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
        },
        "from": "2023-02",
        "to": "2023-04",
        "crimeType": None,
        "total_crimes": 12345,
        "by_type_top": [
            {"crime_type": "Violence and sexual offences", "count": 4200},
            {"crime_type": "Shoplifting", "count": 2100},
        ],
        "monthly_trend": [
            {"month": "2023-02", "count": 4100},
            {"month": "2023-03", "count": 4300},
            {"month": "2023-04", "count": 3945},
        ],
    }


def test_analytics_area_summary_rejects_reversed_month_range_without_db_access():
    response = client.get(
        "/analytics/area/summary",
        params={
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "from": "2023-04",
            "to": "2023-02",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "from must be less than or equal to to"
