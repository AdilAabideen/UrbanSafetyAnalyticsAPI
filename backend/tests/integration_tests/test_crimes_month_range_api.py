import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.db import engine
from app.main import app


client = TestClient(app)


@pytest.fixture(scope="module")
def sample_crime():
    query = text(
        """
        SELECT
            ce.id,
            to_char(ce.month, 'YYYY-MM') AS month_key,
            ST_X(ce.geom) AS lon,
            ST_Y(ce.geom) AS lat
        FROM crime_events ce
        WHERE ce.geom IS NOT NULL
        ORDER BY ce.month DESC, ce.id DESC
        LIMIT 1
        """
    )

    try:
        with engine.connect() as conn:
            row = conn.execute(query).mappings().first()
    except OperationalError as exc:
        pytest.skip(f"Crime month-range integration tests need a reachable database: {exc}")

    if not row:
        pytest.skip("crime_events has no rows with geometry")

    return row


def _sample_bbox(sample_crime, delta=0.01):
    return {
        "minLon": sample_crime["lon"] - delta,
        "minLat": sample_crime["lat"] - delta,
        "maxLon": sample_crime["lon"] + delta,
        "maxLat": sample_crime["lat"] + delta,
    }


def test_crimes_map_month_range_returns_points_featurecollection(sample_crime):
    response = client.get(
        "/crimes/map",
        params={
            **_sample_bbox(sample_crime),
            "zoom": 13,
            "startMonth": sample_crime["month_key"],
            "endMonth": sample_crime["month_key"],
            "limit": 25,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["meta"]["filters"]["month"] is None
    assert data["meta"]["filters"]["startMonth"] == sample_crime["month_key"]
    assert data["meta"]["filters"]["endMonth"] == sample_crime["month_key"]
    assert len(data["features"]) > 0
