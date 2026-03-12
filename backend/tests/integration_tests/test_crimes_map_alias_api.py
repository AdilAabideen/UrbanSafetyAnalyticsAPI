import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

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
            COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
            COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS last_outcome_category,
            COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown') AS lsoa_name,
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
    except SQLAlchemyError as exc:
        pytest.skip(f"Crime alias integration tests need a healthy database: {exc}")

    if not row:
        pytest.skip("crime_events has no rows with geometry")

    return row


def _sample_bbox(sample_crime, delta=0.015):
    return {
        "minLon": sample_crime["lon"] - delta,
        "minLat": sample_crime["lat"] - delta,
        "maxLon": sample_crime["lon"] + delta,
        "maxLat": sample_crime["lat"] + delta,
    }


def test_crimes_alias_route_returns_points_payload(sample_crime):
    response = client.get(
        "/crimes",
        params={
            **_sample_bbox(sample_crime),
            "zoom": 13,
            "month": sample_crime["month_key"],
            "limit": 10,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["meta"]["filters"]["month"] == sample_crime["month_key"]


def test_crimes_map_can_force_clusters_at_higher_zoom(sample_crime):
    response = client.get(
        "/crimes/map",
        params={
            **_sample_bbox(sample_crime, delta=0.03),
            "zoom": 13,
            "mode": "clusters",
            "month": sample_crime["month_key"],
            "lastOutcomeCategory": sample_crime["last_outcome_category"],
            "lsoaName": sample_crime["lsoa_name"],
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["mode"] == "clusters"
    assert data["meta"]["filters"]["lastOutcomeCategory"] == [sample_crime["last_outcome_category"]]
    assert data["meta"]["filters"]["lsoaName"] == [sample_crime["lsoa_name"]]
