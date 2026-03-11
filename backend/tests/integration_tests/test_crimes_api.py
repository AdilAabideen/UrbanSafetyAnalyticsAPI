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
        pytest.skip(f"Crime integration tests need a reachable database: {exc}")

    if not row:
        pytest.skip("crime_events has no rows with geometry")

    return row


def test_crimes_returns_featurecollection(sample_crime):
    r = client.get("/crimes", params={"month": sample_crime["month_key"], "limit": 25})
    assert r.status_code == 200

    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert isinstance(data["features"], list)
    assert len(data["features"]) > 0
    assert data["meta"]["limit"] == 25
    assert data["meta"]["returned"] == len(data["features"])

    feature = data["features"][0]
    assert feature["type"] == "Feature"
    assert feature["geometry"]["type"] == "Point"
    assert "crime_type" in feature["properties"]


def test_crimes_by_id_returns_feature(sample_crime):
    r = client.get(f"/crimes/{sample_crime['id']}")
    assert r.status_code == 200

    data = r.json()
    assert data["type"] == "Feature"
    assert data["properties"]["id"] == sample_crime["id"]
    assert data["geometry"]["type"] == "Point"


def test_crimes_by_id_returns_404_for_missing_row(sample_crime):
    r = client.get("/crimes/999999999999")
    assert r.status_code == 404


def test_crime_stats_respect_bbox_and_month(sample_crime):
    delta = 0.01
    params = {
        "month": sample_crime["month_key"],
        "minLon": sample_crime["lon"] - delta,
        "minLat": sample_crime["lat"] - delta,
        "maxLon": sample_crime["lon"] + delta,
        "maxLat": sample_crime["lat"] + delta,
    }
    r = client.get("/crimes/stats", params=params)
    assert r.status_code == 200

    data = r.json()
    assert data["filters"]["month"] == sample_crime["month_key"]
    assert "bbox" in data["filters"]
    assert data["total"] == sum(data["crime_type_counts"].values())


def test_crimes_bbox_and_limit_work(sample_crime):
    delta = 0.01
    params = {
        "month": sample_crime["month_key"],
        "minLon": sample_crime["lon"] - delta,
        "minLat": sample_crime["lat"] - delta,
        "maxLon": sample_crime["lon"] + delta,
        "maxLat": sample_crime["lat"] + delta,
        "limit": 1,
    }
    r = client.get("/crimes", params=params)
    assert r.status_code == 200

    data = r.json()
    assert data["meta"]["limit"] == 1
    assert data["meta"]["returned"] <= 1
    assert "bbox" in data["meta"]


def test_crimes_invalid_month_returns_400():
    r = client.get("/crimes", params={"month": "2024-01-01"})
    assert r.status_code == 400


def test_crimes_partial_bbox_returns_400():
    r = client.get("/crimes", params={"minLon": -1.5, "minLat": 53.8})
    assert r.status_code == 400


def test_crime_stats_partial_bbox_returns_400():
    r = client.get("/crimes/stats", params={"minLon": -1.5, "minLat": 53.8})
    assert r.status_code == 400
