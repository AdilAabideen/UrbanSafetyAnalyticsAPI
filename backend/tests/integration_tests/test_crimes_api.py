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
    except OperationalError as exc:
        pytest.skip(f"Crime integration tests need a reachable database: {exc}")

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


def test_crimes_map_returns_points_featurecollection(sample_crime):
    params = {
        **_sample_bbox(sample_crime),
        "zoom": 13,
        "month": sample_crime["month_key"],
        "limit": 25,
    }
    response = client.get("/crimes/map", params=params)
    assert response.status_code == 200

    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert data["meta"]["mode"] == "points"
    assert data["meta"]["zoom"] == 13
    assert data["meta"]["limit"] == 25
    assert data["meta"]["returned"] == len(data["features"])
    assert data["meta"]["filters"]["month"] == sample_crime["month_key"]
    assert len(data["features"]) > 0
    assert data["features"][0]["geometry"]["type"] == "Point"
    assert "id" in data["features"][0]["properties"]
    assert "month" in data["features"][0]["properties"]


def test_crimes_map_clusters_mode_returns_cluster_features(sample_crime):
    params = {
        **_sample_bbox(sample_crime, delta=0.02),
        "zoom": 10,
        "month": sample_crime["month_key"],
    }
    response = client.get("/crimes/map", params=params)
    assert response.status_code == 200

    data = response.json()
    assert data["meta"]["mode"] == "clusters"
    assert data["meta"]["nextCursor"] is None
    assert len(data["features"]) > 0
    assert data["features"][0]["properties"]["cluster"] is True
    assert "count" in data["features"][0]["properties"]


def test_crimes_map_crimetype_filter_works(sample_crime):
    params = {
        **_sample_bbox(sample_crime),
        "zoom": 13,
        "month": sample_crime["month_key"],
        "crimeType": sample_crime["crime_type"],
    }
    response = client.get("/crimes/map", params=params)
    assert response.status_code == 200

    data = response.json()
    assert data["meta"]["filters"]["crimeType"] == [sample_crime["crime_type"]]


def test_crimes_map_additional_filters_work(sample_crime):
    params = {
        **_sample_bbox(sample_crime),
        "zoom": 13,
        "month": sample_crime["month_key"],
        "lastOutcomeCategory": sample_crime["last_outcome_category"],
        "lsoaName": sample_crime["lsoa_name"],
    }
    response = client.get("/crimes/map", params=params)
    assert response.status_code == 200

    data = response.json()
    assert data["meta"]["filters"]["lastOutcomeCategory"] == [sample_crime["last_outcome_category"]]
    assert data["meta"]["filters"]["lsoaName"] == [sample_crime["lsoa_name"]]


def test_crimes_by_id_returns_feature(sample_crime):
    response = client.get(f"/crimes/{sample_crime['id']}")
    assert response.status_code == 200

    data = response.json()
    assert data["type"] == "Feature"
    assert data["properties"]["id"] == sample_crime["id"]
    assert data["geometry"]["type"] == "Point"


def test_crimes_by_id_returns_404_for_missing_row(sample_crime):
    response = client.get("/crimes/999999999999")
    assert response.status_code == 404

def test_crimes_map_invalid_cursor_returns_400():
    response = client.get(
        "/crimes/map",
        params={
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "zoom": 13,
            "cursor": "bad-value",
        },
    )
    assert response.status_code == 400


def test_crimes_map_cursor_not_allowed_for_clusters():
    response = client.get(
        "/crimes/map",
        params={
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "zoom": 10,
            "cursor": "2024-01|1",
        },
    )
    assert response.status_code == 400


def test_crimes_map_invalid_month_returns_400():
    response = client.get(
        "/crimes/map",
        params={
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "zoom": 13,
            "month": "2024-01-01",
        },
    )
    assert response.status_code == 400


def test_crimes_map_invalid_mode_returns_400():
    response = client.get(
        "/crimes/map",
        params={
            "minLon": -1.6,
            "minLat": 53.78,
            "maxLon": -1.52,
            "maxLat": 53.82,
            "zoom": 13,
            "mode": "bad",
        },
    )
    assert response.status_code == 400
