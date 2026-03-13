from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)

LEEDS_BBOX = {
    "minLon": -1.60,
    "minLat": 53.78,
    "maxLon": -1.52,
    "maxLat": 53.82,
}


def test_roads_analytics_meta_returns_filter_payload():
    response = client.get("/roads/analytics/meta")
    assert response.status_code == 200

    data = response.json()
    assert "months" in data
    assert "highways" in data
    assert "crime_types" in data
    assert "outcomes" in data
    assert "counts" in data


def test_roads_analytics_overview_returns_summary_payload():
    response = client.get(
        "/roads/analytics/overview",
        params={"from": "2023-02", "to": "2023-06", **LEEDS_BBOX},
    )
    assert response.status_code == 200

    data = response.json()
    assert "total_incidents" in data
    assert "band_breakdown" in data
    assert "insights" in data


def test_roads_analytics_charts_returns_chart_payload():
    response = client.get(
        "/roads/analytics/charts",
        params={"from": "2023-02", "to": "2023-06", **LEEDS_BBOX},
    )
    assert response.status_code == 200

    data = response.json()
    assert "timeseries" in data
    assert "by_highway" in data
    assert "by_crime_type" in data
    assert "by_outcome" in data
    assert "band_breakdown" in data


def test_roads_analytics_risk_returns_ranked_items():
    response = client.get(
        "/roads/analytics/risk",
        params={"from": "2023-02", "to": "2023-06", **LEEDS_BBOX, "limit": 10},
    )
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "meta" in data


def test_roads_analytics_risk_rejects_invalid_sort():
    response = client.get(
        "/roads/analytics/risk",
        params={"from": "2023-02", "to": "2023-06", "sort": "bad"},
    )
    assert response.status_code == 400
