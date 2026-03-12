from datetime import date

import pytest
from fastapi import HTTPException

from app.api import crimes


def test_parse_month_accepts_year_month():
    parsed = crimes._parse_month("2024-01")

    assert parsed == date(2024, 1, 1)


def test_parse_month_rejects_bad_format():
    with pytest.raises(HTTPException) as exc:
        crimes._parse_month("2024-01-15")

    assert exc.value.status_code == 400
    assert exc.value.detail == "month must be in YYYY-MM format"


def test_parse_cursor_accepts_year_month_and_id():
    parsed = crimes._parse_cursor("2024-01|42")

    assert parsed["cursor_month"] == date(2024, 1, 1)
    assert parsed["cursor_id"] == 42


def test_normalize_filter_values_flattens_csv_and_lists():
    normalized = crimes._normalize_filter_values(
        ["burglary, vehicle-crime", "shoplifting"],
        "crimeType",
    )

    assert normalized == ["burglary", "vehicle-crime", "shoplifting"]


def test_resolve_mode_auto_switches_between_clusters_and_points():
    assert crimes._resolve_mode("auto", 10) == "clusters"
    assert crimes._resolve_mode("auto", 13) == "points"


def test_cluster_grid_size_is_larger_at_lower_zoom():
    bbox = {"min_lon": -1.6, "min_lat": 53.78, "max_lon": -1.5, "max_lat": 53.82}

    low_zoom = crimes._cluster_grid_size(bbox, 8)
    high_zoom = crimes._cluster_grid_size(bbox, 11)

    assert low_zoom[0] > high_zoom[0]
    assert low_zoom[1] > high_zoom[1]
