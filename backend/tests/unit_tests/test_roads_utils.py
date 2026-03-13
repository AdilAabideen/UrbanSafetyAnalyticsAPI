from datetime import date

import pytest
from fastapi import HTTPException

from app.api_utils import roads_utils


def test_resolve_from_to_filter_parses_range():
    value = roads_utils._resolve_from_to_filter("2025-01", "2025-03", required=True)

    assert value["from"] == "2025-01"
    assert value["to"] == "2025-03"
    assert value["from_date"] == date(2025, 1, 1)
    assert value["to_date"] == date(2025, 3, 1)


def test_resolve_from_to_filter_requires_pair():
    with pytest.raises(HTTPException) as exc_info:
        roads_utils._resolve_from_to_filter("2025-01", None, required=True)

    assert exc_info.value.status_code == 400
    assert "provided together" in exc_info.value.detail


def test_where_sql_formats_conditions():
    assert roads_utils._where_sql([]) == ""
    assert roads_utils._where_sql(["a = 1", "b = 2"]) == "WHERE a = 1 AND b = 2"


def test_roads_scope_filters_include_bbox_and_highway():
    bbox = {"min_lon": -1.6, "min_lat": 53.7, "max_lon": -1.4, "max_lat": 53.9}
    where_clauses, query_params = roads_utils._roads_scope_filters(bbox, ["primary"])

    assert any("ST_Intersects" in clause for clause in where_clauses)
    assert any("IN :highways" in clause for clause in where_clauses)
    assert query_params["highways"] == ["primary"]
    assert query_params["min_lon"] == -1.6


def test_sort_expression_supports_expected_values():
    assert roads_utils._sort_expression("risk_score").startswith("risk_score DESC")
    assert roads_utils._sort_expression("incident_count").startswith("incident_count DESC")
    assert roads_utils._sort_expression("incidents_per_km").startswith("incidents_per_km DESC")


def test_matched_previous_period_aligns_month_span():
    current = roads_utils._resolve_from_to_filter("2025-03", "2025-05", required=True)
    previous = roads_utils._matched_previous_period(current)

    assert previous["from"] == "2024-12"
    assert previous["to"] == "2025-02"


def test_safe_pct_change_handles_zero_previous():
    assert roads_utils._safe_pct_change(50, 0) is None
    assert roads_utils._safe_pct_change(75, 50) == 50.0
