from datetime import date

import pytest
from fastapi import HTTPException

from app.api import roads


def test_roads_resolve_from_to_filter_accepts_valid_range():
    resolved = roads._resolve_from_to_filter("2023-02", "2023-06", required=True)

    assert resolved["from"] == "2023-02"
    assert resolved["to"] == "2023-06"
    assert resolved["from_date"] == date(2023, 2, 1)
    assert resolved["to_date"] == date(2023, 6, 1)


def test_roads_resolve_from_to_filter_rejects_reversed_range():
    with pytest.raises(HTTPException) as exc:
        roads._resolve_from_to_filter("2023-06", "2023-02", required=True)

    assert exc.value.status_code == 400
    assert exc.value.detail == "from must be less than or equal to to"


def test_roads_sort_expression_defaults_to_incidents_per_km():
    assert roads._sort_expression("incidents_per_km").startswith("incidents_per_km DESC")
    assert roads._sort_expression("incident_count").startswith("incident_count DESC")
