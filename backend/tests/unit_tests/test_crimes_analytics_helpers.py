from datetime import date

import pytest
from fastapi import HTTPException

from app.api import crimes


def test_resolve_from_to_filter_accepts_valid_range():
    resolved = crimes._resolve_from_to_filter("2023-02", "2023-06", required=True)

    assert resolved["from"] == "2023-02"
    assert resolved["to"] == "2023-06"
    assert resolved["params"]["from_month_date"] == date(2023, 2, 1)
    assert resolved["params"]["to_month_date"] == date(2023, 6, 1)


def test_resolve_from_to_filter_rejects_reversed_range():
    with pytest.raises(HTTPException) as exc:
        crimes._resolve_from_to_filter("2023-06", "2023-02", required=True)

    assert exc.value.status_code == 400
    assert exc.value.detail == "from must be less than or equal to to"


def test_shift_month_moves_across_year_boundary():
    shifted = crimes._shift_month(date(2023, 1, 1), -2)

    assert shifted == date(2022, 11, 1)
