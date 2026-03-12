from fastapi import HTTPException

from app.api import crimes


def test_resolve_month_filter_accepts_inclusive_range():
    month_filter = crimes._resolve_month_filter(None, "2023-03", "2023-05")

    assert month_filter["clause"] == "ce.month BETWEEN :start_month_date AND :end_month_date"
    assert month_filter["month"] is None
    assert month_filter["startMonth"] == "2023-03"
    assert month_filter["endMonth"] == "2023-05"


def test_resolve_month_filter_rejects_mixed_single_month_and_range():
    try:
        crimes._resolve_month_filter("2023-03", "2023-03", "2023-05")
    except HTTPException as exc:
        assert exc.status_code == 400
        assert exc.detail == "Use either month or startMonth/endMonth, not both"
        return

    raise AssertionError("Expected HTTPException for mixed month filters")
