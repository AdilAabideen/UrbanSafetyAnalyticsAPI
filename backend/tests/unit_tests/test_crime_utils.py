from datetime import date

import pytest
from fastapi import HTTPException

from app.api_utils import crime_utils


def test_parse_month_accepts_valid_yyyy_mm():
    parsed = crime_utils._parse_month("2025-03")

    assert parsed == date(2025, 3, 1)


def test_parse_month_rejects_invalid_format():
    with pytest.raises(HTTPException) as exc_info:
        crime_utils._parse_month("03-2025")

    assert exc_info.value.status_code == 400
    assert "YYYY-MM" in exc_info.value.detail


def test_resolve_month_filter_rejects_mixed_month_and_range():
    with pytest.raises(HTTPException) as exc_info:
        crime_utils._resolve_month_filter("2025-03", "2025-01", "2025-02")

    assert exc_info.value.status_code == 400
    assert "either month or startMonth/endMonth" in exc_info.value.detail


def test_normalize_filter_values_splits_commas_and_trims_tokens():
    values = crime_utils._normalize_filter_values([" burglary , robbery ", "vehicle crime"], "crimeType")

    assert values == ["burglary", "robbery", "vehicle crime"]


def test_optional_bbox_requires_all_values_together():
    with pytest.raises(HTTPException) as exc_info:
        crime_utils._optional_bbox(-1.6, 53.7, None, 53.9)

    assert exc_info.value.status_code == 400
    assert "must all be provided together" in exc_info.value.detail


def test_resolve_mode_auto_switches_by_zoom():
    assert crime_utils._resolve_mode("auto", 10) == "clusters"
    assert crime_utils._resolve_mode("auto", 14) == "points"
