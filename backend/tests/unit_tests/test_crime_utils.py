from datetime import date

import pytest
from fastapi import HTTPException

from app.api_utils import crime_utils
from tests.inmemory_db import InMemoryDB


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


def test_analytics_snapshot_aggregates_and_caches_results():
    crime_utils._analytics_snapshot_cache.clear()
    crime_utils._analytics_snapshot_inflight.clear()

    db = InMemoryDB(
        {
            "/* crimes_analytics_snapshot */": [
                {
                    "rows": [
                        {
                            "id": 3,
                            "month_date": date(2025, 2, 1),
                            "crime_type": "burglary",
                            "raw_outcome": "Under investigation",
                            "outcome": "Under investigation",
                            "lsoa_code": "LSOA-1",
                            "lsoa_name": "Leeds Central",
                        },
                        {
                            "id": 2,
                            "month_date": date(2025, 1, 1),
                            "crime_type": "robbery",
                            "raw_outcome": None,
                            "outcome": "unknown",
                            "lsoa_code": None,
                            "lsoa_name": "Leeds North",
                        },
                        {
                            "id": 1,
                            "month_date": date(2025, 1, 1),
                            "crime_type": "burglary",
                            "raw_outcome": "Resolved",
                            "outcome": "Resolved",
                            "lsoa_code": "LSOA-1",
                            "lsoa_name": "Leeds Central",
                        },
                    ]
                },
                {
                    "rows": [
                        {
                            "id": 1,
                            "month_date": date(2025, 1, 1),
                            "crime_type": "burglary",
                            "raw_outcome": "Resolved",
                            "outcome": "Resolved",
                            "lsoa_code": "LSOA-1",
                            "lsoa_name": "Leeds Central",
                        }
                    ]
                },
            ]
        }
    )

    range_filter = crime_utils._resolve_from_to_filter("2025-01", "2025-02", required=True)

    snapshot = crime_utils._analytics_snapshot(
        range_filter,
        bbox=None,
        crime_types=None,
        last_outcome_categories=None,
        lsoa_names=None,
        db=db,
        page_size=2,
    )

    assert snapshot["total_crimes"] == 3
    assert snapshot["unique_lsoas"] == 2
    assert snapshot["unique_crime_types"] == 2
    assert snapshot["crimes_with_outcomes"] == 2
    assert snapshot["crime_type_counts"] == {"burglary": 2, "robbery": 1}
    assert snapshot["outcome_counts"] == {
        "Under investigation": 1,
        "unknown": 1,
        "Resolved": 1,
    }
    assert snapshot["monthly_counts"] == {
        "2025-01": 2,
        "2025-02": 1,
    }

    second_snapshot = crime_utils._analytics_snapshot(
        range_filter,
        bbox=None,
        crime_types=None,
        last_outcome_categories=None,
        lsoa_names=None,
        db=db,
        page_size=2,
    )

    assert second_snapshot == snapshot
    assert len(db.executed_sql) == 2
