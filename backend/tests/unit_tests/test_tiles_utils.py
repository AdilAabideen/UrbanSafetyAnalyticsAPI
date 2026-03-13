import pytest
from fastapi import HTTPException

from app.api_utils import tiles_utils
from tests.inmemory_db import InMemoryDB


def test_tile_profile_selects_expected_ranges():
    assert tiles_utils._tile_profile(7)["simplify_tolerance"] == 80
    assert tiles_utils._tile_profile(10)["simplify_tolerance"] == 30
    assert tiles_utils._tile_profile(13)["simplify_tolerance"] == 10
    assert tiles_utils._tile_profile(15)["highways"] is None


def test_validate_tile_coordinates_rejects_out_of_range_values():
    with pytest.raises(HTTPException) as exc_info:
        tiles_utils._validate_tile_coordinates(2, 5, 0)

    assert exc_info.value.status_code == 400
    assert "out of range" in exc_info.value.detail


def test_resolve_month_filter_supports_month_and_range():
    single = tiles_utils._resolve_month_filter("2025-01", None, None, includeRisk=True)
    assert single.clause == "c.month = :month_date"
    assert single.params["month_date"].strftime("%Y-%m") == "2025-01"

    ranged = tiles_utils._resolve_month_filter(None, "2025-01", "2025-03", includeRisk=True)
    assert ranged.clause == "c.month BETWEEN :start_month_date AND :end_month_date"
    assert ranged.params["start_month_date"].strftime("%Y-%m") == "2025-01"


def test_resolve_month_filter_requires_month_when_risk_enabled():
    with pytest.raises(HTTPException) as exc_info:
        tiles_utils._resolve_month_filter(None, None, None, includeRisk=True)

    assert exc_info.value.status_code == 400
    assert "required when includeRisk=true" in exc_info.value.detail


def test_build_highway_and_geom_helpers():
    assert tiles_utils._build_highway_filter_clause(None) == ""
    assert "primary" in tiles_utils._build_highway_filter_clause(("primary",))
    assert tiles_utils._build_geom_expression(0) == "rs.geom"
    assert "ST_Simplify" in tiles_utils._build_geom_expression(12)


def test_build_tile_bytes_returns_raw_bytes_for_non_risk_tiles():
    db = InMemoryDB(
        {
            "/* tiles_roads_only */": {
                "scalar": b"tile-bytes"
            }
        }
    )

    payload = tiles_utils._build_tile_bytes(
        z=10,
        x=500,
        y=340,
        month=None,
        startMonth=None,
        endMonth=None,
        crimeType=None,
        includeRisk=False,
        db=db,
    )

    assert payload == b"tile-bytes"
    assert any("/* tiles_roads_only */" in sql for sql in db.executed_sql)


def test_build_tile_bytes_handles_memoryview_for_risk_tiles():
    def _risk_handler(params):
        assert params["crime_type"] == "burglary"
        assert params["month_date"].strftime("%Y-%m") == "2025-01"
        return {"scalar": memoryview(b"risk-bytes")}

    db = InMemoryDB({"/* tiles_roads_with_risk */": _risk_handler})

    payload = tiles_utils._build_tile_bytes(
        z=10,
        x=500,
        y=340,
        month="2025-01",
        startMonth=None,
        endMonth=None,
        crimeType="burglary",
        includeRisk=True,
        db=db,
    )

    assert payload == b"risk-bytes"
    assert any("/* tiles_roads_with_risk */" in sql for sql in db.executed_sql)
