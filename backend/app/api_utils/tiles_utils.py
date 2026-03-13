from datetime import datetime
from typing import Optional

from fastapi import HTTPException

from ..schemas.tiles_schemas import TileMonthFilter, TileProfile


MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
PBF_MEDIA_TYPE = "application/x-protobuf"
TILE_CACHE_CONTROL = "public, max-age=60"
TILE_EXTENT = 4096
TILE_BUFFER = 64
RISK_LENGTH_FLOOR_M = 100.0
CRIME_WEIGHT = 0.55
COLLISION_WEIGHT = 0.45
SLIGHT_CASUALTY_WEIGHT = 0.5
SERIOUS_CASUALTY_WEIGHT = 2.0
FATAL_CASUALTY_WEIGHT = 5.0
USER_REPORTED_CRIME_WEIGHT = 0.10
ANONYMOUS_USER_REPORT_WEIGHT = 0.5
REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25
USER_REPORTED_SIGNAL_CAP = 3.0


def _tile_profile(z: int) -> TileProfile:
    """Return zoom-specific road class and simplification settings for tile generation."""
    if z <= 8:
        return {
            "highways": ("motorway", "trunk", "primary"),
            "simplify_tolerance": 80,
        }
    if z <= 11:
        return {
            "highways": ("motorway", "trunk", "primary", "secondary", "tertiary"),
            "simplify_tolerance": 30,
        }
    if z <= 13:
        return {
            "highways": (
                "motorway",
                "trunk",
                "primary",
                "secondary",
                "tertiary",
                "residential",
                "unclassified",
                "service",
            ),
            "simplify_tolerance": 10,
        }
    return {
        "highways": None,
        "simplify_tolerance": 0,
    }


def _validate_tile_coordinates(z: int, x: int, y: int) -> None:
    """Validate Slippy Map tile coordinates for a given zoom level."""
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range for zoom level")


def _parse_month(month: str):
    """Parse a YYYY-MM month string into a date."""
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format") from exc


def _resolve_month_filter(
    month: Optional[str],
    startMonth: Optional[str],
    endMonth: Optional[str],
    includeRisk: bool,
) -> TileMonthFilter:
    """Build SQL month filter clauses and bind params for risk tile requests."""
    if month and (startMonth or endMonth):
        raise HTTPException(
            status_code=400,
            detail="Use either month or startMonth/endMonth, not both",
        )

    if startMonth or endMonth:
        if not (startMonth and endMonth):
            raise HTTPException(
                status_code=400,
                detail="startMonth and endMonth must be provided together",
            )

        start_month_date = _parse_month(startMonth)
        end_month_date = _parse_month(endMonth)
        if start_month_date > end_month_date:
            raise HTTPException(
                status_code=400,
                detail="startMonth must be less than or equal to endMonth",
            )

        return TileMonthFilter(
            clause="c.month BETWEEN :start_month_date AND :end_month_date",
            params={
                "start_month_date": start_month_date,
                "end_month_date": end_month_date,
            },
        )

    if month:
        return TileMonthFilter(
            clause="c.month = :month_date",
            params={"month_date": _parse_month(month)},
        )

    if includeRisk:
        raise HTTPException(
            status_code=400,
            detail="month or startMonth/endMonth is required when includeRisk=true",
        )

    return TileMonthFilter(clause=None, params={})


def _build_highway_filter_clause(highways) -> str:
    """Render the static SQL clause for filtering by highway class at low zooms."""
    if not highways:
        return ""

    quoted = ", ".join(f"'{highway}'" for highway in highways)
    return f"AND rs.highway IN ({quoted})"


def _build_geom_expression(simplify_tolerance: int) -> str:
    """Return the road geometry expression with optional simplification."""
    if simplify_tolerance <= 0:
        return "rs.geom"
    return f"ST_Simplify(rs.geom, {simplify_tolerance})"
