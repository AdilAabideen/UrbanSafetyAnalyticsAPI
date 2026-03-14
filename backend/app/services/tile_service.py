from datetime import date, datetime
from typing import Optional

from ..api_utils.tiles_repository import roads_only_tile_query as _roads_only_tile_query
from ..api_utils.tiles_repository import roads_with_risk_tile_query
from ..db import execute as _execute
from ..errors import ValidationError
from ..schemas.tiles_schemas import (
    ANONYMOUS_USER_REPORT_WEIGHT,
    COLLISION_WEIGHT,
    CRIME_WEIGHT,
    FATAL_CASUALTY_WEIGHT,
    REPEAT_AUTHENTICATED_REPORT_WEIGHT,
    RISK_LENGTH_FLOOR_M,
    SERIOUS_CASUALTY_WEIGHT,
    SLIGHT_CASUALTY_WEIGHT,
    TILE_BUFFER,
    TILE_EXTENT,
    USER_REPORTED_CRIME_WEIGHT,
    USER_REPORTED_SIGNAL_CAP,
    TileQueryParams,
    TileMonthFilter
)

# Service to get the road tiles in MVT format.
def get_road_tiles_mvt(
    z: int,
    x: int,
    y: int,
    month: Optional[str],
    startMonth: Optional[str],
    endMonth: Optional[str],
    crimeType: Optional[str],
    includeRisk: bool,
    db,
) -> bytes:
    """Build vector-tile bytes for roads with optional risk overlays."""
    validate_tile_coordinates(z, x, y)

    query_params: TileQueryParams = {
        "z": z,
        "x": x,
        "y": y,
        "extent": TILE_EXTENT,
        "buffer": TILE_BUFFER,
        "risk_length_floor_m": RISK_LENGTH_FLOOR_M,
        "crime_weight": CRIME_WEIGHT,
        "collision_weight": COLLISION_WEIGHT,
        "slight_casualty_weight": SLIGHT_CASUALTY_WEIGHT,
        "serious_casualty_weight": SERIOUS_CASUALTY_WEIGHT,
        "fatal_casualty_weight": FATAL_CASUALTY_WEIGHT,
        "user_report_weight": USER_REPORTED_CRIME_WEIGHT,
        "anonymous_report_weight": ANONYMOUS_USER_REPORT_WEIGHT,
        "repeat_authenticated_report_weight": REPEAT_AUTHENTICATED_REPORT_WEIGHT,
        "user_report_signal_cap": USER_REPORTED_SIGNAL_CAP,
    }

    if includeRisk:
        month_filter = resolve_month_filter(
            month=month,
            startMonth=startMonth,
            endMonth=endMonth,
            includeRisk=includeRisk,
        )
        query_params.update(month_filter.params)
        if crimeType:
            query_params["crime_type"] = crimeType

        tile_query = roads_with_risk_tile_query(
            z=z,
            month_filter_clause=month_filter.clause or "",
            include_crime_type_filter=bool(crimeType),
        )
    else:
        tile_query = _roads_only_tile_query(z)

    tile = _execute(db, tile_query, query_params).scalar_one()
    if isinstance(tile, memoryview):
        return tile.tobytes()
    return tile or b""

# Validate the tile coordinates for the given zoom level.
def validate_tile_coordinates(
    z: int,
    x: int,
    y: int,
) -> None:
    """Validate Slippy Map tile coordinates for a given zoom level."""
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise ValidationError(
            error="INVALID_TILE_COORDINATES",
            message="Tile coordinates out of range for zoom level",
            details={"z": z, "x": x, "y": y},
        )


# Resolve the month filter for the risk tile requests.
def resolve_month_filter(
    month: Optional[str],
    startMonth: Optional[str],
    endMonth: Optional[str],
    includeRisk: bool,
) -> TileMonthFilter:
    """Build SQL month filter clauses and bind params for risk tile requests."""

    # Either Pass a Month or a Range
    if month and (startMonth or endMonth):
        raise ValidationError(
            error="INVALID_MONTH_FILTER",
            message="Use either month or startMonth/endMonth, not both",
            details={"field": "month/startMonth/endMonth"},
        )

    # If a Range is Passed, Ensure Both Start and End Months are Passed
    if startMonth or endMonth:
        if not (startMonth and endMonth):
            raise ValidationError(
                error="INVALID_MONTH_FILTER",
                message="startMonth and endMonth must be provided together",
                details={"field": "startMonth/endMonth"},
            )

        start_month_date = parse_month(startMonth, "startMonth")
        end_month_date = parse_month(endMonth, "endMonth")
        if start_month_date > end_month_date:
            raise ValidationError(
                error="INVALID_MONTH_RANGE",
                message="startMonth must be less than or equal to endMonth",
                details={"field": "startMonth/endMonth"},
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
            params={"month_date": parse_month(month, "month")},
        )

    if includeRisk:
        raise ValidationError(
            error="MISSING_MONTH_FILTER",
            message="month or startMonth/endMonth is required when includeRisk=true",
            details={"field": "includeRisk"},
        )

    return TileMonthFilter(clause=None, params={})

# Parse the month string into a date.
def parse_month(month: str, field_name: str) -> date:
    """Parse a YYYY-MM month string into a date."""
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise ValidationError(
            error="INVALID_MONTH_FORMAT",
            message=f"{field_name} must be in YYYY-MM format",
            details={"field": field_name, "value": month},
        ) from exc
