from datetime import date, datetime
from typing import Optional

from ..api_utils.tiles_repository import roads_only_tile_query as _roads_only_tile_query
from ..api_utils.tiles_repository import roads_with_risk_tile_query
from ..db import execute as _execute
from ..errors import ValidationError
from ..schemas.tiles_schemas import (
    ADJACENT_SEGMENT_DISTANCE_M,
    COLLISION_COMPONENT_WEIGHT,
    COLLISION_DECAY_LAMBDA,
    CRIME_ALPHA,
    CRIME_COMPONENT_WEIGHT,
    CRIME_DECAY_LAMBDA,
    CRIME_HARM_WEIGHTS,
    CURVE_FACTOR_CAP,
    CURVE_WEIGHT,
    DEFAULT_CRIME_HARM_WEIGHT,
    FATAL_CASUALTY_WEIGHT,
    JUNCTION_DEGREE_WEIGHT,
    JUNCTION_DISTANCE_M,
    JUNCTION_FACTOR_CAP,
    RISK_LENGTH_FLOOR_M,
    ROAD_CLASS_DEFAULT_FACTOR,
    ROAD_CLASS_HIGH_FACTOR,
    ROAD_CLASS_MEDIUM_FACTOR,
    RISK_BAND_ORANGE_THRESHOLD,
    RISK_BAND_RED_THRESHOLD,
    SERIOUS_CASUALTY_WEIGHT,
    SLIGHT_CASUALTY_WEIGHT,
    TILE_BUFFER,
    TILE_EXTENT,
    USER_REPORT_ANONYMOUS_WEIGHT,
    USER_REPORT_CLUSTER_CAP,
    USER_REPORT_COMPONENT_WEIGHT,
    USER_REPORT_DECAY_LAMBDA,
    USER_REPORT_DISTINCT_AUTH_WEIGHT,
    USER_REPORT_REPEAT_WEIGHT,
    TileMonthFilter,
    TileQueryParams,
)


# Service to get the road tiles in MVT format.
def get_road_tiles_mvt(
    z: int,
    x: int,
    y: int,
    startMonth: Optional[str],
    endMonth: Optional[str],
    crime: Optional[bool],
    collisions: Optional[bool],
    userReportedEvents: Optional[bool],
    db,
) -> bytes:
    """Build vector-tile bytes for roads with optional risk overlays."""
    validate_tile_coordinates(z, x, y)

    # Determine if the crime, collisions, and user reports are included.
    include_crime = bool(crime)
    include_collisions = bool(collisions)
    include_user_reports = bool(userReportedEvents)
    include_any_risk = include_crime or include_collisions or include_user_reports

    # Build the query parameters.
    query_params: TileQueryParams = {
        "z": z,
        "x": x,
        "y": y,
        "extent": TILE_EXTENT,
        "buffer": TILE_BUFFER,
        "risk_length_floor_m": RISK_LENGTH_FLOOR_M,
        "slight_casualty_weight": SLIGHT_CASUALTY_WEIGHT,
        "serious_casualty_weight": SERIOUS_CASUALTY_WEIGHT,
        "fatal_casualty_weight": FATAL_CASUALTY_WEIGHT,
        "include_crime": include_crime,
        "include_collisions": include_collisions,
        "include_user_reports": include_user_reports,
        "crime_alpha": CRIME_ALPHA,
        "crime_decay_lambda": CRIME_DECAY_LAMBDA,
        "collision_decay_lambda": COLLISION_DECAY_LAMBDA,
        "user_report_decay_lambda": USER_REPORT_DECAY_LAMBDA,
        "crime_component_weight": CRIME_COMPONENT_WEIGHT,
        "collision_component_weight": COLLISION_COMPONENT_WEIGHT,
        "user_report_component_weight": USER_REPORT_COMPONENT_WEIGHT,
        "junction_distance_m": JUNCTION_DISTANCE_M,
        "junction_degree_weight": JUNCTION_DEGREE_WEIGHT,
        "junction_factor_cap": JUNCTION_FACTOR_CAP,
        "curve_weight": CURVE_WEIGHT,
        "curve_factor_cap": CURVE_FACTOR_CAP,
        "adjacent_segment_distance_m": ADJACENT_SEGMENT_DISTANCE_M,
        "user_report_distinct_auth_weight": USER_REPORT_DISTINCT_AUTH_WEIGHT,
        "user_report_anonymous_weight": USER_REPORT_ANONYMOUS_WEIGHT,
        "user_report_repeat_weight": USER_REPORT_REPEAT_WEIGHT,
        "user_report_cluster_cap": USER_REPORT_CLUSTER_CAP,
        "road_class_high_factor": ROAD_CLASS_HIGH_FACTOR,
        "road_class_medium_factor": ROAD_CLASS_MEDIUM_FACTOR,
        "road_class_default_factor": ROAD_CLASS_DEFAULT_FACTOR,
        "harm_anti_social_behaviour": CRIME_HARM_WEIGHTS["Anti-social behaviour"],
        "harm_bicycle_theft": CRIME_HARM_WEIGHTS["Bicycle theft"],
        "harm_burglary": CRIME_HARM_WEIGHTS["Burglary"],
        "harm_criminal_damage_and_arson": CRIME_HARM_WEIGHTS["Criminal damage and arson"],
        "harm_drugs": CRIME_HARM_WEIGHTS["Drugs"],
        "harm_other_crime": CRIME_HARM_WEIGHTS["Other crime"],
        "harm_other_theft": CRIME_HARM_WEIGHTS["Other theft"],
        "harm_possession_of_weapons": CRIME_HARM_WEIGHTS["Possession of weapons"],
        "harm_public_order": CRIME_HARM_WEIGHTS["Public order"],
        "harm_robbery": CRIME_HARM_WEIGHTS["Robbery"],
        "harm_shoplifting": CRIME_HARM_WEIGHTS["Shoplifting"],
        "harm_theft_from_the_person": CRIME_HARM_WEIGHTS["Theft from the person"],
        "harm_vehicle_crime": CRIME_HARM_WEIGHTS["Vehicle crime"],
        "harm_violence_and_sexual_offences": CRIME_HARM_WEIGHTS["Violence and sexual offences"],
        "harm_default": DEFAULT_CRIME_HARM_WEIGHT,
        "risk_band_orange_threshold": RISK_BAND_ORANGE_THRESHOLD,
        "risk_band_red_threshold": RISK_BAND_RED_THRESHOLD,
    }

    # If any risk is included, build the month filter and query.
    if include_any_risk:
        month_filter = resolve_month_filter(
            startMonth=startMonth,
            endMonth=endMonth,
            require_window=True,
        )
        query_params.update(month_filter.params)

        # Build the query for the roads with risk.
        tile_query = roads_with_risk_tile_query(
            z=z,
        )
    else:
        # Build the query for the roads only.
        tile_query = _roads_only_tile_query(z)

    # Execute the query and return the tile.
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
    startMonth: Optional[str],
    endMonth: Optional[str],
    require_window: bool,
) -> TileMonthFilter:
    """Build SQL month filter bind params for risk tile requests."""

    # If no startMonth or endMonth is provided, raise a validation error.
    if not (startMonth or endMonth):
        if require_window:
            raise ValidationError(
                error="MISSING_MONTH_FILTER",
                message="startMonth and endMonth are required when risk toggles are enabled",
                details={"field": "startMonth/endMonth"},
            )
        return TileMonthFilter(clause=None, params={})

    # If only one of startMonth or endMonth is provided, raise a validation error.
    if not (startMonth and endMonth):
        raise ValidationError(
            error="INVALID_MONTH_FILTER",
            message="startMonth and endMonth must be provided together",
            details={"field": "startMonth/endMonth"},
        )

    # Parse the startMonth and endMonth into dates.
    start_month_date = parse_month(startMonth, "startMonth")
    end_month_date = parse_month(endMonth, "endMonth")

    # If the startMonth is greater than the endMonth, raise a validation error.
    if start_month_date > end_month_date:
        raise ValidationError(
            error="INVALID_MONTH_RANGE",
            message="startMonth must be less than or equal to endMonth",
            details={"field": "startMonth/endMonth"},
        )

    # Return the month filter.
    return TileMonthFilter(
        clause="BETWEEN :start_month_date AND :end_month_date",
        params={
            "start_month_date": start_month_date,
            "end_month_date": end_month_date,
        },
    )


# Parse the month string into a date.
def parse_month(month: str, field_name: str) -> date:
    """Parse a YYYY-MM month string into a date."""

    # Parse the month string into a date.
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        # If the month string is not in the correct format, raise a validation error.
        raise ValidationError(
            error="INVALID_MONTH_FORMAT",
            message=f"{field_name} must be in YYYY-MM format",
            details={"field": field_name, "value": month},
        ) from exc
