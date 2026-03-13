from typing import Optional

from fastapi import HTTPException
from sqlalchemy import bindparam

from .crime_utils import (
    _bbox_meta,
    _default_limit,
    _normalize_filter_values,
    _optional_bbox,
    _parse_json,
    _parse_month,
    _required_bbox,
    _resolve_from_to_filter,
    _resolve_mode,
    _resolve_month_filter,
    _where_sql,
)
from ..schemas.colissions_schemas import (
    CollisionAnalyticsFilters,
    CollisionIncidentItem,
)


MAX_COLLISION_LIMIT = 10000


def _collision_time_value(value):
    """Normalize collision time values into HH:MM strings."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M")
    return str(value)


def _parse_collision_cursor(cursor):
    """Parse cursor text `YYYY-MM|collision_index` used by collisions pagination."""
    if cursor is None:
        return None

    try:
        month_key, collision_index = cursor.split("|", 1)
        month_value = _parse_month(month_key, "cursor month")
        if not collision_index.strip():
            raise ValueError("missing collision index")
        return {
            "cursor_month": month_value,
            "cursor_collision_index": collision_index.strip(),
        }
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail="cursor must be in YYYY-MM|collision_index format",
        ) from exc


def _collision_next_cursor(rows, limit):
    """Return pagination cursor for collision incident and map point responses."""
    if len(rows) <= limit or not rows[:limit]:
        return None

    last_row = rows[limit - 1]
    return f"{last_row['month_label']}|{last_row['collision_index']}"


def _bind_collision_filter_params(
    query,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
):
    """Bind expanding list parameters used by collision filters."""
    if severities:
        query = query.bindparams(bindparam("severities", expanding=True))
    if road_types:
        query = query.bindparams(bindparam("road_types", expanding=True))
    if lsoa_codes:
        query = query.bindparams(bindparam("lsoa_codes", expanding=True))
    if weather_conditions:
        query = query.bindparams(bindparam("weather_conditions", expanding=True))
    if light_conditions:
        query = query.bindparams(bindparam("light_conditions", expanding=True))
    if road_surface_conditions:
        query = query.bindparams(bindparam("road_surface_conditions", expanding=True))
    return query


def _collision_filters_meta(
    month,
    start_month,
    end_month,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
):
    """Build map filter metadata payload for collision responses."""
    return {
        "month": month,
        "startMonth": start_month,
        "endMonth": end_month,
        "collisionSeverity": severities,
        "roadType": road_types,
        "lsoaCode": lsoa_codes,
        "weatherCondition": weather_conditions,
        "lightCondition": light_conditions,
        "roadSurfaceCondition": road_surface_conditions,
    }


def _collision_incident_item(row) -> CollisionIncidentItem:
    """Serialize a collision incident row for API output."""
    return {
        "collision_index": row["collision_index"],
        "month": row["month_label"],
        "date": row["collision_date_label"],
        "time": _collision_time_value(row["collision_time"]),
        "collision_severity": row["collision_severity"],
        "road_type": row["road_type"],
        "speed_limit": row["speed_limit"],
        "light_conditions": row["light_conditions"],
        "weather_conditions": row["weather_conditions"],
        "road_surface_conditions": row["road_surface_conditions"],
        "number_of_vehicles": row["number_of_vehicles"],
        "number_of_casualties": row["number_of_casualties"],
        "lsoa_code": row["lsoa_code"],
        "lon": row["longitude"],
        "lat": row["latitude"],
    }


def _collision_map_feature(row):
    """Convert a collision point query row into a GeoJSON feature."""
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "collision_index": row["collision_index"],
            "month": row["month_label"],
            "date": row["collision_date_label"],
            "time": _collision_time_value(row["collision_time"]),
            "collision_severity": row["collision_severity"],
            "road_type": row["road_type"],
            "speed_limit": row["speed_limit"],
            "light_conditions": row["light_conditions"],
            "weather_conditions": row["weather_conditions"],
            "road_surface_conditions": row["road_surface_conditions"],
            "number_of_vehicles": row["number_of_vehicles"],
            "number_of_casualties": row["number_of_casualties"],
            "lsoa_code": row["lsoa_code"],
        },
    }


def _collision_cluster_feature(row):
    """Convert a cluster aggregation row into a GeoJSON feature."""
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "cluster": True,
            "cluster_id": row["cluster_id"],
            "count": row["count"],
            "total_casualties": row["total_casualties"],
            "top_collision_severities": _parse_json(row["top_collision_severities"]) or {},
        },
    }


def _sorted_count_items(counts, field_name):
    """Sort counter values descending for analytics top lists."""
    return [
        {field_name: label, "count": count}
        for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _collision_filter_values(
    collision_severity,
    road_type,
    lsoa_code,
    weather_condition,
    light_condition,
    road_surface_condition,
):
    """Normalize all collision filter lists from query params."""
    return (
        _normalize_filter_values(collision_severity, "collisionSeverity"),
        _normalize_filter_values(road_type, "roadType"),
        _normalize_filter_values(lsoa_code, "lsoaCode"),
        _normalize_filter_values(weather_condition, "weatherCondition"),
        _normalize_filter_values(light_condition, "lightCondition"),
        _normalize_filter_values(road_surface_condition, "roadSurfaceCondition"),
    )


def _collision_analytics_request_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    collision_severity,
    road_type,
    lsoa_code,
    weather_condition,
    light_condition,
    road_surface_condition,
    required_range,
):
    """Normalize shared analytics filters used by summary and timeseries endpoints."""
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    severities, road_types, lsoa_codes, weather_conditions, light_conditions, road_surface_conditions = _collision_filter_values(
        collision_severity,
        road_type,
        lsoa_code,
        weather_condition,
        light_condition,
        road_surface_condition,
    )
    return (
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )


def _collision_map_filters(
    month_filter,
    bbox,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
):
    """Build WHERE clauses and params for collision map queries."""
    where_clauses = [
        "ce.geom IS NOT NULL",
        "ce.longitude IS NOT NULL",
        "ce.latitude IS NOT NULL",
        "ce.longitude BETWEEN :min_lon AND :max_lon",
        "ce.latitude BETWEEN :min_lat AND :max_lat",
        "ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)",
    ]
    query_params = dict(bbox)

    if month_filter["clause"] is not None:
        where_clauses.append(month_filter["clause"])
        query_params.update(month_filter["params"])

    if severities:
        where_clauses.append(
            "COALESCE(NULLIF(ce.collision_severity_label, ''), 'unknown') IN :severities"
        )
        query_params["severities"] = severities

    if road_types:
        where_clauses.append(
            "COALESCE(NULLIF(ce.road_type_label, ''), 'unknown') IN :road_types"
        )
        query_params["road_types"] = road_types

    if lsoa_codes:
        where_clauses.append(
            "COALESCE(NULLIF(ce.lsoa_of_accident_location, ''), 'unknown') IN :lsoa_codes"
        )
        query_params["lsoa_codes"] = lsoa_codes

    if weather_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.weather_conditions_label, ''), 'unknown') IN :weather_conditions"
        )
        query_params["weather_conditions"] = weather_conditions

    if light_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.light_conditions_label, ''), 'unknown') IN :light_conditions"
        )
        query_params["light_conditions"] = light_conditions

    if road_surface_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.road_surface_conditions_label, ''), 'unknown') IN :road_surface_conditions"
        )
        query_params["road_surface_conditions"] = road_surface_conditions

    return where_clauses, query_params


def _collision_analytics_filters(
    range_filter,
    bbox,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
):
    """Build WHERE clauses and params for collision analytics queries."""
    where_clauses = []
    query_params = {}

    if range_filter["clause"] is not None:
        where_clauses.append(range_filter["clause"])
        query_params.update(range_filter["params"])

    if bbox:
        where_clauses.extend(
            [
                "ce.longitude IS NOT NULL",
                "ce.latitude IS NOT NULL",
                "ce.longitude BETWEEN :min_lon AND :max_lon",
                "ce.latitude BETWEEN :min_lat AND :max_lat",
                "ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)",
            ]
        )
        query_params.update(bbox)

    if severities:
        where_clauses.append(
            "COALESCE(NULLIF(ce.collision_severity_label, ''), 'unknown') IN :severities"
        )
        query_params["severities"] = severities

    if road_types:
        where_clauses.append(
            "COALESCE(NULLIF(ce.road_type_label, ''), 'unknown') IN :road_types"
        )
        query_params["road_types"] = road_types

    if lsoa_codes:
        where_clauses.append(
            "COALESCE(NULLIF(ce.lsoa_of_accident_location, ''), 'unknown') IN :lsoa_codes"
        )
        query_params["lsoa_codes"] = lsoa_codes

    if weather_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.weather_conditions_label, ''), 'unknown') IN :weather_conditions"
        )
        query_params["weather_conditions"] = weather_conditions

    if light_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.light_conditions_label, ''), 'unknown') IN :light_conditions"
        )
        query_params["light_conditions"] = light_conditions

    if road_surface_conditions:
        where_clauses.append(
            "COALESCE(NULLIF(ce.road_surface_conditions_label, ''), 'unknown') IN :road_surface_conditions"
        )
        query_params["road_surface_conditions"] = road_surface_conditions

    return where_clauses, query_params


def _collision_analytics_response_filters(
    range_filter,
    bbox,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
) -> CollisionAnalyticsFilters:
    """Build analytics filter metadata shape used in responses."""
    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "collisionSeverity": severities,
        "roadType": road_types,
        "lsoaCode": lsoa_codes,
        "weatherCondition": weather_conditions,
        "lightCondition": light_conditions,
        "roadSurfaceCondition": road_surface_conditions,
        "bbox": None
        if not bbox
        else _bbox_meta(
            bbox["min_lon"],
            bbox["min_lat"],
            bbox["max_lon"],
            bbox["max_lat"],
        ),
    }
