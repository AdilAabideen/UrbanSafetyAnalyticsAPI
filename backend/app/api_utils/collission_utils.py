from collections import Counter
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import bindparam, text

from .crime_utils import (
    _bbox_meta,
    _cluster_grid_size,
    _default_limit,
    _execute,
    _normalize_filter_values,
    _optional_bbox,
    _parse_json,
    _parse_month,
    _required_bbox,
    _resolve_from_to_filter,
    _resolve_mode,
    _resolve_month_filter,
    _shift_month,
    _where_sql,
)
from ..schemas.colissions_schemas import (
    CollisionAnalyticsFilters,
    CollisionIncidentItem,
    CollisionMapResponse,
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


def _collision_snapshot(
    range_filter,
    bbox,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
    db,
    page_size=5000,
):
    """Build an in-memory collision analytics snapshot using paged DB reads."""
    where_clauses, query_params = _collision_analytics_filters(
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )

    monthly_counts = {}
    if range_filter["params"].get("from_month_date") and range_filter["params"].get("to_month_date"):
        month_date = range_filter["params"]["from_month_date"]
        end_month_date = range_filter["params"]["to_month_date"]
        while month_date <= end_month_date:
            monthly_counts[month_date.strftime("%Y-%m")] = 0
            month_date = _shift_month(month_date, 1)

    severity_counts = Counter()
    road_type_counts = Counter()
    casualty_severity_counts = Counter()
    weather_condition_counts = Counter()
    light_condition_counts = Counter()
    unique_lsoas = set()
    total_collisions = 0
    total_casualties = 0
    collisions_with_casualties = 0
    cursor_data = None

    while True:
        cursor_clause = ""
        params = dict(query_params)
        params["row_limit"] = page_size + 1

        if cursor_data:
            cursor_clause = """
            AND (
                ce.month < :cursor_month
                OR (ce.month = :cursor_month AND ce.collision_index < :cursor_collision_index)
            )
            """
            params.update(cursor_data)

        query = text(
            f"""
            /* collisions_analytics_snapshot */
            SELECT
                ce.collision_index,
                ce.month AS month_date,
                COALESCE(NULLIF(ce.collision_severity_label, ''), 'unknown') AS collision_severity,
                COALESCE(NULLIF(ce.road_type_label, ''), 'unknown') AS road_type,
                COALESCE(NULLIF(ce.weather_conditions_label, ''), 'unknown') AS weather_condition,
                COALESCE(NULLIF(ce.light_conditions_label, ''), 'unknown') AS light_condition,
                NULLIF(ce.lsoa_of_accident_location, '') AS lsoa_code,
                COALESCE(ce.number_of_casualties, 0) AS number_of_casualties,
                ce.casualty_severity_counts
            FROM collision_events ce
            {_where_sql(where_clauses)}
            {cursor_clause}
            ORDER BY ce.month DESC, ce.collision_index DESC
            LIMIT :row_limit
            """
        )
        query = _bind_collision_filter_params(
            query,
            severities,
            road_types,
            lsoa_codes,
            weather_conditions,
            light_conditions,
            road_surface_conditions,
        )

        rows = _execute(db, query, params).mappings().all()
        if not rows:
            break

        page_rows = rows[:page_size]
        for row in page_rows:
            month_label = row["month_date"].strftime("%Y-%m")
            monthly_counts[month_label] = monthly_counts.get(month_label, 0) + 1

            total_collisions += 1
            total_casualties += row["number_of_casualties"] or 0
            if (row["number_of_casualties"] or 0) > 0:
                collisions_with_casualties += 1

            severity_counts[row["collision_severity"]] += 1
            road_type_counts[row["road_type"]] += 1
            weather_condition_counts[row["weather_condition"]] += 1
            light_condition_counts[row["light_condition"]] += 1

            if row["lsoa_code"] is not None:
                unique_lsoas.add(row["lsoa_code"])

            for severity_label, count in (_parse_json(row["casualty_severity_counts"]) or {}).items():
                casualty_severity_counts[severity_label] += int(count)

        if len(rows) <= page_size:
            break

        last_row = page_rows[-1]
        cursor_data = {
            "cursor_month": last_row["month_date"],
            "cursor_collision_index": last_row["collision_index"],
        }

    return {
        "total_collisions": total_collisions,
        "total_casualties": total_casualties,
        "unique_lsoas": len(unique_lsoas),
        "collisions_with_casualties": collisions_with_casualties,
        "severity_counts": dict(severity_counts),
        "road_type_counts": dict(road_type_counts),
        "casualty_severity_counts": dict(casualty_severity_counts),
        "weather_condition_counts": dict(weather_condition_counts),
        "light_condition_counts": dict(light_condition_counts),
        "monthly_counts": monthly_counts,
    }


def _collision_points_payload(
    db,
    month_filter,
    bbox,
    zoom,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
    limit,
    cursor_data,
) -> CollisionMapResponse:
    """Build point-mode GeoJSON payload for collision map endpoint."""
    where_clauses, query_params = _collision_map_filters(
        month_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )
    query_params["row_limit"] = limit + 1

    cursor_clause = ""
    if cursor_data:
        cursor_clause = """
        AND (
            ce.month < :cursor_month
            OR (ce.month = :cursor_month AND ce.collision_index < :cursor_collision_index)
        )
        """
        query_params.update(cursor_data)

    query = text(
        f"""
        /* collisions_map_points */
        SELECT
            ce.collision_index,
            to_char(ce.month, 'YYYY-MM') AS month_label,
            to_char(ce.collision_date, 'YYYY-MM-DD') AS collision_date_label,
            ce.collision_time,
            COALESCE(NULLIF(ce.collision_severity_label, ''), 'unknown') AS collision_severity,
            COALESCE(NULLIF(ce.road_type_label, ''), 'unknown') AS road_type,
            COALESCE(NULLIF(ce.speed_limit_label, ''), 'unknown') AS speed_limit,
            COALESCE(NULLIF(ce.light_conditions_label, ''), 'unknown') AS light_conditions,
            COALESCE(NULLIF(ce.weather_conditions_label, ''), 'unknown') AS weather_conditions,
            COALESCE(NULLIF(ce.road_surface_conditions_label, ''), 'unknown') AS road_surface_conditions,
            COALESCE(NULLIF(ce.lsoa_of_accident_location, ''), 'unknown') AS lsoa_code,
            COALESCE(ce.number_of_vehicles, 0) AS number_of_vehicles,
            COALESCE(ce.number_of_casualties, 0) AS number_of_casualties,
            ST_AsGeoJSON(ce.geom) AS geometry
        FROM collision_events ce
        WHERE {" AND ".join(where_clauses)}
        {cursor_clause}
        ORDER BY ce.month DESC, ce.collision_index DESC
        LIMIT :row_limit
        """
    )
    query = _bind_collision_filter_params(
        query,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )

    rows = _execute(db, query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "type": "FeatureCollection",
        "features": [_collision_map_feature(row) for row in page_rows],
        "meta": {
            "mode": "points",
            "zoom": zoom,
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": _collision_next_cursor(rows, limit),
            "filters": _collision_filters_meta(
                month_filter["month"],
                month_filter["startMonth"],
                month_filter["endMonth"],
                severities,
                road_types,
                lsoa_codes,
                weather_conditions,
                light_conditions,
                road_surface_conditions,
            ),
            "bbox": _bbox_meta(
                bbox["min_lon"],
                bbox["min_lat"],
                bbox["max_lon"],
                bbox["max_lat"],
            ),
        },
    }


def _collision_clusters_payload(
    db,
    month_filter,
    bbox,
    zoom,
    severities,
    road_types,
    lsoa_codes,
    weather_conditions,
    light_conditions,
    road_surface_conditions,
    limit,
) -> CollisionMapResponse:
    """Build cluster-mode GeoJSON payload for collision map endpoint."""
    where_clauses, query_params = _collision_map_filters(
        month_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )
    cell_width, cell_height = _cluster_grid_size(bbox, zoom)
    query_params.update(
        {
            "row_limit": limit + 1,
            "zoom": zoom,
            "cell_width": cell_width,
            "cell_height": cell_height,
        }
    )

    query = text(
        f"""
        /* collisions_map_clusters */
        WITH filtered AS (
            SELECT
                COALESCE(NULLIF(ce.collision_severity_label, ''), 'unknown') AS collision_severity,
                COALESCE(ce.number_of_casualties, 0) AS number_of_casualties,
                ce.longitude,
                ce.latitude
            FROM collision_events ce
            WHERE {" AND ".join(where_clauses)}
        ),
        bucketed AS (
            SELECT
                floor((longitude - :min_lon) / :cell_width)::bigint AS cell_x,
                floor((latitude - :min_lat) / :cell_height)::bigint AS cell_y,
                collision_severity,
                number_of_casualties,
                longitude,
                latitude
            FROM filtered
        ),
        cluster_geom AS (
            SELECT
                cell_x,
                cell_y,
                COUNT(*) AS point_count,
                SUM(number_of_casualties) AS total_casualties,
                json_build_object(
                    'type', 'Point',
                    'coordinates', json_build_array(AVG(longitude), AVG(latitude))
                ) AS geometry
            FROM bucketed
            GROUP BY cell_x, cell_y
        ),
        severity_counts AS (
            SELECT
                cell_x,
                cell_y,
                collision_severity,
                COUNT(*) AS severity_count
            FROM bucketed
            GROUP BY cell_x, cell_y, collision_severity
        ),
        ranked_severities AS (
            SELECT
                cell_x,
                cell_y,
                collision_severity,
                severity_count,
                ROW_NUMBER() OVER (
                    PARTITION BY cell_x, cell_y
                    ORDER BY severity_count DESC, collision_severity ASC
                ) AS rn
            FROM severity_counts
        ),
        cluster_severities AS (
            SELECT
                cell_x,
                cell_y,
                COALESCE(
                    jsonb_object_agg(collision_severity, severity_count) FILTER (WHERE rn <= 3),
                    '{{}}'::jsonb
                ) AS top_collision_severities
            FROM ranked_severities
            GROUP BY cell_x, cell_y
        )
        SELECT
            concat(:zoom, ':', cluster_geom.cell_x, ':', cluster_geom.cell_y) AS cluster_id,
            cluster_geom.point_count AS count,
            cluster_geom.total_casualties,
            cluster_geom.geometry,
            cluster_severities.top_collision_severities
        FROM cluster_geom
        JOIN cluster_severities
          ON cluster_severities.cell_x = cluster_geom.cell_x
         AND cluster_severities.cell_y = cluster_geom.cell_y
        ORDER BY cluster_geom.point_count DESC, cluster_geom.cell_y ASC, cluster_geom.cell_x ASC
        LIMIT :row_limit
        """
    )
    query = _bind_collision_filter_params(
        query,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    )

    rows = _execute(db, query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "type": "FeatureCollection",
        "features": [_collision_cluster_feature(row) for row in page_rows],
        "meta": {
            "mode": "clusters",
            "zoom": zoom,
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": None,
            "filters": _collision_filters_meta(
                month_filter["month"],
                month_filter["startMonth"],
                month_filter["endMonth"],
                severities,
                road_types,
                lsoa_codes,
                weather_conditions,
                light_conditions,
                road_surface_conditions,
            ),
            "bbox": _bbox_meta(
                bbox["min_lon"],
                bbox["min_lat"],
                bbox["max_lon"],
                bbox["max_lat"],
            ),
        },
    }


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
