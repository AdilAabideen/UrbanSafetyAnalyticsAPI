from collections import Counter

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError

from .collission_utils import (
    _bind_collision_filter_params,
    _collision_analytics_filters,
    _collision_analytics_response_filters,
    _collision_cluster_feature,
    _collision_filter_values,
    _collision_filters_meta,
    _collision_incident_item,
    _collision_map_feature,
    _collision_map_filters,
    _collision_next_cursor,
    _collision_time_value,
    _parse_collision_cursor,
    _sorted_count_items,
)
from .crime_utils import _bbox_meta, _cluster_grid_size, _parse_json, _resolve_from_to_filter, _shift_month, _where_sql
from ..schemas.colissions_schemas import CollisionMapResponse


def _execute(db, query, params):
    """Execute a collisions analytics query and normalize database failures."""
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


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
