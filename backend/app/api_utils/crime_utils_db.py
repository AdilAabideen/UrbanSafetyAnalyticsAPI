import logging
from collections import Counter
from copy import deepcopy
from threading import Event, Lock
from time import monotonic

from sqlalchemy import bindparam, text
from sqlalchemy.exc import InternalError, OperationalError

from .crime_utils import (
    ANALYTICS_SNAPSHOT_TTL_SECONDS,
    _analytics_filters,
    _analytics_snapshot_cache_key,
    _bbox_meta,
    _bind_analytics_filter_params,
    _cluster_grid_size,
    _crime_filters_meta,
    _parse_json,
    _point_next_cursor,
    _shift_month,
    _where_sql,
)
from ..errors import DependencyError
from ..schemas.crime_schemas import CrimeMapResponse


logger = logging.getLogger(__name__)
_analytics_snapshot_cache = {}
_analytics_snapshot_inflight = {}
_analytics_snapshot_lock = Lock()


def _execute(db, query, params=None):
    """Execute SQL `query` with DB error translation."""
    try:
        return db.execute(query, params or {})
    except (InternalError, OperationalError) as exc:
        logger.warning("Database error during crime analytics query execution", exc_info=exc)
        db.rollback()
        raise DependencyError(
            message="Database unavailable. Postgres query execution failed; inspect the database container and server logs."
        ) from exc


def _map_filters(month_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    """Return the WHERE clauses shared by both map queries."""
    where_clauses = [
        "ce.geom IS NOT NULL",
        "ce.lon IS NOT NULL",
        "ce.lat IS NOT NULL",
        "ce.lon BETWEEN :min_lon AND :max_lon",
        "ce.lat BETWEEN :min_lat AND :max_lat",
        "ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)",
    ]
    query_params = dict(bbox)

    if month_filter["clause"] is not None:
        where_clauses.append(month_filter["clause"])
        query_params.update(month_filter["params"])

    if crime_types:
        where_clauses.append("COALESCE(NULLIF(ce.crime_type, ''), 'unknown') IN :crime_types")
        query_params["crime_types"] = crime_types

    if last_outcome_categories:
        where_clauses.append(
            "COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') IN :last_outcome_categories"
        )
        query_params["last_outcome_categories"] = last_outcome_categories

    if lsoa_names:
        where_clauses.append("COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown') IN :lsoa_names")
        query_params["lsoa_names"] = lsoa_names

    return where_clauses, query_params


def _crime_map_feature(row):
    """Build a GeoJSON point feature from a query row."""
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "id": row["id"],
            "crime_id": row["crime_id"],
            "month": row["month_label"],
            "crime_type": row["crime_type"],
            "last_outcome_category": row["last_outcome_category"],
            "location_text": row["location_text"],
            "reported_by": row["reported_by"],
            "falls_within": row["falls_within"],
            "lsoa_code": row["lsoa_code"],
            "lsoa_name": row["lsoa_name"],
        },
    }


def _cluster_feature(row):
    """Build a GeoJSON cluster feature from a query row."""
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "cluster": True,
            "cluster_id": row["cluster_id"],
            "count": row["count"],
            "top_crime_types": _parse_json(row["top_crime_types"]) or {},
        },
    }


def _crime_points_payload(
    db,
    month_filter,
    bbox,
    zoom,
    crime_types,
    last_outcome_categories,
    lsoa_names,
    limit,
    cursor_data,
) -> CrimeMapResponse:
    """Return the feature collection for the point mode tiles."""
    where_clauses, query_params = _map_filters(
        month_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )
    query_params["row_limit"] = limit + 1

    cursor_clause = ""
    if cursor_data:
        cursor_clause = """
        AND (
            ce.month < :cursor_month
            OR (ce.month = :cursor_month AND ce.id < :cursor_id)
        )
        """
        query_params.update(cursor_data)

    point_query = text(
        f"""
        /* crimes_map_points */
        SELECT
            ce.id,
            ce.crime_id,
            to_char(ce.month, 'YYYY-MM') AS month_label,
            COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
            ce.last_outcome_category,
            ce.location_text,
            ce.reported_by,
            ce.falls_within,
            ce.lsoa_code,
            ce.lsoa_name,
            ST_AsGeoJSON(ce.geom) AS geometry
        FROM crime_events ce
        WHERE {" AND ".join(where_clauses)}
        {cursor_clause}
        ORDER BY ce.month DESC, ce.id DESC
        LIMIT :row_limit
        """
    )

    if crime_types:
        point_query = point_query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories:
        point_query = point_query.bindparams(bindparam("last_outcome_categories", expanding=True))
    if lsoa_names:
        point_query = point_query.bindparams(bindparam("lsoa_names", expanding=True))

    rows = _execute(db, point_query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "type": "FeatureCollection",
        "features": [_crime_map_feature(row) for row in page_rows],
        "meta": {
            "mode": "points",
            "zoom": zoom,
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": _point_next_cursor(rows, limit),
            "filters": _crime_filters_meta(
                month_filter["month"],
                month_filter["startMonth"],
                month_filter["endMonth"],
                crime_types,
                last_outcome_categories,
                lsoa_names,
            ),
            "bbox": _bbox_meta(
                bbox["min_lon"],
                bbox["min_lat"],
                bbox["max_lon"],
                bbox["max_lat"],
            ),
        },
    }


def _crime_clusters_payload(
    db,
    month_filter,
    bbox,
    zoom,
    crime_types,
    last_outcome_categories,
    lsoa_names,
    limit,
) -> CrimeMapResponse:
    """Return the feature collection for cluster-mode tiles."""
    where_clauses, query_params = _map_filters(
        month_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )
    cell_width, cell_height = _cluster_grid_size(bbox, zoom)
    query_params["row_limit"] = limit + 1
    query_params["zoom"] = zoom
    query_params["cell_width"] = cell_width
    query_params["cell_height"] = cell_height

    cluster_query = text(
        f"""
        /* crimes_map_clusters */
        WITH filtered AS (
            SELECT
                COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
                ce.lon,
                ce.lat
            FROM crime_events ce
            WHERE {" AND ".join(where_clauses)}
        ),
        bucketed AS (
            SELECT
                floor((lon - :min_lon) / :cell_width)::bigint AS cell_x,
                floor((lat - :min_lat) / :cell_height)::bigint AS cell_y,
                crime_type,
                lon,
                lat
            FROM filtered
        ),
        cluster_geom AS (
            SELECT
                cell_x,
                cell_y,
                COUNT(*) AS point_count,
                json_build_object(
                    'type', 'Point',
                    'coordinates', json_build_array(AVG(lon), AVG(lat))
                ) AS geometry
            FROM bucketed
            GROUP BY cell_x, cell_y
        ),
        type_counts AS (
            SELECT
                cell_x,
                cell_y,
                crime_type,
                COUNT(*) AS crime_count
            FROM bucketed
            GROUP BY cell_x, cell_y, crime_type
        ),
        ranked_types AS (
            SELECT
                cell_x,
                cell_y,
                crime_type,
                crime_count,
                ROW_NUMBER() OVER (
                    PARTITION BY cell_x, cell_y
                    ORDER BY crime_count DESC, crime_type ASC
                ) AS rn
            FROM type_counts
        ),
        cluster_types AS (
            SELECT
                cell_x,
                cell_y,
                COALESCE(
                    jsonb_object_agg(crime_type, crime_count) FILTER (WHERE rn <= 3),
                    '{{}}'::jsonb
                ) AS top_crime_types
            FROM ranked_types
            GROUP BY cell_x, cell_y
        )
        SELECT
            concat(:zoom, ':', cluster_geom.cell_x, ':', cluster_geom.cell_y) AS cluster_id,
            cluster_geom.point_count AS count,
            cluster_geom.geometry,
            cluster_types.top_crime_types
        FROM cluster_geom
        JOIN cluster_types
          ON cluster_types.cell_x = cluster_geom.cell_x
         AND cluster_types.cell_y = cluster_geom.cell_y
        ORDER BY cluster_geom.point_count DESC, cluster_geom.cell_y ASC, cluster_geom.cell_x ASC
        LIMIT :row_limit
        """
    )

    if crime_types:
        cluster_query = cluster_query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories:
        cluster_query = cluster_query.bindparams(bindparam("last_outcome_categories", expanding=True))
    if lsoa_names:
        cluster_query = cluster_query.bindparams(bindparam("lsoa_names", expanding=True))

    rows = _execute(db, cluster_query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "type": "FeatureCollection",
        "features": [_cluster_feature(row) for row in page_rows],
        "meta": {
            "mode": "clusters",
            "zoom": zoom,
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": None,
            "filters": _crime_filters_meta(
                month_filter["month"],
                month_filter["startMonth"],
                month_filter["endMonth"],
                crime_types,
                last_outcome_categories,
                lsoa_names,
            ),
            "bbox": _bbox_meta(
                bbox["min_lon"],
                bbox["min_lat"],
                bbox["max_lon"],
                bbox["max_lat"],
            ),
        },
    }


def _analytics_snapshot(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names, db, page_size=5000):
    """Return aggregated analytics data, using a short TTL cache."""
    cache_key = _analytics_snapshot_cache_key(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )
    now = monotonic()

    inflight_event = None
    owns_refresh = False
    while True:
        with _analytics_snapshot_lock:
            cached = _analytics_snapshot_cache.get(cache_key)
            if cached and now - cached["created_at"] <= ANALYTICS_SNAPSHOT_TTL_SECONDS:
                return deepcopy(cached["snapshot"])

            inflight_event = _analytics_snapshot_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = Event()
                _analytics_snapshot_inflight[cache_key] = inflight_event
                owns_refresh = True
                break

        inflight_event.wait()
        now = monotonic()

    where_clauses, query_params = _analytics_filters(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )

    monthly_counts = {}
    if range_filter["params"].get("from_month_date") and range_filter["params"].get("to_month_date"):
        month_date = range_filter["params"]["from_month_date"]
        end_month_date = range_filter["params"]["to_month_date"]
        while month_date <= end_month_date:
            monthly_counts[month_date.strftime("%Y-%m")] = 0
            month_date = _shift_month(month_date, 1)

    crime_type_counts = Counter()
    outcome_counts = Counter()
    unique_lsoas = set()
    unique_crime_types = set()
    total_crimes = 0
    crimes_with_outcomes = 0
    cursor_data = None

    try:
        while True:
            cursor_clause = ""
            params = dict(query_params)
            params["row_limit"] = page_size + 1

            if cursor_data:
                cursor_clause = """
                AND (
                    ce.month < :cursor_month
                    OR (ce.month = :cursor_month AND ce.id < :cursor_id)
                )
                """
                params.update(cursor_data)

            query = text(
                f"""
                /* crimes_analytics_snapshot */
                SELECT
                    ce.id,
                    ce.month AS month_date,
                    COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
                    NULLIF(ce.last_outcome_category, '') AS raw_outcome,
                    COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS outcome,
                    NULLIF(ce.lsoa_code, '') AS lsoa_code,
                    NULLIF(ce.lsoa_name, '') AS lsoa_name
                FROM crime_events ce
                {_where_sql(where_clauses)}
                {cursor_clause}
                ORDER BY ce.month DESC, ce.id DESC
                LIMIT :row_limit
                """
            )
            query = _bind_analytics_filter_params(
                query,
                crime_types,
                last_outcome_categories,
                lsoa_names,
            )

            rows = _execute(db, query, params).mappings().all()
            if not rows:
                break

            page_rows = rows[:page_size]
            for row in page_rows:
                month_label = row["month_date"].strftime("%Y-%m")
                monthly_counts[month_label] = monthly_counts.get(month_label, 0) + 1

                total_crimes += 1
                crime_type_counts[row["crime_type"]] += 1
                outcome_counts[row["outcome"]] += 1
                unique_crime_types.add(row["crime_type"])

                lsoa_key = row["lsoa_code"] or row["lsoa_name"]
                if lsoa_key is not None:
                    unique_lsoas.add(lsoa_key)

                if row["raw_outcome"] is not None:
                    crimes_with_outcomes += 1

            if len(rows) <= page_size:
                break

            last_row = page_rows[-1]
            cursor_data = {
                "cursor_month": last_row["month_date"],
                "cursor_id": last_row["id"],
            }

        snapshot = {
            "total_crimes": total_crimes,
            "unique_lsoas": len(unique_lsoas),
            "unique_crime_types": len(unique_crime_types),
            "crimes_with_outcomes": crimes_with_outcomes,
            "crime_type_counts": dict(crime_type_counts),
            "outcome_counts": dict(outcome_counts),
            "monthly_counts": monthly_counts,
        }

        with _analytics_snapshot_lock:
            _analytics_snapshot_cache[cache_key] = {
                "created_at": monotonic(),
                "snapshot": deepcopy(snapshot),
            }

        return snapshot
    finally:
        if owns_refresh:
            with _analytics_snapshot_lock:
                cached_event = _analytics_snapshot_inflight.get(cache_key)
                if cached_event is inflight_event:
                    del _analytics_snapshot_inflight[cache_key]
            inflight_event.set()
