import json
import logging
from collections import Counter
from copy import deepcopy
from datetime import datetime
from threading import Event, Lock
from time import monotonic
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import bindparam, text
from sqlalchemy.exc import InternalError, OperationalError

from ..schemas.crime_schemas import (
    CrimeAnalyticsFilters,
    CrimeIncidentItem,
    CrimeMapResponse,
)


VALID_MAP_MODES = {"auto", "points", "clusters"}
# Keep cached snapshots short so analytics stay fresh
ANALYTICS_SNAPSHOT_TTL_SECONDS = 60

logger = logging.getLogger(__name__)
_analytics_snapshot_cache = {}
_analytics_snapshot_inflight = {}
_analytics_snapshot_lock = Lock()


def _parse_json(value):
    """Return a Python object when the database emits JSON text."""
    if isinstance(value, str):
        return json.loads(value)
    return value


def _parse_month(month, parameter_name="month"):
    """Convert a YYYY-MM string into a date representing the first of that month."""
    if month is None:
        return None

    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{parameter_name} must be in YYYY-MM format") from exc


def _resolve_month_filter(month, startMonth, endMonth):
    """Enforce either a single month or a month range for map filters."""
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

        return {
            "clause": "ce.month BETWEEN :start_month_date AND :end_month_date",
            "params": {
                "start_month_date": start_month_date,
                "end_month_date": end_month_date,
            },
            "month": None,
            "startMonth": start_month_date.strftime("%Y-%m"),
            "endMonth": end_month_date.strftime("%Y-%m"),
        }

    month_date = _parse_month(month)
    if month_date is None:
        return {
            "clause": None,
            "params": {},
            "month": None,
            "startMonth": None,
            "endMonth": None,
        }

    return {
        "clause": "ce.month = :month_date",
        "params": {"month_date": month_date},
        "month": month_date.strftime("%Y-%m"),
        "startMonth": None,
        "endMonth": None,
    }


def _parse_cursor(cursor):
    """Decode a cursor of the form YYYY-MM|id into query parameters."""
    if cursor is None:
        return None

    try:
        month_key, row_id = cursor.split("|", 1)
        return {
            "cursor_month": datetime.strptime(month_key, "%Y-%m").date(),
            "cursor_id": int(row_id),
        }
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="cursor must be in YYYY-MM|id format") from exc


def _normalize_filter_values(values, parameter_name):
    """Normalize comma-separated filter values into a clean list."""
    if not values:
        return None

    normalized = []
    for item in values:
        for token in item.split(","):
            value = token.strip()
            if value:
                normalized.append(value)

    if not normalized:
        raise HTTPException(status_code=400, detail=f"{parameter_name} must contain at least one value")

    return normalized


def _point_next_cursor(rows, limit):
    """Generate the cursor for paginated incidents responses."""
    if len(rows) <= limit or not rows[:limit]:
        return None

    last_row = rows[limit - 1]
    return f"{last_row['month_label']}|{last_row['id']}"


def _incident_item(row) -> CrimeIncidentItem:
    """Serialize a single incident row for API responses."""
    return {
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
        "lon": row["lon"],
        "lat": row["lat"],
    }


def _sorted_count_items(counts: Dict[str, int], field_name: str):
    """Sort aggregated counts for analytics list responses."""
    return [
        {field_name: label, "count": count}
        for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    """Raise if the provided bounding box has invalid ordering."""
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _required_bbox(min_lon, min_lat, max_lon, max_lat):
    """Return a bbox dict after validating coordinate order."""
    _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _optional_bbox(min_lon, min_lat, max_lon, max_lat):
    """Return None or a validated bbox when all four coords present."""
    values = [min_lon, min_lat, max_lon, max_lat]
    if not any(value is not None for value in values):
        return None

    if not all(value is not None for value in values):
        raise HTTPException(
            status_code=400,
            detail="minLon, minLat, maxLon, and maxLat must all be provided together",
        )

    return _required_bbox(min_lon, min_lat, max_lon, max_lat)


def _resolve_mode(mode, zoom):
    """Resolve the explicit or automatic map mode based on zoom level."""
    if mode not in VALID_MAP_MODES:
        raise HTTPException(status_code=400, detail="mode must be one of auto, points, or clusters")

    if mode == "auto":
        return "clusters" if zoom <= 11 else "points"
    return mode


def _default_limit(zoom, mode):
    """Pick a default row limit for map tiles depending on zoom."""
    if mode == "clusters":
        return 1500 if zoom <= 8 else 2500

    if zoom >= 16:
        return 5000
    if zoom >= 14:
        return 3000
    return 2000


def _cluster_grid_size(bbox, zoom):
    """Compute the grid cell dimensions used for clustering."""
    if zoom <= 8:
        divisions = 12
    elif zoom <= 10:
        divisions = 16
    else:
        divisions = 20

    cell_width = max((bbox["max_lon"] - bbox["min_lon"]) / divisions, 0.0001)
    cell_height = max((bbox["max_lat"] - bbox["min_lat"]) / divisions, 0.0001)
    return cell_width, cell_height


def _execute(db, query, params=None):
    """Execute SQL `query` with DB error translation."""
    try:
        return db.execute(query, params or {})
    except InternalError as exc:
        logger.exception("Database internal error during query execution")
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc
    except OperationalError as exc:
        logger.warning("Database operational error during query execution", exc_info=exc)
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
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
        point_query = point_query.bindparams(
            bindparam("last_outcome_categories", expanding=True)
        )
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
        cluster_query = cluster_query.bindparams(
            bindparam("last_outcome_categories", expanding=True)
        )
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


def _crime_filters_meta(month, start_month, end_month, crime_types, last_outcome_categories, lsoa_names):
    """Return the filter metadata bundled with map responses."""
    return {
        "month": month,
        "startMonth": start_month,
        "endMonth": end_month,
        "crimeType": crime_types,
        "lastOutcomeCategory": last_outcome_categories,
        "lsoaName": lsoa_names,
    }


def _bbox_meta(min_lon, min_lat, max_lon, max_lat):
    """Return serialized bounding box metadata."""
    return {
        "minLon": min_lon,
        "minLat": min_lat,
        "maxLon": max_lon,
        "maxLat": max_lat,
    }


def _analytics_response_filters(
    range_filter,
    bbox,
    crime_types,
    last_outcome_categories,
    lsoa_names,
) -> CrimeAnalyticsFilters:
    """Format the filters metadata returned from analytics endpoints."""
    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "crimeType": crime_types,
        "lastOutcomeCategory": last_outcome_categories,
        "lsoaName": lsoa_names,
        "bbox": None
        if not bbox
        else _bbox_meta(
            bbox["min_lon"],
            bbox["min_lat"],
            bbox["max_lon"],
            bbox["max_lat"],
        ),
    }


def _resolve_from_to_filter(from_month, to_month, required=False):
    """Create the SQL clause and metadata for the analytics date range."""
    if from_month or to_month:
        if not (from_month and to_month):
            raise HTTPException(status_code=400, detail="from and to must be provided together")

        from_month_date = _parse_month(from_month, "from")
        to_month_date = _parse_month(to_month, "to")
        if from_month_date > to_month_date:
            raise HTTPException(status_code=400, detail="from must be less than or equal to to")

        return {
            "clause": "ce.month BETWEEN :from_month_date AND :to_month_date",
            "params": {
                "from_month_date": from_month_date,
                "to_month_date": to_month_date,
            },
            "from": from_month_date.strftime("%Y-%m"),
            "to": to_month_date.strftime("%Y-%m"),
        }

    if required:
        raise HTTPException(status_code=400, detail="from and to are required")

    return {
        "clause": None,
        "params": {},
        "from": None,
        "to": None,
    }


def _analytics_request_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    crime_type,
    last_outcome_category,
    lsoa_name,
    required_range,
):
    """Normalize analytics query params into structured filters."""
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    last_outcome_categories = _normalize_filter_values(last_outcome_category, "lastOutcomeCategory")
    lsoa_names = _normalize_filter_values(lsoa_name, "lsoaName")
    return range_filter, bbox, crime_types, last_outcome_categories, lsoa_names


def _analytics_filters(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    """Assemble WHERE clauses for analytic summaries."""
    """Assemble WHERE clauses + params for analytics queries."""
    where_clauses = []
    query_params = {}

    if range_filter["clause"] is not None:
        where_clauses.append(range_filter["clause"])
        query_params.update(range_filter["params"])

    if bbox:
        where_clauses.extend(
            [
                "ce.lon IS NOT NULL",
                "ce.lat IS NOT NULL",
                "ce.lon BETWEEN :min_lon AND :max_lon",
                "ce.lat BETWEEN :min_lat AND :max_lat",
            ]
        )
        query_params.update(bbox)

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


def _bind_analytics_filter_params(query, crime_types, last_outcome_categories, lsoa_names):
    """Bind expanding parameters for the analytics WHERE clause."""
    """Attach expanding bindparams for list filters."""
    if crime_types:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories:
        query = query.bindparams(bindparam("last_outcome_categories", expanding=True))
    if lsoa_names:
        query = query.bindparams(bindparam("lsoa_names", expanding=True))
    return query


def _where_sql(where_clauses):
    """Format SQL WHERE clause text."""
    """Format the WHERE clause string or return empty."""
    if not where_clauses:
        return ""
    return "WHERE " + " AND ".join(where_clauses)


def _shift_month(month_date, offset):
    """Shift a first-of-month date by a number of months."""
    """Shift a month (day=1) by offset months."""
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _analytics_snapshot_cache_key(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    """Construct the cache key for the analytics snapshot."""
    bbox_key = None
    if bbox:
        bbox_key = (
            bbox["min_lon"],
            bbox["min_lat"],
            bbox["max_lon"],
            bbox["max_lat"],
        )

    return (
        range_filter["from"],
        range_filter["to"],
        bbox_key,
        tuple(crime_types or ()),
        tuple(last_outcome_categories or ()),
        tuple(lsoa_names or ()),
    )


def _analytics_snapshot(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names, db, page_size=5000):
    """Return aggregated analytics data, using a TTL cache."""
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
