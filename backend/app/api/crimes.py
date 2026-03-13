from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from .crime_utils import (
    _analytics_snapshot,
    _analytics_filters,
    _bbox_meta,
    _bind_analytics_filter_params,
    _cluster_feature,
    _cluster_grid_size,
    _crime_filters_meta,
    _crime_map_feature,
    _default_limit,
    _execute,
    _map_filters,
    _normalize_filter_values,
    _optional_bbox,
    _parse_cursor,
    _parse_json,
    _parse_month,
    _required_bbox,
    _resolve_from_to_filter,
    _resolve_mode,
    _resolve_month_filter,
    _shift_month,
    _where_sql,
)
from ..db import get_db


router = APIRouter(tags=["crimes"])

MAX_CRIME_LIMIT = 10000


def _point_next_cursor(rows, limit):
    if len(rows) <= limit or not rows[:limit]:
        return None

    last_row = rows[limit - 1]
    return f"{last_row['month_label']}|{last_row['id']}"


def _incident_item(row):
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


def _sorted_count_items(counts, field_name):
    return [
        {field_name: label, "count": count}
        for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


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
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    last_outcome_categories = _normalize_filter_values(last_outcome_category, "lastOutcomeCategory")
    lsoa_names = _normalize_filter_values(lsoa_name, "lsoaName")
    return range_filter, bbox, crime_types, last_outcome_categories, lsoa_names


def _analytics_response_filters(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
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
):
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
):
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


@router.get("/crimes/incidents")
def get_crime_incidents(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    limit: int = Query(250, ge=1, le=1000),
    cursor: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        True,
    )
    cursor_data = _parse_cursor(cursor)

    where_clauses, query_params = _analytics_filters(
        range_filter,
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

    query = text(
        f"""
        SELECT
            ce.id,
            ce.crime_id,
            to_char(ce.month, 'YYYY-MM') AS month_label,
            COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
            COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS last_outcome_category,
            ce.location_text,
            ce.reported_by,
            ce.falls_within,
            ce.lsoa_code,
            ce.lsoa_name,
            ce.lon,
            ce.lat
        FROM crime_events ce
        {_where_sql(where_clauses)}
        {cursor_clause}
        ORDER BY ce.month DESC, ce.id DESC
        LIMIT :row_limit
        """
    )
    query = _bind_analytics_filter_params(query, crime_types, last_outcome_categories, lsoa_names)

    rows = _execute(db, query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "items": [_incident_item(row) for row in page_rows],
        "meta": {
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": _point_next_cursor(rows, limit),
            "filters": _analytics_response_filters(
                range_filter,
                bbox,
                crime_types,
                last_outcome_categories,
                lsoa_names,
            ),
        },
    }


@router.get("/crimes")
@router.get("/crimes/map")
def get_crimes_map(
    minLon: float = Query(..., ge=-180, le=180),
    minLat: float = Query(..., ge=-90, le=90),
    maxLon: float = Query(..., ge=-180, le=180),
    maxLat: float = Query(..., ge=-90, le=90),
    zoom: int = Query(..., ge=0, le=22),
    month: Optional[str] = Query(None),
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=MAX_CRIME_LIMIT),
    mode: str = Query("auto"),
    cursor: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    month_filter = _resolve_month_filter(month, startMonth, endMonth)
    bbox = _required_bbox(minLon, minLat, maxLon, maxLat)
    crime_types = _normalize_filter_values(crimeType, "crimeType")
    last_outcome_categories = _normalize_filter_values(
        lastOutcomeCategory,
        "lastOutcomeCategory",
    )
    lsoa_names = _normalize_filter_values(lsoaName, "lsoaName")
    resolved_mode = _resolve_mode(mode, zoom)
    effective_limit = limit or _default_limit(zoom, resolved_mode)

    if cursor and resolved_mode != "points":
        raise HTTPException(status_code=400, detail="cursor is only supported for points mode")

    cursor_data = _parse_cursor(cursor)

    if resolved_mode == "clusters":
        return _crime_clusters_payload(
            db=db,
            month_filter=month_filter,
            bbox=bbox,
            zoom=zoom,
            crime_types=crime_types,
            last_outcome_categories=last_outcome_categories,
            lsoa_names=lsoa_names,
            limit=effective_limit,
        )

    return _crime_points_payload(
        db=db,
        month_filter=month_filter,
        bbox=bbox,
        zoom=zoom,
        crime_types=crime_types,
        last_outcome_categories=last_outcome_categories,
        lsoa_names=lsoa_names,
        limit=effective_limit,
        cursor_data=cursor_data,
    )


@router.get("/crimes/stats")
def get_crime_stats(
    month: Optional[str] = Query(None),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    db: Session = Depends(get_db),
):
    month_date = _parse_month(month)
    bbox = _optional_bbox(minLon, minLat, maxLon, maxLat)

    where_clauses = []
    query_params = {}

    if month_date is not None:
        where_clauses.append("ce.month = :month_date")
        query_params["month_date"] = month_date

    if bbox:
        where_clauses.append("ce.lon BETWEEN :min_lon AND :max_lon")
        where_clauses.append("ce.lat BETWEEN :min_lat AND :max_lat")
        where_clauses.append("ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)")
        query_params.update(bbox)

    where_clause = ""
    if where_clauses:
        where_clause = "WHERE " + " AND ".join(where_clauses)

    query = text(
        f"""
        SELECT
            COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
            COUNT(*) AS count
        FROM crime_events ce
        {where_clause}
        GROUP BY COALESCE(NULLIF(ce.crime_type, ''), 'unknown')
        ORDER BY count DESC, crime_type ASC
        """
    )
    rows = _execute(db, query, query_params).mappings()
    crime_type_counts = {row["crime_type"]: row["count"] for row in rows}

    filters = {"month": month_date.strftime("%Y-%m") if month_date else None}
    if bbox:
        filters["bbox"] = _bbox_meta(minLon, minLat, maxLon, maxLat)

    return {
        "filters": filters,
        "total": sum(crime_type_counts.values()),
        "crime_type_counts": crime_type_counts,
    }


@router.get("/crime/analytics/summary")
@router.get("/crimes/analytics/summary")
def get_crime_analytics_summary(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        True,
    )
    snapshot = _analytics_snapshot(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
        db,
    )
    type_rows = _sorted_count_items(snapshot["crime_type_counts"], "crime_type")[:10]
    outcome_rows = _sorted_count_items(snapshot["outcome_counts"], "outcome")[:10]

    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "total_crimes": snapshot["total_crimes"],
        "unique_lsoas": snapshot["unique_lsoas"],
        "unique_crime_types": snapshot["unique_crime_types"],
        "top_crime_type": None
        if not type_rows
        else {
            "crime_type": type_rows[0]["crime_type"],
            "count": type_rows[0]["count"],
        },
        "crimes_with_outcomes": snapshot["crimes_with_outcomes"],
        "top_crime_types": type_rows,
        "top_outcomes": outcome_rows,
    }


@router.get("/crime/analytics/timeseries")
@router.get("/crimes/analytics/timeseries")
def get_crime_analytics_timeseries(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        True,
    )
    snapshot = _analytics_snapshot(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
        db,
    )
    rows = [
        {"month": month, "count": count}
        for month, count in snapshot["monthly_counts"].items()
    ]

    return {
        "series": rows,
        "total": sum(row["count"] for row in rows),
    }


@router.get("/crime/analytics/types")
@router.get("/crimes/analytics/types")
@router.get("/crime/analytics")
@router.get("/crimes/analytics")
def get_crime_type_analytics(
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        False,
    )
    snapshot = _analytics_snapshot(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
        db,
    )
    rows = _sorted_count_items(snapshot["crime_type_counts"], "crime_type")
    items = rows[:limit]
    other_count = sum(row["count"] for row in rows[limit:])

    return {
        "items": items,
        "other_count": other_count,
    }


@router.get("/crime/analytics/outcomes")
@router.get("/crimes/analytics/outcomes")
@router.get("/crime/analytics/outcome")
@router.get("/crimes/analytics/outcome")
def get_crime_outcome_analytics(
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        False,
    )
    snapshot = _analytics_snapshot(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
        db,
    )
    rows = _sorted_count_items(snapshot["outcome_counts"], "outcome")
    items = rows[:limit]
    other_count = sum(row["count"] for row in rows[limit:])

    return {
        "items": items,
        "other_count": other_count,
    }


@router.get("/crime/analytics/anomoly")
@router.get("/crime/analytics/anomaly")
@router.get("/crimes/analytics/anomaly")
def get_crime_analytics_anomaly(
    target: str = Query(...),
    baselineMonths: int = Query(6, ge=1, le=24),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    lsoaName: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    target_month_date = _parse_month(target)
    _, bbox, crime_types, last_outcome_categories, lsoa_names = _analytics_request_filters(
        None,
        None,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        lsoaName,
        False,
    )

    base_filter = {
        "clause": None,
        "params": {},
        "from": None,
        "to": None,
    }
    where_clauses, query_params = _analytics_filters(
        base_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )

    target_where = list(where_clauses) + ["ce.month = :target_month_date"]
    target_params = dict(query_params)
    target_params["target_month_date"] = target_month_date

    baseline_end_date = _shift_month(target_month_date, -1)
    baseline_start_date = _shift_month(target_month_date, -baselineMonths)
    baseline_where = list(where_clauses) + ["ce.month BETWEEN :baseline_start_date AND :baseline_end_date"]
    baseline_params = dict(query_params)
    baseline_params["baseline_start_date"] = baseline_start_date
    baseline_params["baseline_end_date"] = baseline_end_date

    target_query = text(
        f"""
        SELECT COUNT(*)::bigint AS target_count
        FROM crime_events ce
        {_where_sql(target_where)}
        """
    )
    baseline_query = text(
        f"""
        WITH months AS (
            SELECT generate_series(
                CAST(:baseline_start_date AS date),
                CAST(:baseline_end_date AS date),
                interval '1 month'
            )::date AS month
        ),
        counts AS (
            SELECT
                ce.month,
                COUNT(*)::bigint AS count
            FROM crime_events ce
            {_where_sql(baseline_where)}
            GROUP BY ce.month
        )
        SELECT COALESCE(AVG(COALESCE(counts.count, 0)), 0)::float AS baseline_mean
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        """
    )

    target_query = _bind_analytics_filter_params(
        target_query,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )
    baseline_query = _bind_analytics_filter_params(
        baseline_query,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )

    target_row = _execute(db, target_query, target_params).mappings().first() or {}
    baseline_row = _execute(db, baseline_query, baseline_params).mappings().first() or {}

    target_count = target_row.get("target_count", 0)
    baseline_mean = baseline_row.get("baseline_mean", 0) or 0
    ratio = None if baseline_mean == 0 else target_count / baseline_mean

    return {
        "target": target_month_date.strftime("%Y-%m"),
        "target_count": target_count,
        "baseline_mean": baseline_mean,
        "ratio": ratio,
        "flag": ratio is not None and ratio >= 1.5,
    }


@router.get("/crimes/{crime_id}")
def get_crime_by_id(
    crime_id: int = Path(..., ge=1),
    db: Session = Depends(get_db),
):
    query = text(
        """
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(ce.geom)::json,
            'properties', json_build_object(
                'id', ce.id,
                'crime_id', ce.crime_id,
                'month', ce.month,
                'reported_by', ce.reported_by,
                'falls_within', ce.falls_within,
                'lon', ce.lon,
                'lat', ce.lat,
                'location_text', ce.location_text,
                'lsoa_code', ce.lsoa_code,
                'lsoa_name', ce.lsoa_name,
                'crime_type', ce.crime_type,
                'last_outcome_category', ce.last_outcome_category,
                'context', ce.context,
                'created_at', ce.created_at
            )
        ) AS feature
        FROM crime_events ce
        WHERE ce.id = :crime_id
        LIMIT 1
        """
    )
    result = _execute(db, query, {"crime_id": crime_id}).scalar_one_or_none()

    if not result:
        raise HTTPException(status_code=404, detail="Crime event not found")

    return _parse_json(result)
