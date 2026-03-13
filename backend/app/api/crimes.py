from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..api_utils.crime_utils import (
    _analytics_request_filters,
    _analytics_response_filters,
    _analytics_filters,
    _bind_analytics_filter_params,
    _default_limit,
    _normalize_filter_values,
    _parse_cursor,
    _parse_json,
    _point_next_cursor,
    _incident_item,
    _resolve_mode,
    _resolve_month_filter,
    _required_bbox,
    _sorted_count_items,
    _where_sql,
)
from ..api_utils.crime_utils_db import (
    _analytics_snapshot,
    _crime_clusters_payload,
    _crime_points_payload,
    _execute,
)
from ..db import get_db
from ..errors import ValidationError
from ..schemas.crime_schemas import (
    CrimeAnalyticsSummaryResponse,
    CrimeDetailFeature,
    CrimeIncidentsResponse,
    CrimeMapResponse,
    CrimeTimeseriesResponse,
)


router = APIRouter(tags=["crimes"])

MAX_CRIME_LIMIT = 10000
# Incident listing for analytics rows
@router.get("/crimes/incidents", response_model=CrimeIncidentsResponse)
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
) -> CrimeIncidentsResponse:
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
        /* crimes_incidents */
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


# Map view endpoint for interactive tiles
@router.get("/crimes/map", response_model=CrimeMapResponse)
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
) -> CrimeMapResponse:
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
        raise ValidationError(
            error="INVALID_REQUEST",
            message="cursor is only supported for points mode",
            details={"field": "cursor"},
        )

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

# Overview card data for analytics
@router.get("/crimes/analytics/summary", response_model=CrimeAnalyticsSummaryResponse)
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
) -> CrimeAnalyticsSummaryResponse:
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

# Monthly trend series used by analytics chart
@router.get("/crimes/analytics/timeseries", response_model=CrimeTimeseriesResponse)
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
) -> CrimeTimeseriesResponse:
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

# Single crime detail endpoint
@router.get("/crimes/{crime_id}", response_model=CrimeDetailFeature)
def get_crime_by_id(
    crime_id: int = Path(..., ge=1),
    db: Session = Depends(get_db),
) -> CrimeDetailFeature:
    query = text(
        """
        /* crimes_detail */
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
