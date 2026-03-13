from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..api_utils.collission_db_utils import (
    _collision_clusters_payload,
    _collision_points_payload,
    _collision_snapshot,
    _execute,
)
from ..api_utils.collission_utils import (
    MAX_COLLISION_LIMIT,
    _bind_collision_filter_params,
    _collision_analytics_filters,
    _collision_analytics_request_filters,
    _collision_analytics_response_filters,
    _collision_filter_values,
    _collision_incident_item,
    _collision_next_cursor,
    _default_limit,
    _parse_collision_cursor,
    _required_bbox,
    _resolve_mode,
    _resolve_month_filter,
    _sorted_count_items,
    _where_sql,
)
from ..db import get_db
from ..schemas.colissions_schemas import (
    CollisionIncidentsResponse,
    CollisionMapResponse,
    CollisionSummaryResponse,
    CollisionTimeseriesResponse,
)


router = APIRouter(tags=["collisions"])


@router.get("/collisions/incidents", response_model=CollisionIncidentsResponse)
def get_collision_incidents(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    collisionSeverity: Optional[List[str]] = Query(None),
    roadType: Optional[List[str]] = Query(None),
    lsoaCode: Optional[List[str]] = Query(None),
    weatherCondition: Optional[List[str]] = Query(None),
    lightCondition: Optional[List[str]] = Query(None),
    roadSurfaceCondition: Optional[List[str]] = Query(None),
    limit: int = Query(250, ge=1, le=1000),
    cursor: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> CollisionIncidentsResponse:
    (
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    ) = _collision_analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        collisionSeverity,
        roadType,
        lsoaCode,
        weatherCondition,
        lightCondition,
        roadSurfaceCondition,
        True,
    )
    cursor_data = _parse_collision_cursor(cursor)

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
        /* collisions_incidents */
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
            ce.longitude,
            ce.latitude
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

    rows = _execute(db, query, query_params).mappings().all()
    truncated = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "items": [_collision_incident_item(row) for row in page_rows],
        "meta": {
            "returned": len(page_rows),
            "limit": limit,
            "truncated": truncated,
            "nextCursor": _collision_next_cursor(rows, limit),
            "filters": _collision_analytics_response_filters(
                range_filter,
                bbox,
                severities,
                road_types,
                lsoa_codes,
                weather_conditions,
                light_conditions,
                road_surface_conditions,
            ),
        },
    }


@router.get("/collisions/map", response_model=CollisionMapResponse)
def get_collisions_map(
    minLon: float = Query(..., ge=-180, le=180),
    minLat: float = Query(..., ge=-90, le=90),
    maxLon: float = Query(..., ge=-180, le=180),
    maxLat: float = Query(..., ge=-90, le=90),
    zoom: int = Query(..., ge=0, le=22),
    month: Optional[str] = Query(None),
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    collisionSeverity: Optional[List[str]] = Query(None),
    roadType: Optional[List[str]] = Query(None),
    lsoaCode: Optional[List[str]] = Query(None),
    weatherCondition: Optional[List[str]] = Query(None),
    lightCondition: Optional[List[str]] = Query(None),
    roadSurfaceCondition: Optional[List[str]] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=MAX_COLLISION_LIMIT),
    mode: str = Query("auto"),
    cursor: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> CollisionMapResponse:
    bbox = _required_bbox(minLon, minLat, maxLon, maxLat)
    month_filter = _resolve_month_filter(month, startMonth, endMonth)
    (
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    ) = _collision_filter_values(
        collisionSeverity,
        roadType,
        lsoaCode,
        weatherCondition,
        lightCondition,
        roadSurfaceCondition,
    )

    resolved_mode = _resolve_mode(mode, zoom)
    resolved_limit = min(limit or _default_limit(zoom, resolved_mode), MAX_COLLISION_LIMIT)
    cursor_data = _parse_collision_cursor(cursor)

    if resolved_mode == "clusters" and cursor_data:
        raise HTTPException(status_code=400, detail="cursor is only supported for points mode")

    if resolved_mode == "points":
        return _collision_points_payload(
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
            resolved_limit,
            cursor_data,
        )

    return _collision_clusters_payload(
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
        resolved_limit,
    )


@router.get("/collisions/analytics/summary", response_model=CollisionSummaryResponse)
def get_collision_analytics_summary(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    collisionSeverity: Optional[List[str]] = Query(None),
    roadType: Optional[List[str]] = Query(None),
    lsoaCode: Optional[List[str]] = Query(None),
    weatherCondition: Optional[List[str]] = Query(None),
    lightCondition: Optional[List[str]] = Query(None),
    roadSurfaceCondition: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
) -> CollisionSummaryResponse:
    (
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    ) = _collision_analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        collisionSeverity,
        roadType,
        lsoaCode,
        weatherCondition,
        lightCondition,
        roadSurfaceCondition,
        True,
    )
    snapshot = _collision_snapshot(
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
        db,
    )
    severity_rows = _sorted_count_items(snapshot["severity_counts"], "collision_severity")
    road_type_rows = _sorted_count_items(snapshot["road_type_counts"], "road_type")
    weather_rows = _sorted_count_items(snapshot["weather_condition_counts"], "weather_condition")
    light_rows = _sorted_count_items(snapshot["light_condition_counts"], "light_condition")

    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "total_collisions": snapshot["total_collisions"],
        "total_casualties": snapshot["total_casualties"],
        "unique_lsoas": snapshot["unique_lsoas"],
        "collisions_with_casualties": snapshot["collisions_with_casualties"],
        "fatal_casualties": snapshot["casualty_severity_counts"].get("Fatal", 0),
        "serious_casualties": snapshot["casualty_severity_counts"].get("Serious", 0),
        "slight_casualties": snapshot["casualty_severity_counts"].get("Slight", 0),
        "avg_casualties_per_collision": 0
        if snapshot["total_collisions"] == 0
        else round(snapshot["total_casualties"] / snapshot["total_collisions"], 2),
        "top_collision_severity": None if not severity_rows else severity_rows[0],
        "top_road_type": None if not road_type_rows else road_type_rows[0],
        "top_weather_condition": None if not weather_rows else weather_rows[0],
        "top_light_condition": None if not light_rows else light_rows[0],
    }


@router.get("/collisions/analytics/timeseries", response_model=CollisionTimeseriesResponse)
def get_collision_analytics_timeseries(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    collisionSeverity: Optional[List[str]] = Query(None),
    roadType: Optional[List[str]] = Query(None),
    lsoaCode: Optional[List[str]] = Query(None),
    weatherCondition: Optional[List[str]] = Query(None),
    lightCondition: Optional[List[str]] = Query(None),
    roadSurfaceCondition: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
) -> CollisionTimeseriesResponse:
    (
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
    ) = _collision_analytics_request_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        collisionSeverity,
        roadType,
        lsoaCode,
        weatherCondition,
        lightCondition,
        roadSurfaceCondition,
        True,
    )
    snapshot = _collision_snapshot(
        range_filter,
        bbox,
        severities,
        road_types,
        lsoa_codes,
        weather_conditions,
        light_conditions,
        road_surface_conditions,
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
