import json
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import bindparam, text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from .crime_utils import _normalize_filter_values, _optional_bbox, _parse_month, _shift_month
from ..db import get_db


router = APIRouter(tags=["roads"])

MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
MVT_CACHE_CONTROL = "public, max-age=60"
MVT_EXTENT = 4096
MVT_BUFFER = 64


def _parse_json(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _validate_tile_coordinates(z, x, y):
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range for zoom level")


def _tile_profile(z):
    if z <= 8:
        return ("motorway", "trunk", "primary"), 80
    if z <= 11:
        return ("motorway", "trunk", "primary", "secondary", "tertiary"), 30
    if z <= 13:
        return (
            "motorway",
            "trunk",
            "primary",
            "secondary",
            "tertiary",
            "residential",
            "unclassified",
            "service",
        ), 10
    return None, 0


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def _resolve_from_to_filter(from_month, to_month, required=False):
    if from_month or to_month:
        if not (from_month and to_month):
            raise HTTPException(status_code=400, detail="from and to must be provided together")

        from_month_date = _parse_month(from_month, "from")
        to_month_date = _parse_month(to_month, "to")
        if from_month_date > to_month_date:
            raise HTTPException(status_code=400, detail="from must be less than or equal to to")

        return {
            "from_date": from_month_date,
            "to_date": to_month_date,
            "from": from_month_date.strftime("%Y-%m"),
            "to": to_month_date.strftime("%Y-%m"),
        }

    if required:
        raise HTTPException(status_code=400, detail="from and to are required")

    return {
        "from_date": None,
        "to_date": None,
        "from": None,
        "to": None,
    }


def _where_sql(where_clauses):
    if not where_clauses:
        return ""
    return "WHERE " + " AND ".join(where_clauses)


def _roads_scope_filters(bbox, highways):
    where_clauses = []
    query_params = {}

    if bbox:
        where_clauses.extend(
            [
                "rs.geom && ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857)",
                "ST_Intersects(rs.geom, ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857))",
            ]
        )
        query_params.update(bbox)

    if highways:
        where_clauses.append("COALESCE(NULLIF(rs.highway, ''), 'unknown') IN :highways")
        query_params["highways"] = highways

    return where_clauses, query_params


def _incident_count_filters(range_filter, crime_types, alias="c"):
    where_clauses = []
    query_params = {}

    if range_filter["from_date"] is not None:
        where_clauses.append(f"{alias}.month BETWEEN :from_month_date AND :to_month_date")
        query_params["from_month_date"] = range_filter["from_date"]
        query_params["to_month_date"] = range_filter["to_date"]

    if crime_types:
        where_clauses.append(f"COALESCE(NULLIF({alias}.crime_type, ''), 'unknown') IN :crime_types")
        query_params["crime_types"] = crime_types

    return where_clauses, query_params


def _bind_roads_analytics_params(query, highways, crime_types):
    sql = str(query)

    if highways and ":highways" in sql:
        query = query.bindparams(bindparam("highways", expanding=True))
    if crime_types and ":crime_types" in sql:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    return query


def _roads_analytics_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    crime_type,
    highway,
    required_range,
):
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    highways = _normalize_filter_values(highway, "highway")
    return range_filter, bbox, crime_types, highways


def _risk_band_expression():
    return """
    CASE
        WHEN pct >= 0.95 THEN 'red'
        WHEN pct >= 0.60 THEN 'orange'
        ELSE 'green'
    END
    """


def _sort_expression(sort):
    if sort == "incident_count":
        return "incident_count DESC, incidents_per_km DESC, segment_id ASC"
    return "incidents_per_km DESC, incident_count DESC, segment_id ASC"


@router.get("/roads")
def get_roads(
    minLon: float = Query(..., ge=-180, le=180),
    minLat: float = Query(..., ge=-90, le=90),
    maxLon: float = Query(..., ge=-180, le=180),
    maxLat: float = Query(..., ge=-90, le=90),
    limit: int = Query(2000, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    _validate_bbox(minLon, minLat, maxLon, maxLat)

    query = text(
        """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', COALESCE(json_agg(feature_row.feature), '[]'::json)
        ) AS feature_collection
        FROM (
            SELECT json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(rs.geom)::json,
                'properties', json_build_object(
                    'id', rs.id,
                    'osm_id', rs.osm_id,
                    'name', rs.name,
                    'highway', rs.highway,
                    'length_m', rs.length_m
                )
            ) AS feature
            FROM road_segments_4326 rs
            WHERE ST_Intersects(
                rs.geom,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            )
            ORDER BY rs.id
            LIMIT :limit
        ) AS feature_row
        """
    )
    result = _execute(
        db,
        query,
        {
            "min_lon": minLon,
            "min_lat": minLat,
            "max_lon": maxLon,
            "max_lat": maxLat,
            "limit": limit,
        },
    ).scalar_one()

    return _parse_json(result) or {"type": "FeatureCollection", "features": []}


@router.get("/roads/nearest")
def get_nearest_road(
    lon: float = Query(..., ge=-180, le=180),
    lat: float = Query(..., ge=-90, le=90),
    db: Session = Depends(get_db),
):
    query = text(
        """
        SELECT
            rs.id,
            rs.osm_id,
            rs.name,
            rs.highway,
            rs.length_m,
            ST_AsGeoJSON(rs.geom) AS geometry
        FROM road_segments_4326 rs
        ORDER BY rs.geom <-> ST_SetSRID(ST_Point(:lon, :lat), 4326)
        LIMIT 1
        """
    )
    road = _execute(db, query, {"lon": lon, "lat": lat}).mappings().first()

    if not road:
        raise HTTPException(status_code=404, detail="No road segments found")

    payload = dict(road)
    payload["geometry"] = _parse_json(payload["geometry"])
    return payload


@router.get("/roads/stats")
def get_road_stats(
    minLon: float = Query(..., ge=-180, le=180),
    minLat: float = Query(..., ge=-90, le=90),
    maxLon: float = Query(..., ge=-180, le=180),
    maxLat: float = Query(..., ge=-90, le=90),
    db: Session = Depends(get_db),
):
    _validate_bbox(minLon, minLat, maxLon, maxLat)

    query = text(
        """
        SELECT
            COALESCE(rs.highway, 'unknown') AS highway,
            COUNT(*) AS count
        FROM road_segments_4326 rs
        WHERE ST_Intersects(
            rs.geom,
            ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
        )
        GROUP BY COALESCE(rs.highway, 'unknown')
        ORDER BY count DESC, highway ASC
        """
    )
    rows = _execute(
        db,
        query,
        {
            "min_lon": minLon,
            "min_lat": minLat,
            "max_lon": maxLon,
            "max_lat": maxLat,
        },
    ).mappings()

    counts = {row["highway"]: row["count"] for row in rows}
    return {
        "bbox": {
            "minLon": minLon,
            "minLat": minLat,
            "maxLon": maxLon,
            "maxLat": maxLat,
        },
        "total": sum(counts.values()),
        "highway_counts": counts,
    }


@router.get("/roads/analytics/meta")
def get_road_analytics_meta(db: Session = Depends(get_db)):
    months_query = text(
        """
        SELECT
            to_char(MIN(c.month), 'YYYY-MM') AS min_month,
            to_char(MAX(c.month), 'YYYY-MM') AS max_month
        FROM segment_month_type_stats c
        """
    )
    highways_query = text(
        """
        SELECT DISTINCT COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway
        FROM road_segments rs
        ORDER BY highway ASC
        """
    )
    counts_query = text(
        """
        SELECT
            COUNT(*)::bigint AS road_segments_total,
            COUNT(*) FILTER (WHERE rs.name IS NOT NULL AND NULLIF(rs.name, '') IS NOT NULL)::bigint AS named_roads_total,
            COALESCE(SUM(rs.length_m), 0)::float AS total_length_m
        FROM road_segments rs
        """
    )

    months_row = _execute(db, months_query, {}).mappings().first() or {}
    highway_rows = _execute(db, highways_query, {}).mappings().all()
    counts_row = _execute(db, counts_query, {}).mappings().first() or {}

    return {
        "months": {
            "min": months_row.get("min_month"),
            "max": months_row.get("max_month"),
        },
        "highways": [row["highway"] for row in highway_rows],
        "counts": {
            "road_segments_total": counts_row.get("road_segments_total", 0),
            "named_roads_total": counts_row.get("named_roads_total", 0),
            "total_length_m": counts_row.get("total_length_m", 0),
        },
    }


@router.get("/roads/analytics/summary")
def get_road_analytics_summary(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        highway,
        True,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    incident_where_clauses, incident_params = _incident_count_filters(range_filter, crime_types)
    query_params.update(incident_params)
    road_where_sql = _where_sql(road_where_clauses)
    incident_where_sql = _where_sql(incident_where_clauses)

    summary_query = text(
        f"""
        WITH roads_scope AS (
            SELECT
                rs.id,
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        ),
        incident_counts AS (
            SELECT
                c.segment_id,
                SUM(c.crime_count)::bigint AS incident_count
            FROM segment_month_type_stats c
            JOIN roads_scope rs ON rs.id = c.segment_id
            {incident_where_sql}
            GROUP BY c.segment_id
        )
        SELECT
            COUNT(*)::bigint AS total_segments,
            COALESCE(SUM(rs.length_m), 0)::float AS total_length_m,
            COUNT(DISTINCT rs.highway)::bigint AS unique_highway_types,
            COUNT(*) FILTER (WHERE COALESCE(ic.incident_count, 0) > 0)::bigint AS segments_with_incidents,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS total_incidents
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        """
    )
    top_highway_query = text(
        f"""
        WITH roads_scope AS (
            SELECT
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT
            highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(length_m), 0)::float AS length_m
        FROM roads_scope
        GROUP BY highway
        ORDER BY segment_count DESC, length_m DESC, highway ASC
        LIMIT 1
        """
    )

    summary_query = _bind_roads_analytics_params(summary_query, highways, crime_types)
    top_highway_query = _bind_roads_analytics_params(top_highway_query, highways, crime_types)

    summary_row = _execute(db, summary_query, query_params).mappings().first() or {}
    top_highway_row = _execute(db, top_highway_query, query_params).mappings().first()

    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "total_segments": summary_row.get("total_segments", 0),
        "total_length_m": summary_row.get("total_length_m", 0),
        "unique_highway_types": summary_row.get("unique_highway_types", 0),
        "top_highway_type": None
        if not top_highway_row
        else {
            "highway": top_highway_row["highway"],
            "segment_count": top_highway_row["segment_count"],
            "length_m": top_highway_row["length_m"],
        },
        "total_incidents": summary_row.get("total_incidents", 0),
        "segments_with_incidents": summary_row.get("segments_with_incidents", 0),
    }


@router.get("/roads/analytics/timeseries")
def get_road_analytics_timeseries(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        highway,
        True,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    incident_where_clauses, incident_params = _incident_count_filters(range_filter, crime_types)
    query_params.update(incident_params)
    road_where_sql = _where_sql(road_where_clauses)
    incident_where_sql = _where_sql(incident_where_clauses)

    query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        ),
        months AS (
            SELECT generate_series(
                CAST(:from_month_date AS date),
                CAST(:to_month_date AS date),
                interval '1 month'
            )::date AS month
        ),
        counts AS (
            SELECT
                c.month,
                SUM(c.crime_count)::bigint AS count
            FROM segment_month_type_stats c
            JOIN roads_scope rs ON rs.id = c.segment_id
            {incident_where_sql}
            GROUP BY c.month
        )
        SELECT
            to_char(months.month, 'YYYY-MM') AS month,
            COALESCE(counts.count, 0)::bigint AS count
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        ORDER BY months.month ASC
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types)

    rows = _execute(db, query, query_params).mappings().all()

    return {
        "series": [{"month": row["month"], "count": row["count"]} for row in rows],
        "total": sum(row["count"] for row in rows),
    }


@router.get("/roads/analytics/highways")
def get_road_analytics_highways(
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        highway,
        False,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    incident_where_clauses, incident_params = _incident_count_filters(range_filter, crime_types)
    query_params.update(incident_params)
    road_where_sql = _where_sql(road_where_clauses)
    incident_where_sql = _where_sql(incident_where_clauses)

    query = text(
        f"""
        WITH roads_scope AS (
            SELECT
                rs.id,
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        ),
        incident_counts AS (
            SELECT
                c.segment_id,
                SUM(c.crime_count)::bigint AS incident_count
            FROM segment_month_type_stats c
            JOIN roads_scope rs ON rs.id = c.segment_id
            {incident_where_sql}
            GROUP BY c.segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS incident_count,
            COALESCE(
                COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0) / NULLIF(SUM(rs.length_m) / 1000.0, 0),
                0
            ) AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types)

    rows = _execute(db, query, query_params).mappings().all()
    items = [
        {
            "highway": row["highway"],
            "segment_count": row["segment_count"],
            "length_m": row["length_m"],
            "incident_count": row["incident_count"],
            "incidents_per_km": row["incidents_per_km"],
        }
        for row in rows[:limit]
    ]
    other_rows = rows[limit:]

    return {
        "items": items,
        "other": {
            "segment_count": sum(row["segment_count"] for row in other_rows),
            "length_m": sum(row["length_m"] for row in other_rows),
            "incident_count": sum(row["incident_count"] for row in other_rows),
        },
    }


@router.get("/roads/analytics/risk")
def get_road_analytics_risk(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    sort: str = Query("incidents_per_km"),
    db: Session = Depends(get_db),
):
    if sort not in {"incidents_per_km", "incident_count"}:
        raise HTTPException(status_code=400, detail="sort must be incidents_per_km or incident_count")

    range_filter, bbox, crime_types, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        highway,
        True,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    incident_where_clauses, incident_params = _incident_count_filters(range_filter, crime_types)
    query_params.update(incident_params)
    query_params["row_limit"] = limit
    road_where_sql = _where_sql(road_where_clauses)
    incident_where_sql = _where_sql(incident_where_clauses)

    query = text(
        f"""
        WITH roads_scope AS (
            SELECT
                rs.id,
                rs.name,
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        ),
        incident_counts AS (
            SELECT
                c.segment_id,
                SUM(c.crime_count)::bigint AS incident_count
            FROM segment_month_type_stats c
            JOIN roads_scope rs ON rs.id = c.segment_id
            {incident_where_sql}
            GROUP BY c.segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(rs.length_m / 1000.0, 0), 0) AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct
            FROM scored
        )
        SELECT
            segment_id,
            name,
            highway,
            length_m,
            incident_count,
            incidents_per_km,
            {_risk_band_expression()} AS band
        FROM ranked
        ORDER BY {_sort_expression(sort)}
        LIMIT :row_limit
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types)

    rows = _execute(db, query, query_params).mappings().all()

    return {
        "items": [
            {
                "segment_id": row["segment_id"],
                "name": row["name"],
                "highway": row["highway"],
                "length_m": row["length_m"],
                "incident_count": row["incident_count"],
                "incidents_per_km": row["incidents_per_km"],
                "band": row["band"],
            }
            for row in rows
        ],
        "meta": {
            "returned": len(rows),
            "limit": limit,
            "sort": sort,
        },
    }


@router.get("/roads/analytics/anomaly")
def get_road_analytics_anomaly(
    target: str = Query(...),
    baselineMonths: int = Query(6, ge=1, le=24),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    target_month_date = _parse_month(target, "target")
    _, bbox, crime_types, highways = _roads_analytics_filters(
        None,
        None,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        highway,
        False,
    )

    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    road_where_sql = _where_sql(road_where_clauses)
    target_filters, target_params = _incident_count_filters(
        {"from_date": target_month_date, "to_date": target_month_date},
        crime_types,
    )
    query_params.update(target_params)

    baseline_end_date = _shift_month(target_month_date, -1)
    baseline_start_date = _shift_month(target_month_date, -baselineMonths)
    baseline_filters, baseline_params = _incident_count_filters(
        {"from_date": baseline_start_date, "to_date": baseline_end_date},
        crime_types,
    )

    target_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COALESCE(SUM(c.crime_count), 0)::bigint AS target_count
        FROM segment_month_type_stats c
        JOIN roads_scope rs ON rs.id = c.segment_id
        {_where_sql(target_filters)}
        """
    )
    baseline_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        ),
        months AS (
            SELECT generate_series(
                CAST(:baseline_start_date AS date),
                CAST(:baseline_end_date AS date),
                interval '1 month'
            )::date AS month
        ),
        counts AS (
            SELECT
                c.month,
                SUM(c.crime_count)::bigint AS count
            FROM segment_month_type_stats c
            JOIN roads_scope rs ON rs.id = c.segment_id
            {_where_sql(baseline_filters)}
            GROUP BY c.month
        )
        SELECT COALESCE(AVG(COALESCE(counts.count, 0)), 0)::float AS baseline_mean
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        """
    )

    baseline_params.update(query_params)
    baseline_params["baseline_start_date"] = baseline_start_date
    baseline_params["baseline_end_date"] = baseline_end_date

    target_query = _bind_roads_analytics_params(target_query, highways, crime_types)
    baseline_query = _bind_roads_analytics_params(baseline_query, highways, crime_types)

    target_row = _execute(db, target_query, query_params).mappings().first() or {}
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


@router.get("/roads/{road_id}")
def get_road_by_id(road_id: int, db: Session = Depends(get_db)):
    query = text(
        """
        SELECT
            rs.id,
            rs.osm_id,
            rs.name,
            rs.highway,
            rs.length_m,
            ST_AsGeoJSON(rs.geom) AS geometry
        FROM road_segments_4326 rs
        WHERE rs.id = :road_id
        LIMIT 1
        """
    )
    road = _execute(db, query, {"road_id": road_id}).mappings().first()

    if not road:
        raise HTTPException(status_code=404, detail="Road segment not found")

    payload = dict(road)
    payload["geometry"] = _parse_json(payload["geometry"])
    return payload
