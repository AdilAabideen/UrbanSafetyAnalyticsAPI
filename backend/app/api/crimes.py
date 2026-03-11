import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..db import get_db


router = APIRouter(tags=["crimes"])


def _parse_json(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def _parse_month(month):
    if month is None:
        return None

    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format") from exc


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _resolve_optional_bbox(min_lon, min_lat, max_lon, max_lat):
    values = [min_lon, min_lat, max_lon, max_lat]
    if not any(value is not None for value in values):
        return None

    if not all(value is not None for value in values):
        raise HTTPException(
            status_code=400,
            detail="minLon, minLat, maxLon, and maxLat must all be provided together",
        )

    _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check BACKEND_DATABASE_URL or DATABASE_URL and Postgres connectivity.",
        ) from exc


@router.get("/crimes")
def get_crimes(
    month: Optional[str] = Query(None),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    limit: int = Query(5000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    month_date = _parse_month(month)
    bbox = _resolve_optional_bbox(minLon, minLat, maxLon, maxLat)
    where_clauses = []
    query_params = {"limit": limit}

    if month_date is not None:
        where_clauses.append("ce.month = :month_date")
        query_params["month_date"] = month_date

    if bbox:
        where_clauses.append(
            """
            ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            AND
            ST_Intersects(
                ce.geom,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            )
            """
        )
        query_params.update(bbox)

    where_clause = ""
    if where_clauses:
        where_clause = "WHERE " + " AND ".join(where_clauses)

    query = text(
        f"""
        WITH filtered AS (
            SELECT
                ce.id,
                ce.crime_id,
                ce.month,
                ce.reported_by,
                ce.falls_within,
                ce.lon,
                ce.lat,
                ce.geom,
                ce.location_text,
                ce.lsoa_code,
                ce.lsoa_name,
                ce.crime_type,
                ce.last_outcome_category,
                ce.context,
                ce.segment_id,
                ce.created_at
            FROM crime_events ce
            {where_clause}
        ),
        limited AS (
            SELECT *
            FROM filtered
            ORDER BY month DESC, id DESC
            LIMIT :limit
        )
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', COALESCE(json_agg(feature_row.feature), '[]'::json),
            'meta', json_build_object(
                'returned', COUNT(feature_row.feature),
                'limit', :limit,
                'truncated', EXISTS (SELECT 1 FROM filtered OFFSET :limit),
                'month', CAST(:month_label AS text),
                'bbox', CAST(:bbox_json AS json)
            )
        ) AS feature_collection
        FROM (
            SELECT json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(limited.geom)::json,
                'properties', json_build_object(
                    'id', limited.id,
                    'crime_id', limited.crime_id,
                    'month', limited.month,
                    'reported_by', limited.reported_by,
                    'falls_within', limited.falls_within,
                    'lon', limited.lon,
                    'lat', limited.lat,
                    'location_text', limited.location_text,
                    'lsoa_code', limited.lsoa_code,
                    'lsoa_name', limited.lsoa_name,
                    'crime_type', limited.crime_type,
                    'last_outcome_category', limited.last_outcome_category,
                    'context', limited.context,
                    'segment_id', limited.segment_id,
                    'created_at', limited.created_at
                )
            ) AS feature
            FROM limited
        ) AS feature_row
        """
    )
    query_params["month_label"] = month_date.strftime("%Y-%m") if month_date else None
    query_params["bbox_json"] = json.dumps(
        {
            "minLon": minLon,
            "minLat": minLat,
            "maxLon": maxLon,
            "maxLat": maxLat,
        }
        if bbox
        else None
    )
    result = _execute(db, query, query_params).scalar_one()
    return _parse_json(result) or {"type": "FeatureCollection", "features": []}


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
    bbox = _resolve_optional_bbox(minLon, minLat, maxLon, maxLat)

    where_clauses = []
    query_params = {}

    if month_date is not None:
        where_clauses.append("ce.month = :month_date")
        query_params["month_date"] = month_date

    if bbox:
        where_clauses.append(
            """
            ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            AND
            ST_Intersects(
                ce.geom,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            )
            """
        )
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
        filters["bbox"] = {
            "minLon": minLon,
            "minLat": minLat,
            "maxLon": maxLon,
            "maxLat": maxLat,
        }

    return {
        "filters": filters,
        "total": sum(crime_type_counts.values()),
        "crime_type_counts": crime_type_counts,
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
                'segment_id', ce.segment_id,
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
