from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..db import get_db


router = APIRouter(prefix="/analytics", tags=["analytics"])


def _parse_month(month, field_name):
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be in YYYY-MM format") from exc


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _execute(db, query, params=None):
    try:
        return db.execute(query, params or {})
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check BACKEND_DATABASE_URL or DATABASE_URL and Postgres connectivity.",
        ) from exc


@router.get("/meta")
def get_analytics_meta(db: Session = Depends(get_db)):
    summary_query = text(
        """
        SELECT
            to_char(MIN(ce.month), 'YYYY-MM') AS min_month,
            to_char(MAX(ce.month), 'YYYY-MM') AS max_month,
            COUNT(*)::bigint AS crime_events_total,
            COUNT(*) FILTER (WHERE ce.geom IS NOT NULL)::bigint AS crime_events_with_geom,
            COUNT(*) FILTER (WHERE ce.segment_id IS NOT NULL)::bigint AS crime_events_snapped,
            (SELECT COUNT(*)::bigint FROM road_segments) AS road_segments_total
        FROM crime_events ce
        """
    )
    crime_types_query = text(
        """
        SELECT DISTINCT ce.crime_type
        FROM crime_events ce
        WHERE ce.crime_type IS NOT NULL
          AND ce.crime_type <> ''
        ORDER BY ce.crime_type ASC
        """
    )

    summary = _execute(db, summary_query).mappings().first() or {}
    crime_type_rows = _execute(db, crime_types_query).mappings().all()

    return {
        "months": {
            "min": summary.get("min_month"),
            "max": summary.get("max_month"),
        },
        "crime_types": [row["crime_type"] for row in crime_type_rows],
        "counts": {
            "crime_events_total": summary.get("crime_events_total", 0),
            "crime_events_with_geom": summary.get("crime_events_with_geom", 0),
            "crime_events_snapped": summary.get("crime_events_snapped", 0),
            "road_segments_total": summary.get("road_segments_total", 0),
        },
    }


@router.get("/area/summary")
def get_area_summary(
    minLon: float = Query(..., ge=-180, le=180),
    minLat: float = Query(..., ge=-90, le=90),
    maxLon: float = Query(..., ge=-180, le=180),
    maxLat: float = Query(..., ge=-90, le=90),
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    crimeType: str = Query(None),
    db: Session = Depends(get_db),
):
    _validate_bbox(minLon, minLat, maxLon, maxLat)

    from_month_date = _parse_month(from_month, "from")
    to_month_date = _parse_month(to_month, "to")
    if from_month_date > to_month_date:
        raise HTTPException(status_code=400, detail="from must be less than or equal to to")

    query_params = {
        "min_lon": minLon,
        "min_lat": minLat,
        "max_lon": maxLon,
        "max_lat": maxLat,
        "from_month_date": from_month_date,
        "to_month_date": to_month_date,
    }
    where_clauses = [
        "ce.geom IS NOT NULL",
        "ce.lon BETWEEN :min_lon AND :max_lon",
        "ce.lat BETWEEN :min_lat AND :max_lat",
        "ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)",
        "ce.month BETWEEN :from_month_date AND :to_month_date",
    ]
    if crimeType:
        where_clauses.append("ce.crime_type = :crime_type")
        query_params["crime_type"] = crimeType

    where_sql = " AND ".join(where_clauses)

    total_query = text(
        f"""
        SELECT COUNT(*)::bigint AS total_crimes
        FROM crime_events ce
        WHERE {where_sql}
        """
    )
    by_type_query = text(
        f"""
        SELECT
            COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
            COUNT(*)::bigint AS count
        FROM crime_events ce
        WHERE {where_sql}
        GROUP BY COALESCE(NULLIF(ce.crime_type, ''), 'unknown')
        ORDER BY count DESC, crime_type ASC
        LIMIT 10
        """
    )
    trend_query = text(
        f"""
        WITH months AS (
            SELECT generate_series(
                CAST(:from_month_date AS date),
                CAST(:to_month_date AS date),
                interval '1 month'
            )::date AS month
        ),
        counts AS (
            SELECT
                ce.month,
                COUNT(*)::bigint AS count
            FROM crime_events ce
            WHERE {where_sql}
            GROUP BY ce.month
        )
        SELECT
            to_char(months.month, 'YYYY-MM') AS month,
            COALESCE(counts.count, 0)::bigint AS count
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        ORDER BY months.month ASC
        """
    )

    total_row = _execute(db, total_query, query_params).mappings().first() or {}
    by_type_rows = _execute(db, by_type_query, query_params).mappings().all()
    trend_rows = _execute(db, trend_query, query_params).mappings().all()

    return {
        "bbox": {
            "minLon": minLon,
            "minLat": minLat,
            "maxLon": maxLon,
            "maxLat": maxLat,
        },
        "from": from_month,
        "to": to_month,
        "crimeType": crimeType,
        "total_crimes": total_row.get("total_crimes", 0),
        "by_type_top": [
            {
                "crime_type": row["crime_type"],
                "count": row["count"],
            }
            for row in by_type_rows
        ],
        "monthly_trend": [
            {
                "month": row["month"],
                "count": row["count"],
            }
            for row in trend_rows
        ],
    }
