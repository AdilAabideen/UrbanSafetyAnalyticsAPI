from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from ..db import get_db


router = APIRouter(tags=["tiles"])

MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
PBF_MEDIA_TYPE = "application/x-protobuf"
TILE_CACHE_CONTROL = "public, max-age=60"
TILE_EXTENT = 4096
TILE_BUFFER = 64


def _validate_tile_coordinates(z, x, y):
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range for zoom level")


def _parse_month(month):
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format") from exc


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check BACKEND_DATABASE_URL or DATABASE_URL and Postgres connectivity.",
        ) from exc


def _roads_only_tile_query():
    return text(
        """
        WITH bounds AS (
            SELECT ST_TileEnvelope(:z, :x, :y) AS geom
        )
        SELECT COALESCE(ST_AsMVT(mvt, 'roads', :extent, 'geom'), ''::bytea) AS tile
        FROM (
            SELECT
                rs.id AS segment_id,
                rs.highway,
                rs.name,
                ST_AsMVTGeom(rs.geom, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            WHERE rs.geom && bounds.geom
        ) AS mvt
        WHERE geom IS NOT NULL
        """
    )


def _roads_with_risk_tile_query(include_crime_type_filter):
    crime_type_clause = ""
    if include_crime_type_filter:
        crime_type_clause = "AND c.crime_type = :crime_type"

    return text(
        f"""
        WITH bounds AS (
            SELECT ST_TileEnvelope(:z, :x, :y) AS geom
        ),
        crime_counts AS (
            SELECT
                c.segment_id,
                COUNT(*) AS crimes
            FROM crime_events c
            WHERE c.month = :month_date
              AND c.segment_id IS NOT NULL
              {crime_type_clause}
            GROUP BY c.segment_id
        ),
        ranked_scores AS (
            SELECT
                rs.id AS segment_id,
                COALESCE(cc.crimes, 0) AS crimes,
                COALESCE(COALESCE(cc.crimes, 0) / NULLIF(rs.length_m / 1000.0, 0), 0) AS crimes_per_km,
                percent_rank() OVER (
                    ORDER BY COALESCE(COALESCE(cc.crimes, 0) / NULLIF(rs.length_m / 1000.0, 0), 0)
                ) AS pct
            FROM road_segments rs
            LEFT JOIN crime_counts cc ON cc.segment_id = rs.id
        )
        SELECT COALESCE(ST_AsMVT(mvt, 'roads', :extent, 'geom'), ''::bytea) AS tile
        FROM (
            SELECT
                rs.id AS segment_id,
                rs.highway,
                rs.name,
                ranked_scores.crimes,
                ranked_scores.crimes_per_km,
                ranked_scores.pct,
                CASE
                    WHEN ranked_scores.pct >= 0.95 THEN 'red'
                    WHEN ranked_scores.pct >= 0.80 THEN 'orange'
                    ELSE 'green'
                END AS band,
                ST_AsMVTGeom(rs.geom, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            LEFT JOIN ranked_scores ON ranked_scores.segment_id = rs.id
            WHERE rs.geom && bounds.geom
        ) AS mvt
        WHERE geom IS NOT NULL
        """
    )


def _build_tile_bytes(z, x, y, month, crimeType, includeRisk, db):
    _validate_tile_coordinates(z, x, y)

    query_params = {
        "z": z,
        "x": x,
        "y": y,
        "extent": TILE_EXTENT,
        "buffer": TILE_BUFFER,
    }

    if includeRisk:
        if month is None:
            raise HTTPException(status_code=400, detail="month is required when includeRisk=true")

        query_params["month_date"] = _parse_month(month)
        if crimeType:
            query_params["crime_type"] = crimeType

        tile_query = _roads_with_risk_tile_query(include_crime_type_filter=bool(crimeType))
    else:
        tile_query = _roads_only_tile_query()

    tile = _execute(db, tile_query, query_params).scalar_one()
    if isinstance(tile, memoryview):
        return tile.tobytes()
    return tile or b""


@router.get("/tiles/roads/{z}/{x}/{y}.mvt")
def get_road_tiles_mvt(
    z: int = Path(..., ge=0, le=22),
    x: int = Path(..., ge=0),
    y: int = Path(..., ge=0),
    month: Optional[str] = Query(None),
    crimeType: Optional[str] = Query(None),
    includeRisk: bool = Query(False),
    db: Session = Depends(get_db),
):
    return Response(
        content=_build_tile_bytes(z, x, y, month, crimeType, includeRisk, db),
        media_type=MVT_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )


@router.get("/tiles/roads/{z}/{x}/{y}.pbf")
def get_road_tiles_pbf(
    z: int = Path(..., ge=0, le=22),
    x: int = Path(..., ge=0),
    y: int = Path(..., ge=0),
    month: Optional[str] = Query(None),
    crimeType: Optional[str] = Query(None),
    includeRisk: bool = Query(False),
    db: Session = Depends(get_db),
):
    return Response(
        content=_build_tile_bytes(z, x, y, month, crimeType, includeRisk, db),
        media_type=PBF_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )
