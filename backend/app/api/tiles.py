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


def _tile_profile(z):
    if z <= 8:
        return {
            "highways": ("motorway", "trunk", "primary"),
            "simplify_tolerance": 80,
        }
    if z <= 11:
        return {
            "highways": ("motorway", "trunk", "primary", "secondary", "tertiary"),
            "simplify_tolerance": 30,
        }
    if z <= 13:
        return {
            "highways": (
                "motorway",
                "trunk",
                "primary",
                "secondary",
                "tertiary",
                "residential",
                "unclassified",
                "service",
            ),
            "simplify_tolerance": 10,
        }
    return {
        "highways": None,
        "simplify_tolerance": 0,
    }


def _validate_tile_coordinates(z, x, y):
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range for zoom level")


def _parse_month(month):
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format") from exc


def _resolve_month_filter(month, startMonth, endMonth, includeRisk):
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

        return (
            "c.month BETWEEN :start_month_date AND :end_month_date",
            {
                "start_month_date": start_month_date,
                "end_month_date": end_month_date,
            },
        )

    if month:
        return "c.month = :month_date", {"month_date": _parse_month(month)}

    if includeRisk:
        raise HTTPException(
            status_code=400,
            detail="month or startMonth/endMonth is required when includeRisk=true",
        )

    return None, {}


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except OperationalError as exc:
        print(exc)
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check BACKEND_DATABASE_URL or DATABASE_URL and Postgres connectivity.",
        ) from exc


def _build_highway_filter_clause(highways):
    if not highways:
        return ""

    quoted = ", ".join(f"'{highway}'" for highway in highways)
    return f"AND rs.highway IN ({quoted})"


def _build_geom_expression(simplify_tolerance):
    if simplify_tolerance <= 0:
        return "rs.geom"
    return f"ST_Simplify(rs.geom, {simplify_tolerance})"


def _roads_only_tile_query(z):
    profile = _tile_profile(z)
    highway_filter_clause = _build_highway_filter_clause(profile["highways"])
    geom_expression = _build_geom_expression(profile["simplify_tolerance"])

    return text(
        f"""
        WITH bounds AS (
            SELECT ST_TileEnvelope(:z, :x, :y) AS geom
        )
        SELECT COALESCE(ST_AsMVT(mvt, 'roads', :extent, 'geom'), ''::bytea) AS tile
        FROM (
            SELECT
                rs.id AS segment_id,
                rs.highway,
                rs.name,
                ST_AsMVTGeom({geom_expression}, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            WHERE rs.geom && bounds.geom
              {highway_filter_clause}
        ) AS mvt
        WHERE geom IS NOT NULL
        """
    )


def _roads_with_risk_tile_query(z, month_filter_clause, include_crime_type_filter):
    profile = _tile_profile(z)
    highway_filter_clause = _build_highway_filter_clause(profile["highways"])
    geom_expression = _build_geom_expression(profile["simplify_tolerance"])
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
                SUM(c.crime_count) AS crimes
            FROM segment_month_type_stats c
            WHERE {month_filter_clause}
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
                    WHEN ranked_scores.pct >= 0.60 THEN 'orange'
                    ELSE 'green'
                END AS band,
                ST_AsMVTGeom({geom_expression}, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            LEFT JOIN ranked_scores ON ranked_scores.segment_id = rs.id
            WHERE rs.geom && bounds.geom
              {highway_filter_clause}
        ) AS mvt
        WHERE geom IS NOT NULL
        """
    )


def _build_tile_bytes(z, x, y, month, startMonth, endMonth, crimeType, includeRisk, db):
    _validate_tile_coordinates(z, x, y)

    query_params = {
        "z": z,
        "x": x,
        "y": y,
        "extent": TILE_EXTENT,
        "buffer": TILE_BUFFER,
    }

    if includeRisk:
        month_filter_clause, month_params = _resolve_month_filter(
            month=month,
            startMonth=startMonth,
            endMonth=endMonth,
            includeRisk=includeRisk,
        )
        query_params.update(month_params)
        if crimeType:
            query_params["crime_type"] = crimeType

        tile_query = _roads_with_risk_tile_query(
            z=z,
            month_filter_clause=month_filter_clause,
            include_crime_type_filter=bool(crimeType),
        )
    else:
        tile_query = _roads_only_tile_query(z)

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
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    crimeType: Optional[str] = Query(None),
    includeRisk: bool = Query(False),
    db: Session = Depends(get_db),
):
    return Response(
        content=_build_tile_bytes(z, x, y, month, startMonth, endMonth, crimeType, includeRisk, db),
        media_type=MVT_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )


@router.get("/tiles/roads/{z}/{x}/{y}.pbf")
def get_road_tiles_pbf(
    z: int = Path(..., ge=0, le=22),
    x: int = Path(..., ge=0),
    y: int = Path(..., ge=0),
    month: Optional[str] = Query(None),
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    crimeType: Optional[str] = Query(None),
    includeRisk: bool = Query(False),
    db: Session = Depends(get_db),
):
    return Response(
        content=_build_tile_bytes(z, x, y, month, startMonth, endMonth, crimeType, includeRisk, db),
        media_type=PBF_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )
