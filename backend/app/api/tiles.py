from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from ..db import get_db


router = APIRouter(tags=["tiles"])

MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
PBF_MEDIA_TYPE = "application/x-protobuf"
TILE_CACHE_CONTROL = "public, max-age=60"
TILE_EXTENT = 4096
TILE_BUFFER = 64
RISK_LENGTH_FLOOR_M = 100.0
CRIME_WEIGHT = 0.55
COLLISION_WEIGHT = 0.45
SLIGHT_CASUALTY_WEIGHT = 0.5
SERIOUS_CASUALTY_WEIGHT = 2.0
FATAL_CASUALTY_WEIGHT = 5.0


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
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
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
                SUM(c.crime_count)::numeric AS crimes
            FROM segment_month_type_stats c
            WHERE {month_filter_clause}
              AND c.segment_id IS NOT NULL
              {crime_type_clause}
            GROUP BY c.segment_id
        ),
        collision_counts AS (
            SELECT
                c.segment_id,
                SUM(c.collision_count)::numeric AS collisions,
                SUM(c.casualty_count)::numeric AS casualties,
                SUM(c.fatal_casualty_count)::numeric AS fatal_casualties,
                SUM(c.serious_casualty_count)::numeric AS serious_casualties,
                SUM(c.slight_casualty_count)::numeric AS slight_casualties
            FROM segment_month_collision_stats c
            WHERE {month_filter_clause}
              AND c.segment_id IS NOT NULL
            GROUP BY c.segment_id
        ),
        road_metrics AS (
            SELECT
                rs.id AS segment_id,
                COALESCE(cc.crimes, 0) AS crimes,
                COALESCE(col.collisions, 0) AS collisions,
                COALESCE(col.casualties, 0) AS casualties,
                COALESCE(col.fatal_casualties, 0) AS fatal_casualties,
                COALESCE(col.serious_casualties, 0) AS serious_casualties,
                COALESCE(col.slight_casualties, 0) AS slight_casualties,
                GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0 AS normalized_km,
                COALESCE(
                    COALESCE(cc.crimes, 0) /
                    NULLIF(GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0, 0),
                    0
                ) AS crimes_per_km,
                (
                    COALESCE(col.collisions, 0) +
                    (COALESCE(col.slight_casualties, 0) * :slight_casualty_weight) +
                    (COALESCE(col.serious_casualties, 0) * :serious_casualty_weight) +
                    (COALESCE(col.fatal_casualties, 0) * :fatal_casualty_weight)
                ) AS collision_severity_points,
                COALESCE(
                    (
                        COALESCE(col.collisions, 0) +
                        (COALESCE(col.slight_casualties, 0) * :slight_casualty_weight) +
                        (COALESCE(col.serious_casualties, 0) * :serious_casualty_weight) +
                        (COALESCE(col.fatal_casualties, 0) * :fatal_casualty_weight)
                    ) /
                    NULLIF(GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0, 0),
                    0
                ) AS collision_density
            FROM road_segments rs
            LEFT JOIN crime_counts cc ON cc.segment_id = rs.id
            LEFT JOIN collision_counts col ON col.segment_id = rs.id
        ),
        active_roads AS (
            SELECT *
            FROM road_metrics
            WHERE crimes > 0 OR collisions > 0
        ),
        ranked_scores AS (
            SELECT
                segment_id,
                crimes,
                collisions,
                casualties,
                fatal_casualties,
                serious_casualties,
                slight_casualties,
                normalized_km,
                crimes_per_km,
                collision_severity_points,
                collision_density,
                percent_rank() OVER (ORDER BY crimes_per_km) AS crime_pct,
                percent_rank() OVER (
                    ORDER BY collision_density
                ) AS collision_pct
            FROM active_roads
        ),
        combined_scores AS (
            SELECT
                segment_id,
                crimes,
                collisions,
                casualties,
                fatal_casualties,
                serious_casualties,
                slight_casualties,
                crimes_per_km,
                collision_severity_points,
                collision_density,
                crime_pct,
                collision_pct,
                ROUND(
                    ((crime_pct * :crime_weight) + (collision_pct * :collision_weight))::numeric,
                    4
                ) AS pct,
                ROUND(
                    (((crime_pct * :crime_weight) + (collision_pct * :collision_weight)) * 100.0)::numeric,
                    2
                ) AS raw_safety_score
            FROM ranked_scores
        ),
        normalized_scores AS (
            SELECT
                segment_id,
                crimes,
                collisions,
                casualties,
                fatal_casualties,
                serious_casualties,
                slight_casualties,
                crimes_per_km,
                collision_severity_points,
                collision_density,
                crime_pct,
                collision_pct,
                percent_rank() OVER (ORDER BY raw_safety_score) AS pct,
                ROUND((percent_rank() OVER (ORDER BY raw_safety_score) * 100.0)::numeric, 2) AS safety_score
            FROM combined_scores
        )
        SELECT COALESCE(ST_AsMVT(mvt, 'roads', :extent, 'geom'), ''::bytea) AS tile
        FROM (
            SELECT
                rs.id AS segment_id,
                rs.highway,
                rs.name,
                COALESCE(road_metrics.crimes, 0) AS crimes,
                COALESCE(road_metrics.crimes_per_km, 0) AS crimes_per_km,
                COALESCE(road_metrics.collisions, 0) AS collisions,
                COALESCE(road_metrics.casualties, 0) AS casualties,
                COALESCE(road_metrics.fatal_casualties, 0) AS fatal_casualties,
                COALESCE(road_metrics.serious_casualties, 0) AS serious_casualties,
                COALESCE(road_metrics.slight_casualties, 0) AS slight_casualties,
                COALESCE(road_metrics.collision_severity_points, 0) AS collision_severity_points,
                COALESCE(road_metrics.collision_density, 0) AS collision_density,
                COALESCE(normalized_scores.crime_pct, 0) AS crime_pct,
                COALESCE(normalized_scores.collision_pct, 0) AS collision_pct,
                COALESCE(normalized_scores.pct, 0) AS pct,
                COALESCE(normalized_scores.safety_score, 0) AS safety_score,
                CASE
                    WHEN COALESCE(normalized_scores.safety_score, 0) >= 50 THEN 'red'
                    WHEN COALESCE(normalized_scores.safety_score, 0) >= 30 THEN 'orange'
                    ELSE 'green'
                END AS band,
                ST_AsMVTGeom({geom_expression}, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            LEFT JOIN road_metrics ON road_metrics.segment_id = rs.id
            LEFT JOIN normalized_scores ON normalized_scores.segment_id = rs.id
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
        "risk_length_floor_m": RISK_LENGTH_FLOOR_M,
        "crime_weight": CRIME_WEIGHT,
        "collision_weight": COLLISION_WEIGHT,
        "slight_casualty_weight": SLIGHT_CASUALTY_WEIGHT,
        "serious_casualty_weight": SERIOUS_CASUALTY_WEIGHT,
        "fatal_casualty_weight": FATAL_CASUALTY_WEIGHT,
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
