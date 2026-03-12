import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

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
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Check BACKEND_DATABASE_URL or DATABASE_URL and Postgres connectivity.",
        ) from exc


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
