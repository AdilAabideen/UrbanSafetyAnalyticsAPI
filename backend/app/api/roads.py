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


def _incident_count_filters(range_filter, crime_types, last_outcome_categories=None, alias="c"):
    where_clauses = []
    query_params = {}

    if range_filter["from_date"] is not None:
        where_clauses.append(f"{alias}.month BETWEEN :from_month_date AND :to_month_date")
        query_params["from_month_date"] = range_filter["from_date"]
        query_params["to_month_date"] = range_filter["to_date"]

    if crime_types:
        where_clauses.append(f"COALESCE(NULLIF({alias}.crime_type, ''), 'unknown') IN :crime_types")
        query_params["crime_types"] = crime_types

    if last_outcome_categories:
        where_clauses.append(
            f"COALESCE(NULLIF({alias}.last_outcome_category, ''), 'unknown') IN :last_outcome_categories"
        )
        query_params["last_outcome_categories"] = last_outcome_categories

    return where_clauses, query_params


def _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories=None):
    sql = str(query)

    if highways and ":highways" in sql:
        query = query.bindparams(bindparam("highways", expanding=True))
    if crime_types and ":crime_types" in sql:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories and ":last_outcome_categories" in sql:
        query = query.bindparams(bindparam("last_outcome_categories", expanding=True))
    return query


def _roads_analytics_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    crime_type,
    last_outcome_category,
    highway,
    required_range,
):
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    last_outcome_categories = _normalize_filter_values(last_outcome_category, "lastOutcomeCategory")
    highways = _normalize_filter_values(highway, "highway")
    return range_filter, bbox, crime_types, last_outcome_categories, highways


def _risk_band_expression(score_alias="risk_score"):
    return """
    CASE
        WHEN {score_alias} >= 90 THEN 'red'
        WHEN {score_alias} >= 70 THEN 'orange'
        ELSE 'green'
    END
    """.format(score_alias=score_alias)


def _sort_expression(sort):
    if sort == "incident_count":
        return "incident_count DESC, risk_score DESC, incidents_per_km DESC, segment_id ASC"
    if sort == "incidents_per_km":
        return "incidents_per_km DESC, risk_score DESC, incident_count DESC, segment_id ASC"
    return "risk_score DESC, incident_count DESC, incidents_per_km DESC, segment_id ASC"


def _matched_previous_period(range_filter):
    month_span = (
        (range_filter["to_date"].year - range_filter["from_date"].year) * 12
        + (range_filter["to_date"].month - range_filter["from_date"].month)
    )
    previous_to_date = _shift_month(range_filter["from_date"], -1)
    previous_from_date = _shift_month(range_filter["from_date"], -(month_span + 1))
    return {
        "from_date": previous_from_date,
        "to_date": previous_to_date,
        "from": previous_from_date.strftime("%Y-%m"),
        "to": previous_to_date.strftime("%Y-%m"),
    }


def _safe_pct_change(current_value, previous_value):
    if previous_value in (None, 0):
        return None
    return round(((current_value - previous_value) / previous_value) * 100.0, 2)


def _road_events_cte(road_where_sql, event_where_sql):
    return f"""
        WITH roads_scope AS (
            SELECT
                rs.id,
                COALESCE(NULLIF(rs.name, ''), '(unnamed road)') AS name,
                COALESCE(NULLIF(rs.highway, ''), 'unknown') AS highway,
                rs.length_m
            FROM road_segments rs
            {road_where_sql}
        ),
        events_scope AS (
            SELECT
                ce.segment_id,
                ce.month,
                rs.highway,
                COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
                COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS outcome
            FROM crime_events ce
            JOIN roads_scope rs ON rs.id = ce.segment_id
            {event_where_sql}
        )
    """


def _road_insight_messages(total_incidents, top_highway, top_crime_type, top_outcome, current_vs_previous_pct):
    if total_incidents == 0:
        return ["No road-linked incidents matched the current filter selection."]

    insights = []
    if top_highway:
        insights.append(
            f"{top_highway['highway']} roads drive the largest incident volume in this selection."
        )
    if top_crime_type:
        insights.append(
            f"{top_crime_type['crime_type']} is the dominant road-linked crime type here."
        )
    if top_outcome:
        insights.append(
            f"Most linked incidents currently end with '{top_outcome['outcome']}'."
        )
    if current_vs_previous_pct is not None:
        direction = "above" if current_vs_previous_pct >= 0 else "below"
        insights.append(
            f"This period is {abs(current_vs_previous_pct):.1f}% {direction} the previous matched period."
        )
    return insights[:4]


def _highway_message(item, total_incidents, total_length_m):
    if item["incident_count"] == 0:
        return f"{item['highway']} roads are present here but have no linked incidents in this selection."

    share_of_incidents = 0 if total_incidents == 0 else (item["incident_count"] / total_incidents) * 100.0
    share_of_length = 0 if total_length_m == 0 else (item["length_m"] / total_length_m) * 100.0
    if share_of_incidents > share_of_length * 1.5:
        return f"{item['highway']} roads are over-indexing for incidents relative to their length."
    if share_of_incidents > 25:
        return f"{item['highway']} roads account for a large share of incidents in this view."
    return f"{item['highway']} roads contribute steady incident volume without dominating the area."


def _risk_item_message(item):
    parts = [f"{item['incident_count']} incidents"]
    if item.get("dominant_crime_type"):
        parts.append(f"driven mainly by {item['dominant_crime_type']}")
    if item.get("previous_period_change_pct") is not None:
        direction = "up" if item["previous_period_change_pct"] >= 0 else "down"
        parts.append(f"{abs(item['previous_period_change_pct']):.1f}% {direction} vs previous period")
    return ", ".join(parts)


def _road_geojson_feature(row):
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "id": row["id"],
            "osm_id": row["osm_id"],
            "name": row["name"],
            "highway": row["highway"],
            "length_m": row["length_m"],
        },
    }


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
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    summary_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        )
        SELECT
            COUNT(*)::bigint AS total_segments,
            COALESCE(SUM(length_m), 0)::float AS total_length_m,
            COUNT(DISTINCT highway)::bigint AS unique_highway_types,
            COUNT(*) FILTER (WHERE incident_count > 0)::bigint AS roads_with_incidents,
            COALESCE(SUM(incident_count), 0)::bigint AS total_incidents,
            COALESCE(SUM(incident_count) / NULLIF(SUM(length_m) / 1000.0, 0), 0)::float AS avg_incidents_per_km
        FROM scored
        """
    )
    top_highway_query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS incident_count,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)) / NULLIF(SUM(rs.length_m) / 1000.0, 0), 0)::float AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC
        LIMIT 1
        """
    )
    top_crime_type_query = text(
        base_cte
        + """
        SELECT
            crime_type,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY crime_type
        ORDER BY count DESC, crime_type ASC
        LIMIT 1
        """
    )
    top_outcome_query = text(
        base_cte
        + """
        SELECT
            outcome,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY outcome
        ORDER BY count DESC, outcome ASC
        LIMIT 1
        """
    )
    top_road_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density
            FROM scored
        ),
        final_ranked AS (
            SELECT
                *,
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM ranked
        )
        SELECT
            segment_id,
            name,
            highway,
            length_m,
            incident_count,
            incidents_per_km,
            risk_score,
            {_risk_band_expression()} AS band
        FROM final_ranked
        ORDER BY risk_score DESC, incident_count DESC, incidents_per_km DESC
        LIMIT 1
        """
    )
    band_breakdown_query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        ranked AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density
            FROM scored
        ),
        final_ranked AS (
            SELECT
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM ranked
        )
        SELECT
            {_risk_band_expression()} AS band,
            COUNT(*)::bigint AS count
        FROM final_ranked
        GROUP BY band
        """
    )

    summary_query = _bind_roads_analytics_params(summary_query, highways, crime_types, last_outcome_categories)
    top_highway_query = _bind_roads_analytics_params(top_highway_query, highways, crime_types, last_outcome_categories)
    top_crime_type_query = _bind_roads_analytics_params(
        top_crime_type_query, highways, crime_types, last_outcome_categories
    )
    top_outcome_query = _bind_roads_analytics_params(top_outcome_query, highways, crime_types, last_outcome_categories)
    top_road_query = _bind_roads_analytics_params(top_road_query, highways, crime_types, last_outcome_categories)
    band_breakdown_query = _bind_roads_analytics_params(
        band_breakdown_query, highways, crime_types, last_outcome_categories
    )

    summary_row = _execute(db, summary_query, query_params).mappings().first() or {}
    top_highway_row = _execute(db, top_highway_query, query_params).mappings().first()
    top_crime_type_row = _execute(db, top_crime_type_query, query_params).mappings().first()
    top_outcome_row = _execute(db, top_outcome_query, query_params).mappings().first()
    top_road_row = _execute(db, top_road_query, query_params).mappings().first()
    band_rows = _execute(db, band_breakdown_query, query_params).mappings().all()

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COUNT(*)::bigint AS incident_count
        FROM crime_events ce
        JOIN roads_scope rs ON rs.id = ce.segment_id
        {_where_sql(previous_where_clauses)}
        """
    )
    previous_query = _bind_roads_analytics_params(previous_query, highways, crime_types, last_outcome_categories)
    previous_query_params = dict(query_params)
    previous_query_params.update(previous_params)
    previous_row = _execute(db, previous_query, previous_query_params).mappings().first() or {}

    total_incidents = summary_row.get("total_incidents", 0)
    previous_incidents = previous_row.get("incident_count", 0)
    current_vs_previous_pct = _safe_pct_change(total_incidents, previous_incidents)
    band_breakdown = {"red": 0, "orange": 0, "green": 0}
    band_breakdown.update({row["band"]: row["count"] for row in band_rows})

    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "total_segments": summary_row.get("total_segments", 0),
        "total_length_m": summary_row.get("total_length_m", 0),
        "unique_highway_types": summary_row.get("unique_highway_types", 0),
        "roads_with_incidents": summary_row.get("roads_with_incidents", 0),
        "segments_with_incidents": summary_row.get("roads_with_incidents", 0),
        "total_incidents": total_incidents,
        "avg_incidents_per_km": summary_row.get("avg_incidents_per_km", 0),
        "top_road": None
        if not top_road_row
        else {
            "segment_id": top_road_row["segment_id"],
            "name": top_road_row["name"],
            "highway": top_road_row["highway"],
            "length_m": top_road_row["length_m"],
            "incident_count": top_road_row["incident_count"],
            "incidents_per_km": top_road_row["incidents_per_km"],
            "risk_score": top_road_row["risk_score"],
            "band": top_road_row["band"],
        },
        "top_highway": None
        if not top_highway_row
        else {
            "highway": top_highway_row["highway"],
            "segment_count": top_highway_row["segment_count"],
            "length_m": top_highway_row["length_m"],
            "incident_count": top_highway_row["incident_count"],
            "incidents_per_km": top_highway_row["incidents_per_km"],
        },
        "top_highway_type": None
        if not top_highway_row
        else {
            "highway": top_highway_row["highway"],
            "segment_count": top_highway_row["segment_count"],
            "length_m": top_highway_row["length_m"],
        },
        "top_crime_type": top_crime_type_row,
        "top_outcome": top_outcome_row,
        "current_period": {"from": range_filter["from"], "to": range_filter["to"], "incident_count": total_incidents},
        "previous_period": {
            "from": previous_filter["from"],
            "to": previous_filter["to"],
            "incident_count": previous_incidents,
        },
        "current_vs_previous_pct": current_vs_previous_pct,
        "band_breakdown": band_breakdown,
        "insights": _road_insight_messages(
            total_incidents,
            top_highway_row,
            top_crime_type_row,
            top_outcome_row,
            current_vs_previous_pct,
        ),
    }


@router.get("/roads/analytics/trends")
@router.get("/roads/analytics/timeseries")
def get_road_analytics_timeseries(
    from_month: str = Query(..., alias="from"),
    to_month: str = Query(..., alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    groupBy: str = Query("overall"),
    groupLimit: int = Query(5, ge=1, le=10),
    db: Session = Depends(get_db),
):
    if groupBy not in {"overall", "highway", "crime_type", "outcome"}:
        raise HTTPException(status_code=400, detail="groupBy must be overall, highway, crime_type, or outcome")

    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)
    group_expr = {
        "overall": "'overall'",
        "highway": "highway",
        "crime_type": "crime_type",
        "outcome": "outcome",
    }[groupBy]

    if groupBy == "overall":
        query = text(
            base_cte
            + """
            ,
            months AS (
                SELECT generate_series(
                    CAST(:from_month_date AS date),
                    CAST(:to_month_date AS date),
                    interval '1 month'
                )::date AS month
            ),
            counts AS (
                SELECT
                    month,
                    COUNT(*)::bigint AS count
                FROM events_scope
                GROUP BY month
            )
            SELECT
                'overall' AS group_key,
                to_char(months.month, 'YYYY-MM') AS month,
                COALESCE(counts.count, 0)::bigint AS count
            FROM months
            LEFT JOIN counts ON counts.month = months.month
            ORDER BY months.month ASC
            """
        )
        rows = _execute(
            db,
            _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories),
            query_params,
        ).mappings().all()
        series = [{"key": "overall", "points": [{"month": row["month"], "count": row["count"]} for row in rows]}]
    else:
        query_params["group_limit"] = groupLimit
        query = text(
            base_cte
            + f"""
            ,
            months AS (
                SELECT generate_series(
                    CAST(:from_month_date AS date),
                    CAST(:to_month_date AS date),
                    interval '1 month'
                )::date AS month
            ),
            top_groups AS (
                SELECT
                    {group_expr} AS group_key,
                    COUNT(*)::bigint AS total
                FROM events_scope
                GROUP BY group_key
                ORDER BY total DESC, group_key ASC
                LIMIT :group_limit
            ),
            counts AS (
                SELECT
                    month,
                    {group_expr} AS group_key,
                    COUNT(*)::bigint AS count
                FROM events_scope
                GROUP BY month, group_key
            )
            SELECT
                top_groups.group_key,
                top_groups.total,
                to_char(months.month, 'YYYY-MM') AS month,
                COALESCE(counts.count, 0)::bigint AS count
            FROM top_groups
            CROSS JOIN months
            LEFT JOIN counts
              ON counts.group_key = top_groups.group_key
             AND counts.month = months.month
            ORDER BY top_groups.total DESC, top_groups.group_key ASC, months.month ASC
            """
        )
        rows = _execute(
            db,
            _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories),
            query_params,
        ).mappings().all()
        grouped = {}
        group_totals = {}
        for row in rows:
            grouped.setdefault(row["group_key"], [])
            grouped[row["group_key"]].append({"month": row["month"], "count": row["count"]})
            group_totals[row["group_key"]] = row["total"]
        series = [
            {"key": key, "total": group_totals.get(key, 0), "points": points}
            for key, points in grouped.items()
        ]

    overall_total = (
        sum(point["count"] for item in series for point in item["points"])
        if groupBy != "overall"
        else sum(point["count"] for point in series[0]["points"]) if series else 0
    )
    overall_points = (
        series[0]["points"]
        if groupBy == "overall"
        else [
            {
                "month": month,
                "count": sum(item["points"][idx]["count"] for item in series),
            }
            for idx, month in enumerate([point["month"] for point in series[0]["points"]])
        ]
        if series
        else []
    )
    peak_point = max(overall_points, key=lambda point: point["count"], default=None)

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COUNT(*)::bigint AS incident_count
        FROM crime_events ce
        JOIN roads_scope rs ON rs.id = ce.segment_id
        {_where_sql(previous_where_clauses)}
        """
    )
    previous_query = _bind_roads_analytics_params(previous_query, highways, crime_types, last_outcome_categories)
    previous_query_params = {key: value for key, value in query_params.items() if key != "group_limit"}
    previous_query_params.update(previous_params)
    previous_total = (
        _execute(db, previous_query, previous_query_params).mappings().first() or {}
    ).get("incident_count", 0)
    current_vs_previous_pct = _safe_pct_change(overall_total, previous_total)

    insights = []
    if peak_point:
        insights.append(f"Peak incident month in this selection is {peak_point['month']}.")
    if current_vs_previous_pct is not None:
        direction = "above" if current_vs_previous_pct >= 0 else "below"
        insights.append(
            f"The current period is {abs(current_vs_previous_pct):.1f}% {direction} the previous matched period."
        )
    if groupBy != "overall" and series:
        insights.append(f"{series[0]['key']} is the leading {groupBy.replace('_', ' ')} trend in this view.")

    return {
        "groupBy": groupBy,
        "series": series,
        "total": overall_total,
        "peak": peak_point,
        "current_vs_previous_pct": current_vs_previous_pct,
        "insights": insights,
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
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        False,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS incident_count,
            COALESCE(
                COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0) / NULLIF(SUM(rs.length_m) / 1000.0, 0),
                0
            )::float AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY incident_count DESC, segment_count DESC, rs.highway ASC
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories)

    rows = _execute(db, query, query_params).mappings().all()
    total_incidents = sum(row["incident_count"] for row in rows)
    total_length_m = sum(row["length_m"] for row in rows)
    items = []
    for row in rows[:limit]:
        items.append(
            {
                "highway": row["highway"],
                "segment_count": row["segment_count"],
                "length_m": row["length_m"],
                "incident_count": row["incident_count"],
                "incidents_per_km": row["incidents_per_km"],
                "share_of_incidents": 0 if total_incidents == 0 else round((row["incident_count"] / total_incidents) * 100.0, 2),
                "share_of_length": 0 if total_length_m == 0 else round((row["length_m"] / total_length_m) * 100.0, 2),
                "message": _highway_message(row, total_incidents, total_length_m),
            }
        )
    other_rows = rows[limit:]
    insights = []
    if items:
        insights.append(f"{items[0]['highway']} roads contribute the highest incident volume in this selection.")
        top_rate = max(items, key=lambda item: item["incidents_per_km"])
        insights.append(f"{top_rate['highway']} roads have the highest incident rate per km among the visible highway groups.")

    return {
        "items": items,
        "other": {
            "segment_count": sum(row["segment_count"] for row in other_rows),
            "length_m": sum(row["length_m"] for row in other_rows),
            "incident_count": sum(row["incident_count"] for row in other_rows),
        },
        "insights": insights,
    }


@router.get("/roads/analytics/breakdowns")
def get_road_analytics_breakdowns(
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    minLon: Optional[float] = Query(None, ge=-180, le=180),
    minLat: Optional[float] = Query(None, ge=-90, le=90),
    maxLon: Optional[float] = Query(None, ge=-180, le=180),
    maxLat: Optional[float] = Query(None, ge=-90, le=90),
    crimeType: Optional[List[str]] = Query(None),
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(10, ge=1, le=25),
    db: Session = Depends(get_db),
):
    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        False,
    )
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    highway_query = text(
        base_cte
        + """
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        )
        SELECT
            rs.highway,
            COUNT(*)::bigint AS segment_count,
            COALESCE(SUM(rs.length_m), 0)::float AS length_m,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)), 0)::bigint AS count,
            COALESCE(SUM(COALESCE(ic.incident_count, 0)) / NULLIF(SUM(rs.length_m) / 1000.0, 0), 0)::float AS incidents_per_km
        FROM roads_scope rs
        LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
        GROUP BY rs.highway
        ORDER BY count DESC, rs.highway ASC
        """
    )
    crime_type_query = text(
        base_cte
        + """
        SELECT
            crime_type,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY crime_type
        ORDER BY count DESC, crime_type ASC
        """
    )
    outcome_query = text(
        base_cte
        + """
        SELECT
            outcome,
            COUNT(*)::bigint AS count
        FROM events_scope
        GROUP BY outcome
        ORDER BY count DESC, outcome ASC
        """
    )
    total_query = text(base_cte + "SELECT COUNT(*)::bigint AS total_incidents FROM events_scope")

    highway_query = _bind_roads_analytics_params(highway_query, highways, crime_types, last_outcome_categories)
    crime_type_query = _bind_roads_analytics_params(crime_type_query, highways, crime_types, last_outcome_categories)
    outcome_query = _bind_roads_analytics_params(outcome_query, highways, crime_types, last_outcome_categories)
    total_query = _bind_roads_analytics_params(total_query, highways, crime_types, last_outcome_categories)

    highway_rows = _execute(db, highway_query, query_params).mappings().all()
    crime_type_rows = _execute(db, crime_type_query, query_params).mappings().all()
    outcome_rows = _execute(db, outcome_query, query_params).mappings().all()
    total_incidents = (_execute(db, total_query, query_params).mappings().first() or {}).get("total_incidents", 0)

    by_highway = [
        {
            "highway": row["highway"],
            "segment_count": row["segment_count"],
            "length_m": row["length_m"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
            "incidents_per_km": row["incidents_per_km"],
        }
        for row in highway_rows[:limit]
    ]
    by_crime_type = [
        {
            "crime_type": row["crime_type"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
        }
        for row in crime_type_rows[:limit]
    ]
    by_outcome = [
        {
            "outcome": row["outcome"],
            "count": row["count"],
            "share": 0 if total_incidents == 0 else round((row["count"] / total_incidents) * 100.0, 2),
        }
        for row in outcome_rows[:limit]
    ]

    insights = []
    if by_highway:
        insights.append(f"{by_highway[0]['highway']} roads are the main carrier of incidents in this selection.")
    if by_crime_type:
        insights.append(f"{by_crime_type[0]['crime_type']} is the leading road-linked crime type.")
    if by_outcome:
        insights.append(f"'{by_outcome[0]['outcome']}' is the most common linked outcome.")

    return {
        "by_highway": by_highway,
        "by_crime_type": by_crime_type,
        "by_outcome": by_outcome,
        "insights": insights,
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
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    sort: str = Query("risk_score"),
    db: Session = Depends(get_db),
):
    if sort not in {"risk_score", "incidents_per_km", "incident_count"}:
        raise HTTPException(status_code=400, detail="sort must be risk_score, incidents_per_km, or incident_count")

    range_filter, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        from_month,
        to_month,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        True,
    )
    previous_filter = _matched_previous_period(range_filter)
    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    event_where_clauses, event_params = _incident_count_filters(
        range_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(event_params)
    query_params["row_limit"] = limit
    road_where_sql = _where_sql(road_where_clauses)
    event_where_sql = _where_sql(event_where_clauses)
    base_cte = _road_events_cte(road_where_sql, event_where_sql)

    previous_where_clauses, previous_params = _incident_count_filters(
        previous_filter,
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    previous_where_sql = _where_sql(previous_where_clauses)

    query = text(
        base_cte
        + f"""
        ,
        incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS incident_count
            FROM events_scope
            GROUP BY segment_id
        ),
        dominant_crime_types AS (
            SELECT
                segment_id,
                crime_type,
                COUNT(*)::bigint AS type_count,
                ROW_NUMBER() OVER (
                    PARTITION BY segment_id
                    ORDER BY COUNT(*) DESC, crime_type ASC
                ) AS rn
            FROM events_scope
            GROUP BY segment_id, crime_type
        ),
        dominant_outcomes AS (
            SELECT
                segment_id,
                outcome,
                COUNT(*)::bigint AS outcome_count,
                ROW_NUMBER() OVER (
                    PARTITION BY segment_id
                    ORDER BY COUNT(*) DESC, outcome ASC
                ) AS rn
            FROM events_scope
            GROUP BY segment_id, outcome
        ),
        previous_events_scope AS (
            SELECT
                ce.segment_id
            FROM crime_events ce
            JOIN roads_scope rs ON rs.id = ce.segment_id
            {previous_where_sql}
        ),
        previous_incident_counts AS (
            SELECT
                segment_id,
                COUNT(*)::bigint AS previous_incident_count
            FROM previous_events_scope
            GROUP BY segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m,
                COALESCE(ic.incident_count, 0)::bigint AS incident_count,
                COALESCE(COALESCE(ic.incident_count, 0) / NULLIF(GREATEST(rs.length_m, 100.0) / 1000.0, 0), 0)::float AS incidents_per_km,
                COALESCE(pic.previous_incident_count, 0)::bigint AS previous_incident_count,
                dct.crime_type AS dominant_crime_type,
                dout.outcome AS dominant_outcome
            FROM roads_scope rs
            LEFT JOIN incident_counts ic ON ic.segment_id = rs.id
            LEFT JOIN previous_incident_counts pic ON pic.segment_id = rs.id
            LEFT JOIN dominant_crime_types dct ON dct.segment_id = rs.id AND dct.rn = 1
            LEFT JOIN dominant_outcomes dout ON dout.segment_id = rs.id AND dout.rn = 1
            WHERE COALESCE(ic.incident_count, 0) > 0
        ),
        metrics AS (
            SELECT
                *,
                percent_rank() OVER (ORDER BY incident_count) AS pct_count,
                percent_rank() OVER (ORDER BY incidents_per_km) AS pct_density,
                SUM(incident_count) OVER ()::bigint AS total_incidents_in_scope
            FROM scored
        ),
        ranked AS (
            SELECT
                *,
                ROUND((((pct_count * 0.65) + (pct_density * 0.35)) * 100.0)::numeric, 2) AS risk_score
            FROM metrics
        )
        SELECT
            segment_id,
            name,
            highway,
            length_m,
            incident_count,
            incidents_per_km,
            dominant_crime_type,
            dominant_outcome,
            previous_incident_count,
            total_incidents_in_scope,
            risk_score,
            {_risk_band_expression()} AS band
        FROM ranked
        ORDER BY {_sort_expression(sort)}
        LIMIT :row_limit
        """
    )
    query = _bind_roads_analytics_params(query, highways, crime_types, last_outcome_categories)

    execution_params = dict(query_params)
    execution_params.update(previous_params)
    rows = _execute(db, query, execution_params).mappings().all()

    return {
        "items": [
            {
                "segment_id": row["segment_id"],
                "name": row["name"],
                "highway": row["highway"],
                "length_m": row["length_m"],
                "incident_count": row["incident_count"],
                "incidents_per_km": row["incidents_per_km"],
                "dominant_crime_type": row["dominant_crime_type"],
                "dominant_outcome": row["dominant_outcome"],
                "share_of_incidents": 0
                if row["total_incidents_in_scope"] in (None, 0)
                else round((row["incident_count"] / row["total_incidents_in_scope"]) * 100.0, 2),
                "previous_period_change_pct": _safe_pct_change(
                    row["incident_count"],
                    row["previous_incident_count"],
                ),
                "risk_score": row["risk_score"],
                "band": row["band"],
                "message": _risk_item_message(
                    {
                        "incident_count": row["incident_count"],
                        "dominant_crime_type": row["dominant_crime_type"],
                        "previous_period_change_pct": _safe_pct_change(
                            row["incident_count"],
                            row["previous_incident_count"],
                        ),
                    }
                ),
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
    lastOutcomeCategory: Optional[List[str]] = Query(None),
    highway: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
):
    target_month_date = _parse_month(target, "target")
    _, bbox, crime_types, last_outcome_categories, highways = _roads_analytics_filters(
        None,
        None,
        minLon,
        minLat,
        maxLon,
        maxLat,
        crimeType,
        lastOutcomeCategory,
        highway,
        False,
    )

    road_where_clauses, query_params = _roads_scope_filters(bbox, highways)
    road_where_sql = _where_sql(road_where_clauses)
    target_filters, target_params = _incident_count_filters(
        {"from_date": target_month_date, "to_date": target_month_date},
        crime_types,
        last_outcome_categories,
        alias="ce",
    )
    query_params.update(target_params)

    baseline_end_date = _shift_month(target_month_date, -1)
    baseline_start_date = _shift_month(target_month_date, -baselineMonths)
    baseline_filters, baseline_params = _incident_count_filters(
        {"from_date": baseline_start_date, "to_date": baseline_end_date},
        crime_types,
        last_outcome_categories,
        alias="ce",
    )

    target_query = text(
        f"""
        WITH roads_scope AS (
            SELECT rs.id
            FROM road_segments rs
            {road_where_sql}
        )
        SELECT COUNT(*)::bigint AS target_count
        FROM crime_events ce
        JOIN roads_scope rs ON rs.id = ce.segment_id
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
                ce.month,
                COUNT(*)::bigint AS count
            FROM crime_events ce
            JOIN roads_scope rs ON rs.id = ce.segment_id
            {_where_sql(baseline_filters)}
            GROUP BY ce.month
        )
        SELECT COALESCE(AVG(COALESCE(counts.count, 0)), 0)::float AS baseline_mean
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        """
    )

    baseline_params.update(query_params)
    baseline_params["baseline_start_date"] = baseline_start_date
    baseline_params["baseline_end_date"] = baseline_end_date

    target_query = _bind_roads_analytics_params(target_query, highways, crime_types, last_outcome_categories)
    baseline_query = _bind_roads_analytics_params(baseline_query, highways, crime_types, last_outcome_categories)

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


@router.get("/roads/{road_id}/geojson")
def get_road_geojson_by_id(road_id: int, db: Session = Depends(get_db)):
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

    return _road_geojson_feature(road)
