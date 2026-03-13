from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError

from ..schemas.tiles_schemas import TileMonthFilter, TileProfile, TileQueryParams


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
USER_REPORTED_CRIME_WEIGHT = 0.10
ANONYMOUS_USER_REPORT_WEIGHT = 0.5
REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25
USER_REPORTED_SIGNAL_CAP = 3.0


def _tile_profile(z: int) -> TileProfile:
    """Return zoom-specific road class and simplification settings for tile generation."""
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


def _validate_tile_coordinates(z: int, x: int, y: int) -> None:
    """Validate Slippy Map tile coordinates for a given zoom level."""
    max_index = (1 << z) - 1
    if x < 0 or y < 0 or x > max_index or y > max_index:
        raise HTTPException(status_code=400, detail="Tile coordinates out of range for zoom level")


def _parse_month(month: str):
    """Parse a YYYY-MM month string into a date."""
    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="month must be in YYYY-MM format") from exc


def _resolve_month_filter(
    month: Optional[str],
    startMonth: Optional[str],
    endMonth: Optional[str],
    includeRisk: bool,
) -> TileMonthFilter:
    """Build SQL month filter clauses and bind params for risk tile requests."""
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

        return TileMonthFilter(
            clause="c.month BETWEEN :start_month_date AND :end_month_date",
            params={
                "start_month_date": start_month_date,
                "end_month_date": end_month_date,
            },
        )

    if month:
        return TileMonthFilter(
            clause="c.month = :month_date",
            params={"month_date": _parse_month(month)},
        )

    if includeRisk:
        raise HTTPException(
            status_code=400,
            detail="month or startMonth/endMonth is required when includeRisk=true",
        )

    return TileMonthFilter(clause=None, params={})


def _execute(db, query, params):
    """Execute SQL with standard database-unavailable translation to HTTP 503."""
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def _build_highway_filter_clause(highways) -> str:
    """Render the static SQL clause for filtering by highway class at low zooms."""
    if not highways:
        return ""

    quoted = ", ".join(f"'{highway}'" for highway in highways)
    return f"AND rs.highway IN ({quoted})"


def _build_geom_expression(simplify_tolerance: int) -> str:
    """Return the road geometry expression with optional simplification."""
    if simplify_tolerance <= 0:
        return "rs.geom"
    return f"ST_Simplify(rs.geom, {simplify_tolerance})"


def _user_report_signal_sql() -> str:
    """Return SQL expression used to blend approved user-reported crime signal."""
    return """
        (
            :user_report_weight * LEAST(
                :user_report_signal_cap,
                distinct_authenticated_users
                + (:anonymous_report_weight * anonymous_reports)
                + (
                    :repeat_authenticated_report_weight
                    * GREATEST(authenticated_reports - distinct_authenticated_users, 0.0)
                )
            )
        )
    """


def _roads_only_tile_query(z: int):
    """Build the SQL query for base road tiles without risk overlays."""
    profile = _tile_profile(z)
    highway_filter_clause = _build_highway_filter_clause(profile["highways"])
    geom_expression = _build_geom_expression(profile["simplify_tolerance"])

    return text(
        f"""
        /* tiles_roads_only */
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


def _roads_with_risk_tile_query(z: int, month_filter_clause: str, include_crime_type_filter: bool):
    """Build the SQL query for road tiles enriched with risk and safety metrics."""
    profile = _tile_profile(z)
    highway_filter_clause = _build_highway_filter_clause(profile["highways"])
    geom_expression = _build_geom_expression(profile["simplify_tolerance"])
    crime_type_clause = ""
    user_report_month_filter_clause = month_filter_clause.replace("c.", "ure.")
    user_report_signal_sql = _user_report_signal_sql()
    if include_crime_type_filter:
        crime_type_clause = "AND c.crime_type = :crime_type"

    return text(
        f"""
        /* tiles_roads_with_risk */
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
        user_report_base AS (
            SELECT
                ure.segment_id,
                ure.month,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE {user_report_month_filter_clause}
              AND ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              {crime_type_clause.replace("c.", "urc.")}
            GROUP BY ure.segment_id, ure.month, urc.crime_type
        ),
        user_report_counts AS (
            SELECT
                segment_id,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal,
                COALESCE(SUM(authenticated_reports + anonymous_reports), 0)::bigint AS approved_user_reports
            FROM user_report_base
            GROUP BY segment_id
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
                COALESCE(cc.crimes, 0) AS official_crimes,
                COALESCE(user_report_counts.user_reported_crime_signal, 0) AS user_reported_crime_signal,
                COALESCE(user_report_counts.approved_user_reports, 0) AS approved_user_reports,
                COALESCE(cc.crimes, 0) + COALESCE(user_report_counts.user_reported_crime_signal, 0) AS crimes,
                COALESCE(col.collisions, 0) AS collisions,
                COALESCE(col.casualties, 0) AS casualties,
                COALESCE(col.fatal_casualties, 0) AS fatal_casualties,
                COALESCE(col.serious_casualties, 0) AS serious_casualties,
                COALESCE(col.slight_casualties, 0) AS slight_casualties,
                GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0 AS normalized_km,
                COALESCE(
                    (
                        COALESCE(cc.crimes, 0) + COALESCE(user_report_counts.user_reported_crime_signal, 0)
                    ) /
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
            LEFT JOIN user_report_counts ON user_report_counts.segment_id = rs.id
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
                COALESCE(road_metrics.official_crimes, 0) AS official_crimes,
                COALESCE(road_metrics.user_reported_crime_signal, 0) AS user_reported_crime_signal,
                COALESCE(road_metrics.approved_user_reports, 0) AS approved_user_reports,
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


def _build_tile_bytes(
    z: int,
    x: int,
    y: int,
    month: Optional[str],
    startMonth: Optional[str],
    endMonth: Optional[str],
    crimeType: Optional[str],
    includeRisk: bool,
    db,
) -> bytes:
    """Build vector-tile bytes for roads with optional risk overlays."""
    _validate_tile_coordinates(z, x, y)

    query_params: TileQueryParams = {
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
        "user_report_weight": USER_REPORTED_CRIME_WEIGHT,
        "anonymous_report_weight": ANONYMOUS_USER_REPORT_WEIGHT,
        "repeat_authenticated_report_weight": REPEAT_AUTHENTICATED_REPORT_WEIGHT,
        "user_report_signal_cap": USER_REPORTED_SIGNAL_CAP,
    }

    if includeRisk:
        month_filter = _resolve_month_filter(
            month=month,
            startMonth=startMonth,
            endMonth=endMonth,
            includeRisk=includeRisk,
        )
        query_params.update(month_filter.params)
        if crimeType:
            query_params["crime_type"] = crimeType

        tile_query = _roads_with_risk_tile_query(
            z=z,
            month_filter_clause=month_filter.clause or "",
            include_crime_type_filter=bool(crimeType),
        )
    else:
        tile_query = _roads_only_tile_query(z)

    tile = _execute(db, tile_query, query_params).scalar_one()
    if isinstance(tile, memoryview):
        return tile.tobytes()
    return tile or b""
