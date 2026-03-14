from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import execute


def get_watchlist_for_analytics(db: Session, *, watchlist_id: int, user_id: int):
    """Fetch one user-owned watchlist with analytics inputs."""
    query = text(
        """
        SELECT
            w.id,
            w.user_id,
            w.min_lon,
            w.min_lat,
            w.max_lon,
            w.max_lat,
            w.start_month,
            w.end_month,
            w.crime_types,
            w.travel_mode
        FROM watchlists w
        WHERE w.id = :watchlist_id
          AND w.user_id = :user_id
        LIMIT 1
        """
    )
    return execute(db, query, {"watchlist_id": watchlist_id, "user_id": user_id}).mappings().first()


def compute_risk_components(
    db: Session,
    *,
    from_date,
    to_date,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_types: List[str],
    crime_decay_lambda: float,
    collision_decay_lambda: float,
    user_report_decay_lambda: float,
    crime_alpha: float,
    road_km_floor: float,
    user_report_cluster_cap: float,
    user_report_distinct_auth_weight: float,
    user_report_anonymous_weight: float,
    user_report_repeat_weight: float,
    user_crime_source_weight: float,
    user_collision_source_weight: float,
    harm_violence_and_sexual_offences: float,
    harm_robbery: float,
    harm_burglary: float,
    harm_vehicle_crime: float,
    harm_criminal_damage_and_arson: float,
    harm_default: float,
) -> Dict[str, Any]:
    """Compute raw algorithm components for one bbox/month window."""
    crime_filter_clause = ""
    user_crime_filter_clause = ""
    params: Dict[str, Any] = {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
        "from_date": from_date,
        "to_date": to_date,
        "crime_decay_lambda": crime_decay_lambda,
        "collision_decay_lambda": collision_decay_lambda,
        "user_report_decay_lambda": user_report_decay_lambda,
        "crime_alpha": crime_alpha,
        "road_km_floor": road_km_floor,
        "user_report_cluster_cap": user_report_cluster_cap,
        "user_report_distinct_auth_weight": user_report_distinct_auth_weight,
        "user_report_anonymous_weight": user_report_anonymous_weight,
        "user_report_repeat_weight": user_report_repeat_weight,
        "user_crime_source_weight": user_crime_source_weight,
        "user_collision_source_weight": user_collision_source_weight,
        "harm_violence_and_sexual_offences": harm_violence_and_sexual_offences,
        "harm_robbery": harm_robbery,
        "harm_burglary": harm_burglary,
        "harm_vehicle_crime": harm_vehicle_crime,
        "harm_criminal_damage_and_arson": harm_criminal_damage_and_arson,
        "harm_default": harm_default,
    }

    # Check if the crime types are provided.
    if crime_types:
        crime_filter_clause = "AND smts.crime_type = ANY(:crime_types)"
        user_crime_filter_clause = "AND urc.crime_type = ANY(:crime_types)"
        params["crime_types"] = crime_types

    # Construct the SQL query to compute the risk components.
    query = text(
        f"""
        WITH bbox AS (
            SELECT ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326) AS geom_4326
        ),
        scoring_window AS (
            SELECT
                CAST(:from_date AS date) AS from_date,
                CAST(:to_date AS date) AS to_date,
                (
                    (EXTRACT(YEAR FROM AGE(CAST(:to_date AS date), CAST(:from_date AS date))) * 12)
                    + EXTRACT(MONTH FROM AGE(CAST(:to_date AS date), CAST(:from_date AS date)))
                    + 1
                )::numeric AS window_months
        ),
        exposure AS (
            SELECT
                MAX(ST_Area(ST_Transform(b.geom_4326, 3857))) / 1000000.0 AS area_km2,
                COALESCE(
                    SUM(CASE WHEN rs.length_m > 0 THEN rs.length_m ELSE 0 END) / 1000.0,
                    0.0
                ) AS road_km
            FROM bbox b
            LEFT JOIN road_segments rs
              ON rs.geom_4326 && b.geom_4326
             AND ST_Intersects(rs.geom_4326, b.geom_4326)
        ),
        crime_monthly AS (
            SELECT
                smts.month,
                SUM(
                    smts.crime_count::numeric *
                    CASE smts.crime_type
                        WHEN 'Violence and sexual offences' THEN :harm_violence_and_sexual_offences
                        WHEN 'Robbery' THEN :harm_robbery
                        WHEN 'Burglary' THEN :harm_burglary
                        WHEN 'Vehicle crime' THEN :harm_vehicle_crime
                        WHEN 'Criminal damage and arson' THEN :harm_criminal_damage_and_arson
                        ELSE :harm_default
                    END
                ) AS harm_points,
                SUM(smts.crime_count)::numeric AS total_crime_count
            FROM segment_month_type_stats smts
            JOIN road_segments rs ON rs.id = smts.segment_id
            CROSS JOIN bbox b
            CROSS JOIN scoring_window sw
            WHERE smts.month BETWEEN sw.from_date AND sw.to_date
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
              {crime_filter_clause}
            GROUP BY smts.month
        ),
        crime_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        EXP(
                            -:crime_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.harm_points
                    ),
                    0.0
                ) AS weighted_harm,
                COALESCE(COUNT(*) FILTER (WHERE cm.total_crime_count > 0), 0)::numeric AS active_months
            FROM crime_monthly cm
            CROSS JOIN scoring_window sw
        ),
        collision_monthly AS (
            SELECT
                smcs.month,
                SUM(
                    COALESCE(smcs.collision_count, 0)
                    + (0.5 * COALESCE(smcs.slight_casualty_count, 0))
                    + (2.0 * COALESCE(smcs.serious_casualty_count, 0))
                    + (5.0 * COALESCE(smcs.fatal_casualty_count, 0))
                )::numeric AS collision_points
            FROM segment_month_collision_stats smcs
            JOIN road_segments rs ON rs.id = smcs.segment_id
            CROSS JOIN bbox b
            CROSS JOIN scoring_window sw
            WHERE smcs.month BETWEEN sw.from_date AND sw.to_date
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
            GROUP BY smcs.month
        ),
        collision_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        EXP(
                            -:collision_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.collision_points
                    ),
                    0.0
                ) AS weighted_collision_points
            FROM collision_monthly cm
            CROSS JOIN scoring_window sw
        ),
        user_events_crime AS (
            SELECT
                'crime'::text AS event_kind,
                ure.month,
                COALESCE(
                    ure.segment_id::text,
                    CONCAT(
                        ROUND(ure.latitude::numeric, 3)::text,
                        ':',
                        ROUND(ure.longitude::numeric, 3)::text
                    )
                ) AS cluster_key,
                ure.reporter_type,
                ure.user_id
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            CROSS JOIN scoring_window sw
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.event_kind = 'crime'
              AND ure.month BETWEEN sw.from_date AND sw.to_date
              AND ure.geom && b.geom_4326
              AND ST_Intersects(ure.geom, b.geom_4326)
              {user_crime_filter_clause}
        ),
        user_events_collision AS (
            SELECT
                'collision'::text AS event_kind,
                ure.month,
                COALESCE(
                    ure.segment_id::text,
                    CONCAT(
                        ROUND(ure.latitude::numeric, 3)::text,
                        ':',
                        ROUND(ure.longitude::numeric, 3)::text
                    )
                ) AS cluster_key,
                ure.reporter_type,
                ure.user_id
            FROM user_reported_events ure
            CROSS JOIN scoring_window sw
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.event_kind = 'collision'
              AND ure.month BETWEEN sw.from_date AND sw.to_date
              AND ure.geom && b.geom_4326
              AND ST_Intersects(ure.geom, b.geom_4326)
        ),
        user_events AS (
            SELECT * FROM user_events_crime
            UNION ALL
            SELECT * FROM user_events_collision
        ),
        user_clusters AS (
            SELECT
                ue.event_kind,
                ue.month,
                ue.cluster_key,
                COUNT(*) FILTER (WHERE ue.reporter_type = 'anonymous')::numeric AS anonymous_reports,
                COUNT(*) FILTER (WHERE ue.reporter_type = 'authenticated')::numeric AS authenticated_reports,
                COUNT(DISTINCT ue.user_id) FILTER (
                    WHERE ue.reporter_type = 'authenticated'
                )::numeric AS distinct_authenticated_users
            FROM user_events ue
            GROUP BY ue.event_kind, ue.month, ue.cluster_key
        ),
        user_cluster_scores AS (
            SELECT
                uc.event_kind,
                uc.month,
                LEAST(
                    :user_report_cluster_cap,
                    (:user_report_distinct_auth_weight * uc.distinct_authenticated_users)
                    + (:user_report_anonymous_weight * uc.anonymous_reports)
                    + (
                        :user_report_repeat_weight
                        * GREATEST(uc.authenticated_reports - uc.distinct_authenticated_users, 0)
                    )
                ) AS cluster_signal
            FROM user_clusters uc
        ),
        user_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN ucs.event_kind = 'crime' THEN :user_crime_source_weight
                            ELSE :user_collision_source_weight
                        END
                        * EXP(
                            -:user_report_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, ucs.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, ucs.month))
                                )::numeric,
                                0
                            )
                        )
                        * ucs.cluster_signal
                    ) FILTER (WHERE ucs.event_kind = 'crime'),
                    0.0
                ) AS user_crime_signal,
                COALESCE(
                    SUM(
                        CASE
                            WHEN ucs.event_kind = 'crime' THEN :user_crime_source_weight
                            ELSE :user_collision_source_weight
                        END
                        * EXP(
                            -:user_report_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, ucs.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, ucs.month))
                                )::numeric,
                                0
                            )
                        )
                        * ucs.cluster_signal
                    ) FILTER (WHERE ucs.event_kind = 'collision'),
                    0.0
                ) AS user_collision_signal
            FROM user_cluster_scores ucs
            CROSS JOIN scoring_window sw
        )
        SELECT
            ex.area_km2,
            ex.road_km,
            GREATEST(ex.road_km, :road_km_floor) AS effective_road_km,
            (
                (:crime_alpha * COALESCE(cs.weighted_harm / NULLIF(ex.area_km2, 0), 0))
                + (
                    (1.0 - :crime_alpha)
                    * COALESCE(cs.active_months / NULLIF(sw.window_months, 0), 0)
                )
            ) AS crime_component,
            COALESCE(cols.weighted_collision_points / NULLIF(GREATEST(ex.road_km, :road_km_floor), 0), 0)
                AS collision_density,
            COALESCE(us.user_crime_signal / NULLIF(ex.area_km2, 0), 0) AS user_crime_density,
            COALESCE(us.user_collision_signal / NULLIF(GREATEST(ex.road_km, :road_km_floor), 0), 0)
                AS user_collision_density
        FROM exposure ex
        CROSS JOIN scoring_window sw
        CROSS JOIN crime_summary cs
        CROSS JOIN collision_summary cols
        CROSS JOIN user_summary us
        """
    )
    # Execute the SQL query and return the result.
    row = execute(db, query, params).mappings().first() or {}
    return dict(row)


def load_historical_rows(db: Session, *, signature_key: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Load historical non-reference rows for one signature."""
    # Construct the SQL query to load the historical rows.
    query = text(
        """
        SELECT id, risk_score, created_at
        FROM risk_score_runs
        WHERE signature_key = :signature_key
          AND reference_bbox_id IS NULL
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    rows = execute(db, query, {"signature_key": signature_key, "limit": limit}).mappings().all()
    return [dict(row) for row in rows]


def nearest_reference_bboxes(db: Session, *, bbox: Dict[str, float], limit: int) -> List[Dict[str, Any]]:
    """Return nearest active reference bboxes to the current bbox center."""
    center_lon = (bbox["min_lon"] + bbox["max_lon"]) / 2.0
    center_lat = (bbox["min_lat"] + bbox["max_lat"]) / 2.0
    query = text(
        """
        SELECT
            id,
            label,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            (
                POWER(((min_lon + max_lon) / 2.0) - :center_lon, 2)
                + POWER(((min_lat + max_lat) / 2.0) - :center_lat, 2)
            ) AS distance_sq
        FROM risk_score_reference_bboxes
        WHERE active = TRUE
        ORDER BY distance_sq ASC, id ASC
        LIMIT :limit
        """
    )
    rows = execute(
        db,
        query,
        {"center_lon": center_lon, "center_lat": center_lat, "limit": limit},
    ).mappings().all()
    return [dict(row) for row in rows]


def latest_reference_score(
    db: Session,
    *,
    reference_bbox_id: int,
    signature_key: str,
) -> Optional[Dict[str, Any]]:
    """Get latest cached reference score row for one signature."""
    query = text(
        """
        SELECT id, risk_score, created_at
        FROM risk_score_runs
        WHERE reference_bbox_id = :reference_bbox_id
          AND signature_key = :signature_key
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    return execute(
        db,
        query,
        {"reference_bbox_id": reference_bbox_id, "signature_key": signature_key},
    ).mappings().first()


def insert_risk_score_run(
    db: Session,
    *,
    watchlist_id: Optional[int],
    reference_bbox_id: Optional[int],
    bbox: Dict[str, float],
    start_month,
    end_month,
    crime_types: List[str],
    travel_mode: str,
    signature_key: str,
    risk_score: int,
    band: str,
    raw_score: float,
    crime_component: float,
    collision_component: float,
    user_component: float,
    execution_time_ms: float,
    comparison_basis: Optional[str],
    comparison_sample_size: Optional[int],
    comparison_percentile: Optional[float],
):
    """Insert one analytics run row and return inserted id."""
    query = text(
        """
        INSERT INTO risk_score_runs (
            watchlist_id,
            reference_bbox_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            start_month,
            end_month,
            crime_types,
            travel_mode,
            signature_key,
            risk_score,
            band,
            raw_score,
            crime_component,
            collision_component,
            user_component,
            execution_time_ms,
            comparison_basis,
            comparison_sample_size,
            comparison_percentile
        ) VALUES (
            :watchlist_id,
            :reference_bbox_id,
            :min_lon,
            :min_lat,
            :max_lon,
            :max_lat,
            :start_month,
            :end_month,
            :crime_types,
            :travel_mode,
            :signature_key,
            :risk_score,
            :band,
            :raw_score,
            :crime_component,
            :collision_component,
            :user_component,
            :execution_time_ms,
            :comparison_basis,
            :comparison_sample_size,
            :comparison_percentile
        )
        RETURNING id
        """
    )
    return execute(
        db,
        query,
        {
            "watchlist_id": watchlist_id,
            "reference_bbox_id": reference_bbox_id,
            "min_lon": bbox["min_lon"],
            "min_lat": bbox["min_lat"],
            "max_lon": bbox["max_lon"],
            "max_lat": bbox["max_lat"],
            "start_month": start_month,
            "end_month": end_month,
            "crime_types": crime_types,
            "travel_mode": travel_mode,
            "signature_key": signature_key,
            "risk_score": risk_score,
            "band": band,
            "raw_score": raw_score,
            "crime_component": crime_component,
            "collision_component": collision_component,
            "user_component": user_component,
            "execution_time_ms": execution_time_ms,
            "comparison_basis": comparison_basis,
            "comparison_sample_size": comparison_sample_size,
            "comparison_percentile": comparison_percentile,
        },
    ).mappings().first()


def list_watchlist_risk_runs(
    db: Session,
    *,
    watchlist_id: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """List persisted risk runs for a watchlist (excluding reference rows)."""
    query = text(
        """
        SELECT
            r.id,
            r.watchlist_id,
            r.start_month,
            r.end_month,
            r.crime_types,
            r.travel_mode,
            r.risk_score,
            r.raw_score,
            r.band,
            r.crime_component,
            r.collision_component,
            r.user_component,
            r.comparison_basis,
            r.comparison_sample_size,
            r.comparison_percentile,
            r.execution_time_ms,
            r.created_at
        FROM risk_score_runs r
        WHERE r.watchlist_id = :watchlist_id
          AND r.reference_bbox_id IS NULL
        ORDER BY r.created_at DESC, r.id DESC
        LIMIT :limit
        """
    )
    rows = execute(db, query, {"watchlist_id": watchlist_id, "limit": limit}).mappings().all()
    return [dict(row) for row in rows]
