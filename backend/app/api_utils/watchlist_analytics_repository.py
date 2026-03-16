import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import execute


def _coerce_json_array(value: Any) -> List[Dict[str, Any]]:
    """Normalize a JSON/JSONB result into a Python list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


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


def fetch_watchlist_basic_metrics(
    db: Session,
    *,
    start_month,
    end_month,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_types: List[str],
) -> Dict[str, Any]:
    """
    Return a lightweight watchlist analytics block:
    - total crimes
    - total collisions
    - total approved user-reported events
    - top 5 dangerous roads
    - crime category breakdown
    """
    crime_filter_clause = ""
    params: Dict[str, Any] = {
        "start_month": start_month,
        "end_month": end_month,
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }
    if crime_types:
        crime_filter_clause = "AND smts.crime_type = ANY(:crime_types)"
        params["crime_types"] = crime_types

    query = text(
        f"""
        WITH bbox AS (
            SELECT ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326) AS geom_4326
        ),
        crime_segments AS (
            SELECT
                smts.segment_id,
                SUM(smts.crime_count)::bigint AS crime_count
            FROM segment_month_type_stats smts
            JOIN road_segments rs ON rs.id = smts.segment_id
            CROSS JOIN bbox b
            WHERE smts.month BETWEEN :start_month AND :end_month
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
              {crime_filter_clause}
            GROUP BY smts.segment_id
        ),
        collision_segments AS (
            SELECT
                smcs.segment_id,
                SUM(smcs.collision_count)::bigint AS collision_count
            FROM segment_month_collision_stats smcs
            JOIN road_segments rs ON rs.id = smcs.segment_id
            CROSS JOIN bbox b
            WHERE smcs.month BETWEEN :start_month AND :end_month
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
            GROUP BY smcs.segment_id
        ),
        user_segment_counts AS (
            SELECT
                ure.segment_id,
                COUNT(*)::bigint AS user_reported_event_count
            FROM user_reported_events ure
            JOIN road_segments rs ON rs.id = ure.segment_id
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.month BETWEEN :start_month AND :end_month
              AND ure.segment_id IS NOT NULL
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
            GROUP BY ure.segment_id
        ),
        user_event_total AS (
            SELECT
                COALESCE(COUNT(*), 0)::bigint AS number_of_user_reported_events
            FROM user_reported_events ure
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.month BETWEEN :start_month AND :end_month
              AND (
                    (ure.geom IS NOT NULL AND ure.geom && b.geom_4326 AND ST_Intersects(ure.geom, b.geom_4326))
                 OR (ure.longitude BETWEEN :min_lon AND :max_lon AND ure.latitude BETWEEN :min_lat AND :max_lat)
              )
        ),
        crime_category_breakdown AS (
            SELECT
                smts.crime_type,
                SUM(smts.crime_count)::bigint AS count
            FROM segment_month_type_stats smts
            JOIN road_segments rs ON rs.id = smts.segment_id
            CROSS JOIN bbox b
            WHERE smts.month BETWEEN :start_month AND :end_month
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
              {crime_filter_clause}
            GROUP BY smts.crime_type
        ),
        top_roads AS (
            SELECT
                rs.id AS segment_id,
                COALESCE(NULLIF(rs.name, ''), 'Unnamed road') AS road_name,
                COALESCE(cs.crime_count, 0)::bigint AS crime_count,
                COALESCE(cols.collision_count, 0)::bigint AS collision_count,
                COALESCE(us.user_reported_event_count, 0)::bigint AS user_reported_event_count,
                (
                    COALESCE(cs.crime_count, 0)::numeric
                    + (COALESCE(cols.collision_count, 0)::numeric * 2.0)
                    + (COALESCE(us.user_reported_event_count, 0)::numeric * 1.5)
                ) AS danger_score
            FROM road_segments rs
            CROSS JOIN bbox b
            LEFT JOIN crime_segments cs ON cs.segment_id = rs.id
            LEFT JOIN collision_segments cols ON cols.segment_id = rs.id
            LEFT JOIN user_segment_counts us ON us.segment_id = rs.id
            WHERE rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
              AND (
                    COALESCE(cs.crime_count, 0) > 0
                 OR COALESCE(cols.collision_count, 0) > 0
                 OR COALESCE(us.user_reported_event_count, 0) > 0
              )
            ORDER BY danger_score DESC, crime_count DESC, collision_count DESC, user_reported_event_count DESC, rs.id
            LIMIT 5
        )
        SELECT
            COALESCE((SELECT SUM(crime_count) FROM crime_segments), 0)::bigint AS number_of_crimes,
            COALESCE((SELECT SUM(collision_count) FROM collision_segments), 0)::bigint AS number_of_collisions,
            COALESCE((SELECT number_of_user_reported_events FROM user_event_total), 0)::bigint
                AS number_of_user_reported_events,
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'segment_id', tr.segment_id,
                            'road_name', tr.road_name,
                            'danger_score', ROUND(tr.danger_score::numeric, 2),
                            'crime_count', tr.crime_count,
                            'collision_count', tr.collision_count,
                            'user_reported_event_count', tr.user_reported_event_count
                        )
                    )
                    FROM top_roads tr
                ),
                '[]'::json
            ) AS most_dangerous_roads,
            COALESCE(
                (
                    SELECT json_agg(
                        json_build_object(
                            'crime_type', ccb.crime_type,
                            'count', ccb.count
                        )
                        ORDER BY ccb.count DESC, ccb.crime_type ASC
                    )
                    FROM crime_category_breakdown ccb
                ),
                '[]'::json
            ) AS crime_category_breakdown
        """
    )
    row = execute(db, query, params).mappings().first() or {}
    return {
        "number_of_crimes": int(row.get("number_of_crimes") or 0),
        "number_of_collisions": int(row.get("number_of_collisions") or 0),
        "number_of_user_reported_events": int(row.get("number_of_user_reported_events") or 0),
        "most_dangerous_roads": _coerce_json_array(row.get("most_dangerous_roads")),
        "crime_category_breakdown": _coerce_json_array(row.get("crime_category_breakdown")),
    }


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
        crime_totals AS (
            SELECT
                COALESCE(SUM(cm.total_crime_count), 0)::bigint AS official_crime_count
            FROM crime_monthly cm
        ),
        collision_monthly AS (
            SELECT
                smcs.month,
                SUM(COALESCE(smcs.collision_count, 0))::bigint AS total_collision_count,
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
        collision_totals AS (
            SELECT
                COALESCE(SUM(cm.total_collision_count), 0)::bigint AS collision_count
            FROM collision_monthly cm
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
        ),
        user_event_totals AS (
            SELECT
                COALESCE(COUNT(*), 0)::bigint AS approved_user_report_count
            FROM user_events
        )
        SELECT
            sw.from_date AS start_month,
            sw.to_date AS end_month,
            COALESCE(sw.window_months, 0)::integer AS months_in_window,
            ex.area_km2,
            ex.road_km,
            GREATEST(ex.road_km, :road_km_floor) AS effective_road_km,
            ct.official_crime_count,
            colt.collision_count,
            ut.approved_user_report_count,
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
        CROSS JOIN crime_totals ct
        CROSS JOIN collision_summary cols
        CROSS JOIN collision_totals colt
        CROSS JOIN user_summary us
        CROSS JOIN user_event_totals ut
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


def fetch_forecast_baseline_rows(
    db: Session,
    *,
    baseline_from_date,
    baseline_to_date,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_types: List[str],
) -> List[Dict[str, Any]]:
    """
    Load monthly baseline rows for recency-weighted watchlist forecasting.

    Each monthly row includes:
    - official crime count
    - user-reported crime signal
    - blended crime_count
    - collision_count
    - collision_points
    """
    crime_type_clause = ""
    user_crime_type_clause = ""
    params: Dict[str, Any] = {
        "baseline_from_date": baseline_from_date,
        "baseline_to_date": baseline_to_date,
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }
    if crime_types:
        crime_type_clause = "AND ce.crime_type = ANY(:crime_types)"
        user_crime_type_clause = "AND urc.crime_type = ANY(:crime_types)"
        params["crime_types"] = crime_types

    query = text(
        f"""
        /* watchlist_forecast_monthly_baseline */
        WITH months AS (
            SELECT generate_series(
                CAST(:baseline_from_date AS date),
                CAST(:baseline_to_date AS date),
                interval '1 month'
            )::date AS month
        ),
        official_crime AS (
            SELECT
                ce.month,
                COUNT(*)::double precision AS official_crime_count
            FROM crime_events ce
            WHERE ce.geom IS NOT NULL
              AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ce.lon BETWEEN :min_lon AND :max_lon
              AND ce.lat BETWEEN :min_lat AND :max_lat
              AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              {crime_type_clause}
            GROUP BY ce.month
        ),
        user_crime_monthly AS (
            SELECT
                ure.month,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.event_kind = 'crime'
              AND ure.admin_approved = TRUE
              AND ure.geom IS NOT NULL
              AND ure.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ure.longitude BETWEEN :min_lon AND :max_lon
              AND ure.latitude BETWEEN :min_lat AND :max_lat
              AND ure.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              {user_crime_type_clause}
            GROUP BY ure.month
        ),
        user_crime_signal AS (
            SELECT
                month,
                (
                    0.10 * LEAST(
                        3.0,
                        distinct_authenticated_users
                        + (0.5 * anonymous_reports)
                        + (0.25 * GREATEST(authenticated_reports - distinct_authenticated_users, 0))
                    )
                )::double precision AS user_reported_crime_signal
            FROM user_crime_monthly
        ),
        collision_monthly AS (
            SELECT
                ce.month,
                COUNT(*)::bigint AS collision_count,
                COALESCE(
                    SUM(
                        1.0
                        + (0.5 * COALESCE(ce.slight_casualty_count, 0))
                        + (2.0 * COALESCE(ce.serious_casualty_count, 0))
                        + (5.0 * COALESCE(ce.fatal_casualty_count, 0))
                    ),
                    0.0
                )::double precision AS collision_points
            FROM collision_events ce
            WHERE ce.geom IS NOT NULL
              AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ce.longitude BETWEEN :min_lon AND :max_lon
              AND ce.latitude BETWEEN :min_lat AND :max_lat
              AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            GROUP BY ce.month
        )
        SELECT
            TO_CHAR(months.month, 'YYYY-MM') AS month,
            COALESCE(official_crime.official_crime_count, 0.0) AS official_crime_count,
            COALESCE(user_crime_signal.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
            (
                COALESCE(official_crime.official_crime_count, 0.0)
                + COALESCE(user_crime_signal.user_reported_crime_signal, 0.0)
            ) AS crime_count,
            COALESCE(collision_monthly.collision_count, 0)::bigint AS collision_count,
            COALESCE(collision_monthly.collision_points, 0.0) AS collision_points
        FROM months
        LEFT JOIN official_crime ON official_crime.month = months.month
        LEFT JOIN user_crime_signal ON user_crime_signal.month = months.month
        LEFT JOIN collision_monthly ON collision_monthly.month = months.month
        ORDER BY months.month ASC
        """
    )
    rows = execute(db, query, params).mappings().all()
    return [dict(row) for row in rows]
