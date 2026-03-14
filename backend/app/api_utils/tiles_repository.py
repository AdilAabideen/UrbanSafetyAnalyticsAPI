# Tiles DB Utils.py
from sqlalchemy import text

from ..schemas.tiles_schemas import TileProfile


# Builds the SQL query for base road tiles without risk overlays.
def roads_only_tile_query(z: int):
    """Build SQL for base road tiles without risk overlays."""
    profile = tile_profile(z)
    highway_filter_clause = build_highway_filter_clause(profile["highways"])
    geom_expression = build_geom_expression(profile["simplify_tolerance"])

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


# Builds the Profile for what will be returned based on the zoom level.
def tile_profile(z: int) -> TileProfile:
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


# Builds the SQL clause for filtering by highway class at low zooms.
def build_highway_filter_clause(highways) -> str:
    """Build the SQL clause for filtering by highway class at low zooms."""
    if not highways:
        return ""
    quoted = ", ".join(f"'{highway}'" for highway in highways)
    return f"AND rs.highway IN ({quoted})"


# Builds the geometry expression for the road tiles.
def build_geom_expression(simplify_tolerance: int) -> str:
    """Return the road geometry expression with optional simplification."""
    if simplify_tolerance <= 0:
        return "rs.geom"
    return f"ST_Simplify(rs.geom, {simplify_tolerance})"


# Builds the SQL query for road tiles enriched with risk and safety metrics.
def roads_with_risk_tile_query(
    z: int,
):
    """Build SQL for road tiles enriched with risk and safety metrics."""

    # Build the profile for the tile.
    profile = tile_profile(z)
    highway_filter_clause = build_highway_filter_clause(profile["highways"])
    geom_expression = build_geom_expression(profile["simplify_tolerance"])

    return text(
        f"""
        /* tiles_roads_with_risk_v2 */
        WITH bounds AS (
            SELECT ST_TileEnvelope(:z, :x, :y) AS geom
        ),
        scoring_window AS (
            SELECT
                CAST(:start_month_date AS date) AS start_month_date,
                CAST(:end_month_date AS date) AS end_month_date,
                (
                    (EXTRACT(YEAR FROM AGE(CAST(:end_month_date AS date), CAST(:start_month_date AS date))) * 12)
                    + EXTRACT(MONTH FROM AGE(CAST(:end_month_date AS date), CAST(:start_month_date AS date)))
                    + 1
                )::numeric AS window_months
        ),
        crime_monthly AS (
            SELECT
                c.segment_id,
                c.month,
                SUM(
                    c.crime_count::numeric *
                    CASE c.crime_type
                        WHEN 'Anti-social behaviour' THEN :harm_anti_social_behaviour
                        WHEN 'Bicycle theft' THEN :harm_bicycle_theft
                        WHEN 'Burglary' THEN :harm_burglary
                        WHEN 'Criminal damage and arson' THEN :harm_criminal_damage_and_arson
                        WHEN 'Drugs' THEN :harm_drugs
                        WHEN 'Other crime' THEN :harm_other_crime
                        WHEN 'Other theft' THEN :harm_other_theft
                        WHEN 'Possession of weapons' THEN :harm_possession_of_weapons
                        WHEN 'Public order' THEN :harm_public_order
                        WHEN 'Robbery' THEN :harm_robbery
                        WHEN 'Shoplifting' THEN :harm_shoplifting
                        WHEN 'Theft from the person' THEN :harm_theft_from_the_person
                        WHEN 'Vehicle crime' THEN :harm_vehicle_crime
                        WHEN 'Violence and sexual offences' THEN :harm_violence_and_sexual_offences
                        ELSE :harm_default
                    END
                ) AS harm_points,
                SUM(c.crime_count)::numeric AS total_crime_count
            FROM segment_month_type_stats c
            CROSS JOIN scoring_window sw
            WHERE :include_crime
              AND c.month BETWEEN sw.start_month_date AND sw.end_month_date
              AND c.segment_id IS NOT NULL
            GROUP BY c.segment_id, c.month
        ),
        crime_component AS (
            SELECT
                cm.segment_id,
                COALESCE(
                    SUM(
                        EXP(
                            -:crime_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.end_month_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.end_month_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.harm_points
                    ) / NULLIF(GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0, 0),
                    0
                ) AS harm_density,
                COALESCE(
                    COUNT(*) FILTER (WHERE cm.total_crime_count > 0)::numeric / NULLIF(sw.window_months, 0),
                    0
                ) AS persistence
            FROM crime_monthly cm
            JOIN road_segments rs ON rs.id = cm.segment_id
            CROSS JOIN scoring_window sw
            GROUP BY cm.segment_id, rs.length_m, sw.window_months, sw.end_month_date
        ),
        crime_component_risk AS (
            SELECT
                cc.segment_id,
                cc.harm_density,
                cc.persistence,
                ((:crime_alpha * cc.harm_density) + ((1.0 - :crime_alpha) * cc.persistence)) AS crime_risk
            FROM crime_component cc
        ),
        collision_monthly AS (
            SELECT
                c.segment_id,
                c.month,
                (
                    COALESCE(c.collision_count, 0)
                    + (COALESCE(c.slight_casualty_count, 0) * :slight_casualty_weight)
                    + (COALESCE(c.serious_casualty_count, 0) * :serious_casualty_weight)
                    + (COALESCE(c.fatal_casualty_count, 0) * :fatal_casualty_weight)
                )::numeric AS collision_severity_points
            FROM segment_month_collision_stats c
            CROSS JOIN scoring_window sw
            WHERE :include_collisions
              AND c.month BETWEEN sw.start_month_date AND sw.end_month_date
              AND c.segment_id IS NOT NULL
        ),
        road_context AS (
            SELECT
                rs.id AS segment_id,
                CASE
                    WHEN rs.highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link')
                        THEN :road_class_high_factor
                    WHEN rs.highway IN ('secondary', 'secondary_link', 'tertiary', 'tertiary_link')
                        THEN :road_class_medium_factor
                    ELSE :road_class_default_factor
                END AS road_class_factor,
                CASE
                    WHEN :include_collisions THEN
                        1.0 + LEAST(
                            :junction_factor_cap,
                            COALESCE(
                                (
                                    SELECT COUNT(*)::numeric
                                    FROM road_segments rj
                                    WHERE rj.id <> rs.id
                                      AND (
                                            ST_DWithin(ST_StartPoint(rs.geom), rj.geom, :junction_distance_m)
                                         OR ST_DWithin(ST_EndPoint(rs.geom), rj.geom, :junction_distance_m)
                                      )
                                ),
                                0
                            ) * :junction_degree_weight
                        )
                    ELSE 1.0
                END AS junction_factor,
                CASE
                    WHEN :include_collisions THEN
                        1.0 + LEAST(
                            :curve_factor_cap,
                            GREATEST(
                                (
                                    CASE
                                        WHEN ST_Distance(ST_StartPoint(rs.geom), ST_EndPoint(rs.geom)) > 0
                                            THEN ST_Length(rs.geom) /
                                                ST_Distance(ST_StartPoint(rs.geom), ST_EndPoint(rs.geom))
                                        ELSE 1.0
                                    END
                                ) - 1.0,
                                0
                            ) * :curve_weight
                        )
                    ELSE 1.0
                END AS curve_factor
            FROM road_segments rs
        ),
        collision_component AS (
            SELECT
                cm.segment_id,
                COALESCE(
                    SUM(
                        EXP(
                            -:collision_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.end_month_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.end_month_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.collision_severity_points
                    ) / NULLIF(GREATEST(COALESCE(rs.length_m, 0), :risk_length_floor_m) / 1000.0, 0),
                    0
                ) AS collision_density
            FROM collision_monthly cm
            JOIN road_segments rs ON rs.id = cm.segment_id
            CROSS JOIN scoring_window sw
            GROUP BY cm.segment_id, rs.length_m, sw.end_month_date
        ),
        collision_component_risk AS (
            SELECT
                cc.segment_id,
                cc.collision_density,
                (
                    cc.collision_density
                    * COALESCE(rc.road_class_factor, 1.0)
                    * COALESCE(rc.junction_factor, 1.0)
                    * COALESCE(rc.curve_factor, 1.0)
                ) AS collision_risk
            FROM collision_component cc
            LEFT JOIN road_context rc ON rc.segment_id = cc.segment_id
        ),
        user_reports_filtered AS (
            SELECT
                ure.id,
                ure.segment_id,
                ure.event_date,
                ure.month,
                ure.reporter_type,
                ure.user_id,
                urc.crime_type
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            CROSS JOIN scoring_window sw
            WHERE :include_user_reports
              AND ure.event_kind = 'crime'
              AND ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.month BETWEEN sw.start_month_date AND sw.end_month_date
        ),
        user_report_targets AS (
            SELECT
                ur.id AS event_id,
                ur.event_date,
                ur.month,
                ur.reporter_type,
                ur.user_id,
                ur.crime_type,
                targets.target_segment_id
            FROM user_reports_filtered ur
            JOIN road_segments rs_base ON rs_base.id = ur.segment_id
            JOIN LATERAL (
                SELECT DISTINCT rn.id AS target_segment_id
                FROM road_segments rn
                WHERE rn.id = ur.segment_id
                   OR ST_DWithin(rn.geom, rs_base.geom, :adjacent_segment_distance_m)
            ) targets ON TRUE
        ),
        user_report_clusters AS (
            SELECT
                urt.target_segment_id AS segment_id,
                urt.event_date,
                urt.month,
                urt.crime_type,
                COUNT(DISTINCT urt.user_id) FILTER (
                    WHERE urt.reporter_type = 'authenticated'
                )::numeric AS distinct_authenticated_users,
                COUNT(*) FILTER (
                    WHERE urt.reporter_type = 'anonymous'
                )::numeric AS anonymous_reports,
                COUNT(*) FILTER (
                    WHERE urt.reporter_type = 'authenticated'
                )::numeric AS authenticated_reports
            FROM user_report_targets urt
            GROUP BY urt.target_segment_id, urt.event_date, urt.month, urt.crime_type
        ),
        user_report_component AS (
            SELECT
                urc.segment_id,
                COALESCE(
                    SUM(
                        EXP(
                            -:user_report_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.end_month_date, urc.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.end_month_date, urc.month))
                                )::numeric,
                                0
                            )
                        ) * LEAST(
                            :user_report_cluster_cap,
                            (:user_report_distinct_auth_weight * urc.distinct_authenticated_users)
                            + (:user_report_anonymous_weight * urc.anonymous_reports)
                            + (
                                :user_report_repeat_weight * GREATEST(
                                    urc.authenticated_reports - urc.distinct_authenticated_users,
                                    0
                                )
                            )
                        )
                    ),
                    0
                ) AS user_report_risk
            FROM user_report_clusters urc
            CROSS JOIN scoring_window sw
            GROUP BY urc.segment_id, sw.end_month_date
        ),
        road_metrics AS (
            SELECT
                rs.id AS segment_id,
                COALESCE(cr.crime_risk, 0) AS crime_risk,
                COALESCE(col.collision_risk, 0) AS collision_risk,
                COALESCE(ur.user_report_risk, 0) AS user_report_risk
            FROM road_segments rs
            LEFT JOIN crime_component_risk cr ON cr.segment_id = rs.id
            LEFT JOIN collision_component_risk col ON col.segment_id = rs.id
            LEFT JOIN user_report_component ur ON ur.segment_id = rs.id
        ),
        active_roads AS (
            SELECT *
            FROM road_metrics
            WHERE (
                (:include_crime AND crime_risk > 0)
                OR (:include_collisions AND collision_risk > 0)
                OR (:include_user_reports AND user_report_risk > 0)
            )
        ),
        ranked_scores AS (
            SELECT
                segment_id,
                CASE
                    WHEN :include_crime THEN percent_rank() OVER (ORDER BY crime_risk)
                    ELSE NULL
                END AS crime_pct,
                CASE
                    WHEN :include_collisions THEN percent_rank() OVER (ORDER BY collision_risk)
                    ELSE NULL
                END AS collision_pct,
                CASE
                    WHEN :include_user_reports THEN percent_rank() OVER (ORDER BY user_report_risk)
                    ELSE NULL
                END AS user_report_pct
            FROM active_roads
        ),
        combined_scores AS (
            SELECT
                segment_id,
                (
                    (
                        COALESCE(crime_pct, 0) * CASE WHEN :include_crime THEN :crime_component_weight ELSE 0 END
                    )
                    + (
                        COALESCE(collision_pct, 0) * CASE WHEN :include_collisions THEN :collision_component_weight ELSE 0 END
                    )
                    + (
                        COALESCE(user_report_pct, 0) * CASE WHEN :include_user_reports THEN :user_report_component_weight ELSE 0 END
                    )
                ) / NULLIF(
                    (CASE WHEN :include_crime THEN :crime_component_weight ELSE 0 END)
                    + (CASE WHEN :include_collisions THEN :collision_component_weight ELSE 0 END)
                    + (CASE WHEN :include_user_reports THEN :user_report_component_weight ELSE 0 END),
                    0
                ) AS pct
            FROM ranked_scores
        ),
        scored_combined AS (
            SELECT
                segment_id,
                pct,
                ROUND((pct * 100.0)::numeric, 2) AS raw_safety_score
            FROM combined_scores
        ),
        normalized_scores AS (
            SELECT
                segment_id,
                percent_rank() OVER (ORDER BY raw_safety_score) AS pct,
                ROUND((percent_rank() OVER (ORDER BY raw_safety_score) * 100.0)::numeric, 2) AS risk_score
            FROM scored_combined
        )
        SELECT COALESCE(ST_AsMVT(mvt, 'roads', :extent, 'geom'), ''::bytea) AS tile
        FROM (
            SELECT
                rs.id AS segment_id,
                rs.highway,
                rs.name,
                COALESCE(normalized_scores.risk_score, 0) AS risk_score,
                CASE
                    WHEN COALESCE(normalized_scores.risk_score, 0) >= :risk_band_red_threshold THEN 'red'
                    WHEN COALESCE(normalized_scores.risk_score, 0) >= :risk_band_orange_threshold THEN 'orange'
                    ELSE 'green'
                END AS band,
                ST_AsMVTGeom({geom_expression}, bounds.geom, :extent, :buffer, true) AS geom
            FROM road_segments rs
            CROSS JOIN bounds
            LEFT JOIN normalized_scores ON normalized_scores.segment_id = rs.id
            WHERE rs.geom && bounds.geom
              {highway_filter_clause}
        ) AS mvt
        WHERE geom IS NOT NULL
        """
    )
