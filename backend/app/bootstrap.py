from sqlalchemy import text

from .services.auth_service import DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD, hash_password
from .db import engine


DDL_STATEMENTS = [
    "CREATE EXTENSION IF NOT EXISTS postgis",
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        is_admin BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS watchlists (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        min_lon DOUBLE PRECISION NOT NULL,
        min_lat DOUBLE PRECISION NOT NULL,
        max_lon DOUBLE PRECISION NOT NULL,
        max_lat DOUBLE PRECISION NOT NULL,
        start_month DATE,
        end_month DATE,
        crime_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        travel_mode TEXT,
        include_collisions BOOLEAN NOT NULL DEFAULT FALSE,
        baseline_months INTEGER NOT NULL DEFAULT 6,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT watchlists_bbox_lon_chk CHECK (min_lon < max_lon),
        CONSTRAINT watchlists_bbox_lat_chk CHECK (min_lat < max_lat),
        CONSTRAINT watchlists_month_range_chk CHECK (
            start_month IS NULL
            OR end_month IS NULL
            OR start_month <= end_month
        )
    )
    """,
    """
    DROP TABLE IF EXISTS watchlist_reports CASCADE
    """,
    """
    DROP TABLE IF EXISTS analytics_risk_score_snapshots CASCADE
    """,
    """
    DROP TABLE IF EXISTS analytics_risk_forecast_snapshots CASCADE
    """,
    """
    DROP TABLE IF EXISTS analytics_hotspot_stability_snapshots CASCADE
    """,
    """
    DROP TABLE IF EXISTS watchlist_analytics_runs CASCADE
    """,
    """
    CREATE TABLE IF NOT EXISTS risk_score_reference_bboxes (
        id BIGSERIAL PRIMARY KEY,
        label TEXT NOT NULL,
        min_lon DOUBLE PRECISION NOT NULL,
        min_lat DOUBLE PRECISION NOT NULL,
        max_lon DOUBLE PRECISION NOT NULL,
        max_lat DOUBLE PRECISION NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT risk_score_reference_bboxes_lon_chk CHECK (min_lon < max_lon),
        CONSTRAINT risk_score_reference_bboxes_lat_chk CHECK (min_lat < max_lat)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS risk_score_runs (
        id BIGSERIAL PRIMARY KEY,
        watchlist_id BIGINT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
        reference_bbox_id BIGINT NULL REFERENCES risk_score_reference_bboxes(id) ON DELETE SET NULL,
        min_lon DOUBLE PRECISION NOT NULL,
        min_lat DOUBLE PRECISION NOT NULL,
        max_lon DOUBLE PRECISION NOT NULL,
        max_lat DOUBLE PRECISION NOT NULL,
        start_month DATE NOT NULL,
        end_month DATE NOT NULL,
        crime_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        travel_mode TEXT NOT NULL,
        signature_key TEXT NOT NULL,
        risk_score INTEGER NOT NULL,
        band TEXT NOT NULL,
        raw_score DOUBLE PRECISION NOT NULL,
        crime_component DOUBLE PRECISION NOT NULL,
        collision_component DOUBLE PRECISION NOT NULL,
        user_component DOUBLE PRECISION NOT NULL,
        execution_time_ms DOUBLE PRECISION NOT NULL,
        comparison_basis TEXT,
        comparison_sample_size INTEGER,
        comparison_percentile DOUBLE PRECISION,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT risk_score_runs_bbox_lon_chk CHECK (min_lon < max_lon),
        CONSTRAINT risk_score_runs_bbox_lat_chk CHECK (min_lat < max_lat),
        CONSTRAINT risk_score_runs_month_range_chk CHECK (start_month <= end_month),
        CONSTRAINT risk_score_runs_travel_mode_chk CHECK (travel_mode IN ('walk', 'drive')),
        CONSTRAINT risk_score_runs_band_chk CHECK (band IN ('low', 'medium', 'high', 'very_high')),
        CONSTRAINT risk_score_runs_comparison_basis_chk
            CHECK (comparison_basis IN ('historical_same_signature', 'reference_bboxes', 'none'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_reported_events (
        id BIGSERIAL PRIMARY KEY,
        event_kind TEXT NOT NULL,
        reporter_type TEXT NOT NULL,
        user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
        event_date DATE NOT NULL,
        event_time TIME,
        month DATE NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        latitude DOUBLE PRECISION NOT NULL,
        geom geometry(Point,4326) NOT NULL,
        segment_id BIGINT,
        snap_distance_m DOUBLE PRECISION,
        description TEXT,
        admin_approved BOOLEAN NOT NULL DEFAULT FALSE,
        moderation_status TEXT NOT NULL DEFAULT 'pending',
        moderation_notes TEXT,
        moderated_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
        moderated_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT user_reported_events_kind_chk
            CHECK (event_kind IN ('crime', 'collision')),
        CONSTRAINT user_reported_events_reporter_type_chk
            CHECK (reporter_type IN ('anonymous', 'authenticated')),
        CONSTRAINT user_reported_events_reporter_user_chk
            CHECK (
                (reporter_type = 'anonymous' AND user_id IS NULL)
                OR (reporter_type = 'authenticated' AND user_id IS NOT NULL)
            ),
        CONSTRAINT user_reported_events_moderation_status_chk
            CHECK (moderation_status IN ('pending', 'approved', 'rejected')),
        CONSTRAINT user_reported_events_admin_approved_chk
            CHECK (
                (moderation_status = 'approved' AND admin_approved = TRUE)
                OR (moderation_status IN ('pending', 'rejected') AND admin_approved = FALSE)
            )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_reported_crime_details (
        event_id BIGINT PRIMARY KEY REFERENCES user_reported_events(id) ON DELETE CASCADE,
        crime_type TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_reported_collision_details (
        event_id BIGINT PRIMARY KEY REFERENCES user_reported_events(id) ON DELETE CASCADE,
        weather_condition TEXT NOT NULL,
        light_condition TEXT NOT NULL,
        number_of_vehicles INTEGER NOT NULL CHECK (number_of_vehicles >= 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collision_events (
        collision_index TEXT PRIMARY KEY,
        collision_year INTEGER NOT NULL,
        collision_date DATE NOT NULL,
        collision_time TIME,
        month DATE NOT NULL,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        geom geometry(Point,4326),
        local_authority_ons_district TEXT NOT NULL,
        lsoa_of_accident_location TEXT,
        collision_severity_code INTEGER,
        collision_severity_label TEXT,
        number_of_vehicles INTEGER,
        number_of_casualties INTEGER,
        speed_limit_code INTEGER,
        speed_limit_label TEXT,
        road_type_code INTEGER,
        road_type_label TEXT,
        light_conditions_code INTEGER,
        light_conditions_label TEXT,
        weather_conditions_code INTEGER,
        weather_conditions_label TEXT,
        road_surface_conditions_code INTEGER,
        road_surface_conditions_label TEXT,
        vehicle_count INTEGER NOT NULL DEFAULT 0,
        vehicle_type_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
        avg_driver_age NUMERIC(5,2),
        driver_sex_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
        casualty_count INTEGER NOT NULL DEFAULT 0,
        casualty_severity_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
        casualty_type_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
        fatal_casualty_count INTEGER NOT NULL DEFAULT 0,
        serious_casualty_count INTEGER NOT NULL DEFAULT 0,
        slight_casualty_count INTEGER NOT NULL DEFAULT 0,
        segment_id BIGINT,
        snap_distance_m DOUBLE PRECISION,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS segment_month_collision_stats (
        segment_id BIGINT NOT NULL,
        month DATE NOT NULL,
        collision_count INTEGER NOT NULL DEFAULT 0,
        casualty_count INTEGER NOT NULL DEFAULT 0,
        fatal_casualty_count INTEGER NOT NULL DEFAULT 0,
        serious_casualty_count INTEGER NOT NULL DEFAULT 0,
        slight_casualty_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (segment_id, month)
    )
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS start_month DATE
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS end_month DATE
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS crime_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS travel_mode TEXT
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS include_collisions BOOLEAN NOT NULL DEFAULT FALSE
    """,
    """
    ALTER TABLE watchlists
    ADD COLUMN IF NOT EXISTS baseline_months INTEGER NOT NULL DEFAULT 6
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'watchlists_month_range_chk'
        ) THEN
            ALTER TABLE watchlists
            ADD CONSTRAINT watchlists_month_range_chk CHECK (
                start_month IS NULL
                OR end_month IS NULL
                OR start_month <= end_month
            );
        END IF;
    END $$;
    """,
    "DROP TABLE IF EXISTS watchlist_preferences",
    "CREATE INDEX IF NOT EXISTS watchlists_user_id_idx ON watchlists(user_id)",
    "CREATE INDEX IF NOT EXISTS risk_score_runs_signature_created_idx ON risk_score_runs(signature_key, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS risk_score_runs_watchlist_created_idx ON risk_score_runs(watchlist_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS risk_score_runs_mode_window_created_idx ON risk_score_runs(travel_mode, start_month, end_month, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS risk_score_runs_reference_signature_created_idx ON risk_score_runs(reference_bbox_id, signature_key, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS risk_score_reference_bboxes_active_idx ON risk_score_reference_bboxes(active)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_geom_gix ON user_reported_events USING GIST (geom)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_month_idx ON user_reported_events(month)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_segment_idx ON user_reported_events(segment_id)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_user_idx ON user_reported_events(user_id)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_moderation_idx ON user_reported_events(moderation_status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_approved_month_idx ON user_reported_events(admin_approved, month)",
    "CREATE INDEX IF NOT EXISTS user_reported_events_kind_idx ON user_reported_events(event_kind)",
    "CREATE INDEX IF NOT EXISTS collision_events_geom_gix ON collision_events USING GIST (geom)",
    "CREATE INDEX IF NOT EXISTS collision_events_month_idx ON collision_events(month)",
    "CREATE INDEX IF NOT EXISTS collision_events_collision_date_idx ON collision_events(collision_date)",
    "CREATE INDEX IF NOT EXISTS collision_events_segment_idx ON collision_events(segment_id)",
    "CREATE INDEX IF NOT EXISTS collision_events_lsoa_idx ON collision_events(lsoa_of_accident_location)",
    "CREATE INDEX IF NOT EXISTS collision_events_severity_label_idx ON collision_events(collision_severity_label)",
    "CREATE INDEX IF NOT EXISTS segment_month_collision_stats_month_idx ON segment_month_collision_stats(month)",
    "CREATE INDEX IF NOT EXISTS segment_month_collision_stats_segment_idx ON segment_month_collision_stats(segment_id)",
]


def initialize_database():
    try:
        with engine.begin() as connection:
            for statement in DDL_STATEMENTS:
                connection.execute(text(statement))

            connection.execute(
                text(
                    """
                    INSERT INTO users (email, password_hash, is_admin)
                    VALUES (:email, :password_hash, TRUE)
                    ON CONFLICT (email) DO UPDATE
                    SET is_admin = TRUE
                    """
                ),
                {
                    "email": DEFAULT_ADMIN_EMAIL,
                    "password_hash": hash_password(DEFAULT_ADMIN_PASSWORD),
                },
            )
    except Exception as exc:
        print(f"Database initialization skipped: {exc}")
