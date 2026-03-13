from sqlalchemy import text

from .auth_utils import DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD, hash_password
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
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT watchlists_bbox_lon_chk CHECK (min_lon < max_lon),
        CONSTRAINT watchlists_bbox_lat_chk CHECK (min_lat < max_lat)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS watchlist_preferences (
        id BIGSERIAL PRIMARY KEY,
        watchlist_id BIGINT NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
        window_months INTEGER NOT NULL,
        crime_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        travel_mode TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'watchlist_preferences'
              AND column_name = 'banding_mode'
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'watchlist_preferences'
              AND column_name = 'travel_mode'
        ) THEN
            ALTER TABLE watchlist_preferences RENAME COLUMN banding_mode TO travel_mode;
        END IF;
    END $$;
    """,
    """
    ALTER TABLE watchlist_preferences
    ADD COLUMN IF NOT EXISTS crime_types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'watchlist_preferences'
              AND column_name = 'crime_type'
        ) THEN
            UPDATE watchlist_preferences
            SET crime_types = CASE
                WHEN crime_type IS NULL OR BTRIM(crime_type) = '' THEN ARRAY[]::TEXT[]
                ELSE ARRAY[crime_type]
            END
            WHERE crime_types = ARRAY[]::TEXT[];

            ALTER TABLE watchlist_preferences DROP COLUMN crime_type;
        END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS watchlists_user_id_idx ON watchlists(user_id)",
    "CREATE INDEX IF NOT EXISTS watchlist_preferences_watchlist_id_idx ON watchlist_preferences(watchlist_id)",
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
