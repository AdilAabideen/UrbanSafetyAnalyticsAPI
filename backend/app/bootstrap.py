from sqlalchemy import text

from .auth_utils import DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD, hash_password
from .db import engine


DDL_STATEMENTS = [
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
