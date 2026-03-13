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
        crime_type TEXT,
        banding_mode TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
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
