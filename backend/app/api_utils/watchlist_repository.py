from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from ..errors import DependencyError
from ..db import execute


def get_watchlist_by_id(db: Session, watchlist_id: int, user_id: int):
    """Fetch one watchlist row for a user."""
    # Construct the SQL query to fetch the watchlist row.
    query = text(
        """
        SELECT
            w.id,
            w.user_id,
            w.name,
            w.min_lon,
            w.min_lat,
            w.max_lon,
            w.max_lat,
            w.start_month,
            w.end_month,
            w.crime_types,
            w.travel_mode,
            w.include_collisions,
            w.baseline_months,
            w.created_at
        FROM watchlists w
        WHERE w.id = :watchlist_id AND w.user_id = :user_id
        LIMIT 1
        """
    )
    return execute(db, query, {"watchlist_id": watchlist_id, "user_id": user_id}).mappings().first()


def list_watchlists_by_user(db: Session, user_id: int):
    """List all watchlists for one user."""
    query = text(
        """
        SELECT
            w.id,
            w.user_id,
            w.name,
            w.min_lon,
            w.min_lat,
            w.max_lon,
            w.max_lat,
            w.start_month,
            w.end_month,
            w.crime_types,
            w.travel_mode,
            w.include_collisions,
            w.baseline_months,
            w.created_at
        FROM watchlists w
        WHERE w.user_id = :user_id
        ORDER BY w.created_at DESC, w.id DESC
        """
    )
    return execute(db, query, {"user_id": user_id}).mappings().all()


def insert_watchlist(
    db: Session,
    *,
    user_id: int,
    name: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
):
    """Insert a watchlist and return its id."""
    query = text(
        """
        INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)
        VALUES (:user_id, :name, :min_lon, :min_lat, :max_lon, :max_lat)
        RETURNING id
        """
    )
    return execute(
        db,
        query,
        {
            "user_id": user_id,
            "name": name,
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
        },
    ).mappings().first()


def update_watchlist_fields(
    db: Session,
    *,
    watchlist_id: int,
    user_id: int,
    update_fields: List[str],
    query_params: Dict[str, Any],
):
    """Update mutable base watchlist fields and return the updated id row."""
    query = text(
        f"""
        UPDATE watchlists
        SET {", ".join(update_fields)}
        WHERE id = :watchlist_id AND user_id = :user_id
        RETURNING id
        """
    )
    params = dict(query_params)
    params["watchlist_id"] = watchlist_id
    params["user_id"] = user_id
    return execute(db, query, params).mappings().first()


def update_watchlist_preference(
    db: Session,
    *,
    watchlist_id: int,
    user_id: int,
    start_month,
    end_month,
    crime_types,
    travel_mode: str,
    include_collisions: bool,
    baseline_months: int,
):
    """Update preference columns stored on watchlists table."""
    query = text(
        """
        UPDATE watchlists
        SET
            start_month = :start_month,
            end_month = :end_month,
            crime_types = :crime_types,
            travel_mode = :travel_mode,
            include_collisions = :include_collisions,
            baseline_months = :baseline_months
        WHERE id = :watchlist_id AND user_id = :user_id
        RETURNING id
        """
    )
    return execute(
        db,
        query,
        {
            "start_month": start_month,
            "end_month": end_month,
            "crime_types": crime_types,
            "travel_mode": travel_mode,
            "include_collisions": include_collisions,
            "baseline_months": baseline_months,
            "watchlist_id": watchlist_id,
            "user_id": user_id,
        },
    ).mappings().first()


def delete_watchlist_row(db: Session, *, watchlist_id: int, user_id: int):
    """Delete one watchlist row and return deleted id row."""
    query = text(
        """
        DELETE FROM watchlists
        WHERE id = :watchlist_id AND user_id = :user_id
        RETURNING id
        """
    )
    return execute(db, query, {"watchlist_id": watchlist_id, "user_id": user_id}).mappings().first()
