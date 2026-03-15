from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from ..errors import DependencyError
from ..db import execute


def get_user_by_id(db: Session, user_id: int):
    """Fetch a user row by id for auth resolution."""
    query = text(
        """
        SELECT
            u.id,
            u.email,
            u.is_admin,
            u.created_at
        FROM users u
        WHERE u.id = :user_id
        LIMIT 1
        """
    )
    return execute(db, query, {"user_id": user_id}).mappings().first()


def snap_to_segment(db: Session, longitude: float, latitude: float):
    """Return nearest road segment candidate and snap distance."""
    query = text(
        """
        SELECT
            rs.id,
            ST_Distance(
                rs.geom_4326::geography,
                ST_SetSRID(ST_Point(:longitude, :latitude), 4326)::geography
            ) AS snap_distance_m
        FROM road_segments rs
        ORDER BY rs.geom_4326 <-> ST_SetSRID(ST_Point(:longitude, :latitude), 4326)
        LIMIT 1
        """
    )
    row = execute(db, query, {"longitude": longitude, "latitude": latitude}).mappings().first()
    if not row:
        return None
    return {
        "segment_id": row["id"],
        "snap_distance_m": float(row["snap_distance_m"]) if row["snap_distance_m"] is not None else None,
    }


def report_select_sql() -> str:
    """Shared base SELECT used by report retrieval/listing queries."""
    return """
        SELECT
            e.id,
            e.event_kind,
            e.reporter_type,
            e.user_id,
            u.email AS reporter_email,
            e.event_date,
            e.event_time,
            e.month,
            e.longitude,
            e.latitude,
            e.segment_id,
            e.snap_distance_m,
            e.description,
            e.admin_approved,
            e.moderation_status,
            e.moderation_notes,
            e.moderated_by,
            e.moderated_at,
            e.created_at,
            cd.crime_type,
            cl.weather_condition,
            cl.light_condition,
            cl.number_of_vehicles
        FROM user_reported_events e
        LEFT JOIN users u ON u.id = e.user_id
        LEFT JOIN user_reported_crime_details cd ON cd.event_id = e.id
        LEFT JOIN user_reported_collision_details cl ON cl.event_id = e.id
    """


def insert_report_base(
    db: Session,
    *,
    event_kind: str,
    reporter_type: str,
    user_id: Optional[int],
    event_date: date,
    event_time: Optional[time],
    month: date,
    longitude: float,
    latitude: float,
    segment_id: Optional[int],
    snap_distance_m: Optional[float],
    description: Optional[str],
) -> int:
    """Insert base reported event row and return the new id."""
    query = text(
        """
        INSERT INTO user_reported_events (
            event_kind,
            reporter_type,
            user_id,
            event_date,
            event_time,
            month,
            longitude,
            latitude,
            geom,
            segment_id,
            snap_distance_m,
            description
        )
        VALUES (
            :event_kind,
            :reporter_type,
            :user_id,
            :event_date,
            :event_time,
            :month,
            :longitude,
            :latitude,
            ST_SetSRID(ST_Point(:longitude, :latitude), 4326),
            :segment_id,
            :snap_distance_m,
            :description
        )
        RETURNING id
        """
    )
    # Execute the query and return the row.
    row = execute(
        db,
        query,
        {
            "event_kind": event_kind,
            "reporter_type": reporter_type,
            "user_id": user_id,
            "event_date": event_date,
            "event_time": event_time,
            "month": month,
            "longitude": longitude,
            "latitude": latitude,
            "segment_id": segment_id,
            "snap_distance_m": snap_distance_m,
            "description": description,
        },
    ).mappings().first()
    return int(row["id"])


def insert_report_crime_details(db: Session, *, event_id: int, crime_type: str) -> None:
    """Insert crime-specific report detail row."""
    query = text(
        """
        INSERT INTO user_reported_crime_details (event_id, crime_type)
        VALUES (:event_id, :crime_type)
        """
    )
    execute(db, query, {"event_id": event_id, "crime_type": crime_type})


def insert_report_collision_details(
    db: Session,
    *,
    event_id: int,
    weather_condition: str,
    light_condition: str,
    number_of_vehicles: int,
) -> None:
    """Insert collision-specific report detail row."""
    query = text(
        """
        INSERT INTO user_reported_collision_details (
            event_id,
            weather_condition,
            light_condition,
            number_of_vehicles
        )
        VALUES (
            :event_id,
            :weather_condition,
            :light_condition,
            :number_of_vehicles
        )
        """
    )
    execute(
        db,
        query,
        {
            "event_id": event_id,
            "weather_condition": weather_condition,
            "light_condition": light_condition,
            "number_of_vehicles": number_of_vehicles,
        },
    )


def fetch_report_by_id(db: Session, report_id: int):
    """Fetch one report row by id."""
    query = text(
        report_select_sql()
        + """
        WHERE e.id = :report_id
        LIMIT 1
        """
    )
    return execute(db, query, {"report_id": report_id}).mappings().first()


def fetch_reports_page(
    db: Session,
    *,
    where_clauses: List[str],
    params: Dict[str, Any],
    limit: int,
    cursor_created_at: Optional[datetime] = None,
    cursor_id: Optional[int] = None,
):
    """Fetch one over-fetched report page for cursor pagination."""
    query_params = dict(params)
    query_params["row_limit"] = limit + 1

    cursor_clause = ""
    if cursor_created_at is not None and cursor_id is not None:
        query_params["cursor_created_at"] = cursor_created_at
        query_params["cursor_id"] = cursor_id
        cursor_clause = """
        AND (
            e.created_at < :cursor_created_at
            OR (e.created_at = :cursor_created_at AND e.id < :cursor_id)
        )
        """

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
    query = text(
        report_select_sql()
        + f"""
        WHERE {where_sql}
        {cursor_clause}
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT :row_limit
        """
    )
    return execute(db, query, query_params).mappings().all()


def update_report_moderation(
    db: Session,
    *,
    report_id: int,
    moderation_status: str,
    admin_approved: bool,
    moderation_notes: Optional[str],
    moderator_id: int,
):
    """Apply moderation update and return updated report id."""
    query = text(
        """
        UPDATE user_reported_events
        SET
            moderation_status = :moderation_status,
            admin_approved = :admin_approved,
            moderation_notes = :moderation_notes,
            moderated_by = :moderator_id,
            moderated_at = NOW(),
            updated_at = NOW()
        WHERE id = :report_id
        RETURNING id
        """
    )
    row = execute(
        db,
        query,
        {
            "report_id": report_id,
            "moderation_status": moderation_status,
            "admin_approved": admin_approved,
            "moderation_notes": moderation_notes,
            "moderator_id": moderator_id,
        },
    ).mappings().first()
    if not row:
        return None
    return int(row["id"])


def fetch_user_event_rows(
    db: Session,
    *,
    where_clauses: List[str],
    params: Dict[str, Any],
    limit: int,
):
    """Fetch filtered user event rows for GeoJSON conversion."""
    # Build the query parameters.
    query_params = dict(params)
    query_params["row_limit"] = limit

    # Build the where SQL clause.
    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    # Build the query.
    query = text(
        report_select_sql()
        + f"""
        /* user_events_geojson */
        WHERE {where_sql}
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT :row_limit
        """
    )
    return execute(db, query, query_params).mappings().all()
