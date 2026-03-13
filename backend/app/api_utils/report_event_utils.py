from datetime import date, datetime
from typing import Optional

from fastapi import Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from .auth_utils import bearer_scheme, decode_access_token, get_current_user
from ..db import get_db
from ..schemas.report_event_schemas import ReportedEventCreateRequest, ReportedEventModerationRequest


ALLOWED_EVENT_KINDS = {"crime", "collision"}
ALLOWED_REPORTER_TYPES = {"anonymous", "authenticated"}
ALLOWED_MODERATION_STATUSES = {"pending", "approved", "rejected"}
MAX_SNAP_DISTANCE_M = 100.0


def _execute(db, query, params=None):
    """Execute a SQL statement and normalize transient DB failures to 503 errors."""
    try:
        return db.execute(query, params or {})
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def get_optional_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
):
    """Resolve the authenticated user when a bearer token is provided, else return None."""
    if credentials is None:
        return None
    if credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication scheme",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = decode_access_token(credentials.credentials)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

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
    user = _execute(db, query, {"user_id": int(user_id)}).mappings().first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return dict(user)


def require_admin(current_user=Depends(get_current_user)):
    """Ensure the resolved user has admin privileges."""
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


def status_query(
    value: Optional[str] = Query(default=None),
) -> Optional[str]:
    """Validate and normalize the moderation status query value."""
    if value is None:
        return None
    if value not in ALLOWED_MODERATION_STATUSES:
        raise HTTPException(status_code=400, detail="status must be pending, approved, or rejected")
    return value


def event_kind_query(
    value: Optional[str] = Query(default=None),
) -> Optional[str]:
    """Validate and normalize the event kind query value."""
    if value is None:
        return None
    if value not in ALLOWED_EVENT_KINDS:
        raise HTTPException(status_code=400, detail="event_kind must be crime or collision")
    return value


def reporter_type_query(
    value: Optional[str] = Query(default=None),
) -> Optional[str]:
    """Validate and normalize the reporter type query value."""
    if value is None:
        return None
    if value not in ALLOWED_REPORTER_TYPES:
        raise HTTPException(status_code=400, detail="reporter_type must be anonymous or authenticated")
    return value


def _normalize_required_text(value: Optional[str], field_name: str) -> str:
    """Trim required text and raise when empty after normalization."""
    normalized = (value or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return normalized


def _normalize_optional_text(value: Optional[str]) -> Optional[str]:
    """Trim optional text and collapse blank values to None."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_coordinates(longitude: float, latitude: float):
    """Validate WGS84 longitude and latitude boundaries."""
    if longitude < -180 or longitude > 180:
        raise HTTPException(status_code=400, detail="longitude must be between -180 and 180")
    if latitude < -90 or latitude > 90:
        raise HTTPException(status_code=400, detail="latitude must be between -90 and 90")


def _parse_month(month_value: Optional[str], field_name: str) -> Optional[date]:
    """Parse YYYY-MM month filters into first-of-month date objects."""
    if month_value is None:
        return None
    try:
        return datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be in YYYY-MM format") from exc


def _parse_cursor(cursor: Optional[str]):
    """Decode a list cursor in created_at|id format."""
    if cursor is None:
        return None
    try:
        created_at_value, report_id = cursor.split("|", 1)
        return {
            "cursor_created_at": datetime.fromisoformat(created_at_value),
            "cursor_id": int(report_id),
        }
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="cursor must be in created_at|id format") from exc


def _next_cursor(rows, limit):
    """Generate the next pagination cursor from an over-fetched row set."""
    if len(rows) <= limit or not rows[:limit]:
        return None
    last_row = rows[limit - 1]
    created_at = _serialize_timestamp(last_row["created_at"])
    return f"{created_at}|{last_row['id']}"


def _serialize_date(value) -> Optional[str]:
    """Serialize date-like values to YYYY-MM-DD."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _serialize_month(value) -> Optional[str]:
    """Serialize month-like values to YYYY-MM."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m")
    return str(value)


def _serialize_time(value) -> Optional[str]:
    """Serialize time-like values to HH:MM."""
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M")
    text_value = str(value)
    return text_value[:5] if len(text_value) >= 5 else text_value


def _serialize_timestamp(value) -> Optional[str]:
    """Serialize datetime-like values to ISO-8601 strings."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _validate_create_payload(payload: ReportedEventCreateRequest):
    """Validate cross-field requirements for creating a reported event."""
    _validate_coordinates(payload.longitude, payload.latitude)

    if payload.event_kind == "crime":
        if payload.crime is None or payload.collision is not None:
            raise HTTPException(
                status_code=400,
                detail="Crime reports must include crime details and must not include collision details",
            )
        payload.crime.crime_type = _normalize_required_text(payload.crime.crime_type, "crime.crime_type")
        return

    if payload.collision is None or payload.crime is not None:
        raise HTTPException(
            status_code=400,
            detail="Collision reports must include collision details and must not include crime details",
        )
    payload.collision.weather_condition = _normalize_required_text(
        payload.collision.weather_condition, "collision.weather_condition"
    )
    payload.collision.light_condition = _normalize_required_text(
        payload.collision.light_condition, "collision.light_condition"
    )


def _event_month(event_date: date) -> date:
    """Return the canonical month bucket for an event date."""
    return event_date.replace(day=1)


def _snap_to_segment(db: Session, longitude: float, latitude: float):
    """Return the closest segment id when within the configured snap threshold."""
    query = text(
        """
        SELECT
            rs.id,
            ST_Distance(
                rs.geom::geography,
                ST_SetSRID(ST_Point(:longitude, :latitude), 4326)::geography
            ) AS snap_distance_m
        FROM road_segments_4326 rs
        ORDER BY rs.geom <-> ST_SetSRID(ST_Point(:longitude, :latitude), 4326)
        LIMIT 1
        """
    )
    row = _execute(db, query, {"longitude": longitude, "latitude": latitude}).mappings().first()
    if not row:
        return None, None
    snap_distance_m = float(row["snap_distance_m"]) if row["snap_distance_m"] is not None else None
    if snap_distance_m is None or snap_distance_m > MAX_SNAP_DISTANCE_M:
        return None, snap_distance_m
    return row["id"], snap_distance_m


def _report_select_sql():
    """Return the shared SELECT statement used by report listing queries."""
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


def _report_to_dict(row, include_admin_fields=False):
    """Serialize a joined report row into the API payload shape."""
    details = {"crime_type": row["crime_type"]} if row["event_kind"] == "crime" else {
        "weather_condition": row["weather_condition"],
        "light_condition": row["light_condition"],
        "number_of_vehicles": row["number_of_vehicles"],
    }
    payload = {
        "id": row["id"],
        "event_kind": row["event_kind"],
        "reporter_type": row["reporter_type"],
        "month": _serialize_month(row["month"]),
        "event_date": _serialize_date(row["event_date"]),
        "event_time": _serialize_time(row["event_time"]),
        "longitude": row["longitude"],
        "latitude": row["latitude"],
        "segment_id": row["segment_id"],
        "snap_distance_m": row["snap_distance_m"],
        "description": row["description"],
        "admin_approved": row["admin_approved"],
        "moderation_status": row["moderation_status"],
        "moderation_notes": row["moderation_notes"],
        "created_at": _serialize_timestamp(row["created_at"]),
        "details": details,
    }
    if include_admin_fields:
        payload.update(
            {
                "user_id": row["user_id"],
                "reporter_email": row["reporter_email"],
                "moderated_by": row["moderated_by"],
                "moderated_at": _serialize_timestamp(row["moderated_at"]),
            }
        )
    return payload


def create_report_record(db: Session, payload: ReportedEventCreateRequest, current_user):
    """Insert a reported event and return the persisted record payload."""
    _validate_create_payload(payload)

    reporter_type = "authenticated" if current_user else "anonymous"
    user_id = current_user["id"] if current_user else None
    segment_id, snap_distance_m = _snap_to_segment(db, payload.longitude, payload.latitude)
    event_month = _event_month(payload.event_date)

    insert_base_query = text(
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

    try:
        base_row = _execute(
            db,
            insert_base_query,
            {
                "event_kind": payload.event_kind,
                "reporter_type": reporter_type,
                "user_id": user_id,
                "event_date": payload.event_date,
                "event_time": payload.event_time,
                "month": event_month,
                "longitude": payload.longitude,
                "latitude": payload.latitude,
                "segment_id": segment_id,
                "snap_distance_m": snap_distance_m,
                "description": _normalize_optional_text(payload.description),
            },
        ).mappings().first()

        report_id = base_row["id"]
        if payload.event_kind == "crime":
            _execute(
                db,
                text(
                    """
                    INSERT INTO user_reported_crime_details (event_id, crime_type)
                    VALUES (:event_id, :crime_type)
                    """
                ),
                {
                    "event_id": report_id,
                    "crime_type": payload.crime.crime_type,
                },
            )
        else:
            _execute(
                db,
                text(
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
                ),
                {
                    "event_id": report_id,
                    "weather_condition": payload.collision.weather_condition,
                    "light_condition": payload.collision.light_condition,
                    "number_of_vehicles": payload.collision.number_of_vehicles,
                },
            )
        db.commit()
    except Exception:
        db.rollback()
        raise

    return get_report_by_id(db, report_id, include_admin_fields=bool(current_user and current_user.get("is_admin")))


def get_report_by_id(db: Session, report_id: int, include_admin_fields=False):
    """Fetch a single reported event by id or raise 404 when not found."""
    query = text(
        _report_select_sql()
        + """
        WHERE e.id = :report_id
        LIMIT 1
        """
    )
    row = _execute(db, query, {"report_id": report_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Reported event not found")
    return _report_to_dict(row, include_admin_fields=include_admin_fields)


def _list_reports(db: Session, where_clauses, params, limit: int, include_admin_fields=False):
    """Run a paginated report listing query and return rows plus cursor."""
    query_params = dict(params)
    query_params["row_limit"] = limit + 1

    cursor_clause = ""
    if query_params.get("cursor_created_at") is not None and query_params.get("cursor_id") is not None:
        cursor_clause = """
        AND (
            e.created_at < :cursor_created_at
            OR (e.created_at = :cursor_created_at AND e.id < :cursor_id)
        )
        """

    query = text(
        _report_select_sql()
        + f"""
        WHERE {" AND ".join(where_clauses)}
        {cursor_clause}
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT :row_limit
        """
    )
    rows = _execute(db, query, query_params).mappings().all()
    page_rows = rows[:limit]
    return {
        "items": [_report_to_dict(row, include_admin_fields=include_admin_fields) for row in page_rows],
        "nextCursor": _next_cursor(rows, limit),
    }


def list_own_reports(
    db: Session,
    user_id: int,
    status_value: Optional[str],
    event_kind: Optional[str],
    limit: int,
    cursor: Optional[str],
):
    """List reports created by the current authenticated user."""
    cursor_data = _parse_cursor(cursor)
    where_clauses = ["e.user_id = :user_id"]
    query_params = {"user_id": user_id}
    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        query_params["status"] = status_value
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        query_params["event_kind"] = event_kind
    if cursor_data:
        query_params.update(cursor_data)

    listing = _list_reports(db, where_clauses, query_params, limit, include_admin_fields=False)
    return {
        "items": listing["items"],
        "meta": {
            "returned": len(listing["items"]),
            "limit": limit,
            "nextCursor": listing["nextCursor"],
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
            },
        },
    }


def list_admin_reports(
    db: Session,
    status_value: Optional[str],
    event_kind: Optional[str],
    reporter_type: Optional[str],
    from_month: Optional[str],
    to_month: Optional[str],
    limit: int,
    cursor: Optional[str],
):
    """List reports for admins with moderation and reporter filters."""
    cursor_data = _parse_cursor(cursor)
    from_month_date = _parse_month(from_month, "from")
    to_month_date = _parse_month(to_month, "to")
    if from_month_date and to_month_date and from_month_date > to_month_date:
        raise HTTPException(status_code=400, detail="from must be less than or equal to to")
    if (from_month_date is None) != (to_month_date is None):
        raise HTTPException(status_code=400, detail="from and to must be provided together")

    where_clauses = ["TRUE"]
    query_params = {}
    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        query_params["status"] = status_value
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        query_params["event_kind"] = event_kind
    if reporter_type is not None:
        where_clauses.append("e.reporter_type = :reporter_type")
        query_params["reporter_type"] = reporter_type
    if from_month_date is not None:
        where_clauses.append("e.month BETWEEN :from_month_date AND :to_month_date")
        query_params["from_month_date"] = from_month_date
        query_params["to_month_date"] = to_month_date
    if cursor_data:
        query_params.update(cursor_data)

    listing = _list_reports(db, where_clauses, query_params, limit, include_admin_fields=True)
    return {
        "items": listing["items"],
        "meta": {
            "returned": len(listing["items"]),
            "limit": limit,
            "nextCursor": listing["nextCursor"],
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
                "reporter_type": reporter_type,
                "from": from_month,
                "to": to_month,
            },
        },
    }


def moderate_report(db: Session, report_id: int, moderator_id: int, payload: ReportedEventModerationRequest):
    """Apply moderation updates to a reported event and return the updated record."""
    moderation_notes = _normalize_optional_text(payload.moderation_notes)
    admin_approved = payload.moderation_status == "approved"
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
    row = _execute(
        db,
        query,
        {
            "report_id": report_id,
            "moderation_status": payload.moderation_status,
            "admin_approved": admin_approved,
            "moderation_notes": moderation_notes,
            "moderator_id": moderator_id,
        },
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Reported event not found")
    db.commit()
    return get_report_by_id(db, report_id, include_admin_fields=True)


def _validate_optional_bbox(
    min_lon: Optional[float],
    min_lat: Optional[float],
    max_lon: Optional[float],
    max_lat: Optional[float],
):
    """Validate optional bbox filters and return SQL parameter names used by queries."""
    values = (min_lon, min_lat, max_lon, max_lat)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise HTTPException(status_code=400, detail="minLon, minLat, maxLon, and maxLat must all be provided together")

    _validate_coordinates(min_lon, min_lat)
    _validate_coordinates(max_lon, max_lat)
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")

    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _report_to_feature(row):
    """Convert a report row into a GeoJSON feature."""
    properties = _report_to_dict(row, include_admin_fields=False)
    longitude = properties.pop("longitude")
    latitude = properties.pop("latitude")
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
        "properties": properties,
    }


def list_user_event_features(
    db: Session,
    status_value: Optional[str],
    event_kind: Optional[str],
    reporter_type: Optional[str],
    from_month: Optional[str],
    to_month: Optional[str],
    admin_approved: Optional[bool],
    min_lon: Optional[float],
    min_lat: Optional[float],
    max_lon: Optional[float],
    max_lat: Optional[float],
    limit: int,
):
    """Return filtered user-reported events as a GeoJSON feature collection."""
    from_month_date = _parse_month(from_month, "from")
    to_month_date = _parse_month(to_month, "to")
    if from_month_date and to_month_date and from_month_date > to_month_date:
        raise HTTPException(status_code=400, detail="from must be less than or equal to to")
    if (from_month_date is None) != (to_month_date is None):
        raise HTTPException(status_code=400, detail="from and to must be provided together")

    bbox = _validate_optional_bbox(min_lon, min_lat, max_lon, max_lat)
    where_clauses = ["TRUE"]
    params = {"row_limit": limit}

    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        params["status"] = status_value
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        params["event_kind"] = event_kind
    if reporter_type is not None:
        where_clauses.append("e.reporter_type = :reporter_type")
        params["reporter_type"] = reporter_type
    if admin_approved is not None:
        where_clauses.append("e.admin_approved = :admin_approved")
        params["admin_approved"] = admin_approved
    if from_month_date is not None:
        where_clauses.append("e.month BETWEEN :from_month_date AND :to_month_date")
        params["from_month_date"] = from_month_date
        params["to_month_date"] = to_month_date
    if bbox is not None:
        where_clauses.extend(
            [
                "e.longitude BETWEEN :min_lon AND :max_lon",
                "e.latitude BETWEEN :min_lat AND :max_lat",
            ]
        )
        params.update(bbox)

    query = text(
        _report_select_sql()
        + f"""
        /* user_events_geojson */
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT :row_limit
        """
    )
    rows = _execute(db, query, params).mappings().all()
    return {
        "type": "FeatureCollection",
        "features": [_report_to_feature(row) for row in rows],
        "meta": {
            "returned": len(rows),
            "limit": limit,
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
                "reporter_type": reporter_type,
                "adminApproved": admin_approved,
                "from": from_month,
                "to": to_month,
                "bbox": None
                if bbox is None
                else {
                    "minLon": bbox["min_lon"],
                    "minLat": bbox["min_lat"],
                    "maxLon": bbox["max_lon"],
                    "maxLat": bbox["max_lat"],
                },
            },
        },
    }
