from datetime import date, datetime
from typing import Any, Dict, Optional

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from ..api_utils import report_events_repository as repository
from ..db import get_db
from ..errors import AuthenticationError, AuthorizationError, NotFoundError, ValidationError
from ..schemas.report_event_schemas import ReportedEventCreateRequest, ReportedEventModerationRequest
from .auth_service import bearer_scheme, decode_access_token


ALLOWED_EVENT_KINDS = {"crime", "collision"}
ALLOWED_REPORTER_TYPES = {"anonymous", "authenticated"}
ALLOWED_MODERATION_STATUSES = {"pending", "approved", "rejected"}
MAX_SNAP_DISTANCE_M = 100.0


def require_admin(current_user):
    """Ensure the resolved user has admin privileges."""
    # Check if the current user is not None and has admin privileges.
    if not current_user or not current_user.get("is_admin"):
        raise AuthorizationError(error="FORBIDDEN", message="Admin access required")
    return current_user


def get_optional_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
):
    """Resolve authenticated user when a bearer token is provided, else return None."""
    if credentials is None:
        return None

    # Check if the authentication scheme is valid.
    if credentials.scheme.lower() != "bearer":
        raise AuthenticationError(
            error="NOT_AUTHENTICATED",
            message="Invalid authentication scheme",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Decode the access token.
    payload = decode_access_token(credentials.credentials)
    user_id = payload.get("sub")

    # Check if the user ID is valid.
    if not user_id:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        resolved_user_id = int(user_id)
    except (TypeError, ValueError) as exc:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    # Get the user from the database.
    user = repository.get_user_by_id(db, resolved_user_id)
    if not user:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return dict(user)


def validate_status_filter(value: Optional[str]) -> Optional[str]:
    """Validate and normalize moderation status filter."""
    if value is None:
        return None

    # Check if the status is valid.
    if value not in ALLOWED_MODERATION_STATUSES:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="status must be pending, approved, or rejected",
            details={"field": "status"},
        )
    return value


def validate_event_kind_filter(value: Optional[str]) -> Optional[str]:
    """Validate and normalize event kind filter."""
    if value is None:
        return None

    # Check if the event kind is valid.
    if value not in ALLOWED_EVENT_KINDS:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="event_kind must be crime or collision",
            details={"field": "event_kind"},
        )
    return value


def validate_reporter_type_filter(value: Optional[str]) -> Optional[str]:
    """Validate and normalize reporter type filter."""
    if value is None:
        return None

    # Check if the reporter type is valid.
    if value not in ALLOWED_REPORTER_TYPES:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="reporter_type must be anonymous or authenticated",
            details={"field": "reporter_type"},
        )
    return value


def normalize_required_text(value: Optional[str], field_name: str) -> str:
    """Trim required text and raise when empty."""

    # Trim the required text and raise when empty.
    normalized = (value or "").strip()

    # Check if the normalized text is empty.
    if not normalized:
        raise ValidationError(
            error="INVALID_REQUEST",
            message=f"{field_name} is required",
            details={"field": field_name},
        )
    return normalized


def normalize_optional_text(value: Optional[str]) -> Optional[str]:
    """Trim optional text and collapse blank values to None."""
    if value is None:
        return None

    # Trim the optional text and collapse blank values to None.
    normalized = value.strip()
    return normalized or None


def validate_coordinates(longitude: float, latitude: float):
    """Validate WGS84 longitude and latitude boundaries."""

    # Check if the longitude is within the boundaries.
    if longitude < -180 or longitude > 180:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="longitude must be between -180 and 180",
            details={"field": "longitude", "value": longitude},
        )

    # Check if the latitude is within the boundaries.
    if latitude < -90 or latitude > 90:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="latitude must be between -90 and 90",
            details={"field": "latitude", "value": latitude},
        )


def parse_month(month_value: Optional[str], field_name: str) -> Optional[date]:
    """Parse YYYY-MM month filter into first-of-month date."""
    if month_value is None:
        return None

    # Parse the month string into a date.
    try:
        return datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        # Raise a validation error if the month string is not in the correct format.
        raise ValidationError(
            error="INVALID_REQUEST",
            message=f"{field_name} must be in YYYY-MM format",
            details={"field": field_name, "value": month_value},
        ) from exc


def parse_cursor(cursor: Optional[str]):
    """Decode a cursor in created_at|id format."""
    if cursor is None:
        return None

    # Decode the cursor.
    try:
        created_at_value, report_id = cursor.split("|", 1)
        return {
            "cursor_created_at": datetime.fromisoformat(created_at_value),
            "cursor_id": int(report_id),
        }
    except (TypeError, ValueError) as exc:
        # Raise a validation error if the cursor is not in the correct format.
        raise ValidationError(
            error="INVALID_REQUEST",
            message="cursor must be in created_at|id format",
            details={"field": "cursor", "value": cursor},
        ) from exc


def next_cursor(rows, limit: int) -> Optional[str]:
    """Generate next cursor from an over-fetched row set."""

    # Check if the rows are less than or equal to the limit and if the rows are not empty.
    if len(rows) <= limit or not rows[:limit]:
        return None

    # Get the last row.
    last_row = rows[limit - 1]

    # Serialize the created_at timestamp.
    created_at = serialize_timestamp(last_row["created_at"])
    return f"{created_at}|{last_row['id']}"


def serialize_date(value) -> Optional[str]:
    """Serialize date-like values to YYYY-MM-DD."""

    # Check if the value is None.
    if value is None:
        return None

    # Check if the value has a strftime method.
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def serialize_month(value) -> Optional[str]:
    """Serialize month-like values to YYYY-MM."""

    # Check if the value is None.
    if value is None:
        return None

    # Check if the value has a strftime method.
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m")
    return str(value)


def serialize_time(value) -> Optional[str]:
    """Serialize time-like values to HH:MM."""

    # Check if the value is None.
    if value is None:
        return None

    # Check if the value has a strftime method.
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M")
    text_value = str(value)
    return text_value[:5] if len(text_value) >= 5 else text_value


def serialize_timestamp(value) -> Optional[str]:
    """Serialize datetime-like values to ISO-8601."""
    if value is None:
        return None

    # Check if the value has a isoformat method.
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def validate_create_payload(payload: ReportedEventCreateRequest):
    """Validate cross-field requirements for creating a reported event."""

    # Validate the coordinates.
    validate_coordinates(payload.longitude, payload.latitude)

    # Check if the event kind is crime.
    if payload.event_kind == "crime":
        # Check if the crime is None or the collision is not None.
        if payload.crime is None or payload.collision is not None:
            raise ValidationError(
                error="INVALID_REQUEST",
                message="Crime reports must include crime details and must not include collision details",
            )

        # Normalize the crime type.
        payload.crime.crime_type = normalize_required_text(payload.crime.crime_type, "crime.crime_type")
        return

    # Check if the collision is None or the crime is not None.
    if payload.collision is None or payload.crime is not None:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="Collision reports must include collision details and must not include crime details",
        )

    # Normalize the weather condition.
    payload.collision.weather_condition = normalize_required_text(
        payload.collision.weather_condition,
        "collision.weather_condition",
    )

    # Normalize the light condition.
    payload.collision.light_condition = normalize_required_text(
        payload.collision.light_condition,
        "collision.light_condition",
    )


def event_month(event_date: date) -> date:
    """Return canonical month bucket for an event date."""
    return event_date.replace(day=1)


def report_to_dict(row, include_admin_fields: bool = False):
    """Serialize a report row into API payload shape."""
    # Check if the event kind is crime.
    details = (
        {"crime_type": row["crime_type"]}
        if row["event_kind"] == "crime"
        else {
            "weather_condition": row["weather_condition"],
            "light_condition": row["light_condition"],
            "number_of_vehicles": row["number_of_vehicles"],
        }
    )

    # Serialize the report row into API payload shape.
    payload = {
        "id": row["id"],
        "event_kind": row["event_kind"],
        "reporter_type": row["reporter_type"],
        "month": serialize_month(row["month"]),
        "event_date": serialize_date(row["event_date"]),
        "event_time": serialize_time(row["event_time"]),
        "longitude": row["longitude"],
        "latitude": row["latitude"],
        "segment_id": row["segment_id"],
        "snap_distance_m": row["snap_distance_m"],
        "description": row["description"],
        "admin_approved": row["admin_approved"],
        "moderation_status": row["moderation_status"],
        "moderation_notes": row["moderation_notes"],
        "created_at": serialize_timestamp(row["created_at"]),
        "details": details,
    }

    # Add admin fields to the payload if include_admin_fields is True.
    if include_admin_fields:
        payload.update(
            {
                "user_id": row["user_id"],
                "reporter_email": row["reporter_email"],
                "moderated_by": row["moderated_by"],
                "moderated_at": serialize_timestamp(row["moderated_at"]),
            }
        )

    return payload


def validate_optional_bbox(
    min_lon: Optional[float],
    min_lat: Optional[float],
    max_lon: Optional[float],
    max_lat: Optional[float],
):
    """Validate optional bbox filters and return SQL-ready parameters."""
    values = (min_lon, min_lat, max_lon, max_lat)
    # Check if all the values are None.
    if all(value is None for value in values):
        return None

    # Check if any of the values are None.
    if any(value is None for value in values):
        raise ValidationError(
            error="INVALID_REQUEST",
            message="minLon, minLat, maxLon, and maxLat must all be provided together",
            details={"field": "bbox"},
        )

    # Validate the coordinates.
    validate_coordinates(min_lon, min_lat)
    validate_coordinates(max_lon, max_lat)

    # Check if the min longitude is greater than the max longitude.
    if min_lon >= max_lon:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="minLon must be less than maxLon",
            details={"field": "minLon/maxLon"},
        )

    # Check if the min latitude is greater than the max latitude.
    if min_lat >= max_lat:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="minLat must be less than maxLat",
            details={"field": "minLat/maxLat"},
        )

    # Return the bbox parameters.
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def report_to_feature(row):
    """Convert report row into GeoJSON feature."""
    # Convert the report row into API payload shape.
    properties = report_to_dict(row, include_admin_fields=False)
    longitude = properties.pop("longitude")
    latitude = properties.pop("latitude")

    # Return the GeoJSON feature.
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
        "properties": properties,
    }


def _validate_month_range(from_month: Optional[str], to_month: Optional[str]):
    """Parse and validate paired month range parameters."""

    # Parse the month range parameters.
    from_month_date = parse_month(from_month, "from")
    to_month_date = parse_month(to_month, "to")

    # Check if the from month date is None and the to month date is not None or vice versa.
    if (from_month_date is None) != (to_month_date is None):
        raise ValidationError(
            error="INVALID_REQUEST",
            message="from and to must be provided together",
            details={"field": "from/to"},
        )

    # Check if the from month date is greater than the to month date.
    if from_month_date and to_month_date and from_month_date > to_month_date:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="from must be less than or equal to to",
            details={"field": "from/to"},
        )

    # Return the from month date and to month date.
    return from_month_date, to_month_date


def create_report(db: Session, payload: ReportedEventCreateRequest, current_user):
    """Create a user-reported event and return serialized payload."""
    validate_create_payload(payload)

    # Check if the current user is authenticated.
    reporter_type = "authenticated" if current_user else "anonymous"

    # Get the user ID.
    user_id = current_user["id"] if current_user else None

    # Get the snap candidate.
    snap_candidate = repository.snap_to_segment(db, payload.longitude, payload.latitude)
    segment_id = None
    snap_distance_m = None
    if snap_candidate is not None:
        # Check if the snap distance is not None and is less than or equal to the max snap distance.
        snap_distance_m = snap_candidate["snap_distance_m"]
        if snap_distance_m is not None and snap_distance_m <= MAX_SNAP_DISTANCE_M:
            segment_id = snap_candidate["segment_id"]

    # Get the report month.
    report_month = event_month(payload.event_date)

    try:
        # Insert the report base.
        report_id = repository.insert_report_base(
            db,
            event_kind=payload.event_kind,
            reporter_type=reporter_type,
            user_id=user_id,
            event_date=payload.event_date,
            event_time=payload.event_time,
            month=report_month,
            longitude=payload.longitude,
            latitude=payload.latitude,
            segment_id=segment_id,
            snap_distance_m=snap_distance_m,
            description=normalize_optional_text(payload.description),
        )

        # Check if the event kind is crime.
        if payload.event_kind == "crime":
            repository.insert_report_crime_details(
                db,
                event_id=report_id,
                crime_type=payload.crime.crime_type,
            )
        else:
            # Insert the report collision details.
            repository.insert_report_collision_details(
                db,
                event_id=report_id,
                weather_condition=payload.collision.weather_condition,
                light_condition=payload.collision.light_condition,
                number_of_vehicles=payload.collision.number_of_vehicles,
            )

        # Commit the transaction.
        db.commit()
    except Exception:
        db.rollback()
        raise

    # Get the report row.
    row = repository.fetch_report_by_id(db, report_id)
    if not row:
        raise NotFoundError(error="REPORTED_EVENT_NOT_FOUND", message="Reported event not found")

    # Check if the current user is admin.
    include_admin_fields = bool(current_user and current_user.get("is_admin"))
    return report_to_dict(row, include_admin_fields=include_admin_fields)


def list_my_reports(
    db: Session,
    user_id: int,
    status_value: Optional[str],
    event_kind: Optional[str],
    limit: int,
    cursor: Optional[str],
):
    """List reports created by current authenticated user."""

    # Parse the cursor.
    cursor_data = parse_cursor(cursor)

    where_clauses = ["e.user_id = :user_id"]
    params: Dict[str, Any] = {"user_id": user_id}

    # Check if the status value is not None.
    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        params["status"] = status_value

    # Check if the event kind is not None.
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        params["event_kind"] = event_kind

    # Fetch the reports page.
    rows = repository.fetch_reports_page(
        db,
        where_clauses=where_clauses,
        params=params,
        limit=limit,
        cursor_created_at=cursor_data["cursor_created_at"] if cursor_data else None,
        cursor_id=cursor_data["cursor_id"] if cursor_data else None,
    )

    # Get the page rows.
    page_rows = rows[:limit]

    # Serialize the page rows into API payload shape.
    items = [report_to_dict(row, include_admin_fields=False) for row in page_rows]

    # Return the reports page.
    return {
        "items": items,
        "meta": {
            "returned": len(items),
            "limit": limit,
            "nextCursor": next_cursor(rows, limit),
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
            },
        },
    }


def list_reports_for_admin(
    db: Session,
    current_user,
    status_value: Optional[str],
    event_kind: Optional[str],
    reporter_type: Optional[str],
    from_month: Optional[str],
    to_month: Optional[str],
    limit: int,
    cursor: Optional[str],
):
    """List reports for admin moderation workflow."""
    require_admin(current_user)

    # Parse the month range.
    from_month_date, to_month_date = _validate_month_range(from_month, to_month)

    # Parse the cursor.
    cursor_data = parse_cursor(cursor)

    # Build the where clauses.
    where_clauses = ["TRUE"]
    params: Dict[str, Any] = {}

    # Check if the status value is not None.
    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        params["status"] = status_value
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        params["event_kind"] = event_kind
    if reporter_type is not None:
        where_clauses.append("e.reporter_type = :reporter_type")
        params["reporter_type"] = reporter_type
    if from_month_date is not None:
        where_clauses.append("e.month BETWEEN :from_month_date AND :to_month_date")
        params["from_month_date"] = from_month_date
        params["to_month_date"] = to_month_date

    # Fetch the reports page.
    rows = repository.fetch_reports_page(
        db,
        where_clauses=where_clauses,
        params=params,
        limit=limit,
        cursor_created_at=cursor_data["cursor_created_at"] if cursor_data else None,
        cursor_id=cursor_data["cursor_id"] if cursor_data else None,
    )

    # Get the page rows.
    page_rows = rows[:limit]

    # Serialize the page rows into API payload shape.
    items = [report_to_dict(row, include_admin_fields=True) for row in page_rows]

    return {
        "items": items,
        "meta": {
            "returned": len(items),
            "limit": limit,
            "nextCursor": next_cursor(rows, limit),
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
                "reporter_type": reporter_type,
                "from": from_month,
                "to": to_month,
            },
        },
    }


def moderate_existing_report(
    db: Session,
    current_user,
    report_id: int,
    payload: ReportedEventModerationRequest,
):
    """Apply moderation update and return updated report payload."""
    require_admin(current_user)

    # Normalize the moderation notes.
    moderation_notes = normalize_optional_text(payload.moderation_notes)
    admin_approved = payload.moderation_status == "approved"

    # Update the report moderation.
    try:
        updated_id = repository.update_report_moderation(
            db,
            report_id=report_id,
            moderation_status=payload.moderation_status,
            admin_approved=admin_approved,
            moderation_notes=moderation_notes,
            moderator_id=current_user["id"],
        )
        if updated_id is None:
            raise NotFoundError(error="REPORTED_EVENT_NOT_FOUND", message="Reported event not found")
        db.commit()
    except Exception:
        db.rollback()
        raise

    # Get the report row.
    row = repository.fetch_report_by_id(db, report_id)

    # Check if the report row is not found.
    if not row:
        raise NotFoundError(error="REPORTED_EVENT_NOT_FOUND", message="Reported event not found")

    return report_to_dict(row, include_admin_fields=True)


def list_user_events_geojson(
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
    """Return filtered user events as a GeoJSON feature collection."""
    from_month_date, to_month_date = _validate_month_range(from_month, to_month)
    bbox = validate_optional_bbox(min_lon, min_lat, max_lon, max_lat)

    # Build the where clauses.
    where_clauses = ["TRUE"]
    params: Dict[str, Any] = {}

    # Check if the status value is not None.
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

    # Fetch the user event rows.
    rows = repository.fetch_user_event_rows(
        db,
        where_clauses=where_clauses,
        params=params,
        limit=limit,
    )

    # Serialize the user event rows into GeoJSON features.
    features = [report_to_feature(row) for row in rows]

    # Return the user event features.
    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "returned": len(features),
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
