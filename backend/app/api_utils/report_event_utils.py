from datetime import date, datetime
from typing import Optional

from fastapi import HTTPException, Query

from ..schemas.report_event_schemas import ReportedEventCreateRequest


ALLOWED_EVENT_KINDS = {"crime", "collision"}
ALLOWED_REPORTER_TYPES = {"anonymous", "authenticated"}
ALLOWED_MODERATION_STATUSES = {"pending", "approved", "rejected"}
MAX_SNAP_DISTANCE_M = 100.0


def require_admin(current_user):
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
