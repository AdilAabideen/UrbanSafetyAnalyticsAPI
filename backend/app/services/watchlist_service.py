from datetime import date
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..api_utils import watchlist_repository
from ..errors import ConflictError, NotFoundError, ValidationError


FIXED_HOTSPOT_K = 20
FIXED_INCLUDE_HOTSPOT_STABILITY = True
FIXED_INCLUDE_FORECAST = True
FIXED_WEIGHT_CRIME = 1.0
FIXED_WEIGHT_COLLISION = 0.8


def _normalize_required_text(value, field_name):
    """Normalize a required text value."""
    
    # Normalize the required text.
    normalized = (value or "").strip()
    if not normalized:
        # Raise a validation error if the normalized text is empty.
        raise ValidationError(
            error="INVALID_REQUEST",
            message=f"{field_name} is required",
            details={"field": field_name},
        )
    return normalized


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    """Validate the bounding box coordinates."""
    # Check if the min longitude is greater than the max longitude.
    if min_lon >= max_lon:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="min_lon must be less than max_lon",
            details={"field": "bbox"},
        )
    # Check if the min latitude is greater than the max latitude.
    if min_lat >= max_lat:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="min_lat must be less than max_lat",
            details={"field": "bbox"},
        )


def _validate_month_range(start_month: date, end_month: date):
    """Validate the month range."""
    # Check if the start month is greater than the end month.
    if start_month > end_month:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="start_month must be less than or equal to end_month",
            details={"field": "month_range"},
        )


def _normalize_crime_types(values):
    """Normalize the crime types."""
    normalized = []
    seen = set()
    # Normalize the crime types by removing duplicates and empty values.
    for value in values or []:
        token = (value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _normalize_watchlist_mode(value, *, error_context: str):
    """Normalize the watchlist mode."""
    normalized = _normalize_required_text(value, "travel_mode").lower()
    aliases = {
        "walk": "walk",
        "walking": "walk",
        "foot": "walk",
        "pedestrian": "walk",
        "drive": "drive",
        "driving": "drive",
        "car": "drive",
        "vehicle": "drive",
    }
    # Get the canonical watchlist mode.
    canonical = aliases.get(normalized)
    if canonical is None:
        # Raise a validation error if the canonical watchlist mode is not found.
        raise ValidationError(
            error="INVALID_TRAVEL_MODE",
            message=f"Unsupported watchlist travel_mode '{value}' for {error_context}. Use walk or drive.",
            details={"field": "travel_mode", "value": value, "context": error_context},
        )
    return canonical


def _serialize_watchlist_mode(value):
    """Serialize the watchlist mode."""
    normalized = (value or "").strip().lower()
    # Get the canonical watchlist mode.
    aliases = {
        "walk": "walk",
        "walking": "walk",
        "foot": "walk",
        "pedestrian": "walk",
        "drive": "drive",
        "driving": "drive",
        "car": "drive",
        "vehicle": "drive",
    }
    return aliases.get(normalized, value)


def _validate_watchlist_collision_mode(mode, include_collisions):
    """Validate the watchlist collision mode."""

    # Check if the include collisions is only supported when travel mode is drive.
    if include_collisions and mode != "drive":

        # Raise a validation error if the include collisions is only supported when travel mode is drive.
        raise ValidationError(
            error="INVALID_MODE_FOR_COLLISIONS",
            message="include_collisions is only supported when travel_mode is drive",
            details={"field": "travel_mode", "include_collisions": include_collisions},
        )


def _preference_to_dict(row):
    """Serialize the watchlist preference."""
    if row["start_month"] is None or row["end_month"] is None or row["travel_mode"] is None:
        return None

    return {
        "start_month": row["start_month"],
        "end_month": row["end_month"],
        "crime_types": list(row["crime_types"] or []),
        "travel_mode": _serialize_watchlist_mode(row["travel_mode"]),
        "include_collisions": bool(row["include_collisions"]),
        "baseline_months": int(row["baseline_months"]),
        "hotspot_k": FIXED_HOTSPOT_K,
        "include_hotspot_stability": FIXED_INCLUDE_HOTSPOT_STABILITY,
        "include_forecast": FIXED_INCLUDE_FORECAST,
        "weight_crime": FIXED_WEIGHT_CRIME,
        "weight_collision": FIXED_WEIGHT_COLLISION,
    }


def _watchlist_to_dict(row):
    """Serialize the watchlist."""
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "name": row["name"],
        "min_lon": row["min_lon"],
        "min_lat": row["min_lat"],
        "max_lon": row["max_lon"],
        "max_lat": row["max_lat"],
        "created_at": row["created_at"],
        "preference": _preference_to_dict(row),
    }


def apply_preference_service(db: Session, watchlist_id: int, user_id: int, preference):
    """Validate and persist watchlist preference values."""
    # Check if the preference is None.
    if preference is None:
        # Return if the preference is None.
        return

    _validate_month_range(preference.start_month, preference.end_month)
    travel_mode = _normalize_watchlist_mode(preference.travel_mode, error_context="watchlist preference")
    include_collisions = bool(preference.include_collisions)
    _validate_watchlist_collision_mode(travel_mode, include_collisions)

    # Update the watchlist preference.
    row = watchlist_repository.update_watchlist_preference(
        db,
        watchlist_id=watchlist_id,
        user_id=user_id,
        start_month=preference.start_month,
        end_month=preference.end_month,
        crime_types=_normalize_crime_types(preference.crime_types),
        travel_mode=travel_mode,
        include_collisions=include_collisions,
        baseline_months=preference.baseline_months,
    )
    if not row:
        # Raise a not found error if the watchlist preference is not found.
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )


def read_watchlists_service(db: Session, user_id: int, watchlist_id: Optional[int] = None):
    """Return one watchlist by id or list all user watchlists."""
    # Check if the watchlist id is not None.
    if watchlist_id is not None:
        row = watchlist_repository.get_watchlist_by_id(db, watchlist_id=watchlist_id, user_id=user_id)
        if not row:
            # Raise a not found error if the watchlist row is not found.
            raise NotFoundError(
                error="WATCHLIST_NOT_FOUND",
                message="Watchlist not found",
            )
        return {"watchlist": _watchlist_to_dict(row)}

    rows = watchlist_repository.list_watchlists_by_user(db, user_id=user_id)
    return {"items": [_watchlist_to_dict(row) for row in rows]}


def create_watchlist_service(db: Session, user_id: int, payload):
    """Create a watchlist and optionally apply preference values."""
    # Normalize the name and validate the bounding box.
    name = _normalize_required_text(payload.name, "name")
    _validate_bbox(payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat)

    try:
        # Insert the watchlist row.
        inserted = watchlist_repository.insert_watchlist(
            db,
            user_id=user_id,
            name=name,
            min_lon=payload.min_lon,
            min_lat=payload.min_lat,
            max_lon=payload.max_lon,
            max_lat=payload.max_lat,
        )
        watchlist_id = inserted["id"]

        # Apply the preference values and Commit the transaction.
        apply_preference_service(
            db,
            watchlist_id=watchlist_id,
            user_id=user_id,
            preference=payload.preference,
        )

        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        # Rollback the transaction if the integrity error occurs.
        if hasattr(db, "rollback"):
            db.rollback()
        raise ConflictError(
            error="CONFLICT",
            message="Unable to create watchlist",
        ) from exc
    except Exception:
        # Rollback the transaction if any other error occurs.
        if hasattr(db, "rollback"):
            db.rollback()
        raise

    watchlist_row = watchlist_repository.get_watchlist_by_id(db, watchlist_id=watchlist_id, user_id=user_id)
    if not watchlist_row:
        # Raise a not found error if the watchlist row is not found.
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )
    return {"watchlist": _watchlist_to_dict(watchlist_row)}


def update_watchlist_service(db: Session, user_id: int, watchlist_id: int, payload):
    """Update base watchlist fields and/or preference values."""
    # Initialize the update fields and query parameters.
    update_fields = []
    query_params = {}

    # Check if the name is provided.
    if payload.name is not None:
        query_params["name"] = _normalize_required_text(payload.name, "name")
        update_fields.append("name = :name")

    # Check if the bounding box values are provided.
    bbox_values = [payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat]
    if any(value is not None for value in bbox_values):
        if not all(value is not None for value in bbox_values):
            # Raise a validation error if the bounding box values are not all provided.
            raise ValidationError(
                error="INVALID_REQUEST",
                message="min_lon, min_lat, max_lon, and max_lat must all be provided together",
                details={"field": "bbox"},
            )

        _validate_bbox(payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat)
        update_fields.extend(
            [
                "min_lon = :min_lon",
                "min_lat = :min_lat",
                "max_lon = :max_lon",
                "max_lat = :max_lat",
            ]
        )
        query_params.update(
            {
                "min_lon": payload.min_lon,
                "min_lat": payload.min_lat,
                "max_lon": payload.max_lon,
                "max_lat": payload.max_lat,
            }
        )

    if not update_fields and payload.preference is None:
        # Raise a validation error if no update fields or preference are provided.
        raise ValidationError(
            error="INVALID_REQUEST",
            message="Provide watchlist fields or preference to update",
        )

    try:
        # Check if any update fields are provided.
        if update_fields:
            updated = watchlist_repository.update_watchlist_fields(
                db,
                watchlist_id=watchlist_id,
                user_id=user_id,
                update_fields=update_fields,
                query_params=query_params,
            )
            if not updated:
                # Raise a not found error if the watchlist row is not found.
                raise NotFoundError(
                    error="WATCHLIST_NOT_FOUND",
                    message="Watchlist not found",
                )
        else:
            existing = watchlist_repository.get_watchlist_by_id(db, watchlist_id=watchlist_id, user_id=user_id)
            if not existing:
                # Raise a not found error if the watchlist row is not found.
                raise NotFoundError(
                    error="WATCHLIST_NOT_FOUND",
                    message="Watchlist not found",
                )

        # Apply the preference values and Commit the transaction.
        apply_preference_service(
            db,
            watchlist_id=watchlist_id,
            user_id=user_id,
            preference=payload.preference,
        )

        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        # Rollback the transaction if the integrity error occurs.
        if hasattr(db, "rollback"):
            db.rollback()
        raise ConflictError(
            error="CONFLICT",
            message="Unable to update watchlist",
        ) from exc
    except Exception:
        # Rollback the transaction if any other error occurs.
        if hasattr(db, "rollback"):
            db.rollback()
        raise

    watchlist_row = watchlist_repository.get_watchlist_by_id(db, watchlist_id=watchlist_id, user_id=user_id)
    if not watchlist_row:
        # Raise a not found error if the watchlist row is not found.
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )
    return {"watchlist": _watchlist_to_dict(watchlist_row)}


def delete_watchlist_service(db: Session, user_id: int, watchlist_id: int):
    """Delete a watchlist row."""
    # Delete the watchlist row.
    deleted = watchlist_repository.delete_watchlist_row(
        db,
        watchlist_id=watchlist_id,
        user_id=user_id,
    )
    if not deleted:
        # Rollback the transaction if the watchlist row is not deleted.
        if hasattr(db, "rollback"):
            db.rollback()
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )

    if hasattr(db, "commit"):
        db.commit()

    return {"deleted": True, "watchlist_id": deleted["id"]}
