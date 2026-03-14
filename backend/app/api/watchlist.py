from datetime import date
from typing import List, Optional, Union

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError
from sqlalchemy.orm import Session

from ..services.auth_service import get_current_user
from ..db import get_db
from ..errors import ConflictError, DependencyError, NotFoundError, ValidationError
from ..schemas.watchlist_schemas import (
    WatchlistDeleteResponse,
    WatchlistListResponse,
    WatchlistSingleResponse,
    WatchlistCreateRequest,
    WatchlistPreferencePayload,
)


router = APIRouter(tags=["watchlists"])

FIXED_HOTSPOT_K = 20
FIXED_INCLUDE_HOTSPOT_STABILITY = True
FIXED_INCLUDE_FORECAST = True
FIXED_WEIGHT_CRIME = 1.0
FIXED_WEIGHT_COLLISION = 0.8



class WatchlistUpdateRequest(BaseModel):
    name: Optional[str] = None
    min_lon: Optional[float] = None
    min_lat: Optional[float] = None
    max_lon: Optional[float] = None
    max_lat: Optional[float] = None
    preference: Optional[WatchlistPreferencePayload] = None


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise DependencyError(
            message="Database unavailable. Postgres query execution failed; inspect the database container and server logs."
        ) from exc


def _normalize_required_text(value, field_name):
    normalized = (value or "").strip()
    if not normalized:
        raise ValidationError(
            error="INVALID_REQUEST",
            message=f"{field_name} is required",
            details={"field": field_name},
        )
    return normalized


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="min_lon must be less than max_lon",
            details={"field": "bbox"},
        )
    if min_lat >= max_lat:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="min_lat must be less than max_lat",
            details={"field": "bbox"},
        )


def _validate_month_range(start_month: date, end_month: date):
    if start_month > end_month:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="start_month must be less than or equal to end_month",
            details={"field": "month_range"},
        )


def _normalize_crime_types(values):
    normalized = []
    seen = set()
    for value in values or []:
        token = (value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _normalize_watchlist_mode(value, *, error_context: str):
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
    canonical = aliases.get(normalized)
    if canonical is None:
        raise ValidationError(
            error="INVALID_TRAVEL_MODE",
            message=f"Unsupported watchlist travel_mode '{value}' for {error_context}. Use walk or drive.",
            details={"field": "travel_mode", "value": value, "context": error_context},
        )
    return canonical


def _serialize_watchlist_mode(value):
    normalized = (value or "").strip().lower()
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
    if include_collisions and mode != "drive":
        raise ValidationError(
            error="INVALID_MODE_FOR_COLLISIONS",
            message="include_collisions is only supported when travel_mode is drive",
            details={"field": "travel_mode", "include_collisions": include_collisions},
        )


def _watchlist_to_dict(row, preferences):
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "name": row["name"],
        "min_lon": row["min_lon"],
        "min_lat": row["min_lat"],
        "max_lon": row["max_lon"],
        "max_lat": row["max_lat"],
        "created_at": row["created_at"],
        "preference": preferences,
    }


def _preference_to_dict(row):
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


def _get_watchlist_row(db, watchlist_id, user_id):
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
    row = _execute(db, query, {"watchlist_id": watchlist_id, "user_id": user_id}).mappings().first()
    if not row:
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )
    return row


def _apply_preference(db, watchlist_id, user_id, preference):
    if preference is None:
        return

    _validate_month_range(preference.start_month, preference.end_month)
    travel_mode = _normalize_watchlist_mode(preference.travel_mode, error_context="watchlist preference")
    include_collisions = bool(preference.include_collisions)
    _validate_watchlist_collision_mode(travel_mode, include_collisions)

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
    row = _execute(
        db,
        query,
        {
            "start_month": preference.start_month,
            "end_month": preference.end_month,
            "watchlist_id": watchlist_id,
            "user_id": user_id,
            "crime_types": _normalize_crime_types(preference.crime_types),
            "travel_mode": travel_mode,
            "include_collisions": include_collisions,
            "baseline_months": preference.baseline_months,
        },
    ).mappings().first()
    if not row:
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )


@router.get(
    "/watchlists",
    response_model=Union[WatchlistListResponse, WatchlistSingleResponse],
)
def read_watchlists(
    watchlist_id: Optional[int] = Query(default=None),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Union[WatchlistListResponse, WatchlistSingleResponse]:
    if watchlist_id is not None:
        row = _get_watchlist_row(db, watchlist_id, current_user["id"])
        return {"watchlist": _watchlist_to_dict(row, _preference_to_dict(row))}

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
    rows = _execute(db, query, {"user_id": current_user["id"]}).mappings().all()
    return {
        "items": [
            _watchlist_to_dict(row, _preference_to_dict(row))
            for row in rows
        ]
    }



@router.post("/watchlists", status_code=status.HTTP_201_CREATED, response_model=WatchlistSingleResponse)
def create_watchlist(
    payload: WatchlistCreateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistSingleResponse:

    name = _normalize_required_text(payload.name, "name")
    _validate_bbox(payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat)

    query = text(
        """
        INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)
        VALUES (:user_id, :name, :min_lon, :min_lat, :max_lon, :max_lat)
        RETURNING
            id
        """
    )
    try:
        row = _execute(
            db,
            query,
            {
                "user_id": current_user["id"],
                "name": name,
                "min_lon": payload.min_lon,
                "min_lat": payload.min_lat,
                "max_lon": payload.max_lon,
                "max_lat": payload.max_lat,
            },
        ).mappings().first()
        _apply_preference(db, row["id"], current_user["id"], payload.preference)
        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise ConflictError(
            error="CONFLICT",
            message="Unable to create watchlist",
        ) from exc

    watchlist_row = _get_watchlist_row(db, row["id"], current_user["id"])
    return {"watchlist": _watchlist_to_dict(watchlist_row, _preference_to_dict(watchlist_row))}


@router.patch("/watchlists/{watchlist_id}", response_model=WatchlistSingleResponse)
def update_watchlist(
    watchlist_id: int,
    payload: WatchlistUpdateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistSingleResponse:
    update_fields = []
    query_params = {
        "watchlist_id": watchlist_id,
        "user_id": current_user["id"],
    }

    if payload.name is not None:
        query_params["name"] = _normalize_required_text(payload.name, "name")
        update_fields.append("name = :name")

    bbox_values = [payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat]
    if any(value is not None for value in bbox_values):
        if not all(value is not None for value in bbox_values):
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
        raise ValidationError(
            error="INVALID_REQUEST",
            message="Provide watchlist fields or preference to update",
        )

    try:
        if update_fields:
            query = text(
                f"""
                UPDATE watchlists
                SET {", ".join(update_fields)}
                WHERE id = :watchlist_id AND user_id = :user_id
                RETURNING
                    id,
                    user_id,
                    name,
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                    start_month,
                    end_month,
                    crime_types,
                    travel_mode,
                    include_collisions,
                    baseline_months,
                    created_at
                """
            )
            row = _execute(db, query, query_params).mappings().first()
            if not row:
                if hasattr(db, "rollback"):
                    db.rollback()
                raise NotFoundError(
                    error="WATCHLIST_NOT_FOUND",
                    message="Watchlist not found",
                )
        else:
            row = _get_watchlist_row(db, watchlist_id, current_user["id"])

        _apply_preference(db, watchlist_id, current_user["id"], payload.preference)
        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise ConflictError(
            error="CONFLICT",
            message="Unable to update watchlist",
        ) from exc

    watchlist_row = _get_watchlist_row(db, watchlist_id, current_user["id"])
    return {"watchlist": _watchlist_to_dict(watchlist_row, _preference_to_dict(watchlist_row))}


@router.delete("/watchlists/{watchlist_id}", response_model=WatchlistDeleteResponse)
def delete_watchlist(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistDeleteResponse:
    query = text(
        """
        DELETE FROM watchlists
        WHERE id = :watchlist_id AND user_id = :user_id
        RETURNING id
        """
    )
    row = _execute(
        db,
        query,
        {
            "watchlist_id": watchlist_id,
            "user_id": current_user["id"],
        },
    ).mappings().first()
    if not row:
        if hasattr(db, "rollback"):
            db.rollback()
        raise NotFoundError(
            error="WATCHLIST_NOT_FOUND",
            message="Watchlist not found",
        )

    if hasattr(db, "commit"):
        db.commit()
    return {"deleted": True, "watchlist_id": row["id"]}

