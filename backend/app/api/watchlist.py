from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError
from sqlalchemy.orm import Session

from ..auth_utils import get_current_user
from ..db import get_db


router = APIRouter(tags=["watchlists"])


class WatchlistPreferencePayload(BaseModel):
    id: Optional[int] = None
    window_months: int = Field(..., ge=1)
    crime_type: Optional[str] = None
    banding_mode: str = Field(..., min_length=1)


class WatchlistCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    preferences: List[WatchlistPreferencePayload] = Field(default_factory=list)


class WatchlistUpdateRequest(BaseModel):
    name: Optional[str] = None
    min_lon: Optional[float] = None
    min_lat: Optional[float] = None
    max_lon: Optional[float] = None
    max_lat: Optional[float] = None
    preferences: Optional[List[WatchlistPreferencePayload]] = None


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def _normalize_required_text(value, field_name):
    normalized = (value or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return normalized


def _normalize_optional_text(value):
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="min_lon must be less than max_lon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="min_lat must be less than max_lat")


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
        "preferences": preferences,
    }


def _preference_to_dict(row):
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "window_months": row["window_months"],
        "crime_type": row["crime_type"],
        "banding_mode": row["banding_mode"],
        "created_at": row["created_at"],
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
            w.created_at
        FROM watchlists w
        WHERE w.id = :watchlist_id AND w.user_id = :user_id
        LIMIT 1
        """
    )
    row = _execute(db, query, {"watchlist_id": watchlist_id, "user_id": user_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return row


def _get_watchlist_preferences(db, watchlist_id):
    query = text(
        """
        SELECT
            wp.id,
            wp.watchlist_id,
            wp.window_months,
            NULLIF(wp.crime_type, '') AS crime_type,
            wp.banding_mode,
            wp.created_at
        FROM watchlist_preferences wp
        WHERE wp.watchlist_id = :watchlist_id
        ORDER BY wp.created_at ASC, wp.id ASC
        """
    )
    rows = _execute(db, query, {"watchlist_id": watchlist_id}).mappings().all()
    return [_preference_to_dict(row) for row in rows]


def _insert_preference_row(db, watchlist_id, user_id, preference):
    banding_mode = _normalize_required_text(preference.banding_mode, "banding_mode")
    query = text(
        """
        INSERT INTO watchlist_preferences (watchlist_id, window_months, crime_type, banding_mode)
        SELECT
            w.id,
            :window_months,
            :crime_type,
            :banding_mode
        FROM watchlists w
        WHERE w.id = :watchlist_id AND w.user_id = :user_id
        RETURNING
            id,
            watchlist_id,
            window_months,
            crime_type,
            banding_mode,
            created_at
        """
    )
    row = _execute(
        db,
        query,
        {
            "watchlist_id": watchlist_id,
            "user_id": user_id,
            "window_months": preference.window_months,
            "crime_type": _normalize_optional_text(preference.crime_type),
            "banding_mode": banding_mode,
        },
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return row


def _update_preference_row(db, watchlist_id, user_id, preference):
    query = text(
        """
        UPDATE watchlist_preferences wp
        SET
            window_months = :window_months,
            crime_type = :crime_type,
            banding_mode = :banding_mode
        FROM watchlists w
        WHERE wp.id = :preference_id
          AND wp.watchlist_id = :watchlist_id
          AND w.id = wp.watchlist_id
          AND w.user_id = :user_id
        RETURNING
            wp.id,
            wp.watchlist_id,
            wp.window_months,
            wp.crime_type,
            wp.banding_mode,
            wp.created_at
        """
    )
    row = _execute(
        db,
        query,
        {
            "preference_id": preference.id,
            "watchlist_id": watchlist_id,
            "user_id": user_id,
            "window_months": preference.window_months,
            "crime_type": _normalize_optional_text(preference.crime_type),
            "banding_mode": _normalize_required_text(preference.banding_mode, "banding_mode"),
        },
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist preference not found")
    return row


def _apply_preference_upserts(db, watchlist_id, user_id, preferences):
    if preferences is None:
        return

    for preference in preferences:
        if preference.id is None:
            _insert_preference_row(db, watchlist_id, user_id, preference)
        else:
            _update_preference_row(db, watchlist_id, user_id, preference)


@router.get("/watchlists")
def read_watchlists(
    watchlist_id: Optional[int] = Query(default=None),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if watchlist_id is not None:
        row = _get_watchlist_row(db, watchlist_id, current_user["id"])
        return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preferences(db, watchlist_id))}

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
            w.created_at
        FROM watchlists w
        WHERE w.user_id = :user_id
        ORDER BY w.created_at DESC, w.id DESC
        """
    )
    rows = _execute(db, query, {"user_id": current_user["id"]}).mappings().all()
    return {
        "items": [
            _watchlist_to_dict(row, _get_watchlist_preferences(db, row["id"]))
            for row in rows
        ]
    }


@router.post("/watchlists", status_code=status.HTTP_201_CREATED)
def create_watchlist(
    payload: WatchlistCreateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    name = _normalize_required_text(payload.name, "name")
    _validate_bbox(payload.min_lon, payload.min_lat, payload.max_lon, payload.max_lat)

    query = text(
        """
        INSERT INTO watchlists (user_id, name, min_lon, min_lat, max_lon, max_lat)
        VALUES (:user_id, :name, :min_lon, :min_lat, :max_lon, :max_lat)
        RETURNING
            id,
            user_id,
            name,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            created_at
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
        _apply_preference_upserts(db, row["id"], current_user["id"], payload.preferences)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Unable to create watchlist") from exc

    return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preferences(db, row["id"]))}


@router.patch("/watchlists/{watchlist_id}")
def update_watchlist(
    watchlist_id: int,
    payload: WatchlistUpdateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
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
            raise HTTPException(
                status_code=400,
                detail="min_lon, min_lat, max_lon, and max_lat must all be provided together",
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

    if not update_fields and payload.preferences is None:
        raise HTTPException(status_code=400, detail="Provide watchlist fields or preferences to update")

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
                    created_at
                """
            )
            row = _execute(db, query, query_params).mappings().first()
            if not row:
                db.rollback()
                raise HTTPException(status_code=404, detail="Watchlist not found")
        else:
            row = _get_watchlist_row(db, watchlist_id, current_user["id"])

        _apply_preference_upserts(db, watchlist_id, current_user["id"], payload.preferences)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Unable to update watchlist") from exc

    return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preferences(db, watchlist_id))}


@router.delete("/watchlists/{watchlist_id}")
def delete_watchlist(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
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
        db.rollback()
        raise HTTPException(status_code=404, detail="Watchlist not found")

    db.commit()
    return {"deleted": True, "watchlist_id": row["id"]}
