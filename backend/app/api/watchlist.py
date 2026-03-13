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
    window_months: int = Field(..., ge=1)
    crime_types: List[str] = Field(default_factory=list)
    travel_mode: str = Field(..., min_length=1)
    include_collisions: bool = False
    baseline_months: int = Field(default=6, ge=3, le=24)
    hotspot_k: int = Field(default=20, ge=5, le=200)
    include_hotspot_stability: bool = True
    include_forecast: bool = True
    weight_crime: float = 1.0
    weight_collision: float = 0.0


class WatchlistCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    preference: Optional[WatchlistPreferencePayload] = None


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
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def _normalize_required_text(value, field_name):
    normalized = (value or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return normalized


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="min_lon must be less than max_lon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="min_lat must be less than max_lat")


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
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "window_months": row["window_months"],
        "crime_types": list(row["crime_types"] or []),
        "travel_mode": row["travel_mode"],
        "include_collisions": bool(row["include_collisions"]),
        "baseline_months": int(row["baseline_months"]),
        "hotspot_k": int(row["hotspot_k"]),
        "include_hotspot_stability": bool(row["include_hotspot_stability"]),
        "include_forecast": bool(row["include_forecast"]),
        "weight_crime": float(row["weight_crime"]),
        "weight_collision": float(row["weight_collision"]),
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


def _get_watchlist_preference(db, watchlist_id):
    query = text(
        """
        SELECT
            wp.id,
            wp.watchlist_id,
            wp.window_months,
            wp.crime_types,
            wp.travel_mode,
            wp.include_collisions,
            wp.baseline_months,
            wp.hotspot_k,
            wp.include_hotspot_stability,
            wp.include_forecast,
            wp.weight_crime,
            wp.weight_collision,
            wp.created_at
        FROM watchlist_preferences wp
        WHERE wp.watchlist_id = :watchlist_id
        ORDER BY wp.created_at DESC, wp.id DESC
        LIMIT 1
        """
    )
    row = _execute(db, query, {"watchlist_id": watchlist_id}).mappings().first()
    if not row:
        return None
    return _preference_to_dict(row)


def _replace_watchlist_preference(db, watchlist_id, user_id, preference):
    _execute(
        db,
        text(
            """
            DELETE FROM watchlist_preferences wp
            USING watchlists w
            WHERE wp.watchlist_id = :watchlist_id
              AND w.id = wp.watchlist_id
              AND w.user_id = :user_id
            """
        ),
        {"watchlist_id": watchlist_id, "user_id": user_id},
    )

    travel_mode = _normalize_required_text(preference.travel_mode, "travel_mode")
    query = text(
        """
        INSERT INTO watchlist_preferences (
            watchlist_id,
            window_months,
            crime_types,
            travel_mode,
            include_collisions,
            baseline_months,
            hotspot_k,
            include_hotspot_stability,
            include_forecast,
            weight_crime,
            weight_collision
        )
        SELECT
            w.id,
            :window_months,
            :crime_types,
            :travel_mode,
            :include_collisions,
            :baseline_months,
            :hotspot_k,
            :include_hotspot_stability,
            :include_forecast,
            :weight_crime,
            :weight_collision
        FROM watchlists w
        WHERE w.id = :watchlist_id AND w.user_id = :user_id
        RETURNING
            id,
            watchlist_id,
            window_months,
            crime_types,
            travel_mode,
            include_collisions,
            baseline_months,
            hotspot_k,
            include_hotspot_stability,
            include_forecast,
            weight_crime,
            weight_collision,
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
            "crime_types": _normalize_crime_types(preference.crime_types),
            "travel_mode": travel_mode,
            "include_collisions": preference.include_collisions,
            "baseline_months": preference.baseline_months,
            "hotspot_k": preference.hotspot_k,
            "include_hotspot_stability": preference.include_hotspot_stability,
            "include_forecast": preference.include_forecast,
            "weight_crime": preference.weight_crime,
            "weight_collision": preference.weight_collision,
        },
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return row


def _apply_preference(db, watchlist_id, user_id, preference):
    if preference is None:
        return
    _replace_watchlist_preference(db, watchlist_id, user_id, preference)


@router.get("/watchlists")
def read_watchlists(
    watchlist_id: Optional[int] = Query(default=None),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if watchlist_id is not None:
        row = _get_watchlist_row(db, watchlist_id, current_user["id"])
        return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preference(db, watchlist_id))}

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
            _watchlist_to_dict(row, _get_watchlist_preference(db, row["id"]))
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
        _apply_preference(db, row["id"], current_user["id"], payload.preference)
        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise HTTPException(status_code=400, detail="Unable to create watchlist") from exc

    return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preference(db, row["id"]))}


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

    if not update_fields and payload.preference is None:
        raise HTTPException(status_code=400, detail="Provide watchlist fields or preference to update")

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
                if hasattr(db, "rollback"):
                    db.rollback()
                raise HTTPException(status_code=404, detail="Watchlist not found")
        else:
            row = _get_watchlist_row(db, watchlist_id, current_user["id"])

        _apply_preference(db, watchlist_id, current_user["id"], payload.preference)
        if hasattr(db, "commit"):
            db.commit()
    except IntegrityError as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise HTTPException(status_code=400, detail="Unable to update watchlist") from exc

    return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preference(db, watchlist_id))}


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
        if hasattr(db, "rollback"):
            db.rollback()
        raise HTTPException(status_code=404, detail="Watchlist not found")

    if hasattr(db, "commit"):
        db.commit()
    return {"deleted": True, "watchlist_id": row["id"]}
