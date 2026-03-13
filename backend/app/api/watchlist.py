import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError
from sqlalchemy.orm import Session

from .analytics import (
    AnalyticsAPIError,
    build_hotspot_stability_payload,
    build_risk_forecast_payload,
    build_risk_score_payload,
)
from ..api_utils.auth_utils import get_current_user
from ..db import get_db


router = APIRouter(tags=["watchlists"])

FIXED_HOTSPOT_K = 20
FIXED_INCLUDE_HOTSPOT_STABILITY = True
FIXED_INCLUDE_FORECAST = True
FIXED_WEIGHT_CRIME = 1.0
FIXED_WEIGHT_COLLISION = 0.8


class WatchlistPreferencePayload(BaseModel):
    window_months: int = Field(..., ge=1)
    crime_types: List[str] = Field(default_factory=list)
    travel_mode: str = Field(..., min_length=1)
    include_collisions: bool = False
    baseline_months: int = Field(default=6, ge=3, le=24)


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
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported watchlist travel_mode '{value}' for {error_context}. Use walk or drive.",
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
        raise HTTPException(
            status_code=400,
            detail="include_collisions is only supported when travel_mode is drive",
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
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "window_months": row["window_months"],
        "crime_types": list(row["crime_types"] or []),
        "travel_mode": _serialize_watchlist_mode(row["travel_mode"]),
        "include_collisions": bool(row["include_collisions"]),
        "baseline_months": int(row["baseline_months"]),
        "hotspot_k": FIXED_HOTSPOT_K,
        "include_hotspot_stability": FIXED_INCLUDE_HOTSPOT_STABILITY,
        "include_forecast": FIXED_INCLUDE_FORECAST,
        "weight_crime": FIXED_WEIGHT_CRIME,
        "weight_collision": FIXED_WEIGHT_COLLISION,
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

    travel_mode = _normalize_watchlist_mode(preference.travel_mode, error_context="watchlist preference")
    include_collisions = bool(preference.include_collisions)
    _validate_watchlist_collision_mode(travel_mode, include_collisions)
    query = text(
        """
        INSERT INTO watchlist_preferences (
            watchlist_id,
            window_months,
            crime_types,
            travel_mode,
            include_collisions,
            baseline_months
        )
        SELECT
            w.id,
            :window_months,
            :crime_types,
            :travel_mode,
            :include_collisions,
            :baseline_months
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
            "include_collisions": include_collisions,
            "baseline_months": preference.baseline_months,
        },
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return row


def _apply_preference(db, watchlist_id, user_id, preference):
    if preference is None:
        return
    _replace_watchlist_preference(db, watchlist_id, user_id, preference)


def _month_label(month_date):
    return month_date.strftime("%Y-%m")


def _shift_month(month_date, offset: int):
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _latest_complete_month(db: Session, include_collisions: bool = False):
    current_month = datetime.now(timezone.utc).date().replace(day=1)
    calendar_latest = _shift_month(current_month, -1)

    crime_row = _execute(db, text("SELECT MAX(month) AS max_month FROM crime_events"), {}).mappings().first() or {}
    crime_max_month = crime_row.get("max_month")
    if crime_max_month is None:
        raise HTTPException(status_code=400, detail="Crime data is unavailable")

    dataset_latest = crime_max_month
    if include_collisions:
        collision_row = _execute(
            db,
            text("SELECT MAX(month) AS max_month FROM collision_events"),
            {},
        ).mappings().first() or {}
        collision_max_month = collision_row.get("max_month")
        if collision_max_month is None:
            raise HTTPException(
                status_code=400,
                detail="Collision data is unavailable while include_collisions is enabled",
            )
        dataset_latest = min(dataset_latest, collision_max_month)

    return min(calendar_latest, dataset_latest)


def _watchlist_bbox(row):
    return {
        "min_lon": row["min_lon"],
        "min_lat": row["min_lat"],
        "max_lon": row["max_lon"],
        "max_lat": row["max_lat"],
    }


def _require_watchlist_preference(db, watchlist_id):
    preference = _get_watchlist_preference(db, watchlist_id)
    if preference is None:
        raise HTTPException(status_code=400, detail="Watchlist preference is required to run analytics")
    return preference


def _crime_type_inputs(preference):
    crime_types = _normalize_crime_types(preference.get("crime_types") or [])
    return crime_types or [None]


def _analytics_error_response(exc: AnalyticsAPIError) -> JSONResponse:
    payload = {"error": exc.error, "message": exc.message}
    if exc.details is not None:
        payload["details"] = exc.details
    return JSONResponse(status_code=exc.status_code, content=payload)


def _store_watchlist_run(
    db: Session,
    *,
    watchlist_id: int,
    report_type: str,
    request_params: Dict[str, Any],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    query = text(
        """
        INSERT INTO watchlist_analytics_runs (
            watchlist_id,
            report_type,
            request_params_json,
            payload_json
        )
        VALUES (
            :watchlist_id,
            :report_type,
            CAST(:request_params_json AS JSONB),
            CAST(:payload_json AS JSONB)
        )
        RETURNING id, created_at
        """
    )
    row = _execute(
        db,
        query,
        {
            "watchlist_id": watchlist_id,
            "report_type": report_type,
            "request_params_json": json.dumps(jsonable_encoder(request_params)),
            "payload_json": json.dumps(jsonable_encoder(payload)),
        },
    ).mappings().first()
    if hasattr(db, "commit"):
        db.commit()
    return {"id": row["id"], "created_at": row["created_at"]}


def _wrap_watchlist_run_response(
    *,
    watchlist_id: int,
    report_type: str,
    request_params: Dict[str, Any],
    result: Dict[str, Any],
    stored: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "watchlist_id": watchlist_id,
        "report_type": report_type,
        "watchlist_run_id": stored["id"],
        "stored_at": stored["created_at"],
        "request": request_params,
        "result": result,
    }


def _coerce_json_value(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _watchlist_run_to_dict(row):
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "report_type": row["report_type"],
        "request": _coerce_json_value(row["request_params_json"]),
        "result": _coerce_json_value(row["payload_json"]),
        "created_at": row["created_at"],
    }


def _read_watchlist_results(
    db: Session,
    *,
    watchlist_id: int,
    report_type: str,
    run_id: Optional[int],
    limit: int,
):
    base_select = """
        SELECT
            war.id,
            war.watchlist_id,
            war.report_type,
            war.request_params_json,
            war.payload_json,
            war.created_at
        FROM watchlist_analytics_runs war
        WHERE war.watchlist_id = :watchlist_id
          AND war.report_type = :report_type
    """
    params = {
        "watchlist_id": watchlist_id,
        "report_type": report_type,
        "limit": limit,
    }
    if run_id is not None:
        query = text(
            base_select
            + """
          AND war.id = :run_id
        ORDER BY war.created_at DESC, war.id DESC
        LIMIT :limit
        """
        )
        params["run_id"] = run_id
    else:
        query = text(
            base_select
            + """
        ORDER BY war.created_at DESC, war.id DESC
        LIMIT :limit
        """
        )

    rows = _execute(db, query, params).mappings().all()

    items = [_watchlist_run_to_dict(row) for row in rows]
    if run_id is not None and not items:
        raise HTTPException(status_code=404, detail="Watchlist result not found")
    return {"items": items}


def _run_watchlist_risk_score(db: Session, watchlist_id: int, watchlist_row, preference):
    mode = _normalize_watchlist_mode(preference["travel_mode"], error_context="watchlist analytics")
    include_collisions = bool(preference["include_collisions"])
    _validate_watchlist_collision_mode(mode, include_collisions)
    latest_complete_month = _latest_complete_month(db, include_collisions)
    from_date = _shift_month(latest_complete_month, -(int(preference["window_months"]) - 1))
    request_params = {
        "from": _month_label(from_date),
        "to": _month_label(latest_complete_month),
        "bbox": _watchlist_bbox(watchlist_row),
        "crime_types": _normalize_crime_types(preference.get("crime_types") or []),
        "mode": mode,
        "includeCollisions": include_collisions,
        "weights": {
            "w_crime": FIXED_WEIGHT_CRIME,
            "w_collision": FIXED_WEIGHT_COLLISION,
        },
    }
    results_by_crime_type = {}
    for crime_type in _crime_type_inputs(preference):
        key = crime_type or "all"
        results_by_crime_type[key] = build_risk_score_payload(
            db,
            from_value=request_params["from"],
            to_value=request_params["to"],
            min_lon=watchlist_row["min_lon"],
            min_lat=watchlist_row["min_lat"],
            max_lon=watchlist_row["max_lon"],
            max_lat=watchlist_row["max_lat"],
            crime_type=crime_type,
            include_collisions=include_collisions,
            mode=mode,
            w_crime=FIXED_WEIGHT_CRIME,
            w_collision=FIXED_WEIGHT_COLLISION,
        )

    result = {"results_by_crime_type": results_by_crime_type}
    stored = _store_watchlist_run(
        db,
        watchlist_id=watchlist_id,
        report_type="risk_score",
        request_params=request_params,
        payload=result,
    )
    return _wrap_watchlist_run_response(
        watchlist_id=watchlist_id,
        report_type="risk_score",
        request_params=request_params,
        result=result,
        stored=stored,
    )


def _run_watchlist_risk_forecast(db: Session, watchlist_id: int, watchlist_row, preference):
    mode = _normalize_watchlist_mode(preference["travel_mode"], error_context="watchlist analytics")
    include_collisions = bool(preference["include_collisions"])
    _validate_watchlist_collision_mode(mode, include_collisions)
    latest_complete_month = _latest_complete_month(db, include_collisions)
    target_month = _shift_month(latest_complete_month, 1)
    request_params = {
        "target": _month_label(target_month),
        "baselineMonths": int(preference["baseline_months"]),
        "bbox": _watchlist_bbox(watchlist_row),
        "crime_types": _normalize_crime_types(preference.get("crime_types") or []),
        "mode": mode,
        "includeCollisions": include_collisions,
        "returnRiskProjection": True,
        "weights": {
            "w_crime": FIXED_WEIGHT_CRIME,
            "w_collision": FIXED_WEIGHT_COLLISION,
        },
    }
    results_by_crime_type = {}
    for crime_type in _crime_type_inputs(preference):
        key = crime_type or "all"
        results_by_crime_type[key] = build_risk_forecast_payload(
            db,
            target=request_params["target"],
            min_lon=watchlist_row["min_lon"],
            min_lat=watchlist_row["min_lat"],
            max_lon=watchlist_row["max_lon"],
            max_lat=watchlist_row["max_lat"],
            crime_type=crime_type,
            baseline_months=int(preference["baseline_months"]),
            method="poisson_mean",
            return_risk_projection=True,
            include_collisions=include_collisions,
            mode=mode,
            w_crime=FIXED_WEIGHT_CRIME,
            w_collision=FIXED_WEIGHT_COLLISION,
        )

    result = {"results_by_crime_type": results_by_crime_type}
    stored = _store_watchlist_run(
        db,
        watchlist_id=watchlist_id,
        report_type="risk_forecast",
        request_params=request_params,
        payload=result,
    )
    return _wrap_watchlist_run_response(
        watchlist_id=watchlist_id,
        report_type="risk_forecast",
        request_params=request_params,
        result=result,
        stored=stored,
    )


def _run_watchlist_hotspot_stability(db: Session, watchlist_id: int, watchlist_row, preference):
    latest_complete_month = _latest_complete_month(db, include_collisions=False)
    from_date = _shift_month(latest_complete_month, -(int(preference["window_months"]) - 1))
    request_params = {
        "from": _month_label(from_date),
        "to": _month_label(latest_complete_month),
        "bbox": _watchlist_bbox(watchlist_row),
        "crime_types": _normalize_crime_types(preference.get("crime_types") or []),
        "k": FIXED_HOTSPOT_K,
        "includeLists": True,
    }
    results_by_crime_type = {}
    for crime_type in _crime_type_inputs(preference):
        key = crime_type or "all"
        results_by_crime_type[key] = build_hotspot_stability_payload(
            db,
            from_value=request_params["from"],
            to_value=request_params["to"],
            k=FIXED_HOTSPOT_K,
            include_lists=True,
            min_lon=watchlist_row["min_lon"],
            min_lat=watchlist_row["min_lat"],
            max_lon=watchlist_row["max_lon"],
            max_lat=watchlist_row["max_lat"],
            crime_type=crime_type,
        )

    result = {"results_by_crime_type": results_by_crime_type}
    stored = _store_watchlist_run(
        db,
        watchlist_id=watchlist_id,
        report_type="hotspot_stability",
        request_params=request_params,
        payload=result,
    )
    return _wrap_watchlist_run_response(
        watchlist_id=watchlist_id,
        report_type="hotspot_stability",
        request_params=request_params,
        result=result,
        stored=stored,
    )


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


@router.post("/watchlists/{watchlist_id}/risk-score/run")
def run_watchlist_risk_score(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    watchlist_row = _get_watchlist_row(db, watchlist_id, current_user["id"])
    preference = _require_watchlist_preference(db, watchlist_id)
    try:
        return _run_watchlist_risk_score(db, watchlist_id, watchlist_row, preference)
    except AnalyticsAPIError as exc:
        return _analytics_error_response(exc)


@router.get("/watchlists/{watchlist_id}/risk-score/results")
def read_watchlist_risk_score_results(
    watchlist_id: int,
    run_id: Optional[int] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_watchlist_row(db, watchlist_id, current_user["id"])
    return _read_watchlist_results(
        db,
        watchlist_id=watchlist_id,
        report_type="risk_score",
        run_id=run_id,
        limit=limit,
    )


@router.post("/watchlists/{watchlist_id}/risk-forecast/run")
def run_watchlist_risk_forecast(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    watchlist_row = _get_watchlist_row(db, watchlist_id, current_user["id"])
    preference = _require_watchlist_preference(db, watchlist_id)
    try:
        return _run_watchlist_risk_forecast(db, watchlist_id, watchlist_row, preference)
    except AnalyticsAPIError as exc:
        return _analytics_error_response(exc)


@router.get("/watchlists/{watchlist_id}/risk-forecast/results")
def read_watchlist_risk_forecast_results(
    watchlist_id: int,
    run_id: Optional[int] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_watchlist_row(db, watchlist_id, current_user["id"])
    return _read_watchlist_results(
        db,
        watchlist_id=watchlist_id,
        report_type="risk_forecast",
        run_id=run_id,
        limit=limit,
    )


@router.post("/watchlists/{watchlist_id}/hotspot-stability/run")
def run_watchlist_hotspot_stability(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    watchlist_row = _get_watchlist_row(db, watchlist_id, current_user["id"])
    preference = _require_watchlist_preference(db, watchlist_id)
    try:
        return _run_watchlist_hotspot_stability(db, watchlist_id, watchlist_row, preference)
    except AnalyticsAPIError as exc:
        return _analytics_error_response(exc)


@router.get("/watchlists/{watchlist_id}/hotspot-stability/results")
def read_watchlist_hotspot_stability_results(
    watchlist_id: int,
    run_id: Optional[int] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_watchlist_row(db, watchlist_id, current_user["id"])
    return _read_watchlist_results(
        db,
        watchlist_id=watchlist_id,
        report_type="hotspot_stability",
        run_id=run_id,
        limit=limit,
    )
