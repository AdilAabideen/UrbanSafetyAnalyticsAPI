import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError
from sqlalchemy.orm import Session

from .analytics import (
    AnalyticsAPIError,
    _error_response as analytics_error_response,
    build_hotspot_stability_payload,
    build_risk_forecast_payload,
    build_risk_score_payload,
)
from .collisions import get_collision_analytics_summary
from .crimes import get_crime_analytics_summary
from .roads import get_road_analytics_overview, get_road_analytics_risk
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


class WatchlistReportWeightsPayload(BaseModel):
    w_crime: float = 1.0
    w_collision: float = 0.0


class WatchlistReportRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    crimeType: Optional[str] = None
    mode: Optional[str] = None
    includeCollisions: Optional[bool] = None
    weights: Optional[WatchlistReportWeightsPayload] = None
    k: Optional[int] = Field(default=None, ge=5, le=200)
    includeHotspotStability: Optional[bool] = None
    includeForecast: Optional[bool] = None
    forecastTarget: Optional[str] = None
    baselineMonths: Optional[int] = Field(default=None, ge=3, le=24)


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


def _normalize_mode(value: Optional[str]) -> Optional[str]:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    lowered = normalized.lower()
    if lowered in {"walk", "walking"}:
        return "walk"
    if lowered in {"drive", "driving"}:
        return "drive"
    return lowered


def _next_month_label(value: str) -> str:
    current = datetime.strptime(value, "%Y-%m").date()
    if current.month == 12:
        next_month = current.replace(year=current.year + 1, month=1, day=1)
    else:
        next_month = current.replace(month=current.month + 1, day=1)
    return next_month.strftime("%Y-%m")


def _default_report_preference(preference):
    return {
        "window_months": 6,
        "crime_types": [],
        "travel_mode": "walk",
        "include_collisions": False,
        "baseline_months": 6,
        "hotspot_k": 20,
        "include_hotspot_stability": True,
        "include_forecast": True,
        "weight_crime": 1.0,
        "weight_collision": 0.0,
        **(preference or {}),
    }


def _resolve_report_settings(preference, payload: WatchlistReportRequest):
    preference = _default_report_preference(preference)
    stored_crime_types = list(preference.get("crime_types") or [])
    applied_crime_type = _normalize_optional_text(payload.crimeType)
    if applied_crime_type is None and len(stored_crime_types) == 1:
        applied_crime_type = stored_crime_types[0]

    mode = _normalize_mode(payload.mode)
    if mode is None:
        mode = _normalize_mode(preference.get("travel_mode")) or "walk"

    include_collisions = (
        payload.includeCollisions
        if payload.includeCollisions is not None
        else bool(preference.get("include_collisions"))
    )
    include_hotspot = (
        payload.includeHotspotStability
        if payload.includeHotspotStability is not None
        else bool(preference.get("include_hotspot_stability"))
    )
    include_forecast = (
        payload.includeForecast
        if payload.includeForecast is not None
        else bool(preference.get("include_forecast"))
    )
    baseline_months = (
        payload.baselineMonths
        if payload.baselineMonths is not None
        else int(preference.get("baseline_months") or 6)
    )
    hotspot_k = payload.k if payload.k is not None else int(preference.get("hotspot_k") or 20)
    weight_crime = (
        payload.weights.w_crime if payload.weights is not None else float(preference.get("weight_crime") or 1.0)
    )
    weight_collision = (
        payload.weights.w_collision
        if payload.weights is not None
        else float(preference.get("weight_collision") or 0.0)
    )
    forecast_target = payload.forecastTarget or _next_month_label(payload.to)

    return {
        "crimeType": applied_crime_type,
        "storedCrimeTypes": stored_crime_types,
        "mode": mode,
        "includeCollisions": include_collisions,
        "weights": {
            "w_crime": weight_crime,
            "w_collision": weight_collision,
        },
        "k": hotspot_k,
        "includeHotspotStability": include_hotspot,
        "includeForecast": include_forecast,
        "forecastTarget": forecast_target,
        "baselineMonths": baseline_months,
        "windowMonths": int(preference.get("window_months") or 6),
    }


def _insert_watchlist_report(db, watchlist_id, user_id, request_payload, payload_json, effective_settings):
    encoded_weights = jsonable_encoder(effective_settings["weights"])
    encoded_payload = jsonable_encoder(payload_json)
    query = text(
        """
        INSERT INTO watchlist_reports (
            watchlist_id,
            user_id,
            from_month,
            to_month,
            forecast_target_month,
            crime_type,
            mode,
            include_collisions,
            weights,
            hotspot_k,
            include_hotspot_stability,
            include_forecast,
            baseline_months,
            payload_json
        )
        VALUES (
            :watchlist_id,
            :user_id,
            :from_month,
            :to_month,
            :forecast_target_month,
            :crime_type,
            :mode,
            :include_collisions,
            CAST(:weights AS JSONB),
            :hotspot_k,
            :include_hotspot_stability,
            :include_forecast,
            :baseline_months,
            CAST(:payload_json AS JSONB)
        )
        RETURNING id, created_at
        """
    )
    return _execute(
        db,
        query,
        {
            "watchlist_id": watchlist_id,
            "user_id": user_id,
            "from_month": datetime.strptime(request_payload.from_, "%Y-%m").date(),
            "to_month": datetime.strptime(request_payload.to, "%Y-%m").date(),
            "forecast_target_month": (
                datetime.strptime(effective_settings["forecastTarget"], "%Y-%m").date()
                if effective_settings["includeForecast"]
                else None
            ),
            "crime_type": effective_settings["crimeType"],
            "mode": effective_settings["mode"],
            "include_collisions": effective_settings["includeCollisions"],
            "weights": json.dumps(encoded_weights),
            "hotspot_k": effective_settings["k"],
            "include_hotspot_stability": effective_settings["includeHotspotStability"],
            "include_forecast": effective_settings["includeForecast"],
            "baseline_months": effective_settings["baselineMonths"],
            "payload_json": json.dumps(encoded_payload),
        },
    ).mappings().first()


def _report_list_item(row):
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "from": row["from_month"].strftime("%Y-%m"),
        "to": row["to_month"].strftime("%Y-%m"),
        "forecast_target": None
        if row["forecast_target_month"] is None
        else row["forecast_target_month"].strftime("%Y-%m"),
        "crime_type": row["crime_type"],
        "mode": row["mode"],
        "include_collisions": bool(row["include_collisions"]),
        "created_at": row["created_at"],
    }


def _get_watchlist_report_row(db, watchlist_id, report_id, user_id):
    query = text(
        """
        SELECT
            wr.id,
            wr.watchlist_id,
            wr.user_id,
            wr.from_month,
            wr.to_month,
            wr.forecast_target_month,
            wr.crime_type,
            wr.mode,
            wr.include_collisions,
            wr.payload_json,
            wr.created_at
        FROM watchlist_reports wr
        JOIN watchlists w ON w.id = wr.watchlist_id
        WHERE wr.id = :report_id
          AND wr.watchlist_id = :watchlist_id
          AND w.user_id = :user_id
        LIMIT 1
        """
    )
    row = _execute(
        db,
        query,
        {"report_id": report_id, "watchlist_id": watchlist_id, "user_id": user_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Report not found")
    return row


def _build_report_general_statistics(
    db,
    watchlist_row,
    payload: WatchlistReportRequest,
    effective_settings,
):
    crime_type_filters = (
        None
        if effective_settings["crimeType"] is None
        else [effective_settings["crimeType"]]
    )

    crime_summary = get_crime_analytics_summary(
        from_month=payload.from_,
        to_month=payload.to,
        minLon=watchlist_row["min_lon"],
        minLat=watchlist_row["min_lat"],
        maxLon=watchlist_row["max_lon"],
        maxLat=watchlist_row["max_lat"],
        crimeType=crime_type_filters,
        lastOutcomeCategory=None,
        lsoaName=None,
        db=db,
    )
    collision_summary = get_collision_analytics_summary(
        from_month=payload.from_,
        to_month=payload.to,
        minLon=watchlist_row["min_lon"],
        minLat=watchlist_row["min_lat"],
        maxLon=watchlist_row["max_lon"],
        maxLat=watchlist_row["max_lat"],
        collisionSeverity=None,
        roadType=None,
        lsoaCode=None,
        weatherCondition=None,
        lightCondition=None,
        roadSurfaceCondition=None,
        db=db,
    )
    road_overview = get_road_analytics_overview(
        from_month=payload.from_,
        to_month=payload.to,
        minLon=watchlist_row["min_lon"],
        minLat=watchlist_row["min_lat"],
        maxLon=watchlist_row["max_lon"],
        maxLat=watchlist_row["max_lat"],
        crimeType=crime_type_filters,
        lastOutcomeCategory=None,
        highway=None,
        db=db,
    )
    road_risk = get_road_analytics_risk(
        from_month=payload.from_,
        to_month=payload.to,
        minLon=watchlist_row["min_lon"],
        minLat=watchlist_row["min_lat"],
        maxLon=watchlist_row["max_lon"],
        maxLat=watchlist_row["max_lat"],
        crimeType=crime_type_filters,
        lastOutcomeCategory=None,
        highway=None,
        limit=5,
        sort="risk_score",
        db=db,
    )
    return {
        "crime_summary": crime_summary,
        "collision_summary": collision_summary,
        "roads_overview": road_overview,
        "top_risky_roads": road_risk.get("items", []),
        "headline": {
            "most_dangerous_road": None if not road_risk.get("items") else road_risk["items"][0],
            "worst_crime_category": crime_summary.get("top_crime_type"),
            "total_crimes": crime_summary.get("total_crimes", 0),
            "total_collisions": collision_summary.get("total_collisions", 0),
        },
    }


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


@router.get("/watchlists/{watchlist_id}/reports")
def read_watchlist_reports(
    watchlist_id: int,
    limit: int = Query(default=20, ge=1, le=100),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_watchlist_row(db, watchlist_id, current_user["id"])
    query = text(
        """
        SELECT
            wr.id,
            wr.watchlist_id,
            wr.from_month,
            wr.to_month,
            wr.forecast_target_month,
            wr.crime_type,
            wr.mode,
            wr.include_collisions,
            wr.created_at
        FROM watchlist_reports wr
        WHERE wr.watchlist_id = :watchlist_id
          AND wr.user_id = :user_id
        ORDER BY wr.created_at DESC, wr.id DESC
        LIMIT :limit
        """
    )
    rows = _execute(
        db,
        query,
        {"watchlist_id": watchlist_id, "user_id": current_user["id"], "limit": limit},
    ).mappings().all()
    return {"items": [_report_list_item(row) for row in rows]}


@router.get("/watchlists/{watchlist_id}/reports/{report_id}")
def read_watchlist_report(
    watchlist_id: int,
    report_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_watchlist_row(db, watchlist_id, current_user["id"])
    row = _get_watchlist_report_row(db, watchlist_id, report_id, current_user["id"])
    payload = dict(row["payload_json"] or {})
    payload["id"] = row["id"]
    payload["snapshot_id"] = row["id"]
    payload["stored_at"] = row["created_at"]
    return {"report": payload}


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
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Unable to create watchlist") from exc

    return {"watchlist": _watchlist_to_dict(row, _get_watchlist_preference(db, row["id"]))}


@router.post("/watchlists/{watchlist_id}/report")
def create_watchlist_report(
    watchlist_id: int,
    payload: WatchlistReportRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        watchlist_row = _get_watchlist_row(db, watchlist_id, current_user["id"])
        preference = _get_watchlist_preference(db, watchlist_id)
        effective_settings = _resolve_report_settings(preference, payload)

        score = build_risk_score_payload(
            db,
            from_value=payload.from_,
            to_value=payload.to,
            min_lon=watchlist_row["min_lon"],
            min_lat=watchlist_row["min_lat"],
            max_lon=watchlist_row["max_lon"],
            max_lat=watchlist_row["max_lat"],
            crime_type=effective_settings["crimeType"],
            include_collisions=effective_settings["includeCollisions"],
            mode=effective_settings["mode"],
            w_crime=effective_settings["weights"]["w_crime"],
            w_collision=effective_settings["weights"]["w_collision"],
        )

        forecast = None
        if effective_settings["includeForecast"]:
            forecast = build_risk_forecast_payload(
                db,
                target=effective_settings["forecastTarget"],
                min_lon=watchlist_row["min_lon"],
                min_lat=watchlist_row["min_lat"],
                max_lon=watchlist_row["max_lon"],
                max_lat=watchlist_row["max_lat"],
                crime_type=effective_settings["crimeType"],
                baseline_months=effective_settings["baselineMonths"],
                method="poisson_mean",
                return_risk_projection=True,
                include_collisions=effective_settings["includeCollisions"],
                mode=effective_settings["mode"],
                w_crime=effective_settings["weights"]["w_crime"],
                w_collision=effective_settings["weights"]["w_collision"],
            )

        hotspot_stability = None
        if effective_settings["includeHotspotStability"]:
            hotspot_stability = build_hotspot_stability_payload(
                db,
                from_value=payload.from_,
                to_value=payload.to,
                k=effective_settings["k"],
                include_lists=False,
                min_lon=watchlist_row["min_lon"],
                min_lat=watchlist_row["min_lat"],
                max_lon=watchlist_row["max_lon"],
                max_lat=watchlist_row["max_lat"],
                crime_type=effective_settings["crimeType"],
            )

        general_statistics = _build_report_general_statistics(
            db,
            watchlist_row,
            payload,
            effective_settings,
        )

        report_body = {
            "watchlist": {
                "id": watchlist_row["id"],
                "name": watchlist_row["name"],
                "bbox": {
                    "minLon": watchlist_row["min_lon"],
                    "minLat": watchlist_row["min_lat"],
                    "maxLon": watchlist_row["max_lon"],
                    "maxLat": watchlist_row["max_lat"],
                },
            },
            "preferences_used": effective_settings,
            "general_statistics": general_statistics,
            "score": score,
            "generated_at": score["generated_at"],
        }
        if forecast is not None:
            report_body["forecast"] = forecast
        if hotspot_stability is not None:
            report_body["hotspot_stability"] = hotspot_stability

        report_row = _insert_watchlist_report(
            db,
            watchlist_id,
            current_user["id"],
            payload,
            report_body,
            effective_settings,
        )
        db.commit()

        report_body["id"] = report_row["id"]
        report_body["snapshot_id"] = report_row["id"]
        report_body["stored_at"] = report_row["created_at"]
        return {"report": report_body}
    except AnalyticsAPIError as exc:
        db.rollback()
        return analytics_error_response(exc)


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
                db.rollback()
                raise HTTPException(status_code=404, detail="Watchlist not found")
        else:
            row = _get_watchlist_row(db, watchlist_id, current_user["id"])

        _apply_preference(db, watchlist_id, current_user["id"], payload.preference)
        db.commit()
    except IntegrityError as exc:
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
        db.rollback()
        raise HTTPException(status_code=404, detail="Watchlist not found")

    db.commit()
    return {"deleted": True, "watchlist_id": row["id"]}
