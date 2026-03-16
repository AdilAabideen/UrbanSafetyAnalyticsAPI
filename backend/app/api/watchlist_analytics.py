from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas.watchlist_analytics_schemas import (
    WatchlistBasicMetricsResponse,
    WatchlistForecastRequest,
    WatchlistForecastResponse,
    WatchlistMapEventsResponse,
    WatchlistRiskRunsResponse,
    WatchlistRiskScoreResponse,
)
from ..services.auth_service import get_current_user
from ..services.watchlist_analytics_service import (
    build_watchlist_basic_metrics_service,
    build_watchlist_forecast_service,
    build_watchlist_map_events_service,
    build_watchlist_risk_score_service,
    list_watchlist_risk_runs_service,
)


router = APIRouter(tags=["watchlist-analytics"])


@router.post(
    "/watchlists/{watchlist_id}/analytics/risk-score",
    response_model=WatchlistRiskScoreResponse,
    summary="Compute Watchlist Risk Score",
    description=(
        "Runs the watchlist risk algorithm using the watchlist bbox and stored preference window "
        "(start_month/end_month). The algorithm builds three core signals inside the bbox: "
        "(1) a crime component using crime-type harm weights, monthly recency decay, area "
        "normalization, and persistence across active months; "
        "(2) a collision component using severity-weighted collision points with recency decay "
        "normalized by effective road length; and "
        "(3) a lightweight user-report support signal with cluster caps and faster recency decay. "
        "Signals are blended with mode-specific weights (walk/drive) into a raw score, then "
        "mapped to a normalized 0-100 risk score. "
        "The run is persisted, then compared against prior runs with the same signature "
        "(month window + crime types + mode). If historical cohort size is below threshold, "
        "the service falls back to nearest reference bboxes and compares against cached or "
        "recalculated reference runs. "
        "Returns a condensed payload with risk result and comparison proof fields."
    ),
    responses={
        200: {
            "description": "Risk score computed and persisted successfully.",
        },
        400: {
            "description": "Watchlist is missing required analytics preference fields or has invalid values.",
        },
        404: {
            "description": "Watchlist not found for the authenticated user.",
        },
    },
)
def compute_watchlist_risk_score(
    watchlist_id: int = Path(
        ...,
        gt=0,
        description="Unique watchlist identifier owned by the authenticated user.",
        example=42,
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistRiskScoreResponse:
    return build_watchlist_risk_score_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )


@router.get(
    "/watchlists/{watchlist_id}/analytics/risk-score/runs",
    response_model=WatchlistRiskRunsResponse,
    summary="List Previous Watchlist Risk Runs",
    description=(
        "Returns previously persisted analytical risk-score runs for the selected watchlist, "
        "ordered newest first. Includes risk score, component breakdown, and stored comparison "
        "metadata for each historical run."
    ),
    responses={
        200: {"description": "Historical risk runs returned successfully."},
        404: {"description": "Watchlist not found for the authenticated user."},
    },
)
def list_watchlist_risk_runs(
    watchlist_id: int = Path(
        ...,
        gt=0,
        description="Unique watchlist identifier owned by the authenticated user.",
        example=42,
    ),
    limit: int = Query(
        50,
        ge=1,
        le=500,
        description="Maximum number of historical runs to return.",
        example=50,
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistRiskRunsResponse:
    return list_watchlist_risk_runs_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
        limit=limit,
    )


@router.get(
    "/watchlists/{watchlist_id}/analytics/basic-metrics",
    response_model=WatchlistBasicMetricsResponse,
    summary="Get Basic Watchlist Metrics",
    description=(
        "Returns a lightweight analytics summary for the watchlist's stored bbox and month window "
        "(start_month to end_month). "
        "This endpoint is intentionally condensed and returns exactly 5 top-level outputs: "
        "number_of_crimes, number_of_collisions, number_of_user_reported_events, "
        "most_dangerous_roads (top 5), and crime_category_breakdown."
    ),
    responses={
        200: {"description": "Basic metrics returned successfully."},
        400: {"description": "Watchlist has missing/invalid analytics month window or bbox values."},
        404: {"description": "Watchlist not found for the authenticated user."},
    },
)
def get_watchlist_basic_metrics(
    watchlist_id: int = Path(
        ...,
        gt=0,
        description="Unique watchlist identifier owned by the authenticated user.",
        example=42,
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistBasicMetricsResponse:
    return build_watchlist_basic_metrics_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )


@router.get(
    "/watchlists/{watchlist_id}/analytics/map-events",
    response_model=WatchlistMapEventsResponse,
    summary="Get Watchlist Map Events",
    description=(
        "Returns GeoJSON point layers scoped to the watchlist bbox and stored month window "
        "(start_month to end_month). The payload includes three FeatureCollections: "
        "official crimes, official collisions, and approved user-reported events. "
        "Designed for frontend map overlays alongside road tiles."
    ),
    responses={
        200: {"description": "Watchlist map event layers returned successfully."},
        400: {"description": "Watchlist has missing/invalid month window or bbox values."},
        404: {"description": "Watchlist not found for the authenticated user."},
    },
)
def get_watchlist_map_events(
    watchlist_id: int = Path(
        ...,
        gt=0,
        description="Unique watchlist identifier owned by the authenticated user.",
        example=42,
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistMapEventsResponse:
    return build_watchlist_map_events_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )


@router.post(
    "/watchlists/{watchlist_id}/analytics/forecast",
    response_model=WatchlistForecastResponse,
    summary="Forecast Watchlist Next Month",
    description=(
        "Computes next-month forecast for a watchlist using the watchlist bbox plus historical monthly "
        "baseline from startMonth to the last complete month. The forecast blends official crime counts, "
        "approved user-reported crime signal, and collision severity points, then applies recency weights "
        "(lambda=0.85) and mode-based emphasis (walk/drive). Returns score, conservative band, expected "
        "crime/collision outputs, and Poisson-style intervals."
    ),
    responses={
        200: {"description": "Forecast computed successfully."},
        400: {"description": "Invalid startMonth/mode or invalid watchlist bbox configuration."},
        404: {"description": "Watchlist not found for the authenticated user."},
    },
)
def forecast_watchlist_next_month(
    request: WatchlistForecastRequest,
    watchlist_id: int = Path(
        ...,
        gt=0,
        description="Unique watchlist identifier owned by the authenticated user.",
        example=2,
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistForecastResponse:
    """Forecast next-month watchlist outcomes without persisting result rows."""
    return build_watchlist_forecast_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
        start_month=request.start_month,
        mode=request.mode,
        crime_types=request.crime_types,
    )
