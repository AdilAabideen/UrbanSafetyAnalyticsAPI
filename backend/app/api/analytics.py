from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..api_utils.analytics_db_utils import (
    build_analytics_meta_payload,
    build_hotspot_stability_payload,
    build_risk_forecast_payload,
    build_risk_score_payload,
)
from ..api_utils.analytics_utils import AnalyticsAPIError
from ..db import get_db
from ..schemas.analytics_schemas import (
    AnalyticsMetaResponse,
    CrimeType,
    ForecastRequest,
    HotspotStabilityResponse,
    RiskForecastResponse,
    RiskScoreRequest,
    RiskScoreResponse,
    RouteCompareItem,
    RouteCompareRequest,
    RouteRiskRequest,
    ScoreWeights,
)


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/meta", response_model=AnalyticsMetaResponse)
def analytics_meta(db: Session = Depends(get_db)) -> AnalyticsMetaResponse:
    return build_analytics_meta_payload(db)


@router.post("/risk/score", response_model=RiskScoreResponse)
def analytics_risk_score(
    request: RiskScoreRequest,
    db: Session = Depends(get_db),
) -> RiskScoreResponse:
    return build_risk_score_payload(
        db,
        from_value=request.from_,
        to_value=request.to,
        min_lon=request.minLon,
        min_lat=request.minLat,
        max_lon=request.maxLon,
        max_lat=request.maxLat,
        crime_type=request.crimeType,
        include_collisions=request.includeCollisions,
        mode=request.mode,
        w_crime=request.weights.w_crime,
        w_collision=request.weights.w_collision,
    )


@router.post("/risk/forecast", response_model=RiskForecastResponse)
def analytics_risk_forecast(
    request: ForecastRequest,
    db: Session = Depends(get_db),
) -> RiskForecastResponse:
    return build_risk_forecast_payload(
        db,
        target=request.target,
        min_lon=request.minLon,
        min_lat=request.minLat,
        max_lon=request.maxLon,
        max_lat=request.maxLat,
        crime_type=request.crimeType,
        baseline_months=request.baselineMonths,
        method=request.method,
        return_risk_projection=request.returnRiskProjection,
        include_collisions=request.includeCollisions,
        mode=request.mode,
        w_crime=request.weights.w_crime,
        w_collision=request.weights.w_collision,
    )


@router.get("/patterns/hotspot-stability", response_model=HotspotStabilityResponse)
def analytics_hotspot_stability(
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
    k: int = Query(20, ge=5, le=200),
    includeLists: bool = Query(False),
    minLon: Optional[float] = Query(None),
    minLat: Optional[float] = Query(None),
    maxLon: Optional[float] = Query(None),
    maxLat: Optional[float] = Query(None),
    crimeType: Optional[CrimeType] = Query(None),
    db: Session = Depends(get_db),
) -> HotspotStabilityResponse:
    return build_hotspot_stability_payload(
        db,
        from_value=from_,
        to_value=to,
        k=k,
        include_lists=includeLists,
        min_lon=minLon,
        min_lat=minLat,
        max_lon=maxLon,
        max_lat=maxLat,
        crime_type=crimeType,
    )


__all__ = [
    "router",
    "AnalyticsAPIError",
    "build_risk_score_payload",
    "build_risk_forecast_payload",
    "build_hotspot_stability_payload",
    "ScoreWeights",
    "RiskScoreRequest",
    "ForecastRequest",
    "RouteRiskRequest",
    "RouteCompareItem",
    "RouteCompareRequest",
]
