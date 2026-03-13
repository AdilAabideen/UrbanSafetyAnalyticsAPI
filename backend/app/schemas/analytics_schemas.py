from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ScoreWeights(BaseModel):
    w_crime: float = 1.0
    w_collision: float = 0.0


class RiskScoreRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    mode: str = "walk"
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class ForecastRequest(BaseModel):
    target: str
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float
    crimeType: Optional[str] = None
    baselineMonths: int = 6
    method: str = "poisson_mean"
    returnRiskProjection: bool = False
    includeCollisions: bool = False
    mode: str = "walk"
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class RouteRiskRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    mode: str = "walk"
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    segment_ids: Optional[List[int]] = None
    route_line: Optional[Dict[str, Any]] = None
    checkConnectivity: bool = True
    threshold_m: float = 20.0
    buffer_m: float = 25.0
    failOnDisconnect: bool = False


class RouteCompareItem(BaseModel):
    name: str
    segment_ids: Optional[List[int]] = None
    route_line: Optional[Dict[str, Any]] = None
    checkConnectivity: bool = True
    threshold_m: float = 20.0
    buffer_m: float = 25.0
    failOnDisconnect: bool = False


class RouteCompareRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    mode: str = "walk"
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    routes: List[RouteCompareItem]
