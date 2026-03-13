from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .enums import CrimeType


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
    crimeType: Optional[CrimeType] = None
    includeCollisions: bool = False
    mode: str = "walk"
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class ForecastRequest(BaseModel):
    target: str
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float
    crimeType: Optional[CrimeType] = None
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
    crimeType: Optional[CrimeType] = None
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
    crimeType: Optional[CrimeType] = None
    includeCollisions: bool = False
    routes: List[RouteCompareItem]


class AnalyticsMetaResponse(BaseModel):
    months: Dict[str, Optional[str]]
    crime_types: List[str]
    counts: Dict[str, Any]


class AnalyticsScope(BaseModel):
    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    target: Optional[str] = None
    baselineMonths: Optional[int] = None
    bbox: Optional[Dict[str, Any]] = None
    mode: Optional[str] = None
    crimeType: Optional[str] = None
    includeCollisions: Optional[bool] = None
    method: Optional[str] = None
    k: Optional[int] = None


class RiskScoreResponse(BaseModel):
    scope: AnalyticsScope
    generated_at: str
    score_basis: str
    risk_score: int
    score: int
    pct: float
    band: str
    metrics: Dict[str, Any]
    explain: Dict[str, Any]


class RiskForecastResponse(BaseModel):
    scope: AnalyticsScope
    generated_at: str
    score_basis: str
    history: List[Dict[str, Any]]
    forecast: Dict[str, Any]
    explanation: Dict[str, Any]


class HotspotStabilitySeriesItem(BaseModel):
    month: str
    jaccard_vs_prev: float
    overlap_count: int


class PersistentHotspotItem(BaseModel):
    segment_id: int
    appearances: int
    appearance_ratio: float


class HotspotSummary(BaseModel):
    months_evaluated: int
    average_jaccard: float
    persistent_hotspot_count: int
    notes: str


class HotspotStabilityResponse(BaseModel):
    scope: AnalyticsScope
    generated_at: str
    stability_series: List[HotspotStabilitySeriesItem]
    persistent_hotspots: List[PersistentHotspotItem]
    summary: HotspotSummary
    topk_by_month: Optional[List[Dict[str, Any]]] = None

