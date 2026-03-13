from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class RoadsAnalyticsMetaResponse(BaseModel):
    months: Dict[str, Optional[str]]
    highways: List[str]
    crime_types: List[str]
    outcomes: List[str]
    counts: Dict[str, Any]


class RoadsAnalyticsOverviewResponse(BaseModel):
    filters: Dict[str, Any]
    total_segments: int
    total_length_m: float
    roads_with_incidents: int
    roads_without_incidents: int
    road_coverage_pct: float
    unique_highway_types: int
    total_incidents: int
    avg_incidents_per_km: float
    top_road: Optional[Dict[str, Any]] = None
    top_highway: Optional[Dict[str, Any]] = None
    top_crime_type: Optional[Dict[str, Any]] = None
    top_outcome: Optional[Dict[str, Any]] = None
    current_period: Dict[str, Any]
    previous_period: Dict[str, Any]
    current_vs_previous_pct: Optional[float] = None
    band_breakdown: Dict[str, int]
    insights: List[str]


class RoadsAnalyticsChartsResponse(BaseModel):
    filters: Dict[str, Any]
    timeseries: Dict[str, Any]
    by_highway: List[Dict[str, Any]]
    by_crime_type: List[Dict[str, Any]]
    by_outcome: List[Dict[str, Any]]
    band_breakdown: Dict[str, int]
    insights: List[str]


class RoadsAnalyticsRiskResponse(BaseModel):
    filters: Dict[str, Any]
    items: List[Dict[str, Any]]
    meta: Dict[str, Any]
