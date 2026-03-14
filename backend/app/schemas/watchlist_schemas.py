from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class WatchlistPreferencePayload(BaseModel):
    start_month: date
    end_month: date
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
    preference: WatchlistPreferencePayload



class WatchlistPreference(BaseModel):
    start_month: date
    end_month: date
    crime_types: List[str]
    travel_mode: str
    include_collisions: bool
    baseline_months: int
    hotspot_k: int
    include_hotspot_stability: bool
    include_forecast: bool
    weight_crime: float
    weight_collision: float


class Watchlist(BaseModel):
    id: int
    user_id: int
    name: str
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    created_at: datetime
    preference: Optional[WatchlistPreference] = None


class WatchlistSingleResponse(BaseModel):
    watchlist: Watchlist


class WatchlistListResponse(BaseModel):
    items: List[Watchlist]


class WatchlistDeleteResponse(BaseModel):
    deleted: bool
    watchlist_id: int


class WatchlistRunResult(BaseModel):
    watchlist_id: int
    report_type: str
    watchlist_run_id: int
    stored_at: datetime
    request: Dict[str, Any]
    result: Dict[str, Any]


class WatchlistRunListItem(BaseModel):
    id: int
    watchlist_id: int
    report_type: str
    request: Dict[str, Any]
    result: Dict[str, Any]
    created_at: datetime


class WatchlistRunListResponse(BaseModel):
    items: List[WatchlistRunListItem]
