from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class WatchlistPreference(BaseModel):
    id: int
    watchlist_id: int
    window_months: int
    crime_types: List[str]
    travel_mode: str
    include_collisions: bool
    baseline_months: int
    hotspot_k: int
    include_hotspot_stability: bool
    include_forecast: bool
    weight_crime: float
    weight_collision: float
    created_at: datetime


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

