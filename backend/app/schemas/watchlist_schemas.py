from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class WatchlistPreferencePayload(BaseModel):
    """Preference payload used when creating/updating watchlists."""

    start_month: date
    end_month: date
    crime_types: List[str] = Field(default_factory=list)
    travel_mode: str = Field(..., min_length=1)
    include_collisions: bool = False
    baseline_months: int = Field(default=6, ge=3, le=24)


class WatchlistCreateRequest(BaseModel):
    """Request payload for creating a watchlist."""

    name: str = Field(..., min_length=1)
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    preference: Optional[WatchlistPreferencePayload] = None


class WatchlistUpdateRequest(BaseModel):
    """Request payload for patching an existing watchlist."""

    name: Optional[str] = None
    min_lon: Optional[float] = None
    min_lat: Optional[float] = None
    max_lon: Optional[float] = None
    max_lat: Optional[float] = None
    preference: Optional[WatchlistPreferencePayload] = None


class WatchlistPreference(BaseModel):
    """Serialized watchlist preference view."""

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
    """Canonical watchlist response object."""

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
    """Single-watchlist response wrapper."""

    watchlist: Watchlist


class WatchlistListResponse(BaseModel):
    """List-watchlists response wrapper."""

    items: List[Watchlist]


class WatchlistDeleteResponse(BaseModel):
    """Delete-watchlist response payload."""

    deleted: bool
    watchlist_id: int
