from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class CollisionBBox(BaseModel):
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float


class CollisionMapFilters(BaseModel):
    month: Optional[str] = None
    startMonth: Optional[str] = None
    endMonth: Optional[str] = None
    collisionSeverity: Optional[List[str]] = None
    roadType: Optional[List[str]] = None
    lsoaCode: Optional[List[str]] = None
    weatherCondition: Optional[List[str]] = None
    lightCondition: Optional[List[str]] = None
    roadSurfaceCondition: Optional[List[str]] = None


class CollisionAnalyticsFilters(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    collisionSeverity: Optional[List[str]] = None
    roadType: Optional[List[str]] = None
    lsoaCode: Optional[List[str]] = None
    weatherCondition: Optional[List[str]] = None
    lightCondition: Optional[List[str]] = None
    roadSurfaceCondition: Optional[List[str]] = None
    bbox: Optional[CollisionBBox] = None


class CollisionMapMeta(BaseModel):
    mode: str
    zoom: int
    returned: int
    limit: int
    truncated: bool
    nextCursor: Optional[str] = None
    filters: CollisionMapFilters
    bbox: CollisionBBox


class CollisionMapResponse(BaseModel):
    type: str
    features: List[Dict[str, Any]]
    meta: CollisionMapMeta


class CollisionIncidentItem(BaseModel):
    collision_index: str
    month: str
    date: str
    time: Optional[str] = None
    collision_severity: str
    road_type: str
    speed_limit: str
    light_conditions: str
    weather_conditions: str
    road_surface_conditions: str
    number_of_vehicles: int
    number_of_casualties: int
    lsoa_code: str
    lon: Optional[float] = None
    lat: Optional[float] = None


class CollisionIncidentsMeta(BaseModel):
    returned: int
    limit: int
    truncated: bool
    nextCursor: Optional[str] = None
    filters: CollisionAnalyticsFilters


class CollisionIncidentsResponse(BaseModel):
    items: List[CollisionIncidentItem]
    meta: CollisionIncidentsMeta


class CollisionCountItem(BaseModel):
    count: int


class CollisionSummaryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    total_collisions: int
    total_casualties: int
    unique_lsoas: int
    collisions_with_casualties: int
    fatal_casualties: int
    serious_casualties: int
    slight_casualties: int
    avg_casualties_per_collision: float
    top_collision_severity: Optional[Dict[str, Any]] = None
    top_road_type: Optional[Dict[str, Any]] = None
    top_weather_condition: Optional[Dict[str, Any]] = None
    top_light_condition: Optional[Dict[str, Any]] = None


class CollisionTimeseriesItem(BaseModel):
    month: str
    count: int


class CollisionTimeseriesResponse(BaseModel):
    series: List[CollisionTimeseriesItem]
    total: int
