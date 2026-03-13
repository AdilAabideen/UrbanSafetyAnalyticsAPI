from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .enums import CrimeOutcome, CrimeType


class CrimeBBox(BaseModel):
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float


class CrimeMapFilters(BaseModel):
    month: Optional[str] = None
    startMonth: Optional[str] = None
    endMonth: Optional[str] = None
    crimeType: Optional[List[CrimeType]] = None
    lastOutcomeCategory: Optional[List[CrimeOutcome]] = None
    lsoaName: Optional[List[str]] = None


class CrimeAnalyticsFilters(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    crimeType: Optional[List[CrimeType]] = None
    lastOutcomeCategory: Optional[List[CrimeOutcome]] = None
    lsoaName: Optional[List[str]] = None
    bbox: Optional[CrimeBBox] = None


class CrimeMapMeta(BaseModel):
    mode: str
    zoom: int
    returned: int
    limit: int
    truncated: bool
    nextCursor: Optional[str] = None
    filters: CrimeMapFilters
    bbox: CrimeBBox


class CrimeMapResponse(BaseModel):
    type: str
    features: List[Dict[str, Any]]
    meta: CrimeMapMeta


class CrimeIncidentItem(BaseModel):
    id: int
    crime_id: Optional[str] = None
    month: str
    crime_type: str
    last_outcome_category: str
    location_text: Optional[str] = None
    reported_by: Optional[str] = None
    falls_within: Optional[str] = None
    lsoa_code: Optional[str] = None
    lsoa_name: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None


class CrimeIncidentsMeta(BaseModel):
    returned: int
    limit: int
    truncated: bool
    nextCursor: Optional[str] = None
    filters: CrimeAnalyticsFilters


class CrimeIncidentsResponse(BaseModel):
    items: List[CrimeIncidentItem]
    meta: CrimeIncidentsMeta


class CrimeTypeCountItem(BaseModel):
    crime_type: str
    count: int


class CrimeOutcomeCountItem(BaseModel):
    outcome: str
    count: int


class CrimeTopCrimeType(BaseModel):
    crime_type: str
    count: int


class CrimeAnalyticsSummaryResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    total_crimes: int
    unique_lsoas: int
    unique_crime_types: int
    top_crime_type: Optional[CrimeTopCrimeType] = None
    crimes_with_outcomes: int
    top_crime_types: List[CrimeTypeCountItem]
    top_outcomes: List[CrimeOutcomeCountItem]


class CrimeTimeseriesItem(BaseModel):
    month: str
    count: int


class CrimeTimeseriesResponse(BaseModel):
    series: List[CrimeTimeseriesItem]
    total: int


class CrimeDetailProperties(BaseModel):
    id: int
    crime_id: Optional[str] = None
    month: Any = None
    reported_by: Optional[str] = None
    falls_within: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None
    location_text: Optional[str] = None
    lsoa_code: Optional[str] = None
    lsoa_name: Optional[str] = None
    crime_type: Optional[str] = None
    last_outcome_category: Optional[str] = None
    context: Optional[str] = None
    created_at: Any = None


class CrimeDetailFeature(BaseModel):
    type: str
    geometry: Dict[str, Any]
    properties: CrimeDetailProperties
