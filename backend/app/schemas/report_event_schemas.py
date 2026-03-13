from datetime import date, datetime, time
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ReportedCrimePayload(BaseModel):
    """Request payload for crime-specific reported-event details."""

    crime_type: str = Field(..., min_length=1)


class ReportedCollisionPayload(BaseModel):
    """Request payload for collision-specific reported-event details."""

    weather_condition: str = Field(..., min_length=1)
    light_condition: str = Field(..., min_length=1)
    number_of_vehicles: int = Field(..., ge=1)


class ReportedEventCreateRequest(BaseModel):
    """Request payload for creating a user-reported event."""

    event_kind: Literal["crime", "collision"]
    event_date: date
    event_time: Optional[time] = None
    longitude: float
    latitude: float
    description: Optional[str] = None
    crime: Optional[ReportedCrimePayload] = None
    collision: Optional[ReportedCollisionPayload] = None


class ReportedEventModerationRequest(BaseModel):
    """Request payload for admin moderation actions on reported events."""

    moderation_status: Literal["approved", "rejected"]
    moderation_notes: Optional[str] = None


class ReportedEventDetails(BaseModel):
    crime_type: Optional[str] = None
    weather_condition: Optional[str] = None
    light_condition: Optional[str] = None
    number_of_vehicles: Optional[int] = None


class ReportedEvent(BaseModel):
    id: int
    event_kind: str
    reporter_type: str
    month: Optional[str] = None
    event_date: Optional[str] = None
    event_time: Optional[str] = None
    longitude: float
    latitude: float
    segment_id: Optional[int] = None
    snap_distance_m: Optional[float] = None
    description: Optional[str] = None
    admin_approved: Optional[bool] = None
    moderation_status: Optional[str] = None
    moderation_notes: Optional[str] = None
    created_at: Optional[str] = None
    details: ReportedEventDetails
    user_id: Optional[int] = None
    reporter_email: Optional[str] = None
    moderated_by: Optional[int] = None
    moderated_at: Optional[str] = None


class SingleReportedEventResponse(BaseModel):
    report: ReportedEvent


class ReportedEventListMeta(BaseModel):
    returned: int
    limit: int
    nextCursor: Optional[str] = None
    filters: Dict[str, Any]


class MyReportedEventsResponse(BaseModel):
    items: List[ReportedEvent]
    meta: ReportedEventListMeta


class AdminReportedEventsResponse(BaseModel):
    items: List[ReportedEvent]
    meta: ReportedEventListMeta


class UserEventFeature(BaseModel):
    type: str
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class UserEventsMeta(BaseModel):
    returned: int
    limit: int
    filters: Dict[str, Any]


class UserEventsResponse(BaseModel):
    type: str
    features: List[UserEventFeature]
    meta: UserEventsMeta

