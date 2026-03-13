from datetime import date, time
from typing import Literal, Optional

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
