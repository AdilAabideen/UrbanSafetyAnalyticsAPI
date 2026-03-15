from datetime import date, time
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class ReportedCrimePayload(BaseModel):
    """Crime-specific details for a reported event."""

    crime_type: str = Field(
        ...,
        min_length=1,
        description="Crime category label (for example: Burglary, Robbery, Vehicle crime).",
        examples=["Burglary"],
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "crime_type": "Burglary",
                }
            ]
        }
    }


class ReportedCollisionPayload(BaseModel):
    """Collision-specific details for a reported event."""

    weather_condition: str = Field(
        ...,
        min_length=1,
        description="Weather condition reported at incident time.",
        examples=["Raining no high winds"],
    )
    light_condition: str = Field(
        ...,
        min_length=1,
        description="Lighting condition reported at incident time.",
        examples=["Darkness - lights lit"],
    )
    number_of_vehicles: int = Field(
        ...,
        ge=1,
        description="Number of vehicles involved in the collision.",
        examples=[2],
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "weather_condition": "Raining no high winds",
                    "light_condition": "Darkness - lights lit",
                    "number_of_vehicles": 2,
                }
            ]
        }
    }


class ReportedEventCreateRequest(BaseModel):
    """Request payload for creating a user-reported event."""

    event_kind: Literal["crime", "collision"] = Field(
        ...,
        description="Type of reported event.",
        examples=["crime"],
    )
    event_date: date = Field(
        ...,
        description="Calendar date when the event happened (`YYYY-MM-DD`).",
        examples=["2026-03-10"],
    )
    event_time: Optional[time] = Field(
        default=None,
        description="Optional local time when the event happened (`HH:MM`).",
        examples=["21:30"],
    )
    longitude: float = Field(
        ...,
        ge=-180,
        le=180,
        description="Event longitude in WGS84.",
        examples=[-1.518],
    )
    latitude: float = Field(
        ...,
        ge=-90,
        le=90,
        description="Event latitude in WGS84.",
        examples=[53.699],
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional free-text description from the reporter.",
        examples=["Suspicious activity near junction after 9pm."],
    )
    crime: Optional[ReportedCrimePayload] = Field(
        default=None,
        description="Required when `event_kind=crime`; must be omitted for collision events.",
    )
    collision: Optional[ReportedCollisionPayload] = Field(
        default=None,
        description="Required when `event_kind=collision`; must be omitted for crime events.",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "event_kind": "crime",
                    "event_date": "2026-03-10",
                    "event_time": "21:30",
                    "longitude": -1.518,
                    "latitude": 53.699,
                    "description": "Suspicious activity near junction after 9pm.",
                    "crime": {"crime_type": "Burglary"},
                },
                {
                    "event_kind": "collision",
                    "event_date": "2026-03-11",
                    "event_time": "08:15",
                    "longitude": -1.522,
                    "latitude": 53.701,
                    "description": "Minor collision during morning traffic.",
                    "collision": {
                        "weather_condition": "Fine no high winds",
                        "light_condition": "Daylight",
                        "number_of_vehicles": 2,
                    },
                },
            ]
        }
    }


class ReportedEventModerationRequest(BaseModel):
    """Request payload for admin moderation actions."""

    moderation_status: Literal["approved", "rejected"] = Field(
        ...,
        description="Admin moderation decision for the report.",
        examples=["approved"],
    )
    moderation_notes: Optional[str] = Field(
        default=None,
        description="Optional admin notes explaining the moderation decision.",
        examples=["Corroborated with local incident cluster."],
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "moderation_status": "approved",
                    "moderation_notes": "Corroborated with local incident cluster.",
                },
                {
                    "moderation_status": "rejected",
                    "moderation_notes": "Insufficient detail and duplicate report.",
                },
            ]
        }
    }


class ReportedEventDetails(BaseModel):
    """Event-type-specific detail fields included in responses."""

    crime_type: Optional[str] = Field(default=None, description="Crime category for crime events.")
    weather_condition: Optional[str] = Field(default=None, description="Weather condition for collision events.")
    light_condition: Optional[str] = Field(default=None, description="Light condition for collision events.")
    number_of_vehicles: Optional[int] = Field(default=None, description="Vehicle count for collision events.")


class ReportedEvent(BaseModel):
    """Canonical reported-event response object."""

    id: int = Field(..., description="Unique report ID.", examples=[1024])
    event_kind: str = Field(..., description="Report type (`crime` or `collision`).", examples=["crime"])
    reporter_type: str = Field(..., description="Reporter identity type.", examples=["authenticated"])
    month: Optional[str] = Field(default=None, description="Event month bucket (`YYYY-MM`).", examples=["2026-03"])
    event_date: Optional[str] = Field(default=None, description="Event date (`YYYY-MM-DD`).", examples=["2026-03-10"])
    event_time: Optional[str] = Field(default=None, description="Event time (`HH:MM`).", examples=["21:30"])
    longitude: float = Field(..., description="Event longitude in WGS84.", examples=[-1.518])
    latitude: float = Field(..., description="Event latitude in WGS84.", examples=[53.699])
    segment_id: Optional[int] = Field(default=None, description="Nearest snapped road segment ID, if within snap threshold.")
    snap_distance_m: Optional[float] = Field(default=None, description="Distance from event point to nearest road segment in meters.")
    description: Optional[str] = Field(default=None, description="Optional user-provided event description.")
    admin_approved: Optional[bool] = Field(default=None, description="Whether the report has been approved by an admin.")
    moderation_status: Optional[str] = Field(default=None, description="Moderation state (`pending`, `approved`, `rejected`).")
    moderation_notes: Optional[str] = Field(default=None, description="Optional moderation notes added by admin.")
    created_at: Optional[str] = Field(default=None, description="ISO-8601 creation timestamp.")
    details: ReportedEventDetails = Field(..., description="Event-type-specific detail fields.")
    user_id: Optional[int] = Field(default=None, description="Reporter user ID (admin views).")
    reporter_email: Optional[str] = Field(default=None, description="Reporter email (admin views).")
    moderated_by: Optional[int] = Field(default=None, description="Admin user ID who moderated this report.")
    moderated_at: Optional[str] = Field(default=None, description="ISO-8601 timestamp when moderation occurred.")


class SingleReportedEventResponse(BaseModel):
    """Single-event wrapper used by create and moderation endpoints."""

    report: ReportedEvent = Field(..., description="Single reported event payload.")


class ReportedEventListMeta(BaseModel):
    """Pagination metadata for list endpoints."""

    returned: int = Field(..., description="Number of items returned in this page.")
    limit: int = Field(..., description="Requested page size limit.")
    nextCursor: Optional[str] = Field(default=None, description="Cursor token for the next page, if available.")
    filters: Dict[str, Any] = Field(..., description="Echo of applied filters for this request.")


class MyReportedEventsResponse(BaseModel):
    """Response model for `/reported-events/mine`."""

    items: List[ReportedEvent] = Field(..., description="Reports created by the authenticated user.")
    meta: ReportedEventListMeta = Field(..., description="Pagination metadata for this result page.")


class AdminReportedEventsResponse(BaseModel):
    """Response model for `/admin/reported-events`."""

    items: List[ReportedEvent] = Field(..., description="Reports visible to moderators/admins.")
    meta: ReportedEventListMeta = Field(..., description="Pagination and filter metadata.")


class UserEventFeature(BaseModel):
    """Single GeoJSON feature representing one user-reported event."""

    type: str = Field(..., description="GeoJSON object type. Always `Feature`.", examples=["Feature"])
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry object containing event coordinates.")
    properties: Dict[str, Any] = Field(..., description="Event attributes attached to the GeoJSON feature.")


class UserEventsMeta(BaseModel):
    """Metadata for GeoJSON user-events endpoint."""

    returned: int = Field(..., description="Number of features returned.")
    limit: int = Field(..., description="Requested feature limit.")
    filters: Dict[str, Any] = Field(..., description="Echo of applied filters for this response.")


class UserEventsResponse(BaseModel):
    """GeoJSON FeatureCollection response for user-reported events."""

    type: str = Field(..., description="GeoJSON collection type. Always `FeatureCollection`.", examples=["FeatureCollection"])
    features: List[UserEventFeature] = Field(..., description="Collection of GeoJSON event features.")
    meta: UserEventsMeta = Field(..., description="Response metadata and applied filters.")
