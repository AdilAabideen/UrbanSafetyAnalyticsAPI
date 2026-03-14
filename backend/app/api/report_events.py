from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas.report_event_schemas import (
    AdminReportedEventsResponse,
    MyReportedEventsResponse,
    ReportedEventCreateRequest,
    ReportedEventModerationRequest,
    SingleReportedEventResponse,
    UserEventsResponse,
)
from ..services.auth_service import get_current_user
from ..services.report_events_service import (
    create_report,
    get_optional_current_user,
    list_my_reports,
    list_reports_for_admin,
    list_user_events_geojson,
    moderate_existing_report,
    validate_event_kind_filter,
    validate_reporter_type_filter,
    validate_status_filter,
)


router = APIRouter(tags=["reported-events"])


def status_query(
    status_value: Optional[str] = Query(
        default=None,
        alias="status",
        description="Filter by moderation status: `pending`, `approved`, or `rejected`.",
    ),
) -> Optional[str]:
    """Parse and validate moderation status query filter."""
    return validate_status_filter(status_value)


def event_kind_query(
    event_kind: Optional[str] = Query(
        default=None,
        description="Filter by event kind: `crime` or `collision`.",
    ),
) -> Optional[str]:
    """Parse and validate event-kind query filter."""
    return validate_event_kind_filter(event_kind)


def reporter_type_query(
    reporter_type: Optional[str] = Query(
        default=None,
        description="Filter by reporter identity type: `anonymous` or `authenticated`.",
    ),
) -> Optional[str]:
    """Parse and validate reporter-type query filter."""
    return validate_reporter_type_filter(reporter_type)


@router.post(
    "/reported-events",
    status_code=status.HTTP_201_CREATED,
    response_model=SingleReportedEventResponse,
    summary="Create a reported event",
    description=(
        "Creates a new user-reported event (crime or collision). "
        "If a bearer token is provided, the report is linked to the authenticated user; "
        "otherwise it is stored as anonymous."
    ),
    response_description="Created reported event payload.",
    responses={
        201: {"description": "Reported event created successfully."},
        400: {"description": "Invalid request payload or validation error."},
        401: {"description": "Authentication token provided but invalid/expired."},
        503: {"description": "Database unavailable."},
    },
)
def create_reported_event(
    payload: ReportedEventCreateRequest,
    current_user=Depends(get_optional_current_user),
    db: Session = Depends(get_db),
):
    return {"report": create_report(db=db, payload=payload, current_user=current_user)}


@router.get(
    "/reported-events/mine",
    response_model=MyReportedEventsResponse,
    summary="List my reported events",
    description=(
        "Returns a cursor-paginated list of reports submitted by the currently authenticated user. "
        "Supports optional moderation status and event-kind filters."
    ),
    response_description="Paginated list of reports owned by the authenticated user.",
    responses={
        200: {"description": "My reports returned successfully."},
        400: {"description": "Invalid filter/cursor parameters."},
        401: {"description": "Authentication required."},
        503: {"description": "Database unavailable."},
    },
)
def read_my_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    limit: int = Query(
        20,
        ge=1,
        le=100,
        description="Maximum number of items to return per page.",
    ),
    cursor: Optional[str] = Query(
        None,
        description="Pagination cursor in `created_at|id` format.",
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return list_my_reports(
        db=db,
        user_id=current_user["id"],
        status_value=status_value,
        event_kind=event_kind,
        limit=limit,
        cursor=cursor,
    )


@router.get(
    "/admin/reported-events",
    response_model=AdminReportedEventsResponse,
    summary="List reported events for admin moderation",
    description=(
        "Admin-only endpoint that returns a cursor-paginated report list with moderation, "
        "reporter identity, event kind, and month-range filtering."
    ),
    response_description="Paginated admin view of reported events.",
    responses={
        200: {"description": "Admin report list returned successfully."},
        400: {"description": "Invalid filter/month/cursor parameters."},
        401: {"description": "Authentication required."},
        403: {"description": "Admin access required."},
        503: {"description": "Database unavailable."},
    },
)
def read_admin_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    reporter_type: Optional[str] = Depends(reporter_type_query),
    from_month: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive start month in `YYYY-MM` format.",
    ),
    to_month: Optional[str] = Query(
        None,
        alias="to",
        description="Inclusive end month in `YYYY-MM` format.",
    ),
    limit: int = Query(
        50,
        ge=1,
        le=200,
        description="Maximum number of items to return per page.",
    ),
    cursor: Optional[str] = Query(
        None,
        description="Pagination cursor in `created_at|id` format.",
    ),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return list_reports_for_admin(
        db=db,
        current_user=current_user,
        status_value=status_value,
        event_kind=event_kind,
        reporter_type=reporter_type,
        from_month=from_month,
        to_month=to_month,
        limit=limit,
        cursor=cursor,
    )


@router.patch(
    "/admin/reported-events/{report_id}/moderation",
    response_model=SingleReportedEventResponse,
    summary="Moderate a reported event",
    description=(
        "Admin-only endpoint to approve or reject a reported event and optionally add moderation notes."
    ),
    response_description="Updated reported event after moderation.",
    responses={
        200: {"description": "Report moderation applied successfully."},
        400: {"description": "Invalid moderation payload."},
        401: {"description": "Authentication required."},
        403: {"description": "Admin access required."},
        404: {"description": "Reported event not found."},
        503: {"description": "Database unavailable."},
    },
)
def moderate_reported_event(
    report_id: int = Path(..., description="Unique ID of the reported event to moderate."),
    payload: ReportedEventModerationRequest = ...,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return {
        "report": moderate_existing_report(
            db=db,
            current_user=current_user,
            report_id=report_id,
            payload=payload,
        )
    }


@router.get(
    "/user-events",
    response_model=UserEventsResponse,
    summary="List user-reported events as GeoJSON",
    description=(
        "Returns user-reported events as a GeoJSON FeatureCollection for map display. "
        "Supports moderation, type, month-range, admin-approved, and optional bounding-box filters."
    ),
    response_description="GeoJSON FeatureCollection of user-reported events.",
    responses={
        200: {"description": "GeoJSON user events returned successfully."},
        400: {"description": "Invalid filter/month/bounding-box parameters."},
        503: {"description": "Database unavailable."},
    },
)
def read_user_event_features(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    reporter_type: Optional[str] = Depends(reporter_type_query),
    from_month: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive start month in `YYYY-MM` format.",
    ),
    to_month: Optional[str] = Query(
        None,
        alias="to",
        description="Inclusive end month in `YYYY-MM` format.",
    ),
    admin_approved: Optional[bool] = Query(
        None,
        alias="adminApproved",
        description="Filter by whether event was admin-approved.",
    ),
    min_lon: Optional[float] = Query(None, alias="minLon", description="Bounding box min longitude."),
    min_lat: Optional[float] = Query(None, alias="minLat", description="Bounding box min latitude."),
    max_lon: Optional[float] = Query(None, alias="maxLon", description="Bounding box max longitude."),
    max_lat: Optional[float] = Query(None, alias="maxLat", description="Bounding box max latitude."),
    limit: int = Query(
        500,
        ge=1,
        le=5000,
        description="Maximum number of features to return.",
    ),
    db: Session = Depends(get_db),
):
    return list_user_events_geojson(
        db=db,
        status_value=status_value,
        event_kind=event_kind,
        reporter_type=reporter_type,
        from_month=from_month,
        to_month=to_month,
        admin_approved=admin_approved,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=limit,
    )
