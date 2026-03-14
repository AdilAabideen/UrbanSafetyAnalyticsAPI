from typing import Optional

from fastapi import APIRouter, Depends, Query, status
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


def status_query(status_value: Optional[str] = Query(default=None, alias="status")) -> Optional[str]:
    """Parse and validate moderation status query filter."""
    return validate_status_filter(status_value)


def event_kind_query(event_kind: Optional[str] = Query(default=None)) -> Optional[str]:
    """Parse and validate event-kind query filter."""
    return validate_event_kind_filter(event_kind)


def reporter_type_query(reporter_type: Optional[str] = Query(default=None)) -> Optional[str]:
    """Parse and validate reporter-type query filter."""
    return validate_reporter_type_filter(reporter_type)


@router.post("/reported-events", status_code=status.HTTP_201_CREATED, response_model=SingleReportedEventResponse)
def create_reported_event(
    payload: ReportedEventCreateRequest,
    current_user=Depends(get_optional_current_user),
    db: Session = Depends(get_db),
):
    return {"report": create_report(db=db, payload=payload, current_user=current_user)}


@router.get("/reported-events/mine", response_model=MyReportedEventsResponse)
def read_my_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
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


@router.get("/admin/reported-events", response_model=AdminReportedEventsResponse)
def read_admin_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    reporter_type: Optional[str] = Depends(reporter_type_query),
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
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
)
def moderate_reported_event(
    report_id: int,
    payload: ReportedEventModerationRequest,
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


@router.get("/user-events", response_model=UserEventsResponse)
def read_user_event_features(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    reporter_type: Optional[str] = Depends(reporter_type_query),
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    admin_approved: Optional[bool] = Query(None, alias="adminApproved"),
    min_lon: Optional[float] = Query(None, alias="minLon"),
    min_lat: Optional[float] = Query(None, alias="minLat"),
    max_lon: Optional[float] = Query(None, alias="maxLon"),
    max_lat: Optional[float] = Query(None, alias="maxLat"),
    limit: int = Query(500, ge=1, le=5000),
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
