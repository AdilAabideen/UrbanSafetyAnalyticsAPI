from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from ..api_utils.report_event_utils import (
    create_report_record,
    event_kind_query,
    get_optional_current_user,
    list_admin_reports,
    list_own_reports,
    list_user_event_features,
    moderate_report,
    reporter_type_query,
    require_admin,
    status_query,
)
from ..api_utils.auth_utils import get_current_user
from ..db import get_db
from ..schemas.report_event_schemas import ReportedEventCreateRequest, ReportedEventModerationRequest


router = APIRouter(tags=["reported-events"])


@router.post("/reported-events", status_code=status.HTTP_201_CREATED)
def create_reported_event(
    payload: ReportedEventCreateRequest,
    current_user=Depends(get_optional_current_user),
    db: Session = Depends(get_db),
):
    return {"report": create_report_record(db, payload, current_user)}


@router.get("/reported-events/mine")
def read_my_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return list_own_reports(db, current_user["id"], status_value, event_kind, limit, cursor)


@router.get("/admin/reported-events")
def read_admin_reported_events(
    status_value: Optional[str] = Depends(status_query),
    event_kind: Optional[str] = Depends(event_kind_query),
    reporter_type: Optional[str] = Depends(reporter_type_query),
    from_month: Optional[str] = Query(None, alias="from"),
    to_month: Optional[str] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    current_user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    return list_admin_reports(
        db,
        status_value,
        event_kind,
        reporter_type,
        from_month,
        to_month,
        limit,
        cursor,
    )


@router.patch("/admin/reported-events/{report_id}/moderation")
def moderate_reported_event(
    report_id: int,
    payload: ReportedEventModerationRequest,
    current_user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    return {"report": moderate_report(db, report_id, current_user["id"], payload)}


@router.get("/user-events")
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
    return list_user_event_features(
        db,
        status_value,
        event_kind,
        reporter_type,
        from_month,
        to_month,
        admin_approved,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
        limit,
    )
