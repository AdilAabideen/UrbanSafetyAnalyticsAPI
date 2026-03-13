from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db
from .report_event_utils import (
    _execute,
    _parse_month,
    _report_select_sql,
    _report_to_dict,
    _validate_coordinates,
    event_kind_query,
    reporter_type_query,
    status_query,
)


router = APIRouter(tags=["user-events"])


def _validate_optional_bbox(
    min_lon: Optional[float],
    min_lat: Optional[float],
    max_lon: Optional[float],
    max_lat: Optional[float],
):
    values = (min_lon, min_lat, max_lon, max_lat)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise HTTPException(status_code=400, detail="minLon, minLat, maxLon, and maxLat must all be provided together")

    _validate_coordinates(min_lon, min_lat)
    _validate_coordinates(max_lon, max_lat)
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")

    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _report_to_feature(row):
    properties = _report_to_dict(row, include_admin_fields=False)
    longitude = properties.pop("longitude")
    latitude = properties.pop("latitude")
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
        "properties": properties,
    }


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
    from_month_date = _parse_month(from_month, "from")
    to_month_date = _parse_month(to_month, "to")
    if from_month_date and to_month_date and from_month_date > to_month_date:
        raise HTTPException(status_code=400, detail="from must be less than or equal to to")
    if (from_month_date is None) != (to_month_date is None):
        raise HTTPException(status_code=400, detail="from and to must be provided together")

    bbox = _validate_optional_bbox(min_lon, min_lat, max_lon, max_lat)
    where_clauses = ["TRUE"]
    params = {"row_limit": limit}

    if status_value is not None:
        where_clauses.append("e.moderation_status = :status")
        params["status"] = status_value
    if event_kind is not None:
        where_clauses.append("e.event_kind = :event_kind")
        params["event_kind"] = event_kind
    if reporter_type is not None:
        where_clauses.append("e.reporter_type = :reporter_type")
        params["reporter_type"] = reporter_type
    if admin_approved is not None:
        where_clauses.append("e.admin_approved = :admin_approved")
        params["admin_approved"] = admin_approved
    if from_month_date is not None:
        where_clauses.append("e.month BETWEEN :from_month_date AND :to_month_date")
        params["from_month_date"] = from_month_date
        params["to_month_date"] = to_month_date
    if bbox is not None:
        where_clauses.extend(
            [
                "e.longitude BETWEEN :min_lon AND :max_lon",
                "e.latitude BETWEEN :min_lat AND :max_lat",
            ]
        )
        params.update(bbox)

    query = text(
        _report_select_sql()
        + f"""
        /* user_events_geojson */
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT :row_limit
        """
    )
    rows = _execute(db, query, params).mappings().all()
    return {
        "type": "FeatureCollection",
        "features": [_report_to_feature(row) for row in rows],
        "meta": {
            "returned": len(rows),
            "limit": limit,
            "filters": {
                "status": status_value,
                "event_kind": event_kind,
                "reporter_type": reporter_type,
                "adminApproved": admin_approved,
                "from": from_month,
                "to": to_month,
                "bbox": None
                if bbox is None
                else {
                    "minLon": bbox["min_lon"],
                    "minLat": bbox["min_lat"],
                    "maxLon": bbox["max_lon"],
                    "maxLat": bbox["max_lat"],
                },
            },
        },
    }
