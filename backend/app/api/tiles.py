# Tiles.py
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Request, Response
from sqlalchemy.orm import Session

from ..db import get_db
from ..errors import ValidationError
from ..schemas.tiles_schemas import MVT_MEDIA_TYPE, TILE_CACHE_CONTROL
from ..services.tile_service import get_road_tiles_mvt as get_road_tiles_mvt_service

router = APIRouter(tags=["tiles"])


@router.get("/tiles/roads/{z}/{x}/{y}.mvt")
def get_road_tiles_mvt(
    request: Request,
    z: int = Path(..., ge=0, le=22),
    x: int = Path(..., ge=0),
    y: int = Path(..., ge=0),
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    crime: Optional[bool] = Query(None),
    collisions: Optional[bool] = Query(None),
    userReportedEvents: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
):
    if "month" in request.query_params:
        raise ValidationError(
            error="MONTH_PARAMETER_REMOVED",
            message="month has been removed; use startMonth and endMonth instead",
            details={"field": "month"},
        )
    if "crimeType" in request.query_params:
        raise ValidationError(
            error="CRIME_TYPE_PARAMETER_REMOVED",
            message="crimeType has been removed from tiles risk input",
            details={"field": "crimeType"},
        )

    tiles_bytes = get_road_tiles_mvt_service(
        z=z,
        x=x,
        y=y,
        startMonth=startMonth,
        endMonth=endMonth,
        crime=crime,
        collisions=collisions,
        userReportedEvents=userReportedEvents,
        db=db,
    )

    return Response(
        content=tiles_bytes,
        media_type=MVT_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )
