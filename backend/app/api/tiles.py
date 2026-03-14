# Tiles.py
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Request, Response
from sqlalchemy.orm import Session

from ..db import get_db
from ..errors import ValidationError
from ..services.tile_service import get_road_tiles_mvt as get_road_tiles_mvt_service

router = APIRouter(tags=["tiles"])


@router.get(
    "/tiles/roads/{z}/{x}/{y}.mvt",
    summary="This is an Endpoint to get the road tiles in MVT format with Risk Overlays",
    description=(
        "Returns a Mapbox Vector Tile (MVT) for roads in the requested slippy-map tile. "
        "Risk scoring is optional and controlled by the `crime`, `collisions`, and "
        "`userReportedEvents` toggles. When any toggle is enabled, both `startMonth` "
        "and `endMonth` are required in `YYYY-MM` format."
    ),
    response_class=Response,
    response_description="Binary Mapbox Vector Tile payload (`application/vnd.mapbox-vector-tile`).",
    responses={
        200: {
            "description": "Binary Mapbox Vector Tile payload (`application/vnd.mapbox-vector-tile`).",
            "content": {
                "application/vnd.mapbox-vector-tile": {
                    "schema": {"type": "string", "format": "binary"},
                }
            },
        },
        400: {
            "description": (
                "Invalid request. Examples: removed `month` or `crimeType`, invalid tile "
                "coordinates, invalid month format, or missing month window when risk "
                "toggles are enabled."
            ),
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "error": {"type": "string"},
                            "message": {"type": "string"},
                            "details": {"type": "object"},
                        },
                    },
                    "examples": {
                        "month_removed": {
                            "summary": "Removed month parameter",
                            "value": {
                                "error": "MONTH_PARAMETER_REMOVED",
                                "message": "month has been removed; use startMonth and endMonth instead",
                                "details": {"field": "month"},
                            },
                        },
                        "missing_window": {
                            "summary": "Missing risk month window",
                            "value": {
                                "error": "MISSING_MONTH_FILTER",
                                "message": "startMonth and endMonth are required when risk toggles are enabled",
                                "details": {"field": "startMonth/endMonth"},
                            },
                        },
                    },
                }
            },
        },
        503: {
            "description": "Database dependency unavailable while generating tile.",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "error": {"type": "string"},
                            "message": {"type": "string"},
                            "details": {"type": "object"},
                        },
                    }
                }
            },
        },
    },
)
def get_road_tiles_mvt(
    request: Request,
    z: int = Path(..., ge=0, le=22, description="Slippy-map zoom level (0-22)."),
    x: int = Path(..., ge=0, description="Slippy-map tile X coordinate."),
    y: int = Path(..., ge=0, description="Slippy-map tile Y coordinate."),
    startMonth: Optional[str] = Query(
        None,
        description=(
            "Inclusive start month for risk scoring in `YYYY-MM` format. "
            "Required when any risk toggle is true."
        ),
    ),
    endMonth: Optional[str] = Query(
        None,
        description=(
            "Inclusive end month for risk scoring in `YYYY-MM` format. "
            "Required when any risk toggle is true."
        ),
    ),
    crime: Optional[bool] = Query(None, description="Include crime component in risk scoring."),
    collisions: Optional[bool] = Query(None, description="Include collisions component in risk scoring."),
    userReportedEvents: Optional[bool] = Query(
        None,
        description="Include user-reported events component in risk scoring.",
    ),
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
        media_type="application/vnd.mapbox-vector-tile",
        headers={"Cache-Control": "public, max-age=60"},
    )
