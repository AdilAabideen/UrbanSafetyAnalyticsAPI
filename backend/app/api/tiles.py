# Tiles.py
from typing import Optional
from fastapi import APIRouter, Depends, Path, Query, Response
from sqlalchemy.orm import Session

from ..schemas.tiles_schemas import MVT_MEDIA_TYPE, PBF_MEDIA_TYPE, TILE_CACHE_CONTROL
from ..db import get_db
from ..schemas.enums import CrimeType

from ..services.tile_service import get_road_tiles_mvt
router = APIRouter(tags=["tiles"])


@router.get("/tiles/roads/{z}/{x}/{y}.mvt")
def get_road_tiles_mvt(
    z: int = Path(..., ge=0, le=22),
    x: int = Path(..., ge=0),
    y: int = Path(..., ge=0),
    month: Optional[str] = Query(None),
    startMonth: Optional[str] = Query(None),
    endMonth: Optional[str] = Query(None),
    crimeType: Optional[CrimeType] = Query(None),
    includeRisk: bool = Query(False),
    db: Session = Depends(get_db),
):
    tiles_bytes = get_road_tiles_mvt(
        z=z,
        x=x,
        y=y,
        month=month,
        startMonth=startMonth,
        endMonth=endMonth,
        crimeType=crimeType,
        includeRisk=includeRisk,
        db=db
    )

    return Response(
        content=tiles_bytes,
        media_type=MVT_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )


