from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Response
from sqlalchemy.orm import Session

from ..api_utils.tiles_db_utils import _build_tile_bytes
from ..api_utils.tiles_utils import MVT_MEDIA_TYPE, PBF_MEDIA_TYPE, TILE_CACHE_CONTROL
from ..db import get_db
from ..schemas.enums import CrimeType


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
    return Response(
        content=_build_tile_bytes(z, x, y, month, startMonth, endMonth, crimeType, includeRisk, db),
        media_type=MVT_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )


@router.get("/tiles/roads/{z}/{x}/{y}.pbf")
def get_road_tiles_pbf(
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
    return Response(
        content=_build_tile_bytes(z, x, y, month, startMonth, endMonth, crimeType, includeRisk, db),
        media_type=PBF_MEDIA_TYPE,
        headers={"Cache-Control": TILE_CACHE_CONTROL},
    )
