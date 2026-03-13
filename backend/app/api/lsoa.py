from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from ..db import get_db
from ..errors import DependencyError
from ..schemas.lsoa_schemas import LsoaCategoriesResponse


router = APIRouter(tags=["lsoa"])


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise DependencyError(
            message="Database unavailable. Postgres query execution failed; inspect the database container and server logs."
        ) from exc


@router.get("/lsoa/categories", response_model=LsoaCategoriesResponse)
def get_lsoa_categories(db: Session = Depends(get_db)) -> LsoaCategoriesResponse:
    query = text(
        """
        SELECT
            COALESCE(NULLIF(MIN(ce.lsoa_code), ''), 'unknown') AS lsoa_code,
            COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown') AS lsoa_name,
            COUNT(*)::bigint AS count,
            MIN(ce.lon) AS min_lon,
            MIN(ce.lat) AS min_lat,
            MAX(ce.lon) AS max_lon,
            MAX(ce.lat) AS max_lat
        FROM crime_events ce
        GROUP BY COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown')
        ORDER BY count DESC, lsoa_name ASC
        """
    )
    rows = _execute(db, query, {}).mappings().all()

    return {
        "items": [
            {
                "lsoa_code": row["lsoa_code"],
                "lsoa_name": row["lsoa_name"],
                "count": row["count"],
                "minLon": row["min_lon"],
                "minLat": row["min_lat"],
                "maxLon": row["max_lon"],
                "maxLat": row["max_lat"],
            }
            for row in rows
        ]
    }
