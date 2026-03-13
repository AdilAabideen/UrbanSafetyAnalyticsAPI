from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from .crime_utils import _execute
from ..db import get_db


router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/meta")
def get_analytics_meta(db: Session = Depends(get_db)):
    summary_query = text(
        """
        SELECT
            to_char(MIN(ce.month), 'YYYY-MM') AS min_month,
            to_char(MAX(ce.month), 'YYYY-MM') AS max_month,
            COUNT(*)::bigint AS crime_events_total,
            COUNT(*) FILTER (WHERE ce.geom IS NOT NULL)::bigint AS crime_events_with_geom,
            COUNT(*) FILTER (WHERE ce.segment_id IS NOT NULL)::bigint AS crime_events_snapped,
            (SELECT COUNT(*)::bigint FROM road_segments) AS road_segments_total
        FROM crime_events ce
        """
    )
    crime_types_query = text(
        """
        SELECT DISTINCT ce.crime_type
        FROM crime_events ce
        WHERE ce.crime_type IS NOT NULL
          AND ce.crime_type <> ''
        ORDER BY ce.crime_type ASC
        """
    )

    summary = _execute(db, summary_query).mappings().first() or {}
    crime_type_rows = _execute(db, crime_types_query).mappings().all()

    return {
        "months": {
            "min": summary.get("min_month"),
            "max": summary.get("max_month"),
        },
        "crime_types": [row["crime_type"] for row in crime_type_rows],
        "counts": {
            "crime_events_total": summary.get("crime_events_total", 0),
            "crime_events_with_geom": summary.get("crime_events_with_geom", 0),
            "crime_events_snapped": summary.get("crime_events_snapped", 0),
            "road_segments_total": summary.get("road_segments_total", 0),
        },
    }
