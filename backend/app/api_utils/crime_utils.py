import json
from datetime import datetime
from typing import Dict, List

from fastapi import HTTPException
from sqlalchemy import bindparam

from ..schemas.crime_schemas import (
    CrimeAnalyticsFilters,
    CrimeIncidentItem,
)


VALID_MAP_MODES = {"auto", "points", "clusters"}
# Keep cached snapshots short so analytics stay fresh
ANALYTICS_SNAPSHOT_TTL_SECONDS = 60


def _parse_json(value):
    """Return a Python object when the database emits JSON text."""
    if isinstance(value, str):
        return json.loads(value)
    return value


def _parse_month(month, parameter_name="month"):
    """Convert a YYYY-MM string into a date representing the first of that month."""
    if month is None:
        return None

    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{parameter_name} must be in YYYY-MM format") from exc


def _resolve_month_filter(month, startMonth, endMonth):
    """Enforce either a single month or a month range for map filters."""
    if month and (startMonth or endMonth):
        raise HTTPException(
            status_code=400,
            detail="Use either month or startMonth/endMonth, not both",
        )

    if startMonth or endMonth:
        if not (startMonth and endMonth):
            raise HTTPException(
                status_code=400,
                detail="startMonth and endMonth must be provided together",
            )

        start_month_date = _parse_month(startMonth)
        end_month_date = _parse_month(endMonth)
        if start_month_date > end_month_date:
            raise HTTPException(
                status_code=400,
                detail="startMonth must be less than or equal to endMonth",
            )

        return {
            "clause": "ce.month BETWEEN :start_month_date AND :end_month_date",
            "params": {
                "start_month_date": start_month_date,
                "end_month_date": end_month_date,
            },
            "month": None,
            "startMonth": start_month_date.strftime("%Y-%m"),
            "endMonth": end_month_date.strftime("%Y-%m"),
        }

    month_date = _parse_month(month)
    if month_date is None:
        return {
            "clause": None,
            "params": {},
            "month": None,
            "startMonth": None,
            "endMonth": None,
        }

    return {
        "clause": "ce.month = :month_date",
        "params": {"month_date": month_date},
        "month": month_date.strftime("%Y-%m"),
        "startMonth": None,
        "endMonth": None,
    }


def _parse_cursor(cursor):
    """Decode a cursor of the form YYYY-MM|id into query parameters."""
    if cursor is None:
        return None

    try:
        month_key, row_id = cursor.split("|", 1)
        return {
            "cursor_month": datetime.strptime(month_key, "%Y-%m").date(),
            "cursor_id": int(row_id),
        }
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="cursor must be in YYYY-MM|id format") from exc


def _normalize_filter_values(values, parameter_name):
    """Normalize comma-separated filter values into a clean list."""
    if not values:
        return None

    normalized = []
    for item in values:
        for token in item.split(","):
            value = token.strip()
            if value:
                normalized.append(value)

    if not normalized:
        raise HTTPException(status_code=400, detail=f"{parameter_name} must contain at least one value")

    return normalized


def _point_next_cursor(rows, limit):
    """Generate the cursor for paginated incidents responses."""
    if len(rows) <= limit or not rows[:limit]:
        return None

    last_row = rows[limit - 1]
    return f"{last_row['month_label']}|{last_row['id']}"


def _incident_item(row) -> CrimeIncidentItem:
    """Serialize a single incident row for API responses."""
    return {
        "id": row["id"],
        "crime_id": row["crime_id"],
        "month": row["month_label"],
        "crime_type": row["crime_type"],
        "last_outcome_category": row["last_outcome_category"],
        "location_text": row["location_text"],
        "reported_by": row["reported_by"],
        "falls_within": row["falls_within"],
        "lsoa_code": row["lsoa_code"],
        "lsoa_name": row["lsoa_name"],
        "lon": row["lon"],
        "lat": row["lat"],
    }


def _sorted_count_items(counts: Dict[str, int], field_name: str):
    """Sort aggregated counts for analytics list responses."""
    return [
        {field_name: label, "count": count}
        for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    """Raise if the provided bounding box has invalid ordering."""
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _required_bbox(min_lon, min_lat, max_lon, max_lat):
    """Return a bbox dict after validating coordinate order."""
    _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _optional_bbox(min_lon, min_lat, max_lon, max_lat):
    """Return None or a validated bbox when all four coords present."""
    values = [min_lon, min_lat, max_lon, max_lat]
    if not any(value is not None for value in values):
        return None

    if not all(value is not None for value in values):
        raise HTTPException(
            status_code=400,
            detail="minLon, minLat, maxLon, and maxLat must all be provided together",
        )

    return _required_bbox(min_lon, min_lat, max_lon, max_lat)


def _resolve_mode(mode, zoom):
    """Resolve the explicit or automatic map mode based on zoom level."""
    if mode not in VALID_MAP_MODES:
        raise HTTPException(status_code=400, detail="mode must be one of auto, points, or clusters")

    if mode == "auto":
        return "clusters" if zoom <= 11 else "points"
    return mode


def _default_limit(zoom, mode):
    """Pick a default row limit for map tiles depending on zoom."""
    if mode == "clusters":
        return 1500 if zoom <= 8 else 2500

    if zoom >= 16:
        return 5000
    if zoom >= 14:
        return 3000
    return 2000


def _cluster_grid_size(bbox, zoom):
    """Compute the grid cell dimensions used for clustering."""
    if zoom <= 8:
        divisions = 12
    elif zoom <= 10:
        divisions = 16
    else:
        divisions = 20

    cell_width = max((bbox["max_lon"] - bbox["min_lon"]) / divisions, 0.0001)
    cell_height = max((bbox["max_lat"] - bbox["min_lat"]) / divisions, 0.0001)
    return cell_width, cell_height


def _crime_filters_meta(month, start_month, end_month, crime_types, last_outcome_categories, lsoa_names):
    """Return the filter metadata bundled with map responses."""
    return {
        "month": month,
        "startMonth": start_month,
        "endMonth": end_month,
        "crimeType": crime_types,
        "lastOutcomeCategory": last_outcome_categories,
        "lsoaName": lsoa_names,
    }


def _bbox_meta(min_lon, min_lat, max_lon, max_lat):
    """Return serialized bounding box metadata."""
    return {
        "minLon": min_lon,
        "minLat": min_lat,
        "maxLon": max_lon,
        "maxLat": max_lat,
    }


def _analytics_response_filters(
    range_filter,
    bbox,
    crime_types,
    last_outcome_categories,
    lsoa_names,
) -> CrimeAnalyticsFilters:
    """Format the filters metadata returned from analytics endpoints."""
    return {
        "from": range_filter["from"],
        "to": range_filter["to"],
        "crimeType": crime_types,
        "lastOutcomeCategory": last_outcome_categories,
        "lsoaName": lsoa_names,
        "bbox": None
        if not bbox
        else _bbox_meta(
            bbox["min_lon"],
            bbox["min_lat"],
            bbox["max_lon"],
            bbox["max_lat"],
        ),
    }


def _resolve_from_to_filter(from_month, to_month, required=False):
    """Create the SQL clause and metadata for the analytics date range."""
    if from_month or to_month:
        if not (from_month and to_month):
            raise HTTPException(status_code=400, detail="from and to must be provided together")

        from_month_date = _parse_month(from_month, "from")
        to_month_date = _parse_month(to_month, "to")
        if from_month_date > to_month_date:
            raise HTTPException(status_code=400, detail="from must be less than or equal to to")

        return {
            "clause": "ce.month BETWEEN :from_month_date AND :to_month_date",
            "params": {
                "from_month_date": from_month_date,
                "to_month_date": to_month_date,
            },
            "from": from_month_date.strftime("%Y-%m"),
            "to": to_month_date.strftime("%Y-%m"),
        }

    if required:
        raise HTTPException(status_code=400, detail="from and to are required")

    return {
        "clause": None,
        "params": {},
        "from": None,
        "to": None,
    }


def _analytics_request_filters(
    from_month,
    to_month,
    min_lon,
    min_lat,
    max_lon,
    max_lat,
    crime_type,
    last_outcome_category,
    lsoa_name,
    required_range,
):
    """Normalize analytics query params into structured filters."""
    range_filter = _resolve_from_to_filter(from_month, to_month, required=required_range)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_types = _normalize_filter_values(crime_type, "crimeType")
    last_outcome_categories = _normalize_filter_values(last_outcome_category, "lastOutcomeCategory")
    lsoa_names = _normalize_filter_values(lsoa_name, "lsoaName")
    return range_filter, bbox, crime_types, last_outcome_categories, lsoa_names


def _analytics_filters(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    """Assemble WHERE clauses + params for analytics queries."""
    where_clauses = []
    query_params = {}

    if range_filter["clause"] is not None:
        where_clauses.append(range_filter["clause"])
        query_params.update(range_filter["params"])

    if bbox:
        where_clauses.extend(
            [
                "ce.lon IS NOT NULL",
                "ce.lat IS NOT NULL",
                "ce.lon BETWEEN :min_lon AND :max_lon",
                "ce.lat BETWEEN :min_lat AND :max_lat",
            ]
        )
        query_params.update(bbox)

    if crime_types:
        where_clauses.append("COALESCE(NULLIF(ce.crime_type, ''), 'unknown') IN :crime_types")
        query_params["crime_types"] = crime_types

    if last_outcome_categories:
        where_clauses.append(
            "COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') IN :last_outcome_categories"
        )
        query_params["last_outcome_categories"] = last_outcome_categories

    if lsoa_names:
        where_clauses.append("COALESCE(NULLIF(ce.lsoa_name, ''), 'unknown') IN :lsoa_names")
        query_params["lsoa_names"] = lsoa_names

    return where_clauses, query_params


def _bind_analytics_filter_params(query, crime_types, last_outcome_categories, lsoa_names):
    """Attach expanding bindparams for list filters."""
    if crime_types:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories:
        query = query.bindparams(bindparam("last_outcome_categories", expanding=True))
    if lsoa_names:
        query = query.bindparams(bindparam("lsoa_names", expanding=True))
    return query


def _where_sql(where_clauses):
    """Format the WHERE clause string or return empty."""
    if not where_clauses:
        return ""
    return "WHERE " + " AND ".join(where_clauses)


def _shift_month(month_date, offset):
    """Shift a month (day=1) by offset months."""
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _analytics_snapshot_cache_key(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    """Construct the cache key for the analytics snapshot."""
    bbox_key = None
    if bbox:
        bbox_key = (
            bbox["min_lon"],
            bbox["min_lat"],
            bbox["max_lon"],
            bbox["max_lat"],
        )

    return (
        range_filter["from"],
        range_filter["to"],
        bbox_key,
        tuple(crime_types or ()),
        tuple(last_outcome_categories or ()),
        tuple(lsoa_names or ()),
    )
