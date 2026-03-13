import json
import logging
from collections import Counter
from copy import deepcopy
from datetime import datetime
from threading import Event, Lock
from time import monotonic

from fastapi import HTTPException
from sqlalchemy import bindparam, text
from sqlalchemy.exc import InternalError, OperationalError


VALID_MAP_MODES = {"auto", "points", "clusters"}
ANALYTICS_SNAPSHOT_TTL_SECONDS = 60

logger = logging.getLogger(__name__)
_analytics_snapshot_cache = {}
_analytics_snapshot_inflight = {}
_analytics_snapshot_lock = Lock()


def _parse_json(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def _parse_month(month, parameter_name="month"):
    if month is None:
        return None

    try:
        return datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{parameter_name} must be in YYYY-MM format") from exc


def _resolve_month_filter(month, startMonth, endMonth):
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


def _validate_bbox(min_lon, min_lat, max_lon, max_lat):
    if min_lon >= max_lon:
        raise HTTPException(status_code=400, detail="minLon must be less than maxLon")
    if min_lat >= max_lat:
        raise HTTPException(status_code=400, detail="minLat must be less than maxLat")


def _required_bbox(min_lon, min_lat, max_lon, max_lat):
    _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _optional_bbox(min_lon, min_lat, max_lon, max_lat):
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
    if mode not in VALID_MAP_MODES:
        raise HTTPException(status_code=400, detail="mode must be one of auto, points, or clusters")

    if mode == "auto":
        return "clusters" if zoom <= 11 else "points"
    return mode


def _default_limit(zoom, mode):
    if mode == "clusters":
        return 1500 if zoom <= 8 else 2500

    if zoom >= 16:
        return 5000
    if zoom >= 14:
        return 3000
    return 2000


def _cluster_grid_size(bbox, zoom):
    if zoom <= 8:
        divisions = 12
    elif zoom <= 10:
        divisions = 16
    else:
        divisions = 20

    cell_width = max((bbox["max_lon"] - bbox["min_lon"]) / divisions, 0.0001)
    cell_height = max((bbox["max_lat"] - bbox["min_lat"]) / divisions, 0.0001)
    return cell_width, cell_height


def _execute(db, query, params=None):
    try:
        return db.execute(query, params or {})
    except InternalError as exc:
        logger.exception("Database internal error during query execution")
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc
    except OperationalError as exc:
        logger.warning("Database operational error during query execution", exc_info=exc)
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc


def _map_filters(month_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
    where_clauses = [
        "ce.geom IS NOT NULL",
        "ce.lon IS NOT NULL",
        "ce.lat IS NOT NULL",
        "ce.lon BETWEEN :min_lon AND :max_lon",
        "ce.lat BETWEEN :min_lat AND :max_lat",
        "ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)",
    ]
    query_params = dict(bbox)

    if month_filter["clause"] is not None:
        where_clauses.append(month_filter["clause"])
        query_params.update(month_filter["params"])

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


def _crime_map_feature(row):
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
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
        },
    }


def _cluster_feature(row):
    return {
        "type": "Feature",
        "geometry": _parse_json(row["geometry"]),
        "properties": {
            "cluster": True,
            "cluster_id": row["cluster_id"],
            "count": row["count"],
            "top_crime_types": _parse_json(row["top_crime_types"]) or {},
        },
    }


def _crime_filters_meta(month, start_month, end_month, crime_types, last_outcome_categories, lsoa_names):
    return {
        "month": month,
        "startMonth": start_month,
        "endMonth": end_month,
        "crimeType": crime_types,
        "lastOutcomeCategory": last_outcome_categories,
        "lsoaName": lsoa_names,
    }


def _bbox_meta(min_lon, min_lat, max_lon, max_lat):
    return {
        "minLon": min_lon,
        "minLat": min_lat,
        "maxLon": max_lon,
        "maxLat": max_lat,
    }


def _resolve_from_to_filter(from_month, to_month, required=False):
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


def _analytics_filters(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
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
    if crime_types:
        query = query.bindparams(bindparam("crime_types", expanding=True))
    if last_outcome_categories:
        query = query.bindparams(bindparam("last_outcome_categories", expanding=True))
    if lsoa_names:
        query = query.bindparams(bindparam("lsoa_names", expanding=True))
    return query


def _where_sql(where_clauses):
    if not where_clauses:
        return ""
    return "WHERE " + " AND ".join(where_clauses)


def _shift_month(month_date, offset):
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _analytics_snapshot_cache_key(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names):
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


def _analytics_snapshot(range_filter, bbox, crime_types, last_outcome_categories, lsoa_names, db, page_size=5000):
    cache_key = _analytics_snapshot_cache_key(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )
    now = monotonic()

    inflight_event = None
    owns_refresh = False
    while True:
        with _analytics_snapshot_lock:
            cached = _analytics_snapshot_cache.get(cache_key)
            if cached and now - cached["created_at"] <= ANALYTICS_SNAPSHOT_TTL_SECONDS:
                return deepcopy(cached["snapshot"])

            inflight_event = _analytics_snapshot_inflight.get(cache_key)
            if inflight_event is None:
                inflight_event = Event()
                _analytics_snapshot_inflight[cache_key] = inflight_event
                owns_refresh = True
                break

        inflight_event.wait()
        now = monotonic()

    where_clauses, query_params = _analytics_filters(
        range_filter,
        bbox,
        crime_types,
        last_outcome_categories,
        lsoa_names,
    )

    monthly_counts = {}
    if range_filter["params"].get("from_month_date") and range_filter["params"].get("to_month_date"):
        month_date = range_filter["params"]["from_month_date"]
        end_month_date = range_filter["params"]["to_month_date"]
        while month_date <= end_month_date:
            monthly_counts[month_date.strftime("%Y-%m")] = 0
            month_date = _shift_month(month_date, 1)

    crime_type_counts = Counter()
    outcome_counts = Counter()
    unique_lsoas = set()
    unique_crime_types = set()
    total_crimes = 0
    crimes_with_outcomes = 0
    cursor_data = None

    try:
        while True:
            cursor_clause = ""
            params = dict(query_params)
            params["row_limit"] = page_size + 1

            if cursor_data:
                cursor_clause = """
                AND (
                    ce.month < :cursor_month
                    OR (ce.month = :cursor_month AND ce.id < :cursor_id)
                )
                """
                params.update(cursor_data)

            query = text(
                f"""
                SELECT
                    ce.id,
                    ce.month AS month_date,
                    COALESCE(NULLIF(ce.crime_type, ''), 'unknown') AS crime_type,
                    NULLIF(ce.last_outcome_category, '') AS raw_outcome,
                    COALESCE(NULLIF(ce.last_outcome_category, ''), 'unknown') AS outcome,
                    NULLIF(ce.lsoa_code, '') AS lsoa_code,
                    NULLIF(ce.lsoa_name, '') AS lsoa_name
                FROM crime_events ce
                {_where_sql(where_clauses)}
                {cursor_clause}
                ORDER BY ce.month DESC, ce.id DESC
                LIMIT :row_limit
                """
            )
            query = _bind_analytics_filter_params(
                query,
                crime_types,
                last_outcome_categories,
                lsoa_names,
            )

            rows = _execute(db, query, params).mappings().all()
            if not rows:
                break

            page_rows = rows[:page_size]
            for row in page_rows:
                month_label = row["month_date"].strftime("%Y-%m")
                monthly_counts[month_label] = monthly_counts.get(month_label, 0) + 1

                total_crimes += 1
                crime_type_counts[row["crime_type"]] += 1
                outcome_counts[row["outcome"]] += 1
                unique_crime_types.add(row["crime_type"])

                lsoa_key = row["lsoa_code"] or row["lsoa_name"]
                if lsoa_key is not None:
                    unique_lsoas.add(lsoa_key)

                if row["raw_outcome"] is not None:
                    crimes_with_outcomes += 1

            if len(rows) <= page_size:
                break

            last_row = page_rows[-1]
            cursor_data = {
                "cursor_month": last_row["month_date"],
                "cursor_id": last_row["id"],
            }

        snapshot = {
            "total_crimes": total_crimes,
            "unique_lsoas": len(unique_lsoas),
            "unique_crime_types": len(unique_crime_types),
            "crimes_with_outcomes": crimes_with_outcomes,
            "crime_type_counts": dict(crime_type_counts),
            "outcome_counts": dict(outcome_counts),
            "monthly_counts": monthly_counts,
        }

        with _analytics_snapshot_lock:
            _analytics_snapshot_cache[cache_key] = {
                "created_at": monotonic(),
                "snapshot": deepcopy(snapshot),
            }

        return snapshot
    finally:
        if owns_refresh:
            with _analytics_snapshot_lock:
                cached_event = _analytics_snapshot_inflight.get(cache_key)
                if cached_event is inflight_event:
                    del _analytics_snapshot_inflight[cache_key]
            inflight_event.set()
