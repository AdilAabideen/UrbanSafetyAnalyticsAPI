import json
import logging
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import bindparam, text
from sqlalchemy.exc import InternalError, OperationalError, TimeoutError as SATimeoutError
from sqlalchemy.orm import Session

from ..db import get_db


router = APIRouter(prefix="/analytics", tags=["analytics"])
logger = logging.getLogger(__name__)

MAX_MONTH_SPAN = 24
USER_REPORTED_CRIME_WEIGHT = 0.10
ANONYMOUS_USER_REPORT_WEIGHT = 0.5
REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25
USER_REPORTED_SIGNAL_CAP = 3.0


class AnalyticsAPIError(Exception):
    def __init__(self, status_code: int, error: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message
        self.details = details


class ScoreWeights(BaseModel):
    w_crime: float = 1.0
    w_collision: float = 0.0


class RiskScoreRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    mode: str = "walk"
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class ForecastRequest(BaseModel):
    target: str
    minLon: float
    minLat: float
    maxLon: float
    maxLat: float
    crimeType: Optional[str] = None
    baselineMonths: int = 6
    method: str = "poisson_mean"
    returnRiskProjection: bool = False
    includeCollisions: bool = False
    mode: str = "walk"
    weights: ScoreWeights = Field(default_factory=ScoreWeights)


class RouteRiskRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    mode: str = "walk"
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    segment_ids: Optional[List[int]] = None
    route_line: Optional[Dict[str, Any]] = None
    checkConnectivity: bool = True
    threshold_m: float = 20.0
    buffer_m: float = 25.0
    failOnDisconnect: bool = False


class RouteCompareItem(BaseModel):
    name: str
    segment_ids: Optional[List[int]] = None
    route_line: Optional[Dict[str, Any]] = None
    checkConnectivity: bool = True
    threshold_m: float = 20.0
    buffer_m: float = 25.0
    failOnDisconnect: bool = False


class RouteCompareRequest(BaseModel):
    from_: str = Field(alias="from")
    to: str
    mode: str = "walk"
    crimeType: Optional[str] = None
    includeCollisions: bool = False
    routes: List[RouteCompareItem]


def _error_response(error: AnalyticsAPIError) -> JSONResponse:
    payload = {"error": error.error, "message": error.message}
    if error.details is not None:
        payload["details"] = error.details
    return JSONResponse(status_code=error.status_code, content=payload)


def _generated_at() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _month_label(month_date) -> str:
    return month_date.strftime("%Y-%m")


def _shift_month(month_date, offset: int):
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _month_span(start_date, end_date) -> int:
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1


def _parse_month(value: str, parameter_name: str):
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise AnalyticsAPIError(
            400,
            "INVALID_MONTH_FORMAT",
            f"{parameter_name} must be in YYYY-MM format",
        ) from exc


def _validate_month_window(from_value: str, to_value: str):
    from_date = _parse_month(from_value, "from")
    to_date = _parse_month(to_value, "to")
    if from_date > to_date:
        raise AnalyticsAPIError(
            400,
            "INVALID_DATE_RANGE",
            "from must be less than or equal to to",
        )

    span = _month_span(from_date, to_date)
    if span > MAX_MONTH_SPAN:
        raise AnalyticsAPIError(
            400,
            "RANGE_TOO_LARGE",
            f"Month ranges may not exceed {MAX_MONTH_SPAN} months",
            {"max_months": MAX_MONTH_SPAN},
        )

    return {
        "from_date": from_date,
        "to_date": to_date,
        "from": _month_label(from_date),
        "to": _month_label(to_date),
        "span_months": span,
    }


def _validate_target_and_baseline(target: str, baseline_months: int):
    if baseline_months < 3 or baseline_months > 24:
        raise AnalyticsAPIError(
            400,
            "INVALID_DATE_RANGE",
            "baselineMonths must be between 3 and 24",
        )

    target_date = _parse_month(target, "target")
    baseline_from_date = _shift_month(target_date, -baseline_months)
    baseline_to_date = _shift_month(target_date, -1)
    return {
        "target_date": target_date,
        "target": _month_label(target_date),
        "baseline_months": baseline_months,
        "baseline_from_date": baseline_from_date,
        "baseline_to_date": baseline_to_date,
        "baseline_from": _month_label(baseline_from_date),
        "baseline_to": _month_label(baseline_to_date),
    }


def _validate_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    if min_lon >= max_lon or min_lat >= max_lat:
        raise AnalyticsAPIError(
            400,
            "INVALID_BBOX",
            "minLon must be less than maxLon and minLat must be less than maxLat",
        )

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
        raise AnalyticsAPIError(
            400,
            "INVALID_BBOX",
            "minLon, minLat, maxLon, and maxLat must all be provided together",
        )
    return _validate_bbox(min_lon, min_lat, max_lon, max_lat)


def _round_rate(value: Any) -> float:
    return round(float(value or 0.0), 3)


def _round_pct(value: Any) -> float:
    return round(float(value or 0.0), 4)


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _scope_payload(
    month_window: Optional[Dict[str, Any]] = None,
    bbox: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    scope: Dict[str, Any] = {}
    if month_window:
        if "target" in month_window:
            scope["target"] = month_window["target"]
            scope["baselineMonths"] = month_window["baseline_months"]
        else:
            scope["from"] = month_window["from"]
            scope["to"] = month_window["to"]
    if bbox:
        scope["bbox"] = {
            "minLon": bbox["min_lon"],
            "minLat": bbox["min_lat"],
            "maxLon": bbox["max_lon"],
            "maxLat": bbox["max_lat"],
        }
    if extras:
        scope.update(extras)
    return scope


def _execute(db: Session, query, params: Optional[Dict[str, Any]] = None):
    try:
        return db.execute(query, params or {})
    except (OperationalError, InternalError, SATimeoutError) as exc:
        logger.exception("Advanced analytics query failed")
        if hasattr(db, "rollback"):
            db.rollback()
        raise AnalyticsAPIError(
            503,
            "DB_UNAVAILABLE",
            "Database unavailable while computing analytics",
        ) from exc


def _band_from_pct(pct: float) -> str:
    if pct >= 0.95:
        return "red"
    if pct >= 0.80:
        return "amber"
    return "green"


def _applied_collision_weight(include_collisions: bool, mode: str, requested_weight: float) -> float:
    if not include_collisions or mode != "drive":
        return 0.0
    return requested_weight if requested_weight != 0 else 1.0


def _band_interpretation(pct: float, noun: str) -> str:
    if pct >= 0.95:
        return f"This {noun} sits in roughly the top 5% of observed risk for the selected period."
    if pct >= 0.80:
        return f"This {noun} sits above the wider network average and falls into the upper 20% of observed risk."
    return f"This {noun} sits below the upper-risk bands for the selected period."


def _user_report_weight_params() -> Dict[str, float]:
    return {
        "user_report_weight": USER_REPORTED_CRIME_WEIGHT,
        "anonymous_report_weight": ANONYMOUS_USER_REPORT_WEIGHT,
        "repeat_authenticated_report_weight": REPEAT_AUTHENTICATED_REPORT_WEIGHT,
        "user_report_signal_cap": USER_REPORTED_SIGNAL_CAP,
    }


def _user_report_signal_sql(
    *,
    authenticated_reports_sql: str,
    distinct_authenticated_users_sql: str,
    anonymous_reports_sql: str,
) -> str:
    return f"""
        (
            :user_report_weight * LEAST(
                :user_report_signal_cap,
                ({distinct_authenticated_users_sql})
                + (:anonymous_report_weight * ({anonymous_reports_sql}))
                + (
                    :repeat_authenticated_report_weight
                    * GREATEST(({authenticated_reports_sql}) - ({distinct_authenticated_users_sql}), 0.0)
                )
            )
        )
    """


def _risk_score_area_metrics(db: Session, month_window, bbox, crime_type: Optional[str]):
    crime_type_clause = ""
    user_report_crime_type_clause = ""
    params = dict(bbox)
    params.update(
        {
            "from_date": month_window["from_date"],
            "to_date": month_window["to_date"],
        }
    )
    params.update(_user_report_weight_params())
    if crime_type:
        crime_type_clause = "AND ce.crime_type = :crime_type"
        user_report_crime_type_clause = "AND urc.crime_type = :crime_type"
        params["crime_type"] = crime_type

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    query = text(
        f"""
        /* analytics_risk_score_area */
        WITH user_report_base AS (
            SELECT
                ure.segment_id,
                ure.month,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.geom IS NOT NULL
              AND ure.longitude BETWEEN :min_lon AND :max_lon
              AND ure.latitude BETWEEN :min_lat AND :max_lat
              AND ure.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              AND ure.month BETWEEN :from_date AND :to_date
              {user_report_crime_type_clause}
            GROUP BY ure.segment_id, ure.month, urc.crime_type
        ),
        user_report_agg AS (
            SELECT
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal,
                COALESCE(SUM(authenticated_reports + anonymous_reports), 0)::bigint AS approved_user_reports
            FROM user_report_base
        )
        SELECT
            COUNT(*)::bigint AS total_crimes,
            COALESCE((SELECT approved_user_reports FROM user_report_agg), 0)::bigint AS approved_user_reports,
            COALESCE((SELECT user_reported_crime_signal FROM user_report_agg), 0.0) AS user_reported_crime_signal,
            (
                SELECT COUNT(*)::bigint
                FROM collision_events col
                WHERE col.geom IS NOT NULL
                  AND col.longitude BETWEEN :min_lon AND :max_lon
                  AND col.latitude BETWEEN :min_lat AND :max_lat
                  AND col.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
                  AND col.month BETWEEN :from_date AND :to_date
            ) AS total_collisions,
            (
                SELECT COALESCE(
                    SUM(
                        1.0
                        + (0.5 * COALESCE(col.slight_casualty_count, 0))
                        + (2.0 * COALESCE(col.serious_casualty_count, 0))
                        + (5.0 * COALESCE(col.fatal_casualty_count, 0))
                    ),
                    0.0
                )
                FROM collision_events col
                WHERE col.geom IS NOT NULL
                  AND col.longitude BETWEEN :min_lon AND :max_lon
                  AND col.latitude BETWEEN :min_lat AND :max_lat
                  AND col.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
                  AND col.month BETWEEN :from_date AND :to_date
            ) AS total_collision_points,
            ST_Area(
                ST_Transform(
                    ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
                    3857
                )
            ) / 1000000.0 AS area_km2
        FROM crime_events ce
        WHERE ce.geom IS NOT NULL
          AND ce.lon BETWEEN :min_lon AND :max_lon
          AND ce.lat BETWEEN :min_lat AND :max_lat
          AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
          AND ce.month BETWEEN :from_date AND :to_date
          {crime_type_clause}
        """
    )
    return _execute(db, query, params).mappings().first() or {
        "total_crimes": 0,
        "approved_user_reports": 0,
        "user_reported_crime_signal": 0.0,
        "total_collisions": 0,
        "total_collision_points": 0.0,
        "area_km2": 0.0,
    }


def _risk_score_segment_metrics(
    db: Session,
    month_window,
    bbox,
    crime_type: Optional[str],
    w_crime: float,
    w_collision: float,
):
    crime_type_clause = ""
    user_report_crime_type_clause = ""
    params = dict(bbox)
    params.update(
        {
            "from_date": month_window["from_date"],
            "to_date": month_window["to_date"],
            "w_crime": w_crime,
            "w_collision_applied": w_collision,
        }
    )
    params.update(_user_report_weight_params())
    if crime_type:
        crime_type_clause = "AND smts.crime_type = :crime_type"
        user_report_crime_type_clause = "AND urc.crime_type = :crime_type"
        params["crime_type"] = crime_type

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    query = text(
        f"""
        /* analytics_risk_score_segments */
        WITH bounds AS (
            SELECT ST_Transform(
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326),
                3857
            ) AS geom
        ),
        crime_agg AS (
            SELECT
                smts.segment_id,
                SUM(smts.crime_count)::double precision AS crimes
            FROM segment_month_type_stats smts
            WHERE smts.month BETWEEN :from_date AND :to_date
              {crime_type_clause}
            GROUP BY smts.segment_id
        ),
        user_report_base AS (
            SELECT
                ure.segment_id,
                ure.month,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.month BETWEEN :from_date AND :to_date
              {user_report_crime_type_clause}
            GROUP BY ure.segment_id, ure.month, urc.crime_type
        ),
        user_report_agg AS (
            SELECT
                segment_id,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal,
                COALESCE(SUM(authenticated_reports + anonymous_reports), 0)::bigint AS approved_user_reports
            FROM user_report_base
            GROUP BY segment_id
        ),
        collision_agg AS (
            SELECT
                smcs.segment_id,
                SUM(smcs.collision_count)::double precision AS collisions,
                SUM(smcs.casualty_count)::double precision AS casualties,
                SUM(smcs.fatal_casualty_count)::double precision AS fatal_casualties,
                SUM(smcs.serious_casualty_count)::double precision AS serious_casualties,
                SUM(smcs.slight_casualty_count)::double precision AS slight_casualties
            FROM segment_month_collision_stats smcs
            WHERE smcs.month BETWEEN :from_date AND :to_date
            GROUP BY smcs.segment_id
        ),
        scored AS (
            SELECT
                rs.id AS segment_id,
                ST_Intersects(rs.geom, bounds.geom) AS in_scope,
                GREATEST(rs.length_m, 100.0) / 1000.0 AS normalized_km,
                COALESCE(crime_agg.crimes, 0.0) AS official_crimes,
                COALESCE(user_report_agg.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
                COALESCE(user_report_agg.approved_user_reports, 0) AS approved_user_reports,
                COALESCE(crime_agg.crimes, 0.0) + COALESCE(user_report_agg.user_reported_crime_signal, 0.0) AS crimes,
                COALESCE(collision_agg.collisions, 0.0) AS collisions,
                COALESCE(collision_agg.casualties, 0.0) AS casualties,
                COALESCE(collision_agg.fatal_casualties, 0.0) AS fatal_casualties,
                COALESCE(collision_agg.serious_casualties, 0.0) AS serious_casualties,
                COALESCE(collision_agg.slight_casualties, 0.0) AS slight_casualties,
                (
                    COALESCE(crime_agg.crimes, 0.0) + COALESCE(user_report_agg.user_reported_crime_signal, 0.0)
                ) / (GREATEST(rs.length_m, 100.0) / 1000.0) AS crime_density,
                COALESCE(user_report_agg.user_reported_crime_signal, 0.0) / (GREATEST(rs.length_m, 100.0) / 1000.0)
                    AS user_report_density,
                COALESCE(collision_agg.collisions, 0.0) / (GREATEST(rs.length_m, 100.0) / 1000.0) AS collision_count_density,
                (
                    COALESCE(collision_agg.collisions, 0.0)
                    + (0.5 * COALESCE(collision_agg.slight_casualties, 0.0))
                    + (2.0 * COALESCE(collision_agg.serious_casualties, 0.0))
                    + (5.0 * COALESCE(collision_agg.fatal_casualties, 0.0))
                ) / (GREATEST(rs.length_m, 100.0) / 1000.0) AS collision_density
            FROM road_segments rs
            CROSS JOIN bounds
            LEFT JOIN crime_agg ON crime_agg.segment_id = rs.id
            LEFT JOIN user_report_agg ON user_report_agg.segment_id = rs.id
            LEFT JOIN collision_agg ON collision_agg.segment_id = rs.id
        ),
        ranked AS (
            SELECT
                scored.*,
                ((:w_crime * scored.crime_density) + (:w_collision_applied * scored.collision_density)) AS combined_density,
                percent_rank() OVER (
                    ORDER BY ((:w_crime * scored.crime_density) + (:w_collision_applied * scored.collision_density))
                ) AS pct
            FROM scored
        ),
        scope_stats AS (
            SELECT
                COUNT(*) FILTER (WHERE in_scope) AS segments_considered,
                COALESCE(AVG(combined_density) FILTER (WHERE in_scope), 0.0) AS avg_density,
                COALESCE(AVG(crime_density) FILTER (WHERE in_scope), 0.0) AS avg_crimes_per_km,
                COALESCE(AVG(user_report_density) FILTER (WHERE in_scope), 0.0) AS avg_user_reported_crime_signal_per_km,
                COALESCE(AVG(collision_count_density) FILTER (WHERE in_scope), 0.0) AS avg_collisions_per_km,
                COALESCE(AVG(collision_density) FILTER (WHERE in_scope), 0.0) AS avg_collision_points_per_km,
                COALESCE(AVG(CASE WHEN pct >= 0.95 THEN 1 ELSE 0 END) FILTER (WHERE in_scope), 0.0) AS red_segment_share
            FROM ranked
        ),
        pct_stats AS (
            SELECT
                CASE
                    WHEN scope_stats.segments_considered = 0 THEN 0.0
                    ELSE COALESCE(
                        SUM(CASE WHEN ranked.combined_density <= scope_stats.avg_density THEN 1 ELSE 0 END)::double precision
                        / NULLIF(COUNT(*), 0),
                        0.0
                    )
                END AS avg_density_pct
            FROM ranked
            CROSS JOIN scope_stats
            GROUP BY scope_stats.segments_considered, scope_stats.avg_density
        )
        SELECT
            scope_stats.segments_considered,
            scope_stats.avg_density,
            scope_stats.avg_crimes_per_km,
            scope_stats.avg_user_reported_crime_signal_per_km,
            scope_stats.avg_collisions_per_km,
            scope_stats.avg_collision_points_per_km,
            scope_stats.red_segment_share,
            pct_stats.avg_density_pct
        FROM scope_stats
        CROSS JOIN pct_stats
        """
    )
    return _execute(db, query, params).mappings().first() or {
        "segments_considered": 0,
        "avg_density": 0.0,
        "avg_crimes_per_km": 0.0,
        "avg_user_reported_crime_signal_per_km": 0.0,
        "avg_collisions_per_km": 0.0,
        "avg_collision_points_per_km": 0.0,
        "red_segment_share": 0.0,
        "avg_density_pct": 0.0,
    }


def _density_percentile(
    db: Session,
    month_window,
    crime_type: Optional[str],
    density_value: float,
    w_crime: float,
    w_collision: float,
):
    crime_type_clause = ""
    user_report_crime_type_clause = ""
    params = {
        "from_date": month_window["from_date"],
        "to_date": month_window["to_date"],
        "density_value": density_value,
        "w_crime": w_crime,
        "w_collision_applied": w_collision,
    }
    params.update(_user_report_weight_params())
    if crime_type:
        crime_type_clause = "AND smts.crime_type = :crime_type"
        user_report_crime_type_clause = "AND urc.crime_type = :crime_type"
        params["crime_type"] = crime_type

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    query = text(
        f"""
        /* analytics_density_percentile */
        WITH crime_agg AS (
            SELECT
                smts.segment_id,
                SUM(smts.crime_count)::double precision AS crimes
            FROM segment_month_type_stats smts
            WHERE smts.month BETWEEN :from_date AND :to_date
              {crime_type_clause}
            GROUP BY smts.segment_id
        ),
        user_report_base AS (
            SELECT
                ure.segment_id,
                ure.month,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.month BETWEEN :from_date AND :to_date
              {user_report_crime_type_clause}
            GROUP BY ure.segment_id, ure.month, urc.crime_type
        ),
        user_report_agg AS (
            SELECT
                segment_id,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal
            FROM user_report_base
            GROUP BY segment_id
        ),
        collision_agg AS (
            SELECT
                smcs.segment_id,
                SUM(smcs.collision_count)::double precision AS collisions,
                SUM(smcs.fatal_casualty_count)::double precision AS fatal_casualties,
                SUM(smcs.serious_casualty_count)::double precision AS serious_casualties,
                SUM(smcs.slight_casualty_count)::double precision AS slight_casualties
            FROM segment_month_collision_stats smcs
            WHERE smcs.month BETWEEN :from_date AND :to_date
            GROUP BY smcs.segment_id
        ),
        scored AS (
            SELECT
                ((:w_crime * (
                    (COALESCE(crime_agg.crimes, 0.0) + COALESCE(user_report_agg.user_reported_crime_signal, 0.0))
                    / (GREATEST(rs.length_m, 100.0) / 1000.0)
                 )) +
                 (:w_collision_applied * (
                    (
                        COALESCE(collision_agg.collisions, 0.0)
                        + (0.5 * COALESCE(collision_agg.slight_casualties, 0.0))
                        + (2.0 * COALESCE(collision_agg.serious_casualties, 0.0))
                        + (5.0 * COALESCE(collision_agg.fatal_casualties, 0.0))
                    ) / (GREATEST(rs.length_m, 100.0) / 1000.0)
                 ))) AS combined_density
            FROM road_segments rs
            LEFT JOIN crime_agg ON crime_agg.segment_id = rs.id
            LEFT JOIN user_report_agg ON user_report_agg.segment_id = rs.id
            LEFT JOIN collision_agg ON collision_agg.segment_id = rs.id
        )
        SELECT
            COALESCE(
                AVG(CASE WHEN scored.combined_density <= :density_value THEN 1 ELSE 0 END),
                0.0
            ) AS density_pct
        FROM scored
        """
    )
    row = _execute(db, query, params).mappings().first() or {"density_pct": 0.0}
    return _round_pct(row.get("density_pct"))


def _forecast_history_rows(
    db: Session,
    forecast_window,
    bbox,
    crime_type: Optional[str],
    include_collisions: bool,
    w_crime: float,
    w_collision: float,
):
    coverage_params = {
        "baseline_from_date": forecast_window["baseline_from_date"],
        "baseline_to_date": forecast_window["baseline_to_date"],
    }
    coverage_query = text(
        """
        /* analytics_risk_forecast_coverage */
        WITH months AS (
            SELECT generate_series(
                CAST(:baseline_from_date AS date),
                CAST(:baseline_to_date AS date),
                interval '1 month'
            )::date AS month
        )
        SELECT COUNT(*) FILTER (WHERE data_months.month IS NULL) AS missing_months
        FROM months
        LEFT JOIN (SELECT DISTINCT ce.month FROM crime_events ce) AS data_months
          ON data_months.month = months.month
        """
    )
    coverage = _execute(db, coverage_query, coverage_params).mappings().first() or {"missing_months": 0}
    if (coverage.get("missing_months") or 0) > 0:
        raise AnalyticsAPIError(
            400,
            "BASELINE_HISTORY_INSUFFICIENT",
            "The dataset does not contain every required baseline month",
            {"missing_months": int(coverage["missing_months"])},
        )

    if include_collisions:
        collision_coverage_query = text(
            """
            /* analytics_risk_forecast_collision_coverage */
            WITH months AS (
                SELECT generate_series(
                    CAST(:baseline_from_date AS date),
                    CAST(:baseline_to_date AS date),
                    interval '1 month'
                )::date AS month
            )
            SELECT COUNT(*) FILTER (WHERE data_months.month IS NULL) AS missing_months
            FROM months
            LEFT JOIN (SELECT DISTINCT ce.month FROM collision_events ce) AS data_months
              ON data_months.month = months.month
            """
        )
        collision_coverage = _execute(db, collision_coverage_query, coverage_params).mappings().first() or {
            "missing_months": 0
        }
        if (collision_coverage.get("missing_months") or 0) > 0:
            raise AnalyticsAPIError(
                400,
                "BASELINE_HISTORY_INSUFFICIENT",
                "Collision data does not contain every required baseline month",
                {"missing_months": int(collision_coverage["missing_months"])},
            )

    crime_type_clause = ""
    user_report_crime_type_clause = ""
    params = dict(bbox)
    params.update(
        {
            "baseline_from_date": forecast_window["baseline_from_date"],
            "baseline_to_date": forecast_window["baseline_to_date"],
        }
    )
    params.update(_user_report_weight_params())
    if crime_type:
        crime_type_clause = "AND ce.crime_type = :crime_type"
        user_report_crime_type_clause = "AND urc.crime_type = :crime_type"
        params["crime_type"] = crime_type

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    history_query = text(
        f"""
        /* analytics_risk_forecast_history */
        WITH months AS (
            SELECT generate_series(
                CAST(:baseline_from_date AS date),
                CAST(:baseline_to_date AS date),
                interval '1 month'
            )::date AS month
        ),
            counts AS (
                SELECT
                    ce.month,
                    COUNT(*)::bigint AS count
                FROM crime_events ce
            WHERE ce.geom IS NOT NULL
              AND ce.lon BETWEEN :min_lon AND :max_lon
              AND ce.lat BETWEEN :min_lat AND :max_lat
              AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
                  AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
                  {crime_type_clause}
                GROUP BY ce.month
        ),
        user_report_base AS (
            SELECT
                ure.month,
                ure.segment_id,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.geom IS NOT NULL
              AND ure.longitude BETWEEN :min_lon AND :max_lon
              AND ure.latitude BETWEEN :min_lat AND :max_lat
              AND ure.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              AND ure.month BETWEEN :baseline_from_date AND :baseline_to_date
              {user_report_crime_type_clause}
            GROUP BY ure.month, ure.segment_id, urc.crime_type
        ),
        user_report_counts AS (
            SELECT
                user_report_base.month,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal,
                COALESCE(SUM(authenticated_reports + anonymous_reports), 0)::bigint AS approved_user_reports
            FROM user_report_base
            GROUP BY user_report_base.month
        )
        SELECT
            TO_CHAR(months.month, 'YYYY-MM') AS month,
            COALESCE(counts.count, 0)::double precision AS official_count,
            COALESCE(user_report_counts.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
            COALESCE(user_report_counts.approved_user_reports, 0)::bigint AS approved_user_reports,
            (COALESCE(counts.count, 0)::double precision + COALESCE(user_report_counts.user_reported_crime_signal, 0.0))
                AS count
        FROM months
        LEFT JOIN counts ON counts.month = months.month
        LEFT JOIN user_report_counts ON user_report_counts.month = months.month
        ORDER BY months.month ASC
        """
    )
    crime_rows = _execute(db, history_query, params).mappings().all()
    crime_map = {row["month"]: float(row["count"] or 0.0) for row in crime_rows}

    collision_map: Dict[str, Dict[str, float]] = {}
    if include_collisions:
        collision_query = text(
            """
            /* analytics_risk_forecast_collision_history */
            WITH months AS (
                SELECT generate_series(
                    CAST(:baseline_from_date AS date),
                    CAST(:baseline_to_date AS date),
                    interval '1 month'
                )::date AS month
            ),
            counts AS (
                SELECT
                    ce.month,
                    COUNT(*)::bigint AS collision_count,
                    COALESCE(
                        SUM(
                            1.0
                            + (0.5 * COALESCE(ce.slight_casualty_count, 0))
                            + (2.0 * COALESCE(ce.serious_casualty_count, 0))
                            + (5.0 * COALESCE(ce.fatal_casualty_count, 0))
                        ),
                        0.0
                    ) AS collision_points
                FROM collision_events ce
                WHERE ce.geom IS NOT NULL
                  AND ce.longitude BETWEEN :min_lon AND :max_lon
                  AND ce.latitude BETWEEN :min_lat AND :max_lat
                  AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
                  AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
                GROUP BY ce.month
            )
            SELECT
                TO_CHAR(months.month, 'YYYY-MM') AS month,
                COALESCE(counts.collision_count, 0)::bigint AS collision_count,
                COALESCE(counts.collision_points, 0.0) AS collision_points
            FROM months
            LEFT JOIN counts ON counts.month = months.month
            ORDER BY months.month ASC
            """
        )
        collision_rows = _execute(db, collision_query, params).mappings().all()
        collision_map = {
            row["month"]: {
                "collision_count": int(row["collision_count"]),
                "collision_points": float(row["collision_points"] or 0.0),
            }
            for row in collision_rows
        }

    history = []
    for row in crime_rows:
        month_label = row["month"]
        collision_count = collision_map.get(month_label, {}).get("collision_count", 0)
        collision_points = collision_map.get(month_label, {}).get("collision_points", 0.0)
        crime_count = float(crime_map.get(month_label, 0.0))
        combined_value = (w_crime * crime_count) + (w_collision * collision_points)
        history.append(
            {
                "month": month_label,
                "official_crime_count": int(row.get("official_count") or 0),
                "approved_user_reports": int(row.get("approved_user_reports") or 0),
                "user_reported_crime_signal": _round_rate(row.get("user_reported_crime_signal")),
                "crime_count": _round_rate(crime_count),
                "collision_count": int(collision_count),
                "collision_points": _round_rate(collision_points),
                "combined_value": _round_rate(combined_value),
            }
        )

    return history


def _hotspot_rows(db: Session, month_window, bbox, crime_type: Optional[str]):
    official_where_clauses = ["smts.month BETWEEN :from_date AND :to_date"]
    user_report_where_clauses = [
        "ure.admin_approved = TRUE",
        "ure.segment_id IS NOT NULL",
        "ure.month BETWEEN :from_date AND :to_date",
    ]
    final_where_clauses = []
    params = {
        "from_date": month_window["from_date"],
        "to_date": month_window["to_date"],
    }
    params.update(_user_report_weight_params())
    if crime_type:
        official_where_clauses.append("smts.crime_type = :crime_type")
        user_report_where_clauses.append("urc.crime_type = :crime_type")
        params["crime_type"] = crime_type
    if bbox:
        final_where_clauses.extend(
            [
                "rs.geom && ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857)",
                "ST_Intersects(rs.geom, ST_Transform(ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326), 3857))",
            ]
        )
        params.update(bbox)

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    query = text(
        f"""
        /* analytics_hotspot_stability_monthly */
        WITH official_agg AS (
            SELECT
                smts.month,
                smts.segment_id,
                SUM(smts.crime_count)::double precision AS official_crimes
            FROM segment_month_type_stats smts
            WHERE {' AND '.join(official_where_clauses)}
            GROUP BY smts.month, smts.segment_id
        ),
        user_report_base AS (
            SELECT
                ure.month,
                ure.segment_id,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE {' AND '.join(user_report_where_clauses)}
            GROUP BY ure.month, ure.segment_id, urc.crime_type
        ),
        user_report_agg AS (
            SELECT
                user_report_base.month,
                user_report_base.segment_id,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal
            FROM user_report_base
            GROUP BY user_report_base.month, user_report_base.segment_id
        ),
        combined_agg AS (
            SELECT
                COALESCE(official_agg.month, user_report_agg.month) AS month,
                COALESCE(official_agg.segment_id, user_report_agg.segment_id) AS segment_id,
                COALESCE(official_agg.official_crimes, 0.0) AS official_crimes,
                COALESCE(user_report_agg.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
                COALESCE(official_agg.official_crimes, 0.0) + COALESCE(user_report_agg.user_reported_crime_signal, 0.0)
                    AS crimes
            FROM official_agg
            FULL OUTER JOIN user_report_agg
              ON user_report_agg.month = official_agg.month
             AND user_report_agg.segment_id = official_agg.segment_id
        )
        SELECT
            combined_agg.month,
            combined_agg.segment_id,
            combined_agg.crimes,
            combined_agg.crimes / (GREATEST(rs.length_m, 100.0) / 1000.0) AS crimes_per_km
        FROM combined_agg
        JOIN road_segments rs ON rs.id = combined_agg.segment_id
        {f"WHERE {' AND '.join(final_where_clauses)}" if final_where_clauses else ""}
        ORDER BY combined_agg.month ASC, crimes_per_km DESC, combined_agg.segment_id ASC
        """
    )
    return _execute(db, query, params).mappings().all()


def _validate_route_line(route_line: Dict[str, Any]) -> str:
    if not isinstance(route_line, dict):
        raise AnalyticsAPIError(400, "INVALID_ROUTE_LINE", "route_line must be a GeoJSON object")
    if route_line.get("type") != "LineString":
        raise AnalyticsAPIError(400, "INVALID_ROUTE_LINE", "route_line must be a GeoJSON LineString")
    coordinates = route_line.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise AnalyticsAPIError(400, "INVALID_ROUTE_LINE", "route_line must contain at least two coordinates")
    return json.dumps(route_line)


def _ordered_values_sql(segment_ids: List[int]) -> Tuple[str, Dict[str, Any]]:
    values_sql = []
    params: Dict[str, Any] = {}
    for index, segment_id in enumerate(segment_ids):
        params[f"segment_id_{index}"] = segment_id
        values_sql.append(f"(:segment_id_{index}, {index + 1})")
    return ", ".join(values_sql), params


def _segment_metadata_by_ids(db: Session, segment_ids: List[int]):
    query = text(
        """
        /* analytics_route_segments_by_ids */
        SELECT
            rs.id AS segment_id,
            rs.name,
            rs.highway,
            rs.length_m
        FROM road_segments rs
        WHERE rs.id IN :segment_ids
        """
    ).bindparams(bindparam("segment_ids", expanding=True))
    rows = _execute(db, query, {"segment_ids": segment_ids}).mappings().all()
    rows_by_id = {int(row["segment_id"]): row for row in rows}
    missing = [segment_id for segment_id in segment_ids if segment_id not in rows_by_id]
    if missing:
        raise AnalyticsAPIError(
            400,
            "INVALID_SEGMENT_ID",
            "One or more segment_ids do not exist",
            {"missing_segment_ids": missing},
        )
    return rows_by_id


def _route_segments_from_line(db: Session, route_geojson: str, buffer_m: float):
    query = text(
        """
        /* analytics_route_segments_from_line */
        WITH route_line AS (
            SELECT ST_Transform(
                ST_SetSRID(ST_GeomFromGeoJSON(:route_geojson), 4326),
                3857
            ) AS geom
        ),
        buffered AS (
            SELECT ST_Buffer(route_line.geom, :buffer_m) AS geom
            FROM route_line
        )
        SELECT
            rs.id AS segment_id,
            rs.name,
            rs.highway,
            rs.length_m
        FROM road_segments rs
        CROSS JOIN buffered
        WHERE ST_Intersects(rs.geom, buffered.geom)
        ORDER BY rs.id ASC
        """
    )
    rows = _execute(db, query, {"route_geojson": route_geojson, "buffer_m": buffer_m}).mappings().all()
    if not rows:
        raise AnalyticsAPIError(404, "NO_SEGMENTS_MATCH_ROUTE", "No road segments matched the supplied route_line")
    return rows


def _connectivity_breaks(db: Session, segment_ids: List[int], threshold_m: float):
    if len(segment_ids) < 2:
        return []

    ordered_values_sql, ordered_params = _ordered_values_sql(segment_ids)
    params = dict(ordered_params)
    params["threshold_m"] = threshold_m
    query = text(
        f"""
        /* analytics_route_connectivity */
        WITH ordered(segment_id, seq) AS (
            VALUES {ordered_values_sql}
        )
        SELECT
            o1.seq AS break_index,
            o1.segment_id AS from_segment_id,
            o2.segment_id AS to_segment_id,
            ST_Distance(rs1.geom, rs2.geom) AS distance_m
        FROM ordered o1
        JOIN ordered o2 ON o2.seq = o1.seq + 1
        JOIN road_segments rs1 ON rs1.id = o1.segment_id
        JOIN road_segments rs2 ON rs2.id = o2.segment_id
        WHERE ST_Distance(rs1.geom, rs2.geom) > :threshold_m
        ORDER BY o1.seq ASC
        """
    )
    rows = _execute(db, query, params).mappings().all()
    breaks = []
    for row in rows:
        breaks.append(
            {
                "index": int(row["break_index"]),
                "from_segment_id": int(row["from_segment_id"]),
                "to_segment_id": int(row["to_segment_id"]),
                "distance_m": _round_rate(row["distance_m"]),
            }
        )
    return breaks


def _route_segment_metrics(
    db: Session,
    segment_ids: List[int],
    month_window,
    crime_type: Optional[str],
):
    crime_type_clause = ""
    user_report_crime_type_clause = ""
    params = {
        "selected_segment_ids": segment_ids,
        "crime_segment_ids": segment_ids,
        "user_report_segment_ids": segment_ids,
        "collision_segment_ids": segment_ids,
        "from_date": month_window["from_date"],
        "to_date": month_window["to_date"],
    }
    params.update(_user_report_weight_params())
    if crime_type:
        crime_type_clause = "AND smts.crime_type = :crime_type"
        user_report_crime_type_clause = "AND urc.crime_type = :crime_type"
        params["crime_type"] = crime_type

    user_report_signal_sql = _user_report_signal_sql(
        authenticated_reports_sql="authenticated_reports",
        distinct_authenticated_users_sql="distinct_authenticated_users",
        anonymous_reports_sql="anonymous_reports",
    )

    query = text(
        f"""
        /* analytics_route_segment_metrics */
        WITH selected AS (
            SELECT
                rs.id AS segment_id,
                rs.name,
                rs.highway,
                rs.length_m
            FROM road_segments rs
            WHERE rs.id IN :selected_segment_ids
        ),
        crime_agg AS (
            SELECT
                smts.segment_id,
                SUM(smts.crime_count)::double precision AS crimes
            FROM segment_month_type_stats smts
            WHERE smts.segment_id IN :crime_segment_ids
              AND smts.month BETWEEN :from_date AND :to_date
              {crime_type_clause}
            GROUP BY smts.segment_id
        ),
        user_report_base AS (
            SELECT
                ure.segment_id,
                ure.month,
                urc.crime_type,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.segment_id IN :user_report_segment_ids
              AND ure.admin_approved = TRUE
              AND ure.segment_id IS NOT NULL
              AND ure.month BETWEEN :from_date AND :to_date
              {user_report_crime_type_clause}
            GROUP BY ure.segment_id, ure.month, urc.crime_type
        ),
        user_report_agg AS (
            SELECT
                segment_id,
                COALESCE(SUM({user_report_signal_sql}), 0.0) AS user_reported_crime_signal,
                COALESCE(SUM(authenticated_reports + anonymous_reports), 0)::bigint AS approved_user_reports
            FROM user_report_base
            GROUP BY segment_id
        ),
        collision_agg AS (
            SELECT
                smcs.segment_id,
                SUM(smcs.collision_count)::double precision AS collisions,
                SUM(smcs.casualty_count)::double precision AS casualties,
                SUM(smcs.fatal_casualty_count)::double precision AS fatal_casualties,
                SUM(smcs.serious_casualty_count)::double precision AS serious_casualties,
                SUM(smcs.slight_casualty_count)::double precision AS slight_casualties
            FROM segment_month_collision_stats smcs
            WHERE smcs.segment_id IN :collision_segment_ids
              AND smcs.month BETWEEN :from_date AND :to_date
            GROUP BY smcs.segment_id
        )
        SELECT
            selected.segment_id,
            selected.name,
            selected.highway,
            selected.length_m,
            COALESCE(crime_agg.crimes, 0.0) AS official_crimes,
            COALESCE(user_report_agg.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
            COALESCE(user_report_agg.approved_user_reports, 0)::bigint AS approved_user_reports,
            COALESCE(crime_agg.crimes, 0.0) + COALESCE(user_report_agg.user_reported_crime_signal, 0.0) AS crimes,
            COALESCE(collision_agg.collisions, 0.0) AS collisions,
            COALESCE(collision_agg.casualties, 0.0) AS casualties,
            COALESCE(collision_agg.fatal_casualties, 0.0) AS fatal_casualties,
            COALESCE(collision_agg.serious_casualties, 0.0) AS serious_casualties,
            COALESCE(collision_agg.slight_casualties, 0.0) AS slight_casualties
        FROM selected
        LEFT JOIN crime_agg ON crime_agg.segment_id = selected.segment_id
        LEFT JOIN user_report_agg ON user_report_agg.segment_id = selected.segment_id
        LEFT JOIN collision_agg ON collision_agg.segment_id = selected.segment_id
        """
    ).bindparams(
        bindparam("selected_segment_ids", expanding=True),
        bindparam("crime_segment_ids", expanding=True),
        bindparam("user_report_segment_ids", expanding=True),
        bindparam("collision_segment_ids", expanding=True),
    )
    return _execute(db, query, params).mappings().all()


def _evaluate_route(
    db: Session,
    month_window,
    mode: str,
    crime_type: Optional[str],
    include_collisions: bool,
    segment_ids: Optional[List[int]] = None,
    route_line: Optional[Dict[str, Any]] = None,
    check_connectivity: bool = True,
    threshold_m: float = 20.0,
    buffer_m: float = 25.0,
    fail_on_disconnect: bool = False,
):
    if include_collisions and mode != "drive":
        raise AnalyticsAPIError(
            400,
            "INVALID_MODE_FOR_COLLISIONS",
            "includeCollisions is only supported when mode is drive",
        )

    has_segment_ids = segment_ids is not None
    has_route_line = route_line is not None
    if has_segment_ids == has_route_line:
        raise AnalyticsAPIError(
            400,
            "INVALID_ROUTE_INPUT",
            "Exactly one of segment_ids or route_line must be provided",
        )

    input_type = "segment_ids" if has_segment_ids else "route_line"
    rows_by_id = {}
    breaks = []
    is_connected = True

    if has_segment_ids:
        if len(segment_ids or []) < 2 or len(segment_ids or []) > 5000:
            raise AnalyticsAPIError(
                400,
                "INVALID_ROUTE_INPUT",
                "segment_ids must contain between 2 and 5000 ids",
            )
        if len(set(segment_ids or [])) != len(segment_ids or []):
            raise AnalyticsAPIError(400, "ROUTE_HAS_DUPLICATES", "segment_ids contains duplicates")

        rows_by_id = _segment_metadata_by_ids(db, segment_ids or [])
        if check_connectivity:
            breaks = _connectivity_breaks(db, segment_ids or [], threshold_m)
            is_connected = len(breaks) == 0
            if breaks and fail_on_disconnect:
                raise AnalyticsAPIError(
                    400,
                    "ROUTE_DISCONNECTED",
                    "The provided route contains disconnected segment pairs",
                    {"breaks": breaks},
                )
        ordered_segment_ids = segment_ids or []
    else:
        route_geojson = _validate_route_line(route_line or {})
        route_rows = _route_segments_from_line(db, route_geojson, buffer_m)
        ordered_segment_ids = [int(row["segment_id"]) for row in route_rows]
        rows_by_id = {int(row["segment_id"]): row for row in route_rows}

    metric_rows = _route_segment_metrics(db, ordered_segment_ids, month_window, crime_type)
    metric_map = {int(row["segment_id"]): row for row in metric_rows}

    segment_summaries = []
    total_length_km = 0.0
    weighted_score_sum = 0.0
    total_crimes = 0.0
    total_official_crimes = 0.0
    total_user_reported_crime_signal = 0.0
    total_approved_user_reports = 0
    total_collisions = 0.0

    for segment_id in ordered_segment_ids:
        row = dict(rows_by_id[segment_id])
        metrics = metric_map.get(segment_id, {})
        crimes = float(metrics.get("crimes") or 0.0)
        official_crimes = float(metrics.get("official_crimes") or 0.0)
        user_reported_crime_signal = float(metrics.get("user_reported_crime_signal") or 0.0)
        approved_user_reports = int(metrics.get("approved_user_reports") or 0)
        collisions = float(metrics.get("collisions") or 0.0)
        casualties = float(metrics.get("casualties") or 0.0)
        fatal_casualties = float(metrics.get("fatal_casualties") or 0.0)
        serious_casualties = float(metrics.get("serious_casualties") or 0.0)
        slight_casualties = float(metrics.get("slight_casualties") or 0.0)
        length_m = float(row.get("length_m") or 0.0)
        actual_length_km = length_m / 1000.0
        normalized_km = max(length_m, 100.0) / 1000.0
        crime_density = _safe_div(crimes, normalized_km)
        collision_points = collisions + (0.5 * slight_casualties) + (2.0 * serious_casualties) + (5.0 * fatal_casualties)
        collision_density = _safe_div(collision_points, normalized_km) if include_collisions and mode == "drive" else 0.0
        score_metric = crime_density + collision_density
        contribution = score_metric * actual_length_km

        total_length_km += actual_length_km
        weighted_score_sum += contribution
        total_crimes += crimes
        total_official_crimes += official_crimes
        total_user_reported_crime_signal += user_reported_crime_signal
        total_approved_user_reports += approved_user_reports
        total_collisions += collisions

        segment_summaries.append(
            {
                "segment_id": int(segment_id),
                "name": row.get("name"),
                "highway": row.get("highway"),
                "official_crimes": _round_rate(official_crimes),
                "approved_user_reports": approved_user_reports,
                "user_reported_crime_signal": _round_rate(user_reported_crime_signal),
                "crimes": _round_rate(crimes),
                "collisions": int(collisions),
                "crimes_per_km": _round_rate(crime_density),
                "collision_density": _round_rate(collision_density),
                "contribution": _round_rate(contribution),
            }
        )

    worst_segments = sorted(
        segment_summaries,
        key=lambda item: item["contribution"],
        reverse=True,
    )[:10]

    score_raw = _safe_div(weighted_score_sum, total_length_km)
    return {
        "input_type": input_type,
        "ordered_segment_ids": ordered_segment_ids,
        "route_stats": {
            "segment_count": len(ordered_segment_ids),
            "total_length_km": _round_rate(total_length_km),
            "official_total_crimes": _round_rate(total_official_crimes),
            "approved_user_reports": total_approved_user_reports,
            "user_reported_crime_signal": _round_rate(total_user_reported_crime_signal),
            "total_crimes": _round_rate(total_crimes),
            "total_collisions": int(total_collisions),
            "score_raw": _round_rate(score_raw),
        },
        "connectivity": {
            "is_connected": is_connected,
            "breaks": breaks,
        },
        "worst_segments": worst_segments,
    }


def build_risk_score_payload(
    db: Session,
    *,
    from_value: str,
    to_value: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_type: Optional[str] = None,
    include_collisions: bool = False,
    mode: str = "walk",
    w_crime: float = 1.0,
    w_collision: float = 0.0,
):
    month_window = _validate_month_window(from_value, to_value)
    bbox = _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    mode = _normalize_string(mode) or "walk"
    crime_type = _normalize_string(crime_type)
    if mode not in {"walk", "drive"}:
        raise AnalyticsAPIError(400, "INVALID_MODE", "mode must be either walk or drive")
    if include_collisions and mode != "drive":
        raise AnalyticsAPIError(
            400,
            "INVALID_MODE_FOR_COLLISIONS",
            "includeCollisions is only supported when mode is drive",
        )

    applied_w_collision = _applied_collision_weight(
        include_collisions,
        mode,
        w_collision,
    )
    area_row = _risk_score_area_metrics(db, month_window, bbox, crime_type)
    segment_row = _risk_score_segment_metrics(
        db,
        month_window,
        bbox,
        crime_type,
        w_crime,
        applied_w_collision,
    )

    area_km2 = float(area_row.get("area_km2") or 0.0)
    total_crimes = int(area_row.get("total_crimes") or 0)
    approved_user_reports = int(area_row.get("approved_user_reports") or 0)
    user_reported_crime_signal = float(area_row.get("user_reported_crime_signal") or 0.0)
    effective_total_crimes = total_crimes + user_reported_crime_signal
    total_collisions = int(area_row.get("total_collisions") or 0)
    total_collision_points = float(area_row.get("total_collision_points") or 0.0)
    pct = _round_pct(segment_row.get("avg_density_pct"))
    score = int(round(100 * pct))
    band = _band_from_pct(pct)
    collision_applied = bool(applied_w_collision)
    score_basis = "crime+collision" if collision_applied else "crime"
    metrics = {
        "total_crimes": total_crimes,
        "approved_user_reports": approved_user_reports,
        "user_reported_crime_signal": _round_rate(user_reported_crime_signal),
        "effective_total_crimes": _round_rate(effective_total_crimes),
        "area_km2": _round_rate(area_km2),
        "crimes_per_km2": _round_rate(_safe_div(total_crimes, area_km2)),
        "effective_crimes_per_km2": _round_rate(_safe_div(effective_total_crimes, area_km2)),
        "segments_considered": int(segment_row.get("segments_considered") or 0),
        "avg_crimes_per_km": _round_rate(segment_row.get("avg_crimes_per_km")),
        "avg_user_reported_crime_signal_per_km": _round_rate(
            segment_row.get("avg_user_reported_crime_signal_per_km")
        ),
        "red_segment_share": _round_rate(segment_row.get("red_segment_share")),
        "weights_applied": {
            "w_crime": _round_rate(w_crime),
            "w_collision": _round_rate(applied_w_collision),
        },
    }
    if collision_applied:
        metrics.update(
            {
                "total_collisions": total_collisions,
                "collisions_per_km2": _round_rate(_safe_div(total_collisions, area_km2)),
                "collision_points_per_km2": _round_rate(_safe_div(total_collision_points, area_km2)),
                "avg_collisions_per_km": _round_rate(segment_row.get("avg_collisions_per_km")),
                "avg_collision_points_per_km": _round_rate(segment_row.get("avg_collision_points_per_km")),
            }
        )

    return {
        "scope": _scope_payload(
            month_window,
            bbox,
            {
                "mode": mode,
                "crimeType": crime_type,
                "includeCollisions": include_collisions,
            },
        ),
        "generated_at": _generated_at(),
        "score_basis": score_basis,
        "risk_score": score,
        "score": score,
        "pct": pct,
        "band": band,
        "metrics": metrics,
        "explain": {
            "reading": _band_interpretation(pct, "bbox"),
            "user_reports": "Approved user-reported crimes are blended into the crime density as a capped low-weight supplement and do not override official counts.",
        },
    }


def build_risk_forecast_payload(
    db: Session,
    *,
    target: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_type: Optional[str] = None,
    baseline_months: int = 6,
    method: str = "poisson_mean",
    return_risk_projection: bool = False,
    include_collisions: bool = False,
    mode: str = "walk",
    w_crime: float = 1.0,
    w_collision: float = 0.0,
):
    bbox = _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    forecast_window = _validate_target_and_baseline(target, baseline_months)
    crime_type = _normalize_string(crime_type)
    method = _normalize_string(method) or "poisson_mean"
    mode = _normalize_string(mode) or "walk"
    if method != "poisson_mean":
        raise AnalyticsAPIError(400, "INVALID_METHOD", "Only poisson_mean is supported in v1")
    if mode not in {"walk", "drive"}:
        raise AnalyticsAPIError(400, "INVALID_MODE", "mode must be either walk or drive")
    if include_collisions and mode != "drive":
        raise AnalyticsAPIError(
            400,
            "INVALID_MODE_FOR_COLLISIONS",
            "includeCollisions is only supported when mode is drive",
        )

    applied_w_collision = _applied_collision_weight(
        include_collisions,
        mode,
        w_collision,
    )

    history_rows = _forecast_history_rows(
        db,
        forecast_window,
        bbox,
        crime_type,
        include_collisions and mode == "drive",
        w_crime,
        applied_w_collision,
    )
    crime_counts = [float(row["crime_count"]) for row in history_rows]
    official_crime_counts = [int(row.get("official_crime_count") or 0) for row in history_rows]
    user_reported_signals = [float(row.get("user_reported_crime_signal") or 0.0) for row in history_rows]
    approved_user_reports = [int(row.get("approved_user_reports") or 0) for row in history_rows]
    collision_counts = [int(row["collision_count"]) for row in history_rows]
    collision_points = [float(row["collision_points"]) for row in history_rows]
    combined_values = [float(row["combined_value"]) for row in history_rows]

    baseline_mean = sum(crime_counts) / len(crime_counts)
    expected_count = int(round(baseline_mean))
    sigma = 1.96 * math.sqrt(max(baseline_mean, 1e-9))
    low = int(math.floor(max(0.0, baseline_mean - sigma)))
    high = int(math.ceil(baseline_mean + sigma))

    collision_baseline_mean = sum(collision_counts) / len(collision_counts)
    collision_points_baseline_mean = sum(collision_points) / len(collision_points)
    combined_baseline_mean = sum(combined_values) / len(combined_values)
    user_reported_baseline_mean = sum(user_reported_signals) / len(user_reported_signals)
    expected_collision_count = int(round(collision_baseline_mean))
    expected_collision_points = _round_rate(collision_points_baseline_mean)
    expected_combined_value = _round_rate(combined_baseline_mean)

    ratio = None
    if baseline_mean > 0:
        ratio = _round_rate(expected_count / baseline_mean)
    elif expected_count == 0:
        ratio = 0.0

    combined_ratio = None
    if combined_baseline_mean > 0:
        combined_ratio = _round_rate(expected_combined_value / combined_baseline_mean)
    elif expected_combined_value == 0:
        combined_ratio = 0.0

    collision_applied = bool(applied_w_collision)
    score_basis = "crime+collision" if collision_applied else "crime"
    history = []
    for row in history_rows:
        item = {
            "month": row["month"],
            "official_crime_count": int(row.get("official_crime_count") or 0),
            "approved_user_reports": int(row.get("approved_user_reports") or 0),
            "user_reported_crime_signal": _round_rate(row.get("user_reported_crime_signal")),
            "crime_count": _round_rate(row["crime_count"]),
        }
        if collision_applied:
            item.update(
                {
                    "collision_count": int(row["collision_count"]),
                    "collision_points": _round_rate(row["collision_points"]),
                    "combined_value": _round_rate(row["combined_value"]),
                }
            )
        history.append(item)

    forecast_payload = {
        "expected_count": expected_count,
        "low": low,
        "high": high,
        "baseline_mean": _round_rate(baseline_mean),
        "ratio": ratio,
        "components": {
            "crimes": {
                "expected_count": expected_count,
                "baseline_mean": _round_rate(baseline_mean),
                "baseline_official_mean": _round_rate(sum(official_crime_counts) / len(official_crime_counts)),
                "baseline_user_reported_signal_mean": _round_rate(user_reported_baseline_mean),
                "baseline_approved_user_reports_mean": _round_rate(sum(approved_user_reports) / len(approved_user_reports)),
            }
        },
    }
    if collision_applied:
        forecast_payload["components"]["collisions"] = {
            "expected_count": expected_collision_count,
            "expected_points": expected_collision_points,
            "baseline_mean": _round_rate(collision_baseline_mean),
            "baseline_points_mean": _round_rate(collision_points_baseline_mean),
            "applied": True,
        }
        forecast_payload["components"]["combined"] = {
            "expected_value": expected_combined_value,
            "baseline_mean": _round_rate(combined_baseline_mean),
            "ratio": combined_ratio,
        }

    response = {
        "scope": _scope_payload(
            forecast_window,
            bbox,
            {
                "crimeType": crime_type,
                "method": method,
                "mode": mode,
                "includeCollisions": include_collisions,
            },
        ),
        "generated_at": _generated_at(),
        "score_basis": score_basis,
        "history": history,
        "forecast": forecast_payload,
        "explanation": {
            "summary": "The forecast uses the mean monthly count over the immediately preceding baseline window and a simple normal approximation around the Poisson mean.",
            "collisions": "When includeCollisions is true in drive mode, the response also reports monthly collision counts and severity-weighted collision points.",
            "user_reports": "Approved user-reported crimes are blended into the crime signal as a capped low-weight supplement before the monthly baseline is averaged.",
        },
    }

    if return_risk_projection:
        predicted_band = "green"
        projection_ratio = combined_ratio if collision_applied else ratio
        if projection_ratio is not None:
            if projection_ratio >= 1.5:
                predicted_band = "red"
            elif projection_ratio >= 1.25:
                predicted_band = "amber"
        elif expected_count > 0:
            predicted_band = "red"

        response["forecast"]["predicted_monthly_count"] = expected_count
        response["forecast"]["predicted_band"] = predicted_band
        response["forecast"]["projection_basis"] = "combined" if collision_applied else "crimes"

    return response


def build_hotspot_stability_payload(
    db: Session,
    *,
    from_value: str,
    to_value: str,
    k: int = 20,
    include_lists: bool = False,
    min_lon: Optional[float] = None,
    min_lat: Optional[float] = None,
    max_lon: Optional[float] = None,
    max_lat: Optional[float] = None,
    crime_type: Optional[str] = None,
):
    month_window = _validate_month_window(from_value, to_value)
    bbox = _optional_bbox(min_lon, min_lat, max_lon, max_lat)
    crime_type = _normalize_string(crime_type)
    rows = _hotspot_rows(db, month_window, bbox, crime_type)

    month_labels = []
    current_month = month_window["from_date"]
    while current_month <= month_window["to_date"]:
        month_labels.append(_month_label(current_month))
        current_month = _shift_month(current_month, 1)

    by_month: Dict[str, List[int]] = {month_label: [] for month_label in month_labels}
    for row in rows:
        month_label = _month_label(row["month"])
        by_month.setdefault(month_label, []).append((int(row["segment_id"]), float(row["crimes_per_km"] or 0.0)))

    topk_by_month: Dict[str, List[int]] = {}
    appearances: Counter = Counter()
    for month_label in month_labels:
        ranked = sorted(by_month.get(month_label, []), key=lambda item: (-item[1], item[0]))
        segment_ids = [segment_id for segment_id, _ in ranked[:k]]
        topk_by_month[month_label] = segment_ids
        appearances.update(segment_ids)

    stability_series = []
    for index in range(1, len(month_labels)):
        previous_ids = set(topk_by_month[month_labels[index - 1]])
        current_ids = set(topk_by_month[month_labels[index]])
        union_size = len(previous_ids | current_ids)
        overlap_count = len(previous_ids & current_ids)
        jaccard = 1.0 if union_size == 0 else overlap_count / union_size
        stability_series.append(
            {
                "month": month_labels[index],
                "jaccard_vs_prev": _round_pct(jaccard),
                "overlap_count": overlap_count,
            }
        )

    persistent_hotspots = [
        {
            "segment_id": segment_id,
            "appearances": appearances_count,
            "appearance_ratio": _round_pct(appearances_count / max(len(month_labels), 1)),
        }
        for segment_id, appearances_count in appearances.most_common(20)
    ]

    response = {
        "scope": _scope_payload(
            month_window,
            bbox,
            {
                "crimeType": crime_type,
                "k": k,
            },
        ),
        "generated_at": _generated_at(),
        "stability_series": stability_series,
        "persistent_hotspots": persistent_hotspots,
        "summary": {
            "months_evaluated": len(month_labels),
            "average_jaccard": _round_pct(
                sum(item["jaccard_vs_prev"] for item in stability_series) / max(len(stability_series), 1)
            ),
            "persistent_hotspot_count": len(persistent_hotspots),
            "notes": "Higher Jaccard values mean the top risky roads are persisting from month to month; lower values mean the hotspot pattern is moving around.",
        },
    }
    if include_lists:
        response["topk_by_month"] = [
            {"month": month_label, "segment_ids": topk_by_month[month_label]}
            for month_label in month_labels
        ]
    return response


@router.get("/meta")
def analytics_meta(db: Session = Depends(get_db)):
    try:
        counts_query = text(
            """
            SELECT
                TO_CHAR(MIN(ce.month), 'YYYY-MM') AS min_month,
                TO_CHAR(MAX(ce.month), 'YYYY-MM') AS max_month,
                COUNT(*)::bigint AS crime_events_total,
                COUNT(*) FILTER (WHERE ce.geom IS NOT NULL)::bigint AS crime_events_with_geom,
                COUNT(*) FILTER (WHERE ce.segment_id IS NOT NULL)::bigint AS crime_events_snapped,
                (SELECT COUNT(*)::bigint FROM road_segments) AS road_segments_total
            FROM crime_events ce
            """
        )
        types_query = text(
            """
            SELECT DISTINCT ce.crime_type
            FROM crime_events ce
            WHERE NULLIF(ce.crime_type, '') IS NOT NULL
            ORDER BY ce.crime_type ASC
            """
        )

        counts_row = _execute(db, counts_query).mappings().first() or {}
        type_rows = _execute(db, types_query).mappings().all()
        return {
            "months": {
                "min": counts_row.get("min_month"),
                "max": counts_row.get("max_month"),
            },
            "crime_types": [row["crime_type"] for row in type_rows],
            "counts": {
                "crime_events_total": int(counts_row.get("crime_events_total") or 0),
                "crime_events_with_geom": int(counts_row.get("crime_events_with_geom") or 0),
                "crime_events_snapped": int(counts_row.get("crime_events_snapped") or 0),
                "road_segments_total": int(counts_row.get("road_segments_total") or 0),
            },
        }
    except AnalyticsAPIError as exc:
        return _error_response(exc)


@router.post("/risk/score")
def analytics_risk_score(request: RiskScoreRequest, db: Session = Depends(get_db)):
    try:
        return build_risk_score_payload(
            db,
            from_value=request.from_,
            to_value=request.to,
            min_lon=request.minLon,
            min_lat=request.minLat,
            max_lon=request.maxLon,
            max_lat=request.maxLat,
            crime_type=request.crimeType,
            include_collisions=request.includeCollisions,
            mode=request.mode,
            w_crime=request.weights.w_crime,
            w_collision=request.weights.w_collision,
        )
    except AnalyticsAPIError as exc:
        return _error_response(exc)


@router.post("/risk/forecast")
def analytics_risk_forecast(request: ForecastRequest, db: Session = Depends(get_db)):
    try:
        return build_risk_forecast_payload(
            db,
            target=request.target,
            min_lon=request.minLon,
            min_lat=request.minLat,
            max_lon=request.maxLon,
            max_lat=request.maxLat,
            crime_type=request.crimeType,
            baseline_months=request.baselineMonths,
            method=request.method,
            return_risk_projection=request.returnRiskProjection,
            include_collisions=request.includeCollisions,
            mode=request.mode,
            w_crime=request.weights.w_crime,
            w_collision=request.weights.w_collision,
        )
    except AnalyticsAPIError as exc:
        return _error_response(exc)


@router.get("/patterns/hotspot-stability")
def analytics_hotspot_stability(
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
    k: int = Query(20, ge=5, le=200),
    includeLists: bool = Query(False),
    minLon: Optional[float] = Query(None),
    minLat: Optional[float] = Query(None),
    maxLon: Optional[float] = Query(None),
    maxLat: Optional[float] = Query(None),
    crimeType: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    try:
        return build_hotspot_stability_payload(
            db,
            from_value=from_,
            to_value=to,
            k=k,
            include_lists=includeLists,
            min_lon=minLon,
            min_lat=minLat,
            max_lon=maxLon,
            max_lat=maxLat,
            crime_type=crimeType,
        )
    except AnalyticsAPIError as exc:
        return _error_response(exc)
