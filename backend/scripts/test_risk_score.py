#!/usr/bin/env python3
"""
Standalone experimental risk-score runner.

Why this script exists:
- You want to test and tune algorithm behavior before wiring a new endpoint.
- You want a lightweight execution path with clear runtime measurements.
- You want the core logic reusable later, so this script calls a shared utility module.
"""

import argparse
import hashlib
import json
import math
import statistics
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError, ProgrammingError, TimeoutError as SATimeoutError


# Make `app.*` imports work when running this script directly from repository root:
#   python backend/scripts/test_risk_score.py ...
# The app package lives under backend/app, so we prepend backend/ to sys.path.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import SessionLocal  # noqa: E402


# ----------------------------- Algorithm constants -----------------------------
# These constants are kept inside this script on purpose so the script remains
# fully standalone and does not depend on analytics utility modules.

# Crime harm weighting requested for v1.
CRIME_HARM_WEIGHTS: Dict[str, float] = {
    "Violence and sexual offences": 3.0,
    "Robbery": 2.5,
    "Burglary": 2.0,
    "Vehicle crime": 1.5,
    "Criminal damage and arson": 1.2,
}
DEFAULT_CRIME_HARM_WEIGHT = 1.0

# Recency decay constants.
CRIME_DECAY_LAMBDA = 0.1625
COLLISION_DECAY_LAMBDA = 0.08
USER_REPORT_DECAY_LAMBDA = 0.51

# User report cluster + source-confidence weighting constants.
USER_REPORT_CLUSTER_CAP = 3.0
USER_REPORT_DISTINCT_AUTH_WEIGHT = 1.0
USER_REPORT_ANONYMOUS_WEIGHT = 0.5
USER_REPORT_REPEAT_WEIGHT = 0.25
USER_CRIME_SOURCE_WEIGHT = 0.10
USER_COLLISION_SOURCE_WEIGHT = 0.08

# Mode-specific bias (always include crime and collisions; mode changes bias).
WALK_WEIGHTS = {"w_crime": 0.65, "w_collision": 0.25, "w_user": 0.10}
DRIVE_WEIGHTS = {"w_crime": 0.40, "w_collision": 0.50, "w_user": 0.10}

# Normalization/safety constants.
CRIME_PERSISTENCE_ALPHA = 0.8
ROAD_KM_FLOOR = 0.25
RAW_SCORE_LOG_DIVISOR = 2.5
RAW_SCORE_MAX_FOR_SCALING = 5000.0
MAX_MONTH_SPAN = 24

# Comparison threshold for active persistence/comparison phase.
COMPARISON_MIN_COHORT = 2
REFERENCE_BBOX_COUNT = 2
SIGNATURE_VERSION = "v2_log_norm"


class RiskScriptError(Exception):
    """Simple script-local typed error (no dependency on analytics modules)."""

    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def _parse_month(value: str, field_name: str):
    """Parse YYYY-MM into a month date."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise RiskScriptError(
            "INVALID_MONTH_FORMAT",
            f"{field_name} must be in YYYY-MM format",
            {"field": field_name, "value": value},
        ) from exc


def _month_span(start_date, end_date) -> int:
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1


def _validate_month_window(from_value: str, to_value: str) -> Dict[str, Any]:
    """
    Validate month range and return normalized dates + labels.
    This keeps the script robust for bad CLI input.
    """
    from_date = _parse_month(from_value, "from")
    to_date = _parse_month(to_value, "to")
    if from_date > to_date:
        raise RiskScriptError(
            "INVALID_DATE_RANGE",
            "from must be less than or equal to to",
            {"from": from_value, "to": to_value},
        )
    span = _month_span(from_date, to_date)
    if span > MAX_MONTH_SPAN:
        raise RiskScriptError(
            "RANGE_TOO_LARGE",
            f"Month ranges may not exceed {MAX_MONTH_SPAN} months",
            {"max_months": MAX_MONTH_SPAN},
        )
    return {
        "from_date": from_date,
        "to_date": to_date,
        "from": from_date.strftime("%Y-%m"),
        "to": to_date.strftime("%Y-%m"),
        "span_months": span,
    }


def _validate_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> Dict[str, float]:
    if min_lon >= max_lon or min_lat >= max_lat:
        raise RiskScriptError(
            "INVALID_BBOX",
            "min_lon must be less than max_lon and min_lat must be less than max_lat",
            {
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            },
        )
    return {
        "min_lon": min_lon,
        "min_lat": min_lat,
        "max_lon": max_lon,
        "max_lat": max_lat,
    }


def _normalize_mode(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized not in {"walk", "drive"}:
        raise RiskScriptError("INVALID_MODE", "mode must be either walk or drive", {"mode": mode})
    return normalized


def _parse_crime_types(raw_value: str) -> List[str]:
    """
    Parse a comma-separated crime type list into a clean list.

    Example:
      "Burglary, Robbery, Vehicle crime" -> ["Burglary", "Robbery", "Vehicle crime"]
    """
    values: List[str] = []
    seen = set()
    for token in (raw_value or "").split(","):
        item = token.strip()
        if not item or item in seen:
            continue
        values.append(item)
        seen.add(item)
    return values


def _safe_float(value: Any) -> float:
    return float(value or 0.0)


def _score_from_raw(raw_score: float) -> int:
    """
    Convert raw score to 0-100 using a smooth saturation curve.
    This avoids unstable linear scaling across wildly different geographies.
    """
    # Log compression prevents very large raw values from collapsing to 100 too quickly.
    raw_non_negative = max(raw_score, 0.0)
    compressed = math.log1p(min(raw_non_negative, RAW_SCORE_MAX_FOR_SCALING))
    bounded = 100.0 * (1.0 - math.exp(-compressed / RAW_SCORE_LOG_DIVISOR))
    bounded = max(0.0, min(100.0, bounded))
    return int(round(bounded))


def _weights_for_mode(mode: str) -> Dict[str, float]:
    if mode == "walk":
        return dict(WALK_WEIGHTS)
    return dict(DRIVE_WEIGHTS)


def _risk_band(score: int) -> str:
    """
    Map numeric score to a compact categorical band.
    This is useful for persisted summaries and quick comparisons.
    """
    if score >= 75:
        return "very_high"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def _canonical_crime_types(values: List[str]) -> List[str]:
    """Return trimmed, deduplicated, stable-sorted crime types."""
    return sorted({(value or "").strip() for value in values if (value or "").strip()})


def _build_signature_key(*, from_value: str, to_value: str, crime_types: List[str], mode: str) -> str:
    """
    Build canonical signature key for same-input cohort comparisons.

    Intentional exclusion:
    - include_collisions (removed from this algorithm path)
    """
    canonical_types = _canonical_crime_types(crime_types)
    payload = f"{SIGNATURE_VERSION}|{from_value}|{to_value}|{mode}|{','.join(canonical_types)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _insert_risk_score_run(
    db,
    *,
    watchlist_id: Optional[int],
    reference_bbox_id: Optional[int],
    bbox: Dict[str, float],
    from_date,
    to_date,
    crime_types: List[str],
    mode: str,
    signature_key: str,
    risk_score: int,
    band: str,
    raw_score: float,
    crime_component: float,
    collision_component: float,
    user_component: float,
    execution_time_ms: float,
    comparison_basis: Optional[str],
    comparison_sample_size: Optional[int],
    comparison_percentile: Optional[float],
):
    """Persist one scored run for future cohort and reference comparisons."""
    query = text(
        """
        INSERT INTO risk_score_runs (
            watchlist_id,
            reference_bbox_id,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            start_month,
            end_month,
            crime_types,
            travel_mode,
            signature_key,
            risk_score,
            band,
            raw_score,
            crime_component,
            collision_component,
            user_component,
            execution_time_ms,
            comparison_basis,
            comparison_sample_size,
            comparison_percentile
        ) VALUES (
            :watchlist_id,
            :reference_bbox_id,
            :min_lon,
            :min_lat,
            :max_lon,
            :max_lat,
            :start_month,
            :end_month,
            :crime_types,
            :travel_mode,
            :signature_key,
            :risk_score,
            :band,
            :raw_score,
            :crime_component,
            :collision_component,
            :user_component,
            :execution_time_ms,
            :comparison_basis,
            :comparison_sample_size,
            :comparison_percentile
        )
        RETURNING id
        """
    )
    params = {
        "watchlist_id": watchlist_id,
        "reference_bbox_id": reference_bbox_id,
        "min_lon": bbox["min_lon"],
        "min_lat": bbox["min_lat"],
        "max_lon": bbox["max_lon"],
        "max_lat": bbox["max_lat"],
        "start_month": from_date,
        "end_month": to_date,
        "crime_types": _canonical_crime_types(crime_types),
        "travel_mode": mode,
        "signature_key": signature_key,
        "risk_score": risk_score,
        "band": band,
        "raw_score": raw_score,
        "crime_component": crime_component,
        "collision_component": collision_component,
        "user_component": user_component,
        "execution_time_ms": execution_time_ms,
        "comparison_basis": comparison_basis,
        "comparison_sample_size": comparison_sample_size,
        "comparison_percentile": comparison_percentile,
    }
    return _execute(db, query, params).mappings().first()


def _percentile_against(scores: List[int], value: int) -> Optional[float]:
    """Return percentile rank in [0, 100] for value against scores."""
    if not scores:
        return None
    at_or_below = sum(1 for score in scores if score <= value)
    return round((at_or_below / len(scores)) * 100.0, 2)


def _nearest_reference_bboxes(db, *, bbox: Dict[str, float], limit: int = REFERENCE_BBOX_COUNT) -> List[Dict[str, Any]]:
    """Pick nearest active reference bboxes by center-point distance."""
    center_lon = (bbox["min_lon"] + bbox["max_lon"]) / 2.0
    center_lat = (bbox["min_lat"] + bbox["max_lat"]) / 2.0
    query = text(
        """
        SELECT
            id,
            label,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            (
                POWER(((min_lon + max_lon) / 2.0) - :center_lon, 2)
                + POWER(((min_lat + max_lat) / 2.0) - :center_lat, 2)
            ) AS distance_sq
        FROM risk_score_reference_bboxes
        WHERE active = TRUE
        ORDER BY distance_sq ASC, id ASC
        LIMIT :limit
        """
    )
    rows = _execute(
        db,
        query,
        {"center_lon": center_lon, "center_lat": center_lat, "limit": limit},
    ).mappings().all()
    return [dict(row) for row in rows]


def _latest_reference_score(
    db,
    *,
    reference_bbox_id: int,
    signature_key: str,
) -> Optional[Dict[str, Any]]:
    """Return latest cached reference score for one reference bbox + signature."""
    query = text(
        """
        SELECT id, risk_score, created_at
        FROM risk_score_runs
        WHERE reference_bbox_id = :reference_bbox_id
          AND signature_key = :signature_key
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    return _execute(
        db,
        query,
        {"reference_bbox_id": reference_bbox_id, "signature_key": signature_key},
    ).mappings().first()


def _rank_against(scores: List[int], value: int) -> Optional[int]:
    """
    Rank where 1 means highest risk.
    We rank the current score against the comparison cohort.
    """
    if not scores:
        return None
    return 1 + sum(1 for score in scores if score > value)


def _distribution(scores: List[int]) -> Dict[str, Optional[float]]:
    """Return compact cohort distribution stats for proof output."""
    if not scores:
        return {"min": None, "median": None, "max": None}
    return {
        "min": float(min(scores)),
        "median": float(statistics.median(scores)),
        "max": float(max(scores)),
    }


def _load_historical_rows(db, *, signature_key: str, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Load same-signature historical run rows (excluding reference bbox runs).
    This gives both scores and row metadata so we can emit evidence.
    """
    query = text(
        """
        SELECT id, risk_score, created_at
        FROM risk_score_runs
        WHERE signature_key = :signature_key
          AND reference_bbox_id IS NULL
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    rows = _execute(db, query, {"signature_key": signature_key, "limit": limit}).mappings().all()
    return [dict(row) for row in rows]



def _execute(db, query, params: Optional[Dict[str, Any]] = None):
    """
    Execute SQL with consistent error conversion.
    We keep this local to the script to avoid app-level analytics dependencies.
    """
    try:
        return db.execute(query, params or {})
    except (OperationalError, InternalError, ProgrammingError, SATimeoutError) as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        raise RiskScriptError(
            "DB_UNAVAILABLE",
            "Database unavailable while computing risk score",
            {"db_error": str(exc)},
        ) from exc


def run_experimental_risk_score(
    db,
    *,
    from_value: str,
    to_value: str,
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    crime_types: Optional[List[str]] = None,
    mode: str = "walk",
) -> Dict[str, Any]:
    """
    Core algorithm implementation, kept inside this script per your request.

    High-level flow:
    1) Validate input (month window, bbox, mode)
    2) Compute exposure terms (area, road length)
    3) Compute crime/collision/user-report signals
    4) Apply mode-biased weights
    5) Convert to final 0-100 score
    """
    month_window = _validate_month_window(from_value, to_value)
    bbox = _validate_bbox(min_lon, min_lat, max_lon, max_lat)
    normalized_mode = _normalize_mode(mode)

    # Normalize crime-type filter (dedupe + trim).
    normalized_crime_types: List[str] = []
    seen = set()
    for crime_type in crime_types or []:
        token = (crime_type or "").strip()
        if not token or token in seen:
            continue
        normalized_crime_types.append(token)
        seen.add(token)

    crime_filter_clause = ""
    user_crime_filter_clause = ""
    params: Dict[str, Any] = {
        **bbox,
        "from_date": month_window["from_date"],
        "to_date": month_window["to_date"],
        "crime_decay_lambda": CRIME_DECAY_LAMBDA,
        "collision_decay_lambda": COLLISION_DECAY_LAMBDA,
        "user_report_decay_lambda": USER_REPORT_DECAY_LAMBDA,
        "crime_alpha": CRIME_PERSISTENCE_ALPHA,
        "road_km_floor": ROAD_KM_FLOOR,
        "user_report_cluster_cap": USER_REPORT_CLUSTER_CAP,
        "user_report_distinct_auth_weight": USER_REPORT_DISTINCT_AUTH_WEIGHT,
        "user_report_anonymous_weight": USER_REPORT_ANONYMOUS_WEIGHT,
        "user_report_repeat_weight": USER_REPORT_REPEAT_WEIGHT,
        "user_crime_source_weight": USER_CRIME_SOURCE_WEIGHT,
        "user_collision_source_weight": USER_COLLISION_SOURCE_WEIGHT,
        "harm_violence_and_sexual_offences": CRIME_HARM_WEIGHTS["Violence and sexual offences"],
        "harm_robbery": CRIME_HARM_WEIGHTS["Robbery"],
        "harm_burglary": CRIME_HARM_WEIGHTS["Burglary"],
        "harm_vehicle_crime": CRIME_HARM_WEIGHTS["Vehicle crime"],
        "harm_criminal_damage_and_arson": CRIME_HARM_WEIGHTS["Criminal damage and arson"],
        "harm_default": DEFAULT_CRIME_HARM_WEIGHT,
    }
    if normalized_crime_types:
        crime_filter_clause = "AND smts.crime_type = ANY(:crime_types)"
        user_crime_filter_clause = "AND urc.crime_type = ANY(:crime_types)"
        params["crime_types"] = normalized_crime_types

    # SQL uses CTEs to keep each algorithm phase readable.
    query = text(
        f"""
        WITH bbox AS (
            SELECT ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326) AS geom_4326
        ),
        scoring_window AS (
            SELECT
                CAST(:from_date AS date) AS from_date,
                CAST(:to_date AS date) AS to_date,
                (
                    (EXTRACT(YEAR FROM AGE(CAST(:to_date AS date), CAST(:from_date AS date))) * 12)
                    + EXTRACT(MONTH FROM AGE(CAST(:to_date AS date), CAST(:from_date AS date)))
                    + 1
                )::numeric AS window_months
        ),
        exposure AS (
            SELECT
                MAX(ST_Area(ST_Transform(b.geom_4326, 3857))) / 1000000.0 AS area_km2,
                COALESCE(
                    SUM(CASE WHEN rs.length_m > 0 THEN rs.length_m ELSE 0 END) / 1000.0,
                    0.0
                ) AS road_km
            FROM bbox b
            LEFT JOIN road_segments rs
              ON rs.geom_4326 && b.geom_4326
             AND ST_Intersects(rs.geom_4326, b.geom_4326)
        ),
        crime_monthly AS (
            SELECT
                smts.month,
                SUM(
                    smts.crime_count::numeric *
                    CASE smts.crime_type
                        WHEN 'Violence and sexual offences' THEN :harm_violence_and_sexual_offences
                        WHEN 'Robbery' THEN :harm_robbery
                        WHEN 'Burglary' THEN :harm_burglary
                        WHEN 'Vehicle crime' THEN :harm_vehicle_crime
                        WHEN 'Criminal damage and arson' THEN :harm_criminal_damage_and_arson
                        ELSE :harm_default
                    END
                ) AS harm_points,
                SUM(smts.crime_count)::numeric AS total_crime_count
            FROM segment_month_type_stats smts
            JOIN road_segments rs ON rs.id = smts.segment_id
            CROSS JOIN bbox b
            CROSS JOIN scoring_window sw
            WHERE smts.month BETWEEN sw.from_date AND sw.to_date
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
              {crime_filter_clause}
            GROUP BY smts.month
        ),
        crime_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        EXP(
                            -:crime_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.harm_points
                    ),
                    0.0
                ) AS weighted_harm,
                COALESCE(COUNT(*) FILTER (WHERE cm.total_crime_count > 0), 0)::numeric AS active_months
            FROM crime_monthly cm
            CROSS JOIN scoring_window sw
        ),
        collision_monthly AS (
            SELECT
                smcs.month,
                SUM(
                    COALESCE(smcs.collision_count, 0)
                    + (0.5 * COALESCE(smcs.slight_casualty_count, 0))
                    + (2.0 * COALESCE(smcs.serious_casualty_count, 0))
                    + (5.0 * COALESCE(smcs.fatal_casualty_count, 0))
                )::numeric AS collision_points
            FROM segment_month_collision_stats smcs
            JOIN road_segments rs ON rs.id = smcs.segment_id
            CROSS JOIN bbox b
            CROSS JOIN scoring_window sw
            WHERE smcs.month BETWEEN sw.from_date AND sw.to_date
              AND rs.geom_4326 && b.geom_4326
              AND ST_Intersects(rs.geom_4326, b.geom_4326)
            GROUP BY smcs.month
        ),
        collision_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        EXP(
                            -:collision_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, cm.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, cm.month))
                                )::numeric,
                                0
                            )
                        ) * cm.collision_points
                    ),
                    0.0
                ) AS weighted_collision_points
            FROM collision_monthly cm
            CROSS JOIN scoring_window sw
        ),
        user_events_crime AS (
            SELECT
                'crime'::text AS event_kind,
                ure.month,
                COALESCE(
                    ure.segment_id::text,
                    CONCAT(
                        ROUND(ure.latitude::numeric, 3)::text,
                        ':',
                        ROUND(ure.longitude::numeric, 3)::text
                    )
                ) AS cluster_key,
                ure.reporter_type,
                ure.user_id
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            CROSS JOIN scoring_window sw
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.event_kind = 'crime'
              AND ure.month BETWEEN sw.from_date AND sw.to_date
              AND ure.geom && b.geom_4326
              AND ST_Intersects(ure.geom, b.geom_4326)
              {user_crime_filter_clause}
        ),
        user_events_collision AS (
            SELECT
                'collision'::text AS event_kind,
                ure.month,
                COALESCE(
                    ure.segment_id::text,
                    CONCAT(
                        ROUND(ure.latitude::numeric, 3)::text,
                        ':',
                        ROUND(ure.longitude::numeric, 3)::text
                    )
                ) AS cluster_key,
                ure.reporter_type,
                ure.user_id
            FROM user_reported_events ure
            CROSS JOIN scoring_window sw
            CROSS JOIN bbox b
            WHERE ure.admin_approved = TRUE
              AND ure.event_kind = 'collision'
              AND ure.month BETWEEN sw.from_date AND sw.to_date
              AND ure.geom && b.geom_4326
              AND ST_Intersects(ure.geom, b.geom_4326)
        ),
        user_events AS (
            SELECT * FROM user_events_crime
            UNION ALL
            SELECT * FROM user_events_collision
        ),
        user_clusters AS (
            SELECT
                ue.event_kind,
                ue.month,
                ue.cluster_key,
                COUNT(*) FILTER (WHERE ue.reporter_type = 'anonymous')::numeric AS anonymous_reports,
                COUNT(*) FILTER (WHERE ue.reporter_type = 'authenticated')::numeric AS authenticated_reports,
                COUNT(DISTINCT ue.user_id) FILTER (
                    WHERE ue.reporter_type = 'authenticated'
                )::numeric AS distinct_authenticated_users
            FROM user_events ue
            GROUP BY ue.event_kind, ue.month, ue.cluster_key
        ),
        user_cluster_scores AS (
            SELECT
                uc.event_kind,
                uc.month,
                LEAST(
                    :user_report_cluster_cap,
                    (:user_report_distinct_auth_weight * uc.distinct_authenticated_users)
                    + (:user_report_anonymous_weight * uc.anonymous_reports)
                    + (
                        :user_report_repeat_weight
                        * GREATEST(uc.authenticated_reports - uc.distinct_authenticated_users, 0)
                    )
                ) AS cluster_signal
            FROM user_clusters uc
        ),
        user_summary AS (
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN ucs.event_kind = 'crime' THEN :user_crime_source_weight
                            ELSE :user_collision_source_weight
                        END
                        * EXP(
                            -:user_report_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, ucs.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, ucs.month))
                                )::numeric,
                                0
                            )
                        )
                        * ucs.cluster_signal
                    ) FILTER (WHERE ucs.event_kind = 'crime'),
                    0.0
                ) AS user_crime_signal,
                COALESCE(
                    SUM(
                        CASE
                            WHEN ucs.event_kind = 'crime' THEN :user_crime_source_weight
                            ELSE :user_collision_source_weight
                        END
                        * EXP(
                            -:user_report_decay_lambda * GREATEST(
                                (
                                    (EXTRACT(YEAR FROM AGE(sw.to_date, ucs.month)) * 12)
                                    + EXTRACT(MONTH FROM AGE(sw.to_date, ucs.month))
                                )::numeric,
                                0
                            )
                        )
                        * ucs.cluster_signal
                    ) FILTER (WHERE ucs.event_kind = 'collision'),
                    0.0
                ) AS user_collision_signal
            FROM user_cluster_scores ucs
            CROSS JOIN scoring_window sw
        )
        SELECT
            ex.area_km2,
            ex.road_km,
            GREATEST(ex.road_km, :road_km_floor) AS effective_road_km,
            (
                (:crime_alpha * COALESCE(cs.weighted_harm / NULLIF(ex.area_km2, 0), 0))
                + (
                    (1.0 - :crime_alpha)
                    * COALESCE(cs.active_months / NULLIF(sw.window_months, 0), 0)
                )
            ) AS crime_component,
            COALESCE(cols.weighted_collision_points / NULLIF(GREATEST(ex.road_km, :road_km_floor), 0), 0)
                AS collision_density,
            COALESCE(us.user_crime_signal / NULLIF(ex.area_km2, 0), 0) AS user_crime_density,
            COALESCE(us.user_collision_signal / NULLIF(GREATEST(ex.road_km, :road_km_floor), 0), 0)
                AS user_collision_density
        FROM exposure ex
        CROSS JOIN scoring_window sw
        CROSS JOIN crime_summary cs
        CROSS JOIN collision_summary cols
        CROSS JOIN user_summary us
        """
    )
    row = _execute(db, query, params).mappings().first() or {}

    # Build final weighted components.
    crime_component = _safe_float(row.get("crime_component"))
    collision_density = _safe_float(row.get("collision_density"))
    user_crime_density = _safe_float(row.get("user_crime_density"))
    user_collision_density = _safe_float(row.get("user_collision_density"))
    user_support = user_crime_density + user_collision_density

    weights = _weights_for_mode(normalized_mode)
    raw_score = (
        weights["w_crime"] * crime_component
        + weights["w_collision"] * collision_density
        + weights["w_user"] * user_support
    )
    risk_score = _score_from_raw(raw_score)

    return {
        "risk_score": risk_score,
        "raw_score": raw_score,
        "components": {
            "crime_component": crime_component,
            "collision_density": collision_density,
            "user_support": user_support,
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    """
    Build CLI argument parser.

    Inputs intentionally match your endpoint-intent contract:
    - bbox (min/max lon/lat)
    - time range (from/to month)
    - crime types array
    - mode (walk/drive)
    """
    parser = argparse.ArgumentParser(
        description="Run the experimental risk-score algorithm for one bbox and month window."
    )
    parser.add_argument("--from", dest="from_month", required=True, help="Start month in YYYY-MM format.")
    parser.add_argument("--to", dest="to_month", required=True, help="End month in YYYY-MM format.")
    parser.add_argument("--min-lon", required=True, type=float, help="Minimum longitude of bbox.")
    parser.add_argument("--min-lat", required=True, type=float, help="Minimum latitude of bbox.")
    parser.add_argument("--max-lon", required=True, type=float, help="Maximum longitude of bbox.")
    parser.add_argument("--max-lat", required=True, type=float, help="Maximum latitude of bbox.")
    parser.add_argument(
        "--crime-types",
        default="",
        help="Comma-separated crime types. Leave empty to include all types.",
    )
    parser.add_argument(
        "--mode",
        default="walk",
        help="Travel mode. Valid values: walk, drive.",
    )
    parser.add_argument(
        "--watchlist-id",
        type=int,
        default=None,
        help="Optional watchlist id to attach to risk_score_runs persistence rows.",
    )
    return parser


def main() -> int:
    """
    Execute one risk-score run and print:
    - final score
    - execution time in milliseconds

    We measure end-to-end runtime from just before DB/session usage to final result,
    so this gives a realistic lightweight performance indicator for this script path.
    """
    parser = _build_parser()
    args = parser.parse_args()

    # Parse and normalize CLI inputs.
    crime_types = _parse_crime_types(args.crime_types)
    normalized_mode = _normalize_mode(args.mode)
    bbox = _validate_bbox(args.min_lon, args.min_lat, args.max_lon, args.max_lat)
    month_window = _validate_month_window(args.from_month, args.to_month)

    # Start timing immediately before DB work for realistic end-to-end latency.
    started_at = perf_counter()
    db = SessionLocal()

    try:
        # Compute current bbox score.
        result = run_experimental_risk_score(
            db,
            from_value=args.from_month,
            to_value=args.to_month,
            min_lon=args.min_lon,
            min_lat=args.min_lat,
            max_lon=args.max_lon,
            max_lat=args.max_lat,
            crime_types=crime_types,
            mode=normalized_mode,
        )

        signature_key = _build_signature_key(
            from_value=args.from_month,
            to_value=args.to_month,
            crime_types=crime_types,
            mode=normalized_mode,
        )

        # First attempt: compare with historical same-signature runs.
        historical_rows = _load_historical_rows(db, signature_key=signature_key)
        historical_scores = [int(row["risk_score"]) for row in historical_rows]
        current_score = int(result["risk_score"])
        comparison_basis = "none"
        comparison_percentile = None
        comparison_sample_size = 0
        comparison_rank = None
        comparison_distribution = {"min": None, "median": None, "max": None}
        evidence_rows: List[Dict[str, Any]] = []
        reference_debug: List[Dict[str, Any]] = []

        if len(historical_scores) >= COMPARISON_MIN_COHORT:
            comparison_basis = "historical_same_signature"
            comparison_sample_size = len(historical_scores)
            comparison_percentile = _percentile_against(historical_scores, current_score)
            comparison_rank = _rank_against(historical_scores, current_score)
            comparison_distribution = _distribution(historical_scores)
            evidence_rows = [
                {
                    "run_id": int(row["id"]),
                    "score": int(row["risk_score"]),
                    "source": "historical",
                    "created_at": row["created_at"],
                }
                for row in historical_rows[:3]
            ]
        else:
            # Fallback: pick nearest static reference bboxes, reuse cached scores
            # where possible, otherwise recalculate and persist those reference runs.
            reference_boxes = _nearest_reference_bboxes(db, bbox=bbox, limit=REFERENCE_BBOX_COUNT)
            reference_scores: List[int] = []
            for ref in reference_boxes:
                cached = _latest_reference_score(
                    db,
                    reference_bbox_id=int(ref["id"]),
                    signature_key=signature_key,
                )
                if cached:
                    score = int(cached["risk_score"])
                    reference_scores.append(score)
                    reference_debug.append(
                        {
                            "reference_bbox_id": int(ref["id"]),
                            "label": ref["label"],
                            "source": "cache",
                            "run_id": int(cached["id"]),
                            "risk_score": score,
                            "created_at": cached["created_at"],
                        }
                    )
                    continue

                ref_result = run_experimental_risk_score(
                    db,
                    from_value=args.from_month,
                    to_value=args.to_month,
                    min_lon=float(ref["min_lon"]),
                    min_lat=float(ref["min_lat"]),
                    max_lon=float(ref["max_lon"]),
                    max_lat=float(ref["max_lat"]),
                    crime_types=crime_types,
                    mode=normalized_mode,
                )
                score = int(ref_result["risk_score"])
                reference_scores.append(score)
                inserted_ref = _insert_risk_score_run(
                    db,
                    watchlist_id=None,
                    reference_bbox_id=int(ref["id"]),
                    bbox={
                        "min_lon": float(ref["min_lon"]),
                        "min_lat": float(ref["min_lat"]),
                        "max_lon": float(ref["max_lon"]),
                        "max_lat": float(ref["max_lat"]),
                    },
                    from_date=month_window["from_date"],
                    to_date=month_window["to_date"],
                    crime_types=crime_types,
                    mode=normalized_mode,
                    signature_key=signature_key,
                    risk_score=score,
                    band=_risk_band(score),
                    raw_score=float(ref_result["raw_score"]),
                    crime_component=float(ref_result["components"]["crime_component"]),
                    collision_component=float(ref_result["components"]["collision_density"]),
                    user_component=float(ref_result["components"]["user_support"]),
                    execution_time_ms=0.0,
                    comparison_basis="none",
                    comparison_sample_size=0,
                    comparison_percentile=None,
                )
                reference_debug.append(
                    {
                        "reference_bbox_id": int(ref["id"]),
                        "label": ref["label"],
                        "source": "recalculated",
                        "run_id": int(inserted_ref["id"]) if inserted_ref else None,
                        "risk_score": score,
                        "created_at": None,
                    }
                )

            if reference_scores:
                comparison_basis = "reference_bboxes"
                comparison_sample_size = len(reference_scores)
                comparison_percentile = _percentile_against(reference_scores, current_score)
                comparison_rank = _rank_against(reference_scores, current_score)
                comparison_distribution = _distribution(reference_scores)
                evidence_rows = [
                    {
                        "run_id": row["run_id"],
                        "score": row["risk_score"],
                        "source": f"reference_{row['source']}",
                        "created_at": row["created_at"],
                        "reference_bbox_id": row["reference_bbox_id"],
                        "reference_label": row["label"],
                    }
                    for row in reference_debug[:3]
                ]

        elapsed_ms = (perf_counter() - started_at) * 1000.0

        # Persist the current run with resolved comparison metadata.
        inserted = _insert_risk_score_run(
            db,
            watchlist_id=args.watchlist_id,
            reference_bbox_id=None,
            bbox=bbox,
            from_date=month_window["from_date"],
            to_date=month_window["to_date"],
            crime_types=crime_types,
            mode=normalized_mode,
            signature_key=signature_key,
            risk_score=int(result["risk_score"]),
            band=_risk_band(int(result["risk_score"])),
            raw_score=float(result["raw_score"]),
            crime_component=float(result["components"]["crime_component"]),
            collision_component=float(result["components"]["collision_density"]),
            user_component=float(result["components"]["user_support"]),
            execution_time_ms=elapsed_ms,
            comparison_basis=comparison_basis,
            comparison_sample_size=comparison_sample_size,
            comparison_percentile=comparison_percentile,
        )

        db.commit()

        # Return a compact payload shape for the script consumer.
        reference_ids = [
            int(row["run_id"])
            for row in reference_debug
            if row.get("run_id") is not None
        ]
        response_payload = {
            "watchlist_id": args.watchlist_id,
            "risk_result": result,
            "comparison": {
                "cohort_type": comparison_basis,
                "cohort_size": comparison_sample_size,
                "subject_score": current_score,
                "rank": comparison_rank,
                "rank_out_of": comparison_sample_size if comparison_sample_size > 0 else None,
                "percentile": comparison_percentile,
                "distribution": comparison_distribution,
                "sample_size": comparison_sample_size,
                "historical_count": len(historical_scores),
                "threshold": COMPARISON_MIN_COHORT,
                "reference_ids": reference_ids,
            },
        }
        print(json.dumps(response_payload, indent=2, default=str))
        return 0
    except RiskScriptError as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        print(f"error={exc.code}")
        print(f"message={exc.message}")
        if exc.details:
            print(f"details={exc.details}")
        return 1
    except Exception as exc:
        if hasattr(db, "rollback"):
            db.rollback()
        print("error=UNEXPECTED_FAILURE")
        print(f"message={exc}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
