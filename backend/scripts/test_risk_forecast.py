#!/usr/bin/env python3
"""
Standalone recency-weighted next-month forecast runner.

Why this script exists:
- You want an experimental forecasting path before wiring endpoint changes.
- You want watchlist-scoped forecasting with optional crime-type filtering.
- You want output to be JSON and avoid persistence for now.
"""

import argparse
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError, ProgrammingError, TimeoutError as SATimeoutError


# Make `app.*` imports work when running this script directly from repository root.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import SessionLocal  # noqa: E402


# ----------------------------- Algorithm constants -----------------------------
RECENCY_LAMBDA = 0.85
USER_REPORT_SCALE = 0.10
USER_REPORT_CAP = 3.0

COLLISION_SLIGHT_WEIGHT = 0.5
COLLISION_SERIOUS_WEIGHT = 2.0
COLLISION_FATAL_WEIGHT = 5.0

RAW_SCORE_SATURATION = 1.6

WALK_WEIGHTS = {"w_crime": 0.65, "w_collision": 0.35}
DRIVE_WEIGHTS = {"w_crime": 0.40, "w_collision": 0.60}


class ForecastScriptError(Exception):
    """Simple script-local typed error."""

    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def _parse_month(value: str, field_name: str) -> date:
    """Parse YYYY-MM into a month date (first day of month)."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise ForecastScriptError(
            "INVALID_MONTH_FORMAT",
            f"{field_name} must be in YYYY-MM format",
            {"field": field_name, "value": value},
        ) from exc


def _to_month_start(value: date) -> date:
    """Normalize a date to first day of its month."""
    return date(value.year, value.month, 1)


def _shift_month(value: date, delta_months: int) -> date:
    """Shift a month-start date by delta months."""
    month_index = (value.year * 12 + value.month - 1) + delta_months
    year = month_index // 12
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _month_span(start_date: date, end_date: date) -> int:
    """Return inclusive number of months between two month-start dates."""
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1


def _last_complete_month(today: Optional[date] = None) -> date:
    """Return most recent fully-complete month as month-start."""
    now_date = today or date.today()
    current_month_start = date(now_date.year, now_date.month, 1)
    return _shift_month(current_month_start, -1)


def _validate_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> Dict[str, float]:
    """Validate and return bbox parameters."""
    if min_lon >= max_lon:
        raise ForecastScriptError(
            "INVALID_BBOX",
            "min_lon must be less than max_lon",
            {"min_lon": min_lon, "max_lon": max_lon},
        )
    if min_lat >= max_lat:
        raise ForecastScriptError(
            "INVALID_BBOX",
            "min_lat must be less than max_lat",
            {"min_lat": min_lat, "max_lat": max_lat},
        )
    return {
        "min_lon": float(min_lon),
        "min_lat": float(min_lat),
        "max_lon": float(max_lon),
        "max_lat": float(max_lat),
    }


def _normalize_mode(value: str) -> str:
    """Normalize mode aliases to walk/drive."""
    normalized = (value or "").strip().lower()
    aliases = {
        "walk": "walk",
        "walking": "walk",
        "foot": "walk",
        "pedestrian": "walk",
        "drive": "drive",
        "driving": "drive",
        "car": "drive",
        "vehicle": "drive",
    }
    canonical = aliases.get(normalized)
    if canonical is None:
        raise ForecastScriptError("INVALID_MODE", "mode must be walk or drive", {"mode": value})
    return canonical


def _weights_for_mode(mode: str) -> Dict[str, float]:
    """Return mode-biased component weights."""
    if mode == "walk":
        return dict(WALK_WEIGHTS)
    return dict(DRIVE_WEIGHTS)


def _parse_crime_types(raw_value: str) -> List[str]:
    """Parse comma-separated crime types into a distinct ordered list."""
    result: List[str] = []
    seen = set()
    for token in (raw_value or "").split(","):
        value = token.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _score_from_raw(raw_score: float) -> int:
    """Convert raw projected value to 0-100 score using smooth saturation."""
    bounded = 100.0 * (1.0 - math.exp(-max(raw_score, 0.0) / RAW_SCORE_SATURATION))
    return int(round(max(0.0, min(100.0, bounded))))


def _projection_band_from_score(score: int) -> str:
    """
    Stricter score-to-band mapping.
    This is intentionally more conservative so elevated scores are not labeled green.
    """
    if score >= 60:
        return "red"
    if score >= 35:
        return "amber"
    return "green"


def _poisson_interval(mu: float) -> Dict[str, int]:
    """Compute simple normal-approx Poisson interval around mean."""
    sigma = 1.96 * math.sqrt(max(mu, 1e-9))
    low = int(math.floor(max(0.0, mu - sigma)))
    high = int(math.ceil(mu + sigma))
    return {"low": low, "high": high}


def _round_float(value: float, digits: int = 4) -> float:
    return round(float(value), digits)


def _weighted_mean(values: Sequence[float], lambda_decay: float = RECENCY_LAMBDA) -> float:
    """
    Compute weighted mean where most recent item gets weight 1.
    Expects values ordered oldest -> newest.
    """
    if not values:
        return 0.0
    weighted_sum = 0.0
    weight_total = 0.0
    for index, value in enumerate(reversed(values)):
        weight = lambda_decay**index
        weighted_sum += weight * float(value)
        weight_total += weight
    if weight_total <= 0:
        return 0.0
    return weighted_sum / weight_total


def _get_watchlist_bbox(db, watchlist_id: int) -> Dict[str, Any]:
    """Load watchlist bbox and basic metadata."""
    row = (
        db.execute(
            text(
                """
                SELECT
                    id,
                    user_id,
                    name,
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat
                FROM watchlists
                WHERE id = :watchlist_id
                LIMIT 1
                """
            ),
            {"watchlist_id": watchlist_id},
        )
        .mappings()
        .first()
    )
    if not row:
        raise ForecastScriptError(
            "WATCHLIST_NOT_FOUND",
            "Watchlist not found",
            {"watchlist_id": watchlist_id},
        )
    bbox = _validate_bbox(
        float(row["min_lon"]),
        float(row["min_lat"]),
        float(row["max_lon"]),
        float(row["max_lat"]),
    )
    return {
        "watchlist_id": int(row["id"]),
        "watchlist_user_id": int(row["user_id"]),
        "watchlist_name": row["name"],
        **bbox,
    }


def _monthly_history(
    db,
    *,
    baseline_from_date: date,
    baseline_to_date: date,
    bbox: Dict[str, float],
    crime_types: List[str],
) -> List[Dict[str, Any]]:
    """Build monthly crime/user/collision baseline rows for one bbox."""
    crime_type_clause = ""
    user_crime_type_clause = ""
    params: Dict[str, Any] = {
        "baseline_from_date": baseline_from_date,
        "baseline_to_date": baseline_to_date,
        **bbox,
    }
    if crime_types:
        crime_type_clause = "AND ce.crime_type = ANY(:crime_types)"
        user_crime_type_clause = "AND urc.crime_type = ANY(:crime_types)"
        params["crime_types"] = crime_types

    query = text(
        f"""
        /* script_recency_weighted_forecast_history */
        WITH months AS (
            SELECT generate_series(
                CAST(:baseline_from_date AS date),
                CAST(:baseline_to_date AS date),
                interval '1 month'
            )::date AS month
        ),
        official_crime AS (
            SELECT
                ce.month,
                COUNT(*)::double precision AS official_crime_count
            FROM crime_events ce
            WHERE ce.geom IS NOT NULL
              AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ce.lon BETWEEN :min_lon AND :max_lon
              AND ce.lat BETWEEN :min_lat AND :max_lat
              AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              {crime_type_clause}
            GROUP BY ce.month
        ),
        user_crime_monthly AS (
            SELECT
                ure.month,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'anonymous')::double precision AS anonymous_reports,
                COUNT(*) FILTER (WHERE ure.reporter_type = 'authenticated')::double precision AS authenticated_reports,
                COUNT(DISTINCT ure.user_id) FILTER (
                    WHERE ure.reporter_type = 'authenticated'
                )::double precision AS distinct_authenticated_users
            FROM user_reported_events ure
            JOIN user_reported_crime_details urc ON urc.event_id = ure.id
            WHERE ure.event_kind = 'crime'
              AND ure.admin_approved = TRUE
              AND ure.geom IS NOT NULL
              AND ure.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ure.longitude BETWEEN :min_lon AND :max_lon
              AND ure.latitude BETWEEN :min_lat AND :max_lat
              AND ure.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
              {user_crime_type_clause}
            GROUP BY ure.month
        ),
        user_crime_signal AS (
            SELECT
                month,
                (
                    {USER_REPORT_SCALE}
                    * LEAST(
                        {USER_REPORT_CAP},
                        distinct_authenticated_users
                        + (0.5 * anonymous_reports)
                        + (
                            0.25 * GREATEST(authenticated_reports - distinct_authenticated_users, 0)
                        )
                    )
                )::double precision AS user_reported_crime_signal,
                (COALESCE(authenticated_reports, 0) + COALESCE(anonymous_reports, 0))::bigint
                    AS approved_user_reports
            FROM user_crime_monthly
        ),
        collision_monthly AS (
            SELECT
                ce.month,
                COUNT(*)::bigint AS collision_count,
                COALESCE(
                    SUM(
                        1.0
                        + ({COLLISION_SLIGHT_WEIGHT} * COALESCE(ce.slight_casualty_count, 0))
                        + ({COLLISION_SERIOUS_WEIGHT} * COALESCE(ce.serious_casualty_count, 0))
                        + ({COLLISION_FATAL_WEIGHT} * COALESCE(ce.fatal_casualty_count, 0))
                    ),
                    0.0
                )::double precision AS collision_points
            FROM collision_events ce
            WHERE ce.geom IS NOT NULL
              AND ce.month BETWEEN :baseline_from_date AND :baseline_to_date
              AND ce.longitude BETWEEN :min_lon AND :max_lon
              AND ce.latitude BETWEEN :min_lat AND :max_lat
              AND ce.geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            GROUP BY ce.month
        )
        SELECT
            TO_CHAR(months.month, 'YYYY-MM') AS month,
            COALESCE(official_crime.official_crime_count, 0.0) AS official_crime_count,
            COALESCE(user_crime_signal.user_reported_crime_signal, 0.0) AS user_reported_crime_signal,
            COALESCE(user_crime_signal.approved_user_reports, 0)::bigint AS approved_user_reports,
            (
                COALESCE(official_crime.official_crime_count, 0.0)
                + COALESCE(user_crime_signal.user_reported_crime_signal, 0.0)
            ) AS crime_count,
            COALESCE(collision_monthly.collision_count, 0)::bigint AS collision_count,
            COALESCE(collision_monthly.collision_points, 0.0) AS collision_points
        FROM months
        LEFT JOIN official_crime ON official_crime.month = months.month
        LEFT JOIN user_crime_signal ON user_crime_signal.month = months.month
        LEFT JOIN collision_monthly ON collision_monthly.month = months.month
        ORDER BY months.month ASC
        """
    )
    rows = db.execute(query, params).mappings().all()
    history: List[Dict[str, Any]] = []
    for row in rows:
        history.append(
            {
                "month": row["month"],
                "official_crime_count": int(round(float(row["official_crime_count"] or 0.0))),
                "approved_user_reports": int(row["approved_user_reports"] or 0),
                "user_reported_crime_signal": _round_float(row["user_reported_crime_signal"] or 0.0),
                "crime_count": _round_float(row["crime_count"] or 0.0),
                "collision_count": int(row["collision_count"] or 0),
                "collision_points": _round_float(row["collision_points"] or 0.0),
            }
        )
    return history


def run_recency_weighted_forecast(
    db,
    *,
    watchlist_id: int,
    start_month: str,
    mode: str,
    crime_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Run recency-weighted forecast for the next month.

    Forecast target month is always one month after the last complete month.
    """
    start_month_date = _to_month_start(_parse_month(start_month, "start_month"))
    baseline_to_date = _last_complete_month()
    if start_month_date > baseline_to_date:
        raise ForecastScriptError(
            "INVALID_DATE_RANGE",
            "start_month must be less than or equal to the last complete month",
            {
                "start_month": start_month_date.strftime("%Y-%m"),
                "last_complete_month": baseline_to_date.strftime("%Y-%m"),
            },
        )
    span = _month_span(start_month_date, baseline_to_date)
    if span <= 0:
        raise ForecastScriptError(
            "INVALID_DATE_RANGE",
            "month range from start_month to last complete month must be > 0",
            {
                "start_month": start_month_date.strftime("%Y-%m"),
                "last_complete_month": baseline_to_date.strftime("%Y-%m"),
            },
        )

    normalized_mode = _normalize_mode(mode)
    normalized_crime_types = _parse_crime_types(",".join(crime_types or [])) if crime_types else []
    weights = _weights_for_mode(normalized_mode)
    watchlist = _get_watchlist_bbox(db, watchlist_id)
    bbox = _validate_bbox(
        watchlist["min_lon"],
        watchlist["min_lat"],
        watchlist["max_lon"],
        watchlist["max_lat"],
    )

    history = _monthly_history(
        db,
        baseline_from_date=start_month_date,
        baseline_to_date=baseline_to_date,
        bbox=bbox,
        crime_types=normalized_crime_types,
    )
    if not history:
        raise ForecastScriptError(
            "BASELINE_HISTORY_INSUFFICIENT",
            "No baseline months were returned for this watchlist and month range",
            {"watchlist_id": watchlist_id},
        )

    crime_values = [float(item["crime_count"]) for item in history]
    collision_points_values = [float(item["collision_points"]) for item in history]
    collision_count_values = [float(item["collision_count"]) for item in history]

    mu_c = _weighted_mean(crime_values, RECENCY_LAMBDA)
    mu_k = _weighted_mean(collision_points_values, RECENCY_LAMBDA)
    mu_k_count = _weighted_mean(collision_count_values, RECENCY_LAMBDA)

    expected_crime_count = int(round(mu_c))
    expected_collision_count = int(round(mu_k_count))
    expected_collision_points = _round_float(mu_k)

    combined_history = [
        (weights["w_crime"] * crime_value) + (weights["w_collision"] * collision_points_value)
        for crime_value, collision_points_value in zip(crime_values, collision_points_values)
    ]
    mu_r = _weighted_mean(combined_history, RECENCY_LAMBDA)
    projected_r = (weights["w_crime"] * mu_c) + (weights["w_collision"] * mu_k)
    ratio = projected_r / max(mu_r, 1e-9)
    ratio = _round_float(ratio)

    score = _score_from_raw(projected_r)
    band = _projection_band_from_score(score)

    crime_interval = _poisson_interval(mu_c)
    collision_count_interval = _poisson_interval(mu_k_count)

    response = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "forecast": {
            "score": score,
            "band": band,
            "expected_crime_count": expected_crime_count,
            "expected_collision_count": expected_collision_count,
            "expected_collision_points": expected_collision_points,
            "intervals": {
                "crimes": crime_interval,
                "collisions_count": collision_count_interval,
            },
            "components": {
                "mu_crime": _round_float(mu_c),
                "mu_collision_points": _round_float(mu_k),
                "mu_collision_count": _round_float(mu_k_count),
                "projected_combined_value": _round_float(projected_r),
                "baseline_combined_mean": _round_float(mu_r),
                "ratio": ratio,
            },
        },
    }
    return response


def main():
    parser = argparse.ArgumentParser(
        description="Run recency-weighted next-month forecast for one watchlist.",
    )
    parser.add_argument("--watchlist-id", required=True, type=int, help="Watchlist id for bbox scope.")
    parser.add_argument("--start-month", required=True, help="Baseline start month in YYYY-MM.")
    parser.add_argument(
        "--crime-types",
        default="",
        help="Optional comma-separated crime types. Leave empty for all crime types.",
    )
    parser.add_argument(
        "--mode",
        default="walk",
        help="Travel mode emphasis: walk or drive (aliases accepted).",
    )
    args = parser.parse_args()

    crime_types = _parse_crime_types(args.crime_types)
    try:
        with SessionLocal() as db:
            payload = run_recency_weighted_forecast(
                db,
                watchlist_id=args.watchlist_id,
                start_month=args.start_month,
                mode=args.mode,
                crime_types=crime_types,
            )
            print(json.dumps(payload, indent=2, default=str))
    except (OperationalError, SATimeoutError, InternalError) as exc:
        error_payload = {
            "error": "DB_UNAVAILABLE",
            "message": "Database is unavailable for forecast execution",
            "details": {"exception": exc.__class__.__name__},
        }
        print(json.dumps(error_payload, indent=2))
        raise SystemExit(2) from exc
    except ProgrammingError as exc:
        error_payload = {
            "error": "QUERY_ERROR",
            "message": "Forecast query failed",
            "details": {"exception": exc.__class__.__name__},
        }
        print(json.dumps(error_payload, indent=2))
        raise SystemExit(3) from exc
    except ForecastScriptError as exc:
        error_payload = {
            "error": exc.code,
            "message": exc.message,
            "details": exc.details,
        }
        print(json.dumps(error_payload, indent=2))
        raise SystemExit(4) from exc


if __name__ == "__main__":
    main()
