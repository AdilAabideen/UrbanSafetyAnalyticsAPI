from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi.responses import JSONResponse


MAX_MONTH_SPAN = 24
USER_REPORTED_CRIME_WEIGHT = 0.10
ANONYMOUS_USER_REPORT_WEIGHT = 0.5
REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25
USER_REPORTED_SIGNAL_CAP = 3.0


class AnalyticsAPIError(Exception):
    """Represents an API-safe analytics failure with HTTP status and structured metadata."""

    def __init__(self, status_code: int, error: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message
        self.details = details


def error_response(error: AnalyticsAPIError) -> JSONResponse:
    """Convert an analytics domain exception into an HTTP JSON response body."""
    payload = {"error": error.error, "message": error.message}
    if error.details is not None:
        payload["details"] = error.details
    return JSONResponse(status_code=error.status_code, content=payload)


def _generated_at() -> str:
    """Return an RFC3339 UTC timestamp used on analytics responses."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_string(value: Optional[str]) -> Optional[str]:
    """Trim optional string inputs and normalize empty strings to None."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def _month_label(month_date) -> str:
    """Render a date-like value to YYYY-MM for analytics payloads."""
    return month_date.strftime("%Y-%m")


def _shift_month(month_date, offset: int):
    """Shift a month date by N months while preserving the first day of month."""
    month_index = (month_date.year * 12 + month_date.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return month_date.replace(year=year, month=month, day=1)


def _month_span(start_date, end_date) -> int:
    """Compute inclusive month span between two month-aligned dates."""
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1


def _parse_month(value: str, parameter_name: str):
    """Parse YYYY-MM input and raise a typed API error on invalid format."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise AnalyticsAPIError(
            400,
            "INVALID_MONTH_FORMAT",
            f"{parameter_name} must be in YYYY-MM format",
        ) from exc


def _validate_month_window(from_value: str, to_value: str):
    """Validate from/to month window and enforce the maximum month span policy."""
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
    """Validate forecast target/baseline configuration and derive baseline boundaries."""
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
    """Validate and normalize required bounding box coordinates."""
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
    """Validate optional bbox inputs when all four coordinates are supplied."""
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
    """Round rate-like metrics to three decimals."""
    return round(float(value or 0.0), 3)


def _round_pct(value: Any) -> float:
    """Round percentile-like metrics to four decimals."""
    return round(float(value or 0.0), 4)


def _safe_div(numerator: float, denominator: float) -> float:
    """Guard division by zero in metric derivations."""
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _scope_payload(
    month_window: Optional[Dict[str, Any]] = None,
    bbox: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the shared scope object for analytics responses."""
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


def _band_from_pct(pct: float) -> str:
    """Map percentile score to risk color band."""
    if pct >= 0.95:
        return "red"
    if pct >= 0.80:
        return "amber"
    return "green"


def _applied_collision_weight(include_collisions: bool, mode: str, requested_weight: float) -> float:
    """Resolve effective collision weighting based on mode and feature flag."""
    if not include_collisions or mode != "drive":
        return 0.0
    return requested_weight if requested_weight != 0 else 1.0


def _band_interpretation(pct: float, noun: str) -> str:
    """Generate a human-readable interpretation line for percentile banding."""
    if pct >= 0.95:
        return f"This {noun} sits in roughly the top 5% of observed risk for the selected period."
    if pct >= 0.80:
        return f"This {noun} sits above the wider network average and falls into the upper 20% of observed risk."
    return f"This {noun} sits below the upper-risk bands for the selected period."


def _user_report_weight_params() -> Dict[str, float]:
    """Return bind parameters for user-reported crime signal weighting."""
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
    """Build the shared SQL fragment for weighted user-reported crime signal."""
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
