import hashlib
import math
import statistics
from time import perf_counter
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..api_utils import watchlist_analytics_repository
from ..errors import NotFoundError, ValidationError


CRIME_HARM_WEIGHTS: Dict[str, float] = {
    "Violence and sexual offences": 3.0,
    "Robbery": 2.5,
    "Burglary": 2.0,
    "Vehicle crime": 1.5,
    "Criminal damage and arson": 1.2,
}
DEFAULT_CRIME_HARM_WEIGHT = 1.0

CRIME_DECAY_LAMBDA = 0.1625
COLLISION_DECAY_LAMBDA = 0.08
USER_REPORT_DECAY_LAMBDA = 0.51

USER_REPORT_CLUSTER_CAP = 3.0
USER_REPORT_DISTINCT_AUTH_WEIGHT = 1.0
USER_REPORT_ANONYMOUS_WEIGHT = 0.5
USER_REPORT_REPEAT_WEIGHT = 0.25
USER_CRIME_SOURCE_WEIGHT = 0.10
USER_COLLISION_SOURCE_WEIGHT = 0.08

WALK_WEIGHTS = {"w_crime": 0.65, "w_collision": 0.25, "w_user": 0.10}
DRIVE_WEIGHTS = {"w_crime": 0.40, "w_collision": 0.50, "w_user": 0.10}

CRIME_PERSISTENCE_ALPHA = 0.8
ROAD_KM_FLOOR = 0.25
RAW_SCORE_SATURATION = 1.6

COMPARISON_MIN_COHORT = 2
REFERENCE_BBOX_COUNT = 2


def _safe_float(value: Any) -> float:
    """Convert the value to a float or return 0.0 if the value is None."""
    return float(value or 0.0)


def _canonical_crime_types(values: List[str]) -> List[str]:
    """Canonicalize the crime types."""
    return sorted({(value or "").strip() for value in values if (value or "").strip()})


def _normalize_mode(mode: Optional[str]) -> str:
    """Normalize the mode."""
    normalized = (mode or "").strip().lower()
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
        raise ValidationError(
            error="INVALID_TRAVEL_MODE",
            message="travel_mode must be walk or drive",
            details={"field": "travel_mode", "value": mode},
        )
    # Return the canonical mode.
    return canonical


def _validate_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float):
    """Validate the bounding box."""
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValidationError(
            error="INVALID_BBOX",
            message="min_lon must be less than max_lon and min_lat must be less than max_lat",
            details={
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            },
        )
    # Return the validated bounding box.


def _weights_for_mode(mode: str) -> Dict[str, float]:
    if mode == "walk":
        # Return the walk weights.
        return dict(WALK_WEIGHTS)
    # Return the drive weights.
    return dict(DRIVE_WEIGHTS)


def _score_from_raw(raw_score: float) -> int:
    """Compute the score from the raw score."""
    bounded = 100.0 * (1.0 - math.exp(-max(raw_score, 0.0) / RAW_SCORE_SATURATION))
    bounded = max(0.0, min(100.0, bounded))
    return int(round(bounded))


def _risk_band(score: int) -> str:
    """Compute the risk band from the score."""
    if score >= 75:
        return "very_high"
    if score >= 50:
        return "high"
    if score >= 25:
        return "medium"
    return "low"


def _build_signature_key(*, from_value: str, to_value: str, crime_types: List[str], mode: str) -> str:
    """Build the signature key."""
    canonical_types = _canonical_crime_types(crime_types)
    payload = f"{from_value}|{to_value}|{mode}|{','.join(canonical_types)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _percentile_against(scores: List[int], value: int) -> Optional[float]:
    """Compute the percentile against the scores."""
    if not scores:
        return None
    at_or_below = sum(1 for score in scores if score <= value)
    return round((at_or_below / len(scores)) * 100.0, 2)


def _rank_against(scores: List[int], value: int) -> Optional[int]:
    """Compute the rank against the scores."""
    if not scores:
        return None
    return 1 + sum(1 for score in scores if score > value)


def _distribution(scores: List[int]) -> Dict[str, Optional[float]]:
    """Compute the distribution of the scores."""
    if not scores:
        return {"min": None, "median": None, "max": None}
    return {
        "min": float(min(scores)),
        "median": float(statistics.median(scores)),
        "max": float(max(scores)),
    }


def _compute_risk_result(
    db: Session,
    *,
    from_date,
    to_date,
    bbox: Dict[str, float],
    crime_types: List[str],
    mode: str,
) -> Dict[str, Any]:
    """Compute the risk result."""

    # Compute the risk components.
    row = watchlist_analytics_repository.compute_risk_components(
        db,
        from_date=from_date,
        to_date=to_date,
        min_lon=bbox["min_lon"],
        min_lat=bbox["min_lat"],
        max_lon=bbox["max_lon"],
        max_lat=bbox["max_lat"],
        crime_types=_canonical_crime_types(crime_types),
        crime_decay_lambda=CRIME_DECAY_LAMBDA,
        collision_decay_lambda=COLLISION_DECAY_LAMBDA,
        user_report_decay_lambda=USER_REPORT_DECAY_LAMBDA,
        crime_alpha=CRIME_PERSISTENCE_ALPHA,
        road_km_floor=ROAD_KM_FLOOR,
        user_report_cluster_cap=USER_REPORT_CLUSTER_CAP,
        user_report_distinct_auth_weight=USER_REPORT_DISTINCT_AUTH_WEIGHT,
        user_report_anonymous_weight=USER_REPORT_ANONYMOUS_WEIGHT,
        user_report_repeat_weight=USER_REPORT_REPEAT_WEIGHT,
        user_crime_source_weight=USER_CRIME_SOURCE_WEIGHT,
        user_collision_source_weight=USER_COLLISION_SOURCE_WEIGHT,
        harm_violence_and_sexual_offences=CRIME_HARM_WEIGHTS["Violence and sexual offences"],
        harm_robbery=CRIME_HARM_WEIGHTS["Robbery"],
        harm_burglary=CRIME_HARM_WEIGHTS["Burglary"],
        harm_vehicle_crime=CRIME_HARM_WEIGHTS["Vehicle crime"],
        harm_criminal_damage_and_arson=CRIME_HARM_WEIGHTS["Criminal damage and arson"],
        harm_default=DEFAULT_CRIME_HARM_WEIGHT,
    )

    # Extract the risk components.
    crime_component = _safe_float(row.get("crime_component"))
    collision_density = _safe_float(row.get("collision_density"))
    user_crime_density = _safe_float(row.get("user_crime_density"))
    user_collision_density = _safe_float(row.get("user_collision_density"))
    user_support = user_crime_density + user_collision_density

    # Compute the raw score.
    weights = _weights_for_mode(mode)
    raw_score = (
        weights["w_crime"] * crime_component
        + weights["w_collision"] * collision_density
        + weights["w_user"] * user_support
    )
    # Compute the risk score.
    risk_score = _score_from_raw(raw_score)

    # Return the risk result.
    return {
        "risk_score": risk_score,
        "raw_score": raw_score,
        "components": {
            "crime_component": crime_component,
            "collision_density": collision_density,
            "user_support": user_support,
        },
    }


def build_watchlist_risk_score_service(
    db: Session,
    *,
    user_id: int,
    watchlist_id: int,
):
    """
    Compute + persist a watchlist risk score and return condensed comparison output.
    """
    # Get the watchlist row.
    row = watchlist_analytics_repository.get_watchlist_for_analytics(
        db,
        watchlist_id=watchlist_id,
        user_id=user_id,
    )
    if not row:
        raise NotFoundError(error="WATCHLIST_NOT_FOUND", message="Watchlist not found")

    # Extract the start and end months.
    start_month = row["start_month"]
    end_month = row["end_month"]
    if start_month is None or end_month is None:
        raise ValidationError(
            error="MISSING_MONTH_WINDOW",
            message="watchlist must have start_month and end_month before running analytics",
            details={"watchlist_id": watchlist_id},
        )
    if start_month > end_month:
        raise ValidationError(
            error="INVALID_MONTH_RANGE",
            message="start_month must be less than or equal to end_month",
            details={"watchlist_id": watchlist_id},
        )

    # Extract the bounding box.
    bbox = {
        "min_lon": float(row["min_lon"]),
        "min_lat": float(row["min_lat"]),
        "max_lon": float(row["max_lon"]),
        "max_lat": float(row["max_lat"]),
    }
    _validate_bbox(**bbox)

    # Normalize the mode.
    mode = _normalize_mode(row.get("travel_mode"))
    crime_types = _canonical_crime_types(list(row.get("crime_types") or []))

    # Build the signature key.
    from_value = start_month.strftime("%Y-%m")
    to_value = end_month.strftime("%Y-%m")
    signature_key = _build_signature_key(
        from_value=from_value,
        to_value=to_value,
        crime_types=crime_types,
        mode=mode,
    )

    # Compute the risk result.
    started_at = perf_counter()
    try:
        # Compute the risk result.
        risk_result = _compute_risk_result(
            db,
            from_date=start_month,
            to_date=end_month,
            bbox=bbox,
            crime_types=crime_types,
            mode=mode,
        )

        # Extract the current score.
        current_score = int(risk_result["risk_score"])

        # Load the historical rows.
        historical_rows = watchlist_analytics_repository.load_historical_rows(
            db,
            signature_key=signature_key,
        )
        historical_scores = [int(item["risk_score"]) for item in historical_rows]

        # Initialize the comparison basis.
        comparison_basis = "none"
        comparison_percentile = None
        comparison_sample_size = 0
        comparison_rank = None
        # Initialize the comparison distribution.
        comparison_distribution = {"min": None, "median": None, "max": None}
        # Initialize the reference IDs.
        reference_ids: List[int] = []

        # Check if the historical scores are sufficient for comparison.
        if len(historical_scores) >= COMPARISON_MIN_COHORT:
            # Set the comparison basis to historical same-signature.
            comparison_basis = "historical_same_signature"
            comparison_sample_size = len(historical_scores)
            comparison_percentile = _percentile_against(historical_scores, current_score)
            comparison_rank = _rank_against(historical_scores, current_score)
            comparison_distribution = _distribution(historical_scores)
        else:
            # Load the nearest reference bboxes.
            reference_rows = watchlist_analytics_repository.nearest_reference_bboxes(
                db,
                bbox=bbox,
                limit=REFERENCE_BBOX_COUNT,
            )
            # Initialize the reference scores.
            reference_scores: List[int] = []
            # Iterate over the reference rows.
            for reference in reference_rows:
                # Check if the reference score is cached, if so, use the cached score.
                cached = watchlist_analytics_repository.latest_reference_score(
                    db,
                    reference_bbox_id=int(reference["id"]),
                    signature_key=signature_key,
                )
                if cached:
                    # Add the cached score to the reference scores.
                    reference_scores.append(int(cached["risk_score"]))
                    reference_ids.append(int(cached["id"]))
                    continue

                # Compute the risk result for the reference bbox.
                ref_bbox = {
                    "min_lon": float(reference["min_lon"]),
                    "min_lat": float(reference["min_lat"]),
                    "max_lon": float(reference["max_lon"]),
                    "max_lat": float(reference["max_lat"]),
                }
                # Compute the risk result for the reference bbox.
                ref_result = _compute_risk_result(
                    db,
                    from_date=start_month,
                    to_date=end_month,
                    bbox=ref_bbox,
                    crime_types=crime_types,
                    mode=mode,
                )
                ref_score = int(ref_result["risk_score"])
                reference_scores.append(ref_score)
                # Insert the reference score run.
                inserted_ref = watchlist_analytics_repository.insert_risk_score_run(
                    db,
                    watchlist_id=None,
                    reference_bbox_id=int(reference["id"]),
                    bbox=ref_bbox,
                    start_month=start_month,
                    end_month=end_month,
                    crime_types=crime_types,
                    travel_mode=mode,
                    signature_key=signature_key,
                    risk_score=ref_score,
                    band=_risk_band(ref_score),
                    raw_score=float(ref_result["raw_score"]),
                    crime_component=float(ref_result["components"]["crime_component"]),
                    collision_component=float(ref_result["components"]["collision_density"]),
                    user_component=float(ref_result["components"]["user_support"]),
                    execution_time_ms=0.0,
                    comparison_basis="none",
                    comparison_sample_size=0,
                    comparison_percentile=None,
                )
                if inserted_ref:
                    reference_ids.append(int(inserted_ref["id"]))

            # Check if the reference scores are sufficient for comparison.
            if reference_scores:
                # Set the comparison basis to reference bboxes.
                comparison_basis = "reference_bboxes"
                comparison_sample_size = len(reference_scores)
                comparison_percentile = _percentile_against(reference_scores, current_score)
                comparison_rank = _rank_against(reference_scores, current_score)
                comparison_distribution = _distribution(reference_scores)

        elapsed_ms = (perf_counter() - started_at) * 1000.0
        # Insert the current score run.
        watchlist_analytics_repository.insert_risk_score_run(
            db,
            watchlist_id=watchlist_id,
            reference_bbox_id=None,
            bbox=bbox,
            start_month=start_month,
            end_month=end_month,
            crime_types=crime_types,
            travel_mode=mode,
            signature_key=signature_key,
            risk_score=current_score,
            band=_risk_band(current_score),
            raw_score=float(risk_result["raw_score"]),
            crime_component=float(risk_result["components"]["crime_component"]),
            collision_component=float(risk_result["components"]["collision_density"]),
            user_component=float(risk_result["components"]["user_support"]),
            execution_time_ms=elapsed_ms,
            comparison_basis=comparison_basis,
            comparison_sample_size=comparison_sample_size,
            comparison_percentile=comparison_percentile,
        )
        # Commit the transaction.
        if hasattr(db, "commit"):
            db.commit()
    except Exception:
        if hasattr(db, "rollback"):
            db.rollback()
        raise

    return {
        "watchlist_id": watchlist_id,
        "risk_result": risk_result,
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
