#!/usr/bin/env python3
"""
Backtest the watchlist forecast model with a rolling-origin evaluation.

This script is intentionally lightweight and reuses existing forecast logic pieces
from the service/repository layer so results stay consistent with API behavior.

Example:
  python backend/scripts/backtest_forecast.py --watchlist-id 2
  python backend/scripts/backtest_forecast.py --watchlist-id 2 --min-baseline-months 4 --show-rows
  python backend/scripts/backtest_forecast.py --watchlist-id 2 --output-csv /tmp/backtest.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# Ensure imports work when running as:
#   python backend/scripts/backtest_forecast.py
BACKEND_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from sqlalchemy import text

from app.api_utils import watchlist_analytics_repository
from app.db import SessionLocal, execute
from app.services.watchlist_analytics_service import (
    _canonical_crime_types,
    _normalize_mode,
    _poisson_interval,
    _score_from_raw,
    _weighted_mean,
    _weights_for_mode,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest the watchlist forecast model.")
    parser.add_argument("--watchlist-id", type=int, required=True, help="Watchlist ID to backtest.")
    parser.add_argument(
        "--user-id",
        type=int,
        default=None,
        help="Optional owner user ID filter. If omitted, watchlist is loaded by ID only.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default=None,
        help="Optional mode override (walk/drive). Defaults to watchlist travel_mode.",
    )
    parser.add_argument(
        "--min-baseline-months",
        type=int,
        default=3,
        help="Minimum history months required before each forecasted target month.",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Optional path to write per-window backtest rows as CSV.",
    )
    parser.add_argument(
        "--show-rows",
        action="store_true",
        help="Print per-window rows to console.",
    )
    return parser.parse_args()


def month_start(value: Any) -> date:
    if isinstance(value, date):
        return value.replace(day=1)
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    raise ValueError(f"Unsupported month value: {value!r}")


def month_token(value: date) -> str:
    return value.strftime("%Y-%m")


def safe_mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def mae(errors: List[float]) -> float:
    if not errors:
        return 0.0
    return safe_mean([abs(item) for item in errors])


def rmse(errors: List[float]) -> float:
    if not errors:
        return 0.0
    return math.sqrt(safe_mean([item * item for item in errors]))


def bias(errors: List[float]) -> float:
    if not errors:
        return 0.0
    return safe_mean(errors)


def fetch_watchlist(db, watchlist_id: int, user_id: Optional[int]) -> Optional[Dict[str, Any]]:
    where_clause = "w.id = :watchlist_id"
    params: Dict[str, Any] = {"watchlist_id": watchlist_id}
    if user_id is not None:
        where_clause += " AND w.user_id = :user_id"
        params["user_id"] = user_id

    query = text(
        f"""
        SELECT
            w.id,
            w.user_id,
            w.name,
            w.min_lon,
            w.min_lat,
            w.max_lon,
            w.max_lat,
            w.start_month,
            w.end_month,
            w.crime_types,
            w.travel_mode
        FROM watchlists w
        WHERE {where_clause}
        LIMIT 1
        """
    )
    row = execute(db, query, params).mappings().first()
    return dict(row) if row else None


def build_model_prediction(history_rows: List[Dict[str, Any]], weights: Dict[str, float]) -> Dict[str, Any]:
    crime_values = [float(item["crime_count"]) for item in history_rows]
    collision_points_values = [float(item["collision_points"]) for item in history_rows]
    collision_count_values = [float(item["collision_count"]) for item in history_rows]

    mu_crime = _weighted_mean(crime_values)
    mu_collision_points = _weighted_mean(collision_points_values)
    mu_collision_count = _weighted_mean(collision_count_values)

    projected_combined_value = (weights["w_crime"] * mu_crime) + (weights["w_collision"] * mu_collision_points)
    projected_score = _score_from_raw(projected_combined_value)

    return {
        "pred_score": int(projected_score),
        "pred_crime_count": int(round(mu_crime)),
        "pred_collision_count": int(round(mu_collision_count)),
        "pred_collision_points": float(mu_collision_points),
        "crime_interval": _poisson_interval(mu_crime),
        "collision_count_interval": _poisson_interval(mu_collision_count),
    }


def build_last_month_baseline(history_rows: List[Dict[str, Any]], weights: Dict[str, float]) -> Dict[str, Any]:
    last = history_rows[-1]
    projected_combined_value = (weights["w_crime"] * float(last["crime_count"])) + (
        weights["w_collision"] * float(last["collision_points"])
    )
    return {"pred_score": int(_score_from_raw(projected_combined_value))}


def build_mean_baseline(history_rows: List[Dict[str, Any]], weights: Dict[str, float]) -> Dict[str, Any]:
    mean_crime = safe_mean([float(item["crime_count"]) for item in history_rows])
    mean_collision_points = safe_mean([float(item["collision_points"]) for item in history_rows])
    projected_combined_value = (weights["w_crime"] * mean_crime) + (weights["w_collision"] * mean_collision_points)
    return {"pred_score": int(_score_from_raw(projected_combined_value))}


def print_summary(rows: List[Dict[str, Any]], elapsed_ms: float, watchlist: Dict[str, Any], mode: str) -> None:
    model_score_errors = [float(item["pred_score"] - item["actual_score"]) for item in rows]
    last_score_errors = [float(item["pred_score_last"] - item["actual_score"]) for item in rows]
    mean_score_errors = [float(item["pred_score_mean"] - item["actual_score"]) for item in rows]

    model_crime_errors = [float(item["pred_crime_count"] - item["actual_crime_count"]) for item in rows]
    model_collision_errors = [float(item["pred_collision_count"] - item["actual_collision_count"]) for item in rows]

    crime_covered = sum(
        1
        for item in rows
        if int(item["crime_interval_low"]) <= int(item["actual_crime_count"]) <= int(item["crime_interval_high"])
    )
    collision_covered = sum(
        1
        for item in rows
        if int(item["collision_interval_low"])
        <= int(item["actual_collision_count"])
        <= int(item["collision_interval_high"])
    )

    n = len(rows)
    crime_coverage_pct = (crime_covered / n * 100.0) if n else 0.0
    collision_coverage_pct = (collision_covered / n * 100.0) if n else 0.0

    print("\n=== Forecast Backtest Summary ===")
    print(f"watchlist_id: {watchlist['id']}")
    print(f"watchlist_name: {watchlist.get('name')}")
    print(f"owner_user_id: {watchlist.get('user_id')}")
    print(f"mode_used: {mode}")
    print(f"window: {month_token(watchlist['start_month'])} -> {month_token(watchlist['end_month'])}")
    print(f"evaluated_forecasts: {n}")
    print(f"execution_time_ms: {elapsed_ms:.2f}")

    print("\nModel vs Actual")
    print(f"- score_mae: {mae(model_score_errors):.4f}")
    print(f"- score_rmse: {rmse(model_score_errors):.4f}")
    print(f"- score_bias: {bias(model_score_errors):.4f}")
    print(f"- crime_count_mae: {mae(model_crime_errors):.4f}")
    print(f"- crime_count_rmse: {rmse(model_crime_errors):.4f}")
    print(f"- collision_count_mae: {mae(model_collision_errors):.4f}")
    print(f"- collision_count_rmse: {rmse(model_collision_errors):.4f}")
    print(f"- crime_interval_coverage_pct: {crime_coverage_pct:.2f}")
    print(f"- collision_interval_coverage_pct: {collision_coverage_pct:.2f}")

    print("\nScore Baseline Comparison (lower MAE is better)")
    print(f"- current_model_score_mae: {mae(model_score_errors):.4f}")
    print(f"- naive_last_month_score_mae: {mae(last_score_errors):.4f}")
    print(f"- trailing_mean_score_mae: {mae(mean_score_errors):.4f}")


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "cutoff_month",
        "target_month",
        "history_months",
        "pred_score",
        "actual_score",
        "pred_score_last",
        "pred_score_mean",
        "pred_crime_count",
        "actual_crime_count",
        "pred_collision_count",
        "actual_collision_count",
        "pred_collision_points",
        "actual_collision_points",
        "crime_interval_low",
        "crime_interval_high",
        "collision_interval_low",
        "collision_interval_high",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    args = parse_args()
    started_at = time.perf_counter()

    if args.min_baseline_months < 1:
        print("ERROR: --min-baseline-months must be >= 1")
        return 1

    db = SessionLocal()
    try:
        try:
            watchlist = fetch_watchlist(db, watchlist_id=args.watchlist_id, user_id=args.user_id)
            if not watchlist:
                print("ERROR: watchlist not found (or not owned by provided --user-id).")
                return 1

            if watchlist.get("start_month") is None or watchlist.get("end_month") is None:
                print("ERROR: watchlist must have start_month and end_month for backtesting.")
                return 1

            start_month = month_start(watchlist["start_month"])
            end_month = month_start(watchlist["end_month"])
            if start_month > end_month:
                print("ERROR: watchlist start_month must be <= end_month.")
                return 1

            mode = _normalize_mode(args.mode if args.mode else watchlist.get("travel_mode"))
            weights = _weights_for_mode(mode)
            crime_types = _canonical_crime_types(list(watchlist.get("crime_types") or []))

            baseline_rows = watchlist_analytics_repository.fetch_forecast_baseline_rows(
                db,
                baseline_from_date=start_month,
                baseline_to_date=end_month,
                min_lon=float(watchlist["min_lon"]),
                min_lat=float(watchlist["min_lat"]),
                max_lon=float(watchlist["max_lon"]),
                max_lat=float(watchlist["max_lat"]),
                crime_types=crime_types,
            )
            monthly_series = [
                {
                    "month": month_start(item["month"]),
                    "crime_count": float(item.get("crime_count") or 0.0),
                    "collision_count": float(item.get("collision_count") or 0.0),
                    "collision_points": float(item.get("collision_points") or 0.0),
                }
                for item in baseline_rows
            ]

            if len(monthly_series) < (args.min_baseline_months + 1):
                print(
                    "ERROR: not enough months for backtest. "
                    f"Need at least {args.min_baseline_months + 1}, found {len(monthly_series)}."
                )
                return 1

            results: List[Dict[str, Any]] = []
            for cutoff_index in range(args.min_baseline_months - 1, len(monthly_series) - 1):
                history = monthly_series[: cutoff_index + 1]
                actual = monthly_series[cutoff_index + 1]

                model = build_model_prediction(history, weights)
                last_baseline = build_last_month_baseline(history, weights)
                mean_baseline = build_mean_baseline(history, weights)

                actual_combined_value = (weights["w_crime"] * float(actual["crime_count"])) + (
                    weights["w_collision"] * float(actual["collision_points"])
                )
                actual_score = _score_from_raw(actual_combined_value)

                results.append(
                    {
                        "cutoff_month": month_token(history[-1]["month"]),
                        "target_month": month_token(actual["month"]),
                        "history_months": len(history),
                        "pred_score": model["pred_score"],
                        "actual_score": int(actual_score),
                        "pred_score_last": last_baseline["pred_score"],
                        "pred_score_mean": mean_baseline["pred_score"],
                        "pred_crime_count": model["pred_crime_count"],
                        "actual_crime_count": int(round(float(actual["crime_count"]))),
                        "pred_collision_count": model["pred_collision_count"],
                        "actual_collision_count": int(round(float(actual["collision_count"]))),
                        "pred_collision_points": round(float(model["pred_collision_points"]), 4),
                        "actual_collision_points": round(float(actual["collision_points"]), 4),
                        "crime_interval_low": model["crime_interval"]["low"],
                        "crime_interval_high": model["crime_interval"]["high"],
                        "collision_interval_low": model["collision_count_interval"]["low"],
                        "collision_interval_high": model["collision_count_interval"]["high"],
                    }
                )

            elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            print_summary(results, elapsed_ms, watchlist, mode)

            if args.show_rows:
                print("\nPer-window rows")
                for row in results:
                    print(
                        f"- cutoff={row['cutoff_month']} target={row['target_month']} "
                        f"pred_score={row['pred_score']} actual_score={row['actual_score']} "
                        f"pred_crime={row['pred_crime_count']} actual_crime={row['actual_crime_count']} "
                        f"pred_collisions={row['pred_collision_count']} actual_collisions={row['actual_collision_count']}"
                    )

            if args.output_csv:
                output_path = Path(args.output_csv).expanduser()
                write_csv(output_path, results)
                print(f"\nCSV written: {output_path}")

            return 0
        except Exception as exc:
            print(f"ERROR: backtest failed: {exc}")
            return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
