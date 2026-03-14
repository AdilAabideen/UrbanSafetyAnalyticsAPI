from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class WatchlistRiskComponents(BaseModel):
    """Internal component breakdown used to build the final risk score."""

    crime_component: float = Field(
        ...,
        description="Crime signal after harm weighting, recency decay, and persistence blending.",
        example=4.0244,
    )
    collision_density: float = Field(
        ...,
        description="Collision signal normalized by effective road exposure for the selected area.",
        example=0.35,
    )
    user_support: float = Field(
        ...,
        description="Combined lightweight user-reported support signal (crime + collision reports).",
        example=0.12,
    )


class WatchlistRiskResult(BaseModel):
    """Core risk result values for this watchlist execution."""

    risk_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="Final normalized risk score in the range 0 to 100.",
        example=63,
    )
    raw_score: float = Field(
        ...,
        description="Pre-normalized algorithm score before conversion to the 0-100 risk scale.",
        example=1.6098,
    )
    components: WatchlistRiskComponents = Field(
        ...,
        description="Algorithm component values used to build raw_score.",
    )


class WatchlistComparisonDistribution(BaseModel):
    """Distribution summary of compared cohort scores."""

    min: Optional[float] = Field(
        default=None,
        description="Minimum risk score found in the comparison cohort.",
        example=50.0,
    )
    median: Optional[float] = Field(
        default=None,
        description="Median risk score for the comparison cohort.",
        example=67.5,
    )
    max: Optional[float] = Field(
        default=None,
        description="Maximum risk score found in the comparison cohort.",
        example=92.0,
    )


class WatchlistComparisonSummary(BaseModel):
    """Condensed proof that the score was compared against other runs."""

    cohort_type: str = Field(
        ...,
        description="Comparison source used: historical_same_signature, reference_bboxes, or none.",
        example="reference_bboxes",
    )
    cohort_size: int = Field(
        ...,
        ge=0,
        description="Number of rows used for the final comparison.",
        example=2,
    )
    subject_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="The current watchlist run's risk score being compared.",
        example=63,
    )
    rank: Optional[int] = Field(
        default=None,
        ge=1,
        description="Rank of subject_score within cohort (1 = highest risk).",
        example=3,
    )
    rank_out_of: Optional[int] = Field(
        default=None,
        ge=1,
        description="Total cohort size used for rank display.",
        example=2,
    )
    percentile: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Percentile position of subject_score within the comparison cohort.",
        example=0.0,
    )
    distribution: WatchlistComparisonDistribution = Field(
        ...,
        description="Simple distribution stats of the comparison cohort.",
    )
    sample_size: int = Field(
        ...,
        ge=0,
        description="Alias of cohort_size retained for compatibility.",
        example=2,
    )
    historical_count: int = Field(
        ...,
        ge=0,
        description="Count of matching historical rows found before fallback decisions.",
        example=1,
    )
    threshold: int = Field(
        ...,
        ge=1,
        description="Minimum historical cohort size required before fallback to reference bboxes.",
        example=2,
    )
    reference_ids: List[int] = Field(
        default_factory=list,
        description="Run IDs used when comparison cohort is built from reference bboxes.",
        example=[9, 5],
    )


class WatchlistRiskScoreResponse(BaseModel):
    """API response for watchlist risk-score execution."""

    watchlist_id: Optional[int] = Field(
        default=None,
        description="Watchlist identifier used for this analytics run.",
        example=42,
    )
    risk_result: WatchlistRiskResult = Field(
        ...,
        description="Computed risk score and component breakdown.",
    )
    comparison: WatchlistComparisonSummary = Field(
        ...,
        description="Condensed comparison proof block.",
    )

    class Config:
        schema_extra = {
            "example": {
                "watchlist_id": 42,
                "risk_result": {
                    "risk_score": 63,
                    "raw_score": 1.6097716609710755,
                    "components": {
                        "crime_component": 4.024429152427689,
                        "collision_density": 0.0,
                        "user_support": 0.0,
                    },
                },
                "comparison": {
                    "cohort_type": "reference_bboxes",
                    "cohort_size": 2,
                    "subject_score": 63,
                    "rank": 3,
                    "rank_out_of": 2,
                    "percentile": 0.0,
                    "distribution": {
                        "min": 85.0,
                        "median": 92.5,
                        "max": 100.0,
                    },
                    "sample_size": 2,
                    "historical_count": 1,
                    "threshold": 2,
                    "reference_ids": [9, 5],
                },
            }
        }


class WatchlistRiskRunItem(BaseModel):
    """One persisted risk analytics run."""

    run_id: int = Field(..., description="Persisted risk run identifier.", example=128)
    created_at: datetime = Field(..., description="Run creation timestamp (UTC).")
    start_month: date = Field(..., description="Run month window start.", example="2024-01-01")
    end_month: date = Field(..., description="Run month window end.", example="2024-03-01")
    crime_types: List[str] = Field(
        default_factory=list,
        description="Crime types filter used in the run; empty means all crimes.",
        example=["Robbery", "Burglary"],
    )
    travel_mode: str = Field(..., description="Mode used by the run: walk or drive.", example="drive")
    band: str = Field(..., description="Risk band derived from risk_score.", example="high")
    risk_result: WatchlistRiskResult = Field(..., description="Risk score and component values for this run.")
    comparison_basis: Optional[str] = Field(
        default=None,
        description="Comparison source used for this run.",
        example="historical_same_signature",
    )
    comparison_sample_size: Optional[int] = Field(
        default=None,
        description="Number of comparison rows used for this run.",
        example=5,
    )
    comparison_percentile: Optional[float] = Field(
        default=None,
        description="Subject percentile against the comparison cohort.",
        example=82.4,
    )
    execution_time_ms: float = Field(..., description="Execution time for the run in milliseconds.", example=412.6)


class WatchlistRiskRunsResponse(BaseModel):
    """List of previous persisted analytics risk runs for a watchlist."""

    watchlist_id: int = Field(..., description="Watchlist identifier.", example=42)
    items: List[WatchlistRiskRunItem] = Field(default_factory=list, description="Most recent runs first.")
