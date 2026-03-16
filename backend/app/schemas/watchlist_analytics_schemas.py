from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


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


class WatchlistComparisonSummary(BaseModel):
    """Condensed proof that the score was compared against other runs."""

    cohort_size: int = Field(
        ...,
        ge=0,
        description="Number of rows used for the final comparison.",
        example=2,
    )
    rank: Optional[int] = Field(
        default=None,
        ge=1,
        description="Rank within cohort (1 = highest risk).",
        example=3,
    )
    rank_out_of: Optional[int] = Field(
        default=None,
        ge=1,
        description="Total cohort size used for rank display.",
        example=2,
    )
    reference_ids: List[int] = Field(
        default_factory=list,
        description="Run IDs used when comparison cohort is built from reference bboxes.",
        example=[9, 5],
    )

class WatchlistRiskDataUsed(BaseModel):
    """Counts of source data records used by this run."""

    official_crime_count: int = Field(
        ...,
        ge=0,
        description="Total official crimes included in the selected bbox/month window after crime-type filtering.",
        example=937822,
    )
    collision_count: int = Field(
        ...,
        ge=0,
        description="Total collisions included in the selected bbox/month window.",
        example=714,
    )
    approved_user_report_count: int = Field(
        ...,
        ge=0,
        description="Total approved user-reported events included in the selected bbox/month window.",
        example=25,
    )


class WatchlistRiskWindow(BaseModel):
    """Month-window metadata used by the algorithm."""

    start_month: date = Field(..., description="Window start month.", example="2025-10-01")
    end_month: date = Field(..., description="Window end month.", example="2026-01-01")
    months_in_window: int = Field(
        ...,
        ge=1,
        description="Number of months included in the scoring window (inclusive).",
        example=4,
    )


class WatchlistRiskNormalizationContext(BaseModel):
    """Normalization context values used by component density calculations."""

    area_km2: float = Field(
        ...,
        ge=0,
        description="Area of the selected bbox in square kilometers.",
        example=12.41,
    )
    road_km: float = Field(
        ...,
        ge=0,
        description="Total road length inside the selected bbox in kilometers.",
        example=185.22,
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
    data_used: WatchlistRiskDataUsed = Field(..., description="Counts of source data used by this run.")
    window: WatchlistRiskWindow = Field(..., description="Month-window metadata used by this run.")
    normalization_context: WatchlistRiskNormalizationContext = Field(
        ...,
        description="Normalization context values for this run.",
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
                    "cohort_size": 2,
                    "rank": 3,
                    "rank_out_of": 2,
                    "reference_ids": [9, 5],
                },
                "data_used": {
                    "official_crime_count": 937822,
                    "collision_count": 714,
                    "approved_user_report_count": 25,
                },
                "window": {
                    "start_month": "2025-10-01",
                    "end_month": "2026-01-01",
                    "months_in_window": 4,
                },
                "normalization_context": {
                    "area_km2": 12.41,
                    "road_km": 185.22,
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


class WatchlistDangerousRoadItem(BaseModel):
    """One road segment ranked by danger score within a watchlist window."""

    segment_id: int = Field(..., description="Road segment identifier.", example=145233)
    road_name: str = Field(
        ...,
        description="Road name if available; falls back to 'Unnamed road'.",
        example="Boar Lane",
    )
    danger_score: float = Field(
        ...,
        ge=0,
        description="Lightweight composite score from crime + collisions + approved user reports.",
        example=87.25,
    )
    crime_count: int = Field(..., ge=0, description="Official crimes on this segment in the window.", example=32)
    collision_count: int = Field(..., ge=0, description="Collisions on this segment in the window.", example=3)
    user_reported_event_count: int = Field(
        ...,
        ge=0,
        description="Approved user-reported events on this segment in the window.",
        example=2,
    )


class WatchlistCrimeCategoryItem(BaseModel):
    """Crime count grouped by category for bar-chart style frontend rendering."""

    crime_type: str = Field(..., description="Crime category label.", example="Anti-social behaviour")
    count: int = Field(..., ge=0, description="Total count for this category in the window.", example=418)


class WatchlistBasicMetricsResponse(BaseModel):
    """
    Basic watchlist analytics response with exactly 5 top-level fields.
    """

    number_of_crimes: int = Field(..., ge=0, description="Total official crimes in the watchlist window.", example=1834)
    number_of_collisions: int = Field(..., ge=0, description="Total collisions in the watchlist window.", example=27)
    number_of_user_reported_events: int = Field(
        ...,
        ge=0,
        description="Total approved user-reported events in the watchlist window.",
        example=11,
    )
    most_dangerous_roads: List[WatchlistDangerousRoadItem] = Field(
        default_factory=list,
        description="Top 5 road segments ranked by a lightweight danger score.",
    )
    crime_category_breakdown: List[WatchlistCrimeCategoryItem] = Field(
        default_factory=list,
        description="Crime counts grouped by category for charting.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "number_of_crimes": 1834,
                "number_of_collisions": 27,
                "number_of_user_reported_events": 11,
                "most_dangerous_roads": [
                    {
                        "segment_id": 145233,
                        "road_name": "Boar Lane",
                        "danger_score": 87.25,
                        "crime_count": 32,
                        "collision_count": 3,
                        "user_reported_event_count": 2,
                    }
                ],
                "crime_category_breakdown": [
                    {"crime_type": "Anti-social behaviour", "count": 418},
                    {"crime_type": "Public order", "count": 201},
                ],
            }
        }
    )


class WatchlistMapFeature(BaseModel):
    """Generic GeoJSON feature for watchlist map event layers."""

    type: str = Field(..., description="GeoJSON object type. Always `Feature`.", example="Feature")
    geometry: dict = Field(..., description="GeoJSON geometry object for the event point.")
    properties: dict = Field(..., description="Event attributes for UI rendering.")


class WatchlistMapFeatureCollection(BaseModel):
    """GeoJSON FeatureCollection for one event source."""

    type: str = Field(
        ...,
        description="GeoJSON collection type. Always `FeatureCollection`.",
        example="FeatureCollection",
    )
    features: List[WatchlistMapFeature] = Field(default_factory=list, description="GeoJSON point features.")


class WatchlistMapEventsResponse(BaseModel):
    """Bundled watchlist map events for crimes, collisions, and approved user reports."""

    crimes: WatchlistMapFeatureCollection = Field(
        ...,
        description="Official crime event points within watchlist bbox and month window.",
    )
    collisions: WatchlistMapFeatureCollection = Field(
        ...,
        description="Official collision event points within watchlist bbox and month window.",
    )
    user_reported_events: WatchlistMapFeatureCollection = Field(
        ...,
        description="Approved user-reported event points within watchlist bbox and month window.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "crimes": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-1.5487, 53.7992]},
                            "properties": {
                                "event_id": 928321,
                                "crime_id": "abc-123",
                                "month": "2026-01-01",
                                "crime_type": "Robbery",
                            },
                        }
                    ],
                },
                "collisions": {"type": "FeatureCollection", "features": []},
                "user_reported_events": {"type": "FeatureCollection", "features": []},
            }
        }
    )


class WatchlistForecastRequest(BaseModel):
    """Input payload for next-month watchlist forecast."""

    start_month: str = Field(
        ...,
        alias="startMonth",
        description="Historical baseline start month in YYYY-MM format.",
        example="2025-01",
    )
    mode: str = Field(
        "walk",
        description="Travel mode emphasis. Supported values: walk or drive (aliases accepted).",
        example="drive",
    )
    crime_types: List[str] = Field(
        default_factory=list,
        alias="crimeTypes",
        description="Optional crime type filter. Empty list means all crime types.",
        example=["Burglary", "Robbery"],
    )

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "startMonth": "2025-01",
                "mode": "drive",
                "crimeTypes": ["Burglary", "Robbery"],
            }
        },
    )


class WatchlistForecastCountInterval(BaseModel):
    """Count interval bounds for one projected component."""

    low: int = Field(..., description="Lower projected count bound.", example=41)
    high: int = Field(..., description="Upper projected count bound.", example=71)


class WatchlistForecastIntervals(BaseModel):
    """Projected uncertainty intervals."""

    crimes: WatchlistForecastCountInterval = Field(..., description="Projected crime count interval.")
    collisions_count: WatchlistForecastCountInterval = Field(
        ...,
        description="Projected collision count interval.",
        example={"low": 0, "high": 2},
    )


class WatchlistForecastComponents(BaseModel):
    """Internal projected means and combined values."""

    mu_crime: float = Field(..., description="Recency-weighted expected crime count mean.", example=56.2241)
    mu_collision_points: float = Field(
        ...,
        description="Recency-weighted expected collision severity points mean.",
        example=0.4037,
    )
    mu_collision_count: float = Field(
        ...,
        description="Recency-weighted expected collision count mean.",
        example=0.2003,
    )
    projected_combined_value: float = Field(
        ...,
        description="Mode-weighted projected combined value for next month.",
        example=36.687,
    )
    baseline_combined_mean: float = Field(
        ...,
        description="Recency-weighted combined mean across baseline months.",
        example=36.687,
    )
    ratio: float = Field(
        ...,
        description="Projected combined value divided by baseline combined mean.",
        example=1.0,
    )


class WatchlistForecastPayload(BaseModel):
    """Forecast result payload."""

    score: int = Field(..., ge=0, le=100, description="Projected score in range 0-100.", example=100)
    band: str = Field(
        ...,
        description="Conservative band from projected score (green/amber/red).",
        example="red",
    )
    expected_crime_count: int = Field(..., ge=0, description="Expected crimes next month.", example=56)
    expected_collision_count: int = Field(..., ge=0, description="Expected collisions next month.", example=0)
    expected_collision_points: float = Field(
        ...,
        ge=0,
        description="Expected severity-weighted collision points next month.",
        example=0.4037,
    )
    intervals: WatchlistForecastIntervals = Field(
        ...,
        description="Poisson-style intervals for projected count metrics.",
    )
    components: WatchlistForecastComponents = Field(
        ...,
        description="Internal projected means and combined comparison values.",
    )


class WatchlistForecastResponse(BaseModel):
    """Response payload for next-month forecast."""

    generated_at: str = Field(
        ...,
        description="RFC3339 UTC timestamp for forecast generation.",
        example="2026-03-14T20:44:54Z",
    )
    forecast: WatchlistForecastPayload = Field(..., description="Projected next-month forecast.")

    class Config:
        schema_extra = {
            "example": {
                "generated_at": "2026-03-14T20:44:54Z",
                "forecast": {
                    "score": 100,
                    "band": "red",
                    "expected_crime_count": 56,
                    "expected_collision_count": 0,
                    "expected_collision_points": 0.4037,
                    "intervals": {
                        "crimes": {"low": 41, "high": 71},
                        "collisions_count": {"low": 0, "high": 2},
                    },
                    "components": {
                        "mu_crime": 56.2241,
                        "mu_collision_points": 0.4037,
                        "mu_collision_count": 0.2003,
                        "projected_combined_value": 36.687,
                        "baseline_combined_mean": 36.687,
                        "ratio": 1.0,
                    },
                },
            }
        }
