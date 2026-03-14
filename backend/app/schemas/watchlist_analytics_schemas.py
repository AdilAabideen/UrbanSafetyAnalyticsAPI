from typing import List, Optional

from pydantic import BaseModel, Field


class WatchlistRiskComponents(BaseModel):
    crime_component: float
    collision_density: float
    user_support: float


class WatchlistRiskResult(BaseModel):
    risk_score: int
    raw_score: float
    components: WatchlistRiskComponents


class WatchlistComparisonDistribution(BaseModel):
    min: Optional[float] = None
    median: Optional[float] = None
    max: Optional[float] = None


class WatchlistComparisonSummary(BaseModel):
    cohort_type: str
    cohort_size: int
    subject_score: int
    rank: Optional[int] = None
    rank_out_of: Optional[int] = None
    percentile: Optional[float] = None
    distribution: WatchlistComparisonDistribution
    sample_size: int
    historical_count: int
    threshold: int
    reference_ids: List[int] = Field(default_factory=list)


class WatchlistRiskScoreResponse(BaseModel):
    watchlist_id: Optional[int] = None
    risk_result: WatchlistRiskResult
    comparison: WatchlistComparisonSummary
