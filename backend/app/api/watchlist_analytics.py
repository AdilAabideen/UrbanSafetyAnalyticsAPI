from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas.watchlist_analytics_schemas import WatchlistRiskScoreResponse
from ..services.auth_service import get_current_user
from ..services.watchlist_analytics_service import build_watchlist_risk_score_service


router = APIRouter(tags=["watchlist-analytics"])


@router.post(
    "/watchlists/{watchlist_id}/analytics/risk-score",
    response_model=WatchlistRiskScoreResponse,
)
def compute_watchlist_risk_score(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistRiskScoreResponse:
    return build_watchlist_risk_score_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )
