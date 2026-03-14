from typing import Optional, Union

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas.watchlist_schemas import (
    WatchlistCreateRequest,
    WatchlistDeleteResponse,
    WatchlistListResponse,
    WatchlistSingleResponse,
    WatchlistUpdateRequest,
)
from ..services.auth_service import get_current_user
from ..services.watchlist_service import (
    create_watchlist_service,
    delete_watchlist_service,
    read_watchlists_service,
    update_watchlist_service,
)


router = APIRouter(tags=["watchlists"])


@router.get(
    "/watchlists",
    response_model=Union[WatchlistListResponse, WatchlistSingleResponse],
)
def read_watchlists(
    watchlist_id: Optional[int] = Query(default=None),
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Union[WatchlistListResponse, WatchlistSingleResponse]:
    return read_watchlists_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )


@router.post("/watchlists", status_code=status.HTTP_201_CREATED, response_model=WatchlistSingleResponse)
def create_watchlist(
    payload: WatchlistCreateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistSingleResponse:
    return create_watchlist_service(
        db=db,
        user_id=current_user["id"],
        payload=payload,
    )


@router.patch("/watchlists/{watchlist_id}", response_model=WatchlistSingleResponse)
def update_watchlist(
    watchlist_id: int,
    payload: WatchlistUpdateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistSingleResponse:
    return update_watchlist_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
        payload=payload,
    )


@router.delete("/watchlists/{watchlist_id}", response_model=WatchlistDeleteResponse)
def delete_watchlist(
    watchlist_id: int,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WatchlistDeleteResponse:
    return delete_watchlist_service(
        db=db,
        user_id=current_user["id"],
        watchlist_id=watchlist_id,
    )
