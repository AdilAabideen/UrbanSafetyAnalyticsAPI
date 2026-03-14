from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas.auth_schemas import (
    AuthRequest,
    LoginResponse,
    MeResponse,
    ProfileUpdateRequest,
    RegisterResponse,
    UpdateMeResponse,
)
from ..services.auth_service import get_current_user, login_user, register_user, update_current_user

router = APIRouter(tags=["auth"])


@router.post("/auth/register", response_model=RegisterResponse)
def register(payload: AuthRequest, db: Session = Depends(get_db)) -> RegisterResponse:
    return register_user(payload, db)


@router.post("/auth/login", response_model=LoginResponse)
def login(payload: AuthRequest, db: Session = Depends(get_db)) -> LoginResponse:
    return login_user(payload, db)


@router.get("/me", response_model=MeResponse)
def me(current_user=Depends(get_current_user)) -> MeResponse:
    return {"user": current_user}


@router.patch("/me", response_model=UpdateMeResponse)
def update_me(
    payload: ProfileUpdateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UpdateMeResponse:
    return update_current_user(payload, current_user, db)
