from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from ..api_utils.auth_utils import create_access_token, get_current_user, hash_password, verify_password
from ..db import get_db
from ..errors import (
    AuthenticationError,
    ConflictError,
    DependencyError,
    NotFoundError,
    ValidationError,
)
from ..schemas.auth_schemas import (
    AuthRequest,
    LoginResponse,
    MeResponse,
    ProfileUpdateRequest,
    RegisterResponse,
    UpdateMeResponse,
)


router = APIRouter(tags=["auth"])


def _execute(db, query, params):
    try:
        return db.execute(query, params)
    except OperationalError as exc:
        db.rollback()
        raise DependencyError(
            message="Database unavailable. Postgres query execution failed; inspect the database container and server logs."
        ) from exc


@router.post("/auth/register", response_model=RegisterResponse)
def register(payload: AuthRequest, db: Session = Depends(get_db)) -> RegisterResponse:
    existing_query = text(
        """
        SELECT u.id
        FROM users u
        WHERE u.email = :email
        LIMIT 1
        """
    )
    existing = _execute(db, existing_query, {"email": payload.email}).mappings().first()
    if existing:
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered")

    insert_query = text(
        """
        INSERT INTO users (email, password_hash, is_admin)
        VALUES (:email, :password_hash, FALSE)
        RETURNING id, email, is_admin, created_at
        """
    )
    try:
        user = _execute(
            db,
            insert_query,
            {
                "email": payload.email,
                "password_hash": hash_password(payload.password),
            },
        ).mappings().first()
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered") from exc

    return {
        "user": {
            "id": user["id"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "created_at": user["created_at"],
        }
    }


@router.post("/auth/login", response_model=LoginResponse)
def login(payload: AuthRequest, db: Session = Depends(get_db)) -> LoginResponse:
    query = text(
        """
        SELECT
            u.id,
            u.email,
            u.password_hash,
            u.is_admin,
            u.created_at
        FROM users u
        WHERE u.email = :email
        LIMIT 1
        """
    )
    user = _execute(db, query, {"email": payload.email}).mappings().first()
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise AuthenticationError(
            error="INVALID_CREDENTIALS",
            message="Invalid email or password",
        )

    access_token = create_access_token(user["id"])
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "created_at": user["created_at"],
        },
    }


@router.get("/me", response_model=MeResponse)
def me(current_user=Depends(get_current_user)) -> MeResponse:
    return {"user": current_user}


@router.patch("/me", response_model=UpdateMeResponse)
def update_me(
    payload: ProfileUpdateRequest,
    current_user=Depends(get_current_user),
    db: Session = Depends(get_db),
 ) -> UpdateMeResponse:
    if payload.email is None and payload.password is None:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="Provide email or password to update",
        )

    update_fields = []
    query_params = {"user_id": current_user["id"]}

    if payload.email is not None:
        update_fields.append("email = :email")
        query_params["email"] = payload.email

    if payload.password is not None:
        update_fields.append("password_hash = :password_hash")
        query_params["password_hash"] = hash_password(payload.password)

    update_query = text(
        f"""
        UPDATE users
        SET {", ".join(update_fields)}
        WHERE id = :user_id
        RETURNING id, email, is_admin, created_at
        """
    )

    try:
        user = _execute(db, update_query, query_params).mappings().first()
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered") from exc

    if not user:
        raise NotFoundError(error="USER_NOT_FOUND", message="User not found")

    return {
        "user": {
            "id": user["id"],
            "email": user["email"],
            "is_admin": user["is_admin"],
            "created_at": user["created_at"],
        }
    }
