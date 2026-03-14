import os
from datetime import datetime, timedelta, timezone

import bcrypt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..api_utils.auth_repository import (
    create_user,
    get_user_by_email,
    get_user_by_id,
    get_user_with_password_by_email,
    update_user,
)
from ..db import get_db
from ..errors import AuthenticationError, ConflictError, NotFoundError, ValidationError
from ..schemas.auth_schemas import AuthRequest, ProfileUpdateRequest


JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))
DEFAULT_ADMIN_EMAIL = os.getenv("DEFAULT_ADMIN_EMAIL", "admin@admin.com")
DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD", "adminpassword")

bearer_scheme = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a hashed password."""
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def create_access_token(user_id: int) -> str:
    """Create an JWT access token for a user."""
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {
        "sub": str(user_id),
        "exp": expires_at,
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    """Decode a JWT access token."""
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except JWTError as exc:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
):
    """Get the current user from the database."""

    # Check if the credentials are valid.
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise AuthenticationError(
            error="NOT_AUTHENTICATED",
            message="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Decode the access token.
    payload = decode_access_token(credentials.credentials)
    user_id = payload.get("sub")

    # Check if the user ID is valid.
    if not user_id:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Get the user from the database If the user is not found, raise an authentication error.
    user = get_user_by_id(db, int(user_id))
    if not user:
        raise AuthenticationError(
            error="INVALID_TOKEN",
            message="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def register_user(payload: AuthRequest, db: Session):
    """Register a new user."""

    # Check if the email is already registered.
    existing = get_user_by_email(db, payload.email)
    if existing:
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered")

    # Create the user in the database.
    try:
        user = create_user(
            db,
            email=payload.email,
            password_hash=hash_password(payload.password),
        )
        db.commit()

    # If the user is not created, raise a conflict error.
    except IntegrityError as exc:
        db.rollback()
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered") from exc

    return {"user": user}


def login_user(payload: AuthRequest, db: Session):
    """Login a user."""

    # Get the user from the database.
    user = get_user_with_password_by_email(db, payload.email)

    # Check if the user is valid and the password is correct.
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise AuthenticationError(
            error="INVALID_CREDENTIALS",
            message="Invalid email or password",
        )

    # Create an access token for the user and return it.
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


def update_current_user(payload: ProfileUpdateRequest, current_user, db: Session):
    """Update the current user."""

    # Check if the email or password is provided.
    if payload.email is None and payload.password is None:
        raise ValidationError(
            error="INVALID_REQUEST",
            message="Provide email or password to update",
        )

    password_hash = hash_password(payload.password) if payload.password is not None else None

    # Update the user in the database.
    try:
        user = update_user(
            db,
            user_id=current_user["id"],
            email=payload.email,
            password_hash=password_hash,
        )
        db.commit()

    # If the user is not updated, raise a conflict error.
    except IntegrityError as exc:
        db.rollback()
        raise ConflictError(error="EMAIL_ALREADY_REGISTERED", message="Email already registered") from exc

    # If the user is not found, raise a not found error.
    if not user:
        raise NotFoundError(error="USER_NOT_FOUND", message="User not found")

    return {"user": user}
