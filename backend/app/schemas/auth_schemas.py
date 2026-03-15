from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class AuthRequest(BaseModel):
    """Credential payload used by register and login endpoints."""

    email: str = Field(..., description="Email address for the account.")
    password: str = Field(
        ...,
        min_length=8,
        description="Plain-text password. Must be at least 8 characters.",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "email": "student@example.com",
                    "password": "StrongPass123",
                }
            ]
        }
    }


class ProfileUpdateRequest(BaseModel):
    """Payload for updating the authenticated user's profile fields."""

    email: Optional[str] = Field(
        default=None,
        description="New email address. Optional.",
    )
    password: Optional[str] = Field(
        default=None,
        min_length=8,
        description="New plain-text password. Optional, minimum 8 characters.",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"email": "new-email@example.com"},
                {"password": "UpdatedPass123"},
                {
                    "email": "new-email@example.com",
                    "password": "UpdatedPass123",
                },
            ]
        }
    }


class UserPayload(BaseModel):
    id: int = Field(..., description="Unique user ID.")
    email: str = Field(..., description="User email address.")
    is_admin: bool = Field(..., description="Whether the user has admin privileges.")
    created_at: datetime = Field(..., description="UTC timestamp when the user was created.")


class RegisterResponse(BaseModel):
    user: UserPayload = Field(..., description="Newly registered user.")


class LoginResponse(BaseModel):
    access_token: str = Field(..., description="JWT bearer access token.")
    token_type: str = Field(..., description="Auth token type. Always 'bearer'.")
    user: UserPayload = Field(..., description="Authenticated user.")


class MeResponse(BaseModel):
    user: UserPayload = Field(..., description="Current authenticated user.")


class UpdateMeResponse(BaseModel):
    user: UserPayload = Field(..., description="Updated authenticated user.")
