from typing import Optional

from pydantic import BaseModel, Field


class AuthRequest(BaseModel):
    """Credential payload used by register and login endpoints."""

    email: str
    password: str = Field(..., min_length=8)


class ProfileUpdateRequest(BaseModel):
    """Payload for updating the authenticated user's profile fields."""

    email: Optional[str] = None
    password: Optional[str] = Field(default=None, min_length=8)
