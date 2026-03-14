from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session

from ..errors import DependencyError
from ..db import execute

def get_user_by_email(db: Session, email: str) -> Optional[Dict[str, Any]]:
    """Get a user by email."""
    query = text(
        """
        SELECT
            u.id,
            u.email,
            u.is_admin,
            u.created_at
        FROM users u
        WHERE u.email = :email
        LIMIT 1
        """
    )
    row = execute(db, query, {"email": email}).mappings().first()
    return dict(row) if row else None


def get_user_with_password_by_email(db: Session, email: str) -> Optional[Dict[str, Any]]:
    """Get a user with password by email."""
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
    row = execute(db, query, {"email": email}).mappings().first()
    return dict(row) if row else None


def get_user_by_id(db: Session, user_id: int) -> Optional[Dict[str, Any]]:
    """Get a user by ID."""
    query = text(
        """
        SELECT
            u.id,
            u.email,
            u.is_admin,
            u.created_at
        FROM users u
        WHERE u.id = :user_id
        LIMIT 1
        """
    )
    row = execute(db, query, {"user_id": user_id}).mappings().first()
    return dict(row) if row else None


def create_user(db: Session, email: str, password_hash: str) -> Dict[str, Any]:
    """Create a user."""
    query = text(
        """
        INSERT INTO users (email, password_hash, is_admin)
        VALUES (:email, :password_hash, FALSE)
        RETURNING id, email, is_admin, created_at
        """
    )
    row = execute(
        db,
        query,
        {
            "email": email,
            "password_hash": password_hash,
        },
    ).mappings().first()
    return dict(row)


def update_user(
    db: Session,
    user_id: int,
    *,
    email: Optional[str] = None,
    password_hash: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Update a user."""
    update_fields = []
    query_params: Dict[str, Any] = {"user_id": user_id}

    # Add the email to the update fields if it is provided.
    if email is not None:
        update_fields.append("email = :email")
        query_params["email"] = email

    # Add the password hash to the update fields if it is provided.
    if password_hash is not None:
        update_fields.append("password_hash = :password_hash")
        query_params["password_hash"] = password_hash

    # If no update fields are provided, return the user.
    if not update_fields:
        return get_user_by_id(db, user_id)

    # Update the user in the database.
    query = text(
        f"""
        UPDATE users
        SET {", ".join(update_fields)}
        WHERE id = :user_id
        RETURNING id, email, is_admin, created_at
        """
    )
    row = execute(db, query, query_params).mappings().first()
    return dict(row) if row else None
