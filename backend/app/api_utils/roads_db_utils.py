from fastapi import HTTPException
from sqlalchemy.exc import InternalError, OperationalError


def _execute(db, query, params):
    """Execute a roads analytics query and normalize database failures."""
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Postgres query execution failed; inspect the database container and server logs.",
        ) from exc
