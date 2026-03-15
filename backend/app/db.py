import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import InternalError, OperationalError
from sqlalchemy.orm import Session
from .errors import DependencyError

load_dotenv()
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DATABASE_URL = os.getenv("BACKEND_DATABASE_URL") or os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("BACKEND_DATABASE_URL or DATABASE_URL is not set")


def _env_int(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


def _engine_kwargs():
    return {
        "pool_pre_ping": True,
        "pool_size": _env_int("DB_POOL_SIZE", 5),
        "max_overflow": _env_int("DB_MAX_OVERFLOW", 5),
        "pool_timeout": _env_int("DB_POOL_TIMEOUT_SECONDS", 30),
        "pool_recycle": _env_int("DB_POOL_RECYCLE_SECONDS", 1800),
    }


def _statement_timeout_ms():
    return _env_int("DB_STATEMENT_TIMEOUT_MS", 15000)


engine = create_engine(DATABASE_URL, **_engine_kwargs())


@event.listens_for(engine, "connect")
def _configure_postgres_session(dbapi_connection, _connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute("SET SESSION max_parallel_workers = 0")
        cursor.execute("SET SESSION max_parallel_workers_per_gather = 0")
        cursor.execute(f"SET SESSION statement_timeout = {_statement_timeout_ms()}")


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Gets a database session, Perfect for FastAPI dependencies.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Execute a SQL query and translate database failures into a DependencyError.
def execute(db: Session, query: str, params: dict):
    """Execute SQL and translate database failures into a DependencyError."""
    try:
        return db.execute(query, params)
    except (InternalError, OperationalError) as exc:
        db.rollback()
        raise DependencyError(
            message="Database unavailable. Postgres query execution failed; inspect the database container and server logs."
        ) from exc
