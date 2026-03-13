import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker


load_dotenv()
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DATABASE_URL = os.getenv("BACKEND_DATABASE_URL") or os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("BACKEND_DATABASE_URL or DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


@event.listens_for(engine, "connect")
def _configure_postgres_session(dbapi_connection, _connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute("SET SESSION max_parallel_workers = 0")
        cursor.execute("SET SESSION max_parallel_workers_per_gather = 0")


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
