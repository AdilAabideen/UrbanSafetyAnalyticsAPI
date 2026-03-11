import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DATABASE_URL = os.getenv("BACKEND_DATABASE_URL") or os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("BACKEND_DATABASE_URL or DATABASE_URL is not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
