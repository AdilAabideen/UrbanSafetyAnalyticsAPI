import os
from datetime import date

import psycopg
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from app.db import SessionLocal

client = TestClient(app)


@pytest.fixture(scope="session")
def db_conn():
    """
    Real Postgres connection for smoke tests only.

    Assumes DATABASE_URL or BACKEND_DATABASE_URL is set and docker compose db is running.
    """
    url = os.getenv("BACKEND_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL/BACKEND_DATABASE_URL not set")

    # psycopg3 accepts SQLAlchemy-style URLs if you strip the driver
    if url.startswith("postgresql+psycopg://"):
        url = "postgresql://" + url.split("://", 1)[1]

    with psycopg.connect(url) as conn:
        yield conn


def test_health_endpoint_ok():
    """API process is up and health route responds with ok=True."""
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload.get("ok") is True


def test_database_basic_schema(db_conn):
    """
    Basic schema smoke test:

    - crime_events table exists
    - urban_risk extension / PostGIS is available enough to query geom
    """
    cur = db_conn.cursor()

    # 1) Table exists
    cur.execute(
        """
        SELECT to_regclass('public.crime_events')
        """
    )
    (regclass,) = cur.fetchone()
    assert regclass == "crime_events", "crime_events table is missing"

    # 2) Simple count works
    cur.execute("SELECT COUNT(*) FROM crime_events")
    (cnt,) = cur.fetchone()
    assert isinstance(cnt, int)

    # 3) Optional: sample a geom to ensure PostGIS column is usable
    cur.execute(
        """
        SELECT geom IS NOT NULL
        FROM crime_events
        WHERE geom IS NOT NULL
        LIMIT 1
        """
    )
    row = cur.fetchone()
    # It's okay if there are no rows yet; this just exercises the column
    # row may be None or (True,)
    assert row is None or row[0] in (True, False)


def test_sessionlocal_can_connect():
    """
    Smoke test that SQLAlchemy SessionLocal can open a connection and run a trivial query.
    """
    try:
        db = SessionLocal()
        result = db.execute(text("SELECT CURRENT_DATE"))
        (today,) = result.fetchone()
        assert isinstance(today, date)
    finally:
        db.close()


def test_smoke_validation_error_uses_standard_error_shape():
    """
    Calling an endpoint without required params should surface a standardized INVALID_REQUEST error.
    """
    response = client.get("/crimes/map")

    assert response.status_code == 400
    data = response.json()
    assert data["error"] == "INVALID_REQUEST"
    assert "details" in data
    assert isinstance(data["details"].get("errors"), list)