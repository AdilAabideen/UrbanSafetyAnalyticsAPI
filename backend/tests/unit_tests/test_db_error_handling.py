from concurrent.futures import ThreadPoolExecutor
from datetime import date
from time import sleep

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import InternalError, OperationalError

from app.api import crime_utils
from app.api.crime_utils import _analytics_filters
from app.api.crime_utils import _analytics_snapshot
from app.api.crime_utils import _execute as crime_execute
from app.api.roads import _execute as roads_execute
from app.api.tiles import _execute as tiles_execute
from app.db import _configure_postgres_session, _engine_kwargs


class ExplodingDB:
    def __init__(self, error):
        self.error = error
        self.rollback_called = False

    def execute(self, query, params):
        raise self.error

    def rollback(self):
        self.rollback_called = True


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RecordingConnection:
    def __init__(self):
        self.cursor_instance = RecordingCursor()

    def cursor(self):
        return self.cursor_instance


@pytest.mark.parametrize("execute_fn", [crime_execute, roads_execute, tiles_execute])
@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, Exception("connection lost")),
        InternalError("SELECT 1", {}, Exception("shared buffer hash table corrupted")),
    ],
)
def test_execute_translates_database_failures_to_503(execute_fn, error):
    db = ExplodingDB(error)

    with pytest.raises(HTTPException) as caught:
        execute_fn(db, "SELECT 1", {})

    assert caught.value.status_code == 503
    assert caught.value.detail == (
        "Database unavailable. Postgres query execution failed; inspect the database container and server logs."
    )
    assert db.rollback_called is True


def test_configure_postgres_session_disables_parallel_workers():
    connection = RecordingConnection()

    _configure_postgres_session(connection, None)

    assert connection.cursor_instance.statements == [
        "SET SESSION max_parallel_workers = 0",
        "SET SESSION max_parallel_workers_per_gather = 0",
        "SET SESSION statement_timeout = 15000",
    ]


def test_engine_kwargs_default_pool_settings(monkeypatch):
    monkeypatch.delenv("DB_POOL_SIZE", raising=False)
    monkeypatch.delenv("DB_MAX_OVERFLOW", raising=False)
    monkeypatch.delenv("DB_POOL_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("DB_POOL_RECYCLE_SECONDS", raising=False)

    assert _engine_kwargs() == {
        "pool_pre_ping": True,
        "pool_size": 5,
        "max_overflow": 5,
        "pool_timeout": 30,
        "pool_recycle": 1800,
    }


def test_analytics_filters_use_lon_lat_bbox_without_geom_predicates():
    where_clauses, query_params = _analytics_filters(
        {"clause": None, "params": {}, "from": None, "to": None},
        {"min_lon": -2.25, "min_lat": 53.55, "max_lon": -1.1, "max_lat": 54.05},
        None,
        None,
        None,
    )

    assert "ce.lon BETWEEN :min_lon AND :max_lon" in where_clauses
    assert "ce.lat BETWEEN :min_lat AND :max_lat" in where_clauses
    assert all("geom" not in clause for clause in where_clauses)
    assert query_params == {
        "min_lon": -2.25,
        "min_lat": 53.55,
        "max_lon": -1.1,
        "max_lat": 54.05,
    }


def test_analytics_snapshot_collapses_duplicate_inflight_requests(monkeypatch):
    class FakeResult:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return self

        def all(self):
            return self.rows

    crime_utils._analytics_snapshot_cache.clear()
    crime_utils._analytics_snapshot_inflight.clear()

    call_count = 0
    started = False
    release = False

    def fake_execute(_db, _query, _params=None):
        nonlocal call_count, started
        call_count += 1
        started = True
        while not release:
            sleep(0.01)
        return FakeResult(
            [
                {
                    "id": 1,
                    "month_date": date(2024, 8, 1),
                    "crime_type": "burglary",
                    "raw_outcome": "under investigation",
                    "outcome": "under investigation",
                    "lsoa_code": "LSOA-1",
                    "lsoa_name": "Area 1",
                }
            ]
        )

    monkeypatch.setattr(crime_utils, "_execute", fake_execute)

    args = (
        {"clause": None, "params": {}, "from": None, "to": None},
        {"min_lon": -2.25, "min_lat": 53.55, "max_lon": -1.1, "max_lat": 54.05},
        None,
        None,
        None,
        object(),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(_analytics_snapshot, *args, page_size=10)
        while not started:
            sleep(0.01)
        second = executor.submit(_analytics_snapshot, *args, page_size=10)
        sleep(0.05)
        release = True
        first_result = first.result()
        second_result = second.result()

    assert call_count == 1
    assert first_result == second_result
