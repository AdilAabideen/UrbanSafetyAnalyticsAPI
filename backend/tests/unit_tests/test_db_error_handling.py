import pytest
from fastapi import HTTPException
from sqlalchemy.exc import InternalError, OperationalError

from app.api.crime_utils import _analytics_filters
from app.api.crime_utils import _execute as crime_execute
from app.api.roads import _execute as roads_execute
from app.api.tiles import _execute as tiles_execute
from app.db import _configure_postgres_session


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
    ]


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
