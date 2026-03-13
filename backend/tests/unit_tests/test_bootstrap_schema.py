from app import bootstrap


class _RecordingConnection:
    def __init__(self):
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))


class _RecordingBegin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _RecordingBegin(self.connection)


def test_initialize_database_creates_tables_indexes_and_admin_seed(monkeypatch):
    connection = _RecordingConnection()

    monkeypatch.setattr(bootstrap, "engine", _RecordingEngine(connection))
    monkeypatch.setattr(bootstrap, "hash_password", lambda password: f"hashed::{password}")

    bootstrap.initialize_database()

    statements = [sql for sql, _ in connection.calls]
    insert_params = next(params for sql, params in connection.calls if "INSERT INTO users" in sql)

    assert any("CREATE TABLE IF NOT EXISTS users" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS watchlists" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS watchlist_preferences" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS user_reported_events" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS user_reported_crime_details" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS user_reported_collision_details" in sql for sql in statements)
    assert any("crime_types TEXT[] NOT NULL" in sql for sql in statements)
    assert any("travel_mode TEXT NOT NULL" in sql for sql in statements)
    assert any("RENAME COLUMN banding_mode TO travel_mode" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS watchlists_user_id_idx" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS watchlist_preferences_watchlist_id_idx" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS user_reported_events_geom_gix" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS user_reported_events_month_idx" in sql for sql in statements)
    assert insert_params["email"] == bootstrap.DEFAULT_ADMIN_EMAIL
    assert insert_params["password_hash"] == f"hashed::{bootstrap.DEFAULT_ADMIN_PASSWORD}"
