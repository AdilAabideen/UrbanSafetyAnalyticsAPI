import importlib.util
from pathlib import Path

from app import bootstrap


ROOT_DIR = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT_DIR / "scripts" / "ingest_collision_master.py"


def _load_ingest_module():
    spec = importlib.util.spec_from_file_location("ingest_collision_master", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


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


def test_collision_bootstrap_adds_table_and_indexes(monkeypatch):
    connection = _RecordingConnection()

    monkeypatch.setattr(bootstrap, "engine", _RecordingEngine(connection))
    monkeypatch.setattr(bootstrap, "hash_password", lambda password: f"hashed::{password}")

    bootstrap.initialize_database()

    statements = [sql for sql, _ in connection.calls]

    assert any("CREATE EXTENSION IF NOT EXISTS postgis" in sql for sql in statements)
    assert any("CREATE TABLE IF NOT EXISTS collision_events" in sql for sql in statements)
    assert any("collision_index TEXT PRIMARY KEY" in sql for sql in statements)
    assert any("vehicle_type_counts JSONB" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS collision_events_geom_gix" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS collision_events_month_idx" in sql for sql in statements)
    assert any("CREATE INDEX IF NOT EXISTS collision_events_segment_idx" in sql for sql in statements)


def test_collision_ingest_row_mapping_derives_month_geom_and_severity_counts():
    ingest = _load_ingest_module()

    params = ingest._csv_row_to_params(
        {
            "collision_index": "abc123",
            "collision_year": "2025",
            "date": "01/02/2025",
            "time": "13:45",
            "latitude": "53.8",
            "longitude": "-1.55",
            "local_authority_ons_district": "E08000035",
            "lsoa_of_accident_location": "E0101",
            "collision_severity_code": "2",
            "collision_severity_label": "Serious",
            "number_of_vehicles": "2",
            "number_of_casualties": "3",
            "speed_limit_code": "30",
            "speed_limit_label": "30 mph",
            "road_type_code": "6",
            "road_type_label": "Single carriageway",
            "light_conditions_code": "1",
            "light_conditions_label": "Daylight",
            "weather_conditions_code": "1",
            "weather_conditions_label": "Fine no high winds",
            "road_surface_conditions_code": "1",
            "road_surface_conditions_label": "Dry",
            "vehicle_count": "2",
            "vehicle_type_counts": '{"Car": 2}',
            "avg_driver_age": "34.5",
            "driver_sex_counts": '{"Male": 1, "Female": 1}',
            "casualty_count": "3",
            "casualty_severity_counts": '{"Fatal": 1, "Serious": 1, "Slight": 1}',
            "casualty_type_counts": '{"Pedestrian": 1, "Car occupant": 2}',
        }
    )

    assert params["collision_index"] == "abc123"
    assert str(params["month"]) == "2025-02-01"
    assert str(params["collision_time"]) == "13:45:00"
    assert params["fatal_casualty_count"] == 1
    assert params["serious_casualty_count"] == 1
    assert params["slight_casualty_count"] == 1
    assert params["vehicle_type_counts"] == '{"Car": 2}'
