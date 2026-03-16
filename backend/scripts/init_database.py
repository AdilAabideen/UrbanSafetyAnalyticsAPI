#!/usr/bin/env python3
"""
Docker-native database initializer (no SQL seed restore).

This script intentionally avoids host-side DB Python dependencies and uses:
- docker compose exec db psql
- docker compose exec db osm2pgsql

That makes setup easier for assessors: Docker Desktop is the only hard dependency.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import List


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
CRIME_DIR = DATA_DIR / "crime"
COLLISION_CSV = DATA_DIR / "master-collision-dataset.csv"
ROADS_PBF = DATA_DIR / "wyosm.pbf"
DEFAULT_MAX_SNAP_DISTANCE_M = 200.0
DEFAULT_REFERENCE_BBOXES = [
    {"label": "Leeds_City_Centre", "min_lat": 53.7900, "min_lon": -1.5600, "max_lat": 53.8060, "max_lon": -1.5300},
    {"label": "Headingley", "min_lat": 53.8120, "min_lon": -1.5950, "max_lat": 53.8260, "max_lon": -1.5650},
    {"label": "Hyde_Park", "min_lat": 53.8040, "min_lon": -1.5820, "max_lat": 53.8160, "max_lon": -1.5600},
    {"label": "Holbeck", "min_lat": 53.7790, "min_lon": -1.5700, "max_lat": 53.7920, "max_lon": -1.5450},
    {"label": "Hunslet", "min_lat": 53.7720, "min_lon": -1.5450, "max_lat": 53.7850, "max_lon": -1.5200},
    {"label": "Roundhay", "min_lat": 53.8260, "min_lon": -1.5100, "max_lat": 53.8450, "max_lon": -1.4700},
    {"label": "Chapel_Allerton", "min_lat": 53.8200, "min_lon": -1.5550, "max_lat": 53.8350, "max_lon": -1.5250},
    {"label": "Harehills", "min_lat": 53.8000, "min_lon": -1.5250, "max_lat": 53.8150, "max_lon": -1.4950},
    {"label": "Cross_Gates", "min_lat": 53.8020, "min_lon": -1.4700, "max_lat": 53.8180, "max_lon": -1.4300},
    {"label": "Morley", "min_lat": 53.7350, "min_lon": -1.6200, "max_lat": 53.7550, "max_lon": -1.5800},
]


def log(message: str) -> None:
    print(f"[init-db] {message}", flush=True)


class StageTimer:
    def __init__(self, name: str):
        self.name = name
        self.start = 0.0

    def __enter__(self):
        self.start = time.perf_counter()
        log(f"Starting: {self.name}")
        return self

    def __exit__(self, exc_type, _exc, _tb):
        elapsed = time.perf_counter() - self.start
        status = "FAILED" if exc_type else "done"
        log(f"Finished: {self.name} ({status}, {elapsed:.2f}s)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize Postgres/PostGIS from raw datasets.")
    parser.add_argument("--force", action="store_true", help="Drop/rebuild target tables if they already contain data.")
    parser.add_argument("--skip-roads", action="store_true", help="Skip road import from wyosm.pbf.")
    parser.add_argument("--skip-crime", action="store_true", help="Skip crime CSV import and crime aggregates.")
    parser.add_argument("--skip-collisions", action="store_true", help="Skip collision CSV import and collision aggregates.")
    parser.add_argument(
        "--max-snap-distance-m",
        type=float,
        default=DEFAULT_MAX_SNAP_DISTANCE_M,
        help="Maximum nearest-road snapping distance in meters.",
    )
    return parser.parse_args()


def run_cmd(cmd: List[str], *, input_text: str | None = None, capture: bool = False) -> str:
    """Run a subprocess command with optional stdin and optional stdout capture."""
    log("$ " + " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        check=True,
        text=True,
        input=input_text,
        capture_output=capture,
    )
    return result.stdout if capture else ""


def compose_exec(args: List[str], *, input_text: str | None = None, capture: bool = False) -> str:
    return run_cmd(["docker", "compose", "exec", "-T", "db", *args], input_text=input_text, capture=capture)


def psql(sql_text: str) -> None:
    """Execute SQL script with ON_ERROR_STOP so failures stop immediately."""
    compose_exec(
        [
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "app",
            "-d",
            "urban_risk",
            "-f",
            "-",
        ],
        input_text=sql_text,
    )


def psql_command(command_text: str) -> None:
    """Execute a single psql -c command (used for psql meta-commands like \\copy)."""
    compose_exec(
        [
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "app",
            "-d",
            "urban_risk",
            "-c",
            command_text,
        ]
    )


def psql_scalar(sql_text: str) -> str:
    """Run scalar SQL query and return raw value."""
    out = compose_exec(
        [
            "psql",
            "-X",
            "-A",
            "-t",
            "-U",
            "app",
            "-d",
            "urban_risk",
            "-c",
            sql_text,
        ],
        capture=True,
    )
    return out.strip()


def ensure_required_files(skip_roads: bool, skip_crime: bool, skip_collisions: bool) -> List[Path]:
    missing: List[Path] = []
    if not skip_roads and not ROADS_PBF.exists():
        missing.append(ROADS_PBF)
    if not skip_crime:
        if not CRIME_DIR.exists():
            missing.append(CRIME_DIR)
        elif not list(CRIME_DIR.glob("*.csv")):
            missing.append(CRIME_DIR / "*.csv")
    if not skip_collisions and not COLLISION_CSV.exists():
        missing.append(COLLISION_CSV)
    return missing


def ensure_db_container_ready(*, require_roads_file: bool) -> None:
    """Start DB container and verify required tools/files are present in-container."""
    run_cmd(["docker", "compose", "up", "-d", "db"])
    try:
        compose_exec(["sh", "-lc", "command -v osm2pgsql >/dev/null"])
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            "DB container is missing osm2pgsql. "
            "Run `make recover-db` to rebuild the db image, then retry initialization."
        ) from exc
    if require_roads_file:
        compose_exec(["sh", "-lc", "test -f /data/wyosm.pbf"])


def table_exists(name: str) -> bool:
    value = psql_scalar(f"SELECT to_regclass('public.{name}') IS NOT NULL;")
    return value.lower() == "t"


def table_count(name: str) -> int:
    if not table_exists(name):
        return 0
    value = psql_scalar(f"SELECT COUNT(*)::bigint FROM {name};")
    return int(value or "0")


def require_force_if_initialized(force: bool) -> None:
    tracked = [
        "road_segments",
        "crime_events",
        "collision_events",
        "segment_month_type_stats",
        "segment_month_collision_stats",
    ]
    counts = {name: table_count(name) for name in tracked}
    if any(count > 0 for count in counts.values()) and not force:
        details = ", ".join(f"{k}={v}" for k, v in counts.items())
        raise RuntimeError(
            "Existing initialized data detected. Re-run with --force to rebuild. "
            f"Current counts: {details}"
        )


def drop_pipeline_tables() -> None:
    with StageTimer("drop existing pipeline tables"):
        psql(
            """
            SET statement_timeout = 0;

            DROP TABLE IF EXISTS segment_month_type_stats CASCADE;
            DROP TABLE IF EXISTS segment_month_collision_stats CASCADE;
            DROP TABLE IF EXISTS crime_events CASCADE;
            DROP TABLE IF EXISTS road_segments CASCADE;

            DO $$
            DECLARE t record;
            BEGIN
                FOR t IN
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename LIKE 'planet_osm_%'
                LOOP
                    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t.tablename);
                END LOOP;
            END $$;
            """
        )


def create_pipeline_tables() -> None:
    """Create only the tables required for analytics/tile pipelines."""
    with StageTimer("create pipeline tables"):
        psql(
            """
            SET statement_timeout = 0;

            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE EXTENSION IF NOT EXISTS hstore;

            CREATE TABLE IF NOT EXISTS road_segments (
                id BIGSERIAL PRIMARY KEY,
                osm_id BIGINT,
                name TEXT,
                highway TEXT,
                oneway TEXT,
                maxspeed TEXT,
                geom geometry(LineString, 3857) NOT NULL,
                geom_4326 geometry(LineString, 4326) NOT NULL,
                length_m DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS road_segments_geom_gix ON road_segments USING GIST (geom);
            CREATE INDEX IF NOT EXISTS road_segments_geom_4326_gix ON road_segments USING GIST (geom_4326);
            CREATE INDEX IF NOT EXISTS road_segments_highway_idx ON road_segments (highway);

            CREATE TABLE IF NOT EXISTS crime_events (
                id BIGSERIAL PRIMARY KEY,
                crime_id TEXT,
                month DATE NOT NULL,
                reported_by TEXT,
                falls_within TEXT,
                lon DOUBLE PRECISION,
                lat DOUBLE PRECISION,
                location TEXT,
                lsoa_code TEXT,
                lsoa_name TEXT,
                crime_type TEXT,
                last_outcome_category TEXT,
                context TEXT,
                geom geometry(Point, 4326),
                segment_id BIGINT,
                snap_distance_m DOUBLE PRECISION,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS crime_events_geom_gix ON crime_events USING GIST (geom);
            CREATE INDEX IF NOT EXISTS crime_events_month_idx ON crime_events(month);
            CREATE INDEX IF NOT EXISTS crime_events_type_idx ON crime_events(crime_type);
            CREATE INDEX IF NOT EXISTS crime_events_segment_idx ON crime_events(segment_id);

            CREATE TABLE IF NOT EXISTS collision_events (
                collision_index TEXT PRIMARY KEY,
                collision_year INTEGER NOT NULL,
                collision_date DATE NOT NULL,
                collision_time TIME,
                month DATE NOT NULL,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                geom geometry(Point,4326),
                local_authority_ons_district TEXT NOT NULL,
                lsoa_of_accident_location TEXT,
                collision_severity_code INTEGER,
                collision_severity_label TEXT,
                number_of_vehicles INTEGER,
                number_of_casualties INTEGER,
                speed_limit_code INTEGER,
                speed_limit_label TEXT,
                road_type_code INTEGER,
                road_type_label TEXT,
                light_conditions_code INTEGER,
                light_conditions_label TEXT,
                weather_conditions_code INTEGER,
                weather_conditions_label TEXT,
                road_surface_conditions_code INTEGER,
                road_surface_conditions_label TEXT,
                vehicle_count INTEGER NOT NULL DEFAULT 0,
                vehicle_type_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
                avg_driver_age NUMERIC(5,2),
                driver_sex_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
                casualty_count INTEGER NOT NULL DEFAULT 0,
                casualty_severity_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
                casualty_type_counts JSONB NOT NULL DEFAULT '{}'::JSONB,
                fatal_casualty_count INTEGER NOT NULL DEFAULT 0,
                serious_casualty_count INTEGER NOT NULL DEFAULT 0,
                slight_casualty_count INTEGER NOT NULL DEFAULT 0,
                segment_id BIGINT,
                snap_distance_m DOUBLE PRECISION,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS collision_events_geom_gix ON collision_events USING GIST (geom);
            CREATE INDEX IF NOT EXISTS collision_events_month_idx ON collision_events(month);
            CREATE INDEX IF NOT EXISTS collision_events_segment_idx ON collision_events(segment_id);

            CREATE TABLE IF NOT EXISTS segment_month_type_stats (
                segment_id BIGINT NOT NULL,
                month DATE NOT NULL,
                crime_type TEXT NOT NULL,
                crime_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (segment_id, month, crime_type)
            );
            CREATE INDEX IF NOT EXISTS segment_month_type_stats_month_idx ON segment_month_type_stats(month);
            CREATE INDEX IF NOT EXISTS segment_month_type_stats_segment_idx ON segment_month_type_stats(segment_id);
            CREATE INDEX IF NOT EXISTS segment_month_type_stats_crime_type_idx ON segment_month_type_stats(crime_type);

            CREATE TABLE IF NOT EXISTS segment_month_collision_stats (
                segment_id BIGINT NOT NULL,
                month DATE NOT NULL,
                collision_count INTEGER NOT NULL DEFAULT 0,
                casualty_count INTEGER NOT NULL DEFAULT 0,
                fatal_casualty_count INTEGER NOT NULL DEFAULT 0,
                serious_casualty_count INTEGER NOT NULL DEFAULT 0,
                slight_casualty_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (segment_id, month)
            );
            CREATE INDEX IF NOT EXISTS segment_month_collision_stats_month_idx ON segment_month_collision_stats(month);
            CREATE INDEX IF NOT EXISTS segment_month_collision_stats_segment_idx ON segment_month_collision_stats(segment_id);
            """
        )


def import_roads() -> None:
    with StageTimer("roads import (osm2pgsql)"):
        compose_exec(
            [
                "sh",
                "-lc",
                (
                    "PGPASSWORD=app osm2pgsql "
                    "--create --slim "
                    "--database urban_risk "
                    "--username app "
                    "--host 127.0.0.1 "
                    "--cache 512 "
                    "--number-processes 2 "
                    "/data/wyosm.pbf"
                ),
            ]
        )


def build_road_segments() -> None:
    with StageTimer("build road_segments"):
        psql(
            """
            SET statement_timeout = 0;
            TRUNCATE TABLE road_segments RESTART IDENTITY;

            INSERT INTO road_segments (
                osm_id,
                name,
                highway,
                oneway,
                maxspeed,
                geom,
                geom_4326,
                length_m
            )
            SELECT
                src.osm_id,
                NULLIF(src.name, '') AS name,
                NULLIF(src.highway, '') AS highway,
                NULLIF(src.oneway, '') AS oneway,
                NULLIF(src.maxspeed, '') AS maxspeed,
                src.geom,
                ST_Transform(src.geom, 4326) AS geom_4326,
                ST_Length(ST_Transform(src.geom, 4326)::geography) AS length_m
            FROM (
                SELECT
                    CASE
                        WHEN (to_jsonb(pl) ->> 'osm_id') ~ '^[-]?[0-9]+$'
                            THEN (to_jsonb(pl) ->> 'osm_id')::bigint
                        ELSE NULL
                    END AS osm_id,
                    (to_jsonb(pl) ->> 'name') AS name,
                    (to_jsonb(pl) ->> 'highway') AS highway,
                    (to_jsonb(pl) ->> 'oneway') AS oneway,
                    (to_jsonb(pl) ->> 'maxspeed') AS maxspeed,
                    (ST_Dump(ST_LineMerge(pl.way))).geom::geometry(LineString, 3857) AS geom
                FROM planet_osm_line pl
                WHERE (to_jsonb(pl) ->> 'highway') IS NOT NULL
            ) AS src
            WHERE src.geom IS NOT NULL
              AND ST_IsValid(src.geom)
              AND ST_NPoints(src.geom) > 1;
            """
        )


def import_crime(crime_files: List[Path]) -> None:
    with StageTimer("crime import"):
        # Stage table creation is plain SQL.
        psql(
            """
            SET statement_timeout = 0;
            DROP TABLE IF EXISTS _crime_stage;
            CREATE UNLOGGED TABLE _crime_stage (
                crime_id TEXT,
                month_text TEXT,
                reported_by TEXT,
                falls_within TEXT,
                longitude TEXT,
                latitude TEXT,
                location TEXT,
                lsoa_code TEXT,
                lsoa_name TEXT,
                crime_type TEXT,
                last_outcome_category TEXT,
                context TEXT
            );
            """
        )

        # Run each copy as a dedicated psql meta-command; this avoids parser edge-cases.
        for file_path in sorted(crime_files):
            rel = file_path.relative_to(REPO_ROOT).as_posix()
            in_container = f"/{rel}"
            log(f"Load crime CSV: {file_path.name}")
            psql_command(
                "\\copy _crime_stage ("
                "crime_id, month_text, reported_by, falls_within, longitude, latitude, "
                "location, lsoa_code, lsoa_name, crime_type, last_outcome_category, context"
                f") FROM '{in_container}' WITH (FORMAT csv, HEADER true)"
            )

        # Transform staged rows into canonical crime_events schema.
        psql(
            """
            SET statement_timeout = 0;
            TRUNCATE TABLE crime_events RESTART IDENTITY;

            INSERT INTO crime_events (
                crime_id,
                month,
                reported_by,
                falls_within,
                lon,
                lat,
                location,
                lsoa_code,
                lsoa_name,
                crime_type,
                last_outcome_category,
                context,
                geom
            )
            WITH parsed AS (
                SELECT
                    NULLIF(crime_id, '') AS crime_id,
                    CASE
                        WHEN NULLIF(month_text, '') IS NULL THEN NULL
                        ELSE TO_DATE(month_text || '-01', 'YYYY-MM-DD')
                    END AS month_value,
                    NULLIF(reported_by, '') AS reported_by,
                    NULLIF(falls_within, '') AS falls_within,
                    CASE
                        WHEN NULLIF(longitude, '') ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN longitude::double precision
                        ELSE NULL
                    END AS lon_value,
                    CASE
                        WHEN NULLIF(latitude, '') ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN latitude::double precision
                        ELSE NULL
                    END AS lat_value,
                    NULLIF(location, '') AS location,
                    NULLIF(lsoa_code, '') AS lsoa_code,
                    NULLIF(lsoa_name, '') AS lsoa_name,
                    NULLIF(crime_type, '') AS crime_type,
                    NULLIF(last_outcome_category, '') AS last_outcome_category,
                    NULLIF(context, '') AS context
                FROM _crime_stage
            )
            SELECT
                p.crime_id,
                p.month_value,
                p.reported_by,
                p.falls_within,
                p.lon_value,
                p.lat_value,
                p.location,
                p.lsoa_code,
                p.lsoa_name,
                p.crime_type,
                p.last_outcome_category,
                p.context,
                CASE
                    WHEN p.lon_value BETWEEN -180 AND 180
                     AND p.lat_value BETWEEN -90 AND 90
                        THEN ST_SetSRID(ST_Point(p.lon_value, p.lat_value), 4326)
                    ELSE NULL
                END AS geom
            FROM parsed p
            WHERE p.month_value IS NOT NULL;

            DROP TABLE IF EXISTS _crime_stage;
            """
        )


def import_collisions() -> None:
    with StageTimer("collision import"):
        in_container = f"/{COLLISION_CSV.relative_to(REPO_ROOT).as_posix()}"
        # Stage table setup first.
        psql(
            f"""
            SET statement_timeout = 0;
            DROP TABLE IF EXISTS _collision_stage;
            CREATE UNLOGGED TABLE _collision_stage (
                collision_index TEXT,
                collision_year TEXT,
                date_text TEXT,
                time_text TEXT,
                latitude TEXT,
                longitude TEXT,
                geom_text TEXT,
                local_authority_ons_district TEXT,
                lsoa_of_accident_location TEXT,
                collision_severity_code TEXT,
                collision_severity_label TEXT,
                number_of_vehicles TEXT,
                number_of_casualties TEXT,
                speed_limit_code TEXT,
                speed_limit_label TEXT,
                road_type_code TEXT,
                road_type_label TEXT,
                light_conditions_code TEXT,
                light_conditions_label TEXT,
                weather_conditions_code TEXT,
                weather_conditions_label TEXT,
                road_surface_conditions_code TEXT,
                road_surface_conditions_label TEXT,
                vehicle_count TEXT,
                vehicle_type_counts TEXT,
                avg_driver_age TEXT,
                driver_sex_counts TEXT,
                casualty_count TEXT,
                casualty_severity_counts TEXT,
                casualty_type_counts TEXT
            );
            """
        )

        # Dedicated meta-command for reliable CSV load.
        psql_command(
            "\\copy _collision_stage ("
            "collision_index, collision_year, date_text, time_text, latitude, longitude, "
            "geom_text, local_authority_ons_district, lsoa_of_accident_location, "
            "collision_severity_code, collision_severity_label, number_of_vehicles, number_of_casualties, "
            "speed_limit_code, speed_limit_label, road_type_code, road_type_label, "
            "light_conditions_code, light_conditions_label, weather_conditions_code, weather_conditions_label, "
            "road_surface_conditions_code, road_surface_conditions_label, vehicle_count, vehicle_type_counts, "
            "avg_driver_age, driver_sex_counts, casualty_count, casualty_severity_counts, casualty_type_counts"
            f") FROM '{in_container}' WITH (FORMAT csv, HEADER true)"
        )

        # Transform staged rows into canonical collision_events schema.
        psql(
            """
            SET statement_timeout = 0;
            TRUNCATE TABLE collision_events;

            INSERT INTO collision_events (
                collision_index,
                collision_year,
                collision_date,
                collision_time,
                month,
                latitude,
                longitude,
                geom,
                local_authority_ons_district,
                lsoa_of_accident_location,
                collision_severity_code,
                collision_severity_label,
                number_of_vehicles,
                number_of_casualties,
                speed_limit_code,
                speed_limit_label,
                road_type_code,
                road_type_label,
                light_conditions_code,
                light_conditions_label,
                weather_conditions_code,
                weather_conditions_label,
                road_surface_conditions_code,
                road_surface_conditions_label,
                vehicle_count,
                vehicle_type_counts,
                avg_driver_age,
                driver_sex_counts,
                casualty_count,
                casualty_severity_counts,
                casualty_type_counts,
                fatal_casualty_count,
                serious_casualty_count,
                slight_casualty_count
            )
            WITH parsed AS (
                SELECT
                    NULLIF(collision_index, '') AS collision_index,
                    CASE
                        WHEN NULLIF(collision_year, '') ~ '^[0-9]+$' THEN collision_year::integer
                        ELSE EXTRACT(YEAR FROM TO_DATE(NULLIF(date_text, ''), 'DD/MM/YYYY'))::integer
                    END AS collision_year,
                    TO_DATE(NULLIF(date_text, ''), 'DD/MM/YYYY') AS collision_date,
                    CASE
                        WHEN NULLIF(time_text, '') IS NULL THEN NULL
                        ELSE NULLIF(time_text, '')::time
                    END AS collision_time,
                    CASE
                        WHEN NULLIF(latitude, '') ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN latitude::double precision
                        ELSE NULL
                    END AS latitude,
                    CASE
                        WHEN NULLIF(longitude, '') ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN longitude::double precision
                        ELSE NULL
                    END AS longitude,
                    COALESCE(NULLIF(local_authority_ons_district, ''), 'UNKNOWN') AS local_authority_ons_district,
                    NULLIF(lsoa_of_accident_location, '') AS lsoa_of_accident_location,
                    CASE
                        WHEN NULLIF(collision_severity_code, '') ~ '^[0-9]+$' THEN collision_severity_code::integer
                        ELSE NULL
                    END AS collision_severity_code,
                    NULLIF(collision_severity_label, '') AS collision_severity_label,
                    CASE
                        WHEN NULLIF(number_of_vehicles, '') ~ '^[0-9]+$' THEN number_of_vehicles::integer
                        ELSE NULL
                    END AS number_of_vehicles,
                    CASE
                        WHEN NULLIF(number_of_casualties, '') ~ '^[0-9]+$' THEN number_of_casualties::integer
                        ELSE 0
                    END AS number_of_casualties,
                    CASE
                        WHEN NULLIF(speed_limit_code, '') ~ '^[0-9]+$' THEN speed_limit_code::integer
                        ELSE NULL
                    END AS speed_limit_code,
                    NULLIF(speed_limit_label, '') AS speed_limit_label,
                    CASE
                        WHEN NULLIF(road_type_code, '') ~ '^[0-9]+$' THEN road_type_code::integer
                        ELSE NULL
                    END AS road_type_code,
                    NULLIF(road_type_label, '') AS road_type_label,
                    CASE
                        WHEN NULLIF(light_conditions_code, '') ~ '^[0-9]+$' THEN light_conditions_code::integer
                        ELSE NULL
                    END AS light_conditions_code,
                    NULLIF(light_conditions_label, '') AS light_conditions_label,
                    CASE
                        WHEN NULLIF(weather_conditions_code, '') ~ '^[0-9]+$' THEN weather_conditions_code::integer
                        ELSE NULL
                    END AS weather_conditions_code,
                    NULLIF(weather_conditions_label, '') AS weather_conditions_label,
                    CASE
                        WHEN NULLIF(road_surface_conditions_code, '') ~ '^[0-9]+$' THEN road_surface_conditions_code::integer
                        ELSE NULL
                    END AS road_surface_conditions_code,
                    NULLIF(road_surface_conditions_label, '') AS road_surface_conditions_label,
                    CASE
                        WHEN NULLIF(vehicle_count, '') ~ '^[0-9]+$' THEN vehicle_count::integer
                        ELSE 0
                    END AS vehicle_count,
                    CASE
                        WHEN NULLIF(vehicle_type_counts, '') IS NULL THEN '{}'::jsonb
                        ELSE vehicle_type_counts::jsonb
                    END AS vehicle_type_counts,
                    CASE
                        WHEN NULLIF(avg_driver_age, '') ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN avg_driver_age::numeric
                        ELSE NULL
                    END AS avg_driver_age,
                    CASE
                        WHEN NULLIF(driver_sex_counts, '') IS NULL THEN '{}'::jsonb
                        ELSE driver_sex_counts::jsonb
                    END AS driver_sex_counts,
                    CASE
                        WHEN NULLIF(casualty_count, '') ~ '^[0-9]+$' THEN casualty_count::integer
                        ELSE 0
                    END AS casualty_count,
                    CASE
                        WHEN NULLIF(casualty_severity_counts, '') IS NULL THEN '{}'::jsonb
                        ELSE casualty_severity_counts::jsonb
                    END AS casualty_severity_counts,
                    CASE
                        WHEN NULLIF(casualty_type_counts, '') IS NULL THEN '{}'::jsonb
                        ELSE casualty_type_counts::jsonb
                    END AS casualty_type_counts
                FROM _collision_stage
            )
            SELECT
                p.collision_index,
                p.collision_year,
                p.collision_date,
                p.collision_time,
                DATE_TRUNC('month', p.collision_date)::date AS month,
                p.latitude,
                p.longitude,
                CASE
                    WHEN p.longitude BETWEEN -180 AND 180
                     AND p.latitude BETWEEN -90 AND 90
                        THEN ST_SetSRID(ST_Point(p.longitude, p.latitude), 4326)
                    ELSE NULL
                END AS geom,
                p.local_authority_ons_district,
                p.lsoa_of_accident_location,
                p.collision_severity_code,
                p.collision_severity_label,
                p.number_of_vehicles,
                p.number_of_casualties,
                p.speed_limit_code,
                p.speed_limit_label,
                p.road_type_code,
                p.road_type_label,
                p.light_conditions_code,
                p.light_conditions_label,
                p.weather_conditions_code,
                p.weather_conditions_label,
                p.road_surface_conditions_code,
                p.road_surface_conditions_label,
                p.vehicle_count,
                p.vehicle_type_counts,
                p.avg_driver_age,
                p.driver_sex_counts,
                p.casualty_count,
                p.casualty_severity_counts,
                p.casualty_type_counts,
                COALESCE((p.casualty_severity_counts ->> 'Fatal')::integer, 0),
                COALESCE((p.casualty_severity_counts ->> 'Serious')::integer, 0),
                COALESCE((p.casualty_severity_counts ->> 'Slight')::integer, 0)
            FROM parsed p
            WHERE p.collision_index IS NOT NULL
              AND p.collision_date IS NOT NULL;

            DROP TABLE IF EXISTS _collision_stage;
            """
        )


def snap_crime(max_snap_distance_m: float) -> None:
    with StageTimer("snap crime events"):
        psql(
            f"""
            SET statement_timeout = 0;

            WITH nearest AS (
                SELECT
                    ce.id AS event_id,
                    rs.id AS segment_id,
                    ST_Distance(rs.geom_4326::geography, ce.geom::geography) AS snap_distance_m
                FROM crime_events ce
                JOIN LATERAL (
                    SELECT r.id, r.geom_4326
                    FROM road_segments r
                    ORDER BY r.geom_4326 <-> ce.geom
                    LIMIT 1
                ) rs ON TRUE
                WHERE ce.geom IS NOT NULL
            )
            UPDATE crime_events ce
            SET
                segment_id = CASE
                    WHEN n.snap_distance_m <= {max_snap_distance_m} THEN n.segment_id
                    ELSE NULL
                END,
                snap_distance_m = n.snap_distance_m
            FROM nearest n
            WHERE ce.id = n.event_id;
            """
        )


def snap_collisions(max_snap_distance_m: float) -> None:
    with StageTimer("snap collision events"):
        psql(
            f"""
            SET statement_timeout = 0;

            WITH nearest AS (
                SELECT
                    ce.collision_index AS event_id,
                    rs.id AS segment_id,
                    ST_Distance(rs.geom_4326::geography, ce.geom::geography) AS snap_distance_m
                FROM collision_events ce
                JOIN LATERAL (
                    SELECT r.id, r.geom_4326
                    FROM road_segments r
                    ORDER BY r.geom_4326 <-> ce.geom
                    LIMIT 1
                ) rs ON TRUE
                WHERE ce.geom IS NOT NULL
            )
            UPDATE collision_events ce
            SET
                segment_id = CASE
                    WHEN n.snap_distance_m <= {max_snap_distance_m} THEN n.segment_id
                    ELSE NULL
                END,
                snap_distance_m = n.snap_distance_m
            FROM nearest n
            WHERE ce.collision_index = n.event_id;
            """
        )


def rebuild_crime_stats() -> None:
    with StageTimer("rebuild segment_month_type_stats"):
        psql(
            """
            SET statement_timeout = 0;
            TRUNCATE TABLE segment_month_type_stats;

            INSERT INTO segment_month_type_stats (
                segment_id,
                month,
                crime_type,
                crime_count
            )
            SELECT
                ce.segment_id,
                ce.month,
                COALESCE(NULLIF(ce.crime_type, ''), 'Unknown') AS crime_type,
                COUNT(*)::integer AS crime_count
            FROM crime_events ce
            WHERE ce.segment_id IS NOT NULL
            GROUP BY ce.segment_id, ce.month, COALESCE(NULLIF(ce.crime_type, ''), 'Unknown');
            """
        )


def rebuild_collision_stats() -> None:
    with StageTimer("rebuild segment_month_collision_stats"):
        psql(
            """
            SET statement_timeout = 0;
            TRUNCATE TABLE segment_month_collision_stats;

            INSERT INTO segment_month_collision_stats (
                segment_id,
                month,
                collision_count,
                casualty_count,
                fatal_casualty_count,
                serious_casualty_count,
                slight_casualty_count
            )
            SELECT
                ce.segment_id,
                ce.month,
                COUNT(*)::integer AS collision_count,
                COALESCE(SUM(ce.number_of_casualties), 0)::integer AS casualty_count,
                COALESCE(SUM(ce.fatal_casualty_count), 0)::integer AS fatal_casualty_count,
                COALESCE(SUM(ce.serious_casualty_count), 0)::integer AS serious_casualty_count,
                COALESCE(SUM(ce.slight_casualty_count), 0)::integer AS slight_casualty_count
            FROM collision_events ce
            WHERE ce.segment_id IS NOT NULL
            GROUP BY ce.segment_id, ce.month;
            """
        )


def analyze_tables() -> None:
    with StageTimer("analyze tables"):
        psql(
            """
            ANALYZE road_segments;
            ANALYZE crime_events;
            ANALYZE collision_events;
            ANALYZE segment_month_type_stats;
            ANALYZE segment_month_collision_stats;
            """
        )


def seed_reference_bboxes() -> None:
    """Seed static reference bboxes used by watchlist risk-score comparison fallback."""
    with StageTimer("seed risk_score_reference_bboxes"):
        labels = ", ".join(f"'{item['label']}'" for item in DEFAULT_REFERENCE_BBOXES)
        values = ",\n                ".join(
            (
                f"('{item['label']}', {item['min_lon']}, {item['min_lat']}, "
                f"{item['max_lon']}, {item['max_lat']}, TRUE)"
            )
            for item in DEFAULT_REFERENCE_BBOXES
        )
        psql(
            f"""
            SET statement_timeout = 0;

            CREATE TABLE IF NOT EXISTS risk_score_reference_bboxes (
                id BIGSERIAL PRIMARY KEY,
                label TEXT NOT NULL,
                min_lon DOUBLE PRECISION NOT NULL,
                min_lat DOUBLE PRECISION NOT NULL,
                max_lon DOUBLE PRECISION NOT NULL,
                max_lat DOUBLE PRECISION NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT risk_score_reference_bboxes_lon_chk CHECK (min_lon < max_lon),
                CONSTRAINT risk_score_reference_bboxes_lat_chk CHECK (min_lat < max_lat)
            );

            DELETE FROM risk_score_reference_bboxes
            WHERE label IN ({labels});

            INSERT INTO risk_score_reference_bboxes (
                label, min_lon, min_lat, max_lon, max_lat, active
            )
            VALUES
                {values};
            """
        )


def print_summary() -> None:
    with StageTimer("summary"):
        rows = psql_scalar(
            """
            SELECT json_build_object(
                'road_segments', (SELECT COUNT(*) FROM road_segments),
                'crime_events', (SELECT COUNT(*) FROM crime_events),
                'collision_events', (SELECT COUNT(*) FROM collision_events),
                'segment_month_type_stats', (SELECT COUNT(*) FROM segment_month_type_stats),
                'segment_month_collision_stats', (SELECT COUNT(*) FROM segment_month_collision_stats),
                'crime_snapped', (SELECT COUNT(*) FROM crime_events WHERE segment_id IS NOT NULL),
                'collision_snapped', (SELECT COUNT(*) FROM collision_events WHERE segment_id IS NOT NULL),
                'crime_min_month', (SELECT MIN(month) FROM crime_events),
                'crime_max_month', (SELECT MAX(month) FROM crime_events),
                'collision_min_month', (SELECT MIN(month) FROM collision_events),
                'collision_max_month', (SELECT MAX(month) FROM collision_events),
                'reference_bboxes', (
                    SELECT COUNT(*) FROM risk_score_reference_bboxes WHERE active = TRUE
                )
            )::text;
            """
        )
        log(f"Initialization summary: {rows}")


def main() -> int:
    args = parse_args()
    total_start = time.perf_counter()

    missing = ensure_required_files(args.skip_roads, args.skip_crime, args.skip_collisions)
    if missing:
        log("Missing required input files:")
        for path in missing:
            log(f"  - {path}")
        return 1

    try:
        with StageTimer("docker db checks"):
            ensure_db_container_ready(require_roads_file=not args.skip_roads)

        require_force_if_initialized(args.force)
        if args.force:
            drop_pipeline_tables()

        create_pipeline_tables()

        if not args.skip_roads:
            import_roads()
            build_road_segments()

        if not args.skip_crime:
            crime_files = list(CRIME_DIR.glob("*.csv"))
            import_crime(crime_files)
            if not args.skip_roads:
                snap_crime(args.max_snap_distance_m)
            rebuild_crime_stats()

        if not args.skip_collisions:
            import_collisions()
            if not args.skip_roads:
                snap_collisions(args.max_snap_distance_m)
            rebuild_collision_stats()

        seed_reference_bboxes()

        analyze_tables()
        print_summary()

        elapsed = time.perf_counter() - total_start
        log(f"All done. Total execution time: {elapsed:.2f}s")
        return 0
    except subprocess.CalledProcessError as exc:
        log(f"Command failed (exit {exc.returncode}): {exc.cmd}")
        return 1
    except Exception as exc:  # pragma: no cover - operational path
        log(f"Initialization failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
