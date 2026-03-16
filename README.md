# Leeds Urban Safety Analytics API

Leeds Urban Safety Analytics API is a FastAPI + PostGIS backend that integrates official UK safety datasets and moderated user-reported incidents into one explainable analytics platform for Leeds/West Yorkshire.

This repository focuses on spatial risk analytics over road segments and area windows, with watchlist-based persistence for reproducible user workflows.

## Project Overview

### Problem
UK safety-related open data is fragmented across separate sources (crime, roads, collisions). Cross-dataset analysis over consistent spatial units and month windows is difficult without a unified data model and query layer.

### Objectives
- Integrate crime, road network, and collision datasets into a spatial database.
- Precompute monthly segment aggregates for fast analytical queries.
- Support moderated user-reported incidents and include approved reports as bounded auxiliary signal.
- Expose explainable analytics for risk scoring, patterns, and forecasting.
- Support watchlists so users can re-run analytics without repeatedly sending geometry and preferences.

### Scope
- Geography is deliberately scoped to Leeds / West Yorkshire.

## Data Sources

- UK Police street-level crime data.
- OpenStreetMap road network data.
- STATS19 collisions (Department for Transport).
- First-party user-reported events (anonymous/authenticated, moderation workflow).

## Data Model and Storage Strategy

The backend uses a layered storage design:

- Raw/core spatial tables for roads and events.
- Snapped event linkage via `segment_id` where relevant.
- Pre-aggregated monthly segment tables for analytics:
  - `segment_month_type_stats`
  - `segment_month_collision_stats`
- Application entities:
  - `users`
  - `watchlists`
  - `user_reported_events` (+ subtype detail tables)
- Persisted analytics tables:
  - `risk_score_runs`
  - `risk_score_reference_bboxes`

This design reduces repeated runtime spatial joins and improves explainability and reproducibility.

## System Architecture

### Stack
- API framework: FastAPI
- Database: PostgreSQL + PostGIS
- SQL layer: SQLAlchemy Core + explicit parameterized SQL

### Architectural Rationale
- Typed request/response contracts and automatic OpenAPI docs.
- Spatial operations and indexing handled directly in PostGIS.
- Explicit SQL prioritizes transparency and query control for analytics-heavy paths.

### File and Folder Map

```text
.
├── backend/
│   ├── app/
│   │   ├── api/                              # FastAPI routes (thin handlers)
│   │   │   ├── auth.py
│   │   │   ├── watchlist.py
│   │   │   ├── watchlist_analytics.py
│   │   │   ├── report_events.py
│   │   │   └── tiles.py
│   │   ├── services/                         # Business logic / orchestration
│   │   │   ├── auth_service.py
│   │   │   ├── watchlist_service.py
│   │   │   ├── watchlist_analytics_service.py
│   │   │   ├── report_events_service.py
│   │   │   └── tile_service.py
│   │   ├── api_utils/                        # Repository-style SQL access
│   │   │   ├── watchlist_repository.py
│   │   │   ├── watchlist_analytics_repository.py
│   │   │   ├── report_events_repository.py
│   │   │   └── tiles_repository.py
│   │   ├── schemas/                          # Pydantic request/response models
│   │   │   ├── watchlist_schemas.py
│   │   │   ├── watchlist_analytics_schemas.py
│   │   │   ├── report_event_schemas.py
│   │   │   └── tiles_schemas.py
│   │   ├── main.py                           # App factory, router wiring, global handlers
│   │   ├── bootstrap.py                      # DB schema bootstrap/index creation
│   │   ├── db.py                             # Engine/session + DB execute wrapper
│   │   └── errors.py                         # Typed app error hierarchy
│   ├── scripts/
│   │   ├── init_database.py                  # Raw data initializer (roads + crime + collisions)
│   │   └── test_risk_score.py                # Standalone algorithm test runner
│   └── tests/
│       ├── smoke_tests.py
│       ├── integration_tests/
│       └── unit_tests/
├── docs/
│   ├── watchlist-analytics-risk-score.md
│   ├── analytics-risk-score-api-diagrams.md
│   └── analytics-risk-forecast-api-diagrams.md
└── docker-compose.yml
```

Design pattern used across backend:
- API layer: HTTP contract + auth dependency + response model.
- Service layer: business rules and orchestration.
- Repository (`api_utils`) layer: SQL and persistence.
- Schema layer: typed payload models for docs and validation.

### Error Handling Model
- Centralized `AppError` hierarchy in `backend/app/errors.py`.
- Global exception handlers normalize API errors into stable JSON shape:
  - `error`
  - `message`
  - `details` (optional)
- DB failures are translated into dependency-style errors rather than leaking raw driver traces.

## API Surface (High-Level)

- `auth`: registration, login, profile.
- `watchlists`: CRUD for bbox + preference management.
- `watchlist-analytics`: persisted risk score compute + historical run retrieval.
- `reported-events`: create/list/moderate user reports.
- `analytics`: legacy/general analytical endpoints.
- `tiles`: vector tile and map-facing risk overlays.

## Documentation

- API Documentation (PDF): [APIDocs.pdf](./APIDocs.pdf)
- Risk score docs: `docs/watchlist-analytics-risk-score.md`
- Risk score diagrams/index: `docs/analytics-risk-score-api-diagrams.md`

## Assessor Quick Start

### Prerequisites
- Docker Desktop installed.
- Docker Desktop running before executing any `docker compose` commands.

### Setup and Start (Recommended Order)

From repository root, run these commands in order:

```bash
make up-db
make init-db-force
make up-app
```

What each command does:
- `make up-db`: starts PostgreSQL/PostGIS.
- `make init-db-force`: performs a full deterministic data initialization from raw datasets.
- `make up-app`: starts FastAPI + frontend.

Initializer input files required:
- `data/wyosm.pbf`
- `data/crime/*.csv`
- `data/master-collision-dataset.csv`

### Expected Runtime

- `make init-db-force` is expected to take some time on first run.
- Most expensive stages are:
  - road import from `wyosm.pbf` (`osm2pgsql`)
  - nearest-road snapping for crime events
- Laptop fan activity during this stage is normal.

### Fast/Quiet Usage Tips (Assessor Friendly)

- Do not rebuild images every run:
  - use `docker compose up -d db`
  - use `docker compose up --build -d db` only after Dockerfile changes
- Do not use `--force` unless you need a full refresh.
- Use partial refresh flags when needed:
  - `--skip-roads --skip-collisions` (crime-only refresh)
  - `--skip-roads --skip-crime` (collision-only refresh)
  - `--skip-crime --skip-collisions` (roads-only refresh)

### Why You See `TRUNCATE TABLE`

`TRUNCATE` during initialization is expected.

- With `--force`, the script intentionally rebuilds pipeline tables deterministically.
- It clears target/staging tables before reloading so there are no duplicates or stale aggregates.
- Without `--force`, the script exits if data already exists.

### Verify Services

```bash
make ps
```

Expected: `db` is healthy and both `api` and `frontend` are up.

### Open Frontend and API Docs

- Frontend: `http://localhost:5173`
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- API docs PDF (repo): `APIDocs.pdf`
- Analytics docs files:
  - `docs/watchlist-analytics-risk-score.md`
  - `docs/analytics-risk-score-api-diagrams.md`

### Login and Use the Frontend

We strongly recommend using the frontend for assessment because it exercises the full API + map workflow.

Login is required for profile and user-scoped features.

- Email: `admin@admin.com`
- Password: `adminpassword`

After logging in on the frontend, you can:
- create and manage watchlists,
- run and view watchlist analytics,
- browse reported events and related map data.

Swagger (`/docs`) is useful for inspecting endpoint contracts directly.

### Shutdown

Stop and remove containers:

```bash
docker compose down
```

Stop without removing:

```bash
docker compose stop
```

### If Docker Daemon Is Not Running

If you see:
- `Cannot connect to the Docker daemon ...`

Then:
1. Start Docker Desktop.
2. Run `docker info` to verify daemon access.
3. Retry `docker compose up -d db`.

## Risk Scoring Design (Current Watchlist Analytics)

Watchlist risk scoring is explainable, persisted, and comparison-aware:
- Loads bbox + month window + preferences from the watchlist.
- Computes crime, collision, and bounded user-report support signals.
- Blends signals by mode (`walk` / `drive`) into `raw_score`, then normalizes to `0..100`.
- Persists runs and compares against same-signature historical runs (or reference bboxes when history is insufficient).

For detailed diagrams and formulas, see:
- `docs/watchlist-analytics-risk-score.md`
- `docs/analytics-risk-score-api-diagrams.md`

## Forecasting

Forecasting is intentionally lightweight:
- baseline-history mean with simple uncertainty bounds,
- strict month-coverage checks before returning output.

## Testing and Reliability

### How To Run Tests

Run tests from repository root.

Activate your Python environment first (example):

```bash
source venv/bin/activate
```

Run the full suite:

```bash
pytest
```

Run smoke tests only:

```bash
pytest backend/tests/smoke_tests.py
```

Run integration tests only:

```bash
pytest backend/tests/integration_tests
```

Run unit tests only:

```bash
pytest backend/tests/unit_tests
```

### Testing Strategy

The test suite uses a layered approach:

- Smoke tests for fast route-level health/regression checks.
- Integration tests across route + service + repository boundaries.
- Unit tests for deterministic business rules and helpers.

At report time, testing was tracked as:
- 65 total tests
- 5 smoke
- 35 integration
- 25 unit

Coverage emphasis is behavior- and contract-focused (validation, ownership, workflows), not only line percentage.

### Reliability Practices

- Centralized error normalization.
- Ownership validation on user-scoped resources.
- SQL parameterization and DB exception translation.
- Spatial query constraints and pre-aggregations to reduce expensive runtime scans.
