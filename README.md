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
│   │   │   ├── analytics.py
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
│   │   │   ├── analytics_schemas.py
│   │   │   └── tiles_schemas.py
│   │   ├── main.py                           # App factory, router wiring, global handlers
│   │   ├── bootstrap.py                      # DB schema bootstrap/index creation
│   │   ├── db.py                             # Engine/session + DB execute wrapper
│   │   └── errors.py                         # Typed app error hierarchy
│   ├── scripts/
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

### Start Everything (API + DB + Frontend)

From repository root:

```bash
docker compose up --build -d
```

This starts:
- PostgreSQL/PostGIS on `localhost:5432`
- FastAPI backend on `localhost:8000`
- Vite frontend on `localhost:5173`

Note: on a fresh clone/first run, database initialization imports a bundled seed dataset and can take a few minutes before services are fully ready.

### Verify Services

```bash
docker compose ps
```

Expected: `db` is healthy and both `api` and `frontend` are up.

### Open the App and API Docs

- Frontend: `http://localhost:5173`
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Recommended Usage

We recommend you use the frontend for assessment and demonstration, rather than calling endpoints manually first.
Open `http://localhost:5173`, authenticate, then exercise map, watchlist, and analytics flows from the UI.
Use Swagger (`/docs`) only when you want to inspect request/response contracts directly.

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
3. Retry `docker compose up --build -d`.

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
