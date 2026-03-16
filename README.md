# Leeds Urban Safety Analytics API

Leeds Urban Safety Analytics API is a FastAPI + PostGIS backend that integrates official UK safety datasets and moderated user-reported incidents into one explainable analytics platform for Leeds/West Yorkshire.

This repository focuses on spatial risk analytics over road segments and area windows, with watchlist-based persistence for reproducible user workflows.

## Table of Contents

- [Project Overview](#project-overview)
- [Data Sources](#data-sources)
- [Data Model and Storage Strategy](#data-model-and-storage-strategy)
- [System Architecture](#system-architecture)
- [API Surface (High-Level)](#api-surface-high-level)
- [Documentation](#documentation)
- [MCP (Optional)](#mcp-optional)
- [Assessor Quick Start](#assessor-quick-start)
- [Risk Scoring Design (Current Watchlist Analytics)](#risk-scoring-design-current-watchlist-analytics)
- [Forecasting and Backtest](#forecasting-and-backtest)
- [Testing and Reliability](#testing-and-reliability)

### Quick Links

- [System Architecture → Stack](#stack)
- [System Architecture → File and Folder Map](#file-and-folder-map)
- [Assessor Quick Start → Prerequisites](#prerequisites)
- [Assessor Quick Start → Setup and Start (Recommended Order)](#setup-and-start-recommended-order)
- [Assessor Quick Start → Open These URLs](#open-these-urls)
- [Testing and Reliability → How To Run Tests](#how-to-run-tests)
- [Testing and Reliability → Testing Strategy](#testing-strategy)
- [Testing and Reliability → Reliability Practices](#reliability-practices)

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
│   └── tests/
│       ├── smoke_tests.py
│       ├── integration_tests/
│       └── unit_tests/
├── frontend/
├── data/
├── docker/
├── docker-compose.yml
├── Makefile
└── APIDocs.pdf
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
- `tiles`: vector tile and map-facing risk overlays.

## Documentation

- API Documentation (PDF): [APIDocs.pdf](./APIDocs.pdf)

## MCP (Optional)

A minimal MCP server tool is included to wrap the existing reported-event creation API.

- Server file: `backend/mcp/reported_events_mcp_server.py`
- Tool exposed: `create_reported_event`
- Wrapped backend route: `POST /reported-events`

Run locally (with API running):

```bash
python backend/mcp/reported_events_mcp_server.py
```

Optional env vars:
- `MCP_API_BASE_URL` (default: `http://localhost:8000`)
- `MCP_HTTP_TIMEOUT_SECONDS` (default: `15`)

## Assessor Quick Start

### Prerequisites
- Docker Desktop installed.
- Docker Desktop running before executing any `docker compose` commands.

### Initializer Input Files Required
- `data/wyosm.pbf`
- `data/crime/*.csv`
- `data/master-collision-dataset.csv`

### Setup and Start (Recommended Order)

From repository root, run these commands in order one by one:

```bash
make up-db
```
```bash
make init-db-force
```
```bash
make up-app
```

What each command does:
- `make up-db`: starts PostgreSQL/PostGIS.
- `make init-db-force`: performs a full deterministic data initialization from raw datasets.
- `make up-app`: starts FastAPI + frontend.

If `make up-db` reports missing `osm2pgsql`, run:

```bash
make recover-db
```

### Open These URLs

## Frontend: `http://localhost:5173`
## Swagger UI: `http://localhost:8000/docs`
## API Docs PDF: `APIDocs.pdf`

### Login and Use the Frontend

We strongly recommend using the frontend for the assessment because it exercises the full API + map workflow end-to-end.

Login is required for profile and user-scoped features.

- Email: `admin@admin.com`
- Password: `adminpassword`

After logging in on the frontend, you can:
- create and manage watchlists,
- run and view watchlist analytics,
- browse reported events and related map data.

### Shutdown

Stop and remove containers:

```bash
docker compose down
```

Stop without removing:

```bash
docker compose stop
```

## Risk Scoring Design (Current Watchlist Analytics)

Watchlist risk scoring is explainable, persisted, and comparison-aware:
- Loads bbox + month window + preferences from the watchlist.
- Computes crime, collision, and bounded user-report support signals.
- Blends signals by mode (`walk` / `drive`) into `raw_score`, then normalizes to `0..100`.
- Persists runs and compares against same-signature historical runs (or reference bboxes when history is insufficient).

## Forecasting and Backtest

Forecasting is intentionally lightweight:
- baseline-history mean with simple uncertainty bounds,
- strict month-coverage checks before returning output.

Backtest script:

```bash
python backend/scripts/backtest_forecast.py --watchlist-id <WATCHLIST_ID>
```

Please input the ID of the watchlist you want to evaluate.

Snapshot result from a local run (`watchlist_id=2`, `mode=walk`, `window=2021-01 -> 2026-03`, `n=60`):

- `score_mae`: `5.3500`
- `score_rmse`: `19.5521`
- `score_bias`: `0.6833`
- `crime_count_mae`: `49.1500`
- `crime_count_rmse`: `84.8225`
- `collision_count_mae`: `0.4000`
- `collision_count_rmse`: `1.0488`
- `crime_interval_coverage_pct`: `51.67`
- `collision_interval_coverage_pct`: `96.67`
- `current_model_score_mae`: `5.3500`
- `trailing_mean_score_mae`: `8.4333`

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
