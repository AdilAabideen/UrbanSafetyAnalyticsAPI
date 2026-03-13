# Backend Endpoints Summary

Base URL:
- `http://127.0.0.1:8000`

Docs:
- Swagger UI: `/docs`
- OpenAPI JSON: `/openapi.json`

This file summarizes the current live backend endpoint surface.

## Core

- `GET /`
  Returns the root API info payload.

- `GET /health`
  Returns a simple health response.

## Auth

- `POST /auth/register`
  Create a user account.

- `POST /auth/login`
  Login and receive a bearer JWT.

- `GET /me`
  Get the currently authenticated user.
  Protected: yes.

- `PATCH /me`
  Update the current user email and/or password.
  Protected: yes.

## Global Discoverability

- `GET /analytics/meta`
  Global metadata endpoint for available months, crime types, and top-level dataset counts.

## Advanced Analytics

- `POST /analytics/risk/score`
  Area risk score for a bbox and time window. Stores a snapshot row and returns `snapshot_id`.

- `POST /analytics/risk/forecast`
  Baseline-window forecast for a bbox and target month. Stores a snapshot row and returns `snapshot_id`.

- `GET /analytics/patterns/hotspot-stability`
  Month-to-month hotspot persistence for a bbox/time window. Stores a snapshot row and returns `snapshot_id`.

## Crimes

- `GET /crimes/incidents`
  Paginated crime list for tables and lists.

- `GET /crimes/map`
  Crime map endpoint for points or clusters inside a bbox.

- `GET /crimes/analytics/summary`
  Crime summary facts for a filtered selection.

- `GET /crimes/analytics/timeseries`
  Monthly crime time series for charts.

- `GET /crimes/{crime_id}`
  Single crime detail as GeoJSON.

## Collisions

- `GET /collisions/incidents`
  Paginated collision list for tables and lists.

- `GET /collisions/map`
  Collision map endpoint for points or clusters inside a bbox.

- `GET /collisions/analytics/summary`
  Collision summary facts for a filtered selection.

- `GET /collisions/analytics/timeseries`
  Monthly collision time series for charts.

## Roads

- `GET /roads/analytics/meta`
  Road filter metadata and coverage counts.

- `GET /roads/analytics/overview`
  High-level road facts, KPI cards, band breakdown, and insight messages.

- `GET /roads/analytics/charts`
  Chart payloads for time series and bar-chart style breakdowns.

- `GET /roads/analytics/risk`
  Ranked risky roads table for the current filters.

## Road Tiles

- `GET /tiles/roads/{z}/{x}/{y}.mvt`
  Mapbox Vector Tile for roads.

- `GET /tiles/roads/{z}/{x}/{y}.pbf`
  Same road tile payload exposed with `.pbf`.

## LSOA

- `GET /lsoa/categories`
  Distinct LSOA filter values with counts and min/max lat/lon bounds.

## Watchlists

- `GET /watchlists`
  List the current user’s watchlists, or fetch one by `watchlist_id`.
  Protected: yes.

- `POST /watchlists`
  Create a watchlist for the current user.
  Protected: yes.

- `PATCH /watchlists/{watchlist_id}`
  Update a watchlist and its preference.
  Protected: yes.

- `DELETE /watchlists/{watchlist_id}`
  Delete a watchlist.
  Protected: yes.

## Current Route Count

- Core: `2`
- Auth: `4`
- Global discoverability: `1`
- Advanced analytics: `3`
- Crimes: `5`
- Collisions: `4`
- Roads analytics: `4`
- Road tiles: `2`
- LSOA: `1`
- Watchlists: `4`

Total current endpoints: `30`
