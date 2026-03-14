# Tile Risk Calculation Algorithm (v2)

This document describes how `GET /tiles/roads/{z}/{x}/{y}.mvt` computes risk after the v2 update.

## API Contract

Query parameters:
- `startMonth` (optional, `YYYY-MM`)
- `endMonth` (optional, `YYYY-MM`)
- `crimeType` (optional)
- `crime` (optional boolean)
- `collisions` (optional boolean)
- `userReportedEvents` (optional boolean)

Removed parameters:
- `month` is removed. If provided, API returns `400`.
- `includeRisk` is removed.

Risk-mode rules:
- If all toggles are omitted/false, the endpoint returns roads-only tiles.
- If any toggle is `true`, both `startMonth` and `endMonth` are required.

## Scoring Backbone (Retained from v1)

The final score still follows the original method:
1. Build per-segment risk components.
2. Convert each enabled component to percentile via `percent_rank()`.
3. Weighted combine enabled component percentiles.
4. Normalize again with `percent_rank()` to get final `risk_score` (0-100).
5. Banding:
   - `green`: `< 30`
   - `orange`: `>= 30` and `< 50`
   - `red`: `>= 50`

## Component Metrics

### 1) Crime Component

`CrimeRisk_i = alpha * HarmDensity_i + (1 - alpha) * Persistence_i`

Where:
- `HarmDensity_i = SUM_t,c(recency_weight_crime(t) * harm_weight(c) * count_i,t,c) / exposure_i`
- `Persistence_i = active_official_crime_months_i / months_in_window`
- `exposure_i = max(length_m, risk_length_floor_m) / 1000`

Notes:
- Harm weighting uses fixed code constants by police crime type.
- Recency weight is exponential: `exp(-lambda * age_in_months)`.
- Persistence uses official crime months only.

### 2) Collision Component

Base density:
- `collision_severity_points = collisions + slight*w_slight + serious*w_serious + fatal*w_fatal`
- `collision_density = SUM_t(recency_weight_collision(t) * collision_severity_points_t) / exposure_i`

Road-context multiplier:
- `CollisionRisk_i = collision_density_i * RoadFactor_i`
- `RoadFactor_i = RoadClassFactor_i * JunctionFactor_i * CurveFactor_i`

Road-factor inputs:
- `RoadClassFactor`: based on OSM highway class.
- `JunctionFactor`: endpoint-nearby segment count proxy.
- `CurveFactor`: sinuosity proxy (`segment_length / endpoint_distance`).

### 3) User-Reported Events Component

User reports are clustered first, then scored:
- Cluster key: same day + same crime type + same segment neighborhood
- Segment neighborhood includes same segment and nearby segments (distance threshold)

Cluster score:
- `score_k = min(cap, a*D_k + b*A_k + c*R_k)`
  - `D_k`: distinct authenticated users
  - `A_k`: anonymous reports
  - `R_k`: repeat authenticated reports

Segment user risk:
- `UserRisk_i = SUM_k(recency_weight_user_reports(t_k) * score_k)`

## Toggle-Aware Fusion

Each component is independent. Disabled components are excluded entirely.

Let enabled components be subset `E`.

`combined_pct_i = SUM_{j in E}(w_j * pct_{i,j}) / SUM_{j in E}(w_j)`

Then final score:
- `risk_score_i = 100 * percent_rank(combined_pct_i)`

This keeps results comparable while respecting user-selected toggles.

## Tile Output Fields

Risk output is intentionally minimal:
- `risk_score`
- `band`

Road identity fields remain for rendering:
- `segment_id`
- `highway`
- `name`
- geometry (`geom`)
