# Analytics Risk Score API - Detailed Flow Diagrams

This document explains how `POST /analytics/risk/score` works in the current codebase.

## 1) Endpoint Entry Point

The FastAPI route in `backend/app/api/analytics.py` is intentionally thin:
- It accepts a `RiskScoreRequest` body.
- It passes fields directly into `build_risk_score_payload(...)`.
- It returns the payload as `RiskScoreResponse`.

```mermaid
flowchart LR
    A["Client POST /analytics/risk/score"] --> B["FastAPI route analytics_risk_score()\nbackend/app/api/analytics.py"]
    B --> C["build_risk_score_payload(...)\nbackend/app/api_utils/analytics_db_utils.py"]
    C --> D["Validate + normalize input"]
    D --> E["Compute area metrics"]
    D --> F["Compute segment metrics"]
    E --> G["Assemble score + metrics + explain"]
    F --> G
    G --> H["RiskScoreResponse JSON"]
```

## 2) Request Contract

`RiskScoreRequest` fields:
- `from`, `to` month window (YYYY-MM)
- `minLon`, `minLat`, `maxLon`, `maxLat` bbox
- optional `crimeType`
- `includeCollisions` boolean
- `mode` (`walk` or `drive`)
- `weights.w_crime`, `weights.w_collision`

Rules applied by service logic:
- month range must be valid and <= 24 months
- bbox min values must be < max values
- `mode` must be `walk` or `drive`
- collisions only allowed when `mode=drive`
- applied collision weight is forced to `0` unless collisions are enabled in `drive`

```mermaid
flowchart TD
    A["build_risk_score_payload()"] --> B["_validate_month_window(from,to)"]
    A --> C["_validate_bbox(minLon,minLat,maxLon,maxLat)"]
    A --> D["normalize mode + crimeType"]
    D --> E{"mode in {walk,drive}?"}
    E -- "no" --> X1["400 INVALID_MODE"]
    E -- "yes" --> F{"includeCollisions && mode != drive?"}
    F -- "yes" --> X2["400 INVALID_MODE_FOR_COLLISIONS"]
    F -- "no" --> G["_applied_collision_weight(...)"]
    G --> H["Continue to DB aggregation"]
```

## 3) Data Sources and Aggregation Path

The endpoint computes two parallel aggregates:
- area-level totals (`_risk_score_area_metrics`)
- segment-level network stats (`_risk_score_segment_metrics`)

```mermaid
flowchart LR
    subgraph AreaMetrics["_risk_score_area_metrics"]
      CE1["crime_events\n(count in bbox + month window)"]
      URE1["user_reported_events + user_reported_crime_details\n(admin_approved only)"]
      COL1["collision_events\n(count + severity points)"]
      CE1 --> AR["area_row"]
      URE1 --> AR
      COL1 --> AR
      BBOX1["bbox envelope -> area_km2"] --> AR
    end

    subgraph SegmentMetrics["_risk_score_segment_metrics"]
      RS["road_segments"]
      SMTS["segment_month_type_stats\n(official crime by segment)"]
      URE2["user_reported_events + user_reported_crime_details\n(weighted user signal)"]
      SMCS["segment_month_collision_stats"]
      RS --> SG["scored CTE"]
      SMTS --> SG
      URE2 --> SG
      SMCS --> SG
      SG --> RK["ranked CTE\ncombined_density + percent_rank"]
      RK --> SS["scope_stats CTE\navg_* + red_segment_share"]
      RK --> PS["pct_stats CTE\navg_density_pct"]
      SS --> SR["segment_row"]
      PS --> SR
    end

    AR --> OUT["build_risk_score_payload response assembly"]
    SR --> OUT
```

## 4) Core Scoring Math

### 4.1 User-reported crime signal (per grouped unit)

The SQL uses capped weighted signal:
- base distinct authenticated users count as `1.0`
- anonymous reports are down-weighted by `0.5`
- repeat authenticated reports are down-weighted by `0.25`
- total inner signal is capped at `3.0`
- final user signal multiplier is `0.10`

Equivalent formula:

```text
user_signal = 0.10 * min(
  3.0,
  distinct_authenticated_users
  + 0.5 * anonymous_reports
  + 0.25 * max(authenticated_reports - distinct_authenticated_users, 0)
)
```

### 4.2 Per-segment densities

```text
normalized_km = max(length_m, 100) / 1000
crime_density = (official_crimes + user_reported_crime_signal) / normalized_km
collision_density = (
  collisions
  + 0.5 * slight_casualties
  + 2.0 * serious_casualties
  + 5.0 * fatal_casualties
) / normalized_km
combined_density = w_crime * crime_density + w_collision_applied * collision_density
```

### 4.3 Percentile and final score

- `percent_rank()` is computed over all segments on `combined_density`.
- For the selected bbox, `avg_density` is computed.
- `avg_density_pct` is the share of ranked rows with density <= bbox average.
- final score:

```text
pct = round(avg_density_pct, 4)
score = round(100 * pct)
```

Band thresholds:
- `red` if `pct >= 0.95`
- `amber` if `0.80 <= pct < 0.95`
- `green` otherwise

```mermaid
flowchart TD
    A["Segment inputs\ncrime + user signal (+ collisions optional)"] --> B["Density per segment"]
    B --> C["combined_density = w_crime*crime_density + w_collision_applied*collision_density"]
    C --> D["percent_rank over network"]
    D --> E["Compute bbox avg_density"]
    E --> F["avg_density_pct"]
    F --> G["score = round(100 * pct)"]
    G --> H{"pct band"}
    H -->|">= 0.95"| I["red"]
    H -->|">= 0.80"| J["amber"]
    H -->|"< 0.80"| K["green"]
```

## 5) Response Assembly

Response fields include:
- `scope` (from/to, bbox, mode, crimeType, includeCollisions)
- `generated_at`
- `score_basis` (`crime` or `crime+collision`)
- `risk_score`, `score`, `pct`, `band`
- `metrics` block (area rates, segment averages, optional collision metrics)
- `explain` block (human-readable interpretation)

```mermaid
sequenceDiagram
    participant Client
    participant API as analytics.py /risk/score
    participant Builder as build_risk_score_payload
    participant DB as Postgres/PostGIS

    Client->>API: POST request (window+bbox+mode+weights)
    API->>Builder: Forward request fields
    Builder->>Builder: Validate month window, bbox, mode
    Builder->>DB: Query _risk_score_area_metrics
    DB-->>Builder: area_row totals/rates
    Builder->>DB: Query _risk_score_segment_metrics
    DB-->>Builder: segment_row percentile anchors
    Builder->>Builder: Compute pct, score, band, metrics
    Builder-->>API: RiskScoreResponse payload
    API-->>Client: 200 JSON
```

## 6) Error Surface

Typical typed errors:
- `400 INVALID_MONTH_FORMAT`
- `400 INVALID_DATE_RANGE`
- `400 RANGE_TOO_LARGE`
- `400 INVALID_BBOX`
- `400 INVALID_MODE`
- `400 INVALID_MODE_FOR_COLLISIONS`
- `503 DB_UNAVAILABLE` (DB execution failures wrapped by `_execute`)

## 7) Practical Reading of the Score

- The score is percentile-like, not raw incident count.
- A higher score means the selected bbox's average segment density sits higher relative to the overall ranked segment distribution.
- User reports affect crime signal, but with strict low-weight and cap controls.
- Collision effects are opt-in and only active in `drive` mode.
