# Watchlist Analytics Forecast Algorithm

This document explains the next-month forecast algorithm used by:

- `POST /watchlists/{watchlist_id}/analytics/forecast`

## 1) Endpoint and Auth

- Route: `POST /watchlists/{watchlist_id}/analytics/forecast`
- Auth: required (Bearer token)
- Scope source: bbox is always read from the watchlist (`min_lon`, `min_lat`, `max_lon`, `max_lat`)
- Persistence: forecast results are **not** persisted

## 2) Input Contract

Request body:

- `startMonth` (required, `YYYY-MM`)
- `mode` (optional, default `walk`; supports aliases that normalize to `walk`/`drive`)
- `crimeTypes` (optional array; empty means all crime types)

Notes:

- Snake case and camel case are both accepted by the schema (`start_month`/`startMonth`, `crime_types`/`crimeTypes`).

## 3) Validation Rules

1. Watchlist must exist and belong to the authenticated user.
2. Watchlist bbox must be valid:
   - `min_lon < max_lon`
   - `min_lat < max_lat`
3. `startMonth` must parse as `YYYY-MM`.
4. Baseline end month is always the **last complete month**.
5. `startMonth <= last_complete_month`.
6. Month span from `startMonth` to baseline end must be `> 0`.

## 4) Monthly Baseline History

For each month `m` in `[startMonth, baseline_end]`, the repository builds:

### 4.1 Crime monthly signal

`crime_count_m = official_crime_count_m + user_reported_crime_signal_m`

Optional `crimeTypes` filter applies to:

- official crime rows
- user-reported crime detail rows

### 4.2 User-reported crime signal

`user_reported_crime_signal_m = 0.10 * min(3.0, D_m + 0.5*A_m + 0.25*max(Auth_m - D_m, 0))`

Where:

- `D_m` = distinct authenticated users
- `A_m` = anonymous reports
- `Auth_m` = authenticated reports

### 4.3 Collision monthly metrics

- `collision_count_m = count(collision events)`
- `collision_points_m = collisions_m + 0.5*slight_m + 2.0*serious_m + 5.0*fatal_m`

## 5) Recency-Weighted Forecast Means

Forecast always predicts the month immediately after baseline end.

Recency weights:

`w_i = 0.85^i`

Where:

- `i = 0` is most recent baseline month
- `i = 1` is previous month
- etc.

Weighted means:

- `mu_crime = weighted_mean(crime_count_m)`
- `mu_collision_points = weighted_mean(collision_points_m)`
- `mu_collision_count = weighted_mean(collision_count_m)`

Point estimates:

- `expected_crime_count = round(mu_crime)`
- `expected_collision_count = round(mu_collision_count)`
- `expected_collision_points = mu_collision_points`

## 6) Uncertainty Intervals (Poisson-style)

Crime:

- `sigma_c = 1.96 * sqrt(max(mu_crime, 1e-9))`
- `low_c = floor(max(0, mu_crime - sigma_c))`
- `high_c = ceil(mu_crime + sigma_c)`

Collision count:

- `sigma_k = 1.96 * sqrt(max(mu_collision_count, 1e-9))`
- `low_k = floor(max(0, mu_collision_count - sigma_k))`
- `high_k = ceil(mu_collision_count + sigma_k)`

## 7) Projected Combined Value, Score, and Band

Mode weights come from existing scoring logic:

- walk: `w_crime=0.65`, `w_collision=0.25`
- drive: `w_crime=0.40`, `w_collision=0.50`

Projected combined value:

`projected_combined_value = w_crime * mu_crime + w_collision * mu_collision_points`

Baseline combined monthly values:

`combined_m = w_crime * crime_count_m + w_collision * collision_points_m`

`baseline_combined_mean = weighted_mean(combined_m)`

Ratio:

`ratio = projected_combined_value / max(baseline_combined_mean, 1e-9)`

Score:

`score = round(100 * (1 - exp(-max(projected_combined_value, 0) / 1.6)))`

Band (conservative):

- `red` if `score >= 60`
- `amber` if `35 <= score < 60`
- `green` otherwise

## 8) Response Shape

```json
{
  "generated_at": "2026-03-14T20:44:54Z",
  "forecast": {
    "score": 100,
    "band": "red",
    "expected_crime_count": 56,
    "expected_collision_count": 0,
    "expected_collision_points": 0.4037,
    "intervals": {
      "crimes": { "low": 41, "high": 71 },
      "collisions_count": { "low": 0, "high": 2 }
    },
    "components": {
      "mu_crime": 56.2241,
      "mu_collision_points": 0.4037,
      "mu_collision_count": 0.2003,
      "projected_combined_value": 36.687,
      "baseline_combined_mean": 36.687,
      "ratio": 1.0
    }
  }
}
```

## 9) Implementation Map

- API: `backend/app/api/watchlist_analytics.py`
- Service: `backend/app/services/watchlist_analytics_service.py`
- Repository: `backend/app/api_utils/watchlist_analytics_repository.py`
- Schemas: `backend/app/schemas/watchlist_analytics_schemas.py`
