# Tile Risk Calculation Algorithm

This document explains how risk is calculated for `GET /tiles/roads/{z}/{x}/{y}.mvt` when `includeRisk=true`.

## 1) Request Gate and Inputs

Risk mode is enabled only when `includeRisk=true`.

Required time filter in risk mode:
- either `month=YYYY-MM`
- or `startMonth=YYYY-MM` and `endMonth=YYYY-MM`

Optional:
- `crimeType` (filters crime and user-reported crime signal)

Validation behavior:
- tile `x,y` must be valid for zoom `z`
- cannot pass both `month` and range
- range must include both `startMonth` and `endMonth`
- `startMonth <= endMonth`

## 2) Data Sources Used

The SQL combines these tables:
- `road_segments` (geometry, segment length)
- `segment_month_type_stats` (official crimes by segment/month/type)
- `user_reported_events` + `user_reported_crime_details` (approved user-reported signal)
- `segment_month_collision_stats` (collisions and casualties by segment/month)

## 3) Constants (Current Weights)

From `backend/app/schemas/tiles_schemas.py`:
- `CRIME_WEIGHT = 0.55`
- `COLLISION_WEIGHT = 0.45`
- `SLIGHT_CASUALTY_WEIGHT = 0.5`
- `SERIOUS_CASUALTY_WEIGHT = 2.0`
- `FATAL_CASUALTY_WEIGHT = 5.0`
- `USER_REPORTED_CRIME_WEIGHT = 0.10`
- `ANONYMOUS_USER_REPORT_WEIGHT = 0.5`
- `REPEAT_AUTHENTICATED_REPORT_WEIGHT = 0.25`
- `USER_REPORTED_SIGNAL_CAP = 3.0`
- `RISK_LENGTH_FLOOR_M = 100.0`

## 4) Computation Pipeline

1. Build monthly crime totals per segment:
   - `official_crimes = SUM(segment_month_type_stats.crime_count)`
   - apply month/range filter
   - apply `crimeType` filter if provided

2. Build approved user-reported crime signal per segment:
   - per `(segment_id, month, crime_type)`:
     - `anonymous_reports`
     - `authenticated_reports`
     - `distinct_authenticated_users`
   - per-row signal:

```text
USER_REPORTED_CRIME_WEIGHT * LEAST(
  USER_REPORTED_SIGNAL_CAP,
  distinct_authenticated_users
  + (ANONYMOUS_USER_REPORT_WEIGHT * anonymous_reports)
  + (REPEAT_AUTHENTICATED_REPORT_WEIGHT
     * GREATEST(authenticated_reports - distinct_authenticated_users, 0))
)
```

   - segment-level signal is the sum of the above across grouped rows:
     - `user_reported_crime_signal = SUM(per-row signal)`

3. Build collision totals per segment:
   - `collisions`
   - `casualties`
   - `fatal_casualties`
   - `serious_casualties`
   - `slight_casualties`
   - same month/range filter as crime

4. Compute base risk metrics per segment:

```text
crimes = official_crimes + user_reported_crime_signal
normalized_km = GREATEST(length_m, RISK_LENGTH_FLOOR_M) / 1000
crimes_per_km = crimes / normalized_km

collision_severity_points =
  collisions
  + (slight_casualties  * SLIGHT_CASUALTY_WEIGHT)
  + (serious_casualties * SERIOUS_CASUALTY_WEIGHT)
  + (fatal_casualties   * FATAL_CASUALTY_WEIGHT)

collision_density = collision_severity_points / normalized_km
```

5. Restrict ranking population to active roads:
   - active roads are only segments where `crimes > 0 OR collisions > 0`

6. Percentile-rank both risk dimensions on active roads:
   - `crime_pct = percent_rank(crimes_per_km)`
   - `collision_pct = percent_rank(collision_density)`

7. Combine into weighted risk percentile:

```text
pct = (crime_pct * CRIME_WEIGHT) + (collision_pct * COLLISION_WEIGHT)
raw_safety_score = pct * 100
```

8. Normalize again across active roads:
   - `safety_score = percent_rank(raw_safety_score) * 100`
   - this produces a 0-100 final relative score

9. Banding:
   - `red` if `safety_score >= 50`
   - `orange` if `safety_score >= 30` and `< 50`
   - `green` otherwise

## 5) Important Interpretation Notes

- The score is relative percentile ranking, not an absolute danger probability.
- Ranking population is all active segments in the computed dataset, then tile geometry is clipped for output.
- Segments with no active signal get `NULL` in ranking CTEs, then are `COALESCE`d to zeros in output, which typically yields low-risk (`green`) display.
- `crimeType` affects official crime and user-reported crime signal; it does not filter collisions.

## 6) Output Attributes per Road Feature

Each vector tile feature can include:
- `crimes`, `official_crimes`, `user_reported_crime_signal`, `approved_user_reports`
- `crimes_per_km`
- `collisions`, `casualties`, `fatal_casualties`, `serious_casualties`, `slight_casualties`
- `collision_severity_points`, `collision_density`
- `crime_pct`, `collision_pct`, `pct`, `safety_score`, `band`

## 7) Short Worked Example

Given one segment:
- `official_crimes=12`
- `user_reported_crime_signal=1.5`
- `length_m=400`
- `collisions=4`, `slight=6`, `serious=1`, `fatal=0`

Then:
- `crimes = 13.5`
- `normalized_km = max(400,100)/1000 = 0.4`
- `crimes_per_km = 13.5 / 0.4 = 33.75`
- `collision_severity_points = 4 + (6*0.5) + (1*2.0) + (0*5.0) = 9`
- `collision_density = 9 / 0.4 = 22.5`

Final `safety_score` depends on where `33.75` and `22.5` rank against other active segments in the same run.
