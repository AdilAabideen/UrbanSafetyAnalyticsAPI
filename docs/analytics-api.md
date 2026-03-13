# Analytics API

Base URL:
- `http://127.0.0.1:8000`

Docs:
- Swagger UI: `/docs`
- OpenAPI JSON: `/openapi.json`

This document covers the current `/analytics/*` surface:
- `GET /analytics/meta`
- `POST /analytics/risk/score`
- `POST /analytics/risk/forecast`
- `GET /analytics/patterns/hotspot-stability`
- `POST /analytics/routes/risk`
- `POST /analytics/routes/compare`

## Shared Conventions

### Months
- Month inputs use `YYYY-MM`.
- Invalid month format returns `400`.
- `from` must be less than or equal to `to`.
- Maximum supported span is `24` months.

### BBox
- BBox fields use:
  - `minLon`
  - `minLat`
  - `maxLon`
  - `maxLat`
- `minLon` must be less than `maxLon`.
- `minLat` must be less than `maxLat`.

### Collision Inclusion
- Collisions are only applied when:
  - `mode = "drive"`
  - `includeCollisions = true`
- If collisions are not applied:
  - the response uses `score_basis = "crime"`
  - collision metrics/components are omitted
- If collisions are applied:
  - the response uses `score_basis = "crime+collision"`
  - collision metrics/components are included

### Error Shape
All validation and analytics errors return JSON in this form:

```json
{
  "error": "INVALID_MONTH_FORMAT",
  "message": "from must be in YYYY-MM format",
  "details": {}
}
```

Common error codes:
- `INVALID_MONTH_FORMAT`
- `INVALID_DATE_RANGE`
- `RANGE_TOO_LARGE`
- `INVALID_BBOX`
- `INVALID_MODE`
- `INVALID_MODE_FOR_COLLISIONS`
- `BASELINE_HISTORY_INSUFFICIENT`
- `INVALID_ROUTE_INPUT`
- `INVALID_SEGMENT_ID`
- `ROUTE_HAS_DUPLICATES`
- `ROUTE_DISCONNECTED`
- `NO_SEGMENTS_MATCH_ROUTE`
- `DB_UNAVAILABLE`

## 1) Analytics Meta

**GET `/analytics/meta`**

Purpose:
- Discover available months
- Discover available crime types
- Get high-level dataset counts

Response:

```json
{
  "months": {
    "min": "2023-02",
    "max": "2026-01"
  },
  "crime_types": [
    "Public order",
    "Shoplifting",
    "Violence and sexual offences"
  ],
  "counts": {
    "crime_events_total": 937822,
    "crime_events_with_geom": 927447,
    "crime_events_snapped": 927447,
    "road_segments_total": 212450
  }
}
```

## 2) Area Risk Score

**POST `/analytics/risk/score`**

Purpose:
- Compute an area-level risk score for a bbox over a month window
- Crime-only by default
- Crime + collision severity when enabled in drive mode

Request body:

```json
{
  "from": "2025-01",
  "to": "2025-03",
  "minLon": -1.60,
  "minLat": 53.78,
  "maxLon": -1.52,
  "maxLat": 53.82,
  "crimeType": null,
  "mode": "drive",
  "includeCollisions": true,
  "weights": {
    "w_crime": 1.0,
    "w_collision": 0.8
  }
}
```

Response:

```json
{
  "scope": {
    "from": "2025-01",
    "to": "2025-03",
    "bbox": {
      "minLon": -1.6,
      "minLat": 53.78,
      "maxLon": -1.52,
      "maxLat": 53.82
    },
    "mode": "drive",
    "crimeType": null,
    "includeCollisions": true
  },
  "generated_at": "2026-03-13T10:00:00Z",
  "score_basis": "crime+collision",
  "risk_score": 92,
  "score": 92,
  "pct": 0.9191,
  "band": "amber",
  "metrics": {
    "total_crimes": 7388,
    "area_km2": 67.142,
    "crimes_per_km2": 110.035,
    "segments_considered": 14940,
    "avg_crimes_per_km": 4.187,
    "red_segment_share": 0.058,
    "weights_applied": {
      "w_crime": 1.0,
      "w_collision": 0.8
    },
    "total_collisions": 99,
    "collisions_per_km2": 1.474,
    "collision_points_per_km2": 3.038,
    "avg_collisions_per_km": 0.045,
    "avg_collision_points_per_km": 0.091
  },
  "explain": {
    "reading": "This bbox sits above the wider network average and falls into the upper 20% of observed risk."
  }
}
```

Notes:
- `risk_score` and `score` are the same numeric value.
- `band` is derived from the score percentile:
  - `red`
  - `amber`
  - `green`
- When collisions are not applied:
  - `score_basis = "crime"`
  - collision metrics are omitted

## 3) Risk Forecast

**POST `/analytics/risk/forecast`**

Purpose:
- Forecast the next month using the preceding baseline window
- Crime-only by default
- Can include collision context in drive mode

Request body:

```json
{
  "target": "2025-07",
  "baselineMonths": 6,
  "minLon": -1.60,
  "minLat": 53.78,
  "maxLon": -1.52,
  "maxLat": 53.82,
  "crimeType": null,
  "mode": "drive",
  "includeCollisions": true,
  "weights": {
    "w_crime": 1.0,
    "w_collision": 0.8
  },
  "returnRiskProjection": true
}
```

Response:

```json
{
  "scope": {
    "target": "2025-07",
    "baselineMonths": 6,
    "bbox": {
      "minLon": -1.6,
      "minLat": 53.78,
      "maxLon": -1.52,
      "maxLat": 53.82
    },
    "crimeType": null,
    "method": "poisson_mean",
    "mode": "drive",
    "includeCollisions": true
  },
  "generated_at": "2026-03-13T10:00:00Z",
  "score_basis": "crime+collision",
  "history": [
    {
      "month": "2025-01",
      "crime_count": 2411,
      "collision_count": 31,
      "collision_points": 63.5,
      "combined_value": 2474.5
    }
  ],
  "forecast": {
    "expected_count": 2555,
    "low": 2456,
    "high": 2655,
    "baseline_mean": 2555.167,
    "ratio": 1.0,
    "components": {
      "crimes": {
        "expected_count": 2555,
        "baseline_mean": 2555.167
      },
      "collisions": {
        "expected_count": 32,
        "expected_points": 66.0,
        "baseline_mean": 31.667,
        "baseline_points_mean": 66.0,
        "applied": true
      },
      "combined": {
        "expected_value": 2621.167,
        "baseline_mean": 2621.167,
        "ratio": 1.0
      }
    },
    "predicted_monthly_count": 2555,
    "predicted_band": "green",
    "projection_basis": "combined"
  },
  "explanation": {
    "summary": "The forecast uses the mean monthly count over the immediately preceding baseline window and a simple normal approximation around the Poisson mean.",
    "collisions": "When includeCollisions is true in drive mode, the response also reports monthly collision counts and severity-weighted collision points."
  }
}
```

Notes:
- In crime-only mode:
  - `score_basis = "crime"`
  - `history` includes only `month` and `crime_count`
  - `forecast.components` includes only `crimes`
- In drive + collision mode:
  - `history` includes collision fields
  - `forecast.components.collisions` and `forecast.components.combined` are included

## 4) Hotspot Stability

**GET `/analytics/patterns/hotspot-stability`**

Purpose:
- Track whether the top risky roads in a time window persist from month to month

Query params:
- `from` required
- `to` required
- `k` optional, default `20`, min `5`, max `200`
- `includeLists` optional, default `false`
- optional bbox:
  - `minLon`
  - `minLat`
  - `maxLon`
  - `maxLat`
- `crimeType` optional

Example:

```txt
/analytics/patterns/hotspot-stability?from=2025-01&to=2025-03&minLon=-1.60&minLat=53.78&maxLon=-1.52&maxLat=53.82&k=20&includeLists=true
```

Response:

```json
{
  "scope": {
    "from": "2025-01",
    "to": "2025-03",
    "bbox": {
      "minLon": -1.6,
      "minLat": 53.78,
      "maxLon": -1.52,
      "maxLat": 53.82
    },
    "crimeType": null,
    "k": 20
  },
  "generated_at": "2026-03-13T10:00:00Z",
  "stability_series": [
    {
      "month": "2025-02",
      "jaccard_vs_prev": 0.6667,
      "overlap_count": 16
    }
  ],
  "persistent_hotspots": [
    {
      "segment_id": 118447,
      "appearances": 3,
      "appearance_ratio": 1.0
    }
  ],
  "summary": {
    "months_evaluated": 3,
    "average_jaccard": 0.7029,
    "persistent_hotspot_count": 20,
    "notes": "Higher Jaccard values mean the top risky roads are persisting from month to month; lower values mean the hotspot pattern is moving around."
  },
  "topk_by_month": [
    {
      "month": "2025-01",
      "segment_ids": [118447, 118667]
    }
  ]
}
```

## 5) Route Risk

**POST `/analytics/routes/risk`**

Purpose:
- Score a single route using either:
  - `segment_ids`
  - `route_line`

Recommended frontend input:
- Prefer `route_line`
- Use `segment_ids` only when you already have a snapped road path

### Option A: `segment_ids`

Request body:

```json
{
  "from": "2025-01",
  "to": "2025-03",
  "segment_ids": [118447, 118667, 119060],
  "checkConnectivity": true,
  "threshold_m": 20
}
```

### Option B: `route_line`

Request body:

```json
{
  "from": "2025-01",
  "to": "2025-03",
  "mode": "drive",
  "includeCollisions": true,
  "buffer_m": 25,
  "route_line": {
    "type": "LineString",
    "coordinates": [
      [-1.560, 53.800],
      [-1.550, 53.805],
      [-1.540, 53.810]
    ]
  }
}
```

Response:

```json
{
  "scope": {
    "from": "2025-01",
    "to": "2025-03",
    "mode": "walk",
    "crimeType": null,
    "includeCollisions": false,
    "input_type": "segment_ids"
  },
  "generated_at": "2026-03-13T10:00:00Z",
  "route_stats": {
    "segment_count": 3,
    "total_length_km": 0.742,
    "total_crimes": 9,
    "total_collisions": 0,
    "score_raw": 12.314,
    "score_pct": 0.82,
    "band": "amber",
    "explanation": "This route sits above the wider network average and falls into the upper 20% of observed risk."
  },
  "connectivity": {
    "is_connected": false,
    "breaks": [
      {
        "index": 2,
        "from_segment_id": 118667,
        "to_segment_id": 119060,
        "distance_m": 31.2
      }
    ]
  },
  "worst_segments": [
    {
      "segment_id": 119060,
      "name": "Some Road",
      "highway": "primary",
      "crimes": 4,
      "collisions": 0,
      "crimes_per_km": 18.421,
      "collision_density": 0.0,
      "contribution": 2.911
    }
  ]
}
```

Notes:
- If `segment_ids` are disconnected:
  - the API still returns a score by default
  - `connectivity.is_connected` will be `false`
  - `connectivity.breaks` will explain where the route breaks
- Use `failOnDisconnect=true` to fail instead

## 6) Route Compare

**POST `/analytics/routes/compare`**

Purpose:
- Compare `2` to `5` routes in one request

Request body:

```json
{
  "from": "2025-01",
  "to": "2025-03",
  "mode": "drive",
  "includeCollisions": true,
  "routes": [
    {
      "name": "Route A",
      "route_line": {
        "type": "LineString",
        "coordinates": [
          [-1.560, 53.800],
          [-1.550, 53.805]
        ]
      }
    },
    {
      "name": "Route B",
      "route_line": {
        "type": "LineString",
        "coordinates": [
          [-1.545, 53.798],
          [-1.535, 53.804]
        ]
      }
    }
  ]
}
```

Response:

```json
{
  "scope": {
    "from": "2025-01",
    "to": "2025-03",
    "mode": "drive",
    "crimeType": null,
    "includeCollisions": true
  },
  "generated_at": "2026-03-13T10:00:00Z",
  "results": [
    {
      "name": "Route A",
      "score_raw": 8.211,
      "score_pct": 0.75,
      "band": "green",
      "total_length_km": 0.742,
      "total_crimes": 9,
      "total_collisions": 1,
      "is_connected": true,
      "break_count": 0,
      "worst_segments": [],
      "explanation": "This route sits below the upper-risk bands for the selected period."
    }
  ],
  "ranking": ["Route A", "Route B"],
  "summary": {
    "safest_route": "Route A",
    "riskiest_route": "Route B",
    "notes": "Routes are ranked from lowest risk score to highest risk score.",
    "deltas_vs_safest": [
      {
        "route_name": "Route B",
        "absolute_delta": 6.341,
        "percent_delta": 0.772
      }
    ]
  }
}
```

Notes:
- If all compared routes have zero observed risk, `summary.notes` explains that directly.
- `band` and `explanation` are returned per route.
