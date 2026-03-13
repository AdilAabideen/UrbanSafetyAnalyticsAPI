# Analytics API

Base URL:
- `http://127.0.0.1:8000`

Docs:
- Swagger UI: `/docs`
- OpenAPI JSON: `/openapi.json`

This document covers the current frontend-facing analytics surface:
- `GET /analytics/meta`
- `POST /analytics/risk/score`
- `POST /analytics/risk/forecast`
- `GET /analytics/patterns/hotspot-stability`

Important implementation note:
- each of the 3 analytics endpoints now stores a snapshot row in Postgres
- every successful response includes:
  - `snapshot_id`
  - `stored_at`

## Shared Conventions

### Months
- Month inputs use `YYYY-MM`
- invalid format returns `400 INVALID_MONTH_FORMAT`
- `from` must be less than or equal to `to`
- maximum supported span is `24` months

### BBox
- BBox fields use:
  - `minLon`
  - `minLat`
  - `maxLon`
  - `maxLat`
- `minLon < maxLon`
- `minLat < maxLat`
- partial bbox values return `400 INVALID_BBOX`

### Collision Inclusion
- collisions only affect scoring when:
  - `mode = "drive"`
  - `includeCollisions = true`
- otherwise the response is crime-only

### Error Shape
Validation and analytics errors use:

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
- `DB_UNAVAILABLE`

## 1) Analytics Meta

**GET `/analytics/meta`**

Purpose:
- discover available month coverage
- discover available crime types
- get high-level dataset counts

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

Use for:
- page bootstrap
- month pickers
- crime type dropdowns

## 2) Risk Score

**POST `/analytics/risk/score`**

Purpose:
- compute an area-level risk score for a bbox over a time window
- crime-only by default
- crime + collision severity when enabled in drive mode

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
  "snapshot_id": 11,
  "stored_at": "2026-03-13T10:00:00Z",
  "score_basis": "crime+collision",
  "risk_score": 92,
  "score": 92,
  "pct": 0.9191,
  "band": "amber",
  "metrics": {
    "total_crimes": 7388,
    "approved_user_reports": 0,
    "user_reported_crime_signal": 0.0,
    "effective_total_crimes": 7388.0,
    "area_km2": 67.142,
    "crimes_per_km2": 110.035,
    "effective_crimes_per_km2": 110.035,
    "segments_considered": 14940,
    "avg_crimes_per_km": 4.187,
    "avg_user_reported_crime_signal_per_km": 0.0,
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
    "reading": "This bbox sits above the wider network average and falls into the upper 20% of observed risk.",
    "user_reports": "Approved user-reported crimes are blended into the crime density as a capped low-weight supplement and do not override official counts."
  }
}
```

Notes:
- `score` and `risk_score` are the same numeric value
- `band` is based on percentile:
  - `red`
  - `amber`
  - `green`
- if collisions are not applied:
  - `score_basis = "crime"`
  - collision metrics are omitted from `metrics`

Use for:
- headline risk card
- area risk panel
- map sidebar summary

## 3) Risk Forecast

**POST `/analytics/risk/forecast`**

Purpose:
- forecast the next month using the immediately preceding baseline window
- crime-only by default
- optionally include collisions in drive mode

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
  "snapshot_id": 21,
  "stored_at": "2026-03-13T10:00:00Z",
  "score_basis": "crime+collision",
  "history": [
    {
      "month": "2025-01",
      "official_crime_count": 2411,
      "approved_user_reports": 0,
      "user_reported_crime_signal": 0.0,
      "crime_count": 2411.0,
      "collision_count": 31,
      "collision_points": 63.5,
      "combined_value": 2461.8
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
        "baseline_mean": 2555.167,
        "baseline_official_mean": 2555.167,
        "baseline_user_reported_signal_mean": 0.0,
        "baseline_approved_user_reports_mean": 0.0
      },
      "collisions": {
        "expected_count": 32,
        "expected_points": 66.0,
        "baseline_mean": 31.667,
        "baseline_points_mean": 66.0,
        "applied": true
      },
      "combined": {
        "expected_value": 2607.967,
        "baseline_mean": 2607.967,
        "ratio": 1.0
      }
    },
    "predicted_monthly_count": 2555,
    "predicted_band": "green",
    "projection_basis": "combined"
  },
  "explanation": {
    "summary": "The forecast uses the mean monthly count over the immediately preceding baseline window and a simple normal approximation around the Poisson mean.",
    "collisions": "When includeCollisions is true in drive mode, the response also reports monthly collision counts and severity-weighted collision points.",
    "user_reports": "Approved user-reported crimes are blended into the crime signal as a capped low-weight supplement before the monthly baseline is averaged."
  }
}
```

Notes:
- in crime-only mode:
  - `score_basis = "crime"`
  - `history` contains crime fields only
  - `forecast.components` contains only `crimes`
- in drive + collision mode:
  - `history` also contains `collision_count`, `collision_points`, `combined_value`
  - `forecast.components.collisions` and `forecast.components.combined` are included

Use for:
- forecast card
- next-month expectation panel
- forecast chart with baseline history

## 4) Hotspot Stability

**GET `/analytics/patterns/hotspot-stability`**

Purpose:
- measure whether the top risky roads are persisting or changing month-to-month

Query params:
- required:
  - `from`
  - `to`
- optional:
  - `k` default `20`, min `5`, max `200`
  - `includeLists` default `false`
  - bbox:
    - `minLon`
    - `minLat`
    - `maxLon`
    - `maxLat`
  - `crimeType`

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
  "snapshot_id": 31,
  "stored_at": "2026-03-13T10:00:00Z",
  "stability_series": [
    {
      "month": "2025-02",
      "jaccard_vs_prev": 0.6667,
      "overlap_count": 16
    },
    {
      "month": "2025-03",
      "jaccard_vs_prev": 0.7391,
      "overlap_count": 17
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

Notes:
- `topk_by_month` is only returned when `includeLists = true`
- `jaccard_vs_prev` is always between `0` and `1`
- higher `average_jaccard` means hotspot roads are more persistent

Use for:
- stability line chart
- persistent hotspots table
- “are risky roads changing or staying the same?” panel

## Suggested Frontend TypeScript Shapes

```ts
export type AnalyticsError = {
  error: string;
  message: string;
  details?: Record<string, unknown>;
};

export type AnalyticsScope = {
  from?: string;
  to?: string;
  target?: string;
  baselineMonths?: number;
  bbox?: {
    minLon: number;
    minLat: number;
    maxLon: number;
    maxLat: number;
  };
  mode?: string;
  crimeType?: string | null;
  includeCollisions?: boolean;
  method?: string;
  k?: number;
};

export type RiskScoreResponse = {
  scope: AnalyticsScope;
  generated_at: string;
  snapshot_id: number;
  stored_at: string;
  score_basis: "crime" | "crime+collision";
  risk_score: number;
  score: number;
  pct: number;
  band: "green" | "amber" | "red";
  metrics: Record<string, unknown>;
  explain: {
    reading: string;
    user_reports?: string;
  };
};

export type RiskForecastResponse = {
  scope: AnalyticsScope;
  generated_at: string;
  snapshot_id: number;
  stored_at: string;
  score_basis: "crime" | "crime+collision";
  history: Array<Record<string, unknown>>;
  forecast: Record<string, unknown>;
  explanation: {
    summary: string;
    collisions?: string;
    user_reports?: string;
  };
};

export type HotspotStabilityResponse = {
  scope: AnalyticsScope;
  generated_at: string;
  snapshot_id: number;
  stored_at: string;
  stability_series: Array<{
    month: string;
    jaccard_vs_prev: number;
    overlap_count: number;
  }>;
  persistent_hotspots: Array<{
    segment_id: number;
    appearances: number;
    appearance_ratio: number;
  }>;
  summary: {
    months_evaluated: number;
    average_jaccard: number;
    persistent_hotspot_count: number;
    notes: string;
  };
  topk_by_month?: Array<{
    month: string;
    segment_ids: number[];
  }>;
};
```
