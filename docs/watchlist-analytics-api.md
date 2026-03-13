# Watchlist Analytics API

Base URL:
- `http://127.0.0.1:8000`

Docs:
- Swagger UI: `/docs`
- OpenAPI JSON: `/openapi.json`

This document is the frontend-facing contract for the watchlist analytics flow.

Important behavior:
- generic analytics endpoints under `/analytics/...` are compute-only
- persistence only happens through watchlist endpoints
- all endpoints in this document require `Authorization: Bearer <token>`

## 1) Core Flow

Frontend flow:
1. Create or update a watchlist with its `preference`
2. Trigger one of the run endpoints
3. Store `watchlist_run_id` from the response
4. Use the matching results endpoint to re-fetch persisted output later

Supported persisted report types:
- `risk_score`
- `risk_forecast`
- `hotspot_stability`

## 2) Watchlist Model

### Watchlist preference input

Use this shape in `POST /watchlists` and `PATCH /watchlists/{watchlist_id}`.

```json
{
  "window_months": 6,
  "crime_types": ["Shoplifting"],
  "travel_mode": "drive",
  "include_collisions": true,
  "baseline_months": 6,
  "hotspot_k": 20,
  "include_hotspot_stability": true,
  "include_forecast": true,
  "weight_crime": 1.0,
  "weight_collision": 0.8
}
```

Rules:
- frontend should only send `travel_mode` as `walk` or `drive`
- `include_collisions = true` is only valid when `travel_mode = "drive"`
- `crime_types` may be empty; if empty, analytics are run against all crime types

### Create watchlist

**POST `/watchlists`**

Request body:

```json
{
  "name": "Leeds Centre",
  "min_lon": -1.60,
  "min_lat": 53.78,
  "max_lon": -1.52,
  "max_lat": 53.82,
  "preference": {
    "window_months": 6,
    "crime_types": ["Shoplifting"],
    "travel_mode": "drive",
    "include_collisions": true,
    "baseline_months": 6,
    "hotspot_k": 20,
    "include_hotspot_stability": true,
    "include_forecast": true,
    "weight_crime": 1.0,
    "weight_collision": 0.8
  }
}
```

Response:

```json
{
  "watchlist": {
    "id": 2,
    "user_id": 1,
    "name": "Leeds Centre",
    "min_lon": -1.6,
    "min_lat": 53.78,
    "max_lon": -1.52,
    "max_lat": 53.82,
    "created_at": "2026-03-13T10:33:42.312702+00:00",
    "preference": {
      "id": 2,
      "watchlist_id": 2,
      "window_months": 6,
      "crime_types": ["Shoplifting"],
      "travel_mode": "drive",
      "include_collisions": true,
      "baseline_months": 6,
      "hotspot_k": 20,
      "include_hotspot_stability": true,
      "include_forecast": true,
      "weight_crime": 1.0,
      "weight_collision": 0.8,
      "created_at": "2026-03-13T10:33:42.312702+00:00"
    }
  }
}
```

### Read watchlists

**GET `/watchlists`**

Query params:
- optional `watchlist_id`

Responses:
- if `watchlist_id` is omitted:

```json
{
  "items": [
    {
      "id": 2,
      "user_id": 1,
      "name": "Leeds Centre",
      "min_lon": -1.6,
      "min_lat": 53.78,
      "max_lon": -1.52,
      "max_lat": 53.82,
      "created_at": "2026-03-13T10:33:42.312702+00:00",
      "preference": {
        "id": 2,
        "watchlist_id": 2,
        "window_months": 6,
        "crime_types": ["Shoplifting"],
        "travel_mode": "drive",
        "include_collisions": true,
        "baseline_months": 6,
        "hotspot_k": 20,
        "include_hotspot_stability": true,
        "include_forecast": true,
        "weight_crime": 1.0,
        "weight_collision": 0.8,
        "created_at": "2026-03-13T10:33:42.312702+00:00"
      }
    }
  ]
}
```

- if `watchlist_id` is provided:

```json
{
  "watchlist": {
    "id": 2,
    "user_id": 1,
    "name": "Leeds Centre",
    "min_lon": -1.6,
    "min_lat": 53.78,
    "max_lon": -1.52,
    "max_lat": 53.82,
    "created_at": "2026-03-13T10:33:42.312702+00:00",
    "preference": {
      "id": 2,
      "watchlist_id": 2,
      "window_months": 6,
      "crime_types": ["Shoplifting"],
      "travel_mode": "drive",
      "include_collisions": true,
      "baseline_months": 6,
      "hotspot_k": 20,
      "include_hotspot_stability": true,
      "include_forecast": true,
      "weight_crime": 1.0,
      "weight_collision": 0.8,
      "created_at": "2026-03-13T10:33:42.312702+00:00"
    }
  }
}
```

### Update watchlist

**PATCH `/watchlists/{watchlist_id}`**

Request body:
- any subset of `name`
- full bbox set together
- optional full `preference`

Example:

```json
{
  "preference": {
    "window_months": 6,
    "crime_types": ["Shoplifting"],
    "travel_mode": "drive",
    "include_collisions": true,
    "baseline_months": 6,
    "hotspot_k": 25,
    "include_hotspot_stability": true,
    "include_forecast": true,
    "weight_crime": 1.0,
    "weight_collision": 0.8
  }
}
```

Response shape is the same as `POST /watchlists`.

### Delete watchlist

**DELETE `/watchlists/{watchlist_id}`**

Response:

```json
{
  "deleted": true,
  "watchlist_id": 2
}
```

Deleting a watchlist also deletes stored analytics rows for that watchlist.

## 3) Run Endpoints

All run endpoints:
- derive their inputs from the watchlist and its stored preference
- return both the persisted wrapper and the computed result
- persist one row in `watchlist_analytics_runs`

Important wrapper note:
- wrapper `request.bbox` uses `snake_case` keys such as `min_lon`
- inner analytics `result.*.scope.bbox` uses `camelCase` keys such as `minLon`

### Shared run response wrapper

```json
{
  "watchlist_id": 2,
  "report_type": "risk_forecast",
  "watchlist_run_id": 12,
  "stored_at": "2026-03-13T12:35:21.389776+00:00",
  "request": {},
  "result": {}
}
```

### Risk score run

**POST `/watchlists/{watchlist_id}/risk-score/run`**

Derived request shape:

```json
{
  "from": "2025-09",
  "to": "2026-02",
  "bbox": {
    "min_lon": -1.6,
    "min_lat": 53.78,
    "max_lon": -1.52,
    "max_lat": 53.82
  },
  "crime_types": ["Shoplifting"],
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
  "watchlist_id": 2,
  "report_type": "risk_score",
  "watchlist_run_id": 1,
  "stored_at": "2026-03-13T12:35:21.389776+00:00",
  "request": {
    "from": "2025-09",
    "to": "2026-02",
    "bbox": {
      "min_lon": -1.6,
      "min_lat": 53.78,
      "max_lon": -1.52,
      "max_lat": 53.82
    },
    "crime_types": ["Shoplifting"],
    "mode": "drive",
    "includeCollisions": true,
    "weights": {
      "w_crime": 1.0,
      "w_collision": 0.8
    }
  },
  "result": {
    "results_by_crime_type": {
      "Shoplifting": {
        "score": 100,
        "risk_score": 100,
        "band": "red",
        "score_basis": "crime+collision"
      }
    }
  }
}
```

### Risk forecast run

**POST `/watchlists/{watchlist_id}/risk-forecast/run`**

Derived request shape:

```json
{
  "target": "2026-03",
  "baselineMonths": 6,
  "bbox": {
    "min_lon": -1.6,
    "min_lat": 53.78,
    "max_lon": -1.52,
    "max_lat": 53.82
  },
  "crime_types": ["Shoplifting"],
  "mode": "drive",
  "includeCollisions": true,
  "returnRiskProjection": true,
  "weights": {
    "w_crime": 1.0,
    "w_collision": 0.8
  }
}
```

Response:

```json
{
  "watchlist_id": 2,
  "report_type": "risk_forecast",
  "watchlist_run_id": 12,
  "stored_at": "2026-03-13T12:35:21.389776+00:00",
  "request": {
    "target": "2026-03",
    "baselineMonths": 6,
    "bbox": {
      "min_lon": -1.6,
      "min_lat": 53.78,
      "max_lon": -1.52,
      "max_lat": 53.82
    },
    "crime_types": ["Shoplifting"],
    "mode": "drive",
    "includeCollisions": true,
    "returnRiskProjection": true,
    "weights": {
      "w_crime": 1.0,
      "w_collision": 0.8
    }
  },
  "result": {
    "results_by_crime_type": {
      "Shoplifting": {
        "score_basis": "crime+collision",
        "forecast": {
          "expected_count": 2555,
          "predicted_band": "green"
        }
      }
    }
  }
}
```

Known risk forecast failure:

```json
{
  "error": "BASELINE_HISTORY_INSUFFICIENT",
  "message": "The dataset does not contain every required baseline month",
  "details": {
    "missing_months": 1
  }
}
```

Frontend handling:
- this is a valid business/data failure, not an auth failure
- keep the watchlist intact
- show the message and avoid retry loops

### Hotspot stability run

**POST `/watchlists/{watchlist_id}/hotspot-stability/run`**

Derived request shape:

```json
{
  "from": "2025-09",
  "to": "2026-02",
  "bbox": {
    "min_lon": -1.6,
    "min_lat": 53.78,
    "max_lon": -1.52,
    "max_lat": 53.82
  },
  "crime_types": ["Shoplifting"],
  "k": 20,
  "includeLists": true
}
```

Response:

```json
{
  "watchlist_id": 2,
  "report_type": "hotspot_stability",
  "watchlist_run_id": 13,
  "stored_at": "2026-03-13T12:35:23.512565+00:00",
  "request": {
    "from": "2025-09",
    "to": "2026-02",
    "bbox": {
      "min_lon": -1.6,
      "min_lat": 53.78,
      "max_lon": -1.52,
      "max_lat": 53.82
    },
    "crime_types": ["Shoplifting"],
    "k": 20,
    "includeLists": true
  },
  "result": {
    "results_by_crime_type": {
      "Shoplifting": {
        "summary": {
          "months_evaluated": 6,
          "average_jaccard": 0.4718,
          "persistent_hotspot_count": 20
        }
      }
    }
  }
}
```

## 4) Results Endpoints

Use these to fetch previously stored rows.

Supported endpoints:
- `GET /watchlists/{watchlist_id}/risk-score/results`
- `GET /watchlists/{watchlist_id}/risk-forecast/results`
- `GET /watchlists/{watchlist_id}/hotspot-stability/results`

Query params:
- optional `run_id`
- optional `limit`, default `20`, min `1`, max `100`

Behavior:
- if `run_id` is omitted, returns newest-first history
- if `run_id` is provided, returns the matching stored row in `items[0]`
- if `run_id` is provided and not found, returns `404`

### Results response shape

```json
{
  "items": [
    {
      "id": 12,
      "watchlist_id": 2,
      "report_type": "risk_forecast",
      "request": {},
      "result": {},
      "created_at": "2026-03-13T12:35:21.389776+00:00"
    }
  ]
}
```

### Risk forecast results example

**GET `/watchlists/{watchlist_id}/risk-forecast/results?run_id=12`**

```json
{
  "items": [
    {
      "id": 12,
      "watchlist_id": 2,
      "report_type": "risk_forecast",
      "request": {
        "target": "2026-03",
        "baselineMonths": 6,
        "bbox": {
          "min_lon": -1.6,
          "min_lat": 53.78,
          "max_lon": -1.52,
          "max_lat": 53.82
        },
        "crime_types": ["Shoplifting"],
        "mode": "drive",
        "includeCollisions": true,
        "returnRiskProjection": true,
        "weights": {
          "w_crime": 1.0,
          "w_collision": 0.8
        }
      },
      "result": {
        "results_by_crime_type": {
          "Shoplifting": {
            "score_basis": "crime+collision",
            "history": [],
            "forecast": {}
          }
        }
      },
      "created_at": "2026-03-13T12:35:21.389776+00:00"
    }
  ]
}
```

## 5) Crime Type Aggregation

For all watchlist run endpoints:
- if `crime_types` contains values, `result.results_by_crime_type` is keyed by each crime type string
- if `crime_types` is empty, the key is `"all"`

Example:

```json
{
  "results_by_crime_type": {
    "all": {}
  }
}
```

## 6) Watchlist-Specific Errors

Common watchlist errors:

### Missing preference

```json
{
  "detail": "Watchlist preference is required to run analytics"
}
```

### Unsupported stored mode

```json
{
  "detail": "Unsupported watchlist travel_mode '...' for watchlist analytics. Use walk or drive."
}
```

### Invalid collision/mode combination

```json
{
  "detail": "include_collisions is only supported when travel_mode is drive"
}
```

### Disabled report type

Forecast disabled:

```json
{
  "detail": "Forecast runs are disabled for this watchlist"
}
```

Hotspot disabled:

```json
{
  "detail": "Hotspot stability runs are disabled for this watchlist"
}
```

### Not found / ownership

```json
{
  "detail": "Watchlist not found"
}
```

### Stored result not found

```json
{
  "detail": "Watchlist result not found"
}
```

## 7) Frontend Notes

- treat `watchlist_run_id` as the stable id for a newly created stored run
- use the matching `.../results?run_id=...` endpoint after a successful run if the UI needs to reload persisted data
- do not send `walking` or `driving` from the frontend; only send `walk` or `drive`
- generic `/analytics/...` endpoints are still useful for ad hoc compute, but they do not persist anything
- for full inner payload details of `risk_score`, `risk_forecast`, and `hotspot_stability`, see [analytics-api.md](/Users/adil/Documents/University/WebServices/API/docs/analytics-api.md)
