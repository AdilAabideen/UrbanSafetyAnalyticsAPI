# Roads Page Frontend Design Doc

Base URL:
- `http://127.0.0.1:8000`

This document is for the frontend agent building the Roads page.

Goal:
- show a map of roads
- show the most dangerous roads in the current area
- let the user filter by time range, crime type, outcome, highway type, and viewport
- show a few strong analytics, not lots of weak ones

Recommended Roads page sections:
- filter bar
- map with road vector tiles
- summary cards + insight messages
- dangerous roads table
- breakdown charts
- trend chart

---

## 1. Shared Filter Model

These are the main Roads analytics filters.

Time:
- `from=YYYY-MM`
- `to=YYYY-MM`

Optional bbox:
- `minLon`
- `minLat`
- `maxLon`
- `maxLat`

Optional categorical filters:
- `crimeType`
- `lastOutcomeCategory`
- `highway`

Notes:
- `crimeType`, `lastOutcomeCategory`, and `highway` can be repeated query params or comma-separated
- if using bbox, send all 4 values
- `from` and `to` are required for most analytics endpoints
- `lastOutcomeCategory` is supported on the upgraded Roads analytics endpoints

Recommended frontend filter state:
```ts
type RoadsFilters = {
  from: string
  to: string
  bbox?: {
    minLon: number
    minLat: number
    maxLon: number
    maxLat: number
  }
  crimeType?: string[]
  lastOutcomeCategory?: string[]
  highway?: string[]
}
```

---

## 2. Primary Endpoints To Use

These are the main Roads page APIs.

### `GET /roads/analytics/meta`
Use for:
- filter bootstrapping
- initial filter defaults
- total road inventory display

Best UI uses:
- populate highway dropdown
- set default time range from `months.min` to `months.max`
- optional small page subtitle like “212k road segments available”

Response:
```json
{
  "months": {
    "min": "2023-02",
    "max": "2026-01"
  },
  "highways": ["primary", "residential", "service"],
  "counts": {
    "road_segments_total": 212450,
    "named_roads_total": 76706,
    "total_length_m": 24448828.42
  }
}
```

---

### `GET /roads/analytics/summary`
Use for:
- KPI cards
- top-line user explanation
- summary panel above the table/chart section

Best UI uses:
- summary cards
- “what is happening here” insight strip
- top road callout
- top crime/outcome callout

Required query params:
- `from`
- `to`

Optional:
- bbox
- `crimeType`
- `lastOutcomeCategory`
- `highway`

Example:
```txt
/roads/analytics/summary?from=2023-02&to=2023-06&minLon=-1.6&minLat=53.78&maxLon=-1.52&maxLat=53.82&crimeType=Shoplifting&lastOutcomeCategory=Under%20investigation
```

Response:
```json
{
  "from": "2023-02",
  "to": "2023-06",
  "total_segments": 5500,
  "total_length_m": 123456.7,
  "unique_highway_types": 6,
  "roads_with_incidents": 420,
  "segments_with_incidents": 420,
  "total_incidents": 1200,
  "avg_incidents_per_km": 9.72,
  "top_road": {
    "segment_id": 7,
    "name": "Test Road",
    "highway": "primary",
    "length_m": 120.5,
    "incident_count": 80,
    "incidents_per_km": 66.39,
    "risk_score": 95.0,
    "band": "red"
  },
  "top_highway": {
    "highway": "residential",
    "segment_count": 3200,
    "length_m": 65432.1,
    "incident_count": 900,
    "incidents_per_km": 13.75
  },
  "top_highway_type": {
    "highway": "residential",
    "segment_count": 3200,
    "length_m": 65432.1
  },
  "top_crime_type": {
    "crime_type": "Shoplifting",
    "count": 350
  },
  "top_outcome": {
    "outcome": "Under investigation",
    "count": 260
  },
  "current_period": {
    "from": "2023-02",
    "to": "2023-06",
    "incident_count": 1200
  },
  "previous_period": {
    "from": "2022-09",
    "to": "2023-01",
    "incident_count": 1000
  },
  "current_vs_previous_pct": 20.0,
  "band_breakdown": {
    "red": 4,
    "orange": 10,
    "green": 406
  },
  "insights": [
    "Residential roads drive the largest incident volume in this selection.",
    "Shoplifting is the dominant road-linked crime type here."
  ]
}
```

Recommended summary card set:
- total incidents
- roads with incidents
- average incidents per km
- top road
- period change vs previous

Recommended insight UI:
- render `insights` as 2 to 4 short callout chips or a small stacked message block

---

### `GET /roads/analytics/risk`
Use for:
- main table of dangerous roads
- ranking panel
- “top roads” list beside the map

Best UI uses:
- sortable table
- top 10 / top 25 roads list
- clicking a row can pan map to that road

Required query params:
- `from`
- `to`

Optional:
- bbox
- `crimeType`
- `lastOutcomeCategory`
- `highway`
- `limit`
- `sort=risk_score|incidents_per_km|incident_count`

Recommended default:
- `sort=risk_score`

Example:
```txt
/roads/analytics/risk?from=2023-02&to=2023-06&minLon=-1.6&minLat=53.78&maxLon=-1.52&maxLat=53.82&sort=risk_score&limit=25
```

Response:
```json
{
  "items": [
    {
      "segment_id": 7,
      "name": "Test Road",
      "highway": "primary",
      "length_m": 120.5,
      "incident_count": 80,
      "incidents_per_km": 66.39,
      "dominant_crime_type": "Shoplifting",
      "dominant_outcome": "Under investigation",
      "share_of_incidents": 6.67,
      "previous_period_change_pct": 60.0,
      "risk_score": 92.5,
      "band": "red",
      "message": "80 incidents, driven mainly by Shoplifting, 60.0% up vs previous period"
    }
  ],
  "meta": {
    "returned": 1,
    "limit": 25,
    "sort": "risk_score"
  }
}
```

Important interpretation:
- `risk_score` is the best default ranking metric
- it blends incident volume with a smoothed incident-rate metric
- it is safer than sorting only by `incidents_per_km`, which can over-reward tiny segments

Recommended table columns:
- road name
- highway
- incident count
- incidents per km
- dominant crime type
- dominant outcome
- change vs previous period
- band

Recommended row interactions:
- click row -> fetch `/roads/{road_id}/geojson`
- highlight selected road on map
- show inline detail drawer

---

### `GET /roads/analytics/breakdowns`
Use for:
- bar charts
- stacked bars
- composition panels

Best UI uses:
- highway breakdown bar chart
- crime type breakdown bar chart
- outcome breakdown bar chart

Optional query params:
- `from`
- `to`
- bbox
- `crimeType`
- `lastOutcomeCategory`
- `highway`
- `limit`

Example:
```txt
/roads/analytics/breakdowns?from=2023-02&to=2023-06&limit=8
```

Response:
```json
{
  "by_highway": [
    {
      "highway": "residential",
      "segment_count": 3200,
      "length_m": 65432.1,
      "count": 900,
      "share": 75.0,
      "incidents_per_km": 13.75
    }
  ],
  "by_crime_type": [
    {
      "crime_type": "Shoplifting",
      "count": 350,
      "share": 29.17
    }
  ],
  "by_outcome": [
    {
      "outcome": "Under investigation",
      "count": 260,
      "share": 21.67
    }
  ],
  "insights": [
    "Residential roads are the main carrier of incidents in this selection.",
    "Shoplifting is the leading road-linked crime type."
  ]
}
```

Recommended chart mapping:
- `by_highway` -> horizontal bar chart
- `by_crime_type` -> vertical or horizontal bar chart
- `by_outcome` -> horizontal bar chart

This endpoint is the preferred chart source.

---

### `GET /roads/analytics/trends`
Alias:
- `/roads/analytics/timeseries`

Use for:
- line chart
- grouped trend chart
- “how is this changing over time?”

Required query params:
- `from`
- `to`

Optional:
- bbox
- `crimeType`
- `lastOutcomeCategory`
- `highway`
- `groupBy=overall|highway|crime_type|outcome`
- `groupLimit`

Examples:
```txt
/roads/analytics/trends?from=2023-02&to=2023-06
```

```txt
/roads/analytics/trends?from=2023-02&to=2023-06&groupBy=crime_type&groupLimit=5
```

Response:
```json
{
  "groupBy": "crime_type",
  "series": [
    {
      "key": "Shoplifting",
      "total": 500,
      "points": [
        { "month": "2023-02", "count": 210 },
        { "month": "2023-03", "count": 290 }
      ]
    }
  ],
  "total": 700,
  "peak": {
    "month": "2023-03",
    "count": 400
  },
  "current_vs_previous_pct": 12.5,
  "insights": [
    "Peak incident month in this selection is 2023-03.",
    "The current period is 12.5% above the previous matched period."
  ]
}
```

Recommended frontend behavior:
- default to `groupBy=overall` for a clean single line
- let users switch to:
  - `crime_type` to compare crime mixes over time
  - `highway` to compare road classes over time
  - `outcome` to compare linked outcomes over time

Recommended chart types:
- `groupBy=overall` -> single line or area chart
- grouped modes -> multi-line chart or stacked bar chart

Important note:
- use `trends` when you want an explanatory time chart
- use `summary.current_vs_previous_pct` for the headline change metric

---

### `GET /roads/analytics/highways`
Use for:
- a dedicated highway-only view
- a simpler highway chart if you want extra messaging per class

This endpoint is still valid, but in most UIs `breakdowns.by_highway` is the better general choice.

Response adds:
- `share_of_incidents`
- `share_of_length`
- `message`
- `insights`

Recommended use:
- optional secondary “road class interpretation” card

---

## 3. Supporting Map / Selection Endpoints

These help the Roads page map and drill-down interactions.

### `GET /tiles/roads/{z}/{x}/{y}.mvt`
Alias:
- `/tiles/roads/{z}/{x}/{y}.pbf`

Use for:
- road layer rendering in MapLibre / Mapbox / deck.gl

Optional query params:
- `includeRisk=true|false`
- `month=YYYY-MM`
- or `startMonth=YYYY-MM&endMonth=YYYY-MM`
- `crimeType`

Recommended use:
- use vector tiles for the map
- use analytics endpoints for side panels and charts

---

### `GET /roads/{road_id}/geojson`
Use for:
- selected road highlight
- road detail side panel

Response:
- GeoJSON `Feature`

Recommended use:
- when user clicks a road row from the risk table
- when user selects a road on map

---

### `GET /roads/nearest`
Use for:
- click-on-map to nearest road

Query params:
- `lon`
- `lat`

Recommended use:
- map click -> nearest road -> fetch `/roads/{road_id}/geojson` for highlight

---

## 4. Recommended Roads Page Flow

### Initial page load
1. call `/roads/analytics/meta`
2. set default `from` / `to`
3. load the road tile source for the map
4. load `/roads/analytics/summary`
5. load `/roads/analytics/risk`
6. load `/roads/analytics/breakdowns`
7. load `/roads/analytics/trends`

### On map move
Update:
- bbox in filter state
- summary
- risk
- breakdowns
- trends

Do not fetch `/roads` for the main map if you are already using vector tiles.

### On filter change
Refetch:
- summary
- risk
- breakdowns
- trends
- tile source URL if the risk tile overlay depends on time/crime filters

### On risk table row click
1. get `segment_id`
2. call `/roads/{segment_id}/geojson`
3. highlight the feature on map
4. optionally pan/fit to the selected road

---

## 5. Suggested UI Layout

### Top row
- date range picker
- crime type multi-select
- outcome multi-select
- highway multi-select
- viewport toggle:
  - `current map area`
  - `all roads`

### Summary row
- total incidents
- roads with incidents
- avg incidents per km
- top road
- period change

### Main body
Left:
- map with road vector tiles

Right:
- dangerous roads table from `/roads/analytics/risk`

### Lower body
- breakdown charts from `/roads/analytics/breakdowns`
- trends chart from `/roads/analytics/trends`
- optional narrative highway panel from `/roads/analytics/highways`

---

## 6. Frontend Priorities

If time is limited, build in this order:

1. filter state + `/roads/analytics/meta`
2. risk table + `/roads/analytics/risk`
3. summary cards + `/roads/analytics/summary`
4. map vector tiles
5. breakdown charts
6. trends chart
7. selected-road highlight

---

## 7. Notes For The Frontend Agent

- The most important Roads page endpoint is `/roads/analytics/risk`
- The best chart endpoint is `/roads/analytics/breakdowns`
- Use `/roads/analytics/trends` instead of the old mental model of a simple timeseries
- Use `risk_score` as the default ranking metric
- Use `insights` from summary/highways/breakdowns/trends as real UI copy, not just debug data
- Treat `/roads/analytics/highways` as optional or secondary
- Treat `/roads/analytics/anomaly` as secondary; summary and trends already tell the more useful story

